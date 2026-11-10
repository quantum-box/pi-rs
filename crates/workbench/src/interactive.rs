//! Terminal presentation over the same journaled CLI used by automation.
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::ui::{
    editor::Editor,
    images::{self, Cleanup, InlineImage, Renderer},
    keys::Keys,
    picker::{Input, Item, Picker, Selection, TreeEntryKind, TreeFilter},
    render::{Entry, Line, ToolPart},
    theme::{Theme, ThemeWatcher},
};
use crossterm::cursor::{Hide, MoveDown, MoveTo, MoveToColumn, MoveUp, Show};
use crossterm::event::{
    DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEventKind, KeyModifiers,
};
use crossterm::style::{Print, ResetColor};
use crossterm::terminal::{self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::{execute, queue};
use pi_rs::{Error, Result, Session};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use super::{Cli, TuiMode, process::Worker, sessions};

pub(super) fn new_session_path() -> PathBuf {
    session_path(&std::env::current_dir().unwrap_or_default(), None)
}

pub(super) fn session_path(
    workspace: &std::path::Path,
    directory: Option<&std::path::Path>,
) -> PathBuf {
    let workspace = workspace
        .canonicalize()
        .unwrap_or_else(|_| workspace.into());
    let hash = Sha256::digest(workspace.as_os_str().as_encoded_bytes());
    directory
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            crate::config::agent_dir()
                .join("sessions")
                .join(format!("{hash:x}"))
        })
        .join(format!("{}.jsonl", uuid::Uuid::new_v4()))
}

pub(super) fn latest_session(
    example: &std::path::Path,
    workspace: &std::path::Path,
) -> Result<PathBuf> {
    let workspace = workspace.canonicalize()?;
    let directory = example
        .parent()
        .ok_or_else(|| Error::Config("session directory unavailable".into()))?;
    let latest = std::fs::read_dir(directory)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|value| value == "jsonl")
        })
        .filter(|entry| {
            super::session_import::read(&entry.path()).is_ok_and(|session| {
                session.working_directory().is_some_and(|cwd| {
                    std::path::Path::new(cwd)
                        .canonicalize()
                        .is_ok_and(|cwd| cwd == workspace)
                })
            })
        })
        .filter_map(|entry| {
            entry
                .metadata()
                .ok()?
                .modified()
                .ok()
                .map(|time| (time, entry.path()))
        })
        .max_by_key(|(time, _)| *time);
    latest
        .map(|(_, path)| path)
        .ok_or_else(|| Error::Config("no saved session in this directory".into()))
}

struct Terminal {
    alternate: bool,
    images: Cleanup,
}
impl Terminal {
    fn enter(mode: Option<TuiMode>, images: Cleanup) -> Result<Self> {
        terminal::enable_raw_mode()?;
        let guard = Self {
            alternate: mode != Some(TuiMode::Regular),
            images,
        };
        if guard.alternate {
            execute!(std::io::stdout(), EnterAlternateScreen)?;
        }
        execute!(std::io::stdout(), EnableBracketedPaste, Hide)?;
        #[cfg(any(unix, windows))]
        execute!(std::io::stdout(), Print("\x1b[?2031h\x1b[?996n"))?;
        Ok(guard)
    }
    #[cfg(unix)]
    fn suspend(guard: &mut Option<Self>, mode: Option<TuiMode>, images: Cleanup) -> Result<()> {
        drop(guard.take());
        // The foreground job owns this process group, including its frontend worker.
        let result = unsafe {
            let mut previous: libc::sigaction = std::mem::zeroed();
            let mut ignored: libc::sigaction = std::mem::zeroed();
            ignored.sa_sigaction = libc::SIG_IGN;
            libc::sigemptyset(&mut ignored.sa_mask);
            if libc::sigaction(libc::SIGINT, &ignored, &mut previous) != 0 {
                Err(std::io::Error::last_os_error())
            } else {
                let status = libc::kill(0, libc::SIGTSTP);
                let result = if status == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                };
                if libc::sigaction(libc::SIGINT, &previous, std::ptr::null_mut()) != 0 {
                    Err(std::io::Error::last_os_error())
                } else {
                    result
                }
            }
        };
        *guard = Some(Self::enter(mode, images)?);
        result.map_err(Error::Io)
    }
}
impl Drop for Terminal {
    fn drop(&mut self) {
        if self.alternate {
            self.images.clear();
        }
        #[cfg(any(unix, windows))]
        let _ = execute!(std::io::stdout(), Print("\x1b[?2031l"));
        let _ = execute!(std::io::stdout(), ResetColor, Show, DisableBracketedPaste);
        if self.alternate {
            let _ = execute!(std::io::stdout(), LeaveAlternateScreen);
        } else {
            let _ = execute!(std::io::stdout(), Print("\r\n"));
        }
        let _ = terminal::disable_raw_mode();
    }
}

#[derive(PartialEq)]
struct RegularLayout {
    width: usize,
    height: usize,
    cell: (f64, f64),
    theme: Theme,
    expand_tools: bool,
    hide_thinking: bool,
}
#[derive(Default)]
struct RegularState {
    committed: usize,
    cursor_row: usize,
    region_height: usize,
    started: bool,
    redraw: bool,
    layout: Option<RegularLayout>,
}

#[derive(Default)]
struct View {
    regular: RegularState,
    graphics: Renderer,
    dialogs: std::collections::VecDeque<crate::ui::dialog::Dialog>,
    setting_editor: Option<(String, crate::ui::dialog::Dialog)>,
    ui_status: std::collections::BTreeMap<String, String>,
    widgets: std::collections::BTreeMap<String, (Vec<String>, pi_rs::WidgetPlacement)>,
    lines: Vec<Entry>,
    picker: Option<Picker>,
    session_name: Option<String>,
    resources: super::resources::Resources,
    model_list: Option<std::sync::mpsc::Receiver<Result<Vec<String>>>>,
    available_models: Vec<String>,
    pending_request: Option<Value>,
    model_cycle: Option<bool>,
    thinking: String,
    expand_tools: bool,
    hide_thinking: bool,
    streaming: String,
    editor: Editor,
    draft_images: Vec<pi_rs::ImageContent>,
    clipboard: Option<std::sync::mpsc::Receiver<Result<super::clipboard::Content>>>,
    scroll: usize,
    tokens: u64,
    history: Vec<String>,
    history_index: Option<usize>,
    request_started: Option<Instant>,
    phase: Option<&'static str>,
    restoring_queue: bool,
}
impl View {
    fn dialog(&self) -> Option<&crate::ui::dialog::Dialog> {
        self.setting_editor
            .as_ref()
            .map(|(_, dialog)| dialog)
            .or_else(|| self.dialogs.front())
    }
    fn ui_request(&mut self, value: &Value) -> Result<Option<(uuid::Uuid, pi_rs::UiResponse)>> {
        let id: uuid::Uuid = serde_json::from_value(value["id"].clone())?;
        let request: pi_rs::UiRequest = serde_json::from_value(value.clone())?;
        request.validate().map_err(Error::Config)?;
        match request {
            pi_rs::UiRequest::Notify { message, .. } => self.append(&message),
            pi_rs::UiRequest::SetStatus { key, text } => {
                if text.is_some()
                    && !self.ui_status.contains_key(&key)
                    && self.ui_status.len() >= 64
                {
                    return Err(Error::Config("too many extension status entries".into()));
                }
                if let Some(text) = text {
                    self.ui_status.insert(key, text);
                } else {
                    self.ui_status.remove(&key);
                }
            }
            pi_rs::UiRequest::SetWidget {
                key,
                lines,
                placement,
            } => {
                if lines.is_some() && !self.widgets.contains_key(&key) && self.widgets.len() >= 64 {
                    return Err(Error::Config("too many extension widgets".into()));
                }
                if let Some(lines) = lines {
                    self.widgets.insert(key, (lines, placement));
                } else {
                    self.widgets.remove(&key);
                }
            }
            pi_rs::UiRequest::SetTitle { title } => execute!(
                std::io::stdout(),
                crossterm::terminal::SetTitle(
                    title
                        .chars()
                        .filter(|character| !character.is_control())
                        .collect::<String>()
                )
            )?,
            pi_rs::UiRequest::SetEditorText { text } => self.editor.replace(text),
            pi_rs::UiRequest::GetEditorText => {
                return Ok(Some((
                    id,
                    pi_rs::UiResponse::Text(self.editor.text.clone()),
                )));
            }
            request => {
                self.scroll = 0;
                self.dialogs
                    .push_back(crate::ui::dialog::Dialog::new(id, request));
            }
        }
        Ok(None)
    }
    fn widget_lines(
        &self,
        theme: &Theme,
        width: usize,
        placement: pi_rs::WidgetPlacement,
    ) -> Vec<Line> {
        self.widgets
            .values()
            .filter(|(_, location)| *location == placement)
            .flat_map(|(lines, _)| lines.iter())
            .flat_map(|text| Entry::Notice(text.clone()).render(theme, width, false, true))
            .collect()
    }
    fn extension_status(&self) -> String {
        if self.ui_status.is_empty() {
            String::new()
        } else {
            format!(
                "{} · ",
                self.ui_status
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" · ")
            )
        }
    }
    fn draft_status(&self) -> String {
        if self.clipboard.is_some() {
            "Reading clipboard · ".into()
        } else if !self.draft_images.is_empty() {
            format!("{} image(s) attached · ", self.draft_images.len())
        } else {
            String::new()
        }
    }
    fn draft_message(&self, content: &str) -> pi_rs::QueuedMessage {
        pi_rs::QueuedMessage {
            input: pi_rs::QueuedInput {
                id: uuid::Uuid::nil(),
                kind: pi_rs::InputKind::FollowUp,
                content: content.into(),
            },
            images: self.draft_images.clone(),
        }
    }
    fn paste_clipboard(&mut self) {
        if self.clipboard.is_some() {
            return;
        }
        let (sender, receiver) = std::sync::mpsc::channel();
        self.clipboard = Some(receiver);
        std::thread::spawn(move || {
            let _ = sender.send(super::clipboard::read());
        });
    }
    fn poll_clipboard(&mut self) -> bool {
        let Some(receiver) = self.clipboard.take() else {
            return false;
        };
        let result = match receiver.try_recv() {
            Ok(result) => result,
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                self.clipboard = Some(receiver);
                return false;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                Err(Error::Config("clipboard reader stopped".into()))
            }
        };
        match result {
            Ok(super::clipboard::Content::Text(text)) => self.editor.insert(&text),
            Ok(super::clipboard::Content::Files(paths)) => {
                match super::clipboard::file_text(
                    &paths,
                    self.editor.text.trim_start().starts_with('!'),
                ) {
                    Ok(text) => {
                        let before = self.editor.text[..self.editor.cursor].chars().next_back();
                        let after = self.editor.text[self.editor.cursor..].chars().next();
                        let leading = if before.is_some_and(|value| !value.is_whitespace()) {
                            " "
                        } else {
                            ""
                        };
                        let trailing = if after.is_some_and(|value| !value.is_whitespace()) {
                            " "
                        } else {
                            ""
                        };
                        self.editor.insert(&format!("{leading}{text}{trailing}"));
                    }
                    Err(error) => self.append(&error.to_string()),
                }
            }
            Ok(super::clipboard::Content::Image(image)) => {
                self.draft_images.push(image);
                if let Err(error) = self.draft_message(&self.editor.text).validate() {
                    self.draft_images.pop();
                    self.append(&error.to_string());
                }
            }
            Err(error) => self.append(&error.to_string()),
        }
        true
    }
    fn append(&mut self, text: &str) {
        self.lines.push(Entry::Notice(clean(text)));
        if self.lines.len() > 4000 {
            let removed = self.lines.len() - 4000;
            self.lines.drain(..removed);
            self.regular.committed = self.regular.committed.saturating_sub(removed);
        }
    }
    fn restore(&mut self, args: &Cli) -> Result<()> {
        let session = Session::open(&args.session)?;
        if session.host_binding().is_some() {
            return Err(Error::Session(
                "a managed session must be opened by its host".into(),
            ));
        }
        self.graphics.reset();
        self.session_name = session.name().map(str::to_owned);
        self.regular.committed = 0;
        self.regular.redraw = true;
        self.lines.clear();
        self.streaming.clear();
        self.thinking.clear();
        self.draft_images.clear();
        self.clipboard = None;
        self.dialogs.clear();
        self.setting_editor = None;
        self.restoring_queue = false;
        self.ui_status.clear();
        self.widgets.clear();
        self.history.clear();
        self.history_index = None;
        self.tokens = 0;
        for record in session.branch() {
            match &record.event {
                pi_rs::Event::User { content, .. }
                | pi_rs::Event::UserWithImages { content, .. } => {
                    self.history.push(content.clone());
                    self.lines.push(Entry::User(content.clone()));
                    if let pi_rs::Event::UserWithImages { images, .. } = &record.event {
                        self.append(&format!("{} attached image(s)", images.len()));
                        self.add_images(images);
                    }
                }
                pi_rs::Event::BashExecution { execution } => {
                    self.lines.push(Entry::Tool {
                        id: format!("shell-{}", record.id),
                        name: if execution.exclude_from_context {
                            "bash (excluded)"
                        } else {
                            "bash"
                        }
                        .into(),
                        arguments: json!({"command":execution.command}).to_string(),
                        output: execution.context_text(),
                        failed: execution.cancelled
                            || execution.exit_code.is_some_and(|code| code != 0),
                        finished: true,
                        parts: Vec::new(),
                        numbered_diff: false,
                    });
                }
                pi_rs::Event::UserImages { images } => {
                    self.append(&format!("{} attached image(s)", images.len()));
                    self.add_images(images);
                }
                pi_rs::Event::ModelFinished {
                    metadata, usage, ..
                } => {
                    if metadata.finish_reason.as_deref() == Some("length") {
                        self.append("Response reached the output token limit.");
                    }
                    if let Some(reasoning) = &metadata.reasoning {
                        self.lines.push(Entry::Thinking(reasoning.clone()));
                    }
                    if let pi_rs::UsageReport::Measured { usage } = usage {
                        self.tokens = self
                            .tokens
                            .saturating_add(usage.input_tokens)
                            .saturating_add(usage.output_tokens);
                    }
                }
                pi_rs::Event::Assistant {
                    content,
                    invocations,
                    ..
                } => {
                    if !content.is_empty() {
                        self.lines.push(Entry::Assistant(content.clone()));
                    }
                    for invocation in invocations {
                        self.add_tool(invocation);
                    }
                }
                pi_rs::Event::ToolPlanned { invocation, .. } => self.add_tool(invocation),
                pi_rs::Event::ToolFinished {
                    operation_id,
                    outcome,
                    ..
                }
                | pi_rs::Event::ToolReconciled {
                    operation_id,
                    outcome,
                } => {
                    self.finish_tool(&operation_id.to_string(), outcome);
                }
                _ => {}
            }
        }
        self.append(if crate::ui::keys::windows_keybindings() {
            "Enter to send · Ctrl+Q to queue · Ctrl+O tools · Ctrl+T thinking · /help"
        } else {
            "Enter to send · Alt+Enter to queue · Ctrl+O tools · Ctrl+T thinking · /help"
        });
        Ok(())
    }
    fn add_images(&mut self, images: &[pi_rs::ImageContent]) {
        if self.graphics.enabled() {
            self.lines.extend(
                images
                    .iter()
                    .filter_map(InlineImage::decode)
                    .map(Entry::Image),
            );
        }
    }
    fn add_tool(&mut self, invocation: &pi_rs::Invocation) {
        self.lines.push(Entry::Tool {
            id: invocation.operation_id.to_string(),
            name: invocation.call.name.clone(),
            arguments: invocation.call.arguments.clone(),
            output: String::new(),
            failed: false,
            finished: false,
            parts: Vec::new(),
            numbered_diff: false,
        });
    }
    fn finish_tool(&mut self, operation: &str, outcome: &pi_rs::Outcome) {
        let rendered_parts = if self.graphics.enabled() {
            match outcome {
                pi_rs::Outcome::Structured { data, .. } => data["content"]
                    .as_array()
                    .filter(|blocks| blocks.iter().any(|block| block["type"] == "image"))
                    .into_iter()
                    .flatten()
                    .filter_map(|value| match value["type"].as_str() {
                        Some("text") => value["text"]
                            .as_str()
                            .map(|text| ToolPart::Text(text.into())),
                        Some("image") => {
                            let image =
                                serde_json::from_value::<pi_rs::ImageContent>(value.clone())
                                    .ok()
                                    .and_then(|content| InlineImage::decode(&content));
                            Some(match image {
                                Some(image) => ToolPart::Image(image),
                                None => ToolPart::Text("[image unavailable]".into()),
                            })
                        }
                        _ => None,
                    })
                    .collect(),
                _ => Vec::new(),
            }
        } else {
            Vec::new()
        };
        if let Some(Entry::Tool {
            output,
            failed,
            finished,
            parts,
            name,
            numbered_diff,
            ..
        }) = self
            .lines
            .iter_mut()
            .rev()
            .find(|entry| matches!(entry, Entry::Tool { id, .. } if id == operation))
        {
            *parts = rendered_parts;
            let diff = match outcome {
                pi_rs::Outcome::SucceededWithDetails { details, .. } if name == "edit" => {
                    details["diff"].as_str().filter(|diff| !diff.is_empty())
                }
                _ => None,
            };
            *numbered_diff = diff.is_some();
            *output = diff.map_or_else(|| display_tool_result(outcome), str::to_owned);
            *failed = !matches!(
                outcome,
                pi_rs::Outcome::Succeeded(_)
                    | pi_rs::Outcome::SucceededWithDetails { .. }
                    | pi_rs::Outcome::Structured { .. }
            );
            *finished = true;
        }
    }
    fn receive(&mut self, value: Value) {
        match value["type"].as_str() {
            Some("response") if value["command"] == "clear_queue" && self.restoring_queue => {
                self.restoring_queue = false;
                if value["success"] == true {
                    if let Ok(messages) = serde_json::from_value::<Vec<pi_rs::QueuedMessage>>(value["data"]["messages"].clone()) {
                        let mut text: Vec<_> = messages.iter().map(|message| message.input.content.clone()).filter(|text| !text.is_empty()).collect();
                        if !self.editor.text.is_empty() { text.push(self.editor.text.clone()); }
                        self.editor.replace(text.join("\n\n"));
                        let mut images: Vec<_> = messages.into_iter().flat_map(|message| message.images).collect();
                        images.append(&mut self.draft_images);
                        self.draft_images = images;
                    }
                } else { self.append(value["error"].as_str().unwrap_or("Queue restore failed")); }
            }
            Some("bash_execution_start") => {
                self.phase = Some("コマンド実行中");
                self.lines.push(Entry::Tool {
                    id: value["executionId"].as_str().unwrap_or_default().into(),
                    name: if value["excludeFromContext"] == true { "bash (excluded)" } else { "bash" }.into(),
                    arguments: json!({"command":value["command"]}).to_string(),
                    output: String::new(),
                    failed: false,
                    finished: false,
                    parts: Vec::new(),
                    numbered_diff: false,
                });
            }
            Some("bash_execution_update") => {
                if let Some(Entry::Tool { output, .. }) = self.lines.iter_mut().rev().find(|entry| matches!(entry, Entry::Tool { id, .. } if id == value["executionId"].as_str().unwrap_or_default())) {
                    output.push_str(value["output"].as_str().unwrap_or_default());
                    if output.len() > 50 * 1024 {
                        let mut start = output.len() - 50 * 1024;
                        while !output.is_char_boundary(start) { start += 1; }
                        output.drain(..start);
                    }
                }
            }
            Some("bash_execution_end") => {
                let outcome = match serde_json::from_value::<pi_rs::BashExecution>(value["data"].clone()) {
                    Ok(execution) if !execution.cancelled && execution.exit_code == Some(0) => pi_rs::Outcome::Succeeded(execution.context_text()),
                    Ok(execution) => pi_rs::Outcome::Failed(execution.context_text()),
                    Err(_) => pi_rs::Outcome::Failed(value["error"].as_str().unwrap_or("Shell execution failed").into()),
                };
                self.finish_tool(value["executionId"].as_str().unwrap_or_default(), &outcome);
            }
            Some("auto_retry_start") => {
                self.phase = Some("再試行待ち");
                self.append(&format!("Retry {}/{} · {} ms · {}", value["attempt"], value["maxAttempts"], value["delayMs"], value["errorMessage"].as_str().unwrap_or_default()));
            }
            Some("auto_retry_end") => { self.phase = None; }
            Some("model_progress") => {
                self.phase = Some(match value["stage"].as_str() {
                    Some("loading_model") => "モデル読込待ち",
                    Some("thinking") => "思考中",
                    Some("generating") => "回答生成中",
                    Some("preparing_tool") => "ツール呼出し準備中",
                    _ => "モデル応答待ち",
                });
            }
            Some("reasoning_delta") => self
                .thinking
                .push_str(value["delta"].as_str().unwrap_or_default()),
            Some("text_delta") => self
                .streaming
                .push_str(value["delta"].as_str().unwrap_or_default()),
            Some("mcp_server") => self.append(&format!(
                "MCP {} · {} · {}",
                value["name"].as_str().unwrap_or(""),
                value["status"].as_str().unwrap_or(""),
                value["error"].as_str().unwrap_or("")
            )),
            Some("error") => self.append(&format!(
                "Error › {}",
                value["message"].as_str().unwrap_or_default()
            )),
            Some("record") => {
                let event = &value["record"]["event"];
                match event["type"].as_str() {
                    Some("user_with_images" | "user_images") => {
                        if let Ok(images) = serde_json::from_value::<Vec<pi_rs::ImageContent>>(
                            event["images"].clone(),
                        ) {
                            self.add_images(&images);
                        }
                    }
                    Some("model_requested") => {
                        self.request_started = Some(Instant::now());
                        self.phase = Some("モデル応答待ち");
                    }
                    Some("assistant") => {
                        self.streaming.clear();
                        let thinking = std::mem::take(&mut self.thinking);
                        if !thinking.is_empty() {
                            self.lines.push(Entry::Thinking(thinking));
                        }
                        let content = event["content"].as_str().unwrap_or_default();
                        if !content.is_empty() {
                            self.lines.push(Entry::Assistant(content.into()));
                        }
                        if let Some(calls) = event["invocations"].as_array() {
                            if !calls.is_empty() {
                                self.phase = Some("ツール実行中");
                            }
                            for call in calls {
                                self.lines.push(Entry::Tool {
                                    id: call["operation_id"].as_str().unwrap_or_default().into(),
                                    name: call["call"]["name"].as_str().unwrap_or("tool").into(),
                                    arguments: call["call"]["arguments"]
                                        .as_str()
                                        .unwrap_or_default()
                                        .into(),
                                    output: String::new(),
                                    failed: false,
                                    finished: false,
                                    parts: Vec::new(),
                                    numbered_diff: false,
                                });
                            }
                        }
                    }
                    Some("tool_planned") => {
                        self.phase = Some("ツール実行中");
                        if let Ok(invocation) = serde_json::from_value(event["invocation"].clone())
                        {
                            self.add_tool(&invocation);
                        }
                    }
                    Some("tool_finished" | "tool_reconciled") => {
                        self.phase = Some("処理を継続中");
                        if let Ok(outcome) = serde_json::from_value(event["outcome"].clone()) {
                            self.finish_tool(
                                event["operation_id"].as_str().unwrap_or_default(),
                                &outcome,
                            );
                        }
                    }
                    Some("model_finished") => {
                        if event["metadata"]["finish_reason"] == "length" {
                            self.append("Response reached the output token limit.");
                        }
                        self.phase = Some("結果を保存中");
                        let usage = &event["usage"]["usage"];
                        self.tokens = self
                            .tokens
                            .saturating_add(usage["input_tokens"].as_u64().unwrap_or(0))
                            .saturating_add(usage["output_tokens"].as_u64().unwrap_or(0));
                    }
                    Some("input_queued") => self.append("Input queued"),
                    _ => {}
                }
            }
            _ => {}
        }
    }
    fn settle(&mut self) {
        if !self.thinking.is_empty() {
            self.lines
                .push(Entry::Thinking(std::mem::take(&mut self.thinking)));
        }
        if !self.streaming.is_empty() {
            self.lines
                .push(Entry::Assistant(std::mem::take(&mut self.streaming)));
        }
        self.request_started = None;
        self.phase = None;
    }
    fn draw(&mut self, args: &Cli, theme: &Theme, busy: bool) -> Result<()> {
        if args.tui_mode == Some(TuiMode::Regular) {
            return self.draw_regular(args, theme, busy);
        }
        let (width, height) = terminal::size()?;
        let width = usize::from(width.max(1));
        let height = usize::from(height.max(7));
        let prompt = wrap(&self.editor.text, width);
        let prompt_height = prompt.len().clamp(1, 6).min(height.saturating_sub(5));
        let mut below = self.widget_lines(theme, width, pi_rs::WidgetPlacement::BelowEditor);
        below.truncate(height.saturating_sub(prompt_height + 6));
        if self.dialog().is_some() {
            below.clear();
        }
        let body_height = height.saturating_sub(prompt_height + 5 + below.len());
        let mut lines: Vec<Line> = self
            .lines
            .iter()
            .flat_map(|entry| entry.render(theme, width, self.expand_tools, !self.hide_thinking))
            .collect();
        if !self.thinking.is_empty() {
            lines.extend(Entry::Thinking(self.thinking.clone()).render(
                theme,
                width,
                false,
                !self.hide_thinking,
            ));
        }
        if !self.streaming.is_empty() {
            lines
                .extend(Entry::Assistant(self.streaming.clone()).render(theme, width, false, true));
        }
        if let Some(picker) = &self.picker {
            lines = picker.render(theme, width, body_height);
        }
        if let Some(dialog) = self.dialog() {
            lines = dialog.render(theme, width, body_height);
        } else {
            lines.extend(self.widget_lines(theme, width, pi_rs::WidgetPlacement::AboveEditor));
        }
        let end = lines
            .len()
            .saturating_sub(self.scroll.min(lines.len().saturating_sub(body_height)));
        let start = end.saturating_sub(body_height);
        let mut out = std::io::stdout().lock();
        let elapsed = self
            .request_started
            .map(|start| start.elapsed())
            .unwrap_or_default();
        let status = if busy {
            let spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
            format!(
                "{} {} {:.1}s · Escで取消 ",
                spinner[(elapsed.as_millis() / 100) as usize % spinner.len()],
                self.phase.unwrap_or("処理中"),
                elapsed.as_secs_f32()
            )
        } else {
            String::new()
        };
        let status = format!("{status}{}{}", self.draft_status(), self.extension_status());
        self.graphics.clear_placements(&mut out)?;
        queue!(out, Hide, MoveTo(0, 0), Clear(ClearType::All))?;
        Line::plain(
            &format!(
                "pi v{} · {} · /help",
                env!("CARGO_PKG_VERSION"),
                self.session_name.as_deref().unwrap_or("new session")
            ),
            theme.color("accent"),
        )
        .draw(&mut out, width)?;
        for (row, line) in lines[start..end].iter().enumerate() {
            queue!(out, MoveTo(0, (row + 1) as u16))?;
            line.draw(&mut out, width)?;
            self.graphics.paint(
                line,
                &mut out,
                row,
                images::visible_rows(&lines[start + row..end]),
            )?;
        }
        let border_row = height - prompt_height - 4 - below.len();
        queue!(out, MoveTo(0, border_row as u16))?;
        Line::plain(
            &format!(
                "{}{}",
                status,
                "─".repeat(width.saturating_sub(UnicodeWidthStr::width(status.as_str())))
            ),
            theme.color("border"),
        )
        .draw(&mut out, width)?;
        let cursor_prefix = wrap(&self.editor.text[..self.editor.cursor], width);
        let cursor_row = cursor_prefix.len().saturating_sub(1);
        let offset = cursor_row.saturating_sub(prompt_height - 1);
        for (row, line) in prompt.iter().skip(offset).take(prompt_height).enumerate() {
            queue!(out, MoveTo(0, (border_row + 1 + row) as u16))?;
            Line::plain(line, theme.color("text")).draw(&mut out, width)?;
        }
        for (row, line) in below.iter().enumerate() {
            queue!(
                out,
                MoveTo(0, (border_row + 1 + prompt_height + row) as u16)
            )?;
            line.draw(&mut out, width)?;
        }
        queue!(out, MoveTo(0, (height - 3) as u16))?;
        Line::plain(&"─".repeat(width), theme.color("border")).draw(&mut out, width)?;
        queue!(out, MoveTo(0, (height - 2) as u16))?;
        Line::plain(&args.workspace.display().to_string(), theme.color("dim"))
            .draw(&mut out, width)?;
        queue!(out, MoveTo(0, (height - 1) as u16))?;
        let model = format!(
            "{} · {}",
            args.model,
            args.thinking.as_deref().unwrap_or("default")
        );
        let model_width = UnicodeWidthStr::width(model.as_str());
        let usage = format!("↑↓ 確定 {} tokens", self.tokens);
        let usage = clip(&usage, width.saturating_sub(model_width + 1));
        let padding = width.saturating_sub(UnicodeWidthStr::width(usage.as_str()) + model_width);
        Line::plain(
            &format!("{usage}{}{model}", " ".repeat(padding)),
            theme.color("dim"),
        )
        .draw(&mut out, width)?;
        let column = cursor_prefix
            .last()
            .map_or(0, |line| UnicodeWidthStr::width(line.as_str()))
            .min(width - 1);
        if let Some(dialog) = self.dialog() {
            if let Some((row, column)) = dialog.cursor(width, body_height) {
                queue!(out, MoveTo(column as u16, (row + 1) as u16), Show)?;
            } else {
                queue!(out, Hide)?;
            }
        } else {
            queue!(
                out,
                MoveTo(column as u16, (border_row + 1 + cursor_row - offset) as u16),
                Show
            )?;
        }
        out.flush()?;
        Ok(())
    }
    fn finish_regular(&mut self) -> Result<()> {
        if self.regular.started {
            execute!(
                std::io::stdout(),
                MoveDown(
                    self.regular
                        .region_height
                        .saturating_sub(self.regular.cursor_row + 1) as u16
                ),
                MoveToColumn(0),
                Print("\r\n")
            )?;
            self.regular.started = false;
        }
        Ok(())
    }

    fn draw_regular(&mut self, args: &Cli, theme: &Theme, busy: bool) -> Result<()> {
        let (width, height) = terminal::size()?;
        let width = usize::from(width.max(1));
        let height = usize::from(height.max(7));
        let layout = RegularLayout {
            width,
            height,
            cell: images::cell_dimensions(),
            theme: theme.clone(),
            expand_tools: self.expand_tools,
            hide_thinking: self.hide_thinking,
        };
        let redraw = self.regular.layout.is_some()
            && (self.regular.redraw
                || self.regular.layout.as_ref() != Some(&layout)
                || self.lines.len() < self.regular.committed);
        self.regular.layout = Some(layout);
        self.regular.redraw = false;
        if self.lines.len() < self.regular.committed {
            self.regular.committed = 0;
        }
        let prompt = wrap(&self.editor.text, width);
        let prompt_height = prompt.len().clamp(1, 6).min(height - 5);
        let mut below = self.widget_lines(theme, width, pi_rs::WidgetPlacement::BelowEditor);
        below.truncate(height.saturating_sub(prompt_height + 6));
        if self.dialog().is_some() {
            below.clear();
        }
        let mut out = std::io::stdout().lock();
        if redraw {
            self.graphics.clear_placements(&mut out)?;
            queue!(
                out,
                Clear(ClearType::All),
                MoveTo(0, 0),
                Clear(ClearType::Purge)
            )?;
        }
        if !redraw {
            self.graphics.clear_active(&mut out)?;
        }
        queue!(out, Hide, MoveToColumn(0))?;
        if self.regular.started && !redraw {
            queue!(
                out,
                MoveUp(self.regular.cursor_row as u16),
                Clear(ClearType::FromCursorDown)
            )?;
        } else {
            queue!(out, Print("\r\n"))?;
            if self.regular.committed == 0 || redraw {
                Line::plain(
                    &format!("pi v{} · /help", env!("CARGO_PKG_VERSION")),
                    theme.color("accent"),
                )
                .draw(&mut out, width)?;
                queue!(out, Print("\r\n"))?;
            }
        }
        if redraw {
            for entry in &self.lines[..self.regular.committed] {
                for line in entry.render(theme, width, self.expand_tools, !self.hide_thinking) {
                    line.draw(&mut out, width)?;
                    self.graphics
                        .paint(&line, &mut out, usize::MAX, usize::MAX)?;
                    queue!(out, Print("\r\n"))?;
                }
            }
        }
        let mut active = Vec::new();
        if !busy && self.picker.is_none() && self.dialog().is_none() && self.scroll == 0 {
            for entry in &self.lines[self.regular.committed..] {
                for line in entry.render(theme, width, self.expand_tools, !self.hide_thinking) {
                    line.draw(&mut out, width)?;
                    self.graphics
                        .paint(&line, &mut out, usize::MAX, usize::MAX)?;
                    queue!(out, Print("\r\n"))?;
                }
            }
            self.regular.committed = self.lines.len();
        } else {
            let start = if self.scroll > 0 {
                0
            } else {
                self.regular.committed
            };
            active.extend(self.lines[start..].iter().flat_map(|entry| {
                entry.render(theme, width, self.expand_tools, !self.hide_thinking)
            }));
            active.extend(Entry::Thinking(self.thinking.clone()).render(
                theme,
                width,
                false,
                !self.hide_thinking,
            ));
            active
                .extend(Entry::Assistant(self.streaming.clone()).render(theme, width, false, true));
            if let Some(picker) = &self.picker {
                active = picker.render(theme, width, height - prompt_height - 4 - below.len());
            }
            let end = active
                .len()
                .saturating_sub(self.scroll.min(active.len().saturating_sub(1)));
            active = active[..end]
                .iter()
                .rev()
                .take(height - prompt_height - 4 - below.len())
                .cloned()
                .collect();
            active.reverse();
        }
        if let Some(dialog) = self.dialog() {
            active = dialog.render(theme, width, height - prompt_height - 4 - below.len());
        } else {
            let available = height - prompt_height - 4 - below.len();
            let mut widgets = self.widget_lines(theme, width, pi_rs::WidgetPlacement::AboveEditor);
            widgets.truncate(available);
            let keep = available.saturating_sub(widgets.len());
            if active.len() > keep {
                active.drain(..active.len() - keep);
            }
            active.extend(widgets);
        }
        for (row, line) in active.iter().enumerate() {
            line.draw(&mut out, width)?;
            self.graphics.paint_active(
                line,
                &mut out,
                row,
                images::visible_rows(&active[row..]),
            )?;
            queue!(out, Print("\r\n"))?;
        }
        let elapsed = self
            .request_started
            .map(|start| start.elapsed().as_secs_f32())
            .unwrap_or_default();
        let status = if busy {
            format!(
                "{} {elapsed:.1}s · Escで取消 ",
                self.phase.unwrap_or("処理中")
            )
        } else {
            String::new()
        };
        let status = format!("{status}{}{}", self.draft_status(), self.extension_status());
        Line::plain(
            &format!(
                "{}{}",
                status,
                "─".repeat(width.saturating_sub(UnicodeWidthStr::width(status.as_str())))
            ),
            theme.color("border"),
        )
        .draw(&mut out, width)?;
        queue!(out, Print("\r\n"))?;
        let prefix = wrap(&self.editor.text[..self.editor.cursor], width);
        let cursor = prefix.len().saturating_sub(1);
        let offset = cursor.saturating_sub(prompt_height - 1);
        for line in prompt.iter().skip(offset).take(prompt_height) {
            Line::plain(line, theme.color("text")).draw(&mut out, width)?;
            queue!(out, Print("\r\n"))?;
        }
        for line in &below {
            line.draw(&mut out, width)?;
            queue!(out, Print("\r\n"))?;
        }
        for line in [
            Line::plain(&"─".repeat(width), theme.color("border")),
            Line::plain(&args.workspace.display().to_string(), theme.color("dim")),
        ] {
            line.draw(&mut out, width)?;
            queue!(out, Print("\r\n"))?;
        }
        Line::plain(
            &format!(
                "↑↓ {} tokens · {} · {}",
                self.tokens,
                args.model,
                args.thinking.as_deref().unwrap_or("default")
            ),
            theme.color("dim"),
        )
        .draw(&mut out, width)?;
        self.regular.cursor_row = active.len() + 1 + cursor - offset;
        self.regular.region_height = active.len() + prompt_height + 4 + below.len();
        let column = prefix
            .last()
            .map_or(0, |line| UnicodeWidthStr::width(line.as_str()))
            .min(width - 1);
        let (column, show_cursor) = if let Some(dialog) = self.dialog() {
            let position = dialog.cursor(width, height - prompt_height - 4 - below.len());
            self.regular.cursor_row = position.map_or(0, |(row, _)| row);
            (position.map_or(0, |(_, column)| column), position.is_some())
        } else {
            (column, true)
        };
        queue!(
            out,
            MoveUp((self.regular.region_height - self.regular.cursor_row - 1) as u16),
            MoveToColumn(column as u16)
        )?;
        if show_cursor {
            queue!(out, Show)?;
        } else {
            queue!(out, Hide)?;
        }
        self.regular.started = true;
        out.flush()?;
        Ok(())
    }
}

fn display_tool_result(outcome: &pi_rs::Outcome) -> String {
    if let pi_rs::Outcome::SucceededWithDetails { output, details } = outcome
        && let Some(patch) = details["patch"].as_str().filter(|patch| !patch.is_empty())
    {
        return format!("{output}\n{patch}");
    }
    if let pi_rs::Outcome::Structured { data, .. } = outcome
        && let Some(blocks) = data["content"].as_array()
    {
        return blocks
            .iter()
            .filter_map(|block| match block["type"].as_str() {
                Some("text") => block["text"].as_str().map(str::to_owned),
                Some("image") => Some(format!(
                    "[image · {}]",
                    block["mimeType"].as_str().unwrap_or("image")
                )),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n");
    }
    outcome.tool_content()
}

const HELP: &str = "/help  /new  /resume [path]  /session  /model <name>  /compact  /tree  /checkout <id>  /quit\n/extensions  /extension <id> <command> [arguments]\n/context-edit <id> <text>  /context-delete <id>\n/notes [text]  /todo [JSON list]  /plan <text|accept|discard>  /review <task>\nShift+Enter inserts a newline. PageUp/PageDown scroll. Up/Down recall prompts.\nEnter while working steers the next model response. Alt+Enter queues a follow-up.";

fn slash(input: &str, args: &mut Cli, view: &mut View) -> Result<Option<Worker>> {
    let (name, rest) = input.split_once(char::is_whitespace).unwrap_or((input, ""));
    let rest = rest.trim();
    match name {
        "/help" | "/hotkeys" => view.append(HELP),
        "/session" => {
            let session = Session::open(&args.session)?;
            view.append(&serde_json::to_string_pretty(&sessions::summary(
                &args.session,
                &session,
            ))?);
        }
        "/name" => {
            let mut session = Session::open(&args.session)?;
            if !rest.is_empty() {
                session.set_name(rest)?;
            }
            view.session_name = session.name().map(str::to_owned);
            view.append(session.name().unwrap_or("Unnamed session"));
        }
        "/context-edit" | "/context-delete" => {
            let (target, content) = rest.split_once(char::is_whitespace).unwrap_or((rest, ""));
            let target = target
                .parse()
                .map_err(|_| Error::Config("provide a message record ID".into()))?;
            let replacement = if name == "/context-delete" {
                None
            } else {
                Some(pi_rs::ContextContent::Text(content.into()))
            };
            Session::open(&args.session)?.edit_context(target, replacement)?;
            view.append("Updated future model context");
        }
        "/label" => {
            let (id, label) = rest.split_once(char::is_whitespace).unwrap_or((rest, ""));
            let mut session = Session::open(&args.session)?;
            let target = id
                .parse()
                .map_err(|_| Error::Config("use /label <record-id> [label]".into()))?;
            session.set_label(target, (!label.is_empty()).then(|| label.into()))?;
            view.append("Label saved");
        }
        "/fork" if rest.is_empty() => {
            let session = Session::open(&args.session)?;
            let items = session
                .branch()
                .iter()
                .filter_map(|record| match &record.event {
                    pi_rs::Event::User { content, .. }
                    | pi_rs::Event::UserWithImages { content, .. } => Some(Item {
                        label: format!(
                            "{} {}",
                            record.id,
                            content.chars().take(90).collect::<String>()
                        ),
                        selection: Selection::Fork(record.id),
                    }),
                    _ => None,
                })
                .collect();
            view.picker = Some(Picker::new("Fork from user message", items));
        }
        "/clone" | "/fork" => {
            let session = Session::open(&args.session)?;
            let (target, input) = if name == "/fork" {
                let target = rest
                    .parse::<u64>()
                    .ok()
                    .or_else(|| {
                        session.branch().iter().rev().find_map(|record| {
                            matches!(
                                record.event,
                                pi_rs::Event::User { .. } | pi_rs::Event::UserWithImages { .. }
                            )
                            .then_some(record.id)
                        })
                    })
                    .ok_or_else(|| {
                        Error::Config("no user message to fork; use /fork <user-record-id>".into())
                    })?;
                let record = session
                    .records()
                    .iter()
                    .find(|record| record.id == target)
                    .ok_or_else(|| Error::Config("invalid fork record".into()))?;
                let (pi_rs::Event::User { content, .. }
                | pi_rs::Event::UserWithImages { content, .. }) = &record.event
                else {
                    return Err(Error::Config("fork must select a user message".into()));
                };
                (record.parent.unwrap_or(1), Some(content.clone()))
            } else {
                (session.head(), None)
            };
            let destination = session_path(&args.workspace, args.session_dir.as_deref());
            sessions::fork(
                &session,
                &args.session,
                target,
                &destination,
                &args.workspace,
            )?;
            drop(session);
            args.session = destination;
            view.restore(args)?;
            if let Some(input) = input {
                view.editor.replace(input);
            }
        }
        "/export" => {
            let destination = if rest.is_empty() {
                args.workspace
                    .join(format!("pi-session-{}.html", uuid::Uuid::new_v4()))
            } else {
                let path = PathBuf::from(rest);
                if path.is_absolute() {
                    path
                } else {
                    args.workspace.join(path)
                }
            };
            let session = Session::open(&args.session)?;
            sessions::export(&session, &destination)?;
            view.append(&format!("Exported {}", destination.display()));
        }
        "/import" => {
            let destination = session_path(&args.workspace, args.session_dir.as_deref());
            let source = std::path::Path::new(rest);
            let source = if source.is_absolute() {
                source.into()
            } else {
                args.workspace.join(source)
            };
            sessions::import(&source, &destination)?;
            switch_session(args, view, destination)?;
        }
        "/copy" => {
            sessions::copy_last(&Session::open(&args.session)?)?;
            view.append("Copied last response");
        }
        "/share" => view.append(&sessions::share_gist(&Session::open(&args.session)?)?),
        "/model" if rest.is_empty() => start_model_picker(args, view),
        "/model" => {
            let selected = super::model_selection::split(rest, &view.available_models)?;
            args.model = selected.model;
            if selected.thinking.is_some() {
                args.thinking = selected.thinking;
            } else {
                apply_scoped_thinking(args);
            }
            super::thinking::adjust(args);
            Session::open(&args.session)?.select_model(&args.model, args.thinking.as_deref())?;
            view.append(&format!("Model › {rest}"));
        }
        "/thinking" if rest.is_empty() => {
            view.picker = Some(Picker::new(
                "Thinking level",
                super::thinking::available(args)
                    .into_iter()
                    .map(|level| Item {
                        label: level.clone(),
                        selection: Selection::Thinking(level),
                    })
                    .collect(),
            ));
        }
        "/thinking" => super::thinking::set(args, rest)?,
        "/new" => {
            args.session = session_path(&args.workspace, args.session_dir.as_deref());
            initialize_session(args)?;
            view.restore(args)?;
        }
        "/resume" if rest.is_empty() => {
            let mut items = Vec::new();
            let mut named = std::collections::HashSet::new();
            let mut documents = std::collections::BTreeMap::new();
            let workspace = args.workspace.canonicalize()?;
            if let Some(parent) = args.session.parent() {
                for path in sessions::stored_paths(parent)? {
                    if let Ok(session) = Session::open(&path) {
                        if session.host_binding().is_some() {
                            continue;
                        }
                        if session.name().is_some_and(|name| !name.is_empty()) {
                            named.insert(path.clone());
                        }
                        let title = session
                            .name()
                            .map(str::to_owned)
                            .or_else(|| {
                                session
                                    .records()
                                    .iter()
                                    .find_map(|record| match &record.event {
                                        pi_rs::Event::User { content, .. }
                                        | pi_rs::Event::UserWithImages { content, .. } => {
                                            Some(content.clone())
                                        }
                                        _ => None,
                                    })
                            })
                            .unwrap_or_else(|| "Empty session".into());
                        let messages = session
                            .records()
                            .iter()
                            .filter_map(|record| match &record.event {
                                pi_rs::Event::User { content, .. }
                                | pi_rs::Event::UserWithImages { content, .. }
                                | pi_rs::Event::Assistant { content, .. } => Some(content.as_str()),
                                _ => None,
                            })
                            .collect::<Vec<_>>()
                            .join(" ");
                        let cwd = session.working_directory().unwrap_or_default();
                        let local = std::path::Path::new(cwd)
                            .canonicalize()
                            .is_ok_and(|cwd| cwd == workspace);
                        let mut document = super::ui::session_search::Document::new(
                            session.name().unwrap_or_default().into(),
                            session.session_id().to_string(),
                            cwd.into(),
                            messages,
                            local,
                        );
                        document.message_count = session.statistics().total_messages;
                        document.activity = session.activity_time().or_else(|| {
                            path.metadata()
                                .and_then(|metadata| metadata.modified())
                                .ok()
                        });
                        document.parent = session.parent_session().map(|parent| {
                            let parent = PathBuf::from(parent);
                            if parent.is_absolute() {
                                parent
                            } else {
                                args.workspace.join(parent)
                            }
                        });
                        documents.insert(path.clone(), document);
                        items.push(Item {
                            label: title,
                            selection: Selection::Resume(path),
                        });
                    }
                }
            }
            let mut picker = Picker::sessions(items, args.session.clone(), named);
            picker.set_session_documents(documents);
            view.picker = Some(picker);
        }
        "/resume" => {
            let path = PathBuf::from(rest);
            if !path.is_file() {
                return Err(Error::Config("session file not found".into()));
            }
            switch_session(args, view, path)?;
        }
        "/compact" => {
            view.pending_request = Some(
                json!({"type":"compact","summary":null,"keep_turns":1,"use_token_settings":true,"instructions":rest}),
            )
        }
        "/mcp" => view.pending_request = Some(json!({"type":"mcp_status"})),
        "/trust" => {
            view.picker = Some(Picker::new(
                "Load project configuration?",
                vec![
                    Item {
                        label: "Trust this project".into(),
                        selection: Selection::Trust(true),
                    },
                    Item {
                        label: "Skip project configuration".into(),
                        selection: Selection::Trust(false),
                    },
                ],
            ));
        }
        "/settings-json" => view.append(&serde_json::to_string_pretty(args.settings.value())?),
        "/prompts" => {
            for (name, prompt) in &view.resources.prompts {
                view.lines
                    .push(Entry::Notice(format!("/{name} · {}", prompt.description)));
            }
        }
        "/skills" => {
            for (name, skill) in &view.resources.skills {
                view.lines.push(Entry::Notice(format!(
                    "{name} · {} · {}",
                    skill.description.trim(),
                    skill.path.display()
                )));
            }
        }
        "/extensions" => view.append(&format!(
            "Native extensions › plan-mode, branch-review, {}",
            args.extensions.join(", ")
        )),
        "/extension" => {
            let mut parts = rest.splitn(3, ' ');
            let id = parts.next().unwrap_or_default();
            let command = parts
                .next()
                .ok_or_else(|| Error::Config("use /extension <id> <command> [arguments]".into()))?;
            let arguments = parts.next().unwrap_or_default();
            view.pending_request = Some(
                json!({"type":"extension","extension_id":id,"command":command,"arguments":arguments}),
            );
        }
        "/notes" | "/todo" if rest.is_empty() => {
            let id = if name == "/notes" {
                "notes"
            } else {
                "todo-list"
            };
            let session = Session::open(&args.session)?;
            let states = session.extension_states();
            view.append(
                &states
                    .get(id)
                    .map(|state| state.data.to_string())
                    .unwrap_or_else(|| "No saved state".into()),
            );
        }
        "/notes" => {
            view.pending_request = Some(
                json!({"type":"extension","extension_id":"notes","command":"set","arguments":rest}),
            )
        }
        "/todo" => {
            view.pending_request = Some(
                json!({"type":"extension","extension_id":"todo-list","command":"set","arguments":rest}),
            )
        }
        "/plan" if !rest.is_empty() => {
            let (command, input) = match rest {
                "accept" => ("accept", ""),
                "discard" => ("discard", ""),
                _ => ("start", rest),
            };
            view.pending_request = Some(
                json!({"type":"extension","extension_id":"plan-mode","command":command,"arguments":input}),
            );
        }
        "/review" if !rest.is_empty() => {
            let child = args
                .session
                .with_file_name(format!("review-{}.jsonl", uuid::Uuid::new_v4()));
            view.pending_request = Some(
                json!({"type":"review","child":child,"from_record":null,"resume":false,"task":rest}),
            );
        }
        "/tree" => {
            let session = Session::open(&args.session)?;
            let labels = session.labels();
            let items = session
                .records()
                .iter()
                .filter(|record| !matches!(record.event, pi_rs::Event::Checkout { .. }))
                .map(|record| {
                    let description = match &record.event {
                        pi_rs::Event::User { content, .. }
                        | pi_rs::Event::UserWithImages { content, .. }
                        | pi_rs::Event::Assistant { content, .. } => {
                            content.chars().take(70).collect::<String>()
                        }
                        _ => super::metadata(record)["event"]
                            .as_str()
                            .unwrap_or("record")
                            .into(),
                    };
                    Item {
                        label: format!(
                            "{} {} {}",
                            record.id,
                            labels.get(&record.id).map(String::as_str).unwrap_or(""),
                            description
                        ),
                        selection: Selection::Checkout(record.id),
                    }
                })
                .collect();
            let head = session.head();
            let entries = session
                .records()
                .iter()
                .filter(|record| !matches!(record.event, pi_rs::Event::Checkout { .. }))
                .map(|record| {
                    let kind = match &record.event {
                        pi_rs::Event::User { .. } | pi_rs::Event::UserWithImages { .. } => {
                            TreeEntryKind::User
                        }
                        pi_rs::Event::ToolFinished { .. } | pi_rs::Event::ToolReconciled { .. } => {
                            TreeEntryKind::Tool
                        }
                        pi_rs::Event::Assistant { content, .. }
                            if content.trim().is_empty() && record.id != head =>
                        {
                            TreeEntryKind::Hidden
                        }
                        pi_rs::Event::Assistant { .. }
                        | pi_rs::Event::BashExecution { .. }
                        | pi_rs::Event::Compacted { .. } => TreeEntryKind::Message,
                        pi_rs::Event::ModelFinished {
                            failure: Some(_), ..
                        } => TreeEntryKind::Message,
                        _ => TreeEntryKind::Bookkeeping,
                    };
                    (record.id, (record.parent, kind))
                })
                .collect();
            let filter =
                TreeFilter::parse(args.settings.string("treeFilterMode").unwrap_or("default"));
            view.picker = Some(Picker::tree(items, entries, labels, head, filter));
        }
        "/checkout" => {
            let id = rest
                .parse()
                .map_err(|_| Error::Config("use /checkout <record-id>".into()))?;
            let mut session = Session::open(&args.session)?;
            session.checkout(id)?;
            drop(session);
            view.restore(args)?;
        }
        _ => {
            if let Some(expanded) = view.resources.expand(input)? {
                view.lines.push(Entry::User(expanded.clone()));
                view.pending_request = Some(json!({"type":"prompt","content":input}));
                return Ok(None);
            }
            view.pending_request =
                Some(json!({"type":"prompt","content":input,"require_command":true}));
        }
    }
    Ok(None)
}

pub(super) async fn run(mut args: Cli) -> Result<()> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        return Err(Error::Config(
            "interactive mode requires a terminal; use run <prompt> for automation".into(),
        ));
    }
    args.allow_commands = true;
    for name in ["notes", "todo"] {
        if !args.no_extensions && !args.extensions.iter().any(|extension| extension == name) {
            args.extensions.push(name.into());
        }
    }
    if let Some(parent) = args
        .session
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    initialize_session(&mut args)?;
    restore_workspace(&mut args)?;
    super::packages::prepare(&args).await?;
    let initial_resources = super::resources::Resources::load(&args)?;
    args.theme_paths = initial_resources.themes.clone();
    let mut theme = Theme::load(
        args.use_theme.as_deref().unwrap_or("system"),
        &args.theme_paths,
        &args.workspace,
        args.trusted,
        false,
    )?;
    let mut view = View {
        expand_tools: args.settings.boolean("expandTools").unwrap_or(false),
        hide_thinking: args.settings.boolean("hideThinkingBlock").unwrap_or(false),
        ..View::default()
    };
    view.restore(&args)?;
    view.resources = initial_resources;
    if args.trust_pending {
        slash("/trust", &mut args, &mut view)?;
    }
    if args
        .resume_session
        .as_ref()
        .is_some_and(|path| path.as_os_str().is_empty())
    {
        slash("/resume", &mut args, &mut view)?;
    }
    let mut keys = Keys::load()?;
    let _terminal = Some(Terminal::enter(args.tui_mode, view.graphics.cleanup())?);
    #[cfg(unix)]
    let mut _terminal = _terminal;
    let mut input = crate::ui::input::Reader::new()?;
    if view.graphics.enabled() {
        crate::ui::images::query_cell_size()?;
    }
    let mut worker: Option<Worker> = None;
    let mut initial = args
        .initial_prompt
        .take()
        .map(|prompt| (prompt, std::mem::take(&mut args.initial_images)));
    let mut dirty = true;
    let frame_interval = Duration::from_millis(16);
    let mut last_frame = Instant::now() - frame_interval;
    let mut last_idle_interrupt: Option<Instant> = None;
    let mut theme_watcher = ThemeWatcher::new(&theme);
    let mut last_theme_check = Instant::now();
    loop {
        if !args.trust_pending
            && view.picker.is_none()
            && let Some((prompt, images)) = initial.take()
        {
            view.lines.push(Entry::User(prompt.clone()));
            view.history.push(prompt.clone());
            start_request(
                &args,
                json!({"type":"prompt","content":prompt,"images":images}),
                &mut worker,
            )?;
        }

        if last_theme_check.elapsed() >= Duration::from_millis(500) {
            match theme_watcher.poll(&mut theme) {
                Ok(changed) => dirty |= changed,
                Err(error) => {
                    view.append(&format!("Theme reload: {error}"));
                    dirty = true;
                }
            }
            last_theme_check = Instant::now();
        }
        if worker
            .as_ref()
            .is_some_and(|active| !active.busy && !active.matches(&args))
        {
            worker.take();
        }
        if let Some(receiver) = &view.model_list {
            match receiver.try_recv() {
                Ok(Ok(models)) => {
                    view.model_list = None;
                    view.available_models = models.clone();
                    if models.is_empty() {
                        view.model_cycle = None;
                        view.append("No models available");
                        dirty = true;
                        continue;
                    }
                    if let Some(reverse) = view.model_cycle.take() {
                        if let Err(error) = cycle_model(&mut args, &mut view, reverse) {
                            view.append(&error.to_string());
                        }
                    } else {
                        view.picker = Some(Picker::new(
                            "Select model",
                            models
                                .into_iter()
                                .map(|model| Item {
                                    label: model.clone(),
                                    selection: Selection::Model(model),
                                })
                                .collect(),
                        ));
                    }
                    dirty = true;
                }
                Ok(Err(error)) => {
                    view.model_list = None;
                    view.model_cycle = None;
                    view.append(&error.to_string());
                    dirty = true;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    view.model_list = None;
                    view.model_cycle = None;
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
            }
        }
        if let Some(active) = &mut worker {
            if active.busy && view.request_started.is_none() {
                view.request_started = Some(Instant::now());
            }
            let events: Vec<_> = active.events.try_iter().collect();
            for value in events {
                if value["type"] == "extension_ui_request" {
                    match view.ui_request(&value) {
                        Ok(Some((id, value))) => active
                            .send(json!({"type":"extension_ui_response","id":id,"value":value}))?,
                        Ok(None) => {}
                        Err(error) => {
                            view.append(&error.to_string());
                            active.send(json!({"type":"extension_ui_response","id":value["id"],"value":null}))?;
                        }
                    }
                    dirty = true;
                    continue;
                }
                if value["type"] == "extension_ui_cancelled" {
                    view.dialogs
                        .retain(|dialog| json!(dialog.id) != value["id"]);
                    dirty = true;
                    continue;
                }
                let settled = value["type"] == "agent_settled";
                if value["type"] == "agent_started" {
                    active.busy = true;
                }
                view.receive(value);
                if settled {
                    active.busy = false;
                    view.dialogs.clear();
                    view.settle();
                }
                dirty = true;
            }
            if let Some(status) = active.child.try_wait()? {
                let mut finished = worker.take().expect("active worker");
                for reader in finished.readers.drain(..) {
                    let _ = reader.join();
                }
                for value in finished.events.try_iter() {
                    view.receive(value);
                }
                view.settle();
                view.dialogs.clear();
                if !status.success() {
                    view.append(&format!(
                        "Run stopped ({status}). Use /session to inspect its saved journal."
                    ));
                }
                view.request_started = None;
                view.phase = None;
                dirty = true;
            }
        }
        dirty |= view.poll_clipboard();
        if (dirty && last_frame.elapsed() >= frame_interval)
            || (is_busy(&worker) && last_frame.elapsed() >= Duration::from_millis(100))
        {
            view.draw(&args, &theme, is_busy(&worker))?;
            dirty = false;
            last_frame = Instant::now();
        }
        let input_wait = if dirty {
            frame_interval.saturating_sub(last_frame.elapsed())
        } else {
            Duration::from_millis(40)
        };
        let event = match input.next(input_wait)? {
            Some(crate::ui::input::InputEvent::Event(event)) => event,
            #[cfg(any(unix, windows))]
            Some(crate::ui::input::InputEvent::CellSize { width, height }) => {
                if images::set_cell_dimensions(width, height) {
                    dirty = true;
                }
                continue;
            }
            #[cfg(any(unix, windows))]
            Some(crate::ui::input::InputEvent::Appearance { dark }) => {
                if crate::ui::theme::set_appearance(dark) {
                    let selection = args.use_theme.as_deref().unwrap_or("system");
                    if selection.contains('/') {
                        match Theme::load(
                            selection,
                            &args.theme_paths,
                            &args.workspace,
                            args.trusted,
                            false,
                        ) {
                            Ok(next) => theme = next,
                            Err(error) => view.append(&format!("Theme reload: {error}")),
                        }
                    }
                    dirty = true;
                }
                continue;
            }
            _ => continue,
        };
        match event {
            Event::Resize(..) => {
                if view.graphics.enabled() {
                    images::query_cell_size()?;
                }
                if args.tui_mode == Some(TuiMode::Regular) {
                    view.finish_regular()?;
                }
                dirty = true;
            }
            Event::Paste(text) => {
                last_idle_interrupt = None;
                if let Some((_, dialog)) = view.setting_editor.as_mut() {
                    dialog.paste(&text);
                } else if let Some(dialog) = view.dialogs.front_mut() {
                    dialog.paste(&text);
                } else {
                    view.editor.insert(&text);
                }
                dirty = true;
            }
            Event::Key(key) if key.kind != KeyEventKind::Release => {
                dirty = true;
                if let Some((name, mut dialog)) = view.setting_editor.take() {
                    match dialog.input(key, &keys) {
                        Some(pi_rs::UiResponse::Text(text)) => {
                            let result = serde_json::from_str(&text).map_err(Error::from).and_then(
                                |value| {
                                    select(
                                        Selection::Setting(name.clone(), value),
                                        &mut args,
                                        &mut view,
                                        &mut theme,
                                        &mut worker,
                                    )
                                },
                            );
                            match result {
                                Ok(()) => view.append("Setting saved"),
                                Err(error) => {
                                    dialog.set_error(error.to_string());
                                    view.setting_editor = Some((name, dialog));
                                }
                            }
                        }
                        Some(_) => {}
                        None => view.setting_editor = Some((name, dialog)),
                    }
                    continue;
                }
                if let Some(dialog) = view.dialogs.front_mut() {
                    if let Some(value) = dialog.input(key, &keys) {
                        let id = dialog.id;
                        view.dialogs.pop_front();
                        if let Some(active) = &mut worker {
                            active.send(
                                json!({"type":"extension_ui_response","id":id,"value":value}),
                            )?;
                        }
                    }
                    continue;
                }
                if let Some(mut picker) = view.picker.take() {
                    match picker.input(key, &keys) {
                        Input::Continue => view.picker = Some(picker),
                        Input::Cancel => {}
                        Input::Save(selection) => {
                            let result = match selection {
                                Selection::Model(model) => {
                                    args.settings.set_user("defaultModel", json!(model))
                                }
                                Selection::Thinking(thinking) => args
                                    .settings
                                    .set_user("defaultThinkingLevel", json!(thinking)),
                                _ => Ok(()),
                            };
                            if let Err(error) = result {
                                view.append(&error.to_string());
                            }
                            view.picker = Some(picker);
                        }
                        Input::Select(selection) => {
                            if let Err(error) =
                                select(selection, &mut args, &mut view, &mut theme, &mut worker)
                            {
                                view.append(&error.to_string());
                            }
                        }
                    }
                    continue;
                }
                let action = keys
                    .action(
                        key,
                        view.editor.text.is_empty() && view.draft_images.is_empty(),
                    )
                    .unwrap_or("");
                if action != "app.interrupt" || is_busy(&worker) {
                    last_idle_interrupt = None;
                }
                if view.editor.action(action) {
                    continue;
                }
                match action {
                    "app.suspend" => {
                        #[cfg(unix)]
                        {
                            if args.tui_mode == Some(TuiMode::Regular) {
                                view.finish_regular()?;
                            }
                            if let Err(error) = Terminal::suspend(
                                &mut _terminal,
                                args.tui_mode,
                                view.graphics.cleanup(),
                            ) {
                                view.append(&error.to_string());
                            }
                            if view.graphics.enabled() {
                                images::query_cell_size()?;
                            }
                            view.graphics.reset();
                            view.regular.redraw = true;
                            view.regular.started = false;
                        }
                        #[cfg(not(unix))]
                        view.append("Suspending the terminal is unavailable on this platform");
                    }
                    "app.message.dequeue" => {
                        match Worker::control(&mut worker, &args, json!({"type":"clear_queue"})) {
                            Ok(()) => view.restoring_queue = true,
                            Err(error) => view.append(&error.to_string()),
                        }
                    }
                    "app.interrupt" if is_busy(&worker) => {
                        if let Err(error) =
                            Worker::control(&mut worker, &args, json!({"type":"clear_queue"}))
                        {
                            view.append(&error.to_string());
                        } else {
                            view.restoring_queue = true;
                        }
                        if let Some(active) = &mut worker {
                            let _ = active.send(json!({"type":"cancel"}));
                        }
                    }
                    "app.interrupt" => {
                        let action = args.settings.string("doubleEscapeAction").unwrap_or("tree");
                        if view.editor.text.starts_with('!') {
                            view.editor.take();
                            last_idle_interrupt = None;
                        } else if action != "none"
                            && view.editor.text.trim().is_empty()
                            && view.draft_images.is_empty()
                            && view.clipboard.is_none()
                        {
                            let now = Instant::now();
                            if last_idle_interrupt.take().is_some_and(|last| {
                                now.duration_since(last) < Duration::from_millis(500)
                            }) {
                                let command = if action == "fork" { "/fork" } else { "/tree" };
                                if let Err(error) = slash(command, &mut args, &mut view) {
                                    view.append(&error.to_string());
                                }
                            } else {
                                last_idle_interrupt = Some(now);
                            }
                        }
                    }
                    "app.session.new" | "app.session.tree" | "app.session.fork"
                    | "app.session.resume"
                        if !is_busy(&worker) =>
                    {
                        let command = match action {
                            "app.session.new" => "/new",
                            "app.session.tree" => "/tree",
                            "app.session.fork" => "/fork",
                            _ => "/resume",
                        };
                        if let Err(error) = slash(command, &mut args, &mut view) {
                            view.append(&error.to_string());
                        }
                    }
                    "app.message.copy" if !is_busy(&worker) => {
                        if let Err(error) = sessions::copy_last(&Session::open(&args.session)?) {
                            view.append(&error.to_string());
                        }
                    }
                    "app.model.cycleForward" | "app.model.cycleBackward" if !is_busy(&worker) => {
                        if let Err(error) =
                            cycle_model(&mut args, &mut view, action == "app.model.cycleBackward")
                        {
                            view.append(&error.to_string());
                        }
                    }
                    "app.thinking.cycle" if !is_busy(&worker) => {
                        if let Err(error) = super::thinking::cycle(&mut args) {
                            view.append(&error.to_string());
                        }
                    }
                    "app.tools.expand" => view.expand_tools = !view.expand_tools,
                    "app.thinking.toggle" => view.hide_thinking = !view.hide_thinking,
                    "app.clipboard.pasteImage" => view.paste_clipboard(),
                    "app.clear" => {
                        if let Some(active) = worker.as_mut().filter(|active| active.busy) {
                            let _ = active.send(json!({"type":"cancel"}));
                        } else if view.editor.text.is_empty()
                            && view.draft_images.is_empty()
                            && view.clipboard.is_none()
                        {
                            break;
                        } else {
                            view.editor.take();
                            view.draft_images.clear();
                            view.clipboard = None;
                        }
                    }
                    "app.exit" => break,
                    "app.editor.external" => {
                        if args.tui_mode == Some(TuiMode::Regular) {
                            view.finish_regular()?;
                        }
                        if let Err(error) = external_editor(&args, &mut view.editor) {
                            view.append(&error.to_string());
                        }
                        view.regular.redraw = true;
                    }
                    "tui.input.tab" => complete_input(&args, &mut view),
                    "app.model.select" if !is_busy(&worker) => start_model_picker(&args, &mut view),
                    "tui.input.submit" | "app.message.followUp" => {
                        if view.clipboard.is_some() {
                            continue;
                        }
                        if !view.draft_images.is_empty()
                            && let Err(error) = view.draft_message(&view.editor.text).validate()
                        {
                            view.append(&error.to_string());
                            continue;
                        }
                        let input = view.editor.take();
                        if input.trim().is_empty() && view.draft_images.is_empty() {
                            continue;
                        }
                        if matches!(input.trim(), "/quit" | "/exit") {
                            break;
                        }
                        view.scroll = 0;
                        view.history.push(input.clone());
                        view.history_index = None;
                        if input.trim() == "/theme" {
                            let names = Theme::discover(
                                &args.theme_paths,
                                &args.workspace,
                                args.trusted,
                                false,
                            );
                            let title = format!(
                                "Select theme · {}",
                                theme
                                    .path
                                    .as_ref()
                                    .map(|path| path.display().to_string())
                                    .unwrap_or_else(|| theme.name.clone())
                            );
                            view.picker = Some(Picker::new(
                                &title,
                                names
                                    .into_iter()
                                    .map(|name| Item {
                                        label: name.clone(),
                                        selection: Selection::Theme(name),
                                    })
                                    .collect(),
                            ));
                        } else if let Some(name) = input.strip_prefix("/theme ") {
                            match Theme::load(
                                name.trim(),
                                &args.theme_paths,
                                &args.workspace,
                                args.trusted,
                                false,
                            ) {
                                Ok(next) => {
                                    args.settings.set_user("theme", json!(name.trim()))?;
                                    args.use_theme = Some(name.trim().into());
                                    theme = next;
                                }
                                Err(error) => view.append(&error.to_string()),
                            }
                        } else if input.trim() == "/hotkeys" {
                            view.append(&keys.describe());
                        } else if input.trim() == "/settings" {
                            let mut items = vec![
                                Item {
                                    label: format!(
                                        "Tool output · {} (toggle)",
                                        if view.expand_tools {
                                            "expanded"
                                        } else {
                                            "collapsed"
                                        }
                                    ),
                                    selection: Selection::Setting(
                                        "expandTools".into(),
                                        json!(!view.expand_tools),
                                    ),
                                },
                                Item {
                                    label: format!(
                                        "Thinking blocks · {} (toggle)",
                                        if view.hide_thinking {
                                            "hidden"
                                        } else {
                                            "shown"
                                        }
                                    ),
                                    selection: Selection::Setting(
                                        "hideThinkingBlock".into(),
                                        json!(!view.hide_thinking),
                                    ),
                                },
                                Item {
                                    label: format!("Save default model · {}", args.model),
                                    selection: Selection::Setting(
                                        "defaultModel".into(),
                                        json!(args.model),
                                    ),
                                },
                                Item {
                                    label: format!(
                                        "Save default thinking · {}",
                                        args.thinking.as_deref().unwrap_or("model default")
                                    ),
                                    selection: Selection::Setting(
                                        "defaultThinkingLevel".into(),
                                        json!(args.thinking),
                                    ),
                                },
                                Item {
                                    label: format!(
                                        "Auto-compaction · {} (toggle)",
                                        if args.settings.auto_compaction_enabled() {
                                            "on"
                                        } else {
                                            "off"
                                        },
                                    ),
                                    selection: Selection::Setting(
                                        "autoCompactionEnabled".into(),
                                        json!(!args.settings.auto_compaction_enabled()),
                                    ),
                                },
                                Item {
                                    label: format!(
                                        "Auto-retry · {} (toggle)",
                                        if args.settings.retry_settings().enabled {
                                            "on"
                                        } else {
                                            "off"
                                        }
                                    ),
                                    selection: Selection::Setting(
                                        "autoRetryEnabled".into(),
                                        json!(!args.settings.retry_settings().enabled),
                                    ),
                                },
                            ];
                            let escape =
                                args.settings.string("doubleEscapeAction").unwrap_or("tree");
                            let next_escape = match escape {
                                "tree" => "fork",
                                "fork" => "none",
                                _ => "tree",
                            };
                            let tree_filter = TreeFilter::parse(
                                args.settings.string("treeFilterMode").unwrap_or("default"),
                            );
                            items.extend([
                                Item {
                                    label: format!("Double-escape action · {escape}"),
                                    selection: Selection::Setting(
                                        "doubleEscapeAction".into(),
                                        json!(next_escape),
                                    ),
                                },
                                Item {
                                    label: format!("Tree filter mode · {}", tree_filter.name()),
                                    selection: Selection::Setting(
                                        "treeFilterMode".into(),
                                        json!(tree_filter.next().name()),
                                    ),
                                },
                            ]);
                            for name in [
                                "theme",
                                "defaultModel",
                                "defaultThinkingLevel",
                                "modelThinkingLevels",
                                "steeringMode",
                                "followUpMode",
                                "tuiMode",
                                "sessionDir",
                                "defaultProjectTrust",
                                "defaultTools",
                                "extensions",
                                "skills",
                                "prompts",
                                "themes",
                                "packages",
                                "codemode",
                                "compaction",
                                "retry",
                            ] {
                                items.push(Item {
                                    label: format!(
                                        "Edit {name} · {}",
                                        args.settings
                                            .value()
                                            .get(name)
                                            .map(Value::to_string)
                                            .unwrap_or_else(|| "default".into())
                                    ),
                                    selection: Selection::EditSetting(name.into()),
                                });
                            }
                            view.picker = Some(Picker::new("Settings", items));
                        } else if input.starts_with('!') {
                            if is_busy(&worker) {
                                view.append("Wait for the current operation before executing a shell command");
                                view.editor.replace(input);
                            } else {
                                let excluded = input.starts_with("!!");
                                let command = input[if excluded { 2 } else { 1 }..].trim();
                                if !command.is_empty() {
                                    start_request(
                                        &args,
                                        json!({"type":"bash","command":command,"exclude_from_context":excluded}),
                                        &mut worker,
                                    )?;
                                }
                            }
                        } else if let Some(active) = worker.as_mut().filter(|active| active.busy) {
                            let kind = if action == "app.message.followUp" {
                                "followup"
                            } else {
                                "steer"
                            };
                            match active.send(
                                json!({"type":kind,"content":input,"images":view.draft_images}),
                            ) {
                                Ok(()) => view.draft_images.clear(),
                                Err(error) => view.append(&error.to_string()),
                            }
                        } else if input.trim() == "/reload" {
                            worker.take();
                            view.dialogs.clear();
                            view.ui_status.clear();
                            view.widgets.clear();
                            args.settings =
                                crate::config::Settings::load(&args.workspace, args.trusted)?;
                            super::packages::prepare(&args).await?;
                            view.resources = super::resources::Resources::load(&args)?;
                            args.theme_paths = view.resources.themes.clone();
                            match Keys::load() {
                                Ok(next) => keys = next,
                                Err(error) => view.append(&error.to_string()),
                            }
                            match Theme::load(
                                args.use_theme.as_deref().unwrap_or("system"),
                                &args.theme_paths,
                                &args.workspace,
                                args.trusted,
                                false,
                            ) {
                                Ok(next) => theme = next,
                                Err(error) => view.append(&error.to_string()),
                            }
                            view.append("Reloaded settings, context, skills, prompts, native extensions, MCP, keybindings and theme");
                        } else if input.starts_with('/') {
                            match slash(&input, &mut args, &mut view) {
                                Ok(Some(next)) => worker = Some(next),
                                Ok(None) => {
                                    if let Some(mut request) = view.pending_request.take() {
                                        let prompt = request["type"] == "prompt";
                                        if prompt {
                                            request["images"] = json!(view.draft_images);
                                        }
                                        match start_request(&args, request, &mut worker) {
                                            Ok(()) if prompt => view.draft_images.clear(),
                                            Ok(()) => {}
                                            Err(error) => view.append(&error.to_string()),
                                        }
                                    }
                                }
                                Err(error) => view.append(&error.to_string()),
                            }
                        } else {
                            view.lines.push(Entry::User(input.clone()));
                            match start_prompt(&args, &input, &view.draft_images, &mut worker) {
                                Ok(()) => view.draft_images.clear(),
                                Err(error) => view.append(&error.to_string()),
                            }
                        }
                    }
                    "tui.altScreen.pageUp" => view.scroll = view.scroll.saturating_add(10),
                    "tui.altScreen.pageDown" => view.scroll = view.scroll.saturating_sub(10),
                    "tui.altScreen.top" => view.scroll = usize::MAX,
                    "tui.altScreen.bottom" => view.scroll = 0,
                    "tui.editor.cursorUp" if view.editor.vertical(false) => {}
                    "tui.editor.cursorDown" if view.editor.vertical(true) => {}
                    "tui.editor.cursorUp" | "tui.editor.historyPrevious"
                        if !view.history.is_empty() =>
                    {
                        let index = view
                            .history_index
                            .unwrap_or(view.history.len())
                            .saturating_sub(1);
                        view.editor.replace(view.history[index].clone());
                        view.history_index = Some(index);
                    }
                    "tui.editor.cursorDown" | "tui.editor.historyNext"
                        if view.history_index.is_some() =>
                    {
                        let index = view.history_index.unwrap() + 1;
                        view.editor
                            .replace(view.history.get(index).cloned().unwrap_or_default());
                        view.history_index = (index < view.history.len()).then_some(index);
                    }
                    "" => {
                        if let KeyCode::Char(character) = key.code
                            && !key.modifiers.intersects(
                                KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER,
                            )
                        {
                            view.editor.insert(&character.to_string());
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    drop(worker);
    if args.tui_mode == Some(TuiMode::Regular) {
        view.finish_regular()?;
    }
    Ok(())
}

fn is_busy(worker: &Option<Worker>) -> bool {
    worker.as_ref().is_some_and(|worker| worker.busy)
}

fn start_prompt(
    args: &Cli,
    input: &str,
    images: &[pi_rs::ImageContent],
    worker: &mut Option<Worker>,
) -> Result<()> {
    start_request(
        args,
        json!({"type":"prompt","content":input,"images":images}),
        worker,
    )
}
fn start_request(args: &Cli, request: Value, worker: &mut Option<Worker>) -> Result<()> {
    Worker::submit(worker, args, request)
}

fn apply_scoped_thinking(args: &mut Cli) {
    args.apply_model_thinking_defaults();
}

fn cycle_model(args: &mut Cli, view: &mut View, reverse: bool) -> Result<()> {
    if view.available_models.is_empty() {
        view.model_cycle = Some(reverse);
        start_model_picker(args, view);
        return Ok(());
    }
    let index = view
        .available_models
        .iter()
        .position(|model| model == &args.model);
    let count = view.available_models.len();
    let next = match (index, reverse) {
        (Some(index), true) => (index + count - 1) % count,
        (Some(index), false) => (index + 1) % count,
        (None, true) => count - 1,
        (None, false) => 0,
    };
    args.model = view.available_models[next].clone();
    apply_scoped_thinking(args);
    super::thinking::adjust(args);
    Session::open(&args.session)?.select_model(&args.model, args.thinking.as_deref())?;
    Ok(())
}

fn start_model_picker(args: &Cli, view: &mut View) {
    if view.model_list.is_some() {
        return;
    }
    let scope = args.model_scope.clone();
    let provider = args.provider;
    let endpoint = args.endpoint.clone();
    let token = args.model_token();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let provider = provider.connect(&endpoint, token)?;
            if scope.is_empty() {
                runtime.block_on(provider.models())
            } else {
                Ok(scope.into_iter().map(|choice| choice.model).collect())
            }
        })();
        let _ = sender.send(result);
    });
    view.model_list = Some(receiver);
    view.append("Loading available models…");
}

fn select(
    selection: Selection,
    args: &mut Cli,
    view: &mut View,
    theme: &mut Theme,
    worker: &mut Option<Worker>,
) -> Result<()> {
    match selection {
        Selection::RenameSession(path, name) => {
            let mut session = Session::open(&path)?;
            if session.host_binding().is_some() {
                return Err(Error::Session(
                    "managed sessions must be renamed by their host".into(),
                ));
            }
            session.set_name(&name)?;
            if path.canonicalize()? == args.session.canonicalize()? {
                view.session_name = session.name().map(str::to_owned);
            }
            drop(session);
            slash("/resume", args, view)?;
        }
        Selection::DeleteSession(path) => {
            if path.canonicalize()? == args.session.canonicalize()? {
                return Err(Error::Config("Cannot delete the active session".into()));
            }
            let session = Session::open(&path)?;
            if session.host_binding().is_some() {
                return Err(Error::Session(
                    "managed sessions must be deleted by their host".into(),
                ));
            }
            std::fs::remove_file(&path)?;
            drop(session);
            slash("/resume", args, view)?;
        }
        Selection::Resume(path) => {
            slash(&format!("/resume {}", path.display()), args, view)?;
        }
        Selection::Label(id, label) => {
            Session::open(&args.session)?.set_label(id, (!label.is_empty()).then_some(label))?;
            slash("/tree", args, view)?;
            if let Some(picker) = &mut view.picker {
                picker.focus_record(id);
            }
        }
        Selection::Checkout(id) => {
            let mut session = Session::open(&args.session)?;
            let record = &session.records()[id as usize - 1];
            let (target, input) = match &record.event {
                pi_rs::Event::User { content, .. }
                | pi_rs::Event::UserWithImages { content, .. } => {
                    (record.parent.unwrap_or(1), Some(content.clone()))
                }
                _ => (id, None),
            };
            session.checkout(target)?;
            drop(session);
            view.restore(args)?;
            if let Some(input) = input {
                view.editor.replace(input);
            }
        }
        Selection::Fork(id) => {
            slash(&format!("/fork {id}"), args, view)?;
        }
        Selection::Model(model) => {
            args.model = model;
            apply_scoped_thinking(args);
            super::thinking::adjust(args);
            Session::open(&args.session)?.select_model(&args.model, args.thinking.as_deref())?;
        }
        Selection::Thinking(thinking) => super::thinking::set(args, &thinking)?,
        Selection::EditSetting(key) => {
            let default = match key.as_str() {
                "theme" => json!(args.use_theme.as_deref().unwrap_or("system")),
                "defaultModel" => json!(args.model),
                "defaultThinkingLevel" => json!(args.thinking.as_deref().unwrap_or("off")),
                "tuiMode" => json!(if args.tui_mode == Some(TuiMode::Regular) {
                    "regular"
                } else {
                    "fullscreen"
                }),
                "sessionDir" => json!(
                    args.session_dir
                        .as_deref()
                        .unwrap_or_else(|| std::path::Path::new(""))
                ),
                "defaultProjectTrust" => json!("ask"),
                "defaultTools" => json!(["read", "bash", "edit", "write"]),
                "modelThinkingLevels" => json!({}),
                "steeringMode" | "followUpMode" => json!("one-at-a-time"),
                "codemode" => json!({"mode":"on", "inlineBudget":3000}),
                "retry" => serde_json::to_value(args.settings.retry_settings())?,
                "compaction" => {
                    let tokens = args.settings.compaction_tokens(&args.model);
                    json!({"enabled":args.settings.auto_compaction_enabled(), "reserveTokens":tokens.reserve_tokens, "keepRecentTokens":tokens.keep_recent_tokens})
                }
                _ => json!([]),
            };
            let value = args.settings.value().get(&key).unwrap_or(&default);
            let dialog = crate::ui::dialog::Dialog::new(
                uuid::Uuid::new_v4(),
                pi_rs::UiRequest::Editor {
                    title: format!(
                        "Edit {key} as JSON · {}",
                        if matches!(key.as_str(), "steeringMode" | "followUpMode") {
                            "default for new sessions"
                        } else {
                            "applies on reload/startup"
                        }
                    ),
                    initial_value: Some(serde_json::to_string_pretty(value)?),
                    timeout: None,
                },
            );
            view.setting_editor = Some((key, dialog));
        }
        Selection::Setting(key, value) => {
            if key == "autoRetryEnabled" {
                let enabled = value
                    .as_bool()
                    .ok_or_else(|| Error::Config("auto-retry must be a boolean".into()))?;
                args.settings.set_auto_retry_enabled(enabled)?;
                Worker::control(
                    worker,
                    args,
                    json!({"type":"set_auto_retry", "enabled":enabled}),
                )?;
            } else if key == "autoCompactionEnabled" {
                let enabled = value
                    .as_bool()
                    .ok_or_else(|| Error::Config("auto-compaction must be a boolean".into()))?;
                args.settings.set_auto_compaction_enabled(enabled)?;
                Worker::control(
                    worker,
                    args,
                    json!({"type":"set_auto_compaction","enabled":enabled}),
                )?;
            } else if key == "theme" {
                let next = Theme::load(
                    value
                        .as_str()
                        .ok_or_else(|| Error::Config("theme must be a string".into()))?,
                    &args.theme_paths,
                    &args.workspace,
                    args.trusted,
                    false,
                )?;
                args.settings.set_user(&key, value.clone())?;
                args.use_theme = value.as_str().map(str::to_owned);
                *theme = next;
            } else {
                args.settings.set_user(&key, value.clone())?;
            }
            match key.as_str() {
                "expandTools" => view.expand_tools = value.as_bool().unwrap_or(false),
                "hideThinkingBlock" => view.hide_thinking = value.as_bool().unwrap_or(false),
                _ => {}
            }
        }
        Selection::Trust(decision) => {
            crate::config::save_trust(&args.workspace, decision)?;
            args.trusted = decision;
            args.trust_pending = false;
            args.settings = crate::config::Settings::load(&args.workspace, decision)?;
            view.resources = super::resources::Resources::load(args)?;
            view.append(if decision {
                "Project configuration trusted"
            } else {
                "Project configuration skipped"
            });
        }
        Selection::Theme(name) => {
            let next = Theme::load(
                &name,
                &args.theme_paths,
                &args.workspace,
                args.trusted,
                false,
            )?;
            args.settings.set_user("theme", json!(name))?;
            args.use_theme = Some(name);
            *theme = next;
        }
    }
    Ok(())
}

fn switch_session(args: &mut Cli, view: &mut View, path: PathBuf) -> Result<()> {
    let mut next = args.clone();
    next.model_explicit = false;
    next.thinking_explicit = false;
    next.session = path;
    restore_workspace(&mut next)?;
    let resources = super::resources::Resources::load(&next)?;
    next.theme_paths = resources.themes.clone();
    view.restore(&next)?;
    view.resources = resources;
    *args = next;
    if args.trust_pending {
        slash("/trust", args, view)?;
    }
    Ok(())
}

fn initialize_session(args: &mut Cli) -> Result<()> {
    let mut session = Session::open(&args.session)?;
    if session.host_binding().is_some() {
        return Err(Error::Session(
            "a managed session must be opened by its host".into(),
        ));
    }
    if session.working_directory().is_none() {
        session.set_working_directory(&args.workspace)?;
    }
    if let Some(name) = &args.name {
        session.set_name(name)?;
    }
    if let Some((model, thinking)) = session.model_selection() {
        args.restore_model_selection(model, thinking);
    }
    super::thinking::adjust(args);
    session.select_model(&args.model, args.thinking.as_deref())?;
    Ok(())
}
pub(super) fn restore_workspace(args: &mut Cli) -> Result<()> {
    let session = Session::open(&args.session)?;
    if let Some((model, thinking)) = session.model_selection() {
        args.restore_model_selection(model, thinking);
    }
    if let Some(cwd) = session.working_directory() {
        let path = PathBuf::from(cwd);
        if !path.is_dir() {
            return Err(Error::Config(
                "session working directory no longer exists".into(),
            ));
        }
        args.workspace = path;
    }
    let decision = crate::config::project_trust(
        &args.workspace,
        if args.no_trust {
            Some(false)
        } else if args.trust || args.project.is_some() {
            Some(true)
        } else {
            None
        },
    )?;
    args.trusted = decision.unwrap_or(false);
    args.trust_pending = decision.is_none();
    args.settings = crate::config::Settings::load(&args.workspace, args.trusted)?;
    drop(session);
    super::thinking::adjust(args);
    Ok(())
}

fn external_editor(args: &Cli, editor: &mut Editor) -> Result<()> {
    let command = args
        .settings
        .string("externalEditor")
        .map(str::to_owned)
        .or_else(|| std::env::var("VISUAL").ok())
        .or_else(|| std::env::var("EDITOR").ok())
        .unwrap_or_else(|| {
            if cfg!(windows) {
                "notepad".into()
            } else {
                "nano".into()
            }
        });
    let parts = shell_words::split(&command).map_err(|error| Error::Config(error.to_string()))?;
    let Some((program, arguments)) = parts.split_first() else {
        return Err(Error::Config("external editor is empty".into()));
    };
    let mut file = tempfile::Builder::new().suffix(".md").tempfile()?;
    file.write_all(editor.text.as_bytes())?;
    execute!(std::io::stdout(), Show, DisableBracketedPaste)?;
    if args.tui_mode != Some(TuiMode::Regular) {
        execute!(std::io::stdout(), LeaveAlternateScreen)?;
    }
    terminal::disable_raw_mode()?;
    let result = std::process::Command::new(program)
        .args(arguments)
        .arg(file.path())
        .status();
    terminal::enable_raw_mode()?;
    execute!(std::io::stdout(), EnableBracketedPaste, Hide)?;
    if args.tui_mode != Some(TuiMode::Regular) {
        execute!(std::io::stdout(), EnterAlternateScreen)?;
    }
    if !result?.success() {
        return Err(Error::Config(
            "external editor exited unsuccessfully".into(),
        ));
    }
    editor.replace(std::fs::read_to_string(file.path())?);
    Ok(())
}

fn complete_input(args: &Cli, view: &mut View) {
    let prefix = &view.editor.text[..view.editor.cursor];
    let start = prefix
        .rfind(char::is_whitespace)
        .map_or(0, |index| index + 1);
    let word = &prefix[start..];
    let mut candidates = if start == 0 && word.starts_with('/') {
        [
            "/help",
            "/hotkeys",
            "/new",
            "/resume",
            "/session",
            "/model",
            "/compact",
            "/tree",
            "/checkout",
            "/quit",
            "/extensions",
            "/extension",
            "/notes",
            "/todo",
            "/plan",
            "/review",
            "/theme",
            "/settings",
            "/reload",
        ]
        .into_iter()
        .filter(|name| name.starts_with(word))
        .map(str::to_owned)
        .collect::<Vec<_>>()
    } else {
        let file_word = word.strip_prefix('@').unwrap_or(word);
        let path = std::path::Path::new(file_word);
        let parent = path.parent().unwrap_or(std::path::Path::new(""));
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        let directory = if file_word.ends_with('/') {
            args.workspace.join(path)
        } else {
            args.workspace.join(parent)
        };
        std::fs::read_dir(directory)
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let file_name = entry.file_name().into_string().ok()?;
                if !file_word.ends_with('/') && !file_name.starts_with(name) {
                    return None;
                }
                let parent = if file_word.ends_with('/') {
                    path
                } else {
                    parent
                };
                let suffix = if entry.file_type().ok()?.is_dir() {
                    "/"
                } else {
                    ""
                };
                Some(format!(
                    "{}{}{suffix}",
                    if word.starts_with('@') { "@" } else { "" },
                    parent.join(file_name).display()
                ))
            })
            .take(100)
            .collect::<Vec<_>>()
    };
    if start == 0 && word.starts_with('/') {
        candidates.extend(
            view.resources
                .prompts
                .keys()
                .map(|name| format!("/{name}"))
                .chain(
                    view.resources
                        .skills
                        .keys()
                        .map(|name| format!("/skill:{name}")),
                )
                .filter(|name| name.starts_with(word)),
        );
        candidates.sort();
        candidates.dedup();
    }
    if candidates.len() == 1 {
        let end = view.editor.cursor;
        let mut text = view.editor.text.clone();
        text.replace_range(start..end, &candidates[0]);
        let cursor = start + candidates[0].len();
        view.editor.replace(text);
        view.editor.cursor = cursor;
    } else if !candidates.is_empty() {
        let mut candidates = candidates;
        candidates.sort();
        view.append(&candidates.join("  "));
    }
}

fn clean(text: &str) -> String {
    text.chars()
        .filter(|character| !character.is_control() || *character == '\n' || *character == '\t')
        .collect()
}
fn clip(text: &str, width: usize) -> String {
    wrap(text, width).into_iter().next().unwrap_or_default()
}
fn wrap(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let text = clean(text).replace('\t', "    ");
    let mut lines = Vec::new();
    for logical in text.split('\n') {
        let mut line = String::new();
        let mut columns = 0;
        for grapheme in logical.graphemes(true) {
            let size = UnicodeWidthStr::width(grapheme);
            if columns + size > width && !line.is_empty() {
                lines.push(std::mem::take(&mut line));
                columns = 0;
            }
            if size <= width {
                line.push_str(grapheme);
                columns += size;
            }
        }
        lines.push(line);
    }
    lines
}

#[cfg(test)]
mod tests {
    #[test]
    fn continue_skips_newer_sessions_from_other_workspaces_and_invalid_files() {
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        let other = directory.path().join("other");
        std::fs::create_dir(&workspace).unwrap();
        std::fs::create_dir(&other).unwrap();
        let valid = directory.path().join("valid.jsonl");
        let mut session = pi_rs::Session::open(&valid).unwrap();
        session.set_working_directory(&workspace).unwrap();
        drop(session);
        let foreign = directory.path().join("foreign.jsonl");
        let mut session = pi_rs::Session::open(&foreign).unwrap();
        session.set_working_directory(&other).unwrap();
        drop(session);
        let invalid = directory.path().join("invalid.jsonl");
        std::fs::write(&invalid, "invalid").unwrap();
        let late = std::time::UNIX_EPOCH + std::time::Duration::from_secs(2_000_000_000);
        for path in [&foreign, &invalid] {
            std::fs::File::options()
                .write(true)
                .open(path)
                .unwrap()
                .set_times(std::fs::FileTimes::new().set_modified(late))
                .unwrap();
        }
        assert_eq!(super::latest_session(&valid, &workspace).unwrap(), valid);
    }

    use super::*;
    #[test]
    fn clipboard_results_update_the_draft_and_keep_image_only_input_valid() {
        let mut view = View::default();
        let (sender, receiver) = std::sync::mpsc::channel();
        view.clipboard = Some(receiver);
        assert!(!view.poll_clipboard());
        assert!(view.draft_status().contains("Reading clipboard"));
        sender
            .send(Ok(super::super::clipboard::Content::Image(
                pi_rs::ImageContent {
                    data: "aGVsbG8=".into(),
                    mime_type: "image/png".into(),
                },
            )))
            .unwrap();
        assert!(view.poll_clipboard());
        assert!(view.clipboard.is_none());
        assert_eq!(view.draft_images.len(), 1);
        assert!(view.draft_status().contains("1 image(s)"));
        assert!(view.draft_message("").validate().is_ok());
        let (sender, receiver) = std::sync::mpsc::channel();
        view.clipboard = Some(receiver);
        sender
            .send(Ok(super::super::clipboard::Content::Text(
                "こんにちは".into(),
            )))
            .unwrap();
        assert!(view.poll_clipboard());
        assert_eq!(view.editor.text, "こんにちは");
        let (sender, receiver) = std::sync::mpsc::channel();
        view.clipboard = Some(receiver);
        sender
            .send(Ok(super::super::clipboard::Content::Image(
                pi_rs::ImageContent {
                    data: "invalid".into(),
                    mime_type: "image/png".into(),
                },
            )))
            .unwrap();
        assert!(view.poll_clipboard());
        assert_eq!(view.draft_images.len(), 1);
    }

    #[test]
    fn pasted_file_paths_preserve_surrounding_draft_text() {
        let mut view = View::default();
        view.editor.replace("leftRIGHT".into());
        view.editor.cursor = 4;
        let (sender, receiver) = std::sync::mpsc::channel();
        view.clipboard = Some(receiver);
        sender
            .send(Ok(super::super::clipboard::Content::Files(vec![
                "/tmp/one file".into(),
                "/tmp/two".into(),
            ])))
            .unwrap();
        view.poll_clipboard();
        assert_eq!(view.editor.text, "left /tmp/one file\n/tmp/two RIGHT");
        view.editor.replace("!cat ".into());
        let (sender, receiver) = std::sync::mpsc::channel();
        view.clipboard = Some(receiver);
        sender
            .send(Ok(super::super::clipboard::Content::Files(vec![
                "/tmp/one file".into(),
            ])))
            .unwrap();
        view.poll_clipboard();
        assert_eq!(view.editor.text, "!cat '/tmp/one file'");
    }

    #[test]
    fn progress_distinguishes_loading_thinking_and_generation() {
        let mut view = View::default();
        for (stage, label) in [
            ("loading_model", "モデル読込待ち"),
            ("thinking", "思考中"),
            ("generating", "回答生成中"),
        ] {
            view.receive(json!({"type":"model_progress","stage":stage}));
            assert_eq!(view.phase, Some(label));
        }
        assert!(view.streaming.is_empty());
        assert_eq!(view.tokens, 0);
    }
    #[test]
    fn editor_keeps_graphemes_intact() {
        let mut editor = Editor::default();
        editor.insert("A👩‍💻日本");
        editor.left();
        editor.backspace();
        assert_eq!(editor.text, "A👩‍💻本");
        editor.left();
        editor.delete();
        assert_eq!(editor.text, "A本");
    }
    #[test]
    fn wrapping_counts_terminal_columns_and_removes_controls() {
        assert_eq!(wrap("日本abc", 4), vec!["日本", "abc"]);
        assert!(!clean("hello\x1b[31m\x07").contains('\x1b'));
    }
    #[test]
    fn regular_history_eviction_preserves_the_uncommitted_entry_boundary() {
        let mut view = View::default();
        for _ in 0..4000 {
            view.append("old");
        }
        view.regular.committed = 4000;
        view.append("new");
        assert_eq!(view.lines.len(), 4000);
        assert_eq!(view.regular.committed, 3999);
    }
}
