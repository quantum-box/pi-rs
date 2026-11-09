//! Terminal presentation over the same journaled CLI used by automation.
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::ui::{
    editor::Editor,
    keys::Keys,
    render::{Entry, Line},
    theme::Theme,
};
use crossterm::cursor::{Hide, MoveTo, Show};
use crossterm::event::{
    self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEventKind, KeyModifiers,
};
use crossterm::style::ResetColor;
use crossterm::terminal::{self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::{execute, queue};
use pi_rs::{Error, Result, Session};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use super::{Cli, process::Worker};

pub(super) fn new_session_path() -> PathBuf {
    let home = crate::config::agent_dir();
    // Group local sessions without including the working directory in filenames.
    let workspace = std::env::current_dir().unwrap_or_default();
    let hash = Sha256::digest(workspace.as_os_str().as_encoded_bytes());
    home.join("sessions")
        .join(format!("{hash:x}"))
        .join(format!("{}.jsonl", uuid::Uuid::new_v4()))
}

pub(super) fn latest_session(example: &std::path::Path) -> Result<PathBuf> {
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

struct Terminal;
impl Terminal {
    fn enter() -> Result<Self> {
        terminal::enable_raw_mode()?;
        let guard = Self;
        execute!(
            std::io::stdout(),
            EnterAlternateScreen,
            EnableBracketedPaste,
            Hide
        )?;
        Ok(guard)
    }
}
impl Drop for Terminal {
    fn drop(&mut self) {
        let _ = execute!(
            std::io::stdout(),
            ResetColor,
            Show,
            DisableBracketedPaste,
            LeaveAlternateScreen
        );
        let _ = terminal::disable_raw_mode();
    }
}

#[derive(Default)]
struct View {
    lines: Vec<Entry>,
    thinking: String,
    expand_tools: bool,
    hide_thinking: bool,
    streaming: String,
    editor: Editor,
    scroll: usize,
    tokens: u64,
    history: Vec<String>,
    history_index: Option<usize>,
    request_started: Option<Instant>,
    phase: Option<&'static str>,
}
impl View {
    fn append(&mut self, text: &str) {
        self.lines.push(Entry::Notice(clean(text)));
        if self.lines.len() > 4000 {
            self.lines.drain(..self.lines.len() - 4000);
        }
    }
    fn restore(&mut self, args: &Cli) -> Result<()> {
        let session = Session::open(&args.session)?;
        if session.host_binding().is_some() {
            return Err(Error::Session(
                "a managed session must be opened by its host".into(),
            ));
        }
        self.lines.clear();
        self.streaming.clear();
        self.thinking.clear();
        self.history.clear();
        self.history_index = None;
        self.tokens = 0;
        for record in session.branch() {
            match &record.event {
                pi_rs::Event::User { content, .. } => {
                    self.history.push(content.clone());
                    self.lines.push(Entry::User(content.clone()));
                }
                pi_rs::Event::ModelFinished {
                    metadata, usage, ..
                } => {
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
                        self.lines.push(Entry::Tool {
                            id: invocation.operation_id.to_string(),
                            name: invocation.call.name.clone(),
                            arguments: invocation.call.arguments.clone(),
                            output: String::new(),
                            failed: false,
                            finished: false,
                        });
                    }
                }
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
        self.append("Enter to send · Alt+Enter to queue · Ctrl+O tools · Ctrl+T thinking · /help");
        Ok(())
    }
    fn finish_tool(&mut self, operation: &str, outcome: &pi_rs::Outcome) {
        if let Some(Entry::Tool {
            output,
            failed,
            finished,
            ..
        }) = self
            .lines
            .iter_mut()
            .rev()
            .find(|entry| matches!(entry, Entry::Tool { id, .. } if id == operation))
        {
            *output = outcome.tool_content();
            *failed = !matches!(
                outcome,
                pi_rs::Outcome::Succeeded(_) | pi_rs::Outcome::Structured { .. }
            );
            *finished = true;
        }
    }
    fn receive(&mut self, value: Value) {
        match value["type"].as_str() {
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
            Some("error") => self.append(&format!(
                "Error › {}",
                value["message"].as_str().unwrap_or_default()
            )),
            Some("record") => {
                let event = &value["record"]["event"];
                match event["type"].as_str() {
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
                                });
                            }
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
    fn draw(&self, args: &Cli, theme: &Theme, busy: bool) -> Result<()> {
        let (width, height) = terminal::size()?;
        let width = usize::from(width.max(1));
        let height = usize::from(height.max(7));
        let prompt = wrap(&self.editor.text, width);
        let prompt_height = prompt.len().clamp(1, 6).min(height.saturating_sub(5));
        let body_height = height.saturating_sub(prompt_height + 5);
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
        queue!(out, Hide, MoveTo(0, 0), Clear(ClearType::All))?;
        Line::plain(
            &format!("pi v{} · /help", env!("CARGO_PKG_VERSION")),
            theme.color("accent"),
        )
        .draw(&mut out, width)?;
        for (row, line) in lines[start..end].iter().enumerate() {
            queue!(out, MoveTo(0, (row + 1) as u16))?;
            line.draw(&mut out, width)?;
        }
        let border_row = height - prompt_height - 4;
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
        queue!(out, MoveTo(0, (height - 3) as u16))?;
        Line::plain(&"─".repeat(width), theme.color("border")).draw(&mut out, width)?;
        queue!(out, MoveTo(0, (height - 2) as u16))?;
        Line::plain(&args.workspace.display().to_string(), theme.color("dim"))
            .draw(&mut out, width)?;
        queue!(out, MoveTo(0, (height - 1) as u16))?;
        let model_width = UnicodeWidthStr::width(args.model.as_str());
        let usage = format!("↑↓ 確定 {} tokens", self.tokens);
        let usage = clip(&usage, width.saturating_sub(model_width + 1));
        let padding = width.saturating_sub(UnicodeWidthStr::width(usage.as_str()) + model_width);
        Line::plain(
            &format!("{usage}{}{model}", " ".repeat(padding), model = args.model),
            theme.color("dim"),
        )
        .draw(&mut out, width)?;
        let column = cursor_prefix
            .last()
            .map_or(0, |line| UnicodeWidthStr::width(line.as_str()))
            .min(width - 1);
        queue!(
            out,
            MoveTo(column as u16, (border_row + 1 + cursor_row - offset) as u16),
            Show
        )?;
        out.flush()?;
        Ok(())
    }
}

const HELP: &str = "/help  /new  /resume [path]  /session  /model <name>  /compact  /tree  /checkout <id>  /quit\n/extensions  /extension <id> <command> [arguments]\n/notes [text]  /todo [JSON list]  /plan <text|accept|discard>  /review <task>\nShift+Enter inserts a newline. PageUp/PageDown scroll. Up/Down recall prompts.\nEnter while working steers the next model response. Alt+Enter queues a follow-up.";

fn slash(input: &str, args: &mut Cli, view: &mut View) -> Result<Option<Worker>> {
    let (name, rest) = input.split_once(char::is_whitespace).unwrap_or((input, ""));
    let rest = rest.trim();
    match name {
        "/help" | "/hotkeys" => view.append(HELP),
        "/session" => view.append(&format!("Session › {}", args.session.display())),
        "/model" if rest.is_empty() => view.append(&format!("Model › {}", args.model)),
        "/model" => {
            args.model = rest.to_owned();
            view.append(&format!("Model › {rest}"));
        }
        "/new" => {
            args.session = new_session_path();
            view.restore(args)?;
        }
        "/resume" if rest.is_empty() => {
            if let Some(parent) = args.session.parent() {
                let mut entries = std::fs::read_dir(parent)?
                    .filter_map(|entry| entry.ok())
                    .map(|entry| entry.path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
                    .collect::<Vec<_>>();
                entries.sort();
                for path in entries {
                    view.append(&path.display().to_string());
                }
            }
            view.append("Use /resume <path> to open a session.");
        }
        "/resume" => {
            let path = PathBuf::from(rest);
            if !path.is_file() {
                return Err(Error::Config("session file not found".into()));
            }
            let prior = std::mem::replace(&mut args.session, path);
            if let Err(error) = view.restore(args) {
                args.session = prior;
                return Err(error);
            }
        }
        "/compact" => return Worker::start(args, &["compact"]).map(Some),
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
            return Worker::start(args, &["extension", id, command, arguments]).map(Some);
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
        "/notes" => return Worker::start(args, &["extension", "notes", "set", rest]).map(Some),
        "/todo" => return Worker::start(args, &["extension", "todo-list", "set", rest]).map(Some),
        "/plan" if !rest.is_empty() => {
            let (command, input) = match rest {
                "accept" => ("accept", ""),
                "discard" => ("discard", ""),
                _ => ("start", rest),
            };
            return Worker::start(args, &["extension", "plan-mode", command, input]).map(Some);
        }
        "/review" if !rest.is_empty() => {
            let child = args
                .session
                .with_file_name(format!("review-{}.jsonl", uuid::Uuid::new_v4()));
            return Worker::start(args, &["review", "--child", &child.to_string_lossy(), rest])
                .map(Some);
        }
        "/tree" => {
            let session = Session::open(&args.session)?;
            for record in session.records() {
                view.append(&format!(
                    "{} ← {:?}  {}",
                    record.id,
                    record.parent,
                    super::metadata(record)["event"]
                        .as_str()
                        .unwrap_or("record")
                ));
            }
        }
        "/checkout" => {
            let id = rest
                .parse()
                .map_err(|_| Error::Config("use /checkout <record-id>".into()))?;
            let mut session = Session::open(&args.session)?;
            session.checkout(id)?;
            view.restore(args)?;
        }
        _ => view.append("Unknown command. Use /help."),
    }
    Ok(None)
}

pub(super) fn run(mut args: Cli) -> Result<()> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        return Err(Error::Config(
            "interactive mode requires a terminal; use run <prompt> for automation".into(),
        ));
    }
    args.coding_tools = true;
    args.allow_commands = true;
    for name in ["notes", "todo"] {
        if !args.extensions.iter().any(|extension| extension == name) {
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
    let mut theme = Theme::load(
        args.use_theme.as_deref().unwrap_or("system"),
        &args.themes,
        &args.workspace,
        args.project.is_some(),
    )?;
    let mut view = View {
        expand_tools: args.settings.boolean("expandTools").unwrap_or(false),
        hide_thinking: args.settings.boolean("hideThinkingBlock").unwrap_or(false),
        ..View::default()
    };
    view.restore(&args)?;
    let mut keys = Keys::load()?;
    let _terminal = Terminal::enter()?;
    let mut worker: Option<Worker> = None;
    let mut dirty = true;
    let mut last_frame = Instant::now();
    loop {
        if let Some(active) = &mut worker {
            if view.request_started.is_none() {
                view.request_started = Some(Instant::now());
            }
            for value in active.events.try_iter() {
                view.receive(value);
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
                if !view.thinking.is_empty() {
                    let partial = std::mem::take(&mut view.thinking);
                    view.lines.push(Entry::Thinking(partial));
                }
                if !view.streaming.is_empty() {
                    let partial = std::mem::take(&mut view.streaming);
                    view.lines.push(Entry::Assistant(partial));
                }
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
        if dirty || (worker.is_some() && last_frame.elapsed() >= Duration::from_millis(100)) {
            view.draw(&args, &theme, worker.is_some())?;
            dirty = false;
            last_frame = Instant::now();
        }
        if !event::poll(Duration::from_millis(40))? {
            continue;
        }
        match event::read()? {
            Event::Resize(..) => dirty = true,
            Event::Paste(text) => {
                view.editor.insert(&text);
                dirty = true;
            }
            Event::Key(key) if key.kind != KeyEventKind::Release => {
                dirty = true;
                let action = keys.action(key, view.editor.text.is_empty()).unwrap_or("");
                if view.editor.action(action) {
                    continue;
                }
                match action {
                    "app.interrupt" if worker.is_some() => {
                        if let Some(active) = &mut worker {
                            let _ = active.send(json!({"type":"cancel"}));
                        }
                    }
                    "app.tools.expand" => view.expand_tools = !view.expand_tools,
                    "app.thinking.toggle" => view.hide_thinking = !view.hide_thinking,
                    "app.clear" => {
                        if let Some(active) = &mut worker {
                            let _ = active.send(json!({"type":"cancel"}));
                        } else if view.editor.text.is_empty() {
                            break;
                        } else {
                            view.editor.take();
                        }
                    }
                    "app.exit" => break,
                    "app.editor.external" => {
                        if let Err(error) = external_editor(&args, &mut view.editor) {
                            view.append(&error.to_string());
                        }
                    }
                    "tui.input.tab" => complete_input(&args, &mut view),
                    "app.model.select" => {
                        view.editor.replace("/model ".into());
                    }
                    "tui.input.submit" | "app.message.followUp" => {
                        let input = view.editor.take();
                        if input.trim().is_empty() {
                            continue;
                        }
                        if matches!(input.trim(), "/quit" | "/exit") {
                            break;
                        }
                        view.scroll = 0;
                        view.history.push(input.clone());
                        view.history_index = None;
                        if let Some(active) = &mut worker {
                            let kind = if action == "app.message.followUp" {
                                "followup"
                            } else {
                                "steer"
                            };
                            if let Err(error) = active.send(json!({"type":kind,"content":input})) {
                                view.append(&error.to_string());
                            }
                        } else if input.trim() == "/theme" {
                            view.append(&format!(
                                "Theme › {}{} · /theme <name>",
                                theme.name,
                                theme
                                    .path
                                    .as_ref()
                                    .map(|path| format!(" ({})", path.display()))
                                    .unwrap_or_default()
                            ));
                        } else if let Some(name) = input.strip_prefix("/theme ") {
                            match Theme::load(
                                name.trim(),
                                &args.themes,
                                &args.workspace,
                                args.project.is_some(),
                            ) {
                                Ok(next) => {
                                    args.settings.set_user("theme", json!(name.trim()))?;
                                    args.use_theme = Some(name.trim().into());
                                    theme = next;
                                }
                                Err(error) => view.append(&error.to_string()),
                            }
                        } else if input.trim() == "/reload" {
                            match Keys::load() {
                                Ok(next) => keys = next,
                                Err(error) => view.append(&error.to_string()),
                            }
                            match Theme::load(
                                args.use_theme.as_deref().unwrap_or("system"),
                                &args.themes,
                                &args.workspace,
                                args.project.is_some(),
                            ) {
                                Ok(next) => theme = next,
                                Err(error) => view.append(&error.to_string()),
                            }
                            view.append("Reloaded keybindings and theme");
                        } else if input.trim() == "/hotkeys" {
                            view.append(&keys.describe());
                        } else if input.trim() == "/settings" {
                            view.append(&serde_json::to_string_pretty(args.settings.value())?);
                        } else if input.starts_with('/') {
                            match slash(&input, &mut args, &mut view) {
                                Ok(next) => worker = next,
                                Err(error) => view.append(&error.to_string()),
                            }
                        } else {
                            view.lines.push(Entry::User(input.clone()));
                            match Worker::start(&args, &["run", &input]) {
                                Ok(next) => worker = Some(next),
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
    execute!(
        std::io::stdout(),
        Show,
        DisableBracketedPaste,
        LeaveAlternateScreen
    )?;
    terminal::disable_raw_mode()?;
    let result = std::process::Command::new(program)
        .args(arguments)
        .arg(file.path())
        .status();
    terminal::enable_raw_mode()?;
    execute!(
        std::io::stdout(),
        EnterAlternateScreen,
        EnableBracketedPaste,
        Hide
    )?;
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
    let candidates = if start == 0 && word.starts_with('/') {
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
    use super::*;
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
}
