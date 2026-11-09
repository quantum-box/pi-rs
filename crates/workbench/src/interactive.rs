//! Terminal presentation over the same journaled CLI used by automation.
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::time::Duration;

use crossterm::cursor::{Hide, MoveTo, Show};
use crossterm::event::{
    self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEventKind, KeyModifiers,
};
use crossterm::style::{Color, Print, ResetColor, SetForegroundColor};
use crossterm::terminal::{self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::{execute, queue};
use pi_rs::{Error, Message, Result, Session};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use super::{Cli, process::Worker};

pub(super) fn new_session_path() -> PathBuf {
    let home = std::env::var_os("PI_RS_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".pi-rs")))
        .unwrap_or_else(|| std::env::temp_dir().join("pi-rs"));
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
struct Editor {
    text: String,
    cursor: usize,
}
impl Editor {
    fn insert(&mut self, text: &str) {
        let text = clean(text);
        self.text.insert_str(self.cursor, &text);
        self.cursor += text.len();
    }
    fn left(&mut self) {
        self.cursor = self.text[..self.cursor]
            .grapheme_indices(true)
            .next_back()
            .map_or(0, |(index, _)| index);
    }
    fn right(&mut self) {
        self.cursor += self.text[self.cursor..]
            .graphemes(true)
            .next()
            .map_or(0, str::len);
    }
    fn backspace(&mut self) {
        let end = self.cursor;
        self.left();
        self.text.replace_range(self.cursor..end, "");
    }
    fn delete(&mut self) {
        let start = self.cursor;
        self.right();
        self.text.replace_range(start..self.cursor, "");
        self.cursor = start;
    }
    fn take(&mut self) -> String {
        self.cursor = 0;
        std::mem::take(&mut self.text)
    }
}

#[derive(Default)]
struct View {
    lines: Vec<String>,
    streaming: String,
    editor: Editor,
    scroll: usize,
    tokens: u64,
    history: Vec<String>,
    history_index: Option<usize>,
}
impl View {
    fn append(&mut self, text: &str) {
        self.lines.extend(clean(text).lines().map(str::to_owned));
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
        self.tokens = 0;
        for record in session.branch() {
            if let pi_rs::Event::ModelFinished {
                usage: pi_rs::UsageReport::Measured { usage },
                ..
            } = &record.event
            {
                self.tokens = self
                    .tokens
                    .saturating_add(usage.input_tokens)
                    .saturating_add(usage.output_tokens);
            }
        }
        for message in session.conversation_at(session.head())? {
            match message {
                Message::User { content } => self.append(&format!("You › {content}")),
                Message::Assistant { content, .. } if !content.is_empty() => {
                    self.append(&format!("Pi › {content}\n"))
                }
                _ => {}
            }
        }
        self.append("Enter to send · Alt+Enter to queue · Esc to stop · /help for commands");
        Ok(())
    }
    fn receive(&mut self, value: Value) {
        match value["type"].as_str() {
            Some("text_delta") => self
                .streaming
                .push_str(value["delta"].as_str().unwrap_or_default()),
            Some("answer" | "review_answer") => {
                self.streaming.clear();
                self.append(&format!(
                    "Pi › {}\n",
                    value["content"].as_str().unwrap_or_default()
                ));
            }
            Some("error") => self.append(&format!(
                "Error › {}",
                value["message"].as_str().unwrap_or_default()
            )),
            Some("record") => {
                let event = &value["record"]["event"];
                match event["type"].as_str() {
                    Some("assistant") => {
                        if let Some(calls) = event["invocations"].as_array() {
                            for call in calls {
                                self.append(&format!(
                                    "Tool › {}",
                                    call["call"]["name"].as_str().unwrap_or("tool")
                                ));
                            }
                        }
                    }
                    Some("tool_finished") => self.append(&format!(
                        "Tool › {}",
                        event["outcome"]["status"].as_str().unwrap_or("finished")
                    )),
                    Some("model_finished") => {
                        let usage = &event["usage"]["usage"];
                        self.tokens = self
                            .tokens
                            .saturating_add(usage["input_tokens"].as_u64().unwrap_or(0))
                            .saturating_add(usage["output_tokens"].as_u64().unwrap_or(0));
                    }
                    Some("input_queued") => self.append("Input queued"),
                    Some("extension_states") => self.append("Extension state saved"),
                    _ => {}
                }
            }
            _ => {}
        }
    }
    fn draw(&self, args: &Cli, busy: bool) -> Result<()> {
        let (width, height) = terminal::size()?;
        let width = usize::from(width.max(1));
        let height = usize::from(height.max(4));
        let prompt = wrap(&format!("> {}", self.editor.text), width);
        let prompt_height = prompt.len().clamp(1, 6).min(height.saturating_sub(3));
        let body_height = height.saturating_sub(prompt_height + 3);
        let mut lines: Vec<String> = self
            .lines
            .iter()
            .flat_map(|line| wrap(line, width))
            .collect();
        if !self.streaming.is_empty() {
            lines.extend(wrap(&format!("Pi › {}", self.streaming), width));
        }
        let end = lines.len().saturating_sub(self.scroll);
        let start = end.saturating_sub(body_height);
        let mut out = std::io::stdout().lock();
        queue!(
            out,
            Hide,
            MoveTo(0, 0),
            Clear(ClearType::All),
            SetForegroundColor(Color::Cyan),
            Print(clip(
                &format!("pi-rs · {}", args.workspace.display()),
                width
            )),
            ResetColor
        )?;
        for (row, line) in lines[start..end].iter().enumerate() {
            queue!(out, MoveTo(0, (row + 1) as u16), Print(line))?;
        }
        let status_row = height - prompt_height - 2;
        queue!(
            out,
            MoveTo(0, status_row as u16),
            SetForegroundColor(Color::DarkGrey),
            Print(clip(
                &format!(
                    "{} · {} · {} tokens",
                    args.model,
                    if busy {
                        "working · Esc cancels"
                    } else {
                        "ready"
                    },
                    self.tokens
                ),
                width
            )),
            ResetColor
        )?;
        let cursor_prefix = wrap(
            &format!("> {}", &self.editor.text[..self.editor.cursor]),
            width,
        );
        let cursor_row = cursor_prefix.len().saturating_sub(1);
        let offset = cursor_row.saturating_sub(prompt_height - 1);
        for (row, line) in prompt.iter().skip(offset).take(prompt_height).enumerate() {
            queue!(out, MoveTo(0, (status_row + 1 + row) as u16), Print(line))?;
        }
        let column = cursor_prefix
            .last()
            .map_or(0, |line| UnicodeWidthStr::width(line.as_str()))
            .min(width - 1);
        queue!(
            out,
            MoveTo(column as u16, (status_row + 1 + cursor_row - offset) as u16),
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
    let mut view = View::default();
    view.restore(&args)?;
    let _terminal = Terminal::enter()?;
    let mut worker: Option<Worker> = None;
    let mut dirty = true;
    loop {
        if let Some(active) = &mut worker {
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
                if !view.streaming.is_empty() {
                    let partial = std::mem::take(&mut view.streaming);
                    view.append(&format!("Pi › {partial}"));
                }
                if !status.success() {
                    view.append(&format!(
                        "Run stopped ({status}). Use /session to inspect its saved journal."
                    ));
                }
                dirty = true;
            }
        }
        if dirty {
            view.draw(&args, worker.is_some())?;
            dirty = false;
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
                match key.code {
                    KeyCode::Esc if worker.is_some() => {
                        if let Some(active) = &mut worker {
                            let _ = active.send(json!({"type":"cancel"}));
                        }
                    }
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        if let Some(active) = &mut worker {
                            let _ = active.send(json!({"type":"cancel"}));
                        } else if view.editor.text.is_empty() {
                            break;
                        } else {
                            view.editor.take();
                        }
                    }
                    KeyCode::Char('d')
                        if key.modifiers.contains(KeyModifiers::CONTROL)
                            && view.editor.text.is_empty() =>
                    {
                        break;
                    }
                    KeyCode::Enter if key.modifiers.contains(KeyModifiers::SHIFT) => {
                        view.editor.insert("\n")
                    }
                    KeyCode::Enter => {
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
                            let kind = if key.modifiers.contains(KeyModifiers::ALT) {
                                "followup"
                            } else {
                                "steer"
                            };
                            if let Err(error) = active.send(json!({"type":kind,"content":input})) {
                                view.append(&error.to_string());
                            }
                        } else if input.starts_with('/') {
                            match slash(&input, &mut args, &mut view) {
                                Ok(next) => worker = next,
                                Err(error) => view.append(&error.to_string()),
                            }
                        } else {
                            view.append(&format!("You › {input}"));
                            match Worker::start(&args, &["run", &input]) {
                                Ok(next) => worker = Some(next),
                                Err(error) => view.append(&error.to_string()),
                            }
                        }
                    }
                    KeyCode::Left => view.editor.left(),
                    KeyCode::Right => view.editor.right(),
                    KeyCode::Home => view.editor.cursor = 0,
                    KeyCode::End => view.editor.cursor = view.editor.text.len(),
                    KeyCode::Backspace => view.editor.backspace(),
                    KeyCode::Delete => view.editor.delete(),
                    KeyCode::PageUp => view.scroll = (view.scroll + 10).min(view.lines.len()),
                    KeyCode::PageDown => view.scroll = view.scroll.saturating_sub(10),
                    KeyCode::Up if !view.history.is_empty() => {
                        let index = view
                            .history_index
                            .unwrap_or(view.history.len())
                            .saturating_sub(1);
                        view.editor.text = view.history[index].clone();
                        view.editor.cursor = view.editor.text.len();
                        view.history_index = Some(index);
                    }
                    KeyCode::Down if view.history_index.is_some() => {
                        let index = view.history_index.unwrap() + 1;
                        view.editor.text = view.history.get(index).cloned().unwrap_or_default();
                        view.editor.cursor = view.editor.text.len();
                        view.history_index = (index < view.history.len()).then_some(index);
                    }
                    KeyCode::Char(character)
                        if !key
                            .modifiers
                            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
                    {
                        view.editor.insert(&character.to_string())
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
