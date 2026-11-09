//! Terminal presentation over the same journaled CLI used by automation.
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::ui::{
    editor::Editor,
    images::{Cleanup, InlineImage, Renderer},
    keys::Keys,
    picker::{Input, Item, Picker, Selection},
    render::{Entry, Line, ToolPart},
    theme::{Theme, ThemeWatcher},
};
use crossterm::cursor::{Hide, MoveDown, MoveTo, MoveToColumn, MoveUp, Show};
use crossterm::event::{
    self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEventKind, KeyModifiers,
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
        Ok(guard)
    }
}
impl Drop for Terminal {
    fn drop(&mut self) {
        if self.alternate {
            self.images.clear();
        }
        let _ = execute!(std::io::stdout(), ResetColor, Show, DisableBracketedPaste);
        if self.alternate {
            let _ = execute!(std::io::stdout(), LeaveAlternateScreen);
        } else {
            let _ = execute!(std::io::stdout(), Print("\r\n"));
        }
        let _ = terminal::disable_raw_mode();
    }
}

#[derive(PartialEq, Eq)]
struct RegularLayout {
    width: usize,
    height: usize,
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
        self.append("Enter to send · Alt+Enter to queue · Ctrl+O tools · Ctrl+T thinking · /help");
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
            ..
        }) = self
            .lines
            .iter_mut()
            .rev()
            .find(|entry| matches!(entry, Entry::Tool { id, .. } if id == operation))
        {
            *parts = rendered_parts;
            *output = display_tool_result(outcome);
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
        if let Some(picker) = &self.picker {
            lines = picker.render(theme, body_height);
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
            self.graphics.paint(line, &mut out, row)?;
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
        queue!(
            out,
            MoveTo(column as u16, (border_row + 1 + cursor_row - offset) as u16),
            Show
        )?;
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
                    self.graphics.paint(&line, &mut out, usize::MAX)?;
                    queue!(out, Print("\r\n"))?;
                }
            }
        }
        let mut active = Vec::new();
        if !busy && self.picker.is_none() && self.scroll == 0 {
            for entry in &self.lines[self.regular.committed..] {
                for line in entry.render(theme, width, self.expand_tools, !self.hide_thinking) {
                    line.draw(&mut out, width)?;
                    self.graphics.paint(&line, &mut out, usize::MAX)?;
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
                active = picker.render(theme, height - prompt_height - 4);
            }
            let end = active
                .len()
                .saturating_sub(self.scroll.min(active.len().saturating_sub(1)));
            active = active[..end]
                .iter()
                .rev()
                .take(height - prompt_height - 4)
                .cloned()
                .collect();
            active.reverse();
            for line in &active {
                line.draw(&mut out, width)?;
                queue!(out, Print("\r\n"))?;
            }
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
        self.regular.region_height = active.len() + prompt_height + 4;
        let column = prefix
            .last()
            .map_or(0, |line| UnicodeWidthStr::width(line.as_str()))
            .min(width - 1);
        queue!(
            out,
            MoveUp((self.regular.region_height - self.regular.cursor_row - 1) as u16),
            MoveToColumn(column as u16),
            Show
        )?;
        self.regular.started = true;
        out.flush()?;
        Ok(())
    }
}

fn display_tool_result(outcome: &pi_rs::Outcome) -> String {
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

const THINKING_LEVELS: &[&str] = &["off", "minimal", "low", "medium", "high", "xhigh", "max"];

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
            sessions::fork(&session, target, &destination, &args.workspace)?;
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
            let selected = super::model_selection::split(rest);
            args.model = selected.model;
            if selected.thinking.is_some() {
                args.thinking = selected.thinking;
            } else {
                apply_scoped_thinking(args);
            }
            Session::open(&args.session)?.select_model(&args.model, args.thinking.as_deref())?;
            view.append(&format!("Model › {rest}"));
        }
        "/thinking" if rest.is_empty() => {
            view.picker = Some(Picker::new(
                "Thinking level",
                THINKING_LEVELS
                    .iter()
                    .map(|level| Item {
                        label: (*level).into(),
                        selection: Selection::Thinking((*level).into()),
                    })
                    .collect(),
            ));
        }
        "/thinking" => {
            if !THINKING_LEVELS.contains(&rest) {
                return Err(Error::Config("unknown thinking level".into()));
            }
            args.thinking = Some(rest.into());
            Session::open(&args.session)?.select_model(&args.model, args.thinking.as_deref())?;
        }
        "/new" => {
            args.session = session_path(&args.workspace, args.session_dir.as_deref());
            initialize_session(args)?;
            view.restore(args)?;
        }
        "/resume" if rest.is_empty() => {
            let mut items = Vec::new();
            let mut named = std::collections::HashSet::new();
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
                                    .branch()
                                    .iter()
                                    .find_map(|record| match &record.event {
                                        pi_rs::Event::User { content, .. }
                                        | pi_rs::Event::UserWithImages { content, .. } => {
                                            Some(content.chars().take(70).collect())
                                        }
                                        _ => None,
                                    })
                            })
                            .unwrap_or_else(|| "Empty session".into());
                        items.push(Item {
                            label: format!(
                                "{title} · {}",
                                path.file_stem().unwrap_or_default().to_string_lossy()
                            ),
                            selection: Selection::Resume(path),
                        });
                    }
                }
            }
            view.picker = Some(Picker::sessions(items, args.session.clone(), named));
        }
        "/resume" => {
            let path = PathBuf::from(rest);
            if !path.is_file() {
                return Err(Error::Config("session file not found".into()));
            }
            switch_session(args, view, path)?;
        }
        "/compact" => {
            view.pending_request = Some(json!({"type":"compact","summary":null,"keep_turns":1}))
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
                            "{} ← {:?} {} {}",
                            record.id,
                            record.parent,
                            labels.get(&record.id).map(String::as_str).unwrap_or(""),
                            description
                        ),
                        selection: Selection::Checkout(record.id),
                    }
                })
                .collect();
            view.picker = Some(Picker::new("Session tree", items));
        }
        "/checkout" => {
            let id = rest
                .parse()
                .map_err(|_| Error::Config("use /checkout <record-id>".into()))?;
            let mut session = Session::open(&args.session)?;
            session.checkout(id)?;
            view.restore(args)?;
        }
        _ => {
            if let Some(expanded) = view.resources.expand(input)? {
                view.lines.push(Entry::User(expanded.clone()));
                view.pending_request = Some(json!({"type":"prompt","content":input}));
                return Ok(None);
            }
            view.append("Unknown command. Use /help.");
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
    let _terminal = Terminal::enter(args.tui_mode, view.graphics.cleanup())?;
    let mut worker: Option<Worker> = None;
    let mut initial = args
        .initial_prompt
        .take()
        .map(|prompt| (prompt, std::mem::take(&mut args.initial_images)));
    let mut dirty = true;
    let mut last_frame = Instant::now();
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
            for value in active.events.try_iter() {
                let settled = value["type"] == "agent_settled";
                if value["type"] == "agent_started" {
                    active.busy = true;
                }
                view.receive(value);
                if settled {
                    active.busy = false;
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
        if dirty || (is_busy(&worker) && last_frame.elapsed() >= Duration::from_millis(100)) {
            view.draw(&args, &theme, is_busy(&worker))?;
            dirty = false;
            last_frame = Instant::now();
        }
        if !event::poll(Duration::from_millis(40))? {
            continue;
        }
        match event::read()? {
            Event::Resize(..) => {
                if args.tui_mode == Some(TuiMode::Regular) {
                    view.finish_regular()?;
                }
                dirty = true;
            }
            Event::Paste(text) => {
                view.editor.insert(&text);
                dirty = true;
            }
            Event::Key(key) if key.kind != KeyEventKind::Release => {
                dirty = true;
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
                            if let Err(error) = select(selection, &mut args, &mut view, &mut theme)
                            {
                                view.append(&error.to_string());
                            }
                        }
                    }
                    continue;
                }
                let action = keys.action(key, view.editor.text.is_empty()).unwrap_or("");
                if view.editor.action(action) {
                    continue;
                }
                match action {
                    "app.interrupt" if is_busy(&worker) => {
                        if let Some(active) = &mut worker {
                            let _ = active.send(json!({"type":"cancel"}));
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
                        let next = args
                            .thinking
                            .as_deref()
                            .and_then(|level| {
                                THINKING_LEVELS.iter().position(|value| *value == level)
                            })
                            .map_or(0, |index| (index + 1) % THINKING_LEVELS.len());
                        args.thinking = Some(THINKING_LEVELS[next].into());
                        Session::open(&args.session)?
                            .select_model(&args.model, args.thinking.as_deref())?;
                    }
                    "app.tools.expand" => view.expand_tools = !view.expand_tools,
                    "app.thinking.toggle" => view.hide_thinking = !view.hide_thinking,
                    "app.clear" => {
                        if let Some(active) = worker.as_mut().filter(|active| active.busy) {
                            let _ = active.send(json!({"type":"cancel"}));
                        } else if view.editor.text.is_empty() {
                            break;
                        } else {
                            view.editor.take();
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
                        if let Some(active) = worker.as_mut().filter(|active| active.busy) {
                            let kind = if action == "app.message.followUp" {
                                "followup"
                            } else {
                                "steer"
                            };
                            if let Err(error) = active.send(json!({"type":kind,"content":input})) {
                                view.append(&error.to_string());
                            }
                        } else if input.trim() == "/theme" {
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
                        } else if input.trim() == "/reload" {
                            worker.take();
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
                        } else if input.trim() == "/hotkeys" {
                            view.append(&keys.describe());
                        } else if input.trim() == "/settings" {
                            view.picker = Some(Picker::new(
                                "Settings",
                                vec![
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
                                ],
                            ));
                        } else if input.starts_with('/') {
                            match slash(&input, &mut args, &mut view) {
                                Ok(Some(next)) => worker = Some(next),
                                Ok(None) => {
                                    if let Some(request) = view.pending_request.take()
                                        && let Err(error) =
                                            start_request(&args, request, &mut worker)
                                    {
                                        view.append(&error.to_string());
                                    }
                                }
                                Err(error) => view.append(&error.to_string()),
                            }
                        } else {
                            view.lines.push(Entry::User(input.clone()));
                            if let Err(error) = start_prompt(&args, &input, &mut worker) {
                                view.append(&error.to_string());
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

fn start_prompt(args: &Cli, input: &str, worker: &mut Option<Worker>) -> Result<()> {
    start_request(args, json!({"type":"prompt","content":input}), worker)
}
fn start_request(args: &Cli, request: Value, worker: &mut Option<Worker>) -> Result<()> {
    Worker::submit(worker, args, request)
}

fn apply_scoped_thinking(args: &mut Cli) {
    if let Some(choice) = args
        .model_scope
        .iter()
        .find(|choice| choice.model == args.model)
        && choice.thinking.is_some()
    {
        args.thinking = choice.thinking.clone();
    }
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
    Session::open(&args.session)?.select_model(&args.model, args.thinking.as_deref())?;
    Ok(())
}

fn start_model_picker(args: &Cli, view: &mut View) {
    if view.model_list.is_some() {
        return;
    }
    let scope = args.model_scope.clone();
    let endpoint = args.endpoint.clone();
    let token = args.model_token();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let provider = pi_rs::ollama::Ollama::new(&endpoint, token)?;
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

fn select(selection: Selection, args: &mut Cli, view: &mut View, theme: &mut Theme) -> Result<()> {
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
            Session::open(&args.session)?.select_model(&args.model, args.thinking.as_deref())?;
        }
        Selection::Thinking(thinking) => {
            args.thinking = Some(thinking);
            Session::open(&args.session)?.select_model(&args.model, args.thinking.as_deref())?;
        }
        Selection::Setting(key, value) => {
            args.settings.set_user(&key, value.clone())?;
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
