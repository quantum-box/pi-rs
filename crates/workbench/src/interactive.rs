//! Terminal presentation over the same journaled CLI used by automation.
use std::collections::VecDeque;
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::ui::{
    editor::Editor,
    images::{self, Cleanup, InlineImage, Renderer},
    keys::Keys,
    mermaid::Mermaid,
    mouse_selection::MouseSelection,
    picker::{Input, Item, Picker, Selection, TreeEntryKind, TreeFilter},
    render::{Entry, Line, SummaryKind, ToolPart, TranscriptOptions},
    theme::{Theme, ThemeWatcher},
    transcript_search::Search,
    viewport::{Viewport, Wheel},
};
use crossterm::cursor::{Hide, MoveDown, MoveTo, MoveToColumn, MoveUp, Show};
use crossterm::event::{
    DisableBracketedPaste, DisableFocusChange, DisableMouseCapture, EnableBracketedPaste,
    EnableFocusChange, EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers, MouseButton,
    MouseEvent, MouseEventKind,
};
use crossterm::style::{Print, ResetColor};
use crossterm::terminal::{self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::{execute, queue};
use pi_rs::{Error, Result, Session};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use super::{Cli, TuiMode, process::Worker, radius_auth, sessions};

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
    progress: Option<Instant>,
    #[cfg(any(unix, windows))]
    color_queries: std::collections::VecDeque<ColorQuery>,
    #[cfg(any(unix, windows))]
    color_requery: bool,
    #[cfg(any(unix, windows))]
    expired_color_markers: usize,
    #[cfg(unix)]
    keyboard: KeyboardProtocol,
}

#[cfg(unix)]
#[derive(PartialEq, Eq)]
enum KeyboardProtocol {
    Probing,
    Kitty,
    Xterm,
}

#[cfg(any(unix, windows))]
struct ColorQuery {
    started: Instant,
    replied: u32,
}

impl Terminal {
    fn enter(mode: Option<TuiMode>, images: Cleanup) -> Result<Self> {
        terminal::enable_raw_mode()?;
        let mut guard = Self {
            alternate: mode != Some(TuiMode::Regular),
            images,
            progress: None,
            #[cfg(any(unix, windows))]
            color_queries: std::collections::VecDeque::new(),
            #[cfg(any(unix, windows))]
            color_requery: false,
            #[cfg(any(unix, windows))]
            expired_color_markers: 0,
            #[cfg(unix)]
            keyboard: KeyboardProtocol::Probing,
        };
        if guard.alternate {
            execute!(
                std::io::stdout(),
                EnterAlternateScreen,
                EnableMouseCapture,
                EnableFocusChange
            )?;
        }
        execute!(std::io::stdout(), EnableBracketedPaste, Hide)?;
        #[cfg(unix)]
        execute!(std::io::stdout(), Print("\x1b[>7u\x1b[?u"))?;
        #[cfg(any(unix, windows))]
        {
            crate::ui::theme::reset_terminal_background();
            execute!(std::io::stdout(), Print("\x1b[?2031h\x1b[?996n"))?;
            guard.query_terminal_colors()?;
        }
        Ok(guard)
    }
    #[cfg(any(unix, windows))]
    fn query_terminal_colors(&mut self) -> Result<()> {
        // Serialize queries so that replies remain attributable when DA1 is absent.
        if let Some(query) = self.color_queries.front() {
            self.color_requery = true;
            if query.replied == (1 << 18) - 1 {
                self.expired_color_markers = self.expired_color_markers.saturating_add(1);
                return self.finish_color_query();
            }
            return Ok(());
        }
        self.color_queries.push_back(ColorQuery {
            started: Instant::now(),
            replied: 0,
        });
        let mut query = String::from("\x1b]10;?\x07\x1b]11;?\x07");
        for index in 0..16 {
            query.push_str(&format!("\x1b]4;{index};?\x07"));
        }
        query.push_str("\x1b[c");
        execute!(std::io::stdout(), Print(query))?;
        Ok(())
    }

    #[cfg(any(unix, windows))]
    fn accepts_color_reply(&mut self, target: u8) -> Result<bool> {
        let is_latest = !self.color_requery;
        let Some(query) = self.color_queries.front_mut() else {
            return Ok(false);
        };
        let flag = 1_u32 << target;
        let fresh = query.replied & flag == 0;
        query.replied |= flag;
        if query.replied == (1 << 18) - 1 && self.color_requery {
            self.expired_color_markers = self.expired_color_markers.saturating_add(1);
            self.finish_color_query()?;
        }
        Ok(is_latest && fresh)
    }

    #[cfg(any(unix, windows))]
    fn finish_color_query(&mut self) -> Result<()> {
        self.color_queries.pop_front();
        if std::mem::take(&mut self.color_requery) {
            self.query_terminal_colors()?;
        }
        Ok(())
    }

    #[cfg(any(unix, windows))]
    fn receive_color_marker(&mut self) -> Result<()> {
        if self.expired_color_markers > 0 {
            self.expired_color_markers -= 1;
            Ok(())
        } else {
            self.finish_color_query()
        }
    }

    #[cfg(any(unix, windows))]
    fn poll_color_timeout(&mut self) -> Result<()> {
        if self.color_requery
            && self
                .color_queries
                .front()
                .is_some_and(|query| query.started.elapsed() >= Duration::from_secs(2))
        {
            self.expired_color_markers = self.expired_color_markers.saturating_add(1);
            self.finish_color_query()?;
        }
        Ok(())
    }

    fn update_progress(&mut self, active: bool) -> Result<()> {
        if active
            && self
                .progress
                .is_none_or(|last| last.elapsed() >= Duration::from_secs(1))
        {
            execute!(std::io::stdout(), Print("\x1b]9;4;3\x07"))?;
            self.progress = Some(Instant::now());
        } else if !active && self.progress.is_some() {
            execute!(std::io::stdout(), Print("\x1b]9;4;0\x07"))?;
            self.progress = None;
        }
        Ok(())
    }
    #[cfg(unix)]
    fn keyboard_reply(&mut self, supported: bool) -> Result<()> {
        if supported && self.keyboard != KeyboardProtocol::Kitty {
            if self.keyboard == KeyboardProtocol::Xterm {
                execute!(std::io::stdout(), Print("\x1b[>4;0m"))?;
            }
            self.keyboard = KeyboardProtocol::Kitty;
        } else if self.keyboard == KeyboardProtocol::Probing {
            execute!(std::io::stdout(), Print("\x1b[>4;2m"))?;
            self.keyboard = KeyboardProtocol::Xterm;
        }
        Ok(())
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
        let _ = self.update_progress(false);
        if self.alternate {
            self.images.clear();
        }
        #[cfg(unix)]
        {
            if self.keyboard == KeyboardProtocol::Xterm {
                let _ = execute!(std::io::stdout(), Print("\x1b[>4;0m"));
            }
            let _ = execute!(std::io::stdout(), Print("\x1b[<u"));
        }
        #[cfg(any(unix, windows))]
        let _ = execute!(std::io::stdout(), Print("\x1b[?2031l"));
        let _ = execute!(std::io::stdout(), ResetColor, Show, DisableBracketedPaste);
        if self.alternate {
            let _ = execute!(
                std::io::stdout(),
                DisableFocusChange,
                DisableMouseCapture,
                LeaveAlternateScreen
            );
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
    output: TranscriptOptions,
}
#[derive(Default)]
struct RegularState {
    committed: usize,
    committed_rows: usize,
    max_rows: usize,
    cursor_row: usize,
    region_height: usize,
    started: bool,
    redraw: bool,
    layout: Option<RegularLayout>,
}

enum LocalEditor {
    Setting(String),
    BranchSummary(u64),
    ProviderKey(super::Provider),
    OpenRouterCode(Option<tokio::sync::oneshot::Sender<String>>),
}

#[derive(Clone, Copy)]
enum ModelListIntent {
    Select,
    Cycle { reverse: bool },
    Scope,
}

struct ModelDiscovery {
    receiver: std::sync::mpsc::Receiver<Result<Vec<String>>>,
    intent: ModelListIntent,
}

enum RemoteEvent {
    Notice(String),
    OpenRouterCode(tokio::sync::oneshot::Sender<String>),
    RefreshModelKeys,
    Finished(Result<Vec<String>>),
}

struct RemoteTask {
    status: &'static str,
    cancelled: &'static str,
    started: Instant,
    receiver: std::sync::mpsc::Receiver<RemoteEvent>,
    _cancel: tokio::sync::oneshot::Sender<()>,
}

impl RemoteTask {
    fn start<F, Fut>(status: &'static str, cancelled: &'static str, work: F) -> Self
    where
        F: FnOnce(std::sync::mpsc::Sender<RemoteEvent>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Vec<String>>>,
    {
        let (sender, receiver) = std::sync::mpsc::channel();
        let (cancel, cancel_rx) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let result = (|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                runtime.block_on(async {
                    tokio::select! {
                        biased;
                        _ = cancel_rx => Ok(None),
                        result = work(sender.clone()) => result.map(Some),
                    }
                })
            })();
            match result {
                Ok(Some(lines)) => {
                    let _ = sender.send(RemoteEvent::Finished(Ok(lines)));
                }
                Err(error) => {
                    let _ = sender.send(RemoteEvent::Finished(Err(error)));
                }
                Ok(None) => {}
            }
        });
        Self {
            status,
            cancelled,
            started: Instant::now(),
            receiver,
            _cancel: cancel,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TranscriptPanel {
    Summary(u64),
    Skill(usize),
}

#[derive(Default)]
struct StreamingTool {
    name: String,
    arguments: crate::streaming_json::StreamingJson,
    preview: Value,
}

#[derive(Default)]
struct View {
    regular: RegularState,
    graphics: Renderer,
    dialogs: std::collections::VecDeque<crate::ui::dialog::Dialog>,
    local_editor: Option<(LocalEditor, crate::ui::dialog::Dialog)>,
    tree_confirmation: Option<u64>,
    tree_navigation: Option<(u64, bool)>,
    branch_error: Option<String>,
    ui_status: std::collections::BTreeMap<String, String>,
    widgets: std::collections::BTreeMap<String, (Vec<String>, pi_rs::WidgetPlacement)>,
    lines: Vec<Entry>,
    picker: Option<Picker>,
    session_name: Option<String>,
    resources: super::resources::Resources,
    model_list: Option<ModelDiscovery>,
    remote_task: Option<RemoteTask>,
    available_models: Vec<String>,
    pending_request: Option<Value>,
    pending_session_name: Option<Option<String>>,
    thinking: String,
    expand_tools: bool,
    expand_key: Option<crossterm::event::KeyEvent>,
    panel_rows: Vec<(std::ops::Range<usize>, TranscriptPanel)>,
    panel_press: Option<(TranscriptPanel, u16, u16)>,
    summarizing: bool,
    hide_thinking: bool,
    streaming: String,
    streaming_tools: std::collections::BTreeMap<u64, StreamingTool>,
    editor: Editor,
    editor_width: usize,
    editor_page: usize,
    draft_images: Vec<pi_rs::ImageContent>,
    clipboard: Option<std::sync::mpsc::Receiver<Result<super::clipboard::Content>>>,
    viewport: Viewport,
    search: Option<Search>,
    mouse: MouseSelection,
    wheel: Wheel,
    fullscreen_width: usize,
    scrollbar_activity: Option<Instant>,
    scrollbar_dragging: bool,
    flash: Option<(String, Instant)>,
    tokens: u64,
    cache_stats: super::cache_stats::Tracker,
    pending_thinking_drop: Option<(uuid::Uuid, usize)>,
    previous_thinking_drop: usize,
    cache_warming_status: Option<super::cache_warming::status::State>,
    model_operations: std::collections::HashSet<String>,
    history: Vec<String>,
    history_index: Option<usize>,
    request_started: Option<Instant>,
    phase: Option<&'static str>,
    shell_running: bool,
    restoring_queue: bool,
}
impl View {
    fn streaming_tool_entries(&self) -> impl Iterator<Item = Entry> + '_ {
        self.streaming_tools.values().filter_map(|tool| {
            if tool.name == super::operation_models::TOOL {
                return None;
            }
            Some(Entry::Tool {
                id: String::new(),
                name: if tool.name.is_empty() {
                    "tool".into()
                } else {
                    tool.name.clone()
                },
                arguments: if tool.preview.is_object() {
                    tool.preview.to_string()
                } else {
                    "{}".into()
                },
                output: String::new(),
                failed: false,
                finished: false,
                parts: Vec::new(),
                numbered_diff: false,
            })
        })
    }

    fn dialog(&self) -> Option<&crate::ui::dialog::Dialog> {
        self.local_editor
            .as_ref()
            .map(|(_, dialog)| dialog)
            .or_else(|| self.dialogs.front())
    }
    fn ui_request(&mut self, value: &Value) -> Result<Option<(uuid::Uuid, pi_rs::UiResponse)>> {
        let id: uuid::Uuid = serde_json::from_value(value["id"].clone())?;
        let request: pi_rs::UiRequest = serde_json::from_value(value.clone())?;
        request.validate().map_err(Error::Config)?;
        crate::ui::dialog::Dialog::validate_keys(&request)?;
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
                self.search = None;
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
            .flat_map(|text| {
                Entry::Notice(text.clone()).render(
                    theme,
                    width,
                    false,
                    true,
                    TranscriptOptions::default(),
                )
            })
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
    fn working_phase(&self) -> &'static str {
        match (
            self.tree_navigation.map(|(_, summarize)| summarize),
            self.phase,
        ) {
            (Some(true), Some("再試行待ち")) => "要約の再試行待ち",
            (Some(true), _) => "Summarizing branch",
            (Some(false), _) => "Switching branch",
            (None, Some(phase)) => phase,
            (None, None) if self.shell_running => "コマンド実行中",
            _ => "処理中",
        }
    }

    fn draft_status(&self) -> String {
        if let Some(task) = &self.remote_task {
            format!(
                "{} {:.1}s · Esc cancels · ",
                task.status,
                task.started.elapsed().as_secs_f32()
            )
        } else if self.clipboard.is_some() {
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
    fn paste_clipboard(&mut self, args: &Cli) {
        if self.clipboard.is_some() {
            return;
        }
        let (sender, receiver) = std::sync::mpsc::channel();
        self.clipboard = Some(receiver);
        let auto_resize = args.settings.resize_model_images();
        let profile = args.model_image_resize();
        std::thread::spawn(move || {
            let _ = sender.send(super::clipboard::read(auto_resize, profile));
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
    fn observe_cache(&mut self, record: &pi_rs::Record, args: &Cli) {
        if let Some(notice) = self.cache_stats.observe(record, &args.model_profiles)
            && args.settings.boolean("showCacheMissNotices") == Some(true)
        {
            if matches!(
                record.event,
                pi_rs::Event::SourceEntry { .. } | pi_rs::Event::CacheWarm { .. }
            ) {
                self.append(&notice);
            } else {
                self.lines.push(Entry::Warning(notice));
            }
        }
    }

    fn observe_thinking_drop(&mut self, event: &pi_rs::Event) -> Option<usize> {
        match event {
            pi_rs::Event::ModelFinished {
                request_id,
                metadata,
                failure: None,
                ..
            } => {
                let count = metadata
                    .diagnostics
                    .iter()
                    .map(pi_rs::AssistantDiagnostic::dropped_thinking_blocks)
                    .sum();
                self.pending_thinking_drop = Some((*request_id, count));
            }
            pi_rs::Event::Assistant { request_id, .. } => {
                let count = self
                    .pending_thinking_drop
                    .take()
                    .filter(|(id, _)| Some(id) == request_id.as_ref())
                    .map_or(0, |(_, count)| count);
                let previous = std::mem::replace(&mut self.previous_thinking_drop, count);
                return (count > previous).then_some(count);
            }
            _ => {}
        }
        None
    }

    fn add_summary(&mut self, record: &pi_rs::Record, session: Option<&Session>) {
        let (kind, text) = match &record.event {
            pi_rs::Event::Compacted {
                summary,
                tokens_before,
                ..
            } => (
                SummaryKind::Compaction {
                    tokens_before: tokens_before.unwrap_or_else(|| {
                        session.map_or(0, |session| {
                            super::rpc_messages::compaction_tokens(session, record)
                        })
                    }),
                },
                summary,
            ),
            pi_rs::Event::BranchSummary { summary, .. } => (SummaryKind::Branch, summary),
            _ => return,
        };
        self.lines.push(Entry::Summary {
            id: record.id,
            kind,
            text: text.clone(),
            expanded: None,
        });
    }

    fn set_expand_tools(&mut self, expanded: bool) {
        self.expand_tools = expanded;
        for entry in &mut self.lines {
            if let Entry::Summary { expanded, .. } | Entry::Skill { expanded, .. } = entry {
                *expanded = None;
            }
        }
    }

    fn panel_at(&self, row: u16) -> Option<TranscriptPanel> {
        self.panel_rows
            .iter()
            .find_map(|(rows, id)| rows.contains(&usize::from(row)).then_some(*id))
    }

    fn panel_expansion(&mut self, panel: TranscriptPanel) -> Option<&mut Option<bool>> {
        let entry = match panel {
            TranscriptPanel::Summary(id) => self.lines.iter_mut().find(
                |entry| matches!(entry, Entry::Summary { id: candidate, .. } if *candidate == id),
            ),
            TranscriptPanel::Skill(index) => self.lines.get_mut(index),
        }?;
        match entry {
            Entry::Summary { expanded, .. } | Entry::Skill { expanded, .. } => Some(expanded),
            _ => None,
        }
    }

    fn show_summary_billing(
        &mut self,
        record: &pi_rs::Record,
        records: &[pi_rs::Record],
        args: &Cli,
    ) {
        if args.settings.boolean("showCacheMissNotices") != Some(true) {
            return;
        }
        let Some((usage, metadata)) = record.summary_usage(records) else {
            return;
        };
        let label = if matches!(record.event, pi_rs::Event::BranchSummary { .. }) {
            "Branch summary"
        } else {
            "Compaction"
        };
        let tokens = super::cache_stats::format_tokens(
            usage.input_tokens.saturating_add(usage.output_tokens),
        );
        let cost = metadata
            .and_then(|details| details.metadata.cost_usd)
            .and_then(|number| number.as_f64())
            .filter(|cost| *cost >= 0.01)
            .map_or_else(String::new, |cost| format!(" (~${cost:.2})"));
        self.lines.push(Entry::Warning(format!(
            "{label}: {tokens} tokens billed{cost}"
        )));
    }

    fn receive_worker(&mut self, mut value: Value, args: &mut Cli) {
        if value["type"] == "resources_discovered" {
            match serde_json::from_value::<pi_rs::DiscoveredResources>(value["resources"].take())
                .map_err(Error::from)
                .and_then(|discovered| {
                    super::resources::Resources::load_with_discovered(args, &discovered)
                }) {
                Ok(resources) => {
                    args.theme_paths = resources.themes.clone();
                    self.resources = resources;
                }
                Err(error) => self.append(&format!("Resource discovery: {error}")),
            }
            return;
        }
        if value["type"] == "cache_warming_status" {
            if value["session"] == json!(args.session) {
                self.cache_warming_status = serde_json::from_value(value["status"].take()).ok();
            }
            return;
        }
        self.receive(&value);
        if value["type"] == "record"
            && matches!(
                value["record"]["event"]["type"].as_str(),
                Some(
                    "model_selected"
                        | "model_requested"
                        | "model_finished"
                        | "cache_warm"
                        | "assistant"
                        | "compacted"
                        | "branch_summary"
                )
            )
            && let Ok(record) = serde_json::from_value::<pi_rs::Record>(value["record"].take())
        {
            if matches!(record.event, pi_rs::Event::ModelRequested { .. }) {
                self.cache_stats.select_provider(args.provider.id());
            }
            self.add_summary(&record, None);
            self.show_summary_billing(&record, &[], args);
            self.observe_cache(&record, args);
            if let Some(count) = self.observe_thinking_drop(&record.event)
                && args.settings.boolean("showCacheMissNotices") == Some(true)
            {
                let noun = if count == 1 {
                    "thinking block"
                } else {
                    "thinking blocks"
                };
                self.lines.push(Entry::Warning(format!(
                    "Anthropic dropped {count} {noun} (details in session)"
                )));
            }
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
        self.viewport.follow();
        self.search = None;
        self.mouse.clear();
        self.panel_rows.clear();
        self.panel_press = None;
        self.summarizing = false;
        self.scrollbar_dragging = false;
        self.scrollbar_activity = None;
        self.flash = None;
        self.streaming.clear();
        self.streaming_tools.clear();
        self.thinking.clear();
        self.draft_images.clear();
        self.clipboard = None;
        self.dialogs.clear();
        self.local_editor = None;
        self.tree_confirmation = None;
        self.restoring_queue = false;
        self.ui_status.clear();
        self.widgets.clear();
        self.history.clear();
        self.history_index = None;
        self.tokens = 0;
        self.cache_stats = super::cache_stats::Tracker::default();
        self.pending_thinking_drop = None;
        self.previous_thinking_drop = 0;
        self.cache_warming_status = None;
        self.model_operations.clear();
        let records = session.records();
        let summary_requests: std::collections::HashSet<_> = records
            .windows(2)
            .filter_map(|pair| {
                if matches!(
                    pair[1].event,
                    pi_rs::Event::Compacted { .. } | pi_rs::Event::BranchSummary { .. }
                ) && let pi_rs::Event::ModelFinished {
                    request_id,
                    reply: None,
                    failure: None,
                    ..
                } = pair[0].event
                {
                    Some(request_id)
                } else {
                    None
                }
            })
            .collect();
        for record in session.branch() {
            match &record.event {
                pi_rs::Event::User { content, .. }
                | pi_rs::Event::UserWithImages { content, .. } => {
                    self.history.push(content.clone());
                    self.lines.push(Entry::user(content.clone()));
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
                    metadata,
                    usage,
                    request_id,
                    ..
                } => {
                    if metadata.finish_reason.as_deref() == Some("length") {
                        self.append("Response reached the output token limit.");
                    }
                    if !summary_requests.contains(request_id)
                        && let Some(reasoning) = &metadata.reasoning
                    {
                        self.lines.push(Entry::Thinking(reasoning.clone()));
                    }
                    if let pi_rs::UsageReport::Measured { usage } = usage {
                        self.tokens = self
                            .tokens
                            .saturating_add(usage.input_tokens)
                            .saturating_add(usage.output_tokens);
                    }
                }
                pi_rs::Event::CacheWarm { usage, .. } => {
                    self.tokens = self
                        .tokens
                        .saturating_add(usage.input_tokens)
                        .saturating_add(usage.output_tokens);
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
            self.add_summary(record, Some(&session));
            self.show_summary_billing(record, records, args);
            self.observe_cache(record, args);
            self.observe_thinking_drop(&record.event);
        }
        if show_startup_header(args) {
            self.append(if crate::ui::keys::windows_keybindings() {
                "Enter to send · Ctrl+Q to queue · Ctrl+O tools · Ctrl+T thinking · /help"
            } else {
                "Enter to send · Alt+Enter to queue · Ctrl+O tools · Ctrl+T thinking · /help"
            });
        }
        if (args.verbose
            || args.settings.value()["quietStartup"].as_str() != Some("header")
                && show_startup_header(args))
            && !args.model_scope.is_empty()
        {
            let scope = args
                .model_scope
                .iter()
                .map(super::model_selection::ScopedChoice::pattern)
                .collect::<Vec<_>>()
                .join(", ");
            self.append(&format!("Models in scope · {scope}"));
        }
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
        if invocation.call.name == super::operation_models::TOOL {
            self.model_operations
                .insert(invocation.operation_id.to_string());
            return;
        }
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
        if self.model_operations.remove(operation) {
            if let Some(usage) = pi_rs_extensions::codemode::ModelUsage::from_outcome(outcome) {
                self.tokens = self.tokens.saturating_add(usage.total_tokens);
            }
            return;
        }
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
    fn receive(&mut self, value: &Value) {
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
                self.shell_running = true;
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
                self.shell_running = false;
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
                    Some("resolving_credentials") => "認証情報を取得中",
                    Some("loading_model") => "モデル読込待ち",
                    Some("thinking") => "思考中",
                    Some("generating") => "回答生成中",
                    Some("preparing_tool") => "ツール呼出し準備中",
                    _ => "モデル応答待ち",
                });
            }
            Some("summary_request_start") => {
                self.summarizing = true;
                self.thinking.clear();
                self.streaming.clear();
                self.streaming_tools.clear();
            }
            Some("tool_call_delta") if !self.summarizing => {
                if let Ok(delta) = serde_json::from_value::<pi_rs::ToolCallDelta>(value["toolCallDelta"].clone()) {
                    let tool = self.streaming_tools.entry(delta.index).or_default();
                    tool.name.push_str(&delta.name);
                    if !delta.arguments.is_empty() {
                        tool.preview = tool.arguments.append(&delta.arguments);
                    }
                    self.phase = Some("ツール呼出し準備中");
                }
            }
            Some("reasoning_delta") if !self.summarizing => self
                .thinking
                .push_str(value["delta"].as_str().unwrap_or_default()),
            Some("text_delta") if !self.summarizing => self
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
            Some("extension_error") => self.append(&format!(
                "Extension {} ({}) · {}",
                value["extension"].as_str().unwrap_or_default(),
                value["event"].as_str().unwrap_or_default(),
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
                        self.streaming_tools.clear();
                        self.request_started = Some(Instant::now());
                        self.phase = Some("モデル応答待ち");
                    }
                    Some("assistant") => {
                        self.streaming_tools.clear();
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
                    Some("compacted" | "branch_summary") => self.summarizing = false,
                    Some("model_finished") => {
                        if !event["failure"].is_null() {
                            self.summarizing = false;
                        }
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
                    Some("cache_warm") => {
                        let usage = &event["usage"];
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
        self.summarizing = false;
        self.streaming_tools.clear();
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
    fn copy_selection(&mut self) -> bool {
        let Some(text) = self.mouse.text() else {
            return false;
        };
        let message = match sessions::copy_text(&text) {
            Ok(()) => "Copied!".to_owned(),
            Err(error) => format!("Copy failed: {error}"),
        };
        self.flash = Some((message, Instant::now()));
        true
    }

    fn fullscreen_mouse(&mut self, event: MouseEvent, args: &Cli) -> bool {
        if self.picker.is_some() || self.dialog().is_some() {
            return false;
        }
        let now = Instant::now();
        let scrollbar = args.settings.string("fullscreenScrollbar") != Some("hidden")
            && self.viewport.scrollbar().is_some();
        let on_scrollbar = scrollbar
            && self.fullscreen_width > 1
            && usize::from(event.column) + 1 >= self.fullscreen_width
            && self.mouse.contains_row(event.row);
        match event.kind {
            MouseEventKind::ScrollUp | MouseEventKind::ScrollDown
                if self.mouse.contains_row(event.row) =>
            {
                let direction = if event.kind == MouseEventKind::ScrollUp {
                    -1
                } else {
                    1
                };
                let lines = self.wheel.lines(
                    direction,
                    args.settings.value()["fullscreenWheelScrollLines"].as_f64(),
                    now,
                );
                let multiplier = if event.modifiers.contains(KeyModifiers::ALT) {
                    5
                } else {
                    1
                };
                self.viewport.scroll_by(direction * lines * multiplier);
                self.scrollbar_activity = Some(now);
            }
            MouseEventKind::Down(MouseButton::Left) if on_scrollbar => {
                self.mouse.clear();
                self.scrollbar_dragging = true;
                self.viewport
                    .seek_scrollbar(self.mouse.relative_row(event.row));
                self.scrollbar_activity = Some(now);
            }
            MouseEventKind::Drag(MouseButton::Left) if self.scrollbar_dragging => {
                self.viewport
                    .seek_scrollbar(self.mouse.relative_row(event.row));
                self.scrollbar_activity = Some(now);
            }
            MouseEventKind::Up(MouseButton::Left) if self.scrollbar_dragging => {
                self.scrollbar_dragging = false;
                self.scrollbar_activity = Some(now);
            }
            MouseEventKind::Moved if on_scrollbar => {
                self.scrollbar_activity = Some(now);
            }
            MouseEventKind::Down(MouseButton::Right) if self.search.is_none() => {
                self.paste_clipboard(args);
            }
            MouseEventKind::Down(MouseButton::Left) if self.search.is_none() => {
                self.panel_press = event
                    .modifiers
                    .is_empty()
                    .then(|| self.panel_at(event.row))
                    .flatten()
                    .map(|id| (id, event.column, event.row));
                self.mouse.press(event.column, event.row, now);
            }
            MouseEventKind::Drag(MouseButton::Left) if self.search.is_none() => {
                self.panel_press = None;
                self.mouse.drag(event.column, event.row);
            }
            MouseEventKind::Up(MouseButton::Left) if self.search.is_none() => {
                let default_expanded = self.expand_tools;
                if let Some((panel, column, row)) = self.panel_press.take()
                    && (column, row) == (event.column, event.row)
                    && self.panel_at(row) == Some(panel)
                    && let Some(expanded) = self.panel_expansion(panel)
                {
                    *expanded = Some(!expanded.unwrap_or(default_expanded));
                    self.mouse.clear();
                } else if self.mouse.release(event.column, event.row)
                    && args.settings.boolean("fullscreenCopyOnSelect") != Some(false)
                {
                    self.copy_selection();
                }
            }
            _ => return false,
        }
        true
    }

    fn prompt_rows(
        &mut self,
        args: &Cli,
        width: usize,
        height: usize,
    ) -> (Vec<String>, usize, usize) {
        let padding = (args.settings.value()["editorPaddingX"]
            .as_u64()
            .unwrap_or(0) as usize)
            .min(width.saturating_sub(1) / 2);
        self.editor_width = width - 2 * padding;
        self.editor_page = (height * 3 / 10).max(5).min(height - 5);
        let (mut rows, cursor, column) = self
            .editor
            .visible_rows(self.editor_width, self.editor_page);
        if padding > 0 {
            let left = " ".repeat(padding);
            for row in &mut rows {
                row.insert_str(0, &left);
            }
        }
        (rows, cursor, column + padding)
    }

    fn draw(&mut self, args: &Cli, theme: &Theme, busy: bool, keys: &Keys) -> Result<()> {
        self.expand_key = keys.first_binding("app.tools.expand");
        if args.tui_mode == Some(TuiMode::Regular) {
            return self.draw_regular(args, theme, busy, keys);
        }
        let (width, height) = terminal::size()?;
        let width = usize::from(width.max(1));
        self.fullscreen_width = width;
        let scrollbar_mode = args
            .settings
            .string("fullscreenScrollbar")
            .unwrap_or("auto");
        let body_width = width - usize::from(scrollbar_mode == "always" && width > 1);
        let height = usize::from(height.max(7));
        let hardware_cursor = args.settings.show_hardware_cursor();
        let (prompt, cursor_row, column) = self.prompt_rows(args, width, height);
        let prompt_height = prompt.len();
        let mut below = self.widget_lines(theme, width, pi_rs::WidgetPlacement::BelowEditor);
        below.truncate(height.saturating_sub(prompt_height + 6));
        if self.dialog().is_some() {
            below.clear();
        }
        let output = self.transcript_options(args, false);
        let header_rows = usize::from(show_startup_header(args));
        let body_height = height.saturating_sub(prompt_height + 4 + header_rows + below.len());
        let mut lines = Vec::new();
        let mut prompts = Vec::new();
        let mut panels = Vec::new();
        for (index, entry) in self.lines.iter().enumerate() {
            if matches!(entry, Entry::User(_) | Entry::Skill { .. }) {
                prompts.push(lines.len());
            }
            let start = lines.len();
            lines.extend(entry.render(
                theme,
                body_width,
                self.expand_tools,
                !self.hide_thinking,
                output,
            ));
            match entry {
                Entry::Summary { id, .. } => panels.push((
                    start..lines.len().saturating_sub(1),
                    TranscriptPanel::Summary(*id),
                )),
                Entry::Skill { .. } => {
                    let panel_height = lines[start..]
                        .iter()
                        .take_while(|line| line.background == Some(theme.color("customMessageBg")))
                        .count();
                    panels.push((start..start + panel_height, TranscriptPanel::Skill(index)));
                }
                _ => {}
            }
        }
        if !self.thinking.is_empty() {
            lines.extend(Entry::Thinking(self.thinking.clone()).render(
                theme,
                body_width,
                false,
                !self.hide_thinking,
                self.transcript_options(args, true),
            ));
        }
        if !self.streaming.is_empty() {
            lines.extend(Entry::Assistant(self.streaming.clone()).render(
                theme,
                body_width,
                false,
                true,
                self.transcript_options(args, true),
            ));
        }
        for entry in self.streaming_tool_entries() {
            lines.extend(entry.render(
                theme,
                body_width,
                self.expand_tools,
                !self.hide_thinking,
                output,
            ));
        }
        if let Some(picker) = &self.picker {
            lines = picker.render(theme, width, body_height, keys);
        }
        let dialog_cursor = self
            .dialog()
            .and_then(|dialog| dialog.cursor(width, body_height));
        if let Some(dialog) = self.dialog() {
            lines = dialog.render(theme, width, body_height);
            if !hardware_cursor
                && let Some((row, column)) = dialog_cursor
                && let Some(line) = lines.get_mut(row)
            {
                line.mark_cursor(column);
            }
        } else {
            lines.extend(self.widget_lines(theme, width, pi_rs::WidgetPlacement::AboveEditor));
        }
        let modal = self.picker.is_some() || self.dialog().is_some();
        let start = if modal {
            0
        } else {
            self.viewport.update(lines.len(), body_height, prompts);
            if let Some(search) = &mut self.search
                && let Some(rows) = search.update(&lines, self.viewport.top())
            {
                self.viewport.reveal(rows);
            }
            self.viewport.top()
        };
        let end = (start + body_height).min(lines.len());
        self.panel_rows.clear();
        if modal {
            self.panel_press = None;
            self.mouse.clear();
        } else {
            self.panel_rows
                .extend(panels.into_iter().filter_map(|(rows, id)| {
                    let first = rows.start.max(start);
                    let last = rows.end.min(end);
                    (first < last)
                        .then(|| (header_rows + first - start..header_rows + last - start, id))
                }));
            self.mouse
                .update(&lines, start, header_rows..header_rows + body_height);
            if let Some(search) = &self.search {
                search.highlight(&mut lines, start..end, theme);
            } else {
                self.mouse.highlight(&mut lines, start..end);
            }
        }
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
                self.working_phase(),
                elapsed.as_secs_f32()
            )
        } else {
            String::new()
        };
        let status = format!(
            "{status}{}{}{}",
            self.draft_status(),
            self.extension_status(),
            self.flash
                .as_ref()
                .map_or(String::new(), |(message, _)| format!("{message} ")),
        );
        self.graphics.clear_placements(&mut out)?;
        queue!(out, Hide, MoveTo(0, 0), Clear(ClearType::All))?;
        if header_rows != 0 {
            Line::themed(
                &format!(
                    "pi v{} · {} · /help",
                    env!("CARGO_PKG_VERSION"),
                    self.session_name.as_deref().unwrap_or("new session")
                ),
                theme,
                "accent",
            )
            .draw(&mut out, width)?;
        }
        for (row, line) in lines[start..end].iter().enumerate() {
            queue!(out, MoveTo(0, (row + header_rows) as u16))?;
            line.draw(&mut out, if modal { width } else { body_width })?;
            self.graphics.paint(
                line,
                &mut out,
                row,
                images::visible_rows(&lines[start + row..end]),
            )?;
        }
        if !modal
            && width > 1
            && (scrollbar_mode == "always"
                || (scrollbar_mode == "auto"
                    && (self.scrollbar_activity.is_some() || self.scrollbar_dragging)))
            && let Some(thumb) = self.viewport.scrollbar()
        {
            for row in 0..body_height {
                let (glyph, role) = if thumb.contains(&row) {
                    (
                        if self.scrollbar_dragging {
                            "█"
                        } else {
                            "┃"
                        },
                        "scrollbarThumb",
                    )
                } else {
                    ("│", "scrollbarTrack")
                };
                queue!(out, MoveTo((width - 1) as u16, (row + header_rows) as u16))?;
                Line::themed(glyph, theme, role).draw(&mut out, 1)?;
            }
        }
        let border_row = height - prompt_height - 4 - below.len();
        queue!(out, MoveTo(0, border_row as u16))?;
        Line::themed(
            &format!(
                "{}{}",
                status,
                "─".repeat(width.saturating_sub(UnicodeWidthStr::width(status.as_str())))
            ),
            theme,
            "border",
        )
        .draw(&mut out, width)?;
        for (row, line) in prompt.iter().enumerate() {
            queue!(out, MoveTo(0, (border_row + 1 + row) as u16))?;
            let mut line = Line::themed(line, theme, "text");
            if row == cursor_row && !hardware_cursor && !modal && self.search.is_none() {
                line.mark_cursor(column);
            }
            line.draw(&mut out, width)?;
        }
        for (row, line) in below.iter().enumerate() {
            queue!(
                out,
                MoveTo(0, (border_row + 1 + prompt_height + row) as u16)
            )?;
            line.draw(&mut out, width)?;
        }
        queue!(out, MoveTo(0, (height - 3) as u16))?;
        Line::themed(&"─".repeat(width), theme, "border").draw(&mut out, width)?;
        queue!(out, MoveTo(0, (height - 2) as u16))?;
        Line::themed(&args.workspace.display().to_string(), theme, "dim").draw(&mut out, width)?;
        queue!(out, MoveTo(0, (height - 1) as u16))?;
        let model = format!(
            "{} · {}",
            args.configured_model_name(&args.model),
            args.thinking.as_deref().unwrap_or("default")
        );
        let model_width = UnicodeWidthStr::width(model.as_str());
        let usage = format!("↑↓ 確定 {} tokens", self.tokens);
        let usage = clip(&usage, width.saturating_sub(model_width + 1));
        let padding = width.saturating_sub(UnicodeWidthStr::width(usage.as_str()) + model_width);
        Line::themed(
            &format!("{usage}{}{model}", " ".repeat(padding)),
            theme,
            "dim",
        )
        .draw(&mut out, width)?;
        if !modal && let Some(search) = &self.search {
            let search_width = (width * 2 / 5).max(32).min(width.saturating_sub(2).max(1));
            let left = width.saturating_sub(search_width + 1);
            let top = header_rows + usize::from(body_height > 3);
            let (mut overlay, cursor_row, cursor_column) =
                search.overlay(theme, keys, search_width);
            if !hardware_cursor && let Some(line) = overlay.get_mut(cursor_row) {
                line.mark_cursor(cursor_column);
            }
            for (row, line) in overlay.iter().take(body_height).enumerate() {
                queue!(out, MoveTo(left as u16, (top + row) as u16))?;
                line.draw(&mut out, search_width)?;
            }
            if cursor_row < body_height {
                queue!(
                    out,
                    MoveTo((left + cursor_column) as u16, (top + cursor_row) as u16)
                )?;
                if hardware_cursor {
                    queue!(out, Show)?;
                }
            }
        } else if let Some((row, column)) = dialog_cursor {
            queue!(out, MoveTo(column as u16, (row + header_rows) as u16))?;
            if hardware_cursor {
                queue!(out, Show)?;
            }
        } else if !modal {
            queue!(
                out,
                MoveTo(column as u16, (border_row + 1 + cursor_row) as u16)
            )?;
            if hardware_cursor {
                queue!(out, Show)?;
            }
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

    fn transcript_options(&self, args: &Cli, streaming: bool) -> TranscriptOptions {
        TranscriptOptions {
            image_width: (self.graphics.enabled() && args.settings.show_terminal_images())
                .then(|| args.settings.terminal_image_width()),
            padding: args.settings.output_padding(),
            expand_key: self.expand_key,
            mermaid: match (args.settings.mermaid_mode(), streaming) {
                ("off", _) | ("final", true) => Mermaid::Off,
                (_, true) => Mermaid::Streaming,
                (_, false) => Mermaid::Final,
            },
        }
    }

    fn replay_transcript(&mut self, args: &Cli, theme: &Theme) -> Result<()> {
        self.graphics.reset();
        let output = self.transcript_options(args, false);
        let width = usize::from(terminal::size()?.0.max(1));
        let mut out = std::io::stdout().lock();
        queue!(out, MoveToColumn(0), Print("\r\n"))?;
        for entry in &self.lines {
            for line in entry.render(theme, width, self.expand_tools, !self.hide_thinking, output) {
                line.draw(&mut out, width)?;
                self.graphics
                    .paint(&line, &mut out, usize::MAX, usize::MAX)?;
                queue!(out, Print("\r\n"))?;
            }
        }
        out.flush()?;
        Ok(())
    }

    fn draw_regular(&mut self, args: &Cli, theme: &Theme, busy: bool, keys: &Keys) -> Result<()> {
        let (width, height) = terminal::size()?;
        let width = usize::from(width.max(1));
        let height = usize::from(height.max(7));
        let hardware_cursor = args.settings.show_hardware_cursor();
        let output = self.transcript_options(args, false);
        let layout = RegularLayout {
            width,
            height,
            cell: images::cell_dimensions(),
            theme: theme.clone(),
            expand_tools: self.expand_tools,
            hide_thinking: self.hide_thinking,
            output,
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
        let (prompt, cursor, column) = self.prompt_rows(args, width, height);
        let prompt_height = prompt.len();
        let mut below = self.widget_lines(theme, width, pi_rs::WidgetPlacement::BelowEditor);
        below.truncate(height.saturating_sub(prompt_height + 6));
        if self.dialog().is_some() {
            below.clear();
        }
        let render_entry = |entry: &Entry| {
            entry.render(theme, width, self.expand_tools, !self.hide_thinking, output)
        };
        let pending = || {
            self.lines[self.regular.committed..]
                .iter()
                .flat_map(render_entry)
        };
        let modal = self.picker.is_some() || self.dialog().is_some();
        let committing = !busy && !modal;
        let mut active = Vec::new();
        if !committing {
            active.extend(pending());
            active.extend(Entry::Thinking(self.thinking.clone()).render(
                theme,
                width,
                false,
                !self.hide_thinking,
                self.transcript_options(args, true),
            ));
            active.extend(Entry::Assistant(self.streaming.clone()).render(
                theme,
                width,
                false,
                true,
                self.transcript_options(args, true),
            ));
            for entry in self.streaming_tool_entries() {
                active.extend(render_entry(&entry));
            }
            if let Some(picker) = &self.picker {
                active =
                    picker.render(theme, width, height - prompt_height - 4 - below.len(), keys);
            }
        }
        let dialog_height = height - prompt_height - 4 - below.len();
        let dialog_cursor = self
            .dialog()
            .and_then(|dialog| dialog.cursor(width, dialog_height));
        if let Some(dialog) = self.dialog() {
            active = dialog.render(theme, width, dialog_height);
            if !hardware_cursor
                && let Some((row, column)) = dialog_cursor
                && let Some(line) = active.get_mut(row)
            {
                line.mark_cursor(column);
            }
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
        let region_height = active.len() + prompt_height + 4 + below.len();
        let rows_to_match = self
            .regular
            .max_rows
            .saturating_sub(self.regular.committed_rows + region_height);
        let redraw = redraw
            || (args.settings.clear_terminal_on_shrink()
                && !modal
                && rows_to_match > 0
                && (!committing || pending().take(rows_to_match).count() < rows_to_match));
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
            if (self.regular.committed == 0 || redraw) && show_startup_header(args) {
                Line::themed(
                    &format!("pi v{} · /help", env!("CARGO_PKG_VERSION")),
                    theme,
                    "accent",
                )
                .draw(&mut out, width)?;
                queue!(out, Print("\r\n"))?;
            }
        }
        if redraw {
            self.regular.committed_rows = 0;
            for line in self.lines[..self.regular.committed]
                .iter()
                .flat_map(render_entry)
            {
                line.draw(&mut out, width)?;
                self.graphics
                    .paint(&line, &mut out, usize::MAX, usize::MAX)?;
                queue!(out, Print("\r\n"))?;
                self.regular.committed_rows += 1;
            }
        }
        if committing {
            for line in pending() {
                line.draw(&mut out, width)?;
                self.graphics
                    .paint(&line, &mut out, usize::MAX, usize::MAX)?;
                queue!(out, Print("\r\n"))?;
                self.regular.committed_rows += 1;
            }
            self.regular.committed = self.lines.len();
        }
        let total_rows = self.regular.committed_rows + region_height;
        self.regular.max_rows = if redraw {
            total_rows
        } else {
            self.regular.max_rows.max(total_rows)
        };
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
            format!("{} {elapsed:.1}s · Escで取消 ", self.working_phase())
        } else {
            String::new()
        };
        let status = format!("{status}{}{}", self.draft_status(), self.extension_status());
        Line::themed(
            &format!(
                "{}{}",
                status,
                "─".repeat(width.saturating_sub(UnicodeWidthStr::width(status.as_str())))
            ),
            theme,
            "border",
        )
        .draw(&mut out, width)?;
        queue!(out, Print("\r\n"))?;
        for (row, line) in prompt.iter().enumerate() {
            let mut line = Line::themed(line, theme, "text");
            if row == cursor && !hardware_cursor && self.picker.is_none() && self.dialog().is_none()
            {
                line.mark_cursor(column);
            }
            line.draw(&mut out, width)?;
            queue!(out, Print("\r\n"))?;
        }
        for line in &below {
            line.draw(&mut out, width)?;
            queue!(out, Print("\r\n"))?;
        }
        for line in [
            Line::themed(&"─".repeat(width), theme, "border"),
            Line::themed(&args.workspace.display().to_string(), theme, "dim"),
        ] {
            line.draw(&mut out, width)?;
            queue!(out, Print("\r\n"))?;
        }
        Line::themed(
            &format!(
                "↑↓ {} tokens · {} · {}",
                self.tokens,
                args.configured_model_name(&args.model),
                args.thinking.as_deref().unwrap_or("default")
            ),
            theme,
            "dim",
        )
        .draw(&mut out, width)?;
        self.regular.cursor_row = active.len() + 1 + cursor;
        self.regular.region_height = region_height;
        let (column, show_cursor) = if self.dialog().is_some() {
            self.regular.cursor_row = dialog_cursor.map_or(0, |(row, _)| row);
            (
                dialog_cursor.map_or(0, |(_, column)| column),
                dialog_cursor.is_some(),
            )
        } else {
            (column, self.picker.is_none())
        };
        queue!(
            out,
            MoveUp((self.regular.region_height - self.regular.cursor_row - 1) as u16),
            MoveToColumn(column as u16)
        )?;
        if hardware_cursor && show_cursor {
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

const HELP: &str = "/help  /new  /resume [path]  /session  /model <name>  /scoped-models  /compact  /tree  /checkout <id>  /quit\n/login [provider]  /logout [provider]\n/extensions  /extension <id> <command> [arguments]\n/context-edit <id> <text>  /context-delete <id>\n/notes [text]  /todo [JSON list]  /plan <text|accept|discard>  /review <task>\nShift+Enter inserts a newline. Ctrl+PageUp/PageDown moves through the editor. Up/Down recall prompts at the editor boundaries.\nFullscreen: PageUp/PageDown scroll. Ctrl+Shift+F finds transcript text. Ctrl+Up/Down jumps between prompts.\nEnter while working steers the next model response. Alt+Enter queues a follow-up.";

fn slash(
    input: &str,
    args: &mut Cli,
    view: &mut View,
    worker: Option<&mut Option<Worker>>,
) -> Result<Option<Worker>> {
    let (name, rest) = input.split_once(char::is_whitespace).unwrap_or((input, ""));
    let rest = rest.trim();
    match name {
        "/help" | "/hotkeys" => view.append(HELP),
        "/session" => {
            let session = Session::snapshot(&args.session)?;
            view.append(&serde_json::to_string_pretty(&sessions::summary(
                &args.session,
                &session,
            ))?);
            view.append(&super::cache_warming::status::describe(
                view.cache_warming_status.as_ref(),
                args.settings.string("cacheWarming").unwrap_or("streaming"),
                session.head(),
            ));
            let stats = &view.cache_stats;
            if stats.miss_count > 0 {
                let cost = if stats.missed_cost >= 0.0001 {
                    format!(" ${:.3}", stats.missed_cost)
                } else {
                    String::new()
                };
                let misses = if stats.miss_count == 1 {
                    "miss"
                } else {
                    "misses"
                };
                view.append(&format!(
                    "Cache Re-billed{cost} ({} tokens, {} {misses})",
                    stats.missed_tokens, stats.miss_count
                ));
            }
        }
        "/name" => {
            let mut session = Session::open(&args.session)?;
            if !rest.is_empty() {
                let name = pi_rs::sanitize_session_name(rest);
                if session.name().unwrap_or_default() != name {
                    session.set_name(&name)?;
                    view.pending_session_name = Some(session.name().map(str::to_owned));
                }
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
            let items: Vec<_> = session
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
            if items.is_empty() {
                view.append("No messages to fork from");
            } else {
                let mut picker = Picker::new("Fork from user message", items);
                picker.select_last();
                view.picker = Some(picker);
            }
        }
        "/clone" | "/fork" => {
            let session = Session::open(&args.session)?;
            let (entry_id, position, target, input) = if name == "/fork" {
                let target = rest
                    .parse::<u64>()
                    .map_err(|_| Error::Config("use /fork <user-record-id>".into()))?;
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
                (
                    record.id,
                    pi_rs::ForkPosition::Before,
                    record.parent.unwrap_or(1),
                    Some(content.clone()),
                )
            } else {
                (
                    session.head(),
                    pi_rs::ForkPosition::At,
                    session.head(),
                    None,
                )
            };
            if Worker::check_session_transition(
                worker,
                args,
                pi_rs::BeforeSessionTransition::Fork { entry_id, position },
            )? {
                view.append("Session fork cancelled");
                return Ok(None);
            }
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
            args.session_transition = Some(pi_rs::SessionStartReason::Fork);
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
        "/share" => {
            let destination = sessions::ShareDestination::configured(&args.settings)?;
            let status = destination.status();
            let path = args.session.clone();
            let workspace = args.workspace.clone();
            view.remote_task = Some(RemoteTask::start(
                status,
                "Share cancelled",
                move |_| async move {
                    let session = Session::snapshot(path)?;
                    let shared = destination.share(session, &workspace).await?;
                    let mut lines = vec![format!("Share URL: {}", shared.url)];
                    if let Some(gist) = shared.gist {
                        lines.push(format!("Gist: {gist}"));
                    }
                    Ok(lines)
                },
            ));
        }
        "/model" if rest.is_empty() => {
            args.reload_model_profiles()?;
            start_model_picker(args, view, ModelListIntent::Select);
        }
        "/scoped-models" => {
            args.reload_model_profiles()?;
            start_model_picker(args, view, ModelListIntent::Scope);
        }
        "/login" if rest.eq_ignore_ascii_case("radius") => show_radius_methods(view),
        "/login" if rest.eq_ignore_ascii_case("radius browser") => {
            start_radius_login(view, radius_auth::Method::Browser)?;
        }
        "/login" if rest.eq_ignore_ascii_case("radius device-code") => {
            start_radius_login(view, radius_auth::Method::Device)?;
        }
        "/logout" if rest.eq_ignore_ascii_case("radius") => start_radius_logout(view)?,
        "/login"
            if rest.eq_ignore_ascii_case("openrouter")
                || rest.eq_ignore_ascii_case("openrouter browser") =>
        {
            start_openrouter_login(view)
        }
        "/login" if rest.eq_ignore_ascii_case("openrouter api-key") => edit_provider_key(
            super::Provider::named("openrouter").expect("built-in OpenRouter provider"),
            view,
        ),
        "/login" | "/logout" => {
            let login = name == "/login";
            if rest.is_empty() {
                let mut items = args
                    .model_profiles
                    .providers()
                    .filter(|provider| login || args.model_keys.has(provider))
                    .map(|provider| Item {
                        label: provider.id().into(),
                        selection: if login {
                            Selection::LoginProvider(provider)
                        } else {
                            Selection::LogoutProvider(provider)
                        },
                    })
                    .collect::<Vec<_>>();
                if login
                    || radius_auth::Gateway::configured()
                        .and_then(|gateway| gateway.has_credential())
                        .unwrap_or(false)
                {
                    items.push(Item {
                        label: "radius · OAuth".into(),
                        selection: if login {
                            Selection::RadiusMethods
                        } else {
                            Selection::LogoutRadius
                        },
                    });
                }
                if login {
                    items.insert(
                        0,
                        Item {
                            label: "openrouter · OAuth".into(),
                            selection: Selection::LoginOpenRouter,
                        },
                    );
                }
                if items.is_empty() {
                    view.append("No saved provider credentials");
                } else {
                    view.picker = Some(Picker::new(
                        if login {
                            "Sign in to a provider"
                        } else {
                            "Sign out of a provider"
                        },
                        items,
                    ));
                }
            } else {
                let provider = args
                    .model_profiles
                    .provider(rest)
                    .ok_or_else(|| Error::Config("unsupported model provider".into()))?;
                if login {
                    edit_provider_key(provider, view);
                } else {
                    remove_provider_key(provider, args, view)?;
                }
            }
        }
        "/model" => {
            args.reload_model_profiles()?;
            select_model_input(args, view, rest)?;
            view.append(&format!("Model › {rest}"));
        }
        "/thinking" if rest.is_empty() => {
            let default = args
                .settings
                .string("defaultThinkingLevel")
                .unwrap_or("off")
                .to_owned();
            let current = args.thinking.clone().unwrap_or_else(|| "off".into());
            let mut picker = Picker::new(
                "Thinking level",
                super::thinking::available(args)
                    .into_iter()
                    .map(|level| Item {
                        label: format!(
                            "{}{level}{}",
                            if level == current { "✓ " } else { "" },
                            if level == default { " · default" } else { "" },
                        ),
                        selection: Selection::Thinking(level),
                    })
                    .collect(),
            );
            picker.focus_thinking(&current);
            view.picker = Some(picker);
        }
        "/thinking" => super::thinking::set(args, rest)?,
        "/new" => {
            if Worker::check_session_transition(
                worker,
                args,
                pi_rs::BeforeSessionTransition::Switch {
                    reason: pi_rs::SessionSwitchReason::New,
                    target_session_file: None,
                },
            )? {
                view.append("New session cancelled");
                return Ok(None);
            }
            let mut next = args.clone();
            next.session = session_path(&args.workspace, args.session_dir.as_deref());
            if let Some(directory) = next.session.parent() {
                std::fs::create_dir_all(directory)?;
            }
            initialize_session(&mut next)?;
            view.restore(&next)?;
            next.session_transition = Some(pi_rs::SessionStartReason::New);
            *args = next;
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
            if Worker::check_session_transition(
                worker,
                args,
                pi_rs::BeforeSessionTransition::Switch {
                    reason: pi_rs::SessionSwitchReason::Resume,
                    target_session_file: Some(path.clone()),
                },
            )? {
                view.append("Session resume cancelled");
                return Ok(None);
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
                        pi_rs::Event::BranchSummary { summary, .. } => format!(
                            "Branch summary: {}",
                            summary.chars().take(56).collect::<String>()
                        ),
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
                        | pi_rs::Event::Compacted { .. }
                        | pi_rs::Event::BranchSummary { .. } => TreeEntryKind::Message,
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
            let mut picker = Picker::tree(items, entries, labels, head, filter);
            picker.set_label_timestamps(session.label_timestamps());
            view.picker = Some(picker);
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
            if let Some(expanded) = view
                .resources
                .expand(input)
                .or_else(|| input.starts_with("/skill:").then(|| input.to_owned()))
            {
                view.lines.push(Entry::user(expanded.clone()));
                view.pending_request = Some(json!({"type":"prompt","content":input}));
                return Ok(None);
            }
            view.pending_request =
                Some(json!({"type":"prompt","content":input,"require_command":true}));
        }
    }
    Ok(None)
}

/// Run the startup session picker on the controlling terminal, leaving stdout
/// and stdin available for print/JSON/RPC mode protocols.
pub(super) fn select_startup_session(args: &mut Cli) -> Result<Option<PathBuf>> {
    #[cfg(unix)]
    let output: Box<dyn Write> = Box::new(
        std::fs::OpenOptions::new()
            .write(true)
            .open("/dev/tty")
            .map_err(|_| {
                Error::Config("--resume requires a controlling terminal to select a session".into())
            })?,
    );
    #[cfg(not(unix))]
    let output: Box<dyn Write> = {
        if !std::io::stderr().is_terminal() {
            return Err(Error::Config(
                "--resume requires a terminal to select a session".into(),
            ));
        }
        Box::new(std::io::stderr())
    };
    struct PickerTerminal(Box<dyn Write>);
    impl Drop for PickerTerminal {
        fn drop(&mut self) {
            let _ = execute!(
                self.0,
                ResetColor,
                Show,
                DisableBracketedPaste,
                LeaveAlternateScreen
            );
            let _ = terminal::disable_raw_mode();
        }
    }
    terminal::enable_raw_mode()?;
    let mut screen = PickerTerminal(output);
    execute!(screen.0, EnterAlternateScreen, EnableBracketedPaste, Hide)?;
    let mut input = crate::ui::input::Reader::new()?;
    let keys = Keys::load()?;
    let mut theme = Theme::load(
        args.use_theme.as_deref().unwrap_or("system"),
        &args.themes,
        &args.workspace,
        args.trusted,
        !args.no_themes,
    )
    .or_else(|error| {
        if !super::resources::Resources::load(args)?
            .extensions
            .is_empty()
        {
            Ok(Theme::system())
        } else {
            Err(error)
        }
    })?;
    let mut view = View::default();
    slash("/resume", args, &mut view, None)?;
    let mut worker = None;
    let mut dirty = true;
    loop {
        if dirty {
            let (width, height) = terminal::size().unwrap_or((80, 24));
            execute!(screen.0, MoveTo(0, 0), Clear(ClearType::All))?;
            if let Some(picker) = &view.picker {
                for (row, line) in picker
                    .render(&theme, width as usize, height as usize, &keys)
                    .iter()
                    .enumerate()
                {
                    if row >= height as usize {
                        break;
                    }
                    queue!(screen.0, MoveTo(0, row as u16))?;
                    line.draw(&mut screen.0, width as usize)?;
                }
            }
            screen.0.flush()?;
            dirty = false;
        }
        match input.next(Duration::from_millis(100))? {
            Some(crate::ui::input::InputEvent::Event(Event::Resize(..))) => dirty = true,
            Some(crate::ui::input::InputEvent::Event(Event::Paste(text))) => {
                if let Some(picker) = &mut view.picker {
                    picker.paste(&text);
                }
                dirty = true;
            }
            Some(crate::ui::input::InputEvent::Event(Event::Key(key)))
                if key.kind != KeyEventKind::Release =>
            {
                dirty = true;
                let mut picker = view.picker.take().expect("startup session picker");
                match picker.input(key, &keys) {
                    Input::Cancel => return Ok(None),
                    Input::Select(Selection::Resume(path)) => return Ok(Some(path)),
                    Input::Select(
                        selection @ (Selection::RenameSession(..) | Selection::DeleteSession(..)),
                    ) => {
                        if let Err(error) =
                            select(selection, args, &mut view, &mut theme, &mut worker)
                        {
                            picker.show_feedback(Err(error.to_string()));
                            view.picker = Some(picker);
                        }
                    }
                    _ => view.picker = Some(picker),
                }
            }
            _ => {}
        }
    }
}

pub(super) async fn run(mut args: Cli, follow_up_prompts: Vec<String>) -> Result<()> {
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
    let mut deferred_theme_error = None;
    let mut theme = match Theme::load(
        args.use_theme.as_deref().unwrap_or("system"),
        &args.theme_paths,
        &args.workspace,
        args.trusted,
        false,
    ) {
        Ok(theme) => theme,
        Err(error) if !initial_resources.extensions.is_empty() => {
            deferred_theme_error = Some(error.to_string());
            Theme::system()
        }
        Err(error) => return Err(error),
    };
    let mut view = View {
        expand_tools: args.settings.boolean("expandTools").unwrap_or(false),
        hide_thinking: args.settings.boolean("hideThinkingBlock").unwrap_or(false),
        ..View::default()
    };
    view.restore(&args)?;
    view.resources = initial_resources;
    if args.trust_pending {
        slash("/trust", &mut args, &mut view, None)?;
    }
    if args
        .resume_session
        .as_ref()
        .is_some_and(|path| path.is_empty())
    {
        slash("/resume", &mut args, &mut view, None)?;
    }
    let mut keys = Keys::load()?;
    let mut _terminal = Some(Terminal::enter(args.tui_mode, view.graphics.cleanup())?);
    let mut input = crate::ui::input::Reader::new()?;
    if view.graphics.enabled() {
        crate::ui::images::query_cell_size()?;
    }
    let mut worker: Option<Worker> = None;
    let mut startup_extensions: Option<Vec<PathBuf>> = None;
    let mut initial: VecDeque<_> = args
        .initial_prompt
        .take()
        .map(|prompt| (prompt, std::mem::take(&mut args.initial_images)))
        .into_iter()
        .chain(
            follow_up_prompts
                .into_iter()
                .map(|prompt| (prompt, Vec::new())),
        )
        .collect();
    let mut dirty = true;
    let frame_interval = Duration::from_millis(16);
    let mut last_frame = Instant::now() - frame_interval;
    let mut last_idle_interrupt: Option<Instant> = None;
    let mut theme_watcher = ThemeWatcher::new(&theme);
    let mut last_theme_check = Instant::now();
    loop {
        if !args.trust_pending
            && worker.is_none()
            && !view.resources.extensions.is_empty()
            && startup_extensions.as_ref() != Some(&view.resources.extensions)
        {
            startup_extensions = Some(view.resources.extensions.clone());
            match Worker::start(&args, &["serve"]) {
                Ok(active) => worker = Some(active),
                Err(error) => view.append(&error.to_string()),
            }
            dirty = true;
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
        if let Some(name) = view.pending_session_name.take()
            && let Err(error) = Worker::notify_session_name(&mut worker, &args, name)
        {
            view.append(&error.to_string());
            dirty = true;
        }
        if let Some(reason) = args.session_transition.take()
            && let Err(error) = Worker::transition(&mut worker, &args, reason)
        {
            view.append(&error.to_string());
            dirty = true;
        }
        if worker
            .as_ref()
            .is_some_and(|active| !active.busy && !active.bashing && !active.matches(&args))
        {
            worker.take();
        }
        if let Some(discovery) = &view.model_list {
            let intent = discovery.intent;
            match discovery.receiver.try_recv() {
                Ok(Ok(models)) => {
                    view.model_list = None;
                    if models.is_empty() {
                        view.append("No models available");
                    } else {
                        match intent {
                            ModelListIntent::Scope => {
                                let enabled = if args.model_scope.is_empty() {
                                    models.clone()
                                } else {
                                    args.model_scope
                                        .iter()
                                        .map(super::model_selection::ScopedChoice::qualified)
                                        .collect()
                                };
                                view.picker = Some(Picker::scoped_models(
                                    args.provider.id(),
                                    models,
                                    enabled,
                                ));
                            }
                            ModelListIntent::Cycle { reverse } => {
                                view.available_models = models;
                                if let Err(error) = cycle_model(&mut args, &mut view, reverse) {
                                    view.append(&error.to_string());
                                }
                            }
                            ModelListIntent::Select => {
                                view.available_models = models.clone();
                                view.picker = Some(Picker::new(
                                    "Select model",
                                    models
                                        .into_iter()
                                        .map(|model| Item {
                                            label: super::model_selection::qualified_model(
                                                &model,
                                                &args.model_profiles,
                                            )
                                            .and_then(|(provider, id)| {
                                                args.model_profiles.model(&provider, id)?.name
                                            })
                                            .map_or_else(
                                                || model.clone(),
                                                |name| format!("{name} · {model}"),
                                            ),
                                            selection: Selection::Model(model),
                                        })
                                        .collect(),
                                ));
                            }
                        }
                    }
                    dirty = true;
                }
                Ok(Err(error)) => {
                    view.model_list = None;
                    view.append(&error.to_string());
                    dirty = true;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    view.model_list = None;
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
            }
        }
        if let Some(active) = &mut worker {
            if (active.busy || active.bashing) && view.request_started.is_none() {
                view.request_started = Some(Instant::now());
            }
            let events: Vec<_> = active.drain_events();
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
                if value["type"] == "backend_ready"
                    && let Some(error) = deferred_theme_error.take()
                {
                    view.append(&error);
                }
                let settled = value["type"] == "agent_settled";
                let success = value["success"] == true;
                if view.tree_navigation.is_some() {
                    match value["type"].as_str() {
                        Some("text_delta" | "reasoning_delta") => continue,
                        Some("error") => {
                            view.branch_error = value["message"].as_str().map(str::to_owned)
                        }
                        _ => {}
                    }
                }
                if value["type"] == "agent_started" {
                    active.busy = true;
                }
                if value["type"] == "bash_execution_end" {
                    active.bashing = false;
                    if !active.busy {
                        view.request_started = None;
                    }
                }
                let resources_changed = value["type"] == "resources_discovered";
                view.receive_worker(value, &mut args);
                if resources_changed && let Some(name) = args.use_theme.as_deref() {
                    match Theme::load(
                        name,
                        &args.theme_paths,
                        &args.workspace,
                        args.trusted,
                        false,
                    ) {
                        Ok(next) => {
                            theme = next;
                            theme_watcher = ThemeWatcher::new(&theme);
                            deferred_theme_error = None;
                        }
                        Err(error) => {
                            if deferred_theme_error.take().is_some() {
                                view.append(&error.to_string());
                            }
                        }
                    }
                }
                if settled {
                    active.busy = false;
                    view.dialogs.clear();
                    view.settle();
                    if let Some((id, _)) = view.tree_navigation.take() {
                        let result = if success {
                            restore_tree_position(&mut args, &mut view, id)
                        } else {
                            reopen_tree(&mut args, &mut view, id).map(|()| {
                                if let Some(picker) = &mut view.picker {
                                    picker.show_feedback(Err(view
                                        .branch_error
                                        .take()
                                        .unwrap_or_else(|| {
                                            "Branch navigation did not complete".into()
                                        })));
                                }
                            })
                        };
                        if let Err(error) = result {
                            view.append(&error.to_string());
                        }
                    }
                }
                dirty = true;
            }
            if let Some(status) = active.child.try_wait()? {
                let mut finished = worker.take().expect("active worker");
                for reader in finished.readers.drain(..) {
                    let _ = reader.join();
                }
                for value in finished.drain_events() {
                    view.receive_worker(value, &mut args);
                }
                view.settle();
                view.cache_warming_status = None;
                view.dialogs.clear();
                if let Some((id, _)) = view.tree_navigation.take()
                    && let Err(error) = reopen_tree(&mut args, &mut view, id)
                {
                    view.append(&error.to_string());
                }
                if !status.success() {
                    view.append(&format!(
                        "Run stopped ({status}). Use /session to inspect its saved journal."
                    ));
                }
                view.request_started = None;
                view.phase = None;
                view.shell_running = false;
                dirty = true;
            }
        }
        if let Some(task) = &view.remote_task {
            match task.receiver.try_recv() {
                Ok(RemoteEvent::Notice(notice)) => {
                    view.append(&notice);
                    dirty = true;
                }
                Ok(RemoteEvent::OpenRouterCode(sender)) => {
                    view.local_editor = Some((
                        LocalEditor::OpenRouterCode(Some(sender)),
                        crate::ui::dialog::Dialog::secret("OpenRouter code or redirect URL".into()),
                    ));
                    dirty = true;
                }
                Ok(RemoteEvent::RefreshModelKeys) => {
                    match super::model_auth::Keys::load() {
                        Ok(keys) => args.model_keys = keys,
                        Err(error) => view.append(&error.to_string()),
                    }
                    dirty = true;
                }
                Ok(RemoteEvent::Finished(result)) => {
                    view.remote_task = None;
                    clear_openrouter_editor(&mut view);
                    match result {
                        Ok(lines) => {
                            for line in lines {
                                view.append(&line);
                            }
                        }
                        Err(error) => view.append(&error.to_string()),
                    }
                    dirty = true;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    view.remote_task = None;
                    clear_openrouter_editor(&mut view);
                    view.append("Operation stopped");
                    dirty = true;
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
            }
        }
        dirty |= view.poll_clipboard();
        if view
            .flash
            .as_ref()
            .is_some_and(|(_, at)| at.elapsed() >= Duration::from_secs(2))
        {
            view.flash = None;
            dirty = true;
        }
        if view
            .scrollbar_activity
            .is_some_and(|at| at.elapsed() >= Duration::from_secs(1))
        {
            view.scrollbar_activity = None;
            dirty = true;
        }
        if let Some(terminal) = &mut _terminal {
            terminal.update_progress(
                args.settings.terminal_progress()
                    && worker.as_ref().is_some_and(|worker| worker.busy),
            )?;
        }
        if !args.trust_pending
            && view.picker.is_none()
            && !is_busy(&worker)
            && let Some((prompt, images)) = initial.pop_front()
        {
            match start_prompt(&args, &prompt, &images, &mut worker) {
                Ok(()) => {
                    let display = if prompt.starts_with("/skill:") {
                        view.resources.expand(&prompt)
                    } else {
                        None
                    };
                    view.lines
                        .push(Entry::user(display.unwrap_or_else(|| prompt.clone())));
                    view.history.push(prompt);
                }
                Err(error) => view.append(&error.to_string()),
            }
            dirty = true;
        }
        if (dirty && last_frame.elapsed() >= frame_interval)
            || ((is_busy(&worker) || view.remote_task.is_some())
                && last_frame.elapsed() >= Duration::from_millis(100))
        {
            view.draw(&args, &theme, is_busy(&worker), &keys)?;
            dirty = false;
            last_frame = Instant::now();
        }
        #[cfg(any(unix, windows))]
        if let Some(terminal) = &mut _terminal {
            terminal.poll_color_timeout()?;
        }
        let input_wait = if dirty {
            frame_interval.saturating_sub(last_frame.elapsed())
        } else {
            Duration::from_millis(40)
        };
        let event = match input.next(input_wait)? {
            Some(crate::ui::input::InputEvent::Event(event)) => event,
            #[cfg(unix)]
            Some(crate::ui::input::InputEvent::KeyboardEnhancementFlags(flags)) => {
                if let Some(terminal) = &mut _terminal {
                    terminal.keyboard_reply(!flags.is_empty())?;
                }
                continue;
            }
            #[cfg(any(unix, windows))]
            Some(crate::ui::input::InputEvent::PrimaryDeviceAttributes) => {
                if let Some(terminal) = &mut _terminal {
                    terminal.receive_color_marker()?;
                    #[cfg(unix)]
                    terminal.keyboard_reply(false)?;
                }
                continue;
            }
            #[cfg(any(unix, windows))]
            Some(crate::ui::input::InputEvent::CellSize { width, height }) => {
                if images::set_cell_dimensions(width, height) {
                    dirty = true;
                }
                continue;
            }
            #[cfg(any(unix, windows))]
            Some(crate::ui::input::InputEvent::TerminalColorInvalid { target }) => {
                if let Some(terminal) = &mut _terminal {
                    terminal.accepts_color_reply(target)?;
                }
                continue;
            }
            #[cfg(any(unix, windows))]
            Some(crate::ui::input::InputEvent::TerminalPalette {
                index,
                red,
                green,
                blue,
            }) => {
                let accepted = match &mut _terminal {
                    Some(terminal) => terminal.accepts_color_reply(index)?,
                    None => false,
                };
                if accepted
                    && crate::ui::theme::set_terminal_palette(index, red, green, blue)
                    && theme.name == "system"
                {
                    theme = Theme::system();
                    dirty = true;
                }
                continue;
            }
            #[cfg(any(unix, windows))]
            Some(
                event @ (crate::ui::input::InputEvent::Appearance { .. }
                | crate::ui::input::InputEvent::TerminalColor { .. }),
            ) => {
                let changed = match event {
                    crate::ui::input::InputEvent::Appearance { dark } => {
                        crate::ui::theme::reset_terminal_background();
                        if let Some(terminal) = &mut _terminal {
                            terminal.query_terminal_colors()?;
                        }
                        if theme.name == "system" {
                            theme = Theme::system();
                            dirty = true;
                        }
                        crate::ui::theme::set_appearance(dark)
                    }
                    crate::ui::input::InputEvent::TerminalColor {
                        foreground,
                        red,
                        green,
                        blue,
                    } => {
                        let accepted = match &mut _terminal {
                            Some(terminal) => {
                                terminal.accepts_color_reply(if foreground { 16 } else { 17 })?
                            }
                            None => false,
                        };
                        accepted
                            && crate::ui::theme::set_terminal_color(foreground, red, green, blue)
                    }
                    _ => unreachable!(),
                };
                if changed {
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
                view.mouse.clear();
                view.scrollbar_dragging = false;
                if view.graphics.enabled() {
                    images::query_cell_size()?;
                }
                if args.tui_mode == Some(TuiMode::Regular) {
                    view.finish_regular()?;
                }
                dirty = true;
            }
            Event::Mouse(event) if args.tui_mode != Some(TuiMode::Regular) => {
                if view.fullscreen_mouse(event, &args) {
                    last_idle_interrupt = None;
                    dirty = true;
                }
            }
            Event::FocusLost if args.tui_mode != Some(TuiMode::Regular) => {
                view.panel_press = None;
                view.mouse.focus_lost();
                view.scrollbar_dragging = false;
                dirty = true;
            }
            Event::Paste(text) => {
                if view.remote_task.is_some() && view.local_editor.is_none() {
                    continue;
                }
                last_idle_interrupt = None;
                if let Some((_, dialog)) = view.local_editor.as_mut() {
                    dialog.paste(&text);
                } else if let Some(dialog) = view.dialogs.front_mut() {
                    dialog.paste(&text);
                } else if let Some(search) = &mut view.search {
                    search.paste(&text);
                } else if let Some(picker) = &mut view.picker {
                    picker.paste(&text);
                } else {
                    view.editor.insert(&text);
                }
                dirty = true;
            }
            Event::Key(key) if key.kind != KeyEventKind::Release => {
                dirty = true;
                if let Some(task) = &view.remote_task {
                    if keys.action(key, true) == Some("app.interrupt") {
                        let message = task.cancelled;
                        view.remote_task = None;
                        clear_openrouter_editor(&mut view);
                        view.append(message);
                        continue;
                    }
                    if view.local_editor.is_none() {
                        continue;
                    }
                }
                if let Some((mut editor, mut dialog)) = view.local_editor.take() {
                    match dialog.input(key, &keys) {
                        Some(pi_rs::UiResponse::Text(text)) => {
                            let result = match &mut editor {
                                LocalEditor::Setting(name) => serde_json::from_str(&text)
                                    .map_err(Error::from)
                                    .and_then(|value| {
                                        select(
                                            Selection::Setting(name.clone(), value),
                                            &mut args,
                                            &mut view,
                                            &mut theme,
                                            &mut worker,
                                        )
                                    })
                                    .map(|()| view.append("Setting saved")),
                                LocalEditor::BranchSummary(id) => start_tree_navigation(
                                    &args,
                                    &mut view,
                                    &mut worker,
                                    *id,
                                    true,
                                    Some(text),
                                ),
                                LocalEditor::OpenRouterCode(sender) => {
                                    if let Some(code) = super::openrouter_auth::manual_code(&text) {
                                        if let Some(sender) = sender.take() {
                                            let _ = sender.send(code);
                                        }
                                        Ok(())
                                    } else {
                                        Err(Error::Config("Enter an OpenRouter authorization code or redirect URL".into()))
                                    }
                                }
                                LocalEditor::ProviderKey(provider) => super::model_auth::save(
                                    provider,
                                    args.model_endpoint_for(
                                        provider,
                                        (provider == &args.provider).then_some(args.model.as_str()),
                                    ),
                                    &text,
                                )
                                .and_then(|()| super::model_auth::Keys::load())
                                .map(|keys| {
                                    args.model_keys = keys;
                                    view.append(&format!("API key saved for {}", provider.id()));
                                }),
                            };
                            if let Err(error) = result {
                                dialog.set_error(error.to_string());
                                view.local_editor = Some((editor, dialog));
                            } else if matches!(editor, LocalEditor::Setting(_)) {
                                let items = settings_items(&args, &view, worker.as_ref());
                                if let Some(picker) = &mut view.picker {
                                    picker.refresh_settings(items);
                                }
                            }
                        }
                        Some(_) => {
                            if let LocalEditor::BranchSummary(id) = editor {
                                confirm_tree(&mut view, id);
                            }
                        }
                        None => view.local_editor = Some((editor, dialog)),
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
                        Input::Cancel => {
                            if let Some(id) = view.tree_confirmation.take()
                                && let Err(error) = reopen_tree(&mut args, &mut view, id)
                            {
                                view.append(&error.to_string());
                            }
                        }
                        Input::CopyTreeRecord(id) => {
                            picker.show_feedback(
                                Session::open(&args.session)
                                    .and_then(|session| sessions::tree_record_text(&session, id))
                                    .and_then(|text| sessions::copy_text(&text))
                                    .map(|()| "Copied entry".into())
                                    .map_err(|error| error.to_string()),
                            );
                            view.picker = Some(picker);
                        }
                        Input::UpdateModelScope { save } => {
                            if let Some((catalog, enabled)) = picker.model_scope() {
                                let result = update_model_scope(
                                    &mut args, &mut view, catalog, enabled, save,
                                );
                                if save {
                                    picker.show_feedback(
                                        result
                                            .map(|()| "Model selection saved to settings".into())
                                            .map_err(|error| error.to_string()),
                                    );
                                }
                            }
                            view.picker = Some(picker);
                        }
                        Input::Save(selection) => {
                            let result = match selection {
                                Selection::Model(model) => select_model_input(
                                    &mut args, &mut view, &model,
                                )
                                .and_then(|()| {
                                    let configured =
                                        format!("{}/{}", args.provider.id(), args.model);
                                    args.settings.set_user("defaultModel", json!(configured))?;
                                    view.append(&format!("Default model: {configured}"));
                                    Ok(())
                                }),
                                Selection::Thinking(thinking) => {
                                    super::thinking::set(&mut args, &thinking).and_then(|()| {
                                        args.settings
                                            .set_user("defaultThinkingLevel", json!(thinking))?;
                                        view.append(&format!("Default thinking level: {thinking}"));
                                        Ok(())
                                    })
                                }
                                _ => Ok(()),
                            };
                            if let Err(error) = result {
                                picker.show_feedback(Err(error.to_string()));
                                view.picker = Some(picker);
                            }
                        }
                        Input::Select(selection) => {
                            view.tree_confirmation = None;
                            let settings = picker.title == "Settings";
                            let result =
                                select(selection, &mut args, &mut view, &mut theme, &mut worker);
                            if settings {
                                picker.refresh_settings(settings_items(
                                    &args,
                                    &view,
                                    worker.as_ref(),
                                ));
                                if let Err(error) = result {
                                    picker.show_feedback(Err(error.to_string()));
                                }
                                view.picker = Some(picker);
                            } else if let Err(error) = result {
                                view.append(&error.to_string());
                            }
                        }
                    }
                    continue;
                }
                if args.tui_mode != Some(TuiMode::Regular) {
                    if keys.matches("tui.altScreen.search", key) {
                        last_idle_interrupt = None;
                        view.mouse.clear();
                        view.search = view.search.is_none().then(Search::default);
                        continue;
                    }
                    if let Some(search) = &mut view.search {
                        if keys.matches("tui.altScreen.searchClose", key) {
                            view.search = None;
                            continue;
                        }
                        if keys.matches("tui.altScreen.searchNext", key) {
                            search.navigate(true);
                            continue;
                        }
                        if keys.matches("tui.altScreen.searchPrevious", key) {
                            search.navigate(false);
                            continue;
                        }
                    }
                    if let Some(action) = keys.viewport_action(key) {
                        view.viewport.navigate(action);
                        view.scrollbar_activity = Some(Instant::now());
                        continue;
                    }
                    if let Some(search) = &mut view.search {
                        search.input(key, &keys);
                        continue;
                    }
                }
                if keys.matches("tui.input.copy", key) && view.copy_selection() {
                    continue;
                }
                view.mouse.clear();
                let action = keys
                    .action(
                        key,
                        view.editor.text.is_empty() && view.draft_images.is_empty(),
                    )
                    .unwrap_or("");
                if action != "app.interrupt" || is_busy(&worker) {
                    last_idle_interrupt = None;
                }
                if view.editor.jump_key(action, key) || view.editor.action(action) {
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
                    "app.interrupt" if worker.as_ref().is_some_and(|active| active.busy) => {
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
                    "app.interrupt" if worker.as_ref().is_some_and(|active| active.bashing) => {
                        if let Some(active) = &mut worker {
                            let _ = active.send(json!({"type":"abort_bash"}));
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
                                if let Err(error) =
                                    slash(command, &mut args, &mut view, Some(&mut worker))
                                {
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
                        if let Err(error) = slash(command, &mut args, &mut view, Some(&mut worker))
                        {
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
                        match super::thinking::cycle(&mut args) {
                            Ok(Some(level)) => view.append(&format!("Thinking level: {level}")),
                            Ok(None) => view.append("Current model does not support thinking"),
                            Err(error) => view.append(&error.to_string()),
                        }
                    }
                    "app.tools.expand" => view.set_expand_tools(!view.expand_tools),
                    "app.thinking.toggle" => view.hide_thinking = !view.hide_thinking,
                    "app.clipboard.pasteImage" => view.paste_clipboard(&args),
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
                        drop(_terminal.take());
                        let result = external_editor(&args, &mut view.editor);
                        _terminal = Some(Terminal::enter(args.tui_mode, view.graphics.cleanup())?);
                        if view.graphics.enabled() {
                            images::query_cell_size()?;
                        }
                        view.graphics.reset();
                        if let Err(error) = result {
                            view.append(&error.to_string());
                        }
                        view.regular.redraw = true;
                    }
                    "tui.input.tab" => complete_input(&args, &mut view),
                    "app.model.select" if !is_busy(&worker) => {
                        start_model_picker(&args, &mut view, ModelListIntent::Select)
                    }
                    "tui.input.submit" if view.editor.continued_line() => {}
                    "tui.input.submit" | "app.message.followUp" => {
                        if view.tree_navigation.is_some() {
                            continue;
                        }
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
                        view.viewport.follow();
                        let auth_command =
                            matches!(input.split_whitespace().next(), Some("/login" | "/logout"));
                        if !auth_command {
                            view.history.push(input.clone());
                        }
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
                            view.picker = Some(Picker::new(
                                "Settings",
                                settings_items(&args, &view, worker.as_ref()),
                            ));
                        } else if input.starts_with('!') {
                            let excluded = input.starts_with("!!");
                            let command = input[if excluded { 2 } else { 1 }..].trim();
                            if !command.is_empty()
                                && let Err(error) = Worker::start_bash(
                                    &mut worker,
                                    &args,
                                    json!({"type":"bash","command":command,"exclude_from_context":excluded}),
                                )
                            {
                                view.append(&error.to_string());
                                view.editor.replace(input);
                            }
                        } else if input.trim() == "/session" && view.draft_images.is_empty() {
                            if let Err(error) =
                                slash(&input, &mut args, &mut view, Some(&mut worker))
                            {
                                view.append(&error.to_string());
                            }
                        } else if is_busy(&worker) && auth_command {
                            view.append("Wait for the current operation before changing provider authentication");
                        } else if worker.as_ref().is_some_and(|active| active.busy) {
                            let kind = if action == "app.message.followUp" {
                                "followup"
                            } else {
                                "steer"
                            };
                            match Worker::control(
                                &mut worker,
                                &args,
                                json!({"type":kind,"content":input,"images":view.draft_images}),
                            ) {
                                Ok(()) => view.draft_images.clear(),
                                Err(error) => view.append(&error.to_string()),
                            }
                        } else if input.starts_with('/') && is_busy(&worker) {
                            view.append("Wait for the shell command to finish");
                            view.editor.replace(input);
                        } else if input.trim() == "/reload" {
                            view.cache_warming_status = None;
                            view.dialogs.clear();
                            view.ui_status.clear();
                            view.widgets.clear();
                            args.settings =
                                crate::config::Settings::load(&args.workspace, args.trusted)?;
                            args.model_keys = super::model_auth::Keys::load()?;
                            args.reload_model_profiles()?;
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
                            args.session_transition = Some(pi_rs::SessionStartReason::Reload);
                        } else if input.starts_with('/') {
                            match slash(&input, &mut args, &mut view, Some(&mut worker)) {
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
                            view.lines.push(Entry::user(input.clone()));
                            match start_prompt(&args, &input, &view.draft_images, &mut worker) {
                                Ok(()) => view.draft_images.clear(),
                                Err(error) => view.append(&error.to_string()),
                            }
                        }
                    }
                    "tui.editor.pageUp" | "tui.editor.pageDown" => {
                        let step = view.editor_page as isize;
                        let delta = if action == "tui.editor.pageUp" {
                            -step
                        } else {
                            step
                        };
                        view.editor.visual_move(view.editor_width, delta);
                    }
                    "tui.editor.cursorUp" if view.editor.visual_move(view.editor_width, -1) => {}
                    "tui.editor.cursorDown" if view.editor.visual_move(view.editor_width, 1) => {}
                    "tui.editor.cursorUp"
                        if view.history_index.is_none()
                            && view.editor.cursor != view.editor.line_start() =>
                    {
                        view.editor.action("tui.editor.cursorLineStart");
                    }
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
                    "tui.editor.cursorDown" => {
                        view.editor.action("tui.editor.cursorLineEnd");
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
    drop(_terminal.take());
    if args.tui_mode != Some(TuiMode::Regular)
        && args.settings.string("fullscreenExitOutput") != Some("resume-hint")
    {
        view.replay_transcript(&args, &theme)?;
    }
    if !args.no_session && args.session.is_file() {
        let path = args.session.to_string_lossy();
        let path = if cfg!(windows) {
            format!("'{}'", path.replace('\'', "''"))
        } else {
            shell_words::quote(&path).into_owned()
        };
        println!("Resume this session with pi --session {path}");
    }
    Ok(())
}

fn show_startup_header(args: &Cli) -> bool {
    args.verbose || args.settings.boolean("quietStartup") != Some(true)
}

fn is_busy(worker: &Option<Worker>) -> bool {
    worker
        .as_ref()
        .is_some_and(|worker| worker.busy || worker.bashing)
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

fn edit_provider_key(provider: super::Provider, view: &mut View) {
    let dialog = crate::ui::dialog::Dialog::secret(format!("{} API key", provider.id()));
    view.local_editor = Some((LocalEditor::ProviderKey(provider), dialog));
}

fn remove_provider_key(provider: super::Provider, args: &mut Cli, view: &mut View) -> Result<()> {
    let removed = super::model_auth::remove(&provider)?;
    args.model_keys = super::model_auth::Keys::load()?;
    view.append(&format!(
        "{} for {}",
        if removed {
            "Removed saved API key"
        } else {
            "No saved API key"
        },
        provider.id()
    ));
    Ok(())
}

fn select_model_input(args: &mut Cli, view: &mut View, input: &str) -> Result<()> {
    let provider = super::model_selection::qualified_model(input, &args.model_profiles)
        .map_or_else(|| args.provider.clone(), |(provider, _)| provider);
    let available = view
        .available_models
        .iter()
        .filter_map(|model| super::model_selection::qualified_model(model, &args.model_profiles))
        .filter(|(candidate, _)| *candidate == provider)
        .map(|(_, model)| model.to_owned())
        .collect::<Vec<_>>();
    let selected = super::model_selection::split_with_profiles(
        input,
        &available,
        &provider,
        &args.model_profiles,
    )?;
    args.change_model(provider, selected.model, selected.thinking)
}

fn cycle_model(args: &mut Cli, view: &mut View, reverse: bool) -> Result<()> {
    use super::model_selection::{ScopedChoice, next_model, qualified_model};
    let discovered;
    let choices = if args.model_scope.is_empty() {
        if view.available_models.is_empty() {
            start_model_picker(args, view, ModelListIntent::Cycle { reverse });
            return Ok(());
        }
        discovered = view
            .available_models
            .iter()
            .filter_map(|model| qualified_model(model, &args.model_profiles))
            .map(|(provider, model)| ScopedChoice {
                provider,
                model: model.to_owned(),
                thinking: None,
            })
            .collect::<Vec<_>>();
        discovered.as_slice()
    } else {
        args.model_scope.as_slice()
    };
    if let Some(choice) = next_model(args, choices, reverse) {
        args.change_model(choice.provider, choice.model, choice.thinking)?;
    } else if args.model_scope.is_empty() {
        view.append("Only one model available");
    } else {
        view.append("Only one model in scope");
    }
    Ok(())
}

fn show_radius_methods(view: &mut View) {
    view.picker = Some(Picker::new(
        "Sign in to Radius",
        vec![
            Item {
                label: "Sign in with browser".into(),
                selection: Selection::LoginRadius(radius_auth::Method::Browser),
            },
            Item {
                label: "Sign in with device code".into(),
                selection: Selection::LoginRadius(radius_auth::Method::Device),
            },
        ],
    ));
}

fn start_openrouter_login(view: &mut View) {
    view.remote_task = Some(RemoteTask::start(
        "Signing in to OpenRouter",
        "OpenRouter login cancelled",
        |sender| async move {
            super::openrouter_auth::login(|url, manual| {
                let _ = sender.send(RemoteEvent::Notice(format!(
                    "Continue in your browser: {url}"
                )));
                let _ = sender.send(RemoteEvent::OpenRouterCode(manual));
            })
            .await?;
            let _ = sender.send(RemoteEvent::RefreshModelKeys);
            Ok(vec!["Signed in to OpenRouter".into()])
        },
    ));
}

fn clear_openrouter_editor(view: &mut View) {
    if matches!(view.local_editor, Some((LocalEditor::OpenRouterCode(_), _))) {
        view.local_editor = None;
    }
}

fn start_radius_login(view: &mut View, method: radius_auth::Method) -> Result<()> {
    let gateway = radius_auth::Gateway::configured()?;
    view.remote_task = Some(RemoteTask::start(
        "Signing in to Radius",
        "Radius login cancelled",
        move |sender| async move {
            gateway
                .login(method, |notice| {
                    let _ = sender.send(RemoteEvent::Notice(notice.to_string()));
                })
                .await?;
            Ok(vec!["Signed in to Radius".into()])
        },
    ));
    Ok(())
}

fn start_radius_logout(view: &mut View) -> Result<()> {
    let gateway = radius_auth::Gateway::configured()?;
    view.remote_task = Some(RemoteTask::start(
        "Signing out of Radius",
        "Radius logout cancelled",
        move |_| async move {
            let message = if gateway.logout().await? {
                "Signed out of Radius"
            } else {
                "No saved Radius credentials"
            };
            Ok(vec![message.into()])
        },
    ));
    Ok(())
}

fn start_model_picker(args: &Cli, view: &mut View, intent: ModelListIntent) {
    if view.model_list.is_some() {
        return;
    }
    let args = args.clone();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let catalogs = runtime.block_on(super::model_selection::catalogs(&args, false))?;
            Ok(catalogs
                .into_iter()
                .flat_map(|catalog| {
                    catalog
                        .models
                        .into_iter()
                        .map(move |model| format!("{}/{model}", catalog.provider.id()))
                })
                .collect())
        })();
        let _ = sender.send(result);
    });
    view.model_list = Some(ModelDiscovery { receiver, intent });
    view.append("Loading available models…");
}

fn update_model_scope(
    args: &mut Cli,
    view: &mut View,
    catalog: &[String],
    enabled: &[String],
    save: bool,
) -> Result<()> {
    use super::model_selection::{ScopedChoice, qualified_model};
    let unrestricted = enabled.is_empty()
        || (enabled == catalog
            && args
                .model_scope
                .iter()
                .all(|choice| choice.thinking.is_none()));
    let scope: Vec<_> = if unrestricted {
        Vec::new()
    } else {
        let previous: std::collections::HashMap<_, _> = args
            .model_scope
            .iter()
            .map(|choice| (choice.qualified(), choice))
            .collect();
        enabled
            .iter()
            .filter_map(|key| {
                let (provider, model) = qualified_model(key, &args.model_profiles)?;
                Some(previous.get(key).map_or_else(
                    || ScopedChoice {
                        provider,
                        model: model.into(),
                        thinking: None,
                    },
                    |choice| (*choice).clone(),
                ))
            })
            .collect()
    };
    if save {
        let patterns: Vec<_> = scope.iter().map(ScopedChoice::pattern).collect();
        args.settings.set_user("enabledModels", json!(patterns))?;
    }
    args.model_scope = scope;
    view.available_models = catalog.to_vec();
    Ok(())
}

fn reopen_tree(args: &mut Cli, view: &mut View, id: u64) -> Result<()> {
    slash("/tree", args, view, None)?;
    if let Some(picker) = &mut view.picker {
        picker.focus_record(id);
    }
    Ok(())
}

fn confirm_tree(view: &mut View, id: u64) {
    view.tree_confirmation = Some(id);
    view.picker = Some(Picker::new(
        "Summarize branch?",
        [
            ("No summary", Selection::CheckoutWithoutSummary(id)),
            ("Summarize", Selection::CheckoutWithSummary(id)),
            (
                "Summarize with custom prompt",
                Selection::EditBranchSummary(id),
            ),
        ]
        .into_iter()
        .map(|(label, selection)| Item {
            label: label.into(),
            selection,
        })
        .collect(),
    ));
}

fn restore_tree_position(args: &mut Cli, view: &mut View, id: u64) -> Result<()> {
    let input = Session::open(&args.session)?
        .records()
        .get(id.saturating_sub(1) as usize)
        .and_then(|record| match &record.event {
            pi_rs::Event::User { content, .. } | pi_rs::Event::UserWithImages { content, .. } => {
                Some(content.clone())
            }
            _ => None,
        });
    restore_workspace(args)?;
    view.restore(args)?;
    if let Some(input) = input {
        view.editor.replace(input);
    }
    Ok(())
}

fn start_tree_navigation(
    args: &Cli,
    view: &mut View,
    worker: &mut Option<Worker>,
    id: u64,
    summarize: bool,
    instructions: Option<String>,
) -> Result<()> {
    let configured_reserve = &args.settings.value()["branchSummary"]["reserveTokens"];
    let reserve_tokens = configured_reserve
        .as_u64()
        .or_else(|| configured_reserve.as_f64().map(|tokens| tokens as u64))
        .unwrap_or(16_384);
    start_request(
        args,
        json!({
            "type":"branch_summary", "selected":id, "instructions":instructions, "reserve_tokens":reserve_tokens, "summarize":summarize
        }),
        worker,
    )?;
    view.branch_error = None;
    view.tree_navigation = Some((id, summarize));
    Ok(())
}

fn settings_items(args: &Cli, view: &View, worker: Option<&Worker>) -> Vec<Item> {
    let cache_mode = args.settings.string("cacheWarming").unwrap_or("streaming");
    let next_cache_mode = match cache_mode {
        "streaming" => "idle",
        "idle" => "off",
        _ => "streaming",
    };
    let queue_setting = |key: &str, label: &str, kind: pi_rs::InputKind| {
        let current = worker
            .and_then(|worker| worker.queue_mode(args, kind))
            .or_else(|| super::process::session_queue_mode(args, kind).ok())
            .unwrap_or_default();
        let mode = if current == pi_rs::QueueMode::All {
            "all"
        } else {
            "one-at-a-time"
        };
        Item {
            label: format!("{label} · {mode}"),
            selection: Selection::Setting(
                key.into(),
                json!(if mode == "all" {
                    "one-at-a-time"
                } else {
                    "all"
                }),
            ),
        }
    };
    let mut items = vec![
        Item {
            label: format!(
                "HTTP idle timeout · {}",
                match args.settings.http_idle_timeout_ms() {
                    0 => "disabled".to_owned(),
                    ms => format!("{} sec", ms as f64 / 1000.0),
                }
            ),
            selection: Selection::Setting(
                "httpIdleTimeoutMs".into(),
                json!(match args.settings.http_idle_timeout_ms() {
                    0 => 30_000,
                    1..=29_999 => 30_000,
                    30_000..=59_999 => 60_000,
                    60_000..=119_999 => 120_000,
                    120_000..=299_999 => 300_000,
                    _ => 0,
                }),
            ),
        },
        queue_setting("steeringMode", "Steering mode", pi_rs::InputKind::Steer),
        queue_setting("followUpMode", "Follow-up mode", pi_rs::InputKind::FollowUp),
        Item {
            label: format!(
                "Tool output · {} (toggle)",
                if view.expand_tools {
                    "expanded"
                } else {
                    "collapsed"
                }
            ),
            selection: Selection::Setting("expandTools".into(), json!(!view.expand_tools)),
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
            selection: Selection::Setting("hideThinkingBlock".into(), json!(!view.hide_thinking)),
        },
        Item {
            label: format!(
                "Cache miss notices · {} (toggle)",
                if args.settings.boolean("showCacheMissNotices") == Some(true) {
                    "on"
                } else {
                    "off"
                }
            ),
            selection: Selection::Setting(
                "showCacheMissNotices".into(),
                json!(args.settings.boolean("showCacheMissNotices") != Some(true)),
            ),
        },
        Item {
            label: format!("Save default model · {}", args.model),
            selection: Selection::Setting(
                "defaultModel".into(),
                json!(args.configured_model_name(&args.model)),
            ),
        },
        Item {
            label: format!("Cache warming · {cache_mode} (cycle)"),
            selection: Selection::Setting("cacheWarming".into(), json!(next_cache_mode)),
        },
        Item {
            label: format!(
                "Save default thinking · {}",
                args.thinking.as_deref().unwrap_or("model default")
            ),
            selection: Selection::Setting("defaultThinkingLevel".into(), json!(args.thinking)),
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
    let resize_images = args.settings.resize_model_images();
    items.push(Item {
        label: format!(
            "Auto-resize images · {} (toggle)",
            if resize_images { "on" } else { "off" }
        ),
        selection: Selection::Setting("images.autoResize".into(), json!(!resize_images)),
    });
    let block_images = args.settings.block_model_images();
    items.push(Item {
        label: format!(
            "Block images · {} (toggle)",
            if block_images { "on" } else { "off" }
        ),
        selection: Selection::Setting("images.blockImages".into(), json!(!block_images)),
    });
    if view.graphics.enabled() {
        let show_images = args.settings.show_terminal_images();
        let image_width = args.settings.terminal_image_width();
        let next_width = match image_width {
            60 => 80,
            80 => 120,
            _ => 60,
        };
        items.extend([
            Item {
                label: format!(
                    "Show images · {} (toggle)",
                    if show_images { "on" } else { "off" }
                ),
                selection: Selection::Setting("terminal.showImages".into(), json!(!show_images)),
            },
            Item {
                label: format!("Image width · {image_width} cells"),
                selection: Selection::Setting("terminal.imageWidthCells".into(), json!(next_width)),
            },
        ]);
    }
    let autocomplete_items = args.settings.autocomplete_max_visible();
    let skill_commands = args.settings.enable_skill_commands();
    items.extend([
        Item {
            label: format!("Autocomplete max items · {autocomplete_items}"),
            selection: Selection::Setting(
                "autocompleteMaxVisible".into(),
                json!(
                    [3, 5, 7, 10, 15, 20]
                        .into_iter()
                        .find(|count| *count > autocomplete_items)
                        .unwrap_or(3)
                ),
            ),
        },
        Item {
            label: format!(
                "Skill commands · {} (toggle)",
                if skill_commands { "on" } else { "off" }
            ),
            selection: Selection::Setting("enableSkillCommands".into(), json!(!skill_commands)),
        },
    ]);
    let escape = args.settings.string("doubleEscapeAction").unwrap_or("tree");
    let next_escape = match escape {
        "tree" => "fork",
        "fork" => "none",
        _ => "tree",
    };
    let tree_filter =
        TreeFilter::parse(args.settings.string("treeFilterMode").unwrap_or("default"));
    let padding = args.settings.value()["editorPaddingX"]
        .as_u64()
        .unwrap_or(0);
    let exit_output = args
        .settings
        .string("fullscreenExitOutput")
        .unwrap_or("transcript");
    let scrollbar = args
        .settings
        .string("fullscreenScrollbar")
        .unwrap_or("auto");
    let copy_on_select = args
        .settings
        .boolean("fullscreenCopyOnSelect")
        .unwrap_or(true);
    items.extend([
        Item {
            label: format!(
                "Terminal progress · {} (toggle)",
                if args.settings.terminal_progress() {
                    "on"
                } else {
                    "off"
                },
            ),
            selection: Selection::Setting(
                "terminal.showTerminalProgress".into(),
                json!(!args.settings.terminal_progress()),
            ),
        },
        Item {
            label: format!("Fullscreen exit output · {exit_output}"),
            selection: Selection::Setting(
                "fullscreenExitOutput".into(),
                json!(if exit_output == "transcript" {
                    "resume-hint"
                } else {
                    "transcript"
                }),
            ),
        },
        Item {
            label: format!("Fullscreen scrollbar · {scrollbar}"),
            selection: Selection::Setting(
                "fullscreenScrollbar".into(),
                json!(match scrollbar {
                    "auto" => "always",
                    "always" => "hidden",
                    _ => "auto",
                }),
            ),
        },
        Item {
            label: format!(
                "Fullscreen copy on select · {}",
                if copy_on_select { "on" } else { "off" }
            ),
            selection: Selection::Setting("fullscreenCopyOnSelect".into(), json!(!copy_on_select)),
        },
        Item {
            label: format!("Editor padding · {padding}"),
            selection: Selection::Setting("editorPaddingX".into(), json!((padding + 1) % 4)),
        },
        Item {
            label: format!("Mermaid rendering · {}", args.settings.mermaid_mode()),
            selection: Selection::Setting(
                "markdown.mermaid".into(),
                json!(match args.settings.mermaid_mode() {
                    "streaming" => "final",
                    "final" => "off",
                    _ => "streaming",
                }),
            ),
        },
        Item {
            label: format!(
                "Clear on shrink · {}",
                if args.settings.clear_terminal_on_shrink() {
                    "on"
                } else {
                    "off"
                }
            ),
            selection: Selection::Setting(
                "terminal.clearOnShrink".into(),
                json!(!args.settings.clear_terminal_on_shrink()),
            ),
        },
        Item {
            label: format!(
                "Hardware cursor · {}",
                if args.settings.show_hardware_cursor() {
                    "on"
                } else {
                    "off"
                }
            ),
            selection: Selection::Setting(
                "showHardwareCursor".into(),
                json!(!args.settings.show_hardware_cursor()),
            ),
        },
        Item {
            label: format!("Output padding · {}", args.settings.output_padding()),
            selection: Selection::Setting(
                "outputPad".into(),
                json!(1 - args.settings.output_padding()),
            ),
        },
        Item {
            label: format!("Double-escape action · {escape}"),
            selection: Selection::Setting("doubleEscapeAction".into(), json!(next_escape)),
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
        "tuiMode",
        "fullscreenWheelScrollLines",
        "sessionDir",
        "sessionShareDestination",
        "shellPath",
        "shellCommandPrefix",
        "defaultProjectTrust",
        "defaultTools",
        "extensions",
        "skills",
        "prompts",
        "themes",
        "packages",
        "codemode",
        "compaction",
        "branchSummary",
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
    items
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
            let name = pi_rs::sanitize_session_name(&name);
            let changed = session.name().unwrap_or_default() != name;
            if changed {
                session.set_name(&name)?;
            }
            if path.canonicalize().ok() == args.session.canonicalize().ok() {
                view.session_name = session.name().map(str::to_owned);
                if changed {
                    view.pending_session_name = Some(view.session_name.clone());
                }
            }
            drop(session);
            slash("/resume", args, view, None)?;
        }
        Selection::DeleteSession(path) => {
            if path.canonicalize().ok() == args.session.canonicalize().ok() {
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
            slash("/resume", args, view, None)?;
        }
        Selection::Resume(path) => {
            slash(
                &format!("/resume {}", path.display()),
                args,
                view,
                Some(worker),
            )?;
        }
        Selection::Label(id, label) => {
            Session::open(&args.session)?.set_label(id, (!label.is_empty()).then_some(label))?;
            slash("/tree", args, view, None)?;
            if let Some(picker) = &mut view.picker {
                picker.focus_record(id);
            }
        }
        Selection::Checkout(id) => {
            if id == Session::open(&args.session)?.head() {
                view.append("Already at this point");
            } else if args.settings.value()["branchSummary"]["skipPrompt"] == true {
                start_tree_navigation(args, view, worker, id, false, None)?;
            } else {
                confirm_tree(view, id);
            }
        }
        Selection::CheckoutWithoutSummary(id) => {
            start_tree_navigation(args, view, worker, id, false, None)?
        }
        Selection::CheckoutWithSummary(id) => {
            start_tree_navigation(args, view, worker, id, true, None)?
        }
        Selection::EditBranchSummary(id) => {
            let dialog = crate::ui::dialog::Dialog::new(
                uuid::Uuid::new_v4(),
                pi_rs::UiRequest::Editor {
                    title: "Custom summarization instructions".into(),
                    initial_value: None,
                    timeout: None,
                },
            );
            view.local_editor = Some((LocalEditor::BranchSummary(id), dialog));
        }
        Selection::Fork(id) => {
            slash(&format!("/fork {id}"), args, view, Some(worker))?;
        }
        Selection::Model(model) => {
            select_model_input(args, view, &model)?;
        }
        Selection::Completion { range, text } => insert_completion(view, range, &text),
        Selection::LoginProvider(provider) => edit_provider_key(provider, view),
        Selection::LoginOpenRouter => start_openrouter_login(view),
        Selection::LogoutProvider(provider) => remove_provider_key(provider, args, view)?,
        Selection::RadiusMethods => show_radius_methods(view),
        Selection::LoginRadius(method) => start_radius_login(view, method)?,
        Selection::LogoutRadius => start_radius_logout(view)?,
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
                "fullscreenWheelScrollLines" => json!("auto"),
                "shellPath" | "shellCommandPrefix" => json!(""),
                "defaultProjectTrust" => json!("ask"),
                "defaultTools" => json!(["read", "bash", "edit", "write"]),
                "modelThinkingLevels" => json!({}),
                "steeringMode" | "followUpMode" => json!("one-at-a-time"),
                "codemode" => json!({"mode":"on", "inlineBudget":3000}),
                "retry" => serde_json::to_value(args.settings.retry_settings())?,
                "branchSummary" => json!({"reserveTokens":16384,"skipPrompt":false}),
                "compaction" => {
                    let tokens = args
                        .settings
                        .compaction_tokens(args.provider.id(), &args.model);
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
            view.local_editor = Some((LocalEditor::Setting(key), dialog));
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
            } else if key == "steeringMode" || key == "followUpMode" {
                let mode: pi_rs::QueueMode = serde_json::from_value(value.clone())?;
                let kind = if key == "steeringMode" {
                    pi_rs::InputKind::Steer
                } else {
                    pi_rs::InputKind::FollowUp
                };
                Worker::queue_mode_control(
                    worker,
                    args,
                    json!({"type":"set_queue_mode", "kind":kind, "mode":mode}),
                )?;
                args.settings.set_user(&key, value.clone())?;
                if let Some(active) = worker {
                    if kind == pi_rs::InputKind::Steer {
                        active.steering_mode = mode;
                    } else {
                        active.follow_up_mode = mode;
                    }
                }
            } else if let Some(member) = key.strip_prefix("terminal.") {
                args.settings
                    .set_user_member("terminal", member, value.clone())?;
            } else if let Some(member) = key.strip_prefix("images.") {
                args.settings
                    .set_user_member("images", member, value.clone())?;
            } else if let Some(member) = key.strip_prefix("markdown.") {
                args.settings
                    .set_user_member("markdown", member, value.clone())?;
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
            if key == "cacheWarming"
                && let Some(worker) = worker
            {
                worker.send(json!({"type":"cache_warming_changed"}))?;
            }
            match key.as_str() {
                "expandTools" => view.set_expand_tools(value.as_bool().unwrap_or(false)),
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
    next.session_transition = Some(pi_rs::SessionStartReason::Resume);
    restore_workspace(&mut next)?;
    let resources = super::resources::Resources::load(&next)?;
    next.theme_paths = resources.themes.clone();
    view.restore(&next)?;
    view.resources = resources;
    *args = next;
    if args.trust_pending {
        slash("/trust", args, view, None)?;
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
        args.restore_model_selection(model, thinking, session.model_provider());
    }
    super::thinking::adjust(args);
    args.record_model_selection(&mut session)?;
    Ok(())
}
pub(super) fn restore_workspace(args: &mut Cli) -> Result<()> {
    let session = Session::open(&args.session)?;
    if let Some((model, thinking)) = session.model_selection() {
        args.restore_model_selection(model, thinking, session.model_provider());
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
    if !std::process::Command::new(program)
        .args(arguments)
        .arg(file.path())
        .status()?
        .success()
    {
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
        .char_indices()
        .rev()
        .find(|(_, character)| character.is_whitespace())
        .map_or(0, |(index, character)| index + character.len_utf8());
    let word = &prefix[start..];
    let mut candidates = if start == 0 && word.starts_with('/') {
        [
            "/help",
            "/hotkeys",
            "/new",
            "/resume",
            "/session",
            "/model",
            "/scoped-models",
            "/login",
            "/logout",
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
        let ends_in_separator = file_word.ends_with(std::path::is_separator);
        let directory = if ends_in_separator {
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
                if !ends_in_separator && !file_name.starts_with(name) {
                    return None;
                }
                let parent = if ends_in_separator { path } else { parent };
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
                .filter(|name| name.starts_with(word)),
        );
        if args.settings.enable_skill_commands() {
            candidates.extend(
                view.resources
                    .skills
                    .keys()
                    .map(|name| format!("/skill:{name}"))
                    .filter(|name| name.starts_with(word)),
            );
        }
    }
    candidates.sort();
    candidates.dedup();
    let range = start..view.editor.cursor;
    match candidates.as_slice() {
        [candidate] => insert_completion(view, range, candidate),
        [] => {}
        _ => {
            view.picker = Some(Picker::completion(
                candidates,
                range,
                args.settings.autocomplete_max_visible(),
            ))
        }
    }
}

fn insert_completion(view: &mut View, range: std::ops::Range<usize>, completion: &str) {
    let mut text = view.editor.text.clone();
    let cursor = range.start + completion.len();
    text.replace_range(range, completion);
    view.editor.replace(text);
    view.editor.cursor = cursor;
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
    fn streams_tool_arguments_without_committing_preview() {
        let mut view = super::View::default();
        view.receive(&serde_json::json!({"type":"tool_call_delta","toolCallDelta":{"index":0,"name":"read","arguments":"{\"path\":\"src/li"}}));
        let entries: Vec<_> = view.streaming_tool_entries().collect();
        assert!(
            matches!(&entries[..], [super::Entry::Tool { name, arguments, finished: false, .. }] if name == "read" && serde_json::from_str::<serde_json::Value>(arguments).unwrap()["path"] == "src/li")
        );
        assert!(view.lines.is_empty());
        view.receive(&serde_json::json!({"type":"record","record":{"event":{"type":"assistant","content":"","invocations":[{"operation_id":"op","call":{"name":"read","arguments":"{\"path\":\"src/lib.rs\"}"}}]}}}));
        assert!(view.streaming_tools.is_empty());
        assert!(
            matches!(&view.lines[..], [super::Entry::Tool { id, name, .. }] if id == "op" && name == "read")
        );
        view.receive(&serde_json::json!({"type":"tool_call_delta","toolCallDelta":{"index":0,"name":"read","arguments":"{\"path\":\"x"}}));
        view.settle();
        assert!(view.streaming_tools.is_empty());
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn complete_color_replies_release_queued_query_before_marker() {
        let mut terminal = std::mem::ManuallyDrop::new(super::Terminal {
            alternate: false,
            images: Default::default(),
            progress: None,
            color_queries: std::collections::VecDeque::from([super::ColorQuery {
                started: std::time::Instant::now(),
                replied: ((1 << 18) - 1) & !1,
            }]),
            color_requery: true,
            expired_color_markers: 0,
            #[cfg(unix)]
            keyboard: super::KeyboardProtocol::Kitty,
        });
        assert!(!terminal.accepts_color_reply(0).unwrap());
        assert_eq!(terminal.color_queries.len(), 1);
        assert_eq!(terminal.color_queries[0].replied, 0);
        assert_eq!(terminal.expired_color_markers, 1);
        terminal.receive_color_marker().unwrap();
        assert!(terminal.accepts_color_reply(0).unwrap());
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn late_color_marker_does_not_complete_the_new_query() {
        let mut terminal = std::mem::ManuallyDrop::new(super::Terminal {
            alternate: false,
            images: Default::default(),
            progress: None,
            color_queries: std::collections::VecDeque::from([super::ColorQuery {
                started: std::time::Instant::now(),
                replied: 0,
            }]),
            color_requery: false,
            expired_color_markers: 1,
            #[cfg(unix)]
            keyboard: super::KeyboardProtocol::Kitty,
        });
        terminal.receive_color_marker().unwrap();
        assert_eq!(terminal.color_queries.len(), 1);
        assert!(terminal.accepts_color_reply(5).unwrap());
        terminal.receive_color_marker().unwrap();
        assert!(terminal.color_queries.is_empty());
    }

    #[test]
    fn new_session_creates_its_storage_directory() {
        use clap::Parser;

        let directory = tempfile::tempdir().unwrap();
        let mut args = super::Cli::parse_from(["pi"]);
        args.workspace = directory.path().to_owned();
        let sessions = directory.path().join("sessions");
        args.session_dir = Some(sessions.clone());
        let mut view = super::View::default();

        super::slash("/new", &mut args, &mut view, None).unwrap();

        assert_eq!(args.session.parent(), Some(sessions.as_path()));
        assert!(args.session.is_file());
        assert!(matches!(
            args.session_transition,
            Some(pi_rs::SessionStartReason::New)
        ));
    }

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
            view.receive(&json!({"type":"model_progress","stage":stage}));
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
