//! Terminal presentation over the same journaled CLI used by automation.
use std::collections::VecDeque;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::ui::{
    editor::Editor,
    images::{self, Cleanup, InlineImage, Renderer},
    keys::Keys,
    mermaid::Mermaid,
    mouse_selection::MouseSelection,
    picker::{
        Input, Item, MouseInput, Picker, Selection, ThemeSettingsAction, TreeEntryKind, TreeFilter,
    },
    render::{Entry, Line, SummaryKind, ToolPart, TranscriptOptions},
    theme::{Theme, ThemeWatcher},
    transcript_search::Search,
    viewport::{Viewport, Wheel},
};
use crossterm::cursor::{Hide, MoveDown, MoveTo, MoveToColumn, MoveUp, Show};
use crossterm::event::{
    DisableBracketedPaste, DisableFocusChange, DisableMouseCapture, EnableBracketedPaste,
    EnableFocusChange, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
    MouseButton, MouseEvent, MouseEventKind,
};
use crossterm::style::{Print, ResetColor};
use crossterm::terminal::{
    self, BeginSynchronizedUpdate, Clear, ClearType, EndSynchronizedUpdate, EnterAlternateScreen,
    LeaveAlternateScreen,
};
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
    Disabled,
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

    fn switch_mode(&mut self, alternate: bool, view: &mut View) -> Result<()> {
        if self.alternate == alternate {
            return Ok(());
        }
        if alternate {
            view.finish_regular()?;
            execute!(
                std::io::stdout(),
                EnterAlternateScreen,
                EnableMouseCapture,
                EnableFocusChange
            )?;
        } else {
            execute!(
                std::io::stdout(),
                DisableFocusChange,
                DisableMouseCapture,
                LeaveAlternateScreen
            )?;
            view.regular.layout = None;
            view.regular.max_rows = 0;
            view.regular.committed_rows = 0;
        }
        self.alternate = alternate;
        view.graphics.reset();
        view.mouse.clear();
        view.scrollbar_dragging = false;
        Ok(())
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
    fn disable_keyboard_protocol(&mut self) {
        if self.keyboard == KeyboardProtocol::Disabled {
            return;
        }
        if self.keyboard == KeyboardProtocol::Xterm {
            let _ = execute!(std::io::stdout(), Print("\x1b[>4;0m"));
        }
        let _ = execute!(std::io::stdout(), Print("\x1b[<u"));
        self.keyboard = KeyboardProtocol::Disabled;
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
        self.disable_keyboard_protocol();
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

// On Windows, Crossterm may execute commands through the console API instead of
// writing escape sequences. Keep the writer live there so its flush before a
// console API call also flushes all preceding text.
struct FrameWriter<W: Write> {
    output: W,
    buffer: Option<Vec<u8>>,
    armed: bool,
}

fn synchronized_updates_supported() -> bool {
    #[cfg(windows)]
    {
        crossterm::ansi_support::supports_ansi()
    }
    #[cfg(not(windows))]
    {
        true
    }
}

impl<W: Write> FrameWriter<W> {
    fn new(output: W, buffered: bool, synchronized: bool) -> std::io::Result<Self> {
        let mut frame = Self {
            output,
            buffer: buffered.then(Vec::new),
            armed: synchronized,
        };
        if synchronized {
            queue!(frame, BeginSynchronizedUpdate)?;
        }
        Ok(frame)
    }

    fn finish(mut self) -> std::io::Result<()> {
        if self.armed {
            queue!(self, EndSynchronizedUpdate)?;
        }
        if let Some(buffer) = self.buffer.take() {
            self.output.write_all(&buffer)?;
        }
        self.output.flush()?;
        self.armed = false;
        Ok(())
    }
}

impl<W: Write> Write for FrameWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        match &mut self.buffer {
            Some(buffer) => buffer.write(bytes),
            None => self.output.write(bytes),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.buffer {
            Some(buffer) => buffer.flush(),
            None => self.output.flush(),
        }
    }
}

impl<W: Write> Drop for FrameWriter<W> {
    fn drop(&mut self) {
        if self.armed {
            // A partial write may already have enabled synchronized updates.
            // Preserve the original error and make a best-effort recovery.
            let _ = queue!(self.output, EndSynchronizedUpdate);
            let _ = self.output.flush();
        }
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
    BedrockProfile,
    BedrockChain,
    OpenRouterCode(Option<tokio::sync::oneshot::Sender<String>>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ThemeSettingsStage {
    Single,
    Automatic,
    Light,
    Dark,
}

struct ThemeSettings {
    parent: Picker,
    original: Theme,
    single: String,
    light: String,
    dark: String,
    stage: ThemeSettingsStage,
}

#[derive(Clone, Copy)]
enum ModelListIntent {
    Select,
    Cycle { reverse: bool },
    Scope,
    ThinkingSettings,
}

struct ModelDiscovery {
    receiver: std::sync::mpsc::Receiver<Result<Vec<String>>>,
    intent: ModelListIntent,
}

struct ThinkingSettings {
    parent: Picker,
    models: Vec<String>,
    // The open picker reflects live choices even when project settings win on reload.
    overrides: serde_json::Map<String, Value>,
    model_picker: Option<Picker>,
    selected: Option<String>,
    levels: Option<std::sync::mpsc::Receiver<Vec<String>>>,
    available_levels: Vec<String>,
}

struct PendingThinking {
    session: PathBuf,
    provider: Option<String>,
    model: String,
    level: String,
    previous_level: String,
}

enum RemoteEvent {
    Notice(String),
    DeviceCode { url: String, code: String },
    OpenRouterCode(tokio::sync::oneshot::Sender<String>),
    ForgetModelKey(&'static str),
    RefreshModelKeys,
    Finished(Result<Vec<String>>),
}

struct AuthReload {
    revision: String,
    receiver: std::sync::mpsc::Receiver<Result<super::model_auth::Keys>>,
    _cancel: tokio::sync::oneshot::Sender<()>,
}
impl AuthReload {
    fn start(revision: String) -> Self {
        Self::start_path(revision, crate::config::agent_dir().join("auth.json"))
    }

    fn start_path(revision: String, path: PathBuf) -> Self {
        let (sender, receiver) = std::sync::mpsc::channel();
        let (cancel, cancelled) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let result = (|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                runtime.block_on(async {
                    tokio::select! {
                        biased;
                        _ = cancelled => Ok(None),
                        result = super::model_auth::Keys::load_path_async(&path) => result.map(Some),
                    }
                })
            })();
            match result {
                Ok(Some(keys)) => {
                    let _ = sender.send(Ok(keys));
                }
                Err(error) => {
                    let _ = sender.send(Err(error));
                }
                Ok(None) => (),
            }
        });
        Self {
            revision,
            receiver,
            _cancel: cancel,
        }
    }
}

static EXTERNAL_EDITOR_ACTIVE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(test)]
static TERMINAL_SIGNAL_DELIVERIES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

fn install_terminal_signal(notify: std::sync::Arc<tokio::sync::Notify>) -> Result<()> {
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("pi-terminal-signal".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = sender.send(Err(error));
                    return;
                }
            };
            runtime.block_on(async move {
                #[cfg(unix)]
                let signals =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt());
                #[cfg(windows)]
                let signals = tokio::signal::windows::ctrl_c();
                #[cfg(any(unix, windows))]
                {
                    let mut signals = match signals {
                        Ok(signals) => signals,
                        Err(error) => {
                            let _ = sender.send(Err(error));
                            return;
                        }
                    };
                    let _ = sender.send(Ok(()));
                    while signals.recv().await.is_some() {
                        if !EXTERNAL_EDITOR_ACTIVE.load(std::sync::atomic::Ordering::SeqCst) {
                            super::process::TERMINAL_INTERRUPTED
                                .store(true, std::sync::atomic::Ordering::SeqCst);
                            notify.notify_one();
                        }
                        #[cfg(test)]
                        TERMINAL_SIGNAL_DELIVERIES
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                }
                #[cfg(not(any(unix, windows)))]
                {
                    let _ = sender.send(Ok(()));
                    while tokio::signal::ctrl_c().await.is_ok() {
                        if !EXTERNAL_EDITOR_ACTIVE.load(std::sync::atomic::Ordering::SeqCst) {
                            super::process::TERMINAL_INTERRUPTED
                                .store(true, std::sync::atomic::Ordering::SeqCst);
                            notify.notify_one();
                        }
                    }
                }
            });
        })?;
    receiver
        .recv()
        .map_err(|_| Error::Config("terminal signal listener stopped".into()))??;
    super::process::TERMINAL_SIGNAL_ENABLED.store(true, std::sync::atomic::Ordering::SeqCst);
    Ok(())
}

struct RemoteTask {
    status: &'static str,
    cancelled: &'static str,
    started: Instant,
    receiver: std::sync::mpsc::Receiver<RemoteEvent>,
    device_code: Option<(String, String)>,
    device_copy: Option<std::sync::mpsc::Receiver<bool>>,
    device_cancel: pi_rs::CancellationToken,
    _cancel: tokio::sync::oneshot::Sender<()>,
    logout_control: Option<super::chatgpt_auth::LogoutControl>,
    meta_login_control: Option<super::meta_auth::LoginControl>,
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
            device_code: None,
            device_copy: None,
            device_cancel: pi_rs::CancellationToken::new(),
            _cancel: cancel,
            logout_control: None,
            meta_login_control: None,
        }
    }

    fn cancel_before_commit(&self) -> bool {
        self.logout_control
            .as_ref()
            .is_none_or(|control| control.cancel_before_commit())
            && self
                .meta_login_control
                .as_ref()
                .is_none_or(|control| control.cancel_before_commit())
    }
}

impl Drop for RemoteTask {
    fn drop(&mut self) {
        self.device_cancel.cancel();
    }
}

async fn run_device_side_effect<T>(
    cancel: pi_rs::CancellationToken,
    action: impl std::future::Future<Output = T>,
) -> Option<T> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => None,
        result = action => Some(result),
    }
}

struct PendingShare {
    id: uuid::Uuid,
    destination: sessions::ShareDestination,
    path: PathBuf,
    workspace: PathBuf,
    started: Instant,
}

fn start_share_upload(
    view: &mut View,
    pending: PendingShare,
    snapshot: Option<sessions::ShareSnapshot>,
) {
    let PendingShare {
        destination,
        path,
        workspace,
        ..
    } = pending;
    let status = destination.status();
    view.remote_task = Some(RemoteTask::start(
        status,
        "Share cancelled",
        move |_| async move {
            let session = Session::snapshot(path)?;
            let shared = destination.share(session, &workspace, snapshot).await?;
            let mut lines = vec![format!("Share URL: {}", shared.url)];
            if let Some(gist) = shared.gist {
                lines.push(format!("Gist: {gist}"));
            }
            Ok(lines)
        },
    ));
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

type LinkRow = (u16, Vec<(std::ops::Range<usize>, String)>);

#[derive(Default)]
struct View {
    async_transitions: bool,
    pending_transition: Option<std::sync::mpsc::Receiver<Result<Cli>>>,
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
    theme_settings: Option<ThemeSettings>,
    thinking_settings: Option<ThinkingSettings>,
    pending_thinking: Option<PendingThinking>,
    session_name: Option<String>,
    resources: super::resources::Resources,
    model_list: Option<ModelDiscovery>,
    remote_task: Option<RemoteTask>,
    pending_share: Option<PendingShare>,
    available_models: Vec<String>,
    pending_request: Option<Value>,
    pending_session_name: Option<Option<String>>,
    thinking: String,
    expand_tools: bool,
    expand_key: Option<crossterm::event::KeyEvent>,
    panel_rows: Vec<(std::ops::Range<usize>, TranscriptPanel)>,
    panel_press: Option<(TranscriptPanel, u16, u16)>,
    link_rows: Vec<LinkRow>,
    link_press: Option<(String, u16, u16)>,
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
    fn invalidate_model_catalog(&mut self) {
        self.available_models.clear();
        self.model_list = None;
        if self.thinking_settings.take().is_some()
            || self.picker.as_ref().is_some_and(|picker| {
                matches!(
                    picker.title.as_str(),
                    "Select model"
                        | "Model Configuration"
                        | "Loading thinking models"
                        | "Thinking: select model"
                )
            })
        {
            self.picker = None;
        }
    }
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

    fn share_blocks_input(&self) -> bool {
        self.pending_share.is_some() && self.dialogs.is_empty() && self.local_editor.is_none()
    }

    fn take_share_response(
        &mut self,
        value: &Value,
    ) -> Option<(PendingShare, Option<sessions::ShareSnapshot>)> {
        if value["type"] != "response"
            || value["command"] != "get_share_context"
            || !self
                .pending_share
                .as_ref()
                .is_some_and(|pending| value["id"] == json!(pending.id))
        {
            return None;
        }
        let snapshot = (value["success"] == true)
            .then(|| serde_json::from_value(value["data"].clone()).ok())
            .flatten();
        Some((self.pending_share.take().expect("matching share"), snapshot))
    }

    fn dialog(&self) -> Option<&crate::ui::dialog::Dialog> {
        self.local_editor
            .as_ref()
            .map(|(_, dialog)| dialog)
            .or_else(|| self.dialogs.front())
    }
    fn dialog_mut(&mut self) -> Option<&mut crate::ui::dialog::Dialog> {
        if let Some((_, dialog)) = &mut self.local_editor {
            Some(dialog)
        } else {
            self.dialogs.front_mut()
        }
    }
    fn cancel_extension_dialog(&mut self, id: &Value) {
        self.dialogs.retain(|dialog| json!(dialog.id) != *id);
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
        } else if self.pending_share.is_some() {
            "Preparing share · Esc cancels · ".into()
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
    fn show_custom_message(&mut self, message: &pi_rs::CustomMessage) {
        if !message.display {
            return;
        }
        let mut text = String::new();
        let mut images = Vec::new();
        match &message.content {
            pi_rs::ContextContent::Text(value) => text.push_str(value),
            pi_rs::ContextContent::Blocks(blocks) => {
                for block in blocks {
                    match block {
                        pi_rs::ContentBlock::Text { text: value, .. } => text.push_str(value),
                        pi_rs::ContentBlock::Image { data, mime_type } => {
                            images.push(pi_rs::ImageContent {
                                data: data.clone(),
                                mime_type: mime_type.clone(),
                            })
                        }
                        _ => {}
                    }
                }
            }
        }
        self.append(&format!("{}: {}", message.custom_type, text));
        if !images.is_empty() {
            self.add_images(&images);
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
            pi_rs::Event::CompactionSummaryMessage {
                summary,
                tokens_before,
            } => (
                SummaryKind::Compaction {
                    tokens_before: *tokens_before,
                },
                summary,
            ),
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
                        | "custom_message"
                )
            )
            && let Ok(record) = serde_json::from_value::<pi_rs::Record>(value["record"].take())
        {
            if let pi_rs::Event::CustomMessage { message }
            | pi_rs::Event::CustomMessageDelivered { message, .. } = &record.event
            {
                self.show_custom_message(message);
            }
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
        self.link_rows.clear();
        self.link_press = None;
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
                pi_rs::Event::CustomMessage { message }
                | pi_rs::Event::CustomMessageDelivered { message, .. } => {
                    self.show_custom_message(message)
                }
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
                pi_rs::Outcome::Structured { data, .. }
                | pi_rs::Outcome::StructuredWithDetails { data, .. } => data["content"]
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
                pi_rs::Outcome::SucceededWithDetails { details, .. }
                | pi_rs::Outcome::TextResult {
                    details: Some(details),
                    is_error: false,
                    ..
                } if name == "edit" => details["diff"].as_str().filter(|diff| !diff.is_empty()),
                _ => None,
            };
            *numbered_diff = diff.is_some();
            *output = diff.map_or_else(|| display_tool_result(outcome), str::to_owned);
            *failed = !matches!(
                outcome,
                pi_rs::Outcome::Succeeded(_)
                    | pi_rs::Outcome::SucceededWithDetails { .. }
                    | pi_rs::Outcome::TextResult {
                        is_error: false,
                        ..
                    }
                    | pi_rs::Outcome::TextResultWithStructuredContent {
                        is_error: false,
                        ..
                    }
            ) && !matches!(outcome, pi_rs::Outcome::Structured { data, .. } | pi_rs::Outcome::StructuredWithDetails { data, .. } if data["isError"] != true);
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

    fn keyboard_copies_selection(
        &self,
        editor_copy: bool,
        message_copy: bool,
        copy_on_select: bool,
    ) -> bool {
        (editor_copy || (message_copy && !copy_on_select)) && self.mouse.text().is_some()
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
                self.link_press = None;
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
                self.link_press = None;
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
                self.link_press = None;
                self.paste_clipboard(args);
            }
            MouseEventKind::Down(MouseButton::Left) if self.search.is_none() => {
                self.panel_press = event
                    .modifiers
                    .is_empty()
                    .then(|| self.panel_at(event.row))
                    .flatten()
                    .map(|id| (id, event.column, event.row));
                let single = self.mouse.press(event.column, event.row, now);
                if self.hyperlink_at(event.column, event.row).is_some() {
                    self.panel_press = None;
                }
                self.link_press = (single && event.modifiers.is_empty())
                    .then(|| self.hyperlink_at(event.column, event.row))
                    .flatten()
                    .map(|url| (url.to_owned(), event.column, event.row));
            }
            MouseEventKind::Drag(MouseButton::Left) if self.search.is_none() => {
                self.panel_press = None;
                self.link_press = None;
                self.mouse.drag(event.column, event.row);
            }
            MouseEventKind::Up(MouseButton::Left) if self.search.is_none() => {
                let default_expanded = self.expand_tools;
                if let Some(url) = self.released_link(event.column, event.row) {
                    self.panel_press = None;
                    self.mouse.clear_gesture();
                    tokio::spawn(async move {
                        let _ = pi_rs_workbench::oauth_callback::open_browser(&url).await;
                    });
                } else if let Some((panel, column, row)) = self.panel_press.take()
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

    fn hyperlink_at(&self, column: u16, row: u16) -> Option<&str> {
        self.link_rows
            .iter()
            .find(|(y, _)| *y == row)?
            .1
            .iter()
            .find(|(columns, _)| columns.contains(&usize::from(column)))
            .map(|(_, url)| url.as_str())
    }

    fn released_link(&mut self, column: u16, row: u16) -> Option<String> {
        let (url, pressed_column, pressed_row) = self.link_press.take()?;
        ((column, row) == (pressed_column, pressed_row)
            && self.hyperlink_at(column, row) == Some(url.as_str()))
        .then_some(url)
    }

    fn cache_links(&mut self, lines: &[Line], start_row: usize, width: usize) {
        self.link_rows.clear();
        for (row, line) in lines.iter().enumerate() {
            let mut col = 0;
            let mut links = Vec::new();
            for span in &line.spans {
                let mut end = col;
                for grapheme in span.text.graphemes(true) {
                    let cells = UnicodeWidthStr::width(grapheme);
                    if end + cells > width {
                        break;
                    }
                    end += cells;
                }
                if let Some(url) = &span.hyperlink
                    && !url.starts_with('-')
                    && col < end
                {
                    links.push((col..end, url.clone()));
                }
                col = end;
            }
            if !links.is_empty() {
                self.link_rows.push(((row + start_row) as u16, links));
            }
        }
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
        let _strike_budget = crate::ui::strike::frame_budget();
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
            lines = dialog.render(theme, width, body_height, keys);
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
        self.link_rows.clear();
        if modal {
            self.panel_press = None;
            self.link_press = None;
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
            self.cache_links(&lines[start..end], header_rows, body_width);
            if let Some(search) = &self.search {
                search.highlight(&mut lines, start..end, theme);
            } else {
                self.mouse.highlight(&mut lines, start..end);
            }
        }
        let stdout = std::io::stdout();
        let mut out = FrameWriter::new(
            stdout.lock(),
            !cfg!(windows),
            synchronized_updates_supported(),
        )?;
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
        out.finish()?;
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

    fn debug_log(&self, args: &Cli, theme: &Theme, width: usize, height: usize) -> Result<String> {
        let width = width.max(1);
        let options = self.transcript_options(args, false);
        let mut lines: Vec<Line> = self
            .lines
            .iter()
            .flat_map(|entry| {
                entry.render(
                    theme,
                    width,
                    self.expand_tools,
                    !self.hide_thinking,
                    options,
                )
            })
            .collect();
        if !self.thinking.is_empty() {
            lines.extend(Entry::Thinking(self.thinking.clone()).render(
                theme,
                width,
                false,
                !self.hide_thinking,
                self.transcript_options(args, true),
            ));
        }
        if !self.streaming.is_empty() {
            lines.extend(Entry::Assistant(self.streaming.clone()).render(
                theme,
                width,
                false,
                true,
                self.transcript_options(args, true),
            ));
        }
        for entry in self.streaming_tool_entries() {
            lines.extend(entry.render(
                theme,
                width,
                self.expand_tools,
                !self.hide_thinking,
                options,
            ));
        }
        lines.extend(self.widget_lines(theme, width, pi_rs::WidgetPlacement::AboveEditor));
        for row in wrap(&self.editor.text, width) {
            lines.push(Line::themed(&row, theme, "text"));
        }
        lines.extend(self.widget_lines(theme, width, pi_rs::WidgetPlacement::BelowEditor));
        lines.push(Line::themed(
            &args.workspace.display().to_string(),
            theme,
            "dim",
        ));
        lines.push(Line::themed(
            &format!(
                "{} · {}",
                args.configured_model_name(&args.model),
                args.thinking.as_deref().unwrap_or("default")
            ),
            theme,
            "dim",
        ));
        let session = Session::snapshot(&args.session)?;
        let (messages, _) = super::rpc_messages::snapshot(&session);
        let mut output = format!(
            "Debug output at {}\nTerminal: {}x{}\nTotal lines: {}\n\n=== All rendered lines with visible widths ===\n",
            chrono::Utc::now().to_rfc3339(),
            width,
            height,
            lines.len()
        );
        for (index, line) in lines.iter().enumerate() {
            let mut rendered = Vec::new();
            line.draw(&mut rendered, width)?;
            let rendered = String::from_utf8_lossy(&rendered);
            output.push_str(&format!(
                "[{index}] (w={width}) {}\n",
                serde_json::to_string(&*rendered)?
            ));
        }
        output.push_str("\n=== Agent messages (JSONL) ===\n");
        for message in messages {
            output.push_str(&serde_json::to_string(&message)?);
            output.push('\n');
        }
        Ok(output)
    }

    fn write_debug_log(&self, args: &Cli, theme: &Theme) -> Result<PathBuf> {
        let (width, height) = terminal::size()?;
        let content = self.debug_log(args, theme, usize::from(width), usize::from(height))?;
        let directory = crate::config::agent_dir();
        std::fs::create_dir_all(&directory)?;
        let path = directory.join("pi-debug.log");
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&path)?;
        file.write_all(content.as_bytes())?;
        Ok(path)
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
            active = dialog.render(theme, width, dialog_height, keys);
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
        let stdout = std::io::stdout();
        let mut out = FrameWriter::new(
            stdout.lock(),
            !cfg!(windows),
            synchronized_updates_supported(),
        )?;
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
        out.finish()?;
        Ok(())
    }
}

fn display_tool_result(outcome: &pi_rs::Outcome) -> String {
    if let pi_rs::Outcome::SucceededWithDetails { output, details }
    | pi_rs::Outcome::TextResult {
        output,
        details: Some(details),
        is_error: false,
    } = outcome
        && let Some(patch) = details["patch"].as_str().filter(|patch| !patch.is_empty())
    {
        return format!("{output}\n{patch}");
    }
    if let pi_rs::Outcome::Structured { data, .. }
    | pi_rs::Outcome::StructuredWithDetails { data, .. } = outcome
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

const HELP: &str = "/help  /new  /resume [path]  /session  /model <name>  /scoped-models  /compact  /tree  /checkout <id>  /quit\n/login [provider]  /logout [provider]\n/extensions  /extension <id> <command> [arguments]\n/context-edit <id> <text>  /context-delete <id>  /debug\n/notes [text]  /todo [JSON list]  /plan <text|accept|discard>  /review <task>\nShift+Enter inserts a newline. Ctrl+PageUp/PageDown moves through the editor. Up/Down recall prompts at the editor boundaries.\nFullscreen: PageUp/PageDown scroll. See /hotkeys for transcript search and navigation.\nEnter while working steers the next model response. Alt+Enter queues a follow-up.";

fn slash(
    input: &str,
    args: &mut Cli,
    view: &mut View,
    worker: Option<&mut Option<Worker>>,
) -> Result<Option<Worker>> {
    let (name, rest) = input.split_once(char::is_whitespace).unwrap_or((input, ""));
    let rest = rest.trim();
    match name {
        "/help" => view.append(HELP),
        "/hotkeys" => view.append(&Keys::load()?.describe()),
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
            args.restore_session_model(&Session::open(&args.session)?);
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
            let id = if destination.is_radius() && !args.trust_pending {
                worker.and_then(|worker| Worker::request_share_context(worker, args).ok())
            } else {
                None
            };
            let pending = PendingShare {
                id: id.unwrap_or_else(uuid::Uuid::nil),
                destination,
                path: args.session.clone(),
                workspace: args.workspace.clone(),
                started: Instant::now(),
            };
            if id.is_some() {
                view.pending_share = Some(pending);
            } else {
                start_share_upload(view, pending, None);
            }
        }
        "/model" if rest.is_empty() => {
            args.reload_model_profiles()?;
            start_model_picker(args, view, ModelListIntent::Select);
        }
        "/scoped-models" => {
            args.reload_model_profiles()?;
            start_model_picker(args, view, ModelListIntent::Scope);
        }
        "/login" if rest.eq_ignore_ascii_case("xai") => start_xai_login(view, &args.settings),
        "/login" if rest.eq_ignore_ascii_case("xai api-key") => {
            edit_provider_key(super::Provider::named("xai").expect("xAI provider"), view)
        }
        "/login" if rest.eq_ignore_ascii_case("meta") => start_meta_login(view, &args.settings),
        "/login" if rest.eq_ignore_ascii_case("meta api-key") => {
            edit_provider_key(super::Provider::named("meta").expect("Meta provider"), view)
        }
        "/login" if rest.eq_ignore_ascii_case("kimi-coding") => {
            start_kimi_login(view, &args.settings)
        }
        "/logout" if rest.eq_ignore_ascii_case("kimi-coding") => start_kimi_logout(view),
        "/login" if rest.eq_ignore_ascii_case("kimi-coding api-key") => edit_provider_key(
            super::Provider::named("kimi-coding").expect("Kimi provider"),
            view,
        ),
        "/login" if rest.eq_ignore_ascii_case("radius") => show_radius_methods(view),
        "/login" if rest.eq_ignore_ascii_case("radius browser") => {
            start_radius_login(view, &args.settings, radius_auth::Method::Browser)?;
        }
        "/login" if rest.eq_ignore_ascii_case("radius device-code") => {
            start_radius_login(view, &args.settings, radius_auth::Method::Device)?;
        }
        "/logout" if rest.eq_ignore_ascii_case("radius") => {
            start_radius_logout(view, &args.settings)?
        }
        "/login" if rest.eq_ignore_ascii_case("openai-chatgpt") => {
            start_chatgpt_login(view, &args.settings, false, None)
        }
        "/login" if rest.eq_ignore_ascii_case("openai-chatgpt new") => {
            start_chatgpt_login(view, &args.settings, true, None)
        }
        "/login" if rest.starts_with("openai-chatgpt account ") => start_chatgpt_login(
            view,
            &args.settings,
            false,
            Some(rest["openai-chatgpt account ".len()..].trim().into()),
        ),
        "/logout" if rest.eq_ignore_ascii_case("openai-chatgpt") => {
            start_chatgpt_logout(view, &args.settings)
        }
        "/login"
            if rest.eq_ignore_ascii_case("openrouter")
                || rest.eq_ignore_ascii_case("openrouter browser") =>
        {
            start_openrouter_login(view, &args.settings)
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
                    .filter(|provider| provider.id() != "openai-chatgpt")
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
                    || radius_auth::Gateway::configured_with_settings(&args.settings)
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
                            label: "xai · Subscription OAuth".into(),
                            selection: Selection::LoginXai,
                        },
                    );
                    items.insert(
                        0,
                        Item {
                            label: "meta · Subscription OAuth".into(),
                            selection: Selection::LoginMeta,
                        },
                    );
                    items.insert(
                        0,
                        Item {
                            label: "kimi-coding · Subscription OAuth".into(),
                            selection: Selection::LoginKimi,
                        },
                    );
                    items.insert(
                        0,
                        Item {
                            label: "openrouter · OAuth".into(),
                            selection: Selection::LoginOpenRouter,
                        },
                    );
                }
                if login
                    || args
                        .model_keys
                        .has(&super::Provider::named("openai-chatgpt").expect("ChatGPT provider"))
                {
                    items.insert(
                        0,
                        Item {
                            label: "openai-chatgpt · Continue with ChatGPT".into(),
                            selection: if login {
                                Selection::LoginChatGpt
                            } else {
                                Selection::LogoutChatGpt
                            },
                        },
                    );
                }
                if login {
                    items.push(Item {
                        label: "ChatGPT · Add account or workspace".into(),
                        selection: Selection::LoginChatGptNew,
                    });
                    for (id, label, active) in super::chatgpt_auth::list_accounts()? {
                        items.push(Item {
                            label: format!(
                                "ChatGPT · {label} · {id}{}",
                                if active { " (selected)" } else { "" }
                            ),
                            selection: Selection::LoginChatGptAccount(id),
                        });
                    }
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
                    if provider.id() == "amazon-bedrock" {
                        show_bedrock_methods(view);
                    } else {
                        edit_provider_key(provider, view);
                    }
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
            if view.async_transitions {
                let (sender, receiver) = std::sync::mpsc::channel();
                std::thread::spawn(move || {
                    let result = {
                        next.refresh_workspace_selection();
                        next.session = session_path(&next.workspace, next.session_dir.as_deref());
                        next.session_transition = Some(pi_rs::SessionStartReason::New);
                        Ok(next)
                    };
                    let _ = sender.send(result);
                });
                view.pending_transition = Some(receiver);
                view.append("Preparing session (Esc to cancel)");
                return Ok(None);
            }
            next.refresh_workspace_selection();
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
                        pi_rs::Event::CompactionSummaryMessage { summary, .. } => format!(
                            "Compaction: {}",
                            summary.chars().take(58).collect::<String>()
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
                        | pi_rs::Event::CompactionSummaryMessage { .. }
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
            args.restore_session_model(&session);
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

// Resolve each theme independently: a malformed resource should not hide a
// valid selected theme or prevent opening a session.
fn load_resolved_theme(
    name: &str,
    paths: &[PathBuf],
    workspace: &Path,
    trusted: bool,
) -> Result<Theme> {
    let error = match Theme::load(name, &[], workspace, trusted, false) {
        Ok(theme) => return Ok(theme),
        Err(error) => error,
    };
    for path in paths {
        if let Ok(theme) = Theme::load(name, std::slice::from_ref(path), workspace, trusted, false)
        {
            return Ok(theme);
        }
    }
    Err(error)
}

// Like Pi's startup UI, do not install missing packages or let an absent or
// malformed theme prevent session selection. Runtime preparation happens later.
fn startup_picker_theme(args: &Cli) -> Result<Theme> {
    let resources = super::resources::Resources::load_for_startup(args)?;
    Ok(load_resolved_theme(
        args.use_theme.as_deref().unwrap_or("system"),
        &resources.themes,
        &args.workspace,
        args.trusted,
    )
    .unwrap_or_else(|_| Theme::system()))
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
    let mut theme = startup_picker_theme(args)?;
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

pub(super) async fn run(
    mut args: Cli,
    follow_up_prompts: Vec<String>,
    mut ready: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<()> {
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
    args = prepare_initial_session_async(args).await?;
    super::packages::prepare(&args).await?;
    let initial_resources = super::resources::Resources::load(&args)?;
    args.theme_paths = initial_resources.themes.clone();
    let mut deferred_theme_error = None;
    let mut theme = match load_resolved_theme(
        args.use_theme.as_deref().unwrap_or("system"),
        &args.theme_paths,
        &args.workspace,
        args.trusted,
    ) {
        Ok(theme) => theme,
        Err(error) => {
            if !initial_resources.extensions.is_empty() {
                deferred_theme_error = Some(error.to_string());
            }
            Theme::system()
        }
    };
    let mut view = View {
        async_transitions: true,
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
    let terminal_signal = std::sync::Arc::new(tokio::sync::Notify::new());
    install_terminal_signal(terminal_signal.clone())?;
    super::startup_ready(&mut ready);
    if view.graphics.enabled() {
        crate::ui::images::query_cell_size()?;
    }
    let mut worker: Option<Worker> = None;
    let mut auth_reload: Option<AuthReload> = None;
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
    let mut last_clear: Option<Instant> = None;
    let mut theme_watcher = ThemeWatcher::new(&theme);
    let mut last_theme_check = Instant::now();
    let mut deferred_input = VecDeque::new();
    let mut interrupted = false;
    let mut external_editor: Option<RunningExternalEditor> = None;
    'interactive: loop {
        if super::process::TERMINAL_INTERRUPTED.load(std::sync::atomic::Ordering::SeqCst) {
            interrupted = true;
            break 'interactive;
        }
        if let Some(task) = &auth_reload {
            match task.receiver.try_recv() {
                Ok(Ok(keys)) => {
                    if args.model_keys.revision() == task.revision {
                        args.model_keys = keys;
                        view.invalidate_model_catalog();
                        view.append("Provider credentials reloaded");
                    } else {
                        view.append("Provider credentials changed during reload; keeping the newer selection");
                    }
                    auth_reload = None;
                    dirty = true;
                }
                Ok(Err(error)) => {
                    view.append(&format!("Auth reload: {error}"));
                    auth_reload = None;
                    dirty = true;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    view.append("Auth reload stopped");
                    auth_reload = None;
                    dirty = true;
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => (),
            }
        }
        if let Some(receiver) = &view.pending_transition {
            let ready = match receiver.try_recv() {
                Ok(result) => Some(result),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => Some(Err(Error::Config(
                    "session transition worker stopped".into(),
                ))),
                Err(std::sync::mpsc::TryRecvError::Empty) => None,
            };
            if let Some(result) = ready {
                if queued_transition_cancel(&mut deferred_input, |timeout| input.next(timeout))? {
                    view.pending_transition = None;
                    view.append("Session transition cancelled");
                    dirty = true;
                    continue;
                }
                view.pending_transition = None;
                match result.and_then(|next| apply_session(&mut args, &mut view, next)) {
                    Ok(()) => {}
                    Err(error) => view.append(&error.to_string()),
                }
                dirty = true;
            }
        }
        if !is_busy(&worker) {
            for event in std::mem::take(&mut args.selection_events) {
                if let Err(error) = Worker::notify_selection(&mut worker, &args, &event) {
                    view.append(&format!("Extension selection notification: {error}"));
                    dirty = true;
                }
            }
        }
        if let Some(terminal) = &mut _terminal {
            let alternate = args.tui_mode != Some(TuiMode::Regular);
            if terminal.alternate != alternate {
                terminal.switch_mode(alternate, &mut view)?;
                dirty = true;
            }
        }
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
        if !is_busy(&worker)
            && let Some(pending) = &view.pending_thinking
            && persist_pending_thinking(pending).is_ok()
        {
            if pending.session == args.session && pending.previous_level != pending.level {
                args.selection_events
                    .push(pi_rs::LifecycleEvent::ThinkingLevelSelect {
                        level: pending.level.clone(),
                        previous_level: pending.previous_level.clone(),
                    });
            }
            view.pending_thinking = None;
        }
        if (view.picker.is_none()
            || view
                .picker
                .as_ref()
                .is_some_and(|picker| picker.title == "Loading thinking models"))
            && view.dialog().is_none()
            && let Some(discovery) = &view.model_list
        {
            let intent = discovery.intent;
            match discovery.receiver.try_recv() {
                Ok(Ok(models)) => {
                    view.model_list = None;
                    if models.is_empty() {
                        if let Some(mut state) = view.thinking_settings.take() {
                            state
                                .parent
                                .show_feedback(Err("No models available".into()));
                            view.picker = Some(state.parent);
                        } else {
                            view.append("No models available");
                        }
                    } else {
                        match intent {
                            ModelListIntent::ThinkingSettings => {
                                let state =
                                    view.thinking_settings.as_mut().expect("thinking settings");
                                state.models = models;
                                let mut picker = Picker::new(
                                    "Thinking: select model",
                                    thinking_model_items(&state.models, &state.overrides),
                                );
                                picker.focus_thinking_model(&format!(
                                    "{}/{}",
                                    args.provider.id(),
                                    args.model
                                ));
                                view.picker = Some(picker);
                            }
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
                    if let Some(mut state) = view.thinking_settings.take() {
                        state.parent.show_feedback(Err(error.to_string()));
                        view.picker = Some(state.parent);
                    } else {
                        view.append(&error.to_string());
                    }
                    dirty = true;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    view.model_list = None;
                    if let Some(mut state) = view.thinking_settings.take() {
                        state
                            .parent
                            .show_feedback(Err("Model discovery stopped".into()));
                        view.picker = Some(state.parent);
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
            }
        }
        if view
            .picker
            .as_ref()
            .is_some_and(|picker| picker.title == "Loading thinking levels")
            && let Some(state) = &mut view.thinking_settings
            && let Some(receiver) = &state.levels
        {
            match receiver.try_recv() {
                Ok(mut levels) => {
                    if levels.is_empty() {
                        levels.push("off".into());
                    }
                    levels.dedup();
                    state.available_levels = levels;
                    state.levels = None;
                    view.picker = Some(thinking_level_picker(state));
                    dirty = true;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    state.levels = None;
                    let mut picker = state.model_picker.take().expect("model picker");
                    picker.show_feedback(Err("Thinking level discovery stopped".into()));
                    view.picker = Some(picker);
                    dirty = true;
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
                if value["type"] == "response" && value["command"] == "get_share_context" {
                    if let Some((pending, snapshot)) = view.take_share_response(&value) {
                        start_share_upload(&mut view, pending, snapshot);
                        dirty = true;
                    }
                    continue;
                }
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
                    if let Some(target) =
                        cancel_running_extension_editor(&mut external_editor, Some(&value))
                    {
                        finish_external_editor(&args, &mut view, &mut _terminal, target, None)?;
                    }
                    view.cancel_extension_dialog(&value["id"]);
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
                    match load_resolved_theme(
                        name,
                        &args.theme_paths,
                        &args.workspace,
                        args.trusted,
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
                    if value["type"] == "response" && value["command"] == "get_share_context" {
                        continue;
                    }
                    view.receive_worker(value, &mut args);
                }
                if let Some(pending) = view.pending_share.take() {
                    start_share_upload(&mut view, pending, None);
                }
                view.settle();
                view.cache_warming_status = None;
                if let Some(target) = cancel_running_extension_editor(&mut external_editor, None) {
                    finish_external_editor(&args, &mut view, &mut _terminal, target, None)?;
                }
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
        if view.dialogs.is_empty()
            && view
                .pending_share
                .as_ref()
                .is_some_and(|pending| pending.started.elapsed() >= Duration::from_secs(15))
        {
            let pending = view.pending_share.take().expect("timed out share");
            start_share_upload(&mut view, pending, None);
            dirty = true;
        }
        if let Some(task) = &view.remote_task {
            match task.receiver.try_recv() {
                Ok(RemoteEvent::Notice(notice)) => {
                    view.append(&notice);
                    dirty = true;
                }
                Ok(RemoteEvent::DeviceCode { url, code }) => {
                    view.append(&format!(
                        "Enter code {code} at {url} · Enter opens browser and copies code"
                    ));
                    if let Some(task) = &mut view.remote_task {
                        task.device_code = Some((url, code));
                    }
                    dirty = true;
                }
                Ok(RemoteEvent::OpenRouterCode(sender)) => {
                    view.local_editor = Some((
                        LocalEditor::OpenRouterCode(Some(sender)),
                        crate::ui::dialog::Dialog::secret("OpenRouter code or redirect URL".into()),
                    ));
                    dirty = true;
                }
                Ok(RemoteEvent::ForgetModelKey(provider)) => {
                    args.model_keys.forget(provider);
                    view.invalidate_model_catalog();
                    dirty = true;
                }
                Ok(RemoteEvent::RefreshModelKeys) => {
                    view.invalidate_model_catalog();
                    args.thinking_profile = None;
                    auth_reload = Some(AuthReload::start(args.model_keys.revision().to_owned()));
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
        if let Some(task) = &mut view.remote_task
            && let Some(receiver) = &task.device_copy
            && let Ok(copied) = receiver.try_recv()
        {
            task.device_copy = None;
            view.append(if copied {
                "Code copied to clipboard"
            } else {
                "Copy code manually"
            });
            dirty = true;
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
            && view.pending_transition.is_none()
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
        if let Some(editor) = &mut external_editor {
            match editor.poll() {
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                outcome => {
                    let editor = external_editor.take().expect("active editor");
                    let result = match outcome {
                        Ok(Some(status)) => editor.result(status),
                        Err(error) => Err(error),
                        Ok(None) => unreachable!(),
                    };
                    let target = editor.target;
                    drop(editor);
                    finish_external_editor(&args, &mut view, &mut _terminal, target, Some(result))?;
                    dirty = true;
                }
            }
        }
        if (dirty && last_frame.elapsed() >= frame_interval)
            || ((is_busy(&worker) || view.remote_task.is_some() || view.pending_share.is_some())
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
        let next_input = if let Some(event) = deferred_input.pop_front() {
            Some(event)
        } else {
            input.next(input_wait)?
        };
        let event = match next_input {
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
                        let changed = accepted
                            && crate::ui::theme::set_terminal_color(foreground, red, green, blue);
                        if accepted && !foreground && theme.name == "system" {
                            theme = Theme::system();
                            dirty = true;
                        }
                        changed
                    }
                    _ => unreachable!(),
                };
                if changed {
                    let selection = args.use_theme.as_deref().unwrap_or("system");
                    if let (Some(settings), Some(picker)) = (&view.theme_settings, &view.picker) {
                        if let Ok(next) = settings.preview(picker, &args) {
                            theme = next;
                        }
                    } else if selection.contains('/') {
                        match load_resolved_theme(
                            selection,
                            &args.theme_paths,
                            &args.workspace,
                            args.trusted,
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
        if view.pending_transition.is_some() {
            match &event {
                Event::Key(key) if key.code == KeyCode::Esc => {
                    view.pending_transition = None;
                    view.append("Session transition cancelled");
                    dirty = true;
                    continue;
                }
                Event::Key(_) | Event::Mouse(_) | Event::Paste(_) => continue,
                _ => {}
            }
        }
        let mut mouse_activate = false;
        let event = match event {
            Event::Mouse(mouse)
                if args.tui_mode != Some(TuiMode::Regular)
                    && view.dialog().is_none()
                    && view.picker.is_some() =>
            {
                let mut picker = view.picker.take().expect("picker checked above");
                let header_rows = usize::from(show_startup_header(&args));
                let action = if usize::from(mouse.column) < view.fullscreen_width
                    && let Some(row) = usize::from(mouse.row).checked_sub(header_rows)
                {
                    picker.mouse(mouse.kind, row)
                } else {
                    picker.clear_mouse_press();
                    MouseInput::Ignored
                };
                if matches!(action, MouseInput::Changed)
                    && let Some(settings) = &view.theme_settings
                {
                    match settings.preview(&picker, &args) {
                        Ok(next) => theme = next,
                        Err(error) => picker.show_feedback(Err(error.to_string())),
                    }
                }
                view.picker = Some(picker);
                match action {
                    MouseInput::Activate => {
                        mouse_activate = true;
                        Event::Key(KeyEvent::new(KeyCode::F(255), KeyModifiers::NONE))
                    }
                    MouseInput::Changed => {
                        last_idle_interrupt = None;
                        dirty = true;
                        continue;
                    }
                    MouseInput::Ignored => continue,
                }
            }
            event => event,
        };
        match event {
            Event::Resize(..) => {
                view.mouse.clear();
                if let Some(picker) = &mut view.picker {
                    picker.clear_mouse_press();
                }
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
                view.link_press = None;
                view.mouse.focus_lost();
                if let Some(picker) = &mut view.picker {
                    picker.clear_mouse_press();
                }
                view.scrollbar_dragging = false;
                dirty = true;
            }
            Event::Paste(text) => {
                if (view.remote_task.is_some()
                    && view.local_editor.is_none()
                    && view.dialogs.is_empty())
                    || view.share_blocks_input()
                {
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
                if !mouse_activate && let Some(picker) = &mut view.picker {
                    picker.clear_mouse_press();
                }
                if view.share_blocks_input() {
                    if keys.action(key, true) == Some("app.interrupt") {
                        view.pending_share = None;
                        view.append("Share cancelled");
                    }
                    continue;
                }
                if view.remote_task.is_some() && view.dialogs.is_empty() {
                    if keys.action(key, true) == Some("app.interrupt") {
                        interrupt_remote_task(&mut view);
                        continue;
                    }
                    if view.local_editor.is_none() {
                        if let Some((url, code)) = take_device_code_action(&mut view, key, &keys) {
                            view.append("Opening browser and copying code");
                            let (sender, receiver) = std::sync::mpsc::channel();
                            let cancel = if let Some(task) = &mut view.remote_task {
                                task.device_copy = Some(receiver);
                                task.device_cancel.clone()
                            } else {
                                continue;
                            };
                            let browser_cancel = cancel.clone();
                            tokio::spawn(async move {
                                let _ = run_device_side_effect(browser_cancel, async move {
                                    pi_rs_workbench::oauth_callback::open_browser(&url).await
                                })
                                .await;
                            });
                            tokio::spawn(async move {
                                if let Some(result) = run_device_side_effect(cancel, async move {
                                    sessions::copy_text_async(&code).await
                                })
                                .await
                                {
                                    let _ = sender.send(result.is_ok());
                                }
                            });
                        }
                        continue;
                    }
                }
                if view
                    .dialog()
                    .is_some_and(|dialog| dialog.external_editor_requested(key, &keys))
                {
                    external_editor = open_external_editor(&args, &mut view, &mut _terminal, true)?;
                    continue;
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
                                LocalEditor::BedrockChain => super::model_auth::save_bedrock_profile(None)
                                    .and_then(|()| super::model_auth::Keys::load())
                                    .map(|keys| { args.model_keys = keys; view.invalidate_model_catalog(); view.append("Using existing AWS credential chain for amazon-bedrock"); }),
                                LocalEditor::BedrockProfile => {
                                    super::model_auth::save_bedrock_profile(Some(text.trim()))
                                        .and_then(|()| super::model_auth::Keys::load())
                                        .map(|keys| {
                                            args.model_keys = keys;
                                            view.invalidate_model_catalog();
                                            view.append("AWS profile saved for amazon-bedrock");
                                        })
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
                    let input = if mouse_activate {
                        picker.activate_mouse_selection(&keys)
                    } else {
                        picker.input(key, &keys)
                    };
                    match input {
                        Input::Continue => {
                            if let Some(settings) = &view.theme_settings {
                                match settings.preview(&picker, &args) {
                                    Ok(next) => theme = next,
                                    Err(error) => picker.show_feedback(Err(error.to_string())),
                                }
                            }
                            view.picker = Some(picker);
                        }
                        Input::Cancel => {
                            if let Some(mut state) = view.thinking_settings.take() {
                                if let Some(model_picker) = state.model_picker.take() {
                                    state.levels = None;
                                    state.selected = None;
                                    view.picker = Some(model_picker);
                                    view.thinking_settings = Some(state);
                                } else {
                                    if picker.title == "Loading thinking models" {
                                        view.model_list = None;
                                    }
                                    state.parent.refresh_settings(settings_items(
                                        &args,
                                        &view,
                                        worker.as_ref(),
                                    ));
                                    view.picker = Some(state.parent);
                                }
                                continue;
                            }
                            if let Some(mut settings) = view.theme_settings.take() {
                                match settings.stage {
                                    ThemeSettingsStage::Single => {
                                        theme = load_resolved_theme(
                                            args.use_theme.as_deref().unwrap_or("system"),
                                            &args.theme_paths,
                                            &args.workspace,
                                            args.trusted,
                                        )
                                        .unwrap_or(settings.original);
                                        view.picker = Some(settings.parent);
                                    }
                                    ThemeSettingsStage::Automatic
                                    | ThemeSettingsStage::Light
                                    | ThemeSettingsStage::Dark => {
                                        settings.stage =
                                            if settings.stage == ThemeSettingsStage::Automatic {
                                                ThemeSettingsStage::Single
                                            } else {
                                                ThemeSettingsStage::Automatic
                                            };
                                        let next = settings.picker(&args);
                                        if let Ok(preview) = settings.preview(&next, &args) {
                                            theme = preview;
                                        }
                                        view.picker = Some(next);
                                        view.theme_settings = Some(settings);
                                    }
                                }
                                continue;
                            }
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
                                    save_default_model(&mut args)?;
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
                            if settings
                                && matches!(&selection, Selection::EditSetting(name) if name == "theme")
                            {
                                let theme_settings = ThemeSettings::new(picker, &args, &theme);
                                view.picker = Some(theme_settings.picker(&args));
                                view.theme_settings = Some(theme_settings);
                                continue;
                            }
                            if settings
                                && matches!(&selection, Selection::EditSetting(name) if name == "modelThinkingLevels")
                            {
                                if view.model_list.is_some() {
                                    picker.show_feedback(Err(
                                        "Model discovery is already in progress".into(),
                                    ));
                                    view.picker = Some(picker);
                                } else {
                                    view.thinking_settings = Some(ThinkingSettings {
                                        parent: picker,
                                        models: Vec::new(),
                                        overrides: args.settings.value()["modelThinkingLevels"]
                                            .as_object()
                                            .cloned()
                                            .unwrap_or_default(),
                                        model_picker: None,
                                        selected: None,
                                        levels: None,
                                        available_levels: Vec::new(),
                                    });
                                    start_model_picker(
                                        &args,
                                        &mut view,
                                        ModelListIntent::ThinkingSettings,
                                    );
                                    view.picker =
                                        Some(Picker::new("Loading thinking models", Vec::new()));
                                }
                                continue;
                            }
                            if let Selection::ThinkingModel(model) = &selection {
                                match start_thinking_levels(&args, model) {
                                    Ok(receiver) => {
                                        let state = view
                                            .thinking_settings
                                            .as_mut()
                                            .expect("thinking settings picker");
                                        state.selected = Some(model.clone());
                                        state.levels = Some(receiver);
                                        state.model_picker = Some(picker);
                                        view.picker = Some(Picker::new(
                                            "Loading thinking levels",
                                            Vec::new(),
                                        ));
                                    }
                                    Err(error) => {
                                        picker.show_feedback(Err(error.to_string()));
                                        view.picker = Some(picker);
                                    }
                                }
                                continue;
                            }
                            if let Selection::ThinkingOverride(level) = &selection {
                                let mut state = view
                                    .thinking_settings
                                    .take()
                                    .expect("thinking settings picker");
                                let model =
                                    state.selected.as_deref().expect("selected thinking model");
                                let result = if let Some(level) = level {
                                    args.settings.set_user_member(
                                        "modelThinkingLevels",
                                        model,
                                        json!(level),
                                    )
                                } else {
                                    args.settings
                                        .remove_user_member("modelThinkingLevels", model)
                                };
                                match result {
                                    Ok(()) => {
                                        if let Some(level) = level {
                                            state.overrides.insert(model.to_owned(), json!(level));
                                        } else {
                                            state.overrides.remove(model);
                                        }
                                        if let Some((provider, id)) =
                                            super::model_selection::qualified_model(
                                                model,
                                                &args.model_profiles,
                                            )
                                            && provider == args.provider
                                            && id == args.model
                                        {
                                            let requested = level.as_deref().unwrap_or_else(|| {
                                                args.settings
                                                    .thinking_without_qualified_override(id)
                                                    .unwrap_or("medium")
                                            });
                                            let effective = super::thinking::clamp(
                                                requested,
                                                &state.available_levels,
                                            )
                                            .to_owned();
                                            args.thinking_profile = Some((
                                                args.model.clone(),
                                                state.available_levels.clone(),
                                            ));
                                            let previous_level = view
                                                .pending_thinking
                                                .as_ref()
                                                .filter(|pending| {
                                                    pending.session == args.session
                                                        && pending.model == args.model
                                                        && pending.provider.as_deref()
                                                            == args.recorded_provider()
                                                })
                                                .map(|pending| pending.previous_level.clone())
                                                .unwrap_or_else(|| {
                                                    args.thinking
                                                        .as_deref()
                                                        .unwrap_or("off")
                                                        .to_owned()
                                                });
                                            args.thinking = Some(effective.clone());
                                            view.pending_thinking = Some(PendingThinking {
                                                session: args.session.clone(),
                                                provider: args
                                                    .recorded_provider()
                                                    .map(str::to_owned),
                                                model: args.model.clone(),
                                                level: effective,
                                                previous_level,
                                            });
                                            if is_busy(&worker) {
                                                view.flash = Some(("Thinking level applies after the current response".into(), Instant::now()));
                                            }
                                        }
                                        let mut models =
                                            state.model_picker.take().expect("model picker");
                                        models.refresh_thinking_models(thinking_model_items(
                                            &state.models,
                                            &state.overrides,
                                        ));
                                        state.selected = None;
                                        view.picker = Some(models);
                                        view.thinking_settings = Some(state);
                                    }
                                    Err(error) => {
                                        picker.show_feedback(Err(error.to_string()));
                                        view.picker = Some(picker);
                                        view.thinking_settings = Some(state);
                                    }
                                }
                                continue;
                            }
                            if let Selection::ThemeSettings(action) = selection {
                                let mut state =
                                    view.theme_settings.take().expect("theme settings picker");
                                let commit = match action {
                                    ThemeSettingsAction::Automatic => {
                                        state.stage = ThemeSettingsStage::Automatic;
                                        None
                                    }
                                    ThemeSettingsAction::Single => {
                                        state.stage = ThemeSettingsStage::Single;
                                        None
                                    }
                                    ThemeSettingsAction::Light => {
                                        state.stage = ThemeSettingsStage::Light;
                                        None
                                    }
                                    ThemeSettingsAction::Dark => {
                                        state.stage = ThemeSettingsStage::Dark;
                                        None
                                    }
                                    ThemeSettingsAction::Apply => {
                                        Some(format!("{}/{}", state.light, state.dark))
                                    }
                                    ThemeSettingsAction::Pick(name) => match state.stage {
                                        ThemeSettingsStage::Single => Some(name),
                                        ThemeSettingsStage::Light => {
                                            state.light = name;
                                            state.stage = ThemeSettingsStage::Automatic;
                                            None
                                        }
                                        ThemeSettingsStage::Dark => {
                                            state.dark = name;
                                            state.stage = ThemeSettingsStage::Automatic;
                                            None
                                        }
                                        ThemeSettingsStage::Automatic => None,
                                    },
                                };
                                if let Some(name) = commit {
                                    match select(
                                        Selection::Theme(name),
                                        &mut args,
                                        &mut view,
                                        &mut theme,
                                        &mut worker,
                                    ) {
                                        Ok(()) => {
                                            let mut parent = state.parent;
                                            parent.refresh_settings(settings_items(
                                                &args,
                                                &view,
                                                worker.as_ref(),
                                            ));
                                            view.picker = Some(parent);
                                        }
                                        Err(error) => {
                                            picker.show_feedback(Err(error.to_string()));
                                            view.picker = Some(picker);
                                            view.theme_settings = Some(state);
                                        }
                                    }
                                } else {
                                    let next = state.picker(&args);
                                    if let Ok(preview) = state.preview(&next, &args) {
                                        theme = preview;
                                    }
                                    view.picker = Some(next);
                                    view.theme_settings = Some(state);
                                }
                                continue;
                            }
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
                if view.keyboard_copies_selection(
                    keys.matches("tui.input.copy", key),
                    keys.matches("app.message.copy", key),
                    args.settings.boolean("fullscreenCopyOnSelect") != Some(false),
                ) && view.copy_selection()
                {
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
                    "app.interrupt" if auth_reload.is_some() && !is_busy(&worker) => {
                        auth_reload = None;
                        view.append("Provider credential reload cancelled");
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
                    "app.message.copy" => {
                        match Session::snapshot(&args.session)
                            .and_then(|session| sessions::copy_last(&session))
                        {
                            Ok(()) => view.flash = Some(("Copied!".into(), Instant::now())),
                            Err(error) => view.append(&error.to_string()),
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
                        if clear_editor_or_exit(&mut view, &mut last_clear, Instant::now()) {
                            break;
                        }
                    }
                    "app.exit" => break,
                    "app.editor.external" => {
                        external_editor =
                            open_external_editor(&args, &mut view, &mut _terminal, false)?;
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
                            match load_resolved_theme(
                                name.trim(),
                                &args.theme_paths,
                                &args.workspace,
                                args.trusted,
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
                        } else if input.trim() == "/debug" {
                            match view.write_debug_log(&args, &theme) {
                                Ok(path) => {
                                    view.append(&format!("Debug log written\n{}", path.display()))
                                }
                                Err(error) => view.append(&format!("Debug log: {error}")),
                            }
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
                        } else if matches!(input.trim(), "/session" | "/share")
                            && view.draft_images.is_empty()
                        {
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
                            auth_reload =
                                Some(AuthReload::start(args.model_keys.revision().to_owned()));
                            view.append("Reloading provider credentials (Esc to cancel)");
                            args.reload_model_profiles()?;
                            tokio::select! {
                                result = super::packages::prepare(&args) => result?,
                                _ = terminal_signal.notified() => { interrupted = true; break 'interactive; },
                            }
                            view.resources = super::resources::Resources::load(&args)?;
                            args.theme_paths = view.resources.themes.clone();
                            match Keys::load() {
                                Ok(next) => keys = next,
                                Err(error) => view.append(&error.to_string()),
                            }
                            match load_resolved_theme(
                                args.use_theme.as_deref().unwrap_or("system"),
                                &args.theme_paths,
                                &args.workspace,
                                args.trusted,
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
                            view.editor.type_character(character);
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    // Stop terminal input and restore the terminal before any worker teardown,
    // which can block while a running command or extension shuts down.
    #[cfg(unix)]
    if let Some(terminal) = &mut _terminal {
        terminal.disable_keyboard_protocol();
    }
    #[cfg(all(unix, debug_assertions))]
    test_input_drain_barrier();
    let _ = input.drain(Duration::from_secs(1), Duration::from_millis(50));
    let finish = if args.tui_mode == Some(TuiMode::Regular) {
        view.finish_regular()
    } else {
        Ok(())
    };
    drop(_terminal.take());
    finish?;
    if is_busy(&worker) {
        drop(worker.take());
    }
    if let Some(pending) = view.pending_thinking.take() {
        persist_pending_thinking(&pending)?;
        if pending.session == args.session && pending.previous_level != pending.level {
            args.selection_events
                .push(pi_rs::LifecycleEvent::ThinkingLevelSelect {
                    level: pending.level,
                    previous_level: pending.previous_level,
                });
        }
    }
    for event in std::mem::take(&mut args.selection_events) {
        if let Err(error) = Worker::notify_selection(&mut worker, &args, &event) {
            eprintln!("Extension selection notification: {error}");
        }
    }
    drop(worker);
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
    if interrupted || super::process::TERMINAL_INTERRUPTED.load(std::sync::atomic::Ordering::SeqCst)
    {
        Err(Error::Cancelled)
    } else {
        Ok(())
    }
}

// Let the PTY integration test deliver a release after protocol disable and
// before the drain starts, regardless of process scheduling.
#[cfg(all(unix, debug_assertions))]
fn test_input_drain_barrier() {
    use std::io::{Read, Write};
    let Some(address) = std::env::var_os("PI_RS_TEST_INPUT_DRAIN_BARRIER") else {
        return;
    };
    let Ok(address) = address.to_string_lossy().parse::<std::net::SocketAddr>() else {
        return;
    };
    if !address.ip().is_loopback() {
        return;
    }
    let Ok(mut socket) = std::net::TcpStream::connect_timeout(&address, Duration::from_secs(5))
    else {
        return;
    };
    let _ = socket.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = socket.set_write_timeout(Some(Duration::from_secs(5)));
    if socket.write_all(b"D").is_ok() {
        let _ = socket.read_exact(&mut [0]);
    }
}

fn show_startup_header(args: &Cli) -> bool {
    args.verbose || args.settings.boolean("quietStartup") != Some(true)
}

fn is_busy(worker: &Option<Worker>) -> bool {
    worker
        .as_ref()
        .is_some_and(|worker| worker.busy || worker.bashing)
}

fn persist_pending_thinking(pending: &PendingThinking) -> Result<()> {
    Session::open(&pending.session)?.select_thinking_level(
        pending.provider.as_deref(),
        &pending.model,
        &pending.level,
    )
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
    args.model_keys.forget(provider.id());
    view.invalidate_model_catalog();
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
    args.change_model(
        provider,
        selected.model,
        selected.thinking,
        pi_rs::ModelSelectSource::Set,
    )
}

fn save_default_model(args: &mut Cli) -> Result<()> {
    let qualified = format!("{}/{}", args.provider.id(), args.model);
    let add_to_scope = !args.model_scope.is_empty()
        && !args
            .model_scope
            .iter()
            .any(|choice| choice.provider == args.provider && choice.model == args.model);
    let mut values = vec![
        ("defaultProvider".into(), json!(args.provider.id())),
        ("defaultModel".into(), json!(args.model)),
    ];
    if add_to_scope {
        let mut enabled = args.settings.strings("enabledModels");
        if !enabled.is_empty()
            && !enabled
                .iter()
                .any(|value| value.eq_ignore_ascii_case(&qualified))
        {
            enabled.push(qualified);
            values.push(("enabledModels".into(), json!(enabled)));
        }
    }
    args.settings.set_user_values(&values)?;
    if add_to_scope {
        args.model_scope.push(super::model_selection::ScopedChoice {
            provider: args.provider.clone(),
            model: args.model.clone(),
            thinking: None,
        });
    }
    Ok(())
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
        args.change_model(
            choice.provider,
            choice.model,
            choice.thinking,
            pi_rs::ModelSelectSource::Cycle,
        )?;
    } else if args.model_scope.is_empty() {
        view.append("Only one model available");
    } else {
        view.append("Only one model in scope");
    }
    Ok(())
}

fn show_bedrock_methods(view: &mut View) {
    view.picker = Some(Picker::new(
        "Select Amazon Bedrock authentication method",
        vec![
            Item {
                label: "Bearer token".into(),
                selection: Selection::BedrockBearer,
            },
            Item {
                label: "AWS profile".into(),
                selection: Selection::BedrockProfile,
            },
            Item {
                label: "Existing AWS credential chain".into(),
                selection: Selection::BedrockChain,
            },
        ],
    ));
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

fn start_kimi_logout(view: &mut View) {
    view.remote_task = Some(RemoteTask::start(
        "Signing out of Kimi Code",
        "Kimi Code logout cancelled",
        move |sender| async move {
            let removed = super::kimi_auth::logout().await?;
            let _ = sender.send(RemoteEvent::ForgetModelKey("kimi-coding"));
            let _ = sender.send(RemoteEvent::RefreshModelKeys);
            Ok(vec![
                if removed {
                    "Signed out of Kimi Code"
                } else {
                    "No saved Kimi Code credential"
                }
                .into(),
            ])
        },
    ));
}

fn take_device_code_action(
    view: &mut View,
    key: KeyEvent,
    keys: &Keys,
) -> Option<(String, String)> {
    if !keys.matches("tui.select.confirm", key) {
        return None;
    }
    view.remote_task.as_mut()?.device_code.take()
}

fn start_xai_login(view: &mut View, settings: &crate::config::Settings) {
    let settings = settings.clone();
    let control = super::meta_auth::LoginControl::default();
    let task_control = control.clone();
    let mut task = RemoteTask::start(
        "Signing in to xAI",
        "xAI login cancelled",
        move |sender| async move {
            super::xai_auth::login(&settings, task_control, |url, code| {
                let _ = sender.send(RemoteEvent::DeviceCode {
                    url: url.to_string(),
                    code: code.to_owned(),
                });
            })
            .await?;
            let _ = sender.send(RemoteEvent::RefreshModelKeys);
            Ok(vec!["Signed in to xAI".into()])
        },
    );
    task.meta_login_control = Some(control);
    view.remote_task = Some(task);
}

fn start_meta_login(view: &mut View, settings: &crate::config::Settings) {
    let settings = settings.clone();
    let control = super::meta_auth::LoginControl::default();
    let task_control = control.clone();
    let mut task = RemoteTask::start(
        "Signing in to Meta",
        "Meta login cancelled",
        move |sender| async move {
            super::meta_auth::login(&settings, task_control, |url, code| {
                let _ = sender.send(RemoteEvent::Notice(format!("Enter code {code} at {url}")));
            })
            .await?;
            let _ = sender.send(RemoteEvent::RefreshModelKeys);
            Ok(vec!["Signed in to Meta".into()])
        },
    );
    task.meta_login_control = Some(control);
    view.remote_task = Some(task);
}

fn start_kimi_login(view: &mut View, settings: &crate::config::Settings) {
    let settings = settings.clone();
    view.remote_task = Some(RemoteTask::start(
        "Signing in to Kimi Code",
        "Kimi Code login cancelled",
        move |sender| async move {
            super::kimi_auth::login(&settings, |url, code| {
                let _ = sender.send(RemoteEvent::Notice(format!("Enter code {code} at {url}")));
            })
            .await?;
            let _ = sender.send(RemoteEvent::RefreshModelKeys);
            Ok(vec!["Signed in to Kimi Code".into()])
        },
    ));
}

fn start_openrouter_login(view: &mut View, settings: &crate::config::Settings) {
    let settings = settings.clone();
    view.remote_task = Some(RemoteTask::start(
        "Signing in to OpenRouter",
        "OpenRouter login cancelled",
        move |sender| async move {
            super::openrouter_auth::login(&settings, |url, manual| {
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

fn start_chatgpt_login(
    view: &mut View,
    settings: &crate::config::Settings,
    new_account: bool,
    account: Option<String>,
) {
    let settings = settings.clone();
    view.remote_task = Some(RemoteTask::start(
        "Signing in with ChatGPT",
        "ChatGPT sign-in cancelled",
        move |sender| async move {
            super::chatgpt_auth::login(&settings, new_account, account.as_deref(), |url| {
                let _ = sender.send(RemoteEvent::Notice(format!(
                    "Continue with ChatGPT in your browser: {url}"
                )));
            })
            .await?;
            let _ = sender.send(RemoteEvent::RefreshModelKeys);
            Ok(vec!["Signed in with ChatGPT".into()])
        },
    ));
}

fn chatgpt_logout_task<F, Fut>(control: super::chatgpt_auth::LogoutControl, logout: F) -> RemoteTask
where
    F: FnOnce(super::chatgpt_auth::LogoutControl) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<bool>> + 'static,
{
    let task_control = control.clone();
    let mut task = RemoteTask::start(
        "Signing out of ChatGPT",
        "ChatGPT sign-out cancelled",
        move |sender| async move {
            let result = logout(task_control).await;
            let _ = sender.send(RemoteEvent::ForgetModelKey("openai-chatgpt"));
            let _ = sender.send(RemoteEvent::RefreshModelKeys);
            result.map(|removed| {
                vec![
                    if removed {
                        "Signed out of ChatGPT"
                    } else {
                        "No saved ChatGPT session"
                    }
                    .into(),
                ]
            })
        },
    );
    task.logout_control = Some(control);
    task
}

fn start_chatgpt_logout(view: &mut View, settings: &crate::config::Settings) {
    let settings = settings.clone();
    view.remote_task = Some(chatgpt_logout_task(
        super::chatgpt_auth::LogoutControl::default(),
        move |control| async move { super::chatgpt_auth::logout_controlled(&settings, control).await },
    ));
}

fn interrupt_remote_task(view: &mut View) {
    let Some(task) = &view.remote_task else {
        return;
    };
    if task.cancel_before_commit() {
        let message = task.cancelled;
        view.remote_task = None;
        clear_openrouter_editor(view);
        view.append(message);
    } else if task.meta_login_control.is_some() {
        view.append(if task.status == "Signing in to xAI" {
            "xAI sign-in is completing; waiting for the result"
        } else {
            "Meta sign-in is completing; waiting for the result"
        });
    } else {
        view.append("ChatGPT sign-out is completing; waiting for the result");
    }
}

fn clear_openrouter_editor(view: &mut View) {
    if matches!(view.local_editor, Some((LocalEditor::OpenRouterCode(_), _))) {
        view.local_editor = None;
    }
}

fn start_radius_login(
    view: &mut View,
    settings: &crate::config::Settings,
    method: radius_auth::Method,
) -> Result<()> {
    let gateway = radius_auth::Gateway::configured_with_settings(settings)?;
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

fn start_radius_logout(view: &mut View, settings: &crate::config::Settings) -> Result<()> {
    let gateway = radius_auth::Gateway::configured_with_settings(settings)?;
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
            let catalogs = match runtime.block_on(super::model_selection::catalogs(&args, false)) {
                Ok(catalogs) => catalogs,
                Err(_) if matches!(intent, ModelListIntent::ThinkingSettings) => Vec::new(),
                Err(error) => return Err(error),
            };
            let mut models: Vec<_> = catalogs
                .into_iter()
                .flat_map(|catalog| {
                    catalog
                        .models
                        .into_iter()
                        .map(move |model| format!("{}/{model}", catalog.provider.id()))
                })
                .collect();
            if matches!(intent, ModelListIntent::ThinkingSettings) {
                let current = format!("{}/{}", args.provider.id(), args.model);
                models.retain(|model| model != &current);
                models.insert(0, current);
            }
            Ok(models)
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

impl ThemeSettings {
    fn new(parent: Picker, args: &Cli, theme: &Theme) -> Self {
        let selected = args.use_theme.as_deref().unwrap_or("system");
        let pair = crate::ui::theme::parse_auto_theme_setting(selected);
        let (light, dark) = pair.unwrap_or((selected, selected));
        Self {
            parent,
            original: theme.clone(),
            single: theme.name.clone(),
            light: light.into(),
            dark: dark.into(),
            stage: if pair.is_some() {
                ThemeSettingsStage::Automatic
            } else {
                ThemeSettingsStage::Single
            },
        }
    }

    fn picker(&self, args: &Cli) -> Picker {
        if self.stage == ThemeSettingsStage::Automatic {
            return Picker::new(
                "Automatic theme",
                vec![
                    Item {
                        label: format!("Light theme · {}", self.light),
                        selection: Selection::ThemeSettings(ThemeSettingsAction::Light),
                    },
                    Item {
                        label: format!("Dark theme · {}", self.dark),
                        selection: Selection::ThemeSettings(ThemeSettingsAction::Dark),
                    },
                    Item {
                        label: "Use automatic theme".into(),
                        selection: Selection::ThemeSettings(ThemeSettingsAction::Apply),
                    },
                    Item {
                        label: "Choose a single theme".into(),
                        selection: Selection::ThemeSettings(ThemeSettingsAction::Single),
                    },
                ],
            );
        }
        let mut items = Vec::new();
        if self.stage == ThemeSettingsStage::Single {
            items.push(Item {
                label: "Automatic · follow terminal appearance".into(),
                selection: Selection::ThemeSettings(ThemeSettingsAction::Automatic),
            });
        }
        items.extend(
            Theme::discover(&args.theme_paths, &args.workspace, args.trusted, false)
                .into_iter()
                .map(|name| Item {
                    label: name.clone(),
                    selection: Selection::ThemeSettings(ThemeSettingsAction::Pick(name)),
                }),
        );
        let (title, current) = match self.stage {
            ThemeSettingsStage::Single => ("Theme", &self.single),
            ThemeSettingsStage::Light => ("Light theme", &self.light),
            ThemeSettingsStage::Dark => ("Dark theme", &self.dark),
            ThemeSettingsStage::Automatic => unreachable!(),
        };
        let mut picker = Picker::new(title, items);
        picker.focus_theme(current);
        picker
    }

    fn preview(&self, picker: &Picker, args: &Cli) -> Result<Theme> {
        let candidate = match (self.stage, picker.selected()) {
            (
                ThemeSettingsStage::Single,
                Some(Selection::ThemeSettings(ThemeSettingsAction::Pick(name))),
            ) => name,
            (
                ThemeSettingsStage::Single,
                Some(Selection::ThemeSettings(ThemeSettingsAction::Automatic)),
            ) => format!("{}/{}", self.light, self.dark),
            (
                ThemeSettingsStage::Light,
                Some(Selection::ThemeSettings(ThemeSettingsAction::Pick(name))),
            ) => format!("{name}/{}", self.dark),
            (
                ThemeSettingsStage::Dark,
                Some(Selection::ThemeSettings(ThemeSettingsAction::Pick(name))),
            ) => format!("{}/{name}", self.light),
            (ThemeSettingsStage::Single, _) => self.single.clone(),
            _ => format!("{}/{}", self.light, self.dark),
        };
        load_resolved_theme(&candidate, &args.theme_paths, &args.workspace, args.trusted)
    }
}

fn thinking_model_items(
    models: &[String],
    overrides: &serde_json::Map<String, Value>,
) -> Vec<Item> {
    models
        .iter()
        .map(|model| {
            let override_level = overrides.get(model).and_then(Value::as_str);
            Item {
                label: override_level
                    .map_or_else(|| model.clone(), |level| format!("{model} · {level}")),
                selection: Selection::ThinkingModel(model.clone()),
            }
        })
        .collect()
}

fn thinking_level_picker(state: &ThinkingSettings) -> Picker {
    let model = state.selected.as_deref().expect("selected model");
    let selected = state.overrides.get(model).and_then(Value::as_str);
    let mut items: Vec<_> = state
        .available_levels
        .iter()
        .map(|level| Item {
            label: format!(
                "{} {level}",
                if selected == Some(level.as_str()) {
                    "✓"
                } else {
                    " "
                }
            ),
            selection: Selection::ThinkingOverride(Some(level.clone())),
        })
        .collect();
    if selected.is_some() {
        items.push(Item {
            label: "Clear override · use default".into(),
            selection: Selection::ThinkingOverride(None),
        });
    }
    let mut picker = Picker::new(&format!("Thinking level · {model}"), items);
    picker.focus_thinking_level(selected);
    picker
}

fn start_thinking_levels(
    args: &Cli,
    model: &str,
) -> Result<std::sync::mpsc::Receiver<Vec<String>>> {
    let (provider, id) = super::model_selection::qualified_model(model, &args.model_profiles)
        .ok_or_else(|| Error::Config("Unknown model provider".into()))?;
    let mut selected = args.clone();
    selected.provider = provider;
    selected.model = id.into();
    selected.thinking_profile = None;
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let levels = super::thinking::available(&mut selected)
            .into_iter()
            .filter(|level| super::thinking::LEVELS.contains(&level.as_str()))
            .collect();
        let _ = sender.send(levels);
    });
    Ok(receiver)
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
                    30_000 => "30 sec".to_owned(),
                    60_000 => "1 min".to_owned(),
                    120_000 => "2 min".to_owned(),
                    300_000 => "5 min".to_owned(),
                    ms => format!("{} sec", ms as f64 / 1000.0),
                }
            ),
            selection: Selection::Setting(
                "httpIdleTimeoutMs".into(),
                json!(match args.settings.http_idle_timeout_ms() {
                    30_000 => 60_000,
                    60_000 => 120_000,
                    120_000 => 300_000,
                    300_000 => 0,
                    _ => 30_000,
                }),
            ),
        },
        queue_setting("steeringMode", "Steering mode", pi_rs::InputKind::Steer),
        queue_setting("followUpMode", "Follow-up mode", pi_rs::InputKind::FollowUp),
        Item {
            label: format!("Theme · {}", args.use_theme.as_deref().unwrap_or("system")),
            selection: Selection::EditSetting("theme".into()),
        },
        Item {
            label: format!(
                "Default thinking level per model · {} configured",
                args.settings.value()["modelThinkingLevels"]
                    .as_object()
                    .map_or(0, |levels| levels.len())
            ),
            selection: Selection::EditSetting("modelThinkingLevels".into()),
        },
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
            label: format!("Save default model · {}/{}", args.provider.id(), args.model),
            selection: Selection::SaveDefaultModel,
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
    let quiet = &args.settings.value()["quietStartup"];
    let quiet_label = if quiet == true {
        "true"
    } else if quiet == "header" {
        "header"
    } else {
        "false"
    };
    let share = args
        .settings
        .string("sessionShareDestination")
        .unwrap_or("auto");
    let trust = args.settings.string("defaultProjectTrust").unwrap_or("ask");
    let wheel = &args.settings.value()["fullscreenWheelScrollLines"];
    let wheel_lines = wheel
        .as_f64()
        .map(|value| value.clamp(1.0, 100.0).floor() as u64);
    items.extend([
        Item {
            label: format!(
                "TUI mode · {}",
                if args.tui_mode == Some(TuiMode::Regular) {
                    "regular"
                } else {
                    "fullscreen"
                }
            ),
            selection: Selection::Setting(
                "tuiMode".into(),
                json!(if args.tui_mode == Some(TuiMode::Regular) {
                    "fullscreen"
                } else {
                    "regular"
                }),
            ),
        },
        Item {
            label: format!("Quiet startup · {quiet_label}"),
            selection: Selection::Setting(
                "quietStartup".into(),
                match quiet_label {
                    "true" => json!("header"),
                    "header" => json!(false),
                    _ => json!(true),
                },
            ),
        },
        Item {
            label: format!("Session share destination · {share}"),
            selection: Selection::Setting(
                "sessionShareDestination".into(),
                json!(match share {
                    "auto" => "gist",
                    "gist" => "radius",
                    _ => "auto",
                }),
            ),
        },
        Item {
            label: format!("Default project trust · {trust}"),
            selection: Selection::Setting(
                "defaultProjectTrust".into(),
                json!(match trust {
                    "ask" => "always",
                    "always" => "never",
                    _ => "ask",
                }),
            ),
        },
        Item {
            label: format!(
                "Fullscreen wheel scrolling · {}",
                wheel_lines.map_or_else(|| "auto".into(), |n| n.to_string())
            ),
            selection: Selection::Setting(
                "fullscreenWheelScrollLines".into(),
                match wheel_lines {
                    None => json!(1),
                    Some(current) => [1, 2, 3, 5, 10]
                        .into_iter()
                        .find(|value| *value > current)
                        .map_or_else(|| json!("auto"), |value| json!(value)),
                },
            ),
        },
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
                    "off" => "final",
                    "final" => "streaming",
                    _ => "off",
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
        "defaultModel",
        "defaultThinkingLevel",
        "httpProxy",
        "sessionDir",
        "shellPath",
        "shellCommandPrefix",
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
                if name == "httpProxy"
                    && args
                        .settings
                        .string(name)
                        .is_some_and(|value| !value.is_empty())
                {
                    "configured".into()
                } else {
                    args.settings
                        .value()
                        .get(name)
                        .map(Value::to_string)
                        .unwrap_or_else(|| "default".into())
                }
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
        Selection::LoginProvider(provider) if provider.id() == "amazon-bedrock" => {
            show_bedrock_methods(view)
        }
        Selection::LoginProvider(provider) => edit_provider_key(provider, view),
        Selection::BedrockBearer => edit_provider_key(super::Provider::Bedrock, view),
        Selection::BedrockProfile => {
            let dialog = crate::ui::dialog::Dialog::new(
                uuid::Uuid::new_v4(),
                pi_rs::UiRequest::Input {
                    title: "Enter AWS profile name".into(),
                    placeholder: None,
                    initial_value: None,
                    timeout: None,
                },
            );
            view.local_editor = Some((LocalEditor::BedrockProfile, dialog));
        }
        Selection::BedrockChain => {
            let dialog = crate::ui::dialog::Dialog::new(
                uuid::Uuid::new_v4(),
                pi_rs::UiRequest::Input {
                    title: "Configure AWS credentials, then press Enter to continue".into(),
                    placeholder: None,
                    initial_value: None,
                    timeout: None,
                },
            );
            view.local_editor = Some((LocalEditor::BedrockChain, dialog));
        }
        Selection::LoginXai => start_xai_login(view, &args.settings),
        Selection::LoginMeta => start_meta_login(view, &args.settings),
        Selection::LoginKimi => start_kimi_login(view, &args.settings),
        Selection::LoginOpenRouter => start_openrouter_login(view, &args.settings),
        Selection::LoginChatGpt => start_chatgpt_login(view, &args.settings, false, None),
        Selection::LoginChatGptNew => start_chatgpt_login(view, &args.settings, true, None),
        Selection::LoginChatGptAccount(id) => {
            start_chatgpt_login(view, &args.settings, false, Some(id))
        }
        Selection::LogoutChatGpt => start_chatgpt_logout(view, &args.settings),
        Selection::LogoutProvider(provider) if provider.id() == "kimi-coding" => {
            start_kimi_logout(view)
        }
        Selection::LogoutProvider(provider) => remove_provider_key(provider, args, view)?,
        Selection::RadiusMethods => show_radius_methods(view),
        Selection::LoginRadius(method) => start_radius_login(view, &args.settings, method)?,
        Selection::LogoutRadius => start_radius_logout(view, &args.settings)?,
        Selection::Thinking(thinking) => super::thinking::set(args, &thinking)?,
        Selection::SaveDefaultModel => save_default_model(args)?,
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
                "shellPath" | "shellCommandPrefix" | "httpProxy" => json!(""),
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
                let next = load_resolved_theme(
                    value
                        .as_str()
                        .ok_or_else(|| Error::Config("theme must be a string".into()))?,
                    &args.theme_paths,
                    &args.workspace,
                    args.trusted,
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
                "tuiMode" => {
                    args.tui_mode = Some(if value == "regular" {
                        TuiMode::Regular
                    } else {
                        TuiMode::Fullscreen
                    })
                }
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
            let next =
                load_resolved_theme(&name, &args.theme_paths, &args.workspace, args.trusted)?;
            args.settings.set_user("theme", json!(name))?;
            args.use_theme = Some(name);
            *theme = next;
        }
        Selection::ThemeSettings(_) => {
            return Err(Error::Config(
                "theme settings require the interactive selector".into(),
            ));
        }
        Selection::ThinkingModel(_) | Selection::ThinkingOverride(_) => {
            return Err(Error::Config(
                "thinking settings require the interactive selector".into(),
            ));
        }
    }
    Ok(())
}

// A bare Escape may need the input decoder's 25 ms disambiguation window.
// Give already-arrived input priority over committing a prepared session.
fn queued_transition_cancel(
    deferred: &mut VecDeque<crate::ui::input::InputEvent>,
    mut next: impl FnMut(Duration) -> std::io::Result<Option<crate::ui::input::InputEvent>>,
) -> Result<bool> {
    let deadline = Instant::now() + Duration::from_millis(30);
    while Instant::now() < deadline {
        match next(deadline.saturating_duration_since(Instant::now()))? {
            Some(crate::ui::input::InputEvent::Event(Event::Key(key)))
                if key.code == KeyCode::Esc && key.kind != KeyEventKind::Release =>
            {
                return Ok(true);
            }
            Some(crate::ui::input::InputEvent::Event(
                Event::Key(_) | Event::Mouse(_) | Event::Paste(_),
            )) => {}
            Some(event) => deferred.push_back(event),
            None => {}
        }
    }
    Ok(false)
}

fn switch_session(args: &mut Cli, view: &mut View, path: PathBuf) -> Result<()> {
    let mut next = args.clone();
    next.session = path;
    next.session_transition = Some(pi_rs::SessionStartReason::Resume);
    if view.async_transitions {
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let result = restore_workspace(&mut next).map(|()| next);
            let _ = sender.send(result);
        });
        view.pending_transition = Some(receiver);
        view.append("Preparing session (Esc to cancel)");
        return Ok(());
    }
    restore_workspace(&mut next)?;
    apply_session(args, view, next)
}

fn apply_session(args: &mut Cli, view: &mut View, mut next: Cli) -> Result<()> {
    let resources = super::resources::Resources::load(&next)?;
    next.theme_paths = resources.themes.clone();
    if matches!(
        next.session_transition,
        Some(pi_rs::SessionStartReason::New)
    ) {
        if let Some(directory) = next.session.parent() {
            std::fs::create_dir_all(directory)?;
        }
        initialize_session(&mut next)?;
    }
    view.restore(&next)?;
    view.resources = resources;
    *args = next;
    if args.trust_pending {
        slash("/trust", args, view, None)?;
    }
    Ok(())
}

async fn prepare_initial_session_async(args: Cli) -> Result<Cli> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let mut args = args;
        let result = restore_workspace(&mut args).map(|()| args);
        let _ = sender.send(result);
    });
    let mut args = receiver
        .await
        .map_err(|_| Error::Config("workspace discovery stopped".into()))??;
    initialize_session(&mut args)?;
    Ok(args)
}

pub(super) fn restore_workspace_cancellable(
    args: &mut Cli,
    cancelled: &std::sync::atomic::AtomicBool,
) -> Result<()> {
    let (sender, receiver) = std::sync::mpsc::channel();
    let mut next = args.clone();
    std::thread::spawn(move || {
        let result = restore_workspace(&mut next).map(|()| next);
        let _ = sender.send(result);
    });
    loop {
        if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::Cancelled);
        }
        match receiver.recv_timeout(Duration::from_millis(50)) {
            Ok(result) => {
                *args = result?;
                return Ok(());
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => (),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(Error::Config("workspace discovery stopped".into()));
            }
        }
    }
}

#[cfg(test)]
fn prepare_initial_session(args: &mut Cli) -> Result<()> {
    restore_workspace(args)?;
    initialize_session(args)
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
    args.restore_session_model(&session);
    super::thinking::adjust(args);
    args.record_automatic_model_selection(&mut session)?;
    Ok(())
}
pub(super) fn restore_workspace(args: &mut Cli) -> Result<()> {
    let session = Session::open(&args.session)?;
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
    args.refresh_workspace_selection();
    args.restore_session_model(&session);
    drop(session);
    super::thinking::adjust(args);
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExternalEditorTarget {
    Main,
    Local(uuid::Uuid),
    Extension(uuid::Uuid),
}

#[cfg(windows)]
type ExternalEditorChild = Box<dyn process_wrap::std::ChildWrapper>;
#[cfg(not(windows))]
type ExternalEditorChild = std::process::Child;

struct ExternalEditorFile {
    _directory: tempfile::TempDir,
    path: std::path::PathBuf,
}
impl ExternalEditorFile {
    fn new(text: &str) -> std::io::Result<Self> {
        let directory = tempfile::Builder::new().prefix("pi-editor-").tempdir()?;
        let path = directory.path().join("prompt.md");
        // Close the file before launching the editor. Windows applications may
        // open it without sharing access with an existing writable handle.
        std::fs::write(&path, text)?;
        Ok(Self {
            _directory: directory,
            path,
        })
    }
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

struct RunningExternalEditor {
    child: ExternalEditorChild,
    file: ExternalEditorFile,
    target: ExternalEditorTarget,
    #[cfg(unix)]
    _foreground: Option<EditorForeground>,
}

#[cfg(all(test, unix))]
static EDITOR_JOB_RESUMES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cfg(unix)]
struct EditorForeground {
    tty: std::fs::File,
    previous: libc::pid_t,
    baseline: libc::termios,
}

#[cfg(unix)]
fn set_foreground(fd: std::os::fd::RawFd, group: libc::pid_t) -> std::io::Result<()> {
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        let mut old: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGTTOU);
        let status = libc::pthread_sigmask(libc::SIG_BLOCK, &set, &mut old);
        if status != 0 {
            return Err(std::io::Error::from_raw_os_error(status));
        }
        let result = if libc::tcsetpgrp(fd, group) == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        };
        libc::pthread_sigmask(libc::SIG_SETMASK, &old, std::ptr::null_mut());
        result
    }
}

// A separate process group stops the Pi job while Pi waits on a pipe. This
// keeps Pi from running past a pending group stop, and lets the helper resume
// an unmanaged launcher without touching the parent shell's process group.
#[cfg(unix)]
struct EditorJobSupervisor {
    pid: libc::pid_t,
    control: std::os::fd::RawFd,
    response: std::os::fd::RawFd,
}

#[cfg(unix)]
impl EditorJobSupervisor {
    fn start(tty: std::os::fd::RawFd, group: libc::pid_t) -> std::io::Result<Self> {
        let mut response = [0; 2];
        let mut control = [0; 2];
        if unsafe { libc::pipe(response.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        if unsafe { libc::pipe(control.as_mut_ptr()) } != 0 {
            let error = std::io::Error::last_os_error();
            unsafe {
                libc::close(response[0]);
                libc::close(response[1]);
            }
            return Err(error);
        }
        for fd in [response[0], response[1], control[0], control[1]] {
            if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
                let error = std::io::Error::last_os_error();
                unsafe {
                    libc::close(response[0]);
                    libc::close(response[1]);
                    libc::close(control[0]);
                    libc::close(control[1]);
                }
                return Err(error);
            }
        }
        let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0
            || limit.rlim_cur > libc::c_int::MAX as libc::rlim_t
        {
            let error = std::io::Error::other("could not bound supervisor file descriptors");
            unsafe {
                libc::close(response[0]);
                libc::close(response[1]);
                libc::close(control[0]);
                libc::close(control[1]);
            }
            return Err(error);
        }
        let max_fd = limit.rlim_cur as libc::c_int;
        let parent = unsafe { libc::getpid() };
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            let error = std::io::Error::last_os_error();
            unsafe {
                libc::close(response[0]);
                libc::close(response[1]);
                libc::close(control[0]);
                libc::close(control[1]);
            }
            return Err(error);
        }
        if pid == 0 {
            // After fork from a multithreaded parent, call only libc and _exit.
            unsafe {
                // Do not retain unrelated worker pipes, sockets, or terminal
                // descriptors while the shell holds a stopped job.
                for fd in 0..max_fd {
                    if fd != response[1] && fd != control[0] && fd != tty {
                        libc::close(fd);
                    }
                }
                let ready = u8::from(libc::setpgid(0, 0) == 0);
                libc::write(response[1], (&ready as *const u8).cast(), 1);
                let mut command = 0u8;
                if ready == 1
                    && libc::read(control[0], (&mut command as *mut u8).cast(), 1) == 1
                    && libc::fcntl(control[0], libc::F_SETFL, libc::O_NONBLOCK) == 0
                    && libc::kill(-group, libc::SIGSTOP) == 0
                {
                    // Give a managing shell time to take the terminal. Keep
                    // waiting while it owns the terminal, including a long
                    // stopped job; foregrounding Pi permits resumption.
                    let mut shell_owned_terminal = false;
                    for attempt in 0.. {
                        let mut delay = libc::timespec {
                            tv_sec: 0,
                            tv_nsec: 10_000_000,
                        };
                        let mut remaining: libc::timespec = std::mem::zeroed();
                        while libc::nanosleep(&delay, &mut remaining) != 0 {
                            delay = remaining;
                        }
                        if libc::read(control[0], (&mut command as *mut u8).cast(), 1) == 0
                            || libc::getppid() != parent
                        {
                            break;
                        }
                        if libc::tcgetpgrp(tty) != group {
                            shell_owned_terminal = true;
                        } else if shell_owned_terminal || attempt >= 99 {
                            libc::kill(-group, libc::SIGCONT);
                            let done = 1u8;
                            libc::write(response[1], (&done as *const u8).cast(), 1);
                            break;
                        }
                    }
                }
                libc::_exit(0);
            }
        }
        unsafe {
            libc::close(response[1]);
            libc::close(control[0]);
        }
        let ready = read_editor_supervisor_byte(response[0]);
        if !matches!(ready, Ok(1)) {
            unsafe {
                libc::close(response[0]);
                libc::close(control[1]);
                libc::kill(pid, libc::SIGKILL);
                libc::waitpid(pid, std::ptr::null_mut(), 0);
            }
            return Err(std::io::Error::other(
                "could not start editor job supervisor",
            ));
        }
        Ok(Self {
            pid,
            control: control[1],
            response: response[0],
        })
    }

    fn stop_and_wait(&self) -> std::io::Result<()> {
        let command = 1u8;
        if unsafe { libc::write(self.control, (&command as *const u8).cast(), 1) } != 1 {
            return Err(std::io::Error::last_os_error());
        }
        if read_editor_supervisor_byte(self.response)? != 1 {
            return Err(std::io::Error::other("editor job supervisor stopped"));
        }
        Ok(())
    }
}

#[cfg(unix)]
fn read_editor_supervisor_byte(fd: std::os::fd::RawFd) -> std::io::Result<u8> {
    let mut byte = 0u8;
    loop {
        let n = unsafe { libc::read(fd, (&mut byte as *mut u8).cast(), 1) };
        if n == 1 {
            return Ok(byte);
        }
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "editor job supervisor stopped",
            ));
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(unix)]
impl Drop for EditorJobSupervisor {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.control);
            libc::close(self.response);
            libc::kill(self.pid, libc::SIGKILL);
            while libc::waitpid(self.pid, std::ptr::null_mut(), 0) < 0
                && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
            {
            }
        }
    }
}

#[cfg(unix)]
impl EditorForeground {
    fn prepare() -> std::io::Result<Option<Self>> {
        use std::os::fd::AsRawFd;
        let tty = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/tty")
        {
            Ok(tty) => tty,
            Err(_) => return Ok(None),
        };
        let previous = unsafe { libc::tcgetpgrp(tty.as_raw_fd()) };
        if previous < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut baseline = unsafe { std::mem::zeroed() };
        if unsafe { libc::tcgetattr(tty.as_raw_fd(), &mut baseline) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Some(Self {
            tty,
            previous,
            baseline,
        }))
    }

    fn restore(&self) -> std::io::Result<()> {
        use std::os::fd::AsRawFd;
        let fd = self.tty.as_raw_fd();
        set_foreground(fd, self.previous)?;
        if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &self.baseline) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    fn suspend_and_resume(&self, editor_group: libc::pid_t) -> std::io::Result<()> {
        use std::os::fd::AsRawFd;
        let fd = self.tty.as_raw_fd();
        // Suspend only when the parent can plausibly supervise a foreground
        // job in this same session. A shared process group or a parent in a
        // different session cannot reliably foreground Pi again.
        let group = unsafe { libc::getpgrp() };
        let parent = unsafe { libc::getppid() };
        let parent_group = unsafe { libc::getpgid(parent) };
        let session = unsafe { libc::getsid(0) };
        let parent_session = unsafe { libc::getsid(parent) };
        if parent_group <= 0
            || parent_group == group
            || session <= 0
            || group == session
            || parent_session != session
            || self.previous != group
        {
            if unsafe { libc::kill(-editor_group, libc::SIGCONT) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            #[cfg(test)]
            EDITOR_JOB_RESUMES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return Ok(());
        }
        let mut editor_settings = unsafe { std::mem::zeroed() };
        if unsafe { libc::tcgetattr(fd, &mut editor_settings) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        set_foreground(fd, self.previous)?;
        if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &self.baseline) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        // A parent in the same session may still lack job control. If no shell
        // takes the terminal, wake the stopped job instead of hanging forever.
        let watchdog = match EditorJobSupervisor::start(fd, group) {
            Ok(watchdog) => watchdog,
            Err(_) => {
                if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &editor_settings) } != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                set_foreground(fd, editor_group)?;
                if unsafe { libc::kill(-editor_group, libc::SIGCONT) } != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                #[cfg(test)]
                EDITOR_JOB_RESUMES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Ok(());
            }
        };
        // The helper stops the whole Pi group. Blocking for its response keeps
        // this thread from running past an asynchronously delivered SIGSTOP.
        watchdog.stop_and_wait()?;
        drop(watchdog);
        // Restore the editor's terminal state before handing input back.
        if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &editor_settings) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        set_foreground(fd, editor_group)?;
        if unsafe { libc::kill(-editor_group, libc::SIGCONT) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        #[cfg(test)]
        EDITOR_JOB_RESUMES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for EditorForeground {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

impl RunningExternalEditor {
    fn cancelled_by(&self, event: &Value) -> bool {
        event["type"] == "extension_ui_cancelled"
            && matches!(self.target, ExternalEditorTarget::Extension(id) if json!(id) == event["id"])
    }
    fn start(command: &str, text: &str, target: ExternalEditorTarget) -> Result<Self> {
        let parts =
            shell_words::split(command).map_err(|error| Error::Config(error.to_string()))?;
        let Some((program, arguments)) = parts.split_first() else {
            return Err(Error::Config("external editor is empty".into()));
        };
        let file = ExternalEditorFile::new(text)?;
        #[cfg(unix)]
        let foreground = EditorForeground::prepare()?;
        let mut process = std::process::Command::new(program);
        process.args(arguments).arg(file.path());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            process.process_group(0);
        }
        #[cfg(windows)]
        let child = {
            let mut wrapped = process_wrap::std::CommandWrap::from(process);
            wrapped.wrap(process_wrap::std::JobObject);
            wrapped.spawn()
        };
        #[cfg(not(windows))]
        let child = process.spawn();
        let child = child
            .map_err(|_| Error::Config(format!("Failed to launch external editor: {command}")))?;
        #[cfg(unix)]
        let mut child = child;
        #[cfg(unix)]
        if let Some(foreground) = &foreground {
            use std::os::fd::AsRawFd;
            if let Err(error) =
                set_foreground(foreground.tty.as_raw_fd(), child.id() as libc::pid_t)
            {
                if !matches!(child.try_wait(), Ok(Some(_))) {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(error.into());
                }
            } else {
                unsafe {
                    libc::kill(-(child.id() as libc::pid_t), libc::SIGCONT);
                }
            }
        }
        Ok(Self {
            child,
            file,
            target,
            #[cfg(unix)]
            _foreground: foreground,
        })
    }

    fn poll(&mut self) -> Result<Option<std::process::ExitStatus>> {
        #[cfg(unix)]
        if let Some(foreground) = &self._foreground {
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            let rc = unsafe {
                libc::waitid(
                    libc::P_PID,
                    self.child.id() as libc::id_t,
                    &mut info,
                    libc::WSTOPPED | libc::WNOHANG,
                )
            };
            if rc != 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() != std::io::ErrorKind::Interrupted
                    && error.raw_os_error() != Some(libc::ECHILD)
                {
                    return Err(error.into());
                }
            } else if unsafe { info.si_pid() } != 0
                && let Err(error) = foreground.suspend_and_resume(self.child.id() as libc::pid_t)
            {
                if let Some(status) = self.child.try_wait()? {
                    return Ok(Some(status));
                }
                return Err(error.into());
            }
        }
        Ok(self.child.try_wait()?)
    }

    fn cancel(&mut self) {
        #[cfg(unix)]
        unsafe {
            libc::kill(-(self.child.id() as libc::pid_t), libc::SIGKILL);
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn result(&self, status: std::process::ExitStatus) -> Result<String> {
        if !status.success() {
            let exit = status
                .code()
                .map_or_else(|| status.to_string(), |code| format!("code {code}"));
            return Err(Error::Config(format!(
                "External editor exited with {exit}; prompt unchanged"
            )));
        }
        Ok(external_editor_text(&std::fs::read(self.file.path())?))
    }
}

impl Drop for RunningExternalEditor {
    fn drop(&mut self) {
        // A wrapper can exit while its descendants keep running. The Windows
        // job must be terminated even after the wrapper has been reaped.
        #[cfg(windows)]
        self.cancel();
        #[cfg(not(windows))]
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            self.cancel();
        }
    }
}

// Cancel only an extension-owned editor. A worker exit invalidates all its
// dialogs; an explicit cancellation is correlated to a single request.
fn cancel_running_extension_editor(
    running: &mut Option<RunningExternalEditor>,
    event: Option<&Value>,
) -> Option<ExternalEditorTarget> {
    let editor = running.as_ref()?;
    if !matches!(editor.target, ExternalEditorTarget::Extension(_))
        || event.is_some_and(|event| !editor.cancelled_by(event))
    {
        return None;
    }
    let mut editor = running.take()?;
    let target = editor.target;
    editor.cancel();
    drop(editor);
    Some(target)
}

fn open_external_editor(
    args: &Cli,
    view: &mut View,
    terminal: &mut Option<Terminal>,
    dialog: bool,
) -> Result<Option<RunningExternalEditor>> {
    if args.tui_mode == Some(TuiMode::Regular) {
        view.finish_regular()?;
    }
    drop(terminal.take());
    EXTERNAL_EDITOR_ACTIVE.store(true, std::sync::atomic::Ordering::SeqCst);
    let (target, text) = if dialog {
        let local = view.local_editor.is_some();
        let dialog = view.dialog().expect("active editor dialog");
        (
            if local {
                ExternalEditorTarget::Local(dialog.id)
            } else {
                ExternalEditorTarget::Extension(dialog.id)
            },
            dialog.external_editor_text().to_owned(),
        )
    } else {
        (ExternalEditorTarget::Main, view.editor.text.clone())
    };
    let command = external_editor_command(
        args.settings.string("externalEditor"),
        std::env::var("VISUAL").ok().as_deref(),
        std::env::var("EDITOR").ok().as_deref(),
    );
    match RunningExternalEditor::start(&command, &text, target) {
        Ok(editor) => Ok(Some(editor)),
        Err(error) => {
            finish_external_editor(args, view, terminal, target, Some(Err(error)))?;
            Ok(None)
        }
    }
}

fn finish_external_editor(
    args: &Cli,
    view: &mut View,
    terminal: &mut Option<Terminal>,
    target: ExternalEditorTarget,
    result: Option<Result<String>>,
) -> Result<()> {
    if let Some(result) = result {
        match target {
            ExternalEditorTarget::Main => match result {
                Ok(text) => view.editor.replace(text),
                Err(error) => view.append(&error.to_string()),
            },
            ExternalEditorTarget::Local(id) | ExternalEditorTarget::Extension(id) => {
                if let Some(dialog) = view.dialog_mut().filter(|dialog| dialog.id == id) {
                    dialog.external_edit(|draft| {
                        draft.replace(result?);
                        Ok(())
                    });
                }
            }
        }
    }
    *terminal = Some(Terminal::enter(args.tui_mode, view.graphics.cleanup())?);
    EXTERNAL_EDITOR_ACTIVE.store(false, std::sync::atomic::Ordering::SeqCst);
    if view.graphics.enabled() {
        images::query_cell_size()?;
    }
    view.graphics.reset();
    view.regular.redraw = true;
    Ok(())
}

fn external_editor_command(
    configured: Option<&str>,
    visual: Option<&str>,
    editor: Option<&str>,
) -> String {
    configured
        .filter(|command| {
            !command
                .trim_matches(|c: char| (c.is_whitespace() && c != '\u{0085}') || c == '\u{feff}')
                .is_empty()
        })
        .or_else(|| visual.filter(|command| !command.is_empty()))
        .or_else(|| editor.filter(|command| !command.is_empty()))
        .unwrap_or(if cfg!(windows) { "notepad" } else { "nano" })
        .to_owned()
}

#[cfg(all(test, unix))]
fn run_external_editor(command: &str, editor: &mut Editor) -> Result<()> {
    let parts = shell_words::split(command).map_err(|error| Error::Config(error.to_string()))?;
    let Some((program, arguments)) = parts.split_first() else {
        return Err(Error::Config("external editor is empty".into()));
    };
    let mut file = tempfile::Builder::new().suffix(".md").tempfile()?;
    file.write_all(editor.text.as_bytes())?;
    let status = std::process::Command::new(program)
        .args(arguments)
        .arg(file.path())
        .status()
        .map_err(|_| Error::Config(format!("Failed to launch external editor: {command}")))?;
    if !status.success() {
        let exit = status
            .code()
            .map_or_else(|| status.to_string(), |code| format!("code {code}"));
        return Err(Error::Config(format!(
            "External editor exited with {exit}; prompt unchanged"
        )));
    }
    let bytes = std::fs::read(file.path())?;
    editor.replace(external_editor_text(&bytes));
    Ok(())
}

fn external_editor_text(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let text = text.strip_prefix('\u{feff}').unwrap_or(&text);
    text.strip_suffix('\n').unwrap_or(text).to_owned()
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
            "/debug",
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

// Pi treats Ctrl+C as a timed pair, independent of agent activity and editor contents.
fn clear_editor_or_exit(view: &mut View, last_clear: &mut Option<Instant>, now: Instant) -> bool {
    if last_clear
        .is_some_and(|last| now.saturating_duration_since(last) < Duration::from_millis(500))
    {
        return true;
    }
    view.editor.take();
    view.draft_images.clear();
    *last_clear = Some(now);
    false
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    #[test]
    fn signal_listener_survives_interrupt_during_editor() {
        use std::sync::atomic::Ordering;
        use std::time::{Duration, Instant};
        const CHILD: &str = "PI_RS_EDITOR_SIGNAL_REARM_TEST";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "interactive::tests::signal_listener_survives_interrupt_during_editor",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        super::EXTERNAL_EDITOR_ACTIVE.store(true, Ordering::SeqCst);
        super::install_terminal_signal(std::sync::Arc::new(tokio::sync::Notify::new())).unwrap();
        unsafe {
            libc::kill(libc::getpid(), libc::SIGINT);
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while super::TERMINAL_SIGNAL_DELIVERIES.load(Ordering::SeqCst) < 1 {
            assert!(Instant::now() < deadline, "editor signal was not received");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!crate::process::TERMINAL_INTERRUPTED.load(Ordering::SeqCst));
        super::EXTERNAL_EDITOR_ACTIVE.store(false, Ordering::SeqCst);
        unsafe {
            libc::kill(libc::getpid(), libc::SIGINT);
        }
        while !crate::process::TERMINAL_INTERRUPTED.load(Ordering::SeqCst) {
            assert!(
                Instant::now() < deadline,
                "signal listener did not survive editor interrupt"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[cfg(unix)]
    #[test]
    fn editor_supervisor_does_not_hold_unrelated_pipes() {
        let mut pipe = [0; 2];
        assert_eq!(unsafe { libc::pipe(pipe.as_mut_ptr()) }, 0);
        let supervisor = super::EditorJobSupervisor::start(0, unsafe { libc::getpgrp() }).unwrap();
        unsafe {
            libc::close(pipe[1]);
        }
        let mut byte = 0u8;
        let mut poll = libc::pollfd {
            fd: pipe[0],
            events: libc::POLLIN | libc::POLLHUP,
            revents: 0,
        };
        let ready = unsafe { libc::poll(&mut poll, 1, 2000) };
        assert!(ready > 0, "supervisor retained unrelated pipe");
        assert_eq!(
            unsafe { libc::read(pipe[0], (&mut byte as *mut u8).cast(), 1) },
            0
        );
        unsafe {
            libc::close(pipe[0]);
        }
        drop(supervisor);
    }

    #[cfg(unix)]
    fn editor_pty_regression(test_name: &str, suspend: bool, shared_group: bool) {
        use std::io::{Read, Write};
        use std::os::fd::{AsRawFd, FromRawFd};
        use std::os::unix::{fs::PermissionsExt, process::CommandExt};
        use std::process::Stdio;
        use std::time::{Duration, Instant};
        const CHILD: &str = "PI_RS_EDITOR_JOB_CONTROL_TEST";
        const SUPERVISOR: &str = "PI_RS_EDITOR_TEST_SUPERVISOR";
        let managed = suspend && !shared_group && !test_name.contains("without_supervisor");
        let plain_launcher = test_name.contains("without_job_control_launcher");
        let background_case = test_name.contains("background_job_continues_workers");
        let worker_case = test_name.contains("pauses_owned_worker") || background_case;
        if let Some(program) = std::env::var_os(SUPERVISOR) {
            use std::os::unix::process::CommandExt;
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", test_name, "--nocapture"])
                .env_remove(SUPERVISOR)
                .env(CHILD, &program)
                .process_group(0)
                .spawn()
                .unwrap();
            super::set_foreground(0, child.id() as libc::pid_t).unwrap();
            if plain_launcher {
                assert!(child.wait().unwrap().success());
                println!("LAUNCHER_FINISHED");
                return;
            }
            let mut status = 0;
            assert_eq!(
                unsafe { libc::waitpid(child.id() as libc::pid_t, &mut status, libc::WUNTRACED) },
                child.id() as libc::pid_t
            );
            assert!(libc::WIFSTOPPED(status));
            println!("SUPERVISOR_SAW_STOP");
            if worker_case {
                let marker = std::path::PathBuf::from(&program).with_extension("worker");
                let before = std::fs::read(&marker).unwrap().len();
                std::thread::sleep(Duration::from_millis(500));
                assert_eq!(
                    std::fs::read(&marker).unwrap().len(),
                    before,
                    "worker advanced while Pi was stopped"
                );
            }
            if background_case {
                let shell_group = unsafe { libc::getpgrp() };
                super::set_foreground(0, shell_group).unwrap();
                assert_eq!(
                    unsafe { libc::kill(-(child.id() as libc::pid_t), libc::SIGCONT) },
                    0
                );
                let marker = std::path::PathBuf::from(&program).with_extension("worker");
                let before = std::fs::read(&marker).unwrap().len();
                std::thread::sleep(Duration::from_millis(500));
                assert!(
                    std::fs::read(&marker).unwrap().len() > before,
                    "worker did not continue while backgrounded"
                );
                assert_eq!(unsafe { libc::tcgetpgrp(0) }, shell_group);
            }
            if test_name.contains("supervisor_retains_terminal") {
                let shell_group = unsafe { libc::getpgrp() };
                super::set_foreground(0, shell_group).unwrap();
                std::thread::sleep(Duration::from_millis(1500));
                assert_eq!(unsafe { libc::tcgetpgrp(0) }, shell_group);
            }
            if test_name.contains("editor_exits_while_suspended") {
                let pid_file = std::path::PathBuf::from(&program).with_extension("pid");
                let pid: libc::pid_t = std::fs::read_to_string(pid_file)
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                assert_eq!(unsafe { libc::kill(-pid, libc::SIGKILL) }, 0);
            }
            super::set_foreground(0, child.id() as libc::pid_t).unwrap();
            assert_eq!(
                unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGCONT) },
                0
            );
            assert!(child.wait().unwrap().success());
            return;
        }
        if let Some(program) = std::env::var_os(CHILD) {
            let until = Instant::now() + Duration::from_secs(5);
            while unsafe { libc::tcgetpgrp(0) } != unsafe { libc::getpgrp() } {
                assert!(Instant::now() < until, "supervisor did not foreground Pi");
                std::thread::sleep(Duration::from_millis(2));
            }
            if shared_group {
                assert_eq!(unsafe { libc::getpgrp() }, unsafe {
                    libc::getpgid(libc::getppid())
                });
            }
            let mut baseline: libc::termios = unsafe { std::mem::zeroed() };
            assert_eq!(unsafe { libc::tcgetattr(0, &mut baseline) }, 0);
            let mut editor = super::RunningExternalEditor::start(
                program.to_str().unwrap(),
                "draft",
                super::ExternalEditorTarget::Main,
            )
            .unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while std::fs::read(editor.file.path()).unwrap() != b"started" {
                assert!(Instant::now() < deadline, "editor did not start");
                std::thread::sleep(Duration::from_millis(5));
            }
            let worker_marker = std::path::PathBuf::from(&program).with_extension("worker");
            let mut test_worker = if worker_case {
                let worker = std::process::Command::new("/bin/sh")
                    .args([
                        "-c",
                        "while :; do printf x >> \"$1\"; sleep 0.05; done",
                        "worker",
                    ])
                    .arg(&worker_marker)
                    .spawn()
                    .unwrap();
                while !worker_marker.exists() {
                    assert!(Instant::now() < deadline, "worker did not start");
                    std::thread::sleep(Duration::from_millis(5));
                }
                Some(worker)
            } else {
                None
            };
            if suspend {
                assert_eq!(
                    unsafe { libc::tcgetpgrp(0) },
                    editor.child.id() as libc::pid_t
                );
                // Consume any stop notification from initial foreground handoff.
                let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
                let rc = unsafe {
                    libc::waitid(
                        libc::P_PID,
                        editor.child.id() as libc::id_t,
                        &mut info,
                        libc::WSTOPPED | libc::WNOHANG,
                    )
                };
                assert_eq!(rc, 0);
                println!("READY_FOR_CTRL_Z");
                let go = std::path::PathBuf::from(&program).with_extension("go");
                while !go.exists() {
                    assert!(Instant::now() < deadline, "controller did not send Ctrl+Z");
                    std::thread::sleep(Duration::from_millis(2));
                }
                let mut announced = false;
                loop {
                    if editor.poll().unwrap().is_some() {
                        break;
                    }
                    if !announced
                        && super::EDITOR_JOB_RESUMES.load(std::sync::atomic::Ordering::SeqCst) > 0
                    {
                        assert_eq!(
                            unsafe { libc::tcgetpgrp(0) },
                            editor.child.id() as libc::pid_t
                        );
                        let mut resumed: libc::termios = unsafe { std::mem::zeroed() };
                        assert_eq!(unsafe { libc::tcgetattr(0, &mut resumed) }, 0);
                        assert_eq!(resumed.c_lflag & (libc::ECHO | libc::ICANON), 0);
                        if worker_case {
                            let before = std::fs::read(&worker_marker).unwrap().len();
                            while std::fs::read(&worker_marker).unwrap().len() <= before {
                                assert!(Instant::now() < deadline, "worker did not resume");
                                std::thread::sleep(Duration::from_millis(5));
                            }
                        }
                        println!("EDITOR_RESUMED");
                        announced = true;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "editor did not finish after resume"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                }
            } else {
                editor.cancel();
            }
            if let Some(worker) = &mut test_worker {
                let _ = worker.kill();
                let _ = worker.wait();
            }
            editor
                ._foreground
                .as_ref()
                .expect("PTY foreground")
                .restore()
                .expect("restore PTY foreground and original termios");
            drop(editor);
            let mut restored: libc::termios = unsafe { std::mem::zeroed() };
            assert_eq!(unsafe { libc::tcgetattr(0, &mut restored) }, 0);
            // Darwin sets PENDIN internally when canonical input is restored;
            // it is a pending-input state bit, not an editor terminal setting.
            #[cfg(target_vendor = "apple")]
            let (restored_flags, baseline_flags) = (
                restored.c_lflag & !libc::PENDIN,
                baseline.c_lflag & !libc::PENDIN,
            );
            #[cfg(not(target_vendor = "apple"))]
            let (restored_flags, baseline_flags) = (restored.c_lflag, baseline.c_lflag);
            assert_eq!(
                restored_flags, baseline_flags,
                "terminal local flags were not restored"
            );
            assert_eq!(
                restored.c_iflag, baseline.c_iflag,
                "terminal input flags were not restored"
            );
            assert_eq!(restored.c_oflag, baseline.c_oflag);
            assert_eq!(restored.c_cflag, baseline.c_cflag);
            assert_eq!(restored.c_cc, baseline.c_cc);
            println!("PARENT_FINISHED");
            return;
        }
        let directory = tempfile::tempdir().unwrap();
        let program = directory.path().join("editor");
        std::fs::write(&program, "#!/bin/sh\nstty -echo -icanon\nprintf '%s' \"$$\" > \"${0}.pid\"\nprintf started > \"$1\"\nprintf 'EDITOR_READY\\n'\nexec sleep 30\n").unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut master = -1;
        let mut slave = -1;
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut master,
                    &mut slave,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            },
            0
        );
        let mut master = unsafe { std::fs::File::from_raw_fd(master) };
        let slave = unsafe { std::fs::File::from_raw_fd(slave) };
        let executable = std::env::current_exe().unwrap();
        let mut command = if shared_group {
            let mut command = std::process::Command::new("/bin/sh");
            command
                .arg("-c")
                .arg("\"$@\"; rc=$?; printf 'SHELL_ALIVE\\n'; exit $rc")
                .arg("fixture-shell")
                .arg(&executable);
            command
        } else {
            std::process::Command::new(&executable)
        };
        command
            .args(["--exact", test_name, "--nocapture"])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave));
        if managed {
            command.env(SUPERVISOR, &program);
        } else {
            command.env(CHILD, &program);
        }
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY as libc::c_ulong, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn().unwrap();
        unsafe {
            libc::fcntl(master.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK);
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut output = Vec::new();
        let mut sent = false;
        let mut resumed = false;
        loop {
            let mut bytes = [0; 1024];
            if let Ok(n) = master.read(&mut bytes) {
                output.extend_from_slice(&bytes[..n]);
            }
            if suspend
                && !sent
                && output
                    .windows(b"READY_FOR_CTRL_Z".len())
                    .any(|part| part == b"READY_FOR_CTRL_Z")
            {
                master.write_all(&[26]).unwrap();
                std::fs::write(program.with_extension("go"), "go").unwrap();
                sent = true;
            }
            if suspend && sent && !resumed {
                let mut status = 0;
                let rc = unsafe {
                    libc::waitpid(
                        child.id() as libc::pid_t,
                        &mut status,
                        libc::WUNTRACED | libc::WNOHANG,
                    )
                };
                if shared_group && rc > 0 && libc::WIFSTOPPED(status) {
                    unsafe {
                        libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL);
                    }
                    let _ = child.wait();
                    panic!("shell was stopped along with the editor");
                }
                if rc == 0 {
                    // The supervisor resumes a managed job; unsupervised and
                    // shared-group cases must resume without external SIGCONT.
                    // Wait for the child to confirm it restored editor foreground.
                    let until = Instant::now() + Duration::from_secs(8);
                    let expected = if test_name.contains("editor_exits_while_suspended") {
                        b"PARENT_FINISHED".as_slice()
                    } else {
                        b"EDITOR_RESUMED".as_slice()
                    };
                    while !output.windows(expected.len()).any(|part| part == expected) {
                        let mut bytes = [0; 1024];
                        if let Ok(n) = master.read(&mut bytes) {
                            output.extend_from_slice(&bytes[..n]);
                        }
                        assert!(
                            Instant::now() < until,
                            "editor was not foregrounded after resume: {}",
                            String::from_utf8_lossy(&output)
                        );
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    if !test_name.contains("editor_exits_while_suspended") {
                        master.write_all(&[3]).unwrap();
                    }
                    resumed = true;
                } else if rc > 0 {
                    panic!(
                        "child exited before stopping: {}",
                        String::from_utf8_lossy(&output)
                    );
                }
            }
            if let Some(status) = child.try_wait().unwrap() {
                let mut bytes = [0; 1024];
                if let Ok(n) = master.read(&mut bytes) {
                    output.extend_from_slice(&bytes[..n]);
                }
                assert!(
                    status.success() && (!suspend || resumed),
                    "{}",
                    String::from_utf8_lossy(&output)
                );
                assert!(String::from_utf8_lossy(&output).contains("PARENT_FINISHED"));
                if managed {
                    assert!(
                        String::from_utf8_lossy(&output).contains(if plain_launcher {
                            "LAUNCHER_FINISHED"
                        } else {
                            "SUPERVISOR_SAW_STOP"
                        })
                    );
                }
                if shared_group {
                    assert!(String::from_utf8_lossy(&output).contains("SHELL_ALIVE"));
                }
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("editor PTY timed out: {}", String::from_utf8_lossy(&output));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[cfg(unix)]
    #[test]
    fn stopped_editor_suspends_and_resumes_parent_job() {
        editor_pty_regression(
            "interactive::tests::stopped_editor_suspends_and_resumes_parent_job",
            true,
            false,
        );
    }

    #[cfg(unix)]
    #[test]
    fn editor_suspend_keeps_shared_group_shell_running() {
        editor_pty_regression(
            "interactive::tests::editor_suspend_keeps_shared_group_shell_running",
            true,
            true,
        );
    }

    #[cfg(unix)]
    #[test]
    fn editor_suspend_without_supervisor_resumes_without_sigcont() {
        editor_pty_regression(
            "interactive::tests::editor_suspend_without_supervisor_resumes_without_sigcont",
            true,
            false,
        );
    }

    #[cfg(unix)]
    #[test]
    fn stopped_editor_pauses_owned_worker_until_resume() {
        editor_pty_regression(
            "interactive::tests::stopped_editor_pauses_owned_worker_until_resume",
            true,
            false,
        );
    }

    #[cfg(unix)]
    #[test]
    fn editor_background_job_continues_workers_until_foreground() {
        editor_pty_regression(
            "interactive::tests::editor_background_job_continues_workers_until_foreground",
            true,
            false,
        );
    }

    #[cfg(unix)]
    #[test]
    fn editor_supervisor_retains_terminal_until_foreground_resume() {
        editor_pty_regression(
            "interactive::tests::editor_supervisor_retains_terminal_until_foreground_resume",
            true,
            false,
        );
    }

    #[cfg(unix)]
    #[test]
    fn editor_suspend_without_job_control_launcher_resumes() {
        editor_pty_regression(
            "interactive::tests::editor_suspend_without_job_control_launcher_resumes",
            true,
            false,
        );
    }

    #[cfg(unix)]
    #[test]
    fn editor_exits_while_suspended_without_stopping_tui() {
        editor_pty_regression(
            "interactive::tests::editor_exits_while_suspended_without_stopping_tui",
            true,
            false,
        );
    }

    #[cfg(windows)]
    #[test]
    fn cancelling_external_editor_kills_wrapper_descendants() {
        use std::time::{Duration, Instant};
        let directory = tempfile::tempdir().unwrap();
        let wrapper = directory.path().join("editor.cmd");
        let marker = directory.path().join("descendant-finished");
        let ready = directory.path().join("wrapper-ready");
        std::fs::write(&wrapper, format!(
            "@echo off\r\nstart \"\" /b powershell.exe -NoProfile -NonInteractive -Command \"Start-Sleep -Seconds 3; Set-Content -LiteralPath '{}' -Value leaked\"\r\necho edited>\"%~1\"\r\necho ready>\"{}\"\r\npowershell.exe -NoProfile -NonInteractive -Command \"Start-Sleep -Seconds 30\"\r\n",
            marker.display().to_string().replace('\'', "''"),
            ready.display(),
        )).unwrap();
        let mut editor = super::RunningExternalEditor::start(
            &format!("\"{}\"", wrapper.display()),
            "draft",
            super::ExternalEditorTarget::Main,
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.exists() {
            assert!(Instant::now() < deadline, "wrapper did not start");
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            std::fs::read_to_string(editor.file.path())
                .unwrap()
                .contains("edited")
        );
        editor.cancel();
        drop(editor);
        std::thread::sleep(Duration::from_secs(4));
        assert!(!marker.exists(), "wrapper descendant outlived cancellation");
    }

    #[cfg(windows)]
    #[test]
    fn exiting_external_editor_wrapper_does_not_leave_descendants() {
        use std::time::{Duration, Instant};
        let directory = tempfile::tempdir().unwrap();
        let wrapper = directory.path().join("editor.cmd");
        let marker = directory.path().join("descendant-finished");
        std::fs::write(&wrapper, format!(
            "@echo off\r\nstart \"\" /b powershell.exe -NoProfile -NonInteractive -Command \"Start-Sleep -Seconds 3; Set-Content -LiteralPath '{}' -Value leaked\"\r\necho edited>\"%~1\"\r\nexit /b 0\r\n",
            marker.display().to_string().replace('\'', "''"),
        )).unwrap();
        let mut editor = super::RunningExternalEditor::start(
            &format!("\"{}\"", wrapper.display()),
            "draft",
            super::ExternalEditorTarget::Main,
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while editor.child.try_wait().unwrap().is_none() {
            assert!(Instant::now() < deadline, "wrapper did not exit");
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            std::fs::read_to_string(editor.file.path())
                .unwrap()
                .contains("edited")
        );
        drop(editor);
        std::thread::sleep(Duration::from_secs(4));
        assert!(!marker.exists(), "descendant survived its wrapper");
    }

    #[cfg(unix)]
    #[test]
    fn cancelled_editor_restores_original_termios() {
        editor_pty_regression(
            "interactive::tests::cancelled_editor_restores_original_termios",
            false,
            false,
        );
    }

    #[cfg(unix)]
    #[test]
    fn foreground_editor_receives_interrupt_without_killing_parent() {
        use std::io::{Read, Write};
        use std::os::fd::{AsRawFd, FromRawFd};
        use std::os::unix::{fs::PermissionsExt, process::CommandExt};
        use std::process::Stdio;
        const CHILD: &str = "PI_RS_FOREGROUND_EDITOR_TEST";
        if let Some(program) = std::env::var_os(CHILD) {
            let mut editor = super::RunningExternalEditor::start(
                program.to_str().unwrap(),
                "draft",
                super::ExternalEditorTarget::Main,
            )
            .unwrap();
            let status = editor.child.wait().unwrap();
            assert!(!status.success());
            drop(editor);
            println!("PARENT_SURVIVED");
            return;
        }
        let directory = tempfile::tempdir().unwrap();
        let program = directory.path().join("editor");
        std::fs::write(
            &program,
            "#!/bin/sh\ntrap 'exit 130' INT\nprintf 'EDITOR_READY\\n'\nwhile :; do sleep 1; done\n",
        )
        .unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut master = -1;
        let mut slave = -1;
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut master,
                    &mut slave,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            },
            0
        );
        let mut master = unsafe { std::fs::File::from_raw_fd(master) };
        let slave = unsafe { std::fs::File::from_raw_fd(slave) };
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "interactive::tests::foreground_editor_receives_interrupt_without_killing_parent",
                "--nocapture",
            ])
            .env(CHILD, &program)
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave));
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY as libc::c_ulong, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn().unwrap();
        unsafe {
            libc::fcntl(master.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK);
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut output = Vec::new();
        let mut sent = false;
        let status = loop {
            let mut bytes = [0; 1024];
            if let Ok(n) = master.read(&mut bytes) {
                output.extend_from_slice(&bytes[..n]);
            }
            if !sent
                && output
                    .windows(b"EDITOR_READY".len())
                    .any(|part| part == b"EDITOR_READY")
            {
                master.write_all(&[3]).unwrap();
                sent = true;
            }
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "editor test timed out: {}",
                    String::from_utf8_lossy(&output)
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        let mut bytes = [0; 1024];
        if let Ok(n) = master.read(&mut bytes) {
            output.extend_from_slice(&bytes[..n]);
        }
        assert!(
            sent && status.success(),
            "{}",
            String::from_utf8_lossy(&output)
        );
        assert!(String::from_utf8_lossy(&output).contains("PARENT_SURVIVED"));
    }
    #[cfg(unix)]
    #[test]
    fn running_external_editor_is_cancelled_only_by_its_request() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let program = directory.path().join("editor");
        std::fs::write(
            &program,
            "#!/bin/sh\nprintf started > \"$1\"\nexec sleep 30\n",
        )
        .unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
        let id = uuid::Uuid::new_v4();
        let mut editor = super::RunningExternalEditor::start(
            program.to_str().unwrap(),
            "draft",
            super::ExternalEditorTarget::Extension(id),
        )
        .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::fs::read(editor.file.path()).unwrap() != b"started" {
            assert!(std::time::Instant::now() < deadline, "editor did not start");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(!editor.cancelled_by(
            &serde_json::json!({"type":"extension_ui_cancelled", "id":uuid::Uuid::new_v4()})
        ));
        assert!(editor.child.try_wait().unwrap().is_none());
        assert!(
            editor.cancelled_by(&serde_json::json!({"type":"extension_ui_cancelled", "id":id}))
        );
        let started = std::time::Instant::now();
        editor.cancel();
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        assert!(editor.child.try_wait().unwrap().is_some());
    }
    #[cfg(unix)]
    #[test]
    fn worker_exit_cancels_only_extension_editor_and_removes_tempfile() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let program = directory.path().join("editor");
        std::fs::write(&program, "#!/bin/sh\nexec sleep 30\n").unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
        let id = uuid::Uuid::new_v4();
        let editor = super::RunningExternalEditor::start(
            program.to_str().unwrap(),
            "draft",
            super::ExternalEditorTarget::Extension(id),
        )
        .unwrap();
        let pid = editor.child.id() as libc::pid_t;
        let path = editor.file.path().to_owned();
        let mut running = Some(editor);
        assert!(
            super::cancel_running_extension_editor(
                &mut running,
                Some(
                    &serde_json::json!({"type":"extension_ui_cancelled", "id":uuid::Uuid::new_v4()})
                ),
            )
            .is_none()
        );
        assert!(running.is_some());
        assert_eq!(
            super::cancel_running_extension_editor(&mut running, None),
            Some(super::ExternalEditorTarget::Extension(id))
        );
        assert!(running.is_none());
        assert!(!path.exists());
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );

        let main = super::RunningExternalEditor::start(
            program.to_str().unwrap(),
            "draft",
            super::ExternalEditorTarget::Main,
        )
        .unwrap();
        let mut running = Some(main);
        assert!(super::cancel_running_extension_editor(&mut running, None).is_none());
        assert!(running.is_some());
    }

    #[test]
    fn external_editor_selects_first_nonempty_command() {
        assert_eq!(
            super::external_editor_command(
                Some("  \u{feff}"),
                Some("visual --wait"),
                Some("editor")
            ),
            "visual --wait"
        );
        assert_eq!(
            super::external_editor_command(
                Some("configured --wait"),
                Some("visual"),
                Some("editor")
            ),
            "configured --wait"
        );
        assert_eq!(
            super::external_editor_command(None, Some(""), Some("editor")),
            "editor"
        );
        assert_eq!(
            super::external_editor_command(None, Some(""), Some("")),
            if cfg!(windows) { "notepad" } else { "nano" }
        );
        assert_eq!(
            super::external_editor_command(Some("\u{0085}"), None, None),
            "\u{0085}"
        );
    }

    #[test]
    fn external_editor_strips_one_bom_and_one_final_lf() {
        assert_eq!(
            super::external_editor_text(b"\xef\xbb\xbfprompt\n\n"),
            "prompt\n"
        );
        assert_eq!(super::external_editor_text(b"prompt\r\n"), "prompt\r");
        assert_eq!(
            super::external_editor_text(b"\xef\xbb\xbf\xef\xbb\xbftext"),
            "\u{feff}text"
        );
        assert_eq!(super::external_editor_text(b"bad\xff\n"), "bad\u{fffd}");
        assert_eq!(super::external_editor_text(b""), "");
    }

    #[test]
    fn external_editor_dialog_cancellation_removes_only_correlated_request() {
        let id = uuid::Uuid::new_v4();
        let next = uuid::Uuid::new_v4();
        let mut view = super::View::default();
        view.ui_request(&json!({"type":"extension_ui_request","id":id,"method":"editor","title":"Edit","prefill":"original"})).unwrap();
        view.ui_request(
            &json!({"type":"extension_ui_request","id":next,"method":"editor","title":"Next"}),
        )
        .unwrap();
        view.dialog_mut().unwrap().external_edit(|draft| {
            draft.replace("edited".into());
            Ok(())
        });
        view.cancel_extension_dialog(&json!(id));
        assert_eq!(view.dialogs.len(), 1);
        assert_eq!(view.dialogs.front().unwrap().id, next);
    }

    #[cfg(unix)]
    #[test]
    fn external_editor_round_trip_updates_only_after_success() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let program = dir.path().join("editor");
        std::fs::write(
            &program,
            "#!/bin/sh\nprintf '\\357\\273\\277edited\\n\\n' > \"$1\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut editor = super::Editor::default();
        editor.replace("original".into());
        super::run_external_editor(program.to_str().unwrap(), &mut editor).unwrap();
        assert_eq!(editor.text, "edited\n");
        assert_eq!(editor.cursor, editor.text.len());
        std::fs::write(&program, "#!/bin/sh\nprintf corrupted > \"$1\"\nexit 1\n").unwrap();
        assert!(super::run_external_editor(program.to_str().unwrap(), &mut editor).is_err());
        assert_eq!(editor.text, "edited\n");
    }

    #[cfg(unix)]
    #[test]
    fn external_editor_round_trip_from_extension_dialog() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let program = dir.path().join("editor");
        std::fs::write(
            &program,
            "#!/bin/sh\nprintf '\\357\\273\\277dialog\\n' > \"$1\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut dialog = super::super::ui::dialog::Dialog::new(
            uuid::Uuid::new_v4(),
            pi_rs::UiRequest::Editor {
                title: "Edit".into(),
                initial_value: Some("initial".into()),
                timeout: None,
            },
        );
        dialog
            .external_edit(|editor| super::run_external_editor(program.to_str().unwrap(), editor));
        std::fs::write(&program, "#!/bin/sh\nprintf bad > \"$1\"\nexit 1\n").unwrap();
        dialog
            .external_edit(|editor| super::run_external_editor(program.to_str().unwrap(), editor));
        let keys = super::Keys::default();
        assert_eq!(
            dialog.input(
                KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL),
                &keys
            ),
            Some(pi_rs::UiResponse::Text("dialog".into()))
        );
        assert_eq!(
            dialog.input(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), &keys),
            Some(pi_rs::UiResponse::Cancelled)
        );
    }

    #[test]
    fn live_frame_flushes_text_before_console_side_effects() {
        use std::io::Write;
        let output = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        struct Sink(std::rc::Rc<std::cell::RefCell<Vec<u8>>>);
        impl Write for Sink {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.borrow_mut().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut frame = super::FrameWriter::new(Sink(output.clone()), false, true).unwrap();
        write!(frame, "preceding text").unwrap();
        // Crossterm flushes the writer immediately before a WinAPI command.
        frame.flush().unwrap();
        assert!(output.borrow().ends_with(b"preceding text"));
        frame.finish().unwrap();
        assert!(output.borrow().ends_with(b"\x1b[?2026l"));
    }

    #[test]
    fn frame_without_ansi_never_emits_synchronization_sequences() {
        use std::io::Write;
        let mut output = Vec::new();
        {
            let mut frame = super::FrameWriter::new(&mut output, false, false).unwrap();
            write!(frame, "first").unwrap();
            frame.flush().unwrap();
            write!(frame, "second").unwrap();
            frame.finish().unwrap();
        }
        assert_eq!(output, b"firstsecond");
        {
            let mut frame = super::FrameWriter::new(&mut output, false, false).unwrap();
            write!(frame, "aborted").unwrap();
        }
        assert_eq!(output, b"firstsecondaborted");
    }

    #[test]
    fn failed_frame_write_attempts_to_end_synchronized_update() {
        use std::io::Write;
        for buffered in [false, true] {
            let output = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
            struct FailOnce {
                output: std::rc::Rc<std::cell::RefCell<Vec<u8>>>,
                remaining: usize,
                failed: bool,
            }
            impl Write for FailOnce {
                fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                    if self.remaining == 0 && !self.failed {
                        self.failed = true;
                        return Err(std::io::Error::other("injected write failure"));
                    }
                    let count = if self.failed {
                        bytes.len()
                    } else {
                        bytes.len().min(self.remaining)
                    };
                    self.output.borrow_mut().extend_from_slice(&bytes[..count]);
                    self.remaining = self.remaining.saturating_sub(count);
                    Ok(count)
                }
                fn flush(&mut self) -> std::io::Result<()> {
                    Ok(())
                }
            }
            let mut frame = super::FrameWriter::new(
                FailOnce {
                    output: output.clone(),
                    remaining: 10,
                    failed: false,
                },
                buffered,
                true,
            )
            .unwrap();
            let result = write!(frame, "some frame content").and_then(|()| frame.finish());
            assert!(result.is_err());
            let bytes = output.borrow();
            assert!(bytes.starts_with(b"\x1b[?2026h"));
            assert!(bytes.ends_with(b"\x1b[?2026l"));
        }
    }

    #[test]
    fn cancelled_logout_reload_keeps_removed_key_unavailable() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("auth.json");
        std::fs::write(&path, "{}").unwrap();
        let held = super::super::auth_lock::acquire(&path).unwrap();
        let endpoint = super::super::Provider::named("openrouter")
            .unwrap()
            .default_endpoint();
        let mut keys = super::super::model_auth::Keys::parse(&serde_json::json!({
            "openrouter": {"type":"oauth", "access":"old", "endpoint":endpoint}
        }));
        assert!(
            keys.operation_credential("openrouter", endpoint, true)
                .is_some()
        );
        keys.forget("openrouter");
        let reload = super::AuthReload::start_path(keys.revision().to_owned(), path);
        assert!(
            reload
                .receiver
                .recv_timeout(std::time::Duration::from_millis(150))
                .is_err()
        );
        drop(reload);
        drop(held);
        assert!(
            keys.operation_credential("openrouter", endpoint, true)
                .is_none()
        );
    }

    fn chatgpt_logout_fixture(directory: &std::path::Path) {
        let credential = pi_rs::openai::oauth::Credential {
            client_id: "client".into(),
            subject: "subject".into(),
            email: None,
            id_token: "id".into(),
            access_token: "access".into(),
            refresh_token: "refresh".into(),
            expires_at: 4_000_000_000,
            scopes: vec!["chatgpt.tokens.use.direct".into()],
        };
        std::fs::write(
            directory.join("auth.json"),
            serde_json::json!({"openai-chatgpt":{"type":"oauth","credential":credential}})
                .to_string(),
        )
        .unwrap();
        std::fs::write(
            directory.join("chatgpt-sessions.json"),
            serde_json::json!({"client":credential}).to_string(),
        )
        .unwrap();
    }

    #[test]
    fn device_code_action_is_one_shot_and_cancelled_with_its_task() {
        let keys = super::Keys::default();
        let enter = crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Enter,
            crossterm::event::KeyModifiers::NONE,
        );
        let other = crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('x'),
            crossterm::event::KeyModifiers::NONE,
        );
        let make_task = || {
            super::RemoteTask::start("Signing in to xAI", "xAI login cancelled", |_| async {
                std::future::pending::<pi_rs::Result<Vec<String>>>().await
            })
        };
        let mut task = make_task();
        task.device_code = Some(("https://auth.x.ai/activate".into(), "CODE".into()));
        let mut view = super::View {
            remote_task: Some(task),
            ..Default::default()
        };
        assert!(super::take_device_code_action(&mut view, other, &keys).is_none());
        let (url, code) = super::take_device_code_action(&mut view, enter, &keys).unwrap();
        assert_eq!(url, "https://auth.x.ai/activate");
        assert_eq!(code, "CODE");
        assert!(super::take_device_code_action(&mut view, enter, &keys).is_none());
        let (sender, receiver) = std::sync::mpsc::channel();
        view.remote_task.as_mut().unwrap().device_copy = Some(receiver);
        super::interrupt_remote_task(&mut view);
        assert!(view.remote_task.is_none());
        assert!(
            sender.send(true).is_err(),
            "late clipboard results must not reach another login"
        );
        assert!(super::take_device_code_action(&mut view, enter, &keys).is_none());

        let mut task = make_task();
        task.device_code = Some(("https://auth.x.ai/activate".into(), "OTHER".into()));
        view.remote_task = Some(task);
        super::interrupt_remote_task(&mut view);
        assert!(
            super::take_device_code_action(&mut view, enter, &keys).is_none(),
            "cancellation discards an unused action"
        );
    }

    #[tokio::test]
    async fn paused_device_side_effects_stop_on_cancel_and_login_replacement() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let make_task = || {
            super::RemoteTask::start("Signing in to xAI", "xAI login cancelled", |_| async {
                std::future::pending::<pi_rs::Result<Vec<String>>>().await
            })
        };
        for replace in [false, true] {
            let mut view = super::View {
                remote_task: Some(make_task()),
                ..Default::default()
            };
            let cancel = view.remote_task.as_ref().unwrap().device_cancel.clone();
            let effects = Arc::new(AtomicUsize::new(0));
            let mut releases = Vec::new();
            let mut started = Vec::new();
            let mut jobs = Vec::new();
            for _ in 0..2 {
                let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                let (release_tx, release_rx) = tokio::sync::oneshot::channel();
                releases.push(release_tx);
                started.push(started_rx);
                let effects = effects.clone();
                jobs.push(tokio::spawn(super::run_device_side_effect(
                    cancel.clone(),
                    async move {
                        let _ = started_tx.send(());
                        let _ = release_rx.await;
                        effects.fetch_add(1, Ordering::SeqCst);
                    },
                )));
            }
            for ready in started {
                ready.await.unwrap();
            }
            if replace {
                view.remote_task = Some(make_task());
            } else {
                super::interrupt_remote_task(&mut view);
            }
            for release in releases {
                let _ = release.send(());
            }
            for job in jobs {
                assert!(job.await.unwrap().is_none());
            }
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            if replace {
                assert!(view.remote_task.is_some());
            }
        }
    }

    #[test]
    fn blocked_meta_login_commit_remains_observed_after_escape() {
        use super::super::meta_auth::{Credential, LoginControl};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("auth.json");
        std::fs::write(&path, "{}").unwrap();
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_extension("lock"))
            .unwrap();
        lock.lock().unwrap();
        let control = LoginControl::default();
        let worker_control = control.clone();
        let worker_path = path.clone();
        let mut task = super::RemoteTask::start(
            "Signing in to Meta",
            "Meta login cancelled",
            move |_| async move {
                super::super::meta_auth::save_login(
                    &worker_path,
                    None,
                    Credential {
                        kind: "oauth".into(),
                        access: "key".into(),
                        refresh: "identity".into(),
                        expires: 1,
                    },
                    worker_control,
                )
                .await?;
                Ok(vec!["Signed in to Meta".into()])
            },
        );
        task.meta_login_control = Some(control.clone());
        let mut view = super::View {
            remote_task: Some(task),
            ..Default::default()
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !control.is_committing() {
            assert!(std::time::Instant::now() < deadline, "commit never started");
            std::thread::yield_now();
        }
        super::interrupt_remote_task(&mut view);
        let task = view
            .remote_task
            .take()
            .expect("commit must remain observed");
        assert!(
            task.receiver.try_recv().is_err(),
            "save must still be blocked"
        );
        lock.unlock().unwrap();
        assert!(matches!(
            task.receiver
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap(),
            super::RemoteEvent::Finished(Ok(_))
        ));
        let auth: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(auth["meta"]["access"], "key");
    }

    #[test]
    fn committed_chatgpt_logout_remains_observed_after_escape() {
        for success in [true, false] {
            let directory = tempfile::tempdir().unwrap();
            chatgpt_logout_fixture(directory.path());
            let path = directory.path().to_owned();
            let lock = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path.join("chatgpt-refresh.lock"))
                .unwrap();
            lock.lock().unwrap();
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let (cleanup_tx, cleanup_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let session_lock_path = path.join("chatgpt-sessions.lock");
            let control = super::super::chatgpt_auth::LogoutControl::default();
            let task = super::chatgpt_logout_task(control, move |control| async move {
                super::super::chatgpt_auth::logout_with_cleanup_hook(
                    path,
                    lock,
                    control,
                    move |_| async move {
                        started_tx.send(()).unwrap();
                        release_rx.await.unwrap();
                        if success {
                            Ok(())
                        } else {
                            Err(pi_rs::Error::Config("fixture failure".into()))
                        }
                    },
                    move || {
                        let probe = std::fs::OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(session_lock_path)
                            .unwrap();
                        let blocked =
                            matches!(probe.try_lock(), Err(std::fs::TryLockError::WouldBlock));
                        cleanup_tx.send(blocked).unwrap();
                    },
                )
                .await
            });
            let mut view = super::View {
                remote_task: Some(task),
                ..Default::default()
            };
            started_rx
                .recv_timeout(std::time::Duration::from_secs(3))
                .unwrap();
            super::interrupt_remote_task(&mut view);
            assert!(
                view.remote_task.is_some(),
                "committed logout must remain observed"
            );
            // Hold the session-store lock so the post-revocation cleanup is
            // deterministically still in progress when the gate is released.
            let session_lock = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(directory.path().join("chatgpt-sessions.lock"))
                .unwrap();
            session_lock.lock().unwrap();
            release_tx.send(()).unwrap();
            assert!(
                cleanup_rx
                    .recv_timeout(std::time::Duration::from_secs(3))
                    .unwrap(),
                "cleanup did not encounter the held session-store lock"
            );
            // `started_rx` means logout already owns the auth lock. Reading
            // through that lock waits for durable local removal, keeping slow
            // filesystem syncs outside the event-delivery deadline below.
            assert!(matches!(
                view.remote_task.as_ref().unwrap().receiver.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ));
            drop(session_lock);
            let auth =
                super::super::config::read_object(&directory.path().join("auth.json")).unwrap();
            assert!(auth.get("openai-chatgpt").is_none());
            let task = view.remote_task.take().unwrap();
            assert!(matches!(
                task.receiver
                    .recv_timeout(std::time::Duration::from_secs(3))
                    .unwrap(),
                super::RemoteEvent::ForgetModelKey("openai-chatgpt")
            ));
            assert!(matches!(
                task.receiver
                    .recv_timeout(std::time::Duration::from_secs(3))
                    .unwrap(),
                super::RemoteEvent::RefreshModelKeys
            ));
            match task
                .receiver
                .recv_timeout(std::time::Duration::from_secs(3))
                .unwrap()
            {
                super::RemoteEvent::Finished(Ok(lines)) if success => {
                    assert!(lines.iter().any(|line| line.contains("Signed out")))
                }
                super::RemoteEvent::Finished(Err(error)) if !success => assert!(
                    error
                        .to_string()
                        .contains("remote revocation was not confirmed")
                ),
                _ => panic!("logout did not report its final result"),
            }
            let auth =
                super::super::config::read_object(&directory.path().join("auth.json")).unwrap();
            assert!(auth.get("openai-chatgpt").is_none());
        }
    }

    #[test]
    fn uncommitted_chatgpt_logout_is_cancelled_by_escape() {
        let directory = tempfile::tempdir().unwrap();
        chatgpt_logout_fixture(directory.path());
        let path = directory.path().to_owned();
        let shared = super::super::auth_lock::acquire(&path.join("auth.json")).unwrap();
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.join("chatgpt-refresh.lock"))
            .unwrap();
        lock.lock().unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let control = super::super::chatgpt_auth::LogoutControl::default();
        let task = super::chatgpt_logout_task(control, move |control| async move {
            super::super::chatgpt_auth::logout_with(path, lock, control, move |_| async move {
                let _ = started_tx.send(());
                Ok(())
            })
            .await
        });
        let mut view = super::View {
            remote_task: Some(task),
            ..Default::default()
        };
        super::interrupt_remote_task(&mut view);
        assert!(view.remote_task.is_none());
        drop(shared);
        assert!(
            started_rx
                .recv_timeout(std::time::Duration::from_secs(3))
                .is_err()
        );
        let auth = super::super::config::read_object(&directory.path().join("auth.json")).unwrap();
        assert!(auth.get("openai-chatgpt").is_some());
    }

    #[test]
    fn pending_share_keeps_extension_dialog_and_response_correlated() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let share_id = uuid::Uuid::new_v4();
        let ui_id = uuid::Uuid::new_v4();
        let mut view = super::View {
            pending_share: Some(super::PendingShare {
                id: share_id,
                destination: super::sessions::ShareDestination::Gist,
                path: Default::default(),
                workspace: Default::default(),
                started: std::time::Instant::now(),
            }),
            ..Default::default()
        };
        assert!(view.share_blocks_input());
        assert!(view.take_share_response(&json!({"type":"response","command":"get_share_context","id":uuid::Uuid::new_v4(),"success":true})).is_none());
        assert!(view.ui_request(&json!({"type":"extension_ui_request","id":ui_id,"method":"input","title":"Startup question"})).unwrap().is_none());
        assert!(!view.share_blocks_input());
        let dialog = view.dialogs.front_mut().unwrap();
        assert_eq!(dialog.id, ui_id);
        dialog.paste("answer");
        let response = dialog.input(
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
            &super::Keys::default(),
        );
        assert!(matches!(response, Some(pi_rs::UiResponse::Text(value)) if value == "answer"));
        view.dialogs.pop_front();
        assert!(view.share_blocks_input());
        let (pending, snapshot) = view.take_share_response(&json!({"type":"response","command":"get_share_context","id":share_id,"success":false})).unwrap();
        assert_eq!(pending.id, share_id);
        assert!(snapshot.is_none());
        assert!(view.pending_share.is_none());
    }

    #[test]
    fn queued_escape_wins_over_ready_new_session() {
        use clap::Parser;
        use std::collections::VecDeque;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("new.jsonl");
        let mut next = super::Cli::parse_from(["pi"]);
        next.session = path.clone();
        next.session_transition = Some(pi_rs::SessionStartReason::New);
        let (sender, receiver) = std::sync::mpsc::channel();
        sender.send(Ok::<_, pi_rs::Error>(next)).unwrap();
        let ready = receiver.try_recv().unwrap().unwrap();
        let mut input = VecDeque::from([crate::ui::input::InputEvent::Event(
            crossterm::event::Event::Key(crossterm::event::KeyEvent::new(
                crossterm::event::KeyCode::Esc,
                crossterm::event::KeyModifiers::NONE,
            )),
        )]);
        let mut preserved = VecDeque::new();
        let cancelled =
            super::queued_transition_cancel(&mut preserved, |_| Ok(input.pop_front())).unwrap();
        if !cancelled {
            super::apply_session(
                &mut super::Cli::parse_from(["pi"]),
                &mut super::View::default(),
                ready,
            )
            .unwrap();
        }
        assert!(cancelled);
        assert!(!path.exists());
    }

    #[test]
    fn cancelled_new_session_does_not_create_a_journal_after_discovery() {
        use clap::Parser;
        use std::io::{Read, Write};
        use std::time::Duration;
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("work");
        std::fs::create_dir_all(workspace.join(".pi")).unwrap();
        std::fs::write(
            workspace.join(".pi/settings.json"),
            r#"{"enabledModels":["ollama/dynamic"]}"#,
        )
        .unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let (accepted, ready) = std::sync::mpsc::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = [0; 2048];
            let _ = socket.read(&mut buf).unwrap();
            accepted.send(()).unwrap();
            gate.recv_timeout(Duration::from_secs(5)).unwrap();
            let body = r#"{"data":[{"id":"dynamic"}]}"#;
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let mut args = super::Cli::parse_from(["pi", "--offline"]);
        args.workspace = workspace.clone();
        args.session = root.path().join("current.jsonl");
        args.session_dir = Some(root.path().join("sessions"));
        args.trust = true;
        args.settings = super::super::config::Settings::load(&workspace, true).unwrap();
        args.model_profiles = super::super::model_profiles::Profiles::parse(&serde_json::json!({"providers":{"ollama":{"baseUrl":endpoint,"models":[{"id":"alpha"}]}}})).unwrap();
        args.endpoint = args
            .model_profiles
            .endpoint(&super::super::Provider::Ollama)
            .into();
        args.model = "alpha".into();
        args.remember_initial_selection();
        args.selection_discovery_pending = true;
        let mut view = super::View {
            async_transitions: true,
            ..Default::default()
        };
        super::slash("/new", &mut args, &mut view, None).unwrap();
        ready.recv_timeout(Duration::from_secs(3)).unwrap();
        let cancelled = view.pending_transition.take().unwrap();
        release.send(()).unwrap();
        let prepared = cancelled
            .recv_timeout(Duration::from_secs(3))
            .unwrap()
            .unwrap();
        assert!(!prepared.session.exists());
        assert!(!root.path().join("sessions").exists());
        assert_eq!(args.session, root.path().join("current.jsonl"));
        server.join().unwrap();
    }

    #[test]
    fn interactive_workspace_switch_prepares_in_background_before_applying_scope() {
        use clap::Parser;
        use std::io::{Read, Write};
        use std::time::Duration;
        let root = tempfile::tempdir().unwrap();
        let launch = root.path().join("launch");
        let target = root.path().join("target");
        std::fs::create_dir_all(&launch).unwrap();
        std::fs::create_dir_all(target.join(".pi")).unwrap();
        std::fs::write(
            target.join(".pi/settings.json"),
            r#"{"enabledModels":["ollama/dynamic"]}"#,
        )
        .unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let (accepted, ready) = std::sync::mpsc::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = [0; 2048];
            let _ = socket.read(&mut buf).unwrap();
            accepted.send(()).unwrap();
            gate.recv_timeout(Duration::from_secs(10)).unwrap();
            let body = r#"{"data":[{"id":"dynamic"}]}"#;
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let path = root.path().join("target.jsonl");
        pi_rs::Session::open(&path)
            .unwrap()
            .set_working_directory(&target)
            .unwrap();
        let mut args = super::Cli::parse_from(["pi", "--offline"]);
        args.workspace = launch;
        args.session = root.path().join("launch.jsonl");
        args.trust = true;
        args.model_profiles = super::super::model_profiles::Profiles::parse(&serde_json::json!({"providers":{"ollama":{"baseUrl":endpoint,"models":[{"id":"alpha"}]}}})).unwrap();
        args.endpoint = args
            .model_profiles
            .endpoint(&super::super::Provider::Ollama)
            .into();
        args.model = "alpha".into();
        args.remember_initial_selection();
        let mut view = super::View {
            async_transitions: true,
            ..Default::default()
        };
        super::switch_session(&mut args, &mut view, path.clone()).unwrap();
        ready.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(args.model, "alpha");
        assert_ne!(args.session, path);
        assert!(matches!(
            view.pending_transition.as_ref().unwrap().try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        release.send(()).unwrap();
        let next = view
            .pending_transition
            .take()
            .unwrap()
            .recv_timeout(Duration::from_secs(3))
            .unwrap()
            .unwrap();
        super::apply_session(&mut args, &mut view, next).unwrap();
        assert_eq!(args.session, path);
        assert_eq!(args.model, "dynamic");
        server.join().unwrap();
    }

    #[test]
    fn startup_loads_saved_workspace_settings_before_recording_selection() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let launch = directory.path().join("launch");
        let target = directory.path().join("target");
        for (path, level) in [(&launch, "low"), (&target, "high")] {
            std::fs::create_dir_all(path.join(".pi")).unwrap();
            std::fs::write(
                path.join(".pi/settings.json"),
                format!(r#"{{"defaultThinkingLevel":"{level}"}}"#),
            )
            .unwrap();
        }
        let session_path = directory.path().join("session.jsonl");
        let mut session = pi_rs::Session::open(&session_path).unwrap();
        session.set_working_directory(&target).unwrap();
        drop(session);
        let mut args = super::Cli::parse_from(["pi"]);
        args.session = session_path.clone();
        args.workspace = launch.clone();
        args.settings = super::super::config::Settings::load(&launch, true).unwrap();
        args.trust = true;
        args.model = "fixture".into();
        args.thinking = Some("low".into());
        args.remember_initial_selection();
        super::prepare_initial_session(&mut args).unwrap();
        assert_eq!(
            args.workspace.canonicalize().unwrap(),
            target.canonicalize().unwrap()
        );
        assert_eq!(args.thinking.as_deref(), Some("high"));
        let session = pi_rs::Session::open(&session_path).unwrap();
        assert_eq!(
            session.model_selection(),
            Some(("fixture".into(), Some("high".into())))
        );
    }
    #[test]
    fn imported_branch_checkout_fork_and_switch_restore_selection() {
        use clap::Parser;
        use serde_json::json;

        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("pi.jsonl");
        let entries = [
            json!({"type":"session","version":3}),
            json!({"type":"model_change","id":"a","parentId":null,"provider":"ollama","modelId":"model-a"}),
            json!({"type":"message","id":"u1","parentId":"a","message":{"role":"user","content":"first"}}),
            json!({"type":"message","id":"b","parentId":"u1","message":{"role":"assistant","provider":"ollama","model":"model-b","content":[]}}),
            json!({"type":"message","id":"u2","parentId":"b","message":{"role":"user","content":"second"}}),
        ];
        std::fs::write(
            &source,
            entries.iter().map(|e| format!("{e}\n")).collect::<String>(),
        )
        .unwrap();
        let imported = super::super::session_import::read(&source).unwrap();
        let first = imported
            .records()
            .iter()
            .find(|r| matches!(&r.event, pi_rs::Event::User { content, .. } if content == "first"))
            .unwrap()
            .id;
        let second = imported
            .records()
            .iter()
            .find(|r| matches!(&r.event, pi_rs::Event::User { content, .. } if content == "second"))
            .unwrap()
            .id;
        let session_path = directory.path().join("native.jsonl");
        imported.export_jsonl(&session_path).unwrap();

        let mut args = super::Cli::parse_from(["pi"]);
        args.workspace = directory.path().into();
        args.session_dir = Some(directory.path().into());
        args.session = session_path;
        args.model = "initial".into();
        args.discovered_models = vec![
            ("ollama".into(), "model-a".into()),
            ("ollama".into(), "model-b".into()),
        ];
        args.no_trust = true;
        args.remember_initial_selection();
        let mut view = super::View::default();
        super::slash(&format!("/checkout {second}"), &mut args, &mut view, None).unwrap();
        assert_eq!(args.model, "model-b");
        super::slash(&format!("/checkout {first}"), &mut args, &mut view, None).unwrap();
        assert_eq!(args.model, "model-a");
        super::slash(&format!("/checkout {second}"), &mut args, &mut view, None).unwrap();
        super::slash(&format!("/fork {first}"), &mut args, &mut view, None).unwrap();
        assert_eq!(args.model, "model-a");

        let invalid_source = directory.path().join("invalid-pi.jsonl");
        let invalid = [
            json!({"type":"session","version":3}),
            json!({"type":"thinking_level_change","id":"t","parentId":null,"thinkingLevel":"high"}),
            json!({"type":"message","id":"m","parentId":"t","message":{"role":"assistant","model":"missing-provider","content":[]}}),
        ];
        std::fs::write(
            &invalid_source,
            invalid.iter().map(|e| format!("{e}\n")).collect::<String>(),
        )
        .unwrap();
        let invalid_session = directory.path().join("invalid-native.jsonl");
        super::super::session_import::read(&invalid_source)
            .unwrap()
            .export_jsonl(&invalid_session)
            .unwrap();
        super::switch_session(&mut args, &mut view, invalid_session).unwrap();
        assert_eq!(args.model, "initial");
        assert_eq!(args.thinking.as_deref(), Some("high"));
    }
    #[test]
    fn patched_results_render_status_and_details_from_record_events() {
        use super::*;
        let mut view = View::default();
        for (id, is_error) in [("one", false), ("two", true)] {
            view.receive(&json!({"type":"record","record":{"event":{"type":"assistant","content":"","invocations":[{"operation_id":id,"call":{"name":"edit","arguments":"{}"}}]}}}));
            view.receive(&json!({"type":"record","record":{"event":{"type":"tool_finished","operation_id":id,"outcome":{"status":"text_result","detail":{"output":"exact", "details":{"diff":"diff output"}, "is_error":is_error}}}}}));
        }
        assert!(
            matches!(&view.lines[0], Entry::Tool { failed: false, finished: true, output, numbered_diff: true, .. } if output == "diff output")
        );
        assert!(
            matches!(&view.lines[1], Entry::Tool { failed: true, finished: true, output, numbered_diff: false, .. } if output == "exact")
        );
    }

    #[test]
    fn clear_requires_two_presses_within_500_ms_even_when_empty() {
        let mut view = super::View::default();
        let mut last = None;
        let now = std::time::Instant::now();
        assert!(!super::clear_editor_or_exit(&mut view, &mut last, now));
        assert!(super::clear_editor_or_exit(
            &mut view,
            &mut last,
            now + std::time::Duration::from_millis(499)
        ));
        assert!(!super::clear_editor_or_exit(
            &mut view,
            &mut last,
            now + std::time::Duration::from_millis(500)
        ));
        assert!(super::clear_editor_or_exit(
            &mut view,
            &mut last,
            now + std::time::Duration::from_millis(999)
        ));
    }

    #[test]
    fn clear_removes_draft_before_a_second_press() {
        let mut view = super::View::default();
        view.editor.insert("unsent draft");
        view.draft_images.push(pi_rs::ImageContent {
            data: "aGVsbG8=".into(),
            mime_type: "image/png".into(),
        });
        let (_sender, receiver) = std::sync::mpsc::channel();
        view.clipboard = Some(receiver);
        let mut last = None;
        let now = std::time::Instant::now();
        assert!(!super::clear_editor_or_exit(&mut view, &mut last, now));
        assert!(view.editor.text.is_empty());
        assert!(view.draft_images.is_empty());
        assert!(view.clipboard.is_some());
        _sender
            .send(Ok(super::super::clipboard::Content::Text("pasted".into())))
            .unwrap();
        assert!(view.poll_clipboard());
        assert_eq!(view.editor.text, "pasted");
        view.editor.take();
        view.editor.insert("new draft");
        assert!(super::clear_editor_or_exit(
            &mut view,
            &mut last,
            now + std::time::Duration::from_millis(1)
        ));
        assert_eq!(view.editor.text, "new draft");
    }

    #[test]
    fn link_clicks_use_visible_cells_and_require_the_original_target() {
        let mut view = super::View::default();
        let mut line = super::Line::plain("あ", crossterm::style::Color::Reset);
        let mut link = super::Line::plain("link", crossterm::style::Color::Reset)
            .spans
            .remove(0);
        link.hyperlink = Some("https://example.test".into());
        line.spans.push(link);
        view.cache_links(&[line], 2, 5);
        assert_eq!(view.hyperlink_at(1, 2), None);
        assert_eq!(view.hyperlink_at(2, 2), Some("https://example.test"));
        assert_eq!(view.hyperlink_at(4, 2), Some("https://example.test"));
        assert_eq!(view.hyperlink_at(5, 2), None);
        view.link_press = Some(("https://example.test".into(), 2, 2));
        assert_eq!(
            view.released_link(2, 2).as_deref(),
            Some("https://example.test")
        );
        view.link_press = Some(("https://example.test".into(), 2, 2));
        assert_eq!(view.released_link(3, 2), None);
        view.link_press = Some(("https://example.test".into(), 2, 2));
        view.link_rows.clear();
        assert_eq!(view.released_link(2, 2), None);
    }

    #[test]
    fn copy_shortcut_prefers_selected_text_when_auto_copy_is_disabled() {
        let mut view = super::View::default();
        assert!(!view.keyboard_copies_selection(false, true, false));
        let lines = [super::Line::plain(
            "selected text",
            crossterm::style::Color::Reset,
        )];
        view.mouse.update(&lines, 0, 0..1);
        view.mouse.press(0, 0, std::time::Instant::now());
        assert!(view.mouse.release(7, 0));
        assert_eq!(view.mouse.text().as_deref(), Some("selected"));
        assert!(view.keyboard_copies_selection(false, true, false));
        assert!(!view.keyboard_copies_selection(false, true, true));
        assert!(view.keyboard_copies_selection(true, false, true));
    }

    #[test]
    fn credential_change_discards_cached_and_inflight_model_lists() {
        let mut view = super::View::default();
        view.available_models
            .push("openai-chatgpt/account-a-model".into());
        let (sender, receiver) = std::sync::mpsc::channel();
        view.model_list = Some(super::ModelDiscovery {
            receiver,
            intent: super::ModelListIntent::Select,
        });
        view.picker = Some(super::Picker::new("Select model", vec![]));
        view.invalidate_model_catalog();
        assert!(view.available_models.is_empty());
        assert!(view.model_list.is_none());
        assert!(view.picker.is_none());
        assert!(sender.send(Ok(vec!["stale".into()])).is_err());
    }

    #[test]
    fn debug_log_includes_rendered_transcript_and_session_messages() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let mut args = super::Cli::parse_from(["pi"]);
        args.session = directory.path().join("session.jsonl");
        let mut session = pi_rs::Session::open(&args.session).unwrap();
        session
            .append_user_with_images("question".into(), Vec::new(), None)
            .unwrap();
        drop(session);
        let mut view = super::View::default();
        view.lines.push(super::Entry::user("question".into()));
        view.streaming = "answer in progress".into();
        let output = view
            .debug_log(&args, &super::Theme::system(), 80, 24)
            .unwrap();
        assert!(output.contains("Terminal: 80x24"));
        assert!(output.contains("=== All rendered lines with visible widths ==="));
        assert!(output.contains("answer in progress"));
        assert!(output.contains("=== Agent messages (JSONL) ==="));
        assert!(output.contains("\"role\":\"user\""));
        assert!(output.contains("question"));
    }

    #[test]
    fn custom_message_display_flag_controls_terminal_visibility() {
        let mut view = super::View::default();
        let mut message = pi_rs::CustomMessage {
            custom_type: "example".into(),
            content: pi_rs::ContextContent::Text("visible content".into()),
            display: false,
            details: Some(serde_json::json!({"private": "metadata"})),
        };
        view.show_custom_message(&message);
        assert!(view.lines.is_empty());
        message.display = true;
        view.show_custom_message(&message);
        assert!(
            matches!(&view.lines[..], [super::Entry::Notice(text)] if text == "example: visible content")
        );
    }

    #[test]
    fn thinking_model_picker_keeps_current_model_when_discovery_fails() {
        use clap::Parser;
        let mut args = super::Cli::parse_from(["pi"]);
        args.model = "offline-fixture".into();
        args.endpoint = "http://127.0.0.1:1/v1/chat/completions".into();
        args.offline = true;
        let mut view = super::View::default();
        super::start_model_picker(&args, &mut view, super::ModelListIntent::ThinkingSettings);
        let models = view
            .model_list
            .as_ref()
            .unwrap()
            .receiver
            .recv_timeout(std::time::Duration::from_secs(15))
            .unwrap()
            .unwrap();
        assert_eq!(
            models.first().map(String::as_str),
            Some("ollama/offline-fixture")
        );
    }
    #[test]
    fn startup_picker_uses_resolved_themes_and_skips_malformed_ignored_files() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        let themes = workspace.join(".pi/themes");
        std::fs::create_dir_all(&themes).unwrap();
        std::fs::write(themes.join(".fdignore"), "ignored.json\n").unwrap();
        std::fs::write(themes.join("ignored.json"), "not valid JSON").unwrap();
        let mut selected: serde_json::Value =
            serde_json::from_str(include_str!("../assets/themes/dark.json")).unwrap();
        selected["name"] = serde_json::json!("selected");
        std::fs::write(themes.join("selected.json"), selected.to_string()).unwrap();
        let mut args = super::Cli::parse_from([
            "pi",
            "--no-extensions",
            "--no-skills",
            "--no-prompt-templates",
            "--no-context-files",
        ]);
        args.workspace = workspace;
        args.trusted = true;
        args.settings = super::super::config::Settings::default();
        args.use_theme = Some("ignored".into());
        assert_eq!(super::startup_picker_theme(&args).unwrap().name, "system");
        args.use_theme = Some("selected".into());
        assert_eq!(super::startup_picker_theme(&args).unwrap().name, "selected");
        args.no_themes = true;
        assert_eq!(super::startup_picker_theme(&args).unwrap().name, "system");
        args.themes.push(themes.join("selected.json"));
        assert_eq!(super::startup_picker_theme(&args).unwrap().name, "selected");
    }

    #[test]
    fn resume_picker_loads_unprepared_local_package_without_installing() {
        const CHILD: &str = "PI_RS_TEST_RESUME_PACKAGE_THEME_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let home = tempfile::tempdir().unwrap();
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "interactive::tests::resume_picker_loads_unprepared_local_package_without_installing", "--nocapture"])
                .env("PI_RS_HOME", home.path())
                .env("CARGO_NET_OFFLINE", "true")
                .env(CHILD, "1")
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn().unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(45);
            while child.try_wait().unwrap().is_none() && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            if child.try_wait().unwrap().is_none() {
                let _ = child.kill();
                let _ = child.wait();
                panic!("package preparation blocked past the deadline");
            }
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            return;
        }
        use clap::Parser;
        let home = super::super::config::agent_dir();
        let source = home.join("local-package");
        let themes = source.join("themes");
        let workspace = home.join("workspace");
        std::fs::create_dir_all(&themes).unwrap();
        std::fs::create_dir_all(&workspace).unwrap();
        let mut selected: serde_json::Value =
            serde_json::from_str(include_str!("../assets/themes/dark.json")).unwrap();
        selected["name"] = serde_json::json!("package-theme");
        std::fs::write(themes.join("theme.json"), selected.to_string()).unwrap();
        std::fs::write(
            home.join("settings.json"),
            serde_json::json!({"packages":[source]}).to_string(),
        )
        .unwrap();
        let mut args = super::Cli::parse_from([
            "pi",
            "--no-extensions",
            "--no-skills",
            "--no-prompt-templates",
            "--no-context-files",
        ]);
        args.workspace = workspace;
        args.settings = super::super::config::Settings::default();
        args.use_theme = Some("package-theme".into());
        let index = home.join("packages/index.json");
        assert_eq!(
            super::startup_picker_theme(&args).unwrap().name,
            "package-theme"
        );
        assert!(!index.exists(), "picker must not prepare packages");
        std::fs::write(
            home.join("settings.json"),
            serde_json::json!({"packages":["cargo:never-install-this-package"]}).to_string(),
        )
        .unwrap();
        assert_eq!(super::startup_picker_theme(&args).unwrap().name, "system");
        assert!(
            !index.exists(),
            "missing remote packages must not be installed"
        );

        // A directly selected local package remains explicit with auto-discovery off.
        std::fs::write(home.join("settings.json"), "{}").unwrap();
        args.no_themes = true;
        args.extensions.push(source.to_string_lossy().into_owned());
        assert_eq!(
            super::startup_picker_theme(&args).unwrap().name,
            "package-theme"
        );
        args.extensions.clear();
        args.no_themes = false;

        // Global declarations work without project trust, but package filters
        // still apply before any local theme enters the picker.
        std::fs::write(
            home.join("settings.json"),
            serde_json::json!({"packages":[{"source":source,"autoload":false}]}).to_string(),
        )
        .unwrap();
        assert_eq!(super::startup_picker_theme(&args).unwrap().name, "system");
        std::fs::write(home.join("settings.json"), serde_json::json!({"packages":[{"source":source,"autoload":false,"themes":["+themes/theme.json"]},"cargo:never-install-this-package"]}).to_string()).unwrap();
        assert_eq!(
            super::startup_picker_theme(&args).unwrap().name,
            "package-theme"
        );
        assert!(!index.exists());

        // A project package is visible only after project trust is granted.
        std::fs::write(home.join("settings.json"), "{}").unwrap();
        let project = args.workspace.join(".pi");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(
            project.join("settings.json"),
            serde_json::json!({"packages":[source]}).to_string(),
        )
        .unwrap();
        assert_eq!(super::startup_picker_theme(&args).unwrap().name, "system");
        args.trusted = true;
        assert_eq!(
            super::startup_picker_theme(&args).unwrap().name,
            "package-theme"
        );
        args.trusted = false;

        // Cargo metadata is parsed as data: even a build script is never run.
        let cargo_source = home.join("cargo-package");
        std::fs::create_dir_all(cargo_source.join("palette")).unwrap();
        std::fs::write(cargo_source.join("Cargo.toml"), "[package]\nname='fixture'\nversion='0.1.0'\n[package.metadata.pi]\nthemes=['palette/theme.json']\n").unwrap();
        let sentinel = home.join("build-ran");
        std::fs::write(
            cargo_source.join("build.rs"),
            format!(
                "fn main() {{ std::fs::write({:?}, \"bad\").unwrap(); }}",
                sentinel
            ),
        )
        .unwrap();
        selected["name"] = serde_json::json!("cargo-theme");
        std::fs::write(
            cargo_source.join("palette/theme.json"),
            selected.to_string(),
        )
        .unwrap();
        std::fs::write(
            home.join("settings.json"),
            serde_json::json!({"packages":[cargo_source]}).to_string(),
        )
        .unwrap();
        args.use_theme = Some("cargo-theme".into());
        assert_eq!(
            super::startup_picker_theme(&args).unwrap().name,
            "cargo-theme"
        );
        assert!(!sentinel.exists());
        assert!(!index.exists());

        args.use_theme = Some("package-theme".into());
        std::fs::write(
            home.join("settings.json"),
            serde_json::json!({"packages":[source]}).to_string(),
        )
        .unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(super::super::packages::prepare(&args))
            .unwrap();
        assert!(index.exists());
        assert_eq!(
            super::startup_picker_theme(&args).unwrap().name,
            "package-theme"
        );
        let broken = home.join("broken.json");
        std::fs::write(&broken, "not JSON").unwrap();
        args.themes.push(broken);
        assert_eq!(
            super::startup_picker_theme(&args).unwrap().name,
            "package-theme"
        );

        // Pi package.json manifests work before and after normal preparation,
        // including a BOM; scripts are data and are never executed.
        let pi_source = home.join("pi-package");
        std::fs::create_dir_all(pi_source.join("assets")).unwrap();
        selected["name"] = serde_json::json!("pi-theme");
        std::fs::write(pi_source.join("assets/visual.json"), selected.to_string()).unwrap();
        std::fs::create_dir_all(pi_source.join("extensions")).unwrap();
        let extension = pi_source.join("extensions/unexpected");
        std::fs::write(
            &extension,
            format!("#!/bin/sh\ntouch '{}'\n", sentinel.display()),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&extension, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let pi_manifest = serde_json::json!({"version":"1.0.0", "pi":{"themes":["assets/visual.json"]},
            "scripts":{"prepare":"must-not-run"}});
        std::fs::write(
            pi_source.join("package.json"),
            format!("\u{feff}{pi_manifest}"),
        )
        .unwrap();
        std::fs::write(
            home.join("settings.json"),
            serde_json::json!({"packages":[pi_source]}).to_string(),
        )
        .unwrap();
        args.use_theme = Some("pi-theme".into());
        assert_eq!(super::startup_picker_theme(&args).unwrap().name, "pi-theme");
        assert!(
            !sentinel.exists(),
            "picker must not execute package extensions"
        );
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(super::super::packages::prepare(&args))
            .unwrap();
        assert_eq!(super::startup_picker_theme(&args).unwrap().name, "pi-theme");
        assert!(
            !sentinel.exists(),
            "preparation must not execute undeclared extensions"
        );
        let index_text = std::fs::read_to_string(&index).unwrap();
        assert!(
            index_text.contains("pi-package"),
            "package must actually be prepared"
        );

        // Simulate an index written before package.json themes were supported.
        // It may also contain native resources that the Pi manifest excludes.
        let mut old_index: serde_json::Value = serde_json::from_str(&index_text).unwrap();
        let cached = old_index
            .as_object_mut()
            .unwrap()
            .values_mut()
            .find(|entry| {
                entry["source"]
                    .as_str()
                    .and_then(|source| std::fs::canonicalize(source).ok())
                    == std::fs::canonicalize(&pi_source).ok()
            })
            .unwrap();
        cached["resources"]["themes"] = serde_json::json!([]);
        let canonical_source = pi_source.canonicalize().unwrap();
        cached["resources"]["extensions"] =
            serde_json::json!([canonical_source.join("extensions/unexpected")]);
        cached["resources"]["skills"] =
            serde_json::json!([canonical_source.join("skills/old/SKILL.md")]);
        cached["resources"]["prompts"] =
            serde_json::json!([canonical_source.join("prompts/old.md")]);
        let old_index = old_index.to_string();
        std::fs::write(&index, &old_index).unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(super::super::packages::prepare(&args))
            .unwrap();
        assert_eq!(super::startup_picker_theme(&args).unwrap().name, "pi-theme");
        let resources = super::super::packages::resources(&args).unwrap();
        assert_eq!(
            resources.themes,
            vec![pi_source.join("assets/visual.json").canonicalize().unwrap()]
        );
        assert!(resources.extensions.is_empty());
        assert_eq!(std::fs::read_to_string(&index).unwrap(), old_index);
        assert!(!sentinel.exists());
        std::fs::write(
            home.join("settings.json"),
            serde_json::json!({"packages":[{"source":pi_source,"autoload":false}]}).to_string(),
        )
        .unwrap();
        assert_eq!(super::startup_picker_theme(&args).unwrap().name, "system");
        assert!(
            super::super::packages::resources(&args)
                .unwrap()
                .themes
                .is_empty()
        );
        std::fs::write(
            home.join("settings.json"),
            serde_json::json!({"packages":[pi_source]}).to_string(),
        )
        .unwrap();

        // The higher-priority Pi manifest must not hide an unusable Cargo file.
        std::fs::write(
            pi_source.join("pi-package.json"),
            r#"{"pi":{"themes":["assets/visual.json"]}}"#,
        )
        .unwrap();
        std::fs::write(pi_source.join("Cargo.toml"), "[invalid").unwrap();
        assert_eq!(super::startup_picker_theme(&args).unwrap().name, "system");
        let resources = super::super::packages::resources(&args).unwrap();
        assert!(resources.themes.is_empty() && resources.extensions.is_empty());
        assert!(!sentinel.exists());
        std::fs::remove_file(pi_source.join("Cargo.toml")).unwrap();
        std::fs::remove_file(pi_source.join("pi-package.json")).unwrap();

        // A cached pre-upgrade package must obey each current manifest format.
        // Force preparation through a transient declaration as well as checking
        // the cached resolution path used by normal startup.
        std::fs::create_dir_all(pi_source.join("src")).unwrap();
        std::fs::write(pi_source.join("src/lib.rs"), "pub fn fixture() {}\n").unwrap();
        for format in ["pi-package", "cargo", "package"] {
            std::fs::write(&index, &old_index).unwrap();
            match format {
                "pi-package" => std::fs::write(pi_source.join("pi-package.json"), r#"{"pi":{"themes":["assets/visual.json"]}}"#).unwrap(),
                "cargo" => std::fs::write(pi_source.join("Cargo.toml"), "[package]\nname='upgrade-fixture'\nversion='0.1.0'\nedition='2021'\n[package.metadata.pi]\nthemes=['assets/visual.json']\n").unwrap(),
                _ => {}
            }
            assert_eq!(
                super::startup_picker_theme(&args).unwrap().name,
                "pi-theme",
                "{format}"
            );
            let resources = super::super::packages::resources(&args).unwrap();
            assert_eq!(resources.themes.len(), 1, "{format}");
            assert!(
                resources.extensions.is_empty()
                    && resources.skills.is_empty()
                    && resources.prompts.is_empty(),
                "{format}"
            );
            args.extensions
                .push(pi_source.to_string_lossy().into_owned());
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(super::super::packages::prepare(&args))
                .unwrap();
            assert_ne!(
                std::fs::read_to_string(&index).unwrap(),
                old_index,
                "{format} was not prepared"
            );
            let prepared = super::super::packages::transient_resources(&args).unwrap();
            assert!(
                prepared.extensions.is_empty()
                    && prepared.skills.is_empty()
                    && prepared.prompts.is_empty(),
                "{format}"
            );
            assert!(
                !sentinel.exists(),
                "{format} executed an undeclared extension"
            );
            args.extensions.clear();
            if format == "pi-package" {
                std::fs::remove_file(pi_source.join("pi-package.json")).unwrap();
            }
            if format == "cargo" {
                std::fs::remove_file(pi_source.join("Cargo.toml")).unwrap();
            }
        }

        // Still-declared cached resources remain available after filtering.
        std::fs::create_dir_all(pi_source.join("skills/old")).unwrap();
        std::fs::create_dir_all(pi_source.join("prompts")).unwrap();
        std::fs::write(
            pi_source.join("skills/old/SKILL.md"),
            "---\nname: old\n---\n",
        )
        .unwrap();
        std::fs::write(pi_source.join("prompts/old.md"), "prompt").unwrap();
        std::fs::write(&index, &old_index).unwrap();
        std::fs::write(pi_source.join("pi-package.json"), r#"{"pi":{"extensions":["extensions/unexpected"],"skills":["skills/old"],"prompts":["prompts/old.md"],"themes":["assets/visual.json"]}}"#).unwrap();
        let declared = super::super::packages::resources(&args).unwrap();
        assert_eq!(declared.extensions.len(), 1);
        assert_eq!(declared.skills.len(), 1);
        assert_eq!(declared.prompts.len(), 1);
        assert!(!sentinel.exists());
        std::fs::remove_file(pi_source.join("pi-package.json")).unwrap();

        // Old indexes for local, Git and registry packages all carry a package
        // root. Revalidate that root and apply exclusions to the whole list.
        use sha2::Digest;
        let manifest_path = pi_source.join("pi-package.json");
        let manifest_with_exclusion = r#"{"pi":{"extensions":["extensions","!extensions/unexpected"],"themes":["assets/visual.json"]}}"#;
        for source in [
            pi_source
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            "git:https://example.invalid/example/pkg".into(),
            "cargo:example-pkg@1.0.0".into(),
        ] {
            std::fs::write(&manifest_path, manifest_with_exclusion).unwrap();
            let mut entries: serde_json::Value = serde_json::from_str(&old_index).unwrap();
            let old_key = entries
                .as_object()
                .unwrap()
                .iter()
                .find(|(_, entry)| {
                    entry["source"]
                        .as_str()
                        .and_then(|s| std::fs::canonicalize(s).ok())
                        == Some(canonical_source.clone())
                })
                .map(|(key, _)| key.clone())
                .unwrap();
            let mut cached = entries.as_object_mut().unwrap().remove(&old_key).unwrap();
            cached["source"] = serde_json::json!(source);
            let key = format!("{:x}", sha2::Sha256::digest(source.as_bytes()));
            entries.as_object_mut().unwrap().insert(key, cached);
            std::fs::write(&index, entries.to_string()).unwrap();
            std::fs::write(
                home.join("settings.json"),
                serde_json::json!({"packages":[source]}).to_string(),
            )
            .unwrap();
            assert_eq!(
                super::startup_picker_theme(&args).unwrap().name,
                "pi-theme",
                "{source}"
            );
            let resources = super::super::packages::resources(&args).unwrap();
            assert!(
                resources.extensions.is_empty()
                    && resources.skills.is_empty()
                    && resources.prompts.is_empty(),
                "{source}"
            );
            assert_eq!(resources.themes.len(), 1, "{source}");
            std::fs::write(&manifest_path, r#"{"pi":null}"#).unwrap();
            assert_eq!(
                super::startup_picker_theme(&args).unwrap().name,
                "system",
                "{source}"
            );
            let invalid = super::super::packages::resources(&args).unwrap();
            assert!(
                invalid.extensions.is_empty()
                    && invalid.skills.is_empty()
                    && invalid.prompts.is_empty()
                    && invalid.themes.is_empty(),
                "{source}"
            );
            assert!(!sentinel.exists(), "{source} executed a cached extension");
        }
        // Fresh local preparation must not launch the excluded executable.
        std::fs::write(&manifest_path, manifest_with_exclusion).unwrap();
        std::fs::write(&index, &old_index).unwrap();
        std::fs::write(
            home.join("settings.json"),
            serde_json::json!({"packages":[pi_source]}).to_string(),
        )
        .unwrap();
        args.extensions
            .push(pi_source.to_string_lossy().into_owned());
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(super::super::packages::prepare(&args))
            .unwrap();
        assert_ne!(std::fs::read_to_string(&index).unwrap(), old_index);
        assert!(
            super::super::packages::transient_resources(&args)
                .unwrap()
                .extensions
                .is_empty()
        );
        assert!(
            !sentinel.exists(),
            "preparation executed an excluded extension"
        );
        args.extensions.clear();
        std::fs::remove_file(&manifest_path).unwrap();

        // Cargo without Pi metadata must honor package.json during both
        // cached resolution and a fresh preparation of the package.
        let cargo_manifest = pi_source.join("Cargo.toml");
        let package_manifest = pi_source.join("package.json");
        let cargo_plain =
            "[package]\nname='mixed-upgrade-fixture'\nversion='0.1.0'\nedition='2021'\n";
        std::fs::write(&cargo_manifest, cargo_plain).unwrap();
        std::fs::write(&package_manifest, format!("\u{feff}{pi_manifest}")).unwrap();
        std::fs::write(&index, &old_index).unwrap();
        assert_eq!(super::startup_picker_theme(&args).unwrap().name, "pi-theme");
        let cached = super::super::packages::resources(&args).unwrap();
        assert_eq!(cached.themes.len(), 1);
        assert!(
            cached.extensions.is_empty() && cached.skills.is_empty() && cached.prompts.is_empty()
        );
        args.extensions
            .push(pi_source.to_string_lossy().into_owned());
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(super::super::packages::prepare(&args))
            .unwrap();
        assert_ne!(
            std::fs::read_to_string(&index).unwrap(),
            old_index,
            "mixed package not prepared"
        );
        let prepared = super::super::packages::transient_resources(&args).unwrap();
        assert_eq!(prepared.themes.len(), 1);
        assert!(
            prepared.extensions.is_empty()
                && prepared.skills.is_empty()
                && prepared.prompts.is_empty()
        );
        assert!(
            !sentinel.exists(),
            "mixed package executed undeclared extension"
        );
        args.extensions.clear();

        // Even when Cargo or pi-package.json has higher precedence, a present
        // malformed package.json must fail before Cargo runs its build script.
        for higher in ["none", "cargo", "native"] {
            let cargo = if higher == "cargo" {
                format!("{cargo_plain}[package.metadata.pi]\nthemes=['assets/visual.json']\n")
            } else {
                cargo_plain.to_owned()
            };
            std::fs::write(&cargo_manifest, cargo).unwrap();
            if higher == "native" {
                std::fs::write(
                    &manifest_path,
                    r#"{"pi":{"themes":["assets/visual.json"]}}"#,
                )
                .unwrap();
            }
            std::fs::write(&package_manifest, r#"{"pi":{"extensions":42}}"#).unwrap();
            std::fs::write(
                pi_source.join("build.rs"),
                format!(
                    "fn main() {{ std::fs::write({:?}, \"bad\").unwrap(); }}",
                    sentinel
                ),
            )
            .unwrap();
            std::fs::write(&index, &old_index).unwrap();
            assert_eq!(
                super::startup_picker_theme(&args).unwrap().name,
                "system",
                "{higher}"
            );
            let invalid = super::super::packages::resources(&args).unwrap();
            assert!(
                invalid.extensions.is_empty()
                    && invalid.skills.is_empty()
                    && invalid.prompts.is_empty()
                    && invalid.themes.is_empty(),
                "{higher}"
            );
            let mut empty_index: serde_json::Value = serde_json::from_str(&old_index).unwrap();
            empty_index.as_object_mut().unwrap().retain(|_, entry| {
                entry["source"]
                    .as_str()
                    .and_then(|s| std::fs::canonicalize(s).ok())
                    != Some(canonical_source.clone())
            });
            let empty_index = empty_index.to_string();
            std::fs::write(&index, &empty_index).unwrap();
            args.extensions
                .push(pi_source.to_string_lossy().into_owned());
            assert!(
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(super::super::packages::prepare(&args))
                    .is_err(),
                "{higher}"
            );
            assert_eq!(
                std::fs::read_to_string(&index).unwrap(),
                empty_index,
                "{higher}"
            );
            assert!(!sentinel.exists(), "{higher} ran build script or extension");
            args.extensions.clear();
            std::fs::remove_file(pi_source.join("build.rs")).unwrap();
            if higher == "native" {
                std::fs::remove_file(&manifest_path).unwrap();
            }
        }
        std::fs::remove_file(&cargo_manifest).unwrap();
        std::fs::write(&package_manifest, format!("\u{feff}{pi_manifest}")).unwrap();

        // Invalid resource fields and invalid pi values must be rejected both
        // for cached startup and before preparation can run any build script.
        for case in [
            "native-null",
            "native-string",
            "native-array",
            "native-extensions",
            "cargo-scalar",
            "cargo-extensions",
            "package-extensions",
            "package-skills",
            "package-prompts",
            "package-themes",
        ] {
            std::fs::write(&index, &old_index).unwrap();
            let (name, body) = match case {
                "native-null" => ("pi-package.json", r#"{"pi":null}"#.to_owned()),
                "native-string" => ("pi-package.json", r#"{"pi":"bad"}"#.to_owned()),
                "native-array" => ("pi-package.json", r#"{"pi":[]}"#.to_owned()),
                "native-extensions" => ("pi-package.json", r#"{"pi":{"extensions":42}}"#.to_owned()),
                "cargo-scalar" => ("Cargo.toml", "[package]\nname='invalid-fixture'\nversion='0.1.0'\n[package.metadata]\npi=42\n".to_owned()),
                "cargo-extensions" => ("Cargo.toml", "[package]\nname='invalid-fixture'\nversion='0.1.0'\n[package.metadata.pi]\nextensions=42\n".to_owned()),
                _ => ("package.json", format!(r#"{{"pi":{{"{}":42,"themes":[]}}}}"#, case.strip_prefix("package-").unwrap())),
            };
            // Avoid duplicate JSON keys for the invalid themes case.
            let body = if case == "package-themes" {
                r#"{"pi":{"themes":42}}"#.to_owned()
            } else {
                body
            };
            std::fs::write(pi_source.join(name), body).unwrap();
            if case.starts_with("cargo-") {
                std::fs::write(
                    pi_source.join("build.rs"),
                    format!(
                        "fn main() {{ std::fs::write({:?}, \"bad\").unwrap(); }}",
                        sentinel
                    ),
                )
                .unwrap();
            }
            assert_eq!(
                super::startup_picker_theme(&args).unwrap().name,
                "system",
                "{case}"
            );
            let resources = super::super::packages::resources(&args).unwrap();
            assert!(
                resources.extensions.is_empty()
                    && resources.skills.is_empty()
                    && resources.prompts.is_empty()
                    && resources.themes.is_empty(),
                "{case}"
            );
            args.extensions
                .push(pi_source.to_string_lossy().into_owned());
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(super::super::packages::prepare(&args))
                .unwrap();
            assert_eq!(
                std::fs::read_to_string(&index).unwrap(),
                old_index,
                "{case}"
            );
            assert!(!sentinel.exists(), "{case} executed an extension");
            args.extensions.clear();
            if name != "package.json" {
                std::fs::remove_file(pi_source.join(name)).unwrap();
            }
            if case.starts_with("cargo-") {
                std::fs::remove_file(pi_source.join("build.rs")).unwrap();
            }
            std::fs::write(
                pi_source.join("package.json"),
                format!("\u{feff}{pi_manifest}"),
            )
            .unwrap();
        }

        // Present but malformed or unreadable manifests must never be treated
        // as absent and trigger conventional executable discovery.
        for case in ["malformed", "unreadable", "invalid-pi", "invalid-root"] {
            let invalid_source = home.join(format!("invalid-{case}"));
            std::fs::create_dir_all(invalid_source.join("extensions")).unwrap();
            let unexpected = invalid_source.join("extensions/unexpected");
            std::fs::write(
                &unexpected,
                format!("#!/bin/sh\ntouch '{}'\n", sentinel.display()),
            )
            .unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&unexpected, std::fs::Permissions::from_mode(0o755))
                    .unwrap();
            }
            match case {
                "malformed" => {
                    std::fs::write(invalid_source.join("package.json"), "{not valid JSON").unwrap()
                }
                "invalid-pi" => {
                    std::fs::write(invalid_source.join("package.json"), r#"{"pi":"invalid"}"#)
                        .unwrap()
                }
                "invalid-root" => {
                    std::fs::write(invalid_source.join("package.json"), "[]").unwrap()
                }
                _ => {
                    // Reading a directory as a file fails on all supported platforms.
                    std::fs::create_dir(invalid_source.join("package.json")).unwrap();
                }
            }
            std::fs::write(
                home.join("settings.json"),
                serde_json::json!({"packages":[invalid_source]}).to_string(),
            )
            .unwrap();
            let before = std::fs::read(&index).unwrap();
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(super::super::packages::prepare(&args))
                .unwrap();
            assert!(!sentinel.exists(), "{case} manifest executed an extension");
            assert_eq!(
                std::fs::read(&index).unwrap(),
                before,
                "{case} package was indexed"
            );
        }

        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            use std::os::unix::fs::{PermissionsExt, symlink};
            for name in ["pi-package.json", "Cargo.toml"] {
                for kind in ["fifo", "broken-link"] {
                    let invalid = home.join(format!("invalid-{name}-{kind}"));
                    std::fs::create_dir_all(invalid.join("extensions")).unwrap();
                    let unexpected = invalid.join("extensions/unexpected");
                    std::fs::write(
                        &unexpected,
                        format!("#!/bin/sh\ntouch '{}'\n", sentinel.display()),
                    )
                    .unwrap();
                    std::fs::set_permissions(&unexpected, std::fs::Permissions::from_mode(0o755))
                        .unwrap();
                    let path = invalid.join(name);
                    if kind == "fifo" {
                        let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
                        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
                    } else {
                        symlink(invalid.join("missing"), &path).unwrap();
                    }
                    std::fs::write(
                        home.join("settings.json"),
                        serde_json::json!({"packages":[invalid]}).to_string(),
                    )
                    .unwrap();
                    let before = std::fs::read(&index).unwrap();
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(super::super::packages::prepare(&args))
                        .unwrap();
                    assert!(!sentinel.exists(), "{name} {kind} executed an extension");
                    assert_eq!(
                        std::fs::read(&index).unwrap(),
                        before,
                        "{name} {kind} was indexed"
                    );
                }
            }
        }

        // A regular Cargo manifest still follows the normal offline build path.
        let valid_cargo = home.join("valid-cargo");
        std::fs::create_dir_all(valid_cargo.join("src")).unwrap();
        std::fs::create_dir_all(valid_cargo.join("palette")).unwrap();
        std::fs::write(valid_cargo.join("Cargo.toml"), "[package]\nname='local-theme-fixture'\nversion='0.1.0'\nedition='2021'\n[package.metadata.pi]\nthemes=['palette/theme.json']\n").unwrap();
        std::fs::write(valid_cargo.join("src/lib.rs"), "pub fn fixture() {}\n").unwrap();
        selected["name"] = serde_json::json!("prepared-cargo-theme");
        std::fs::write(valid_cargo.join("palette/theme.json"), selected.to_string()).unwrap();
        std::fs::write(
            home.join("settings.json"),
            serde_json::json!({"packages":[valid_cargo]}).to_string(),
        )
        .unwrap();
        args.offline = true;
        args.use_theme = Some("prepared-cargo-theme".into());
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(super::super::packages::prepare(&args))
            .unwrap();
        assert!(
            std::fs::read_to_string(&index)
                .unwrap()
                .contains("valid-cargo")
        );
        assert_eq!(
            super::startup_picker_theme(&args).unwrap().name,
            "prepared-cargo-theme"
        );
        assert!(!sentinel.exists());
    }

    #[test]
    fn theme_settings_preview_skips_unrelated_invalid_theme() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let broken = directory.path().join("broken.json");
        let selected = directory.path().join("selected.json");
        std::fs::write(&broken, "not JSON").unwrap();
        let mut value: serde_json::Value =
            serde_json::from_str(include_str!("../assets/themes/dark.json")).unwrap();
        value["name"] = serde_json::json!("fixture-theme");
        std::fs::write(&selected, value.to_string()).unwrap();
        let mut args = super::Cli::parse_from(["pi"]);
        args.theme_paths = vec![broken, selected];
        let original = super::Theme::system();
        let state =
            super::ThemeSettings::new(super::Picker::new("Settings", vec![]), &args, &original);
        let mut picker = state.picker(&args);
        picker.paste("fixture-theme");
        assert_eq!(state.preview(&picker, &args).unwrap().name, "fixture-theme");
    }

    #[test]
    fn theme_settings_previews_selection_without_changing_original() {
        use clap::Parser;
        let mut args = super::Cli::parse_from(["pi"]);
        args.use_theme = Some("dark".into());
        let dark = super::Theme::load("dark", &[], &args.workspace, false, false).unwrap();
        let state = super::ThemeSettings::new(super::Picker::new("Settings", vec![]), &args, &dark);
        let mut picker = state.picker(&args);
        picker.paste("light");
        assert_eq!(state.preview(&picker, &args).unwrap().name, "light");
        assert_eq!(state.original.name, "dark");
        assert_eq!(args.use_theme.as_deref(), Some("dark"));
    }
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
    fn new_and_empty_saved_sessions_use_initial_model_and_default_thinking() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let mut args = super::Cli::parse_from(["pi", "--offline"]);
        args.workspace = directory.path().to_owned();
        args.session_dir = Some(directory.path().to_owned());
        args.session = directory.path().join("current.jsonl");
        args.endpoint = "https://example.invalid/v1/chat/completions".into();
        args.endpoint_explicit = true;
        args.initial_connection = Some((super::super::Provider::Ollama, args.endpoint.clone()));
        std::fs::create_dir_all(directory.path().join(".pi")).unwrap();
        std::fs::write(
            directory.path().join(".pi/settings.json"),
            r#"{"defaultThinkingLevel":"low"}"#,
        )
        .unwrap();
        args.settings = super::super::config::Settings::load(directory.path(), true).unwrap();
        args.model = "alpha".into();
        args.thinking = Some("low".into());
        args.no_trust = true;
        args.remember_initial_selection();
        let empty_path = directory.path().join("saved-empty.jsonl");
        drop(pi_rs::Session::open(&empty_path).unwrap());
        let mut view = super::View {
            available_models: vec!["ollama/alpha".into(), "ollama/beta".into()],
            ..Default::default()
        };
        super::slash("/model beta:high", &mut args, &mut view, None).unwrap();
        assert_eq!(args.model, "beta");
        assert_eq!(args.thinking.as_deref(), Some("high"));
        super::slash("/new", &mut args, &mut view, None).unwrap();
        assert_eq!(args.model, "alpha");
        assert_eq!(args.thinking.as_deref(), Some("low"));
        let session = pi_rs::Session::open(&args.session).unwrap();
        assert_eq!(
            session.model_selection(),
            Some(("alpha".into(), Some("low".into())))
        );
        assert_eq!(session.model_provider(), None);
        drop(session);
        super::switch_session(&mut args, &mut view, empty_path).unwrap();
        assert_eq!(args.model, "alpha");
    }

    #[test]
    fn new_session_restores_original_cli_model_and_thinking_overrides() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let mut args = super::Cli::parse_from(["pi", "--offline"]);
        args.workspace = directory.path().into();
        args.session_dir = Some(directory.path().into());
        args.session = directory.path().join("current.jsonl");
        args.endpoint = "https://example.invalid/v1/chat/completions".into();
        args.endpoint_explicit = true;
        args.initial_connection = Some((super::super::Provider::Ollama, args.endpoint.clone()));
        args.model = "alpha".into();
        args.thinking = Some("low".into());
        args.model_explicit = true;
        args.thinking_explicit = true;
        args.remember_initial_selection();
        let mut view = super::View {
            available_models: vec!["ollama/alpha".into(), "ollama/beta".into()],
            ..Default::default()
        };
        super::slash("/model beta:high", &mut args, &mut view, None).unwrap();
        assert_eq!(
            (args.model.as_str(), args.thinking.as_deref()),
            ("beta", Some("high"))
        );
        super::slash("/new", &mut args, &mut view, None).unwrap();
        assert_eq!(
            (args.model.as_str(), args.thinking.as_deref()),
            ("alpha", Some("low"))
        );
        let session = pi_rs::Session::open(&args.session).unwrap();
        assert_eq!(
            session.model_selection(),
            Some(("alpha".into(), Some("low".into())))
        );
    }

    #[test]
    fn new_session_restores_individual_cli_overrides() {
        use clap::Parser;
        for (model_explicit, thinking_explicit, expected_thinking) in
            [(true, false, "medium"), (false, true, "low")]
        {
            let directory = tempfile::tempdir().unwrap();
            let mut args = super::Cli::parse_from(["pi", "--offline"]);
            args.workspace = directory.path().into();
            args.session_dir = Some(directory.path().into());
            args.session = directory.path().join("current.jsonl");
            args.endpoint = "https://example.invalid/v1/chat/completions".into();
            args.endpoint_explicit = true;
            args.initial_connection = Some((super::super::Provider::Ollama, args.endpoint.clone()));
            args.model = "alpha".into();
            args.thinking = Some("low".into());
            args.model_explicit = model_explicit;
            args.thinking_explicit = thinking_explicit;
            args.remember_initial_selection();
            let mut view = super::View {
                available_models: vec!["ollama/alpha".into(), "ollama/beta".into()],
                ..Default::default()
            };
            super::slash("/model beta:high", &mut args, &mut view, None).unwrap();
            super::slash("/new", &mut args, &mut view, None).unwrap();
            assert_eq!(
                (args.model.as_str(), args.thinking.as_deref()),
                ("alpha", Some(expected_thinking))
            );
        }
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
