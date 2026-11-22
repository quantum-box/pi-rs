//! Shared subprocess transport for terminal and RPC frontends.
use super::Cli;
use pi_rs::{Error, Result, SessionShutdownReason, SessionStartReason};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub(super) const REFRESH_BARRIER_ENV: &str = "PI_RS_WORKER_REFRESH_BARRIER";
pub(super) const LIVE_CONTROLS_ENV: &str = "PI_RS_WORKER_LIVE_CONTROLS";
pub(super) const SESSION_TRANSITION_ENV: &str = "PI_RS_WORKER_SESSION_TRANSITION";
pub(super) const SELECTION_PROVENANCE_ENV: &str = "PI_RS_WORKER_SELECTION_PROVENANCE";
const MAX_SETTINGS_BOOTSTRAP: usize = 16 * 1024 * 1024;

pub(super) fn read_settings_bootstrap() -> Result<super::config::Settings> {
    read_settings_frame(&mut std::io::stdin().lock())
}

fn read_settings_frame(input: &mut impl std::io::Read) -> Result<super::config::Settings> {
    let mut length = [0_u8; 8];
    input.read_exact(&mut length)?;
    let length = usize::try_from(u64::from_be_bytes(length))
        .map_err(|_| Error::Config("invalid worker settings length".into()))?;
    if length > MAX_SETTINGS_BOOTSTRAP {
        return Err(Error::Config(
            "worker settings snapshot is too large".into(),
        ));
    }
    let mut bytes = vec![0; length];
    input.read_exact(&mut bytes)?;
    serde_json::from_slice(&bytes)
        .map_err(|_| Error::Config("invalid worker settings snapshot".into()))
}

pub(super) static TERMINAL_INTERRUPTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
pub(super) static RPC_INTERRUPTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
pub(super) static TERMINAL_SIGNAL_ENABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
fn terminal_signal_enabled() -> bool {
    TERMINAL_SIGNAL_ENABLED.load(std::sync::atomic::Ordering::SeqCst)
}

static IN_FLIGHT_REFRESH: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub(super) struct RefreshBarrier {
    _process: std::sync::MutexGuard<'static, ()>,
    _worker: Option<std::fs::File>,
}

/// The auth lock must be released before the worker barrier: once the barrier
/// becomes available, a supervisor may immediately kill the worker.
pub(super) struct RefreshAuthGuard {
    auth: super::auth_lock::Guard,
    _barrier: RefreshBarrier,
}

impl std::ops::Deref for RefreshAuthGuard {
    type Target = super::auth_lock::Guard;

    fn deref(&self) -> &Self::Target {
        &self.auth
    }
}

pub(super) fn guard_refresh_auth(
    barrier: RefreshBarrier,
    auth: super::auth_lock::Guard,
) -> RefreshAuthGuard {
    RefreshAuthGuard {
        auth,
        _barrier: barrier,
    }
}

pub(super) fn stop_new_refreshes() -> std::sync::MutexGuard<'static, ()> {
    IN_FLIGHT_REFRESH
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(super) fn refresh_barrier() -> Result<RefreshBarrier> {
    let process = stop_new_refreshes();
    let worker = std::env::var_os(REFRESH_BARRIER_ENV)
        .map(|path| {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
        })
        .transpose()?;
    if let Some(file) = &worker {
        file.lock()?;
    }
    Ok(RefreshBarrier {
        _process: process,
        _worker: worker,
    })
}

// Used for operations that remain cancellable until their irreversible step.
pub(super) fn refresh_barrier_cancellable(
    cancelled: impl Fn() -> bool,
) -> Result<Option<RefreshBarrier>> {
    let process = loop {
        if cancelled() {
            return Ok(None);
        }
        match IN_FLIGHT_REFRESH.try_lock() {
            Ok(guard) => break guard,
            Err(std::sync::TryLockError::Poisoned(error)) => break error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                std::thread::sleep(Duration::from_millis(25))
            }
        }
    };
    let worker = std::env::var_os(REFRESH_BARRIER_ENV)
        .map(|path| {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
        })
        .transpose()?;
    if let Some(file) = &worker {
        loop {
            if cancelled() {
                return Ok(None);
            }
            match file.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) => {
                    std::thread::sleep(Duration::from_millis(25))
                }
                Err(std::fs::TryLockError::Error(error)) => return Err(Error::Io(error)),
            }
        }
    }
    Ok(Some(RefreshBarrier {
        _process: process,
        _worker: worker,
    }))
}

fn shutdown_reason(reason: SessionStartReason) -> SessionShutdownReason {
    match reason {
        SessionStartReason::Startup => SessionShutdownReason::Quit,
        SessionStartReason::Reload => SessionShutdownReason::Reload,
        SessionStartReason::New => SessionShutdownReason::New,
        SessionStartReason::Resume => SessionShutdownReason::Resume,
        SessionStartReason::Fork => SessionShutdownReason::Fork,
    }
}

pub(super) struct Worker {
    pub(super) compacting: bool,
    pub(super) retrying: bool,
    pub(super) bashing: bool,
    pub(super) steering_mode: pi_rs::QueueMode,
    pub(super) follow_up_mode: pi_rs::QueueMode,
    session: std::path::PathBuf,
    pub(super) busy: bool,
    persistent: bool,
    prompt_shutdown: bool,
    shutdown: Option<(SessionShutdownReason, Option<std::path::PathBuf>)>,
    configuration: Value,
    pub(super) child: Child,
    stdin: Option<ChildStdin>,
    refresh_barrier: tempfile::NamedTempFile,
    events: Receiver<Value>,
    pending_events: Vec<Value>,
    extension_sources: std::collections::BTreeMap<String, Value>,
    pub(super) message_renderer_types: std::collections::BTreeSet<String>,
    settings_snapshot: super::config::Settings,
    workspace: std::path::PathBuf,
    trusted: bool,
    pub(super) readers: Vec<JoinHandle<()>>,
}
fn retain_warning_on_start_error(error: Error, pending: &[Value]) -> Error {
    let warnings: Vec<_> = pending
        .iter()
        .filter(|event| event["type"] == "warning")
        .filter_map(|event| event["message"].as_str())
        .collect();
    if warnings.is_empty() {
        error
    } else {
        Error::Config(format!("{}\n{}", warnings.join("\n"), error))
    }
}
impl Worker {
    pub(super) fn settings_for<'a>(
        slot: &'a Option<Self>,
        args: &Cli,
    ) -> Option<&'a super::config::Settings> {
        let worker = slot.as_ref()?;
        let same_workspace = worker.workspace == args.workspace
            || matches!((worker.workspace.canonicalize(), args.workspace.canonicalize()), (Ok(a), Ok(b)) if a == b);
        (same_workspace && worker.trusted == args.trusted).then_some(&worker.settings_snapshot)
    }
    pub(super) fn start(args: &Cli, operation: &[&str]) -> Result<Self> {
        Self::start_with_transition(args, operation, None, None)
    }

    fn start_with_transition(
        args: &Cli,
        operation: &[&str],
        transition: Option<(SessionStartReason, Option<std::path::PathBuf>)>,
        fallback: Option<&super::config::Settings>,
    ) -> Result<Self> {
        let history = args.settings.worker_fallback(&args.workspace, args.trusted);
        let settings = args.settings.reload_with_fallback(
            &args.workspace,
            args.trusted,
            history.as_ref().or(fallback).unwrap_or(&args.settings),
        )?;
        let mut snapshot_args = args.clone();
        snapshot_args.settings = settings.clone();
        let steering_mode = session_queue_mode(&snapshot_args, pi_rs::InputKind::Steer)?;
        let follow_up_mode = session_queue_mode(&snapshot_args, pi_rs::InputKind::FollowUp)?;
        let refresh_barrier = tempfile::NamedTempFile::new()?;
        let mut command = Command::new(std::env::current_exe()?);
        if args.mode != Some(super::OutputMode::Rpc) {
            command.arg("--terminal-managed-worker");
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt;
                // Protect the startup interval before the worker installs its
                // listener; a caught disposition is installed immediately in
                // run_cli, before starting any child processes.
                unsafe {
                    command.pre_exec(|| {
                        if libc::signal(libc::SIGINT, libc::SIG_IGN) == libc::SIG_ERR {
                            return Err(std::io::Error::last_os_error());
                        }
                        Ok(())
                    });
                }
            }
        }
        command.env(REFRESH_BARRIER_ENV, refresh_barrier.path());
        command.env_remove("PI_RS_MODEL_TOKEN");
        command.env_remove(SESSION_TRANSITION_ENV);
        command.env(
            SELECTION_PROVENANCE_ENV,
            serde_json::to_string(&(args.model_explicit, args.thinking_explicit))?,
        );
        command.env(
            LIVE_CONTROLS_ENV,
            serde_json::to_string(&args.settings.live_controls())?,
        );
        if let Some(transition) = transition {
            command.env(SESSION_TRANSITION_ENV, serde_json::to_string(&transition)?);
        }
        let endpoint = args.model_endpoint_for(&args.provider, Some(&args.model));
        if let Some(token) = args.model_override_token(&args.provider, endpoint) {
            command.env("PI_RS_MODEL_TOKEN", token);
        }

        let mut startup_warnings = Vec::new();
        let bootstrap = if operation.first() == Some(&"serve") {
            startup_warnings.extend(
                settings
                    .diagnostics()
                    .iter()
                    .filter(|warning| !args.settings.diagnostics().contains(warning))
                    .map(|warning| json!({"type":"warning", "message":warning})),
            );
            let bytes = serde_json::to_vec(&settings)?;
            if bytes.len() > MAX_SETTINGS_BOOTSTRAP {
                return Err(Error::Config(
                    "worker settings snapshot is too large".into(),
                ));
            }
            command.arg("--worker-settings-stdin");
            Some(bytes)
        } else {
            None
        };
        command
            .arg("--session")
            .arg(&args.session)
            .arg("--workspace")
            .arg(&args.workspace)
            .args([
                "--provider",
                args.provider.id(),
                "--model",
                &args.model,
                "--trace",
                "--stdin-control",
            ])
            .arg("--max-requests")
            .arg(args.max_requests.to_string())
            .arg("--max-output-tokens")
            .arg(args.max_output_tokens.to_string())
            .arg("--output-budget")
            .arg(args.output_budget.to_string())
            .arg("--model-timeout-secs")
            .arg(args.model_timeout_secs.to_string());
        if let Some(endpoint) = args.explicit_endpoint_for(&args.provider) {
            command.arg("--endpoint").arg(endpoint);
        }
        if args.no_session || args.ephemeral_worker_session {
            command.arg("--ephemeral-worker-session");
        }
        if let Some(timeout) = args.tool_timeout_secs {
            command.arg("--tool-timeout-secs").arg(timeout.to_string());
        }
        if let Some(thinking) = &args.thinking
            && super::thinking::LEVELS.contains(&thinking.as_str())
        {
            command.arg("--thinking").arg(thinking);
        }
        if args.trusted {
            command.arg("--trust");
        } else {
            command.arg("--no-trust");
        }
        if args.allow_commands {
            command.arg("--allow-commands");
        }
        if args.coding_tools {
            command.arg("--coding-tools");
        }
        if args.no_tools {
            command.arg("--no-tools");
        }
        if args.no_builtin_tools {
            command.arg("--no-builtin-tools");
        }
        if let Some(tools) = &args.tools {
            command.arg("--tools").arg(tools);
        }
        if let Some(tools) = &args.exclude_tools {
            command.arg("--exclude-tools").arg(tools);
        }
        for (disabled, flag) in [
            (args.no_extensions, "--no-extensions"),
            (args.no_themes, "--no-themes"),
            (args.no_skills, "--no-skills"),
            (args.no_prompt_templates, "--no-prompt-templates"),
            (args.no_context_files, "--no-context-files"),
            (args.no_mcp, "--no-mcp"),
            (args.offline, "--offline"),
        ] {
            if disabled {
                command.arg(flag);
            }
        }
        for path in &args.prompt_template {
            command.arg("--prompt-template").arg(path);
        }
        for extension in &args.extensions {
            command.arg("--extension").arg(extension);
        }
        if let Some(path) = &args.project {
            command.arg("--project").arg(path);
        }
        for skill in &args.skill {
            command.arg("--skill").arg(skill);
        }
        if let Some(url) = &args.orders {
            command.arg("--orders").arg(url);
        }
        if let Some(path) = &args.system_prompt {
            command.arg("--system-prompt").arg(path);
        }
        for argument in &args.append_system_prompt {
            command.arg("--append-system-prompt").arg(argument);
        }
        if let Some(text) = &args.instruction {
            command.arg("--instruction").arg(text);
        }
        if let Some(limit) = args.context_window {
            command.arg("--context-window").arg(limit.to_string());
        }
        for extension in &args.disable_extension {
            command.arg("--disable-extension").arg(extension);
        }
        #[cfg(feature = "evaluation-fixtures")]
        if let Some(url) = &args.fixtures {
            command.arg("--fixtures").arg(url);
        }
        if args.mode == Some(super::OutputMode::Rpc) {
            command.args(["--mode", "rpc"]);
        }
        for (name, value) in &args.extension_flags {
            match value {
                pi_rs::ExtensionFlagValue::Boolean(true) => command.arg(format!("--{name}")),
                pi_rs::ExtensionFlagValue::Boolean(false) => command.arg(format!("--{name}=false")),
                pi_rs::ExtensionFlagValue::String(value) => {
                    command.arg(format!("--{name}={value}"))
                }
            };
        }

        command
            .args(operation)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn()?;
        let mut stdin = child.stdin.take();
        if let Some(bytes) = bootstrap {
            let input = stdin.as_mut().expect("piped stdin");
            let sent = input
                .write_all(&(bytes.len() as u64).to_be_bytes())
                .and_then(|()| input.write_all(&bytes));
            if let Err(error) = sent {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error.into());
            }
        }
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let (sender, events) = mpsc::channel();
        let errors = sender.clone();
        let readers = vec![
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    let Ok(line) = line else { break };
                    if let Ok(event) = serde_json::from_str(&line) {
                        let _ = sender.send(event);
                    }
                }
            }),
            std::thread::spawn(move || {
                for line in BufReader::new(stderr).lines() {
                    let Ok(line) = line else { break };
                    let _ = errors.send(json!({"type":"error", "message":line}));
                }
            }),
        ];
        let persistent = operation.first() == Some(&"serve");
        if persistent {
            args.settings
                .remember_worker_snapshot(&args.workspace, args.trusted, &settings);
        }
        Ok(Self {
            compacting: operation.first() == Some(&"compact"),
            retrying: false,
            bashing: false,
            steering_mode,
            follow_up_mode,
            session: args.session.clone(),
            busy: !persistent,
            persistent,
            prompt_shutdown: false,
            shutdown: None,
            configuration: configuration(args),
            child,
            stdin,
            refresh_barrier,
            events,
            pending_events: startup_warnings,
            extension_sources: Default::default(),
            message_renderer_types: Default::default(),
            settings_snapshot: settings,
            workspace: args.workspace.clone(),
            trusted: args.trusted,
            readers,
        })
    }
    pub(super) fn submit(slot: &mut Option<Self>, args: &Cli, request: Value) -> Result<()> {
        if let Some(active) = slot.as_mut().filter(|worker| worker.matches(args)) {
            if active.busy {
                return Err(Error::Config("worker is still running".into()));
            }
            active.request(args, request)
        } else {
            if slot.as_ref().is_some_and(|worker| worker.bashing) {
                return Err(Error::Config(
                    "cannot change the worker while a shell command is running".into(),
                ));
            }
            let previous = Self::settings_for(slot, args).cloned();
            let pending = slot
                .take()
                .map(|mut worker| std::mem::take(&mut worker.pending_events))
                .unwrap_or_default();
            let mut next = Self::start_with_transition(args, &["serve"], None, previous.as_ref())
                .map_err(|error| retain_warning_on_start_error(error, &pending))?;
            next.pending_events.splice(0..0, pending);
            next.request(args, request)
                .map_err(|error| retain_warning_on_start_error(error, &next.pending_events))?;
            *slot = Some(next);
            Ok(())
        }
    }
    pub(super) fn control(slot: &mut Option<Self>, args: &Cli, mut request: Value) -> Result<()> {
        prepare_images(args, &mut request)?;
        if slot
            .as_ref()
            .is_none_or(|worker| !worker.matches(args) || worker.session != args.session)
        {
            if slot
                .as_ref()
                .is_some_and(|worker| worker.busy || worker.bashing)
            {
                return Err(Error::Config(
                    "cannot redirect controls for an active session".into(),
                ));
            }
            let previous = Self::settings_for(slot, args).cloned();
            let pending = slot
                .take()
                .map(|mut worker| std::mem::take(&mut worker.pending_events))
                .unwrap_or_default();
            let mut next = Self::start_with_transition(args, &["serve"], None, previous.as_ref())
                .map_err(|error| retain_warning_on_start_error(error, &pending))?;
            next.pending_events.splice(0..0, pending);
            *slot = Some(next);
        }
        slot.as_mut().expect("queue worker").send(request)
    }
    pub(super) fn queue_mode_control(
        slot: &mut Option<Self>,
        args: &Cli,
        request: Value,
    ) -> Result<()> {
        if let Some(active) = slot.as_mut()
            && (active.busy || active.bashing)
            && active.session == args.session
        {
            return active.send(request);
        }
        Self::control(slot, args, request)
    }
    pub(super) fn start_bash(slot: &mut Option<Self>, args: &Cli, request: Value) -> Result<()> {
        if slot.as_ref().is_some_and(|worker| worker.bashing) {
            return Err(Error::Config("a shell command is already running".into()));
        }
        Self::control(slot, args, request)?;
        slot.as_mut().expect("shell worker").bashing = true;
        Ok(())
    }
    pub(super) fn adopt_live_model(&mut self, args: &Cli) {
        self.configuration = configuration(args);
    }

    pub(super) fn matches(&self, args: &Cli) -> bool {
        self.persistent && self.configuration == configuration(args)
    }
    pub(super) fn queue_mode(
        &self,
        args: &Cli,
        kind: pi_rs::InputKind,
    ) -> Option<pi_rs::QueueMode> {
        (self.session == args.session).then_some(if kind == pi_rs::InputKind::Steer {
            self.steering_mode
        } else {
            self.follow_up_mode
        })
    }
    pub(super) fn drain_events(&mut self) -> Vec<Value> {
        let mut events = std::mem::take(&mut self.pending_events);
        events.extend(
            self.events
                .try_iter()
                .filter(|event| event["type"] != "session_transition_checked"),
        );
        events
            .into_iter()
            .filter_map(|mut event| {
                if event["type"] == "worker_extension_sources" {
                    self.extension_sources = event["sources"]
                        .as_object()
                        .into_iter()
                        .flat_map(|sources| sources.iter())
                        .map(|(id, path)| (id.clone(), path.clone()))
                        .collect();
                    return None;
                }
                if event["type"] == "extension_error"
                    && let Some(source) = event["extension"]
                        .as_str()
                        .and_then(|id| self.extension_sources.get(id))
                {
                    if event.get("extensionPath").is_some() {
                        event["nativeExtensionPath"] = source.clone();
                    } else {
                        event["extensionPath"] = source.clone();
                    }
                }
                Some(event)
            })
            .collect()
    }

    pub(super) fn take_pending_warnings(&mut self) -> Vec<String> {
        let mut warnings = Vec::new();
        self.pending_events.retain(|event| {
            if event["type"] == "warning" {
                if let Some(message) = event["message"].as_str() {
                    warnings.push(message.to_owned());
                }
                false
            } else {
                true
            }
        });
        warnings
    }

    pub(super) fn interrupt_shutdown(&mut self) {
        self.prompt_shutdown = true;
    }

    pub(super) fn check_session_transition(
        slot: Option<&mut Option<Self>>,
        args: &Cli,
        event: pi_rs::BeforeSessionTransition,
    ) -> Result<bool> {
        Self::check_session_transition_cancellable(None, slot, args, event)
    }

    pub(super) fn check_session_transition_cancellable(
        cancelled: Option<&std::sync::atomic::AtomicBool>,
        slot: Option<&mut Option<Self>>,
        args: &Cli,
        event: pi_rs::BeforeSessionTransition,
    ) -> Result<bool> {
        let Some(slot) = slot else {
            return Ok(false);
        };
        let Some(active) = Self::extension_worker(slot, args)? else {
            return Ok(false);
        };
        let id = json!(uuid::Uuid::new_v4());
        active.send(json!({"type":"check_session_transition","id":id,"event":event}))?;
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if TERMINAL_INTERRUPTED.load(std::sync::atomic::Ordering::SeqCst)
                || cancelled.is_some_and(|flag| flag.load(std::sync::atomic::Ordering::SeqCst))
            {
                active.interrupt_shutdown();
                return Err(Error::Cancelled);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Config("session transition check timed out".into()));
            }
            let wait = if cancelled.is_some() || terminal_signal_enabled() {
                remaining.min(Duration::from_millis(50))
            } else {
                remaining
            };
            let event = match active.events.recv_timeout(wait) {
                Ok(event) => event,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(error) => {
                    return Err(Error::Config(format!(
                        "could not check the session transition: {error}"
                    )));
                }
            };
            if event["type"] == "session_transition_checked" {
                if event["id"] != id {
                    continue;
                }
                if let Some(error) = event["error"].as_str() {
                    return Err(Error::Config(error.to_owned()));
                }
                return Ok(event["cancelled"] == true);
            }
            active.pending_events.push(event);
        }
    }
    fn extension_worker<'a>(
        slot: &'a mut Option<Self>,
        args: &Cli,
    ) -> Result<Option<&'a mut Self>> {
        if slot.is_none() {
            let explicit = args
                .extensions
                .iter()
                .any(|extension| !super::resource_paths::named_extension(extension));
            if explicit
                || !super::resources::Resources::load(args)?
                    .extensions
                    .is_empty()
            {
                *slot = Some(Self::start(args, &["serve"])?);
            }
        }
        Ok(slot.as_mut())
    }

    pub(super) fn request_share_context(slot: &mut Option<Self>, args: &Cli) -> Result<uuid::Uuid> {
        let mut previous = None;
        let mut pending = Vec::new();
        if slot
            .as_ref()
            .is_some_and(|worker| !worker.busy && !worker.bashing && !worker.matches(args))
        {
            previous = Self::settings_for(slot, args).cloned();
            if let Some(mut worker) = slot.take() {
                pending = std::mem::take(&mut worker.pending_events);
            }
        }
        if slot.is_none() {
            let mut next = Self::start_with_transition(args, &["serve"], None, previous.as_ref())
                .map_err(|error| retain_warning_on_start_error(error, &pending))?;
            next.pending_events.splice(0..0, pending);
            *slot = Some(next);
        }
        let worker = slot.as_mut().expect("share worker");
        if worker.session != args.session || !worker.persistent {
            return Err(Error::Config(
                "The active worker belongs to another session".into(),
            ));
        }
        let id = uuid::Uuid::new_v4();
        worker.send(json!({"type":"get_share_context", "id":id, "session":worker.session}))?;
        Ok(id)
    }

    pub(super) fn notify_session_name(
        slot: &mut Option<Self>,
        args: &Cli,
        name: Option<String>,
    ) -> Result<()> {
        if let Some(active) = Self::extension_worker(slot, args)?
            && active.session == args.session
        {
            active
                .send(json!({"type":"session_info_changed","session":args.session,"name":name}))?;
        }
        Ok(())
    }
    pub(super) fn notify_selection(
        slot: &mut Option<Self>,
        args: &Cli,
        event: &pi_rs::LifecycleEvent,
    ) -> Result<()> {
        Self::notify_selection_cancellable(None, slot, args, event)
    }

    pub(super) fn notify_selection_cancellable(
        cancelled: Option<&std::sync::atomic::AtomicBool>,
        slot: &mut Option<Self>,
        args: &Cli,
        event: &pi_rs::LifecycleEvent,
    ) -> Result<()> {
        let Some(active) = Self::extension_worker(slot, args)? else {
            return Ok(());
        };
        if active.session != args.session {
            return Ok(());
        }
        let id = json!(uuid::Uuid::new_v4());
        active.send(
            json!({"type":"selection_changed", "id":id, "session":args.session, "event":event}),
        )?;
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if TERMINAL_INTERRUPTED.load(std::sync::atomic::Ordering::SeqCst)
                || cancelled.is_some_and(|flag| flag.load(std::sync::atomic::Ordering::SeqCst))
            {
                active.interrupt_shutdown();
                return Err(Error::Cancelled);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Config(
                    "model selection notification timed out".into(),
                ));
            }
            let wait = if terminal_signal_enabled() || cancelled.is_some() {
                remaining.min(Duration::from_millis(50))
            } else {
                remaining
            };
            let received = match active.events.recv_timeout(wait) {
                Ok(value) => value,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(error) => {
                    return Err(Error::Config(format!(
                        "could not notify model selection: {error}"
                    )));
                }
            };
            if received["type"] == "selection_notified" && received["id"] == id {
                return Ok(());
            }
            active.pending_events.push(received);
        }
    }
    pub(super) fn transition(
        slot: &mut Option<Self>,
        args: &Cli,
        reason: SessionStartReason,
    ) -> Result<()> {
        let Some(active) = slot.as_mut() else {
            return Ok(());
        };
        if active.session == args.session && !matches!(reason, SessionStartReason::Reload) {
            return Ok(());
        }
        if active.busy || active.bashing {
            return Err(Error::Config(
                "wait for the current operation before changing sessions".into(),
            ));
        }
        let replacing_session = active.session != args.session;
        let previous = replacing_session.then(|| active.session.clone());
        active.shutdown = Some((
            shutdown_reason(reason),
            replacing_session.then(|| args.session.clone()),
        ));
        let mut pending_events = std::mem::take(&mut active.pending_events);
        let extension_sources = std::mem::take(&mut active.extension_sources);
        let fallback = Self::settings_for(slot, args).cloned();
        drop(slot.take());
        let mut next = Self::start_with_transition(
            args,
            &["serve"],
            Some((reason, previous)),
            fallback.as_ref(),
        )
        .map_err(|error| retain_warning_on_start_error(error, &pending_events))?;
        pending_events.append(&mut next.pending_events);
        next.pending_events = pending_events;
        next.extension_sources = extension_sources;
        *slot = Some(next);
        Ok(())
    }

    pub(super) fn request(&mut self, args: &Cli, mut request: Value) -> Result<()> {
        prepare_images(args, &mut request)?;
        if self.bashing && self.session != args.session {
            return Err(Error::Config(
                "cannot change sessions while a shell command is running".into(),
            ));
        }
        self.send(json!({"type":"configure","model":args.model,"thinking":args.thinking,"reported_thinking":super::thinking::reported(args).is_some(),"reported_reasoning":super::thinking::adopted_reasoning(args),"session":args.session}))?;
        self.compacting = request["type"] == "compact";
        self.send(request)?;
        self.session = args.session.clone();
        self.busy = true;
        Ok(())
    }
    pub(super) fn send(&mut self, value: Value) -> Result<()> {
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| Error::Config("agent input is closed".into()))?;
        serde_json::to_writer(&mut *stdin, &value)?;
        writeln!(stdin)?;
        stdin.flush()?;
        Ok(())
    }
}

pub(super) fn session_queue_mode(args: &Cli, kind: pi_rs::InputKind) -> Result<pi_rs::QueueMode> {
    if !args.no_session && !args.ephemeral_worker_session
        && let Ok(session) = pi_rs::Session::snapshot(&args.session)
        && session.branch().iter().any(|record| matches!(record.event, pi_rs::Event::QueueModeSelected { kind: selected, .. } if selected == kind))
    {
        return Ok(session.queue_mode(kind));
    }
    super::backend::configured_queue_mode(
        args,
        if kind == pi_rs::InputKind::Steer {
            "steeringMode"
        } else {
            "followUpMode"
        },
    )
}
fn prepare_images(args: &Cli, request: &mut Value) -> Result<()> {
    let Some(profile) = args.model_image_resize() else {
        return Ok(());
    };
    let images = match request["type"].as_str() {
        Some("prompt" | "steer" | "followup") => request.get_mut("images"),
        Some("queue_input") => request
            .get_mut("message")
            .and_then(|message| message.get_mut("images")),
        _ => None,
    };
    if let Some(Value::Array(images)) = images {
        for image in images {
            let content = serde_json::from_value(std::mem::take(image))?;
            let prepared = pi_rs_workbench::image_profile::normalize_content(content, profile)?;
            *image = serde_json::to_value(prepared)?;
        }
    }
    Ok(())
}

fn configuration(args: &Cli) -> Value {
    use sha2::{Digest, Sha256};
    let token_fingerprint = args
        .model_token()
        .map(|token| format!("{:x}", Sha256::digest(token.as_bytes())));
    json!({
        "workspace":args.workspace,
        "provider":args.provider.id(),"endpoint":args.model_endpoint_for(&args.provider, Some(&args.model)),"token_fingerprint":token_fingerprint,"extensions":args.extensions,"disabled":args.disable_extension,
        "extension_flags":args.extension_flags,
        "model_profiles":args.model_profiles.revision(),"model_keys":args.model_keys.revision(),
        "block_images":args.settings.block_model_images(),"resize_images":args.settings.resize_model_images(),"image_profile":args.model_image_resize(),
        "project":args.project,"skills":args.skill,"trusted":args.trusted,"orders":args.orders,
        "system_prompt":args.system_prompt,"append_system_prompt":args.append_system_prompt,"instruction":args.instruction,"commands":args.allow_commands,
        "tools":args.tools,"exclude_tools":args.exclude_tools,"no_tools":args.no_tools,"no_builtin_tools":args.no_builtin_tools,
        "no_extensions":args.no_extensions,"no_themes":args.no_themes,"no_skills":args.no_skills,"no_prompt_templates":args.no_prompt_templates,
        "no_context_files":args.no_context_files,"no_mcp":args.no_mcp,"prompt_templates":args.prompt_template,
        "coding_tools":args.coding_tools,"requests":args.max_requests,"output_tokens":args.max_output_tokens,
        "output_budget":args.output_budget,"context_window":args.context_window,
        "model_timeout":args.model_timeout_secs,"http_idle_timeout":args.settings.http_idle_timeout_ms(),"tool_timeout":args.tool_timeout_secs,
        "proxy_fingerprint":args.settings.string("httpProxy").map(|proxy| format!("{:x}", Sha256::digest(proxy.as_bytes())))
    })
}

impl Drop for Worker {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let message = if let Some((reason, target)) = &self.shutdown {
                json!({"type":"shutdown","reason":reason,"target_session_file":target})
            } else {
                json!({"type": if self.persistent { "shutdown" } else { "cancel" }})
            };
            let _ = self.send(message);
            self.stdin.take();
            let deadline = Instant::now()
                + Duration::from_secs(
                    if self.prompt_shutdown
                        || TERMINAL_INTERRUPTED.load(std::sync::atomic::Ordering::SeqCst)
                        || RPC_INTERRUPTED.load(std::sync::atomic::Ordering::SeqCst)
                    {
                        2
                    } else if self.persistent {
                        30
                    } else {
                        5
                    },
                );
            terminate_after_grace(&mut self.child, self.refresh_barrier.as_file(), deadline);
        }
        let _ = self.child.wait();
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }
}

fn terminate_after_grace(child: &mut Child, barrier: &std::fs::File, deadline: Instant) {
    let mut deadline = deadline;
    while child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
        if TERMINAL_INTERRUPTED.load(std::sync::atomic::Ordering::SeqCst)
            || RPC_INTERRUPTED.load(std::sync::atomic::Ordering::SeqCst)
        {
            deadline = deadline.min(Instant::now() + Duration::from_secs(2));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    if child.try_wait().ok().flatten().is_none() {
        // The child locks this file before the token exchange. Holding it while
        // killing prevents a new exchange from starting after the grace period.
        if barrier.lock().is_ok() {
            let _ = child.kill();
            let _ = child.wait();
            let _ = barrier.unlock();
        } else {
            let _ = child.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn settings_bootstrap_frame_preserves_following_input_and_rejects_invalid_lengths() {
        use std::io::Read;
        let settings = super::super::config::Settings::default();
        let bytes = serde_json::to_vec(&settings).unwrap();
        let mut wire = (bytes.len() as u64).to_be_bytes().to_vec();
        wire.extend_from_slice(&bytes);
        wire.extend_from_slice(b"next-control-line\n");
        let mut input = std::io::Cursor::new(wire);
        super::read_settings_frame(&mut input).unwrap();
        let mut rest = String::new();
        input.read_to_string(&mut rest).unwrap();
        assert_eq!(rest, "next-control-line\n");
        assert!(
            super::read_settings_frame(&mut std::io::Cursor::new(u64::MAX.to_be_bytes())).is_err()
        );
        assert!(
            super::read_settings_frame(&mut std::io::Cursor::new(100_u64.to_be_bytes())).is_err()
        );
    }
    use clap::Parser;
    #[cfg(unix)]
    #[test]
    fn rpc_transition_wait_and_worker_shutdown_respond_to_cancellation() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        };
        use std::time::{Duration, Instant};
        let root = tempfile::tempdir().unwrap();
        let mut args = super::Cli::parse_from(["pi"]);
        args.session = root.path().join("session.jsonl");
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take();
        let (sender, events) = mpsc::channel();
        let worker = super::Worker {
            compacting: false,
            retrying: false,
            bashing: false,
            steering_mode: pi_rs::QueueMode::All,
            follow_up_mode: pi_rs::QueueMode::All,
            session: args.session.clone(),
            busy: false,
            persistent: true,
            prompt_shutdown: false,
            shutdown: None,
            configuration: serde_json::Value::Null,
            child,
            stdin,
            events,
            pending_events: Vec::new(),
            extension_sources: Default::default(),
            message_renderer_types: Default::default(),
            settings_snapshot: args.settings.clone(),
            workspace: args.workspace.clone(),
            trusted: args.trusted,
            readers: Vec::new(),
            refresh_barrier: tempfile::NamedTempFile::new().unwrap(),
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let flag = cancelled.clone();
            let task = tokio::task::spawn_blocking(move || {
                let _sender = sender;
                let mut slot = Some(worker);
                let result = super::Worker::check_session_transition_cancellable(
                    Some(&flag),
                    Some(&mut slot),
                    &args,
                    pi_rs::BeforeSessionTransition::Switch {
                        reason: pi_rs::SessionSwitchReason::New,
                        target_session_file: None,
                    },
                );
                assert!(matches!(result, Err(pi_rs::Error::Cancelled)));
            });
            tokio::time::sleep(Duration::from_millis(150)).await;
            cancelled.store(true, Ordering::SeqCst);
            drop(task);
        });
        let start = Instant::now();
        drop(runtime);
        assert!(
            start.elapsed() < Duration::from_secs(4),
            "RPC transition delayed runtime shutdown"
        );
    }

    #[test]
    fn process_shutdown_waits_for_detached_refresh() {
        use std::sync::mpsc;
        use std::time::Duration;
        let active = super::refresh_barrier().unwrap();
        let (sender, receiver) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let _shutdown = super::stop_new_refreshes();
            sender.send(()).unwrap();
        });
        assert!(receiver.recv_timeout(Duration::from_millis(150)).is_err());
        drop(active);
        receiver.recv_timeout(Duration::from_secs(3)).unwrap();
        waiter.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn worker_teardown_waits_for_refresh_barrier_after_grace() {
        use std::sync::mpsc;
        use std::time::{Duration, Instant};
        let barrier = tempfile::NamedTempFile::new().unwrap();
        let held = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(barrier.path())
            .unwrap();
        held.lock().unwrap();
        let mut child = std::process::Command::new("sleep")
            .arg("10")
            .spawn()
            .unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            super::terminate_after_grace(&mut child, barrier.as_file(), Instant::now());
            done_tx.send(()).unwrap();
        });
        assert!(done_rx.recv_timeout(Duration::from_millis(150)).is_err());
        held.unlock().unwrap();
        done_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        waiter.join().unwrap();
    }

    #[tokio::test]
    async fn worker_teardown_waits_for_child_anthropic_refresh_to_persist() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        struct Reap(std::process::Child);
        impl Drop for Reap {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        std::fs::write(&path, serde_json::json!({"anthropic":{"type":"oauth","access":"old","refresh":"old-refresh","expires":0}}).to_string()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let barrier = tempfile::NamedTempFile::new().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "anthropic_auth::tests::share_barrier_refresh_fixture",
                "--nocapture",
            ])
            .env("PI_RS_TEST_SHARE_BARRIER_AUTH", &path)
            .env(
                "PI_RS_TEST_SHARE_BARRIER_URL",
                format!("http://{}/v1/oauth/token", listener.local_addr().unwrap()),
            )
            .env(super::REFRESH_BARRIER_ENV, barrier.path())
            .env("PI_RS_TEST_PAUSE_AUTH_UNLOCK", dir.path())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let mut release = child.stdin.take().unwrap();
        let mut child = Reap(child);
        let (mut socket, _) =
            tokio::time::timeout(std::time::Duration::from_secs(10), listener.accept())
                .await
                .unwrap()
                .unwrap();
        let request = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut bytes = Vec::new();
            let mut buffer = [0; 4096];
            loop {
                let n = socket.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
                if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|value| value.parse().unwrap())
                        })
                        .unwrap();
                    if bytes.len() >= end + 4 + length {
                        break bytes[end + 4..].to_vec();
                    }
                }
            }
        })
        .await
        .unwrap();
        let request: serde_json::Value = serde_json::from_slice(&request).unwrap();
        assert_eq!(request["refresh_token"], "old-refresh");
        let body = r#"{"access_token":"new-access","refresh_token":"rotated","expires_in":3600}"#;
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !dir.path().join("unlock-entered").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let probe = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(barrier.path())
            .unwrap();
        assert!(
            matches!(probe.try_lock(), Err(std::fs::TryLockError::WouldBlock)),
            "worker barrier was released before auth unlock"
        );
        let mut termination = tokio::task::spawn_blocking(move || {
            super::terminate_after_grace(
                &mut child.0,
                barrier.as_file(),
                std::time::Instant::now(),
            );
            child.0.wait().unwrap()
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(150), &mut termination)
                .await
                .is_err(),
            "worker terminated before releasing the auth lock"
        );
        std::io::Write::write_all(&mut release, b"x").unwrap();
        drop(release);
        let status = tokio::time::timeout(std::time::Duration::from_secs(15), termination)
            .await
            .unwrap()
            .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(libc::SIGKILL));
        }
        #[cfg(windows)]
        assert_eq!(status.code(), Some(1));
        let guard = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            super::super::auth_lock::acquire_async(&path),
        )
        .await
        .unwrap()
        .unwrap();
        let stored = super::super::config::read_object_unlocked(&path).unwrap();
        assert_eq!(stored["anthropic"]["access"], "new-access");
        assert_eq!(stored["anthropic"]["refresh"], "rotated");
        drop(guard);
    }

    #[test]
    fn queue_mode_uses_current_branch_even_when_session_is_locked() {
        let root = tempfile::tempdir().unwrap();
        let mut args = super::Cli::parse_from(["pi"]);
        args.session = root.path().join("session.jsonl");
        let mut session = pi_rs::Session::open(&args.session).unwrap();
        session
            .set_queue_mode(pi_rs::InputKind::Steer, pi_rs::QueueMode::All)
            .unwrap();
        assert_eq!(
            super::session_queue_mode(&args, pi_rs::InputKind::Steer).unwrap(),
            pi_rs::QueueMode::All
        );
        assert_eq!(
            super::session_queue_mode(&args, pi_rs::InputKind::FollowUp).unwrap(),
            pi_rs::QueueMode::OneAtATime
        );
    }
}
