//! Shared subprocess transport for terminal and RPC frontends.
use super::Cli;
use pi_rs::{Error, Result, SessionShutdownReason, SessionStartReason};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub(super) const LIVE_CONTROLS_ENV: &str = "PI_RS_WORKER_LIVE_CONTROLS";
pub(super) const SESSION_TRANSITION_ENV: &str = "PI_RS_WORKER_SESSION_TRANSITION";

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
    shutdown: Option<(SessionShutdownReason, Option<std::path::PathBuf>)>,
    configuration: Value,
    pub(super) child: Child,
    stdin: Option<ChildStdin>,
    events: Receiver<Value>,
    pending_events: Vec<Value>,
    pub(super) readers: Vec<JoinHandle<()>>,
}
impl Worker {
    pub(super) fn start(args: &Cli, operation: &[&str]) -> Result<Self> {
        Self::start_with_transition(args, operation, None)
    }

    fn start_with_transition(
        args: &Cli,
        operation: &[&str],
        transition: Option<(SessionStartReason, Option<std::path::PathBuf>)>,
    ) -> Result<Self> {
        let steering_mode = session_queue_mode(args, pi_rs::InputKind::Steer)?;
        let follow_up_mode = session_queue_mode(args, pi_rs::InputKind::FollowUp)?;
        let mut command = Command::new(std::env::current_exe()?);
        command.env_remove("PI_RS_MODEL_TOKEN");
        command.env_remove(SESSION_TRANSITION_ENV);
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
        let stdin = child.stdin.take();
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
        Ok(Self {
            compacting: operation.first() == Some(&"compact"),
            retrying: false,
            bashing: false,
            steering_mode,
            follow_up_mode,
            session: args.session.clone(),
            busy: !persistent,
            persistent,
            shutdown: None,
            configuration: configuration(args),
            child,
            stdin,
            events,
            pending_events: Vec::new(),
            readers,
        })
    }
    pub(super) fn start_request(args: &Cli, request: Value) -> Result<Self> {
        let mut worker = Self::start(args, &["serve"])?;
        worker.request(args, request)?;
        Ok(worker)
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
            slot.take();
            *slot = Some(Self::start_request(args, request)?);
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
            slot.take();
            *slot = Some(Self::start(args, &["serve"])?);
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
    }

    pub(super) fn check_session_transition(
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
            let event = active
                .events
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .map_err(|error| {
                    Error::Config(format!("could not check the session transition: {error}"))
                })?;
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
            let received = active
                .events
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .map_err(|error| {
                    Error::Config(format!("could not notify model selection: {error}"))
                })?;
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
        let pending_events = std::mem::take(&mut active.pending_events);
        drop(slot.take());
        let mut next = Self::start_with_transition(args, &["serve"], Some((reason, previous)))?;
        next.pending_events = pending_events;
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
        self.send(json!({"type":"configure","model":args.model,"thinking":args.thinking,"reported_thinking":super::thinking::reported(args).is_some(),"session":args.session}))?;
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
            let deadline =
                Instant::now() + Duration::from_secs(if self.persistent { 30 } else { 5 });
            while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
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
