//! Shared subprocess transport for terminal and RPC frontends.
use super::Cli;
use pi_rs::{Error, Result};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub(super) struct Worker {
    pub(super) compacting: bool,
    pub(super) bashing: bool,
    pub(super) steering_mode: pi_rs::QueueMode,
    pub(super) follow_up_mode: pi_rs::QueueMode,
    session: std::path::PathBuf,
    pub(super) busy: bool,
    persistent: bool,
    configuration: Value,
    pub(super) child: Child,
    stdin: Option<ChildStdin>,
    pub(super) events: Receiver<Value>,
    pub(super) readers: Vec<JoinHandle<()>>,
}
impl Worker {
    pub(super) fn start(args: &Cli, operation: &[&str]) -> Result<Self> {
        let steering_mode = super::backend::configured_queue_mode(args, "steeringMode")?;
        let follow_up_mode = super::backend::configured_queue_mode(args, "followUpMode")?;
        let mut command = Command::new(std::env::current_exe()?);
        if let Some(token) = args.model_token() {
            command.env("PI_RS_MODEL_TOKEN", token);
        }

        command
            .arg("--session")
            .arg(&args.session)
            .arg("--workspace")
            .arg(&args.workspace)
            .args([
                "--model",
                &args.model,
                "--endpoint",
                &args.endpoint,
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
        if let Some(timeout) = args.tool_timeout_secs {
            command.arg("--tool-timeout-secs").arg(timeout.to_string());
        }
        if let Some(thinking) = &args.thinking {
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
            bashing: false,
            steering_mode,
            follow_up_mode,
            session: args.session.clone(),
            busy: !persistent,
            persistent,
            configuration: configuration(args),
            child,
            stdin,
            events,
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
            slot.take();
            *slot = Some(Self::start_request(args, request)?);
            Ok(())
        }
    }
    pub(super) fn control(slot: &mut Option<Self>, args: &Cli, request: Value) -> Result<()> {
        if slot
            .as_ref()
            .is_none_or(|worker| !worker.matches(args) || worker.session != args.session)
        {
            if slot.as_ref().is_some_and(|worker| worker.busy) {
                return Err(Error::Config(
                    "cannot redirect controls for an active session".into(),
                ));
            }
            slot.take();
            *slot = Some(Self::start(args, &["serve"])?);
        }
        slot.as_mut().expect("queue worker").send(request)
    }
    pub(super) fn matches(&self, args: &Cli) -> bool {
        self.persistent && self.configuration == configuration(args)
    }
    pub(super) fn request(&mut self, args: &Cli, request: Value) -> Result<()> {
        self.send(json!({"type":"configure","model":args.model,"thinking":args.thinking,"session":args.session}))?;
        self.compacting = request["type"] == "compact";
        self.bashing = request["type"] == "bash";
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
fn configuration(args: &Cli) -> Value {
    use sha2::{Digest, Sha256};
    let token_fingerprint = args
        .model_token()
        .map(|token| format!("{:x}", Sha256::digest(token.as_bytes())));
    json!({
        "workspace":args.workspace,
        "endpoint":args.endpoint,"token_fingerprint":token_fingerprint,"extensions":args.extensions,"disabled":args.disable_extension,
        "project":args.project,"skills":args.skill,"trusted":args.trusted,"orders":args.orders,
        "system_prompt":args.system_prompt,"append_system_prompt":args.append_system_prompt,"instruction":args.instruction,"commands":args.allow_commands,
        "tools":args.tools,"exclude_tools":args.exclude_tools,"no_tools":args.no_tools,"no_builtin_tools":args.no_builtin_tools,
        "no_extensions":args.no_extensions,"no_themes":args.no_themes,"no_skills":args.no_skills,"no_prompt_templates":args.no_prompt_templates,
        "no_context_files":args.no_context_files,"no_mcp":args.no_mcp,"prompt_templates":args.prompt_template,
        "coding_tools":args.coding_tools,"requests":args.max_requests,"output_tokens":args.max_output_tokens,
        "output_budget":args.output_budget,"context_window":args.context_window,
        "model_timeout":args.model_timeout_secs,"tool_timeout":args.tool_timeout_secs
    })
}

impl Drop for Worker {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.send(json!({"type": if self.persistent { "shutdown" } else { "cancel" }}));
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
