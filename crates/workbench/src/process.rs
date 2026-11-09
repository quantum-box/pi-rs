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
    pub(super) child: Child,
    stdin: Option<ChildStdin>,
    pub(super) events: Receiver<Value>,
    pub(super) readers: Vec<JoinHandle<()>>,
}
impl Worker {
    pub(super) fn start(args: &Cli, operation: &[&str]) -> Result<Self> {
        let mut command = Command::new(std::env::current_exe()?);
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
            .arg(args.model_timeout_secs.to_string())
            .arg("--tool-timeout-secs")
            .arg(args.tool_timeout_secs.to_string());
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
        Ok(Self {
            compacting: operation.first() == Some(&"compact"),
            child,
            stdin,
            events,
            readers,
        })
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
impl Drop for Worker {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.send(json!({"type":"cancel"}));
            self.stdin.take();
            let deadline = Instant::now() + Duration::from_secs(5);
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
