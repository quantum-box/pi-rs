//! Local Bash and PowerShell resolution and per-session tool metadata.
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use pi_rs::{Error, Result};
use tokio::process::Command;

const SESSION_ENVIRONMENT: &[&str] = &[
    "PI_SESSION_ID",
    "PI_SESSION_FILE",
    "PI_PROVIDER",
    "PI_MODEL",
    "PI_REASONING_LEVEL",
];

#[derive(Clone, Default)]
pub struct ShellConfig {
    path: Option<String>,
    prefix: Option<String>,
    session: Arc<RwLock<Option<ShellContext>>>,
}

pub struct ShellContext {
    pub id: uuid::Uuid,
    pub path: Option<PathBuf>,
    pub provider: String,
    pub model: String,
    pub reasoning_level: Option<String>,
}

pub(super) struct Launch {
    pub command: Command,
    pub input: Option<String>,
}

impl ShellConfig {
    pub fn new(path: Option<&str>, prefix: Option<&str>) -> Self {
        Self {
            path: path.filter(|value| !value.is_empty()).map(str::to_owned),
            prefix: prefix.filter(|value| !value.is_empty()).map(str::to_owned),
            ..Self::default()
        }
    }

    pub fn set_context(&self, context: ShellContext) {
        *self.session.write().expect("shell session context") = Some(context);
    }

    pub(super) fn uses_custom_bash(&self) -> bool {
        self.path.is_some() || self.prefix.is_some()
    }

    pub(super) fn launch(
        &self,
        root: &Path,
        powershell: bool,
        local: bool,
        script: &str,
    ) -> Result<Launch> {
        let mut launch = if powershell {
            let executable = powershell_executable();
            let mut command = Command::new(executable);
            command.args(["-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command"])
                .arg(format!("$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new(); {script}"));
            Launch {
                command,
                input: None,
            }
        } else {
            let executable = if local {
                self.bash_executable(root)?
            } else {
                "bash".into()
            };
            let legacy_wsl = is_legacy_wsl_bash(&executable);
            let mut command = Command::new(executable);
            let script = self
                .prefix
                .as_ref()
                .map_or_else(|| script.to_owned(), |prefix| format!("{prefix}\n{script}"));
            let input = if legacy_wsl {
                command.arg("-s");
                Some(script)
            } else {
                command.args(["-c", &script]);
                None
            };
            Launch { command, input }
        };
        if local {
            for name in SESSION_ENVIRONMENT {
                launch.command.env_remove(name);
            }
            if let Some(session) = &*self.session.read().expect("shell session context") {
                launch
                    .command
                    .env("PI_SESSION_ID", session.id.to_string())
                    .env("PI_PROVIDER", &session.provider)
                    .env("PI_MODEL", &session.model);
                if let Some(path) = &session.path {
                    launch.command.env("PI_SESSION_FILE", path);
                }
                if let Some(level) = &session.reasoning_level {
                    launch.command.env("PI_REASONING_LEVEL", level);
                }
            }
        }
        Ok(launch)
    }

    fn bash_executable(&self, root: &Path) -> Result<PathBuf> {
        if let Some(path) = &self.path {
            let path = crate::coding_paths::resolve(root, &crate::coding_paths::normalize(path)?);
            if path.is_file() {
                return Ok(path);
            }
            return Err(Error::Config(format!(
                "Custom shell path not found: {}",
                path.display()
            )));
        }
        if cfg!(windows) {
            return ["ProgramFiles", "ProgramFiles(x86)"]
                .into_iter()
                .filter_map(std::env::var_os)
                .map(|folder| {
                    PathBuf::from(folder)
                        .join("Git")
                        .join("bin")
                        .join("bash.exe")
                })
                .find(|path| path.is_file())
                .or_else(|| on_path("bash.exe"))
                .ok_or_else(|| {
                    Error::Config(
                        "No Bash executable found. Set shellPath or install Git for Windows."
                            .into(),
                    )
                });
        }
        let system = Path::new("/bin/bash");
        Ok(if system.is_file() {
            system.into()
        } else {
            on_path("bash").unwrap_or_else(|| "sh".into())
        })
    }
}

fn powershell_executable() -> PathBuf {
    on_path("pwsh.exe")
        .or_else(|| {
            let path = PathBuf::from(std::env::var_os("ProgramFiles")?)
                .join("PowerShell")
                .join("7")
                .join("pwsh.exe");
            path.is_file().then_some(path)
        })
        .or_else(|| on_path("powershell.exe"))
        .or_else(|| {
            let path = PathBuf::from(std::env::var_os("SystemRoot")?)
                .join("System32")
                .join("WindowsPowerShell")
                .join("v1.0")
                .join("powershell.exe");
            path.is_file().then_some(path)
        })
        .unwrap_or_else(|| "powershell.exe".into())
}

fn on_path(name: &str) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|folder| folder.join(name))
        .find(|path| path.is_file())
}

fn is_legacy_wsl_bash(path: &Path) -> bool {
    if !cfg!(windows) || !path.is_absolute() {
        return false;
    }
    let system_root = std::env::var_os("SystemRoot")
        .map(PathBuf::from)
        .or_else(|| {
            let root = path.parent()?.parent()?;
            root.file_name()?
                .eq_ignore_ascii_case("Windows")
                .then(|| root.to_owned())
        });
    system_root
        .filter(|root| root.is_absolute())
        .is_some_and(|root| {
            ["System32", "Sysnative"].iter().any(|directory| {
                path.as_os_str()
                    .eq_ignore_ascii_case(root.join(directory).join("bash.exe").as_os_str())
            })
        })
}
