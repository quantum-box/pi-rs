//! Local Bash and PowerShell resolution and per-session tool metadata.
use std::collections::BTreeMap;
use std::ffi::OsString;
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

#[derive(Clone)]
pub struct ShellConfig {
    path: Option<String>,
    prefix: Option<String>,
    session: Arc<RwLock<Option<ShellSession>>>,
    expose_session_environment: bool,
    spawn_hook: Option<Arc<ShellSpawnHook>>,
    inherit_remote_environment: bool,
}

/// Inputs a model-invoked shell tool passes to its execution backend.
pub struct ShellSpawnContext {
    pub command: String,
    pub cwd: PathBuf,
    pub env: BTreeMap<OsString, OsString>,
}

pub type ShellSpawnHook = dyn Fn(ShellSpawnContext) -> Result<ShellSpawnContext> + Send + Sync;

impl Default for ShellConfig {
    fn default() -> Self {
        Self {
            path: None,
            prefix: None,
            session: Arc::default(),
            expose_session_environment: true,
            spawn_hook: None,
            inherit_remote_environment: false,
        }
    }
}

pub struct ShellContext {
    pub id: uuid::Uuid,
    pub path: Option<PathBuf>,
    pub provider: String,
    pub model: String,
    pub reasoning_level: Option<String>,
}

struct ShellSession {
    context: ShellContext,
    external_id: Option<String>,
}

pub(super) struct Launch {
    pub command: Command,
    pub input: Option<String>,
    pub cwd: PathBuf,
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
        *self.session.write().expect("shell session context") = Some(ShellSession {
            context,
            external_id: None,
        });
    }

    /// Use an externally visible Pi ID while retaining the UUID in SDK context.
    pub fn set_context_with_external_id(&self, context: ShellContext, external_id: String) {
        *self.session.write().expect("shell session context") = Some(ShellSession {
            context,
            external_id: Some(external_id),
        });
    }

    /// Set a hook for model-invoked shell tools; user `!` commands bypass it.
    pub fn with_spawn_hook(mut self, hook: Arc<ShellSpawnHook>) -> Self {
        self.spawn_hook = Some(hook);
        self
    }

    /// Control whether model-invoked tools receive PI_* session metadata.
    pub fn with_session_environment(mut self, enabled: bool) -> Self {
        self.expose_session_environment = enabled;
        self
    }

    /// Opt in to passing the host environment to an injected shell backend.
    pub fn with_remote_environment(mut self, inherit: bool) -> Self {
        self.inherit_remote_environment = inherit;
        self
    }

    pub(super) fn command_with_prefix(&self, script: &str) -> String {
        self.prefix
            .as_ref()
            .map_or_else(|| script.to_owned(), |prefix| format!("{prefix}\n{script}"))
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
        model_tool: bool,
    ) -> Result<Launch> {
        let context = self.spawn_context(root, local, script, model_tool, true)?;
        let script = context.command;
        let mut launch = if powershell {
            let executable = powershell_executable();
            let mut command = Command::new(executable);
            command.args(["-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command"])
                .arg(format!("$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new(); {script}"));
            Launch {
                command,
                input: None,
                cwd: context.cwd,
            }
        } else {
            let executable = if local {
                self.bash_executable(root)?
            } else {
                "bash".into()
            };
            let legacy_wsl = is_legacy_wsl_bash(&executable);
            let mut command = Command::new(executable);
            let input = if legacy_wsl {
                command.arg("-s");
                Some(script)
            } else {
                command.args(["-c", &script]);
                None
            };
            Launch {
                command,
                input,
                cwd: context.cwd,
            }
        };
        launch.command.env_clear().envs(context.env);
        Ok(launch)
    }

    pub(super) fn spawn_context(
        &self,
        root: &Path,
        local: bool,
        script: &str,
        model_tool: bool,
        local_backend: bool,
    ) -> Result<ShellSpawnContext> {
        let mut context = ShellSpawnContext {
            command: self.command_with_prefix(script),
            cwd: root.to_path_buf(),
            env: if local_backend || self.inherit_remote_environment {
                std::env::vars_os().collect()
            } else {
                BTreeMap::new()
            },
        };
        if local_backend || self.inherit_remote_environment {
            let path_key = context
                .env
                .keys()
                .find(|key| key.to_string_lossy().eq_ignore_ascii_case("PATH"))
                .cloned()
                .unwrap_or_else(|| "PATH".into());
            let path = context.env.get(&path_key).cloned().unwrap_or_default();
            let bin = crate::agent_dir().join("bin");
            let paths: Vec<_> = std::env::split_paths(&path).collect();
            if !paths.contains(&bin) {
                let path = if path.is_empty() {
                    bin.into_os_string()
                } else {
                    std::env::join_paths(std::iter::once(bin).chain(paths))
                        .map_err(|error| Error::Config(format!("invalid shell PATH: {error}")))?
                };
                context.env.insert(path_key, path);
            }
        }
        if model_tool {
            for name in SESSION_ENVIRONMENT {
                context.env.remove(&OsString::from(name));
            }
            if local
                && (local_backend || self.inherit_remote_environment)
                && self.expose_session_environment
                && let Some(stored) = &*self.session.read().expect("shell session context")
            {
                let session = &stored.context;
                let session_id = stored
                    .external_id
                    .clone()
                    .unwrap_or_else(|| session.id.to_string());
                // An imported ID may contain NUL, which cannot be passed to a
                // child process. Do not fail shell execution or substitute a
                // different identity in that case.
                if !session_id.contains('\0') {
                    context
                        .env
                        .insert("PI_SESSION_ID".into(), session_id.into());
                }
                context
                    .env
                    .insert("PI_PROVIDER".into(), session.provider.clone().into());
                context
                    .env
                    .insert("PI_MODEL".into(), session.model.clone().into());
                if let Some(path) = &session.path {
                    context
                        .env
                        .insert("PI_SESSION_FILE".into(), path.as_os_str().to_owned());
                }
                if let Some(level) = &session.reasoning_level {
                    context
                        .env
                        .insert("PI_REASONING_LEVEL".into(), level.into());
                }
            }
        }
        if model_tool && let Some(hook) = &self.spawn_hook {
            context = hook(context)?;
        }
        Ok(context)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_session_identity_is_forwarded_safely_and_can_be_reset() {
        let config = ShellConfig::default();
        let id = uuid::Uuid::new_v4();
        let context = || ShellContext {
            id,
            path: None,
            provider: "fixture".into(),
            model: "model".into(),
            reasoning_level: None,
        };
        let root = Path::new(".");
        config.set_context_with_external_id(context(), "legacy ID 日本語".into());
        let spawn = config
            .spawn_context(root, true, "echo", true, true)
            .unwrap();
        assert_eq!(
            spawn
                .env
                .get(std::ffi::OsStr::new("PI_SESSION_ID"))
                .unwrap(),
            "legacy ID 日本語"
        );
        config.set_context_with_external_id(context(), "bad\0id".into());
        let spawn = config
            .spawn_context(root, true, "echo", true, true)
            .unwrap();
        assert!(
            !spawn
                .env
                .contains_key(std::ffi::OsStr::new("PI_SESSION_ID"))
        );
        assert_eq!(
            spawn.env.get(std::ffi::OsStr::new("PI_MODEL")).unwrap(),
            "model"
        );
        config.set_context(context());
        let spawn = config
            .spawn_context(root, true, "echo", true, true)
            .unwrap();
        assert_eq!(
            spawn
                .env
                .get(std::ffi::OsStr::new("PI_SESSION_ID"))
                .unwrap(),
            &OsString::from(id.to_string())
        );
    }
}
