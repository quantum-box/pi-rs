//! Host-admitted native extension registration and terminal worker handoff.
use pi_rs::Result;
use pi_rs_extensions::ExecutableExtension;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(transparent)]
pub(super) struct Registrations(BTreeMap<PathBuf, Registration>);

#[derive(Clone, Deserialize, Serialize)]
struct Registration {
    sha256: String,
    // A failed Describe is kept so startup cannot retry it without the sandbox.
    description: Option<Value>,
}

impl Registrations {
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(super) fn load(&self, path: &Path) -> Option<Result<Option<ExecutableExtension>>> {
        #[cfg(target_os = "macos")]
        if !self.is_empty() {
            let canonical = path.canonicalize().ok();
            let (path, registration) = self
                .0
                .get_key_value(path)
                .or_else(|| self.0.get_key_value(canonical.as_ref()?))?;
            return Some((|| {
                let Some(description) = &registration.description else {
                    return Ok(None);
                };
                // Use the original regular file and verify its bytes again.
                // A changed or missing file never retries Describe.
                macos::verified_executable(path, &registration.sha256)?;
                pi_rs_extensions::project_trust::load_described(path, description).map(Some)
            })());
        }
        #[cfg(not(target_os = "macos"))]
        let _ = path;
        None
    }
}

pub(super) fn resolve_initial(args: &mut super::Cli) -> Result<Option<bool>> {
    #[cfg(target_os = "macos")]
    if !args.no_extensions && args.extension_flags.is_empty() {
        return macos::resolve(args);
    }
    super::config::project_trust(&args.workspace, args.explicit_project_trust())
}

#[cfg(target_os = "macos")]
mod macos {
    use super::*;
    use crate::config::{self, ProjectTrustVote};
    use crate::resource_paths::{UserTrustExtension, user_trust_extensions, user_trust_root};
    use pi_rs::{Error, Extension};
    use pi_rs_extensions::project_trust::{Decision, Vote};
    use sha2::{Digest, Sha256};
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Write};
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::os::unix::process::CommandExt;
    use std::process::Command;

    const SANDBOX: &str = "/usr/bin/sandbox-exec";
    const PROFILE: &str = r#"
        (version 1)
        (deny default)
        (allow sysctl-read)
        (allow file-read-metadata (literal "/") (literal "/bin") (literal "/bin/bash")
            (literal "/private") (literal "/private/tmp") (literal (param "RUNDIR"))
            (literal (param "VOTER")) (subpath "/usr/lib") (subpath "/System/Library"))
        (allow file-read-data (literal "/") (literal "/bin/bash") (literal (param "VOTER"))
            (subpath "/usr/lib") (subpath "/System/Library"))
        (allow file-map-executable (literal "/bin/bash") (literal (param "VOTER"))
            (subpath "/usr/lib") (subpath "/System/Library"))
        (allow process-exec (literal "/bin/bash") (literal (param "VOTER")))
        (deny file-read* (subpath (param "WORKSPACE")))
    "#;

    pub(super) fn resolve(args: &mut crate::Cli) -> Result<Option<bool>> {
        let agent = config::agent_dir();
        let explicit = args.explicit_project_trust();
        let registrations = &mut args.trust_registrations.0;
        let cancelled = &args.command_cancel;
        let disabled = &args.disable_extension;
        let decision = config::resolve_project_trust(
            &args.workspace,
            explicit,
            &agent,
            |workspace| {
                let workspace = workspace.to_owned();
                let sources = admission_sources(&agent, &workspace, disabled);
                let (sources, error) = match sources {
                    Ok(sources) => (sources, None),
                    Err(error) => (Vec::new(), Some(Err(error.to_string()))),
                };
                error
                    .into_iter()
                    .chain(sources.into_iter().map(move |source| {
                        let id = source.id.clone();
                        dispatch(&workspace, source, cancelled, registrations)
                            .map_err(|error| format!("{id}: {error}"))
                    }))
            },
            |error| {
                eprintln!(
                    "Warning: project_trust: {}",
                    error
                        .chars()
                        .take(400)
                        .flat_map(char::escape_default)
                        .collect::<String>()
                )
            },
        )?;
        if cancelled.is_cancelled() {
            return Err(Error::Cancelled);
        }
        Ok(decision)
    }

    fn admission_sources(
        agent: &Path,
        workspace: &Path,
        disabled: &[String],
    ) -> Result<Vec<UserTrustExtension>> {
        if !Path::new(SANDBOX).is_file()
            || workspace.starts_with("/System/Library")
            || workspace.starts_with("/usr/lib")
        {
            return Ok(Vec::new());
        }
        let Some(agent) = user_trust_root(agent, workspace) else {
            return Ok(Vec::new());
        };
        if agent.to_str().is_none() || workspace.to_str().is_none() {
            return Ok(Vec::new());
        }
        let file = match private_user_file(&agent.join("settings.json")) {
            Ok(file) => file,
            Err(_) => return Ok(Vec::new()),
        };
        let settings = read_limited(file, 1024 * 1024)?;
        let settings = settings.strip_prefix(b"\xef\xbb\xbf").unwrap_or(&settings);
        let Ok(settings) = serde_json::from_slice(settings) else {
            return Ok(Vec::new());
        };
        user_trust_extensions(&agent, &settings, disabled)
    }

    fn private_user_file(path: &Path) -> Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.mode() & 0o022 != 0
            || metadata.uid() != unsafe { libc::geteuid() }
        {
            return Err(Error::Config(
                "expected a user-owned regular file with no writable aliases".into(),
            ));
        }
        Ok(file)
    }

    fn read_limited(file: File, limit: u64) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        file.take(limit + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > limit {
            return Err(Error::Config(
                "file exceeds the project_trust size limit".into(),
            ));
        }
        Ok(bytes)
    }

    pub(super) fn verified_executable(path: &Path, sha256: &str) -> Result<Vec<u8>> {
        let file = private_user_file(path)?;
        if file.metadata()?.mode() & 0o100 == 0 {
            return Err(Error::Config(
                "project_trust voter must be executable".into(),
            ));
        }
        let bytes = read_limited(file, 64 * 1024 * 1024)?;
        if format!("{:x}", Sha256::digest(&bytes)) != sha256 {
            return Err(Error::Config("project_trust voter SHA-256 changed".into()));
        }
        Ok(bytes)
    }

    fn dispatch(
        workspace: &Path,
        source: UserTrustExtension,
        cancelled: &pi_rs::CancellationToken,
        registrations: &mut BTreeMap<PathBuf, Registration>,
    ) -> std::result::Result<Option<ProjectTrustVote>, String> {
        if cancelled.is_cancelled() {
            return Ok(None);
        }
        let bytes =
            verified_executable(&source.path, &source.sha256).map_err(|error| error.to_string())?;
        let temporary = tempfile::tempdir_in("/private/tmp").map_err(|error| error.to_string())?;
        if temporary.path().starts_with(workspace) {
            return Ok(None);
        }
        let voter = temporary.path().join("voter");
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o500)
            .open(&voter)
            .map_err(|error| error.to_string())?;
        output
            .write_all(&bytes)
            .map_err(|error| error.to_string())?;
        output
            .set_permissions(std::fs::Permissions::from_mode(0o500))
            .map_err(|error| error.to_string())?;
        drop(output);
        let argument = |prefix: &str, path: &Path| {
            let mut argument = std::ffi::OsString::from(prefix);
            argument.push(path.as_os_str());
            argument
        };
        // Caller-supplied file descriptors can bypass filesystem policy. Rust
        // opens its own files/pipes close-on-exec, but inherited fds need care.
        let descriptors: Vec<libc::c_int> = std::fs::read_dir("/dev/fd")
            .map_err(|error| error.to_string())?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter_map(|name| name.to_str()?.parse().ok())
            .filter(|descriptor| *descriptor > 2)
            .collect();
        let mut command = Command::new(SANDBOX);
        unsafe {
            command.pre_exec(move || {
                for &descriptor in &descriptors {
                    if libc::fcntl(descriptor, libc::F_SETFD, libc::FD_CLOEXEC) == -1 {
                        let error = std::io::Error::last_os_error();
                        if error.raw_os_error() != Some(libc::EBADF) {
                            return Err(error);
                        }
                    }
                }
                Ok(())
            });
        }
        command
            .current_dir(temporary.path())
            .env_clear()
            .arg("-p")
            .arg(PROFILE)
            .arg("-D")
            .arg(argument("VOTER=", &voter))
            .arg("-D")
            .arg(argument("RUNDIR=", temporary.path()))
            .arg("-D")
            .arg(argument("WORKSPACE=", workspace))
            .arg(&voter);
        let registration = registrations.entry(source.path).or_insert(Registration {
            sha256: source.sha256,
            description: None,
        });
        let mut description =
            pi_rs_extensions::project_trust::describe(command, workspace, cancelled)?;
        let extension = pi_rs_extensions::project_trust::load_described(&voter, &description)
            .map_err(|error| error.to_string())?;
        if extension.id() != source.id {
            return Err("Describe ID does not match the user admission".into());
        }
        let object = description
            .as_object_mut()
            .expect("validated description object");
        let vote = object.remove("project_trust");
        let error = object.remove("project_trust_error");
        registration.description = Some(description);
        if let Some(error) = error {
            return Err(error
                .as_str()
                .unwrap_or("invalid project_trust error")
                .to_owned());
        }
        let Some(vote) = vote else { return Ok(None) };
        let vote: Vote = serde_json::from_value(vote).map_err(|error| error.to_string())?;
        let trusted = match vote.trusted {
            Decision::Yes => true,
            Decision::No => false,
            Decision::Undecided => return Ok(None),
        };
        Ok(Some(ProjectTrustVote {
            trusted,
            remember: vote.remember,
        }))
    }
}
