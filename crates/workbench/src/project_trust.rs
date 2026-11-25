//! Host-admitted native extension registration and terminal worker handoff.
use pi_rs::{Error, Result};
use pi_rs_extensions::ExecutableExtension;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The directory approved for this terminal activation, also checked by its worker.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct WorkspaceIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
    created: Option<std::time::SystemTime>,
    #[serde(skip)]
    _directory: Option<std::sync::Arc<std::fs::File>>,
}

impl WorkspaceIdentity {
    pub(super) fn capture(path: &Path) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
            let path = path.canonicalize()?;
            let directory = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(&path)?;
            let metadata = directory.metadata()?;
            let created = if cfg!(target_os = "macos") {
                Some(metadata.created()?)
            } else {
                metadata.created().ok()
            };
            Ok(Self {
                path,
                device: metadata.dev(),
                inode: metadata.ino(),
                created,
                _directory: Some(directory.into()),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Err(Error::Config(
                "project workspace identity is unsupported".into(),
            ))
        }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn verify(&self, path: &Path) -> Result<()> {
        self.reopen(path).map(drop)
    }

    pub(super) fn reopen(&self, path: &Path) -> Result<Self> {
        let current = Self::capture(path)?;
        if current.path != self.path
            || current.device != self.device
            || current.inode != self.inode
            || current.created != self.created
        {
            return Err(Error::Config(
                "project workspace changed; reopen it to resolve trust again".into(),
            ));
        }
        Ok(current)
    }
}

/// Resolve the target before deciding whether an untrusted workspace contains it.
pub(super) fn outside_untrusted_workspace(args: &super::Cli, path: &Path) -> Result<bool> {
    if args.trusted {
        return Ok(true);
    }
    args.verify_project_workspace()?;
    let workspace = args.workspace.canonicalize()?;
    let Ok(path) = path.canonicalize() else {
        return Ok(false);
    };
    if path.starts_with(&workspace) {
        return Ok(false);
    }
    let workspace = same_file::Handle::from_path(workspace)?;
    let ancestors = path.ancestors().skip(usize::from(!path.is_dir()));
    for ancestor in ancestors {
        if same_file::Handle::from_path(ancestor)? == workspace {
            return Ok(false);
        }
    }
    Ok(true)
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(transparent)]
pub(super) struct Registrations(BTreeMap<PathBuf, Registration>);

#[derive(Clone, Deserialize, Serialize)]
struct Registration {
    sha256: String,
    verified: bool,
    // A failed Describe is kept so startup cannot retry it without the sandbox.
    description: Option<Value>,
}

impl Registrations {
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(super) fn load(
        &self,
        path: &Path,
        trusted_workspace: Option<&Path>,
    ) -> Option<Result<Option<ExecutableExtension>>> {
        #[cfg(target_os = "macos")]
        if !self.is_empty() {
            let canonical = path.canonicalize().ok();
            let trusted_workspace = trusted_workspace.and_then(|path| path.canonicalize().ok());
            let (path, registration) = self
                .0
                .get_key_value(path)
                .or_else(|| self.0.get_key_value(canonical.as_ref()?))
                .or_else(|| {
                    // Discovery and package sources may already have
                    // dereferenced a replaced admission path. Retain the
                    // original policy for that target, including failures.
                    let canonical = canonical.as_ref()?;
                    self.0.iter().find(|(source, registration)| {
                        // An initially ineligible user alias must not shadow a
                        // separate project executable after project approval.
                        (registration.verified
                            || !trusted_workspace
                                .as_ref()
                                .is_some_and(|workspace| canonical.starts_with(workspace)))
                            && source.canonicalize().is_ok_and(|path| &path == canonical)
                    })
                })?;
            return Some((|| {
                let Some(description) = &registration.description else {
                    return Ok(None);
                };
                // Use the original regular file and verify its bytes again.
                // A changed or missing file never retries Describe.
                let bytes = macos::verified_executable(path, &registration.sha256)?;
                pi_rs_extensions::project_trust::load_described(path, description, bytes).map(Some)
            })());
        }
        #[cfg(not(target_os = "macos"))]
        let _ = (path, trusted_workspace);
        None
    }
}

pub(super) fn resolve_initial(args: &mut super::Cli) -> Result<Option<bool>> {
    #[cfg(target_os = "macos")]
    if !args.no_extensions && args.extension_flags.is_empty() {
        let identity = args
            .project_workspace
            .clone()
            .ok_or_else(|| Error::Config("missing workspace identity for project trust".into()))?;
        return macos::resolve(args, &identity);
    }
    super::config::project_trust(&args.workspace, args.explicit_project_trust())
}

#[cfg(target_os = "macos")]
mod macos {
    use super::*;
    use crate::config::{self, ProjectTrustVote};
    use crate::resource_paths::{UserTrustExtension, user_trust_extensions, user_trust_root};
    use pi_rs::Extension;
    use pi_rs_extensions::project_trust::{Decision, Vote};
    use sha2::{Digest, Sha256};
    use std::fs::{File, OpenOptions};
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
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

    pub(super) fn resolve(
        args: &mut crate::Cli,
        identity: &WorkspaceIdentity,
    ) -> Result<Option<bool>> {
        let agent = config::agent_dir();
        let explicit = args.explicit_project_trust();
        let registrations = &mut args.trust_registrations.0;
        let cancelled = &args.command_cancel;
        let disabled = &args.disable_extension;
        let decision = config::resolve_project_trust_guarded(
            &args.workspace,
            explicit,
            &agent,
            Some(identity),
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
            || {
                if cancelled.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                identity.verify(&args.workspace)
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
        let registration = registrations
            .entry(source.path.clone())
            .or_insert(Registration {
                sha256: source.sha256,
                verified: false,
                description: None,
            });
        let bytes = verified_executable(&source.path, &registration.sha256)
            .map_err(|error| error.to_string())?;
        registration.verified = true;
        let (temporary, voter) =
            pi_rs_extensions::project_trust::stage(&bytes).map_err(|error| error.to_string())?;
        let workspace_handle =
            same_file::Handle::from_path(workspace).map_err(|error| error.to_string())?;
        for ancestor in temporary.path().ancestors() {
            if same_file::Handle::from_path(ancestor).map_err(|error| error.to_string())?
                == workspace_handle
            {
                return Err("native voter scratch overlaps the workspace".into());
            }
        }
        let argument = |prefix: &str, path: &Path| {
            let mut argument = std::ffi::OsString::from(prefix);
            argument.push(path.as_os_str());
            argument
        };
        // Caller-supplied file descriptors can bypass filesystem policy. Rust
        // opens its own files/pipes close-on-exec, but inherited fds need care.
        let highest_open = std::fs::read_dir("/dev/fd")
            .map_err(|error| error.to_string())?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter_map(|name| name.to_str()?.parse::<libc::c_int>().ok())
            .max()
            .unwrap_or(2);
        let mut max_files: libc::c_int = 0;
        let mut length = std::mem::size_of_val(&max_files);
        let status = unsafe {
            libc::sysctlbyname(
                c"kern.maxfilesperproc".as_ptr(),
                std::ptr::from_mut(&mut max_files).cast(),
                &mut length,
                std::ptr::null_mut(),
                0,
            )
        };
        let last_descriptor = highest_open.max(max_files);
        if status != 0
            || length != std::mem::size_of_val(&max_files)
            || max_files < 3
            || last_descriptor > 1_048_576
        {
            return Err("could not bound inherited native voter descriptors".into());
        }
        let mut command = Command::new(SANDBOX);
        unsafe {
            command.pre_exec(move || {
                // Scan after fork; a descriptor opened by another thread
                // between the parent scan and spawn is also covered.
                for descriptor in 3..=last_descriptor {
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
        let mut description =
            pi_rs_extensions::project_trust::describe(command, workspace, cancelled)?;
        let extension =
            pi_rs_extensions::project_trust::load_described(&source.path, &description, bytes)
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
