//! OSC 7577 reports for terminal-managed session restoration.
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use pi_rs::{Error, Result, Session};
use sha2::{Digest, Sha256};

use super::{Cli, Command};

pub(super) const TOKEN_ENV: &str = "OSC7577_TOKEN";
const MAX_FRAME_BYTES: usize = 2048;
const MAX_ARGV_ELEMENTS: usize = 64;

#[derive(Clone, Default)]
pub(super) struct ExplicitLimits {
    max_requests: Option<usize>,
    max_output_tokens: Option<u32>,
    output_budget: Option<u32>,
    model_timeout_secs: Option<u64>,
    context_window: Option<u32>,
}

impl ExplicitLimits {
    pub fn from_matches(matches: &clap::ArgMatches) -> Self {
        fn explicit<T: Clone + Send + Sync + 'static>(
            matches: &clap::ArgMatches,
            name: &str,
        ) -> Option<T> {
            (matches.value_source(name) == Some(clap::parser::ValueSource::CommandLine))
                .then(|| matches.get_one::<T>(name).cloned())
                .flatten()
        }
        Self {
            max_requests: explicit(matches, "max_requests"),
            max_output_tokens: explicit(matches, "max_output_tokens"),
            output_budget: explicit(matches, "output_budget"),
            model_timeout_secs: explicit(matches, "model_timeout_secs"),
            context_window: explicit(matches, "context_window"),
        }
    }

    fn append_args(&self, argv: &mut Vec<String>) {
        for (flag, value) in [
            (
                "max-requests",
                self.max_requests.map(|value| value.to_string()),
            ),
            (
                "max-output-tokens",
                self.max_output_tokens.map(|value| value.to_string()),
            ),
            (
                "output-budget",
                self.output_budget.map(|value| value.to_string()),
            ),
            (
                "model-timeout-secs",
                self.model_timeout_secs.map(|value| value.to_string()),
            ),
            (
                "context-window",
                self.context_window.map(|value| value.to_string()),
            ),
        ] {
            if let Some(value) = value {
                argv.push(format!("--{flag}={value}"));
            }
        }
    }
}

/// Validate the explicit workspace before looking at journal metadata or trust.
pub(super) fn bind_startup(args: &mut Cli) -> Result<()> {
    if args.command.is_some()
        || args.mode.is_some()
        || args.print
        || !args.prompt.is_empty()
        || args.list_models.is_some()
        || args.export_session.is_some()
        || args.ephemeral_worker_session
        || args.terminal_managed_worker
        || args.stdin_control
    {
        return Err(Error::Config(
            "terminal resume requires an interactive session".into(),
        ));
    }
    args.workspace = canonical_workspace(&args.workspace)
        .filter(|workspace| *workspace == args.workspace)
        .ok_or_else(|| Error::Config("terminal resume requires a canonical workspace".into()))?;
    snapshot_bound_session(args)?;
    args.bind_existing_session();
    args.command = Some(Command::Interactive);
    Ok(())
}

pub(super) fn bind_worker(args: &mut Cli) -> Result<()> {
    if !matches!(args.command, Some(Command::Serve))
        || !args.terminal_managed_worker
        || !args.stdin_control
    {
        return Err(Error::Config(
            "terminal resume worker requires a managed serve command".into(),
        ));
    }
    snapshot_bound_session(args)?;
    args.bind_existing_session();
    Ok(())
}

pub(super) fn needs_fresh_approval(args: &Cli) -> bool {
    args.resume_requires_existing()
        && !args.terminal_resume_approved
        && (args.terminal_resume_confirm_project || !protected_project(&args.workspace, args))
}

pub(super) fn snapshot_bound_session(args: &Cli) -> Result<Session> {
    let (path, workspace) = args
        .bound_resume_paths(&args.session)
        .unwrap_or((&args.session, &args.workspace));
    require_journal_path(path)?;
    let session = Session::snapshot(path)?;
    check_workspace(&session, workspace)?;
    Ok(session)
}

pub(super) fn open_bound_session(args: &Cli) -> Result<Session> {
    open_bound_session_at(args, &args.session)
}

pub(super) fn open_bound_session_at(args: &Cli, path: &Path) -> Result<Session> {
    let (path, workspace) = args
        .bound_resume_paths(path)
        .ok_or_else(|| Error::Config("terminal resume journal has no bound workspace".into()))?;
    require_journal_path(path)?;
    let session = Session::open_existing(path)?;
    check_workspace(&session, workspace)?;
    Ok(session)
}

fn check_workspace(session: &Session, workspace: &Path) -> Result<()> {
    if session.host_binding().is_some() {
        return Err(Error::Session(
            "a managed session must be opened by its host".into(),
        ));
    }
    if session
        .working_directory()
        .is_some_and(|cwd| Path::new(cwd) != workspace)
    {
        return Err(Error::Config(
            "terminal resume journal workspace differs from the registered workspace".into(),
        ));
    }
    Ok(())
}

fn require_journal_path(path: &Path) -> Result<()> {
    if protected_journal_path(path).as_deref() != Some(path) {
        return Err(Error::Config(
            "terminal resume requires an existing private regular journal at a canonical path"
                .into(),
        ));
    }
    Ok(())
}

fn canonical_workspace(path: &Path) -> Option<PathBuf> {
    let path = path.canonicalize().ok()?;
    path.is_dir().then_some(path)
}

#[cfg(unix)]
fn protected_journal_path(path: &Path) -> Option<PathBuf> {
    use std::os::unix::fs::MetadataExt;

    // Publish the canonical parent and keep the final entry itself intact.
    let path = std::path::absolute(path).ok()?;
    let parent = path.parent()?.canonicalize().ok()?;
    let path = parent.join(path.file_name()?);
    let metadata = std::fs::symlink_metadata(&path).ok()?;
    // SAFETY: geteuid has no arguments or memory-safety preconditions.
    let uid = unsafe { libc::geteuid() };
    if !metadata.is_file()
        || metadata.uid() != uid
        || metadata.mode() & 0o077 != 0
        || metadata.nlink() != 1
        || !enforces_posix_permissions(&path)
        || !has_no_granting_acl(&path)
        || !protected_directories(&parent, uid, true)
    {
        return None;
    }
    Some(path)
}

#[cfg(unix)]
fn protected_workspace(path: &Path) -> bool {
    // SAFETY: geteuid has no arguments or memory-safety preconditions.
    let uid = unsafe { libc::geteuid() };
    canonical_workspace(path).as_deref() == Some(path) && protected_directories(path, uid, false)
}

#[cfg(unix)]
fn protected_project(workspace: &Path, args: &Cli) -> bool {
    use super::resource_paths::{project_agent_skills, project_protection_roots};

    if !protected_workspace(workspace) {
        return false;
    }
    // SAFETY: geteuid has no arguments or memory-safety preconditions.
    let uid = unsafe { libc::geteuid() };
    let project = workspace.join(".pi");
    let Some(project_metadata) = protected_project_entry(&project, uid) else {
        return false;
    };
    if project_metadata.is_some_and(|metadata| !metadata.is_dir()) {
        return false;
    }
    let settings = project.join("settings.json");
    let mcp = project.join("mcp.json");
    for path in [
        settings.clone(),
        mcp,
        project.join("SYSTEM.md"),
        project.join("APPEND_SYSTEM.md"),
    ] {
        match protected_project_entry(&path, uid) {
            Some(metadata) if metadata.as_ref().is_none_or(|metadata| metadata.is_file()) => (),
            _ => return false,
        }
    }
    // Only read declarations after their path passes the filesystem checks.
    let Ok(settings) = super::config::read_object(&settings) else {
        return false;
    };
    let mcp_requested = !args.no_mcp
        && (!args.no_extensions || args.extensions.iter().any(|entry| entry == "builtin:mcp"));
    if mcp_requested
        && !super::mcp_config::project_mcp_requires_approval(
            args,
            workspace,
            &super::config::agent_dir(),
        )
        .is_ok_and(|requires_approval| !requires_approval)
    {
        return false;
    }
    // Project packages can resolve through local manifests or a package cache.
    // Their complete resource scope is only known during package preparation.
    if settings
        .get("packages")
        .is_some_and(|packages| !packages.as_array().is_some_and(Vec::is_empty))
    {
        return false;
    }
    let Some(mut roots) = project_protection_roots(&settings, &project) else {
        return false;
    };
    roots.extend(
        project_agent_skills(workspace)
            .into_iter()
            .map(|(root, _)| root),
    );
    // Bound the metadata work before interactive startup. Large or unreadable
    // resource trees use the same approval prompt as other unverified paths.
    let mut entries_left = 4096;
    roots
        .into_iter()
        .all(|root| protected_resource_tree(&root, uid, &mut entries_left))
}

#[cfg(unix)]
fn protected_project_entry(path: &Path, uid: u32) -> Option<Option<std::fs::Metadata>> {
    use std::os::unix::fs::MetadataExt;

    if !path.is_absolute() {
        return None;
    }
    let ancestors: Vec<_> = path.ancestors().collect();
    let mut parent_writable = false;
    for (index, entry) in ancestors.iter().enumerate().rev() {
        let metadata = match std::fs::symlink_metadata(entry) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound && !parent_writable => {
                // A loader that normalizes dot segments could still reach an
                // existing entry after a missing component followed by `..`.
                let tail = path.strip_prefix(entry.parent()?).ok()?;
                return (!tail
                    .components()
                    .any(|part| part == std::path::Component::ParentDir))
                .then_some(None);
            }
            Err(_) => return None,
        };
        let directory = index != 0;
        if !protected_project_metadata(entry, &metadata, uid, directory)
            || (directory && !metadata.is_dir())
        {
            return None;
        }
        if !directory {
            return Some(Some(metadata));
        }
        parent_writable = metadata.mode() & 0o022 != 0;
    }
    None
}

#[cfg(unix)]
fn protected_project_metadata(
    path: &Path,
    metadata: &std::fs::Metadata,
    uid: u32,
    ancestor: bool,
) -> bool {
    use std::os::unix::fs::MetadataExt;

    let mode = metadata.mode();
    (metadata.is_dir() || metadata.is_file())
        && (metadata.uid() == 0 || metadata.uid() == uid)
        && (mode & 0o022 == 0 || (ancestor && metadata.is_dir() && mode & 0o1000 != 0))
        && enforces_posix_permissions(path)
        && has_no_granting_acl(path)
}

#[cfg(unix)]
fn protected_resource_tree(root: &Path, uid: u32, entries_left: &mut usize) -> bool {
    let Some(metadata) = protected_project_entry(root, uid) else {
        return false;
    };
    if !metadata.is_some_and(|metadata| metadata.is_dir()) {
        return true;
    }
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return false;
        };
        for entry in entries {
            let Some(remaining) = entries_left.checked_sub(1) else {
                return false;
            };
            *entries_left = remaining;
            let Ok(entry) = entry else {
                return false;
            };
            let path = entry.path();
            let Ok(metadata) = std::fs::symlink_metadata(&path) else {
                return false;
            };
            if !protected_project_metadata(&path, &metadata, uid, false) {
                return false;
            }
            if metadata.is_dir() {
                pending.push(path);
            }
        }
    }
    true
}

#[cfg(unix)]
fn protected_directories(path: &Path, uid: u32, allow_sticky_leaf: bool) -> bool {
    use std::os::unix::fs::MetadataExt;

    path.ancestors().enumerate().all(|(depth, directory)| {
        std::fs::symlink_metadata(directory).is_ok_and(|metadata| {
            let mode = metadata.mode();
            // A sticky ancestor protects the next owned entry. An untrusted
            // writer could add project configuration directly in the workspace.
            let sticky_allowed = (depth > 0 || allow_sticky_leaf) && mode & 0o1000 != 0;
            metadata.is_dir()
                && (metadata.uid() == 0 || metadata.uid() == uid)
                && (mode & 0o022 == 0 || sticky_allowed)
                && enforces_posix_permissions(directory)
                && has_no_granting_acl(directory)
        })
    })
}

#[cfg(not(unix))]
fn protected_journal_path(_: &Path) -> Option<PathBuf> {
    None
}

#[cfg(not(unix))]
fn protected_project(_: &Path, _: &Cli) -> bool {
    false
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn enforces_posix_permissions(path: &Path) -> bool {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: the path is NUL-terminated and statfs initializes the output on success.
    unsafe {
        libc::statfs(path.as_ptr(), stat.as_mut_ptr()) == 0
            && permissions_mount(&stat.assume_init())
    }
}

#[cfg(target_os = "macos")]
fn permissions_mount(stat: &libc::statfs) -> bool {
    let filesystem: Vec<_> = stat.f_fstypename.iter().map(|&byte| byte as u8).collect();
    macos_permissions_mount(stat.f_flags, &filesystem)
}

#[cfg(target_os = "macos")]
fn macos_permissions_mount(flags: u32, filesystem: &[u8]) -> bool {
    // Darwin aliases MNT_UNKNOWNPERMISSIONS to MNT_IGNORE_OWNERSHIP.
    flags & libc::MNT_LOCAL as u32 != 0
        && flags & libc::MNT_IGNORE_OWNERSHIP as u32 == 0
        && matches!(
            filesystem.split(|&byte| byte == 0).next(),
            Some(b"apfs" | b"hfs" | b"tmpfs")
        )
}

#[cfg(target_os = "linux")]
fn permissions_mount(stat: &libc::statfs) -> bool {
    linux_permissions_mount(stat.f_type as u32)
}

#[cfg(target_os = "linux")]
fn linux_permissions_mount(kind: u32) -> bool {
    // Only local filesystems with known kernel-enforced owner and mode semantics.
    [
        libc::EXT4_SUPER_MAGIC as u32,
        libc::XFS_SUPER_MAGIC as u32,
        libc::BTRFS_SUPER_MAGIC as u32,
        libc::TMPFS_MAGIC as u32,
    ]
    .contains(&kind)
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn enforces_posix_permissions(_: &Path) -> bool {
    false
}

#[cfg(all(unix, not(target_os = "macos")))]
fn has_no_granting_acl(_: &Path) -> bool {
    // POSIX ACL grants are limited by the group permission bits.
    true
}

#[cfg(target_os = "macos")]
fn has_no_granting_acl(path: &Path) -> bool {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    // Darwin extended ACLs can grant permissions independently of mode bits.
    const ACL_TYPE_EXTENDED: libc::c_uint = 0x100;
    const ACL_FIRST_ENTRY: libc::c_int = 0;
    const ACL_NEXT_ENTRY: libc::c_int = -1;
    const ACL_EXTENDED_DENY: libc::c_uint = 2;
    unsafe extern "C" {
        fn acl_get_link_np(path: *const libc::c_char, kind: libc::c_uint) -> *mut libc::c_void;
        fn acl_get_entry(
            acl: *mut libc::c_void,
            id: libc::c_int,
            entry: *mut *mut libc::c_void,
        ) -> libc::c_int;
        fn acl_get_tag_type(entry: *mut libc::c_void, tag: *mut libc::c_uint) -> libc::c_int;
        fn acl_free(acl: *mut libc::c_void) -> libc::c_int;
    }
    let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: path is NUL-terminated. Entries borrow the returned ACL until
    // acl_free, and each output pointer refers to a writable local variable.
    unsafe {
        let acl = acl_get_link_np(path.as_ptr(), ACL_TYPE_EXTENDED);
        if acl.is_null() {
            return io::Error::last_os_error().raw_os_error() == Some(libc::ENOENT);
        }
        let mut id = ACL_FIRST_ENTRY;
        let only_denials = loop {
            let mut entry = std::ptr::null_mut();
            if acl_get_entry(acl, id, &mut entry) != 0 {
                // Darwin returns EINVAL after the last entry of a valid ACL.
                break io::Error::last_os_error().raw_os_error() == Some(libc::EINVAL);
            }
            let mut tag = 0;
            if acl_get_tag_type(entry, &mut tag) != 0 || tag != ACL_EXTENDED_DENY {
                break false;
            }
            id = ACL_NEXT_ENTRY;
        };
        acl_free(acl);
        only_denials
    }
}

pub(super) struct Reporter {
    token: String,
    executable: PathBuf,
    candidate: Option<String>,
    reported: bool,
}

impl Reporter {
    pub fn new(token: String) -> Option<Self> {
        if token.is_empty() {
            return None;
        }
        Some(Self {
            token,
            executable: std::env::current_exe().ok()?,
            candidate: None,
            reported: false,
        })
    }

    pub fn report(&mut self, args: &Cli, force: bool) -> io::Result<()> {
        self.report_with(args, force, write_frame)
    }

    fn report_with(
        &mut self,
        args: &Cli,
        force: bool,
        write: impl FnOnce(&str) -> io::Result<()>,
    ) -> io::Result<()> {
        let candidate = report_body(&self.executable, args, false);
        if !force && candidate == self.candidate {
            return Ok(());
        }
        // Resource checks belong to report emission and restore, rather than
        // every pass through the terminal input loop.
        let body = if candidate.is_some()
            && args.trusted
            && canonical_workspace(&args.workspace)
                .is_none_or(|workspace| !protected_project(&workspace, args))
        {
            report_body(&self.executable, args, true)
        } else {
            candidate.clone()
        };
        if let Some(body) = body {
            let frame = self.frame(&body);
            if frame.len() <= MAX_FRAME_BYTES {
                write(&frame)?;
                self.candidate = candidate;
                self.reported = true;
                return Ok(());
            }
        }
        self.clear_with(write)?;
        self.candidate = candidate;
        Ok(())
    }

    pub fn clear(&mut self) -> io::Result<()> {
        self.clear_with(write_frame)
    }

    fn clear_with(&mut self, write: impl FnOnce(&str) -> io::Result<()>) -> io::Result<()> {
        if self.reported {
            write(&self.frame("clear=1"))?;
            self.reported = false;
        }
        self.candidate = None;
        Ok(())
    }

    fn frame(&self, report: &str) -> String {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        frame(report, &self.token, std::process::id(), time)
    }
}

fn report_body(executable: &Path, args: &Cli, confirm_project: bool) -> Option<String> {
    if args.no_session
        || args.ephemeral_worker_session
        || args.trust_pending
        || args.api_key.is_some()
        || args.endpoint_explicit
        || args.provider_argument.is_some()
        || args.project.is_some()
        || !args.extensions.is_empty()
        || !args.extension_flags.is_empty()
        || args.system_prompt.is_some()
        || !args.append_system_prompt.is_empty()
        || args.instruction.is_some()
    {
        return None;
    }
    let workspace = canonical_workspace(&args.workspace)?;
    let session = protected_journal_path(&args.session)?;
    let mut argv = vec![
        executable.to_str()?.to_owned(),
        "--terminal-resume".into(),
        "--session".into(),
        session.to_str()?.into(),
        "--workspace".into(),
        workspace.to_str()?.into(),
    ];
    if confirm_project {
        argv.push("--terminal-resume-confirm-project".into());
    }
    for (restricted, flag) in [
        (args.no_trust || !args.trusted, "--no-trust"),
        (args.no_mcp, "--no-mcp"),
        (args.no_extensions, "--no-extensions"),
        (args.offline, "--offline"),
        (args.no_tools, "--no-tools"),
        (args.no_builtin_tools, "--no-builtin-tools"),
        (args.no_themes, "--no-themes"),
        (args.no_skills, "--no-skills"),
        (args.no_prompt_templates, "--no-prompt-templates"),
        (args.no_context_files, "--no-context-files"),
    ] {
        if restricted {
            argv.push(flag.into());
        }
    }
    for (flag, value) in [
        ("tools", &args.tools),
        ("exclude-tools", &args.exclude_tools),
    ] {
        if let Some(value) = value {
            argv.push(format!("--{flag}={value}"));
        }
    }
    argv.extend(
        args.disable_extension
            .iter()
            .map(|id| format!("--disable-extension={id}")),
    );
    args.resume_limits.append_args(&mut argv);
    if let Some(seconds) = args.tool_timeout_secs {
        argv.push(format!("--tool-timeout-secs={seconds}"));
    }
    if let Some(directory) = &args.session_dir {
        argv.push("--session-dir".into());
        argv.push(
            std::path::absolute(directory)
                .ok()?
                .into_os_string()
                .into_string()
                .ok()?,
        );
    }
    if !executable.is_absolute()
        || argv.len() > MAX_ARGV_ELEMENTS
        || argv.iter().any(|arg| arg.chars().any(char::is_control))
        || workspace.to_str()?.chars().any(char::is_control)
    {
        return None;
    }
    let argv = argv
        .iter()
        .map(|arg| STANDARD.encode(arg))
        .collect::<Vec<_>>()
        .join(",");
    Some(format!(
        "argv={argv}:cwd={}",
        STANDARD.encode(workspace.to_str()?)
    ))
}

fn frame(report: &str, token: &str, pid: u32, time: u64) -> String {
    let body = format!("pid={pid}:time={time}:{report}");
    let mut hash = Sha256::new();
    // The protocol authenticates the body followed by a colon and the token.
    hash.update(body.as_bytes());
    hash.update(b":");
    hash.update(token.as_bytes());
    format!("\x1b]7577;{body}:mac={:x}\x1b\\", hash.finalize())
}

fn write_frame(frame: &str) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    stdout.write_all(frame.as_bytes())?;
    stdout.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn assert_report_requires_project_confirmation(args: &Cli, executable: PathBuf) {
        let mut reporter = Reporter {
            token: "fixture-token".into(),
            executable,
            candidate: None,
            reported: false,
        };
        reporter
            .report_with(args, false, |frame| {
                assert!(frame.contains(&STANDARD.encode("--terminal-resume-confirm-project")));
                Ok(())
            })
            .unwrap();
        assert!(reporter.reported);
    }

    #[test]
    fn program_resume_frames_sign_the_exact_report_and_clear() {
        assert_eq!(
            frame("clear=1", "fixture-token", 42, 123),
            "\x1b]7577;pid=42:time=123:clear=1:mac=7dbea4915ed1c543a94fb74e05da24f1d534bcd57196c06774b5857372595a7c\x1b\\"
        );
    }

    #[cfg(unix)]
    fn fixture_args(root: &Path) -> Cli {
        use clap::Parser;
        let mut args = Cli::parse_from([
            "pi",
            "--offline",
            "--no-trust",
            "--no-mcp",
            "--no-extensions",
            "--no-tools",
            "--tools=read",
            "--disable-extension=todo",
        ]);
        args.workspace = root.to_owned();
        args.session = root.join("session spaced.jsonl");
        Session::open(&args.session).unwrap();
        args
    }

    #[cfg(unix)]
    #[test]
    fn program_resume_encodes_workspace_and_restrictive_argv() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let mut args = fixture_args(&root);
        if !root.ancestors().all(enforces_posix_permissions) {
            return;
        }
        let executable = root.join("pi");
        let body = report_body(&executable, &args, false).unwrap();
        let argv = body
            .strip_prefix("argv=")
            .unwrap()
            .split_once(":cwd=")
            .unwrap();
        let decode = |arg| String::from_utf8(STANDARD.decode(arg).unwrap()).unwrap();
        assert_eq!(decode(argv.1), root.to_str().unwrap());
        assert_eq!(
            argv.0.split(',').map(decode).collect::<Vec<_>>(),
            [
                executable.to_str().unwrap(),
                "--terminal-resume",
                "--session",
                args.session.to_str().unwrap(),
                "--workspace",
                root.to_str().unwrap(),
                "--no-trust",
                "--no-mcp",
                "--no-extensions",
                "--offline",
                "--no-tools",
                "--tools=read",
                "--disable-extension=todo",
            ]
        );
        assert!(report_body(Path::new("relative"), &args, false).is_none());
        assert!(report_body(&root.join("separator\u{1b}"), &args, false).is_none());
        args.api_key = Some("fixture-credential".into());
        assert!(report_body(&executable, &args, false).is_none());
    }

    #[test]
    fn program_resume_preserves_only_explicit_limits() {
        let (matches, _) = super::super::cli_compat::parse(
            [
                "pi",
                "--max-requests=12",
                "--max-output-tokens",
                "256",
                "--output-budget=512",
                "--model-timeout-secs=20",
                "--context-window",
                "4096",
            ]
            .map(std::ffi::OsString::from),
        )
        .unwrap();
        let mut argv = Vec::new();
        ExplicitLimits::from_matches(&matches).append_args(&mut argv);
        assert_eq!(
            argv,
            [
                "--max-requests=12",
                "--max-output-tokens=256",
                "--output-budget=512",
                "--model-timeout-secs=20",
                "--context-window=4096",
            ]
        );

        let (defaults, _) =
            super::super::cli_compat::parse([std::ffi::OsString::from("pi")]).unwrap();
        argv.clear();
        ExplicitLimits::from_matches(&defaults).append_args(&mut argv);
        assert!(argv.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn program_resume_suppresses_explicit_system_instructions() {
        use clap::Parser;

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        if !root.ancestors().all(enforces_posix_permissions) {
            return;
        }
        let baseline = fixture_args(&root);
        let executable = root.join("pi");
        assert!(report_body(&executable, &baseline, false).is_some());
        for flag in ["--system-prompt", "--append-system-prompt", "--instruction"] {
            let mut args = Cli::parse_from(["pi", flag, "opaque fixture input"]);
            args.session = baseline.session.clone();
            args.workspace = root.clone();
            assert!(report_body(&executable, &args, false).is_none(), "{flag}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn program_resume_suppresses_explicit_projects_and_extensions() {
        use clap::Parser;

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        if !root.ancestors().all(enforces_posix_permissions) {
            return;
        }
        let baseline = fixture_args(&root);
        let executable = root.join("pi");
        assert!(report_body(&executable, &baseline, false).is_some());
        let cases: &[&[&str]] = &[
            &["--project", "component with rules"],
            &["--extension", "./local extension"],
            &["--extension", "builtin:mcp"],
            &["--no-extensions", "--extension=builtin:mcp"],
        ];
        for flags in cases {
            let mut args = Cli::parse_from(std::iter::once("pi").chain(flags.iter().copied()));
            args.session = baseline.session.clone();
            args.workspace = root.clone();
            assert!(
                report_body(&executable, &args, false).is_none(),
                "{flags:?}"
            );
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn program_resume_rejects_unenforced_and_unknown_macos_mounts() {
        let local = libc::MNT_LOCAL as u32;
        let ignore_ownership = libc::MNT_IGNORE_OWNERSHIP as u32;
        assert!(macos_permissions_mount(local, b"apfs\0"));
        assert!(!macos_permissions_mount(
            local | ignore_ownership,
            b"apfs\0"
        ));
        assert!(!macos_permissions_mount(0, b"apfs\0"));
        assert!(!macos_permissions_mount(local, b"msdos\0"));
        assert!(!macos_permissions_mount(local, b"unknown\0"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn program_resume_rejects_network_overlay_and_unknown_linux_mounts() {
        assert!(linux_permissions_mount(libc::EXT4_SUPER_MAGIC as u32));
        assert!(!linux_permissions_mount(libc::NFS_SUPER_MAGIC as u32));
        assert!(!linux_permissions_mount(libc::OVERLAYFS_SUPER_MAGIC as u32));
        assert!(!linux_permissions_mount(0));
    }

    #[cfg(unix)]
    #[test]
    fn program_resume_requires_fresh_approval_for_a_mutable_workspace() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let mut args = fixture_args(&root);
        let workspace = root.join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        std::fs::create_dir(workspace.join(".git")).unwrap();
        args.workspace = workspace.clone();
        args.bind_existing_session();
        args.trusted = true;
        args.no_trust = false;
        args.trust = true;

        std::fs::set_permissions(&workspace, std::fs::Permissions::from_mode(0o1777)).unwrap();
        assert!(!protected_workspace(&workspace));
        assert_eq!(args.project_trust().unwrap(), None);
        if protected_journal_path(&args.session).is_some() {
            assert_report_requires_project_confirmation(&args, root.join("pi"));
            std::fs::set_permissions(&workspace, std::fs::Permissions::from_mode(0o700)).unwrap();
            assert_eq!(args.project_trust().unwrap(), Some(true));
            args.terminal_resume_confirm_project = true;
            assert_eq!(args.project_trust().unwrap(), None);
        }

        args.terminal_resume_approved = true;
        assert_eq!(args.project_trust().unwrap(), Some(true));
        args.no_trust = true;
        assert_eq!(args.project_trust().unwrap(), Some(false));
    }

    #[cfg(unix)]
    #[test]
    fn program_resume_checks_trust_bearing_project_paths() {
        use std::fs;
        use std::os::unix::fs::{PermissionsExt, symlink};

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        if !root.ancestors().all(enforces_posix_permissions) {
            return;
        }
        let mut args = fixture_args(&root);
        let project = root.join(".pi");
        let settings = project.join("settings.json");
        let mcp = project.join("mcp.json");
        let extension = project.join("extensions/bin/example");
        let configured = root.join("extra/prompts/example.md");
        let skill = root.join(".agents/skills/example/SKILL.md");
        fs::create_dir(root.join(".git")).unwrap();
        for path in [&extension, &configured, &skill] {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, "fixture").unwrap();
        }
        fs::write(&settings, r#"{"prompts":["../extra/prompts/*.md"]}"#).unwrap();
        fs::write(&mcp, "{}").unwrap();
        args.bind_existing_session();
        args.trusted = true;
        args.trust = true;
        args.no_trust = false;
        assert_eq!(args.project_trust().unwrap(), Some(true));

        for path in [&project, &settings, &mcp, &extension, &configured, &skill] {
            let permissions = fs::metadata(path).unwrap().permissions();
            fs::set_permissions(path, fs::Permissions::from_mode(permissions.mode() | 0o020))
                .unwrap();
            assert_eq!(args.project_trust().unwrap(), None, "{}", path.display());
            assert_report_requires_project_confirmation(&args, root.join("pi"));
            fs::set_permissions(path, permissions).unwrap();
        }

        let shared = root.join("shared-config");
        fs::rename(&project, &shared).unwrap();
        symlink(&shared, &project).unwrap();
        assert_eq!(args.project_trust().unwrap(), None);
        args.no_trust = true;
        assert_eq!(args.project_trust().unwrap(), Some(false));
        args.no_trust = false;
        args.terminal_resume_approved = true;
        assert_eq!(args.project_trust().unwrap(), Some(true));
        args.terminal_resume_approved = false;
        args.resume_existing_session = None;
        assert_eq!(args.project_trust().unwrap(), Some(true));
    }

    #[cfg(unix)]
    #[test]
    fn program_resume_requires_approval_for_project_mcp_local_launches() {
        use std::fs;

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        if !root.ancestors().all(enforces_posix_permissions) {
            return;
        }
        let mut args = fixture_args(&root);
        let project = root.join(".pi");
        fs::create_dir(&project).unwrap();
        fs::create_dir(root.join(".git")).unwrap();
        args.bind_existing_session();
        args.trusted = true;
        args.no_trust = false;
        args.no_mcp = false;
        args.no_extensions = false;
        args.trust = true;
        let mut explicit_mcp = args.clone();
        explicit_mcp.no_extensions = true;
        explicit_mcp.extensions.push("builtin:mcp".into());

        let mcp = project.join("mcp.json");
        for (config, approval) in [
            (r#"{"command":"fixture-only","enabled":false}"#, false),
            (r#"{"url":"https://fixture.invalid/mcp"}"#, false),
            (
                r#"{"url":"https://fixture.invalid/mcp","headers":{"Authorization":"Bearer ${FIXTURE_TOKEN}"}}"#,
                false,
            ),
            (
                r#"{"url":"https://fixture.invalid/mcp","headers":{"X-Fixture":"!fixture-only"},"enabled":false}"#,
                false,
            ),
            (r#"{"command":"fixture-only","cwd":"."}"#, true),
            (
                r#"{"url":"https://fixture.invalid/mcp","headers":{"X-Fixture":"!fixture-only"}}"#,
                true,
            ),
        ] {
            fs::write(&mcp, format!(r#"{{"mcpServers":{{"fixture":{config}}}}}"#)).unwrap();
            assert_eq!(
                args.project_trust().unwrap(),
                (!approval).then_some(true),
                "{config}"
            );
            assert_eq!(
                explicit_mcp.project_trust().unwrap(),
                (!approval).then_some(true),
                "{config}"
            );
            assert!(report_body(&root.join("pi"), &explicit_mcp, false).is_none());
            if approval {
                assert_report_requires_project_confirmation(&args, root.join("pi"));
                args.terminal_resume_approved = true;
                assert_eq!(args.project_trust().unwrap(), Some(true));
                args.terminal_resume_approved = false;
                args.no_trust = true;
                assert_eq!(args.project_trust().unwrap(), Some(false));
                args.no_trust = false;
            }
        }
        args.no_mcp = true;
        assert_eq!(args.project_trust().unwrap(), Some(true));
    }

    #[cfg(unix)]
    #[test]
    fn program_resume_rejects_a_different_workspace_and_missing_journal() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let mut args = fixture_args(&root);
        if !root.ancestors().all(enforces_posix_permissions) {
            return;
        }
        let other = root.join("other workspace");
        std::fs::create_dir(&other).unwrap();
        Session::open(&args.session)
            .unwrap()
            .set_working_directory(&other)
            .unwrap();
        let error = bind_startup(&mut args).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("differs from the registered workspace")
        );
        assert_eq!(args.workspace, root);
        assert!(args.command.is_none());

        args.workspace = other;
        bind_startup(&mut args).unwrap();
        assert!(matches!(args.command, Some(Command::Interactive)));
        std::fs::remove_file(&args.session).unwrap();
        assert!(open_bound_session(&args).is_err());
        assert!(Session::open_existing(&args.session).is_err());
        assert!(!args.session.exists());
    }

    #[cfg(unix)]
    #[test]
    fn program_resume_skips_shared_and_non_regular_journals() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let mut args = fixture_args(&root);
        if !root.ancestors().all(enforces_posix_permissions) {
            return;
        }
        let executable = root.join("pi");
        std::fs::set_permissions(&args.session, std::fs::Permissions::from_mode(0o620)).unwrap();
        assert!(report_body(&executable, &args, false).is_none());

        let unsafe_directory = root.join("shared");
        std::fs::create_dir(&unsafe_directory).unwrap();
        std::fs::set_permissions(&unsafe_directory, std::fs::Permissions::from_mode(0o777))
            .unwrap();
        args.session = unsafe_directory.join("session.jsonl");
        Session::open(&args.session).unwrap();
        assert!(report_body(&executable, &args, false).is_none());

        let alias = root.join("alias.jsonl");
        symlink(&args.session, &alias).unwrap();
        args.session = alias;
        assert!(report_body(&executable, &args, false).is_none());

        args.session = unsafe_directory;
        assert!(report_body(&executable, &args, false).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn program_resume_clears_when_a_previously_reported_journal_disappears() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let args = fixture_args(&root);
        if !root.ancestors().all(enforces_posix_permissions) {
            return;
        }
        let mut reporter = Reporter {
            token: "fixture-token".into(),
            executable: root.join("pi"),
            candidate: None,
            reported: false,
        };
        let mut frames = Vec::new();
        let mut write = |frame: &str| {
            frames.push(frame.to_owned());
            Ok(())
        };
        reporter.report_with(&args, false, &mut write).unwrap();
        reporter.report_with(&args, false, &mut write).unwrap();
        std::fs::remove_file(&args.session).unwrap();
        reporter.report_with(&args, false, &mut write).unwrap();
        reporter.report_with(&args, false, &mut write).unwrap();

        assert_eq!(frames.len(), 2);
        assert!(frames[0].contains(":argv="));
        assert!(frames[1].contains(":clear=1:"));
        assert!(!args.session.exists());
    }
}
