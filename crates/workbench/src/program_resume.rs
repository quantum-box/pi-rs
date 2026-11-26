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
    require_journal_path(&args.session)?;
    check_workspace(&Session::snapshot(&args.session)?, &args.workspace)?;
    args.command = Some(Command::Interactive);
    Ok(())
}

pub(super) fn open_bound_session(args: &Cli) -> Result<Session> {
    require_journal_path(&args.session)?;
    let session = Session::open_existing(&args.session)?;
    check_workspace(&session, &args.workspace)?;
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

    // Resolve directory aliases before publishing a path. The journal entry
    // itself must be a regular file, rather than an alias for another file.
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
        || !has_no_granting_acl(&path)
    {
        return None;
    }
    for directory in parent.ancestors() {
        let metadata = std::fs::symlink_metadata(directory).ok()?;
        if !metadata.is_dir()
            || (metadata.uid() != 0 && metadata.uid() != uid)
            || (metadata.mode() & 0o022 != 0 && metadata.mode() & 0o1000 == 0)
            || !has_no_granting_acl(directory)
        {
            return None;
        }
    }
    Some(path)
}

#[cfg(not(unix))]
fn protected_journal_path(_: &Path) -> Option<PathBuf> {
    // Automatic restore requires a verified owner-only file and protected ancestry.
    None
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
    reported: Option<String>,
}

impl Reporter {
    pub fn new(token: String) -> Option<Self> {
        if token.is_empty() {
            return None;
        }
        Some(Self {
            token,
            executable: std::env::current_exe().ok()?,
            reported: None,
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
        let body = report_body(&self.executable, args);
        if !force && body == self.reported {
            return Ok(());
        }
        if let Some(body) = body {
            let frame = self.frame(&body);
            if frame.len() <= MAX_FRAME_BYTES {
                write(&frame)?;
                self.reported = Some(body);
                return Ok(());
            }
        }
        self.clear_with(write)
    }

    pub fn clear(&mut self) -> io::Result<()> {
        self.clear_with(write_frame)
    }

    fn clear_with(&mut self, write: impl FnOnce(&str) -> io::Result<()>) -> io::Result<()> {
        if self.reported.is_some() {
            write(&self.frame("clear=1"))?;
            self.reported = None;
        }
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

fn report_body(executable: &Path, args: &Cli) -> Option<String> {
    if args.no_session
        || args.ephemeral_worker_session
        || args.trust_pending
        || args.api_key.is_some()
        || args.endpoint_explicit
        || args.provider_argument.is_some()
        || !args.extension_flags.is_empty()
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
        let executable = root.join("pi");
        let body = report_body(&executable, &args).unwrap();
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
        assert!(report_body(Path::new("relative"), &args).is_none());
        assert!(report_body(&root.join("separator\u{1b}"), &args).is_none());
        args.api_key = Some("fixture-credential".into());
        assert!(report_body(&executable, &args).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn program_resume_rejects_a_different_workspace_and_missing_journal() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let mut args = fixture_args(&root);
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
        let executable = root.join("pi");
        std::fs::set_permissions(&args.session, std::fs::Permissions::from_mode(0o620)).unwrap();
        assert!(report_body(&executable, &args).is_none());

        let unsafe_directory = root.join("shared");
        std::fs::create_dir(&unsafe_directory).unwrap();
        std::fs::set_permissions(&unsafe_directory, std::fs::Permissions::from_mode(0o777))
            .unwrap();
        args.session = unsafe_directory.join("session.jsonl");
        Session::open(&args.session).unwrap();
        assert!(report_body(&executable, &args).is_none());

        let alias = root.join("alias.jsonl");
        symlink(&args.session, &alias).unwrap();
        args.session = alias;
        assert!(report_body(&executable, &args).is_none());

        args.session = unsafe_directory;
        assert!(report_body(&executable, &args).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn program_resume_clears_when_a_previously_reported_journal_disappears() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let args = fixture_args(&root);
        let mut reporter = Reporter {
            token: "fixture-token".into(),
            executable: root.join("pi"),
            reported: None,
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
