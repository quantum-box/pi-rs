//! OSC 7577 reports for terminal-managed session restoration.
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};

pub(super) const TOKEN_ENV: &str = "OSC7577_TOKEN";
const MAX_FRAME_BYTES: usize = 2048;

pub(super) struct Reporter {
    token: String,
    executable: PathBuf,
    reported: Option<(PathBuf, PathBuf)>,
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

    pub fn report(&mut self, session: &Path, workspace: &Path, force: bool) -> io::Result<()> {
        if !force
            && self
                .reported
                .as_ref()
                .is_some_and(|(path, cwd)| path == session && cwd == workspace)
        {
            return Ok(());
        }
        let report = session
            .is_file()
            .then(|| {
                let session = std::path::absolute(session).ok()?;
                let workspace = std::path::absolute(workspace).ok()?;
                report_body(&self.executable, &session, &workspace)
            })
            .flatten();
        if let Some(body) = report {
            let frame = self.frame(&body);
            if frame.len() <= MAX_FRAME_BYTES {
                write_frame(&frame)?;
                self.reported = Some((session.to_owned(), workspace.to_owned()));
                return Ok(());
            }
        }
        self.clear()
    }

    pub fn clear(&mut self) -> io::Result<()> {
        if self.reported.is_some() {
            write_frame(&self.frame("clear=1"))?;
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

fn report_body(executable: &Path, session: &Path, workspace: &Path) -> Option<String> {
    fn encode_absolute(path: &Path) -> Option<String> {
        let path_text = path.to_str()?;
        (path.is_absolute() && !path_text.chars().any(char::is_control))
            .then(|| STANDARD.encode(path_text))
    }
    Some(format!(
        "argv={},{},{}:cwd={}",
        encode_absolute(executable)?,
        STANDARD.encode("--session"),
        encode_absolute(session)?,
        encode_absolute(workspace)?,
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

    #[test]
    fn program_resume_encodes_absolute_paths_without_shell_quoting() {
        let root = tempfile::tempdir().unwrap();
        let root = std::path::absolute(root.path()).unwrap();
        let executable = root.join("pi");
        let session = root.join("session spaced.jsonl");
        let working = root.join("a directory");
        let body = report_body(&executable, &session, &working).unwrap();
        assert_eq!(
            body,
            format!(
                "argv={},{},{}:cwd={}",
                STANDARD.encode(executable.to_str().unwrap()),
                "LS1zZXNzaW9u",
                STANDARD.encode(session.to_str().unwrap()),
                STANDARD.encode(working.to_str().unwrap()),
            )
        );
        assert!(report_body(Path::new("relative"), &session, &working).is_none());
        assert!(report_body(&executable, &root.join("separator\u{1b}"), &working).is_none());
    }
}
