//! Direct process execution for trusted native extensions, matching Pi's `exec` API.
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use pi_rs_core::{CancellationToken, Error, Result};
use tokio::io::AsyncReadExt;

const DEFAULT_MAX_BUFFER: usize = 256 * 1024 * 1024;
// V8's maximum string length on 64-bit platforms, used by Pi v1.5.0.
const MAX_BUFFER: usize = (1 << 29) - 24;
const EXIT_IDLE: Duration = Duration::from_millis(100);
const FORCE_KILL: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Default)]
pub struct ExecOptions {
    pub cwd: Option<PathBuf>,
    pub timeout: Option<Duration>,
    pub max_buffer: Option<usize>,
    pub cancellation: Option<CancellationToken>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub code: i32,
    pub killed: bool,
    pub truncated: bool,
}

/// Execute `command` directly with literal arguments; no shell is involved.
/// `default_cwd` is the extension's workspace and may be overridden in options.
pub async fn exec_command(
    command: &str,
    args: &[String],
    default_cwd: &Path,
    options: &ExecOptions,
) -> Result<ExecResult> {
    let cap = options.max_buffer.unwrap_or(DEFAULT_MAX_BUFFER);
    if cap > MAX_BUFFER {
        return Err(Error::Config(format!(
            "maxBuffer must be between 0 and {MAX_BUFFER} bytes"
        )));
    }
    let mut child = match tokio::process::Command::new(command)
        .args(args)
        .current_dir(options.cwd.as_deref().unwrap_or(default_cwd))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            return Ok(ExecResult {
                stdout: String::new(),
                stderr: if error.kind() == std::io::ErrorKind::NotFound {
                    format!("spawn {command} ENOENT")
                } else {
                    error.to_string()
                },
                code: 1,
                killed: false,
                truncated: false,
            });
        }
    };
    let mut stdout = child.stdout.take();
    let mut stderr = child.stderr.take();
    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();
    let mut stdout_chunk = [0; 8192];
    let mut stderr_chunk = [0; 8192];
    let deadline = options
        .timeout
        .filter(|timeout| !timeout.is_zero())
        .map(|timeout| tokio::time::Instant::now() + timeout);
    let mut force_kill = None;
    let mut idle = None;
    let mut status = None;
    let mut killed = false;
    let mut truncated = false;
    loop {
        if status.is_some() && stdout.is_none() && stderr.is_none() {
            break;
        }
        tokio::select! {
            _ = wait_cancel(options.cancellation.as_ref()), if !killed => {
                terminate(&mut child);
                killed = true;
                force_kill = Some(tokio::time::Instant::now() + FORCE_KILL);
            }
            _ = wait_deadline(deadline), if !killed && status.is_none() => {
                terminate(&mut child);
                killed = true;
                force_kill = Some(tokio::time::Instant::now() + FORCE_KILL);
            }
            _ = wait_deadline(force_kill), if status.is_none() => {
                let _ = child.start_kill();
                force_kill = None;
            }
            result = child.wait(), if status.is_none() => {
                status = Some(result?);
                idle = Some(tokio::time::Instant::now() + EXIT_IDLE);
            }
            _ = wait_deadline(idle), if status.is_some() => break,
            result = read(&mut stdout, &mut stdout_chunk), if stdout.is_some() => {
                let count = result?;
                if count == 0 { stdout = None; }
                else {
                    let remaining = cap.saturating_sub(stdout_bytes.len() + stderr_bytes.len());
                    stdout_bytes.extend_from_slice(&stdout_chunk[..count.min(remaining)]);
                    if count > remaining {
                        truncated = true; stdout = None; stderr = None;
                        if !killed { terminate(&mut child); killed = true; force_kill = Some(tokio::time::Instant::now() + FORCE_KILL); }
                    }
                    if status.is_some() { idle = Some(tokio::time::Instant::now() + EXIT_IDLE); }
                }
            }
            result = read(&mut stderr, &mut stderr_chunk), if stderr.is_some() => {
                let count = result?;
                if count == 0 { stderr = None; }
                else {
                    let remaining = cap.saturating_sub(stdout_bytes.len() + stderr_bytes.len());
                    stderr_bytes.extend_from_slice(&stderr_chunk[..count.min(remaining)]);
                    if count > remaining {
                        truncated = true; stdout = None; stderr = None;
                        if !killed { terminate(&mut child); killed = true; force_kill = Some(tokio::time::Instant::now() + FORCE_KILL); }
                    }
                    if status.is_some() { idle = Some(tokio::time::Instant::now() + EXIT_IDLE); }
                }
            }
        }
    }
    let mut code = status.and_then(|status| status.code()).unwrap_or(0);
    if truncated && code == 0 {
        code = 1;
    }
    Ok(ExecResult {
        stdout: decode(stdout_bytes, truncated),
        stderr: decode(stderr_bytes, truncated),
        code,
        killed,
        truncated,
    })
}

async fn read<R: tokio::io::AsyncRead + Unpin>(
    stream: &mut Option<R>,
    buffer: &mut [u8],
) -> std::io::Result<usize> {
    match stream {
        Some(stream) => stream.read(buffer).await,
        None => std::future::pending().await,
    }
}

async fn wait_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn wait_cancel(token: Option<&CancellationToken>) {
    match token {
        Some(token) => token.cancelled().await,
        None => std::future::pending().await,
    }
}

fn terminate(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    if let Some(id) = child.id() {
        // Signal only the immediate process, as Pi's extension exec does.
        unsafe {
            libc::kill(id as i32, libc::SIGTERM);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = child.start_kill();
    }
}

fn decode(mut bytes: Vec<u8>, truncated: bool) -> String {
    if truncated && !bytes.is_empty() {
        let mut start = bytes.len() - 1;
        while start > 0 && bytes[start] & 0xc0 == 0x80 && bytes.len() - start < 4 {
            start -= 1;
        }
        if let Err(error) = std::str::from_utf8(&bytes[start..])
            && error.error_len().is_none()
        {
            bytes.truncate(start);
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[tokio::test]
    async fn direct_exec_preserves_arguments_cwd_and_separate_streams() {
        let root = tempfile::tempdir().unwrap();
        let other = root.path().join("other");
        std::fs::create_dir(&other).unwrap();
        let other = other.canonicalize().unwrap();
        let options = ExecOptions {
            cwd: Some(other.clone()),
            ..Default::default()
        };
        let result = exec_command(
            "sh",
            &args(&[
                "-c",
                "printf '%s|%s' \"$1\" \"$PWD\"; printf problem >&2; exit 7",
                "sh",
                "literal; echo injected",
            ]),
            root.path(),
            &options,
        )
        .await
        .unwrap();
        assert_eq!(
            result.stdout,
            format!("literal; echo injected|{}", other.display())
        );
        assert_eq!(result.stderr, "problem");
        assert_eq!(result.code, 7);
        assert!(!result.killed && !result.truncated);
        let missing = exec_command(
            "pi-rs-command-that-does-not-exist",
            &[],
            root.path(),
            &ExecOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(missing.code, 1);
        assert!(missing.stderr.contains("ENOENT"));
        assert!(!missing.killed);
    }

    #[tokio::test]
    async fn combined_cap_preserves_utf8_and_zero_limit() {
        let root = tempfile::tempdir().unwrap();
        for (script, cap, stdout, stderr) in [
            ("printf 'a€🙂'", 5, "a€", ""),
            ("printf 'a€🙂' >&2", 5, "", "a€"),
            ("printf x", 0, "", ""),
        ] {
            let options = ExecOptions {
                max_buffer: Some(cap),
                ..Default::default()
            };
            let result = exec_command("sh", &args(&["-c", script]), root.path(), &options)
                .await
                .unwrap();
            assert_eq!(
                (result.stdout.as_str(), result.stderr.as_str()),
                (stdout, stderr)
            );
            assert!(result.killed && result.truncated && result.code != 0);
        }
        let options = ExecOptions {
            max_buffer: Some(3),
            ..Default::default()
        };
        let result = exec_command("sh", &args(&["-c", "printf abc"]), root.path(), &options)
            .await
            .unwrap();
        assert_eq!(result.stdout, "abc");
        assert!(!result.truncated && !result.killed);
        let options = ExecOptions {
            max_buffer: Some(4),
            ..Default::default()
        };
        let result = exec_command(
            "sh",
            &args(&["-c", "printf ab; printf cdef >&2"]),
            root.path(),
            &options,
        )
        .await
        .unwrap();
        assert_eq!(result.stdout.len() + result.stderr.len(), 4);
        assert!(result.killed && result.truncated && result.code != 0);
        let invalid = ExecOptions {
            max_buffer: Some(MAX_BUFFER + 1),
            ..Default::default()
        };
        assert!(
            exec_command("sh", &[], root.path(), &invalid)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn timeout_and_cancellation_keep_partial_output() {
        let root = tempfile::tempdir().unwrap();
        let options = ExecOptions {
            timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        };
        let result = exec_command(
            "sh",
            &args(&["-c", "printf ready; exec sleep 30"]),
            root.path(),
            &options,
        )
        .await
        .unwrap();
        assert_eq!(result.stdout, "ready");
        assert!(result.killed && !result.truncated);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let options = ExecOptions {
            cancellation: Some(cancel),
            ..Default::default()
        };
        let result = exec_command("sleep", &args(&["30"]), root.path(), &options)
            .await
            .unwrap();
        assert!(result.killed && !result.truncated);
    }

    #[tokio::test]
    async fn output_limit_force_kills_a_process_that_ignores_term() {
        let root = tempfile::tempdir().unwrap();
        let options = ExecOptions {
            max_buffer: Some(1),
            ..Default::default()
        };
        let result = tokio::time::timeout(
            Duration::from_secs(20),
            exec_command(
                "sh",
                &args(&["-c", "trap '' TERM; printf xx; exec sleep 30"]),
                root.path(),
                &options,
            ),
        )
        .await
        .expect("force kill must terminate the process")
        .unwrap();
        assert_eq!(result.stdout, "x");
        assert!(result.killed && result.truncated && result.code != 0);
    }

    #[tokio::test]
    async fn post_exit_pipe_drain_keeps_active_descendant_output() {
        let root = tempfile::tempdir().unwrap();
        let result = exec_command(
            "sh",
            &args(&[
                "-c",
                "(sleep 0.05; printf first; sleep 0.05; printf second) &",
            ]),
            root.path(),
            &ExecOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(result.stdout, "firstsecond");
        assert_eq!(result.code, 0);
    }

    #[test]
    fn truncated_decoder_only_discards_incomplete_suffix() {
        assert_eq!(decode(vec![b'a', 0xff, 0xe2, 0x82], true), "a�");
        assert_eq!(decode(vec![0xe2], false), "�");
        assert_eq!(decode("日本".as_bytes().to_vec(), true), "日本");
    }
}
