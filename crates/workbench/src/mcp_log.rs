//! Private, best-effort log of MCP server message notifications.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::Value;

const ROTATE_AFTER: u64 = 5 * 1024 * 1024;
const MAX_RECORD: usize = 256 * 1024;
const TRUNCATED: &str = " [truncated]\n";

fn push_safe(output: &mut String, input: &str) {
    for value in input.chars() {
        if output.len() >= MAX_RECORD {
            break;
        }
        if value.is_control() {
            output.extend(value.escape_default());
        } else {
            output.push(value);
        }
    }
}

fn format_message(
    now: DateTime<Utc>,
    server: &str,
    level: &str,
    logger: Option<&str>,
    data: &Value,
) -> String {
    let mut log = format!("{} [", now.to_rfc3339_opts(SecondsFormat::Millis, true));
    push_safe(&mut log, server);
    log.push_str("] ");
    log.push_str(level);
    if let Some(logger) = logger.filter(|name| !name.is_empty()) {
        log.push(' ');
        push_safe(&mut log, logger);
        log.push(':');
    }
    log.push(' ');
    let text = match data {
        Value::String(text) => std::borrow::Cow::Borrowed(text.as_str()),
        data => std::borrow::Cow::Owned(data.to_string()),
    };
    let mut lines = text.split('\n').peekable();
    let mut first = true;
    while let Some(line) = lines.next() {
        if log.len() >= MAX_RECORD {
            break;
        }
        if !first {
            log.push_str("\n    ");
        }
        first = false;
        let line = if lines.peek().is_some() {
            line.strip_suffix('\r').unwrap_or(line)
        } else {
            line
        };
        push_safe(&mut log, line);
    }
    if log.len() >= MAX_RECORD {
        let mut boundary = MAX_RECORD - TRUNCATED.len();
        while !log.is_char_boundary(boundary) {
            boundary -= 1;
        }
        log.truncate(boundary);
        log.push_str(TRUNCATED);
    } else {
        log.push('\n');
    }
    log
}

fn open_private(path: &Path, append: bool) -> io::Result<File> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.is_file() => {
            return Err(io::Error::other("MCP log path is not a regular file"));
        }
        Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
        _ => {}
    }
    let mut options = OpenOptions::new();
    options
        .create(true)
        .truncate(false)
        .read(!append)
        .write(!append)
        .append(append);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    options.open(path)
}

fn append_record(directory: &Path, line: &str) -> io::Result<()> {
    fs::create_dir_all(directory)?;
    let lock = open_private(&directory.join("mcp.log.lock"), false)?;
    lock.lock()?;
    let path = directory.join("mcp.log");
    let mut file = open_private(&path, true)?;
    if file.metadata()?.len() > ROTATE_AFTER {
        drop(file);
        fs::rename(&path, directory.join("mcp.log.1"))?;
        file = open_private(&path, true)?;
    }
    file.write_all(line.as_bytes())
}

pub(super) async fn record(
    server: std::sync::Arc<str>,
    level: &str,
    logger: Option<String>,
    data: Value,
) {
    let line = format_message(Utc::now(), &server, level, logger.as_deref(), &data);
    let _ = tokio::task::spawn_blocking(move || append_record(&crate::agent_dir(), &line)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_multiline_messages_and_bounds_payloads() {
        let now = "2026-11-21T09:00:00.123Z".parse().unwrap();
        assert_eq!(
            format_message(
                now,
                "docs",
                "warning",
                Some("api"),
                &Value::from("first\r\nsecond\n")
            ),
            "2026-11-21T09:00:00.123Z [docs] warning api: first\n    second\n    \n"
        );
        assert_eq!(
            format_message(
                now,
                "docs",
                "info",
                Some("a\nb"),
                &serde_json::json!({"code":1})
            ),
            "2026-11-21T09:00:00.123Z [docs] info a\\nb: {\"code\":1}\n"
        );
        let large = format_message(
            now,
            "docs",
            "info",
            None,
            &Value::from("文".repeat(MAX_RECORD)),
        );
        assert!(large.len() <= MAX_RECORD);
        assert!(large.ends_with(TRUNCATED));
    }

    #[test]
    fn rotates_once_before_appending_a_new_complete_record() {
        let home = tempfile::tempdir().unwrap();
        let current = home.path().join("mcp.log");
        let previous = home.path().join("mcp.log.1");
        fs::write(&previous, "older\n").unwrap();
        File::create(&current)
            .unwrap()
            .set_len(ROTATE_AFTER + 1)
            .unwrap();
        append_record(home.path(), "first\n").unwrap();
        append_record(home.path(), "second\n").unwrap();
        assert_eq!(fs::metadata(previous).unwrap().len(), ROTATE_AFTER + 1);
        assert_eq!(fs::read_to_string(current).unwrap(), "first\nsecond\n");
    }
}
