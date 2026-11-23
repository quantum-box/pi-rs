//! Private, best-effort log of MCP server message notifications.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::future::Future;
use std::io::{self, Write};
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, Utc};
use rmcp::{
    RoleClient,
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage, ServerNotification},
    transport::Transport,
};
use serde_json::Value;

const ROTATE_AFTER: u64 = 5 * 1024 * 1024;
const MAX_RECORD: usize = 256 * 1024;
const TRUNCATED: &str = " [truncated]\n";
const WRITE_BUDGET: Duration = Duration::from_millis(50);
static WRITERS: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(4)));

pub(super) struct LoggingTransport<T> {
    inner: T,
    server: Option<Arc<str>>,
    pending: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<T> LoggingTransport<T> {
    pub(super) fn new(inner: T, server: Option<Arc<str>>) -> Self {
        Self {
            inner,
            server,
            pending: None,
        }
    }
}

impl<T: Transport<RoleClient>> Transport<RoleClient> for LoggingTransport<T> {
    type Error = T::Error;

    fn send(
        &mut self,
        message: ClientJsonRpcMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.inner.send(message)
    }

    #[allow(deprecated)]
    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        loop {
            // rmcp selects over receive and outbound work. Keep the write
            // future here so that selecting a different branch cannot drop
            // a received log or expose a later reply before the write.
            if let Some(pending) = self.pending.as_mut() {
                pending.await;
                self.pending = None;
            }
            let message = self.inner.receive().await?;
            let Some(name) = &self.server else {
                return Some(message);
            };
            let ServerJsonRpcMessage::Notification(notification) = message else {
                return Some(message);
            };
            let ServerNotification::LoggingMessageNotification(log) = notification.notification
            else {
                return Some(ServerJsonRpcMessage::Notification(notification));
            };
            let level = serde_json::to_value(log.params.level).unwrap_or_default();
            let level = level.as_str().unwrap_or("info").to_owned();
            self.pending = Some(Box::pin(record(
                name.clone(),
                level,
                log.params.logger,
                log.params.data,
            )));
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        if let Some(pending) = self.pending.take() {
            pending.await;
        }
        self.inner.close().await
    }
}

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
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::other("MCP log path is not a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
    }
    Ok(file)
}

fn append_record(directory: &Path, line: &str, deadline: Instant) -> io::Result<()> {
    fs::create_dir_all(directory)?;
    let lock = open_private(&directory.join("mcp.log.lock"), false)?;
    loop {
        match lock.try_lock() {
            Ok(()) => break,
            Err(TryLockError::WouldBlock) => {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err(io::ErrorKind::TimedOut.into());
                }
                std::thread::sleep(left.min(Duration::from_millis(2)));
            }
            Err(TryLockError::Error(error)) => return Err(error),
        }
    }
    let path = directory.join("mcp.log");
    let mut file = open_private(&path, true)?;
    if file.metadata()?.len() > ROTATE_AFTER {
        drop(file);
        fs::rename(&path, directory.join("mcp.log.1"))?;
        file = open_private(&path, true)?;
    }
    file.write_all(line.as_bytes())
}

async fn record(server: Arc<str>, level: String, logger: Option<String>, data: Value) {
    let Ok(permit) = WRITERS.clone().try_acquire_owned() else {
        return;
    };
    let now = Utc::now();
    let deadline = Instant::now() + WRITE_BUDGET;
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let line = format_message(now, &server, &level, logger.as_deref(), &data);
        if Instant::now() < deadline {
            let _ = append_record(&crate::agent_dir(), &line, deadline);
        }
    });
    let _ = tokio::time::timeout(WRITE_BUDGET, task).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pending_log_survives_a_cancelled_receive() {
        struct Queue(Option<ServerJsonRpcMessage>);

        impl Transport<RoleClient> for Queue {
            type Error = io::Error;

            fn send(
                &mut self,
                _message: ClientJsonRpcMessage,
            ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
                std::future::ready(Ok(()))
            }

            async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
                self.0.take()
            }

            async fn close(&mut self) -> Result<(), Self::Error> {
                Ok(())
            }
        }

        let response = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "result": {}
        }))
        .unwrap();
        let (release, pending) = tokio::sync::oneshot::channel();
        let mut transport = LoggingTransport::new(Queue(Some(response)), None);
        transport.pending = Some(Box::pin(async move { pending.await.unwrap() }));

        tokio::select! {
            biased;
            _ = transport.receive() => panic!("reply overtook the log write"),
            _ = std::future::ready(()) => {}
        }
        release.send(()).unwrap();
        assert!(matches!(
            transport.receive().await,
            Some(ServerJsonRpcMessage::Response(_))
        ));
        assert!(transport.pending.is_none());
    }

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
        append_record(home.path(), "first\n", Instant::now() + WRITE_BUDGET).unwrap();
        append_record(home.path(), "second\n", Instant::now() + WRITE_BUDGET).unwrap();
        assert_eq!(fs::metadata(previous).unwrap().len(), ROTATE_AFTER + 1);
        assert_eq!(fs::read_to_string(current).unwrap(), "first\nsecond\n");
    }

    #[test]
    fn a_busy_log_lock_does_not_hold_up_notifications() {
        let home = tempfile::tempdir().unwrap();
        let lock = open_private(&home.path().join("mcp.log.lock"), false).unwrap();
        lock.lock().unwrap();
        let error =
            append_record(home.path(), "skip\n", Instant::now() + WRITE_BUDGET).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(!home.path().join("mcp.log").exists());
    }

    #[cfg(unix)]
    #[test]
    fn makes_existing_logs_private_before_appending() {
        use std::os::unix::fs::PermissionsExt;
        let home = tempfile::tempdir().unwrap();
        let log = home.path().join("mcp.log");
        fs::write(&log, "existing\n").unwrap();
        fs::set_permissions(&log, fs::Permissions::from_mode(0o644)).unwrap();
        append_record(home.path(), "append\n", Instant::now() + WRITE_BUDGET).unwrap();
        assert_eq!(
            fs::metadata(&log).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(fs::read_to_string(log).unwrap(), "existing\nappend\n");
    }
}
