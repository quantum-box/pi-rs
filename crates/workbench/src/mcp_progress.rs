//! Correlate MCP tool progress with the request token assigned by rmcp.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use rmcp::{
    RoleClient,
    model::{
        ClientJsonRpcMessage, ClientRequest, GetMeta, ProgressNotificationParam, ProgressToken,
        ServerJsonRpcMessage, ServerNotification,
    },
    transport::Transport,
};
use serde_json::json;
use tokio::{sync::watch, time::Instant};
use uuid::Uuid;

#[derive(Default)]
pub(super) struct ToolProgress(Mutex<HashMap<Uuid, ActiveCall>>);

struct ActiveCall {
    token: Option<ProgressToken>,
    timeout: Duration,
    updates: UpdatesSender,
}

pub(super) struct Update {
    deadline: Instant,
    progress: Option<ProgressNotificationParam>,
}

pub(super) type Updates = watch::Receiver<Update>;
pub(super) type UpdatesSender = watch::Sender<Update>;

pub(super) fn channel(timeout: Duration) -> (UpdatesSender, Updates) {
    watch::channel(Update {
        deadline: Instant::now() + timeout,
        progress: None,
    })
}

pub(super) struct Registration<'a> {
    calls: &'a ToolProgress,
    operation: Uuid,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        self.calls
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.operation);
    }
}

impl ToolProgress {
    pub(super) fn register(
        &self,
        operation: Uuid,
        timeout: Duration,
        updates: UpdatesSender,
    ) -> Registration<'_> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                operation,
                ActiveCall {
                    token: None,
                    timeout,
                    updates,
                },
            );
        Registration {
            calls: self,
            operation,
        }
    }
}

pub(super) struct ProgressTransport<T> {
    inner: T,
    calls: std::sync::Arc<ToolProgress>,
}

impl<T> ProgressTransport<T> {
    pub(super) fn new(inner: T, calls: std::sync::Arc<ToolProgress>) -> Self {
        Self { inner, calls }
    }
}

impl<T: Transport<RoleClient>> Transport<RoleClient> for ProgressTransport<T> {
    type Error = T::Error;

    fn send(
        &mut self,
        message: ClientJsonRpcMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        if let ClientJsonRpcMessage::Request(request) = &message
            && let ClientRequest::CallToolRequest(call) = &request.request
            && let Some(meta) = &call.params.meta
            && let Some(operation) = meta
                .0
                .get("operationId")
                .and_then(serde_json::Value::as_str)
                .and_then(|id| Uuid::parse_str(id).ok())
            && let Some(active) = self
                .calls
                .0
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get_mut(&operation)
        {
            active.token = request.request.get_meta().get_progress_token();
        }
        self.inner.send(message)
    }

    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        let message = self.inner.receive().await?;
        if let ServerJsonRpcMessage::Notification(notification) = &message
            && let ServerNotification::ProgressNotification(progress) = &notification.notification
            && let Some(active) = self
                .calls
                .0
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .values()
                .find(|call| call.token.as_ref() == Some(&progress.params.progress_token))
        {
            active.updates.send_if_modified(|update| {
                let now = Instant::now();
                if now > update.deadline {
                    return false;
                }
                update.deadline = now + active.timeout;
                update.progress = Some(progress.params.clone());
                true
            });
        }
        Some(message)
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.inner.close().await
    }
}

pub(super) async fn wait_for_idle(mut updates: Updates) {
    loop {
        let deadline = updates.borrow_and_update().deadline;
        tokio::select! {
            Ok(()) = updates.changed() => {}
            _ = tokio::time::sleep_until(deadline) => {
                if updates.borrow().deadline == deadline {
                    return;
                }
            }
        }
    }
}

pub(super) async fn report(
    host: &dyn pi_rs::ToolHost,
    server: &str,
    tool: &str,
    mut updates: Updates,
) {
    while updates.changed().await.is_ok() {
        let progress = updates.borrow_and_update().progress.clone();
        if let Some(progress) = progress {
            let text = progress.message.unwrap_or_else(|| {
                let mut buffer = ryu_js::Buffer::new();
                let mut text = format!("Progress {}", buffer.format(progress.progress));
                if let Some(total) = progress.total {
                    text.push('/');
                    text.push_str(buffer.format(total));
                }
                text
            });
            let _ = host
                .report_progress(json!({
                    "content": [{"type": "text", "text": text}],
                    "details": {"server": server, "tool": tool},
                }))
                .await;
            tokio::task::yield_now().await;
        }
    }
}
