//! Correlated extension interactions between the worker and its frontend.
use pi_rs::{CancellationToken, ExtensionResult, ExtensionUi, UiRequest, UiResponse};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Default)]
pub struct Broker {
    pending: Arc<Mutex<BTreeMap<Uuid, oneshot::Sender<UiResponse>>>>,
}
struct Pending {
    id: Uuid,
    requests: Arc<Mutex<BTreeMap<Uuid, oneshot::Sender<UiResponse>>>>,
    completed: bool,
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.requests
            .lock()
            .expect("extension UI requests")
            .remove(&self.id);
        if !self.completed {
            super::output(serde_json::json!({"type":"extension_ui_cancelled","id":self.id}));
        }
    }
}
impl Broker {
    pub fn resolve(&self, id: Uuid, value: UiResponse) {
        if let Some(sender) = self
            .pending
            .lock()
            .expect("extension UI requests")
            .remove(&id)
        {
            let _ = sender.send(value);
        }
    }
}
#[async_trait::async_trait]
impl ExtensionUi for Broker {
    async fn request(
        &self,
        request: UiRequest,
        cancellation: CancellationToken,
    ) -> ExtensionResult<UiResponse> {
        request.validate()?;
        let id = Uuid::new_v4();
        let response = if request.needs_response() {
            let (sender, receiver) = oneshot::channel();
            let mut requests = self.pending.lock().expect("extension UI requests");
            if requests.len() >= 16 {
                return Err("too many pending extension UI requests".into());
            }
            requests.insert(id, sender);
            Some(receiver)
        } else {
            None
        };
        let mut guard = Pending {
            id,
            requests: self.pending.clone(),
            completed: !request.needs_response(),
        };
        let mut event = serde_json::to_value(&request).map_err(|error| error.to_string())?;
        event["type"] = serde_json::json!("extension_ui_request");
        event["id"] = serde_json::json!(id);
        super::output(event);
        let Some(response) = response else {
            return Ok(UiResponse::Cancelled);
        };
        let deadline = request
            .timeout()
            .map(|millis| {
                tokio::time::Instant::now()
                    .checked_add(std::time::Duration::from_millis(millis))
                    .ok_or_else(|| "extension UI timeout is too large".to_owned())
            })
            .transpose()?;
        let expiry = async {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        let value = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err("extension UI cancelled".into()),
            _ = expiry => return Ok(UiResponse::Cancelled),
            response = response => response.map_err(|_| "extension UI response was lost")?,
        };
        guard.completed = true;
        if !request.accepts_response(&value) {
            return Err("invalid extension UI response".into());
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn replies_are_correlated_and_cancelled_dialogs_release_their_slot() {
        let broker = Arc::new(Broker::default());
        let task_broker = broker.clone();
        let task = tokio::spawn(async move {
            task_broker
                .request(
                    UiRequest::Confirm {
                        title: "Confirm".into(),
                        message: "Continue?".into(),
                        timeout: None,
                    },
                    CancellationToken::new(),
                )
                .await
        });
        tokio::task::yield_now().await;
        let id = *broker.pending.lock().unwrap().keys().next().unwrap();
        broker.resolve(id, UiResponse::Confirmed(true));
        assert_eq!(task.await.unwrap().unwrap(), UiResponse::Confirmed(true));
        assert!(broker.pending.lock().unwrap().is_empty());
        let cancellation = CancellationToken::new();
        let token = cancellation.clone();
        let task_broker = broker.clone();
        let task = tokio::spawn(async move {
            task_broker
                .request(
                    UiRequest::Input {
                        title: "Text".into(),
                        placeholder: None,
                        initial_value: None,
                        timeout: None,
                    },
                    token,
                )
                .await
        });
        tokio::task::yield_now().await;
        cancellation.cancel();
        assert!(task.await.unwrap().is_err());
        assert!(broker.pending.lock().unwrap().is_empty());
    }
}

#[cfg(test)]
mod timeout_tests {
    use super::*;
    #[tokio::test]
    async fn dialog_timeout_returns_cancelled_and_releases_the_request() {
        let broker = Broker::default();
        let result = broker
            .request(
                UiRequest::Select {
                    title: "Select".into(),
                    options: vec!["one".into()],
                    timeout: Some(1),
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(result, UiResponse::Cancelled);
        assert!(broker.pending.lock().unwrap().is_empty());
    }
}
