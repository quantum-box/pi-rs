use std::{
    collections::BTreeMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::FutureExt;
use http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};

use crate::runtime::time::{Instant, timeout};
use crate::{Extension, ExtensionState, Notification, Observer, Registry, Session};

/// HTTP metadata from an agent's provider response, before its body is consumed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderResponseEvent {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
}

tokio::task_local! {
    static RESPONSE_HOOKS: Option<Arc<ResponseHooks>>;
}

pub(crate) struct ResponseHooks {
    handlers: Vec<(Arc<dyn Extension>, Option<ExtensionState>)>,
    started: Instant,
    model_timeout: Duration,
    errors: Mutex<Vec<(String, String)>>,
}

impl ResponseHooks {
    pub(crate) fn for_session(
        registry: &Registry,
        session: &Session,
        model_timeout: Duration,
    ) -> Option<Arc<Self>> {
        let handlers: Vec<_> = registry
            .extensions
            .iter()
            .filter(|extension| extension.handles_after_provider_response())
            .cloned()
            .collect();
        if handlers.is_empty() {
            return None;
        }
        let mut states = session.extension_states();
        Some(Arc::new(Self {
            handlers: handlers
                .into_iter()
                .map(|extension| {
                    let state = states.remove(extension.id());
                    (extension, state)
                })
                .collect(),
            started: Instant::now(),
            model_timeout,
            errors: Mutex::default(),
        }))
    }

    pub(crate) async fn scope<F: Future>(hooks: Option<Arc<Self>>, future: F) -> F::Output {
        if hooks.is_some() || RESPONSE_HOOKS.try_with(Option::is_some).unwrap_or(false) {
            RESPONSE_HOOKS.scope(hooks, future).await
        } else {
            future.await
        }
    }

    pub(crate) fn report_errors(&self, observer: &mut Observer<'_>) {
        let errors = std::mem::take(&mut *self.errors.lock().expect("provider response errors"));
        for (extension, message) in errors {
            observer(Notification::ExtensionLifecycleError {
                extension: &extension,
                event: "after_provider_response",
                message: &message,
            });
        }
    }
}

/// Whether the current request has a provider response observer.
#[doc(hidden)]
pub fn observes_provider_response() -> bool {
    RESPONSE_HOOKS.try_with(Option::is_some).unwrap_or(false)
}

/// Report an HTTP response before reading its body. Outside an agent request
/// with subscribed extensions, reporting has no effect. The adapter supplies
/// request secrets and its configured headers so that reflected credentials,
/// including explicitly sensitive header values, can be withheld.
pub async fn observe_provider_response(
    status: StatusCode,
    headers: &HeaderMap,
    request_secrets: &[String],
    request_headers: &HeaderMap,
) {
    let Ok(Some(hooks)) = RESPONSE_HOOKS.try_with(Clone::clone) else {
        return;
    };
    let mut visible = BTreeMap::<String, String>::new();
    let secrets: Vec<_> = request_secrets
        .iter()
        .map(String::as_str)
        .chain(
            request_headers
                .values()
                .filter(|value| value.is_sensitive())
                .filter_map(|value| value.to_str().ok()),
        )
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect();
    let contains_secret = |text: &str| secrets.iter().any(|&secret| text.contains(secret));
    for (name, value) in headers {
        let name = name.as_str();
        if value.is_sensitive()
            || crate::retry::is_provider_auth_header(name)
            || matches!(name, "set-cookie" | "cookie")
        {
            continue;
        }
        let Ok(value) = value.to_str() else {
            continue;
        };
        let value = value.trim();
        visible
            .entry(name.to_owned())
            .and_modify(|previous| {
                previous.push_str(", ");
                previous.push_str(value);
            })
            .or_insert_with(|| value.to_owned());
    }
    // Repeated response fields can become a secret only after they are joined.
    visible.retain(|name, value| !contains_secret(name) && !contains_secret(value));
    let event = ProviderResponseEvent {
        status: status.as_u16(),
        headers: visible,
    };
    // Reserve part of the remaining request time for reading and decoding the body.
    let remaining = hooks.model_timeout.saturating_sub(hooks.started.elapsed());
    let handler_timeout = (remaining / 2).min(Duration::from_secs(5))
        / u32::try_from(hooks.handlers.len()).unwrap_or(u32::MAX);
    for (extension, state) in &hooks.handlers {
        let callback = AssertUnwindSafe(async {
            extension
                .after_provider_response(&event, state.as_ref())
                .await
        })
        .catch_unwind();
        let outcome = timeout(handler_timeout, RESPONSE_HOOKS.scope(None, callback)).await;
        let message = match outcome {
            Ok(Ok(Ok(()))) => continue,
            Ok(Ok(Err(message))) => message,
            Ok(Err(_)) => "provider response hook panicked".into(),
            Err(_) => "provider response hook timed out".into(),
        };
        hooks
            .errors
            .lock()
            .expect("provider response errors")
            .push((extension.id().to_owned(), message));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Agent, CancellationToken, ExtensionResult, Limits, MemoryStore, Model, ModelEvent,
        ModelRequest, ModelStream, Result,
    };
    use futures_util::StreamExt;
    use http::{HeaderName, HeaderValue};

    type Calls = Arc<Mutex<Vec<&'static str>>>;

    struct Handler(&'static str, Calls);

    #[async_trait::async_trait]
    impl Extension for Handler {
        fn id(&self) -> &str {
            self.0
        }

        fn handles_after_provider_response(&self) -> bool {
            true
        }

        async fn after_provider_response(
            &self,
            event: &ProviderResponseEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<()> {
            assert_eq!(event.status, 200);
            assert_eq!(
                event.headers,
                BTreeMap::from([("x-request-id".into(), "a, b".into())])
            );
            self.1.lock().unwrap().push(self.0);
            // Transport calls made by a callback do not re-enter the agent hook.
            observe_provider_response(
                StatusCode::CREATED,
                &HeaderMap::new(),
                &[],
                &HeaderMap::new(),
            )
            .await;
            match self.0 {
                "first" => Err("fixture callback failed".into()),
                "panic" => panic!("fixture callback panicked"),
                "stall" => std::future::pending().await,
                _ => Ok(()),
            }
        }
    }

    struct Transport(Calls);

    #[async_trait::async_trait]
    impl Model for Transport {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            let mut headers = HeaderMap::new();
            headers.append(
                HeaderName::from_bytes(b"X-Request-ID").unwrap(),
                HeaderValue::from_static(" a "),
            );
            headers.append("x-request-id", HeaderValue::from_static("b"));
            headers.insert("set-cookie", HeaderValue::from_static("provider-cookie"));
            headers.insert("authorization", HeaderValue::from_static("provider-secret"));
            headers.insert("x-reflected", HeaderValue::from_static("Bearer test-token"));
            headers.insert("x-invalid", HeaderValue::from_bytes(&[0xff]).unwrap());
            let mut sensitive = HeaderValue::from_static("sensitive");
            sensitive.set_sensitive(true);
            headers.insert("x-private", sensitive);

            let mut request_headers = HeaderMap::new();
            let mut sensitive = HeaderValue::from_static(" rate limit ");
            sensitive.set_sensitive(true);
            request_headers.insert("x-private-input", sensitive);
            headers.insert("x-custom-echo", HeaderValue::from_static("rate limit"));
            headers.append("x-split-echo", HeaderValue::from_static("split"));
            headers.append("x-split-echo", HeaderValue::from_static("token"));
            let calls = self.0.clone();
            let stream = futures_util::stream::once(async move {
                observe_provider_response(
                    StatusCode::OK,
                    &headers,
                    &["test-token".into(), "split, token".into()],
                    &request_headers,
                )
                .await;
                calls.lock().unwrap().push("body");
                Ok(ModelEvent::Text("answer".into()))
            })
            .chain(futures_util::stream::iter([Ok(ModelEvent::Complete {
                calls: Vec::new(),
                usage: None,
            })]));
            Ok(Box::pin(stream))
        }
    }

    #[tokio::test]
    async fn provider_response_hooks_run_in_order_before_body_and_report_errors() {
        let calls = Calls::default();
        let mut registry = Registry::default();
        for name in ["first", "panic", "stall", "second"] {
            registry
                .register(Arc::new(Handler(name, calls.clone())))
                .unwrap();
        }
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let transport = Transport(calls.clone());
        let agent = Agent {
            model: &transport,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "",
            limits: Limits {
                model_timeout: Duration::from_secs(1),
                ..Limits::default()
            },
        };
        let (_tx, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
        let mut errors = Vec::new();
        let answer = agent
            .run(
                &mut session,
                Some("question".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |event| {
                    if let Notification::ExtensionLifecycleError {
                        extension,
                        event,
                        message,
                    } = event
                    {
                        errors.push((extension.to_owned(), event, message.to_owned()));
                    }
                },
            )
            .await
            .unwrap();
        assert_eq!(answer, "answer");
        assert_eq!(
            *calls.lock().unwrap(),
            ["first", "panic", "stall", "second", "body"]
        );
        assert_eq!(
            errors,
            [
                (
                    "first".into(),
                    "after_provider_response",
                    "fixture callback failed".into()
                ),
                (
                    "panic".into(),
                    "after_provider_response",
                    "provider response hook panicked".into()
                ),
                (
                    "stall".into(),
                    "after_provider_response",
                    "provider response hook timed out".into()
                ),
            ]
        );
    }
}
