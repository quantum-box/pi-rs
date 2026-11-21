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
use serde_json::Value;

use crate::runtime::time::{Instant, timeout};
use crate::{
    Extension, ExtensionResult, ExtensionState, ModelApi, Notification, Observer, Registry, Session,
};

/// HTTP metadata from an agent's provider response, before its body is consumed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderResponseEvent {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
}

/// A parsed provider event before the adapter normalizes its contents.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderStreamEvent {
    pub provider: String,
    pub api: ModelApi,
    pub model: String,
    pub data: Value,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Hook {
    Response,
    Stream,
}

impl Hook {
    fn name(self) -> &'static str {
        match self {
            Self::Response => "after_provider_response",
            Self::Stream => "provider_stream_event",
        }
    }
}

struct Handler {
    extension: Arc<dyn Extension>,
    state: Option<ExtensionState>,
    response: bool,
    stream: bool,
}

impl Handler {
    fn subscribes(&self, hook: Hook) -> bool {
        match hook {
            Hook::Response => self.response,
            Hook::Stream => self.stream,
        }
    }
}

struct Failure {
    handler: usize,
    hook: Hook,
    message: String,
    timed_out: bool,
}

tokio::task_local! {
    static PROVIDER_HOOKS: Option<Arc<ProviderHooks>>;
}

pub(crate) struct ProviderHooks {
    handlers: Vec<Handler>,
    started: Instant,
    model_timeout: Duration,
    errors: Mutex<Vec<Failure>>,
}

impl ProviderHooks {
    pub(crate) fn for_session(
        registry: &Registry,
        session: &Session,
        model_timeout: Duration,
    ) -> Option<Arc<Self>> {
        let handlers: Vec<_> = registry
            .extensions
            .iter()
            .filter_map(|extension| {
                let response = extension.handles_after_provider_response();
                let stream = extension.handles_provider_stream_event();
                (response || stream).then(|| Handler {
                    extension: extension.clone(),
                    state: None,
                    response,
                    stream,
                })
            })
            .collect();
        if handlers.is_empty() {
            return None;
        }
        let mut states = session.extension_states();
        let mut handlers = handlers;
        for handler in &mut handlers {
            handler.state = states.remove(handler.extension.id());
        }
        Some(Arc::new(Self {
            handlers,
            started: Instant::now(),
            model_timeout,
            errors: Mutex::default(),
        }))
    }

    pub(crate) async fn scope<F: Future>(hooks: Option<Arc<Self>>, future: F) -> F::Output {
        if hooks.is_some() || PROVIDER_HOOKS.try_with(Option::is_some).unwrap_or(false) {
            PROVIDER_HOOKS.scope(hooks, future).await
        } else {
            future.await
        }
    }

    pub(crate) fn report_errors(&self, observer: &mut Observer<'_>) {
        let errors = std::mem::take(&mut *self.errors.lock().expect("provider hook errors"));
        for failure in errors {
            observer(Notification::ExtensionLifecycleError {
                extension: self.handlers[failure.handler].extension.id(),
                event: failure.hook.name(),
                message: &failure.message,
            });
        }
    }

    fn subscribed(hook: Hook) -> Option<Arc<Self>> {
        PROVIDER_HOOKS
            .try_with(|hooks| {
                hooks
                    .as_ref()
                    .filter(|hooks| {
                        hooks
                            .handlers
                            .iter()
                            .any(|handler| handler.subscribes(hook))
                    })
                    .cloned()
            })
            .ok()
            .flatten()
    }

    fn handler_timeout(&self, hook: Hook) -> Duration {
        let remaining = self.model_timeout.saturating_sub(self.started.elapsed());
        let subscribers = self
            .handlers
            .iter()
            .filter(|handler| handler.subscribes(hook))
            .count();
        (remaining / 2).min(Duration::from_secs(5)) / u32::try_from(subscribers).unwrap_or(u32::MAX)
    }

    async fn notify<F: Future<Output = ExtensionResult<()>>>(
        &self,
        index: usize,
        hook: Hook,
        duration: Duration,
        future: F,
    ) {
        if self
            .errors
            .lock()
            .expect("provider hook errors")
            .iter()
            .any(|failure| failure.handler == index && failure.hook == hook && failure.timed_out)
        {
            return;
        }
        let callback = AssertUnwindSafe(future).catch_unwind();
        let event = match hook {
            Hook::Response => "provider response hook",
            Hook::Stream => "provider stream hook",
        };
        let outcome = timeout(duration, PROVIDER_HOOKS.scope(None, callback)).await;
        let (message, timed_out) = match outcome {
            Ok(Ok(Ok(()))) => return,
            Ok(Ok(Err(message))) => (message, false),
            Ok(Err(_)) => (format!("{event} panicked"), false),
            Err(_) => (format!("{event} timed out"), true),
        };
        let mut errors = self.errors.lock().expect("provider hook errors");
        if let Some(previous) = errors
            .iter_mut()
            .find(|failure| failure.handler == index && failure.hook == hook)
        {
            if timed_out {
                previous.message = message;
                previous.timed_out = true;
            }
        } else {
            errors.push(Failure {
                handler: index,
                hook,
                message,
                timed_out,
            });
        }
    }
}

/// Whether the current request has a provider response observer.
#[doc(hidden)]
pub fn observes_provider_response() -> bool {
    ProviderHooks::subscribed(Hook::Response).is_some()
}

/// Whether the current request has a provider stream observer.
#[doc(hidden)]
pub fn observes_provider_stream() -> bool {
    ProviderHooks::subscribed(Hook::Stream).is_some()
}

fn protected_values<'a>(
    request_secrets: &'a [String],
    request_headers: &'a HeaderMap,
) -> Vec<&'a str> {
    request_secrets
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
        .collect()
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
    let Some(hooks) = ProviderHooks::subscribed(Hook::Response) else {
        return;
    };
    let mut visible = BTreeMap::<String, String>::new();
    let secrets = protected_values(request_secrets, request_headers);
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
    let duration = hooks.handler_timeout(Hook::Response);
    for (index, handler) in hooks
        .handlers
        .iter()
        .enumerate()
        .filter(|(_, handler)| handler.response)
    {
        hooks
            .notify(index, Hook::Response, duration, async {
                handler
                    .extension
                    .after_provider_response(&event, handler.state.as_ref())
                    .await
            })
            .await;
    }
}

/// Report a parsed wire event before normalizing it into agent events.
pub async fn observe_provider_stream(
    provider: &str,
    api: ModelApi,
    model: &str,
    data: &Value,
    request_secrets: &[String],
    request_headers: &HeaderMap,
) {
    let Some(hooks) = ProviderHooks::subscribed(Hook::Stream) else {
        return;
    };
    let secrets = protected_values(request_secrets, request_headers);
    let mut data = data.clone();
    scrub_stream_data(&mut data, &secrets);
    let event = ProviderStreamEvent {
        provider: provider.into(),
        api,
        model: model.into(),
        data,
    };
    let duration = hooks.handler_timeout(Hook::Stream);
    for (index, handler) in hooks
        .handlers
        .iter()
        .enumerate()
        .filter(|(_, handler)| handler.stream)
    {
        hooks
            .notify(index, Hook::Stream, duration, async {
                handler
                    .extension
                    .provider_stream_event(&event, handler.state.as_ref())
                    .await
            })
            .await;
    }
}

fn scrub_stream_data(value: &mut Value, secrets: &[&str]) {
    match value {
        Value::String(text) => {
            if secrets.iter().any(|&secret| text.contains(secret)) {
                *text = "[redacted]".into();
            }
        }
        Value::Object(fields) => {
            fields.retain(|name, value| {
                if crate::retry::is_provider_auth_header(name)
                    || matches!(name.to_ascii_lowercase().as_str(), "cookie" | "set-cookie")
                    || secrets.iter().any(|&secret| name.contains(secret))
                {
                    return false;
                }
                scrub_stream_data(value, secrets);
                true
            });
        }
        Value::Array(items) => items
            .iter_mut()
            .for_each(|value| scrub_stream_data(value, secrets)),
        _ => {}
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

    #[tokio::test]
    async fn provider_stream_events_precede_normalization_and_hide_credentials() {
        struct StreamObserver(Arc<Mutex<Vec<ProviderStreamEvent>>>);
        #[async_trait::async_trait]
        impl Extension for StreamObserver {
            fn id(&self) -> &str {
                "stream-observer"
            }

            fn handles_provider_stream_event(&self) -> bool {
                true
            }

            async fn provider_stream_event(
                &self,
                event: &ProviderStreamEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<()> {
                self.0.lock().unwrap().push(event.clone());
                Err("fixture observation failed".into())
            }
        }

        struct StreamTransport;
        #[async_trait::async_trait]
        impl Model for StreamTransport {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
                Ok(Box::pin(
                    futures_util::stream::once(async {
                        let headers = HeaderMap::new();
                        for ordinal in [0, 1] {
                            observe_provider_stream(
                                "fixture",
                                ModelApi::OpenaiResponses,
                                "model-id",
                                &serde_json::json!({
                                    "ordinal": ordinal,
                                    "unknown": ["preserved", "prefix fixture-token suffix"],
                                    "authorization": "unconfigured credential"
                                }),
                                &["fixture-token".into()],
                                &headers,
                            )
                            .await;
                        }
                        Ok(ModelEvent::Text("answer".into()))
                    })
                    .chain(futures_util::stream::iter([Ok(
                        ModelEvent::Complete {
                            calls: Vec::new(),
                            usage: None,
                        },
                    )])),
                ))
            }
        }

        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut registry = Registry::default();
        registry
            .register(Arc::new(StreamObserver(seen.clone())))
            .unwrap();
        let agent = Agent {
            model: &StreamTransport,
            model_name: "model-id",
            registry: &registry,
            system_prompt: "",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let (_tx, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
        let mut errors = 0;
        let answer = agent
            .run(
                &mut session,
                Some("question".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |event| match event {
                    Notification::Text("answer") => assert_eq!(seen.lock().unwrap().len(), 2),
                    Notification::ExtensionLifecycleError {
                        event: "provider_stream_event",
                        ..
                    } => errors += 1,
                    _ => {}
                },
            )
            .await
            .unwrap();
        assert_eq!(answer, "answer");
        assert_eq!(errors, 1);
        let events = seen.lock().unwrap();
        for (ordinal, event) in events.iter().enumerate() {
            assert_eq!(event.provider, "fixture");
            assert_eq!(event.api, ModelApi::OpenaiResponses);
            assert_eq!(event.model, "model-id");
            assert_eq!(
                event.data,
                serde_json::json!({
                    "ordinal": ordinal,
                    "unknown": ["preserved", "[redacted]"]
                })
            );
        }
    }
}
