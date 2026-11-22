use std::{
    collections::BTreeMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::FutureExt;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
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

/// The configured headers available to a request observer before transport authentication.
pub type BeforeProviderHeaders = BTreeMap<String, Option<String>>;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Hook {
    Headers,
    Response,
    Stream,
}

impl Hook {
    fn name(self) -> &'static str {
        match self {
            Self::Headers => "before_provider_headers",
            Self::Response => "after_provider_response",
            Self::Stream => "provider_stream_event",
        }
    }
}

struct Handler {
    extension: Arc<dyn Extension>,
    state: Option<ExtensionState>,
    headers: bool,
    response: bool,
    stream: bool,
}

impl Handler {
    fn subscribes(&self, hook: Hook) -> bool {
        match hook {
            Hook::Headers => self.headers,
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
    stream_budget: Mutex<Duration>,
    errors: Mutex<Vec<Failure>>,
    external_id: Option<String>,
}

impl ProviderHooks {
    pub(crate) fn for_session(
        registry: &Registry,
        session: &Session,
        model_timeout: Duration,
    ) -> Option<Arc<Self>> {
        Self::snapshot(registry, session, model_timeout, false)
    }

    pub(crate) fn for_header_preparation(
        registry: &Registry,
        session: &Session,
        model_timeout: Duration,
    ) -> Option<Arc<Self>> {
        Self::snapshot(registry, session, model_timeout, true)
    }

    fn snapshot(
        registry: &Registry,
        session: &Session,
        model_timeout: Duration,
        preparing_headers: bool,
    ) -> Option<Arc<Self>> {
        let handlers: Vec<_> = registry
            .extensions
            .iter()
            .filter_map(|extension| {
                let headers = preparing_headers && extension.handles_before_provider_headers();
                let response = !preparing_headers && extension.handles_after_provider_response();
                let stream = !preparing_headers && extension.handles_provider_stream_event();
                (headers || response || stream).then(|| Handler {
                    extension: extension.clone(),
                    state: None,
                    headers,
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
            stream_budget: Mutex::new(model_timeout / 4),
            errors: Mutex::default(),
            external_id: preparing_headers.then(|| session.external_session_id()),
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
            Hook::Headers => "provider header hook",
            Hook::Response => "provider response hook",
            Hook::Stream => "provider stream hook",
        };
        let duration = match hook {
            Hook::Headers | Hook::Response => duration,
            Hook::Stream => {
                duration.min(*self.stream_budget.lock().expect("provider stream budget"))
            }
        };
        let started = Instant::now();
        let outcome = if duration.is_zero() && hook == Hook::Stream {
            None
        } else {
            Some(timeout(duration, PROVIDER_HOOKS.scope(None, callback)).await)
        };
        if hook == Hook::Stream {
            let mut remaining = self.stream_budget.lock().expect("provider stream budget");
            *remaining = remaining.saturating_sub(started.elapsed());
        }
        let (message, timed_out) = match outcome {
            Some(Ok(Ok(Ok(())))) => return,
            Some(Ok(Ok(Err(message)))) => (message, false),
            Some(Ok(Err(_))) => (format!("{event} panicked"), false),
            Some(Err(_)) => (format!("{event} timed out"), true),
            None => (
                "provider stream hook exhausted its request budget".into(),
                true,
            ),
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
                .iter()
                .filter_map(|(name, value)| {
                    if value.is_sensitive() || crate::retry::is_provider_auth_header(name.as_str())
                    {
                        value.to_str().ok()
                    } else {
                        None
                    }
                })
                .flat_map(|value| {
                    let value = value.trim_matches([' ', '\t']);
                    let credential = value.split_once([' ', '\t']).and_then(|(scheme, value)| {
                        (scheme.eq_ignore_ascii_case("bearer")
                            || scheme.eq_ignore_ascii_case("basic"))
                        .then_some(value)
                    });
                    [Some(value), credential].into_iter().flatten()
                }),
        )
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect()
}

/// The active session ID used when preparing configured provider headers.
#[doc(hidden)]
pub fn provider_header_session_id() -> Option<String> {
    ProviderHooks::subscribed(Hook::Headers)?
        .external_id
        .clone()
}

fn protected_header_name(name: &str) -> bool {
    crate::retry::is_provider_auth_header(name)
        || name.starts_with("x-amz-")
        || matches!(
            name,
            "host"
                | "cookie"
                | "set-cookie"
                | "content-length"
                | "connection"
                | "keep-alive"
                | "proxy-connection"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
        )
}

/// Run request observers in registration order before freezing adapter configuration.
/// Returns the complete header map when a subscriber was invoked.
#[doc(hidden)]
pub async fn prepare_provider_headers(
    headers: &HeaderMap,
    request_secrets: &[String],
) -> Option<HeaderMap> {
    let hooks = ProviderHooks::subscribed(Hook::Headers)?;
    let secrets = protected_values(request_secrets, headers);
    let mut protected = std::collections::BTreeSet::new();
    let mut current = headers.clone();
    for (name, value) in headers {
        if protected_header_name(name.as_str())
            || value.is_sensitive()
            || value.to_str().is_err()
            || secrets.iter().any(|&secret| {
                name.as_str().contains(secret)
                    || value.to_str().is_ok_and(|value| value.contains(secret))
            })
        {
            protected.insert(name.as_str().to_owned());
        }
    }
    let duration = hooks.handler_timeout(Hook::Headers);
    for (index, handler) in hooks
        .handlers
        .iter()
        .enumerate()
        .filter(|(_, handler)| handler.headers)
    {
        let mut visible = BeforeProviderHeaders::new();
        for (name, value) in &current {
            if protected.contains(name.as_str()) || protected_header_name(name.as_str()) {
                continue;
            }
            let Ok(value) = value.to_str() else { continue };
            visible
                .entry(name.as_str().to_owned())
                .and_modify(|previous| {
                    if let Some(previous) = previous {
                        previous.push_str(", ");
                        previous.push_str(value);
                    }
                })
                .or_insert_with(|| Some(value.to_owned()));
        }
        visible.retain(|name, value| {
            let contains_secret = value
                .as_ref()
                .is_some_and(|value| secrets.iter().any(|&secret| value.contains(secret)));
            if contains_secret {
                protected.insert(name.clone());
            }
            !contains_secret
        });
        let mut edited = visible.clone();
        hooks
            .notify(index, Hook::Headers, duration, async {
                handler
                    .extension
                    .before_provider_headers(&mut edited, handler.state.as_ref())
                    .await?;
                current = apply_header_edits(&current, &visible, edited, &protected)?;
                Ok(())
            })
            .await;
    }
    Some(current)
}

fn apply_header_edits(
    current: &HeaderMap,
    previous: &BeforeProviderHeaders,
    edited: BeforeProviderHeaders,
    protected: &std::collections::BTreeSet<String>,
) -> ExtensionResult<HeaderMap> {
    let mut next = current.clone();
    for name in previous.keys().filter(|name| !edited.contains_key(*name)) {
        next.remove(name.as_str());
    }
    for (name, value) in edited {
        if previous.get(&name) == Some(&value) {
            continue;
        }
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| "invalid provider header name".to_owned())?;
        if protected.contains(name.as_str()) || protected_header_name(name.as_str()) {
            return Err("provider header hook cannot change managed or sensitive headers".into());
        }
        if let Some(value) = value {
            let value = HeaderValue::from_str(value.trim_matches([' ', '\t']))
                .map_err(|_| "invalid provider header value".to_owned())?;
            next.try_insert(name, value)
                .map_err(|_| "provider header hook exceeded the HTTP header limit".to_owned())?;
        } else {
            next.remove(name);
        }
    }
    Ok(next)
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
                let field = name.to_ascii_lowercase().replace('_', "-");
                if crate::retry::is_provider_auth_header(&field)
                    || matches!(
                        field.as_str(),
                        "cookie"
                            | "set-cookie"
                            | "apikey"
                            | "access-token"
                            | "accesstoken"
                            | "refresh-token"
                            | "refreshtoken"
                    )
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
        scalar => {
            if !secrets.is_empty() && secrets.contains(&scalar.to_string().as_str()) {
                *scalar = Value::String("[redacted]".into());
            }
        }
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
                                    "numeric": 8675309,
                                    "unknown": ["preserved", "prefix fixture-token suffix"],
                                    "authorization": "unconfigured credential",
                                    "refreshToken": "unconfigured refresh credential"
                                }),
                                &["fixture-token".into(), "8675309".into()],
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
                    "numeric": "[redacted]",
                    "unknown": ["preserved", "[redacted]"]
                })
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn provider_stream_callbacks_share_one_budget_across_frames() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct SlowObserver(AtomicUsize);
        #[async_trait::async_trait]
        impl Extension for SlowObserver {
            fn id(&self) -> &str {
                "slow"
            }
            fn handles_provider_stream_event(&self) -> bool {
                true
            }
            async fn provider_stream_event(
                &self,
                _: &ProviderStreamEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<()> {
                tokio::time::sleep(Duration::from_millis(7)).await;
                self.0.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }

        let extension = Arc::new(SlowObserver(AtomicUsize::new(0)));
        let mut registry = Registry::default();
        registry.register(extension.clone()).unwrap();
        let session = Session::with_store(MemoryStore::default()).unwrap();
        let hooks = ProviderHooks::for_session(&registry, &session, Duration::from_millis(80));
        ProviderHooks::scope(hooks.clone(), async {
            for _ in 0..10 {
                observe_provider_stream(
                    "fixture",
                    ModelApi::OpenaiResponses,
                    "model-id",
                    &Value::Null,
                    &[],
                    &HeaderMap::new(),
                )
                .await;
            }
        })
        .await;
        assert_eq!(extension.0.load(Ordering::Relaxed), 2);
        let mut errors = Vec::new();
        hooks.unwrap().report_errors(&mut |event| {
            if let Notification::ExtensionLifecycleError { message, .. } = event {
                errors.push(message.to_owned());
            }
        });
        assert_eq!(errors, ["provider stream hook timed out"]);
    }

    #[tokio::test]
    async fn provider_header_hooks_edit_only_unprotected_configuration_in_order() {
        struct Edit(&'static str);
        #[async_trait::async_trait]
        impl Extension for Edit {
            fn id(&self) -> &str {
                self.0
            }
            fn handles_before_provider_headers(&self) -> bool {
                true
            }
            async fn before_provider_headers(
                &self,
                headers: &mut BeforeProviderHeaders,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<()> {
                assert!(provider_header_session_id().is_none());
                for hidden in [
                    "authorization",
                    "x-api-key",
                    "x-private",
                    "x-combined",
                    "x-reflected",
                    "x-gateway-echo",
                    "x-proxy-echo",
                    "chatgpt-account-id",
                    "x-codex-echo",
                ] {
                    assert!(!headers.contains_key(hidden));
                }
                match self.0 {
                    "edit" => {
                        assert_eq!(headers["x-repeated"].as_deref(), Some("a, b"));
                        headers.insert("X-TRACE".into(), Some(" edited ".into()));
                        headers.insert("x-delete".into(), None);
                    }
                    "blocked" => {
                        headers.insert("ChatGPT-Account-ID".into(), Some("bad".into()));
                        headers.insert("x-rejected".into(), Some("bad".into()));
                    }
                    "later" => {
                        assert_eq!(headers["x-trace"].as_deref(), Some("edited"));
                        assert!(!headers.contains_key("x-delete"));
                        assert!(!headers.contains_key("x-rejected"));
                        headers.insert("x-completion".into(), Some("yes".into()));
                    }
                    _ => unreachable!(),
                }
                Ok(())
            }
        }

        let mut registry = Registry::default();
        for id in ["edit", "blocked", "later"] {
            registry.register(Arc::new(Edit(id))).unwrap();
        }
        let session = Session::with_store(MemoryStore::default()).unwrap();
        let hooks =
            ProviderHooks::for_header_preparation(&registry, &session, Duration::from_secs(5));
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static(" \tBearer credential \t"),
        );
        headers.insert("x-api-key", HeaderValue::from_static("gateway-key"));
        headers.insert(
            "proxy-authorization",
            HeaderValue::from_static("\t Basic proxy-key \t"),
        );
        headers.insert(
            "x-proxy-echo",
            HeaderValue::from_static("reflected proxy-key"),
        );
        headers.insert(
            "x-gateway-echo",
            HeaderValue::from_static("reflected gateway-key"),
        );
        headers.insert(
            "x-codex-echo",
            HeaderValue::from_static("reflected codex-account"),
        );
        headers.insert("x-trace", HeaderValue::from_static("initial"));
        headers.insert("x-delete", HeaderValue::from_static("delete"));
        headers.insert("x-reflected", HeaderValue::from_static("echo credential"));
        headers.append("x-repeated", HeaderValue::from_static("a"));
        headers.append("x-repeated", HeaderValue::from_static("b"));
        headers.append("x-combined", HeaderValue::from_static("first"));
        headers.append("x-combined", HeaderValue::from_static("second"));
        let mut private = HeaderValue::from_static("hidden");
        private.set_sensitive(true);
        headers.insert("x-private", private);
        let edited = ProviderHooks::scope(hooks.clone(), async {
            assert_eq!(
                provider_header_session_id().as_deref(),
                Some(session.external_session_id().as_str())
            );
            prepare_provider_headers(&headers, &["first, second".into(), "codex-account".into()])
                .await
                .unwrap()
        })
        .await;
        assert_eq!(edited["authorization"], " \tBearer credential \t");
        assert!(!edited.contains_key("chatgpt-account-id"));
        assert_eq!(edited["x-codex-echo"], "reflected codex-account");
        assert_eq!(edited["x-reflected"], "echo credential");
        assert_eq!(edited["x-private"], "hidden");
        assert_eq!(edited["x-trace"], "edited");
        assert_eq!(edited["x-completion"], "yes");
        assert_eq!(edited.get_all("x-repeated").iter().count(), 2);
        assert_eq!(edited.get_all("x-combined").iter().count(), 2);
        assert!(!edited.contains_key("x-delete"));
        let mut failures = Vec::new();
        hooks.unwrap().report_errors(&mut |notification| {
            if let Notification::ExtensionLifecycleError {
                extension, event, ..
            } = notification
            {
                failures.push((extension.to_owned(), event));
            }
        });
        assert_eq!(
            failures,
            [("blocked".to_owned(), "before_provider_headers")]
        );
    }

    #[tokio::test]
    async fn provider_header_turn_edits_do_not_leak_into_threshold_summary() {
        struct Change;
        #[async_trait::async_trait]
        impl Extension for Change {
            fn id(&self) -> &str {
                "change-headers"
            }
            fn handles_before_provider_headers(&self) -> bool {
                true
            }
            async fn before_provider_headers(
                &self,
                headers: &mut BeforeProviderHeaders,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<()> {
                headers.insert("x-route".into(), Some("turn-only".into()));
                Ok(())
            }
        }

        #[derive(Clone)]
        struct RecordingModel {
            headers: HeaderMap,
            used: Arc<Mutex<Vec<(&'static str, String)>>>,
        }

        impl RecordingModel {
            fn answer(&self, kind: &'static str, content: &str) -> ModelStream {
                self.used
                    .lock()
                    .unwrap()
                    .push((kind, self.headers["x-route"].to_str().unwrap().into()));
                Box::pin(futures_util::stream::iter([
                    Ok(ModelEvent::Text(content.into())),
                    Ok(ModelEvent::Complete {
                        calls: Vec::new(),
                        usage: None,
                    }),
                ]))
            }
        }

        #[async_trait::async_trait]
        impl Model for RecordingModel {
            async fn prepare_context<'a>(
                &'a self,
                _: &ModelRequest,
            ) -> Result<Option<Arc<dyn Model + 'a>>> {
                let modified = prepare_provider_headers(&self.headers, &[]).await;
                Ok(modified.map(|headers| {
                    Arc::new(Self {
                        headers,
                        used: self.used.clone(),
                    }) as Arc<dyn Model + 'a>
                }))
            }

            async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
                Ok(self.answer("turn", "answer"))
            }

            async fn stream_for_summary(
                &self,
                _: uuid::Uuid,
                _: uuid::Uuid,
                _: ModelRequest,
            ) -> Result<ModelStream> {
                Ok(self.answer("summary", "summary"))
            }
        }

        let model = RecordingModel {
            headers: HeaderMap::from_iter([(
                HeaderName::from_static("x-route"),
                HeaderValue::from_static("original"),
            )]),
            used: Arc::default(),
        };
        let mut registry = Registry::default();
        registry.register(Arc::new(Change)).unwrap();
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "",
            limits: Limits {
                model_requests: 2,
                context_window: Some(1000),
                output_tokens_per_request: 16,
                compaction_tokens: Some(crate::CompactionTokens {
                    reserve_tokens: Some(1000),
                    keep_recent_tokens: None,
                }),
                ..Limits::default()
            },
        };
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append(crate::Event::User {
                content: "old".into(),
                input_id: None,
            })
            .unwrap();
        session
            .append(crate::Event::Assistant {
                content: "old reply".into(),
                invocations: Vec::new(),
                usage: None,
                request_id: None,
            })
            .unwrap();
        let (_tx, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
        let answer = agent
            .run(
                &mut session,
                Some("new".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        assert_eq!(answer, "answer");
        assert_eq!(
            *model.used.lock().unwrap(),
            [
                ("summary", "original".to_owned()),
                ("turn", "turn-only".to_owned()),
            ]
        );
    }
}
