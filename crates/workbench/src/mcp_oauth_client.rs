//! Preserve OAuth refreshes while keeping MCP requests cancellable.
use futures_util::stream::BoxStream;
use oauth2::TokenResponse;
use reqwest_mcp::header::{HeaderName, HeaderValue};
use rmcp::{
    model::ClientJsonRpcMessage,
    transport::{
        auth::{AuthClient, AuthError},
        streamable_http_client::{
            SseError, StreamableHttpClient, StreamableHttpError, StreamableHttpPostResponse,
        },
    },
};
use sse_stream::Sse;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

type HttpError = StreamableHttpError<reqwest_mcp::Error>;
type Headers = HashMap<HeaderName, HeaderValue>;
type EventStream = BoxStream<'static, std::result::Result<Sse, SseError>>;

#[derive(Clone)]
pub struct SafeAuthClient(
    pub(crate) AuthClient<reqwest_mcp::Client>,
    pub(crate) Arc<Mutex<DispatchState>>,
);

#[derive(Default)]
pub(crate) struct DispatchState {
    pub(crate) closed: bool,
    pub(crate) cancellation: Option<pi_rs::CancellationToken>,
    #[cfg(test)]
    before_commit: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    after_commit: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl SafeAuthClient {
    async fn dispatch<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, HttpError>>,
    ) -> Result<T, HttpError> {
        dispatch(&self.1, future).await
    }
    async fn token(&self, force: bool) -> Result<String, AuthError> {
        let client = self.0.clone();
        crate::mcp_refresh::run(async move {
            if !force {
                return client.get_access_token().await;
            }
            let manager = client.auth_manager.lock().await;
            match manager.refresh_token().await {
                Ok(token) => Ok(token.access_token().secret().to_owned()),
                Err(AuthError::AuthorizationRequired | AuthError::TokenRefreshRejected(_)) => {
                    Err(AuthError::AuthorizationRequired)
                }
                Err(error) => Err(error),
            }
        })
        .await
        .map_err(|()| AuthError::InternalError("MCP OAuth operation stopped".into()))?
    }
}

async fn dispatch<T>(
    gate: &Mutex<DispatchState>,
    future: impl std::future::Future<Output = Result<T, HttpError>>,
) -> Result<T, HttpError> {
    let mut future = std::pin::pin!(future);
    let committed = std::sync::atomic::AtomicBool::new(false);
    std::future::poll_fn(|cx| {
        let state = gate.lock().unwrap_or_else(|error| error.into_inner());
        let cancelled = || {
            state.closed
                || state
                    .cancellation
                    .as_ref()
                    .is_some_and(pi_rs::CancellationToken::is_cancelled)
        };
        if cancelled() {
            return std::task::Poll::Ready(Err(HttpError::UnexpectedServerResponse(
                "MCP request cancelled before dispatch".into(),
            )));
        }
        if !committed.load(std::sync::atomic::Ordering::SeqCst) {
            #[cfg(test)]
            if let Some(hook) = &state.before_commit {
                hook();
            }
            // This is the dispatch-start boundary. Cancellation can be set
            // without this mutex, so recheck after committing and before
            // polling the HTTP future. A cancellation observed here wins.
            committed.store(true, std::sync::atomic::Ordering::SeqCst);
            if cancelled() {
                return std::task::Poll::Ready(Err(HttpError::UnexpectedServerResponse(
                    "MCP request cancelled before dispatch".into(),
                )));
            }
            #[cfg(test)]
            if let Some(hook) = &state.after_commit {
                hook();
            }
        }
        future.as_mut().poll(cx)
    })
    .await
}

impl SafeAuthClient {
    pub(crate) async fn call<T, F, Fut>(
        &self,
        token: Option<String>,
        call: F,
    ) -> Result<T, HttpError>
    where
        F: Fn(Option<String>) -> Fut,
        Fut: std::future::Future<Output = Result<T, HttpError>>,
    {
        let token = match token {
            Some(token) => Some(token),
            None => match self.token(false).await {
                Ok(token) => Some(token),
                Err(AuthError::AuthorizationRequired) => None,
                Err(error) => return Err(error.into()),
            },
        };
        // Only the token transaction outlives this future. The actual call and
        // retry stay on the caller's task, so dropping it cannot dispatch them.
        match self.dispatch(call(token.clone())).await {
            Err(HttpError::AuthRequired(challenge)) => {
                let Some(sent) = token else {
                    return Err(HttpError::AuthRequired(challenge));
                };
                match self.token(true).await {
                    Ok(fresh) if fresh != sent => self.dispatch(call(Some(fresh))).await,
                    Ok(_) | Err(AuthError::AuthorizationRequired) => {
                        Err(HttpError::AuthRequired(challenge))
                    }
                    Err(error) => Err(error.into()),
                }
            }
            result => result,
        }
    }
}
impl StreamableHttpClient for SafeAuthClient {
    type Error = reqwest_mcp::Error;
    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        token: Option<String>,
        headers: Headers,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        self.call(token, |token| {
            self.0.http_client.post_message(
                uri.clone(),
                message.clone(),
                session_id.clone(),
                token,
                headers.clone(),
            )
        })
        .await
    }
    async fn post_message_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        token: Option<String>,
        headers: Headers,
        max: usize,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        self.call(token, |token| {
            self.0.http_client.post_message_with_max_sse_event_size(
                uri.clone(),
                message.clone(),
                session_id.clone(),
                token,
                headers.clone(),
                max,
            )
        })
        .await
    }
    async fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last: Option<String>,
        token: Option<String>,
        headers: Headers,
    ) -> Result<EventStream, HttpError> {
        self.call(token, |token| {
            self.0.http_client.get_stream(
                uri.clone(),
                session_id.clone(),
                last.clone(),
                token,
                headers.clone(),
            )
        })
        .await
    }
    async fn get_stream_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last: Option<String>,
        token: Option<String>,
        headers: Headers,
        max: usize,
    ) -> Result<EventStream, HttpError> {
        self.call(token, |token| {
            self.0.http_client.get_stream_with_max_sse_event_size(
                uri.clone(),
                session_id.clone(),
                last.clone(),
                token,
                headers.clone(),
                max,
            )
        })
        .await
    }
    async fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        token: Option<String>,
        headers: Headers,
    ) -> Result<(), HttpError> {
        self.call(token, |token| {
            self.0.http_client.delete_session(
                uri.clone(),
                session_id.clone(),
                token,
                headers.clone(),
            )
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn cancellation_and_dispatch_have_a_defined_start_boundary() {
        for cancel_before_commit in [true, false] {
            let token = pi_rs::CancellationToken::new();
            let (reached_tx, reached_rx) = std::sync::mpsc::channel();
            let (resume_tx, resume_rx) = std::sync::mpsc::channel();
            let resume_rx = Mutex::new(resume_rx);
            let hook: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
                reached_tx.send(()).unwrap();
                resume_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            });
            let state = DispatchState {
                cancellation: Some(token.clone()),
                before_commit: cancel_before_commit.then(|| hook.clone()),
                after_commit: (!cancel_before_commit).then_some(hook),
                ..Default::default()
            };
            let gate = Arc::new(Mutex::new(state));
            let polls = Arc::new(AtomicUsize::new(0));
            let worker_polls = polls.clone();
            let worker = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(dispatch(&gate, async move {
                    worker_polls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }))
            });
            reached_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            token.cancel();
            resume_tx.send(()).unwrap();
            let result = worker.join().unwrap();
            assert_eq!(result.is_err(), cancel_before_commit);
            assert_eq!(
                polls.load(Ordering::SeqCst),
                usize::from(!cancel_before_commit)
            );
        }
    }
}
