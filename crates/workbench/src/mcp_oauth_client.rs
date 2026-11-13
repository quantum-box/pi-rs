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
use std::{collections::HashMap, sync::Arc};

type HttpError = StreamableHttpError<reqwest_mcp::Error>;
type Headers = HashMap<HeaderName, HeaderValue>;
type EventStream = BoxStream<'static, std::result::Result<Sse, SseError>>;

#[derive(Clone)]
pub struct SafeAuthClient(pub(crate) AuthClient<reqwest_mcp::Client>);

impl SafeAuthClient {
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
        match call(token.clone()).await {
            Err(HttpError::AuthRequired(challenge)) => {
                let Some(sent) = token else {
                    return Err(HttpError::AuthRequired(challenge));
                };
                match self.token(true).await {
                    Ok(fresh) if fresh != sent => call(Some(fresh)).await,
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
