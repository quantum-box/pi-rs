//! Forward a configured model-provider bearer token without storing a second copy.
use std::collections::HashMap;
use std::sync::Arc;

use futures_util::{future::BoxFuture, stream::BoxStream};
use reqwest_mcp::header::{HeaderName, HeaderValue};
use rmcp::model::ClientJsonRpcMessage;
use rmcp::transport::streamable_http_client::{
    SseError, StreamableHttpClient, StreamableHttpError, StreamableHttpPostResponse,
};
use sse_stream::Sse;

pub type TokenResolver =
    Arc<dyn Fn() -> BoxFuture<'static, pi_rs::Result<Option<String>>> + Send + Sync>;

type HttpError = StreamableHttpError<reqwest_mcp::Error>;
type EventStream = BoxStream<'static, std::result::Result<Sse, SseError>>;
type Headers = HashMap<HeaderName, HeaderValue>;

#[derive(Clone)]
pub(crate) struct ProviderClient {
    pub client: reqwest_mcp::Client,
    pub endpoint: Arc<str>,
    pub resolver: TokenResolver,
}

impl ProviderClient {
    async fn token(&self, uri: &str) -> Result<Option<String>, HttpError> {
        if uri != self.endpoint.as_ref() {
            return Err(HttpError::UnexpectedServerResponse(
                "MCP provider token endpoint changed".into(),
            ));
        }
        let token = (self.resolver)().await.ok().flatten().filter(|token| {
            !token.is_empty()
                && token.len() <= 16 * 1024
                && token.bytes().all(|byte| byte.is_ascii_graphic())
        });
        token.map(Some).ok_or_else(|| {
            HttpError::UnexpectedServerResponse("MCP provider authentication is unavailable".into())
        })
    }
}

impl StreamableHttpClient for ProviderClient {
    type Error = reqwest_mcp::Error;

    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        _auth_header: Option<String>,
        headers: Headers,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        let token = self.token(&uri).await?;
        self.client
            .post_message(uri, message, session_id, token, headers)
            .await
    }

    async fn post_message_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        _auth_header: Option<String>,
        headers: Headers,
        max_sse_event_size: usize,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        let token = self.token(&uri).await?;
        self.client
            .post_message_with_max_sse_event_size(
                uri,
                message,
                session_id,
                token,
                headers,
                max_sse_event_size,
            )
            .await
    }

    async fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        _auth_header: Option<String>,
        headers: Headers,
    ) -> Result<EventStream, HttpError> {
        let token = self.token(&uri).await?;
        self.client
            .get_stream(uri, session_id, last_event_id, token, headers)
            .await
    }

    async fn get_stream_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        _auth_header: Option<String>,
        headers: Headers,
        max_sse_event_size: usize,
    ) -> Result<EventStream, HttpError> {
        let token = self.token(&uri).await?;
        self.client
            .get_stream_with_max_sse_event_size(
                uri,
                session_id,
                last_event_id,
                token,
                headers,
                max_sse_event_size,
            )
            .await
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        _auth_header: Option<String>,
        headers: Headers,
    ) -> Result<(), HttpError> {
        let token = self.token(&uri).await?;
        self.client
            .delete_session(uri, session_id, token, headers)
            .await
    }
}
