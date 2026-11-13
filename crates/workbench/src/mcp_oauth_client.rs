//! Preserve rmcp's refresh transaction when a transport request is cancelled.
use futures_util::stream::BoxStream;
use reqwest_mcp::header::{HeaderName, HeaderValue};
use rmcp::{
    model::ClientJsonRpcMessage,
    transport::{
        auth::AuthClient,
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
    async fn run<T: Send + 'static>(
        &self,
        f: impl std::future::Future<Output = Result<T, HttpError>> + Send + 'static,
    ) -> Result<T, HttpError> {
        crate::mcp_refresh::run(f).await.map_err(|()| {
            HttpError::UnexpectedServerResponse("MCP OAuth operation stopped".into())
        })?
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
        let client = self.0.clone();
        self.run(async move {
            client
                .post_message(uri, message, session_id, token, headers)
                .await
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
        let client = self.0.clone();
        self.run(async move {
            client
                .post_message_with_max_sse_event_size(uri, message, session_id, token, headers, max)
                .await
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
        let client = self.0.clone();
        self.run(async move {
            client
                .get_stream(uri, session_id, last, token, headers)
                .await
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
        let client = self.0.clone();
        self.run(async move {
            client
                .get_stream_with_max_sse_event_size(uri, session_id, last, token, headers, max)
                .await
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
        let client = self.0.clone();
        self.run(async move { client.delete_session(uri, session_id, token, headers).await })
            .await
    }
}
