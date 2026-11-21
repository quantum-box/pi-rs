//! Codex WebSocket transport over the same TLS and proxy settings as Responses SSE.
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use pi_rs_core::{Error, Result};
use reqwest::header::{self, HeaderMap, HeaderValue};
use reqwest::{Client, StatusCode, Url, Version};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        handshake::derive_accept_key,
        protocol::{Role, WebSocketConfig},
    },
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const IDLE_TTL: Duration = Duration::from_secs(5 * 60);
const MAX_SOCKET_AGE: Duration = Duration::from_secs(55 * 60);
const MAX_IDLE_CONNECTIONS: usize = 8;
const MAX_FALLBACK_SESSIONS: usize = 64;
const MAX_CACHED_CONTEXT: usize = 2 * 1024 * 1024;
type Socket = WebSocketStream<reqwest::Upgraded>;

static POOL: Mutex<VecDeque<IdleConnection>> = Mutex::new(VecDeque::new());
static FALLBACK: Mutex<VecDeque<String>> = Mutex::new(VecDeque::new());

pub(super) struct Request<'a> {
    pub client: &'a Client,
    pub endpoint: &'a Url,
    pub headers: &'a HeaderMap,
    pub token: &'a str,
    pub account: &'a str,
    pub session: Option<&'a str>,
    pub timeout: Option<Duration>,
    pub cache_context: bool,
    pub reuse_connection: bool,
}

struct IdleConnection {
    key: String,
    connected: Instant,
    expires: Instant,
    expiry_task: tokio::task::AbortHandle,
    generation: uuid::Uuid,
    socket: Socket,
    previous: Option<PreviousResponse>,
}

struct PreviousResponse {
    request_options: Value,
    prefix: Vec<Value>,
    id: String,
}

impl PreviousResponse {
    fn from_completed(request: &Value, response: &Value) -> Option<Self> {
        let input = request["input"].as_array()?;
        let output = response["output"].as_array()?;
        let id = response["id"].as_str().filter(|id| {
            !id.is_empty() && id.len() <= 512 && id.bytes().all(|byte| byte.is_ascii_graphic())
        })?;
        if serde_json::to_vec(request).ok()?.len() + serde_json::to_vec(output).ok()?.len()
            > MAX_CACHED_CONTEXT
        {
            return None;
        }
        let prefix = input.iter().chain(output).cloned().collect();
        Some(Self {
            request_options: request_options(request)?,
            prefix,
            id: id.to_owned(),
        })
    }

    fn continuation(&self, request: &Value) -> Option<Value> {
        if self.request_options != request_options(request)? {
            return None;
        }
        let suffix = request["input"]
            .as_array()?
            .strip_prefix(self.prefix.as_slice())?;
        // A replay of the same conversation still sends a full request.
        if suffix.is_empty() {
            return None;
        }
        let mut result = request.clone();
        result["input"] = json!(suffix);
        result["previous_response_id"] = json!(self.id);
        Some(result)
    }
}

fn request_options(request: &Value) -> Option<Value> {
    Some(Value::Object(
        request
            .as_object()?
            .iter()
            .filter(|(key, _)| *key != "input")
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    ))
}

fn falls_back(key: &str) -> bool {
    FALLBACK
        .lock()
        .is_ok_and(|keys| keys.iter().any(|known| known == key))
}

fn remember_fallback(key: String) {
    let Ok(mut keys) = FALLBACK.lock() else {
        return;
    };
    if keys.iter().any(|known| known == &key) {
        return;
    }
    if keys.len() >= MAX_FALLBACK_SESSIONS {
        keys.pop_front();
    }
    keys.push_back(key);
}

fn take_connection(key: &str) -> Option<IdleConnection> {
    let mut pool = POOL.lock().ok()?;
    let now = Instant::now();
    pool.retain(|entry| {
        let fresh = entry.expires > now && now.duration_since(entry.connected) < MAX_SOCKET_AGE;
        if !fresh {
            entry.expiry_task.abort();
        }
        fresh
    });
    let index = pool.iter().position(|entry| entry.key == key)?;
    let entry = pool.remove(index)?;
    entry.expiry_task.abort();
    Some(entry)
}

fn return_connection(
    key: String,
    connected: Instant,
    socket: Socket,
    previous: Option<PreviousResponse>,
) {
    let Ok(mut pool) = POOL.lock() else { return };
    if let Some(index) = pool.iter().position(|entry| entry.key == key)
        && let Some(entry) = pool.remove(index)
    {
        entry.expiry_task.abort();
    }
    if pool.len() >= MAX_IDLE_CONNECTIONS
        && let Some(entry) = pool.pop_front()
    {
        entry.expiry_task.abort();
    }
    let generation = uuid::Uuid::new_v4();
    let expiry_task = tokio::spawn(async move {
        tokio::time::sleep(IDLE_TTL).await;
        if let Ok(mut pool) = POOL.lock() {
            pool.retain(|entry| entry.generation != generation);
        }
    })
    .abort_handle();
    pool.push_back(IdleConnection {
        key,
        connected,
        expires: Instant::now() + IDLE_TTL,
        expiry_task,
        generation,
        socket,
        previous,
    });
}

#[derive(Clone)]
struct Handshake {
    client: Client,
    endpoint: Url,
    headers: HeaderMap,
    account: String,
    token: String,
    session: String,
}

impl Handshake {
    async fn connect(&self) -> Result<Socket> {
        let nonce =
            base64::engine::general_purpose::STANDARD.encode(uuid::Uuid::new_v4().as_bytes());
        let mut headers = self.headers.clone();
        for name in [
            "authorization",
            "chatgpt-account-id",
            "openai-beta",
            "accept",
            "content-type",
            "content-length",
            "connection",
            "upgrade",
            "sec-websocket-key",
            "sec-websocket-version",
            "sec-websocket-protocol",
            "sec-websocket-extensions",
            "session-id",
            "x-client-request-id",
        ] {
            headers.remove(name);
        }
        headers
            .entry("originator")
            .or_insert(HeaderValue::from_static("pi"));
        let response = self
            .client
            .get(self.endpoint.clone())
            .version(Version::HTTP_11)
            .headers(headers)
            .bearer_auth(&self.token)
            .header("chatgpt-account-id", &self.account)
            .header("openai-beta", "responses_websockets=2026-02-06")
            .header("session-id", &self.session)
            .header("x-client-request-id", &self.session)
            .header(header::CONNECTION, "Upgrade")
            .header(header::UPGRADE, "websocket")
            .header("sec-websocket-version", "13")
            .header("sec-websocket-key", &nonce)
            .timeout(CONNECT_TIMEOUT)
            .send()
            .await
            .map_err(|_| unavailable())?;
        let headers = response.headers();
        let contains = |name: header::HeaderName, value: &str| {
            headers
                .get_all(name)
                .iter()
                .filter_map(|header| header.to_str().ok())
                .flat_map(|header| header.split(','))
                .any(|found| found.trim().eq_ignore_ascii_case(value))
        };
        if response.status() != StatusCode::SWITCHING_PROTOCOLS
            || !contains(header::CONNECTION, "upgrade")
            || !contains(header::UPGRADE, "websocket")
            || headers
                .get("sec-websocket-accept")
                .and_then(|value| value.to_str().ok())
                != Some(derive_accept_key(nonce.as_bytes()).as_str())
            || headers.contains_key("sec-websocket-protocol")
            || headers.contains_key("sec-websocket-extensions")
        {
            return Err(unavailable());
        }
        let socket = response.upgrade().await.map_err(|_| unavailable())?;
        Ok(WebSocketStream::from_raw_socket(
            socket,
            Role::Client,
            Some(
                WebSocketConfig::default()
                    .max_message_size(Some(super::MAX_STREAM))
                    .max_frame_size(Some(super::MAX_STREAM)),
            ),
        )
        .await)
    }
}

fn unavailable() -> Error {
    Error::RetryableModel("Codex WebSocket unavailable".into())
}

fn disconnected() -> Error {
    Error::RetryableModel("Codex WebSocket interrupted".into())
}

async fn send(socket: &mut Socket, mut body: Value, timeout: Option<Duration>) -> Result<()> {
    body["type"] = json!("response.create");
    let send = socket.send(Message::Text(body.to_string().into()));
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, send)
            .await
            .map_err(|_| disconnected())?,
        None => send.await,
    }
    .map_err(|_| disconnected())
}

async fn next(socket: &mut Socket, timeout: Option<Duration>) -> Result<Message> {
    let next = socket.next();
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, next)
            .await
            .map_err(|_| disconnected())?,
        None => next.await,
    }
    .ok_or_else(disconnected)?
    .map_err(|_| disconnected())
}

pub(super) async fn open(
    options: Request<'_>,
    body: &Value,
) -> Result<Option<super::ResponseEvents>> {
    let pool_key = options.session.and_then(|session| {
        let mut headers: Vec<_> = options
            .headers
            .iter()
            .filter(|(name, _)| *name != "x-client-request-id" && *name != "session-id")
            .map(|(name, value)| (name.as_str(), value.as_bytes()))
            .collect();
        headers.sort_unstable();
        pi_rs_core::protected_model_fingerprint(&(
            "codex-websocket-v1",
            tokio::runtime::Handle::current().id().to_string(),
            options.endpoint.as_str(),
            options.account,
            options.token,
            session,
            headers,
        ))
    });
    let handshake = Handshake {
        client: options.client.clone(),
        endpoint: options.endpoint.clone(),
        headers: options.headers.clone(),
        account: options.account.into(),
        token: options.token.into(),
        session: options
            .session
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
    };
    if pool_key.as_deref().is_some_and(falls_back) {
        return Ok(None);
    }
    let cached = pool_key
        .as_deref()
        .filter(|_| options.reuse_connection)
        .and_then(take_connection);
    let (mut socket, mut connected, previous) = match cached {
        Some(entry) => (entry.socket, entry.connected, entry.previous),
        None => match handshake.connect().await {
            Ok(socket) => (socket, Instant::now(), None),
            // No inference request was sent. The caller can try SSE safely.
            Err(_) => {
                if let Some(key) = pool_key {
                    remember_fallback(key);
                }
                return Ok(None);
            }
        },
    };
    let continuation = options
        .cache_context
        .then_some(previous)
        .flatten()
        .and_then(|previous| previous.continuation(body));
    let mut sent_continuation = continuation.is_some();
    let body = body.clone();
    let timeout = options.timeout;
    let cache_context = options.cache_context;
    let reuse_connection = options.reuse_connection;
    send(
        &mut socket,
        continuation.unwrap_or_else(|| body.clone()),
        timeout,
    )
    .await?;

    Ok(Some(Box::pin(async_stream::try_stream! {
        let mut seen = 0usize;
        let mut has_output = false;
        loop {
            let event = match next(&mut socket, timeout).await? {
                Message::Text(text) => text,
                Message::Ping(_) | Message::Pong(_) => continue,
                Message::Close(_) => Err(disconnected())?,
                _ => Err(Error::Model("unexpected Codex WebSocket message".into()))?,
            };
            seen = seen.saturating_add(event.len());
            if seen > super::MAX_STREAM {
                Err(Error::Model("OpenAI stream exceeds 32 MiB".into()))?;
            }
            let event: Value = serde_json::from_str(event.as_str())
                .map_err(|_| Error::Model("invalid Codex WebSocket event".into()))?;
            let kind = event["type"].as_str().unwrap_or_default();
            let failure_code = event["error"]["code"].as_str().or_else(|| event["code"].as_str())
                .or_else(|| event["response"]["error"]["code"].as_str());
            if sent_continuation && !has_output
                && matches!(kind, "error" | "response.failed")
                && failure_code == Some("previous_response_not_found")
            {
                socket = handshake.connect().await?;
                connected = Instant::now();
                send(&mut socket, body.clone(), timeout).await?;
                sent_continuation = false;
                continue;
            }
            has_output |= super::recognized_response_event(kind);
            if matches!(kind, "response.done" | "response.completed" | "response.incomplete") {
                let response = &event["response"];
                if response["status"] == "completed" && reuse_connection
                    && let Some(key) = pool_key
                {
                    let previous = cache_context.then(|| PreviousResponse::from_completed(&body, response)).flatten();
                    return_connection(key, connected, socket, previous);
                }
                yield event;
                return;
            }
            yield event;
        }
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CodexTransport, OpenAiResponses};
    use pi_rs_core::{Message as CoreMessage, Model, ModelEvent, ModelRequest};

    fn client(endpoint: String, transport: CodexTransport) -> OpenAiResponses {
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"https://api.openai.com/auth":{"chatgpt_account_id":"fixture-account"}}"#);
        OpenAiResponses::new_codex(&endpoint, format!("e30.{payload}.fixture"), "openai-codex")
            .unwrap()
            .with_codex_transport(transport)
    }

    fn prompt(text: &str) -> CoreMessage {
        CoreMessage::User {
            content: text.into(),
        }
    }

    fn request(messages: Vec<CoreMessage>) -> ModelRequest {
        ModelRequest {
            model: "gpt-6.1-sol".into(),
            messages,
            tools: vec![],
            max_output_tokens: 64,
        }
    }

    #[tokio::test]
    async fn codex_websocket_reuses_transport_and_sends_the_compatible_input_suffix() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_hdr_async(
                tcp,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    assert_eq!(request.uri().path(), "/backend-api/codex/responses");
                    assert_eq!(
                        request.headers()["openai-beta"],
                        "responses_websockets=2026-02-06"
                    );
                    assert_eq!(request.headers()["chatgpt-account-id"], "fixture-account");
                    assert!(request.headers().contains_key("session-id"));
                    Ok(response)
                },
            )
            .await
            .unwrap();
            for turn in 0..2 {
                let text = socket.next().await.unwrap().unwrap().into_text().unwrap();
                let body: Value = serde_json::from_str(text.as_str()).unwrap();
                assert_eq!(body["type"], "response.create");
                assert_eq!(body["store"], false);
                assert_eq!(body["input"].as_array().unwrap().len(), 1);
                if turn == 0 {
                    assert!(body.get("previous_response_id").is_none());
                    assert_eq!(body["input"][0]["content"], "first");
                } else {
                    assert_eq!(body["previous_response_id"], "fixture-0");
                    assert_eq!(body["input"][0]["content"], "second");
                }
                let completion = json!({
                    "type":"response.completed","response":{
                        "id":format!("fixture-{turn}"),"status":"completed",
                        "output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}]
                    }
                });
                socket
                    .send(Message::Text(completion.to_string().into()))
                    .await
                    .unwrap();
            }
        });
        let endpoint = format!("http://{address}/backend-api/codex/responses");
        let model = client(endpoint, CodexTransport::WebSocketCached)
            .with_session_cache_key(&uuid::Uuid::new_v4().to_string());
        let first = model
            .stream(request(vec![prompt("first")]))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let replay = first
            .into_iter()
            .find_map(|event| match event.unwrap() {
                ModelEvent::Metadata(metadata) => metadata.replay,
                _ => None,
            })
            .unwrap();
        let second = model
            .stream(request(vec![
                prompt("first"),
                CoreMessage::Assistant {
                    content: "done".into(),
                    calls: vec![],
                    replay: Some(replay),
                },
                prompt("second"),
            ]))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(
            second.last(),
            Some(Ok(ModelEvent::Complete { .. }))
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn codex_websocket_upgrade_failure_falls_back_to_one_sse_request() {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for method in ["GET", "POST", "POST"] {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut socket = BufReader::new(tcp);
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                assert_eq!(
                    line.trim_end(),
                    format!("{method} /backend-api/codex/responses HTTP/1.1")
                );
                let mut length = 0;
                let mut header_lines = String::new();
                loop {
                    line.clear();
                    socket.read_line(&mut line).await.unwrap();
                    if line.trim().is_empty() {
                        break;
                    }
                    let lowercase = line.to_ascii_lowercase();
                    if let Some(value) = lowercase.strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap();
                    }
                    header_lines.push_str(&lowercase);
                }
                if method == "GET" {
                    assert_eq!(length, 0);
                    assert!(header_lines.contains("upgrade: websocket"));
                    socket.get_mut().write_all(b"HTTP/1.1 426 Upgrade Required\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                } else {
                    let mut body = vec![0; length];
                    socket.read_exact(&mut body).await.unwrap();
                    assert!(header_lines.contains("openai-beta: responses=experimental"));
                    let completion = "data: {\"type\":\"response.done\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{completion}",
                        completion.len()
                    );
                    socket
                        .get_mut()
                        .write_all(response.as_bytes())
                        .await
                        .unwrap();
                }
            }
        });
        let model = client(
            format!("http://{address}/backend-api/codex/responses"),
            CodexTransport::Auto,
        )
        .with_session_cache_key(&uuid::Uuid::new_v4().to_string());
        let events = model
            .stream(request(vec![prompt("first")]))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(
            events.last(),
            Some(Ok(ModelEvent::Complete { .. }))
        ));
        let second = model
            .stream(request(vec![prompt("second")]))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(
            second.last(),
            Some(Ok(ModelEvent::Complete { .. }))
        ));
        server.await.unwrap();
    }
}
