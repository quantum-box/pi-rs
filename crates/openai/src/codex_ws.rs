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
const IDLE_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const IDLE_TTL: Duration = Duration::from_secs(5 * 60);
// Leave room for a long response before the server's one-hour limit.
const MAX_SOCKET_AGE: Duration = Duration::from_secs(45 * 60);
const MAX_IDLE_CONNECTIONS: usize = 8;
const MAX_FALLBACK_SESSIONS: usize = 64;
const FALLBACK_COOLDOWN: Duration = Duration::from_secs(60);
const MAX_CACHED_CONTEXT: usize = 2 * 1024 * 1024;
type Socket = WebSocketStream<reqwest::Upgraded>;

static POOL: Mutex<VecDeque<IdleConnection>> = Mutex::new(VecDeque::new());
static FALLBACK: Mutex<VecDeque<(String, Instant)>> = Mutex::new(VecDeque::new());

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
    let Ok(mut keys) = FALLBACK.lock() else {
        return false;
    };
    let now = Instant::now();
    keys.retain(|(_, expires)| *expires > now);
    keys.iter().any(|(known, _)| known == key)
}

fn remember_fallback(key: String) {
    let Ok(mut keys) = FALLBACK.lock() else {
        return;
    };
    keys.retain(|(known, _)| known != &key);
    if keys.len() >= MAX_FALLBACK_SESSIONS {
        keys.pop_front();
    }
    keys.push_back((key, Instant::now() + FALLBACK_COOLDOWN));
}

fn take_connection(key: &str) -> Option<IdleConnection> {
    let mut pool = POOL.lock().ok()?;
    let now = Instant::now();
    pool.retain(|entry| {
        let fresh = entry.expires > now;
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

fn idle_expiry(connected: Instant, now: Instant) -> Option<Instant> {
    let deadline = connected + MAX_SOCKET_AGE;
    (now < deadline).then(|| deadline.min(now + IDLE_TTL))
}

fn return_connection(
    key: String,
    connected: Instant,
    socket: Socket,
    previous: Option<PreviousResponse>,
) {
    let now = Instant::now();
    let Some(expires) = idle_expiry(connected, now) else {
        return;
    };
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
    let idle_for = expires.saturating_duration_since(Instant::now());
    let expiry_task = tokio::spawn(async move {
        tokio::time::sleep(idle_for).await;
        if let Ok(mut pool) = POOL.lock() {
            pool.retain(|entry| entry.generation != generation);
        }
    })
    .abort_handle();
    pool.push_back(IdleConnection {
        key,
        connected,
        expires,
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
    let fields = body
        .as_object_mut()
        .expect("Responses payload is an object");
    fields.remove("stream");
    fields.insert("type".into(), json!("response.create"));
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

async fn probe_idle_socket(socket: &mut Socket, timeout: Option<Duration>) -> bool {
    let nonce = uuid::Uuid::new_v4().as_bytes().to_vec();
    let timeout = timeout.map_or(IDLE_PROBE_TIMEOUT, |timeout| {
        timeout.min(IDLE_PROBE_TIMEOUT)
    });
    tokio::time::timeout(timeout, async {
        if socket
            .send(Message::Ping(nonce.clone().into()))
            .await
            .is_err()
        {
            return false;
        }
        while let Some(Ok(message)) = socket.next().await {
            match message {
                Message::Pong(payload) if payload.as_ref() == nonce => return true,
                Message::Ping(_) | Message::Pong(_) => {}
                _ => return false,
            }
        }
        false
    })
    .await
    .unwrap_or(false)
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
    let cached = if let Some(mut entry) = cached {
        // Confirm the idle connection before sending billable inference.
        probe_idle_socket(&mut entry.socket, options.timeout)
            .await
            .then_some(entry)
    } else {
        None
    };
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
            let refused = matches!(kind, "error" | "response.failed")
                || (kind == "response.done" && event["response"]["status"] == "failed");
            if sent_continuation && !has_output && refused
                && failure_code == Some("previous_response_not_found")
            {
                socket = handshake.connect().await?;
                connected = Instant::now();
                send(&mut socket, body.clone(), timeout).await?;
                sent_continuation = false;
                continue;
            }
            has_output |= super::recognized_response_event(kind)
                && !matches!(kind, "response.created" | "response.queued" | "response.in_progress");
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

    async fn read_request(socket: &mut WebSocketStream<tokio::net::TcpStream>) -> Value {
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Text(text) => return serde_json::from_str(text.as_str()).unwrap(),
                Message::Ping(_) | Message::Pong(_) => continue,
                message => panic!("unexpected request frame: {message:?}"),
            }
        }
    }

    #[test]
    fn idle_reuse_ends_after_five_idle_minutes_or_forty_five_total_minutes() {
        let connected = Instant::now();
        let idle_since = connected + Duration::from_secs(5 * 60);
        let near_age_limit = connected + Duration::from_secs(44 * 60 + 59);
        let age_limit = connected + Duration::from_secs(45 * 60);
        assert_eq!(
            idle_expiry(connected, idle_since),
            Some(connected + Duration::from_secs(10 * 60))
        );
        assert_eq!(idle_expiry(connected, near_age_limit), Some(age_limit));
        assert_eq!(idle_expiry(connected, age_limit), None);
        assert_eq!(
            idle_expiry(connected, age_limit + Duration::from_secs(1)),
            None
        );
    }

    #[tokio::test]
    async fn codex_websocket_transmits_grammar_and_decodes_raw_input() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let body = read_request(&mut socket).await;
            assert_eq!(body["tools"][0]["type"], "custom");
            assert_eq!(body["tools"][0]["format"]["syntax"], "regex");
            let item =
                json!({"type":"custom_tool_call","call_id":"c1","name":"read","input":"hello"});
            for event in [
                json!({"type":"response.output_item.added","output_index":0,"item":{"type":"custom_tool_call","call_id":"c1","name":"read","input":""}}),
                json!({"type":"response.custom_tool_call_input.delta","output_index":0,"delta":"hello"}),
                json!({"type":"response.done","response":{"status":"completed","output":[item]}}),
            ] {
                socket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .unwrap();
            }
        });
        let client = client(
            format!("http://{address}/backend-api/codex/responses"),
            CodexTransport::WebSocket,
        )
        .with_grammar_tool_support(true);
        let mut request = request(vec![prompt("read a file")]);
        let tool = pi_rs_core::ToolSpec {
            name: "read".into(),
            description: "read".into(),
            parameters: json!({"type":"object","required":["path"],"properties":{"path":{"type":"string"}}}),
        };
        request.tools.push(tool.clone());
        let declaration = pi_rs_core::ToolDeclaration {
            spec: tool,
            constrained_sampling: Some(pi_rs_core::ToolConstraint::Grammar {
                variants: pi_rs_core::ToolGrammarVariants {
                    openai_regex: Some(".+".into()),
                    ..Default::default()
                },
            }),
        };
        let mut stream = client
            .stream_with_tool_declarations(request, &[declaration], false)
            .await
            .unwrap();
        let mut arguments = None;
        while let Some(event) = stream.next().await {
            if let ModelEvent::Complete { calls, .. } = event.unwrap() {
                arguments = Some(calls[0].arguments.clone());
            }
        }
        assert_eq!(
            serde_json::from_str::<Value>(&arguments.unwrap()).unwrap(),
            json!({"path":"hello"})
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn codex_websocket_reuses_transport_and_sends_the_compatible_input_suffix() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            // Tungstenite owns the handshake callback's error type.
            #[allow(clippy::result_large_err)]
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
                let body = read_request(&mut socket).await;
                assert_eq!(body["type"], "response.create");
                assert!(body.get("stream").is_none());
                assert_eq!(body["store"], false);
                assert_eq!(body["input"].as_array().unwrap().len(), 1);
                if turn == 0 {
                    assert!(body.get("previous_response_id").is_none());
                    assert_eq!(body["input"][0]["content"], "first");
                } else {
                    assert_eq!(body["previous_response_id"], "fixture-0");
                    assert_eq!(body["input"][0]["content"], "second");
                    for event in [
                        json!({"type":"response.created","response":{"id":"expired"}}),
                        json!({"type":"response.done","response":{"status":"failed","error":{"code":"previous_response_not_found"}}}),
                    ] {
                        socket
                            .send(Message::Text(event.to_string().into()))
                            .await
                            .unwrap();
                    }
                    break;
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
            drop(socket);
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let body = read_request(&mut socket).await;
            assert_eq!(body["input"].as_array().unwrap().len(), 3);
            assert!(body.get("previous_response_id").is_none());
            let completion = json!({"type":"response.completed","response":{
                "id":"replacement","status":"completed","output":[]
            }});
            socket
                .send(Message::Text(completion.to_string().into()))
                .await
                .unwrap();
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
    async fn codex_websocket_reconnects_before_sending_on_a_closed_idle_socket() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (closed, idle_closed) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut closed = Some(closed);
            for turn in 0..2 {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
                let body = read_request(&mut socket).await;
                assert!(body.get("previous_response_id").is_none());
                assert_eq!(body["input"].as_array().unwrap().len(), turn + 1);
                let completion = json!({"type":"response.completed","response":{
                    "id":"fixture", "status":"completed", "output":[]
                }});
                socket
                    .send(Message::Text(completion.to_string().into()))
                    .await
                    .unwrap();
                if let Some(closed) = closed.take() {
                    socket.close(None).await.unwrap();
                    closed.send(()).unwrap();
                }
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
        assert!(matches!(
            first.last(),
            Some(Ok(ModelEvent::Complete { .. }))
        ));
        idle_closed.await.unwrap();
        let second = tokio::time::timeout(Duration::from_secs(5), async {
            model
                .stream(request(vec![prompt("first"), prompt("second")]))
                .await
                .unwrap()
                .collect::<Vec<_>>()
                .await
        })
        .await
        .unwrap();
        assert!(matches!(
            second.last(),
            Some(Ok(ModelEvent::Complete { .. }))
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn codex_websocket_opens_a_new_connection_when_cached_context_expires() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let response_id = uuid::Uuid::new_v4().to_string();
        let first_response_id = response_id.clone();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut original = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let first = read_request(&mut original).await;
            assert_eq!(first["input"].as_array().unwrap().len(), 1);
            let completion = |id: &str| {
                Message::Text(
                    json!({
                        "type":"response.completed",
                        "response":{"id":id,"status":"completed","output":[]}
                    })
                    .to_string()
                    .into(),
                )
            };
            original.send(completion(&first_response_id)).await.unwrap();

            // Keep the first socket open until the client explicitly retires it.
            let (tcp, _) = tokio::select! {
                incoming = original.next() => {
                    assert!(!matches!(incoming, Some(Ok(Message::Ping(_) | Message::Text(_)))));
                    listener.accept().await.unwrap()
                }
                replacement = listener.accept() => replacement.unwrap(),
            };
            let mut replacement = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let body = read_request(&mut replacement).await;
            assert!(body.get("previous_response_id").is_none());
            assert_eq!(body["input"].as_array().unwrap().len(), 2);
            replacement.send(completion("replacement")).await.unwrap();
        });
        let model = client(
            format!("http://{address}/backend-api/codex/responses"),
            CodexTransport::WebSocketCached,
        )
        .with_session_cache_key(&uuid::Uuid::new_v4().to_string());
        let first = model
            .stream(request(vec![prompt("first")]))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(
            first.last(),
            Some(Ok(ModelEvent::Complete { .. }))
        ));
        {
            let mut pool = POOL.lock().unwrap();
            let entry = pool
                .iter_mut()
                .find(|entry| {
                    entry
                        .previous
                        .as_ref()
                        .is_some_and(|previous| previous.id == response_id)
                })
                .expect("first session is cached");
            entry.expires = Instant::now();
        }
        let second = tokio::time::timeout(Duration::from_secs(5), async {
            model
                .stream(request(vec![prompt("first"), prompt("second")]))
                .await
                .unwrap()
                .collect::<Vec<_>>()
                .await
        })
        .await
        .unwrap();
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
