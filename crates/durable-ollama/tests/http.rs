use pi_rs_core::{CancellationToken, Error, Message, ModelRequest};
use pi_rs_durable::model::{ProviderAdapter, ProviderAttempt, collect_generation};
use pi_rs_durable_ollama::OllamaAdapter;
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
    time::Duration,
};
use uuid::Uuid;

fn attempt() -> ProviderAttempt {
    ProviderAttempt {
        session_id: Uuid::new_v4(),
        request_id: Uuid::new_v4(),
        turn: 0,
        retries: 0,
        restarts: 0,
    }
}
fn request() -> ModelRequest {
    ModelRequest {
        model: "test".into(),
        messages: vec![Message::User {
            content: "hello".into(),
        }],
        tools: vec![],
        max_output_tokens: 100,
    }
}
fn server(
    response: &'static str,
    delay: bool,
) -> (String, mpsc::Receiver<Value>, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut b = vec![];
        let end;
        loop {
            let mut part = [0; 4096];
            let n = socket.read(&mut part).unwrap();
            assert!(n > 0);
            b.extend_from_slice(&part[..n]);
            if let Some(i) = b.windows(4).position(|w| w == b"\r\n\r\n") {
                end = i + 4;
                break;
            }
        }
        let headers = String::from_utf8_lossy(&b[..end]);
        let len: usize = headers
            .lines()
            .find_map(|l| {
                l.to_ascii_lowercase()
                    .strip_prefix("content-length: ")
                    .map(|s| s.parse().unwrap())
            })
            .unwrap();
        while b.len() - end < len {
            let mut part = [0; 4096];
            let n = socket.read(&mut part).unwrap();
            assert!(n > 0);
            b.extend_from_slice(&part[..n]);
        }
        tx.send(serde_json::from_slice(&b[end..end + len]).unwrap())
            .unwrap();
        if delay {
            std::thread::sleep(Duration::from_millis(300));
        }
        let _ = socket.write_all(response.as_bytes());
    });
    (format!("http://{addr}"), rx, thread)
}
fn http(body: &str) -> &'static str {
    Box::leak(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).into_boxed_str())
}
#[tokio::test]
async fn sends_replaced_body_and_collects_text_tools_usage() {
    let response = http(
        "data: {\"id\":\"id1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\",\"tool_calls\":[{\"index\":0,\"id\":\"call1\",\"function\":{\"name\":\"read\",\"arguments\":\"{\\\"p\\\":\"}}]},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"1}\"}}]},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":4}}\n\ndata: [DONE]\n\n",
    );
    let (url, rx, thread) = server(response, false);
    let adapter = OllamaAdapter::new(&url).unwrap();
    let a = attempt();
    let r = request();
    let mut body = adapter.prepare(&a, &r).await.unwrap();
    body["messages"][0]["content"] = json!("hook replacement");
    body["temperature"] = json!(0.3);
    adapter.validate(&a, &r, &body).unwrap();
    let result = collect_generation(
        adapter.dispatch(&a, &r, body.clone()),
        &CancellationToken::new(),
        |_| {},
    )
    .await
    .unwrap();
    assert_eq!(rx.recv().unwrap(), body);
    assert_eq!(result.content, "hi");
    assert_eq!(result.calls[0].arguments, "{\"p\":1}");
    assert_eq!(result.calls[0].name, "read");
    assert_eq!(result.usage.unwrap().output_tokens, 4);
    thread.join().unwrap();
}
#[tokio::test]
async fn rejected_rewrites_and_bad_endpoints() {
    let a = attempt();
    let r = request();
    let adapter = OllamaAdapter::new("http://127.0.0.1:1234").unwrap();
    let mut body = adapter.prepare(&a, &r).await.unwrap();
    body["model"] = json!("other");
    assert!(adapter.dispatch(&a, &r, body).await.is_err());
    for url in [
        "http://example.com",
        "http://localhost:11434",
        "http://127.0.0.1:11434/path",
        "https://127.0.0.1",
        "http://user@127.0.0.1",
    ] {
        assert!(OllamaAdapter::new(url).is_err());
    }
}
#[tokio::test]
async fn malformed_and_incomplete_streams_fail() {
    for content in ["data: nope\n\n", "data: {\"choices\":[]}\n\n"] {
        let (url, rx, thread) = server(http(content), false);
        let adapter = OllamaAdapter::new(&url).unwrap();
        let a = attempt();
        let r = request();
        let body = adapter.prepare(&a, &r).await.unwrap();
        assert!(
            collect_generation(
                adapter.dispatch(&a, &r, body),
                &CancellationToken::new(),
                |_| {}
            )
            .await
            .is_err()
        );
        rx.recv().unwrap();
        thread.join().unwrap();
    }
}
#[tokio::test]
async fn cancellation_drops_pending_dispatch() {
    let (url, rx, thread) = server(http(""), true);
    let adapter = OllamaAdapter::new(&url).unwrap();
    let a = attempt();
    let r = request();
    let body = adapter.prepare(&a, &r).await.unwrap();
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        c.cancel();
    });
    assert!(matches!(
        collect_generation(adapter.dispatch(&a, &r, body), &cancel, |_| {}).await,
        Err(Error::Cancelled)
    ));
    rx.recv().unwrap();
    thread.join().unwrap();
}

struct UnusedModel;
#[async_trait::async_trait]
impl pi_rs_core::Model for UnusedModel {
    async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<pi_rs_core::ModelStream> {
        panic!("regular model must not be called")
    }
}
struct Rewrite;
#[async_trait::async_trait]
impl pi_rs_durable::extension::Hook for Rewrite {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        body: &Value,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision, GenerationEvent};
        if event != GenerationEvent::BeforeProviderRequest {
            return Ok(GenerationDecision::Pass);
        }
        let mut payload = body["payload"].clone();
        payload["temperature"] = json!(0.25);
        Ok(GenerationDecision::Replace(json!({"payload":payload})))
    }
}
#[tokio::test]
async fn durable_hook_reaches_http_server() {
    use pi_rs_durable::{
        extension::Extension,
        recovery::UncertainPolicy,
        runtime::{Config, Harness, Options, Status},
    };
    use std::sync::Arc;
    let (url, rx, thread) = server(
        http(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
        ),
        false,
    );
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(
        dir.path().join("db"),
        Options {
            model: Arc::new(UnusedModel),
            tools: vec![],
            restart_limit: 3,
            max_turns: 3,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        },
    )
    .unwrap();
    h.install_provider_adapter(Arc::new(OllamaAdapter::new(&url).unwrap()))
        .unwrap();
    h.install_extension(Extension {
        name: "rewrite".into(),
        hooks: vec![Arc::new(Rewrite)],
        ..Default::default()
    })
    .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "system".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.select_extensions(id, vec!["rewrite".into()]).unwrap();
    h.submit_and_run(id, None, "hello".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(rx.recv().unwrap()["temperature"], json!(0.25));
    assert!(matches!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
    thread.join().unwrap();
}
