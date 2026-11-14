use pi_rs_core::{CancellationToken, Error, Message, ModelRequest};
use pi_rs_durable::model::{ProviderAdapter, ProviderAttempt, collect_generation};
use pi_rs_durable_openai::OpenAiResponsesAdapter;
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
    server_with_options(response, delay, false, false)
}
fn server_with_options(
    response: &'static str,
    delay: bool,
    fragment: bool,
    pause_after_headers: bool,
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
        let headers = String::from_utf8_lossy(&b[..end]).into_owned();
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
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-secret\r\n")
        );
        tx.send(serde_json::from_slice(&b[end..end + len]).unwrap())
            .unwrap();
        if delay {
            std::thread::sleep(Duration::from_millis(300));
        }
        if pause_after_headers {
            let end = response.find("\r\n\r\n").unwrap() + 4;
            let _ = socket.write_all(&response.as_bytes()[..end]);
            std::thread::sleep(Duration::from_millis(300));
            let _ = socket.write_all(&response.as_bytes()[end..]);
        } else if fragment {
            for byte in response.as_bytes() {
                if socket.write_all(&[*byte]).is_err() {
                    break;
                }
                std::thread::sleep(Duration::from_micros(100));
            }
        } else {
            let _ = socket.write_all(response.as_bytes());
        }
    });
    (format!("http://{addr}"), rx, thread)
}

fn http(body: &str) -> &'static str {
    Box::leak(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).into_boxed_str())
}

#[tokio::test]
async fn exact_body_fragmented_events_and_replay() {
    let events = concat!(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"猫\"}\r\n\r\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"call_id\":\"c\",\"name\":\"read\"}}\r\n\r\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"{}\"}\r\n\r\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\"},{\"type\":\"function_call\",\"call_id\":\"c\",\"name\":\"read\",\"arguments\":\"{}\"}],\"usage\":{\"input_tokens\":3,\"output_tokens\":4}}}\r\n\r\n"
    );
    let (url, rx, thread) = server_with_options(http(events), false, true, false);
    let adapter = OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap();
    let a = attempt();
    let r = request();
    let mut body = adapter.prepare(&a, &r).await.unwrap();
    body["input"][0]["content"] = json!("rewritten");
    body["temperature"] = json!(0.3);
    let result = collect_generation(
        adapter.dispatch(&a, &r, body.clone()),
        &CancellationToken::new(),
        |_| {},
    )
    .await
    .unwrap();
    assert_eq!(rx.recv().unwrap(), body);
    assert_eq!(result.content, "猫");
    assert_eq!(result.calls[0].arguments, "{}");
    assert_eq!(result.usage.unwrap().output_tokens, 4);
    assert!(result.metadata.replay.is_some());
    thread.join().unwrap();
}
#[tokio::test]
async fn rejects_unsafe_rewrites_and_bad_endpoints() {
    let adapter =
        OpenAiResponsesAdapter::new("http://127.0.0.1:1234/responses", "fixture-secret".into())
            .unwrap();
    let a = attempt();
    let r = request();
    for (key, value) in [
        ("model", json!("other")),
        ("stream", json!(false)),
        ("store", json!(true)),
        ("max_output_tokens", json!(101)),
        ("tools", json!([{"type":"web_search"}])),
        ("background", json!(true)),
        ("max_tool_calls", json!(999)),
    ] {
        let mut b = adapter.prepare(&a, &r).await.unwrap();
        b[key] = value;
        assert!(adapter.dispatch(&a, &r, b).await.is_err());
    }
    for url in [
        "http://example.com",
        "http://localhost:1234",
        "https://user@example.com",
        "https://example.com/?key=x",
    ] {
        assert!(OpenAiResponsesAdapter::new(url, "fixture-secret".into()).is_err());
    }
}
#[tokio::test]
async fn malformed_incomplete_and_mismatched_streams_fail() {
    for content in [
        "data: nope\n\n",
        "data: {\"type\":\"response.created\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"function_call\"}]}}\n\n",
    ] {
        let (url, rx, thread) = server(http(content), false);
        let ad = OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap();
        let a = attempt();
        let r = request();
        let body = ad.prepare(&a, &r).await.unwrap();
        assert!(
            collect_generation(ad.dispatch(&a, &r, body), &CancellationToken::new(), |_| {})
                .await
                .is_err()
        );
        rx.recv().unwrap();
        thread.join().unwrap();
    }
}
#[tokio::test]
async fn cancellation_while_opening_and_reading() {
    for after_headers in [false, true] {
        let (url, rx, thread) =
            server_with_options(http("data: {}\n\n"), !after_headers, false, after_headers);
        let ad = OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap();
        let a = attempt();
        let r = request();
        let body = ad.prepare(&a, &r).await.unwrap();
        let cancel = CancellationToken::new();
        let c = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            c.cancel();
        });
        assert!(matches!(
            collect_generation(ad.dispatch(&a, &r, body), &cancel, |_| {}).await,
            Err(Error::Cancelled)
        ));
        rx.recv().unwrap();
        thread.join().unwrap();
    }
}
#[tokio::test]
async fn redirect_is_not_followed() {
    let target = TcpListener::bind("127.0.0.1:0").unwrap();
    target.set_nonblocking(true).unwrap();
    let resp=Box::leak(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{}/stolen\r\nContent-Length: 0\r\n\r\n",target.local_addr().unwrap()).into_boxed_str());
    let (url, rx, thread) = server(resp, false);
    let ad = OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap();
    let a = attempt();
    let r = request();
    let body = ad.prepare(&a, &r).await.unwrap();
    assert!(ad.dispatch(&a, &r, body).await.is_err());
    rx.recv().unwrap();
    thread.join().unwrap();
    assert!(target.accept().is_err());
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
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n",
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
    h.install_provider_adapter(Arc::new(
        OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap(),
    ))
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

#[tokio::test]
async fn oversized_unterminated_event_is_bounded() {
    let data = format!("data: {}", "a".repeat(4 * 1024 * 1024 + 1));
    let (url, rx, thread) = server(http(&data), false);
    let adapter = OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap();
    let a = attempt();
    let r = request();
    let body = adapter.prepare(&a, &r).await.unwrap();
    let err = collect_generation(
        adapter.dispatch(&a, &r, body),
        &CancellationToken::new(),
        |_| {},
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("exceeds 4 MiB"));
    rx.recv().unwrap();
    thread.join().unwrap();
}

#[tokio::test]
async fn output_limit_marks_partial_tool_calls_as_truncated() {
    let events = concat!(
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"call_id\":\"c\",\"name\":\"read\"}}\n\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"{\"}\n\n",
        "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"output\":[{\"type\":\"function_call\",\"call_id\":\"c\",\"name\":\"read\",\"arguments\":\"{\"}]}}\n\n"
    );
    let (url, rx, thread) = server(http(events), false);
    let adapter = OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap();
    let a = attempt();
    let r = request();
    let body = adapter.prepare(&a, &r).await.unwrap();
    let result = collect_generation(
        adapter.dispatch(&a, &r, body),
        &CancellationToken::new(),
        |_| {},
    )
    .await
    .unwrap();
    assert!(result.is_truncated());
    assert!(result.metadata.replay.is_none());
    assert_eq!(result.calls[0].arguments, "{");
    rx.recv().unwrap();
    thread.join().unwrap();
}

#[tokio::test]
async fn completed_tool_items_must_be_complete_and_at_the_streamed_index() {
    for (index, status) in [(0, "incomplete"), (1, "completed")] {
        let events = format!(
            "data: {{\"type\":\"response.output_item.added\",\"output_index\":{index},\"item\":{{\"type\":\"function_call\",\"call_id\":\"c\",\"name\":\"read\"}}}}\n\ndata: {{\"type\":\"response.function_call_arguments.delta\",\"output_index\":{index},\"delta\":\"{{}}\"}}\n\ndata: {{\"type\":\"response.completed\",\"response\":{{\"status\":\"completed\",\"output\":[{{\"type\":\"function_call\",\"status\":\"{status}\",\"call_id\":\"c\",\"name\":\"read\",\"arguments\":\"{{}}\"}}]}}}}\n\n"
        );
        let (url, rx, thread) = server(http(&events), false);
        let adapter = OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap();
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
async fn server_controlled_incomplete_reason_is_not_exposed() {
    let events = "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"fixture-secret\"},\"output\":[]}}\n\n";
    let (url, rx, thread) = server(http(events), false);
    let adapter = OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap();
    let a = attempt();
    let r = request();
    let body = adapter.prepare(&a, &r).await.unwrap();
    let error = collect_generation(
        adapter.dispatch(&a, &r, body),
        &CancellationToken::new(),
        |_| {},
    )
    .await
    .unwrap_err();
    assert!(!error.to_string().contains("fixture-secret"));
    rx.recv().unwrap();
    thread.join().unwrap();
}

#[tokio::test]
async fn completed_tool_call_with_empty_arguments_is_rejected() {
    let events = concat!(
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"call_id\":\"c\",\"name\":\"read\"}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"function_call\",\"call_id\":\"c\",\"name\":\"read\",\"arguments\":\"\"}]}}\n\n"
    );
    let (url, rx, thread) = server(http(events), false);
    let adapter = OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap();
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
