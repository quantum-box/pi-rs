use pi_rs_core::{CancellationToken, Error, Message, ModelRequest};
use pi_rs_durable::model::{ProviderAdapter, ProviderAttempt, collect_generation};
use pi_rs_durable_anthropic::AnthropicAdapter;
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
                .contains("x-api-key: fixture-secret\r\n")
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
async fn post_hook_body_text_and_tool_stream() {
    let events = concat!(
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-1\",\"usage\":{\"input_tokens\":3}}}\n\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"猫\"}}\n\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"c\",\"name\":\"read\",\"input\":{}}}\n\n",
        "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"x\\\":1}\"}}\n\n",
        "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":4}}\n\n",
        "data: {\"type\":\"message_stop\"}\n\n"
    );
    let (url, rx, thread) = server_with_options(http(events), false, true, false);
    let ad = AnthropicAdapter::new(&url, "fixture-secret".into()).unwrap();
    let a = attempt();
    let r = request();
    let mut body = ad.prepare(&a, &r).await.unwrap();
    body["messages"][0]["content"][0]["text"] = json!("rewritten");
    body["temperature"] = json!(0.3);
    let result = collect_generation(
        ad.dispatch(&a, &r, body.clone()),
        &CancellationToken::new(),
        |_| {},
    )
    .await
    .unwrap();
    assert_eq!(rx.recv().unwrap(), body);
    assert_eq!(result.content, "猫");
    assert_eq!(result.calls[0].arguments, "{\"x\":1}");
    assert_eq!(result.usage.unwrap().output_tokens, 4);
    thread.join().unwrap();
}
#[tokio::test]
async fn invalid_requests_and_endpoints_are_rejected() {
    let ad = AnthropicAdapter::new("http://127.0.0.1:1", "fixture-secret".into()).unwrap();
    let a = attempt();
    let r = request();
    for (key, value) in [
        ("model", json!("other")),
        ("stream", json!(false)),
        ("max_tokens", json!(101)),
        ("tools", json!([])),
        ("metadata", json!({})),
    ] {
        let mut b = ad.prepare(&a, &r).await.unwrap();
        b[key] = value;
        assert!(ad.dispatch(&a, &r, b).await.is_err());
    }
    for url in [
        "http://example.com",
        "http://localhost",
        "https://user@example.com",
        "https://example.com/?key=secret",
        "https://example.com/#x",
    ] {
        assert!(AnthropicAdapter::new(url, "secret".into()).is_err());
    }
    assert!(AnthropicAdapter::new("https://example.com", "bad\nkey".into()).is_err());
}
#[tokio::test]
async fn errors_and_truncated_streams_fail() {
    for content in [
        "data: nope\n\n",
        "data: {\"type\":\"message_start\",\"message\":{}}\n\n",
        "data: {\"type\":\"error\",\"error\":{\"type\":\"api_error\"}}\n\n",
    ] {
        let (url, rx, t) = server(http(content), false);
        let ad = AnthropicAdapter::new(&url, "fixture-secret".into()).unwrap();
        let a = attempt();
        let r = request();
        let b = ad.prepare(&a, &r).await.unwrap();
        assert!(
            collect_generation(ad.dispatch(&a, &r, b), &CancellationToken::new(), |_| {})
                .await
                .is_err()
        );
        rx.recv().unwrap();
        t.join().unwrap();
    }
}
#[tokio::test]
async fn cancellation_drops_pending_response() {
    let (url, rx, t) = server(http(""), true);
    let ad = AnthropicAdapter::new(&url, "fixture-secret".into()).unwrap();
    let a = attempt();
    let r = request();
    let b = ad.prepare(&a, &r).await.unwrap();
    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        trigger.cancel();
    });
    assert!(matches!(
        collect_generation(ad.dispatch(&a, &r, b), &cancel, |_| {}).await,
        Err(Error::Cancelled)
    ));
    rx.recv().unwrap();
    t.join().unwrap();
}
#[tokio::test]
async fn redirect_and_http_error_are_not_followed_or_exposed() {
    for response in [
        "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/\r\nContent-Length: 0\r\n\r\n",
        "HTTP/1.1 400 Bad Request\r\nContent-Length: 14\r\n\r\nfixture-secret",
    ] {
        let (url, rx, t) = server(response, false);
        let ad = AnthropicAdapter::new(&url, "fixture-secret".into()).unwrap();
        let a = attempt();
        let r = request();
        let b = ad.prepare(&a, &r).await.unwrap();
        let error = match ad.dispatch(&a, &r, b).await {
            Err(e) => e,
            Ok(_) => panic!("unexpected success"),
        };
        assert!(!error.to_string().contains("fixture-secret"));
        rx.recv().unwrap();
        t.join().unwrap();
    }
}
#[tokio::test]
async fn token_limit_is_marked_as_truncated() {
    let events = concat!(
        "data: {\"type\":\"message_start\",\"message\":{}}\n\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"}}\n\n",
        "data: {\"type\":\"message_stop\"}\n\n"
    );
    let (url, rx, t) = server(http(events), false);
    let ad = AnthropicAdapter::new(&url, "fixture-secret".into()).unwrap();
    let a = attempt();
    let r = request();
    let b = ad.prepare(&a, &r).await.unwrap();
    let generation = collect_generation(ad.dispatch(&a, &r, b), &CancellationToken::new(), |_| {})
        .await
        .unwrap();
    assert!(generation.is_truncated());
    rx.recv().unwrap();
    t.join().unwrap();
}
#[tokio::test]
async fn cancellation_during_stream() {
    let (url, rx, t) = server_with_options(
        http("data: {\"type\":\"message_start\",\"message\":{}}\n\n"),
        false,
        false,
        true,
    );
    let ad = AnthropicAdapter::new(&url, "fixture-secret".into()).unwrap();
    let a = attempt();
    let r = request();
    let b = ad.prepare(&a, &r).await.unwrap();
    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        trigger.cancel();
    });
    assert!(matches!(
        collect_generation(ad.dispatch(&a, &r, b), &cancel, |_| {}).await,
        Err(Error::Cancelled)
    ));
    rx.recv().unwrap();
    t.join().unwrap();
}
