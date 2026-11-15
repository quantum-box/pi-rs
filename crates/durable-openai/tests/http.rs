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

struct CountingTool(std::sync::atomic::AtomicUsize);

#[async_trait::async_trait]
impl pi_rs_core::Tool for CountingTool {
    fn spec(&self) -> pi_rs_core::ToolSpec {
        pi_rs_core::ToolSpec {
            name: "read".into(),
            description: "test tool".into(),
            parameters: json!({"type":"object","properties":{}}),
        }
    }

    async fn invoke(&self, _: Value, _: pi_rs_core::ToolContext) -> pi_rs_core::Outcome {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        pi_rs_core::Outcome::Succeeded("ok".into())
    }
}

#[tokio::test]
async fn runtime_never_dispatches_incomplete_or_empty_calls() {
    use pi_rs_durable::{
        recovery::{ReplayPolicy, UncertainPolicy},
        runtime::{Config, Harness, Options, RegisteredTool, Status},
    };
    use std::sync::{Arc, atomic::Ordering};

    // Include valid JSON on an incomplete response: syntax alone must not authorize it.
    for (incomplete, arguments, expected_invocations) in [
        (true, "", 0),
        (true, "{", 0),
        (true, "{}", 0),
        (false, "", 0),
        (false, "{}", 1), // Positive control: the registered tool is reachable.
    ] {
        let terminal = if incomplete {
            json!({"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[{"type":"function_call","call_id":"c","name":"read","arguments":arguments}]}})
        } else {
            json!({"type":"response.completed","response":{"status":"completed","output":[{"type":"function_call","call_id":"c","name":"read","arguments":arguments}]}})
        };
        let added = json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"c","name":"read"}});
        let delta = json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":arguments});
        let delta_event = if arguments.is_empty() {
            String::new()
        } else {
            format!("data: {delta}\n\n")
        };
        let events = format!("data: {added}\n\n{delta_event}data: {terminal}\n\n");
        let (url, rx, thread) = server(http(&events), false);
        let dir = tempfile::tempdir().unwrap();
        let tool = Arc::new(CountingTool(std::sync::atomic::AtomicUsize::new(0)));
        let mut h = Harness::open_paused(
            dir.path().join("db"),
            Options {
                model: Arc::new(UnusedModel),
                tools: vec![RegisteredTool {
                    tool: tool.clone(),
                    replay: ReplayPolicy::Unsafe,
                }],
                restart_limit: 1,
                max_turns: 1,
                uncertain_policy: UncertainPolicy::InterruptedResult,
            },
        )
        .unwrap();
        h.install_provider_adapter(Arc::new(
            OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap(),
        ))
        .unwrap();
        let id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "system".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        h.select_provider_adapter(id, true).unwrap();
        h.submit_and_run(id, None, "hello".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            tool.0.load(Ordering::SeqCst),
            expected_invocations,
            "incomplete={incomplete}, arguments={arguments:?}"
        );
        let conversation = h.conversation(id).unwrap();
        if incomplete {
            assert!(matches!(
                &conversation.submissions[0].status,
                Status::Unanswered { .. }
            ));
            assert!(
                matches!(conversation.messages.last(), Some(Message::Tool { call_id, .. }) if call_id == "c")
            );
            let result = &conversation.content_blocks[&(conversation.messages.len() - 1)];
            assert!(
                result.blocks[0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("truncated")
            );
        }
        rx.recv().unwrap();
        thread.join().unwrap();
    }
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

struct ContinueBlocks {
    cancel: CancellationToken,
    yields: std::sync::atomic::AtomicUsize,
    before: std::sync::atomic::AtomicUsize,
    seen: std::sync::Mutex<Vec<Value>>,
}
#[async_trait::async_trait]
impl pi_rs_durable::extension::Hook for ContinueBlocks {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        value: &Value,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision, GenerationEvent};
        use std::sync::atomic::Ordering;
        match event {
            GenerationEvent::OnYield if self.yields.fetch_add(1, Ordering::SeqCst) == 0 => {
                Ok(GenerationDecision::Continue(json!([
                    {"type":"text", "text":"first"},
                    {"type":"image", "data":"aGk=", "mimeType":"image/png", "detail":"high"},
                    {"type":"text", "text":"last"}
                ])))
            }
            GenerationEvent::BeforeRequest if value.get("content_blocks").is_some() => {
                self.seen
                    .lock()
                    .unwrap()
                    .push(value["content_blocks"].clone());
                if self.before.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.cancel.cancel();
                    std::future::pending::<()>().await;
                }
                Ok(GenerationDecision::Pass)
            }
            GenerationEvent::BeforeProviderRequest => {
                let mut payload = value["payload"].clone();
                payload["temperature"] = json!(0.25);
                Ok(GenerationDecision::Replace(json!({"payload":payload})))
            }
            _ => Ok(GenerationDecision::Pass),
        }
    }
}

#[tokio::test]
async fn yield_blocks_reopen_and_dispatch_in_order() {
    use pi_rs_durable::{
        extension::Extension,
        recovery::UncertainPolicy,
        runtime::{Config, Harness, Options, Status},
    };
    use std::sync::Arc;
    let events = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n";
    let (url, first_rx, first_thread) = server(http(events), false);
    let options = || Options {
        model: Arc::new(UnusedModel),
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let hook = Arc::new(ContinueBlocks {
        cancel: cancel.clone(),
        yields: std::sync::atomic::AtomicUsize::new(0),
        before: std::sync::atomic::AtomicUsize::new(0),
        seen: std::sync::Mutex::new(vec![]),
    });
    let extension = || Extension {
        name: "blocks".into(),
        hooks: vec![hook.clone()],
        ..Default::default()
    };
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_provider_adapter(Arc::new(
        OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap(),
    ))
    .unwrap();
    h.install_extension(extension()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "system".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.select_extensions(id, vec!["blocks".into()]).unwrap();
    assert!(
        h.submit_and_run(id, None, "initial".into(), &cancel)
            .await
            .is_err()
    );
    first_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    first_thread.join().unwrap();
    drop(h);
    let (url, rx, thread) = server(http(events), false);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let reopened = h.conversation(id).unwrap();
    let binding = reopened.content_blocks.values().next().unwrap().clone();
    assert_eq!(binding.blocks[1]["detail"], json!("high"));
    h.install_provider_adapter(Arc::new(
        OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap(),
    ))
    .unwrap();
    h.install_extension(extension()).unwrap();
    assert!(
        h.conversation(id)
            .unwrap()
            .content_blocks
            .values()
            .any(|b| b.blocks.len() == 3)
    );
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let body = rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let content = &body["input"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["role"] == "user" && v["content"].is_array())
        .unwrap()["content"];
    assert_eq!(
        content,
        &json!([
            {"type":"input_text","text":"first"},
            {"type":"input_image","image_url":"data:image/png;base64,aGk=","detail":"high"},
            {"type":"input_text","text":"last"}
        ])
    );
    assert_eq!(body["temperature"], json!(0.25));
    assert_eq!(
        h.conversation(id)
            .unwrap()
            .content_blocks
            .values()
            .next()
            .unwrap()
            .message_id,
        binding.message_id
    );
    assert!(matches!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
    assert_eq!(hook.seen.lock().unwrap().len(), 2);
    let fork = h.fork(id).unwrap();
    assert_eq!(
        h.conversation(fork).unwrap().content_blocks,
        h.conversation(id).unwrap().content_blocks
    );
    // This fixture's extension has not declared block-aware compaction support.
    assert!(h.request_manual_compaction(id, 0, None).is_err());
    thread.join().unwrap();
}

#[tokio::test]
async fn nonvision_catalog_projects_images_on_real_adapter_after_reopen() {
    use pi_rs_durable::{
        extension::Extension,
        recovery::UncertainPolicy,
        runtime::{Config, Harness, Options, Status},
    };
    use std::sync::Arc;
    let events = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n";
    let (url, first_rx, first_thread) = server(http(events), false);
    let options = || Options {
        model: Arc::new(UnusedModel),
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let hook = Arc::new(ContinueBlocks {
        cancel: cancel.clone(),
        yields: std::sync::atomic::AtomicUsize::new(0),
        before: std::sync::atomic::AtomicUsize::new(0),
        seen: std::sync::Mutex::new(vec![]),
    });
    let extension = || Extension {
        name: "blocks".into(),
        hooks: vec![hook.clone()],
        ..Default::default()
    };
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_provider_adapter(Arc::new(
        OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap(),
    ))
    .unwrap();
    h.install_extension(extension()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "o3-mini".into(),
            system_prompt: "system".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.select_extensions(id, vec!["blocks".into()]).unwrap();
    assert!(
        h.submit_and_run(id, None, "initial".into(), &cancel)
            .await
            .is_err()
    );
    first_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    first_thread.join().unwrap();
    drop(h);
    let (url, rx, thread) = server(http(events), false);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let reopened = h.conversation(id).unwrap();
    let binding = reopened.content_blocks.values().next().unwrap().clone();
    assert_eq!(binding.blocks[1]["detail"], json!("high"));
    h.install_provider_adapter(Arc::new(
        OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap(),
    ))
    .unwrap();
    h.install_extension(extension()).unwrap();
    assert!(
        h.conversation(id)
            .unwrap()
            .content_blocks
            .values()
            .any(|b| b.blocks.len() == 3)
    );
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let body = rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let content = &body["input"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["role"] == "user" && v["content"].is_array())
        .unwrap()["content"];
    assert_eq!(
        content,
        &json!([
            {"type":"input_text","text":"first"},
            {"type":"input_text","text":"(image omitted: model does not support images)"},
            {"type":"input_text","text":"last"}
        ])
    );
    assert_eq!(body["temperature"], json!(0.25));
    assert_eq!(
        h.conversation(id)
            .unwrap()
            .content_blocks
            .values()
            .next()
            .unwrap()
            .message_id,
        binding.message_id
    );
    assert!(matches!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
    assert_eq!(hook.seen.lock().unwrap().len(), 2);
    let fork = h.fork(id).unwrap();
    assert_eq!(
        h.conversation(fork).unwrap().content_blocks,
        h.conversation(id).unwrap().content_blocks
    );
    // This fixture's extension has not declared block-aware compaction support.
    assert!(h.request_manual_compaction(id, 0, None).is_err());
    thread.join().unwrap();
}

#[tokio::test]
async fn ordered_block_encoder_rejects_unrepresentable_metadata_and_hook_rewrite() {
    use pi_rs_durable::{model::DurableRequest, transcript::MessageContentBlocks};
    use std::collections::BTreeMap;
    let adapter =
        OpenAiResponsesAdapter::new("http://127.0.0.1:1234", "fixture-secret".into()).unwrap();
    let mut req = request();
    req.messages[0] = Message::User {
        content: String::new(),
    };
    let mut durable = DurableRequest {
        request: req,
        blocks: BTreeMap::from([(
            0,
            MessageContentBlocks {
                message_id: Uuid::new_v4(),
                blocks: vec![json!({"type":"text","text":"a","textSignature":"opaque"})],
            },
        )]),
    };
    assert!(adapter.prepare_durable(&attempt(), &durable).await.is_err());
    durable.blocks.get_mut(&0).unwrap().blocks = vec![
        json!({"type":"text","text":"a"}),
        json!({"type":"text","text":"b"}),
    ];
    let mut body = adapter.prepare_durable(&attempt(), &durable).await.unwrap();
    adapter
        .validate_durable(&attempt(), &durable, &body)
        .unwrap();
    body["input"][0]["content"][1]["text"] = json!("changed");
    assert!(
        adapter
            .validate_durable(&attempt(), &durable, &body)
            .is_err()
    );
}

struct RewriteBlocks;
#[async_trait::async_trait]
impl pi_rs_durable::extension::Hook for RewriteBlocks {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        body: &Value,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision, GenerationEvent};
        if event != GenerationEvent::BeforeRequest {
            return Ok(GenerationDecision::Pass);
        }
        let mut value = body.clone();
        value["content_blocks"] = json!({});
        Ok(GenerationDecision::Replace(value))
    }
}

#[tokio::test]
async fn before_request_cannot_discard_block_binding() {
    use pi_rs_durable::{
        extension::Extension,
        recovery::UncertainPolicy,
        runtime::{Config, Harness, Options},
    };
    use std::sync::Arc;
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
        OpenAiResponsesAdapter::new("http://127.0.0.1:1", "fixture-secret".into()).unwrap(),
    ))
    .unwrap();
    h.install_extension(Extension {
        name: "rewrite-blocks".into(),
        hooks: vec![Arc::new(RewriteBlocks)],
        ..Default::default()
    })
    .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.select_extensions(id, vec!["rewrite-blocks".into()])
        .unwrap();
    h.append_user_blocks(id, vec![json!({"type":"text","text":"preserved"})])
        .unwrap();
    let err = h
        .submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("cannot replace durable content bindings"),
        "{err}"
    );
}

fn ordered_tool_request() -> pi_rs_durable::model::DurableToolRequest {
    let mut request = request();
    request.messages = vec![
        Message::User {
            content: "go".into(),
        },
        Message::Assistant {
            content: String::new(),
            calls: vec![pi_rs_core::ToolCall {
                id: "c1".into(),
                name: "read".into(),
                arguments: "{}".into(),
            }],
            replay: None,
        },
        Message::Tool {
            call_id: "c1".into(),
            content: String::new(),
        },
    ];
    pi_rs_durable::model::DurableToolRequest {
        request: pi_rs_durable::model::DurableRequest {
            request,
            blocks: Default::default(),
        },
        tool_blocks: [(
            2,
            pi_rs_durable::transcript::MessageContentBlocks {
                message_id: Uuid::new_v4(),
                blocks: vec![
                    json!({"type":"text","text":"first"}),
                    json!({"type":"image","mimeType":"image/png","data":"aGk="}),
                    json!({"type":"text","text":"last"}),
                ],
            },
        )]
        .into_iter()
        .collect(),
    }
}

#[tokio::test]
async fn tool_blocks_reach_http_in_order_and_reject_hook_tampering() {
    let (url, rx, thread) = server(
        http(
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n",
        ),
        false,
    );
    let adapter = OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap();
    let a = attempt();
    let req = ordered_tool_request();
    let body = adapter.prepare_with_tool_blocks(&a, &req).await.unwrap();
    assert_eq!(
        body["input"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["type"] == "function_call_output")
            .unwrap()["output"]
            .clone(),
        json!([{"type":"input_text","text":"first\nlast"},{"type":"input_image","detail":"auto","image_url":"data:image/png;base64,aGk="}])
    );
    adapter.validate_with_tool_blocks(&a, &req, &body).unwrap();
    let mut changed = body.clone();
    let key = if changed.get("input").is_some() {
        "input"
    } else {
        "messages"
    };
    changed[key] = json!([]);
    assert!(
        adapter
            .validate_with_tool_blocks(&a, &req, &changed)
            .is_err()
    );
    let mut duplicate = req.clone();
    duplicate.request.request.messages.push(Message::Tool {
        call_id: "c1".into(),
        content: String::new(),
    });
    assert!(
        adapter
            .prepare_with_tool_blocks(&a, &duplicate)
            .await
            .is_err()
    );
    collect_generation(
        adapter.dispatch(&a, &req.request.request, body.clone()),
        &CancellationToken::new(),
        |_| {},
    )
    .await
    .unwrap();
    assert_eq!(rx.recv().unwrap(), body);
    thread.join().unwrap();
}

struct ToolBlocksFixture(std::sync::Arc<std::sync::atomic::AtomicUsize>);
#[async_trait::async_trait]
impl pi_rs_core::Tool for ToolBlocksFixture {
    fn spec(&self) -> pi_rs_core::ToolSpec {
        pi_rs_core::ToolSpec {
            name: "read".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: pi_rs_core::ToolContext) -> pi_rs_core::Outcome {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        pi_rs_core::Outcome::Succeeded("fixture".into())
    }
}
struct BlockAndInterrupt(std::sync::atomic::AtomicBool);
#[async_trait::async_trait]
impl pi_rs_durable::extension::Hook for BlockAndInterrupt {
    fn supports_runtime_tool_blocks(&self) -> bool {
        true
    }
    async fn after_tool_blocks(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        result: &pi_rs_durable::extension::ToolBlockResult,
        _: bool,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolBlockResult>, String> {
        let mut result = result.clone();
        result.output = vec![
            json!({"type":"text","text":"first"}),
            json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
            json!({"type":"text","text":"last"}),
        ];
        Ok(Some(result))
    }
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        value: &Value,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision, GenerationEvent};
        if event == GenerationEvent::BeforeRequest
            && value.get("tool_content_blocks").is_some()
            && self.0.load(std::sync::atomic::Ordering::SeqCst)
        {
            let mut changed = value.clone();
            changed["tool_content_blocks"] = json!({});
            return Ok(GenerationDecision::Replace(changed));
        }
        Ok(GenerationDecision::Pass)
    }
}
#[tokio::test]
async fn persisted_tool_blocks_resume_to_http_without_reexecution() {
    use pi_rs_durable::{
        extension::Extension,
        recovery::{ReplayPolicy, UncertainPolicy},
        runtime::{Config, Harness, Options, RegisteredTool},
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let first = concat!(
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"call_id\":\"c1\",\"name\":\"read\"}}\n\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"{}\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"function_call\",\"call_id\":\"c1\",\"name\":\"read\",\"arguments\":\"{}\"}]}}\n\n"
    );
    let (url, rx, thread) = server(http(first), false);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let options = || Options {
        model: Arc::new(UnusedModel),
        tools: vec![RegisteredTool {
            tool: Arc::new(ToolBlocksFixture(calls.clone())),
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let hook = Arc::new(BlockAndInterrupt(AtomicBool::new(true)));
    let extension = || Extension {
        name: "blocks".into(),
        hooks: vec![hook.clone()],
        ..Default::default()
    };
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_provider_adapter(Arc::new(
        OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap(),
    ))
    .unwrap();
    h.install_extension(extension()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.select_extensions(id, vec!["blocks".into()]).unwrap();
    assert!(
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .is_err()
    );
    rx.recv().unwrap();
    thread.join().unwrap();
    drop(h);
    hook.0.store(false, Ordering::SeqCst);
    let (url, rx, thread) = server(
        http(
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n",
        ),
        false,
    );
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_provider_adapter(Arc::new(
        OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap(),
    ))
    .unwrap();
    h.install_extension(extension()).unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let body = rx.recv().unwrap();
    let result = body["input"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["type"] == "function_call_output")
        .unwrap();
    assert_eq!(result["call_id"], "c1");
    assert_eq!(
        result["output"],
        json!([{"type":"input_text","text":"first\nlast"},{"type":"input_image","detail":"auto","image_url":"data:image/png;base64,aGk="}])
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    thread.join().unwrap();
}

#[tokio::test]
async fn upstream_tool_status_and_image_profile_fixtures() {
    use pi_rs_durable::model::ProviderResultRequest;
    let mut adapter = OpenAiResponsesAdapter::new("http://127.0.0.1:1", "key".into()).unwrap();
    let builtin = adapter.image_profile("gpt-4").unwrap().supports_images;
    adapter
        .set_model_descriptor(
            "gpt-4",
            &json!({"input": if builtin { vec!["text"] } else { vec!["text", "image"] }}),
        )
        .unwrap();
    assert_eq!(
        adapter.image_profile("gpt-4").unwrap().supports_images,
        !builtin
    );
    let a = attempt();
    let req = ordered_tool_request();
    for error in [None, Some(false), Some(true)] {
        let status = error
            .map(|v| [(2, v)].into_iter().collect())
            .unwrap_or_default();
        let result = ProviderResultRequest {
            request: req.clone(),
            tool_errors: status,
        };
        let body = adapter
            .prepare_with_result_metadata(&a, &result)
            .await
            .unwrap();
        let item = body["input"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["type"] == "function_call_output")
            .unwrap();
        assert_eq!(
            item["output"],
            json!([{"type":"input_text","text":"first\nlast"},{"type":"input_image","detail":"auto","image_url":"data:image/png;base64,aGk="}])
        );
        assert!(item.get("is_error").is_none());
        adapter
            .validate_with_result_metadata(&a, &result, &body)
            .unwrap();
    }
    adapter
        .set_model_descriptor("test", &json!({"input":["text"]}))
        .unwrap();
    assert!(!adapter.image_profile("test").unwrap().supports_images);
    let result = ProviderResultRequest {
        request: req.clone(),
        tool_errors: [(2, true)].into_iter().collect(),
    };
    let mut body = adapter
        .prepare_with_result_metadata(&a, &result)
        .await
        .unwrap();
    let item = body["input"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["type"] == "function_call_output")
        .unwrap();
    assert_eq!(
        item["output"],
        "first\n(tool image omitted: model does not support images)\nlast"
    );
    assert!(!body.to_string().contains("aGk="));
    adapter
        .validate_with_result_metadata(&a, &result, &body)
        .unwrap();
    body["input"] = json!([]);
    assert!(
        adapter
            .validate_with_result_metadata(&a, &result, &body)
            .is_err()
    );
    adapter
        .set_model_descriptor("test", &json!({"input":["text","image"]}))
        .unwrap();
    assert!(adapter.image_profile("test").unwrap().supports_images);
    assert!(
        adapter
            .prepare_with_result_metadata(&a, &result)
            .await
            .unwrap()
            .to_string()
            .contains("aGk=")
    );
    assert!(
        adapter
            .set_model_descriptor("test", &json!({"input":"image"}))
            .is_err()
    );
}

#[tokio::test]
async fn status_aware_body_reaches_fake_http() {
    use pi_rs_durable::model::ProviderResultRequest;
    let (url, rx, thread) = server(
        http(
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n",
        ),
        false,
    );
    let adapter = OpenAiResponsesAdapter::new(&url, "fixture-secret".into()).unwrap();
    let a = attempt();
    let result = ProviderResultRequest {
        request: ordered_tool_request(),
        tool_errors: [(2, true)].into_iter().collect(),
    };
    let body = adapter
        .prepare_with_result_metadata(&a, &result)
        .await
        .unwrap();
    adapter
        .validate_with_result_metadata(&a, &result, &body)
        .unwrap();
    collect_generation(
        adapter.dispatch(&a, &result.request.request.request, body.clone()),
        &CancellationToken::new(),
        |_| {},
    )
    .await
    .unwrap();
    assert_eq!(rx.recv().unwrap(), body);
    thread.join().unwrap();
}

mod lifecycle {
    use super::*;
    const LIFECYCLE_CALL: &str = "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"call_id\":\"c1\",\"name\":\"read\"}}\n\ndata: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"{}\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"function_call\",\"call_id\":\"c1\",\"name\":\"read\",\"arguments\":\"{}\"}]}}\n\n";
    const LIFECYCLE_DONE: &str = "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n";
    fn lifecycle_adapter(url: &str) -> std::sync::Arc<dyn pi_rs_durable::model::ProviderAdapter> {
        let mut adapter = OpenAiResponsesAdapter::new(url, "fixture-secret".into()).unwrap();
        adapter
            .set_model_descriptor(
                "test",
                &json!({"input":["text"],"inputLimits":{"images":{"resize":{"maxWidth":512}}}}),
            )
            .unwrap();
        std::sync::Arc::new(adapter)
    }
    fn lifecycle_assert(body: &Value) {
        let item = body["input"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["type"] == "function_call_output")
            .unwrap();
        assert!(item.get("is_error").is_none());
        assert_eq!(
            item["output"],
            "before\n(tool image omitted: model does not support images)\nafter"
        );
    }
    include!("../../durable/tests/fixtures/provider_tool_lifecycle.rs");
}
