use pi_rs_core::{CancellationToken, Error, Message, ModelRequest, ToolSpec};
use pi_rs_durable::model::{DurableRequest, ProviderAdapter, ProviderAttempt, collect_generation};
use pi_rs_durable::transcript::MessageContentBlocks;
use pi_rs_durable_google::GoogleAdapter;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use uuid::Uuid;

#[tokio::test]
async fn ordered_durable_blocks_are_preserved_and_hook_tampering_is_rejected() {
    let adapter = GoogleAdapter::new("http://127.0.0.1:1", "key").unwrap();
    let mut base = request();
    base.messages = vec![
        Message::User {
            content: "before".into(),
        },
        Message::User {
            content: String::new(),
        },
        Message::User {
            content: "after".into(),
        },
    ];
    let durable = DurableRequest {
        request: base,
        blocks: [(
            1,
            MessageContentBlocks {
                message_id: Uuid::new_v4(),
                blocks: vec![
                    json!({"type":"text","text":"first"}),
                    json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
                    json!({"type":"text","text":"last"}),
                ],
            },
        )]
        .into_iter()
        .collect(),
    };
    let a = attempt();
    let mut body = adapter.prepare_durable(&a, &durable).await.unwrap();
    assert_eq!(
        body["contents"][0]["parts"],
        json!([{"text":"before"},{"text":"first"},{"inlineData":{"mimeType":"image/png","data":"aGk="}},{"text":"last"},{"text":"after"}])
    );
    adapter.validate_durable(&a, &durable, &body).unwrap();
    body["contents"][0]["parts"][1]["text"] = json!("changed");
    assert!(adapter.validate_durable(&a, &durable, &body).is_err());
    let mut bad = durable.clone();
    bad.blocks.get_mut(&1).unwrap().blocks[0]["textSignature"] = json!("opaque");
    assert!(adapter.prepare_durable(&a, &bad).await.is_err());
    bad = durable.clone();
    bad.blocks.get_mut(&1).unwrap().blocks.clear();
    assert!(adapter.prepare_durable(&a, &bad).await.is_err());
    bad = durable;
    bad.blocks.get_mut(&1).unwrap().message_id = Uuid::nil();
    assert!(adapter.prepare_durable(&a, &bad).await.is_err());
}

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
        model: "gemini-3-flash".into(),
        messages: vec![Message::User {
            content: "hello".into(),
        }],
        tools: vec![ToolSpec {
            name: "read".into(),
            description: "Read".into(),
            parameters: json!({"type":"object"}),
        }],
        max_output_tokens: 50,
    }
}
async fn server(response: &'static str, hold: bool) -> (String, tokio::task::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1beta", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut data = Vec::new();
        let mut buf = [0; 4096];
        loop {
            let n = socket.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            data.extend_from_slice(&buf[..n]);
            if let Some(i) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&data[..i]);
                let length: usize = headers
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .and_then(|s| s.parse().ok())
                    })
                    .unwrap();
                if data.len() >= i + 4 + length {
                    break;
                }
            }
        }
        socket.write_all(response.as_bytes()).await.unwrap();
        if hold {
            let mut b = [0; 1];
            let _ = socket.read(&mut b).await;
        }
        data
    });
    (url, task)
}
fn sse(data: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata: {data}\n\n"
    )
}
#[tokio::test]
async fn sends_exact_hook_body_and_decodes_text_and_tool() {
    let response = Box::leak(sse(r#"{"candidates":[{"content":{"parts":[{"text":"ok"},{"functionCall":{"name":"read","id":"c1","args":{"path":"a"}},"thoughtSignature":"sig"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":3}}"#).into_boxed_str());
    let (url, server) = server(response, false).await;
    let adapter = GoogleAdapter::new(&url, "secret-key").unwrap();
    let req = request();
    let a = attempt();
    let mut body = adapter.prepare(&a, &req).await.unwrap();
    body["contents"][0]["parts"][0]["text"] = json!("after hook");
    let result = collect_generation(
        adapter.dispatch(&a, &req, body.clone()),
        &CancellationToken::new(),
        |_| {},
    )
    .await
    .unwrap();
    assert_eq!(result.content, "ok");
    assert_eq!(result.calls[0].id, "c1");
    assert_eq!(result.calls[0].name, "read");
    assert_eq!(result.usage.as_ref().unwrap().output_tokens, 3);
    assert_eq!(
        result.metadata.replay.as_ref().unwrap().blocks[1]["thoughtSignature"],
        "sig"
    );
    let mut followup = req.clone();
    followup.messages.push(result.assistant_message());
    followup.messages.push(Message::Tool {
        call_id: "c1".into(),
        content: "result".into(),
    });
    let followup = adapter.prepare(&a, &followup).await.unwrap();
    assert_eq!(
        followup["contents"][1]["parts"][1]["thoughtSignature"],
        "sig"
    );
    assert_eq!(
        followup["contents"][2]["parts"][0]["functionResponse"]["id"],
        "c1"
    );
    let data = server.await.unwrap();
    let split = data.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let headers = String::from_utf8_lossy(&data[..split]).to_ascii_lowercase();
    assert!(
        headers.starts_with("post /v1beta/models/gemini-3-flash:streamgeneratecontent?alt=sse")
    );
    assert!(headers.contains("x-goog-api-key: secret-key"));
    let sent: Value = serde_json::from_slice(&data[split + 4..]).unwrap();
    assert_eq!(sent, body);
    assert!(!sent.to_string().contains("secret-key"));
}
#[tokio::test]
async fn invalid_hook_changes_are_rejected_before_network() {
    let adapter = GoogleAdapter::new("http://127.0.0.1:9", "secret").unwrap();
    let req = request();
    let a = attempt();
    let body = adapter.prepare(&a, &req).await.unwrap();
    for changed in [
        json!({"generationConfig":{"maxOutputTokens":51}}),
        json!({"generationConfig":{"candidateCount":2}}),
        json!({"tools":[]}),
        json!({"cachedContent":"x"}),
    ] {
        let mut modified = body.clone();
        for (k, v) in changed.as_object().unwrap() {
            if k == "generationConfig" {
                for (x, y) in v.as_object().unwrap() {
                    modified[k][x] = y.clone();
                }
            } else {
                modified[k] = v.clone();
            }
        }
        assert!(matches!(
            adapter.dispatch(&a, &req, modified).await,
            Err(Error::Config(_))
        ));
    }
    for endpoint in [
        "http://example.com",
        "http://localhost:123",
        "https://user:pass@example.com",
        "https://example.com?key=secret",
        "https://example.com/#x",
    ] {
        assert!(GoogleAdapter::new(endpoint, "key").is_err());
    }
}
#[tokio::test]
async fn errors_and_truncation_do_not_complete_tool_calls() {
    for (data, is_length) in [
        (r#"{"error":{"code":429,"message":"secret"}}"#, false),
        (
            r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"read","args":{}}}]},"finishReason":"MAX_TOKENS"}]}"#,
            true,
        ),
        (
            r#"{"candidates":[{"content":{"parts":[{"text":"partial"}]}}]}"#,
            false,
        ),
    ] {
        let response = Box::leak(sse(data).into_boxed_str());
        let (url, task) = server(response, false).await;
        let adapter = GoogleAdapter::new(&url, "key").unwrap();
        let req = request();
        let a = attempt();
        let body = adapter.prepare(&a, &req).await.unwrap();
        let result = collect_generation(
            adapter.dispatch(&a, &req, body),
            &CancellationToken::new(),
            |_| {},
        )
        .await;
        if is_length {
            let result = result.unwrap();
            assert!(result.is_truncated());
            assert!(result.calls.is_empty());
        } else {
            assert!(result.is_err());
        }
        task.await.unwrap();
    }
}
#[tokio::test]
async fn cancellation_drops_pending_stream() {
    let (url, task) = server(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n",
        true,
    )
    .await;
    let adapter = GoogleAdapter::new(&url, "key").unwrap();
    let req = request();
    let a = attempt();
    let body = adapter.prepare(&a, &req).await.unwrap();
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        signal.cancel();
    });
    assert!(matches!(
        collect_generation(adapter.dispatch(&a, &req, body), &cancel, |_| {}).await,
        Err(Error::Cancelled)
    ));
    tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn rejects_redirects_and_http_errors_without_exposing_body() {
    for response in [
        "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:9/steal\r\nContent-Length: 0\r\n\r\n",
        "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 6\r\n\r\nsecret",
    ] {
        let (url, task) = server(response, false).await;
        let adapter = GoogleAdapter::new(&url, "key").unwrap();
        let req = request();
        let a = attempt();
        let body = adapter.prepare(&a, &req).await.unwrap();
        let err = match adapter.dispatch(&a, &req, body).await {
            Err(err) => err,
            Ok(_) => panic!("unexpected success"),
        };
        assert!(!err.to_string().contains("secret"));
        if response.contains("429") {
            assert!(matches!(err, Error::RetryableModel(_)));
        } else {
            assert!(matches!(err, Error::Model(_)));
        }
        task.await.unwrap();
    }
}

#[tokio::test]
async fn rejects_malformed_hook_contents_before_dispatch() {
    let adapter = GoogleAdapter::new("http://127.0.0.1:9", "key").unwrap();
    let req = request();
    let a = attempt();
    let body = adapter.prepare(&a, &req).await.unwrap();
    for contents in [
        json!([null]),
        json!([{"role":"user","parts":[{}]}]),
        json!([{"role":"admin","parts":[{"text":"hi"}]}]),
        json!([{"role":"user","parts":[{"text":42}]}]),
    ] {
        let mut modified = body.clone();
        modified["contents"] = contents;
        assert!(matches!(
            adapter.dispatch(&a, &req, modified).await,
            Err(Error::Config(_))
        ));
    }
}

#[tokio::test]
async fn rejects_partial_calls_duplicate_ids_and_candidates_after_finish() {
    for data in [
        r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"read","partialArgs":[{"jsonPath":"$.path","stringValue":"x"}],"willContinue":true}}]},"finishReason":"STOP"}]}"#,
        r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"read","id":"same","args":{}}},{"functionCall":{"name":"read","id":"same","args":{}}}]},"finishReason":"STOP"}]}"#,
        "{\"candidates\":[{\"finishReason\":\"MAX_TOKENS\"}]}\n\ndata: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"read\",\"args\":{}}}]},\"finishReason\":\"STOP\"}]}",
        r#"{"candidates":[{"content":{"parts":[null]},"finishReason":"STOP"}]}"#,
    ] {
        let response = Box::leak(sse(data).into_boxed_str());
        let (url, task) = server(response, false).await;
        let adapter = GoogleAdapter::new(&url, "key").unwrap();
        let req = request();
        let a = attempt();
        let body = adapter.prepare(&a, &req).await.unwrap();
        assert!(
            collect_generation(
                adapter.dispatch(&a, &req, body),
                &CancellationToken::new(),
                |_| {}
            )
            .await
            .is_err()
        );
        task.await.unwrap();
    }
}

#[tokio::test]
async fn base_path_and_model_ids_cannot_change_request_target() {
    let response = Box::leak(sse(r#"{"candidates":[{"finishReason":"STOP"}]}"#).into_boxed_str());
    let (url, task) = server(response, false).await;
    let adapter = GoogleAdapter::new(&format!("{url}/nested/"), "key").unwrap();
    let mut req = request();
    let a = attempt();
    for invalid in ["../steal", "models/../steal", "a?key=x", "a%2fb", ".", ".."] {
        req.model = invalid.into();
        assert!(matches!(
            adapter.prepare(&a, &req).await,
            Err(Error::Config(_))
        ));
    }
    req.model = "models/gemini-3-flash".into();
    let body = adapter.prepare(&a, &req).await.unwrap();
    collect_generation(
        adapter.dispatch(&a, &req, body),
        &CancellationToken::new(),
        |_| {},
    )
    .await
    .unwrap();
    let data = task.await.unwrap();
    assert!(
        String::from_utf8_lossy(&data).starts_with(
            "POST /v1beta/nested/models/gemini-3-flash:streamGenerateContent?alt=sse "
        )
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
async fn google_tool_blocks_reject_unrepresentable_order_and_tampering() {
    let adapter = GoogleAdapter::new("http://127.0.0.1:1", "key").unwrap();
    let a = attempt();
    let mut req = ordered_tool_request();
    assert!(adapter.prepare_with_tool_blocks(&a, &req).await.is_err());
    req.tool_blocks.get_mut(&2).unwrap().blocks.pop();
    let mut body = adapter.prepare_with_tool_blocks(&a, &req).await.unwrap();
    let response = &body["contents"][2]["parts"][0]["functionResponse"];
    assert_eq!(response["response"]["output"], "first");
    assert_eq!(response["parts"][0]["inlineData"]["data"], "aGk=");
    adapter.validate_with_tool_blocks(&a, &req, &body).unwrap();
    body["contents"][2]["parts"][0]["functionResponse"]["parts"][0]["inlineData"]["data"] =
        json!("YQ==");
    assert!(adapter.validate_with_tool_blocks(&a, &req, &body).is_err());
    req.request.request.model = "gemini-2.5-flash".into();
    assert!(adapter.prepare_with_tool_blocks(&a, &req).await.is_err());
}

#[tokio::test]
async fn google_tool_result_reaches_fake_http() {
    let response = Box::leak(
        sse(r#"{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}]}"#)
            .into_boxed_str(),
    );
    let (url, server) = server(response, false).await;
    let adapter = GoogleAdapter::new(&url, "secret-key").unwrap();
    let a = attempt();
    let mut req = ordered_tool_request();
    req.tool_blocks.get_mut(&2).unwrap().blocks.pop();
    let body = adapter.prepare_with_tool_blocks(&a, &req).await.unwrap();
    adapter.validate_with_tool_blocks(&a, &req, &body).unwrap();
    collect_generation(
        adapter.dispatch(&a, &req.request.request, body.clone()),
        &CancellationToken::new(),
        |_| {},
    )
    .await
    .unwrap();
    let bytes = server.await.unwrap();
    let split = bytes.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let sent: Value = serde_json::from_slice(&bytes[split + 4..]).unwrap();
    assert_eq!(sent, body);
    assert_eq!(
        sent["contents"][2]["parts"][0]["functionResponse"]["parts"][0]["inlineData"]["data"],
        "aGk="
    );
}
