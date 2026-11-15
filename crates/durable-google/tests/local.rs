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

// Expected wire cases transcribed from pi's google-shared.ts at c3cc91632d0a.
#[tokio::test]
async fn upstream_tool_result_fixtures_and_hook_validation() {
    let adapter = GoogleAdapter::new("http://127.0.0.1:1", "key").unwrap();
    let a = attempt();
    assert_eq!(adapter.id(), "google-generative-ai/v1");
    let cases: Value =
        serde_json::from_str(include_str!("fixtures/upstream_tool_results.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let mut req = ordered_tool_request();
        req.request.request.model = case["model"].as_str().unwrap().into();
        req.tool_blocks.get_mut(&2).unwrap().blocks = case["blocks"].as_array().unwrap().clone();
        let before = req.tool_blocks[&2].blocks.clone();
        let body = adapter.prepare_with_tool_blocks(&a, &req).await.unwrap();
        assert_eq!(
            json!(&body["contents"].as_array().unwrap()[2..]),
            case["expected"],
            "{}",
            case["name"]
        );
        assert_eq!(req.tool_blocks[&2].blocks, before);
        adapter.validate_with_tool_blocks(&a, &req, &body).unwrap();
        let mut changed = body.clone();
        changed["contents"][2]["parts"][0]["functionResponse"]["response"]["output"] =
            json!("tampered");
        assert!(
            adapter
                .validate_with_tool_blocks(&a, &req, &changed)
                .is_err()
        );
    }
}

#[tokio::test]
async fn ordered_mode_is_explicit_and_fenced_by_adapter_identity() {
    let adapter = GoogleAdapter::new_ordered_tool_blocks("http://127.0.0.1:1", "key").unwrap();
    let a = attempt();
    let req = ordered_tool_request();
    assert_eq!(adapter.id(), "google-generative-ai-ordered-tool-blocks/v1");
    let mut body = adapter.prepare_with_tool_blocks(&a, &req).await.unwrap();
    let name = format!("pi-{}-1", req.tool_blocks[&2].message_id);
    assert_eq!(
        body["contents"][2]["parts"][0]["functionResponse"]["response"]["output"],
        json!(["first",{"$ref":name},"last"])
    );
    assert_eq!(
        body["contents"][2]["parts"][0]["functionResponse"]["parts"][0]["inlineData"]["displayName"],
        name
    );
    adapter.validate_with_tool_blocks(&a, &req, &body).unwrap();
    body["contents"][2]["parts"][0]["functionResponse"]["parts"][0]["inlineData"]["displayName"] =
        json!("changed");
    assert!(adapter.validate_with_tool_blocks(&a, &req, &body).is_err());
    let mut old = req.clone();
    old.request.request.model = "gemini-2.5-flash".into();
    assert!(adapter.prepare_with_tool_blocks(&a, &old).await.is_err());
    let mut invalid = req;
    invalid.tool_blocks.get_mut(&2).unwrap().blocks[1]["mimeType"] = json!("image/gif");
    assert!(
        adapter
            .prepare_with_tool_blocks(&a, &invalid)
            .await
            .is_err()
    );
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
    let req = ordered_tool_request();
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
    assert_eq!(
        sent["contents"][2]["parts"][0]["functionResponse"]["response"]["output"],
        "first\nlast"
    );
}

#[tokio::test]
async fn consecutive_tool_results_follow_upstream_image_placement() {
    let adapter = GoogleAdapter::new("http://127.0.0.1:1", "key").unwrap();
    let a = attempt();
    for model in ["gemini-3-flash", "gemini-2.5-flash"] {
        let mut req = ordered_tool_request();
        req.request.request.model = model.into();
        if let Message::Assistant { calls, .. } = &mut req.request.request.messages[1] {
            calls.push(pi_rs_core::ToolCall {
                id: "c2".into(),
                name: "read".into(),
                arguments: "{}".into(),
            });
        }
        req.request.request.messages.push(Message::Tool {
            call_id: "c2".into(),
            content: String::new(),
        });
        req.tool_blocks.insert(
            3,
            pi_rs_durable::transcript::MessageContentBlocks {
                message_id: Uuid::new_v4(),
                blocks: vec![
                    json!({"type":"text","text":"second"}),
                    json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
                ],
            },
        );
        let body = adapter.prepare_with_tool_blocks(&a, &req).await.unwrap();
        let contents = body["contents"].as_array().unwrap();
        if model == "gemini-3-flash" {
            assert_eq!(contents.len(), 3);
            let results = contents[2]["parts"].as_array().unwrap();
            assert_eq!(results.len(), 2);
            assert_eq!(
                results[0]["functionResponse"]["response"]["output"],
                "first\nlast"
            );
            assert_eq!(
                results[1]["functionResponse"]["response"]["output"],
                "second"
            );
        } else {
            assert_eq!(contents.len(), 6);
            assert_eq!(
                contents[2]["parts"][0]["functionResponse"]["response"]["output"],
                "first\nlast"
            );
            assert_eq!(contents[3]["parts"][0]["text"], "Tool result image:");
            assert_eq!(
                contents[4]["parts"][0]["functionResponse"]["response"]["output"],
                "second"
            );
            assert_eq!(contents[5]["parts"][0]["text"], "Tool result image:");
        }
        adapter.validate_with_tool_blocks(&a, &req, &body).unwrap();
    }
}

struct UnusedModelForMode;
#[async_trait::async_trait]
impl pi_rs_core::Model for UnusedModelForMode {
    async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<pi_rs_core::ModelStream> {
        panic!("configured provider must handle requests")
    }
}

#[tokio::test]
async fn reopen_cannot_silently_switch_tool_result_mode() {
    use pi_rs_durable::{
        recovery::UncertainPolicy,
        runtime::{Config, Harness, Options},
    };
    use std::sync::Arc;
    let path = std::env::temp_dir().join(format!("pi-google-mode-{}", Uuid::new_v4()));
    let options = || Options {
        model: Arc::new(UnusedModelForMode),
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let id;
    {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.install_provider_adapter(Arc::new(
            GoogleAdapter::new_ordered_tool_blocks("http://127.0.0.1:1", "key").unwrap(),
        ))
        .unwrap();
        id = h
            .create_conversation(Config {
                model: "gemini-3-flash".into(),
                system_prompt: String::new(),
                max_output_tokens: 100,
            })
            .unwrap();
        h.select_provider_adapter(id, true).unwrap();
        h.submit(id, None, "hello".into()).unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.install_provider_adapter(Arc::new(GoogleAdapter::new(&endpoint, "key").unwrap()))
            .unwrap();
        let err = h
            .resume(&CancellationToken::new())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("selected provider adapter unavailable"),
            "{err}"
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    }
    drop(listener);
    let response = Box::leak(
        sse(r#"{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}]}"#)
            .into_boxed_str(),
    );
    let (endpoint, server) = server(response, false).await;
    {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.install_provider_adapter(Arc::new(
            GoogleAdapter::new_ordered_tool_blocks(&endpoint, "key").unwrap(),
        ))
        .unwrap();
        h.resume(&CancellationToken::new()).await.unwrap();
    }
    server.await.unwrap();
    for suffix in ["", "-wal", "-shm", ".owner"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test]
async fn tool_error_and_model_input_fixtures_follow_upstream() {
    use pi_rs_durable::model::ProviderResultRequest;
    let a = attempt();
    let fixtures: Value =
        serde_json::from_str(include_str!("fixtures/upstream_tool_metadata.json")).unwrap();
    for fixture in fixtures.as_array().unwrap() {
        let model = fixture["model"].as_str().unwrap();
        let images_allowed = fixture["image_input"].as_bool().unwrap();
        let is_error = fixture["is_error"].as_bool().unwrap();
        let image_only = fixture["image_only"].as_bool().unwrap();
        let mut adapter = GoogleAdapter::new("http://127.0.0.1:1", "key").unwrap();
        adapter
            .set_model_descriptor(
                model,
                &if images_allowed {
                    json!({"input":["text","image"]})
                } else {
                    json!({"input":["text"]})
                },
            )
            .unwrap();
        let mut req = ordered_tool_request();
        req.request.request.model = model.into();
        if image_only {
            req.tool_blocks.get_mut(&2).unwrap().blocks =
                vec![json!({"type":"image","data":"aGk=","mimeType":"image/png"})];
        }
        let result = ProviderResultRequest {
            request: req,
            tool_errors: [(2, is_error)].into_iter().collect(),
        };
        let mut body = adapter
            .prepare_with_result_metadata(&a, &result)
            .await
            .unwrap();
        let response = &body["contents"][2]["parts"][0]["functionResponse"];
        let key = if is_error { "error" } else { "output" };
        let other = if is_error { "output" } else { "error" };
        let text = fixture["expected"].as_str().unwrap();
        assert_eq!(response["response"][key], text);
        assert!(response["response"].get(other).is_none());
        if images_allowed && model.starts_with("gemini-3") {
            assert_eq!(response["parts"][0]["inlineData"]["data"], "aGk=");
        }
        if !images_allowed {
            assert!(!body.to_string().contains("aGk="));
        }
        if images_allowed && model.starts_with("gemini-2") {
            assert_eq!(
                body["contents"][3]["parts"][1]["inlineData"]["data"],
                "aGk="
            );
        }
        adapter
            .validate_with_result_metadata(&a, &result, &body)
            .unwrap();
        body["contents"][2]["parts"][0]["functionResponse"]["response"] =
            json!({"output":"tampered"});
        assert!(
            adapter
                .validate_with_result_metadata(&a, &result, &body)
                .is_err()
        );
    }
    let mut adapter = GoogleAdapter::new("http://127.0.0.1:1", "key").unwrap();
    for invalid in [
        json!({}),
        json!({"input":"image"}),
        json!({"input":["image",3]}),
        json!({"input":["audio"]}),
    ] {
        assert!(adapter.set_model_descriptor("m", &invalid).is_err());
    }
}

#[tokio::test]
async fn tool_status_also_applies_to_plain_results_and_ordered_mode() {
    use pi_rs_durable::model::ProviderResultRequest;
    let a = attempt();
    let mut adapter = GoogleAdapter::new_ordered_tool_blocks("http://127.0.0.1:1", "key").unwrap();
    let req = ordered_tool_request();
    let request = ProviderResultRequest {
        request: req,
        tool_errors: [(2, true)].into_iter().collect(),
    };
    let body = adapter
        .prepare_with_result_metadata(&a, &request)
        .await
        .unwrap();
    assert_eq!(
        body["contents"][2]["parts"][0]["functionResponse"]["response"]["error"][0],
        "first"
    );
    adapter
        .set_model_descriptor("gemini-3-flash", &json!({"input":["text"]}))
        .unwrap();
    let body = adapter
        .prepare_with_result_metadata(&a, &request)
        .await
        .unwrap();
    assert_eq!(
        body["contents"][2]["parts"][0]["functionResponse"]["response"]["error"],
        json!(["first", "last"])
    );
    assert!(!body.to_string().contains("aGk="));
    let mut plain = request.clone();
    plain.request.tool_blocks.clear();
    if let Message::Tool { content, .. } = &mut plain.request.request.request.messages[2] {
        *content = "failure".into();
    }
    let body = adapter
        .prepare_with_result_metadata(&a, &plain)
        .await
        .unwrap();
    assert_eq!(
        body["contents"][2]["parts"][0]["functionResponse"]["response"]["error"],
        "failure"
    );
    let mut invalid = plain;
    invalid.tool_errors.insert(1, true);
    assert!(
        adapter
            .prepare_with_result_metadata(&a, &invalid)
            .await
            .is_err()
    );
}

struct UnusedModel;
#[async_trait::async_trait]
impl pi_rs_core::Model for UnusedModel {
    async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<pi_rs_core::ModelStream> {
        panic!("provider must handle the request")
    }
}
struct ImageTool(std::sync::Arc<std::sync::atomic::AtomicUsize>);
#[async_trait::async_trait]
impl pi_rs_core::Tool for ImageTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "read".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: pi_rs_core::ToolContext) -> pi_rs_core::Outcome {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        pi_rs_core::Outcome::Failed("failure".into())
    }
}
struct ImageBlocks(std::sync::atomic::AtomicBool, bool, bool);
#[async_trait::async_trait]
impl pi_rs_durable::extension::Hook for ImageBlocks {
    fn supports_runtime_tool_blocks(&self) -> bool {
        true
    }
    async fn after_tool_result(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        result: &pi_rs_durable::extension::ToolResult,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolResult>, String> {
        if !self.2 {
            return Ok(None);
        }
        let mut result = result.clone();
        result.is_error = false;
        Ok(Some(result))
    }
    async fn after_tool_blocks(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        result: &pi_rs_durable::extension::ToolBlockResult,
        _: bool,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolBlockResult>, String> {
        let mut result = result.clone();
        if !self.1 {
            result.output = vec![
                json!({"type":"text","text":"first"}),
                json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
                json!({"type":"text","text":"last"}),
            ];
        }
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
async fn failed_tool_result_reopens_and_reaches_google_http() {
    use pi_rs_durable::{
        extension::Extension,
        recovery::{ReplayPolicy, UncertainPolicy},
        runtime::{Config, Harness, Options, RegisteredTool},
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    for (plain, images_allowed, missing_tool, flip) in [
        (false, true, false, false),
        (true, true, false, false),
        (false, false, false, false),
        (true, true, true, false),
        (false, true, false, true),
    ] {
        let first = Box::leak(sse(r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"read","id":"c1","args":{}}}]},"finishReason":"STOP"}]}"#).into_boxed_str());
        let (url, server1) = server(first, false).await;
        let path = std::env::temp_dir().join(format!("pi-google-block-{}", Uuid::new_v4()));
        let calls = Arc::new(AtomicUsize::new(0));
        let options = || Options {
            model: Arc::new(UnusedModel),
            tools: if missing_tool {
                vec![]
            } else {
                vec![RegisteredTool {
                    tool: Arc::new(ImageTool(calls.clone())),
                    replay: ReplayPolicy::Unsafe,
                }]
            },
            restart_limit: 3,
            max_turns: 3,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        };
        let hook = Arc::new(ImageBlocks(AtomicBool::new(true), plain, flip));
        let extension = || Extension {
            name: "blocks".into(),
            hooks: vec![hook.clone()],
            ..Default::default()
        };
        let adapter = |endpoint: &str| {
            let mut adapter = GoogleAdapter::new(endpoint, "secret-key").unwrap();
            if !images_allowed {
                adapter
                    .set_model_descriptor("gemini-3-flash", &json!({"input":["text"]}))
                    .unwrap();
            }
            Arc::new(adapter)
        };
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.install_provider_adapter(adapter(&url)).unwrap();
        h.install_extension(extension()).unwrap();
        let id = h
            .create_conversation(Config {
                model: "gemini-3-flash".into(),
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
        server1.await.unwrap();
        drop(h);
        hook.0.store(false, Ordering::SeqCst);
        let final_response = Box::leak(
            sse(
                r#"{"candidates":[{"content":{"parts":[{"text":"done"}]},"finishReason":"STOP"}]}"#,
            )
            .into_boxed_str(),
        );
        let (url, server2) = server(final_response, false).await;
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.install_provider_adapter(adapter(&url)).unwrap();
        h.install_extension(extension()).unwrap();
        let before = h.conversation(id).unwrap().content_blocks;
        let binding = before
            .values()
            .find(|b| b.blocks.len() == if plain { 1 } else { 3 })
            .unwrap();
        let expected_text = if plain {
            binding.blocks[0]["text"].as_str().unwrap().to_owned()
        } else {
            "first\nlast".to_owned()
        };
        let occurrence_id = binding.message_id;
        h.resume_conversation(id, &CancellationToken::new())
            .await
            .unwrap();
        let bytes = server2.await.unwrap();
        let split = bytes.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        let sent: Value = serde_json::from_slice(&bytes[split + 4..]).unwrap();
        let response = sent["contents"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|x| x["parts"].as_array().unwrap())
            .find_map(|x| x.get("functionResponse"))
            .unwrap();
        assert_eq!(
            response["response"][if flip { "output" } else { "error" }],
            expected_text
        );
        if plain || !images_allowed {
            assert!(response.get("parts").is_none());
        } else {
            assert_eq!(response["parts"][0]["inlineData"]["data"], "aGk=");
        }
        let result_metadata = h
            .conversation(id)
            .unwrap()
            .tool_results
            .into_iter()
            .find(|m| m.call_id == "c1")
            .unwrap();
        assert_eq!(result_metadata.is_error, !flip);
        assert_eq!(result_metadata.content_block_id, Some(occurrence_id));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            if missing_tool { 0 } else { 1 }
        );
        assert_eq!(h.conversation(id).unwrap().content_blocks, before);
        let fork = h.fork(id).unwrap();
        let forked = h.conversation(fork).unwrap();
        assert_eq!(forked.content_blocks, before);
        let forked_meta = forked
            .tool_results
            .iter()
            .find(|m| m.call_id == "c1")
            .unwrap();
        assert_eq!(forked_meta.content_block_id, Some(occurrence_id));
        assert_eq!(forked_meta.is_error, !flip);
        drop(h);
        for suffix in ["", "-wal", "-shm", ".owner"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }
}
