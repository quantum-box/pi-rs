use aws_smithy_eventstream::frame::write_message_to;
use aws_smithy_types::event_stream::{Header, HeaderValue, Message as Frame};
use futures_util::StreamExt;
use pi_rs_bedrock::Bedrock;
use pi_rs_core::{Message, Model, ModelEvent, ModelRequest, ToolSpec};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn event(kind: &str, body: serde_json::Value) -> Vec<u8> {
    let frame = Frame::new(bytes::Bytes::from(body.to_string()))
        .add_header(Header::new(
            ":message-type",
            HeaderValue::String("event".into()),
        ))
        .add_header(Header::new(
            ":event-type",
            HeaderValue::String(kind.to_owned().into()),
        ))
        .add_header(Header::new(
            ":content-type",
            HeaderValue::String("application/json".into()),
        ));
    let mut bytes = Vec::new();
    write_message_to(&frame, &mut bytes).unwrap();
    bytes
}

#[tokio::test]
async fn converse_stream_sends_signed_request_and_collects_tool_and_usage() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut data = Vec::new();
        let mut buffer = [0; 4096];
        let (header_end, length) = loop {
            let n = socket.read(&mut buffer).await.unwrap();
            assert!(n > 0);
            data.extend_from_slice(&buffer[..n]);
            if let Some(pos) = data.windows(4).position(|x| x == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&data[..pos]);
                let len = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                            .map(|(_, v)| v.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                break (pos + 4, len);
            }
        };
        while data.len() < header_end + length {
            let n = socket.read(&mut buffer).await.unwrap();
            assert!(n > 0);
            data.extend_from_slice(&buffer[..n]);
        }
        let headers = String::from_utf8_lossy(&data[..header_end]);
        assert!(headers.starts_with("POST /model/test-model/converse-stream "));
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("authorization: aws4-hmac-sha256")
        );
        assert!(headers.contains("Credential=PROFILEKEY/"));
        let body: serde_json::Value =
            serde_json::from_slice(&data[header_end..header_end + length]).unwrap();
        assert_eq!(body["inferenceConfig"]["maxTokens"], 128);
        assert_eq!(body["system"], json!([{"text":"system"}]));
        assert_eq!(
            body["messages"][1]["content"][0]["toolUse"]["input"]["q"],
            "x"
        );
        assert_eq!(
            body["messages"][2]["content"][0]["toolResult"]["status"],
            "error"
        );
        assert_eq!(body["messages"][0]["content"][0]["text"], "<empty>");
        let calls = &body["messages"][1]["content"];
        assert_eq!(calls[2]["toolUse"]["input"], json!({}));
        assert_eq!(calls[3]["toolUse"]["input"], json!({"path":r"C:\work"}));
        assert_eq!(calls[4]["toolUse"]["input"], json!({}));
        assert_eq!(calls[5]["toolUse"]["input"], json!({}));
        let id0 = calls[0]["toolUse"]["toolUseId"].as_str().unwrap();
        let id1 = calls[1]["toolUse"]["toolUseId"].as_str().unwrap();
        assert_ne!(id0, id1);
        assert!(id0.len() <= 64 && id1.len() <= 64);
        assert!(
            id0.bytes()
                .chain(id1.bytes())
                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
        );
        let results = &body["messages"][2]["content"];
        assert_eq!(results[0]["toolResult"]["toolUseId"], id0);
        assert_eq!(results[1]["toolResult"]["toolUseId"], id1);
        assert_eq!(results[0]["toolResult"]["content"][0]["text"], "<empty>");
        assert_eq!(
            results[1]["toolResult"]["content"][1]["image"]["source"]["bytes"],
            "aGVsbG8="
        );
        assert_eq!(
            body["toolConfig"]["tools"][0]["toolSpec"]["inputSchema"]["json"]["minimum"],
            -1
        );
        let frames = [
            event("messageStart",json!({"role":"assistant"})),
            event("contentBlockDelta",json!({"contentBlockIndex":0,"delta":{"text":"Hi"}})),
            event("contentBlockStop",json!({"contentBlockIndex":0})),
            event("contentBlockStart",json!({"contentBlockIndex":1,"start":{"toolUse":{"toolUseId":"call1","name":"lookup"}}})),
            event("contentBlockDelta",json!({"contentBlockIndex":1,"delta":{"toolUse":{"input":"{\"x\":"}}})),
            event("contentBlockDelta",json!({"contentBlockIndex":1,"delta":{"toolUse":{"input":"1}"}}})),
            event("contentBlockStop",json!({"contentBlockIndex":1})),
            event("messageStop",json!({"stopReason":"tool_use"})),
            event("metadata",json!({"usage":{"inputTokens":12,"outputTokens":4,"totalTokens":16,"cacheReadInputTokens":3,"cacheWriteInputTokens":2},"metrics":{"latencyMs":10}})),
        ].concat();
        socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/vnd.amazon.eventstream\r\ncontent-length: {}\r\n\r\n",frames.len()).as_bytes()).await.unwrap();
        socket.write_all(&frames).await.unwrap();
    });
    #[allow(deprecated)]
    let files = aws_config::profile::profile_file::ProfileFiles::builder()
        .with_contents(
            aws_config::profile::profile_file::ProfileFileKind::Credentials,
            "[selected]\naws_access_key_id = PROFILEKEY\naws_secret_access_key = PROFILESECRET\n",
        )
        .build();
    let provider = aws_config::profile::ProfileFileCredentialsProvider::builder()
        .profile_name("selected")
        .profile_files(files)
        .build();
    let conf = aws_sdk_bedrockruntime::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_bedrockruntime::config::Region::new("us-east-1"))
        .credentials_provider(provider)
        .endpoint_url(format!("http://{address}"))
        .build();
    let model = Bedrock::new(aws_sdk_bedrockruntime::Client::from_conf(conf));
    let request = ModelRequest {
        model: "test-model".into(),
        messages: vec![
            Message::System {
                content: " \n".into(),
            },
            Message::System {
                content: "system".into(),
            },
            Message::System {
                content: "\t".into(),
            },
            Message::User {
                content: "  ".into(),
            },
            Message::Assistant {
                content: "  ".into(),
                calls: vec![
                    pi_rs_core::ToolCall {
                        id: format!("{}!", "a".repeat(70)),
                        name: "lookup".into(),
                        arguments: "{\"q\":\"x\"}".into(),
                    },
                    pi_rs_core::ToolCall {
                        id: format!("{}?", "a".repeat(70)),
                        name: "lookup".into(),
                        arguments: "{}".into(),
                    },
                    pi_rs_core::ToolCall {
                        id: "empty".into(),
                        name: "lookup".into(),
                        arguments: "  ".into(),
                    },
                    pi_rs_core::ToolCall {
                        id: "repair".into(),
                        name: "lookup".into(),
                        arguments: r#"{"path":"C:\work"}"#.into(),
                    },
                    pi_rs_core::ToolCall {
                        id: "invalid".into(),
                        name: "lookup".into(),
                        arguments: "{broken".into(),
                    },
                    pi_rs_core::ToolCall {
                        id: "array".into(),
                        name: "lookup".into(),
                        arguments: "[1]".into(),
                    },
                ],
                replay: None,
            },
            Message::Tool {
                call_id: format!("{}!", "a".repeat(70)),
                content: "  ".into(),
                is_error: true,
                is_structured: Some(false),
            },
            Message::Tool {
                call_id: format!("{}?", "a".repeat(70)),
                content: json!({"data":{"content":[{"type":"image","mimeType":"image/png","data":"aGVsbG8="}]}}).to_string(),
                is_error: false,
                is_structured: Some(true),
            },
            Message::Tool {
                call_id: "empty".into(),
                content: "empty result".into(),
                is_error: true,
                is_structured: Some(false),
            },
            Message::Tool {
                call_id: "repair".into(),
                content: "repaired result".into(),
                is_error: false,
                is_structured: Some(false),
            },
            Message::Tool {
                call_id: "invalid".into(),
                content: "invalid result".into(),
                is_error: true,
                is_structured: Some(false),
            },
            Message::Tool {
                call_id: "array".into(),
                content: "array result".into(),
                is_error: true,
                is_structured: Some(false),
            },
        ],
        tools: vec![ToolSpec {
            name: "lookup".into(),
            description: "Lookup".into(),
            parameters: json!({"type":"object","minimum":-1}),
        }],
        max_output_tokens: 128,
    };
    let events = model
        .stream(request)
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e,ModelEvent::Text(t) if t=="Hi"))
    );
    assert!(events.iter().any(|e| matches!(e,ModelEvent::Complete {calls,usage:Some(u)} if calls.len()==1 && calls[0].arguments=="{\"x\":1}" && u.input_tokens==17 && u.output_tokens==4)));
    assert!(events.iter().any(|e| matches!(e, ModelEvent::Metadata(m) if m.cached_input_tokens == Some(3) && m.cache_creation_input_tokens == Some(2) && m.finish_reason.as_deref() == Some("tool_calls"))));
    server.await.unwrap();
}

#[tokio::test]
async fn truncated_tool_block_never_completes() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0; 8192];
        let _ = socket.read(&mut buffer).await.unwrap();
        let frames=[
            event("messageStart",json!({"role":"assistant"})),
            event("contentBlockStart",json!({"contentBlockIndex":0,"start":{"toolUse":{"toolUseId":"call","name":"lookup"}}})),
            event("contentBlockDelta",json!({"contentBlockIndex":0,"delta":{"toolUse":{"input":"{"}}})),
            event("messageStop",json!({"stopReason":"max_tokens"})),
        ].concat();
        socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/vnd.amazon.eventstream\r\ncontent-length: {}\r\n\r\n",frames.len()).as_bytes()).await.unwrap();
        socket.write_all(&frames).await.unwrap();
    });
    let conf = aws_sdk_bedrockruntime::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_bedrockruntime::config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "secret", None, None, "test",
        ))
        .endpoint_url(format!("http://{address}"))
        .build();
    let model = Bedrock::new(aws_sdk_bedrockruntime::Client::from_conf(conf));
    let request = ModelRequest {
        model: "test-model".into(),
        messages: vec![
            Message::User {
                content: "  ".into(),
            },
            Message::Assistant {
                content: " ".into(),
                calls: vec![
                    pi_rs_core::ToolCall {
                        id: format!("{}!", "a".repeat(70)),
                        name: "lookup".into(),
                        arguments: "{}".into(),
                    },
                    pi_rs_core::ToolCall {
                        id: format!("{}?", "a".repeat(70)),
                        name: "lookup".into(),
                        arguments: "{}".into(),
                    },
                ],
                replay: None,
            },
            Message::Tool {
                call_id: format!("{}!", "a".repeat(70)),
                content: " ".into(),
                is_error: false,
                is_structured: Some(false),
            },
            Message::Tool {
                call_id: format!("{}?", "a".repeat(70)),
                content: "done".into(),
                is_error: false,
                is_structured: Some(false),
            },
        ],
        tools: vec![],
        max_output_tokens: 128,
    };
    let events = model
        .stream(request)
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().any(|e| e.is_err()));
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Ok(ModelEvent::Complete { .. })))
    );
    server.await.unwrap();
}

#[tokio::test]
async fn service_errors_include_safe_diagnostics_and_classify_stream_validation() {
    for streaming in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 8192];
            let _ = socket.read(&mut buffer).await.unwrap();
            if streaming {
                let frame = Frame::new(bytes::Bytes::from_static(br#"{"message":"bad input"}"#))
                    .add_header(Header::new(
                        ":message-type",
                        HeaderValue::String("exception".into()),
                    ))
                    .add_header(Header::new(
                        ":exception-type",
                        HeaderValue::String("validationException".into()),
                    ))
                    .add_header(Header::new(
                        ":content-type",
                        HeaderValue::String("application/json".into()),
                    ));
                let mut bytes = Vec::new();
                write_message_to(&frame, &mut bytes).unwrap();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nx-amzn-requestid: stream-request\r\ncontent-type: application/vnd.amazon.eventstream\r\ncontent-length: {}\r\n\r\n",bytes.len()).as_bytes()).await.unwrap();
                socket.write_all(&bytes).await.unwrap();
            } else {
                let body = br#"{"message":"bad input"}"#;
                socket.write_all(format!("HTTP/1.1 400 Bad Request\r\nx-amzn-requestid: request-123\r\nx-amzn-errortype: ValidationException\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",body.len()).as_bytes()).await.unwrap();
                socket.write_all(body).await.unwrap();
            }
        });
        let config = aws_sdk_bedrockruntime::Config::builder()
            .behavior_version_latest()
            .region(aws_sdk_bedrockruntime::config::Region::new("us-east-1"))
            .credentials_provider(aws_credential_types::Credentials::new(
                "test", "secret", None, None, "test",
            ))
            .endpoint_url(format!("http://{address}"))
            .build();
        let model = Bedrock::new(aws_sdk_bedrockruntime::Client::from_conf(config));
        let request = ModelRequest {
            model: "test-model".into(),
            messages: vec![Message::User {
                content: "hi".into(),
            }],
            tools: vec![],
            max_output_tokens: 16,
        };
        let error = match model.stream(request).await {
            Ok(mut stream) => stream.next().await.unwrap().unwrap_err(),
            Err(error) => error,
        };
        assert!(matches!(error, pi_rs_core::Error::Model(_)), "{error:?}");
        let message = error.to_string();
        assert!(
            message.contains("ValidationException") || message.contains("validationException"),
            "{message}"
        );
        assert!(message.contains("bad input"), "{message}");
        assert!(
            message.contains(if streaming {
                "stream-request"
            } else {
                "request-123"
            }),
            "{message}"
        );
        server.await.unwrap();
    }
}

#[tokio::test]
async fn tool_free_summary_keeps_history_as_text_without_tool_config() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut data = Vec::new();
        let mut buf = [0; 8192];
        let (end, len) = loop {
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            data.extend_from_slice(&buf[..n]);
            if let Some(pos) = data.windows(4).position(|x| x == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&data[..pos]);
                let len = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                            .map(|(_, v)| v.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                break (pos + 4, len);
            }
        };
        while data.len() < end + len {
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            data.extend_from_slice(&buf[..n]);
        }
        let body: serde_json::Value = serde_json::from_slice(&data[end..end + len]).unwrap();
        assert!(body.get("toolConfig").is_none());
        let messages = body["messages"].as_array().unwrap();
        assert!(
            messages
                .iter()
                .flat_map(|m| m["content"].as_array().unwrap())
                .all(|b| b.get("text").is_some())
        );
        let wire = body.to_string();
        assert!(wire.contains("Historical tool call"));
        assert!(wire.contains("Historical tool result"));
        let result = messages
            .iter()
            .flat_map(|m| m["content"].as_array().unwrap())
            .find_map(|b| {
                b["text"]
                    .as_str()
                    .filter(|text| text.starts_with("Historical tool result"))
            })
            .unwrap();
        assert_eq!(
            result,
            format!(
                "Historical tool result (id; error):\n{}",
                "quote \" and slash \\ and newline\n".repeat(1000)
            )
        );
        assert!(wire.contains("quote"));
        assert!(!wire.contains("toolUse"));
        assert!(!wire.contains("toolResult"));
        let frames = [event("messageStop", json!({"stopReason":"end_turn"}))].concat();
        socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/vnd.amazon.eventstream\r\ncontent-length: {}\r\n\r\n",frames.len()).as_bytes()).await.unwrap();
        socket.write_all(&frames).await.unwrap();
    });
    let conf = aws_sdk_bedrockruntime::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_bedrockruntime::config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "secret", None, None, "test",
        ))
        .endpoint_url(format!("http://{address}"))
        .build();
    let model = Bedrock::new(aws_sdk_bedrockruntime::Client::from_conf(conf));
    let request = ModelRequest {
        model: "test-model".into(),
        tools: vec![],
        max_output_tokens: 128,
        messages: vec![
            Message::User {
                content: "start".into(),
            },
            Message::Assistant {
                content: String::new(),
                calls: vec![pi_rs_core::ToolCall {
                    id: "id".into(),
                    name: "shell".into(),
                    arguments: "{bad json".into(),
                }],
                replay: None,
            },
            Message::Tool {
                call_id: "id".into(),
                content: "quote \" and slash \\ and newline\n".repeat(1000),
                is_error: true,
                is_structured: Some(false),
            },
            Message::User {
                content: "Summarize".into(),
            },
        ],
    };
    let events = model
        .stream(request)
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().all(Result::is_ok));
    server.await.unwrap();
}

#[derive(Debug)]
struct MissingCredentials;
impl aws_credential_types::provider::ProvideCredentials for MissingCredentials {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::ready(Err(
            aws_credential_types::provider::error::CredentialsError::not_loaded(
                "sensitive-test-detail",
            ),
        ))
    }
}

#[tokio::test]
async fn missing_region_and_credentials_are_actionable_but_connection_failures_retry() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let request = || ModelRequest {
        model: "test-model".into(),
        messages: vec![Message::User {
            content: "hello".into(),
        }],
        tools: vec![],
        max_output_tokens: 128,
    };
    let base = || {
        aws_sdk_bedrockruntime::Config::builder()
            .behavior_version_latest()
            .endpoint_url(format!("http://{address}"))
            .retry_config(aws_sdk_bedrockruntime::config::retry::RetryConfig::disabled())
    };
    let credentials =
        || aws_credential_types::Credentials::new("test", "secret", None, None, "test");
    let model = Bedrock::new(aws_sdk_bedrockruntime::Client::from_conf(
        base().credentials_provider(credentials()).build(),
    ));
    let error = match model.stream(request()).await {
        Err(e) => e,
        Ok(_) => panic!("missing region accepted"),
    };
    assert!(matches!(error, pi_rs_core::Error::Config(_)));
    assert!(error.to_string().contains("AWS_REGION"));
    let model = Bedrock::new(aws_sdk_bedrockruntime::Client::from_conf(
        base()
            .region(aws_sdk_bedrockruntime::config::Region::new("us-east-1"))
            .credentials_provider(MissingCredentials)
            .build(),
    ));
    let error = match model.stream(request()).await {
        Err(e) => e,
        Ok(_) => panic!("missing credentials accepted"),
    };
    assert!(matches!(error, pi_rs_core::Error::Config(_)), "{error:?}");
    assert!(error.to_string().contains("AWS_PROFILE"));
    assert!(!error.to_string().contains("sensitive-test-detail"));
    let model = Bedrock::new(aws_sdk_bedrockruntime::Client::from_conf(
        base()
            .region(aws_sdk_bedrockruntime::config::Region::new("us-east-1"))
            .credentials_provider(credentials())
            .build(),
    ));
    let error = match model.stream(request()).await {
        Err(e) => e,
        Ok(_) => panic!("closed port accepted"),
    };
    assert!(
        matches!(error, pi_rs_core::Error::RetryableModel(_)),
        "{error:?}"
    );
}

#[cfg(any(unix, windows))]
#[tokio::test]
async fn inspectable_sdk_credentials_and_bearer_reuse_measured_capacity() {
    use aws_credential_types::provider::SharedCredentialsProvider;
    for bearer in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut data = Vec::new();
            let mut buffer = [0; 4096];
            let (header_end, length) = loop {
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                data.extend_from_slice(&buffer[..count]);
                if let Some(pos) = data.windows(4).position(|part| part == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&data[..pos]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.split_once(':')
                                .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                                .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                        })
                        .unwrap();
                    break (pos + 4, length);
                }
            };
            while data.len() < header_end + length {
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                data.extend_from_slice(&buffer[..count]);
            }
            let headers = String::from_utf8_lossy(&data[..header_end]).to_ascii_lowercase();
            if bearer {
                assert!(headers.contains("authorization: bearer private-token"));
            } else {
                assert!(headers.contains("authorization: aws4-hmac-sha256"));
            }
            let frames = [
                event("messageStart", json!({"role":"assistant"})),
                event("contentBlockDelta", json!({"contentBlockIndex":0,"delta":{"text":"done"}})),
                event("contentBlockStop", json!({"contentBlockIndex":0})),
                event("messageStop", json!({"stopReason":"end_turn"})),
                event("metadata", json!({"usage":{"inputTokens":20,"outputTokens":8,"totalTokens":28,"cacheReadInputTokens":2000,"cacheWriteInputTokens":1000},"metrics":{"latencyMs":1}})),
            ].concat();
            socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/vnd.amazon.eventstream\r\ncontent-length: {}\r\n\r\n", frames.len()).as_bytes()).await.unwrap();
            socket.write_all(&frames).await.unwrap();
        });
        let credentials = aws_credential_types::Credentials::new(
            "access",
            "private-secret",
            None,
            None,
            "fixture",
        );
        let config = aws_types::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(aws_types::region::Region::new("us-east-1"))
            .endpoint_url(format!("http://{address}"))
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .build();
        let mut model = Bedrock::from_sdk_config(config);
        if bearer {
            model = model.with_bearer_token("private-token".into()).unwrap();
        }
        let registry = pi_rs_core::Registry::default();
        let mut agent = pi_rs_core::Agent {
            registry: &registry,
            system_prompt: "short",
            model: &model,
            model_name: "fixture",
            limits: pi_rs_core::Limits::default(),
        };
        agent.limits.context_window = Some(5000);
        agent.limits.output_tokens_per_request = 64;
        let mut session =
            pi_rs_core::Session::with_store(pi_rs_core::MemoryStore::default()).unwrap();
        session.auto_compaction_control().set_enabled(false);
        let (_, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
        agent
            .run(
                &mut session,
                Some("first".into()),
                &mut inputs,
                &pi_rs_core::CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        server.await.unwrap();
        let journal = serde_json::to_string(&session.records()).unwrap();
        assert!(!journal.contains("private-secret") && !journal.contains("private-token"));
        agent.limits.context_window = Some(1000);
        let result = agent
            .run(
                &mut session,
                Some("next".into()),
                &mut inputs,
                &pi_rs_core::CancellationToken::new(),
                &mut |_| {},
            )
            .await;
        assert!(
            matches!(result, Err(pi_rs_core::Error::Model(message)) if message.contains("automatic compaction is disabled")),
            "bearer={bearer}"
        );
    }
}

#[cfg(any(unix, windows))]
#[tokio::test]
async fn prepared_bedrock_dispatch_uses_the_credential_it_fingerprinted() {
    use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider, future};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[derive(Debug)]
    struct Rotating(Arc<AtomicUsize>);
    impl ProvideCredentials for Rotating {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            let first = self.0.fetch_add(1, Ordering::SeqCst) == 0;
            future::ProvideCredentials::ready(Ok(aws_credential_types::Credentials::new(
                if first { "FIRSTKEY" } else { "SECONDKEY" },
                "secret",
                None,
                None,
                "fixture",
            )))
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut data = Vec::new();
        let mut buffer = [0; 4096];
        let (header_end, length) = loop {
            let count = socket.read(&mut buffer).await.unwrap();
            assert!(count > 0);
            data.extend_from_slice(&buffer[..count]);
            if let Some(pos) = data.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&data[..pos]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                            .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                    })
                    .unwrap();
                break (pos + 4, length);
            }
        };
        while data.len() < header_end + length {
            let count = socket.read(&mut buffer).await.unwrap();
            assert!(count > 0);
            data.extend_from_slice(&buffer[..count]);
        }
        assert!(String::from_utf8_lossy(&data[..header_end]).contains("Credential=FIRSTKEY/"));
        let frames = [
            event("messageStart", json!({"role":"assistant"})),
            event(
                "contentBlockDelta",
                json!({"contentBlockIndex":0,"delta":{"text":"done"}}),
            ),
            event("contentBlockStop", json!({"contentBlockIndex":0})),
            event("messageStop", json!({"stopReason":"end_turn"})),
        ]
        .concat();
        socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/vnd.amazon.eventstream\r\ncontent-length: {}\r\n\r\n", frames.len()).as_bytes()).await.unwrap();
        socket.write_all(&frames).await.unwrap();
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let config = aws_types::SdkConfig::builder()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .region(aws_types::region::Region::new("us-east-1"))
        .endpoint_url(format!("http://{address}"))
        .credentials_provider(SharedCredentialsProvider::new(Rotating(calls.clone())))
        .build();
    let prepared = Bedrock::from_sdk_config(config)
        .prepare_bedrock()
        .await
        .unwrap()
        .unwrap();
    assert!(prepared.context_identity().is_some());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let request = ModelRequest {
        model: "fixture".into(),
        messages: vec![Message::User {
            content: "go".into(),
        }],
        tools: vec![],
        max_output_tokens: 64,
    };
    let mut response = prepared.stream(request).await.unwrap();
    while let Some(event) = response.next().await {
        event.unwrap();
    }
    server.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
