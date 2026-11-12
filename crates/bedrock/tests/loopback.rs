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
        assert_eq!(body["system"][0]["text"], "system");
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
                content: "system".into(),
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
                content: "done".into(),
                is_error: false,
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
