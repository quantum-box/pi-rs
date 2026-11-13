use aws_smithy_eventstream::frame::write_message_to;
use aws_smithy_types::event_stream::{Header, HeaderValue, Message as Frame};
use base64::Engine;
use futures_util::StreamExt;
use pi_rs_bedrock::Bedrock;
use pi_rs_core::{ImageContent, Message, Model, ModelEvent, ModelRequest};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
const PROFILE_ARN: &str =
    "arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/opaque";

fn event(kind: &str, body: Value) -> Vec<u8> {
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
    let mut out = Vec::new();
    write_message_to(&frame, &mut out).unwrap();
    out
}

#[tokio::test]
async fn bearer_images_and_reasoning_replay_round_trip() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for round in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut data = Vec::new();
            let mut buf = [0; 8192];
            let (end, len) = loop {
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                data.extend_from_slice(&buf[..n]);
                if let Some(pos) = data.windows(4).position(|x| x == b"\r\n\r\n") {
                    let h = String::from_utf8_lossy(&data[..pos]);
                    let len = h
                        .lines()
                        .find_map(|l| {
                            l.split_once(':')
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
            let headers = String::from_utf8_lossy(&data[..end]).to_ascii_lowercase();
            assert!(headers.contains("authorization: bearer test-token"));
            assert!(!headers.contains("aws4-hmac-sha256"));
            let body: Value = serde_json::from_slice(&data[end..end + len]).unwrap();
            assert_eq!(body["messages"][0]["content"][0]["image"]["format"], "png");
            assert_eq!(
                body["messages"][0]["content"][0]["image"]["source"]["bytes"],
                "aGVsbG8="
            );
            if round == 1 {
                let blocks = &body["messages"][1]["content"];
                assert_eq!(
                    blocks[0]["reasoningContent"]["reasoningText"]["text"],
                    "think"
                );
                assert_eq!(
                    blocks[0]["reasoningContent"]["reasoningText"]["signature"],
                    "sig-nature"
                );
                assert_eq!(blocks[1]["text"], "answer");
                assert_eq!(blocks[2]["reasoningContent"]["redactedContent"], "AQIDBA==");
                assert_eq!(
                    blocks[3]["reasoningContent"]["reasoningText"],
                    json!({"text":"", "signature":"omitted-sig"})
                );
            }
            let frames=if round==0 { vec![
                event("messageStart",json!({"role":"assistant"})),
                event("contentBlockDelta",json!({"contentBlockIndex":0,"delta":{"reasoningContent":{"text":"think"}}})),
                event("contentBlockDelta",json!({"contentBlockIndex":0,"delta":{"reasoningContent":{"signature":"sig-"}}})),
                event("contentBlockDelta",json!({"contentBlockIndex":0,"delta":{"reasoningContent":{"signature":"nature"}}})),
                event("contentBlockStop",json!({"contentBlockIndex":0})),
                event("contentBlockDelta",json!({"contentBlockIndex":1,"delta":{"text":"answer"}})),
                event("contentBlockStop",json!({"contentBlockIndex":1})),
                event("contentBlockDelta",json!({"contentBlockIndex":2,"delta":{"reasoningContent":{"redactedContent":"AQI="}}})),
                event("contentBlockDelta",json!({"contentBlockIndex":2,"delta":{"reasoningContent":{"redactedContent":"AwQ="}}})),
                event("contentBlockStop",json!({"contentBlockIndex":2})),
                event("contentBlockDelta",json!({"contentBlockIndex":3,"delta":{"reasoningContent":{"signature":"omitted-sig"}}})),
                event("contentBlockStop",json!({"contentBlockIndex":3})),
                event("messageStop",json!({"stopReason":"end_turn"})),
            ] } else {vec![event("messageStop",json!({"stopReason":"end_turn"}))]}.concat();
            socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/vnd.amazon.eventstream\r\ncontent-length: {}\r\n\r\n",frames.len()).as_bytes()).await.unwrap();
            socket.write_all(&frames).await.unwrap();
        }
    });
    let conf = aws_sdk_bedrockruntime::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_bedrockruntime::config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "should-not-sign",
            "secret",
            None,
            None,
            "test",
        ))
        .endpoint_url(format!("http://{addr}"))
        .build();
    let model = Bedrock::new(aws_sdk_bedrockruntime::Client::from_conf(conf))
        .with_bearer_token("test-token".into())
        .unwrap()
        .with_model_name(PROFILE_ARN, Some("Claude Sonnet"));
    let image = ImageContent {
        data: "aGVsbG8=".into(),
        mime_type: "image/png".into(),
    };
    let user = Message::UserWithImages {
        content: String::new(),
        images: vec![image],
    };
    let request = |messages| ModelRequest {
        model: PROFILE_ARN.into(),
        messages,
        tools: vec![],
        max_output_tokens: 128,
    };
    let events = model
        .stream(request(vec![user.clone()]))
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
            .any(|e| matches!(e,ModelEvent::Reasoning(text) if text=="think"))
    );
    let replay = events
        .into_iter()
        .find_map(|event| match event {
            ModelEvent::Metadata(m) => m.replay,
            _ => None,
        })
        .unwrap();
    let assistant = Message::Assistant {
        content: "answer".into(),
        calls: vec![],
        replay: Some(replay),
    };
    let events = model
        .stream(request(vec![
            user,
            assistant,
            Message::User {
                content: "next".into(),
            },
        ]))
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().all(Result::is_ok));
    server.await.unwrap();
}

#[tokio::test]
async fn claude_error_tool_image_follows_result() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
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
                    .find_map(|l| {
                        l.split_once(':')
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
        let body: Value = serde_json::from_slice(&data[end..end + len]).unwrap();
        let blocks = &body["messages"][2]["content"];
        assert_eq!(blocks[0]["toolResult"]["status"], "error");
        assert_eq!(
            blocks[0]["toolResult"]["content"].as_array().unwrap().len(),
            1
        );
        assert_eq!(blocks[1]["toolResult"]["status"], "error");
        assert!(blocks[2]["text"].as_str().unwrap().contains("Images"));
        assert_eq!(blocks[3]["image"]["source"]["bytes"], "aGVsbG8=");
        assert!(blocks[4]["text"].as_str().unwrap().contains("Images"));
        assert_eq!(blocks[5]["image"]["source"]["bytes"], "aGVsbG8=");
        let frames = event("messageStop", json!({"stopReason":"end_turn"}));
        socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/vnd.amazon.eventstream\r\ncontent-length: {}\r\n\r\n",frames.len()).as_bytes()).await.unwrap();
        socket.write_all(&frames).await.unwrap();
    });
    let config = aws_sdk_bedrockruntime::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_bedrockruntime::config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "secret", None, None, "test",
        ))
        .endpoint_url(format!("http://{addr}"))
        .build();
    let model = Bedrock::new(aws_sdk_bedrockruntime::Client::from_conf(config))
        .with_model_name(PROFILE_ARN, Some("Claude Sonnet"));
    let image_result =
        json!({"data":{"content":[{"type":"image","mimeType":"image/png","data":"aGVsbG8="}]}})
            .to_string();
    let request = ModelRequest {
        model: PROFILE_ARN.into(),
        max_output_tokens: 128,
        tools: vec![pi_rs_core::ToolSpec {
            name: "lookup".into(),
            description: "lookup".into(),
            parameters: json!({"type":"object"}),
        }],
        messages: vec![
            Message::User {
                content: "hi".into(),
            },
            Message::Assistant {
                content: String::new(),
                calls: vec![
                    pi_rs_core::ToolCall {
                        id: "id".into(),
                        name: "lookup".into(),
                        arguments: "{}".into(),
                    },
                    pi_rs_core::ToolCall {
                        id: "id2".into(),
                        name: "lookup".into(),
                        arguments: "{}".into(),
                    },
                ],
                replay: None,
            },
            Message::Tool {
                call_id: "id".into(),
                content: image_result.clone(),
                is_error: true,
                is_structured: Some(true),
            },
            Message::Tool {
                call_id: "id2".into(),
                content: image_result,
                is_error: true,
                is_structured: Some(true),
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

#[tokio::test]
async fn summary_sends_structured_images_as_images_not_base64_text() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let data = base64::engine::general_purpose::STANDARD.encode(vec![42; 100_000]);
    let expected = data.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut body = Vec::new();
        let mut buf = [0; 8192];
        let (end, len) = loop {
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            body.extend_from_slice(&buf[..n]);
            if let Some(pos) = body.windows(4).position(|x| x == b"\r\n\r\n") {
                let h = String::from_utf8_lossy(&body[..pos]);
                let len = h
                    .lines()
                    .find_map(|l| {
                        l.split_once(':')
                            .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                            .map(|(_, v)| v.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                break (pos + 4, len);
            }
        };
        while body.len() < end + len {
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            body.extend_from_slice(&buf[..n]);
        }
        let request: Value = serde_json::from_slice(&body[end..end + len]).unwrap();
        assert!(request.get("toolConfig").is_none());
        let blocks = request["messages"][2]["content"].as_array().unwrap();
        let text = blocks[0]["text"].as_str().unwrap();
        assert!(text.contains("[image attached]"));
        assert!(!text.contains(&expected));
        assert_eq!(blocks[1]["image"]["source"]["bytes"], expected);
        let frames = event("messageStop", json!({"stopReason":"end_turn"}));
        socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/vnd.amazon.eventstream\r\ncontent-length: {}\r\n\r\n",frames.len()).as_bytes()).await.unwrap();
        socket.write_all(&frames).await.unwrap();
    });
    let conf = aws_sdk_bedrockruntime::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_bedrockruntime::config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "secret", None, None, "test",
        ))
        .endpoint_url(format!("http://{addr}"))
        .build();
    let model = Bedrock::new(aws_sdk_bedrockruntime::Client::from_conf(conf));
    let result = Message::Tool {
        call_id: "id".into(),
        content: json!({"data":{"content":[{"type":"image","mimeType":"image/png","data":data}]}})
            .to_string(),
        is_error: false,
        is_structured: Some(true),
    };
    assert!(pi_rs_core::estimate_message_tokens(&result) < 2000);
    let request = ModelRequest {
        model: "test-model".into(),
        tools: vec![],
        max_output_tokens: 128,
        messages: vec![
            Message::User {
                content: "hello".into(),
            },
            Message::Assistant {
                content: String::new(),
                calls: vec![pi_rs_core::ToolCall {
                    id: "id".into(),
                    name: "lookup".into(),
                    arguments: "{}".into(),
                }],
                replay: None,
            },
            result,
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

#[tokio::test]
async fn lazy_clones_keep_authentication_independent() {
    const CHILD: &str = "PI_RS_BEDROCK_CLONE_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        for (first, second) in [("token-a", "token-b"), ("token-b", "token-a")] {
            let original = Bedrock::from_env();
            let first_client = original.clone().with_bearer_token(first.into()).unwrap();
            let second_client = original.clone().with_bearer_token(second.into()).unwrap();
            for (client, model) in [
                (&first_client, first),
                (&original, "original"),
                (&second_client, second),
            ] {
                let request = ModelRequest {
                    model: model.into(),
                    messages: vec![Message::User {
                        content: "hello".into(),
                    }],
                    tools: vec![],
                    max_output_tokens: 16,
                };
                let events = client
                    .stream(request)
                    .await
                    .unwrap()
                    .collect::<Vec<_>>()
                    .await;
                assert!(events.iter().all(Result::is_ok));
            }
        }
        return;
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "lazy_clones_keep_authentication_independent"])
        .env(CHILD, "1")
        .env("AWS_REGION", "us-east-1")
        .env("AWS_DEFAULT_REGION", "us-east-1")
        .env("AWS_ACCESS_KEY_ID", "SYNTHETIC_ACCESS_KEY")
        .env("AWS_SECRET_ACCESS_KEY", "synthetic-secret")
        .env("AWS_ENDPOINT_URL_BEDROCK_RUNTIME", format!("http://{addr}"))
        .env("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env_remove("AWS_BEARER_TOKEN_BEDROCK")
        .env_remove("AWS_SESSION_TOKEN")
        .env_remove("AWS_PROFILE")
        .spawn()
        .unwrap();
    let server = async {
        let mut observed = Vec::new();
        for _ in 0..6 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut data = Vec::new();
            let mut buf = [0; 4096];
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
                                .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                                .map(|(_, value)| value.trim().parse::<usize>().unwrap())
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
            let headers = String::from_utf8_lossy(&data[..end]).to_ascii_lowercase();
            let path = headers
                .lines()
                .next()
                .unwrap()
                .split_whitespace()
                .nth(1)
                .unwrap()
                .to_owned();
            let authorization = headers
                .lines()
                .find(|line| line.starts_with("authorization:"))
                .unwrap()
                .to_owned();
            observed.push((path, authorization));
            let frames = event("messageStop", json!({"stopReason":"end_turn"}));
            socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/vnd.amazon.eventstream\r\ncontent-length: {}\r\n\r\n",frames.len()).as_bytes()).await.unwrap();
            socket.write_all(&frames).await.unwrap();
        }
        observed
    };
    let observed = match tokio::time::timeout(std::time::Duration::from_secs(20), server).await {
        Ok(result) => result,
        Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child did not complete loopback requests")
        }
    };
    assert!(child.wait().unwrap().success());
    for (path, authorization) in observed {
        if path.contains("/original/") {
            assert!(authorization.starts_with("authorization: aws4-hmac-sha256"));
            assert!(authorization.contains("synthetic_access_key"));
        } else if path.contains("/token-a/") {
            assert_eq!(authorization, "authorization: bearer token-a");
        } else if path.contains("/token-b/") {
            assert_eq!(authorization, "authorization: bearer token-b");
        } else {
            panic!("unexpected request path {path}");
        }
    }
}
