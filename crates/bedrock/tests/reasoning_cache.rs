use aws_smithy_eventstream::frame::write_message_to;
use aws_smithy_types::event_stream::{Header, HeaderValue, Message as Frame};
use futures_util::StreamExt;
use pi_rs_bedrock::Bedrock;
use pi_rs_core::{CacheRetention, Message, Model, ModelEvent, ModelRequest, ToolSpec};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

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
    let mut bytes = Vec::new();
    write_message_to(&frame, &mut bytes).unwrap();
    bytes
}
async fn capture<F>(
    region: &str,
    setup: F,
    request: ModelRequest,
    summary: bool,
) -> (Value, Vec<ModelEvent>)
where
    F: FnOnce(Bedrock) -> Bedrock,
{
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
        let body = serde_json::from_slice::<Value>(&data[end..end + len]).unwrap();
        let events=[event("messageStop",json!({"stopReason":"end_turn"})),event("metadata",json!({"usage":{"inputTokens":10,"outputTokens":4,"totalTokens":14,"cacheReadInputTokens":2,"cacheWriteInputTokens":5,"cacheDetails":[{"ttl":"1h","inputTokens":3},{"ttl":"5m","inputTokens":2}]},"metrics":{"latencyMs":1}}))].concat();
        socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/vnd.amazon.eventstream\r\ncontent-length: {}\r\n\r\n",events.len()).as_bytes()).await.unwrap();
        socket.write_all(&events).await.unwrap();
        body
    });
    let config = aws_sdk_bedrockruntime::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_bedrockruntime::config::Region::new(
            region.to_owned(),
        ))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "secret", None, None, "test",
        ))
        .endpoint_url(format!("http://{addr}"))
        .build();
    let model = setup(Bedrock::new(aws_sdk_bedrockruntime::Client::from_conf(
        config,
    )));
    let stream = if summary {
        model
            .stream_for_summary(Uuid::nil(), Uuid::nil(), request)
            .await
            .unwrap()
    } else {
        model.stream(request).await.unwrap()
    };
    let events = stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    (server.await.unwrap(), events)
}
fn request(model: &str, max: u32) -> ModelRequest {
    ModelRequest {
        model: model.into(),
        max_output_tokens: max,
        tools: vec![ToolSpec {
            name: "lookup".into(),
            description: "lookup".into(),
            parameters: json!({"type":"object"}),
        }],
        messages: vec![
            Message::System {
                content: "system".into(),
            },
            Message::User {
                content: "hello".into(),
            },
        ],
    }
}

#[tokio::test]
async fn budgeted_claude_fits_host_limit_and_adds_short_checkpoints() {
    let (body, events) = capture(
        "us-east-1",
        |c| {
            c.with_cache_retention(CacheRetention::Short)
                .with_reasoning(Some("high"), None)
                .unwrap()
        },
        request("anthropic.claude-sonnet-4-5", 4096),
        false,
    )
    .await;
    assert_eq!(body["inferenceConfig"]["maxTokens"], 4096);
    assert_eq!(
        body["additionalModelRequestFields"],
        json!({"thinking":{"type":"enabled","budget_tokens":3072,"display":"summarized"},"anthropic_beta":["interleaved-thinking-2025-05-14"]})
    );
    assert_eq!(body["system"][1], json!({"cachePoint":{"type":"default"}}));
    assert_eq!(
        body["messages"][0]["content"][1],
        json!({"cachePoint":{"type":"default"}})
    );
    assert_eq!(body["toolConfig"]["tools"].as_array().unwrap().len(), 1);
    assert!(events.iter().any(|e|matches!(e,ModelEvent::Metadata(m) if m.cache_write_1h_tokens==Some(3) && m.cache_creation_input_tokens==Some(5))));
}

#[tokio::test]
async fn adaptive_profile_long_cache_and_govcloud_rules() {
    let arn = "arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/opaque";
    let (body, _) = capture(
        "us-east-1",
        |c| {
            c.with_model_name(arn, Some("Claude Opus 4.8"))
                .with_cache_retention(CacheRetention::Long)
                .with_reasoning(Some("xhigh"), None)
                .unwrap()
        },
        request(arn, 512),
        false,
    )
    .await;
    assert_eq!(
        body["additionalModelRequestFields"],
        json!({"thinking":{"type":"adaptive","display":"summarized","block_binding":{"prefix_mismatch_behavior":"drop_block"}},"output_config":{"effort":"xhigh"},"anthropic_beta":["thinking-binding-controls-2026-08-01"]})
    );
    assert_eq!(
        body["system"][1]["cachePoint"],
        json!({"type":"default","ttl":"1h"})
    );
    assert_eq!(
        body["messages"][0]["content"][1]["cachePoint"],
        json!({"type":"default","ttl":"1h"})
    );
    let (mapped, _) = capture(
        "us-east-1",
        |c| {
            c.with_model_name(arn, Some("Claude Opus 4.8"))
                .with_reasoning(Some("xhigh"), Some("high"))
                .unwrap()
        },
        request(arn, 512),
        false,
    )
    .await;
    assert_eq!(
        mapped["additionalModelRequestFields"]["output_config"]["effort"],
        "high"
    );
    let (gov, _) = capture(
        "us-gov-west-1",
        |c| {
            c.with_cache_retention(CacheRetention::None)
                .with_reasoning(Some("medium"), None)
                .unwrap()
        },
        request("us-gov.anthropic.claude-opus-4-8", 512),
        false,
    )
    .await;
    assert_eq!(
        gov["additionalModelRequestFields"],
        json!({"thinking":{"type":"adaptive"},"output_config":{"effort":"medium"}})
    );
    assert_eq!(gov["system"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn long_cache_requires_model_support_or_explicit_override() {
    for (model, name, force, override_support, long) in [
        ("anthropic.claude-opus-4-1", None, false, None, false),
        ("anthropic.claude-3-7-sonnet", None, false, None, false),
        ("anthropic.claude-opus-4-5", None, false, None, true),
        ("opaque", Some("Claude Sonnet 4.6"), false, None, true),
        ("opaque", None, true, None, false),
        ("anthropic.claude-opus-4-1", None, false, Some(true), true),
        ("anthropic.claude-opus-4-5", None, false, Some(false), false),
    ] {
        let (body, _) = capture(
            "us-east-1",
            |c| {
                c.with_model_name(model, name)
                    .with_force_cache(force)
                    .with_long_cache_support(override_support)
                    .with_cache_retention(CacheRetention::Long)
            },
            request(model, 512),
            false,
        )
        .await;
        let expected = if long {
            json!({"type":"default","ttl":"1h"})
        } else {
            json!({"type":"default"})
        };
        assert_eq!(body["system"][1]["cachePoint"], expected, "{model}");
        assert_eq!(
            body["messages"][0]["content"][1]["cachePoint"], expected,
            "{model}"
        );
    }
}

#[tokio::test]
async fn adaptive_max_defaults_to_max_and_honors_mapping() {
    for (model, name, mapped, expected) in [
        ("anthropic.claude-opus-4-6", None, None, "max"),
        ("anthropic.claude-sonnet-4-6", None, None, "max"),
        ("opaque", Some("Claude Opus 4.8"), None, "max"),
        ("anthropic.claude-opus-4-6", None, Some("high"), "high"),
    ] {
        let (body, _) = capture(
            "us-east-1",
            |c| {
                c.with_model_name(model, name)
                    .with_reasoning(Some("max"), mapped)
                    .unwrap()
            },
            request(model, 512),
            false,
        )
        .await;
        assert_eq!(
            body["additionalModelRequestFields"]["output_config"]["effort"], expected,
            "{model}"
        );
    }
    let levels = pi_rs_bedrock::thinking_levels("anthropic.claude-opus-4-6", None);
    assert!(levels.iter().any(|level| level == "max"));
    assert!(!levels.iter().any(|level| level == "xhigh"));
}

#[tokio::test]
async fn claude_default_on_thinking_is_disabled_for_off_and_summaries() {
    for (model, name) in [
        ("anthropic.claude-opus-5", None),
        ("anthropic.claude-sonnet-5", None),
        ("anthropic.claude-haiku-5-5", None),
        ("opaque", Some("Claude Opus 5")),
    ] {
        for (level, summary) in [(None, false), (Some("off"), false), (Some("high"), true)] {
            let (body, _) = capture(
                "us-east-1",
                |c| {
                    c.with_model_name(model, name)
                        .with_reasoning(level, None)
                        .unwrap()
                },
                request(model, 512),
                summary,
            )
            .await;
            assert_eq!(
                body["additionalModelRequestFields"],
                json!({"thinking":{"type":"disabled"}}),
                "{model}"
            );
        }
    }
    let mut req = request("anthropic.claude-sonnet-5-5", 512);
    req.tools.clear();
    let (body, _) = capture(
        "us-east-1",
        |c| c.with_reasoning(Some("high"), None).unwrap(),
        req,
        true,
    )
    .await;
    assert_eq!(
        body["additionalModelRequestFields"],
        json!({"thinking":{"type":"between_tools"}})
    );
}

#[tokio::test]
async fn claude_models_that_cannot_disable_thinking_report_configuration_error() {
    let config = aws_sdk_bedrockruntime::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_bedrockruntime::config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "secret", None, None, "test",
        ))
        .endpoint_url("http://127.0.0.1:1")
        .build();
    let client = Bedrock::new(aws_sdk_bedrockruntime::Client::from_conf(config))
        .with_reasoning(Some("off"), None)
        .unwrap();
    for model in [
        "anthropic.claude-fable-5",
        "anthropic.claude-mythos-5-1",
        "anthropic.claude-opus-5-5",
        "anthropic.claude-sonnet-5-5",
    ] {
        let error = match client.stream(request(model, 512)).await {
            Ok(_) => panic!("accepted thinking off for {model}"),
            Err(e) => e,
        };
        assert!(
            matches!(error, pi_rs_core::Error::Config(_)),
            "{model}: {error}"
        );
        assert!(
            error.to_string().contains("cannot disable thinking"),
            "{model}: {error}"
        );
    }
}

#[tokio::test]
async fn required_thinking_models_keep_default_and_use_low_effort_for_summaries() {
    for (model, name) in [
        ("anthropic.claude-fable-5", None),
        ("anthropic.claude-mythos-5-1", None),
        ("anthropic.claude-opus-5-5", None),
        ("opaque", Some("Claude Opus 5.5")),
        ("anthropic.claude-sonnet-5-5", None),
    ] {
        let (normal, _) = capture(
            "us-east-1",
            |c| c.with_model_name(model, name),
            request(model, 512),
            false,
        )
        .await;
        assert!(
            normal.get("additionalModelRequestFields").is_none(),
            "{model}"
        );
        let (summary, _) = capture(
            "us-east-1",
            |c| {
                c.with_model_name(model, name)
                    .with_reasoning(Some("max"), None)
                    .unwrap()
            },
            request(model, 512),
            true,
        )
        .await;
        assert_eq!(summary["inferenceConfig"]["maxTokens"], 512);
        assert_eq!(
            summary["additionalModelRequestFields"]["thinking"]["type"], "adaptive",
            "{model}"
        );
        assert_eq!(
            summary["additionalModelRequestFields"]["output_config"]["effort"], "low",
            "{model}"
        );
        assert!(
            !pi_rs_bedrock::thinking_levels(model, name)
                .iter()
                .any(|level| level == "off"),
            "{model}"
        );
    }
}

#[tokio::test]
async fn gpt_mapping_summary_and_unsupported_cache_models() {
    let (oss, _) = capture(
        "us-east-1",
        |c| {
            c.with_cache_retention(CacheRetention::Short)
                .with_reasoning(Some("xhigh"), None)
                .unwrap()
        },
        request("openai.gpt-oss-120b", 1024),
        false,
    )
    .await;
    assert_eq!(
        oss["additionalModelRequestFields"],
        json!({"reasoning_effort":"high"})
    );
    assert_eq!(oss["messages"][0]["content"].as_array().unwrap().len(), 1);
    let (mapped_oss, _) = capture(
        "us-east-1",
        |c| c.with_reasoning(Some("xhigh"), Some("low")).unwrap(),
        request("openai.gpt-oss-120b", 1024),
        false,
    )
    .await;
    assert_eq!(
        mapped_oss["additionalModelRequestFields"]["reasoning_effort"],
        "low"
    );
    let (gpt, _) = capture(
        "us-east-1",
        |c| {
            c.with_force_cache(true)
                .with_cache_retention(CacheRetention::Short)
                .with_reasoning(Some("minimal"), Some("medium"))
                .unwrap()
        },
        request("openai.gpt-5", 1024),
        false,
    )
    .await;
    assert_eq!(
        gpt["additionalModelRequestFields"],
        json!({"reasoning":{"effort":"medium"}})
    );
    assert!(gpt["messages"][0]["content"][1].get("cachePoint").is_some());
    let (off_mapped, _) = capture(
        "us-east-1",
        |c| c.with_reasoning(Some("off"), Some("none")).unwrap(),
        request("openai.gpt-5", 1024),
        false,
    )
    .await;
    assert_eq!(
        off_mapped["additionalModelRequestFields"]["reasoning"]["effort"],
        "none"
    );
    let (off_unmapped, _) = capture(
        "us-east-1",
        |c| c.with_reasoning(Some("off"), None).unwrap(),
        request("openai.gpt-5", 1024),
        false,
    )
    .await;
    assert!(off_unmapped.get("additionalModelRequestFields").is_none());
    let (summary, _) = capture(
        "us-east-1",
        |c| {
            c.with_cache_retention(CacheRetention::Long)
                .with_reasoning(Some("high"), None)
                .unwrap()
        },
        request("anthropic.claude-sonnet-4-5", 512),
        true,
    )
    .await;
    assert_eq!(summary["system"].as_array().unwrap().len(), 1);
    assert_eq!(
        summary["messages"][0]["content"].as_array().unwrap().len(),
        1
    );
    assert!(summary.get("additionalModelRequestFields").is_none());
}

#[tokio::test]
async fn insufficient_budget_fails_before_network() {
    let config = aws_sdk_bedrockruntime::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_bedrockruntime::config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "secret", None, None, "test",
        ))
        .endpoint_url("http://127.0.0.1:1")
        .build();
    let model = Bedrock::new(aws_sdk_bedrockruntime::Client::from_conf(config))
        .with_reasoning(Some("low"), None)
        .unwrap();
    let error = match model
        .stream(request("anthropic.claude-sonnet-4-5", 1024))
        .await
    {
        Ok(_) => panic!("accepted insufficient output limit"),
        Err(e) => e,
    };
    assert!(matches!(error, pi_rs_core::Error::Config(_)));
    assert!(error.to_string().contains("2048"));
}

#[tokio::test]
async fn checkpoints_skip_blank_system_and_assistant_tail() {
    let mut req = request("anthropic.claude-sonnet-4-5", 4096);
    req.messages = vec![
        Message::System {
            content: "   ".into(),
        },
        Message::User {
            content: "hi".into(),
        },
        Message::Assistant {
            content: "answer".into(),
            calls: vec![],
            replay: None,
        },
    ];
    let (body, _) = capture(
        "us-east-1",
        |c| c.with_cache_retention(CacheRetention::Short),
        req,
        false,
    )
    .await;
    assert!(body.get("system").is_none());
    assert_eq!(body["messages"][1]["role"], "assistant");
    assert_eq!(body["messages"][1]["content"], json!([{"text":"answer"}]));
    assert_eq!(body["messages"][0]["content"], json!([{"text":"hi"}]));
}
