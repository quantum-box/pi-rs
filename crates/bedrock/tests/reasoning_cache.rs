use aws_smithy_eventstream::frame::write_message_to;
use aws_smithy_types::event_stream::{Header, HeaderValue, Message as Frame};
use futures_util::StreamExt;
use pi_rs_bedrock::{Bedrock, ThinkingBudgets, ThinkingDisplay, ToolChoice};
use pi_rs_core::{
    CacheRetention, EffectiveToolSampling, EnrichedModelEvent, Message, Model, ModelEvent,
    ModelRequest, StrictToolPolicy, ToolConstraint, ToolDeclaration, ToolDispatchContext, ToolSpec,
};
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
    capture_request(region, setup, request, summary, None).await
}

async fn capture_request<F>(
    region: &str,
    setup: F,
    request: ModelRequest,
    summary: bool,
    declaration: Option<ToolDeclaration>,
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
    let config = aws_types::SdkConfig::builder()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .region(aws_types::region::Region::new(region.to_owned()))
        .credentials_provider(
            aws_credential_types::provider::SharedCredentialsProvider::new(
                aws_credential_types::Credentials::new("test", "secret", None, None, "test"),
            ),
        )
        .endpoint_url(format!("http://{addr}"))
        .build();
    let model = setup(Bedrock::from_sdk_config(config))
        .prepare_bedrock()
        .await
        .unwrap()
        .unwrap();
    if let Some(declaration) = declaration {
        let context = dispatch_context(summary);
        let prepared = model
            .prepare_bound_tool_dispatch(&request, &[declaration], &context)
            .unwrap();
        let mut stream = prepared.dispatch().await.unwrap();
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            if let EnrichedModelEvent::Legacy(event) = event.unwrap().event {
                events.push(event);
            }
        }
        return (server.await.unwrap(), events);
    }
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
async fn temperature_and_request_metadata_match_pi_payloads() {
    let (default, _) = capture("us-east-1", |model| model, request("fixture", 128), false).await;
    assert_eq!(default["inferenceConfig"], json!({"maxTokens": 128}));
    assert!(default.get("requestMetadata").is_none());

    let (configured, _) = capture(
        "us-east-1",
        |model| {
            model.with_temperature(0.0).unwrap().with_request_metadata(
                [
                    ("team".into(), "fixture".into()),
                    ("run".into(), "42".into()),
                ]
                .into(),
            )
        },
        request("fixture", 128),
        false,
    )
    .await;
    assert_eq!(
        configured["inferenceConfig"],
        json!({"maxTokens": 128, "temperature": 0.0})
    );
    assert_eq!(
        configured["requestMetadata"],
        json!({"team": "fixture", "run": "42"})
    );
    assert!(configured.get("additionalModelRequestFields").is_none());

    let (empty_tags, _) = capture(
        "us-east-1",
        |model| {
            model
                .with_temperature(1.0)
                .unwrap()
                .with_request_metadata(Default::default())
        },
        request("fixture", 128),
        true,
    )
    .await;
    assert_eq!(
        empty_tags["inferenceConfig"],
        json!({"maxTokens": 128, "temperature": 1.0})
    );
    assert_eq!(empty_tags["requestMetadata"], json!({}));
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
async fn custom_budgets_display_and_interleaved_follow_claude_mode() {
    let budgets = ThinkingBudgets {
        low: Some(1500),
        high: Some(6000),
        ..Default::default()
    };
    for (level, max, expected) in [
        ("low", 4096, 1500),
        ("xhigh", 4096, 3072),
        ("max", 10000, 6000),
    ] {
        let (body, _) = capture(
            "us-east-1",
            |c| {
                c.with_thinking_budgets(budgets.clone())
                    .with_thinking_display(ThinkingDisplay::Omitted)
                    .with_interleaved_thinking(false)
                    .with_reasoning(Some(level), None)
                    .unwrap()
            },
            request("anthropic.claude-sonnet-4-5", max),
            false,
        )
        .await;
        assert_eq!(body["inferenceConfig"]["maxTokens"], max);
        assert_eq!(
            body["additionalModelRequestFields"],
            json!({"thinking":{"type":"enabled","budget_tokens":expected,"display":"omitted"}})
        );
    }
    let (adaptive, _) = capture(
        "us-east-1",
        |c| {
            c.with_thinking_budgets(budgets)
                .with_thinking_display(ThinkingDisplay::Omitted)
                .with_interleaved_thinking(false)
                .with_reasoning(Some("medium"), None)
                .unwrap()
        },
        request("anthropic.claude-sonnet-4-6", 512),
        false,
    )
    .await;
    assert_eq!(
        adaptive["additionalModelRequestFields"],
        json!({"thinking":{"type":"adaptive","display":"omitted"},"output_config":{"effort":"medium"}})
    );
    let (gov, _) = capture(
        "us-gov-west-1",
        |c| {
            c.with_thinking_display(ThinkingDisplay::Omitted)
                .with_interleaved_thinking(false)
                .with_reasoning(Some("low"), None)
                .unwrap()
        },
        request("anthropic.claude-sonnet-4-5", 4096),
        false,
    )
    .await;
    assert_eq!(
        gov["additionalModelRequestFields"],
        json!({"thinking":{"type":"enabled","budget_tokens":2048}})
    );
}

#[tokio::test]
async fn custom_budget_validation_only_applies_to_selected_level() {
    let budgets = json!({"low":1500.0,"high":0});
    for (model, level, summary, max) in [
        ("anthropic.claude-sonnet-4-5", Some("low"), false, 4096),
        ("anthropic.claude-sonnet-4-6", Some("high"), false, 512),
        ("openai.gpt-5", Some("high"), false, 512),
        ("anthropic.claude-sonnet-4-5", Some("off"), false, 512),
        ("anthropic.claude-sonnet-4-5", Some("high"), true, 512),
    ] {
        let (body, _) = capture(
            "us-east-1",
            |c| {
                c.with_thinking_budget_settings(budgets.clone())
                    .with_reasoning(level, None)
                    .unwrap()
            },
            request(model, max),
            summary,
        )
        .await;
        if level == Some("low") {
            assert_eq!(
                body["additionalModelRequestFields"]["thinking"]["budget_tokens"],
                1500
            );
        }
    }
    let config = aws_sdk_bedrockruntime::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_bedrockruntime::config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "secret", None, None, "test",
        ))
        .endpoint_url("http://127.0.0.1:1")
        .build();
    for value in [
        json!([]),
        json!({"high":1023}),
        json!({"high":1024.5}),
        json!({"high":null}),
        json!({"high":4294967296_u64}),
    ] {
        let client = Bedrock::new(aws_sdk_bedrockruntime::Client::from_conf(config.clone()))
            .with_thinking_budget_settings(value.clone())
            .with_reasoning(Some("max"), None)
            .unwrap();
        let error = match client
            .stream(request("anthropic.claude-sonnet-4-5", 8192))
            .await
        {
            Ok(_) => panic!("accepted {value}"),
            Err(e) => e,
        };
        assert!(
            matches!(error, pi_rs_core::Error::Config(_)),
            "{value}: {error}"
        );
    }
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
        assert_eq!(
            normal["additionalModelRequestFields"]["thinking"]["type"], "adaptive",
            "{model}"
        );
        assert_eq!(
            normal["additionalModelRequestFields"]["thinking"]["display"], "summarized",
            "{model}"
        );
        assert!(
            normal["additionalModelRequestFields"]
                .get("output_config")
                .is_none(),
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
    for (region, display) in [("us-east-1", Some("omitted")), ("us-gov-west-1", None)] {
        let (body, _) = capture(
            region,
            |c| c.with_thinking_display(ThinkingDisplay::Omitted),
            request("anthropic.claude-opus-5-5", 512),
            false,
        )
        .await;
        assert_eq!(
            body["additionalModelRequestFields"]["thinking"]["display"].as_str(),
            display
        );
        assert!(
            body["additionalModelRequestFields"]
                .get("output_config")
                .is_none()
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

#[tokio::test]
async fn tool_choice_matches_pi_converse_stream_payload() {
    for (choice, wire) in [
        (None, None),
        (Some(ToolChoice::Auto), Some(json!({"auto":{}}))),
        (Some(ToolChoice::Any), Some(json!({"any":{}}))),
        (
            Some(ToolChoice::Tool {
                name: "lookup".into(),
            }),
            Some(json!({"tool":{"name":"lookup"}})),
        ),
        (Some(ToolChoice::None), None),
    ] {
        let disabled = choice == Some(ToolChoice::None);
        let (body, _) = capture(
            "us-east-1",
            |client| match choice {
                Some(choice) => client.with_tool_choice(choice),
                None => client,
            },
            request("test-model", 128),
            false,
        )
        .await;
        assert_eq!(body.get("toolConfig").is_none(), disabled);
        assert_eq!(body["toolConfig"].get("toolChoice"), wire.as_ref());
        if !disabled {
            assert_eq!(body["toolConfig"]["tools"][0]["toolSpec"]["name"], "lookup");
        }
    }

    let mut tool_free = request("test-model", 128);
    tool_free.tools.clear();
    let (body, _) = capture(
        "us-east-1",
        |client| client.with_tool_choice(ToolChoice::Any),
        tool_free,
        false,
    )
    .await;
    assert!(body.get("toolConfig").is_none());
}

#[tokio::test]
async fn disabled_tool_choice_keeps_tool_history_as_text() {
    let mut request = request("anthropic.claude-sonnet-5-5", 4096);
    request.messages.extend([
        Message::Assistant {
            content: String::new(),
            calls: vec![pi_rs_core::ToolCall {
                id: "call".into(),
                name: "lookup".into(),
                arguments: "{}".into(),
            }],
            replay: None,
        },
        Message::Tool {
            call_id: "call".into(),
            content: "found".into(),
            is_error: false,
            is_structured: Some(false),
        },
    ]);
    let (body, _) = capture(
        "us-east-1",
        |client| {
            client
                .with_tool_choice(ToolChoice::None)
                .with_reasoning(Some("off"), None)
                .unwrap()
        },
        request.clone(),
        false,
    )
    .await;
    assert!(body.get("toolConfig").is_none());
    assert_eq!(
        body["additionalModelRequestFields"]["thinking"]["type"],
        "between_tools"
    );
    assert!(
        body["messages"][1]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("Historical tool call")
    );
    assert!(
        body["messages"][2]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("Historical tool result")
    );

    let client = Bedrock::from_env();
    let estimate = client
        .clone()
        .with_tool_choice(ToolChoice::None)
        .estimate_request_tokens(&request);
    request.tools.clear();
    assert_eq!(estimate, client.estimate_request_tokens(&request));
}

#[tokio::test]
async fn bedrock_rejects_forced_tool_choice_with_budgeted_claude_thinking() {
    let config = aws_sdk_bedrockruntime::Config::builder()
        .behavior_version_latest()
        .region(aws_types::region::Region::new("us-east-1"))
        .build();
    let client = Bedrock::new(aws_sdk_bedrockruntime::Client::from_conf(config))
        .with_reasoning(Some("low"), None)
        .unwrap();
    for choice in [
        ToolChoice::Any,
        ToolChoice::Tool {
            name: "lookup".into(),
        },
    ] {
        let result = client
            .clone()
            .with_tool_choice(choice)
            .stream(request("anthropic.claude-sonnet-4-5", 4096))
            .await;
        assert!(
            matches!(result, Err(pi_rs_core::Error::Config(message)) if message.contains("extended thinking"))
        );
    }
}

fn dispatch_context(summary: bool) -> ToolDispatchContext {
    ToolDispatchContext {
        request_id: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
        external_id: "fixture".into(),
        summary,
    }
}

fn constrained_request(policy: StrictToolPolicy) -> (ModelRequest, ToolDeclaration) {
    let mut request = request("test-model", 128);
    request.tools[0].parameters = json!({
        "type":"object", "properties":{"path":{"type":"string"},"limit":{"type":"integer"}},
        "required":["path"]
    });
    let declaration = ToolDeclaration {
        spec: request.tools[0].clone(),
        constrained_sampling: Some(ToolConstraint::JsonSchema { strict: policy }),
    };
    (request, declaration)
}

#[tokio::test]
async fn prepared_bedrock_tools_bind_wire_schema_and_preferred_fallback() {
    for (supported, summary) in [(true, false), (true, true), (false, false)] {
        let (request, declaration) = constrained_request(StrictToolPolicy::Prefer);
        let client = Bedrock::from_env().with_strict_tool_support(supported);
        let prepared = client
            .prepare_bound_tool_dispatch(
                &request,
                std::slice::from_ref(&declaration),
                &dispatch_context(summary),
            )
            .unwrap();
        assert_eq!(
            matches!(
                prepared.effective_tools()[0].sampling,
                EffectiveToolSampling::StrictJsonSchema { .. }
            ),
            supported,
        );
        let reservation = prepared.reservation_request().tools[0].parameters.clone();
        if supported {
            let mut required: Vec<_> = reservation["required"]
                .as_array()
                .unwrap()
                .iter()
                .map(|name| name.as_str().unwrap())
                .collect();
            required.sort_unstable();
            assert_eq!(required, ["limit", "path"]);
            assert_eq!(
                reservation["properties"]["limit"]["anyOf"][1],
                json!({"type":"null"})
            );
            assert_eq!(reservation["additionalProperties"], false);
        } else {
            assert_eq!(reservation, request.tools[0].parameters);
        }

        let (body, _) = capture_request(
            "us-east-1",
            |client| client.with_strict_tool_support(supported),
            request,
            summary,
            Some(declaration),
        )
        .await;
        let tool = &body["toolConfig"]["tools"][0]["toolSpec"];
        assert_eq!(tool["inputSchema"]["json"], reservation);
        assert_eq!(tool.get("strict"), supported.then_some(&Value::Bool(true)));
    }
}

#[test]
fn required_bedrock_constraint_fails_if_unsupported_or_tools_are_disabled() {
    let (request, declaration) = constrained_request(StrictToolPolicy::Require);
    let unsupported = Bedrock::from_env();
    let disabled = unsupported
        .clone()
        .with_strict_tool_support(true)
        .with_tool_choice(ToolChoice::None);
    for client in [unsupported, disabled] {
        let result = client.prepare_bound_tool_dispatch(
            &request,
            std::slice::from_ref(&declaration),
            &dispatch_context(false),
        );
        assert!(
            matches!(result, Err(pi_rs_core::Error::Config(message)) if message.contains("strict"))
        );
    }
}

#[tokio::test]
async fn bedrock_declaration_stream_requires_preparation_for_strict_tools() {
    let (request, declaration) = constrained_request(StrictToolPolicy::Prefer);
    let client = Bedrock::from_env().with_strict_tool_support(true);
    let result = client
        .stream_detailed_with_tool_declarations(
            Uuid::nil(),
            Uuid::nil(),
            "fixture",
            request,
            &[declaration],
        )
        .await;
    assert!(
        matches!(result, Err(pi_rs_core::Error::Config(message)) if message.contains("requires prepared dispatch"))
    );
}
