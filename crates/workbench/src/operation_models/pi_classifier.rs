//! Shared decision and legacy classifier wire format.
use pi_rs::{Error, Result};
use reqwest::header::{AUTHORIZATION, COOKIE, HeaderMap, PROXY_AUTHORIZATION};
use serde_json::{Map, Value, json};

use super::{Api, Cost, Entry, wire};

fn label(api: Api) -> &'static str {
    match api {
        Api::PiDecision => "Pi Decision",
        _ => "Pi Classifier",
    }
}

pub(super) fn payload(entry: &Entry, context: &Value) -> Result<Value> {
    let (state, questions) = wire::classifier_context(context)?;
    let images = wire::image_parts(&context["images"])?;
    let mut context = json!({"state":state,"questions":questions});
    if !images.is_empty() {
        if !entry.accepts_images() {
            return Err(Error::Config(format!(
                "{} model {} does not support image input",
                label(entry.api),
                entry.model.id
            )));
        }
        context["images"] = json!(
            images
                .iter()
                .map(|image| json!({
                    "type":"image","data":image.data,"mimeType":image.mime_type,
                }))
                .collect::<Vec<_>>()
        );
    }
    Ok(json!({"model":entry.model.id,"context":context}))
}

pub(super) fn answers(api: Api, questions: &Value, values: &Value) -> Result<Value> {
    let label = label(api);
    let values = values
        .as_object()
        .ok_or_else(|| Error::Model(format!("{label} returned an unexpected response")))?;
    let mut result = Map::new();
    for (id, question) in questions.as_object().into_iter().flatten() {
        let answer = values
            .get(id)
            .ok_or_else(|| Error::Model(format!("{label} did not return an answer for {id}")))?;
        let invalid = || Error::Model(format!("{label} returned an invalid answer for {id}"));
        let bounded = |value: &Value, max: f64| {
            value
                .as_f64()
                .filter(|n| (0.0..=max).contains(n))
                .ok_or_else(invalid)
        };
        let item = match (question["type"].as_str(), answer["type"].as_str()) {
            (Some("bool"), Some("bool")) => {
                bounded(&answer["probability"], 1.0)?;
                json!({"type":"bool","probability":answer["probability"]})
            }
            (Some("score"), Some("score")) => {
                let max = question["criteria"]
                    .as_array()
                    .map_or(0, |criteria| criteria.len().saturating_sub(1));
                bounded(&answer["score"], max as f64)?;
                bounded(&answer["confidence"], 1.0)?;
                json!({"type":"score","score":answer["score"],"confidence":answer["confidence"]})
            }
            (Some("choice"), Some("choice")) => {
                let criteria = question["criteria"].as_object().ok_or_else(invalid)?;
                let choice = answer["choice"]
                    .as_str()
                    .filter(|choice| criteria.contains_key(*choice))
                    .ok_or_else(invalid)?;
                let probabilities = answer["probabilities"].as_object().ok_or_else(invalid)?;
                for (key, probability) in probabilities {
                    if !criteria.contains_key(key) {
                        return Err(invalid());
                    }
                    bounded(probability, 1.0)?;
                }
                bounded(&answer["confidence"], 1.0)?;
                json!({"type":"choice","choice":choice,"probabilities":probabilities,"confidence":answer["confidence"]})
            }
            _ => return Err(invalid()),
        };
        result.insert(id.clone(), item);
    }
    Ok(Value::Object(result))
}

pub(super) fn usage(cost: &Cost, usage: &Value) -> Option<Value> {
    let supplied = &usage["cost"];
    let reported_total = supplied["total"].as_f64().filter(|total| total.is_finite());
    if !usage["input"].is_number() && !usage["output"].is_number() && reported_total.is_none() {
        return None;
    }
    let amount = |value: &Value| value.as_f64().filter(|n| n.is_finite() && *n >= 0.0);
    let counts: [f64; 4] = ["input", "output", "cacheRead", "cacheWrite"]
        .map(|field| amount(&usage[field]).unwrap_or_default());
    let [input, output, cache_read, cache_write] = counts;
    let total = amount(&usage["totalTokens"]).unwrap_or_else(|| counts.iter().sum());
    let image_output = amount(&usage["imageOutput"]);
    let cost = if let Some(total) = reported_total {
        let mut priced: Map<String, Value> = ["input", "output", "cacheRead", "cacheWrite"]
            .into_iter()
            .map(|field| {
                (
                    field.into(),
                    json!(amount(&supplied[field]).unwrap_or_default()),
                )
            })
            .collect();
        priced.insert("total".into(), json!(total.max(0.0)));
        if let Some(value) = supplied["totalFromProvider"].as_bool() {
            priced.insert("totalFromProvider".into(), json!(value));
        }
        Value::Object(priced)
    } else {
        let priced_image_tokens = if cost.image_output.is_some() {
            image_output.unwrap_or(output).min(output)
        } else {
            0.0
        };
        wire::token_cost(cost, counts, priced_image_tokens)
    };
    let mut result = json!({
        "input":input,"output":output,"cacheRead":cache_read,"cacheWrite":cache_write,
        "totalTokens":total,"cost":cost,
    });
    if let Some(reasoning) = amount(&usage["reasoning"]) {
        result["reasoning"] = json!(reasoning);
    }
    if let Some(image_output) = image_output {
        result["imageOutput"] = json!(image_output);
    }
    Some(result)
}

fn normalize_diagnostic(value: &str) -> String {
    let value: String = value
        .chars()
        .map(|ch| {
            if ch.is_control()
                || matches!(ch, '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
            {
                ' '
            } else {
                ch
            }
        })
        .collect();
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn bearer_token(value: &str) -> Option<&str> {
    let mut fields = value.split_ascii_whitespace();
    let scheme = fields.next()?;
    let token = fields.next()?;
    (scheme.eq_ignore_ascii_case("bearer")
        && fields.next().is_none()
        && token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~+/=".contains(&byte)))
    .then_some(token)
}

fn has_opaque_credentials(headers: &HeaderMap) -> bool {
    headers.iter().any(|(name, value)| {
        if name == AUTHORIZATION || name == PROXY_AUTHORIZATION {
            return value.to_str().ok().and_then(bearer_token).is_none();
        }
        name == COOKIE || value.is_sensitive()
    })
}

pub(super) async fn http_error_message(
    mut response: reqwest::Response,
    request_headers: &HeaderMap,
) -> String {
    const MAX_ERROR_BODY: usize = 16 * 1024;
    let status = response.status().as_u16();
    let fallback = || format!("Pi Decision returned HTTP {status}");
    if response
        .content_length()
        .is_some_and(|length| length > MAX_ERROR_BODY as u64)
    {
        return fallback();
    }
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) if chunk.len() <= MAX_ERROR_BODY - body.len() => {
                body.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            _ => return fallback(),
        }
    }
    let Ok(body) = serde_json::from_slice::<Value>(&body) else {
        return fallback();
    };
    let diagnostic = &body["error"];
    if has_opaque_credentials(request_headers) {
        // A cookie, authorization value, or custom header can contain several
        // credentials. Keep only known codes instead of echoing arbitrary text.
        return match diagnostic["code"].as_str() {
            Some(
                code @ ("authentication_error"
                | "forbidden"
                | "insufficient_quota"
                | "internal_error"
                | "invalid_api_key"
                | "invalid_question"
                | "invalid_request"
                | "invalid_request_error"
                | "not_found"
                | "permission_denied"
                | "rate_limit_exceeded"
                | "server_error"
                | "service_unavailable"
                | "unauthorized"),
            ) => format!("Pi Decision returned HTTP {status} ({code})"),
            _ => fallback(),
        };
    }
    let clean = |field: &str, limit| {
        let value = normalize_diagnostic(diagnostic[field].as_str()?);
        // Compare before truncation so even a credential crossing the display
        // limit is omitted in full.
        if request_headers
            .values()
            .filter(|header| header.is_sensitive())
            .any(|header| {
                let Ok(secret) = std::str::from_utf8(header.as_bytes()) else {
                    return false;
                };
                let secret = normalize_diagnostic(secret);
                (!secret.is_empty() && value.contains(&secret))
                    || bearer_token(&secret).is_some_and(|token| value.contains(token))
            })
        {
            return Some("[redacted]".to_owned());
        }
        let value: String = value.chars().take(limit).collect();
        (!value.is_empty()).then(|| value.trim_end().to_owned())
    };
    match (clean("code", 64), clean("message", 512)) {
        (Some(code), Some(message)) => {
            format!("Pi Decision returned HTTP {status} ({code}): {message}")
        }
        (Some(detail), None) | (None, Some(detail)) => {
            format!("Pi Decision returned HTTP {status}: {detail}")
        }
        (None, None) => fallback(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Cli,
        model_auth::{Credential, Keys},
        model_profiles::Profiles,
        operation_models::{Catalogue, Operations},
    };
    use clap::Parser;
    use pi_rs::{CancellationToken, Outcome, Tool, ToolContext};
    use std::collections::BTreeMap;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};

    #[test]
    fn usage_preserves_cost_only_responses_and_optional_token_splits() {
        let cost = Cost::default();
        let cost_only = usage(
            &cost,
            &json!({"cost":{"total":0.12,"totalFromProvider":true}}),
        )
        .unwrap();
        assert_eq!(cost_only["totalTokens"].as_f64(), Some(0.0));
        assert_eq!(cost_only["cost"]["total"].as_f64(), Some(0.12));
        assert_eq!(cost_only["cost"]["totalFromProvider"], true);
        assert!(cost_only.get("reasoning").is_none());
        assert!(cost_only.get("imageOutput").is_none());
        assert!(usage(&cost, &json!({"reasoning":7})).is_none());

        let cost = Cost {
            output: 2.0,
            image_output: Some(5.0),
            ..Cost::default()
        };
        let estimated = usage(
            &cost,
            &json!({
                "input": 10, "output": 1_000_000, "reasoning": 0, "imageOutput": 200_000
            }),
        )
        .unwrap();
        assert_eq!(estimated["totalTokens"].as_f64(), Some(1_000_010.0));
        assert_eq!(estimated["reasoning"].as_f64(), Some(0.0));
        assert_eq!(estimated["imageOutput"].as_f64(), Some(200_000.0));
        assert_eq!(estimated["cost"]["output"].as_f64(), Some(2.6));
    }

    #[test]
    fn usage_prices_duplicate_tiers_and_large_input_like_pi() {
        let cost = serde_json::from_value(json!({
            "tiers": [
                {"inputTokensAbove":100, "input":1.0},
                {"inputTokensAbove":100, "input":3.0},
                {"inputTokensAbove":50, "input":4.0}
            ]
        }))
        .unwrap();
        let priced = usage(&cost, &json!({"input":200})).unwrap();
        let total = priced["cost"]["total"].as_f64().unwrap();
        assert!((total - 0.0002).abs() < 1e-12, "{total}");

        let cost = Cost {
            input: 10.0,
            cache_read: 10.0,
            ..Cost::default()
        };
        let priced = usage(&cost, &json!({"input":1e308,"cacheRead":1e307})).unwrap();
        assert!(priced["cost"]["input"].as_f64().unwrap().is_finite());
        assert!(priced["cost"]["cacheRead"].as_f64().unwrap().is_finite());
    }

    async fn request(socket: &mut TcpStream) -> (String, BTreeMap<String, String>, Value) {
        let mut reader = BufReader::new(socket);
        let mut route = String::new();
        reader.read_line(&mut route).await.unwrap();
        let mut headers = BTreeMap::new();
        loop {
            let mut line = String::new();
            assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
            if line == "\r\n" {
                break;
            }
            let (name, value) = line.split_once(':').unwrap();
            headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
        }
        let mut body = vec![0; headers["content-length"].parse().unwrap()];
        reader.read_exact(&mut body).await.unwrap();
        (route, headers, serde_json::from_slice(&body).unwrap())
    }

    fn questions() -> Value {
        json!({
            "q":{"type":"bool","instructions":"Match?","criteria":{"true":"yes","false":"no"}},
            "c":{"type":"choice","instructions":"Which?","criteria":{"match":"yes","other":"no"}},
            "s":{"type":"score","instructions":"How much?","criteria":["none", "some"]},
        })
    }

    fn fixture_answers() -> Value {
        json!({
            "q":{"type":"bool","probability":0.75},
            "c":{"type":"choice","choice":"match","probabilities":{"match":0.75,"other":0.25},"confidence":0.75},
            "s":{"type":"score","score":0.5,"confidence":1},
        })
    }

    #[tokio::test]
    async fn pi_classifier_uses_configured_wire_protocol_and_retries() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let context = json!({"state":{"name":"fixture"}, "questions":questions(),
            "images":[{"type":"image","mimeType":"image/png","data":"YQ=="}]});
        let expected = json!({"model":"judge","context":context});
        let result = fixture_answers();
        let response = json!({
            "answers":result,
            "responseId":"legacy-fixture",
            "usage":{"input":10,"output":4,"cacheRead":2,"cacheWrite":1,"totalTokens":19,
            "cost":{"total":0.02,"totalFromProvider":true}}
        })
        .to_string();
        let server = tokio::spawn(async move {
            for first in [true, false] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (route, headers, body) = request(&mut socket).await;
                assert_eq!(route, "POST /v1/classify HTTP/1.1\r\n");
                assert_eq!(headers["authorization"], "Bearer fixture");
                assert_eq!(body, expected);
                let reply = if first {
                    "HTTP/1.1 429 Too Many Requests\r\nretry-after-ms: 0\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_owned()
                } else {
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response}",
                        response.len()
                    )
                };
                socket.write_all(reply.as_bytes()).await.unwrap();
            }
        });
        let config = json!({"relay":{"baseUrl":format!("http://{address}/v1"),
            "apiKey":"fixture","models":[{"type":"classifier","api":"pi-classifier",
            "id":"judge","input":["text","image"]}]}});
        let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
        let entry = catalogue
            .0
            .iter()
            .find(|entry| entry.provider == "relay")
            .unwrap();
        assert_eq!(entry.descriptor()["api"], "pi-classifier");
        let operation = Operations::new(&Cli::parse_from(["pi"])).unwrap();
        let mut data = json!({});
        tokio::time::timeout(
            Duration::from_secs(5),
            operation.perform(entry, &context, &Keys::default(), &mut data),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();
        assert_eq!(data["answers"], result);
        assert!(data.get("responseId").is_none());
        assert_eq!(data["usage"]["totalTokens"], 19.0);
        assert_eq!(data["usage"]["cost"]["total"], 0.02);
        assert_eq!(data["usage"]["cost"]["totalFromProvider"], true);

        let mut invalid = result;
        invalid["c"]["probabilities"]["unknown"] = json!(0);
        assert!(answers(Api::PiClassifier, &context["questions"], &invalid).is_err());
    }

    fn decision_operations(base: &str) -> Operations {
        let mut args = Cli::parse_from(["pi"]);
        args.model_profiles = Profiles::parse(&json!({"providers":{"decision-fixture":{
            "baseUrl":base,"api":"pi-decision","apiKey":"fixture-decision-key",
            "headers":{"x-private-fixture":"  fixture-private-header\t value  "},"models":[{
                "type":"classifier","id":"judge","input":["text","image"]
            }]
        }}}))
        .unwrap();
        Operations::new(&args).unwrap()
    }

    #[test]
    fn provider_level_decision_configuration_isolated_from_bundled_routes() {
        let mut config = json!({"providers":{"openai":{
            "api":"pi-decision","baseUrl":"https://decision.invalid/v1",
            "apiKey":"fixture-decision-key","headers":{"x-private-fixture":"private"},
            "authHeader":true,
            "models":[{"type":"classifier","id":"judge"},{
                "id":"chat-fixture","api":"openai-responses","baseUrl":"https://chat.invalid/v1",
                "headers":{"x-chat-fixture":"chat-only"}
            }]
        }}});
        let profiles = Profiles::parse(&config).unwrap();
        let chat = profiles.provider("openai").unwrap();
        assert_eq!(profiles.endpoint(&chat), chat.default_endpoint());
        assert!(
            profiles
                .credential(&chat, profiles.endpoint(&chat))
                .is_none()
        );
        assert!(
            profiles
                .model_ids(&chat)
                .iter()
                .any(|id| id == "chat-fixture")
        );
        let chat_endpoint = profiles.model_endpoint(&chat, "chat-fixture").unwrap();
        assert_eq!(chat_endpoint, "https://chat.invalid/v1/responses");
        assert!(profiles.credential(&chat, chat_endpoint).is_none());
        let headers = profiles
            .header_settings(&chat, chat_endpoint, Some("chat-fixture"))
            .static_headers(&chat, None)
            .unwrap();
        assert_eq!(headers["x-chat-fixture"], "chat-only");
        assert!(!headers.contains_key("x-private-fixture"));
        assert!(!headers.contains_key("authorization"));

        let catalogue = &profiles.operations().0;
        let decision = catalogue
            .iter()
            .find(|entry| entry.model.id == "judge")
            .unwrap();
        assert!(matches!(decision.api, Api::PiDecision));
        assert_eq!(
            decision.endpoint.as_str(),
            "https://decision.invalid/v1/decision"
        );
        assert_eq!(decision.api_key.as_deref(), Some("fixture-decision-key"));
        assert_eq!(decision.model.headers["x-private-fixture"], "private");
        assert!(!decision.model.headers.contains_key("x-chat-fixture"));
        let builtin = catalogue
            .iter()
            .find(|entry| entry.provider == "openai" && matches!(entry.api, Api::OpenAiDecisions))
            .unwrap();
        assert_eq!(
            builtin.endpoint.as_str(),
            "https://api.openai.com/v1/decisions"
        );
        assert!(builtin.api_key.is_none());
        assert!(!builtin.model.headers.contains_key("x-private-fixture"));

        config["providers"]["openai"]["models"][1]["baseUrl"] = Value::Null;
        assert!(
            matches!(Profiles::parse(&config), Err(Error::Config(message))
            if message.contains("openai/chat-fixture requires its own baseUrl"))
        );
    }

    #[test]
    fn provider_level_decision_rejects_missing_or_invalid_chat_api() {
        let mut config = json!({"providers":{"openai":{
            "api":"pi-decision","baseUrl":"http://127.0.0.1:1/v1",
            "models":[{"type":"classifier","id":"judge"},{"type":"chat",
                "id":"gpt-4.1","baseUrl":"http://127.0.0.1:2/v1"}]
        }}});
        assert!(
            matches!(Profiles::parse(&config), Err(Error::Config(message))
            if message.contains("openai/gpt-4.1 requires its own supported chat api"))
        );
        for api in ["fixture-unsupported-api", "pi-decision"] {
            config["providers"]["openai"]["models"][1]["api"] = json!(api);
            assert!(
                matches!(Profiles::parse(&config), Err(Error::Config(message))
                if message.contains("openai/gpt-4.1 requires its own supported chat api")),
                "{api}"
            );
        }
    }

    #[test]
    fn model_level_decision_defaults_do_not_override_bundled_routes() {
        let mut configured = json!({"providers":{"openai":{
            "baseUrl":"http://127.0.0.1:1/v1",
            "apiKey":"fixture-decision-key",
            "headers":{"x-decision-fixture":"operation-only"},
            "models":[{"type":"classifier","id":"judge","api":"pi-decision"}]
        }}});
        let baseline = Profiles::parse(&json!({})).unwrap();
        let openai = baseline.provider("openai").unwrap();
        for model_scoped_base in [false, true] {
            if model_scoped_base {
                let provider = &mut configured["providers"]["openai"];
                let base = provider.as_object_mut().unwrap().remove("baseUrl").unwrap();
                provider["models"][0]["baseUrl"] = base;
            }
            let profiles = Profiles::parse(&configured).unwrap();
            let endpoint = profiles.endpoint(&openai);
            assert_eq!(endpoint, baseline.endpoint(&openai));
            assert_eq!(
                profiles.model_endpoint(&openai, "gpt-4.1"),
                baseline.model_endpoint(&openai, "gpt-4.1")
            );
            assert!(profiles.credential(&openai, endpoint).is_none());
            let headers = profiles
                .header_settings(&openai, endpoint, Some("gpt-4.1"))
                .static_headers(&openai, None)
                .unwrap();
            assert!(!headers.contains_key("x-decision-fixture"));

            let catalogue = &profiles.operations().0;
            let decision = catalogue
                .iter()
                .find(|entry| entry.model.id == "judge")
                .unwrap();
            assert_eq!(decision.endpoint.as_str(), "http://127.0.0.1:1/v1/decision");
            assert_eq!(decision.api_key.as_deref(), Some("fixture-decision-key"));
            assert_eq!(
                decision.model.headers["x-decision-fixture"],
                "operation-only"
            );
            let builtin = catalogue
                .iter()
                .find(|entry| {
                    entry.provider == "openai" && matches!(entry.api, Api::OpenAiDecisions)
                })
                .unwrap();
            assert!(builtin.is_official());
            assert!(builtin.api_key.is_none());
            assert!(!builtin.model.headers.contains_key("x-decision-fixture"));
        }
    }

    #[test]
    fn azure_chat_environment_stays_out_of_the_decision_profile() {
        let mut configured = json!({"providers":{"Azure":{
            "api":"pi-decision","apiKey":"fixture-decision-key",
            "models":[{"type":"classifier","id":"judge"}]
        }}});
        let chat_env =
            |name: &str| (name == "AZURE_OPENAI_BASE_URL").then(|| "http://127.0.0.1:1".to_owned());
        assert!(
            matches!(Profiles::parse_with_azure_env(&configured, chat_env), Err(Error::Config(message))
            if message.contains("operation provider azure requires baseUrl"))
        );

        configured["providers"]["Azure"]["baseUrl"] = json!("http://127.0.0.1:2/v1");
        let baseline = Profiles::parse_with_azure_env(&json!({}), chat_env).unwrap();
        let profiles = Profiles::parse_with_azure_env(&configured, chat_env).unwrap();
        let azure = profiles.provider("azure").unwrap();
        let chat = profiles.endpoint(&azure);
        assert_eq!(chat, baseline.endpoint(&azure));
        assert!(!chat.is_empty());
        assert!(profiles.credential(&azure, chat).is_none());
        let decision = profiles
            .operations()
            .0
            .iter()
            .find(|entry| entry.model.id == "judge")
            .unwrap();
        assert_eq!(decision.endpoint.as_str(), "http://127.0.0.1:2/v1/decision");
        assert_eq!(decision.api_key.as_deref(), Some("fixture-decision-key"));
    }

    #[test]
    fn ordinary_mixed_profile_keeps_chat_defaults_and_separates_explicit_operation_routes() {
        let profiles = Profiles::parse(&json!({"providers":{"openai":{
            "baseUrl":"http://127.0.0.1:1/v1","apiKey":"fixture-chat-key",
            "headers":{"x-chat-fixture":"chat-default"},
            "models":[
                {"id":"chat-fixture","type":"chat"},
                {"id":"judge","type":"classifier","api":"pi-decision","baseUrl":"http://127.0.0.1:2/v1","headers":{"x-decision-fixture":"operation-only"}}
            ]
        }}})).unwrap();
        let openai = profiles.provider("openai").unwrap();
        let chat = profiles.endpoint(&openai);
        assert_eq!(chat, "http://127.0.0.1:1/v1/responses");
        assert_eq!(
            profiles
                .credential(&openai, chat)
                .as_ref()
                .and_then(Credential::literal),
            Some("fixture-chat-key")
        );
        let decision = profiles
            .operations()
            .0
            .iter()
            .find(|entry| entry.model.id == "judge")
            .unwrap();
        assert_eq!(decision.endpoint.as_str(), "http://127.0.0.1:2/v1/decision");
        assert!(decision.api_key.is_none());
        assert!(!decision.model.headers.contains_key("x-chat-fixture"));
        assert_eq!(
            decision.model.headers["x-decision-fixture"],
            "operation-only"
        );
    }

    #[test]
    fn mixed_custom_provider_keeps_model_scoped_chat_and_decision_routes() {
        let profiles = Profiles::parse(&json!({"providers":{"mixed-fixture":{
            "api":"pi-decision","apiKey":"fixture-decision-key",
            "models":[
                {"type":"classifier","id":"judge","baseUrl":"https://decision.invalid/v1"},
                {"type":"chat","id":"chat","api":"openai-completions","baseUrl":"https://chat.invalid/v1"}
            ]
        }}})).unwrap();
        let chat = profiles.provider("mixed-fixture").unwrap();
        let endpoint = profiles.model_endpoint(&chat, "chat").unwrap();
        assert_eq!(endpoint, "https://chat.invalid/v1/chat/completions");
        assert!(profiles.credential(&chat, endpoint).is_none());
        let decision = profiles
            .operations()
            .0
            .iter()
            .find(|entry| entry.provider == "mixed-fixture")
            .unwrap();
        assert_eq!(
            decision.endpoint.as_str(),
            "https://decision.invalid/v1/decision"
        );
        assert_eq!(decision.api_key.as_deref(), Some("fixture-decision-key"));
    }

    async fn invoke(operation: &Operations, args: Value, cancellation: CancellationToken) -> Value {
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            operation.invoke(
                args,
                ToolContext {
                    operation_id: uuid::Uuid::new_v4(),
                    cancellation,
                },
            ),
        )
        .await
        .expect("fixture operation timed out");
        let Outcome::Structured { data, .. } = result else {
            panic!("expected a structured result: {result:?}");
        };
        data
    }

    #[tokio::test]
    async fn pi_decision_catalogue_requests_results_and_in_flight_cancellation() {
        const CHILD: &str = "PI_RS_DECISION_FIXTURE_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let home = tempfile::tempdir().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "operation_models::pi_classifier::tests::pi_decision_catalogue_requests_results_and_in_flight_cancellation",
                ])
                .current_dir(home.path())
                .env("PI_RS_HOME", home.path())
                .env(CHILD, "1")
                .output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stdout)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            return;
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let operation = decision_operations(&format!(
            "http://{}/relay/v1/",
            listener.local_addr().unwrap()
        ));
        for available in [false, true] {
            let listing = invoke(
                &operation,
                json!({"action":"list","type":"classifier",
                "provider":"decision-fixture","id":"judge","available":available}),
                CancellationToken::new(),
            )
            .await;
            assert_eq!(listing.as_array().unwrap().len(), 1);
            assert_eq!(listing[0]["api"], "pi-decision");
        }
        assert_eq!(
            invoke(
                &operation,
                json!({"action":"resolve","type":"classifier",
            "provider":"decision-fixture","id":"judge"}),
                CancellationToken::new()
            )
            .await,
            true
        );

        let text = json!({"state":{"subject":"fixture"},"questions":questions()});
        let mut with_image = text.clone();
        with_image["images"] = json!([{"type":"image","data":"YQ==","mimeType":"image/png"}]);
        let image_request = json!({"model":"judge","context":with_image});
        let text_request = json!({"model":"judge","context":text});
        let (received, pending_request) = tokio::sync::oneshot::channel();
        let (close, finished) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            for (index, expected) in [image_request, text_request.clone(), text_request]
                .into_iter()
                .enumerate()
            {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (route, headers, body) = request(&mut socket).await;
                assert_eq!(route, "POST /relay/v1/decision HTTP/1.1\r\n");
                assert_eq!(headers["authorization"], "Bearer fixture-decision-key");
                assert_eq!(body, expected);
                if index == 2 {
                    received.send(()).unwrap();
                    finished.await.unwrap();
                    return;
                }
                let mut answers = fixture_answers();
                if index == 1 {
                    answers["q"]["probability"] = json!(2);
                }
                let body = json!({"answers":answers, "responseId":"decision-response",
                    "usage":{"input":10,"output":4,"cacheRead":2,"cacheWrite":1,
                        "reasoning":0,"imageOutput":2,
                        "cost":{"total":0.02,"totalFromProvider":true}}})
                .to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let classify = |context: &Value| {
            json!({"action":"classify","provider":"decision-fixture",
            "id":"judge","context":context})
        };
        let success = invoke(&operation, classify(&with_image), CancellationToken::new()).await;
        assert_eq!(success["api"], "pi-decision");
        assert_eq!(success["provider"], "decision-fixture");
        assert_eq!(success["model"], "judge");
        assert_eq!(success["stopReason"], "stop");
        assert_eq!(success["answers"], fixture_answers());
        assert_eq!(success["responseId"], "decision-response");
        assert_eq!(success["usage"]["totalTokens"], 17.0);
        assert_eq!(success["usage"]["reasoning"], 0.0);
        assert_eq!(success["usage"]["imageOutput"], 2.0);
        assert_eq!(success["usage"]["cost"]["total"], 0.02);
        assert_eq!(success["usage"]["cost"]["totalFromProvider"], true);

        // Explicit empty images are omitted on the wire.
        let mut empty_images = text.clone();
        empty_images["images"] = json!([]);
        let malformed = invoke(
            &operation,
            classify(&empty_images),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(malformed["stopReason"], "error");
        assert_eq!(malformed["answers"], json!({}));
        assert!(
            malformed["errorMessage"]
                .as_str()
                .unwrap()
                .contains("invalid answer for q")
        );
        assert_eq!(malformed["responseId"], success["responseId"]);
        assert_eq!(malformed["usage"], success["usage"]);

        let cancel = CancellationToken::new();
        let request = tokio::spawn({
            let operation = operation.clone();
            let cancel = cancel.clone();
            let args = classify(&text);
            async move { invoke(&operation, args, cancel).await }
        });
        tokio::time::timeout(Duration::from_secs(5), pending_request)
            .await
            .unwrap()
            .unwrap();
        cancel.cancel();
        let aborted = request.await.unwrap();
        assert_eq!(aborted["stopReason"], "aborted");
        assert_eq!(aborted["api"], "pi-decision");
        assert_eq!(aborted["answers"], json!({}));
        assert!(aborted.get("responseId").is_none());
        close.send(()).unwrap();
        server.await.unwrap();

        let auth_path = crate::config::agent_dir().join("auth.json");
        let _lock = crate::auth_lock::acquire_async(&auth_path).await.unwrap();
        let cancellation = CancellationToken::new();
        let pending = invoke(&operation, classify(&text), cancellation.clone());
        tokio::pin!(pending);
        tokio::select! {
            biased;
            result = &mut pending => panic!("operation finished with auth.json still locked: {result}"),
            _ = tokio::task::yield_now() => (),
        }
        cancellation.cancel();
        let aborted = tokio::time::timeout(Duration::from_secs(1), &mut pending)
            .await
            .expect("credential loading did not observe cancellation");
        assert_eq!(aborted["stopReason"], "aborted");
        assert_eq!(aborted["api"], "pi-decision");
        assert_eq!(aborted["answers"], json!({}));
    }

    #[tokio::test]
    async fn pi_decision_preserves_only_bounded_sanitized_terminal_diagnostics() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let operation =
            decision_operations(&format!("http://{}/v1", listener.local_addr().unwrap()));
        let mut entry = operation
            .args
            .model_profiles
            .operations()
            .0
            .iter()
            .find(|entry| entry.provider == "decision-fixture")
            .unwrap()
            .clone();
        entry.model.headers.clear();
        let server = tokio::spawn(async move {
            for (status, retry_after, body) in [
                ("429 Too Many Requests", 0, json!({"error":{"message":"retry diagnostic"}}).to_string()),
                ("400 Bad Request", 0, json!({"error":{
                    "code":"invalid_question", "message":format!("Invalid\nquestion\x1b {} hidden-tail", "a".repeat(600)),
                    "details":{"value":"fixture-decision-key"}
                }}).to_string()),
                ("401 Unauthorized", 0, json!({"error":{
                    "code":"echo: Bearer\n fixture-decision-key",
                    "message":"echo: fixture-decision-key"
                }}).to_string()),
                ("400 Bad Request", 0, json!({"error":{"message":"x".repeat(17 * 1024)}}).to_string()),
                ("429 Too Many Requests", 61, json!({"error":{
                    "code":"rate_limit_exceeded",
                    "message":"echo: fixture-decision-key"
                }}).to_string()),
            ] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (route, headers, _) = request(&mut socket).await;
                assert_eq!(route, "POST /v1/decision HTTP/1.1\r\n");
                assert_eq!(headers["authorization"], "Bearer fixture-decision-key");
                let response = format!(
                    "HTTP/1.1 {status}\r\nRetry-After: {retry_after}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });

        let context = json!({"state":{},"questions":questions()});
        let terminal_error = || async {
            tokio::time::timeout(
                Duration::from_secs(5),
                operation.perform(&entry, &context, &Keys::default(), &mut json!({})),
            )
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
        };
        let invalid = terminal_error().await;
        assert!(
            invalid.contains("HTTP 400 (invalid_question): Invalid question"),
            "{invalid}"
        );
        assert!(!invalid.chars().any(char::is_control));
        for omitted in ["retry diagnostic", "hidden-tail", "fixture-decision-key"] {
            assert!(!invalid.contains(omitted));
        }
        let denied = terminal_error().await;
        assert!(
            denied.contains("HTTP 401 ([redacted]): [redacted]"),
            "{denied}"
        );
        assert!(!denied.contains("fixture-decision-key"));
        let oversized = terminal_error().await;
        assert!(
            oversized.ends_with("Pi Decision returned HTTP 400"),
            "{oversized}"
        );
        let rejected_delay = terminal_error().await;
        assert!(
            rejected_delay.ends_with(
                "Pi Decision returned HTTP 429 (rate_limit_exceeded): [redacted]. model operation requested a retry delay over 60 seconds"
            ),
            "{rejected_delay}"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn pi_decision_suppresses_diagnostics_for_opaque_credentials() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let operation =
            decision_operations(&format!("http://{}/v1", listener.local_addr().unwrap()));
        let entry = operation
            .args
            .model_profiles
            .operations()
            .0
            .iter()
            .find(|entry| entry.provider == "decision-fixture")
            .unwrap();
        let cases = [
            (
                "cookie",
                "session=fixture-session; other=fixture-other",
                "invalid_api_key",
                "fixture-session",
            ),
            (
                "authorization",
                "Basic Zml4dHVyZQ==",
                "Zml4dHVyZQ==",
                "Zml4dHVyZQ==",
            ),
            (
                "cookie",
                "session=fixture-session",
                "fixture-session",
                "fixture-session",
            ),
            (
                "x-fixture-auth",
                r#"{"session":"fixture-custom-session","account":"fixture-account"}"#,
                "invalid_api_key",
                "fixture-custom-session",
            ),
            (
                "x-fixture-auth",
                "session=fixture-custom-session; account=fixture-account",
                "fixture-custom-session",
                "fixture-custom-session",
            ),
        ];
        let server = tokio::spawn(async move {
            for (header, value, code, component) in cases {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (route, headers, _) = request(&mut socket).await;
                assert_eq!(route, "POST /v1/decision HTTP/1.1\r\n");
                assert_eq!(headers[header], value);
                let body = json!({"error":{"code":code,"message":format!("Denied {component}")}})
                    .to_string();
                let response = format!(
                    "HTTP/1.1 401 Unauthorized\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let context = json!({"state":{},"questions":questions()});
        for (header, value, code, component) in cases {
            let mut entry = entry.clone();
            entry.model.headers.clear();
            entry.model.headers.insert(header.into(), value.into());
            let diagnostic = tokio::time::timeout(
                Duration::from_secs(5),
                operation.perform(&entry, &context, &Keys::default(), &mut json!({})),
            )
            .await
            .unwrap()
            .unwrap_err()
            .to_string();
            let expected = if code == "invalid_api_key" {
                "Pi Decision returned HTTP 401 (invalid_api_key)"
            } else {
                "Pi Decision returned HTTP 401"
            };
            assert!(diagnostic.ends_with(expected), "{diagnostic}");
            assert!(!diagnostic.contains(component), "{diagnostic}");
            assert!(!diagnostic.contains("Denied"), "{diagnostic}");
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn pi_decision_does_not_follow_redirects() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let redirect = format!(
            "http://{}/relay/v1/decision",
            destination.local_addr().unwrap()
        );
        let operation = decision_operations(&format!(
            "http://{}/relay/v1",
            listener.local_addr().unwrap()
        ));
        let entry = operation
            .args
            .model_profiles
            .operations()
            .0
            .iter()
            .find(|entry| entry.provider == "decision-fixture")
            .unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (route, headers, _) = request(&mut socket).await;
            assert_eq!(route, "POST /relay/v1/decision HTTP/1.1\r\n");
            assert_eq!(headers["authorization"], "Bearer fixture-decision-key");
            let response = format!(
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: {redirect}\r\nContent-Length: 0\r\n\r\n"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let context = json!({"state":{},"questions":questions()});
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            operation.perform(entry, &context, &Keys::default(), &mut json!({})),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(error.to_string().contains("HTTP 307"));
        server.await.unwrap();
        assert_eq!(
            destination.into_std().unwrap().accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn pi_decision_credentials_and_headers_stay_bound_to_the_origin_and_route() {
        let legacy = "https://relay.invalid/v1/classify";
        let decision = "https://relay.invalid/v1/decision";
        let other_origin = "https://other.invalid/v1/decision";
        let providers = json!({"relay":{
            "baseUrl":legacy,"apiKey":"fixture-provider-key","headers":{"X-Provider-Key":"fixture-header"},
            "models":[
                {"type":"classifier","api":"pi-classifier","id":"legacy"},
                {"type":"classifier","api":"pi-decision","id":"decision","baseUrl":decision},
                {"type":"classifier","api":"pi-decision","id":"other","baseUrl":other_origin,
                    "headers":{"X-Model-Key":"fixture-model-header"}}
            ]
        }});
        let catalogue = Catalogue::parse(providers.as_object().unwrap()).unwrap();
        let entry = |id| {
            catalogue
                .0
                .iter()
                .find(|entry| entry.provider == "relay" && entry.model.id == id)
                .unwrap()
        };
        assert_eq!(entry("legacy").endpoint.as_str(), legacy);
        assert_eq!(
            entry("legacy").api_key.as_deref(),
            Some("fixture-provider-key")
        );
        assert_eq!(
            entry("legacy").model.headers["x-provider-key"],
            "fixture-header"
        );
        assert_eq!(entry("decision").endpoint.as_str(), decision);
        assert_eq!(entry("other").endpoint.as_str(), other_origin);
        assert_eq!(
            entry("other").model.headers["x-model-key"],
            "fixture-model-header"
        );

        for (id, exact, wrong) in [
            ("decision", decision, [legacy, other_origin]),
            ("other", other_origin, [legacy, decision]),
        ] {
            let entry = entry(id);
            assert!(entry.api_key.is_none());
            assert!(!entry.model.headers.contains_key("x-provider-key"));
            let stored = |endpoint| {
                Keys::parse(&json!({"relay":{
                    "type":"api_key","key":"fixture-stored-key","endpoint":endpoint
                }}))
            };
            for endpoint in wrong {
                assert!(
                    entry
                        .credential_with_env(&stored(endpoint), |_| None)
                        .is_none()
                );
            }
            let credential = entry.credential_with_env(&stored(exact), |_| None).unwrap();
            assert_eq!(credential.literal(), Some("fixture-stored-key"));
        }
    }

    #[test]
    fn pi_decision_requires_a_configured_base_and_does_not_inherit_official_auth() {
        let configured = |base: Option<&str>| {
            let mut model =
                json!({"type":"classifier","api":"pi-decision","id":"fixture-decision"});
            if let Some(base) = base {
                model["baseUrl"] = json!(base);
            }
            Catalogue::parse(json!({"openai":{"models":[model]}}).as_object().unwrap())
        };
        assert!(
            matches!(configured(None), Err(Error::Config(message)) if message.contains("requires baseUrl"))
        );
        let catalogue = configured(Some("https://api.openai.com/v1")).unwrap();
        let entry = catalogue
            .0
            .iter()
            .find(|entry| entry.model.id == "fixture-decision")
            .unwrap();
        assert!(!entry.is_official());
        let ambient = |name: &str| (name == "OPENAI_API_KEY").then(|| "fixture-env-key".to_owned());
        let entry_key = |endpoint| {
            Keys::parse(&json!({"openai":{
                "type":"api_key","key":"fixture-stored-key","endpoint":endpoint
            }}))
        };
        for endpoint in [None, Some("https://api.openai.com/v1")] {
            assert!(
                entry
                    .credential_with_env(&entry_key(endpoint), ambient)
                    .is_none()
            );
        }
        let credential = entry
            .credential_with_env(&entry_key(Some(entry.endpoint.as_str())), ambient)
            .unwrap();
        assert_eq!(credential.literal(), Some("fixture-stored-key"));
    }
}
