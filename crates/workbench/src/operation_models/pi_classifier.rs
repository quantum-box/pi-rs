//! Shared decision and legacy classifier wire format.
use pi_rs::{Error, Result};
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
    if !usage["input"].is_number() && !usage["output"].is_number() {
        return None;
    }
    let amount = |value: &Value| value.as_f64().filter(|n| *n >= 0.0);
    let counts: [f64; 4] = ["input", "output", "cacheRead", "cacheWrite"]
        .map(|field| amount(&usage[field]).unwrap_or_default());
    let [input, output, cache_read, cache_write] = counts;
    let total = amount(&usage["totalTokens"]).unwrap_or_else(|| counts.iter().sum());
    let supplied = &usage["cost"];
    let cost = if let Some(total) = supplied["total"].as_f64() {
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
        wire::token_cost(cost, counts)
    };
    Some(json!({
        "input":input,"output":output,"cacheRead":cache_read,"cacheWrite":cache_write,
        "totalTokens":total,"cost":cost,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Cli,
        model_auth::Keys,
        model_profiles::Profiles,
        operation_models::{Catalogue, Operations},
    };
    use clap::Parser;
    use pi_rs::{CancellationToken, Outcome, Tool, ToolContext};
    use std::collections::BTreeMap;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};

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
            "baseUrl":base,"apiKey":"fixture-decision-key","models":[{
                "type":"classifier","api":"pi-decision","id":"judge","input":["text","image"]
            }]
        }}}))
        .unwrap();
        Operations::new(&args).unwrap()
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
