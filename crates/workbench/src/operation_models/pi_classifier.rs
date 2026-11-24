//! Pi's classifier gateway protocol.
use pi_rs::{Error, Result};
use serde_json::{Map, Value, json};

use super::{Cost, Entry, wire};

pub(super) fn payload(entry: &Entry, context: &Value) -> Result<Value> {
    let (state, questions) = wire::classifier_context(context)?;
    let images = wire::image_parts(&context["images"])?;
    let mut context = json!({"state":state,"questions":questions});
    if !images.is_empty() {
        if !entry.accepts_images() {
            return Err(Error::Config(format!(
                "Pi Classifier model {} does not support image input",
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

pub(super) fn answers(questions: &Value, values: &Value) -> Result<Value> {
    let values = values
        .as_object()
        .ok_or_else(|| Error::Model("Pi Classifier returned an unexpected response".into()))?;
    let mut result = Map::new();
    for (id, question) in questions.as_object().into_iter().flatten() {
        let answer = values.get(id).ok_or_else(|| {
            Error::Model(format!("Pi Classifier did not return an answer for {id}"))
        })?;
        let invalid = || Error::Model(format!("Pi Classifier returned an invalid answer for {id}"));
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
        operation_models::{Catalogue, Operations},
    };
    use clap::Parser;
    use std::time::Duration;

    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    #[tokio::test]
    async fn pi_classifier_uses_configured_wire_protocol_and_retries() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let context = json!({"state":{"name":"fixture"}, "questions":{
            "q":{"type":"bool","instructions":"Match?","criteria":{"true":"yes","false":"no"}},
            "c":{"type":"choice","instructions":"Which?","criteria":{"match":"yes","other":"no"}},
            "s":{"type":"score","instructions":"How much?","criteria":["none", "some"]},
        }, "images":[{"type":"image","mimeType":"image/png","data":"YQ=="}]});
        let expected = json!({"model":"judge","context":context});
        let result = json!({
            "q":{"type":"bool","probability":0.75},
            "c":{"type":"choice","choice":"match","probabilities":{"match":0.75,"other":0.25},"confidence":0.75},
            "s":{"type":"score","score":0.5,"confidence":1},
        });
        let response = json!({
            "answers":result,
            "usage":{"input":10,"output":4,"cacheRead":2,"cacheWrite":1,"totalTokens":19,
            "cost":{"total":0.02,"totalFromProvider":true}}
        })
        .to_string();
        let server = tokio::spawn(async move {
            for first in [true, false] {
                let (socket, _) = listener.accept().await.unwrap();
                let mut request = BufReader::new(socket);
                let mut line = String::new();
                request.read_line(&mut line).await.unwrap();
                assert_eq!(line, "POST /v1/classify HTTP/1.1\r\n");
                let mut length = None;
                let mut authorized = false;
                loop {
                    line.clear();
                    assert!(request.read_line(&mut line).await.unwrap() > 0);
                    if line == "\r\n" {
                        break;
                    }
                    let (name, value) = line.split_once(':').unwrap();
                    if name.eq_ignore_ascii_case("content-length") {
                        length = Some(value.trim().parse().unwrap());
                    }
                    if name.eq_ignore_ascii_case("authorization") {
                        authorized = value.trim() == "Bearer fixture";
                    }
                }
                assert!(authorized);
                let mut body = vec![0; length.unwrap()];
                request.read_exact(&mut body).await.unwrap();
                assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), expected);
                let reply = if first {
                    "HTTP/1.1 429 Too Many Requests\r\nretry-after-ms: 0\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_owned()
                } else {
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response}",
                        response.len()
                    )
                };
                request.get_mut().write_all(reply.as_bytes()).await.unwrap();
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
        assert_eq!(data["usage"]["totalTokens"], 19.0);
        assert_eq!(data["usage"]["cost"]["total"], 0.02);
        assert_eq!(data["usage"]["cost"]["totalFromProvider"], true);

        let mut invalid = result;
        invalid["c"]["probabilities"]["unknown"] = json!(0);
        assert!(answers(&context["questions"], &invalid).is_err());
    }
}
