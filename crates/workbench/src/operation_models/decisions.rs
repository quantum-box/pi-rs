//! OpenAI Decisions request and response conversion.
use super::{Entry, wire::parse_image};
use pi_rs::{Error, Result};
use serde_json::{Map, Value, json};

pub(super) fn payload(entry: &Entry, context: &Value) -> Result<Value> {
    let state = context["state"]
        .as_object()
        .ok_or_else(|| Error::Config("classifier context.state must be an object".into()))?;
    let questions = context["questions"]
        .as_object()
        .filter(|q| !q.is_empty() && q.len() <= 256)
        .ok_or_else(|| Error::Config("classifier context requires 1–256 questions".into()))?;
    let mut wire = Vec::new();
    for (name, question) in questions {
        let invalid = || Error::Config(format!("invalid classifier question {name}"));
        let instructions = question["instructions"].as_str().ok_or_else(invalid)?;
        let item = match question["type"].as_str() {
            Some("bool") => {
                let yes = question["criteria"]["true"].as_str().ok_or_else(invalid)?;
                let no = question["criteria"]["false"].as_str().ok_or_else(invalid)?;
                let meanings = [("True means: ", yes), ("False means: ", no)]
                    .into_iter()
                    .filter(|(_, value)| !value.is_empty())
                    .map(|(prefix, value)| format!("{prefix}{value}"))
                    .collect::<Vec<_>>();
                let instructions = if meanings.is_empty() {
                    instructions.to_owned()
                } else {
                    format!("{instructions}\n\n{}", meanings.join("\n"))
                };
                json!({"type":"predicate","name":name,"instructions":instructions})
            }
            Some("choice") => {
                let criteria = question["criteria"]
                    .as_object()
                    .filter(|c| !c.is_empty())
                    .ok_or_else(invalid)?;
                let choices = criteria
                    .iter()
                    .map(|(value, desc)| {
                        let desc = desc.as_str().ok_or_else(invalid)?;
                        Ok(if desc.is_empty() {
                            json!({"value":value})
                        } else {
                            json!({"value":value,"description":desc})
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                json!({"type":"choice","name":name,"instructions":instructions,"choices":choices})
            }
            Some("score") => {
                let criteria = question["criteria"]
                    .as_array()
                    .filter(|c| !c.is_empty())
                    .ok_or_else(invalid)?;
                let levels = criteria
                    .iter()
                    .map(|label| {
                        label
                            .as_str()
                            .map(|label| json!({"label":label}))
                            .ok_or_else(invalid)
                    })
                    .collect::<Result<Vec<_>>>()?;
                json!({"type":"score","name":name,"instructions":instructions,"levels":levels})
            }
            _ => return Err(invalid()),
        };
        wire.push(item);
    }
    let state = serde_json::to_string(state)?;
    let input = match context.get("images") {
        None | Some(Value::Null) => json!(state),
        Some(value) => {
            let images = value
                .as_array()
                .ok_or_else(|| Error::Config("images must be an array".into()))?;
            if images.len() > 128 {
                return Err(Error::Config(
                    "OpenAI Decisions accepts at most 128 images".into(),
                ));
            }
            if images.is_empty() {
                json!(state)
            } else {
                if !entry.accepts_images() {
                    return Err(Error::Config(
                        "classifier model does not support image input".into(),
                    ));
                }
                let mut content = vec![json!({"type":"input_text","text":state})];
                for image in images {
                    let image = parse_image(image)?;
                    content.push(json!({"type":"input_image","image_url":format!("data:{};base64,{}",image.mime_type,image.data)}));
                }
                json!([{"role":"user","content":content}])
            }
        }
    };
    Ok(json!({"model":entry.model.id,"input":input,"questions":wire}))
}

pub(super) fn answers(questions: &Value, values: &Value) -> Result<Value> {
    let values = values
        .as_array()
        .ok_or_else(|| Error::Model("OpenAI Decisions returned an unexpected response".into()))?;
    let mut by_name = Map::new();
    for value in values {
        if let Some(name) = value["name"].as_str() {
            by_name.insert(name.to_owned(), value.clone());
        }
    }
    let mut result = Map::new();
    for (id, question) in questions.as_object().into_iter().flatten() {
        let answer = by_name.get(id).ok_or_else(|| {
            Error::Model(format!(
                "OpenAI Decisions did not return an answer for {id}"
            ))
        })?;
        if answer["type"] == "refusal" {
            return Err(Error::Model(format!(
                "OpenAI Decisions refused to answer {id}"
            )));
        }
        let invalid = || {
            Error::Model(format!(
                "OpenAI Decisions returned an invalid answer for {id}"
            ))
        };
        let number = |name: &str| {
            answer[name]
                .as_f64()
                .filter(|n| n.is_finite())
                .ok_or_else(invalid)
        };
        let item = match (question["type"].as_str(), answer["type"].as_str()) {
            (Some("bool"), Some("predicate")) => {
                json!({"type":"bool","probability":number("probability")?})
            }
            (Some("score"), Some("score")) => {
                json!({"type":"score","score":number("score")?,"confidence":number("confidence")?})
            }
            (Some("choice"), Some("choice")) => {
                let choice = answer["choice"].as_str().ok_or_else(invalid)?;
                let probabilities = answer["probabilities"].as_array().ok_or_else(invalid)?;
                let mut mapped = Map::new();
                for p in probabilities {
                    let key = p["value"].as_str().ok_or_else(invalid)?;
                    let value = p["probability"]
                        .as_f64()
                        .filter(|n| n.is_finite())
                        .ok_or_else(invalid)?;
                    mapped.insert(key.to_owned(), json!(value));
                }
                json!({"type":"choice","choice":choice,"probabilities":mapped,"confidence":number("confidence")?})
            }
            _ => return Err(invalid()),
        };
        result.insert(id.clone(), item);
    }
    Ok(json!(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation_models::{Catalogue, wire::usage};

    #[test]
    fn decisions_catalogue_wire_answers_and_pricing() {
        let catalog = Catalogue::parse(&Map::new()).unwrap();
        let entry = catalog
            .0
            .iter()
            .find(|e| e.provider == "openai" && e.model.id == "gpt-6-luna")
            .unwrap();
        assert_eq!(
            entry.endpoint.as_str(),
            "https://api.openai.com/v1/decisions"
        );
        assert_eq!(
            entry.descriptor()["cost"]["tiers"][0]["inputTokensAbove"],
            272000
        );
        let keys = crate::model_auth::Keys::parse(&json!({}));
        let env = |name: &str| (name == "OPENAI_API_KEY").then(|| "fixture-key".to_owned());
        assert_eq!(
            entry.credential_with_env(&keys, env).unwrap().literal(),
            Some("fixture-key")
        );
        let custom = Catalogue::parse(
            json!({"openai":{"baseUrl":"http://127.0.0.1:1/v1"}})
                .as_object()
                .unwrap(),
        )
        .unwrap();
        let custom_entry = custom
            .0
            .iter()
            .find(|e| e.provider == "openai" && e.model.id == "gpt-6-luna")
            .unwrap();
        assert!(custom_entry.credential_with_env(&keys, env).is_none());
        let context = json!({"state":{"message":"hello"},"questions":{
            "bool":{"type":"bool","instructions":"ok?","criteria":{"true":"yes","false":"no"}},
            "choice":{"type":"choice","instructions":"which?","criteria":{"a":"first","b":""}},
            "score":{"type":"score","instructions":"how?","criteria":["low","high"]}
        },"images":[{"type":"image","mimeType":"image/png","data":"YQ=="}]});
        let request = payload(entry, &context).unwrap();
        let mut no_images = context.clone();
        no_images["images"] = Value::Null;
        assert_eq!(
            payload(entry, &no_images).unwrap()["input"],
            "{\"message\":\"hello\"}"
        );
        assert_eq!(
            request["input"][0]["content"][1]["image_url"],
            "data:image/png;base64,YQ=="
        );
        assert_eq!(
            request["questions"][0]["instructions"],
            "ok?\n\nTrue means: yes\nFalse means: no"
        );
        assert_eq!(request["questions"][1]["choices"][1], json!({"value":"b"}));
        let body = json!([
            {"name":"score","type":"score","score":1.0,"confidence":0.9},
            {"name":"bool","type":"predicate","probability":0.75},
            {"name":"choice","type":"choice","choice":"a","confidence":0.8,"probabilities":[{"value":"a","probability":0.9}]}
        ]);
        let result = answers(&context["questions"], &body).unwrap();
        assert_eq!(result["bool"], json!({"type":"bool","probability":0.75}));
        assert_eq!(result["choice"]["probabilities"]["a"], 0.9);
        assert!(
            answers(
                &context["questions"],
                &json!([{"name":"bool","type":"refusal"}])
            )
            .is_err()
        );
        let billed = usage(entry, &json!({"input_tokens":300000,"output_tokens":0})).unwrap();
        assert!((billed["cost"]["total"].as_f64().unwrap() - 0.06).abs() < 1e-12);
        let image_config = json!({"relay":{"baseUrl":"http://127.0.0.1:1", "models":[{
            "type":"image", "id":"fixture", "api":"openrouter-images",
            "cost":{"input":1.0,"cacheRead":1.0,"tiers":[{"inputTokensAbove":100,"input":2.0,"cacheRead":2.0}]}
        }]}});
        let image_catalog = Catalogue::parse(image_config.as_object().unwrap()).unwrap();
        let image_entry = image_catalog
            .0
            .iter()
            .find(|e| e.provider == "relay")
            .unwrap();
        let billed = usage(
            image_entry,
            &json!({"prompt_tokens":120,"prompt_tokens_details":{"cached_tokens":80}}),
        )
        .unwrap();
        assert!((billed["cost"]["total"].as_f64().unwrap() - 0.00024).abs() < 1e-12);
        let mut too_many = context.clone();
        too_many["images"] = json!(vec![
            json!({"type":"image","mimeType":"image/png","data":"YQ=="});
            129
        ]);
        assert!(payload(entry, &too_many).is_err());
    }

    #[tokio::test]
    async fn configured_decisions_uses_endpoint_and_preserves_usage_on_refusal() {
        use crate::{Cli, model_auth::Keys, operation_models::Operations};
        use clap::Parser;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 8192];
            let length = stream.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..length]).to_string();
            let body =
                json!({"answers":[{"name":"q","type":"refusal"}],"usage":{"input_tokens":5}})
                    .to_string();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            request
        });
        let config = json!({"relay":{"baseUrl":format!("http://{address}/v1"),"apiKey":"fixture", "models":[{"type":"classifier","id":"judge","api":"openai-decisions"}]}});
        let catalog = Catalogue::parse(config.as_object().unwrap()).unwrap();
        let entry = catalog.0.iter().find(|e| e.provider == "relay").unwrap();
        let operation = Operations {
            args: Cli::parse_from(["pi"]),
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
        };
        let context = json!({"state":{},"questions":{"q":{"type":"bool","instructions":"ok?","criteria":{"true":"yes","false":"no"}}}});
        let mut data = json!({"answers":{}});
        let result = operation
            .perform(entry, &context, &Keys::parse(&json!({})), &mut data)
            .await;
        assert!(result.unwrap_err().to_string().contains("refused"));
        assert_eq!(data["usage"]["input"], 5);
        assert_eq!(data["answers"], json!({}));
        let request = server.await.unwrap();
        assert!(request.starts_with("POST /v1/decisions "));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture")
        );
    }
}
