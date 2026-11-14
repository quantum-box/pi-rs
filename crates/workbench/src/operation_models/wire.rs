//! System One classification and OpenRouter image request/response conversion.
use base64::Engine;
use pi_rs::{Error, Result};
use serde_json::{Map, Value, json};

use super::{Api, Entry, ImageInput};

fn classifier_envelope(entry: &Entry, mut body: Value) -> Value {
    if matches!(entry.api, Api::WorkersAiSystemOne) {
        body.as_object_mut()
            .expect("classifier request")
            .remove("model");
        json!({"model":entry.model.id,"input":body})
    } else {
        body
    }
}

pub(super) fn classifier_response(body: &Value) -> Result<&Value> {
    if body["success"] == false {
        return Err(Error::Model("Cloudflare Workers AI request failed".into()));
    }
    let result = body["result"]
        .as_object()
        .ok_or_else(|| Error::Model("invalid Cloudflare Workers AI response".into()))?;
    if result.contains_key("answers") {
        return Ok(&body["result"]);
    }
    if result.get("state").and_then(Value::as_str) != Some("Completed") {
        return Err(Error::Model(
            "Cloudflare Workers AI run did not complete".into(),
        ));
    }
    body["result"]["result"]
        .as_object()
        .map(|_| &body["result"]["result"])
        .ok_or_else(|| Error::Model("invalid Cloudflare Workers AI response".into()))
}

fn image_parts(values: &Value) -> Result<Vec<pi_rs::ImageContent>> {
    if values.is_null() {
        return Ok(vec![]);
    }
    let values = values
        .as_array()
        .ok_or_else(|| Error::Config("images must be an array".into()))?;
    if values.len() > 32 {
        return Err(Error::Config("too many model input images".into()));
    }
    values.iter().map(parse_image).collect()
}

fn parse_image(value: &Value) -> Result<pi_rs::ImageContent> {
    let image: pi_rs::ImageContent = serde_json::from_value(value.clone())
        .map_err(|_| Error::Config("invalid model image block".into()))?;
    if !matches!(
        image.mime_type.as_str(),
        "image/png" | "image/jpeg" | "image/webp" | "image/gif"
    ) || image.data.is_empty()
        || base64::engine::general_purpose::STANDARD
            .decode(&image.data)
            .is_err()
    {
        return Err(Error::Config(
            "model image must be a base64 PNG, JPEG, WebP or GIF".into(),
        ));
    }
    Ok(image)
}

fn image_url(image: &pi_rs::ImageContent) -> String {
    format!("data:{};base64,{}", image.mime_type, image.data)
}

pub(super) fn classifier_payload(entry: &Entry, context: &Value) -> Result<(Value, bool)> {
    let state = context
        .get("state")
        .ok_or_else(|| Error::Config("classifier context requires state".into()))?;
    let questions = context["questions"]
        .as_object()
        .filter(|questions| !questions.is_empty() && questions.len() <= 256)
        .ok_or_else(|| Error::Config("classifier context requires 1–256 questions".into()))?;
    let mut wire = Map::new();
    for (id, question) in questions {
        let mut question = question.clone();
        let criteria = &question["criteria"];
        let valid = match question["type"].as_str() {
            Some("choice") => criteria.as_object().is_some_and(|criteria| {
                !criteria.is_empty() && criteria.values().all(Value::is_string)
            }),
            Some("score") => criteria.as_array().is_some_and(|criteria| {
                !criteria.is_empty() && criteria.iter().all(Value::is_string)
            }),
            Some("bool") => criteria["true"].is_string() && criteria["false"].is_string(),
            _ => false,
        };
        if !valid || !question["instructions"].is_string() {
            return Err(Error::Config(format!("invalid classifier question {id}")));
        }
        if question["type"] == "bool" {
            question["type"] = json!("noul");
        }
        wire.insert(id.clone(), question);
    }
    let mut body = json!({"model":entry.model.id,"state":state,"questions":wire});
    let images = image_parts(&context["images"])?;
    if images.is_empty() {
        return Ok((classifier_envelope(entry, body), false));
    }
    if !entry.accepts_images() {
        return Err(Error::Config(
            "classifier model does not support image input".into(),
        ));
    }
    let mode = entry.image_input();
    match mode {
        ImageInput::Assets => {
            let mut assets = Map::new();
            let mut values = Vec::new();
            for (index, image) in images.iter().enumerate() {
                let format = match image.mime_type.as_str() {
                    "image/png" => "png",
                    "image/jpeg" => "jpeg",
                    _ => {
                        return Err(Error::Config(
                            "classifier assets require PNG or JPEG".into(),
                        ));
                    }
                };
                let name = format!("<image-{}>", index + 1);
                assets.insert(
                    name.clone(),
                    json!({"type":"image","format":format,"data_base64":image.data}),
                );
                values.push(json!(name));
            }
            values.push(state.clone());
            body["assets"] = json!(assets);
            body["state"] = json!(values);
        }
        ImageInput::StateImageUrls => {
            let mut values: Vec<_> = images
                .iter()
                .map(|image| json!({"type":"image_url","image_url":{"url":image_url(image)}}))
                .collect();
            values.push(state.clone());
            body["state"] = json!(values);
        }
        ImageInput::Images => {
            if images.len() > 4 || images.iter().any(|image| image.mime_type == "image/gif") {
                return Err(Error::Config(
                    "classifier accepts up to four PNG, JPEG or WebP images".into(),
                ));
            }
            body["images"] = json!(images.iter().map(image_url).collect::<Vec<_>>());
        }
    }
    let opt_in = entry
        .model
        .compat
        .send_image_opt_in_header
        .unwrap_or(!matches!(entry.api, Api::WorkersAiSystemOne) && !entry.openrouter_compat());
    Ok((classifier_envelope(entry, body), opt_in))
}

pub(super) fn classifier_answers(questions: &Value, answers: &Value) -> Result<Value> {
    let mut result = Map::new();
    for (id, question) in questions.as_object().into_iter().flatten() {
        let answer = &answers[id];
        let invalid = || Error::Model(format!("classifier returned an invalid answer for {id}"));
        let number = |field: &str| answer[field].as_f64().ok_or_else(invalid);
        let item = match (question["type"].as_str(), answer["type"].as_str()) {
            (Some("bool"), Some("noul")) => json!({"type":"bool","probability":number("noul")?}),
            (Some("score"), Some("score")) => {
                json!({"type":"score","score":number("score")?,"confidence":number("confidence")?})
            }
            (Some("choice"), Some("choice")) => {
                let choice = answer["choice"].as_str().ok_or_else(invalid)?;
                let probabilities = answer["probabilities"]
                    .as_object()
                    .filter(|values| values.values().all(Value::is_number))
                    .ok_or_else(invalid)?;
                json!({"type":"choice","choice":choice,"probabilities":probabilities,"confidence":number("confidence")?})
            }
            _ => return Err(invalid()),
        };
        result.insert(id.clone(), item);
    }
    Ok(json!(result))
}

pub(super) fn image_payload(entry: &Entry, context: &Value) -> Result<Value> {
    let input = context["input"]
        .as_array()
        .filter(|items| !items.is_empty() && items.len() <= 64)
        .ok_or_else(|| Error::Config("image generation requires 1–64 input blocks".into()))?;
    let mut content = Vec::new();
    for block in input {
        match block["type"].as_str() {
            Some("text") if block["text"].is_string() => {
                content.push(json!({"type":"text","text":block["text"]}))
            }
            Some("image") if entry.accepts_images() => content.push(
                json!({"type":"image_url","image_url":{"url":image_url(&parse_image(block)?)}}),
            ),
            Some("image") => {
                return Err(Error::Config(
                    "image model does not support image input".into(),
                ));
            }
            _ => return Err(Error::Config("invalid image generation input block".into())),
        }
    }
    let outputs_text = entry
        .model
        .output
        .as_ref()
        .is_some_and(|types| types.iter().any(|kind| kind == "text"));
    Ok(
        json!({"model":entry.model.id,"messages":[{"role":"user","content":content}],"stream":false,
        "modalities":if outputs_text { vec!["image","text"] } else { vec!["image"] }}),
    )
}

pub(super) fn image_output(body: &Value) -> Result<Vec<Value>> {
    let choices = body["choices"]
        .as_array()
        .ok_or_else(|| Error::Model("image provider response is missing its choices".into()))?;
    let Some(choice) = choices.first() else {
        return Ok(Vec::new());
    };
    let Some(message) = choice.get("message").filter(|message| message.is_object()) else {
        return Err(Error::Model(
            "image provider response is missing its message".into(),
        ));
    };
    let mut blocks = Vec::new();
    if let Some(content) = message["content"].as_str().filter(|text| !text.is_empty()) {
        blocks.push(json!({"type":"text","text":content}));
    }
    for image in message["images"].as_array().into_iter().flatten() {
        let Some((mime_type, data)) = image["image_url"]
            .as_str()
            .or_else(|| image["image_url"]["url"].as_str())
            .and_then(|url| url.strip_prefix("data:"))
            .and_then(|url| url.split_once(";base64,"))
        else {
            continue;
        };
        let value = json!({"type":"image","mimeType":mime_type,"data":data});
        parse_image(&value)?;
        blocks.push(value);
    }
    Ok(blocks)
}

pub(super) fn usage(entry: &Entry, value: &Value) -> Option<Value> {
    let value = value.as_object()?;
    let count = |name: &str| {
        value
            .get(name)
            .and_then(Value::as_u64)
            .unwrap_or_default()
            .min(u64::from(u32::MAX))
    };
    let (input, output, cache_read, cache_write) = match entry.api {
        Api::SystemOne | Api::WorkersAiSystemOne => {
            if !value.contains_key("input_tokens") && !value.contains_key("output_tokens") {
                return None;
            }
            (count("input_tokens"), count("output_tokens"), 0, 0)
        }
        Api::OpenRouterImages => {
            let prompt = count("prompt_tokens");
            let detail = value.get("prompt_tokens_details");
            let cached = detail
                .and_then(|value| value["cached_tokens"].as_u64())
                .unwrap_or_default()
                .min(u64::from(u32::MAX));
            let written = detail
                .and_then(|value| value["cache_write_tokens"].as_u64())
                .unwrap_or_default()
                .min(u64::from(u32::MAX));
            let read = cached.saturating_sub(written);
            (
                prompt.saturating_sub(read + written),
                count("completion_tokens"),
                read,
                written,
            )
        }
    };
    let cost = &entry.model.cost;
    let input_cost = cost.input * input as f64 / 1_000_000.0;
    let output_cost = cost.output * output as f64 / 1_000_000.0;
    let read_cost = cost.cache_read * cache_read as f64 / 1_000_000.0;
    let write_cost = cost.cache_write * cache_write as f64 / 1_000_000.0;
    Some(json!({
        "input":input,"output":output,"cacheRead":cache_read,"cacheWrite":cache_write,
        "totalTokens":input+output+cache_read+cache_write,
        "cost":{"input":input_cost,"output":output_cost,"cacheRead":read_cost,"cacheWrite":write_cost,
            "total":input_cost+output_cost+read_cost+write_cost},
    }))
}
