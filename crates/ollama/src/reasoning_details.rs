//! Assemble OpenRouter reasoning details without rendering opaque provider data.
use serde_json::Value;

pub(super) fn is_valid(detail: &Value) -> bool {
    let Some(object) = detail.as_object() else {
        return false;
    };
    if !object
        .get("id")
        .is_none_or(|id| id.is_null() || id.is_string())
        || !object.get("format").is_none_or(Value::is_string)
        || !object.get("index").is_none_or(Value::is_number)
    {
        return false;
    }
    match detail["type"].as_str() {
        Some("reasoning.encrypted") => detail["data"].is_string(),
        Some("reasoning.summary") => detail["summary"].is_string(),
        Some("reasoning.text") => {
            detail["text"].is_string()
                && object
                    .get("signature")
                    .is_none_or(|signature| signature.is_null() || signature.is_string())
        }
        _ => false,
    }
}

pub(super) fn append(details: &mut Vec<Value>, detail: Value) {
    if !is_valid(&detail) {
        return;
    }
    let field = match detail["type"].as_str() {
        Some("reasoning.text") => Some("text"),
        Some("reasoning.summary") => Some("summary"),
        _ => None,
    };
    if let Some(field) = field
        && let Some(last) = details.last_mut()
        && last["type"] == detail["type"]
    {
        let Value::String(mut text) = last[field].take() else {
            unreachable!("stored reasoning detail is validated");
        };
        text.push_str(detail[field].as_str().expect("validated reasoning text"));
        last[field] = Value::String(text);
        for key in ["id", "index", "format", "signature"] {
            if (last[key].is_null() || last[key].as_str() == Some(""))
                && let Some(value) = detail.get(key)
            {
                last[key] = value.clone();
            }
        }
    } else {
        details.push(detail);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn combines_text_and_summary_deltas_without_combining_encrypted_data() {
        let mut details = Vec::new();
        for detail in [
            json!({"type":"reasoning.text","text":"first ","id":null}),
            json!({"type":"reasoning.text","text":"second","id":"r","signature":"signed","index":0}),
            json!({"type":"reasoning.summary","summary":"short "}),
            json!({"type":"reasoning.summary","summary":"summary","index":1}),
            json!({"type":"reasoning.encrypted","data":"opaque-one","index":2}),
            json!({"type":"reasoning.encrypted","data":"opaque-two","index":3}),
            json!({"type":"reasoning.encrypted","data":10}),
        ] {
            append(&mut details, detail);
        }
        assert_eq!(
            details,
            [
                json!({"type":"reasoning.text","text":"first second","id":"r","signature":"signed","index":0}),
                json!({"type":"reasoning.summary","summary":"short summary","index":1}),
                json!({"type":"reasoning.encrypted","data":"opaque-one","index":2}),
                json!({"type":"reasoning.encrypted","data":"opaque-two","index":3}),
            ]
        );
    }
}
