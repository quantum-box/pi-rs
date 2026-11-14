//! Lossless durable user content. Runtime integration is separate from this type.
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The original input shape, including block order and all block metadata.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum UserContent {
    Text(String),
    Blocks(Vec<Value>),
}

impl UserContent {
    /// Parse a typed hook result. Empty arrays are valid for direct hooks, but
    /// upstream rejects them at the child-task result trust boundary.
    pub fn from_yield(value: Value, child_task: bool) -> Option<Self> {
        let parsed: Self = serde_json::from_value(value).ok()?;
        match &parsed {
            Self::Text(_) => Some(parsed),
            Self::Blocks(blocks)
                if (!child_task || !blocks.is_empty()) && blocks.iter().all(valid_block) =>
            {
                Some(parsed)
            }
            _ => None,
        }
    }

    /// Convert only where the current core message can represent the input.
    /// `None` means callers must use a durable-aware provider, never truncate.
    pub fn try_core(&self) -> Option<pi_rs_core::Message> {
        match self {
            Self::Text(text) => Some(pi_rs_core::Message::User {
                content: text.clone(),
            }),
            Self::Blocks(blocks) => {
                crate::extension::yield_input(&serde_json::to_value(blocks).ok()?)
            }
        }
    }
}

fn valid_block(value: &Value) -> bool {
    let Some(block) = value.as_object() else {
        return false;
    };
    match block.get("type").and_then(Value::as_str) {
        Some("text") => block.get("text").is_some_and(Value::is_string),
        Some("image") => {
            block.get("data").is_some_and(Value::is_string)
                && block.get("mimeType").is_some_and(Value::is_string)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn keeps_order_metadata_and_direct_empty_array() {
        for value in [
            json!([]),
            json!([
                {"type": "text", "text": "one", "textSignature": "opaque"},
                {"type": "image", "data": "aGk=", "mimeType": "image/png"},
                {"type": "text", "text": "two"},
            ]),
        ] {
            let content = UserContent::from_yield(value.clone(), false).unwrap();
            assert_eq!(serde_json::to_value(&content).unwrap(), value);
            assert!(content.try_core().is_none());
        }
        assert!(UserContent::from_yield(json!([]), true).is_none());
        assert!(UserContent::from_yield(json!([{ "type": "text" }]), true).is_none());
        let multiple = json!([{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]);
        let content = UserContent::from_yield(multiple.clone(), true).unwrap();
        assert_eq!(serde_json::to_value(&content).unwrap(), multiple);
        assert!(content.try_core().is_none());
        assert!(UserContent::from_yield(json!(""), true).unwrap().try_core().is_some());
    }
}
