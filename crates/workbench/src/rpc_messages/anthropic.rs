//! Safe display projection of an Anthropic replay; opaque replay data stays private.
use pi_rs::{ModelApi, ProviderReplay, ToolCall};
use serde_json::{Value, json};

pub(super) fn blocks(
    replay: &ProviderReplay,
    content: &str,
    reasoning: Option<&str>,
    calls: &[ToolCall],
) -> Option<Vec<Value>> {
    if !replay.supports_api(ModelApi::AnthropicMessages) || replay.is_context_edit() {
        return None;
    }
    let mut result = Vec::new();
    let mut text = String::new();
    let mut thought = String::new();
    let mut calls = calls.iter();
    for block in &replay.blocks {
        match block["type"].as_str()? {
            "text" if block["_pi_thinking"] != true => {
                let value = block["text"].as_str()?;
                text.push_str(value);
                result.push(json!({"type":"text", "text":value}));
            }
            "thinking" => {
                let value = block["thinking"].as_str()?;
                thought.push_str(value);
                result.push(json!({"type":"thinking", "thinking":value}));
            }
            "redacted_thinking" if block["data"].as_str().is_some_and(|data| !data.is_empty()) => {
                thought.push_str("[Reasoning redacted]");
                result.push(
                    json!({"type":"thinking", "thinking":"[Reasoning redacted]", "redacted":true}),
                );
            }
            "tool_use" => {
                let call = calls.next()?;
                if block["id"] != call.id
                    || block["name"] != call.name
                    || block["input"] != serde_json::from_str::<Value>(&call.arguments).ok()?
                {
                    return None;
                }
                result.push(super::super::rpc_events::tool_call_value(call));
            }
            _ => return None,
        }
    }
    (text == content && thought == reasoning.unwrap_or_default() && calls.next().is_none())
        .then_some(result)
}

#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum AssistantDisplayBlock {
    Text { text: String },
    ToolCall { id: String },
}

/// Validate display metadata against the current projection before exposing it.
pub(super) fn display_blocks(
    metadata: &Value,
    content: &str,
    calls: &[ToolCall],
) -> Option<Vec<Value>> {
    let layout = metadata.get("anthropicDisplayLayoutV1")?.as_array()?;
    if layout.len() > 256 {
        return None;
    }
    let mut text = String::new();
    let mut result = Vec::new();
    let mut size = 0usize;
    let mut remaining = calls.iter();
    for block in layout {
        let block: AssistantDisplayBlock = serde_json::from_value(block.clone()).ok()?;
        match &block {
            AssistantDisplayBlock::Text { text: value } => {
                size = size.checked_add(value.len())?;
                if size > 1024 * 1024 {
                    return None;
                }
                text.push_str(value);
                result.push(json!({"type":"text","text":value}));
            }
            AssistantDisplayBlock::ToolCall { id } => {
                size = size.checked_add(id.len())?;
                if size > 1024 * 1024 {
                    return None;
                }
                let call = remaining.next()?;
                if call.id != *id {
                    return None;
                }
                result.push(super::super::rpc_events::tool_call_value(call));
            }
        }
    }
    (text == content && remaining.next().is_none()).then_some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_rs::ResolvedModelIdentity;

    #[test]
    fn display_layout_requires_exact_text_and_surviving_calls() {
        let layout = json!({"anthropicDisplayLayoutV1":[{"type":"text","text":"one"},{"type":"tool_call","id":"call"},{"type":"text","text":"two"}]});
        let call = ToolCall {
            id: "call".into(),
            name: "read".into(),
            arguments: "{}".into(),
        };
        let result = display_blocks(&layout, "onetwo", std::slice::from_ref(&call)).unwrap();
        assert_eq!(result[1]["id"], "call");
        assert!(display_blocks(&layout, "changed", std::slice::from_ref(&call)).is_none());
        assert!(display_blocks(&layout, "onetwo", &[]).is_none());
        assert!(display_blocks(&layout, "onetwo", &[call.clone(), call]).is_none());
        assert!(
            display_blocks(
                &json!({"anthropicDisplayLayoutV1":[{"type":"text","text":"one","extra":true}]}),
                "one",
                &[]
            )
            .is_none()
        );
    }

    #[test]
    fn rejects_stale_or_unknown_replay_and_hides_opaque_fields() {
        let mut replay = ProviderReplay {
            identity: ResolvedModelIdentity {
                provider: "anthropic".into(),
                model: "model".into(),
            },
            blocks: vec![
                json!({"type":"thinking","thinking":"a","signature":"secret"}),
                json!({"type":"redacted_thinking","data":"opaque"}),
                json!({"type":"text","text":"b"}),
            ],
        };
        let result = blocks(&replay, "b", Some("a[Reasoning redacted]"), &[]).unwrap();
        assert_eq!(
            result,
            vec![
                json!({"type":"thinking","thinking":"a"}),
                json!({"type":"thinking","thinking":"[Reasoning redacted]","redacted":true}),
                json!({"type":"text","text":"b"})
            ]
        );
        assert!(blocks(&replay, "changed", Some("a[Reasoning redacted]"), &[]).is_none());
        assert!(blocks(&replay, "b", Some("changed"), &[]).is_none());
        replay.blocks.push(json!({"type":"unknown"}));
        assert!(blocks(&replay, "b", Some("a[Reasoning redacted]"), &[]).is_none());
    }
}
