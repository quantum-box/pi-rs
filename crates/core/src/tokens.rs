//! Character-based estimates for native model-visible content.
use crate::{Event, Message, ModelRequest, extract_tool_images};

/// Latest usable measured context plus estimates for messages that followed it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContextUsageEstimate {
    pub tokens: u64,
    pub usage_tokens: u64,
    pub trailing_tokens: u64,
    pub last_usage_record: Option<u64>,
}

const ESTIMATED_IMAGE_CHARS: u64 = 4800;

fn text_chars(text: &str) -> u64 {
    text.encode_utf16().count() as u64
}

/// Approximate a message using Pi's four UTF-16 units per token and image budget.
pub fn estimate_message_tokens(message: &Message) -> u64 {
    let chars = match message {
        Message::System { content } | Message::User { content } => text_chars(content),
        Message::UserWithImages { content, images } => text_chars(content)
            .saturating_add((images.len() as u64).saturating_mul(ESTIMATED_IMAGE_CHARS)),
        Message::Assistant { content, calls, .. } => {
            calls.iter().fold(text_chars(content), |chars, call| {
                let arguments = serde_json::from_str::<serde_json::Value>(&call.arguments)
                    .map(|value| value.to_string())
                    .unwrap_or_else(|_| call.arguments.clone());
                chars
                    .saturating_add(text_chars(&call.name))
                    .saturating_add(text_chars(&arguments))
            })
        }
        Message::Tool {
            content,
            is_structured,
            ..
        } => {
            let (content, images) = if *is_structured {
                extract_tool_images(content.clone())
            } else {
                (content.clone(), Vec::new())
            };
            text_chars(&content)
                .saturating_add((images.len() as u64).saturating_mul(ESTIMATED_IMAGE_CHARS))
        }
    };
    chars.div_ceil(4)
}

/// Input estimate including the current system prompt and advertised tool schemas.
pub fn estimate_request_tokens(request: &ModelRequest) -> u64 {
    let mut system_chars = 0_u64;
    let mut tokens = 0_u64;
    for message in &request.messages {
        if let Message::System { content } = message {
            system_chars = system_chars.saturating_add(text_chars(content));
        } else {
            tokens = tokens.saturating_add(estimate_message_tokens(message));
        }
    }
    if !request.tools.is_empty()
        && let Ok(tools) = serde_json::to_string(&request.tools)
    {
        system_chars = system_chars.saturating_add(text_chars(&tools));
    }
    tokens.saturating_add(system_chars.div_ceil(4))
}

pub(crate) fn estimate_source_tokens(
    session: &crate::Session,
    source: u64,
    message: &Message,
) -> u64 {
    match session
        .records()
        .get(source.saturating_sub(1) as usize)
        .map(|record| &record.event)
    {
        Some(Event::BashExecution { execution }) => text_chars(&execution.command)
            .saturating_add(text_chars(&execution.output))
            .div_ceil(4),
        _ => estimate_message_tokens(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_uses_utf16_units_and_ignores_role_envelopes() {
        for (text, expected) in [
            ("", 0),
            ("abcd", 1),
            ("abcde", 2),
            ("日本語", 1),
            ("🦀🦀🦀", 2),
        ] {
            assert_eq!(
                estimate_message_tokens(&Message::User {
                    content: text.into()
                }),
                expected
            );
        }
        assert_eq!(
            estimate_message_tokens(&Message::UserWithImages {
                content: "hello".into(),
                images: vec![crate::ImageContent {
                    mime_type: "image/png".into(),
                    data: "a".repeat(5000)
                }],
            }),
            1202
        );
    }

    #[test]
    fn structured_tool_images_have_a_fixed_cost_and_preserve_original_data() {
        let result = |data: &str| Message::Tool {
            is_structured: true,
            is_error: false,
            call_id: "lookup".into(),
            content: serde_json::json!({"data":{"content":[
                {"type":"text","text":"image"},
                {"type":"image","mimeType":"image/png","data":data}
            ]}})
            .to_string(),
        };
        let short = result("AA==");
        let long = result(&"A".repeat(5000));
        assert_eq!(
            estimate_message_tokens(&short),
            estimate_message_tokens(&long)
        );
        assert!(estimate_message_tokens(&short) >= 1200);
        assert!(
            matches!(long, Message::Tool { content, .. } if content.contains(&"A".repeat(5000)))
        );
    }

    #[test]
    fn plain_json_tool_output_is_counted_as_text() {
        let content = serde_json::json!({"data":{"content":[{"type":"image","mimeType":"image/png","data":"A".repeat(5000)}]}}).to_string();
        let plain = Message::Tool {
            call_id: "id".into(),
            content: content.clone(),
            is_error: false,
            is_structured: false,
        };
        let structured = Message::Tool {
            call_id: "id".into(),
            content,
            is_error: false,
            is_structured: true,
        };
        assert!(estimate_message_tokens(&plain) > estimate_message_tokens(&structured));
    }

    #[test]
    fn tool_calls_count_name_and_normalized_arguments() {
        assert_eq!(
            estimate_message_tokens(&Message::Assistant {
                content: "ok".into(),
                replay: None,
                calls: vec![crate::ToolCall {
                    id: "not-counted".into(),
                    name: "read".into(),
                    arguments: "{ \"path\" : \"a\" }".into()
                }],
            }),
            5
        );
    }
}
