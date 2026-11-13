//! Character-based estimates for native model-visible content.
use crate::{Event, Message, ModelRequest};

/// Latest usable measured context plus estimates for messages that followed it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContextUsageEstimate {
    pub tokens: u64,
    pub usage_tokens: u64,
    pub trailing_tokens: u64,
    pub last_usage_record: Option<u64>,
}

/// Hash a serializable adapter configuration without persisting its contents.
pub fn fingerprint_model_context<T: serde::Serialize>(configuration: &T) -> Option<String> {
    use sha2::{Digest, Sha256};
    serde_json::to_vec(configuration)
        .ok()
        .map(|bytes| format!("{:x}", Sha256::digest(bytes)))
}

const ESTIMATED_IMAGE_CHARS: u64 = 4800;

fn text_chars(text: &str) -> u64 {
    text.encode_utf16().count() as u64
}

/// Approximate a message using Pi's four UTF-16 units per token and image budget.
pub fn estimate_message_tokens(message: &Message) -> u64 {
    native_message_chars(message).div_ceil(4)
}

fn native_message_chars(message: &Message) -> u64 {
    if let Message::Tool {
        content,
        is_structured,
        ..
    } = message
    {
        let (text, images) = if crate::tool_content_is_structured(content, *is_structured) {
            crate::extract_tool_images(content.clone())
        } else {
            (content.clone(), Vec::new())
        };
        return text_chars(&text)
            .saturating_add((images.len() as u64).saturating_mul(ESTIMATED_IMAGE_CHARS));
    }
    pi_message_chars(message)
}

fn pi_message_chars(message: &Message) -> u64 {
    match message {
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
            let blocks = crate::tool_content_is_structured(content, *is_structured)
                .then(|| serde_json::from_str::<serde_json::Value>(content).ok())
                .flatten();
            blocks
                .as_ref()
                .and_then(|value| value["data"]["content"].as_array())
                .map_or_else(
                    || text_chars(content),
                    |blocks| {
                        blocks.iter().fold(0_u64, |chars, block| {
                            chars.saturating_add(match block["type"].as_str() {
                                Some("text") => block["text"].as_str().map(text_chars).unwrap_or(0),
                                Some("image") => ESTIMATED_IMAGE_CHARS,
                                _ => 0,
                            })
                        })
                    },
                )
        }
    }
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

/// Estimate serialized replay blocks, replacing recognized inline image bytes
/// with the same fixed image allowance used for ordinary image messages.
pub fn estimate_replay_tokens(blocks: &[serde_json::Value]) -> u64 {
    fn strip_images(value: &mut serde_json::Value) -> u64 {
        match value {
            serde_json::Value::String(text)
                if text.starts_with("data:image/") && text.contains(";base64,") =>
            {
                text.clear();
                1
            }
            serde_json::Value::Array(items) => items
                .iter_mut()
                .map(strip_images)
                .fold(0, u64::saturating_add),
            serde_json::Value::Object(object) => {
                let inline_image = object
                    .get("mimeType")
                    .or_else(|| object.get("media_type"))
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|mime| mime.starts_with("image/"));
                let image_block =
                    object.get("type").and_then(serde_json::Value::as_str) == Some("image");
                let mut images: u64 = 0;
                if (inline_image || image_block)
                    && object.get("data").is_some_and(serde_json::Value::is_string)
                {
                    object.insert("data".into(), serde_json::Value::String(String::new()));
                    images = 1;
                }
                images.saturating_add(
                    object
                        .values_mut()
                        .map(strip_images)
                        .fold(0, u64::saturating_add),
                )
            }
            _ => 0,
        }
    }
    let mut blocks = serde_json::Value::Array(blocks.to_vec());
    let images = strip_images(&mut blocks);
    text_chars(&blocks.to_string())
        .saturating_add(images.saturating_mul(ESTIMATED_IMAGE_CHARS))
        .div_ceil(4)
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
        Some(
            Event::BranchSummary { summary, .. } | Event::CompactionSummaryMessage { summary, .. },
        ) => text_chars(summary).div_ceil(4),
        Some(Event::Assistant { request_id, .. }) => {
            let branch = session.branch();
            let replacement = branch.iter().rev().find_map(|record| match &record.event {
                Event::ContextEdited {
                    target,
                    replacement,
                } if *target == source => Some(replacement),
                _ => None,
            });
            if replacement.is_none()
                && let Some(original) = session.imported_assistant_message(source)
                && matches!(original["stopReason"].as_str(), Some("error" | "aborted"))
                && let Some(blocks) = original["content"].as_array()
            {
                let chars = blocks.iter().fold(0_u64, |chars, block| {
                    chars.saturating_add(match block["type"].as_str() {
                        Some("text") => block["text"].as_str().map(text_chars).unwrap_or(0),
                        Some("thinking") => block["thinking"].as_str().map(text_chars).unwrap_or(0),
                        Some("toolCall") => block["name"]
                            .as_str()
                            .map(text_chars)
                            .unwrap_or(0)
                            .saturating_add(text_chars(&block["arguments"].to_string())),
                        _ => 0,
                    })
                });
                return chars.div_ceil(4);
            }
            let reasoning = match replacement {
                Some(Some(crate::ContextContent::Blocks(blocks))) => {
                    blocks.iter().fold(0_u64, |chars, block| match block {
                        crate::ContentBlock::Thinking { thinking } => {
                            chars.saturating_add(text_chars(thinking))
                        }
                        _ => chars,
                    })
                }
                Some(_) => 0,
                None => request_id
                    .and_then(|request_id| {
                        branch.iter().find_map(|record| match &record.event {
                            Event::ModelFinished {
                                request_id: id,
                                metadata,
                                ..
                            } if *id == request_id => metadata.reasoning.as_deref(),
                            _ => None,
                        })
                    })
                    .map(text_chars)
                    .unwrap_or(0),
            };
            pi_message_chars(message)
                .saturating_add(reasoning)
                .div_ceil(4)
        }
        _ => pi_message_chars(message).div_ceil(4),
    }
}

pub(crate) fn estimate_budget_source_tokens(
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

/// A trailing Pi system message retains its original blocks for estimation.
pub(crate) fn estimate_system_delta(session: &crate::Session, record: &crate::Record) -> u64 {
    if let Event::SystemCheckpoint {
        sections,
        tools_added,
        ..
    } = &record.event
        && let Some(parent) = record
            .parent
            .and_then(|id| session.records().get(id.saturating_sub(1) as usize))
        && let Event::SourceEntry { format, entry } = &parent.event
        && format == "pi"
        && entry["type"] == "message"
        && entry["message"]["role"] == "system"
        && let Some(blocks) = entry["message"]["content"].as_array()
    {
        let mut chars = blocks.iter().fold(0_u64, |chars, block| {
            chars.saturating_add(match block["type"].as_str() {
                Some("text") => block["text"].as_str().map(text_chars).unwrap_or(0),
                Some("image") => ESTIMATED_IMAGE_CHARS,
                _ => 0,
            })
        });
        for (_, value) in sections {
            if let Some(value) = value {
                chars = chars.saturating_add(text_chars(value));
            }
        }
        if !tools_added.is_empty() {
            chars = chars.saturating_add(text_chars(
                &serde_json::to_string(tools_added).unwrap_or_default(),
            ));
        }
        return chars.div_ceil(4);
    }
    estimate_system_tokens(&[record])
}

pub(crate) fn estimate_system_tokens(branch: &[&crate::Record]) -> u64 {
    let mut contents = Vec::new();
    let mut sections = std::collections::BTreeMap::new();
    let mut tools: Vec<crate::ToolSpec> = Vec::new();
    for (index, record) in branch.iter().enumerate() {
        if matches!(
            record.event,
            Event::Compacted {
                system_prompt: Some(_),
                ..
            }
        ) || (matches!(record.event, Event::Compacted { .. })
            && imported_entry(branch, index, "compaction"))
        {
            contents.clear();
            sections.clear();
            tools.clear();
        }
        let (content, changes, added, removed) = match &record.event {
            Event::SystemCheckpoint {
                content,
                sections,
                tools_added,
                tools_removed,
            } => (content, sections, tools_added, tools_removed.as_slice()),
            Event::Compacted {
                system_prompt: Some(snapshot),
                ..
            } => (
                &snapshot.content,
                &snapshot.sections,
                &snapshot.tools,
                &[][..],
            ),
            _ => continue,
        };
        if !content.is_empty() {
            contents.push(content.as_str());
        }
        for (name, value) in changes {
            if let Some(value) = value {
                sections.insert(name, value);
            } else {
                sections.remove(name);
            }
        }
        tools.retain(|tool| !removed.contains(&tool.name));
        for tool in added {
            if let Some(index) = tools.iter().position(|item| item.name == tool.name) {
                tools[index] = tool.clone();
            } else {
                tools.push(tool.clone());
            }
        }
    }
    let mut chars = text_chars(&contents.join("\n\n"));
    for value in sections.values() {
        chars = chars.saturating_add(text_chars(value));
    }
    if !tools.is_empty() {
        chars = chars.saturating_add(text_chars(
            &serde_json::to_string(&tools).unwrap_or_default(),
        ));
    }
    chars.div_ceil(4)
}

fn imported_entry(branch: &[&crate::Record], index: usize, kind: &str) -> bool {
    branch[..index]
        .iter()
        .rev()
        .find_map(|record| match &record.event {
            Event::ModelFinished { reply: None, .. } => None,
            Event::SourceEntry { format, entry } => Some(format == "pi" && entry["type"] == kind),
            _ => Some(false),
        })
        .unwrap_or(false)
}

/// Native system checkpoints update a single runtime prompt; changes that
/// replace existing state make earlier measured usage unsuitable as a base.
pub(crate) fn non_additive_native_system_checkpoints(
    branch: &[&crate::Record],
) -> std::collections::BTreeSet<u64> {
    let mut sections = std::collections::BTreeSet::new();
    let mut tools = std::collections::BTreeSet::new();
    let mut invalidations = std::collections::BTreeSet::new();
    for (index, record) in branch.iter().enumerate() {
        let (changes, added, removed) = match &record.event {
            Event::SystemCheckpoint {
                sections,
                tools_added,
                tools_removed,
                ..
            } => (sections, tools_added, tools_removed.as_slice()),
            Event::Compacted {
                system_prompt: Some(snapshot),
                ..
            } => {
                sections.clear();
                tools.clear();
                (&snapshot.sections, &snapshot.tools, &[][..])
            }
            Event::Compacted {
                system_prompt: None,
                ..
            } if imported_entry(branch, index, "compaction") => {
                sections.clear();
                tools.clear();
                continue;
            }
            _ => continue,
        };
        let non_additive = changes.iter().any(|(name, _)| sections.contains(name))
            || removed.iter().any(|name| tools.contains(name))
            || added.iter().any(|tool| tools.contains(&tool.name));
        if non_additive
            && matches!(record.event, Event::SystemCheckpoint { .. })
            && !imported_entry(branch, index, "message")
        {
            invalidations.insert(record.id);
        }
        for (name, value) in changes {
            if value.is_some() {
                sections.insert(name.clone());
            } else {
                sections.remove(name);
            }
        }
        for name in removed {
            tools.remove(name);
        }
        for tool in added {
            tools.insert(tool.name.clone());
        }
    }
    invalidations
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_inline_images_use_fixed_allowance_but_signatures_remain_text() {
        let block =
            |n| serde_json::json!({"inlineData":{"mimeType":"image/png","data":"A".repeat(n)}});
        assert_eq!(
            estimate_replay_tokens(&[block(4)]),
            estimate_replay_tokens(&[block(20000)])
        );
        let signature = serde_json::json!({"thoughtSignature":"x".repeat(5000)});
        assert!(estimate_replay_tokens(&[signature]) > 1200);
    }

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
            is_structured: Some(true),
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
        assert_eq!(pi_message_chars(&short).div_ceil(4), 1202);
        assert!(
            matches!(long, Message::Tool { content, .. } if content.contains(&"A".repeat(5000)))
        );
    }

    #[test]
    fn structured_tool_text_and_images_ignore_envelope_and_metadata() {
        let message = Message::Tool {
            call_id: "id".into(), is_error: false, is_structured: Some(true),
            content: serde_json::json!({"data":{"content":[{"type":"text","text":"a"},{"type":"image","data":"AA==","mimeType":"image/png"},{"type":"text","text":"🦀"}],"extra":"irrelevant"},"attachments":[]}).to_string(),
        };
        assert_eq!(pi_message_chars(&message).div_ceil(4), 1201);
    }

    #[test]
    fn native_request_budget_preserves_envelope_and_fixed_image_allowance() {
        let content = serde_json::json!({"data":{"content":[{"type":"text","text":"ok"},{"type":"image","data":"AA==","mimeType":"image/png"}],"extra":"x".repeat(12000)},"attachments":[]}).to_string();
        let message = Message::Tool {
            call_id: "call".into(),
            content: content.clone(),
            is_error: false,
            is_structured: Some(true),
        };
        let request = ModelRequest {
            model: "fixture".into(),
            messages: vec![message.clone()],
            tools: vec![],
            max_output_tokens: 1,
        };
        let (extracted, images) = crate::extract_tool_images(content.clone());
        assert_eq!(pi_message_chars(&message).div_ceil(4), 1201);
        assert_eq!(
            estimate_request_tokens(&request),
            (text_chars(&extracted) + images.len() as u64 * ESTIMATED_IMAGE_CHARS).div_ceil(4)
        );
        assert!(estimate_request_tokens(&request) > 4000);
        let mut large_image = request;
        let expected = if let Message::Tool { content, .. } = &mut large_image.messages[0] {
            *content = serde_json::json!({"data":{"content":[{"type":"image","data":"A".repeat(20000),"mimeType":"image/png"}]}}).to_string();
            let (text, images) = crate::extract_tool_images(content.clone());
            (text_chars(&text) + images.len() as u64 * ESTIMATED_IMAGE_CHARS).div_ceil(4)
        } else {
            unreachable!()
        };
        assert_eq!(estimate_request_tokens(&large_image), expected);
    }

    #[test]
    fn plain_json_tool_output_is_counted_as_text() {
        let content = serde_json::json!({"data":{"content":[{"type":"image","mimeType":"image/png","data":"A".repeat(5000)}]}}).to_string();
        let plain = Message::Tool {
            call_id: "id".into(),
            content: content.clone(),
            is_error: false,
            is_structured: Some(false),
        };
        let structured = Message::Tool {
            call_id: "id".into(),
            content,
            is_error: false,
            is_structured: Some(true),
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
