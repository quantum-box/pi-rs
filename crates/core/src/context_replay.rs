//! Provider-specific replay for content-only edits, built after tool-pair repair.
use crate::{ContentBlock, ContextContent, ModelApi, ModelMetadata, ProviderReplay, ToolCall};
use serde_json::{Value, json};

pub(crate) fn replacement_replay(
    replacement: &ContextContent,
    metadata: &ModelMetadata,
    calls: &[ToolCall],
) -> Option<Box<ProviderReplay>> {
    let ContextContent::Blocks(blocks) = replacement else {
        return None;
    };
    let identity = metadata
        .replay
        .as_ref()
        .map(|r| r.identity.clone())
        .or_else(|| metadata.replay_identity.clone())
        .or_else(|| metadata.resolved_model.clone())?;
    let api = metadata.provider_api?;
    let mut result = Vec::new();
    if api == ModelApi::PiMessages {
        // The gateway binds replay to its endpoint as well as provider and model.
        result.push(metadata.replay.as_ref()?.blocks.first()?.clone());
    }
    for block in blocks {
        let value = match (api, block) {
            (ModelApi::PiMessages, ContentBlock::ToolCall { id, .. })
                if !calls.iter().any(|call| call.id == *id) =>
            {
                None
            }
            (ModelApi::PiMessages, _) => serde_json::to_value(block).ok(),
            (ModelApi::AnthropicMessages, ContentBlock::Text { text }) => {
                Some(json!({"type":"text","text":text}))
            }
            (
                ModelApi::AnthropicMessages,
                ContentBlock::Thinking {
                    thinking,
                    thinking_signature,
                    redacted,
                },
            ) => {
                let signature = thinking_signature
                    .as_deref()
                    .filter(|s| !s.trim().is_empty());
                if *redacted {
                    signature.map(|s| json!({"type":"redacted_thinking","data":s}))
                } else if let Some(signature) = signature {
                    Some(json!({"type":"thinking","thinking":thinking,"signature":signature}))
                } else if !thinking.trim().is_empty() {
                    Some(json!({"type":"text","text":thinking,"_pi_thinking":true}))
                } else {
                    None
                }
            }
            (ModelApi::GoogleGenerativeAi, ContentBlock::Text { text }) => {
                Some(json!({"text":text}))
            }
            (
                ModelApi::GoogleGenerativeAi,
                ContentBlock::Thinking {
                    thinking,
                    thinking_signature,
                    redacted: _,
                },
            ) => {
                if thinking.trim().is_empty()
                    && thinking_signature.as_deref().unwrap_or("").is_empty()
                {
                    None
                } else {
                    let mut part = json!({"thought":true,"text":thinking});
                    if let Some(s) = thinking_signature.as_deref().filter(|s| !s.is_empty()) {
                        part["thoughtSignature"] = json!(s);
                    }
                    Some(part)
                }
            }
            (ModelApi::BedrockConverseStream, ContentBlock::Text { text }) => {
                Some(json!({"type":"text","text":text}))
            }
            (
                ModelApi::BedrockConverseStream,
                ContentBlock::Thinking {
                    thinking,
                    thinking_signature,
                    redacted,
                },
            ) => {
                if *redacted {
                    thinking_signature
                        .as_ref()
                        .map(|s| json!({"type":"reasoning","redacted":s}))
                } else if !thinking.is_empty() {
                    Some(json!({"type":"reasoning","text":thinking,"signature":thinking_signature}))
                } else {
                    None
                }
            }
            (ModelApi::OpenaiResponses, ContentBlock::Text { text }) => {
                Some(json!({"role":"assistant","content":text}))
            }
            (
                ModelApi::OpenaiResponses,
                ContentBlock::Thinking {
                    thinking_signature, ..
                },
            ) => thinking_signature
                .as_deref()
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
                .filter(|v| v["type"] == "reasoning"),
            (
                ModelApi::OpenaiCompletions,
                ContentBlock::Thinking {
                    thinking,
                    thinking_signature,
                    ..
                },
            ) => {
                if let Some(Value::Array(details)) = thinking_signature
                    .as_deref()
                    .and_then(|s| serde_json::from_str::<Value>(s).ok())
                {
                    result.extend(details);
                    None
                } else if !thinking.is_empty() {
                    Some(
                        json!({"type":"context_edit_reasoning","text":thinking,"field":thinking_signature}),
                    )
                } else {
                    None
                }
            }
            (
                _,
                ContentBlock::ToolCall {
                    id,
                    name,
                    arguments,
                },
            ) if calls.iter().any(|call| call.id == *id) => match api {
                ModelApi::AnthropicMessages | ModelApi::BedrockConverseStream => {
                    Some(json!({"type":"tool_use","id":id,"name":name,"input":arguments}))
                }
                ModelApi::GoogleGenerativeAi => {
                    Some(json!({"functionCall":{"id":id,"name":name,"args":arguments}}))
                }
                ModelApi::OpenaiResponses => Some(
                    json!({"type":"function_call","call_id":id,"name":name,"arguments":arguments.to_string()}),
                ),
                _ => None,
            },
            _ => None,
        };
        if let Some(value) = value {
            result.push(value);
        }
    }
    // The chat-completions adapter constructs visible text and calls itself.
    if result.is_empty() || (api == ModelApi::PiMessages && result.len() == 1) {
        return None;
    }
    result.push(json!({"_pi_context_edit_api": api.as_str()}));
    Some(Box::new(ProviderReplay {
        identity,
        blocks: result,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Event, MemoryStore, Message, ResolvedModelIdentity, Session, UsageReport};
    use uuid::Uuid;

    #[test]
    fn edited_thinking_survives_projection_and_reopen_with_order_and_signatures() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let request_id = Uuid::new_v4();
        session
            .append(Event::ModelFinished {
                request_id,
                metadata: ModelMetadata {
                    resolved_model: Some(ResolvedModelIdentity {
                        provider: "anthropic".into(),
                        model: "claude".into(),
                    }),
                    provider_api: Some(ModelApi::AnthropicMessages),
                    ..Default::default()
                },
                usage: UsageReport::Unconfirmed,
                failure: None,
                partial: String::new(),
                reply: None,
            })
            .unwrap();
        let target = session
            .append(Event::Assistant {
                content: "old".into(),
                invocations: vec![],
                usage: None,
                request_id: Some(request_id),
            })
            .unwrap()
            .id;
        let replacement: ContextContent = serde_json::from_value(json!([
            {"type":"thinking","thinking":"private","thinkingSignature":"sig"},
            {"type":"text","text":"visible"},
            {"type":"thinking","thinking":"","thinkingSignature":"opaque","redacted":true}
        ]))
        .unwrap();
        session.edit_context(target, Some(replacement)).unwrap();
        let fork = Session::from_records(session.fork_records(session.head()).unwrap()).unwrap();
        for session in [&session, &fork] {
            let messages = session.current_messages();
            let Message::Assistant {
                content,
                replay: Some(replay),
                ..
            } = &messages[0]
            else {
                panic!("missing replay")
            };
            assert_eq!(content, "visible");
            assert_eq!(
                replay.blocks,
                vec![
                    json!({"type":"thinking","thinking":"private","signature":"sig"}),
                    json!({"type":"text","text":"visible"}),
                    json!({"type":"redacted_thinking","data":"opaque"}),
                    json!({"_pi_context_edit_api":"anthropic-messages"})
                ]
            );
            assert_eq!(replay.identity.provider, "anthropic");
        }
        session.checkout(target).unwrap();
        assert!(
            matches!(&session.current_messages()[0], Message::Assistant { content, replay: None, .. } if content == "old")
        );
    }

    #[test]
    fn deleting_tool_result_keeps_thinking_without_replaying_orphaned_call() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let request_id = Uuid::new_v4();
        let operation_id = Uuid::new_v4();
        session
            .append(Event::ModelFinished {
                request_id,
                metadata: ModelMetadata {
                    provider_api: Some(ModelApi::AnthropicMessages),
                    replay_identity: Some(ResolvedModelIdentity {
                        provider: "anthropic".into(),
                        model: "claude".into(),
                    }),
                    ..Default::default()
                },
                usage: UsageReport::Unconfirmed,
                failure: None,
                partial: String::new(),
                reply: None,
            })
            .unwrap();
        let target = session
            .append(Event::Assistant {
                content: String::new(),
                usage: None,
                request_id: Some(request_id),
                invocations: vec![crate::Invocation {
                    operation_id,
                    call: ToolCall {
                        id: "call".into(),
                        name: "read".into(),
                        arguments: "{}".into(),
                    },
                }],
            })
            .unwrap()
            .id;
        let result = session
            .append(Event::ToolFinished {
                operation_id,
                outcome: crate::Outcome::Succeeded("ok".into()),
                states: Default::default(),
            })
            .unwrap()
            .id;
        let replacement = serde_json::from_value(json!([{"type":"thinking","thinking":"secret","thinkingSignature":"sig"},{"type":"toolCall","id":"call","name":"read","arguments":{}}])).unwrap();
        session.edit_context(target, Some(replacement)).unwrap();
        session.edit_context(result, None).unwrap();
        let messages = session.current_messages();
        let [
            Message::Assistant {
                calls,
                replay: Some(replay),
                ..
            },
        ] = messages.as_slice()
        else {
            panic!("missing thinking-only message")
        };
        assert!(calls.is_empty());
        assert_eq!(
            replay.blocks,
            vec![
                json!({"type":"thinking","thinking":"secret","signature":"sig"}),
                json!({"_pi_context_edit_api":"anthropic-messages"})
            ]
        );
    }

    #[test]
    fn replacement_replay_obeys_provider_formats_and_repaired_calls() {
        let replacement: ContextContent = serde_json::from_value(json!([
            {"type":"text","text":"before"},
            {"type":"thinking","thinking":"reason","thinkingSignature":"signature"},
            {"type":"toolCall","id":"missing","name":"read","arguments":{}},
            {"type":"text","text":"after"}
        ]))
        .unwrap();
        let metadata = |api| ModelMetadata {
            provider_api: Some(api),
            replay_identity: Some(ResolvedModelIdentity {
                provider: "scope".into(),
                model: "model".into(),
            }),
            ..Default::default()
        };
        let google =
            replacement_replay(&replacement, &metadata(ModelApi::GoogleGenerativeAi), &[]).unwrap();
        assert_eq!(
            google.blocks,
            vec![
                json!({"text":"before"}),
                json!({"thought":true,"text":"reason","thoughtSignature":"signature"}),
                json!({"text":"after"}),
                json!({"_pi_context_edit_api":"google-generative-ai"})
            ]
        );
        assert!(google.supports_api(ModelApi::GoogleGenerativeAi));
        assert!(!google.supports_api(ModelApi::AnthropicMessages));
        let bedrock = replacement_replay(
            &replacement,
            &metadata(ModelApi::BedrockConverseStream),
            &[],
        )
        .unwrap();
        assert_eq!(
            bedrock.blocks[1],
            json!({"type":"reasoning","text":"reason","signature":"signature"})
        );
        assert!(bedrock.blocks.iter().all(|b| b["type"] != "tool_use"));
    }

    #[test]
    fn thinking_only_replacement_is_retained_and_old_json_defaults_work() {
        let old: ContentBlock =
            serde_json::from_value(json!({"type":"thinking","thinking":"x"})).unwrap();
        assert!(matches!(
            old,
            ContentBlock::Thinking {
                thinking_signature: None,
                redacted: false,
                ..
            }
        ));
        let replacement: ContextContent = serde_json::from_value(
            json!([{"type":"thinking","thinking":"","thinkingSignature":"opaque","redacted":true}]),
        )
        .unwrap();
        let metadata = ModelMetadata {
            provider_api: Some(ModelApi::AnthropicMessages),
            resolved_model: Some(ResolvedModelIdentity {
                provider: "anthropic".into(),
                model: "claude".into(),
            }),
            ..Default::default()
        };
        assert_eq!(
            replacement_replay(&replacement, &metadata, &[])
                .unwrap()
                .blocks,
            vec![
                json!({"type":"redacted_thinking","data":"opaque"}),
                json!({"_pi_context_edit_api":"anthropic-messages"})
            ]
        );
    }
}
