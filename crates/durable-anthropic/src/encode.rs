use pi_rs_core::{Error, Message, ModelRequest, ProviderReplay, Result, ToolCall};
use serde_json::{Value, json};
pub(crate) fn encode_request(request: ModelRequest, replay_scope: &str) -> Result<Value> {
    encode_request_with_blocks(request, replay_scope, None)
}
pub(crate) fn encode_request_with_blocks(
    request: ModelRequest,
    replay_scope: &str,
    durable: Option<&pi_rs_durable::model::DurableRequest>,
) -> Result<Value> {
    if let Some(durable) = durable {
        let mut ids = std::collections::BTreeSet::new();
        if durable.blocks.iter().any(|(index, binding)| {
            *index >= request.messages.len()
                || binding.message_id.is_nil()
                || !ids.insert(binding.message_id)
        }) {
            return Err(Error::Config("invalid durable content bindings".into()));
        }
    }
    let mut system = Vec::new();
    let mut turns: Vec<(&str, Vec<Value>)> = Vec::new();
    for (index, message) in request.messages.into_iter().enumerate() {
        let (role, blocks) = if let Some(binding) = durable.and_then(|d| d.blocks.get(&index)) {
            if !matches!(&message, Message::User { content } if content.is_empty()) {
                return Err(Error::Config("durable block placeholder mismatch".into()));
            }
            let mut blocks = Vec::new();
            for block in &binding.blocks {
                let object = block
                    .as_object()
                    .ok_or_else(|| Error::Config("invalid content block".into()))?;
                match block["type"].as_str() {
                    Some("text") if object.len() == 2 => {
                        let text = block["text"]
                            .as_str()
                            .ok_or_else(|| Error::Config("invalid text block".into()))?;
                        blocks.push(json!({"type":"text", "text":text}));
                    }
                    Some("image")
                        if object.len() == 3
                            && object
                                .keys()
                                .all(|k| ["type", "data", "mimeType"].contains(&k.as_str())) =>
                    {
                        let image = pi_rs_core::ImageContent {
                            data: block["data"]
                                .as_str()
                                .ok_or_else(|| Error::Config("invalid image".into()))?
                                .into(),
                            mime_type: block["mimeType"]
                                .as_str()
                                .ok_or_else(|| Error::Config("invalid image".into()))?
                                .into(),
                        };
                        image.validate()?;
                        blocks.push(json!({"type":"image","source":{"type":"base64","media_type":image.mime_type,"data":image.data}}));
                    }
                    _ => return Err(Error::Config("unsupported durable block metadata".into())),
                }
            }
            if blocks.is_empty() {
                return Err(Error::Config(
                    "empty user block array unsupported by Anthropic".into(),
                ));
            }
            ("user", blocks)
        } else {
            match message {
                Message::System { content } => {
                    system.push(content);
                    continue;
                }
                Message::User { content } => ("user", text_block(content).into_iter().collect()),
                Message::UserWithImages { content, images } => {
                    let mut blocks: Vec<_> = text_block(content).into_iter().collect();
                    for image in images {
                        image.validate()?;
                        blocks.push(json!({"type":"image","source":{
                            "type":"base64","media_type":image.mime_type,"data":image.data
                        }}));
                    }
                    ("user", blocks)
                }
                Message::Assistant {
                    content,
                    calls,
                    replay,
                } => {
                    let blocks = match replay {
                        Some(replay)
                            if replay_matches(
                                &replay,
                                replay_scope,
                                &request.model,
                                &content,
                                &calls,
                            ) =>
                        {
                            replay.blocks
                        }
                        _ => {
                            let mut blocks: Vec<_> = text_block(content).into_iter().collect();
                            blocks.extend(calls.into_iter().map(|call| tool_block(&call)));
                            blocks
                        }
                    };
                    ("assistant", blocks)
                }
                Message::Tool { call_id, content } => (
                    "user",
                    vec![json!({
                        "type":"tool_result","tool_use_id":call_id,"content":content
                    })],
                ),
            }
        };
        if blocks.is_empty() {
            continue;
        }
        if let Some((previous, content)) = turns.last_mut()
            && *previous == role
        {
            content.extend(blocks);
        } else {
            turns.push((role, blocks));
        }
    }
    if turns.is_empty() {
        return Err(Error::Model(
            "Anthropic requires at least one conversation message".into(),
        ));
    }
    let messages: Vec<_> = turns
        .into_iter()
        .map(|(role, content)| json!({"role":role,"content":content}))
        .collect();
    let mut body = json!({"model":request.model,"messages":messages,"max_tokens":request.max_output_tokens,"stream":true});
    if !system.is_empty() {
        body["system"] = json!(system.join("\n\n"));
    }
    if !request.tools.is_empty() {
        body["tools"] = json!(request.tools.into_iter().map(|tool| {
            json!({"name":tool.name,"description":tool.description,"input_schema":tool.parameters})
        }).collect::<Vec<_>>());
    }
    Ok(body)
}

fn text_block(content: String) -> Option<Value> {
    (!content.is_empty()).then(|| json!({"type":"text","text":content}))
}

fn tool_block(call: &ToolCall) -> Value {
    let input = serde_json::from_str::<Value>(&call.arguments)
        .ok()
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}));
    json!({"type":"tool_use","id":call.id,"name":call.name,"input":input})
}

fn replay_matches(
    replay: &ProviderReplay,
    scope: &str,
    model: &str,
    text: &str,
    calls: &[ToolCall],
) -> bool {
    if replay.identity.provider != scope || replay.identity.model != model {
        return false;
    }
    let rendered = replay
        .blocks
        .iter()
        .filter(|block| block["type"] == "text")
        .filter_map(|block| block["text"].as_str())
        .collect::<String>();
    if rendered != text {
        return false;
    }
    let mut replay_calls = replay
        .blocks
        .iter()
        .filter(|block| block["type"] == "tool_use");
    for call in calls {
        let Some(block) = replay_calls.next() else {
            return false;
        };
        let expected = tool_block(call);
        if ["id", "name", "input"]
            .iter()
            .any(|key| block[*key] != expected[*key])
        {
            return false;
        }
    }
    replay_calls.next().is_none()
}
