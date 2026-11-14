use pi_rs_core::{Error, Message, ModelRequest, ProviderReplay, Result, ToolCall};
use serde_json::{Value, json};
pub(crate) fn encode_request(request: ModelRequest, replay_scope: &str) -> Result<Value> {
    let mut system = Vec::new();
    let mut turns: Vec<(&str, Vec<Value>)> = Vec::new();
    for message in request.messages {
        let (role, blocks) = match message {
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
