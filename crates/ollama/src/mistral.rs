//! Wire details specific to the Mistral chat endpoint.
use std::collections::{HashMap, HashSet};

use pi_rs_core::{Error, ImageContent, Message, ModelApi, ResolvedModelIdentity, Result};
use serde_json::{Value, json};

#[derive(Clone, Debug, Default)]
pub(super) struct Options {
    pub images: bool,
    pub prompt_mode: bool,
    pub effort: Option<String>,
}

pub(super) fn messages(
    input: Vec<Message>,
    identity: &ResolvedModelIdentity,
    supports_images: bool,
) -> Result<Vec<Value>> {
    let mut messages = Vec::with_capacity(input.len());
    let mut ids = CallIds::default();
    let mut names = HashMap::new();
    for message in input {
        match message {
            Message::System { content } if !content.is_empty() => {
                messages.push(json!({"role":"system","content":content}));
            }
            Message::System { .. } => {}
            Message::User { content } => messages.push(json!({"role":"user","content":content})),
            Message::UserWithImages { content, images } => {
                let mut content_parts = Vec::new();
                if !content.is_empty() {
                    content_parts.push(text_part(&content));
                }
                if supports_images {
                    content_parts.extend(images.into_iter().map(image_part));
                } else if content_parts.is_empty() && !images.is_empty() {
                    messages.push(json!({"role":"user","content":"(image omitted: model does not support images)"}));
                    continue;
                }
                if !content_parts.is_empty() {
                    messages.push(json!({"role":"user","content":content_parts}));
                }
            }
            Message::Assistant {
                content,
                calls,
                replay,
            } => {
                let parts = replay
                    .as_deref()
                    .filter(|replay| {
                        replay.identity == *identity
                            && replay.supports_api(ModelApi::MistralConversations)
                    })
                    .and_then(|replay| replay_parts(&replay.blocks))
                    .unwrap_or_else(|| {
                        (!content.trim().is_empty())
                            .then(|| text_part(&content))
                            .into_iter()
                            .collect()
                    });
                if parts.is_empty() && calls.is_empty() {
                    continue;
                }
                let mut wire = json!({"role":"assistant","prefix":false});
                if !parts.is_empty() {
                    wire["content"] = json!(parts);
                }
                if !calls.is_empty() {
                    wire["tool_calls"] = json!(
                        calls
                            .into_iter()
                            .map(|call| {
                                let id = ids.normalize(&call.id);
                                names.insert(call.id, call.name.clone());
                                json!({"id":id,"type":"function","index":0,"function":{
                                    "name":call.name,"arguments":call.arguments
                                }})
                            })
                            .collect::<Vec<_>>()
                    );
                }
                messages.push(wire);
            }
            Message::Tool {
                call_id,
                content,
                is_error,
                is_structured,
            } => {
                let name = names.get(&call_id).ok_or_else(|| {
                    Error::Model("Mistral tool result has no preceding call".into())
                })?;
                let (text, images) =
                    if pi_rs_core::tool_content_is_structured(&content, is_structured) {
                        pi_rs_core::extract_tool_images(content)
                    } else {
                        (content, Vec::new())
                    };
                let projected = if pi_rs_core::tool_content_is_structured(&text, is_structured) {
                    serde_json::from_str::<Value>(&text).ok().and_then(|data| {
                        let blocks = data.pointer("/data/content")?.as_array()?;
                        Some(
                            blocks
                                .iter()
                                .filter_map(|block| {
                                    (block["type"] == "text")
                                        .then(|| block["text"].as_str())
                                        .flatten()
                                })
                                .collect::<Vec<_>>()
                                .join("\n"),
                        )
                    })
                } else {
                    None
                };
                let text = tool_text(
                    projected.as_deref().unwrap_or(&text),
                    !images.is_empty(),
                    supports_images,
                    is_error,
                );
                let mut parts = vec![text_part(&text)];
                if supports_images {
                    parts.extend(images.into_iter().map(image_part));
                }
                messages.push(json!({"role":"tool","tool_call_id":ids.normalize(&call_id),
                    "name":name,"content":parts}));
            }
        }
    }
    Ok(messages)
}

fn replay_parts(blocks: &[Value]) -> Option<Vec<Value>> {
    if let Some(parts) = blocks
        .iter()
        .find_map(|block| block.get("mistral_content")?.as_array())
    {
        return Some(parts.clone());
    }
    let parts = blocks
        .iter()
        .filter_map(|block| {
            let text = block["text"].as_str()?.trim();
            if text.is_empty() {
                return None;
            }
            match block["type"].as_str()? {
                "context_edit_text" => Some(text_part(text)),
                "context_edit_reasoning" => Some(thinking_part(text)),
                _ => None,
            }
        })
        .collect::<Vec<_>>();
    (!parts.is_empty()).then_some(parts)
}

fn thinking_part(text: &str) -> Value {
    json!({"type":"thinking","thinking":[text_part(text)]})
}

fn text_part(text: &str) -> Value {
    json!({"type":"text","text":text})
}

fn image_part(image: ImageContent) -> Value {
    json!({"type":"image_url","image_url":format!("data:{};base64,{}",image.mime_type,image.data)})
}

fn tool_text(text: &str, images: bool, supports_images: bool, error: bool) -> String {
    let prefix = if error { "[tool error] " } else { "" };
    let text = text.trim();
    match (text.is_empty(), images, supports_images) {
        (false, true, false) => {
            format!("{prefix}{text}\n[tool image omitted: model does not support images]")
        }
        (false, _, _) => format!("{prefix}{text}"),
        (true, true, true) => format!("{prefix}(see attached image)"),
        (true, true, false) => format!("{prefix}(image omitted: model does not support images)"),
        (true, false, _) => format!("{prefix}(no tool output)"),
    }
}

#[derive(Default)]
struct CallIds {
    names: HashMap<String, String>,
    used: HashSet<String>,
}

impl CallIds {
    fn normalize(&mut self, original: &str) -> String {
        self.names
            .entry(original.to_owned())
            .or_insert_with(|| {
                (0..)
                    .map(|attempt| call_id(original, attempt))
                    .find(|id| self.used.insert(id.clone()))
                    .expect("tool call IDs have an unbounded retry sequence")
            })
            .clone()
    }
}

pub(super) fn stream_call_index(
    id: Option<&str>,
    function: Option<&super::WireFunction>,
    position: usize,
    had_calls: bool,
    calls: &std::collections::BTreeMap<usize, super::CallDelta>,
) -> Result<usize> {
    if let Some(id) = id.filter(|id| !id.is_empty() && *id != "null") {
        if let Some(index) = calls
            .iter()
            .find_map(|(index, call)| (call.id == id).then_some(*index))
        {
            return Ok(index);
        }
        if had_calls && calls.values().any(|call| call.id.is_empty()) {
            if let Some((index, active)) = calls.first_key_value()
                && calls.len() == 1
                && position == 0
                && function.is_none_or(|function| {
                    let name = function.name.as_deref().unwrap_or_default();
                    let has_arguments = function
                        .arguments
                        .as_ref()
                        .is_some_and(|value| !value.as_str().is_some_and(str::is_empty));
                    name.is_empty()
                        || (name == active.name
                            && (!has_arguments
                                || (!active.arguments.is_empty()
                                    && serde_json::from_str::<Value>(&active.arguments)
                                        .is_err_and(|error| error.is_eof()))))
                })
            {
                return Ok(*index);
            }
            let starts_new_call = function
                .and_then(|function| function.name.as_deref())
                .filter(|name| !name.is_empty())
                .is_some_and(|name| {
                    calls
                        .values()
                        .filter(|call| call.id.is_empty())
                        .all(|call| {
                            !call.name.is_empty()
                                && call.name != name
                                && serde_json::from_str::<Value>(&call.arguments)
                                    .is_ok_and(|args| args.is_object())
                        })
                });
            if !starts_new_call {
                return Err(Error::Model(
                    "ambiguous Mistral tool call ID without an index".into(),
                ));
            }
        }
        return Ok((position..64)
            .find(|index| !calls.contains_key(index))
            .unwrap_or(64));
    }
    if !had_calls {
        return Ok((position..64)
            .find(|index| !calls.contains_key(index))
            .unwrap_or(64));
    }
    if calls.len() == 1 && position == 0 {
        return Ok(*calls.first_key_value().expect("one active tool call").0);
    }
    Err(Error::Model(
        "ambiguous Mistral tool call continuation without an ID or index".into(),
    ))
}

pub(super) fn call_id(original: &str, attempt: usize) -> String {
    let normalized: String = original
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect();
    if attempt == 0 && normalized.len() == 9 {
        return normalized;
    }
    let base = if normalized.is_empty() {
        original
    } else {
        &normalized
    };
    let seed = if attempt == 0 {
        base.to_owned()
    } else {
        format!("{base}:{attempt}")
    };
    let mut first = 0xdead_beefu32;
    let mut second = 0x41c6_ce57u32;
    for unit in seed.encode_utf16().map(u32::from) {
        first = (first ^ unit).wrapping_mul(2654435761);
        second = (second ^ unit).wrapping_mul(1597334677);
    }
    first = (first ^ (first >> 16)).wrapping_mul(2246822507)
        ^ (second ^ (second >> 13)).wrapping_mul(3266489909);
    second = (second ^ (second >> 16)).wrapping_mul(2246822507)
        ^ (first ^ (first >> 13)).wrapping_mul(3266489909);
    format!("{}{}", base36(second), base36(first))
        .chars()
        .take(9)
        .collect()
}

fn base36(mut number: u32) -> String {
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut result = Vec::new();
    loop {
        result.push(char::from(DIGITS[(number % 36) as usize]));
        number /= 36;
        if number == 0 {
            return result.into_iter().rev().collect();
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Segment {
    Text(String),
    Thinking(String),
}

pub(super) fn streamed_segments(content: Option<Value>) -> Vec<Segment> {
    let parts = match content {
        Some(Value::String(text)) => {
            return (!text.is_empty())
                .then_some(Segment::Text(text))
                .into_iter()
                .collect();
        }
        Some(Value::Array(parts)) => parts,
        _ => return Vec::new(),
    };
    parts
        .into_iter()
        .filter_map(|part| {
            if let Some(text) = part.as_str() {
                return (!text.is_empty()).then(|| Segment::Text(text.to_owned()));
            }
            match part["type"].as_str() {
                Some("thinking") => {
                    let text: String = part["thinking"]
                        .as_array()?
                        .iter()
                        .filter_map(|part| part["text"].as_str())
                        .collect();
                    (!text.is_empty()).then_some(Segment::Thinking(text))
                }
                Some("text") => {
                    let text = part["text"].as_str()?.to_owned();
                    (!text.is_empty()).then_some(Segment::Text(text))
                }
                _ => None,
            }
        })
        .collect()
}

pub(super) fn record(segments: &mut Vec<Segment>, segment: Segment, new_block: bool) {
    if !new_block {
        match (segments.last_mut(), &segment) {
            (Some(Segment::Text(existing)), Segment::Text(delta))
            | (Some(Segment::Thinking(existing)), Segment::Thinking(delta)) => {
                existing.push_str(delta);
                return;
            }
            _ => {}
        }
    }
    segments.push(segment);
}

pub(super) fn replay_block(segments: Vec<Segment>) -> Value {
    let parts: Vec<_> = segments
        .into_iter()
        .map(|segment| match segment {
            Segment::Text(text) => text_part(&text),
            Segment::Thinking(text) => thinking_part(&text),
        })
        .collect();
    json!({"mistral_content":parts})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Ollama;
    use futures_util::StreamExt;
    use pi_rs_core::{Model, ModelEvent, ModelRequest, ToolCall};
    use std::io::{BufRead, BufReader, Read, Write};

    #[test]
    fn rejects_ambiguous_mistral_tool_continuations() {
        let calls = [0, 1]
            .into_iter()
            .map(|index| {
                (
                    index,
                    crate::CallDelta {
                        id: format!("fixture-{index}"),
                        ..Default::default()
                    },
                )
            })
            .collect();
        let read_delta = super::super::WireFunction {
            name: Some("read".into()),
            arguments: Some(json!("}")),
        };
        let other_delta = super::super::WireFunction {
            name: Some("other".into()),
            arguments: Some(json!("{}")),
        };
        let named_id = super::super::WireFunction {
            name: Some("read".into()),
            arguments: None,
        };
        let anonymous = [(
            0,
            crate::CallDelta {
                name: "read".into(),
                arguments: "{".into(),
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();
        assert_eq!(
            stream_call_index(Some("late"), None, 0, true, &anonymous).unwrap(),
            0
        );
        assert_eq!(
            stream_call_index(Some("late"), Some(&read_delta), 0, true, &anonymous).unwrap(),
            0
        );
        assert!(stream_call_index(Some("late"), Some(&other_delta), 0, true, &anonymous).is_err());
        let complete = [(
            0,
            crate::CallDelta {
                name: "read".into(),
                arguments: "{}".into(),
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();
        assert!(stream_call_index(Some("late"), Some(&read_delta), 0, true, &complete).is_err());
        assert_eq!(
            stream_call_index(Some("new"), Some(&other_delta), 0, true, &complete).unwrap(),
            1
        );

        assert_eq!(
            stream_call_index(Some("late"), Some(&named_id), 0, true, &complete).unwrap(),
            0
        );
        assert!(stream_call_index(None, None, 0, true, &calls).is_err());
        assert_eq!(
            stream_call_index(Some("fixture-1"), None, 0, true, &calls).unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn mistral_streams_thinking_and_replays_function_history() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let fixture = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut headers = String::new();
            let mut length = 0;
            loop {
                let mut line = String::new();
                assert_ne!(socket.read_line(&mut line).unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
                let lower = line.to_ascii_lowercase();
                if let Some(value) = lower.strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap();
                }
                headers.push_str(&lower);
            }
            let mut bytes = vec![0; length];
            socket.read_exact(&mut bytes).unwrap();
            let request: Value = serde_json::from_slice(&bytes).unwrap();
            assert!(headers.starts_with("post /v1/chat/completions http/"));
            assert!(headers.contains("authorization: bearer fixture\r\n"));
            assert!(headers.contains("accept: text/event-stream\r\n"));
            assert_eq!(request["max_tokens"], 32);
            assert_eq!(request["reasoning_effort"], "high");
            assert_eq!(request["prompt_cache_key"], "session-fixture");
            assert!(request.get("stream_options").is_none());
            let first = json!({"id":"first","choices":[{"index":0,"delta":{"content":[
                {"type":"thinking","thinking":[{"type":"text","text":"con"}]}, ""
            ]}}]});
            let second = json!({"id":"second","choices":[{"index":0,"delta":{"content":[
                {"type":"thinking","thinking":[{"type":"text","text":"sider"}]},
                {"type":"text","text":"a tool"}
            ],"tool_calls":[
                {"id":"null","function":{"name":"read","arguments":"{"}},
                {"function":{"name":"list","arguments":{}}}
            ]}}]});
            let third = json!({"choices":[{"index":0,"delta":{
                "tool_calls":[
                    {"index":0,"id":"late-call","function":{"name":"read","arguments":"\"file\":\"doc\"}"}},
                    {"index":1,"function":{"name":"list"}}
                ]
            },"finish_reason":"tool_calls"}],
                "usage":{"prompt_tokens":12,"completion_tokens":3,"num_cached_tokens":7}});
            let output = format!("data: {first}\n\ndata: {second}\n\ndata: {third}\n\n");
            write!(socket.get_mut(),
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{output}",
                output.len()).unwrap();
        });
        let client = Ollama::new(&endpoint, Some("fixture".into()))
            .unwrap()
            .with_provider("mistral")
            .with_mistral(true, false, Some("high"))
            .with_prompt_cache_policy(true, false)
            .with_session_cache_key("session-fixture");
        let request = ModelRequest {
            model: "fixture".into(),
            messages: vec![Message::User {
                content: "hello".into(),
            }],
            tools: vec![],
            max_output_tokens: 32,
        };
        let mut stream = client.stream(request).await.unwrap();
        let mut metadata = None;
        let mut calls: Vec<ToolCall> = Vec::new();
        let mut text = String::new();
        let mut streamed_ids = Vec::new();
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                ModelEvent::Metadata(value) => metadata = Some(value),
                ModelEvent::Complete {
                    calls: value,
                    usage,
                } => {
                    calls = value;
                    assert_eq!(usage.unwrap().input_tokens, 12);
                }
                ModelEvent::Text(delta) => text.push_str(&delta),
                ModelEvent::ToolCallDelta(delta) if !delta.id.is_empty() => {
                    streamed_ids.push(delta.id)
                }
                _ => {}
            }
        }
        fixture.join().unwrap();
        let metadata = metadata.unwrap();
        assert_eq!(metadata.provider_api, Some(ModelApi::MistralConversations));
        assert_eq!(metadata.provider_request_id.as_deref(), Some("first"));
        assert_eq!(metadata.reasoning.as_deref(), Some("consider"));
        assert_eq!(metadata.cached_input_tokens, Some(7));
        assert_eq!(text, "a tool");
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].name, "read");
        assert_eq!(calls[0].arguments, r#"{"file":"doc"}"#);
        assert_eq!(calls[1].name, "list");
        assert_ne!(calls[0].id, calls[1].id);
        assert_eq!(streamed_ids, [calls[0].id.clone(), calls[1].id.clone()]);
        let call_id = calls[0].id.clone();
        assert_eq!(call_id, "late-call");
        assert_eq!(calls[1].id, super::call_id("toolcall:1", 0));
        let wire = messages(
            vec![
                Message::Assistant {
                    content: text,
                    calls,
                    replay: metadata.replay,
                },
                Message::Tool {
                    call_id,
                    is_error: true,
                    is_structured: Some(true),
                    content: json!({"data":{"content":[{"type":"text","text":" failure "},
                        {"type":"image","mimeType":"image/png","data":"YQ=="}]}})
                    .to_string(),
                },
            ],
            &metadata.replay_identity.unwrap(),
            true,
        )
        .unwrap();
        assert_eq!(
            wire[0]["content"],
            json!([thinking_part("consider"), text_part("a tool")])
        );
        assert_eq!(wire[0]["tool_calls"][0]["index"], 0);
        assert_eq!(wire[0]["tool_calls"][0]["id"], wire[1]["tool_call_id"]);
        assert_eq!(wire[1]["name"], "read");
        assert_eq!(wire[1]["content"][0]["text"], "[tool error] failure");
        assert_eq!(
            wire[1]["content"][1]["image_url"],
            "data:image/png;base64,YQ=="
        );
    }
}
