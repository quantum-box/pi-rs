//! OpenAI custom grammar tools carry raw strings rather than JSON objects.
use std::collections::BTreeMap;

use pi_rs_core::{Error, Message, ModelRequest, Result, ToolCall};
use serde_json::{Value, json};

pub(super) fn project_input(
    request: &ModelRequest,
    input: &mut Value,
    properties: &BTreeMap<String, String>,
    namespace: Option<&str>,
) -> Result<()> {
    let input = input.as_array_mut().expect("Responses input array");
    if properties.is_empty() && !input.iter().any(|item| item["type"] == "custom_tool_call") {
        return Ok(());
    }
    let arguments: BTreeMap<_, _> = request
        .messages
        .iter()
        .filter_map(|message| match message {
            Message::Assistant { calls, .. } => Some(calls),
            _ => None,
        })
        .flatten()
        .map(|call| {
            (
                (call.id.as_str(), call.name.as_str()),
                call.arguments.as_str(),
            )
        })
        .collect();
    let mut custom_calls = std::collections::BTreeSet::new();
    for item in input {
        if item["type"] == "function_call_output" {
            if item["call_id"]
                .as_str()
                .is_some_and(|id| custom_calls.contains(id))
            {
                item["type"] = json!("custom_tool_call_output");
            }
            continue;
        }
        let was_custom = item["type"] == "custom_tool_call";
        if !was_custom && item["type"] != "function_call" {
            continue;
        }
        let id = super::field(item, "call_id")?;
        let name = super::field(item, "name")?;
        let projected_input = properties.get(name).and_then(|property| {
            let arguments = arguments
                .get(&(id, name))
                .copied()
                .or_else(|| item["arguments"].as_str())?;
            pi_rs_core::lossless_openai_grammar_input(arguments, property)
        });
        if let Some(raw) = projected_input {
            custom_calls.insert(id.to_owned());
            if was_custom && item["input"] == raw {
                continue;
            }
            let replay_id = item["id"].as_str().filter(|id| id.starts_with("ctc_"));
            let mut projected =
                json!({"type":"custom_tool_call","call_id":id,"name":name,"input":raw});
            if let Some(replay_id) = replay_id {
                projected["id"] = json!(replay_id);
            }
            *item = projected;
        } else if was_custom {
            let args = arguments.get(&(id, name)).ok_or_else(|| {
                Error::Model("missing OpenAI grammar tool replay arguments".into())
            })?;
            let mut projected =
                json!({"type":"function_call","call_id":id,"name":name,"arguments":args});
            if let Some(namespace) = namespace {
                projected["namespace"] = json!(namespace);
            }
            *item = projected;
        }
    }
    Ok(())
}

pub(super) struct Input {
    raw: String,
    closed: bool,
}

impl Input {
    pub(super) fn start(
        item: &Value,
        properties: &BTreeMap<String, String>,
    ) -> Result<(Self, ToolCall)> {
        let name = super::field(item, "name")?;
        let property = properties
            .get(name)
            .ok_or_else(|| Error::Model("unadvertised OpenAI grammar tool".into()))?;
        let initial = item
            .get("input")
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| Error::Model("invalid OpenAI tool input".into()))
            })
            .transpose()?
            .unwrap_or("");
        let encoded = quoted(initial);
        let prefix = format!("{{{}:\"{}", quoted(property), inner(&encoded));
        Ok((
            Self {
                raw: initial.to_owned(),
                closed: false,
            },
            ToolCall {
                id: super::field(item, "call_id")?.into(),
                name: name.into(),
                arguments: prefix,
            },
        ))
    }

    pub(super) fn append(&mut self, call: &mut ToolCall, raw: &str) -> Result<String> {
        if self.closed {
            return Err(Error::Model(
                "OpenAI sent tool input after completion".into(),
            ));
        }
        self.raw.push_str(raw);
        let delta = quoted(raw);
        let delta = inner(&delta).to_owned();
        call.arguments.push_str(&delta);
        Ok(delta)
    }

    pub(super) fn finish(&mut self, call: &mut ToolCall, raw: &str) -> Result<String> {
        let suffix = raw
            .strip_prefix(&self.raw)
            .ok_or_else(|| Error::Model("OpenAI tool input changed during stream".into()))?;
        if self.closed && suffix.is_empty() {
            return Ok(String::new());
        }
        let mut delta = self.append(call, suffix)?;
        delta.push_str("\"}");
        call.arguments.push_str("\"}");
        self.closed = true;
        Ok(delta)
    }
}

fn quoted(text: &str) -> String {
    json!(text).to_string()
}

fn inner(quoted: &str) -> &str {
    &quoted[1..quoted.len() - 1]
}
