//! Project tool-result groups into Chat Completions messages.
use super::{Ollama, ResolvedModelIdentity};
use pi_rs_core::Message;
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub(super) struct ChatHistory<'a> {
    adapter: &'a Ollama,
    identity: &'a ResolvedModelIdentity,
    messages: Vec<Value>,
    images: Vec<Value>,
    tool_names: BTreeMap<String, String>,
}

impl<'a> ChatHistory<'a> {
    pub(super) fn new(adapter: &'a Ollama, identity: &'a ResolvedModelIdentity) -> Self {
        Self {
            adapter,
            identity,
            messages: Vec::new(),
            images: Vec::new(),
            tool_names: BTreeMap::new(),
        }
    }

    pub(super) fn push(&mut self, message: Message) {
        if let Message::Assistant { calls, .. } = &message
            && self.adapter.compatibility.requires_tool_result_name == Some(true)
        {
            for call in calls {
                self.tool_names.insert(call.id.clone(), call.name.clone());
            }
        }
        let is_tool_result = matches!(message, Message::Tool { .. });
        let mut encoded = self
            .adapter
            .compatible_message(message, self.identity)
            .into_iter();
        let Some(mut wire) = encoded.next() else {
            return;
        };
        if is_tool_result {
            let attached: Vec<_> = encoded
                .next()
                .and_then(|mut message| match message["content"].take() {
                    Value::Array(content) => Some(content),
                    _ => None,
                })
                .unwrap_or_default();
            let has_images = attached.len() > 1;
            self.images.extend(attached.into_iter().skip(1));
            if wire["content"]
                .as_str()
                .is_some_and(|text| text.trim().is_empty())
            {
                wire["content"] = if has_images {
                    json!("(see attached image)")
                } else {
                    json!("(no tool output)")
                };
            }
            if let Some(name) = wire["tool_call_id"]
                .as_str()
                .and_then(|id| self.tool_names.get(id))
                && !name.is_empty()
            {
                wire["name"] = json!(name);
            }
            self.messages.push(wire);
            return;
        }
        self.push_boundary(wire);
    }

    pub(super) fn push_boundary(&mut self, message: Value) {
        self.flush_images();
        if message["role"] == "user" {
            self.bridge_user();
        }
        self.messages.push(message);
    }

    pub(super) fn finish(mut self) -> Vec<Value> {
        self.flush_images();
        self.messages
    }

    fn flush_images(&mut self) {
        if self.images.is_empty() {
            return;
        }
        self.bridge_user();
        let mut content = vec![json!({"type":"text","text":"Attached image(s) from tool result:"})];
        content.append(&mut self.images);
        self.messages.push(json!({"role":"user","content":content}));
    }

    fn bridge_user(&mut self) {
        if self
            .adapter
            .compatibility
            .requires_assistant_after_tool_result
            == Some(true)
            && self
                .messages
                .last()
                .is_some_and(|message| message["role"] == "tool")
        {
            self.messages.extend(self.adapter.compatible_message(
                Message::Assistant {
                    content: "I have processed the tool results.".into(),
                    calls: Vec::new(),
                    replay: None,
                },
                self.identity,
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intervening_system_checkpoint_does_not_add_an_assistant_bridge() {
        let adapter = Ollama::new("http://localhost", None)
            .unwrap()
            .with_chat_compatibility(
                serde_json::from_value(json!({"requiresAssistantAfterToolResult":true})).unwrap(),
                None,
                false,
                true,
            );
        let identity = adapter.replay_identity("fixture");
        let mut history = ChatHistory::new(&adapter, &identity);
        history.push(Message::Tool {
            call_id: "prior".into(),
            content: "done".into(),
            is_error: false,
            is_structured: Some(false),
        });
        history.push_boundary(json!({"role":"system","tools":[{"type":"function"}]}));
        history.push(Message::User {
            content: "next".into(),
        });
        let messages = history.finish();
        assert_eq!(
            messages
                .iter()
                .map(|message| message["role"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["tool", "system", "user"]
        );
    }
}
