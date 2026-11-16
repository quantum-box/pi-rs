//! Bounded, advisory display boundaries collected from existing stream events.
use crate::{EnrichedModelEvent, ModelEvent, ToolCall};
use serde_json::{Value, json};
use std::collections::BTreeMap;

const MAX_BYTES: usize = 1024 * 1024;
const MAX_ITEMS: usize = 256;

enum Item {
    Text(Option<String>),
    Tool(String),
}

#[derive(Default)]
pub(super) struct Candidate {
    items: BTreeMap<u64, Item>,
    bytes: usize,
    invalid: bool,
}

impl Candidate {
    pub(super) fn observe(&mut self, event: &EnrichedModelEvent) {
        if self.invalid {
            return;
        }
        match event {
            EnrichedModelEvent::TextStart { index } => {
                if self.items.len() >= MAX_ITEMS
                    || self.items.insert(*index, Item::Text(None)).is_some()
                {
                    self.invalid = true;
                }
            }
            EnrichedModelEvent::TextDelta { index, .. } => {
                if !matches!(self.items.get(index), Some(Item::Text(None))) {
                    self.invalid = true;
                }
            }
            EnrichedModelEvent::TextEnd { index, content } => {
                self.bytes = self.bytes.saturating_add(content.len());
                match self.items.get_mut(index) {
                    Some(Item::Text(slot @ None)) if self.bytes <= MAX_BYTES => {
                        *slot = Some(content.clone())
                    }
                    _ => self.invalid = true,
                }
            }
            EnrichedModelEvent::Legacy(ModelEvent::ToolCallDelta(delta)) => {
                match self.items.get(&delta.index) {
                    None if !delta.id.is_empty() && self.items.len() < MAX_ITEMS => {
                        self.bytes = self.bytes.saturating_add(delta.id.len());
                        if self.bytes <= MAX_BYTES {
                            self.items.insert(delta.index, Item::Tool(delta.id.clone()));
                        } else {
                            self.invalid = true;
                        }
                    }
                    Some(Item::Tool(id)) if delta.id.is_empty() || delta.id == *id => {}
                    _ => self.invalid = true,
                }
            }
            EnrichedModelEvent::Legacy(ModelEvent::Text(_) | ModelEvent::Reasoning(_))
            | EnrichedModelEvent::ReasoningStart { .. }
            | EnrichedModelEvent::ReasoningDelta { .. }
            | EnrichedModelEvent::ReasoningEnd { .. } => self.invalid = true,
            _ => {}
        }
    }

    pub(super) fn finish(
        self,
        content: &str,
        calls: impl Iterator<Item = ToolCall>,
    ) -> Option<Value> {
        if self.invalid || self.items.is_empty() {
            return None;
        }
        let mut text = String::new();
        let mut calls = calls;
        let mut blocks = Vec::new();
        for item in self.items.into_values() {
            match item {
                Item::Text(Some(value)) => {
                    text.push_str(&value);
                    blocks.push(json!({"type":"text", "text":value}));
                }
                Item::Tool(id) => {
                    if calls.next()?.id != id {
                        return None;
                    }
                    blocks.push(json!({"type":"tool_call", "id":id}));
                }
                Item::Text(None) => return None,
            }
        }
        (text == content && calls.next().is_none())
            .then(|| json!({"anthropicDisplayLayoutV1":blocks}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ToolCallDelta;
    #[test]
    fn orders_tool_calls_and_rejects_ambiguous_or_stale_items() {
        let mut candidate = Candidate::default();
        candidate.observe(&EnrichedModelEvent::TextStart { index: 0 });
        candidate.observe(&EnrichedModelEvent::TextEnd {
            index: 0,
            content: "first".into(),
        });
        candidate.observe(&EnrichedModelEvent::Legacy(ModelEvent::ToolCallDelta(
            ToolCallDelta {
                index: 1,
                id: "id".into(),
                name: "read".into(),
                arguments: String::new(),
            },
        )));
        candidate.observe(&EnrichedModelEvent::TextStart { index: 2 });
        candidate.observe(&EnrichedModelEvent::TextEnd {
            index: 2,
            content: "last".into(),
        });
        let call = ToolCall {
            id: "id".into(),
            name: "read".into(),
            arguments: "{}".into(),
        };
        assert_eq!(
            candidate.finish("firstlast", [call].into_iter()).unwrap()["anthropicDisplayLayoutV1"],
            json!([
                {"type":"text","text":"first"},{"type":"tool_call","id":"id"},{"type":"text","text":"last"}
            ])
        );
        let mut stale = Candidate::default();
        stale.observe(&EnrichedModelEvent::TextStart { index: 0 });
        stale.observe(&EnrichedModelEvent::TextEnd {
            index: 0,
            content: "old".into(),
        });
        assert!(stale.finish("new", std::iter::empty()).is_none());
        let mut duplicate = Candidate::default();
        duplicate.observe(&EnrichedModelEvent::TextStart { index: 0 });
        duplicate.observe(&EnrichedModelEvent::TextStart { index: 0 });
        assert!(duplicate.finish("", std::iter::empty()).is_none());
    }
}
