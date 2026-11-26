use std::collections::HashMap;
use std::io::Write;

use chrono::{DateTime, SecondsFormat};
use pi_rs_core::Message;
use pi_rs_durable::runtime::{Scheduler, SessionEntry, SessionTree, SessionTreeEntry};
use serde_json::{Value, json, value::RawValue};
use uuid::Uuid;

use super::{text_message, user_message};

const UNKNOWN_TIMESTAMP: &str = "1970-01-01T00:00:00.000Z";

pub(super) struct Snapshot {
    tree: SessionTree,
    entries: Vec<Value>,
}

pub(super) fn snapshot(scheduler: &Scheduler, id: Uuid) -> Result<Snapshot, String> {
    let tree = scheduler
        .session_entries_start(id)
        .and_then(|capture| scheduler.session_tree(&capture))
        .map_err(|error| error.to_string())?;
    let mut tool_names = HashMap::new();
    let entries = tree
        .entries
        .iter()
        .map(|node| project(node, &mut tool_names))
        .collect();
    Ok(Snapshot { tree, entries })
}

impl Snapshot {
    pub(super) fn entries_after(self, since: Option<&Value>) -> Result<Value, String> {
        let start = match since {
            None => 0,
            Some(since) => self
                .entries
                .iter()
                .position(|entry| &entry["id"] == since)
                .map(|index| index + 1)
                .ok_or_else(|| {
                    let shown = since
                        .as_str()
                        .map_or_else(|| since.to_string(), str::to_owned);
                    format!("Entry not found: {shown}")
                })?,
        };
        let mut response = json!({"leafId": self.tree.leaf_id});
        response["entries"] = Value::Array(self.entries.into_iter().skip(start).collect());
        Ok(response)
    }

    pub(super) fn nested(&self) -> Result<Box<RawValue>, String> {
        self.write_nested().map_err(|error| error.to_string())
    }

    fn write_nested(&self) -> std::io::Result<Box<RawValue>> {
        let mut index_by_id = HashMap::<Uuid, usize>::new();
        let mut children = vec![Vec::new(); self.entries.len()];
        let mut roots = Vec::new();
        for (index, node) in self.tree.entries.iter().enumerate() {
            match node.parent_id.and_then(|id| index_by_id.get(&id).copied()) {
                Some(parent) => children[parent].push(index),
                None => roots.push(index),
            }
            index_by_id.insert(node.entry.id(), index);
        }

        // Write one open list per level. A long conversation should not
        // serialize or drop a deeply recursive serde_json::Value.
        let mut bytes = b"{\"tree\":[".to_vec();
        let mut lists = vec![roots.iter().enumerate()];
        while let Some(list) = lists.last_mut() {
            if let Some((sibling, &index)) = list.next() {
                if sibling != 0 {
                    bytes.write_all(b",")?;
                }
                bytes.write_all(b"{\"entry\":")?;
                serde_json::to_writer(&mut bytes, &self.entries[index])?;
                if let Some(label) = self.tree.entries[index].label.as_ref() {
                    bytes.write_all(b",\"label\":")?;
                    serde_json::to_writer(&mut bytes, &label.label)?;
                    bytes.write_all(b",\"labelTimestamp\":")?;
                    serde_json::to_writer(&mut bytes, &timestamp(label.timestamp_ms))?;
                }
                bytes.write_all(b",\"children\":[")?;
                lists.push(children[index].iter().enumerate());
            } else {
                lists.pop();
                bytes.write_all(if lists.is_empty() { b"]" } else { b"]}" })?;
            }
        }
        bytes.write_all(b",\"leafId\":")?;
        serde_json::to_writer(&mut bytes, &self.tree.leaf_id)?;
        bytes.write_all(b"}")?;
        let json = String::from_utf8(bytes).map_err(std::io::Error::other)?;
        RawValue::from_string(json).map_err(std::io::Error::other)
    }
}

fn project(node: &SessionTreeEntry, tool_names: &mut HashMap<String, String>) -> Value {
    if node.parent_id.is_none() {
        tool_names.clear();
    }
    let mut entry = match &node.entry {
        SessionEntry::Message(entry) => json!({
            "type": "message",
            "message": project_message(&entry.message, entry.blocks.as_deref(), tool_names)
        }),
        SessionEntry::SessionInfo { entry, .. } => {
            let mut value = json!({"type":"session_info"});
            if let Some(name) = &entry.name {
                value["name"] = json!(name);
            }
            value
        }
        SessionEntry::Custom { entry, .. } => {
            let mut value = json!({"type":"custom", "customType":entry.kind});
            if let Some(data) = &entry.data {
                value["data"] = data.clone();
            }
            value
        }
        SessionEntry::Label { entry, .. } => {
            let mut value = json!({"type":"label", "targetId":entry.target_id});
            if let Some(label) = &entry.label {
                value["label"] = json!(label);
            }
            value
        }
    };
    entry["id"] = json!(node.entry.id());
    entry["parentId"] = json!(node.parent_id);
    entry["timestamp"] = match &node.entry {
        SessionEntry::Label { entry, .. } => json!(timestamp(entry.timestamp_ms)),
        _ => json!(UNKNOWN_TIMESTAMP),
    };
    entry
}

fn timestamp(millis: i64) -> String {
    DateTime::from_timestamp_millis(millis)
        .map(|time| time.to_rfc3339_opts(SecondsFormat::Millis, true))
        .unwrap_or_else(|| UNKNOWN_TIMESTAMP.to_owned())
}

fn project_message(
    message: &Message,
    blocks: Option<&[Value]>,
    tool_names: &mut HashMap<String, String>,
) -> Value {
    match message {
        Message::System { content } => text_message("system", content),
        Message::User { content } => {
            blocks.map_or_else(|| text_message("user", content), user_message)
        }
        Message::UserWithImages { content, images } => {
            if let Some(blocks) = blocks {
                return user_message(blocks);
            }
            let mut blocks = vec![json!({"type":"text", "text":content})];
            blocks.extend(images.iter().map(|image| {
                json!({
                    "type":"image", "mimeType":image.mime_type, "data":image.data
                })
            }));
            user_message(&blocks)
        }
        Message::Assistant { content, calls, .. } => {
            let mut message = text_message("assistant", content);
            let blocks = message["content"]
                .as_array_mut()
                .expect("text message content");
            if content.is_empty() && !calls.is_empty() {
                blocks.clear();
            }
            for call in calls {
                tool_names.insert(call.id.clone(), call.name.clone());
                let arguments = serde_json::from_str::<Value>(&call.arguments)
                    .ok()
                    .filter(Value::is_object)
                    .unwrap_or_else(|| json!({}));
                blocks.push(json!({
                    "type":"toolCall", "id":call.id, "name":call.name, "arguments":arguments
                }));
            }
            if !calls.is_empty() {
                message["stopReason"] = json!("toolUse");
            }
            message
        }
        Message::Tool {
            call_id,
            content,
            is_error,
            ..
        } => {
            let mut message = text_message("toolResult", content);
            if let Some(blocks) = blocks {
                message["content"] = json!(blocks);
            }
            message["toolCallId"] = json!(call_id);
            message["toolName"] = json!(tool_names.get(call_id).map_or("", String::as_str));
            message["isError"] = json!(is_error);
            message
        }
    }
}

#[cfg(test)]
mod tests {
    use pi_rs_durable::{
        observer::Cursor,
        runtime::{SessionInfoEntry, SessionLabelEntry},
    };

    use super::*;

    #[test]
    fn nested_rpc_tree_preserves_forest_and_labels() {
        let first = Uuid::from_u128(1);
        let second = Uuid::from_u128(2);
        let third = Uuid::from_u128(3);
        let make = |id, parent_id, label| SessionTreeEntry {
            entry: SessionEntry::SessionInfo {
                entry: SessionInfoEntry { id, name: None },
                origin: first,
                observation: Cursor(1),
            },
            parent_id,
            label,
        };
        let tree = SessionTree {
            entries: vec![
                make(
                    first,
                    None,
                    Some(SessionLabelEntry {
                        id: Uuid::from_u128(4),
                        target_id: first,
                        label: Some("bookmark".into()),
                        timestamp_ms: 1234,
                    }),
                ),
                make(second, Some(first), None),
                make(third, None, None),
            ],
            leaf_id: Some(third),
        };
        let entries = tree
            .entries
            .iter()
            .map(|node| project(node, &mut HashMap::new()))
            .collect();
        let snapshot = Snapshot { tree, entries };
        let rhs: Value = serde_json::from_str(snapshot.nested().unwrap().get()).unwrap();
        assert_eq!(rhs["leafId"], json!(third));
        assert_eq!(rhs["tree"][0]["label"], "bookmark");
        assert_eq!(rhs["tree"][0]["labelTimestamp"], "1970-01-01T00:00:01.234Z");
        assert_eq!(rhs["tree"][0]["children"][0]["entry"]["id"], json!(second));
        assert_eq!(rhs["tree"][1]["entry"]["id"], json!(third));
    }

    #[test]
    fn serializes_deep_trees_without_recursive_values() {
        let mut tree = SessionTree {
            entries: Vec::new(),
            leaf_id: None,
        };
        for sequence in 1..=4096 {
            let id = Uuid::from_u128(sequence);
            tree.entries.push(SessionTreeEntry {
                entry: SessionEntry::SessionInfo {
                    entry: SessionInfoEntry { id, name: None },
                    origin: Uuid::from_u128(1),
                    observation: Cursor(sequence as u64),
                },
                parent_id: tree.leaf_id.replace(id),
                label: None,
            });
        }
        let entries = tree
            .entries
            .iter()
            .map(|node| project(node, &mut HashMap::new()))
            .collect();
        let snapshot = Snapshot { tree, entries };
        let nested = snapshot.nested().unwrap();
        assert_eq!(nested.get().matches("\"children\":").count(), 4096);
        assert!(nested.get().ends_with(&format!(
            ",\"leafId\":\"{}\"}}",
            snapshot.tree.leaf_id.unwrap()
        )));
    }
}
