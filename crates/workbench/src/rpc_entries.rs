//! Pi session entries omit the native journal's execution bookkeeping.
use std::collections::HashMap;

use pi_rs::{Error, Event, ModelMetadata, Record, Result, Session};
use serde_json::{Value, json};

use super::rpc_messages::Projection;

const UNKNOWN_TIMESTAMP: &str = "1970-01-01T00:00:00.000Z";

#[derive(Clone, Copy, Default)]
struct Position<'a> {
    visible: Option<u64>,
    imported: Option<u64>,
    model: Option<(&'a str, &'a str, &'a str)>,
}

pub(super) struct Entries {
    values: Vec<Value>,
    leaf: Option<String>,
}

impl Entries {
    pub(super) fn new(session: &Session) -> Self {
        let mut entries = Vec::new();
        let mut positions = Vec::with_capacity(session.records().len() + 1);
        positions.push(Position::default());
        let mut source_ids = HashMap::new();
        let mut messages = Projection::default();

        for record in session.records() {
            messages.record(record);
            let parent = record
                .parent
                .map_or_else(Position::default, |id| positions[id as usize]);
            let mut position = parent;
            let entry = match &record.event {
                Event::SourceEntry { format, entry } if format == "pi" => {
                    position.imported = (entry["type"] != "session").then_some(record.id);
                    if position.imported.is_some() {
                        if let Some(source_id) = entry["id"].as_str() {
                            source_ids.insert(source_id, record.id);
                        }
                        let mut entry = entry.clone();
                        for reference in ["targetId", "fromId", "firstKeptEntryId"] {
                            if let Some(id) =
                                entry[reference].as_str().and_then(|id| source_ids.get(id))
                            {
                                entry[reference] = json!(id.to_string());
                            }
                        }
                        Some(entry)
                    } else {
                        None
                    }
                }
                // An imported source entry already includes the visible representation.
                _ if parent
                    .imported
                    .is_some_and(|id| imported_conversion(session, id, record)) =>
                {
                    position.imported = None;
                    if let Event::ModelSelected {
                        model,
                        provider,
                        thinking,
                    } = &record.event
                    {
                        position.model =
                            Some(selection(model, provider.as_deref(), thinking.as_deref()));
                    }
                    None
                }
                Event::ModelSelected {
                    model,
                    provider,
                    thinking,
                } => {
                    let current = selection(model, provider.as_deref(), thinking.as_deref());
                    position.model = Some(current);
                    let model_changed = parent
                        .model
                        .is_none_or(|old| (old.0, old.1) != (current.0, current.1));
                    let thinking_changed = parent.model.is_none_or(|old| old.2 != current.2);
                    let model_entry =
                        || json!({"type":"model_change","provider":current.0,"modelId":current.1});
                    if model_changed && thinking_changed {
                        let id = format!("{}-model", record.id);
                        entries.push(with_identity(
                            model_entry(),
                            &id,
                            wire_id(parent.visible),
                            record,
                        ));
                        let thinking =
                            json!({"type":"thinking_level_change","thinkingLevel":current.2});
                        entries.push(with_identity(
                            thinking,
                            &record.id.to_string(),
                            Some(id),
                            record,
                        ));
                        position.visible = Some(record.id);
                        positions.push(position);
                        continue;
                    }
                    if model_changed {
                        Some(model_entry())
                    } else if thinking_changed {
                        Some(json!({"type":"thinking_level_change","thinkingLevel":current.2}))
                    } else {
                        None
                    }
                }
                Event::Label { target, label } => {
                    let target = positions
                        .get(*target as usize)
                        .and_then(|target| target.visible);
                    target.map(|target| {
                        let mut entry = json!({"type":"label","targetId":target.to_string()});
                        if let Some(label) = label {
                            entry["label"] = json!(label);
                        }
                        entry
                    })
                }
                Event::SessionInfo {
                    name: Some(name), ..
                } => Some(json!({"type":"session_info","name":name})),
                Event::ContextEdited {
                    target,
                    replacement,
                } => Some(json!({
                    "type":"context_edit","targetId":wire_id(positions.get(*target as usize).and_then(|target| target.visible)),
                    "replacement":replacement.as_ref().map(|content| json!({"content":content}))
                })),
                Event::Compacted {
                    summary,
                    retain_from,
                    usage,
                    ..
                } => {
                    let first = retain_from
                        .and_then(|id| positions.get(id as usize)?.visible)
                        .unwrap_or(record.id);
                    let mut entry = json!({
                        "type":"compaction","summary":summary,"firstKeptEntryId":first.to_string(),
                        "tokensBefore":super::rpc_messages::compaction_tokens(session, record)
                    });
                    add_usage(&mut entry, *usage);
                    Some(entry)
                }
                Event::BranchSummary {
                    summary,
                    from,
                    usage,
                    read_files,
                    modified_files,
                    ..
                } => {
                    let from = from.and_then(|id| positions.get(id as usize)?.visible);
                    let mut entry = json!({
                        "type":"branch_summary","summary":summary,"fromId":wire_id(from),
                        "details":{"readFiles":read_files,"modifiedFiles":modified_files}
                    });
                    add_usage(&mut entry, *usage);
                    Some(entry)
                }
                Event::ExtensionStates { states } => Some(json!({
                    "type":"custom","customType":"pi-rs:extension-states","data":states
                })),
                _ => messages
                    .raw_message(session, record)
                    .map(|message| json!({"type":"message","message":message})),
            };
            if let Some(entry) = entry {
                entries.push(with_identity(
                    entry,
                    &record.id.to_string(),
                    wire_id(parent.visible),
                    record,
                ));
                position.visible = Some(record.id);
                if !matches!(record.event, Event::SourceEntry { .. }) {
                    position.imported = None;
                }
            }
            positions.push(position);
        }
        Self {
            values: entries,
            leaf: wire_id(positions[session.head() as usize].visible),
        }
    }

    pub(super) fn after(self, since: Option<&Value>) -> Result<Value> {
        let start = if let Some(since) = since {
            let id = since
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| since.to_string());
            self.values
                .iter()
                .position(|entry| entry["id"] == id)
                .ok_or_else(|| Error::Config(format!("Entry not found: {id}")))?
                + 1
        } else {
            0
        };
        let mut result = json!({"leafId":self.leaf});
        result["entries"] = Value::Array(self.values.into_iter().skip(start).collect());
        Ok(result)
    }

    pub(super) fn tree(self) -> Value {
        let mut labels = HashMap::new();
        for entry in &self.values {
            if entry["type"] == "label"
                && let Some(target) = entry["targetId"].as_str()
            {
                if let Some(label) = entry["label"].as_str().filter(|label| !label.is_empty()) {
                    labels.insert(
                        target.to_owned(),
                        (label.to_owned(), entry["timestamp"].clone()),
                    );
                } else {
                    labels.remove(target);
                }
            }
        }
        let mut children = HashMap::<String, Vec<Value>>::new();
        let mut roots = Vec::new();
        for entry in self.values.into_iter().rev() {
            let id = entry["id"].as_str().expect("entry has id");
            let parent = entry["parentId"].as_str().map(str::to_owned);
            let mut descendants = children.remove(id).unwrap_or_default();
            descendants.reverse();
            descendants.sort_by(|a, b| {
                a["entry"]["timestamp"]
                    .as_str()
                    .cmp(&b["entry"]["timestamp"].as_str())
            });
            let mut node = json!({"entry":entry});
            node["children"] = Value::Array(descendants);
            if let Some((label, timestamp)) = labels.remove(id) {
                node["label"] = json!(label);
                node["labelTimestamp"] = timestamp;
            }
            if let Some(parent) = parent {
                children.entry(parent).or_default().push(node);
            } else {
                roots.push(node);
            }
        }
        roots.reverse();
        let mut result = json!({"leafId":self.leaf});
        result["tree"] = Value::Array(roots);
        result
    }

    pub(super) fn fork_messages(self) -> Value {
        let messages: Vec<_> = self
            .values
            .into_iter()
            .filter_map(|entry| {
                if entry["type"] != "message" || entry["message"]["role"] != "user" {
                    return None;
                }
                let content = &entry["message"]["content"];
                let text = content.as_str().map(str::to_owned).or_else(|| {
                    content.as_array().map(|blocks| {
                        blocks
                            .iter()
                            .filter(|block| block["type"] == "text")
                            .filter_map(|block| block["text"].as_str())
                            .collect::<Vec<_>>()
                            .join("")
                    })
                })?;
                (!text.is_empty()).then(|| json!({"entryId":entry["id"],"text":text}))
            })
            .collect();
        json!({"messages":messages})
    }
}

fn imported_conversion(session: &Session, source: u64, record: &Record) -> bool {
    let source = &session.records()[source as usize - 1];
    let Event::SourceEntry { entry, .. } = &source.event else {
        return false;
    };
    if record.timestamp != source.timestamp || record.run_id.is_some() {
        return false;
    }
    matches!(
        (entry["type"].as_str(), &record.event),
        (
            Some("message"),
            Event::User { .. }
                | Event::UserWithImages { .. }
                | Event::Assistant { .. }
                | Event::ToolFinished { .. }
                | Event::SystemCheckpoint { .. }
                | Event::BashExecution { .. }
        ) | (
            Some("custom_message"),
            Event::User { .. } | Event::UserWithImages { .. }
        ) | (
            Some("model_change" | "thinking_level_change"),
            Event::ModelSelected { .. }
        ) | (Some("session_info"), Event::SessionInfo { .. })
            | (Some("label"), Event::Label { .. })
            | (Some("context_edit"), Event::ContextEdited { .. })
            | (Some("compaction"), Event::Compacted { .. })
            | (Some("branch_summary"), Event::BranchSummary { .. })
            | (Some("custom"), Event::ExtensionStates { .. })
    )
}

pub(super) fn editable_record(session: &Session, id: u64) -> u64 {
    let Some(source) = session.records().get(id.saturating_sub(1) as usize) else {
        return id;
    };
    let Event::SourceEntry { format, .. } = &source.event else {
        return id;
    };
    if format != "pi" {
        return id;
    }
    session
        .records()
        .iter()
        .skip(id as usize)
        .take_while(|record| {
            !matches!(
                record.event,
                Event::SourceEntry { .. } | Event::Checkout { .. }
            ) && record.timestamp == source.timestamp
                && record.run_id.is_none()
        })
        .find(|record| imported_conversion(session, id, record))
        .map_or(id, |record| record.id)
}

fn with_identity(mut entry: Value, id: &str, parent: Option<String>, record: &Record) -> Value {
    entry["id"] = json!(id);
    entry["parentId"] = json!(parent);
    entry["timestamp"] = json!(record.timestamp.as_deref().unwrap_or(UNKNOWN_TIMESTAMP));
    entry
}

fn wire_id(id: Option<u64>) -> Option<String> {
    id.map(|id| id.to_string())
}

fn selection<'a>(
    model: &'a str,
    provider: Option<&'a str>,
    thinking: Option<&'a str>,
) -> (&'a str, &'a str, &'a str) {
    (
        provider.unwrap_or("ollama"),
        model,
        thinking.unwrap_or("off"),
    )
}

fn add_usage(entry: &mut Value, usage: Option<pi_rs::Usage>) {
    if let Some(usage) = usage {
        entry["usage"] = super::rpc_events::usage_value(usage, &ModelMetadata::default());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn imported_entries_keep_branch_parents_and_remap_references() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        for entry in [
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"one","parentId":null,"message":{"role":"user","content":"root"}}),
            json!({"type":"message","id":"two","parentId":"one","message":{"role":"user","content":"first branch"}}),
            json!({"type":"message","id":"three","parentId":"one","message":{"role":"user","content":"second branch"}}),
            json!({"type":"label","id":"four","parentId":"three","targetId":"two","label":"bookmark"}),
            json!({"type":"context_edit","id":"five","parentId":"four","targetId":"three","replacement":{"content":"edited"}}),
        ] {
            writeln!(file, "{entry}").unwrap();
        }
        let session = super::super::session_import::read(file.path()).unwrap();
        let result = Entries::new(&session).after(None).unwrap();
        let entries = result["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[2]["parentId"], entries[0]["id"]);
        assert_eq!(entries[3]["targetId"], entries[1]["id"]);
        assert_eq!(entries[4]["targetId"], entries[2]["id"]);
        assert_eq!(entries[2]["message"]["content"], "second branch");
        assert_eq!(result["leafId"], entries[4]["id"]);
        let messages = Entries::new(&session).fork_messages();
        assert_eq!(messages["messages"].as_array().unwrap().len(), 3);
        assert_eq!(messages["messages"][2]["entryId"], entries[2]["id"]);
        let tree = Entries::new(&session).tree();
        assert_eq!(tree["tree"][0]["children"][0]["label"], "bookmark");
        let target = entries[2]["id"].as_str().unwrap().parse().unwrap();
        assert!(matches!(
            &session.records()[editable_record(&session, target) as usize - 1].event,
            Event::User { content, .. } if content == "second branch"
        ));
    }
}
