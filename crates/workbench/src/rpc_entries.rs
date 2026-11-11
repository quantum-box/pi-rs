//! Pi session entries omit the native journal's execution bookkeeping.
use std::collections::HashMap;

use pi_rs::{Error, Event, Record, Result, Session};
use serde_json::{Value, json};

use super::rpc_messages::Projection;

const UNKNOWN_TIMESTAMP: &str = "1970-01-01T00:00:00.000Z";

pub(super) fn session_header(session: &Session, cwd: &std::path::Path) -> Value {
    json!({
        "type":"session", "version":3, "id":session.session_id(),
        "timestamp":session.records().first().and_then(|record| record.timestamp.as_deref()).unwrap_or(UNKNOWN_TIMESTAMP),
        "cwd":session.working_directory().map_or_else(|| cwd.to_string_lossy().into_owned(), str::to_owned)
    })
}

#[derive(Clone, Copy, Default)]
struct Position {
    visible: Option<u64>,
    imported: Option<u64>,
    model: Option<u64>,
}

pub(super) struct Entries {
    values: Vec<Value>,
    leaf: Option<String>,
}

impl Entries {
    pub(super) fn new(session: &Session) -> Self {
        let mut cursor = Cursor::default();
        let values = session
            .records()
            .iter()
            .flat_map(|record| cursor.append(session.records(), record, Some(session)))
            .collect();
        Self {
            values,
            leaf: wire_id(cursor.positions[session.head() as usize].visible),
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

    pub(super) fn write_share_branch(
        self,
        session: &Session,
        cwd: &std::path::Path,
    ) -> Result<Vec<u8>> {
        let by_id: HashMap<_, _> = self
            .values
            .iter()
            .map(|entry| (entry["id"].as_str().expect("entry has id"), entry))
            .collect();
        let mut branch = Vec::new();
        let mut id = self.leaf.as_deref();
        while let Some(entry) = id.and_then(|id| by_id.get(id)) {
            if entry["customType"] != "pi-rs:extension-states" {
                branch.push(*entry);
            }
            id = entry["parentId"].as_str();
        }
        let header = session_header(session, cwd);
        let mut body = serde_json::to_vec(&header)?;
        body.push(b'\n');
        let mut parent = Value::Null;
        for entry in branch.into_iter().rev() {
            let mut entry = entry.clone();
            entry["parentId"] = parent;
            serde_json::to_writer(&mut body, &entry)?;
            body.push(b'\n');
            if body.len() > 128 * 1024 * 1024 {
                return Err(Error::Config("session share exceeds 128 MiB".into()));
            }
            parent = entry["id"].clone();
        }
        Ok(body)
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

pub(super) struct Cursor {
    positions: Vec<Position>,
    source_ids: HashMap<String, u64>,
    messages: Projection,
}

impl Default for Cursor {
    fn default() -> Self {
        Self {
            positions: vec![Position::default()],
            source_ids: HashMap::new(),
            messages: Projection::default(),
        }
    }
}

impl Cursor {
    pub(super) fn advance(&mut self, records: &[Record]) {
        for record in records.iter().skip(self.positions.len() - 1) {
            self.append(records, record, None);
        }
    }

    pub(super) fn appended(&mut self, records: &[Record]) -> Vec<Value> {
        let Some((record, prefix)) = records.split_last() else {
            return Vec::new();
        };
        self.advance(prefix);
        if record.id as usize != self.positions.len() {
            return Vec::new();
        }
        self.append(records, record, None)
    }

    fn append(
        &mut self,
        records: &[Record],
        record: &Record,
        session: Option<&Session>,
    ) -> Vec<Value> {
        let mut entries = Vec::new();
        self.messages.record(record);
        let parent = record
            .parent
            .map_or_else(Position::default, |id| self.positions[id as usize]);
        let mut position = parent;
        let entry = match &record.event {
            Event::SourceEntry { format, entry } if format == "pi" => {
                position.imported = (entry["type"] != "session").then_some(record.id);
                if position.imported.is_some() {
                    if let Some(source_id) = entry["id"].as_str() {
                        self.source_ids.insert(source_id.to_owned(), record.id);
                    }
                    let mut entry = entry.clone();
                    for reference in ["targetId", "fromId", "firstKeptEntryId"] {
                        if let Some(id) = entry[reference]
                            .as_str()
                            .and_then(|id| self.source_ids.get(id))
                        {
                            entry[reference] = json!(id.to_string());
                        }
                    }
                    Some(entry)
                } else {
                    None
                }
            }
            Event::CacheWarm {
                metadata,
                usage,
                note,
            } => {
                let identity = metadata.resolved_model.as_ref();
                let mut entry = json!({
                    "type":"usage",
                    "kind":"cache_warm",
                    "provider":identity.map(|id| id.provider.as_str()).unwrap_or_default(),
                    "model":identity.map(|id| id.model.as_str()).unwrap_or_default(),
                    "usage":super::rpc_events::usage_value(*usage, metadata)
                });
                if let Some(note) = note {
                    entry["note"] = json!(note);
                }
                Some(entry)
            }
            // An imported source entry already includes the visible representation.
            _ if parent
                .imported
                .is_some_and(|id| imported_conversion(records, id, record)) =>
            {
                position.imported = None;
                if matches!(record.event, Event::ModelSelected { .. }) {
                    position.model = Some(record.id);
                }
                None
            }
            Event::ModelSelected {
                model,
                provider,
                thinking,
            } => {
                let current = selection(model, provider.as_deref(), thinking.as_deref());
                position.model = Some(record.id);
                let previous = parent.model.and_then(|id| {
                    let Event::ModelSelected {
                        model,
                        provider,
                        thinking,
                    } = &records[id as usize - 1].event
                    else {
                        return None;
                    };
                    Some(selection(model, provider.as_deref(), thinking.as_deref()))
                });
                let model_changed =
                    previous.is_none_or(|old| (old.0, old.1) != (current.0, current.1));
                let thinking_changed = previous.is_none_or(|old| old.2 != current.2);
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
                    self.positions.push(position);
                    return entries;
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
                let target = self
                    .positions
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
                "type":"context_edit","targetId":wire_id(self.positions.get(*target as usize).and_then(|target| target.visible)),
                "replacement":replacement.as_ref().map(|content| json!({"content":content}))
            })),
            Event::Compacted {
                summary,
                retain_from,
                ..
            } => {
                let first = retain_from
                    .and_then(|id| self.positions.get(id as usize)?.visible)
                    .unwrap_or(record.id);
                let mut entry = json!({
                    "type":"compaction","summary":summary,"firstKeptEntryId":first.to_string(),
                    "tokensBefore":compaction_tokens(records, record, session)
                });
                add_summary_usage(&mut entry, records, record);
                Some(entry)
            }
            Event::BranchSummary {
                summary,
                from,
                extension_summary,
                read_files,
                modified_files,
                ..
            } => {
                let from = from.and_then(|id| self.positions.get(id as usize)?.visible);
                let mut entry = json!({
                    "type":"branch_summary","summary":summary,"fromId":wire_id(from),
                    "details":{"readFiles":read_files,"modifiedFiles":modified_files}
                });
                if let Some(extension) = extension_summary {
                    entry["fromHook"] = json!(true);
                    if let Some(details) = &extension.details {
                        entry["details"] = details.clone();
                    } else {
                        entry
                            .as_object_mut()
                            .expect("branch entry")
                            .remove("details");
                    }
                }
                add_summary_usage(&mut entry, records, record);
                Some(entry)
            }
            Event::ExtensionStates { states } => Some(json!({
                "type":"custom","customType":"pi-rs:extension-states","data":states
            })),
            _ => self
                .messages
                .raw_message(records, record)
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
        self.positions.push(position);
        entries
    }
}

fn compaction_tokens(records: &[Record], record: &Record, session: Option<&Session>) -> u64 {
    if let Event::Compacted {
        tokens_before: Some(tokens),
        ..
    } = &record.event
    {
        return *tokens;
    }
    if let Some(session) = session {
        return super::rpc_messages::compaction_tokens(session, record);
    }
    Session::from_records(records[..record.id as usize].to_vec()).map_or(0, |session| {
        super::rpc_messages::compaction_tokens(&session, record)
    })
}

fn imported_conversion(records: &[Record], source: u64, record: &Record) -> bool {
    let source = &records[source as usize - 1];
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
        .find(|record| imported_conversion(session.records(), id, record))
        .map_or(id, |record| record.id)
}

pub(super) fn changes(session: &Session, previous: usize) -> Vec<Value> {
    let records = session.records();
    let Some((prefix, appended)) = records.split_at_checked(previous) else {
        return Vec::new();
    };
    if appended.is_empty() {
        return Vec::new();
    }
    let mut cursor = Cursor::default();
    cursor.advance(prefix);
    appended
        .iter()
        .flat_map(|record| cursor.append(records, record, Some(session)))
        .collect()
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

fn add_summary_usage(entry: &mut Value, records: &[Record], record: &Record) {
    if let Some((usage, metadata)) = record.summary_usage(records) {
        let metadata = metadata.map(|details| details.metadata).unwrap_or_default();
        entry["usage"] = super::rpc_events::usage_value(usage, &metadata);
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
        let share = Entries::new(&session)
            .write_share_branch(&session, std::path::Path::new("."))
            .unwrap();
        let share: Vec<Value> = share
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        assert_eq!(share[0]["type"], "session");
        assert_eq!(share[0]["version"], 3);
        assert_eq!(share[1], entries[0]);
        assert_eq!(share[2..], entries[2..]);
    }
}
