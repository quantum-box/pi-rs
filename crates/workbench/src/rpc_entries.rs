//! Pi session entries omit the native journal's execution bookkeeping.
use std::collections::{HashMap, HashSet};

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
    response_model: Option<u64>,
    explicit_model_seen: bool,
}

pub(super) struct Entries {
    values: Vec<Value>,
    leaf: Option<String>,
}

impl Entries {
    pub(super) fn apply_message_counts(&self, stats: &mut pi_rs::SessionStats) {
        stats.total_messages = 0;
        stats.user_messages = 0;
        stats.assistant_messages = 0;
        stats.tool_results = 0;
        stats.tool_calls = 0;
        for entry in self
            .values
            .iter()
            .filter(|entry| entry["type"] == "message")
        {
            stats.total_messages += 1;
            let message = &entry["message"];
            match message["role"].as_str() {
                Some("user") => stats.user_messages += 1,
                Some("toolResult") => stats.tool_results += 1,
                Some("assistant") => {
                    stats.assistant_messages += 1;
                    if let Some(blocks) = message["content"].as_array() {
                        stats.tool_calls += blocks
                            .iter()
                            .filter(|block| block["type"] == "toolCall")
                            .count() as u64;
                    }
                }
                _ => {}
            }
        }
    }

    pub(super) fn is_user_message(&self, id: u64) -> bool {
        let id = id.to_string();
        self.values.iter().any(|entry| {
            entry["id"] == id && entry["type"] == "message" && entry["message"]["role"] == "user"
        })
    }

    pub(super) fn has_leaf(&self) -> bool {
        self.leaf.is_some()
    }

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
        let mut remaining: HashSet<_> = self
            .values
            .iter()
            .filter_map(|entry| entry["id"].as_str().map(str::to_owned))
            .collect();
        let mut children = HashMap::<String, Vec<Value>>::new();
        let mut roots = Vec::new();
        for entry in self.values.into_iter().rev() {
            let id = entry["id"].as_str().expect("entry has id");
            remaining.remove(id);
            let parent = entry["parentId"]
                .as_str()
                .filter(|parent| remaining.contains(*parent))
                .map(str::to_owned);
            let mut descendants = children.remove(id).unwrap_or_default();
            descendants.reverse();
            descendants.sort_by_cached_key(|node| {
                node["entry"]["timestamp"]
                    .as_str()
                    .and_then(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).ok())
                    .map(|timestamp| timestamp.timestamp_millis())
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
                if entry["type"] == "model_change" {
                    position.response_model = None;
                    position.explicit_model_seen = true;
                } else if entry["type"] == "message"
                    && entry["message"]["role"] == "assistant"
                    && entry["message"]["api"] != "pi-virtual"
                {
                    position.response_model = Some(record.id);
                }
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
            Event::SourceEntry { format, .. } if format == "pi-rs:native-model-selection" => {
                position.imported = None;
                None
            }
            Event::SourceEntry { format, .. }
                if matches!(
                    format.as_str(),
                    "pi-rs:thinking-selection" | "pi-rs:request-identity"
                ) =>
            {
                position.imported = None;
                None
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
                // A codemode source entry converts into both its original
                // custom entry and a derived extension-state snapshot.
                if !matches!(&record.event, Event::CustomEntry { entry } if entry.custom_type == "codemode-store")
                {
                    position.imported = None;
                }
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
                let thinking_only = record.parent.and_then(|id| records.get(id as usize - 1)).is_some_and(|parent| matches!(&parent.event, Event::SourceEntry { format, .. } if format == "pi-rs:thinking-selection"));
                if thinking_only {
                    let entry = json!({"type":"thinking_level_change","thinkingLevel":current.2});
                    entries.push(with_identity(
                        entry,
                        &record.id.to_string(),
                        wire_id(parent.visible),
                        record,
                    ));
                    position.visible = Some(record.id);
                    self.positions.push(position);
                    return entries;
                }
                position.response_model = None;
                position.explicit_model_seen = true;
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
                // A response may have been routed by a virtual selection. An
                // explicit native selection must override that selection even
                // when it names the same physical model as the response.
                let model_changed = !parent.explicit_model_seen
                    || parent.response_model.is_some()
                    || previous.is_none_or(|old| (old.0, old.1) != (current.0, current.1));
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
            Event::CustomEntry { entry } => {
                let mut value = json!({"type":"custom", "customType":entry.custom_type});
                if let Some(data) = &entry.data {
                    value["data"] = data.clone();
                }
                Some(value)
            }
            Event::CustomMessage { message } => {
                let mut value = json!({"type":"custom_message", "customType":message.custom_type, "content":message.content, "display":message.display});
                if let Some(details) = &message.details {
                    value["details"] = details.clone();
                }
                Some(value)
            }
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
                | Event::CustomMessage { .. }
                | Event::Assistant { .. }
                | Event::BranchSummary { .. }
                | Event::CompactionSummaryMessage { .. }
                | Event::ToolFinished { .. }
                | Event::SystemCheckpoint { .. }
                | Event::BashExecution { .. }
        ) | (
            Some("custom_message"),
            Event::User { .. } | Event::UserWithImages { .. } | Event::CustomMessage { .. }
        ) | (
            Some("model_change" | "thinking_level_change"),
            Event::ModelSelected { .. }
        ) | (Some("session_info"), Event::SessionInfo { .. })
            | (Some("label"), Event::Label { .. })
            | (Some("context_edit"), Event::ContextEdited { .. })
            | (Some("compaction"), Event::Compacted { .. })
            | (Some("branch_summary"), Event::BranchSummary { .. })
            | (
                Some("custom"),
                Event::ExtensionStates { .. } | Event::CustomEntry { .. }
            )
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
    fn native_custom_entries_are_visible_without_entering_messages() {
        let mut session = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        session
            .append_user_with_images("question".into(), Vec::new(), None)
            .unwrap();
        let before = session.current_messages().len();
        session
            .append_custom_entry(pi_rs::CustomEntry {
                custom_type: "bookmark".into(),
                data: Some(json!({"note":"remember"})),
            })
            .unwrap();
        let entries = Entries::new(&session);
        let entry = entries
            .values
            .iter()
            .find(|entry| entry["type"] == "custom" && entry["customType"] == "bookmark")
            .unwrap();
        assert_eq!(entry["data"]["note"], "remember");
        assert!(entry["id"].is_string());
        assert!(entry["parentId"].is_string());
        assert_eq!(session.current_messages().len(), before);
    }

    #[test]
    fn native_custom_messages_preserve_rpc_metadata_and_context_edits() {
        let mut session = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        session
            .append_custom_message(pi_rs::CustomMessage {
                custom_type: "hint".into(),
                content: pi_rs::ContextContent::Text("original".into()),
                display: false,
                details: Some(json!({"private":true})),
            })
            .unwrap();
        let target = session.head();
        let entries = Entries::new(&session);
        let entry = entries
            .values
            .iter()
            .find(|entry| entry["type"] == "custom_message")
            .unwrap();
        assert_eq!(entry["customType"], "hint");
        assert_eq!(entry["content"], "original");
        assert_eq!(entry["display"], false);
        assert_eq!(entry["details"]["private"], true);
        let (messages, _) = super::super::rpc_messages::snapshot(&session);
        assert_eq!(messages[0]["role"], "custom");
        assert_eq!(messages[0]["content"], "original");
        assert_eq!(messages[0]["details"]["private"], true);
        session
            .edit_context(target, Some(pi_rs::ContextContent::Text("edited".into())))
            .unwrap();
        let (messages, native) = super::super::rpc_messages::snapshot(&session);
        assert_eq!(messages[0]["role"], "custom");
        assert_eq!(messages[0]["content"], "edited");
        assert_eq!(messages[0]["details"]["private"], true);
        assert!(matches!(&native[0], pi_rs::Message::User { content } if content == "edited"));
    }

    #[test]
    fn forking_accepts_only_visible_user_message_entries() {
        let entries = Entries {
            values: vec![
                json!({"id":"1","type":"custom_message","content":"text"}),
                json!({"id":"2","type":"message","message":{"role":"user","content":[]}}),
                json!({"id":"3","type":"message","message":{"role":"assistant","content":[]}}),
            ],
            leaf: Some("3".into()),
        };
        assert!(!entries.is_user_message(1));
        assert!(entries.is_user_message(2));
        assert!(!entries.is_user_message(3));
        assert!(!entries.is_user_message(4));
    }

    #[test]
    fn statistics_count_original_message_roles_after_import() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        for entry in [
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"question"}}),
            json!({"type":"custom_message","id":"c","parentId":"u","customType":"fixture","content":"extension context","display":true}),
            json!({"type":"message","id":"h","parentId":"c","message":{"role":"hookMessage","content":"hook context"}}),
            json!({"type":"message","id":"s","parentId":"h","message":{"role":"system","content":"system context"}}),
            json!({"type":"message","id":"a","parentId":"s","message":{"role":"assistant","content":[{"type":"text","text":"answer"}]}}),
        ] {
            writeln!(file, "{entry}").unwrap();
        }
        let session = super::super::session_import::read(file.path()).unwrap();
        let mut stats = session.statistics();
        Entries::new(&session).apply_message_counts(&mut stats);
        assert_eq!(stats.total_messages, 4);
        assert_eq!(stats.user_messages, 1);
        assert_eq!(stats.assistant_messages, 1);
    }

    #[test]
    fn tree_orders_children_by_instant_and_keeps_equal_times_in_append_order() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "{}", json!({"type":"session","version":3})).unwrap();
        for (id, parent, timestamp) in [
            ("root", None, "2025-01-01T08:00:00Z"),
            ("later", Some("root"), "2025-01-01T09:30:00Z"),
            ("earlier", Some("root"), "2025-01-01T10:00:00+02:00"),
            ("equal", Some("root"), "2025-01-01T08:00:00.000Z"),
        ] {
            let entry = json!({"type":"message","id":id,"parentId":parent,"timestamp":timestamp,
                "message":{"role":"user","content":id}});
            writeln!(file, "{entry}").unwrap();
        }
        let session = super::super::session_import::read(file.path()).unwrap();
        let tree = Entries::new(&session).tree();
        let children = tree["tree"][0]["children"].as_array().unwrap();
        let names: Vec<_> = children
            .iter()
            .map(|node| node["entry"]["message"]["content"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["earlier", "equal", "later"]);
        assert_eq!(tree["leafId"], children[1]["entry"]["id"]);
    }

    #[test]
    fn tree_exposes_orphan_and_self_parented_entries_as_roots() {
        let entries = Entries {
            values: vec![
                json!({"id":"root","parentId":null}),
                json!({"id":"orphan","parentId":"missing"}),
                json!({"id":"self","parentId":"self"}),
                json!({"id":"child","parentId":"orphan"}),
            ],
            leaf: Some("child".into()),
        };
        let tree = entries.tree();
        let roots = tree["tree"].as_array().unwrap();
        let ids: Vec<_> = roots
            .iter()
            .map(|node| node["entry"]["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["root", "orphan", "self"]);
        assert_eq!(roots[1]["children"][0]["entry"]["id"], "child");
    }

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
