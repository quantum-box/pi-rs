//! Explicit, read-only import of external session snapshots.
use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;

use pi_rs::pi_import::{Branch, convert, required};
use pi_rs::{Error, Event, Record, Result, Session};
use serde_json::Value;
use uuid::Uuid;

pub fn read(path: &Path) -> Result<Session> {
    let mut text = String::new();
    std::fs::File::open(path)?
        .take(128 * 1024 * 1024 + 1)
        .read_to_string(&mut text)?;
    if text.len() > 128 * 1024 * 1024 {
        return Err(Error::Session("import exceeds 128 MiB".into()));
    }
    let entries = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            if line.len() > 16 * 1024 * 1024 {
                return Err(Error::Session("imported entry exceeds 16 MiB".into()));
            }
            Ok(serde_json::from_str::<Value>(line)?)
        })
        .collect::<Result<Vec<_>>>()?;
    if entries.is_empty() {
        return Err(Error::Session("imported session is empty".into()));
    }
    if entries
        .first()
        .is_some_and(|entry| entry["type"] == "session")
    {
        import_pi(entries)
    } else {
        Session::from_serialized_records(entries)
    }
}

// Source entries form an append-only forest. Binary lifting lets repeated
// compactions validate their retained boundary without scanning the branch.
struct SourceAncestry {
    nodes: Vec<(usize, Vec<usize>)>,
}
impl Default for SourceAncestry {
    fn default() -> Self {
        Self {
            nodes: vec![(0, Vec::new())],
        }
    }
}
impl SourceAncestry {
    fn add(&mut self, parent: usize) -> usize {
        let mut jumps = vec![parent];
        while let Some(next) = self.nodes[*jumps.last().unwrap()].1.get(jumps.len() - 1) {
            jumps.push(*next);
        }
        let index = self.nodes.len();
        self.nodes.push((self.nodes[parent].0 + 1, jumps));
        index
    }

    fn is_ancestor(&self, ancestor: usize, mut node: usize) -> bool {
        let (ancestor_depth, node_depth) = (self.nodes[ancestor].0, self.nodes[node].0);
        if ancestor_depth > node_depth {
            return false;
        }
        let mut difference = node_depth - ancestor_depth;
        let mut bit = 0;
        while difference != 0 {
            if difference & 1 != 0 {
                node = self.nodes[node].1[bit];
            }
            difference >>= 1;
            bit += 1;
        }
        node == ancestor
    }
}

struct Importer {
    records: Vec<Record>,
    timestamp: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum BranchSelection {
    Native,
    Unresolved {
        thinking: Option<String>,
    },
    Selected {
        provider: String,
        model: String,
        thinking: Option<String>,
    },
}

/// Resolve Pi source entries only when restoring a branch, with the current catalog.
/// A native selection made after import takes precedence over the imported history.
/// The catalog returns whether a registered model is virtual; None means absent.
pub(super) fn branch_selection(
    session: &Session,
    catalog: impl Fn(&str, &str) -> Option<bool>,
) -> BranchSelection {
    let branch = session.branch();
    let projection = super::rpc_messages::Projection::new(branch.iter().copied());
    let thinking = branch
        .iter()
        .rev()
        .find_map(|record| match &record.event {
            Event::ModelSelected { thinking, .. } => Some(thinking.clone()),
            Event::BranchModelSelected { .. }
            | Event::BranchSummary {
                fallback_model: Some(_),
                ..
            } => Some(None),
            Event::SourceEntry { format, entry }
                if format == "pi" && entry["type"] == "thinking_level_change" =>
            {
                Some(entry["thinkingLevel"].as_str().map(str::to_owned))
            }
            _ => None,
        })
        .flatten();
    let mut saw_pi = false;
    for (index, record) in branch.iter().enumerate().rev() {
        match &record.event {
            Event::BranchModelSelected {
                provider, model, ..
            } => {
                return BranchSelection::Selected {
                    provider: provider.clone(),
                    model: model.clone(),
                    thinking,
                };
            }
            Event::BranchSummary {
                fallback_model: Some(selection),
                ..
            } => {
                return BranchSelection::Selected {
                    provider: selection.provider.clone(),
                    model: selection.model.clone(),
                    thinking,
                };
            }
            _ => {}
        }
        if let Event::Assistant {
            request_id: Some(request_id),
            ..
        } = &record.event
        {
            let prior = branch[..index].iter().rev().find(|prior| {
                !matches!(
                    prior.event,
                    Event::ModelFinished { .. } | Event::ModelSelected { .. }
                )
            });
            let imported = prior.is_some_and(|prior| prior.timestamp == record.timestamp && matches!(&prior.event, Event::SourceEntry { format, entry } if format == "pi" && entry["type"] == "message" && entry["message"]["role"] == "assistant"));
            if !imported && branch[..index].iter().any(|prior| matches!(&prior.event, Event::ModelRequested { request_id: Some(id), .. } if id == request_id)) {
                let mut change = None;
                for (i, prior) in branch[..index].iter().enumerate().rev() {
                    match &prior.event {
                        Event::BranchModelSelected { provider, model, .. } => return BranchSelection::Selected { provider: provider.clone(), model: model.clone(), thinking },
                        Event::BranchSummary { fallback_model: Some(selection), .. } => return BranchSelection::Selected { provider: selection.provider.clone(), model: selection.model.clone(), thinking },
                        _ => {}
                    }
                    if matches!(prior.event, Event::ModelSelected { .. }) {
                        let derived = i.checked_sub(1).and_then(|i| branch.get(i)).is_some_and(|parent| matches!(&parent.event, Event::SourceEntry { format, .. } if format == "pi-rs:thinking-selection" || (format == "pi" && parent.timestamp == prior.timestamp && prior.run_id.is_none())));
                        if !derived {
                            if let Event::ModelSelected { model, provider: Some(provider), .. } = &prior.event {
                                // A native explicit selection is authoritative even when the
                                // response names a routed physical model and the catalog is absent.
                                return BranchSelection::Selected { provider: provider.clone(), model: model.clone(), thinking };
                            }
                            return BranchSelection::Native;
                        }
                    }
                    if let Event::SourceEntry { format, entry } = &prior.event && format == "pi" && entry["type"] == "model_change" {
                        change = Some(entry);
                        break;
                    }
                }
                if let Some(change) = change && let (Some(provider), Some(model)) = (change["provider"].as_str(), change["modelId"].as_str()) && catalog(provider, model) == Some(true) {
                    return selected(&change["provider"], &change["modelId"], thinking);
                }
                if let Some(message) = projection.raw_message(session.records(), record) {
                    return selected(&message["provider"], &message["model"], thinking);
                }
            }
        }
        if matches!(record.event, Event::ModelSelected { .. }) {
            let thinking_only = index.checked_sub(1).and_then(|i| branch.get(i)).is_some_and(|parent| matches!(&parent.event, Event::SourceEntry { format, .. } if format == "pi-rs:thinking-selection"));
            if thinking_only {
                continue;
            }
            let imported = index.checked_sub(1).and_then(|i| branch.get(i)).is_some_and(|parent| {
                if record.timestamp != parent.timestamp || record.run_id.is_some() { return false; }
                let (Event::SourceEntry { format, entry }, Event::ModelSelected { model, provider, thinking }) = (&parent.event, &record.event) else { return false; };
                if format != "pi" { return false; }
                match entry["type"].as_str() {
                    Some("model_change") => entry["modelId"].as_str() == Some(model) && entry["provider"].as_str() == provider.as_deref(),
                    Some("thinking_level_change") => entry["thinkingLevel"].as_str() == thinking.as_deref() && branch[..index - 1].iter().any(|prior| matches!(&prior.event, Event::SourceEntry { format, entry } if format == "pi" && (entry["type"] == "model_change" || (entry["type"] == "message" && entry["message"]["role"] == "assistant" && entry["message"]["model"].as_str().is_some())))),
                    Some("message") if entry["message"]["role"] == "assistant" => entry["message"]["model"].as_str() == Some(model) && entry["message"]["provider"].as_str() == provider.as_deref(),
                    _ => false,
                }
            });
            if !imported {
                return BranchSelection::Native;
            }
        }
        let Event::SourceEntry { format, entry } = &record.event else {
            continue;
        };
        if format != "pi" {
            continue;
        }
        saw_pi = true;
        if entry["type"] == "model_change" {
            return selected(&entry["provider"], &entry["modelId"], thinking);
        }
        let message = &entry["message"];
        if entry["type"] != "message"
            || message["role"] != "assistant"
            || message["api"] == "pi-virtual"
        {
            continue;
        }
        let change = branch[..index]
            .iter()
            .rev()
            .find_map(|record| match &record.event {
                Event::SourceEntry { format, entry }
                    if format == "pi" && entry["type"] == "model_change" =>
                {
                    Some(entry)
                }
                _ => None,
            });
        if let Some(change) = change
            && let (Some(provider), Some(model)) =
                (change["provider"].as_str(), change["modelId"].as_str())
            && catalog(provider, model) == Some(true)
        {
            return selected(&change["provider"], &change["modelId"], thinking);
        }
        return selected(&message["provider"], &message["model"], thinking);
    }
    if saw_pi {
        BranchSelection::Unresolved { thinking }
    } else {
        BranchSelection::Native
    }
}

fn selected(provider: &Value, model: &Value, thinking: Option<String>) -> BranchSelection {
    match (
        provider.as_str().filter(|s| !s.is_empty()),
        model.as_str().filter(|s| !s.is_empty()),
    ) {
        (Some(provider), Some(model)) => BranchSelection::Selected {
            provider: provider.into(),
            model: model.into(),
            thinking,
        },
        _ => BranchSelection::Unresolved { thinking },
    }
}
impl Importer {
    fn append(&mut self, branch: &mut Branch, event: Event) -> u64 {
        // Imported Pi history predates native turn-end callback markers. Keep
        // it at journal version 2; new native appends upgrade it to version 3.
        let current_head = self
            .records
            .last()
            .map(|record| match record.event {
                Event::Checkout { target } => target,
                _ => record.id,
            })
            .unwrap_or(0);
        if current_head != branch.head {
            self.records.push(Record {
                timestamp: self.timestamp.clone(),
                version: 2,
                id: self.records.len() as u64 + 1,
                parent: Some(current_head),
                run_id: None,
                event: Event::Checkout {
                    target: branch.head,
                },
            });
        }
        let id = self.records.len() as u64 + 1;
        self.records.push(Record {
            timestamp: self.timestamp.clone(),
            version: 2,
            id,
            parent: (branch.head != 0).then_some(branch.head),
            run_id: None,
            event,
        });
        branch.head = id;
        id
    }
}
fn import_pi(entries: Vec<Value>) -> Result<Session> {
    if entries
        .iter()
        .any(|entry| entry.to_string().len() > 8 * 1024 * 1024)
    {
        return Err(Error::Session("Pi import entry exceeds 8 MiB".into()));
    }
    let header = &entries[0];
    let external_id = header["id"].as_str();
    let version = header["version"].as_u64().unwrap_or(1);
    if !(1..=3).contains(&version) {
        return Err(Error::Session(format!(
            "unsupported Pi session version {version}"
        )));
    }
    let mut importer = Importer {
        records: Vec::new(),
        timestamp: header["timestamp"].as_str().map(str::to_owned),
    };
    let mut compaction_metadata = BTreeMap::new();
    let mut root = Branch::default();
    let native_id = external_id
        .and_then(|id| Uuid::parse_str(id).ok())
        .unwrap_or_else(Uuid::new_v4);
    importer.append(
        &mut root,
        Event::Created {
            session_id: native_id,
        },
    );
    if let Some(id) = external_id.filter(|id| *id != native_id.to_string()) {
        importer.append(
            &mut root,
            Event::SourceEntry {
                format: "pi-rs:external-session-id".into(),
                entry: serde_json::json!({"native_id": native_id, "id": id}),
            },
        );
    }
    importer.append(
        &mut root,
        Event::SessionInfo {
            name: None,
            cwd: header["cwd"].as_str().map(str::to_owned),
        },
    );
    importer.append(
        &mut root,
        Event::SourceEntry {
            format: "pi".into(),
            entry: header.clone(),
        },
    );
    if let Some(path) = header["parentSession"]
        .as_str()
        .filter(|path| !path.is_empty())
    {
        importer.append(&mut root, Event::ParentSession { path: path.into() });
    }
    let mut branches = BTreeMap::<String, Branch>::new();
    let mut ancestry = SourceAncestry::default();
    let mut previous = None;
    let mut legacy_ids: Vec<String> = Vec::new();
    let entry_count = entries.len();
    let source_ids: std::collections::BTreeSet<_> = entries
        .iter()
        .skip(1)
        .filter_map(|entry| entry["id"].as_str().map(str::to_owned))
        .collect();
    for (index, entry) in entries.into_iter().enumerate().skip(1) {
        importer.timestamp = entry["timestamp"].as_str().map(str::to_owned);
        let id = if version == 1 {
            entry["id"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| format!("legacy-{index}"))
        } else {
            required(&entry, "id")?.to_owned()
        };
        if id.is_empty() {
            return Err(Error::Session("imported entry ID is empty".into()));
        }
        if version > 1 && !entry["parentId"].is_null() && !entry["parentId"].is_string() {
            return Err(Error::Session("invalid imported parent ID".into()));
        }
        if branches.contains_key(&id) {
            return Err(Error::Session(format!("duplicate imported entry {id}")));
        }
        let parent = if version == 1 {
            previous.as_deref()
        } else {
            entry["parentId"].as_str()
        };
        let mut branch = match parent {
            Some(parent) => match branches.get(parent) {
                Some(branch) => branch.clone(),
                // Pi exposes a genuinely missing parent as a separate tree root.
                // Do not silently sever a forward reference or a cycle.
                None if !source_ids.contains(parent) => {
                    let mut branch = root.clone();
                    branch.missing_ancestors = true;
                    branch
                }
                None => return Err(Error::Session(format!("missing imported parent {parent}"))),
            },
            None => root.clone(),
        };
        // Pi v1 stored compaction boundaries as indices into the full file,
        // including the session header. Later formats use entry IDs instead.
        let mut converted_entry = None;
        if version == 1
            && entry["type"] == "compaction"
            && let Some(kept_index) = entry["firstKeptEntryIndex"]
                .as_u64()
                .and_then(|i| usize::try_from(i).ok())
        {
            // The current entry already has an ID in Pi's migration. A later
            // v1 entry has not been assigned one yet, so no earlier message
            // is retained when it is selected as the boundary.
            let target = if kept_index == index || (kept_index > index && kept_index < entry_count)
            {
                Some(&id)
            } else {
                kept_index.checked_sub(1).and_then(|i| legacy_ids.get(i))
            };
            if let Some(target) = target {
                let mut migrated = entry.clone();
                migrated["id"] = Value::String(id.clone());
                migrated["firstKeptEntryId"] = Value::String(target.clone());
                converted_entry = Some(migrated);
            }
        }
        let converted = converted_entry.as_ref().unwrap_or(&entry);
        if converted["type"] == "compaction"
            && (branch.missing_ancestors
                || converted["firstKeptEntryId"]
                    .as_str()
                    .and_then(|target| branches.get(target))
                    .is_some_and(|target| target.missing_ancestors))
        {
            return Err(Error::Session(
                "cannot import compaction involving an orphaned branch".into(),
            ));
        }
        if converted["type"] == "compaction"
            && let Some(target) = converted["firstKeptEntryId"].as_str()
            && let Some(target_branch) = branches.get(target)
            && !ancestry.is_ancestor(target_branch.ancestry_index, branch.ancestry_index)
        {
            return Err(Error::Session(format!(
                "imported compaction target {target} is outside its branch"
            )));
        }
        let parent_ancestry = branch.ancestry_index;
        let events = convert(converted, &mut branch, &branches)?;
        let extra_compaction_metadata = (entry["type"] == "compaction"
            && (entry.get("details").is_some() || entry["fromHook"] == true))
            .then(|| {
                let mut metadata = serde_json::json!({"fromExtension": entry["fromHook"] == true});
                if let Some(details) = entry.get("details") {
                    metadata["details"] = details.clone();
                }
                metadata
            });
        let source_id = importer.append(
            &mut branch,
            Event::SourceEntry {
                format: "pi".into(),
                entry,
            },
        );
        // Persist the number of native records produced by this Pi entry.
        // A zero is meaningful: later native children are not conversions.
        compaction_metadata.insert(
            source_id,
            serde_json::json!({
                "pi_rs_import_conversion_count_v1": events.len()
            }),
        );
        branch.entry_start = branch.head + u64::from(!events.is_empty());
        branch.context_target = None;
        for event in events {
            let is_compaction = matches!(event, Event::Compacted { .. });
            let context = matches!(
                event,
                Event::User { .. }
                    | Event::CustomMessage { .. }
                    | Event::CustomMessageDelivered { .. }
                    | Event::Assistant { .. }
                    | Event::ToolFinished { .. }
                    | Event::ToolReconciled { .. }
                    | Event::ImportedUnattributedToolResult { .. }
            );
            let id = importer.append(&mut branch, event);
            if is_compaction && let Some(metadata) = &extra_compaction_metadata {
                compaction_metadata.insert(id, metadata.clone());
            }
            if context {
                branch.context_target = Some(id);
            }
        }
        branch.ancestry_index = ancestry.add(parent_ancestry);
        branches.insert(id.clone(), branch);
        legacy_ids.push(id.clone());
        previous = Some(id);
    }
    Session::from_records_with_metadata(importer.records, compaction_metadata)
}
pub(super) use pi_rs::pi_import::imported_custom_message;

#[cfg(test)]
mod tests {
    use super::*;
    use pi_rs::{Message, Outcome};
    use serde_json::json;

    #[test]
    fn shared_import_google_tool_id_rule_matches_provider() {
        // The core importer cannot depend on the Google adapter. Keep this
        // parity check when either model-family rule changes.
        for model in [
            "gemini-2.5-pro",
            "gemini-3-pro",
            "Gemini-03-flash",
            "models/gemini-live-3-preview",
            "gemini-unknown",
            "claude-test",
            "gpt-oss-20b",
            "other",
        ] {
            assert_eq!(
                pi_rs::pi_import::google_uses_tool_call_ids(model),
                pi_rs::google::uses_tool_call_ids(model),
                "{model}"
            );
        }
    }

    #[test]
    fn fork_v1_uses_later_global_label_state_after_reopen() {
        let session = import_pi(vec![
            json!({"type":"session","version":1}),
            json!({"type":"message","message":{"role":"user","content":"first"}}),
            json!({"type":"label","targetId":"legacy-1","label":"later","timestamp":"2024-01-03T00:00:00Z"}),
        ]).unwrap();
        let target = session
            .records()
            .iter()
            .find(|r| matches!(&r.event, Event::User { content, .. } if content == "first"))
            .unwrap()
            .id;
        let fork = session.fork_session(target).unwrap();
        let check = |fork: &Session| {
            assert!(fork.labels().values().any(|label| label == "later"));
            assert!(
                fork.label_timestamps()
                    .values()
                    .any(|time| time == "2024-01-03T00:00:00Z")
            );
        };
        check(&fork);
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fork.jsonl");
        fork.export_jsonl(&path).unwrap();
        check(&read(&path).unwrap());
    }

    #[test]
    fn fork_imported_labels_resolve_globally_and_remap_compaction() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"first"}}),
            json!({"type":"message","id":"b","parentId":"a","message":{"role":"user","content":"second"}}),
            json!({"type":"label","id":"l","parentId":"b","targetId":"a","label":"stale","timestamp":"2024-01-01T00:00:00Z"}),
            json!({"type":"compaction","id":"c","parentId":"l","firstKeptEntryId":"l","summary":"summary","tokensBefore":42,"fromHook":true,"details":{"source":"extension"}}),
            json!({"type":"label","id":"clear","parentId":"b","targetId":"a","timestamp":"2024-01-02T00:00:00Z"}),
            json!({"type":"label","id":"set","parentId":"clear","targetId":"b","label":"current","timestamp":"2024-01-03T00:00:00Z"}),
        ]).unwrap();
        let compact_id = session
            .records()
            .iter()
            .find(|r| matches!(r.event, Event::Compacted { .. }))
            .unwrap()
            .id;
        let fork = session.fork_session(compact_id).unwrap();
        let entries = super::super::rpc_entries::Entries::new(&fork)
            .after(None)
            .unwrap();
        let entries = entries["entries"].as_array().unwrap();
        let labels: Vec<_> = entries.iter().filter(|e| e["type"] == "label").collect();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0]["label"], "current");
        assert_eq!(labels[0]["timestamp"], "2024-01-03T00:00:00Z");
        let second = entries
            .iter()
            .find(|e| e["type"] == "message" && e["message"]["content"] == "second")
            .unwrap();
        assert_eq!(labels[0]["targetId"], second["id"]);
        let compact = entries.iter().find(|e| e["type"] == "compaction").unwrap();
        assert_eq!(compact["firstKeptEntryId"], compact["id"]);
        let native = fork
            .records()
            .iter()
            .find(|r| matches!(r.event, Event::Compacted { .. }))
            .unwrap();
        assert_eq!(
            fork.record_metadata(native.id),
            Some(&json!({"fromExtension":true,"details":{"source":"extension"}}))
        );
        let messages = fork.current_messages();
        assert!(!messages.iter().any(
            |m| matches!(m, Message::User { content } if content == "first" || content == "second")
        ));
    }

    #[test]
    fn fork_imported_sources_and_compaction_survive_reopen() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"retained"}}),
            json!({"type":"custom","id":"x","parentId":"u","customType":"fixture","data":{"k":1}}),
            json!({"type":"compaction","id":"c","parentId":"x","firstKeptEntryId":"u","summary":"summary","tokensBefore":10}),
        ]).unwrap();
        let fork = session.fork_session(session.head()).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fork.jsonl");
        fork.export_jsonl(&path).unwrap();
        let reopened = read(&path).unwrap();
        for session in [&fork, &reopened] {
            let projection = super::super::rpc_entries::Entries::new(session)
                .after(None)
                .unwrap();
            let entries = projection["entries"].as_array().unwrap();
            assert!(
                entries
                    .iter()
                    .any(|e| e["type"] == "message" && e["message"]["content"] == "retained")
            );
            assert!(
                entries
                    .iter()
                    .any(|e| e["type"] == "custom" && e["customType"] == "fixture")
            );
            assert!(entries.iter().any(|e| e["type"] == "compaction"));
            assert!(
                session
                    .current_messages()
                    .iter()
                    .any(|m| matches!(m, Message::User { content } if content == "retained"))
            );
        }
    }

    #[test]
    fn fork_imported_label_and_native_clear_share_target_identity() {
        let mut session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"hello"}}),
            json!({"type":"label","id":"l","parentId":"u","targetId":"u","label":"stale"}),
        ]).unwrap();
        let target = session.head();
        let source = session
            .records()
            .iter()
            .find(|r| {
                matches!(&r.event,
            Event::SourceEntry { format, entry } if format == "pi" && entry["id"] == "u")
            })
            .unwrap()
            .id;
        session.set_label(source, None).unwrap();
        let fork = session.fork_session(target).unwrap();
        assert!(fork.labels().is_empty());
    }

    #[test]
    fn fork_imported_multirecord_targets_and_native_clear_share_identity() {
        for entry in [
            json!({"type":"message","id":"a","parentId":"u","message":{"role":"assistant","content":[{"type":"text","text":"answer"}],"stopReason":"stop","usage":{"input":1,"output":1}}}),
            json!({"type":"compaction","id":"a","parentId":"u","firstKeptEntryId":"u","summary":"summary","tokensBefore":10,"usage":{"input":1,"output":1}}),
        ] {
            let mut session = import_pi(vec![
                json!({"type":"session","version":3}),
                json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"hello"}}),
                entry,
                json!({"type":"label","id":"l","parentId":"a","targetId":"a","label":"stale"}),
            ]).unwrap();
            let target = session.head();
            let source = session
                .records()
                .iter()
                .find(|r| {
                    matches!(&r.event,
                Event::SourceEntry { format, entry } if format == "pi" && entry["id"] == "a")
                })
                .unwrap()
                .id;
            assert!(matches!(
                session.records()[source as usize].event,
                Event::ModelFinished { .. }
            ));
            session.set_label(source, None).unwrap();
            assert!(session.fork_session(target).unwrap().labels().is_empty());
        }
    }

    #[test]
    fn fork_completes_partial_imported_user_and_assistant_conversions() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"hello"}}),
            json!({"type":"message","id":"a","parentId":"u","message":{"role":"assistant","content":[{"type":"text","text":"answer"}],"stopReason":"stop"}}),
        ]).unwrap();
        let user_source = session.records().iter().find(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["id"] == "u")).unwrap().id;
        let assistant_source = session.records().iter().find(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["id"] == "a")).unwrap().id;
        let assistant_first = assistant_source + 1;
        assert!(matches!(
            session.records()[assistant_source as usize].event,
            Event::ModelFinished { .. }
        ));
        let user = session.fork_session(user_source).unwrap();
        assert!(
            user.current_messages()
                .iter()
                .any(|m| matches!(m, Message::User { content } if content == "hello"))
        );
        for target in [assistant_source, assistant_first] {
            let fork = session.fork_session(target).unwrap();
            assert!(
                fork.records()
                    .iter()
                    .any(|r| matches!(r.event, Event::Assistant { .. }))
            );
        }
        assert!(session.fork_session(session.head()).is_ok());
    }

    #[test]
    fn fork_rejects_forged_zero_conversion_for_imported_user() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"hello"}}),
        ]).unwrap();
        let source = imported.records().iter().find(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["id"] == "u")).unwrap().id;
        let mut metadata: BTreeMap<_, _> = imported
            .records()
            .iter()
            .filter_map(|record| {
                imported
                    .record_metadata(record.id)
                    .map(|value| (record.id, value.clone()))
            })
            .collect();
        metadata.insert(source, json!({"pi_rs_import_conversion_count_v1":0}));
        let forged =
            Session::from_records_with_metadata(imported.records().to_vec(), metadata).unwrap();
        assert!(forged.fork_session(forged.head()).is_err());
    }

    #[test]
    fn invalid_projection_does_not_restore_or_record_model_selection() {
        use clap::Parser;
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"hello"}}),
        ]).unwrap();
        let source = imported.records().iter().find(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["id"] == "u")).unwrap().id;
        let mut metadata: BTreeMap<_, _> = imported
            .records()
            .iter()
            .filter_map(|r| imported.record_metadata(r.id).map(|m| (r.id, m.clone())))
            .collect();
        metadata.insert(source, json!({"pi_rs_import_conversion_count_v1":0}));
        let mut session =
            Session::from_records_with_metadata(imported.records().to_vec(), metadata).unwrap();
        let mut args = super::super::Cli::parse_from(["pi"]);
        let before = session.records().len();
        assert!(args.restore_session_model(&session).is_err());
        assert!(args.record_automatic_model_selection(&mut session).is_err());
        assert_eq!(session.records().len(), before);
    }

    #[test]
    fn fork_rejects_forged_zero_conversion_for_imported_compaction() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"hello"}}),
            json!({"type":"compaction","id":"c","parentId":"u","firstKeptEntryId":"u","summary":"summary","tokensBefore":10}),
        ]).unwrap();
        let source = imported.records().iter().find(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["id"] == "c")).unwrap().id;
        let mut metadata: BTreeMap<_, _> = imported
            .records()
            .iter()
            .filter_map(|record| {
                imported
                    .record_metadata(record.id)
                    .map(|value| (record.id, value.clone()))
            })
            .collect();
        metadata.insert(source, json!({"pi_rs_import_conversion_count_v1":0}));
        let forged =
            Session::from_records_with_metadata(imported.records().to_vec(), metadata).unwrap();
        assert!(forged.fork_session(forged.head()).is_err());
    }

    #[test]
    fn fork_rejects_forged_assistant_conversion_counts() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"text","text":"answer"}],"stopReason":"stop"}}),
        ]).unwrap();
        let source = imported.records().iter().find(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["id"] == "a")).unwrap().id;
        for count in [0, 3] {
            let mut records = imported.records().to_vec();
            if count == 3 {
                let mut child = records.last().unwrap().clone();
                child.id += 1;
                child.parent = Some(child.id - 1);
                child.event = Event::SessionInfo {
                    name: Some("native".into()),
                    cwd: None,
                };
                records.push(child);
            }
            let mut metadata: BTreeMap<_, _> = imported
                .records()
                .iter()
                .filter_map(|record| {
                    imported
                        .record_metadata(record.id)
                        .map(|value| (record.id, value.clone()))
                })
                .collect();
            metadata.insert(source, json!({"pi_rs_import_conversion_count_v1":count}));
            let forged = Session::from_records_with_metadata(records, metadata).unwrap();
            assert!(
                forged.fork_session(forged.head()).is_err(),
                "accepted forged assistant count {count}"
            );
        }
    }

    #[test]
    fn fork_rejects_imported_assistant_with_mismatched_request_identity() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"text","text":"answer"}],"stopReason":"stop"}}),
        ]).unwrap();
        let mut records = imported.records().to_vec();
        let assistant = records
            .iter_mut()
            .find(|record| matches!(record.event, Event::Assistant { .. }))
            .unwrap();
        if let Event::Assistant { request_id, .. } = &mut assistant.event {
            *request_id = Some(Uuid::new_v4());
        }
        let metadata = imported
            .records()
            .iter()
            .filter_map(|record| {
                imported
                    .record_metadata(record.id)
                    .map(|value| (record.id, value.clone()))
            })
            .collect();
        let forged = Session::from_records_with_metadata(records, metadata).unwrap();
        assert!(forged.fork_session(forged.head()).is_err());
    }

    #[test]
    fn fork_verifies_legacy_call_before_marked_tool_result() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"toolCall","id":"call","name":"read","arguments":{}}]}}),
            json!({"type":"message","id":"r","parentId":"a","message":{"role":"toolResult","toolCallId":"call","toolName":"read","content":[{"type":"text","text":"done"}],"isError":false}}),
        ]).unwrap();
        let source = imported.records().iter().find(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["id"] == "a")).unwrap().id;
        let mut metadata: BTreeMap<_, _> = imported
            .records()
            .iter()
            .filter_map(|record| {
                imported
                    .record_metadata(record.id)
                    .map(|value| (record.id, value.clone()))
            })
            .collect();
        metadata.remove(&source);
        let mixed =
            Session::from_records_with_metadata(imported.records().to_vec(), metadata.clone())
                .unwrap();
        assert!(mixed.fork_session(mixed.head()).is_ok());
        let mut broken = imported.records().to_vec();
        let result = broken
            .iter_mut()
            .find(|record| matches!(record.event, Event::ToolFinished { .. }))
            .unwrap();
        if let Event::ToolFinished { operation_id, .. } = &mut result.event {
            *operation_id = Uuid::new_v4();
        }
        let broken = Session::from_records_with_metadata(broken, metadata).unwrap();
        assert!(broken.fork_session(broken.head()).is_err());
    }

    #[test]
    fn stream_rejects_invalid_unmarked_source_before_descendants() {
        let base = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        let mut records = base.records().to_vec();
        let mut invalid = records[0].clone();
        invalid.id = 2;
        invalid.parent = Some(1);
        invalid.timestamp = None;
        invalid.event = Event::SourceEntry {
            format: "pi".into(),
            entry: json!({"type":"message","id":"r","parentId":null,"message":{"role":"toolResult","toolCallId":"missing","content":"bad"}}),
        };
        records.push(invalid.clone());
        let mut descendant = invalid;
        descendant.id = 3;
        descendant.parent = Some(2);
        descendant.event = Event::SourceEntry {
            format: "pi".into(),
            entry: json!({"type":"message","id":"u","parentId":"r","message":{"role":"user","content":"later"}}),
        };
        records.push(descendant.clone());
        descendant.id = 4;
        descendant.parent = Some(3);
        descendant.event = Event::User {
            content: "later".into(),
            input_id: None,
        };
        records.push(descendant);
        let session = Session::from_records(records).unwrap();
        let mut replay = pi_rs::pi_import::StreamVerifier::new();
        assert!(
            replay
                .advance(session.records(), &BTreeMap::new(), true)
                .is_err()
        );
        assert_eq!(replay.processed(), 1);
        assert!(replay.counts().is_empty());
        assert!(
            replay
                .advance(session.records(), &BTreeMap::new(), true)
                .is_err()
        );
        assert_eq!(replay.processed(), 1);
    }

    #[test]
    fn fork_rejects_forged_positive_compaction_conversion_count() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"hello"}}),
            json!({"type":"compaction","id":"c","parentId":"u","firstKeptEntryId":"u","summary":"summary","tokensBefore":10}),
        ]).unwrap();
        let source = imported.records().iter().find(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["id"] == "c")).unwrap().id;
        let mut records = imported.records().to_vec();
        let mut child = records.last().unwrap().clone();
        child.id += 1;
        child.parent = Some(child.id - 1);
        child.event = Event::SessionInfo {
            name: Some("native".into()),
            cwd: None,
        };
        records.push(child);
        let mut metadata: BTreeMap<_, _> = imported
            .records()
            .iter()
            .filter_map(|record| {
                imported
                    .record_metadata(record.id)
                    .map(|value| (record.id, value.clone()))
            })
            .collect();
        metadata.insert(source, json!({"pi_rs_import_conversion_count_v1":2}));
        let forged = Session::from_records_with_metadata(records, metadata).unwrap();
        assert!(forged.fork_session(forged.head()).is_err());
    }

    #[test]
    fn fork_rejects_forged_conversion_claiming_native_model_child() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"thinking_level_change","id":"t","parentId":null,"thinkingLevel":"high"}),
        ])
        .unwrap();
        let source = imported.head();
        let mut records = imported.records().to_vec();
        let mut child = records.last().unwrap().clone();
        child.id += 1;
        child.parent = Some(source);
        child.event = Event::ModelSelected {
            model: "native".into(),
            provider: None,
            thinking: Some("high".into()),
        };
        records.push(child);
        let mut metadata: BTreeMap<_, _> = imported
            .records()
            .iter()
            .filter_map(|record| {
                imported
                    .record_metadata(record.id)
                    .map(|value| (record.id, value.clone()))
            })
            .collect();
        metadata.insert(source, json!({"pi_rs_import_conversion_count_v1":1}));
        let forged = Session::from_records_with_metadata(records, metadata).unwrap();
        assert!(forged.fork_session(forged.head()).is_err());
    }

    #[test]
    fn fork_ignores_unrelated_sibling_bad_conversion_metadata() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"one"}}),
            json!({"type":"message","id":"b","parentId":null,"message":{"role":"user","content":"two"}}),
        ]).unwrap();
        let first = imported
            .records()
            .iter()
            .find(|r| matches!(&r.event, Event::User { content, .. } if content == "one"))
            .unwrap()
            .id;
        let other = imported
            .records()
            .iter()
            .find(|r| matches!(&r.event, Event::SourceEntry { entry, .. } if entry["id"] == "b"))
            .unwrap()
            .id;
        let mut metadata: BTreeMap<_, _> = imported
            .records()
            .iter()
            .filter_map(|r| imported.record_metadata(r.id).map(|m| (r.id, m.clone())))
            .collect();
        metadata.insert(other, json!({"pi_rs_import_conversion_count_v1":u64::MAX}));
        let session =
            Session::from_records_with_metadata(imported.records().to_vec(), metadata).unwrap();
        assert!(session.fork_session(first).is_ok());
    }

    #[test]
    fn fork_rejects_relevant_duplicate_source_ids_including_v1_synthetic() {
        for version in [1, 3] {
            let entries = if version == 1 {
                vec![
                    json!({"type":"session","version":1}),
                    json!({"type":"message","message":{"role":"user","content":"one"}}),
                    json!({"type":"message","id":"legacy-2","message":{"role":"user","content":"two"}}),
                ]
            } else {
                vec![
                    json!({"type":"session","version":3}),
                    json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"one"}}),
                    json!({"type":"message","id":"b","parentId":null,"message":{"role":"user","content":"two"}}),
                ]
            };
            let imported = import_pi(entries).unwrap();
            let target = imported
                .records()
                .iter()
                .find(|r| matches!(&r.event, Event::User { content, .. } if content == "one"))
                .unwrap()
                .id;
            let mut records = imported.records().to_vec();
            let source = records.iter_mut().find(|r| matches!(&r.event, Event::SourceEntry { entry, .. } if entry["id"] == if version == 1 { "legacy-2" } else { "b" })).unwrap();
            if let Event::SourceEntry { entry, .. } = &mut source.event {
                entry["id"] = json!(if version == 1 { "legacy-1" } else { "a" });
            }
            let metadata = imported
                .records()
                .iter()
                .filter_map(|r| imported.record_metadata(r.id).map(|m| (r.id, m.clone())))
                .collect();
            let session = Session::from_records_with_metadata(records, metadata).unwrap();
            assert!(
                session
                    .fork_session(target)
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("duplicate")
            );
        }
    }

    #[test]
    fn fork_allows_header_id_equal_to_entry_id() {
        for entries in [
            vec![
                json!({"type":"session","version":3,"id":"a"}),
                json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"hello"}}),
            ],
            vec![
                json!({"type":"session","version":1,"id":"legacy-1"}),
                json!({"type":"message","message":{"role":"user","content":"hello"}}),
            ],
        ] {
            let session = import_pi(entries).unwrap();
            assert!(session.fork_session(session.head()).is_ok());
        }
    }

    #[test]
    fn fork_rejects_missing_nonstring_or_empty_relevant_source_id() {
        for version in [2, 3] {
            for value in [None, Some(json!(17)), Some(json!(""))] {
                let imported = import_pi(vec![
                    json!({"type":"session","version":version}),
                    json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"hello"}}),
                ]).unwrap();
                let mut records = imported.records().to_vec();
                let source = records.iter_mut().find(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["id"] == "a")).unwrap();
                if let Event::SourceEntry { entry, .. } = &mut source.event {
                    if let Some(value) = value {
                        entry["id"] = value;
                    } else {
                        entry.as_object_mut().unwrap().remove("id");
                    }
                }
                let metadata = imported
                    .records()
                    .iter()
                    .filter_map(|r| imported.record_metadata(r.id).map(|m| (r.id, m.clone())))
                    .collect();
                let session = Session::from_records_with_metadata(records, metadata).unwrap();
                assert!(
                    session
                        .fork_session(session.head())
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("source ID")
                );
            }
        }
    }

    #[test]
    fn fork_replays_many_linear_codemode_updates_with_bounded_branch_copies() {
        let mut entries = vec![json!({"type":"session","version":3})];
        let mut parent = Value::Null;
        for index in 0..400 {
            let id = format!("e{index}");
            entries.push(json!({"type":"custom","id":id,"parentId":parent,"customType":"codemode-store", "data":{"set":{(format!("key{index}")):"x".repeat(100)},"delete":[]}}));
            parent = json!(id);
        }
        let session = import_pi(entries).unwrap();
        let fork = session.fork_session(session.head()).unwrap();
        assert_eq!(
            fork.records()
                .iter()
                .filter(|record| matches!(record.event, Event::ExtensionStates { .. }))
                .count(),
            400
        );
    }

    #[test]
    fn fork_replays_large_inherited_store_across_many_relevant_siblings() {
        let mut entries = vec![
            json!({"type":"session","version":3}),
            json!({"type":"custom","id":"seed","parentId":null,"customType":"codemode-store","data":{"set":{"large":"x".repeat(1024 * 1024)},"delete":[]}}),
        ];
        for index in 0..160 {
            entries.push(json!({"type":"label","id":format!("l{index}"),"parentId":"seed","targetId":"seed","label":format!("label{index}")}));
        }
        let session = import_pi(entries).unwrap();
        let target = session
            .records()
            .iter()
            .find(|record| matches!(record.event, Event::ExtensionStates { .. }))
            .unwrap()
            .id;
        let fork = session.fork_session(target).unwrap();
        assert!(fork.labels().values().any(|label| label == "label159"));
    }

    #[test]
    fn fork_ignores_unreferenced_duplicate_sibling_source_ids() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"one"}}),
            json!({"type":"message","id":"b","parentId":null,"message":{"role":"user","content":"two"}}),
            json!({"type":"message","id":"c","parentId":null,"message":{"role":"user","content":"three"}}),
        ]).unwrap();
        let target = imported
            .records()
            .iter()
            .find(|r| matches!(&r.event, Event::User { content, .. } if content == "one"))
            .unwrap()
            .id;
        let mut records = imported.records().to_vec();
        let source = records
            .iter_mut()
            .find(|r| matches!(&r.event, Event::SourceEntry { entry, .. } if entry["id"] == "c"))
            .unwrap();
        if let Event::SourceEntry { entry, .. } = &mut source.event {
            entry["id"] = json!("b");
        }
        let metadata = imported
            .records()
            .iter()
            .filter_map(|r| imported.record_metadata(r.id).map(|m| (r.id, m.clone())))
            .collect();
        let session = Session::from_records_with_metadata(records, metadata).unwrap();
        assert!(session.fork_session(target).is_ok());
    }

    #[test]
    fn fork_imported_sibling_does_not_require_other_branch_conversions() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"one"}}),
            json!({"type":"message","id":"b","parentId":null,"message":{"role":"user","content":"two"}}),
        ]).unwrap();
        let first = imported
            .records()
            .iter()
            .find(|record| matches!(&record.event, Event::User { content, .. } if content == "one"))
            .unwrap()
            .id;
        let fork = imported.fork_session(first).unwrap();
        assert!(
            fork.current_messages()
                .iter()
                .any(|message| matches!(message, Message::User { content } if content == "one"))
        );
        assert!(
            !fork
                .current_messages()
                .iter()
                .any(|message| matches!(message, Message::User { content } if content == "two"))
        );
    }

    #[test]
    fn fork_does_not_treat_native_child_of_zero_conversion_as_imported() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"thinking_level_change","id":"t","parentId":null,"thinkingLevel":"high"}),
        ])
        .unwrap();
        let source = session.head();
        let mut records = session.records().to_vec();
        let mut native = records.last().unwrap().clone();
        native.id += 1;
        native.parent = Some(source);
        native.event = Event::ModelSelected {
            model: "native".into(),
            provider: None,
            thinking: Some("high".into()),
        };
        records.push(native);
        let metadata = session
            .records()
            .iter()
            .filter_map(|r| session.record_metadata(r.id).map(|m| (r.id, m.clone())))
            .collect();
        let mut session = Session::from_records_with_metadata(records, metadata).unwrap();
        let native = session.head();
        let changes = super::super::rpc_entries::changes(&session, (native - 1) as usize).unwrap();
        assert!(changes.iter().any(|entry| entry["type"] == "model_change"));
        let mut cursor = super::super::rpc_entries::Cursor::default();
        cursor.set_verified_counts(&session.validated_pi_conversion_counts().unwrap());
        cursor.advance(&session.records()[..(native - 1) as usize]);
        let live = cursor.appended(session.records(), None);
        assert_eq!(changes, live);
        session.set_label(source, Some("source".into())).unwrap();
        session.set_label(native, Some("native".into())).unwrap();
        let fork = session.fork_session(session.head()).unwrap();
        assert_eq!(fork.labels().len(), 2);
        let projection = super::super::rpc_entries::Entries::new(&fork)
            .after(None)
            .unwrap();
        let labels: Vec<_> = projection["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["type"] == "label")
            .collect();
        assert_eq!(labels.len(), 2);
        assert_ne!(labels[0]["targetId"], labels[1]["targetId"]);
    }

    #[test]
    fn fork_resolves_verified_legacy_import_label_target() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"hello"}}),
            json!({"type":"label","id":"l","parentId":"u","targetId":"u","label":"saved"}),
        ]).unwrap();
        let mut legacy = Session::from_records(imported.records().to_vec()).unwrap();
        let target = legacy.head();
        assert!(legacy.fork_session(target).is_ok());
        let source = legacy
            .records()
            .iter()
            .find(|r| {
                matches!(&r.event,
            Event::SourceEntry { entry, .. } if entry["id"] == "u")
            })
            .unwrap()
            .id;
        legacy.set_label(source, None).unwrap();
        assert!(legacy.fork_session(target).unwrap().labels().is_empty());
    }

    #[test]
    fn fork_records_can_be_reconstructed_and_forked_again() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"hello"}}),
            json!({"type":"label","id":"l","parentId":"u","targetId":"u","label":"saved"}),
        ]).unwrap();
        let records = imported.fork_records(imported.head()).unwrap();
        let restored = Session::from_records(records).unwrap();
        let fork = restored.fork_session(restored.head()).unwrap();
        assert!(fork.labels().values().any(|label| label == "saved"));
    }

    #[test]
    fn fork_normalizes_unmarked_legacy_partial_conversions() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"hello"}}),
            json!({"type":"message","id":"a","parentId":"u","message":{"role":"assistant","content":[{"type":"text","text":"answer"}],"stopReason":"stop"}}),
        ]).unwrap();
        let session = Session::from_records(imported.records().to_vec()).unwrap();
        let user = session.records().iter().find(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["id"] == "u")).unwrap().id;
        let assistant = session.records().iter().find(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["id"] == "a")).unwrap().id;
        let fork = session.fork_session(user).unwrap();
        assert!(
            fork.current_messages()
                .iter()
                .any(|message| matches!(message, Message::User { content } if content == "hello"))
        );
        let fork = session.fork_session(assistant + 1).unwrap();
        assert!(
            fork.records()
                .iter()
                .any(|record| matches!(record.event, Event::Assistant { .. }))
        );
    }

    #[test]
    fn fork_rejects_competing_unmarked_legacy_conversions() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"hello"}}),
        ]).unwrap();
        let source = imported.records().iter().find(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["id"] == "u")).unwrap().id;
        let mut records = imported.records().to_vec();
        let mut checkout = records.last().unwrap().clone();
        checkout.id = records.len() as u64 + 1;
        checkout.parent = Some(checkout.id - 1);
        checkout.event = Event::Checkout { target: source };
        records.push(checkout);
        let mut duplicate = records[source as usize].clone();
        duplicate.id = records.len() as u64 + 1;
        duplicate.parent = Some(source);
        records.push(duplicate);
        let legacy = Session::from_records(records).unwrap();
        let mut replay = pi_rs::pi_import::StreamVerifier::new();
        assert!(
            replay
                .advance(
                    &legacy.records()[..source as usize + 1],
                    &BTreeMap::new(),
                    false
                )
                .unwrap()
                .is_none()
        );
        assert!(
            replay
                .advance(legacy.records(), &BTreeMap::new(), false)
                .err()
                .unwrap()
                .to_string()
                .contains("ambiguous")
        );
        assert!(
            legacy
                .validated_pi_conversion_counts()
                .err()
                .unwrap()
                .to_string()
                .contains("ambiguous")
        );
        assert!(
            legacy
                .fork_session(source)
                .err()
                .unwrap()
                .to_string()
                .contains("ambiguous")
        );
    }

    #[test]
    fn fork_resolves_verified_legacy_multirecord_alias() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"text","text":"answer"}],"stopReason":"stop"}}),
            json!({"type":"message","id":"b","parentId":null,"message":{"role":"user","content":"other"}}),
        ]).unwrap();
        let mut legacy = Session::from_records(imported.records().to_vec()).unwrap();
        let source = legacy
            .records()
            .iter()
            .find(|r| matches!(&r.event, Event::SourceEntry { entry, .. } if entry["id"] == "a"))
            .unwrap()
            .id;
        let assistant = legacy
            .records()
            .iter()
            .find(|r| matches!(r.event, Event::Assistant { .. }))
            .unwrap()
            .id;
        let other = legacy
            .records()
            .iter()
            .find(|r| matches!(&r.event, Event::User { content, .. } if content == "other"))
            .unwrap()
            .id;
        legacy.set_label(assistant, Some("saved".into())).unwrap();
        legacy.set_label(source, None).unwrap();
        assert!(legacy.fork_session(assistant).unwrap().labels().is_empty());
        assert!(legacy.fork_session(other).is_ok());
    }

    #[test]
    fn fork_v1_compaction_rewrites_removed_label_boundary() {
        let session = import_pi(vec![
            json!({"type":"session","version":1}),
            json!({"type":"message","message":{"role":"user","content":"old"}}),
            json!({"type":"label","targetId":"legacy-1","label":"bookmark"}),
            json!({"type":"compaction","firstKeptEntryIndex":2,"summary":"summary","tokensBefore":10}),
        ]).unwrap();
        let fork = session.fork_session(session.head()).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fork.jsonl");
        fork.export_jsonl(&path).unwrap();
        let reopened = read(&path).unwrap();
        for session in [&fork, &reopened] {
            let projection = super::super::rpc_entries::Entries::new(session)
                .after(None)
                .unwrap();
            let entries = projection["entries"].as_array().unwrap();
            let compact = entries.iter().find(|e| e["type"] == "compaction").unwrap();
            assert_eq!(compact["firstKeptEntryId"], compact["id"]);
            assert!(
                !session
                    .current_messages()
                    .iter()
                    .any(|m| matches!(m, Message::User { content } if content == "old"))
            );
        }
    }

    #[test]
    fn imported_compaction_metadata_survives_native_export_and_reload() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"question"}}),
            json!({"type":"compaction","id":"c","parentId":"u","firstKeptEntryId":"u","summary":"summary","tokensBefore":42,"fromHook":true,"details":{"source":"extension"}}),
        ]).unwrap();
        let id = session
            .records()
            .iter()
            .find(|record| matches!(record.event, Event::Compacted { .. }))
            .unwrap()
            .id;
        let expected = json!({"fromExtension":true,"details":{"source":"extension"}});
        assert_eq!(session.record_metadata(id), Some(&expected));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("native.jsonl");
        session.export_jsonl(&path).unwrap();
        let restored = read(&path).unwrap();
        assert_eq!(restored.record_metadata(id), Some(&expected));
    }

    #[test]
    fn imported_history_keeps_legacy_version_and_accepts_native_appends() {
        let mut session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"imported"}}),
        ]).unwrap();
        assert!(session.records().iter().all(|record| record.version == 2));
        session
            .append_user_with_images("native".into(), vec![], None)
            .unwrap();
        let current = Session::with_store(pi_rs::MemoryStore::default())
            .unwrap()
            .records()[0]
            .version;
        assert_eq!(session.records().last().unwrap().version, current);
        let restored = Session::from_records(session.records().to_vec()).unwrap();
        assert_eq!(
            restored.conversation_at(restored.head()).unwrap(),
            session.conversation_at(session.head()).unwrap()
        );
    }

    #[test]
    fn imported_assistant_total_tokens_survive_projection_and_edits() {
        let entries = vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"question"}}),
            json!({"type":"message","id":"a","parentId":"u","message":{"role":"assistant","content":[],"stopReason":"stop","usage":{"totalTokens":234}}}),
            json!({"type":"model_change","id":"m","parentId":"a","provider":"ollama","modelId":"new"}),
        ];
        let session = import_pi(entries).unwrap();
        assert_eq!(session.context_usage_estimate().tokens, 234);
        assert_eq!(session.context_usage(1000).unwrap().tokens, Some(234));
    }

    #[test]
    fn imported_total_only_usage_keeps_post_compaction_estimate_available() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"question"}}),
            json!({"type":"compaction","id":"c","parentId":"u","summary":"summary","firstKeptEntryId":"u","tokensBefore":10}),
            json!({"type":"message","id":"a","parentId":"c","message":{"role":"assistant","content":[{"type":"thinking","thinking":"reasoning"}],"stopReason":"stop","usage":{"totalTokens":234}}}),
            json!({"type":"context_edit","id":"e","parentId":"a","targetId":"u","replacement":{"content":"changed"}}),
        ]).unwrap();
        assert!(session.context_usage_estimate().last_usage_record.is_none());
        assert!(session.context_usage(1000).unwrap().tokens.is_some());
    }

    #[test]
    fn imported_edit_keeps_signatures_but_unknown_endpoint_cannot_replay() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","provider":"google","api":"google-generative-ai","model":"gemini","content":[{"type":"text","text":"old"}],"stopReason":"stop"}}),
            json!({"type":"context_edit","id":"e","parentId":"a","targetId":"a","replacement":{"content":[{"type":"text","text":"new","textSignature":"text-sig"},{"type":"thinking","thinking":"private","thinkingSignature":"sig"},{"type":"toolCall","id":"call","name":"read","arguments":{},"thoughtSignature":"call-sig"}]}}),
        ]).unwrap();
        assert!(
            matches!(&session.current_messages()[0], Message::Assistant { content, replay: None, .. } if content == "new")
        );
        let event = session
            .records()
            .iter()
            .find_map(|r| match &r.event {
                Event::ContextEdited {
                    replacement: Some(content),
                    ..
                } => Some(serde_json::to_value(content).unwrap()),
                _ => None,
            })
            .unwrap();
        assert_eq!(event[0]["textSignature"], "text-sig");
        assert_eq!(event[1]["thinkingSignature"], "sig");
        assert_eq!(event[2]["thoughtSignature"], "call-sig");
    }

    #[test]
    fn imported_assistant_edit_counts_replacement_thinking() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"thinking","thinking":"old thinking"},{"type":"text","text":"old"}],"stopReason":"stop","usage":{"totalTokens":100}}}),
            json!({"type":"context_edit","id":"e","parentId":"a","targetId":"a","replacement":{"content":[{"type":"text","text":"a"},{"type":"thinking","thinking":"🦀bc"}]}}),
        ]).unwrap();
        assert_eq!(session.context_usage_estimate().tokens, 2);
    }

    #[test]
    fn retained_tool_result_is_estimated_without_its_compacted_assistant() {
        let mut entries = vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","stopReason":"toolUse","content":[{"type":"toolCall","id":"call","name":"read","arguments":{}}]}}),
            json!({"type":"message","id":"t","parentId":"a","message":{"role":"toolResult","toolCallId":"call","content":[{"type":"text","text":"12345678"}],"isError":false}}),
            json!({"type":"compaction","id":"c","parentId":"t","firstKeptEntryId":"t","summary":"sum","tokensBefore":100}),
        ];
        let session = import_pi(entries.clone()).unwrap();
        assert_eq!(session.context_usage_estimate().tokens, 3);
        entries.push(json!({"type":"context_edit","id":"e","parentId":"c","targetId":"t","replacement":{"content":"abcd"}}));
        assert_eq!(
            import_pi(entries).unwrap().context_usage_estimate().tokens,
            2
        );
    }

    #[test]
    fn imported_summary_messages_count_raw_summary_in_fresh_and_trailing_estimates() {
        let base = vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"b","parentId":null,"message":{"role":"branchSummary","summary":"abcd"}}),
            json!({"type":"message","id":"c","parentId":"b","message":{"role":"compactionSummary","summary":"12345678","tokensBefore":100}}),
        ];
        assert_eq!(
            import_pi(base.clone())
                .unwrap()
                .context_usage_estimate()
                .tokens,
            3
        );
        let mut trailing = vec![
            base[0].clone(),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"text","text":"ok"}],"stopReason":"stop","usage":{"totalTokens":100}}}),
        ];
        let mut b = base[1].clone();
        b["parentId"] = json!("a");
        trailing.push(b);
        trailing.push(base[2].clone());
        assert_eq!(
            import_pi(trailing).unwrap().context_usage_estimate().tokens,
            103
        );
    }

    #[test]
    fn imported_system_images_are_counted_in_trailing_system_delta() {
        let system = json!({"type":"message","id":"s","parentId":"a","message":{"role":"system","content":[{"type":"text","text":"ab"},{"type":"image","data":"AA==","mimeType":"image/png"},{"type":"text","text":"cd"}]}});
        let mut fresh_system = system.clone();
        fresh_system["parentId"] = serde_json::Value::Null;
        // Pi's fallback resolves current system content to text first.
        assert_eq!(
            import_pi(vec![json!({"type":"session","version":3}), fresh_system])
                .unwrap()
                .context_usage_estimate()
                .tokens,
            2
        );
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"text","text":"ok"}],"stopReason":"stop","usage":{"totalTokens":100}}}),
            system,
        ]).unwrap();
        assert_eq!(session.context_usage_estimate().tokens, 1301);
    }

    #[test]
    fn imported_system_section_replacement_uses_pi_trailing_delta() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"s1","parentId":null,"message":{"role":"system","content":"","sections":{"rules":"12345678"}}}),
            json!({"type":"message","id":"a","parentId":"s1","message":{"role":"assistant","content":[{"type":"text","text":"ok"}],"stopReason":"stop","usage":{"totalTokens":100}}}),
            json!({"type":"message","id":"s2","parentId":"a","message":{"role":"system","content":"","sections":{"rules":"abcd"}}}),
        ]).unwrap();
        assert_eq!(session.context_usage_estimate().tokens, 101);
    }

    #[test]
    fn imported_compaction_without_system_snapshot_discards_old_system_from_estimate() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"s","parentId":null,"message":{"role":"system","content":"12345678"}}),
            json!({"type":"message","id":"u","parentId":"s","message":{"role":"user","content":"abcd"}}),
            json!({"type":"compaction","id":"c","parentId":"u","firstKeptEntryId":"u","summary":"sum","tokensBefore":100}),
        ]).unwrap();
        assert_eq!(session.context_usage_estimate().tokens, 2);
    }

    #[test]
    fn failed_imported_assistant_estimates_partial_tool_calls_without_executing_them() {
        for stop in ["error", "aborted"] {
            let session = import_pi(vec![
                json!({"type":"session","version":3}),
                json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","stopReason":stop,"usage":{"totalTokens":100},"content":[{"type":"text","text":"a"},{"type":"thinking","thinking":"bc"},{"type":"toolCall","id":"call","name":"read","arguments":{"path":"x"}}]}}),
            ]).unwrap();
            // 3 text/thinking + 4 tool name + 12 compact JSON characters = 19.
            assert_eq!(session.context_usage_estimate().tokens, 5);
            assert!(session.operations().is_empty());
        }
    }

    #[test]
    fn continuation_counts_empty_assistants_and_respects_omissions() {
        use clap::Parser;
        for reason in ["error", "aborted", "stop"] {
            let entries = vec![
                json!({"type":"session","version":3}),
                json!({"type":"model_change","id":"m","parentId":null,"provider":"ollama","modelId":"alpha"}),
                json!({"type":"thinking_level_change","id":"t","parentId":"m","thinkingLevel":"low"}),
                json!({"type":"message","id":"u","parentId":"t","message":{"role":"user","content":"question"}}),
                json!({"type":"message","id":"a","parentId":"u","message":{"role":"assistant","provider":"ollama","model":"alpha","stopReason":reason,"content":[]}}),
                json!({"type":"context_edit","id":"eu","parentId":"a","targetId":"u","replacement":null}),
            ];
            let session = import_pi(entries.clone()).unwrap();
            assert!(super::super::rpc_messages::snapshot(&session).0.is_empty());
            assert!(
                super::super::rpc_entries::Entries::new(&session)
                    .has_session_context()
                    .unwrap()
            );
            let mut args = super::super::Cli::parse_from(["pi"]);
            args.model = "alpha".into();
            args.model_scope
                .push(super::super::model_selection::ScopedChoice {
                    provider: super::super::Provider::Ollama,
                    model: "alpha".into(),
                    thinking: Some("high".into()),
                });
            args.remember_initial_selection();
            args.restore_session_model(&session).unwrap();
            assert_eq!(args.thinking.as_deref(), Some("low"));

            let mut omitted = entries.clone();
            omitted.push(json!({"type":"context_edit","id":"ea","parentId":"eu","targetId":"a","replacement":null}));
            let omitted = import_pi(omitted).unwrap();
            assert!(
                !super::super::rpc_entries::Entries::new(&omitted)
                    .has_session_context()
                    .unwrap()
            );
            args.restore_session_model(&omitted).unwrap();
            assert_eq!(args.thinking.as_deref(), Some("high"));

            let mut replaced = entries;
            replaced.push(json!({"type":"context_edit","id":"ea","parentId":"eu","targetId":"a","replacement":{"content":[]}}));
            let replaced = import_pi(replaced).unwrap();
            assert!(
                super::super::rpc_entries::Entries::new(&replaced)
                    .has_session_context()
                    .unwrap()
            );
        }
    }

    #[test]
    fn continuation_counts_unpaired_tool_result_before_repair() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","provider":"ollama","model":"m","content":[{"type":"toolCall","id":"call","name":"read","arguments":{}}]}}),
            json!({"type":"message","id":"r","parentId":"a","message":{"role":"toolResult","toolCallId":"call","toolName":"read","content":[{"type":"text","text":"result"}],"isError":false}}),
            json!({"type":"context_edit","id":"e","parentId":"r","targetId":"a","replacement":null}),
        ]).unwrap();
        assert!(super::super::rpc_messages::snapshot(&session).0.is_empty());
        assert!(
            super::super::rpc_entries::Entries::new(&session)
                .has_session_context()
                .unwrap()
        );
    }

    #[test]
    fn continuation_counts_native_empty_failed_assistant() {
        let mut session = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        session
            .append_user_with_images("question".into(), Vec::new(), None)
            .unwrap();
        let user = session.head();
        let mut records = session.records().to_vec();
        let request_id = Uuid::new_v4();
        for event in [
            Event::ModelRequested {
                model: "alpha".into(),
                messages: 1,
                tools: 0,
                max_output_tokens: 128,
                request_id: Some(request_id),
                context_head: None,
                context_sha256: None,
                context_transformed: None,
                context_configuration_sha256: None,
                tool_decision: None,
            },
            Event::ModelFinished {
                request_id,
                metadata: pi_rs::ModelMetadata::default(),
                usage: pi_rs::UsageReport::Unconfirmed,
                failure: Some(pi_rs::ModelFailure::Failed),
                partial: String::new(),
                reply: None,
            },
            Event::Assistant {
                content: String::new(),
                invocations: Vec::new(),
                usage: None,
                request_id: Some(request_id),
            },
        ] {
            let parent = records.last().unwrap().id;
            let version = records.last().unwrap().version;
            records.push(Record {
                id: parent + 1,
                parent: Some(parent),
                version,
                timestamp: None,
                run_id: None,
                event,
            });
        }
        let mut session = Session::from_records(records).unwrap();
        session.edit_context(user, None).unwrap();
        assert!(super::super::rpc_messages::snapshot(&session).0.is_empty());
        assert!(
            super::super::rpc_entries::Entries::new(&session)
                .has_session_context()
                .unwrap()
        );
    }

    #[test]
    fn continuation_uses_active_branch_and_latest_edit() {
        let mut session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"question"}}),
            json!({"type":"context_edit","id":"omit","parentId":"u","targetId":"u","replacement":null}),
            json!({"type":"context_edit","id":"restore","parentId":"omit","targetId":"u","replacement":{"content":"restored"}}),
            json!({"type":"custom","id":"other","parentId":"omit","customType":"metadata"}),
        ]).unwrap();
        assert!(
            !super::super::rpc_entries::Entries::new(&session)
                .has_session_context()
                .unwrap()
        );
        let restored = session.records().iter().find(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["id"] == "restore")).unwrap().id;
        session.checkout(restored).unwrap();
        assert!(
            super::super::rpc_entries::Entries::new(&session)
                .has_session_context()
                .unwrap()
        );
    }

    #[test]
    fn continuation_counts_system_and_compaction_but_not_empty_branch_summary() {
        let system = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"s","parentId":null,"message":{"role":"system","content":""}}),
        ]).unwrap();
        assert!(
            super::super::rpc_entries::Entries::new(&system)
                .has_session_context()
                .unwrap()
        );
        let compacted = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"compaction","id":"c","parentId":null,"firstKeptEntryId":"c","summary":"","tokensBefore":0}),
        ]).unwrap();
        assert!(
            super::super::rpc_entries::Entries::new(&compacted)
                .has_session_context()
                .unwrap()
        );
        let summary = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"branch_summary","id":"s","parentId":null,"fromId":"unused","summary":""}),
        ]).unwrap();
        assert!(
            !super::super::rpc_entries::Entries::new(&summary)
                .has_session_context()
                .unwrap()
        );
    }

    fn native_completion(mut session: Session, provider: &str, model: &str) -> Session {
        session.record_request_identity(provider, model).unwrap();
        let mut records = session.records().to_vec();
        let request_id = Uuid::new_v4();
        for event in [
            Event::ModelRequested {
                model: model.into(),
                messages: 1,
                tools: 0,
                max_output_tokens: 128,
                request_id: Some(request_id),
                context_head: None,
                context_sha256: None,
                context_transformed: None,
                context_configuration_sha256: None,
                tool_decision: None,
            },
            Event::ModelFinished {
                request_id,
                metadata: pi_rs::ModelMetadata {
                    resolved_model: Some(pi_rs::ResolvedModelIdentity {
                        provider: "anthropic".into(),
                        model: "reported-model".into(),
                    }),
                    ..Default::default()
                },
                usage: pi_rs::UsageReport::Unconfirmed,
                failure: None,
                partial: String::new(),
                reply: None,
            },
            Event::Assistant {
                content: "native response".into(),
                invocations: Vec::new(),
                usage: None,
                request_id: Some(request_id),
            },
        ] {
            let parent = records.last().unwrap().id;
            let version = records.last().unwrap().version;
            records.push(Record {
                id: parent + 1,
                parent: Some(parent),
                version,
                timestamp: None,
                run_id: None,
                event,
            });
        }
        Session::from_records(records).unwrap()
    }

    #[test]
    fn native_response_restores_actual_request_identity_after_import() {
        use clap::Parser;
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"model_change","id":"change","parentId":null,"provider":"anthropic","modelId":"old"}),
            json!({"type":"message","id":"a","parentId":"change","message":{"role":"assistant","model":"missing-provider","content":[]}}),
        ]).unwrap();
        let session = native_completion(imported, "ollama", "actual");
        let messages = super::super::rpc_messages::snapshot(&session).0;
        let message = messages
            .iter()
            .find(|message| message["content"][0]["text"] == "native response")
            .unwrap();
        assert_eq!(message["provider"], "ollama");
        assert_eq!(message["model"], "actual");
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("native.jsonl");
        session.export_jsonl(&path).unwrap();
        let reopened = read(&path).unwrap();
        assert!(
            matches!(branch_selection(&reopened, |_, _| None), BranchSelection::Selected { provider, model, .. } if provider == "ollama" && model == "actual")
        );
        let mut args = super::super::Cli::parse_from(["pi"]);
        args.model = "changed-default".into();
        args.remember_initial_selection();
        args.restore_session_model(&reopened).unwrap();
        assert_eq!(args.model, "actual");
    }

    #[test]
    fn native_fallback_after_import_survives_routed_response_and_reopen() {
        let imported = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"model_change","id":"change","parentId":null,"provider":"old","modelId":"shared"}),
        ]).unwrap();
        let source = imported.records().iter().find(|record| matches!(&record.event, Event::SourceEntry { format, entry } if format == "pi" && entry["type"] == "model_change")).unwrap().id;
        let mut records = imported.records()[..source as usize].to_vec();
        let previous = records.last().unwrap();
        let timestamp = if previous.timestamp.as_deref() == Some("2026-01-01T00:00:00.000Z") {
            "2026-01-01T00:00:00.000000Z"
        } else {
            "2026-01-01T00:00:00.000Z"
        };
        let version = previous.version;
        records.push(Record {
            id: source + 1,
            parent: Some(source),
            version,
            timestamp: Some(timestamp.into()),
            run_id: None,
            event: Event::ModelSelected {
                provider: Some("ollama".into()),
                model: "virtual".into(),
                thinking: None,
            },
        });
        let session = native_completion(
            Session::from_records(records).unwrap(),
            "physical",
            "actual",
        );
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("reopen.jsonl");
        session.export_jsonl(&path).unwrap();
        let reopened = read(&path).unwrap();
        assert!(
            matches!(branch_selection(&reopened, |provider, model| Some(provider == "ollama" && model == "virtual")), BranchSelection::Selected { provider, model, .. } if provider == "ollama" && model == "virtual")
        );
        assert!(
            matches!(branch_selection(&reopened, |_, _| None), BranchSelection::Selected { provider, model, .. } if provider == "ollama" && model == "virtual")
        );
        use clap::Parser;
        let mut args = super::super::Cli::parse_from(["pi"]);
        args.model = "initial".into();
        args.remember_initial_selection();
        args.restore_session_model(&reopened).unwrap();
        assert_eq!(args.provider.id(), "ollama");
        assert_eq!(args.model, "virtual");
    }

    #[test]
    fn thinking_only_change_preserves_virtual_selection_in_shared_session() {
        use clap::Parser;
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"model_change","id":"change","parentId":null,"provider":"router","modelId":"virtual"}),
            json!({"type":"message","id":"a","parentId":"change","message":{"role":"assistant","provider":"ollama","model":"physical","content":[]}}),
        ]).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("native.jsonl");
        session.export_jsonl(&path).unwrap();
        let mut args = super::super::Cli::parse_from(["pi", "--offline"]);
        args.session = path.clone();
        args.model = "physical".into();
        args.thinking = Some("low".into());
        super::super::thinking::set(&mut args, "high").unwrap();
        let mut session = Session::open(&path).unwrap();
        assert!(
            matches!(branch_selection(&session, |_, _| Some(true)), BranchSelection::Selected { model, thinking: Some(level), .. } if model == "virtual" && level == "high")
        );
        let bytes = super::super::rpc_entries::Entries::new(&session)
            .write_share_branch(&session, directory.path())
            .unwrap();
        let entries: Vec<Value> = std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry["type"] == "model_change")
                .count(),
            1
        );
        let share = directory.path().join("share.jsonl");
        std::fs::write(&share, bytes).unwrap();
        let reopened = read(&share).unwrap();
        assert!(
            matches!(branch_selection(&reopened, |_, _| Some(true)), BranchSelection::Selected { model, thinking: Some(level), .. } if model == "virtual" && level == "high")
        );
        let before = session.records().len();
        session
            .select_provider_model(None, "physical", Some("high"))
            .unwrap();
        assert!(session.records().len() > before);
        let bytes = super::super::rpc_entries::Entries::new(&session)
            .write_share_branch(&session, directory.path())
            .unwrap();
        std::fs::write(&share, bytes).unwrap();
        let reopened = read(&share).unwrap();
        assert!(
            matches!(branch_selection(&reopened, |_, _| Some(true)), BranchSelection::Selected { model, .. } if model == "physical")
        );
    }

    #[test]
    fn virtual_selection_survives_thinking_change_and_native_response() {
        let mut session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"model_change","id":"change","parentId":null,"provider":"router","modelId":"virtual"}),
            json!({"type":"message","id":"a","parentId":"change","message":{"role":"assistant","provider":"ollama","model":"physical","content":[]}}),
        ]).unwrap();
        session
            .select_thinking_level(None, "physical", "high")
            .unwrap();
        let session = native_completion(session, "ollama", "physical");
        assert!(
            matches!(branch_selection(&session, |_, _| Some(true)), BranchSelection::Selected { model, thinking: Some(level), .. } if model == "virtual" && level == "high")
        );
        assert!(
            matches!(branch_selection(&session, |_, _| None), BranchSelection::Selected { model, thinking: Some(level), .. } if model == "physical" && level == "high")
        );
    }
    #[test]
    fn branch_selection_uses_live_catalog_and_survives_round_trip() {
        let mut session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"thinking_level_change","id":"think","parentId":null,"thinkingLevel":"high"}),
            json!({"type":"model_change","id":"change","parentId":"think","provider":"router","modelId":"virtual"}),
            json!({"type":"message","id":"physical","parentId":"change","message":{"role":"assistant","provider":"physical","model":"actual","content":[]}}),
            json!({"type":"message","id":"failed-route","parentId":"physical","message":{"role":"assistant","provider":"router","model":"virtual","api":"pi-virtual","stopReason":"error","content":[]}}),
            json!({"type":"message","id":"other","parentId":"think","message":{"role":"assistant","provider":"other","model":"model","content":[]}}),
        ]).unwrap();
        assert_eq!(
            branch_selection(&session, |_, _| None),
            BranchSelection::Selected {
                provider: "other".into(),
                model: "model".into(),
                thinking: Some("high".into())
            }
        );
        let target = session.records().iter().find(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["id"] == "failed-route")).unwrap().id;
        session.checkout(target).unwrap();
        let virtual_selection = BranchSelection::Selected {
            provider: "router".into(),
            model: "virtual".into(),
            thinking: Some("high".into()),
        };
        assert_eq!(
            branch_selection(&session, |_, _| Some(true)),
            virtual_selection
        );
        assert_eq!(
            branch_selection(&session, |_, _| Some(false)),
            BranchSelection::Selected {
                provider: "physical".into(),
                model: "actual".into(),
                thinking: Some("high".into())
            }
        );
        assert_eq!(
            branch_selection(&session, |_, _| None),
            BranchSelection::Selected {
                provider: "physical".into(),
                model: "actual".into(),
                thinking: Some("high".into())
            }
        );
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("export.jsonl");
        session.export_jsonl(&path).unwrap();
        let reopened = read(&path).unwrap();
        assert_eq!(
            branch_selection(&reopened, |_, _| Some(true)),
            virtual_selection
        );
    }

    #[test]
    fn imported_tool_call_ids_can_be_reused_after_a_new_turn() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"toolCall","id":"same","name":"read","arguments":{"path":"old"}}]}}),
            json!({"type":"message","id":"u","parentId":"a","message":{"role":"user","content":"retry"}}),
            json!({"type":"message","id":"b","parentId":"u","message":{"role":"assistant","content":[{"type":"toolCall","id":"same","name":"read","arguments":{"path":"new"}}]}}),
            json!({"type":"message","id":"r","parentId":"b","message":{"role":"toolResult","toolCallId":"same","content":[{"type":"text","text":"new result"}],"isError":false}}),
        ]).unwrap();
        let starts: Vec<_> = session
            .records()
            .iter()
            .filter_map(|r| match &r.event {
                Event::ToolStarted { operation_id } => Some(*operation_id),
                _ => None,
            })
            .collect();
        assert_eq!(starts.len(), 2);
        assert_ne!(starts[0], starts[1]);
        assert!(!session.records().iter().any(|r| matches!(&r.event,
            Event::ToolFinished { operation_id, .. } if *operation_id == starts[1])));
        assert!(session.records().iter().any(|r| matches!(&r.event,
            Event::SourceEntry { entry, .. } if entry["id"] == "r")));
        assert!(!session.records().iter().any(|r| matches!(&r.event,
            Event::ToolFinished { operation_id, .. } if *operation_id == starts[0])));
    }

    #[test]
    fn imported_assistant_can_reuse_call_id_but_duplicate_in_one_message_fails() {
        let assistant = |id: &str, parent: Value, calls: Value| json!({"type":"message","id":id,"parentId":parent,"message":{"role":"assistant","content":calls}});
        let call = json!({"type":"toolCall","id":"same","name":"read","arguments":{}});
        let header = json!({"type":"session","version":3});
        assert!(
            import_pi(vec![
                header.clone(),
                assistant("a", Value::Null, json!([call.clone()])),
                assistant("b", json!("a"), json!([call.clone()])),
            ])
            .is_ok()
        );
        assert!(
            import_pi(vec![
                header,
                assistant("a", Value::Null, json!([call.clone(), call])),
            ])
            .is_err()
        );
    }

    #[test]
    fn reused_unfinished_ids_leave_all_ambiguous_results_unresolved() {
        for second_name in ["write", "read"] {
            let session = import_pi(vec![
                json!({"type":"session","version":3}),
                json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"toolCall","id":"x","name":"write","arguments":{}}]}}),
                json!({"type":"message","id":"b","parentId":"a","message":{"role":"assistant","content":[{"type":"toolCall","id":"x","name":second_name,"arguments":{}}]}}),
                json!({"type":"message","id":"r1","parentId":"b","message":{"role":"toolResult","toolCallId":"x","toolName":"write","content":[{"type":"text","text":"first"}]}}),
                json!({"type":"message","id":"r2","parentId":"r1","message":{"role":"toolResult","toolCallId":"x","toolName":second_name,"content":[{"type":"text","text":"second"}]}}),
            ]).unwrap();
            assert_eq!(
                session
                    .records()
                    .iter()
                    .filter(|r| matches!(r.event, Event::ToolStarted { .. }))
                    .count(),
                2
            );
            assert!(
                !session
                    .records()
                    .iter()
                    .any(|r| matches!(r.event, Event::ToolFinished { .. }))
            );
            for id in ["r1", "r2"] {
                assert!(session.records().iter().any(
                    |r| matches!(&r.event, Event::SourceEntry { entry, .. } if entry["id"] == id)
                ));
            }
        }
    }

    #[test]
    fn ambiguous_tool_result_is_visible_and_editable_without_settling_calls() {
        let base = vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"toolCall","id":"x","name":"write","arguments":{}}]}}),
            json!({"type":"message","id":"b","parentId":"a","message":{"role":"assistant","content":[{"type":"toolCall","id":"x","name":"read","arguments":{}}]}}),
            json!({"type":"message","id":"r","parentId":"b","message":{"role":"toolResult","toolCallId":"x","toolName":"write","content":[{"type":"text","text":"original result"}],"isError":true}}),
        ];
        let session = import_pi(base.clone()).unwrap();
        assert!(super::super::sessions::html(&session).contains("original result"));
        assert!(super::super::sessions::html(&session).contains("Unattributed tool result"));
        let (visible, _) = super::super::rpc_messages::snapshot(&session);
        assert!(visible.iter().any(|v| v["role"] == "user"
            && v["nativeUnattributed"] == true
            && v["content"].to_string().contains("original result")
            && v["content"].to_string().contains(", error)")));
        assert!(!visible.iter().any(|v| v["role"] == "toolResult"));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("reopened.jsonl");
        session.export_jsonl(&path).unwrap();
        let reopened = read(&path).unwrap();
        assert!(super::super::sessions::html(&reopened).contains("original result"));
        assert!(
            !session
                .records()
                .iter()
                .any(|r| matches!(r.event, Event::ToolFinished { .. }))
        );
        let mut reconciled = import_pi(base.clone()).unwrap();
        let operation_ids: Vec<_> = reconciled
            .operations()
            .into_iter()
            .map(|(invocation, _)| invocation.operation_id)
            .collect();
        for operation_id in operation_ids {
            reconciled
                .reconcile(
                    operation_id,
                    pi_rs::Outcome::Succeeded("confirmed separately".into()),
                )
                .unwrap();
        }
        assert!(reconciled.conversation_at(reconciled.head()).unwrap().iter().any(
            |m| matches!(m, pi_rs::Message::User { content } if content.contains("original result") && content.contains(", error)"))
        ));
        let mut success = base.clone();
        success[3]["message"]["isError"] = json!(false);
        let success = import_pi(success).unwrap();
        assert!(success.current_messages().iter().any(
            |m| matches!(m, pi_rs::Message::User { content } if content.contains(", success)"))
        ));
        let mut edited = base.clone();
        edited.push(json!({"type":"context_edit","id":"e","parentId":"r","targetId":"r","replacement":{"content":[{"type":"text","text":"replacement"}]}}));
        let edited = import_pi(edited).unwrap();
        let (edited_visible, _) = super::super::rpc_messages::snapshot(&edited);
        assert!(edited_visible.iter().any(|v| v["role"] == "user"
            && v["nativeUnattributed"] == true
            && v["content"].to_string().contains("replacement")
            && v["content"].to_string().contains(", error)")));
        assert!(!edited_visible.iter().any(|v| v["role"] == "toolResult"));
        assert!(edited.current_messages().iter().any(|m| matches!(m, pi_rs::Message::User { content } if content.contains("Historical unattributed") && content.contains("replacement"))));
        let mut omitted = base;
        omitted.push(json!({"type":"context_edit","id":"e","parentId":"r","targetId":"r","replacement":null}));
        let omitted = import_pi(omitted).unwrap();
        assert!(!omitted.current_messages().iter().any(|m| matches!(m, pi_rs::Message::User { content } if content.contains("Historical unattributed"))));
    }

    #[test]
    fn large_unattributed_result_survives_native_export_and_reopen() {
        let large = "x".repeat(7 * 1024 * 1024);
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"toolCall","id":"x","name":"write","arguments":{}}]}}),
            json!({"type":"message","id":"b","parentId":"a","message":{"role":"assistant","content":[{"type":"toolCall","id":"x","name":"read","arguments":{}}]}}),
            json!({"type":"message","id":"r","parentId":"b","message":{"role":"toolResult","toolCallId":"x","content":[{"type":"text","text":large}]}}),
        ]).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("large.jsonl");
        session.export_jsonl(&path).unwrap();
        let reopened = read(&path).unwrap();
        assert!(reopened.records().iter().any(|r| matches!(&r.event,
            Event::ImportedUnattributedToolResult { content: pi_rs::ContextContent::Blocks(blocks), .. }
                if matches!(&blocks[0], pi_rs::ContentBlock::Text { text, .. } if text.len() == 7 * 1024 * 1024))));
    }

    #[test]
    fn imported_late_tool_result_still_matches_without_reuse() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"toolCall","id":"call","name":"read","arguments":{}}]}}),
            json!({"type":"message","id":"u","parentId":"a","message":{"role":"user","content":"interrupt"}}),
            json!({"type":"message","id":"r","parentId":"u","message":{"role":"toolResult","toolCallId":"call","content":[{"type":"text","text":"late"}],"isError":false}}),
        ]).unwrap();
        assert!(
            session
                .records()
                .iter()
                .any(|r| matches!(r.event, Event::ToolFinished { .. }))
        );
    }

    #[test]
    fn missing_parent_import_preserves_orphan_branch_and_source() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"root","parentId":null,"message":{"role":"user","content":"other branch"}}),
            json!({"type":"message","id":"orphan","parentId":"missing","message":{"role":"user","content":"recovered"}}),
            json!({"type":"message","id":"child","parentId":"orphan","message":{"role":"user","content":"next"}}),
        ]).unwrap();
        let check = |session: &pi_rs::Session| {
            let source = |id: &str| {
                session
                    .records()
                    .iter()
                    .find(|record| {
                        matches!(&record.event,
                Event::SourceEntry { entry, .. } if entry["id"] == id)
                    })
                    .unwrap()
            };
            let orphan_id = source("orphan").id.to_string();
            let child_id = source("child").id.to_string();
            assert!(
                matches!(&source("orphan").event, Event::SourceEntry { entry, .. } if entry["parentId"] == "missing")
            );
            let tree = super::super::rpc_entries::Entries::new(session).tree();
            let orphan = tree["tree"]
                .as_array()
                .unwrap()
                .iter()
                .find(|node| node["entry"]["id"] == orphan_id)
                .unwrap();
            assert!(orphan["entry"]["parentId"].is_null());
            assert_eq!(orphan["children"][0]["entry"]["id"], child_id);
            let messages = session.current_messages();
            let text = messages
                .iter()
                .filter_map(|m| match m {
                    pi_rs::Message::User { content } => Some(content.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(text, ["recovered", "next"]);
        };
        check(&session);
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("reopened.jsonl");
        session.export_jsonl(&path).unwrap();
        check(&read(&path).unwrap());
    }

    #[test]
    fn compaction_rejects_boundary_from_another_complete_branch() {
        let header = json!({"type":"session","version":3});
        let first = json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"first"}});
        let second = json!({"type":"message","id":"b","parentId":null,"message":{"role":"user","content":"other branch"}});
        let compact = json!({"type":"compaction","id":"c","parentId":"b","firstKeptEntryId":"a","summary":"summary","tokensBefore":100});
        let error = match import_pi(vec![header, first, second, compact]) {
            Ok(_) => panic!("cross-branch compaction was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("outside its branch"));
    }

    #[test]
    fn compaction_ancestor_and_self_boundaries_keep_rpc_identity_after_reopen() {
        for target in ["a", "b", "c"] {
            let session = import_pi(vec![
                json!({"type":"session","version":3}),
                json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"before"}}),
                json!({"type":"thinking_level_change","id":"b","parentId":"a","thinkingLevel":"off"}),
                json!({"type":"compaction","id":"c","parentId":"b","firstKeptEntryId":target,"summary":"summary","tokensBefore":100}),
            ]).unwrap();
            let check = |session: &pi_rs::Session| {
                let target_record = session
                    .records()
                    .iter()
                    .find(|record| {
                        matches!(&record.event,
                    Event::SourceEntry { entry, .. } if entry["id"] == target)
                    })
                    .unwrap();
                let projected = super::super::rpc_entries::Entries::new(session)
                    .after(None)
                    .unwrap();
                let compaction = projected["entries"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|entry| entry["type"] == "compaction")
                    .unwrap();
                assert_eq!(compaction["firstKeptEntryId"], target_record.id.to_string());
                let messages = session.current_messages();
                assert!(messages.iter().any(|message| matches!(message, pi_rs::Message::User { content } if content.contains("summary"))));
                assert_eq!(messages.iter().any(|message| matches!(message, pi_rs::Message::User { content } if content == "before")), target == "a");
            };
            check(&session);
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("session.jsonl");
            session.export_jsonl(&path).unwrap();
            check(&read(&path).unwrap());
        }
    }

    #[test]
    fn source_ancestry_matches_parent_walk_for_branched_forest() {
        let mut index = SourceAncestry::default();
        let mut parents = vec![0];
        for child in 1..512 {
            let parent = (child * 97 + 31) % child;
            assert_eq!(index.add(parent), child);
            parents.push(parent);
        }
        for node in 0..512 {
            let mut expected = std::collections::BTreeSet::new();
            let mut current = node;
            loop {
                expected.insert(current);
                if current == 0 {
                    break;
                }
                current = parents[current];
            }
            for ancestor in 0..512 {
                assert_eq!(
                    index.is_ancestor(ancestor, node),
                    expected.contains(&ancestor)
                );
            }
        }
    }

    #[test]
    fn source_ancestry_handles_deep_and_divergent_branches() {
        let mut ancestry = SourceAncestry::default();
        let first = ancestry.add(0);
        let mut leaf = first;
        for _ in 0..8192 {
            leaf = ancestry.add(leaf);
        }
        let sibling = ancestry.add(first);
        assert!(ancestry.is_ancestor(first, leaf));
        assert!(ancestry.is_ancestor(leaf, leaf));
        assert!(!ancestry.is_ancestor(sibling, leaf));
        assert!(!ancestry.is_ancestor(leaf, sibling));
        assert!(ancestry.nodes[leaf].1.len() <= 15);
    }

    #[test]
    fn import_rejects_compaction_boundary_on_another_branch() {
        assert!(import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"other"}}),
            json!({"type":"message","id":"b","parentId":"missing","message":{"role":"user","content":"orphan"}}),
            json!({"type":"compaction","id":"c","parentId":"b","firstKeptEntryId":"a","summary":"summary","tokensBefore":100}),
        ]).is_err());
    }

    #[test]
    fn import_rejects_compaction_referring_to_orphan_from_another_branch() {
        assert!(import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":"missing","message":{"role":"user","content":"orphan"}}),
            json!({"type":"message","id":"b","parentId":null,"message":{"role":"user","content":"other"}}),
            json!({"type":"compaction","id":"c","parentId":"b","firstKeptEntryId":"a","summary":"summary","tokensBefore":100}),
        ]).is_err());
    }

    #[test]
    fn import_rejects_forward_parent_reference() {
        assert!(import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"child","parentId":"later","message":{"role":"user","content":"child"}}),
            json!({"type":"message","id":"later","parentId":null,"message":{"role":"user","content":"parent"}}),
        ]).is_err());
    }

    #[test]
    fn incomplete_latest_assistant_is_unresolved_and_uses_initial_selection() {
        use clap::Parser;
        for message in [
            json!({"role":"assistant","provider":"anthropic","content":[]}),
            json!({"role":"assistant","model":"new","content":[]}),
        ] {
            let session = import_pi(vec![
                json!({"type":"session","version":3}),
                json!({"type":"model_change","id":"change","parentId":null,"provider":"anthropic","modelId":"old"}),
                json!({"type":"message","id":"assistant","parentId":"change","message":message}),
            ]).unwrap();
            assert_eq!(
                branch_selection(&session, |_, _| Some(false)),
                BranchSelection::Unresolved { thinking: None }
            );
            let mut args = super::super::Cli::parse_from(["pi"]);
            args.model = "initial".into();
            args.provider = super::super::Provider::Google;
            args.remember_initial_selection();
            args.model = "previous-session".into();
            args.provider = super::super::Provider::Ollama;
            args.restore_session_model(&session).unwrap();
            assert_eq!(args.provider.id(), "google");
            assert_eq!(args.model, "initial");
        }
    }

    #[test]
    fn native_selection_after_import_takes_precedence() {
        let mut session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","provider":"p","model":"m","content":[]}}),
        ]).unwrap();
        session
            .select_provider_model(Some("google"), "chosen", Some("low"))
            .unwrap();
        assert_eq!(
            branch_selection(&session, |_, _| None),
            BranchSelection::Native
        );
    }
    #[test]
    fn native_selection_after_unconverted_thinking_entry_is_not_imported() {
        let mut session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"thinking_level_change","id":"think","parentId":null,"thinkingLevel":"high"}),
        ]).unwrap();
        session
            .select_provider_model(Some("google"), "chosen", Some("low"))
            .unwrap();
        assert_eq!(
            branch_selection(&session, |_, _| None),
            BranchSelection::Native
        );
        let projected = super::super::rpc_entries::Entries::new(&session)
            .after(None)
            .unwrap();
        assert!(
            projected["entries"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["type"] == "model_change" && entry["modelId"] == "chosen")
        );
    }
    #[test]
    fn legacy_native_selection_after_unconverted_thinking_is_not_misclassified() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"thinking_level_change","id":"think","parentId":null,"thinkingLevel":"high"}),
        ]).unwrap();
        let mut records = session.records().to_vec();
        let version = records.last().unwrap().version;
        records.push(Record {
            id: records.len() as u64 + 1,
            version,
            parent: Some(session.head()),
            timestamp: None,
            run_id: None,
            event: Event::ModelSelected {
                model: "native".into(),
                provider: Some("google".into()),
                thinking: Some("high".into()),
            },
        });
        let legacy = Session::from_records(records).unwrap();
        assert_eq!(
            branch_selection(&legacy, |_, _| None),
            BranchSelection::Native
        );
    }
    #[test]
    fn assistant_model_restoration_does_not_duplicate_pi_projection() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"model_change","id":"change","parentId":null,"provider":"anthropic","modelId":"old"}),
            json!({"type":"message","id":"a","parentId":"change","message":{"role":"assistant","provider":"anthropic","model":"new","content":[{"type":"toolCall","id":"call","name":"read","arguments":{}}]}}),
            json!({"type":"message","id":"result","parentId":"a","message":{"role":"toolResult","toolCallId":"call","toolName":"read","content":[{"type":"text","text":"done"}],"isError":false}}),
        ]).unwrap();
        let projected = super::super::rpc_entries::Entries::new(&session)
            .after(None)
            .unwrap();
        let entries = projected["entries"].as_array().unwrap();
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry["message"]["role"] == "assistant")
                .count(),
            1
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry["message"]["role"] == "toolResult")
                .count(),
            1
        );
        let directory = tempfile::tempdir().unwrap();
        let shared = super::super::rpc_entries::Entries::new(&session)
            .write_share_branch(&session, directory.path())
            .unwrap();
        let path = directory.path().join("shared.jsonl");
        std::fs::write(&path, shared).unwrap();
        assert!(read(&path).is_ok());
    }

    #[test]
    fn reselecting_explicit_model_after_routed_response_survives_share() {
        for selected_thinking in ["low", "high"] {
            let mut session = import_pi(vec![
                json!({"type":"session","version":3}),
                json!({"type":"model_change","id":"change","parentId":null,"provider":"anthropic","modelId":"a"}),
                json!({"type":"thinking_level_change","id":"think","parentId":"change","thinkingLevel":"low"}),
                json!({"type":"message","id":"assistant","parentId":"think","message":{"role":"assistant","provider":"anthropic","model":"b","content":[]}}),
            ]).unwrap();
            assert_eq!(
                session.model_selection(),
                Some(("a".into(), Some("low".into())))
            );
            assert_eq!(session.model_provider(), Some("anthropic"));
            let before = session.records().len();
            session
                .select_provider_model(Some("anthropic"), "a", Some(selected_thinking))
                .unwrap();
            assert!(session.records().len() > before);
            assert_eq!(
                branch_selection(&session, |_, _| None),
                BranchSelection::Native
            );
            let directory = tempfile::tempdir().unwrap();
            let bytes = super::super::rpc_entries::Entries::new(&session)
                .write_share_branch(&session, directory.path())
                .unwrap();
            let entries: Vec<Value> = std::str::from_utf8(&bytes)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            let last_change = entries
                .iter()
                .rev()
                .find(|entry| entry["type"] == "model_change")
                .unwrap();
            assert_eq!(last_change["modelId"], "a");
            let path = directory.path().join("share.jsonl");
            std::fs::write(&path, bytes).unwrap();
            let reopened = read(&path).unwrap();
            assert_eq!(
                branch_selection(&reopened, |_, _| None),
                BranchSelection::Selected {
                    provider: "anthropic".into(),
                    model: "a".into(),
                    thinking: Some(selected_thinking.into())
                }
            );
        }
    }

    #[test]
    fn explicitly_selecting_physical_response_overrides_virtual_selection_in_share() {
        let mut session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"model_change","id":"change","parentId":null,"provider":"router","modelId":"virtual"}),
            json!({"type":"message","id":"assistant","parentId":"change","message":{"role":"assistant","provider":"anthropic","model":"physical","content":[]}}),
        ]).unwrap();
        assert!(
            matches!(branch_selection(&session, |_, _| Some(true)), BranchSelection::Selected { model, .. } if model == "virtual")
        );
        session
            .select_provider_model(Some("anthropic"), "physical", None)
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let bytes = super::super::rpc_entries::Entries::new(&session)
            .write_share_branch(&session, directory.path())
            .unwrap();
        let entries: Vec<Value> = std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            entries
                .iter()
                .rev()
                .find(|entry| entry["type"] == "model_change")
                .unwrap()["modelId"],
            "physical"
        );
        let path = directory.path().join("share.jsonl");
        std::fs::write(&path, bytes).unwrap();
        let reopened = read(&path).unwrap();
        assert!(
            matches!(branch_selection(&reopened, |_, _| Some(true)), BranchSelection::Selected { provider, model, .. } if provider == "anthropic" && model == "physical")
        );
    }

    #[test]
    fn restoration_distinguishes_unknown_and_authoritative_catalog_misses() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join(".pi")).unwrap();
        std::fs::write(
            directory.path().join(".pi/settings.json"),
            r#"{"defaultThinkingLevel":"low","modelThinkingLevels":{"ollama/known":"xhigh"}}"#,
        )
        .unwrap();
        let mut args = super::super::Cli::parse_from(["pi"]);
        args.settings = super::super::config::Settings::load(directory.path(), true).unwrap();
        args.model = "initial".into();
        args.remember_initial_selection();
        args.discovered_models
            .push(("ollama".into(), "known".into()));
        for (model, expected) in [
            ("not-in-offline-catalog", "not-in-offline-catalog"),
            ("known", "known"),
        ] {
            args.model = "previous-branch".into();
            args.thinking = Some("high".into());
            let session = import_pi(vec![
                json!({"type":"session","version":3}),
                json!({"type":"message","id":"assistant","parentId":null,"message":{"role":"assistant","provider":"ollama","model":model,"content":[]}}),
            ]).unwrap();
            args.restore_session_model(&session).unwrap();
            assert_eq!(args.model, expected);
            assert_eq!(args.thinking.as_deref(), Some("low"));
        }
        args.model_scope
            .push(super::super::model_selection::ScopedChoice {
                provider: super::super::Provider::Ollama,
                model: "known".into(),
                thinking: Some("high".into()),
            });
        let scoped = import_pi(vec![json!({"type":"session","version":3}), json!({"type":"message","id":"m","parentId":null,"message":{"role":"assistant","provider":"ollama","model":"known","content":[]}})]).unwrap();
        args.restore_session_model(&scoped).unwrap();
        assert_eq!(args.thinking.as_deref(), Some("low"));
        args.model_scope.clear();
        let mut native = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        native
            .select_provider_model(None, "saved-dynamic", Some("low"))
            .unwrap();
        native
            .append_user_with_images("existing".into(), Vec::new(), None)
            .unwrap();
        let native_path = directory.path().join("saved-native.jsonl");
        native.export_jsonl(&native_path).unwrap();
        let reopened = read(&native_path).unwrap();
        args.restore_session_model(&reopened).unwrap();
        assert_eq!(args.model, "saved-dynamic");
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"assistant","parentId":null,"message":{"role":"assistant","provider":"anthropic","model":"nonexistent-model-id","content":[]}}),
        ]).unwrap();
        args.model = "previous-branch".into();
        args.restore_session_model(&session).unwrap();
        assert_eq!(args.model, "initial");
        assert_eq!(args.thinking.as_deref(), Some("low"));
        args.complete_catalogs.push((
            "ollama".into(),
            args.endpoint_for(&super::super::Provider::Ollama)
                .to_owned(),
        ));
        let unknown_dynamic = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"assistant","parentId":null,"message":{"role":"assistant","provider":"ollama","model":"missing-from-complete-catalog","content":[]}}),
        ]).unwrap();
        args.restore_session_model(&unknown_dynamic).unwrap();
        assert_eq!(args.model, "initial");
    }

    #[test]
    fn automatic_recording_does_not_override_imported_virtual_selection() {
        use clap::Parser;
        let mut session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"model_change","id":"change","parentId":null,"provider":"router","modelId":"virtual"}),
            json!({"type":"message","id":"assistant","parentId":"change","message":{"role":"assistant","provider":"anthropic","model":"physical","content":[]}}),
        ]).unwrap();
        let mut args = super::super::Cli::parse_from(["pi"]);
        args.provider = super::super::Provider::Anthropic;
        args.model = "physical".into();
        let before = session.records().len();
        args.record_automatic_model_selection(&mut session).unwrap();
        assert!(
            session.records()[before..]
                .iter()
                .all(|record| !matches!(record.event, Event::ModelSelected { .. }))
        );
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("share.jsonl");
        let bytes = super::super::rpc_entries::Entries::new(&session)
            .write_share_branch(&session, directory.path())
            .unwrap();
        std::fs::write(&path, bytes).unwrap();
        let shared = read(&path).unwrap();
        assert!(
            matches!(branch_selection(&shared, |_, _| Some(true)), BranchSelection::Selected { model, .. } if model == "virtual")
        );
        args.record_model_selection(&mut session).unwrap();
        let bytes = super::super::rpc_entries::Entries::new(&session)
            .write_share_branch(&session, directory.path())
            .unwrap();
        std::fs::write(&path, bytes).unwrap();
        let shared = read(&path).unwrap();
        assert!(
            matches!(branch_selection(&shared, |_, _| Some(true)), BranchSelection::Selected { model, .. } if model == "physical")
        );
    }

    #[test]
    fn startup_thinking_override_does_not_select_physical_response_model() {
        use clap::Parser;
        let mut session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"model_change","id":"change","parentId":null,"provider":"router","modelId":"virtual"}),
            json!({"type":"message","id":"assistant","parentId":"change","message":{"role":"assistant","provider":"ollama","model":"physical","content":[]}}),
        ]).unwrap();
        let mut args = super::super::Cli::parse_from(["pi", "--thinking", "high"]);
        args.model = "physical".into();
        args.model_explicit = false;
        args.thinking_explicit = true;
        args.thinking = Some("high".into());
        args.record_automatic_model_selection(&mut session).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("share.jsonl");
        let bytes = super::super::rpc_entries::Entries::new(&session)
            .write_share_branch(&session, directory.path())
            .unwrap();
        std::fs::write(&path, bytes).unwrap();
        let shared = read(&path).unwrap();
        assert!(
            matches!(branch_selection(&shared, |_, _| Some(true)), BranchSelection::Selected { model, thinking: Some(level), .. } if model == "virtual" && level == "high")
        );
        args.model_explicit = true;
        args.record_automatic_model_selection(&mut session).unwrap();
        let bytes = super::super::rpc_entries::Entries::new(&session)
            .write_share_branch(&session, directory.path())
            .unwrap();
        std::fs::write(&path, bytes).unwrap();
        let shared = read(&path).unwrap();
        assert!(
            matches!(branch_selection(&shared, |_, _| Some(true)), BranchSelection::Selected { model, .. } if model == "physical")
        );
    }

    #[test]
    fn startup_thinking_establishes_initial_model_and_repairs_marker_only_history() {
        use clap::Parser;
        for legacy in [false, true] {
            let mut session = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
            if legacy {
                let mut records = session.records().to_vec();
                for event in [
                    Event::SourceEntry {
                        format: "pi-rs:thinking-selection".into(),
                        entry: Value::Null,
                    },
                    Event::ModelSelected {
                        provider: None,
                        model: "initial".into(),
                        thinking: Some("high".into()),
                    },
                ] {
                    let parent = records.last().unwrap().id;
                    let version = records.last().unwrap().version;
                    records.push(Record {
                        id: parent + 1,
                        parent: Some(parent),
                        version,
                        timestamp: None,
                        run_id: None,
                        event,
                    });
                }
                session = Session::from_records(records).unwrap();
            }
            let mut args = super::super::Cli::parse_from(["pi", "--thinking", "high"]);
            args.model = "initial".into();
            args.model_explicit = false;
            args.thinking_explicit = true;
            args.thinking = Some("high".into());
            args.record_automatic_model_selection(&mut session).unwrap();
            let projected = super::super::rpc_entries::Entries::new(&session)
                .after(None)
                .unwrap();
            let entries = projected["entries"].as_array().unwrap();
            assert!(
                entries
                    .iter()
                    .any(|entry| entry["type"] == "model_change" && entry["modelId"] == "initial")
            );
            assert!(
                entries
                    .iter()
                    .any(|entry| entry["type"] == "thinking_level_change"
                        && entry["thinkingLevel"] == "high")
            );
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("share.jsonl");
            let bytes = super::super::rpc_entries::Entries::new(&session)
                .write_share_branch(&session, directory.path())
                .unwrap();
            std::fs::write(&path, bytes).unwrap();
            let shared = read(&path).unwrap();
            assert!(
                matches!(branch_selection(&shared, |_, _| None), BranchSelection::Selected { model, thinking: Some(level), .. } if model == "initial" && level == "high")
            );
            session
                .select_provider_model(None, "initial", Some("high"))
                .unwrap();
            let projected = super::super::rpc_entries::Entries::new(&session)
                .after(None)
                .unwrap();
            assert!(
                projected["entries"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|entry| entry["type"] == "model_change" && entry["modelId"] == "initial")
            );
        }
    }

    #[test]
    fn incomplete_prior_model_change_does_not_hide_complete_response() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"model_change","id":"change","parentId":null,"modelId":"a"}),
            json!({"type":"message","id":"assistant","parentId":"change","message":{"role":"assistant","provider":"ollama","model":"b","content":[]}}),
        ]).unwrap();
        assert_eq!(
            branch_selection(&session, |_, _| None),
            BranchSelection::Selected {
                provider: "ollama".into(),
                model: "b".into(),
                thinking: None
            }
        );
    }

    #[test]
    fn unavailable_provider_and_failed_activation_restore_initial_and_branch_thinking() {
        use clap::Parser;
        for provider in ["unknown-provider", "google"] {
            let session = import_pi(vec![
                json!({"type":"session","version":3}),
                json!({"type":"thinking_level_change","id":"think","parentId":null,"thinkingLevel":"high"}),
                json!({"type":"message","id":"assistant","parentId":"think","message":{"role":"assistant","provider":provider,"model":"b","content":[]}}),
            ]).unwrap();
            let mut args = super::super::Cli::parse_from(["pi"]);
            args.model = "initial".into();
            args.thinking = Some("low".into());
            args.remember_initial_selection();
            args.model = "previous".into();
            args.initial_connection =
                Some((super::super::Provider::Google, "invalid endpoint".into()));
            args.endpoint_explicit = true;
            args.discovered_models.push(("google".into(), "b".into()));
            args.restore_session_model(&session).unwrap();
            assert_eq!(args.provider.id(), "ollama");
            assert_eq!(args.model, "initial");
            assert_eq!(args.thinking.as_deref(), Some("high"));
        }
    }
    #[test]
    fn imports_v1_compaction_index_and_preserves_original_entry() {
        let compaction = json!({"type":"compaction","firstKeptEntryIndex":2,"summary":"earlier"});
        let session = import_pi(vec![
            json!({"type":"session","version":1}),
            json!({"type":"message","message":{"role":"user","content":"discarded"}}),
            json!({"type":"message","message":{"role":"user","content":"retained"}}),
            compaction.clone(),
            json!({"type":"message","message":{"role":"user","content":"after"}}),
        ])
        .unwrap();
        let messages = session.current_messages();
        assert_eq!(
            messages,
            vec![
                Message::User {
                    content: "Prior conversation summary:\nearlier".into(),
                },
                Message::User {
                    content: "retained".into()
                },
                Message::User {
                    content: "after".into()
                },
            ]
        );
        assert!(session.records().iter().any(|record| matches!(
            &record.event, Event::SourceEntry { entry, .. } if entry == &compaction
        )));
        let fork = Session::from_records(session.fork_records(session.head()).unwrap()).unwrap();
        assert_eq!(fork.current_messages(), messages);
    }

    #[test]
    fn v1_compaction_at_current_or_future_index_keeps_no_earlier_messages() {
        for kept_index in [2, 3] {
            let compaction =
                json!({"type":"compaction","firstKeptEntryIndex":kept_index,"summary":"earlier"});
            let session = import_pi(vec![
                json!({"type":"session","version":1}),
                json!({"type":"message","message":{"role":"user","content":"discarded"}}),
                compaction.clone(),
                json!({"type":"message","message":{"role":"user","content":"after"}}),
            ])
            .unwrap();
            assert_eq!(
                session.current_messages(),
                vec![
                    Message::User {
                        content: "Prior conversation summary:\nearlier".into()
                    },
                    Message::User {
                        content: "after".into()
                    },
                ]
            );
            assert!(session.records().iter().any(|record| matches!(
                &record.event, Event::SourceEntry { entry, .. } if entry == &compaction
            )));
            let fork =
                Session::from_records(session.fork_records(session.head()).unwrap()).unwrap();
            assert_eq!(fork.current_messages(), session.current_messages());
        }
    }

    #[test]
    fn imported_session_uuid_survives_export_and_fork_gets_a_new_id() {
        let id = Uuid::new_v4();
        let session = import_pi(vec![
            json!({"type":"session","version":3,"id":id.to_string()}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"hello"}}),
        ]).unwrap();
        assert_eq!(session.session_id(), id);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("imported.jsonl");
        session.export_jsonl(&path).unwrap();
        assert_eq!(read(&path).unwrap().session_id(), id);
        let fork = Session::from_records(session.fork_records(session.head()).unwrap()).unwrap();
        assert_ne!(fork.session_id(), id);
    }

    #[test]
    fn imported_non_uuid_session_id_remains_supported() {
        let session =
            import_pi(vec![json!({"type":"session","version":3,"id":"custom-id"})]).unwrap();
        assert_ne!(session.session_id(), Uuid::nil());
        assert_eq!(session.external_session_id(), "custom-id");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("export.jsonl");
        session.export_jsonl(&path).unwrap();
        let reopened = read(&path).unwrap();
        assert_eq!(reopened.external_session_id(), "custom-id");
        assert_eq!(reopened.session_id(), session.session_id());
        let fork = Session::from_records(reopened.fork_records(reopened.head()).unwrap()).unwrap();
        assert_ne!(fork.session_id(), reopened.session_id());
        assert_eq!(fork.external_session_id(), fork.session_id().to_string());
        for valid in [
            "",
            "-invalid",
            "invalid.",
            "a/b",
            "a b",
            "a\r\nb",
            "é",
            "a",
            "test.id_1-2",
            "A0",
        ] {
            let imported =
                import_pi(vec![json!({"type":"session","version":3,"id":valid})]).unwrap();
            assert_eq!(imported.external_session_id(), valid);
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("export.jsonl");
            imported.export_jsonl(&path).unwrap();
            let reopened = read(&path).unwrap();
            assert_eq!(reopened.external_session_id(), valid);
            let fork =
                Session::from_records(reopened.fork_records(reopened.head()).unwrap()).unwrap();
            assert_eq!(fork.external_session_id(), fork.session_id().to_string());
        }
    }
    #[test]
    fn imported_tool_details_remain_in_history_outside_model_context() {
        let details = json!({"patch":"--- old\n+++ new", "firstChangedLine":1});
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"toolCall","id":"call","name":"edit","arguments":{}}]}}),
            json!({"type":"message","id":"t","parentId":"a","message":{"role":"toolResult","toolCallId":"call","content":[{"type":"text","text":"done"}],"details":details}}),
        ]).unwrap();
        assert!(session.records().iter().any(|record| matches!(&record.event,
            Event::ToolFinished { outcome: Outcome::SucceededWithDetails { details: value, .. }, .. } if value == &details)));
        assert!(
            session
                .current_messages()
                .iter()
                .any(|message| matches!(message, Message::Tool {content, ..} if content == "done"))
        );
    }

    #[test]
    fn imported_image_tool_details_survive_export_without_entering_model_context() {
        for is_error in [false, true] {
            let details = json!({"private_marker":"metadata", "nested":[1, 2]});
            let blocks = json!([{"type":"text","text":"result"},{"type":"image","data":"aW1hZ2U=","mimeType":"image/png"}]);
            let session = import_pi(vec![
                json!({"type":"session","version":3}),
                json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":[{"type":"toolCall","id":"call","name":"read","arguments":{}}]}}),
                json!({"type":"message","id":"t","parentId":"a","message":{"role":"toolResult","toolCallId":"call","content":blocks,"isError":is_error,"details":details}}),
            ]).unwrap();
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("export.jsonl");
            session.export_jsonl(&path).unwrap();
            let reopened = read(&path).unwrap();
            let outcome = reopened
                .records()
                .iter()
                .find_map(|record| match &record.event {
                    Event::ToolFinished { outcome, .. } => Some(outcome),
                    _ => None,
                })
                .unwrap();
            assert!(
                matches!(outcome, Outcome::StructuredWithDetails { data, details: stored, .. } if data["content"] == blocks && stored == &details)
            );
            assert_eq!(super::super::rpc_events::tool_failed(outcome), is_error);
            let rpc = super::super::rpc_events::tool_event_result_value(
                &outcome.tool_content(),
                Some(outcome),
                Some(outcome.is_structured()),
            );
            assert_eq!(rpc["details"], details);
            assert_eq!(rpc["content"], blocks);
            assert_eq!(rpc["content"][1]["data"], "aW1hZ2U=");
            assert!(reopened.current_messages().iter().any(|message| matches!(message,
                Message::Tool { content, .. } if !content.contains("private_marker") && content.contains("aW1hZ2U=")
            )));
        }
    }

    #[test]
    fn imported_activity_prefers_message_time_to_outer_entry_time() {
        let session = import_pi(vec![
            json!({"type":"session","version":3,"timestamp":"2000-01-01T00:00:00Z"}),
            json!({"type":"message","id":"u","parentId":null,"timestamp":"2030-01-01T00:00:00Z","message":{"role":"user","content":"question","timestamp":1_600_000_000_000_i64}}),
            json!({"type":"session_info","id":"n","parentId":"u","timestamp":"2040-01-01T00:00:00Z","name":"rename"}),
        ]).unwrap();
        assert_eq!(
            session.activity_time(),
            Some(std::time::UNIX_EPOCH + std::time::Duration::from_millis(1_600_000_000_000))
        );
    }

    #[test]
    fn imported_timestamps_and_label_changes_preserve_source_times() {
        let created = "2026-01-02T03:04:05.000Z";
        let labeled = "2026-01-02T03:06:07.123Z";
        let session = import_pi(vec![
            json!({"type":"session","version":3,"timestamp":created}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"question"}}),
            json!({"type":"label","id":"l","parentId":"u","targetId":"u","label":"bookmark","timestamp":labeled}),
        ]).unwrap();
        assert_eq!(session.records()[0].timestamp.as_deref(), Some(created));
        let user = session
            .records()
            .iter()
            .find(|record| {
                matches!(
                    record.event,
                    Event::User { .. } | Event::UserWithImages { .. }
                )
            })
            .unwrap();
        assert!(user.timestamp.is_none());
        let target = *session.labels().keys().next().unwrap();
        assert_eq!(
            session.label_timestamps().get(&target).map(String::as_str),
            Some(labeled)
        );
        let restored = Session::from_records(session.records().to_vec()).unwrap();
        assert_eq!(restored.label_timestamps(), session.label_timestamps());
    }

    #[test]
    fn imports_parent_session_without_adding_model_context() {
        let session = import_pi(vec![
            json!({"type":"session","version":3,"parentSession":"../parent.jsonl"}),
        ])
        .unwrap();
        assert_eq!(session.parent_session(), Some("../parent.jsonl"));
        assert!(session.current_messages().is_empty());
        let legacy = import_pi(vec![json!({"type":"session","version":1})]).unwrap();
        assert!(legacy.parent_session().is_none());
    }

    #[test]
    fn imports_compaction_system_prompt_and_recognizes_later_updates() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"question"}}),
            json!({"type":"compaction","id":"c","parentId":"u","firstKeptEntryId":"u","summary":"summary","usage":{"input":7},"systemMessage":{
                "role":"system","content":[{"type":"text","text":"saved"},{"type":"text","text":"instructions"}],"sections":{"rules":"original"},
                "toolsAdded":[{"name":"read","description":"Read","parameters":{"type":"object"}}]
            }}),
            json!({"type":"message","id":"s","parentId":"c","message":{"role":"system","content":"new"}}),
        ]).unwrap();
        let snapshot = session
            .records()
            .iter()
            .find_map(|record| match &record.event {
                Event::Compacted {
                    system_prompt: Some(snapshot),
                    ..
                } => Some(snapshot),
                _ => None,
            })
            .unwrap();
        assert_eq!(snapshot.content, "saved\ninstructions");
        assert_eq!(
            snapshot.sections,
            vec![("rules".into(), Some("original".into()))]
        );
        assert_eq!(snapshot.tools[0].name, "read");
        assert!(
            super::super::sessions::html(&session).contains("System Prompt Update: instructions")
        );
        let expected = snapshot.clone();
        let mut legacy = session.records().to_vec();
        let mut index = None;
        for (position, record) in legacy.iter_mut().enumerate() {
            if let Event::Compacted { system_prompt, .. } = &mut record.event {
                *system_prompt = None;
                index = Some(position);
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let journal = directory.path().join("legacy.jsonl");
        let raw = legacy
            .iter()
            .map(|record| serde_json::to_string(record).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        std::fs::write(&journal, &raw).unwrap();
        let reopened = Session::open(&journal).unwrap();
        let Event::Compacted { system_prompt, .. } = &reopened.records()[index.unwrap()].event
        else {
            panic!("compaction should be retained");
        };
        assert_eq!(system_prompt.as_deref(), Some(expected.as_ref()));
        assert_eq!(std::fs::read_to_string(&journal).unwrap(), raw);
    }

    #[test]
    fn imports_ordered_system_sections_and_tool_changes() {
        let entry: Value = serde_json::from_str(r#"{"type":"message","id":"system","parentId":null,"message":{"role":"system","content":[{"type":"text","text":"base"}],"sections":{"z":"first","a":"second","removed":null},"toolsAdded":[{"name":"read","description":"Read","parameters":{"type":"object"}}],"toolsRemoved":[{"name":"old"}]}}"#).unwrap();
        let session = import_pi(vec![json!({"type":"session","version":3}), entry]).unwrap();
        let checkpoint = session
            .records()
            .iter()
            .find_map(|record| match &record.event {
                Event::SystemCheckpoint {
                    content,
                    sections,
                    tools_added,
                    tools_removed,
                } => Some((content, sections, tools_added, tools_removed)),
                _ => None,
            })
            .unwrap();
        assert_eq!(checkpoint.0, "base");
        assert_eq!(
            checkpoint.1,
            &vec![
                ("z".into(), Some("first".into())),
                ("a".into(), Some("second".into())),
                ("removed".into(), None)
            ]
        );
        assert_eq!(checkpoint.2[0].name, "read");
        assert_eq!(checkpoint.3, &["old"]);
    }

    #[test]
    fn imported_costs_survive_reopening_and_unknown_costs_are_not_zeroed() {
        let mut entries = vec![
            json!({"type":"session","version":3,"cwd":"/tmp/import"}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"question"}}),
            json!({"type":"message","id":"a","parentId":"u","message":{"role":"assistant","content":"first","usage":{"input":1,"output":1,"cost":{"total":0.25}}}}),
            json!({"type":"message","id":"b","parentId":"u","message":{"role":"assistant","content":"branch","usage":{"input":1,"output":1,"cost":{"total":0.5}}}}),
            json!({"type":"usage","id":"usage","parentId":"b","usage":{"input":1,"output":1,"cost":{"total":0.125}}}),
            json!({"type":"compaction","id":"c","parentId":"usage","firstKeptEntryId":"u","summary":"summary","usage":{"input":1,"output":1,"cost":{"total":0.0625}}}),
        ];
        let session = import_pi(entries.clone()).unwrap();
        assert_eq!(session.statistics().cost, Some(0.9375));
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("costs.jsonl");
        session.export_jsonl(&file).unwrap();
        assert_eq!(Session::open(file).unwrap().statistics().cost, Some(0.9375));
        entries[3]["message"]["usage"]["cost"] = Value::Null;
        assert_eq!(import_pi(entries.clone()).unwrap().statistics().cost, None);
        entries[3]["message"]["usage"]["cost"] = json!({"total":-1});
        assert_eq!(import_pi(entries.clone()).unwrap().statistics().cost, None);
        entries[3]["message"]
            .as_object_mut()
            .unwrap()
            .remove("usage");
        assert_eq!(import_pi(entries).unwrap().statistics().cost, None);
    }

    #[test]
    fn statistics_include_all_branches_and_imported_cached_and_summary_usage() {
        let entries = vec![
            json!({"type":"session","version":3,"cwd":"/tmp/import"}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"question"}}),
            json!({"type":"message","id":"a","parentId":"u","message":{"role":"assistant","content":[{"type":"toolCall","id":"call","name":"read","arguments":{"path":"x"}}],"usage":{"input":10,"output":4,"cacheRead":3,"cacheWrite":2}}}),
            json!({"type":"message","id":"t","parentId":"a","message":{"role":"toolResult","toolCallId":"call","content":"result","usage":{"input":2,"output":1,"cacheRead":1}}}),
            json!({"type":"usage","id":"usage","parentId":"t","usage":{"input":5,"output":1}}),
            json!({"type":"compaction","id":"c","parentId":"usage","firstKeptEntryId":"u","summary":"summary","usage":{"input":7,"output":2}}),
            json!({"type":"message","id":"branch","parentId":"u","message":{"role":"user","content":"other branch"}}),
        ];
        let session = import_pi(entries).unwrap();
        let stats = serde_json::to_value(session.statistics()).unwrap();
        assert_eq!(stats["userMessages"], 2);
        assert_eq!(stats["assistantMessages"], 1);
        assert_eq!(stats["toolCalls"], 1);
        assert_eq!(stats["toolResults"], 1);
        assert_eq!(stats["totalMessages"], 4);
        assert_eq!(
            stats["tokens"],
            json!({"input":24,"output":8,"cacheRead":4,"cacheWrite":2,"total":38})
        );
    }

    #[test]
    fn imports_branches_and_settled_tools_without_replaying_effects() {
        let entries = vec![
            json!({"type":"session","version":3,"cwd":"/tmp/import"}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"question"}}),
            json!({"type":"message","id":"a","parentId":"u","message":{"role":"assistant","content":[{"type":"thinking","thinking":"reason"},{"type":"toolCall","id":"call","name":"write","arguments":{"path":"x"}}],"model":"fixture","provider":"local","usage":{"input":4,"output":2}}}),
            json!({"type":"message","id":"t","parentId":"a","message":{"role":"toolResult","toolCallId":"call","content":[{"type":"text","text":"saved"}]}}),
            json!({"type":"message","id":"answer","parentId":"t","message":{"role":"assistant","content":[{"type":"text","text":"done"}]}}),
            json!({"type":"message","id":"branch","parentId":"u","message":{"role":"user","content":"another branch"}}),
        ];
        let session = import_pi(entries).unwrap();
        assert!(session.operations().is_empty());
        let messages = session.conversation_at(session.head()).unwrap();
        assert_eq!(
            messages,
            vec![
                Message::User {
                    content: "question".into()
                },
                Message::User {
                    content: "another branch".into()
                }
            ]
        );
        let completed = session.records().iter().find(|record| matches!(&record.event, Event::Assistant { content, .. } if content == "done")).unwrap();
        let messages = session.conversation_at(completed.id).unwrap();
        assert!(
            messages.iter().any(
                |message| matches!(message, Message::Tool { content, .. } if content == "saved")
            )
        );
        assert_eq!(
            session
                .records()
                .iter()
                .filter(|record| matches!(record.event, Event::SourceEntry { .. }))
                .count(),
            6
        );
    }
    #[test]
    fn imports_failed_assistant_with_incomplete_tool_call_without_replaying_it() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"question"}}),
            json!({"type":"message","id":"a","parentId":"u","message":{"role":"assistant","content":[{"type":"text","text":"partial"},{"type":"toolCall"}],"stopReason":"error","errorMessage":"connection lost"}}),
        ]).unwrap();
        assert!(session.operations().is_empty());
        assert!(session.records().iter().any(|record| matches!(
            record.event,
            Event::ModelFinished {
                failure: Some(pi_rs::ModelFailure::Failed),
                ..
            }
        )));
        let (messages, _) = super::super::rpc_messages::snapshot(&session);
        assert_eq!(messages.last().unwrap()["errorMessage"], "connection lost");
        assert_eq!(messages.last().unwrap()["stopReason"], "error");
    }

    #[test]
    fn rejects_unsupported_content() {
        let header = json!({"type":"session","version":3});
        assert!(import_pi(vec![header, json!({"type":"message","id":"x","parentId":null,"message":{"role":"user","content":[{"type":"image","data":"abc"}]}})]).is_err());
    }
    #[test]
    fn imported_images_and_compaction_keep_the_retained_turn() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"old","parentId":null,"message":{"role":"user","content":"old"}}),
            json!({"type":"message","id":"image","parentId":"old","message":{"role":"user","content":[{"type":"text","text":"look"},{"type":"image","data":"aW1hZ2U=","mimeType":"image/png"}]}}),
            json!({"type":"compaction","id":"compact","parentId":"image","summary":"previous","firstKeptEntryId":"image"}),
        ]).unwrap();
        let messages = session.conversation_at(session.head()).unwrap();
        assert_eq!(messages.len(), 2);
        assert!(
            matches!(&messages[1], Message::UserWithImages { content, images } if content == "look" && images.len() == 1)
        );
    }
    #[test]
    fn imports_compaction_summary_message_without_compacting_the_branch() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"question"}}),
            json!({"type":"message","id":"s","parentId":"u","message":{"role":"compactionSummary","summary":"earlier facts","tokensBefore":123,"timestamp":456}}),
        ]).unwrap();
        let messages = session.conversation_at(session.head()).unwrap();
        assert_eq!(messages.len(), 2);
        assert!(
            matches!(&messages[1], Message::User { content } if content == "The conversation history before this point was compacted into the following summary:\n\n<summary>\nearlier facts\n</summary>")
        );
        let (visible, _) = super::super::rpc_messages::snapshot(&session);
        assert_eq!(visible.last().unwrap()["role"], "compactionSummary");
        assert_eq!(visible.last().unwrap()["tokensBefore"], 123);
        let entries = super::super::rpc_entries::Entries::new(&session)
            .after(None)
            .unwrap();
        assert_eq!(entries["entries"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn imports_branch_summary_message_with_reference_context() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"root","parentId":null,"message":{"role":"user","content":"question"}}),
            json!({"type":"message","id":"branch","parentId":"root","message":{"role":"user","content":"old branch"}}),
            json!({"type":"message","id":"summary","parentId":"root","message":{"role":"branchSummary","summary":"branch facts","fromId":"branch","timestamp":123}}),
        ]).unwrap();
        let messages = session.conversation_at(session.head()).unwrap();
        assert!(
            matches!(&messages[1], Message::User { content } if content == "The following is a summary of a branch that this conversation came back from:\n\n<summary>\nbranch facts</summary>")
        );
        let (visible, _) = super::super::rpc_messages::snapshot(&session);
        assert_eq!(visible.last().unwrap()["role"], "branchSummary");
        assert_eq!(visible.last().unwrap()["summary"], "branch facts");
        let entries = super::super::rpc_entries::Entries::new(&session)
            .after(None)
            .unwrap();
        assert_eq!(entries["entries"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn imports_legacy_custom_messages_without_losing_visibility_or_metadata() {
        let session = import_pi(vec![
            json!({"type":"session","version":2}),
            json!({"type":"message","id":"a","parentId":null,"message":{"role":"custom","customType":"private","content":"hidden context","display":false,"details":{"source":"extension"}}}),
            json!({"type":"message","id":"b","parentId":"a","message":{"role":"hookMessage","content":"old hook"}}),
        ]).unwrap();
        let custom: Vec<_> = session
            .records()
            .iter()
            .filter_map(|record| match &record.event {
                Event::CustomMessage { message }
                | Event::CustomMessageDelivered { message, .. } => Some(message),
                _ => None,
            })
            .collect();
        assert_eq!(custom.len(), 2);
        assert!(!custom[0].display);
        assert_eq!(custom[0].custom_type, "private");
        assert_eq!(custom[0].details, Some(json!({"source":"extension"})));
        assert!(!custom[1].display);
        assert_eq!(session.conversation_at(session.head()).unwrap().len(), 2);
        let entries = super::super::rpc_entries::Entries::new(&session)
            .after(None)
            .unwrap();
        assert_eq!(entries["entries"].as_array().unwrap().len(), 2);
        let (messages, _) = super::super::rpc_messages::snapshot(&session);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["customType"], "private");
        assert_eq!(messages[0]["display"], false);
        assert_eq!(messages[1]["role"], "hookMessage");
    }

    #[test]
    fn sparse_custom_messages_project_empty_and_survive_round_trip() {
        for explicit_null in [false, true] {
            let mut custom = json!({"type":"custom_message","id":"custom","parentId":null,"customType":"hint","display":true,"details":{"source":"test"}});
            if explicit_null {
                custom["content"] = Value::Null;
            }
            let session =
                import_pi(vec![json!({"type":"session","version":3}), custom.clone()]).unwrap();
            let record = session.records().iter().find(|record| matches!(&record.event,
                Event::CustomMessage { message } if message.custom_type == "hint"
                    && message.display && message.details == Some(json!({"source":"test"}))
                    && matches!(&message.content, pi_rs::ContextContent::Blocks(blocks) if blocks.is_empty())
            )).expect("empty custom message");
            assert!(
                matches!(session.conversation_at(record.id).unwrap().as_slice(), [Message::User { content }] if content.is_empty())
            );
            assert!(session.records().iter().any(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry == &custom)));
            let (messages, _) = super::super::rpc_messages::snapshot(&session);
            assert_eq!(messages[0]["content"], json!([]));
            assert_eq!(messages[0]["details"], json!({"source":"test"}));
            let entries = super::super::rpc_entries::Entries::new(&session)
                .after(None)
                .unwrap();
            let raw = &entries["entries"][0];
            assert_eq!(raw.get("content"), custom.get("content"));
            assert_eq!(raw["details"], custom["details"]);
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("native.jsonl");
            session.export_jsonl(&path).unwrap();
            let reopened = read(&path).unwrap();
            let (messages, _) = super::super::rpc_messages::snapshot(&reopened);
            assert_eq!(messages[0]["content"], json!([]));
            assert!(reopened.records().iter().any(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry == &custom)));
        }
        for invalid in [json!(false), json!(42), json!({})] {
            let entry = json!({"type":"custom_message","id":"bad","parentId":null,"customType":"hint","display":true,"content":invalid});
            assert!(import_pi(vec![json!({"type":"session","version":3}), entry]).is_err());
        }
    }

    #[test]
    fn sparse_custom_message_edits_remain_branch_local() {
        let mut session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"custom_message","id":"custom","parentId":null,"customType":"hint","display":false}),
            json!({"type":"context_edit","id":"edit","parentId":"custom","targetId":"custom","replacement":{"content":"edited"}}),
            json!({"type":"message","id":"left","parentId":"edit","message":{"role":"user","content":"left"}}),
            json!({"type":"message","id":"right","parentId":"custom","message":{"role":"user","content":"right"}}),
        ]).unwrap();
        let left = session
            .records()
            .iter()
            .find(
                |record| matches!(&record.event, Event::User { content, .. } if content == "left"),
            )
            .unwrap()
            .id;
        assert!(
            matches!(&session.conversation_at(left).unwrap()[0], Message::User { content } if content == "edited")
        );
        assert!(
            matches!(&session.conversation_at(session.head()).unwrap()[0], Message::User { content } if content.is_empty())
        );
        let (messages, _) = super::super::rpc_messages::snapshot(&session);
        assert_eq!(messages[0]["content"], json!([]));
        session.checkout(left).unwrap();
        let (messages, _) = super::super::rpc_messages::snapshot(&session);
        assert_eq!(messages[0]["content"], json!("edited"));
    }

    #[test]
    fn imports_custom_entries_and_messages_with_metadata_and_context_edits() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"custom","id":"entry","parentId":null,"customType":"bookmark","data":null}),
            json!({"type":"custom_message","id":"message","parentId":"entry","customType":"hint","display":false,"details":null,"content":[{"type":"text","text":"before"},{"type":"image","data":"aW1hZ2U=","mimeType":"image/png"},{"type":"text","text":"after"}]}),
            json!({"type":"context_edit","id":"edit","parentId":"message","targetId":"message","replacement":{"content":"replacement"}}),
        ]).unwrap();
        assert!(session.records().iter().any(|record| matches!(
            &record.event, Event::CustomEntry { entry }
            if entry.custom_type == "bookmark" && entry.data == Some(Value::Null)
        )));
        assert!(session.records().iter().any(|record| matches!(
            &record.event, Event::CustomMessage { message }
            if message.custom_type == "hint" && !message.display && message.details == Some(Value::Null)
                && matches!(&message.content, pi_rs::ContextContent::Blocks(blocks) if blocks.len() == 3)
        )));
        assert!(
            matches!(session.conversation_at(session.head()).unwrap().as_slice(),
            [Message::User { content }] if content == "replacement")
        );
        let entries = super::super::rpc_entries::Entries::new(&session)
            .after(None)
            .unwrap();
        let entries = entries["entries"].as_array().unwrap();
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry["type"] == "custom")
                .count(),
            1
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry["type"] == "custom_message")
                .count(),
            1
        );
    }

    #[test]
    fn codemode_store_import_follows_the_selected_branch() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"custom","id":"base","parentId":null,"customType":"codemode-store","data":{"set":{"value":1},"delete":[]}}),
            json!({"type":"custom","id":"left","parentId":"base","customType":"codemode-store","data":{"set":{"value":2},"delete":[]}}),
            json!({"type":"custom","id":"right","parentId":"base","customType":"codemode-store","data":{"set":{"other":3},"delete":[]}}),
        ]).unwrap();
        assert_eq!(
            session.extension_states()["codemode"].data,
            json!({"value":1,"other":3})
        );
        let entries = super::super::rpc_entries::Entries::new(&session)
            .after(None)
            .unwrap();
        let entries = entries["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 3);
        assert!(
            entries
                .iter()
                .all(|entry| entry["customType"] == "codemode-store")
        );
    }
    #[test]
    fn imported_context_edits_are_branch_relative_and_preserve_original_entries() {
        let session = import_pi(vec![
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"user","parentId":null,"message":{"role":"user","content":"original"}}),
            json!({"type":"context_edit","id":"edit","parentId":"user","targetId":"user","replacement":{"content":"edited"}}),
            json!({"type":"message","id":"left","parentId":"edit","message":{"role":"user","content":"left"}}),
            json!({"type":"message","id":"right","parentId":"user","message":{"role":"user","content":"right"}}),
        ]).unwrap();
        let left = session
            .records()
            .iter()
            .find(
                |record| matches!(&record.event, Event::User { content, .. } if content == "left"),
            )
            .unwrap()
            .id;
        assert!(
            matches!(&session.conversation_at(left).unwrap()[0], Message::User { content } if content == "edited")
        );
        assert!(
            matches!(&session.conversation_at(session.head()).unwrap()[0], Message::User { content } if content == "original")
        );
    }
    #[test]
    fn native_import_never_repairs_or_mutates_the_source() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("broken.jsonl");
        let content = "{\"truncated\":";
        std::fs::write(&path, content).unwrap();
        assert!(read(&path).is_err());
        assert_eq!(std::fs::read_to_string(path).unwrap(), content);
    }
}
