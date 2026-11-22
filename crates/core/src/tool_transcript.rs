//! Replay model-facing tool loadouts without changing the public message shape.
use crate::{Event, Record, ToolDeclaration, ToolSpec, ToolTranscriptChange};
use std::collections::{BTreeMap, BTreeSet};

const LOADOUT: &str = "pi-rs:tool-loadout-v1";

pub(crate) fn changed_loadout(branch: &[&Record], active: &[ToolDeclaration]) -> Option<Event> {
    let prior = branch
        .iter()
        .rev()
        .take_while(|record| {
            !matches!(
                record.event,
                Event::Compacted {
                    system_prompt: Some(_),
                    ..
                }
            )
        })
        .find_map(|record| {
            if let Event::SourceEntry { format, entry } = &record.event
                && format == LOADOUT
            {
                return serde_json::from_value::<Vec<ToolDeclaration>>(entry.clone()).ok();
            }
            None
        });
    (prior.as_deref() != Some(active)).then(|| Event::SourceEntry {
        format: LOADOUT.into(),
        entry: serde_json::json!(active),
    })
}

/// Reconstruct the model-facing loadout after each recorded change. Numbers in
/// `message_sources` correspond to a retained request's conversation items,
/// excluding the leading system message and optional compaction summary.
pub(crate) fn replay(
    branch: &[&Record],
    message_sources: &[u64],
    first_conversation_index: usize,
    registered: &[ToolSpec],
    active: &[ToolDeclaration],
) -> Vec<ToolTranscriptChange> {
    let retained_sources: BTreeSet<_> = message_sources.iter().copied().collect();
    // Allow an imported declaration to explain a retained call even when the
    // current registry no longer has that tool. Execution uses the live loadout.
    let mut permitted: BTreeSet<&str> = registered.iter().map(|tool| tool.name.as_str()).collect();
    for record in branch {
        if let Event::Assistant { invocations, .. } = &record.event
            && retained_sources.contains(&record.id)
        {
            permitted.extend(
                invocations
                    .iter()
                    .map(|invocation| invocation.call.name.as_str()),
            );
        }
    }
    let mut loadout = Vec::<ToolDeclaration>::new();
    let mut snapshots = BTreeMap::new();
    let mut history = branch;
    let snapshot_index = branch.iter().rposition(|record| {
        matches!(
            record.event,
            Event::Compacted {
                system_prompt: Some(_),
                ..
            }
        )
    });
    if let Some(index) = snapshot_index {
        let compacted = branch[index];
        let Event::Compacted {
            system_prompt: Some(snapshot),
            ..
        } = &compacted.event
        else {
            unreachable!("selected compaction contains its system prompt")
        };
        let before = &branch[..index];
        let parent = |id| before.iter().find(|record| record.id == id).copied();
        let source = compacted.parent.and_then(parent).and_then(|record| {
            if matches!(record.event, Event::ModelFinished { .. }) {
                record.parent.and_then(parent)
            } else {
                Some(record)
            }
        });
        let original = source.and_then(|source| {
            let Event::SourceEntry { format, entry } = &source.event else {
                return None;
            };
            (format == "pi"
                && entry["type"] == "compaction"
                && source.timestamp == compacted.timestamp)
                .then_some(entry)
        });
        let declarations = imported_declarations(original, "/systemMessage/toolsAdded");
        loadout = snapshot
            .tools
            .iter()
            .filter(|tool| permitted.contains(tool.name.as_str()))
            .map(|tool| imported_declaration(tool, &declarations))
            .collect();
        // Pi uses the last compaction's snapshot as the leading declaration,
        // including for messages retained from before the compaction record.
        snapshots.insert(0, loadout.clone());
        history = &branch[index + 1..];
    }

    let mut pi_source = None;
    for record in history {
        let first_native_loadout = snapshots.is_empty()
            && matches!(&record.event, Event::SourceEntry { format, .. } if format == LOADOUT);
        match &record.event {
            Event::SourceEntry { format, entry } if format == "pi" => {
                pi_source = Some((record.id, entry));
                continue;
            }
            Event::SystemCheckpoint {
                tools_added,
                tools_removed,
                ..
            } if !tools_added.is_empty() || !tools_removed.is_empty() => {
                loadout.retain(|tool| !tools_removed.contains(&tool.spec.name));
                // Imported checkpoints retain plain ToolSpecs in the prepared
                // journal. Read optional constraints from their paired Pi entry.
                let source = pi_source
                    .filter(|(id, _)| Some(*id) == record.parent)
                    .map(|(_, source)| source);
                let original = imported_declarations(source, "/message/toolsAdded");
                for tool in tools_added
                    .iter()
                    .filter(|tool| permitted.contains(tool.name.as_str()))
                {
                    let declaration = imported_declaration(tool, &original);
                    if let Some(existing) = loadout
                        .iter_mut()
                        .find(|existing| existing.spec.name == tool.name)
                    {
                        *existing = declaration;
                    } else {
                        loadout.push(declaration);
                    }
                }
            }
            Event::SourceEntry { format, entry } if format == LOADOUT => {
                let Ok(tools) = serde_json::from_value::<Vec<ToolDeclaration>>(entry.clone())
                else {
                    continue;
                };
                loadout = tools;
            }
            _ => continue,
        }
        let index = if first_native_loadout {
            0
        } else {
            message_sources
                .iter()
                .rposition(|source| *source <= record.id)
                .map_or_else(
                    || snapshot_index.map_or(0, |_| first_conversation_index),
                    |index| first_conversation_index + index + 1,
                )
        };
        // Changes with no model-visible item between them share one boundary.
        snapshots.insert(index, loadout.clone());
    }
    let final_index = if snapshots.is_empty() {
        0
    } else {
        first_conversation_index + message_sources.len()
    };
    if loadout != active || snapshots.is_empty() {
        snapshots.insert(final_index, active.to_vec());
    }

    let mut previous = Vec::<ToolDeclaration>::new();
    snapshots
        .into_iter()
        .filter_map(|(message_index, next)| {
            let names: BTreeSet<_> = next.iter().map(|tool| tool.spec.name.as_str()).collect();
            let removed: Vec<_> = previous
                .iter()
                .filter(|tool| !names.contains(tool.spec.name.as_str()))
                .map(|tool| tool.spec.name.clone())
                .collect();
            let by_name: BTreeMap<_, _> = previous
                .iter()
                .map(|tool| (tool.spec.name.as_str(), tool))
                .collect();
            let added: Vec<_> = next
                .iter()
                .filter(|tool| by_name.get(tool.spec.name.as_str()).copied() != Some(*tool))
                .cloned()
                .collect();
            previous = next;
            (!added.is_empty() || !removed.is_empty()).then_some(ToolTranscriptChange {
                message_index,
                added,
                removed,
            })
        })
        .collect()
}

fn imported_declarations(source: Option<&serde_json::Value>, path: &str) -> Vec<ToolDeclaration> {
    source
        .and_then(|entry| entry.pointer(path)?.as_array())
        .into_iter()
        .flatten()
        .filter_map(|tool| serde_json::from_value(tool.clone()).ok())
        .collect()
}

fn imported_declaration(tool: &ToolSpec, declarations: &[ToolDeclaration]) -> ToolDeclaration {
    declarations
        .iter()
        .find(|declaration| declaration.spec == *tool)
        .cloned()
        .unwrap_or_else(|| ToolDeclaration {
            spec: tool.clone(),
            constrained_sampling: None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryStore, Session};

    fn tool(name: &str, description: &str) -> ToolDeclaration {
        ToolDeclaration {
            spec: ToolSpec {
                name: name.into(),
                description: description.into(),
                parameters: serde_json::json!({"type":"object"}),
            },
            constrained_sampling: None,
        }
    }

    #[test]
    fn initial_native_catalog_stays_at_the_head_after_the_first_request() {
        let read = tool("read", "read a file");
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let user = session
            .append(Event::User {
                content: "first".into(),
                input_id: None,
            })
            .unwrap()
            .id;
        let active = [read];
        let registered = [active[0].spec.clone()];
        let before = replay(&session.branch(), &[user], 1, &registered, &active);
        let persisted = changed_loadout(&session.branch(), &active).unwrap();
        session.append(persisted).unwrap();
        let after = replay(&session.branch(), &[user], 1, &registered, &active);
        assert_eq!(after, before);
        assert_eq!(after[0].message_index, 0);
    }

    #[test]
    fn imported_tool_constraint_comes_from_the_paired_source_entry() {
        let mut declaration = tool("read", "read a file");
        declaration.constrained_sampling = Some(crate::ToolConstraint::JsonSchema {
            strict: crate::StrictToolPolicy::Prefer,
        });
        let entry = serde_json::json!({
            "type":"message", "message":{"role":"system","toolsAdded":[declaration.clone()]}
        });
        let mut events = crate::pi_import::convert(
            &entry,
            &mut crate::pi_import::Branch::default(),
            &BTreeMap::new(),
        )
        .unwrap();
        let source = Record {
            timestamp: None,
            run_id: None,
            version: 1,
            id: 1,
            parent: None,
            event: Event::SourceEntry {
                format: "pi".into(),
                entry,
            },
        };
        let checkpoint = Record {
            id: 2,
            parent: Some(1),
            event: events.remove(0),
            ..source.clone()
        };
        let changes = replay(
            &[&source, &checkpoint],
            &[],
            1,
            std::slice::from_ref(&declaration.spec),
            std::slice::from_ref(&declaration),
        );
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].added, [declaration]);
    }

    #[test]
    fn imported_compaction_tools_replace_the_head_before_retained_messages() {
        let old = tool("read", "before compaction");
        let mut replacement = tool("read", "after compaction");
        replacement.constrained_sampling = Some(crate::ToolConstraint::JsonSchema {
            strict: crate::StrictToolPolicy::Require,
        });
        let entry = serde_json::json!({
            "type":"compaction", "id":"compact", "firstKeptEntryId":"compact", "summary":"prior",
            "systemMessage":{"content":"snapshot","toolsAdded":[replacement.clone()]}
        });
        let mut events = crate::pi_import::convert(
            &entry,
            &mut crate::pi_import::Branch::default(),
            &BTreeMap::new(),
        )
        .unwrap();
        let old_loadout = Record {
            timestamp: None,
            run_id: None,
            version: 1,
            id: 1,
            parent: None,
            event: Event::SourceEntry {
                format: LOADOUT.into(),
                entry: serde_json::json!([old]),
            },
        };
        let retained = Record {
            id: 2,
            parent: Some(1),
            event: Event::User {
                content: "retained".into(),
                input_id: None,
            },
            ..old_loadout.clone()
        };
        let source = Record {
            id: 3,
            parent: Some(2),
            event: Event::SourceEntry {
                format: "pi".into(),
                entry,
            },
            ..old_loadout.clone()
        };
        let compacted = Record {
            id: 4,
            parent: Some(3),
            event: events.remove(0),
            ..old_loadout.clone()
        };
        let branch = [&old_loadout, &retained, &source, &compacted];
        let active = std::slice::from_ref(&replacement);
        let replayed = replay(
            &branch,
            &[retained.id],
            2,
            std::slice::from_ref(&replacement.spec),
            active,
        );
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].message_index, 0);
        assert_eq!(replayed[0].added, active);
        assert!(replayed[0].removed.is_empty());
        assert!(changed_loadout(&branch, active).is_some());

        let later = tool("write", "after the compaction summary");
        let update = Record {
            id: 5,
            parent: Some(4),
            event: Event::SourceEntry {
                format: LOADOUT.into(),
                entry: serde_json::json!([later.clone()]),
            },
            ..old_loadout.clone()
        };
        let replayed = replay(
            &[&old_loadout, &retained, &source, &compacted, &update],
            &[],
            2,
            &[replacement.spec.clone(), later.spec.clone()],
            std::slice::from_ref(&later),
        );
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].message_index, 0);
        assert_eq!(replayed[0].added, active);
        assert_eq!(replayed[1].message_index, 2);
        assert_eq!(replayed[1].added, [later]);
    }

    #[test]
    fn restored_loadouts_keep_constraints_without_requiring_them_in_legacy_entries() {
        let plain = tool("read", "read a file");
        let mut strict = plain.clone();
        strict.constrained_sampling = Some(crate::ToolConstraint::JsonSchema {
            strict: crate::StrictToolPolicy::Require,
        });
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append(Event::SourceEntry {
                format: LOADOUT.into(),
                entry: serde_json::json!([plain.spec.clone()]),
            })
            .unwrap();
        let user = session
            .append(Event::User {
                content: "first".into(),
                input_id: None,
            })
            .unwrap()
            .id;
        session
            .append(changed_loadout(&session.branch(), std::slice::from_ref(&strict)).unwrap())
            .unwrap();
        let active = [strict];
        let changes = replay(
            &session.branch(),
            &[user],
            1,
            std::slice::from_ref(&plain.spec),
            &active,
        );
        assert_eq!(changes[0].added, [plain]);
        assert_eq!(changes[1].message_index, 2);
        assert_eq!(changes[1].added, active);
        assert!(changed_loadout(&session.branch(), &active).is_none());
    }

    #[test]
    fn native_history_keeps_tools_removed_from_the_current_registry() {
        let old = tool("old", "original extension tool");
        let active = tool("read", "current tool");
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let loadout = changed_loadout(&session.branch(), std::slice::from_ref(&old)).unwrap();
        session.append(loadout).unwrap();
        let user = session
            .append(Event::User {
                content: "first".into(),
                input_id: None,
            })
            .unwrap()
            .id;
        let assistant = session
            .append(Event::Assistant {
                content: "done".into(),
                invocations: Vec::new(),
                usage: None,
                request_id: None,
            })
            .unwrap()
            .id;
        let active = [active];
        let registered = [active[0].spec.clone()];
        let changes = replay(
            &session.branch(),
            &[user, assistant],
            1,
            &registered,
            &active,
        );
        assert_eq!(changes[0].message_index, 0);
        assert_eq!(changes[0].added, [old]);
        assert_eq!(changes[1].message_index, 3);
        assert_eq!(changes[1].removed, ["old"]);
        assert_eq!(changes[1].added, active);
    }

    #[test]
    fn imported_tool_history_keeps_unregistered_tools_used_by_retained_calls() {
        let old = tool("old", "historical tool");
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append(Event::SystemCheckpoint {
                content: String::new(),
                sections: Vec::new(),
                tools_added: vec![old.spec.clone(), tool("unused", "unused tool").spec],
                tools_removed: Vec::new(),
            })
            .unwrap();
        let assistant = session
            .append(Event::Assistant {
                content: String::new(),
                invocations: vec![crate::Invocation {
                    operation_id: uuid::Uuid::new_v4(),
                    call: crate::ToolCall {
                        id: "call-old".into(),
                        name: old.spec.name.clone(),
                        arguments: "{}".into(),
                    },
                }],
                usage: None,
                request_id: None,
            })
            .unwrap()
            .id;
        let changes = replay(&session.branch(), &[assistant], 1, &[], &[]);
        assert_eq!(changes[0].added, [old]);
        assert_eq!(changes[1].removed, ["old"]);
        assert!(changes[1].added.is_empty());
        assert!(replay(&session.branch(), &[], 1, &[], &[]).is_empty());
    }

    #[test]
    fn reopened_session_replays_tool_changes_without_unregistered_tools() {
        let initial = tool("read", "read a file");
        let discovered = tool("search", "search files");
        let denied = tool("private", "hidden");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tool-transcript.jsonl");
        let mut session = Session::open(&path).unwrap();
        session
            .append(Event::SystemCheckpoint {
                content: "initial".into(),
                sections: Vec::new(),
                tools_added: vec![initial.spec.clone(), denied.spec],
                tools_removed: Vec::new(),
            })
            .unwrap();
        let user = session
            .append(Event::User {
                content: "first".into(),
                input_id: None,
            })
            .unwrap()
            .id;
        let loadout =
            changed_loadout(&session.branch(), std::slice::from_ref(&discovered)).unwrap();
        session.append(loadout).unwrap();
        let next = session
            .append(Event::User {
                content: "second".into(),
                input_id: None,
            })
            .unwrap()
            .id;

        drop(session);
        let session = Session::open(&path).unwrap();
        let registered = [initial.spec.clone(), discovered.spec.clone()];
        let changes = replay(
            &session.branch(),
            &[user, next],
            1,
            &registered,
            std::slice::from_ref(&discovered),
        );
        assert_eq!(changes[0].message_index, 0);
        assert_eq!(changes[0].added, [initial]);
        assert_eq!(changes[1].message_index, 2);
        assert_eq!(changes[1].removed, ["read"]);
        assert_eq!(
            changes[1].added.as_slice(),
            std::slice::from_ref(&discovered)
        );
        assert!(changed_loadout(&session.branch(), &[discovered]).is_none());
    }
}
