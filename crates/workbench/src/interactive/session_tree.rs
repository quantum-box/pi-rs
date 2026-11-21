use crate::{
    sessions,
    ui::picker::{Item, Picker, Selection, TreeDescription, TreeEntryKind, TreeFilter},
};
use pi_rs::{Event, ModelFailure, Session};
use serde_json::Value;

pub(super) fn picker(session: &Session, filter: TreeFilter) -> Picker {
    let thinking = thinking_rows(session);
    let mut calls = std::collections::HashMap::new();
    let mut assistant_completions = std::collections::HashSet::new();
    for record in session.records() {
        match &record.event {
            Event::Assistant { invocations, .. } => {
                if let Some(completion) = sessions::tree_assistant_completion(session, record) {
                    assistant_completions.insert(completion.id);
                }
                calls.extend(
                    invocations
                        .iter()
                        .map(|entry| (entry.operation_id, &entry.call)),
                );
            }
            Event::ToolPlanned { invocation, .. } => {
                calls.insert(invocation.operation_id, &invocation.call);
            }
            _ => {}
        }
    }
    let head = session.head();
    let labels = session.labels();
    let mut entries = std::collections::BTreeMap::new();
    let mut descriptions = std::collections::BTreeMap::new();
    let mut items = Vec::new();
    for record in session.records() {
        let imported_system = sessions::tree_imported_system_text(session, record);
        let (kind, description) = match &record.event {
            Event::Checkout { .. } => continue,
            Event::SystemCheckpoint { .. } if imported_system.is_some() => (
                TreeEntryKind::Message,
                tree_description("[system]", "dim", String::new()),
            ),
            _ if thinking.contains_key(&record.id) => (
                TreeEntryKind::Bookkeeping,
                tree_description(
                    format!("[thinking: {}]", thinking[&record.id]),
                    "dim",
                    String::new(),
                ),
            ),
            Event::User { content, .. } | Event::UserWithImages { content, .. } => (
                TreeEntryKind::User,
                tree_description("user: ", "accent", tree_preview(content, 200)),
            ),
            Event::Assistant { content, .. } => {
                let completion = sessions::tree_assistant_completion(session, record);
                let failure = completion.and_then(|record| match record.event {
                    Event::ModelFinished { failure, .. } => failure,
                    _ => None,
                });
                let source_error =
                    completion.and_then(|record| sessions::tree_completion_error(session, record));
                let kind = if content.trim().is_empty() && failure.is_none() && record.id != head {
                    TreeEntryKind::Hidden
                } else {
                    TreeEntryKind::Message
                };
                (
                    kind,
                    tree_assistant_description(content, failure, source_error),
                )
            }
            Event::ModelFinished {
                failure: Some(failure),
                partial,
                ..
            } if !assistant_completions.contains(&record.id) => (
                TreeEntryKind::Message,
                tree_assistant_description(
                    partial,
                    Some(*failure),
                    sessions::tree_completion_error(session, record),
                ),
            ),
            Event::ToolFinished { operation_id, .. }
            | Event::ToolReconciled { operation_id, .. } => (
                TreeEntryKind::Tool,
                tree_description(
                    calls
                        .get(operation_id)
                        .map_or_else(|| "[tool]".into(), |call| tree_tool_description(call)),
                    "muted",
                    String::new(),
                ),
            ),
            Event::ImportedUnattributedToolResult { tool_name, .. } => (
                TreeEntryKind::Tool,
                tree_description(format!("[{tool_name}]"), "muted", String::new()),
            ),
            Event::CustomMessage { message } | Event::CustomMessageDelivered { message, .. } => (
                TreeEntryKind::Message,
                tree_description(
                    format!("[{}]: ", message.custom_type),
                    "customMessageLabel",
                    tree_preview(&sessions::tree_context_text(&message.content), 200),
                ),
            ),
            Event::BranchSummary { summary, .. } => (
                TreeEntryKind::Message,
                tree_description("[branch summary]: ", "warning", tree_preview(summary, 200)),
            ),
            Event::Compacted { tokens_before, .. } => (
                TreeEntryKind::Message,
                tree_compaction_description(*tokens_before),
            ),
            Event::CompactionSummaryMessage { tokens_before, .. } => (
                TreeEntryKind::Message,
                tree_compaction_description(Some(*tokens_before)),
            ),
            Event::BashExecution { execution } => (
                TreeEntryKind::Message,
                tree_description(
                    format!("[bash]: {}", tree_preview(&execution.command, 200)),
                    "dim",
                    String::new(),
                ),
            ),
            Event::CacheWarm { .. } | Event::SourceEntry { .. } | Event::ModelFinished { .. } => (
                TreeEntryKind::Hidden,
                tree_description("", "dim", String::new()),
            ),
            event => {
                let heading = match event {
                    Event::ModelSelected { model, .. }
                    | Event::BranchModelSelected { model, .. } => {
                        format!("[model: {model}]")
                    }
                    Event::CustomEntry { entry } => format!("[custom: {}]", entry.custom_type),
                    Event::SessionInfo { name, .. } => {
                        format!("[title: {}]", name.as_deref().unwrap_or("empty"))
                    }
                    Event::Label { label, .. } => {
                        format!("[label: {}]", label.as_deref().unwrap_or("(cleared)"))
                    }
                    Event::ContextEdited {
                        target,
                        replacement,
                    } => {
                        let action = if replacement.is_some() {
                            "replace"
                        } else {
                            "omit"
                        };
                        format!("[context {action}: {target}]")
                    }
                    _ => format!(
                        "[{}]",
                        crate::metadata(record)["event"]
                            .as_str()
                            .unwrap_or("record")
                    ),
                };
                (
                    TreeEntryKind::Bookkeeping,
                    tree_description(heading, "dim", String::new()),
                )
            }
        };
        let mut label = format!("{} {}{}", record.id, description.heading, description.body);
        if let Some(content) = imported_system {
            label.push(' ');
            label.push_str(&tree_preview(content, 200));
        }
        items.push(Item {
            label,
            selection: Selection::Checkout(record.id),
        });
        entries.insert(record.id, (record.parent, kind));
        descriptions.insert(record.id, description);
    }
    let mut picker = Picker::tree(items, entries, labels, head, filter);
    picker.set_tree_descriptions(descriptions);
    picker.set_label_timestamps(session.label_timestamps());
    picker
}

fn thinking_rows(session: &Session) -> std::collections::HashMap<u64, &str> {
    let mut imported = Vec::new();
    let mut rows = std::collections::HashMap::new();
    for record in session.records() {
        match &record.event {
            Event::SourceEntry { format, entry }
                if format == "pi" && entry["type"] == "thinking_level_change" =>
            {
                if let Some(level) = entry["thinkingLevel"].as_str() {
                    imported.push((record.id, level));
                }
            }
            Event::ModelSelected { thinking, .. }
                if tree_parent_is_native_thinking_marker(session, record.parent) =>
            {
                rows.insert(record.id, thinking.as_deref().unwrap_or("off"));
            }
            _ => {}
        }
    }
    if !imported.is_empty()
        && let Ok(counts) = session.validated_pi_conversion_counts()
    {
        for (source, level) in imported {
            // A thinking entry before model selection has no conversion.
            // Otherwise use the converted record, which also holds its label.
            if let Some(count) = counts.get(&source) {
                rows.insert(source + u64::from(*count > 0), level);
            }
        }
    }
    rows
}

fn tree_parent_is_native_thinking_marker(session: &Session, parent: Option<u64>) -> bool {
    parent
        .and_then(|id| id.checked_sub(1))
        .and_then(|index| session.records().get(index as usize))
        .is_some_and(|parent| {
            matches!(&parent.event, Event::SourceEntry { format, .. } if format == "pi-rs:thinking-selection")
        })
}

fn tree_description(
    heading: impl Into<String>,
    heading_role: &'static str,
    body: String,
) -> TreeDescription {
    TreeDescription {
        heading: heading.into().replace(['\r', '\n', '\t'], " "),
        heading_role,
        body,
        body_role: "text",
    }
}

fn tree_preview(text: &str, limit: usize) -> String {
    // Pi's previews are measured in UTF-16 code units. Keep Rust strings
    // valid when the preview boundary falls in the middle of a surrogate pair.
    let mut units = 0;
    let preview: String = text
        .chars()
        .take_while(|character| {
            units += character.len_utf16();
            units <= limit
        })
        .map(|character| match character {
            '\n' | '\r' | '\t' => ' ',
            character => character,
        })
        .collect();
    preview.trim().to_owned()
}

fn tree_assistant_description(
    content: &str,
    failure: Option<ModelFailure>,
    source_error: Option<&str>,
) -> TreeDescription {
    let content = tree_preview(content, 200);
    let (body, role) = if !content.is_empty() {
        (content, "text")
    } else {
        match failure {
            Some(ModelFailure::Cancelled) => ("(aborted)".into(), "muted"),
            Some(_) => (
                source_error.map_or_else(|| "(error)".into(), |message| tree_preview(message, 80)),
                "error",
            ),
            None => ("(no content)".into(), "muted"),
        }
    };
    let mut description = tree_description("assistant: ", "success", body);
    description.body_role = role;
    description
}

fn tree_compaction_description(tokens_before: Option<u64>) -> TreeDescription {
    let heading = tokens_before.map_or_else(
        || "[compaction]".into(),
        |tokens| {
            let thousands = tokens / 1000 + u64::from(tokens % 1000 >= 500);
            format!("[compaction: {thousands}k tokens]")
        },
    );
    tree_description(heading, "borderAccent", String::new())
}

fn tree_tool_description(call: &pi_rs::ToolCall) -> String {
    let arguments: Value = serde_json::from_str(&call.arguments).unwrap_or(Value::Null);
    let field = |name| arguments[name].as_str().filter(|value| !value.is_empty());
    let path = || field("path").or_else(|| field("file_path"));
    let name = &call.name;
    match name.as_str() {
        "read" | "write" | "edit" => format!("[{name}: {}]", path().unwrap_or_default()),
        "bash" => {
            let raw = field("command").unwrap_or_default();
            let suffix = if raw.encode_utf16().count() > 50 {
                "..."
            } else {
                ""
            };
            format!("[bash: {}{suffix}]", tree_preview(raw.trim(), 50))
        }
        "grep" | "find" => {
            let raw = field("pattern").unwrap_or_default();
            let pattern = if name == "grep" {
                format!("/{raw}/")
            } else {
                raw.into()
            };
            format!("[{name}: {pattern} in {}]", path().unwrap_or("."))
        }
        "ls" => format!("[ls: {}]", path().unwrap_or(".")),
        _ => {
            let args = if arguments.is_object() {
                arguments.to_string()
            } else {
                call.arguments.clone()
            };
            let suffix = if args.encode_utf16().count() > 40 {
                "..."
            } else {
                ""
            };
            format!("[{name}: {}{suffix}]", tree_preview(&args, 40))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ui::{keys::Keys, theme::Theme};
    use serde_json::json;

    #[test]
    fn tree_distinguishes_thinking_changes_from_model_changes() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("pi.jsonl");
        let entries = [
            json!({"type":"session","version":3}),
            json!({"type":"thinking_level_change","id":"high","parentId":null,"thinkingLevel":"high"}),
            json!({"type":"model_change","id":"model","parentId":"high","provider":"ollama","modelId":"fixture-a"}),
            json!({"type":"thinking_level_change","id":"low","parentId":"model","thinkingLevel":"low"}),
            json!({"type":"message","id":"user","parentId":"low","message":{"role":"user","content":"question"}}),
        ];
        let fixture = entries
            .iter()
            .map(|entry| format!("{entry}\n"))
            .collect::<String>();
        std::fs::write(&path, fixture).unwrap();
        let mut session = crate::session_import::read(&path).unwrap();
        session
            .select_thinking_level(Some("ollama"), "fixture-a", "medium")
            .unwrap();

        let theme = Theme::system();
        let keys = Keys::default();
        for (filter, expected) in [(TreeFilter::Default, 0), (TreeFilter::All, 1)] {
            let lines = picker(&session, filter).render(&theme, 100, 28, &keys);
            for heading in [
                "[thinking: high]",
                "[model: fixture-a]",
                "[thinking: low]",
                "[thinking: medium]",
            ] {
                let matching = lines
                    .iter()
                    .filter(|line| line.spans.iter().any(|span| span.text == heading))
                    .count();
                assert_eq!(matching, expected, "{heading}");
            }
        }
    }

    #[test]
    fn imported_tree_rows_use_pi_message_labels_and_theme_roles() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("pi.jsonl");
        let timestamp = "2025-02-03T01:02:03Z";
        let entries = [
            json!({"type":"session","version":3,"id":"tree-parity","cwd":root.path(),"timestamp":timestamp}),
            json!({"type":"message","id":"user","parentId":null,"timestamp":timestamp,
                "message":{"role":"user","content":"user\ntext"}}),
            json!({"type":"message","id":"system","parentId":"user","timestamp":timestamp,
                "message":{"role":"system","content":"unique-system-sentinel\nkeep line breaks"}}),
            json!({"type":"message","id":"error","parentId":"system","timestamp":timestamp,
                "message":{"role":"assistant","content":[],"stopReason":"error","errorMessage":"fixture\nerror"}}),
            json!({"type":"custom_message","id":"custom","parentId":"error","timestamp":timestamp,
                "customType":"checkpoint","content":[{"type":"text","text":"first\t"},{"type":"text","text":"second"}],"display":false}),
            json!({"type":"label","id":"label","parentId":"custom","timestamp":timestamp,"targetId":"custom","label":"milestone"}),
        ];
        let fixture = entries
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&path, fixture).unwrap();
        let mut session = crate::session_import::read(&path).unwrap();
        let mut picker = picker(&session, TreeFilter::Default);
        let keys = Keys::default();
        let theme = Theme::load("dark", &[], root.path(), false, false).unwrap();
        let rendered = picker.render(&theme, 100, 22, &keys);
        let text = rendered
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.text.as_str())
                    .collect::<String>()
            })
            .collect::<Vec<_>>();
        assert!(
            text.iter().any(|line| line == "  • user: user text"),
            "{text:?}"
        );
        assert_eq!(
            text.iter()
                .filter(|line| line.contains("assistant:"))
                .count(),
            1,
            "{text:?}"
        );
        assert!(
            text.iter()
                .any(|line| line == "  • assistant: fixture error"),
            "{text:?}"
        );
        assert!(
            text.iter()
                .any(|line| line == "› • [milestone] [checkpoint]: first second"),
            "{text:?}"
        );
        let spans: Vec<_> = rendered.iter().flat_map(|line| &line.spans).collect();
        for (text, role, bold) in [
            ("user: ", "accent", false),
            ("assistant: ", "success", false),
            ("fixture error", "error", false),
            ("[milestone] ", "warning", false),
            ("[checkpoint]: ", "customMessageLabel", true),
        ] {
            assert!(spans.iter().any(|span| span.text == text
                && span.color == theme.color(role)
                && span.bold == bold));
        }
        let Some(Selection::Checkout(id)) = picker.selected() else {
            panic!("expected the custom message to have focus");
        };
        assert_eq!(
            sessions::tree_record_text(&session, id).unwrap(),
            "first\tsecond"
        );
        let failed = session
            .records()
            .iter()
            .find(|record| matches!(&record.event, Event::Assistant { content, .. } if content.is_empty()))
            .unwrap();
        assert_eq!(
            sessions::tree_record_text(&session, failed.id).unwrap(),
            "fixture\nerror"
        );
        picker.input(
            crossterm::event::KeyEvent::new(
                crossterm::event::KeyCode::Char('T'),
                crossterm::event::KeyModifiers::SHIFT,
            ),
            &keys,
        );
        assert!(picker.render(&theme, 100, 22, &keys).iter().any(|line| {
            line.spans
                .iter()
                .any(|span| span.text.starts_with("25/") && span.color == theme.color("muted"))
        }));
        assert!(text.iter().any(|line| line == "  • [system]"));
        session.set_label(id, Some("updated label".into())).unwrap();
        let updated = super::picker(&session, TreeFilter::Default);
        picker.refresh_tree(updated, id);
        picker.input(
            crossterm::event::KeyEvent::new(
                crossterm::event::KeyCode::Char('a'),
                crossterm::event::KeyModifiers::CONTROL,
            ),
            &keys,
        );
        assert!(
            picker.render(&theme, 100, 22, &keys).iter().any(|line| {
                line.spans
                    .iter()
                    .any(|span| span.text == "[label: updated label]")
            }),
            "all entries include the newly committed label record"
        );

        let mut system_search = super::picker(&session, TreeFilter::Default);
        system_search.paste("unique-system-sentinel");
        let Some(Selection::Checkout(id)) = system_search.selected() else {
            panic!("expected the imported system message to match the search");
        };
        assert!(matches!(
            session.records()[id as usize - 1].event,
            Event::SystemCheckpoint { .. }
        ));
        assert_eq!(
            sessions::tree_record_text(&session, id).unwrap(),
            "unique-system-sentinel\nkeep line breaks"
        );
    }
}
