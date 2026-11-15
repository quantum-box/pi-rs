//! File operations from the projected context being compacted.
use crate::{Event, Message, Session, ToolCall};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

#[derive(Default)]
pub(crate) struct FileOperations {
    read: HashSet<String>,
    modified: HashSet<String>,
    truncated: bool,
}

impl FileOperations {
    fn add_call(&mut self, call: &ToolCall) {
        let target = match call.name.as_str() {
            "read" => &mut self.read,
            "write" | "edit" => &mut self.modified,
            _ => return,
        };
        if let Ok(arguments) = crate::parse_tool_arguments(&call.arguments)
            && let Some(path) = arguments
                .get("path")
                .and_then(|path| path.as_str())
                .filter(|path| !path.is_empty())
        {
            target.insert(path.to_owned());
        }
    }

    pub(crate) fn collect(session: &Session, boundary: Option<u64>) -> Self {
        let branch = session.branch();
        let mut files = Self::default();
        if let Some(previous) = branch
            .iter()
            .rev()
            .find(|record| matches!(record.event, Event::Compacted { .. }))
            && let Some(metadata) = session.record_metadata(previous.id)
            && (metadata["fromExtension"] != true || metadata["nativeGeneratedFileDetails"] == true)
        {
            files.truncated = metadata["nativeFileDetailsTruncated"] == true;
            for (key, target) in [
                ("readFiles", &mut files.read),
                ("modifiedFiles", &mut files.modified),
            ] {
                if let Some(paths) = metadata["details"][key].as_array() {
                    target.extend(
                        paths
                            .iter()
                            .filter_map(|value| value.as_str())
                            .map(str::to_owned),
                    );
                }
            }
        }
        let messages = session.current_message_entries();
        let result_operations: HashMap<_, _> = branch
            .iter()
            .filter_map(|record| match record.event {
                Event::ToolFinished { operation_id, .. }
                | Event::ToolReconciled { operation_id, .. } => Some((record.id, operation_id)),
                _ => None,
            })
            .collect();
        let mut stack = Vec::new();
        for (source, message) in messages
            .iter()
            .filter(|(id, _)| boundary.is_none_or(|cut| *id < cut))
        {
            match message {
                Message::Assistant { calls, .. } => {
                    calls.iter().for_each(|call| files.add_call(call))
                }
                Message::Tool { .. } => {
                    if let Some(operation_id) = result_operations.get(source) {
                        stack.push(*operation_id);
                    }
                }
                _ => {}
            }
        }
        let mut children: HashMap<Uuid, Vec<&crate::Invocation>> = HashMap::new();
        for record in &branch {
            if boundary.is_some_and(|cut| record.id >= cut) {
                continue;
            }
            if let Event::ToolPlanned {
                invocation,
                parent_operation,
            } = &record.event
            {
                children
                    .entry(*parent_operation)
                    .or_default()
                    .push(invocation);
            }
        }
        let mut visited = HashSet::new();
        while let Some(parent) = stack.pop() {
            if !visited.insert(parent) {
                continue;
            }
            if let Some(nested) = children.get(&parent) {
                for invocation in nested {
                    files.add_call(&invocation.call);
                    stack.push(invocation.operation_id);
                }
            }
        }
        files.read.retain(|path| !files.modified.contains(path));
        files
    }

    fn sorted(values: &HashSet<String>) -> Vec<&str> {
        let mut result: Vec<_> = values.iter().map(String::as_str).collect();
        result.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
        result
    }
    #[cfg(test)]
    pub(crate) fn details(&self) -> serde_json::Value {
        serde_json::json!({"readFiles":Self::sorted(&self.read), "modifiedFiles":Self::sorted(&self.modified)})
    }
    // Bound the encoded JSON, not the unescaped path length: controls may
    // expand sixfold on the journal wire. Keep modified files first.
    pub(crate) fn bounded_details(&self, limit: usize) -> (serde_json::Value, bool) {
        let mut remaining = limit.saturating_sub(64);
        let mut truncated = self.truncated;
        let mut collect = |paths: &HashSet<String>| {
            let mut kept = Vec::new();
            for path in Self::sorted(paths) {
                let encoded = serde_json::to_string(&path)
                    .expect("string serialization")
                    .len()
                    .saturating_add(1);
                if encoded <= remaining {
                    remaining -= encoded;
                    kept.push(path.to_owned());
                } else {
                    truncated = true;
                }
            }
            kept
        };
        let modified = collect(&self.modified);
        let read = collect(&self.read);
        (
            serde_json::json!({"readFiles":read,"modifiedFiles":modified}),
            truncated,
        )
    }
    pub(crate) fn suffix(&self, limit: usize) -> Option<String> {
        let mut size = 0usize;
        for (name, values) in [
            ("read-files", &self.read),
            ("modified-files", &self.modified),
        ] {
            if values.is_empty() {
                continue;
            }
            size = size
                .saturating_add(format!("\n\n<{name}>\n").len())
                .saturating_add(format!("\n</{name}>").len())
                .saturating_add(values.len().saturating_sub(1));
            for value in values {
                size = size.saturating_add(value.len());
            }
            if size > limit {
                return None;
            }
        }
        let mut sections = Vec::new();
        for (name, values) in [
            ("read-files", &self.read),
            ("modified-files", &self.modified),
        ] {
            let values = Self::sorted(values);
            if !values.is_empty() {
                sections.push(format!("<{name}>\n{}\n</{name}>", values.join("\n")));
            }
        }
        if sections.is_empty() {
            Some(String::new())
        } else {
            Some(format!("\n\n{}", sections.join("\n\n")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Invocation, MemoryStore, Outcome};

    fn call(name: &str, id: &str, path: &str) -> Invocation {
        Invocation {
            operation_id: Uuid::new_v4(),
            call: ToolCall {
                id: id.into(),
                name: name.into(),
                arguments: serde_json::json!({"path":path}).to_string(),
            },
        }
    }

    #[test]
    fn collects_projected_prefix_and_nested_calls_without_retained_files() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append(Event::User {
                content: "old".into(),
                input_id: None,
            })
            .unwrap();
        let calls = vec![
            call("read", "r1", "b"),
            call("read", "r2", "a"),
            call("write", "w", "b"),
            call("edit", "e", "z"),
        ];
        let assistant_id = session
            .append(Event::Assistant {
                content: String::new(),
                invocations: calls.clone(),
                usage: None,
                request_id: None,
            })
            .unwrap()
            .id;
        let nested = call("edit", "nested", "nested");
        session
            .append(Event::ToolPlanned {
                invocation: nested.clone(),
                parent_operation: calls[0].operation_id,
            })
            .unwrap();
        session
            .append(Event::ToolFinished {
                operation_id: nested.operation_id,
                outcome: Outcome::Succeeded("ok".into()),
                states: Default::default(),
            })
            .unwrap();
        for call in calls {
            session
                .append(Event::ToolFinished {
                    operation_id: call.operation_id,
                    outcome: Outcome::Succeeded("ok".into()),
                    states: Default::default(),
                })
                .unwrap();
        }
        let boundary = session
            .append(Event::User {
                content: "retained".into(),
                input_id: None,
            })
            .unwrap()
            .id;
        let retained = call("read", "r1", "retained-file");
        session
            .append(Event::Assistant {
                content: String::new(),
                invocations: vec![retained.clone()],
                usage: None,
                request_id: None,
            })
            .unwrap();
        let retained_nested = call("write", "retained-nested", "retained-nested-file");
        session
            .append(Event::ToolPlanned {
                invocation: retained_nested.clone(),
                parent_operation: retained.operation_id,
            })
            .unwrap();
        session
            .append(Event::ToolFinished {
                operation_id: retained_nested.operation_id,
                outcome: Outcome::Succeeded("ok".into()),
                states: Default::default(),
            })
            .unwrap();
        session
            .append(Event::ToolFinished {
                operation_id: retained.operation_id,
                outcome: Outcome::Succeeded("ok".into()),
                states: Default::default(),
            })
            .unwrap();
        let files = FileOperations::collect(&session, Some(boundary));
        assert_eq!(
            files.details(),
            serde_json::json!({"readFiles":["a"],"modifiedFiles":["b","nested","z"]})
        );
        assert_eq!(
            files.suffix(usize::MAX).unwrap(),
            "\n\n<read-files>\na\n</read-files>\n\n<modified-files>\nb\nnested\nz\n</modified-files>"
        );
        let suffix = files.suffix(usize::MAX).unwrap();
        assert_eq!(files.suffix(suffix.len()), Some(suffix.clone()));
        assert_eq!(files.suffix(suffix.len() - 1), None);
        session
            .append(Event::ContextEdited {
                target: assistant_id,
                replacement: None,
            })
            .unwrap();
        assert_eq!(
            FileOperations::collect(&session, Some(boundary)).details(),
            serde_json::json!({"readFiles":[],"modifiedFiles":[]})
        );
    }

    #[test]
    fn carries_prior_generated_details_but_not_hook_details() {
        for from_hook in [false, true] {
            let mut session = Session::with_store(MemoryStore::default()).unwrap();
            session
                .append(Event::User {
                    content: "old".into(),
                    input_id: None,
                })
                .unwrap();
            session.append_with_record_metadata(Event::Compacted { summary:"previous".into(), system_prompt:None, retain_from:None, tokens_before:None, states:Default::default(), usage:None, usage_metadata:None }, serde_json::json!({"fromExtension":from_hook,"details":{"readFiles":["before"],"modifiedFiles":["changed"]}})).unwrap();
            session
                .append(Event::User {
                    content: "new".into(),
                    input_id: None,
                })
                .unwrap();
            let files = FileOperations::collect(&session, Some(session.head()));
            let expected = if from_hook {
                serde_json::json!({"readFiles":[],"modifiedFiles":[]})
            } else {
                serde_json::json!({"readFiles":["before"],"modifiedFiles":["changed"]})
            };
            assert_eq!(files.details(), expected);
        }
    }

    #[test]
    fn file_lists_sort_like_javascript_utf16() {
        let mut files = FileOperations::default();
        files
            .read
            .extend(["\u{e000}".into(), "😀".into(), "a".into()]);
        assert_eq!(
            files.details()["readFiles"],
            serde_json::json!(["a", "😀", "\u{e000}"])
        );
    }

    #[test]
    fn bounded_details_counts_escaped_bytes_and_prioritizes_modified_files() {
        let mut files = FileOperations::default();
        files.modified.insert("modified".into());
        files.read.insert("\0".repeat(50));
        let (details, truncated) = files.bounded_details(128);
        assert!(truncated);
        assert_eq!(details["modifiedFiles"], serde_json::json!(["modified"]));
        assert_eq!(details["readFiles"], serde_json::json!([]));
        assert!(serde_json::to_vec(&details).unwrap().len() <= 128);
    }
}
