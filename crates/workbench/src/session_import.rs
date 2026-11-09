//! Explicit, read-only import of external session snapshots.
use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;

use base64::Engine;
use pi_rs::{Error, Event, Invocation, Outcome, Record, Result, Session, ToolCall, Usage};
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
        Session::from_records(
            entries
                .into_iter()
                .map(serde_json::from_value)
                .collect::<std::result::Result<Vec<_>, _>>()?,
        )
    }
}

#[derive(Clone, Default)]
struct Branch {
    head: u64,
    entry_start: u64,
    calls: BTreeMap<String, Uuid>,
    model: String,
    thinking: Option<String>,
    store: BTreeMap<String, Value>,
}
struct Importer {
    records: Vec<Record>,
}
impl Importer {
    fn append(&mut self, branch: &mut Branch, event: Event) -> u64 {
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
    let version = header["version"].as_u64().unwrap_or(1);
    if !(1..=3).contains(&version) {
        return Err(Error::Session(format!(
            "unsupported Pi session version {version}"
        )));
    }
    let mut importer = Importer {
        records: Vec::new(),
    };
    let mut root = Branch::default();
    importer.append(
        &mut root,
        Event::Created {
            session_id: Uuid::new_v4(),
        },
    );
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
    let mut branches = BTreeMap::<String, Branch>::new();
    let mut previous = None;
    for (index, entry) in entries.into_iter().enumerate().skip(1) {
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
            Some(parent) => branches
                .get(parent)
                .cloned()
                .ok_or_else(|| Error::Session(format!("missing imported parent {parent}")))?,
            None => root.clone(),
        };
        let events = convert(&entry, &mut branch, &branches)?;
        importer.append(
            &mut branch,
            Event::SourceEntry {
                format: "pi".into(),
                entry,
            },
        );
        branch.entry_start = branch.head + u64::from(!events.is_empty());
        for event in events {
            importer.append(&mut branch, event);
        }
        branches.insert(id.clone(), branch);
        previous = Some(id);
    }
    Session::from_records(importer.records)
}
fn required<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .ok_or_else(|| Error::Session(format!("imported entry is missing {field}")))
}
fn content(value: &Value) -> Result<String> {
    if value.is_null() {
        return Ok(String::new());
    }
    if let Some(text) = value.as_str() {
        return Ok(text.into());
    }
    let blocks = value
        .as_array()
        .ok_or_else(|| Error::Session("invalid imported message content".into()))?;
    let mut text = String::new();
    for block in blocks {
        match block["type"].as_str() {
            Some("text") => text.push_str(required(block, "text")?),
            Some("thinking" | "toolCall") => {}
            _ => {
                return Err(Error::Session(
                    "this imported message content requires multimodal support".into(),
                ));
            }
        }
    }
    Ok(text)
}
fn user_content(value: &Value) -> Result<(String, Vec<pi_rs::ImageContent>)> {
    let Some(blocks) = value.as_array() else {
        return Ok((content(value)?, Vec::new()));
    };
    let mut text = String::new();
    let mut images = Vec::new();
    for block in blocks {
        if block["type"] == "image" {
            let mime = required(block, "mimeType")?;
            let data = required(block, "data")?;
            if !matches!(
                mime,
                "image/png" | "image/jpeg" | "image/gif" | "image/webp"
            ) || base64::engine::general_purpose::STANDARD
                .decode(data)
                .is_err()
            {
                return Err(Error::Session("invalid imported image".into()));
            }
            images.push(pi_rs::ImageContent {
                data: data.into(),
                mime_type: mime.into(),
            });
        } else {
            text.push_str(&content(&Value::Array(vec![block.clone()]))?);
        }
    }
    Ok((text, images))
}
fn push_user(events: &mut Vec<Event>, value: &Value) -> Result<()> {
    let (content, images) = user_content(value)?;
    events.push(Event::User {
        content,
        input_id: None,
    });
    if !images.is_empty() {
        events.push(Event::UserImages { images });
    }
    Ok(())
}
fn convert(
    entry: &Value,
    branch: &mut Branch,
    branches: &BTreeMap<String, Branch>,
) -> Result<Vec<Event>> {
    let mut events = Vec::new();
    match required(entry, "type")? {
        "message" => {
            let message = &entry["message"];
            match required(message, "role")? {
                "user" => push_user(&mut events, &message["content"])?,
                "assistant" => {
                    let mut invocations = Vec::new();
                    if let Some(blocks) = message["content"].as_array() {
                        for block in blocks.iter().filter(|block| block["type"] == "toolCall") {
                            let id = required(block, "id")?.to_owned();
                            let operation_id = Uuid::new_v4();
                            if branch.calls.insert(id.clone(), operation_id).is_some() {
                                return Err(Error::Session(format!(
                                    "duplicate unfinished imported tool call {id}"
                                )));
                            }
                            invocations.push(Invocation {
                                operation_id,
                                call: ToolCall {
                                    id,
                                    name: required(block, "name")?.into(),
                                    arguments: block["arguments"].to_string(),
                                },
                            });
                        }
                    }
                    let started: Vec<_> = invocations
                        .iter()
                        .map(|invocation| Event::ToolStarted {
                            operation_id: invocation.operation_id,
                        })
                        .collect();
                    let usage = message.get("usage").map(|usage| Usage {
                        input_tokens: usage["input"].as_u64().unwrap_or(0),
                        output_tokens: usage["output"].as_u64().unwrap_or(0),
                    });
                    let request_id = Uuid::new_v4();
                    let reasoning = message["content"]
                        .as_array()
                        .map(|blocks| {
                            blocks
                                .iter()
                                .filter(|block| block["type"] == "thinking")
                                .filter_map(|block| block["thinking"].as_str())
                                .collect::<Vec<_>>()
                                .join("\n")
                        })
                        .filter(|text| !text.is_empty());
                    events.push(Event::ModelFinished {
                        request_id,
                        metadata: pi_rs::ModelMetadata {
                            finish_reason: message["stopReason"].as_str().map(str::to_owned),
                            reasoning,
                            resolved_model: message["model"].as_str().map(|model| {
                                pi_rs::ResolvedModelIdentity {
                                    provider: message["provider"]
                                        .as_str()
                                        .unwrap_or_default()
                                        .into(),
                                    model: model.into(),
                                }
                            }),
                            ..Default::default()
                        },
                        usage: usage
                            .map(|usage| pi_rs::UsageReport::Measured { usage })
                            .unwrap_or(pi_rs::UsageReport::Unconfirmed),
                        failure: None,
                        partial: String::new(),
                        reply: None,
                    });
                    events.push(Event::Assistant {
                        content: content(&message["content"])?,
                        invocations,
                        usage,
                        request_id: Some(request_id),
                    });
                    events.extend(started);
                }
                "toolResult" => {
                    let id = required(message, "toolCallId")?;
                    let operation_id = branch.calls.remove(id).ok_or_else(|| {
                        Error::Session(format!("imported tool result has no call {id}"))
                    })?;
                    let (text, images) = user_content(&message["content"])?;
                    let outcome = if !images.is_empty() {
                        Outcome::Structured {
                            data: serde_json::json!({"content":message["content"],"isError":message["isError"] == true}),
                            attachments: Vec::new(),
                        }
                    } else if message["isError"] == true {
                        Outcome::Failed(text)
                    } else {
                        Outcome::Succeeded(text)
                    };
                    events.push(Event::ToolFinished {
                        operation_id,
                        outcome,
                        states: Default::default(),
                    });
                }
                "custom" | "hookMessage" => push_user(&mut events, &message["content"])?,
                "bashExecution" => {
                    if message["excludeFromContext"] != true {
                        let mut text = format!("Ran `{}`\n", required(message, "command")?);
                        let output = message["output"].as_str().unwrap_or_default();
                        if output.is_empty() {
                            text.push_str("(no output)");
                        } else {
                            text.push_str(&format!("```\n{output}\n```"));
                        }
                        if message["cancelled"] == true {
                            text.push_str("\n\n(command cancelled)");
                        } else if let Some(code) =
                            message["exitCode"].as_i64().filter(|code| *code != 0)
                        {
                            text.push_str(&format!("\n\nCommand exited with code {code}"));
                        }
                        if message["truncated"] == true
                            && let Some(path) = message["fullOutputPath"].as_str()
                        {
                            text.push_str(&format!("\n\n[Output truncated. Full output: {path}]"));
                        }
                        events.push(Event::User {
                            content: text,
                            input_id: None,
                        });
                    }
                }
                "branchSummary" | "compactionSummary" => events.push(Event::User {
                    content: required(message, "summary")?.into(),
                    input_id: None,
                }),
                "system" => {}
                role => {
                    return Err(Error::Session(format!(
                        "unsupported imported message role {role}"
                    )));
                }
            }
        }
        "model_change" => {
            branch.model = required(entry, "modelId")?.into();
            events.push(Event::ModelSelected {
                model: branch.model.clone(),
                thinking: branch.thinking.clone(),
            });
        }
        "thinking_level_change" => {
            branch.thinking = Some(required(entry, "thinkingLevel")?.into());
            if !branch.model.is_empty() {
                events.push(Event::ModelSelected {
                    model: branch.model.clone(),
                    thinking: branch.thinking.clone(),
                });
            }
        }
        "session_info" => events.push(Event::SessionInfo {
            name: entry["name"].as_str().map(str::to_owned),
            cwd: None,
        }),
        "label" => {
            let target = required(entry, "targetId")?;
            let target = branches
                .get(target)
                .ok_or_else(|| Error::Session(format!("missing imported label target {target}")))?
                .entry_start;
            events.push(Event::Label {
                target,
                label: entry["label"].as_str().map(str::to_owned),
            });
        }
        "compaction" => {
            let target = required(entry, "firstKeptEntryId")?;
            let retain_from = if Some(target) == entry["id"].as_str() {
                None
            } else {
                Some(
                    branches
                        .get(target)
                        .ok_or_else(|| {
                            Error::Session(format!("missing imported compaction target {target}"))
                        })?
                        .entry_start,
                )
            };
            events.push(Event::Compacted {
                summary: required(entry, "summary")?.into(),
                retain_from,
                states: Default::default(),
                usage: None,
            });
        }
        "branch_summary" => events.push(Event::User {
            content: format!("Prior branch summary:\n{}", required(entry, "summary")?),
            input_id: None,
        }),
        "custom_message" => push_user(&mut events, &entry["content"])?,
        "custom" if entry["customType"] == "codemode-store" => {
            if let (Some(set), Some(deleted)) = (
                entry["data"]["set"].as_object(),
                entry["data"]["delete"].as_array(),
            ) && deleted.iter().all(Value::is_string)
            {
                for key in deleted.iter().filter_map(Value::as_str) {
                    branch.store.remove(key);
                }
                branch
                    .store
                    .extend(set.iter().map(|(key, value)| (key.clone(), value.clone())));
                let data = serde_json::to_value(&branch.store)?;
                if data.to_string().len() > 4 * 1024 * 1024 {
                    return Err(Error::Session(
                        "imported codemode store exceeds size limit".into(),
                    ));
                }
                events.push(Event::ExtensionStates {
                    states: [(
                        "codemode".into(),
                        pi_rs::ExtensionState { version: 1, data },
                    )]
                    .into(),
                });
            }
        }
        "custom" | "usage" => {}
        "context_edit" => {
            return Err(Error::Session(
                "context edits in imported sessions require projection support".into(),
            ));
        }
        kind => {
            return Err(Error::Session(format!(
                "unsupported imported entry type {kind}"
            )));
        }
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_rs::Message;
    use serde_json::json;
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
    fn rejects_missing_parents_and_unsupported_content() {
        let header = json!({"type":"session","version":3});
        assert!(import_pi(vec![header.clone(), json!({"type":"message","id":"x","parentId":"missing","message":{"role":"user","content":"x"}})]).is_err());
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
