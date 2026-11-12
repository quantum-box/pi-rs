//! Explicit, read-only import of external session snapshots.
use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;

use base64::Engine;
use pi_rs::{Error, Event, Invocation, Outcome, Record, Result, Session, ToolCall, Usage};
use serde_json::Value;
use uuid::Uuid;

fn imported_cost(value: &Value) -> Option<serde_json::Number> {
    match &value["cost"]["total"] {
        Value::Number(number)
            if number
                .as_f64()
                .is_some_and(|value| value >= 0.0 && value.is_finite()) =>
        {
            Some(number.clone())
        }
        _ => None,
    }
}

fn append_imported_usage(events: &mut Vec<Event>, value: &Value) -> Option<Usage> {
    value.as_object()?;
    let cache_read = value["cacheRead"].as_u64().unwrap_or(0);
    let cache_write = value["cacheWrite"].as_u64().unwrap_or(0);
    let usage = Usage {
        input_tokens: value["input"]
            .as_u64()
            .unwrap_or(0)
            .saturating_add(cache_read)
            .saturating_add(cache_write),
        output_tokens: value["output"].as_u64().unwrap_or(0),
    };
    events.push(Event::ModelFinished {
        request_id: Uuid::new_v4(),
        metadata: pi_rs::ModelMetadata {
            cost_usd: imported_cost(value),
            cost_breakdown: pi_rs::UsageCostBreakdown::from_value(&value["cost"]),
            cost_from_provider: value["cost"]["totalFromProvider"].as_bool(),
            cache_write_1h_tokens: value["cacheWrite1h"].as_u64(),
            cached_input_tokens: Some(cache_read),
            cache_creation_input_tokens: Some(cache_write),
            ..Default::default()
        },
        usage: pi_rs::UsageReport::Measured { usage },
        failure: None,
        partial: String::new(),
        reply: None,
    });
    Some(usage)
}

fn summary_usage_metadata(events: &[Event]) -> Option<Box<pi_rs::SummaryUsageMetadata>> {
    let (_, metadata) = pi_rs::SummaryUsageMetadata::from_completion(events.last()?)?;
    Some(Box::new(metadata))
}

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
    context_target: Option<u64>,
    calls: BTreeMap<String, Uuid>,
    model: String,
    provider: Option<String>,
    thinking: Option<String>,
    store: BTreeMap<String, Value>,
}
struct Importer {
    records: Vec<Record>,
    timestamp: Option<String>,
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
    let mut root = Branch::default();
    importer.append(
        &mut root,
        Event::Created {
            session_id: header["id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok())
                .unwrap_or_else(Uuid::new_v4),
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
    if let Some(path) = header["parentSession"]
        .as_str()
        .filter(|path| !path.is_empty())
    {
        importer.append(&mut root, Event::ParentSession { path: path.into() });
    }
    let mut branches = BTreeMap::<String, Branch>::new();
    let mut previous = None;
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
        branch.context_target = None;
        for event in events {
            let context = matches!(
                event,
                Event::User { .. }
                    | Event::CustomMessage { .. }
                    | Event::Assistant { .. }
                    | Event::ToolFinished { .. }
                    | Event::ToolReconciled { .. }
            );
            let id = importer.append(&mut branch, event);
            if context {
                branch.context_target = Some(id);
            }
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
fn imported_custom_message(entry: &Value, legacy: bool) -> Result<pi_rs::CustomMessage> {
    let value = entry
        .get("content")
        .ok_or_else(|| Error::Session("imported custom message is missing content".into()))?;
    let content = if let Some(text) = value.as_str() {
        pi_rs::ContextContent::Text(text.into())
    } else {
        let blocks = value
            .as_array()
            .ok_or_else(|| Error::Session("invalid imported custom message content".into()))?;
        let mut result = Vec::with_capacity(blocks.len());
        for block in blocks {
            match block["type"].as_str() {
                Some("text") => result.push(pi_rs::ContentBlock::Text {
                    text: required(block, "text")?.into(),
                }),
                Some("image") => {
                    let (_, images) = user_content(&Value::Array(vec![block.clone()]))?;
                    let image = &images[0];
                    result.push(pi_rs::ContentBlock::Image {
                        data: image.data.clone(),
                        mime_type: image.mime_type.clone(),
                    });
                }
                _ => {
                    return Err(Error::Session(
                        "invalid imported custom message block".into(),
                    ));
                }
            }
        }
        pi_rs::ContextContent::Blocks(result)
    };
    Ok(pi_rs::CustomMessage {
        custom_type: if legacy {
            entry["customType"].as_str().unwrap_or_default()
        } else {
            required(entry, "customType")?
        }
        .into(),
        content,
        display: if legacy {
            entry["display"].as_bool().unwrap_or(false)
        } else {
            entry["display"].as_bool().ok_or_else(|| {
                Error::Session("imported custom message is missing display".into())
            })?
        },
        details: entry.get("details").cloned(),
    })
}

fn anthropic_replay(message: &Value) -> Option<Box<pi_rs::ProviderReplay>> {
    if message["provider"] != "anthropic" || message["api"] != "anthropic-messages" {
        return None;
    }
    let model = message["model"].as_str()?;
    let mut signed = false;
    let mut replay = Vec::new();
    for block in message["content"].as_array()? {
        let raw = match block["type"].as_str()? {
            "text" => serde_json::json!({"type":"text","text":block["text"].as_str()?}),
            "toolCall" => serde_json::json!({
                "type":"tool_use", "id":block["id"].as_str()?, "name":block["name"].as_str()?,
                "input":block["arguments"].as_object()?,
            }),
            "thinking" => {
                let Some(signature) = block["thinkingSignature"]
                    .as_str()
                    .filter(|value| !value.trim().is_empty())
                else {
                    continue;
                };
                signed = true;
                if block["redacted"] == true {
                    serde_json::json!({"type":"redacted_thinking","data":signature})
                } else {
                    serde_json::json!({"type":"thinking","thinking":block["thinking"].as_str()?,"signature":signature})
                }
            }
            _ => return None,
        };
        replay.push(raw);
    }
    signed.then(|| {
        Box::new(pi_rs::ProviderReplay {
            identity: pi_rs::ResolvedModelIdentity {
                provider: "anthropic".into(),
                model: model.into(),
            },
            blocks: replay,
        })
    })
}

fn google_replay(message: &Value) -> Option<Box<pi_rs::ProviderReplay>> {
    if message["provider"] != "google" || message["api"] != "google-generative-ai" {
        return None;
    }
    let model = message["model"].as_str()?;
    let mut signed = false;
    let mut parts = Vec::new();
    for block in message["content"].as_array()? {
        let (mut part, signature) = match block["type"].as_str()? {
            "text" => (
                serde_json::json!({"text":block["text"].as_str()?}),
                &block["textSignature"],
            ),
            "thinking" => (
                serde_json::json!({"thought":true,"text":block["thinking"].as_str()?}),
                &block["thinkingSignature"],
            ),
            "toolCall" => {
                let mut call = serde_json::json!({
                    "name":block["name"].as_str()?,"args":block["arguments"].as_object()?,
                });
                if pi_rs::google::uses_tool_call_ids(model) {
                    call["id"] = serde_json::json!(block["id"].as_str()?);
                }
                (
                    serde_json::json!({"functionCall":call}),
                    &block["thoughtSignature"],
                )
            }
            _ => return None,
        };
        if let Some(signature) = signature.as_str().filter(|signature| !signature.is_empty()) {
            signed = true;
            part["thoughtSignature"] = serde_json::json!(signature);
        }
        parts.push(part);
    }
    signed.then(|| {
        Box::new(pi_rs::ProviderReplay {
            identity: pi_rs::ResolvedModelIdentity {
                provider: "google".into(),
                model: model.strip_prefix("models/").unwrap_or(model).into(),
            },
            blocks: parts,
        })
    })
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
                    let failure = match message["stopReason"].as_str() {
                        Some("error") => Some(pi_rs::ModelFailure::Failed),
                        Some("aborted") => Some(pi_rs::ModelFailure::Cancelled),
                        _ => None,
                    };
                    let mut invocations = Vec::new();
                    if failure.is_none()
                        && let Some(blocks) = message["content"].as_array()
                    {
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
                        input_tokens: usage["input"]
                            .as_u64()
                            .unwrap_or(0)
                            .saturating_add(usage["cacheRead"].as_u64().unwrap_or(0))
                            .saturating_add(usage["cacheWrite"].as_u64().unwrap_or(0)),
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
                            cost_usd: imported_cost(&message["usage"]),
                            cost_breakdown: pi_rs::UsageCostBreakdown::from_value(
                                &message["usage"]["cost"],
                            ),
                            cost_from_provider: message["usage"]["cost"]["totalFromProvider"]
                                .as_bool(),
                            cache_write_1h_tokens: message["usage"]["cacheWrite1h"].as_u64(),
                            cached_input_tokens: message["usage"]["cacheRead"].as_u64(),
                            cache_creation_input_tokens: message["usage"]["cacheWrite"].as_u64(),
                            provider_api: message["api"].as_str().and_then(pi_rs::ModelApi::named),
                            finish_reason: message["stopReason"].as_str().map(str::to_owned),
                            reasoning,
                            diagnostics: pi_rs::AssistantDiagnostic::from_pi_diagnostics(
                                &message["diagnostics"],
                            ),
                            replay: anthropic_replay(message).or_else(|| google_replay(message)),
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
                        failure,
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
                    append_imported_usage(&mut events, &message["usage"]);
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
                    } else if let Some(details) =
                        message.get("details").filter(|details| !details.is_null())
                    {
                        Outcome::SucceededWithDetails {
                            output: text,
                            details: details.clone(),
                        }
                    } else {
                        Outcome::Succeeded(text)
                    };
                    events.push(Event::ToolFinished {
                        operation_id,
                        outcome,
                        states: Default::default(),
                    });
                }
                "custom" | "hookMessage" => events.push(Event::CustomMessage {
                    message: imported_custom_message(message, true)?,
                }),
                "bashExecution" => events.push(Event::BashExecution {
                    execution: serde_json::from_value(message.clone())?,
                }),
                "branchSummary" => events.push(Event::BranchSummary {
                    summary: required(message, "summary")?.into(),
                    extension_summary: None,
                    from: message["fromId"]
                        .as_str()
                        .and_then(|id| branches.get(id))
                        .map(|branch| branch.head),
                    target: None,
                    usage: None,
                    usage_metadata: None,
                    read_files: Vec::new(),
                    modified_files: Vec::new(),
                }),
                "compactionSummary" => events.push(Event::CompactionSummaryMessage {
                    summary: required(message, "summary")?.into(),
                    tokens_before: message["tokensBefore"].as_u64().unwrap_or(0),
                }),
                "system" => {
                    let pi_rs::CompactedSystemPrompt {
                        content,
                        sections,
                        tools: tools_added,
                    } = pi_rs::CompactedSystemPrompt::from_pi(message)?;
                    let tools_removed = message
                        .get("toolsRemoved")
                        .map(|value| {
                            value
                                .as_array()
                                .ok_or_else(|| Error::Session("invalid removed tools".into()))?
                                .iter()
                                .map(|tool| required(tool, "name").map(str::to_owned))
                                .collect::<Result<Vec<_>>>()
                        })
                        .transpose()?
                        .unwrap_or_default();
                    events.push(Event::SystemCheckpoint {
                        content,
                        sections,
                        tools_added,
                        tools_removed,
                    });
                }
                role => {
                    return Err(Error::Session(format!(
                        "unsupported imported message role {role}"
                    )));
                }
            }
        }
        "model_change" => {
            branch.model = required(entry, "modelId")?.into();
            branch.provider = entry["provider"].as_str().map(str::to_owned);
            events.push(Event::ModelSelected {
                model: branch.model.clone(),
                thinking: branch.thinking.clone(),
                provider: branch.provider.clone(),
            });
        }
        "thinking_level_change" => {
            branch.thinking = Some(required(entry, "thinkingLevel")?.into());
            if !branch.model.is_empty() {
                events.push(Event::ModelSelected {
                    model: branch.model.clone(),
                    thinking: branch.thinking.clone(),
                    provider: branch.provider.clone(),
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
            let usage = append_imported_usage(&mut events, &entry["usage"]);
            let usage_metadata = summary_usage_metadata(&events);
            events.push(Event::Compacted {
                summary: required(entry, "summary")?.into(),
                system_prompt: entry
                    .get("systemMessage")
                    .filter(|message| !message.is_null())
                    .map(pi_rs::CompactedSystemPrompt::from_pi)
                    .transpose()?
                    .map(Box::new),
                retain_from,
                tokens_before: entry["tokensBefore"].as_u64(),
                states: Default::default(),
                usage,
                usage_metadata,
            });
        }
        "branch_summary" => {
            let usage = append_imported_usage(&mut events, &entry["usage"]);
            let usage_metadata = summary_usage_metadata(&events);
            let files = |key: &str| {
                entry["details"][key]
                    .as_array()
                    .filter(|_| entry["fromHook"] != true)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            };
            events.push(Event::BranchSummary {
                summary: required(entry, "summary")?.into(),
                extension_summary: (entry["fromHook"] == true).then(|| {
                    pi_rs::ExtensionTreeSummary {
                        details: entry.get("details").cloned(),
                    }
                }),
                from: entry["fromId"]
                    .as_str()
                    .and_then(|id| branches.get(id))
                    .map(|branch| branch.head),
                target: None,
                usage,
                usage_metadata,
                read_files: files("readFiles"),
                modified_files: files("modifiedFiles"),
            });
        }
        "custom_message" => events.push(Event::CustomMessage {
            message: imported_custom_message(entry, false)?,
        }),
        "custom" => {
            events.push(Event::CustomEntry {
                entry: pi_rs::CustomEntry {
                    custom_type: required(entry, "customType")?.into(),
                    data: entry.get("data").cloned(),
                },
            });
            if entry["customType"] == "codemode-store"
                && let (Some(set), Some(deleted)) = (
                    entry["data"]["set"].as_object(),
                    entry["data"]["delete"].as_array(),
                )
                && deleted.iter().all(Value::is_string)
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
        "usage" => {
            append_imported_usage(&mut events, &entry["usage"]);
        }
        "context_edit" => {
            let source = required(entry, "targetId")?;
            let target = branches
                .get(source)
                .and_then(|branch| branch.context_target)
                .ok_or_else(|| {
                    Error::Session(format!("invalid imported context edit target {source}"))
                })?;
            let replacement = if entry["replacement"].is_null() {
                None
            } else {
                Some(serde_json::from_value(
                    entry["replacement"]["content"].clone(),
                )?)
            };
            events.push(Event::ContextEdited {
                target,
                replacement,
            });
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
                Event::CustomMessage { message } => Some(message),
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
