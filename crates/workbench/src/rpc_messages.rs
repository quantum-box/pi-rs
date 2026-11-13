//! Project the native session transcript into the Pi RPC message format.
use std::collections::{HashMap, HashSet};

use pi_rs::{
    Event, Invocation, Message, ModelMetadata, ModelRequest, Record, Session, Usage, UsageReport,
};
use serde_json::{Value, json};
use uuid::Uuid;

use super::Provider;

#[derive(Clone, Copy)]
struct ModelIdentity<'a> {
    provider: &'a str,
    model: &'a str,
}

#[derive(Clone, Copy)]
struct Finished<'a> {
    metadata: &'a ModelMetadata,
    usage: Option<Usage>,
}

pub(super) fn pending_request(session: &Session) -> Option<Uuid> {
    for record in session.branch().into_iter().rev() {
        match &record.event {
            Event::ModelRequested { request_id, .. } => return *request_id,
            Event::Assistant { .. }
            | Event::ModelFinished {
                failure: Some(_), ..
            } => return None,
            _ => {}
        }
    }
    None
}

fn latest_compaction<'a>(branch: &[&'a Record]) -> Option<&'a Record> {
    branch
        .iter()
        .rev()
        .copied()
        .find(|record| matches!(record.event, Event::Compacted { .. }))
}

fn display_only(record: &Record, compaction: Option<&Record>) -> bool {
    match &record.event {
        Event::SystemCheckpoint { .. } => compaction.is_none_or(|entry| record.id > entry.id),
        Event::BashExecution { execution } if execution.exclude_from_context => {
            let retain_from = compaction.and_then(|entry| match entry.event {
                Event::Compacted { retain_from, .. } => Some(retain_from.unwrap_or(entry.id)),
                _ => None,
            });
            retain_from.is_none_or(|retain| record.id >= retain)
        }
        _ => false,
    }
}

pub(super) fn count(session: &Session) -> usize {
    let branch = session.branch();
    let compaction = latest_compaction(&branch);
    session.current_message_entries().len()
        + branch
            .iter()
            .filter(|record| display_only(record, compaction))
            .count()
        + usize::from(compaction.is_some_and(|record| {
            matches!(
                record.event,
                Event::Compacted {
                    system_prompt: Some(_),
                    ..
                }
            )
        }))
}

pub(super) fn last_assistant_text(session: &Session) -> Option<String> {
    let (messages, _) = snapshot(session);
    let message = messages.iter().rev().find(|message| {
        message["role"] == "assistant"
            && !(message["stopReason"] == "aborted"
                && message["content"].as_array().is_some_and(Vec::is_empty))
    })?;
    let blocks = message["content"].as_array()?;
    let text: String = blocks
        .iter()
        .filter(|block| block["type"] == "text")
        .filter_map(|block| block["text"].as_str())
        .collect();
    let text =
        text.trim_matches(|c: char| (c.is_whitespace() && c != '\u{0085}') || c == '\u{feff}');
    (!text.is_empty()).then(|| text.to_owned())
}

pub(super) fn snapshot(session: &Session) -> (Vec<Value>, Vec<Message>) {
    let branch = session.branch();
    let entries = session.current_message_entries();
    let projection = Projection::new(branch.iter().copied());
    let mut imported_entries = HashMap::new();
    let mut import = None;
    let mut edited = HashSet::new();
    let compaction = latest_compaction(&branch);
    for record in &branch {
        if let Event::SourceEntry { format, entry } = &record.event {
            import = (format == "pi").then_some(entry);
        } else if let Some(source) = import
            && !matches!(record.event, Event::ModelFinished { .. })
        {
            if matches_imported_message(source, &record.event) {
                imported_entries.insert(record.id, source);
            }
            import = None;
        }
        if let Event::ContextEdited { target, .. } = &record.event {
            edited.insert(*target);
        }
    }
    let mut extra_messages = branch
        .into_iter()
        .filter(|record| display_only(record, compaction))
        .filter_map(|record| {
            let value = match &record.event {
                Event::SystemCheckpoint {
                    content,
                    sections,
                    tools_added,
                    tools_removed,
                } => imported_entries
                    .get(&record.id)
                    .and_then(|source| source.get("message"))
                    .map(|message| normalize_system_message(message, record))
                    .unwrap_or_else(|| {
                        system_value(record, content, sections, tools_added, tools_removed)
                    }),
                _ => shell_message(record)?,
            };
            Some((record.id, value))
        })
        .peekable();
    let mut visible = Vec::with_capacity(entries.len());
    let mut native = Vec::with_capacity(entries.len());
    for (id, message) in entries {
        let Some(record) = session.records().get(id.saturating_sub(1) as usize) else {
            continue;
        };
        // Keep display-only messages out of an assistant/tool batch.
        if !matches!(message, Message::Tool { .. })
            && !matches!(record.event, Event::Compacted { .. })
        {
            while extra_messages
                .peek()
                .is_some_and(|(source, _)| *source < id)
            {
                visible.extend(extra_messages.next().map(|(_, value)| value));
            }
        }
        let timestamp = record.timestamp_millis().unwrap_or_default();
        let value = if let Event::Compacted {
            summary,
            system_prompt,
            ..
        } = &record.event
        {
            if let Some(system) = system_prompt {
                let value = imported_entries
                    .get(&id)
                    .and_then(|source| source.get("systemMessage"))
                    .map(|message| normalize_system_message(message, record))
                    .unwrap_or_else(|| {
                        system_value(
                            record,
                            &system.content,
                            &system.sections,
                            &system.tools,
                            &[],
                        )
                    });
                visible.push(value);
            }
            json!({"role":"compactionSummary","summary":summary,"tokensBefore":compaction_tokens(session, record),"timestamp":timestamp})
        } else {
            projection.message(session.records(), record, &message, edited.contains(&id))
        };
        let value = imported_entries
            .get(&id)
            .filter(|source| source["type"] != "compaction")
            .and_then(|source| imported_message(source, &value, timestamp, edited.contains(&id)))
            .unwrap_or(value);
        visible.push(value);
        native.push(message);
    }
    visible.extend(extra_messages.map(|(_, value)| value));
    (visible, native)
}

fn normalize_system_message(message: &Value, record: &Record) -> Value {
    let mut value = message.clone();
    value["role"] = json!("system");
    if value["content"].is_null() {
        value["content"] = json!("");
    }
    if value["timestamp"].is_null() {
        value["timestamp"] = json!(record.timestamp_millis().unwrap_or_default());
    }
    value
}

fn system_value(
    record: &Record,
    content: &str,
    sections: &[(String, Option<String>)],
    added: &[pi_rs::ToolSpec],
    removed: &[String],
) -> Value {
    let mut value = json!({"role":"system","content":content,"timestamp":record.timestamp_millis().unwrap_or_default()});
    if !sections.is_empty() {
        value["sections"] = Value::Object(
            sections
                .iter()
                .map(|(name, text)| (name.clone(), json!(text)))
                .collect(),
        );
    }
    if !added.is_empty() {
        value["toolsAdded"] = json!(added);
    }
    if !removed.is_empty() {
        value["toolsRemoved"] =
            Value::Array(removed.iter().map(|name| json!({"name":name})).collect());
    }
    value
}

#[derive(Default)]
pub(super) struct Projection {
    requests: HashMap<Uuid, u64>,
    finished: HashMap<Uuid, u64>,
    models: HashMap<u64, u64>,
    request_identities: HashMap<u64, u64>,
    tools: HashMap<Uuid, (u64, usize)>,
}

impl Projection {
    pub(super) fn new<'a>(records: impl IntoIterator<Item = &'a Record>) -> Self {
        let mut projection = Self::default();
        for record in records {
            projection.record(record);
        }
        projection
    }

    pub(super) fn record(&mut self, record: &Record) {
        let identity = match &record.event {
            Event::SourceEntry { format, .. } if format == "pi-rs:request-identity" => {
                Some(record.id)
            }
            _ => record
                .parent
                .and_then(|parent| self.request_identities.get(&parent).copied()),
        };
        if let Some(identity) = identity {
            self.request_identities.insert(record.id, identity);
        }
        let selected = if matches!(record.event, Event::ModelSelected { .. }) {
            Some(record.id)
        } else {
            record
                .parent
                .and_then(|parent| self.models.get(&parent).copied())
        };
        if let Some(selected) = selected {
            self.models.insert(record.id, selected);
        }
        match &record.event {
            Event::ModelRequested {
                request_id: Some(id),
                ..
            } => {
                self.requests.insert(*id, record.id);
            }
            Event::ModelFinished { request_id, .. } => {
                self.finished.insert(*request_id, record.id);
            }
            Event::Assistant { invocations, .. } => {
                self.tools.extend(
                    invocations
                        .iter()
                        .enumerate()
                        .map(|(index, invocation)| (invocation.operation_id, (record.id, index))),
                );
            }
            _ => {}
        }
    }

    fn selected<'a>(&self, records: &'a [Record], id: u64) -> Option<ModelIdentity<'a>> {
        let model = self.models.get(&id)?;
        let Event::ModelSelected {
            model, provider, ..
        } = &records[*model as usize - 1].event
        else {
            return None;
        };
        Some(ModelIdentity {
            model,
            provider: provider.as_deref().unwrap_or("ollama"),
        })
    }

    fn requested<'a>(&self, records: &'a [Record], id: Uuid) -> Option<ModelIdentity<'a>> {
        let request = *self.requests.get(&id)?;
        let Event::ModelRequested { model, .. } = &records[request as usize - 1].event else {
            return None;
        };
        let recorded_provider = self.request_provider(records, id);
        Some(ModelIdentity {
            model,
            provider: recorded_provider
                .or_else(|| {
                    self.selected(records, request)
                        .filter(|selected| {
                            selected.model == model && Provider::named(selected.provider).is_none()
                        })
                        .map(|selected| selected.provider)
                })
                .or_else(|| {
                    self.finished(records, id)
                        .and_then(|finished| finished.metadata.resolved_model.as_ref())
                        .map(|identity| identity.provider.as_str())
                })
                .or_else(|| self.selected(records, request).map(|model| model.provider))
                .unwrap_or("ollama"),
        })
    }

    fn request_provider<'a>(&self, records: &'a [Record], id: Uuid) -> Option<&'a str> {
        let request = *self.requests.get(&id)?;
        let Event::ModelRequested { model, .. } = &records[request as usize - 1].event else {
            return None;
        };
        self.request_identities
            .get(&request)
            .and_then(|id| records.get(*id as usize - 1))
            .and_then(|record| match &record.event {
                Event::SourceEntry { entry, .. } if entry["model"].as_str() == Some(model) => {
                    entry["provider"].as_str()
                }
                _ => None,
            })
    }

    fn finished<'a>(&self, records: &'a [Record], id: Uuid) -> Option<Finished<'a>> {
        let request = self.finished.get(&id)?;
        let Event::ModelFinished {
            metadata, usage, ..
        } = &records[*request as usize - 1].event
        else {
            return None;
        };
        let usage = match usage {
            UsageReport::Measured { usage } => Some(*usage),
            _ => None,
        };
        Some(Finished { metadata, usage })
    }

    fn invocation<'a>(&self, records: &'a [Record], id: &Uuid) -> Option<&'a Invocation> {
        let (record, index) = self.tools.get(id)?;
        let Event::Assistant { invocations, .. } = &records[*record as usize - 1].event else {
            return None;
        };
        invocations.get(*index)
    }

    pub(super) fn raw_message(&self, records: &[Record], record: &Record) -> Option<Value> {
        let message = match &record.event {
            Event::User { content, .. } => Message::User {
                content: content.clone(),
            },
            Event::UserWithImages {
                content, images, ..
            } => Message::UserWithImages {
                content: content.clone(),
                images: images.clone(),
            },
            Event::CompactionSummaryMessage { summary, .. } => Message::User {
                content: summary.clone(),
            },
            Event::Assistant {
                content,
                invocations,
                ..
            } => Message::Assistant {
                content: content.clone(),
                calls: invocations
                    .iter()
                    .map(|invocation| invocation.call.clone())
                    .collect(),
                replay: None,
            },
            Event::ToolFinished {
                operation_id,
                outcome,
                ..
            }
            | Event::ToolReconciled {
                operation_id,
                outcome,
            } => Message::Tool {
                call_id: self.invocation(records, operation_id)?.call.id.clone(),
                content: outcome.tool_content(),
                is_error: outcome.is_error(),
                is_structured: Some(outcome.is_structured()),
            },
            Event::BashExecution { .. } => return shell_message(record),
            Event::SystemCheckpoint { content, .. } => Message::System {
                content: content.clone(),
            },
            _ => return None,
        };
        Some(self.message(records, record, &message, false))
    }

    fn message(
        &self,
        records: &[Record],
        record: &Record,
        message: &Message,
        edited: bool,
    ) -> Value {
        let timestamp = record.timestamp_millis().unwrap_or_default();
        if let Event::CustomMessage { message: custom } = &record.event {
            let content = if edited {
                match message {
                    Message::User { content } => json!(content),
                    Message::UserWithImages { content, images } => {
                        let mut blocks = Vec::new();
                        if !content.is_empty() {
                            blocks.push(json!({"type":"text", "text":content}));
                        }
                        blocks.extend(images.iter().map(|image| json!({"type":"image", "mimeType":image.mime_type, "data":image.data})));
                        json!(blocks)
                    }
                    _ => json!(custom.content),
                }
            } else {
                json!(custom.content)
            };
            let mut value = json!({"role":"custom", "customType":custom.custom_type, "content":content, "display":custom.display, "timestamp":timestamp});
            if let Some(details) = &custom.details {
                value["details"] = details.clone();
            }
            return value;
        }
        match message {
            Message::System { content } => {
                json!({"role":"system","content":content,"timestamp":timestamp})
            }
            Message::User { content } => match &record.event {
                Event::BashExecution { .. } => shell_message(record).expect("shell record"),
                Event::BranchSummary { summary, from, .. } => {
                    json!({"role":"branchSummary","summary":summary,"fromId":from.map(|id| id.to_string()),"timestamp":timestamp})
                }
                Event::CompactionSummaryMessage {
                    summary,
                    tokens_before,
                } => {
                    json!({"role":"compactionSummary","summary":summary,"tokensBefore":tokens_before,"timestamp":timestamp})
                }
                _ => json!({"role":"user","content":content,"timestamp":timestamp}),
            },
            Message::UserWithImages { content, images } => {
                let mut blocks = Vec::new();
                if !content.is_empty() {
                    blocks.push(json!({"type":"text","text":content}));
                }
                blocks.extend(images.iter().map(|image| {
                    json!({
                        "type":"image","mimeType":image.mime_type,"data":image.data
                    })
                }));
                json!({"role":"user","content":blocks,"timestamp":timestamp})
            }
            Message::Assistant {
                content,
                calls,
                replay,
            } => {
                let (request, usage) = match &record.event {
                    Event::Assistant {
                        request_id, usage, ..
                    } => (*request_id, *usage),
                    _ => (None, None),
                };
                let finalized = request.and_then(|request| self.finished(records, request));
                let model = request
                    .and_then(|request| self.requested(records, request))
                    .or_else(|| self.selected(records, record.id));
                let replay_route = replay.as_ref().and_then(|replay| {
                    let (api, rest) = replay.identity.provider.split_once(':')?;
                    let (provider, _) = rest.split_once(':')?;
                    matches!(api, "anthropic-messages" | "google-generative-ai")
                        .then_some((api, provider))
                });
                let provider = replay_route
                    .map(|(_, provider)| provider)
                    .or_else(|| {
                        replay
                            .as_ref()
                            .map(|replay| replay.identity.provider.as_str())
                    })
                    .or_else(|| {
                        finalized.and_then(|entry| {
                            entry
                                .metadata
                                .resolved_model
                                .as_ref()
                                .map(|identity| identity.provider.as_str())
                        })
                    })
                    .or_else(|| model.map(|model| model.provider))
                    .unwrap_or("ollama");
                let provider = model
                    .filter(|selected| Provider::named(selected.provider).is_none())
                    .map_or(provider, |selected| selected.provider);
                let provider = request
                    .and_then(|id| self.request_provider(records, id))
                    .unwrap_or(provider);
                let api = finalized
                    .and_then(|entry| entry.metadata.provider_api.map(pi_rs::ModelApi::as_str))
                    .or_else(|| replay_route.map(|(api, _)| api))
                    .unwrap_or_else(|| {
                        Provider::named(provider)
                            .or_else(|| {
                                replay
                                    .as_ref()
                                    .and_then(|replay| Provider::named(&replay.identity.provider))
                            })
                            .or_else(|| {
                                finalized
                                    .and_then(|entry| entry.metadata.resolved_model.as_ref())
                                    .and_then(|identity| Provider::named(&identity.provider))
                            })
                            .map_or_else(
                                || {
                                    if replay
                                        .as_deref()
                                        .is_some_and(super::pi_messages::is_pi_messages_replay)
                                    {
                                        "pi-messages"
                                    } else {
                                        "openai-completions"
                                    }
                                },
                                |provider| provider.api(),
                            )
                    });
                let model_id = model
                    .filter(|model| !model.model.is_empty())
                    .map(|model| model.model)
                    .or_else(|| replay.as_ref().map(|replay| replay.identity.model.as_str()))
                    .unwrap_or("");
                let mut blocks = Vec::new();
                if let Some(reasoning) =
                    finalized.and_then(|entry| entry.metadata.reasoning.as_ref())
                    && !edited
                {
                    blocks.push(json!({"type":"thinking","thinking":reasoning}));
                }
                if !content.is_empty() {
                    blocks.push(json!({"type":"text","text":content}));
                }
                blocks.extend(calls.iter().map(super::rpc_events::tool_call_value));
                let stop = if finalized
                    .is_some_and(|entry| entry.metadata.finish_reason.as_deref() == Some("length"))
                {
                    "length"
                } else if calls.is_empty() {
                    "stop"
                } else {
                    "toolUse"
                };
                let empty = ModelMetadata::default();
                let metadata = finalized.map_or(&empty, |entry| entry.metadata);
                let usage = finalized
                    .and_then(|entry| entry.usage)
                    .or(usage)
                    .unwrap_or_default();
                let mut value = json!({
                    "role":"assistant","content":blocks,"provider":provider,
                    "api":api,
                    "model":model_id,"timestamp":timestamp,"stopReason":stop,
                    "usage":super::rpc_events::usage_value(usage, metadata)
                });
                if let Some(id) = &metadata.provider_request_id {
                    value["responseId"] = json!(id);
                }
                if let Some(identity) = &metadata.resolved_model {
                    value["responseModel"] = json!(identity.model);
                }
                if !metadata.diagnostics.is_empty() {
                    value["diagnostics"] = json!(metadata.diagnostics);
                }
                value
            }
            Message::Tool {
                call_id,
                content,
                is_error,
                is_structured,
            } => {
                let operation = match &record.event {
                    Event::ToolFinished {
                        operation_id,
                        outcome,
                        ..
                    }
                    | Event::ToolReconciled {
                        operation_id,
                        outcome,
                    } => Some((operation_id, outcome)),
                    _ => None,
                };
                let name = operation
                    .and_then(|(id, _)| self.invocation(records, id))
                    .map_or("", |invocation| invocation.call.name.as_str());
                let outcome = operation.map(|(_, outcome)| outcome);
                let mut value =
                    super::rpc_events::tool_result_value(content, outcome, *is_structured);
                value["role"] = json!("toolResult");
                value["toolCallId"] = json!(call_id);
                value["toolName"] = json!(name);
                value["timestamp"] = json!(timestamp);
                value["isError"] = json!(outcome.map_or(*is_error, super::rpc_events::tool_failed));
                value
            }
        }
    }
}

fn matches_imported_message(source: &Value, event: &Event) -> bool {
    let role = source["message"]["role"].as_str();
    match event {
        Event::User { .. } | Event::UserWithImages { .. } => {
            source["type"] == "custom_message"
                || matches!(
                    role,
                    Some("user" | "custom" | "hookMessage" | "branchSummary" | "compactionSummary")
                )
        }
        Event::SystemCheckpoint { .. } => role == Some("system"),
        Event::CustomMessage { .. } => {
            matches!(role, Some("custom" | "hookMessage")) || source["type"] == "custom_message"
        }
        Event::Compacted { .. } => source["type"] == "compaction",
        Event::BranchSummary { .. } => role == Some("branchSummary"),
        Event::CompactionSummaryMessage { .. } => role == Some("compactionSummary"),
        Event::Assistant { .. } => role == Some("assistant"),
        Event::ToolFinished { .. } | Event::ToolReconciled { .. } => role == Some("toolResult"),
        _ => false,
    }
}

fn imported_message(
    source: &Value,
    projected: &Value,
    timestamp: i64,
    edited: bool,
) -> Option<Value> {
    let mut message = if source["type"] == "custom_message" {
        let mut custom = json!({
            "role":"custom","customType":source["customType"],
            "content":source["content"],"display":source["display"],"timestamp":timestamp
        });
        if let Some(details) = source.get("details") {
            custom["details"] = details.clone();
        }
        custom
    } else {
        source.get("message")?.clone()
    };
    if edited {
        message["content"] = projected["content"].clone();
        if message["role"] == "assistant" {
            message["stopReason"] = projected["stopReason"].clone();
        }
    }
    Some(message)
}

fn shell_message(record: &Record) -> Option<Value> {
    let Event::BashExecution { execution } = &record.event else {
        return None;
    };
    let mut value = serde_json::to_value(execution).ok()?;
    value["role"] = json!("bashExecution");
    value["timestamp"] = json!(record.timestamp_millis().unwrap_or_default());
    Some(value)
}

pub(super) fn compaction_tokens(session: &Session, record: &Record) -> u64 {
    if let Event::Compacted {
        tokens_before: Some(tokens),
        ..
    } = &record.event
    {
        return *tokens;
    }
    let Some(parent) = record.parent else {
        return 0;
    };
    let source = |id: u64| session.records().get(id.checked_sub(1)? as usize);
    let previous = source(parent);
    let imported = match previous {
        Some(Record {
            event: Event::ModelFinished { .. },
            parent: Some(source_id),
            ..
        }) => source(*source_id),
        record => record,
    };
    if let Some(Record {
        event: Event::SourceEntry { entry, .. },
        ..
    }) = imported
        && entry["type"] == "compaction"
        && let Some(tokens) = entry["tokensBefore"].as_u64()
    {
        return tokens;
    }
    session.conversation_at(parent).map_or(0, |messages| {
        pi_rs::estimate_request_tokens(&ModelRequest {
            model: String::new(),
            messages,
            tools: Vec::new(),
            max_output_tokens: 0,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn markerless_custom_provider_keeps_logical_identity() {
        let mut session = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        session
            .select_provider_model(Some("relay"), "selected-model", None)
            .unwrap();
        let mut records = session.records().to_vec();
        let request_id = Uuid::new_v4();
        for event in [
            Event::ModelRequested {
                model: "selected-model".into(),
                messages: 0,
                tools: 0,
                max_output_tokens: 128,
                request_id: Some(request_id),
                context_head: None,
                context_sha256: None,
                context_transformed: None,
                context_configuration_sha256: None,
            },
            Event::ModelFinished {
                request_id,
                metadata: ModelMetadata {
                    resolved_model: Some(pi_rs::ResolvedModelIdentity {
                        provider: "anthropic".into(),
                        model: "physical-model".into(),
                    }),
                    ..Default::default()
                },
                usage: UsageReport::Unconfirmed,
                failure: None,
                partial: String::new(),
                reply: None,
            },
            Event::Assistant {
                content: "answer".into(),
                invocations: Vec::new(),
                usage: None,
                request_id: Some(request_id),
            },
        ] {
            let parent = records.last().unwrap().id;
            records.push(Record {
                id: parent + 1,
                parent: Some(parent),
                version: 2,
                timestamp: None,
                run_id: None,
                event,
            });
        }
        let session = Session::from_records(records).unwrap();
        let messages = snapshot(&session).0;
        let assistant = messages
            .iter()
            .find(|message| message["role"] == "assistant")
            .unwrap();
        assert_eq!(assistant["provider"], "relay");
        assert_eq!(assistant["model"], "selected-model");
        assert_eq!(assistant["responseModel"], "physical-model");
    }

    #[test]
    fn rpc_projects_system_updates_and_the_active_compaction_snapshot() {
        let mut fixture = tempfile::NamedTempFile::new().unwrap();
        for entry in [
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"u","parentId":null,"message":{"role":"user","content":"kept"}}),
            json!({"type":"message","id":"s","parentId":"u","message":{"role":"system","content":"earlier","timestamp":7}}),
            json!({"type":"compaction","id":"c","parentId":"s","firstKeptEntryId":"u","summary":"summary","systemMessage":{
                "role":"system","content":[{"type":"text","text":"saved"}],"timestamp":42,
                "toolsAdded":[{"name":"read","description":"Read","parameters":{}}]
            }}),
            json!({"type":"message","id":"update","parentId":"c","message":{"role":"system","content":null,"sections":{"rules":"updated"}}}),
            json!({"type":"message","id":"next","parentId":"update","message":{"role":"user","content":"next"}}),
        ] {
            writeln!(fixture, "{entry}").unwrap();
        }
        let mut session = super::super::session_import::read(fixture.path()).unwrap();
        let (visible, native) = snapshot(&session);
        let roles = |messages: &[Value]| {
            messages
                .iter()
                .map(|value| value["role"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            roles(&visible),
            ["system", "compactionSummary", "user", "system", "user"]
        );
        assert_eq!(visible[0]["content"][0]["text"], "saved");
        assert_eq!(visible[0]["timestamp"], 42);
        assert_eq!(visible[0]["toolsAdded"][0]["name"], "read");
        assert_eq!(visible[3]["content"], "");
        assert_eq!(visible[3]["sections"]["rules"], "updated");
        assert_eq!(native.len(), 3);
        assert_eq!(count(&session), visible.len());

        let previous = session
            .records()
            .iter()
            .find(|record| matches!(record.event, Event::SystemCheckpoint { .. }))
            .unwrap()
            .id;
        session.checkout(previous).unwrap();
        let (visible, _) = snapshot(&session);
        assert_eq!(roles(&visible), ["user", "system"]);
        assert_eq!(visible[1]["content"], "earlier");
        assert_eq!(count(&session), visible.len());
    }
}
