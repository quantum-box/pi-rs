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

pub(super) fn count(session: &Session) -> usize {
    let branch = session.branch();
    let retain = branch
        .iter()
        .rev()
        .find_map(|record| match record.event {
            Event::Compacted { retain_from, .. } => Some(retain_from.unwrap_or(record.id)),
            _ => None,
        })
        .unwrap_or_default();
    session.current_message_entries().len() + branch.iter().filter(|record| {
        record.id >= retain && matches!(&record.event, Event::BashExecution { execution } if execution.exclude_from_context)
    }).count()
}

pub(super) fn snapshot(session: &Session) -> (Vec<Value>, Vec<Message>) {
    let branch = session.branch();
    let entries = session.current_message_entries();
    let projection = Projection::new(branch.iter().copied());
    let mut imported_entries = HashMap::new();
    let mut import = None;
    let mut edited = HashSet::new();
    let mut retained_from = 0;
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
        match &record.event {
            Event::ContextEdited { target, .. } => {
                edited.insert(*target);
            }
            Event::Compacted { retain_from, .. } => {
                retained_from = retain_from.unwrap_or(record.id)
            }
            _ => {}
        }
    }
    let mut excluded_shells = branch.into_iter().filter(|record| {
        record.id >= retained_from && matches!(&record.event, Event::BashExecution { execution } if execution.exclude_from_context)
    }).peekable();
    let mut visible = Vec::with_capacity(entries.len());
    let mut native = Vec::with_capacity(entries.len());
    for (id, message) in entries {
        let Some(record) = session.records().get(id.saturating_sub(1) as usize) else {
            continue;
        };
        // Keep display-only shell messages out of an assistant/tool batch.
        if !matches!(message, Message::Tool { .. })
            && !matches!(record.event, Event::Compacted { .. })
        {
            while excluded_shells.peek().is_some_and(|shell| shell.id < id) {
                if let Some(shell) = excluded_shells.next().and_then(shell_message) {
                    visible.push(shell);
                }
            }
        }
        let timestamp = record.timestamp_millis().unwrap_or_default();
        let value = if let Event::Compacted { summary, .. } = &record.event {
            json!({"role":"compactionSummary","summary":summary,"tokensBefore":compaction_tokens(session, record),"timestamp":timestamp})
        } else {
            projection.message(session.records(), record, &message, edited.contains(&id))
        };
        let value = imported_entries
            .get(&id)
            .and_then(|source| imported_message(source, &value, timestamp, edited.contains(&id)))
            .unwrap_or(value);
        visible.push(value);
        native.push(message);
    }
    visible.extend(excluded_shells.filter_map(shell_message));
    (visible, native)
}

#[derive(Default)]
pub(super) struct Projection {
    requests: HashMap<Uuid, u64>,
    finished: HashMap<Uuid, u64>,
    models: HashMap<u64, u64>,
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
        Some(ModelIdentity {
            model,
            provider: self
                .selected(records, request)
                .map_or("ollama", |model| model.provider),
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
        match message {
            Message::System { content } => {
                json!({"role":"system","content":content,"timestamp":timestamp})
            }
            Message::User { content } => match &record.event {
                Event::BashExecution { .. } => shell_message(record).expect("shell record"),
                Event::BranchSummary { summary, from, .. } => {
                    json!({"role":"branchSummary","summary":summary,"fromId":from.map(|id| id.to_string()),"timestamp":timestamp})
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
                value
            }
            Message::Tool { call_id, content } => {
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
                let mut value = super::rpc_events::tool_result_value(content, outcome);
                value["role"] = json!("toolResult");
                value["toolCallId"] = json!(call_id);
                value["toolName"] = json!(name);
                value["timestamp"] = json!(timestamp);
                value["isError"] = json!(outcome.is_some_and(super::rpc_events::tool_failed));
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
    let Some(parent) = record.parent else {
        return 0;
    };
    if let Some(Record {
        event: Event::SourceEntry { entry, .. },
        ..
    }) = session.records().get(parent.saturating_sub(1) as usize)
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
