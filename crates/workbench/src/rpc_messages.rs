//! Project the native session transcript into the Pi RPC message format.
mod anthropic;
use std::collections::{BTreeMap, HashMap, HashSet};

use pi_rs::{
    Event, Invocation, Message, ModelMetadata, ModelRequest, Record, Session, Usage, UsageReport,
};
use serde_json::{Value, json};
use uuid::Uuid;

use super::Provider;

#[derive(Clone, Copy)]
pub(super) enum MetadataSource<'a> {
    Session(&'a Session),
    Snapshot(&'a BTreeMap<u64, Value>),
}

impl<'a> MetadataSource<'a> {
    fn get(self, id: u64) -> Option<&'a Value> {
        match self {
            Self::Session(session) => session.record_metadata(id),
            Self::Snapshot(metadata) => metadata.get(&id),
        }
    }
}

#[derive(Clone, Copy)]
struct ModelIdentity<'a> {
    provider: &'a str,
    model: &'a str,
}

#[derive(Clone, Copy)]
struct Finished<'a> {
    metadata: &'a ModelMetadata,
    usage: Option<Usage>,
    failure: Option<pi_rs::ModelFailure>,
    record_id: u64,
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

fn additional_assistants<'a>(
    session: &'a Session,
    entries: &[(u64, Message)],
) -> impl Iterator<Item = pi_rs::AssistantTranscriptEntry<'a>> {
    let projected: HashSet<_> = entries.iter().map(|(id, _)| *id).collect();
    session
        .assistant_transcript()
        .into_iter()
        .filter(move |entry| {
            !projected.contains(&entry.record_id)
                && (entry.stop_reason == Some("stop")
                    || entry.is_edited()
                    || matches!(
                        session.records()[entry.record_id as usize - 1].event,
                        Event::ModelFinished { .. }
                    ))
        })
}

pub(super) fn count(session: &Session) -> usize {
    let branch = session.branch();
    let compaction = latest_compaction(&branch);
    let entries = session.current_message_entries();
    entries.len()
        + additional_assistants(session, &entries).count()
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

pub(super) fn snapshot(session: &Session) -> (Vec<Value>, Vec<Message>) {
    let branch = session.branch();
    let entries = session.current_message_entries();
    let mut projection = Projection::new(branch.iter().copied());
    let mut imported_entries = HashMap::new();
    let mut import = None;
    let mut edits = HashMap::new();
    let compaction = latest_compaction(&branch);
    let additional: HashMap<_, _> = additional_assistants(session, &entries)
        .map(|entry| (entry.record_id, entry))
        .collect();
    let mut has_imported_shell_error = false;
    for record in &branch {
        if let Event::SourceEntry { format, entry } = &record.event {
            import = (format == "pi").then_some(entry);
            has_imported_shell_error |= import.is_some_and(|entry| {
                let message = &entry["message"];
                message["role"] == "bashExecution" && message["fullOutputError"].is_string()
            });
        } else if let Some(source) = import
            && !matches!(record.event, Event::ModelFinished { .. })
        {
            if matches_imported_message(source, &record.event) {
                imported_entries.insert(record.id, source);
            }
            import = None;
        }
        if let Event::ContextEdited {
            target,
            replacement,
        } = &record.event
        {
            edits.insert(*target, replacement);
        }
    }
    if has_imported_shell_error {
        projection.verified_pi_conversions =
            session.validated_pi_conversion_counts().unwrap_or_default();
    }
    let mut extra_messages = branch
        .into_iter()
        .filter(|record| display_only(record, compaction) || additional.contains_key(&record.id))
        .filter_map(|record| {
            let value = match &record.event {
                Event::ModelFinished { .. } if additional.contains_key(&record.id) => {
                    let mut value = projection.turn_partial(session.records(), record)?;
                    if let Some(replacement) = edits.get(&record.id) {
                        match replacement {
                            None => return None,
                            Some(pi_rs::ContextContent::Text(text)) => {
                                value["content"] = json!([{"type":"text", "text":text}])
                            }
                            Some(pi_rs::ContextContent::Blocks(blocks)) => {
                                value["content"] = json!(blocks)
                            }
                        }
                    }
                    value
                }
                Event::Assistant { .. } => {
                    // Tool-pair repair can elide an empty reply from model
                    // context. Its original stop still matters in the transcript.
                    let edited = edits.contains_key(&record.id);
                    let message = Message::Assistant {
                        content: additional.get(&record.id)?.text().into_owned(),
                        calls: vec![],
                        replay: None,
                    };
                    let value = projection.message(
                        session.records(),
                        record,
                        &message,
                        edited,
                        Some(MetadataSource::Session(session)),
                    );
                    imported_entries
                        .get(&record.id)
                        .and_then(|source| {
                            let mut imported = imported_message(
                                source,
                                &value,
                                record.timestamp_millis().unwrap_or_default(),
                                edited,
                            )?;
                            // An elided reply has no retained calls. Preserve
                            // untouched text and thinking blocks verbatim.
                            if !edited && let Some(blocks) = imported["content"].as_array_mut() {
                                blocks.retain(|block| block["type"] != "toolCall");
                            }
                            Some(imported)
                        })
                        .unwrap_or(value)
                }
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
                _ => shell_message(
                    session.records(),
                    record,
                    Some(MetadataSource::Session(session)),
                    &projection.verified_pi_conversions,
                )?,
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
            projection.message(
                session.records(),
                record,
                &message,
                edits.contains_key(&id),
                Some(MetadataSource::Session(session)),
            )
        };
        let value = imported_entries
            .get(&id)
            .filter(|source| source["type"] != "compaction")
            .and_then(|source| imported_message(source, &value, timestamp, edits.contains_key(&id)))
            .unwrap_or(value);
        let mut value = value;
        if matches!(record.event, Event::ImportedUnattributedToolResult { .. }) {
            value["nativeUnattributed"] = json!(true);
        }
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

pub(super) fn system_value(
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
    verified_pi_conversions: BTreeMap<u64, u64>,
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
        let selected = if matches!(
            record.event,
            Event::ModelSelected { .. }
                | Event::BranchModelSelected { .. }
                | Event::BranchSummary {
                    fallback_model: Some(_),
                    ..
                }
        ) {
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
        match &records[*model as usize - 1].event {
            Event::ModelSelected {
                model, provider, ..
            } => Some(ModelIdentity {
                model,
                provider: provider.as_deref().unwrap_or("ollama"),
            }),
            Event::BranchModelSelected {
                model, provider, ..
            } => Some(ModelIdentity { model, provider }),
            Event::BranchSummary {
                fallback_model: Some(selection),
                ..
            } => Some(ModelIdentity {
                model: &selection.model,
                provider: &selection.provider,
            }),
            _ => None,
        }
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
            metadata,
            usage,
            failure,
            ..
        } = &records[*request as usize - 1].event
        else {
            return None;
        };
        let usage = match usage {
            UsageReport::Measured { usage } => Some(*usage),
            _ => None,
        };
        Some(Finished {
            metadata,
            usage,
            failure: *failure,
            record_id: *request,
        })
    }

    fn invocation<'a>(&self, records: &'a [Record], id: &Uuid) -> Option<&'a Invocation> {
        let (record, index) = self.tools.get(id)?;
        let Event::Assistant { invocations, .. } = &records[*record as usize - 1].event else {
            return None;
        };
        invocations.get(*index)
    }

    pub(super) fn turn_partial(&self, records: &[Record], record: &Record) -> Option<Value> {
        let failure = pi_rs::turn_partial_failure(records, record)?;
        let Event::ModelFinished {
            request_id,
            partial,
            ..
        } = &record.event
        else {
            return None;
        };
        let mut assistant_record = record.clone();
        assistant_record.event = Event::Assistant {
            content: partial.clone(),
            invocations: Vec::new(),
            usage: None,
            request_id: Some(*request_id),
        };
        let message = Message::Assistant {
            content: partial.clone(),
            calls: Vec::new(),
            replay: None,
        };
        let mut value = self.message(records, &assistant_record, &message, false, None);
        value["stopReason"] = json!(pi_rs::native_assistant_stop_reason(
            None,
            Some(failure),
            false
        ));
        Some(value)
    }

    pub(super) fn raw_message(
        &self,
        records: &[Record],
        record: &Record,
        metadata: Option<MetadataSource<'_>>,
    ) -> Option<Value> {
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
            Event::BashExecution { .. } => {
                return shell_message(records, record, metadata, &self.verified_pi_conversions);
            }
            Event::SystemCheckpoint { content, .. } => Message::System {
                content: content.clone(),
            },
            _ => return None,
        };
        Some(self.message(records, record, &message, false, metadata))
    }

    fn message(
        &self,
        records: &[Record],
        record: &Record,
        message: &Message,
        edited: bool,
        metadata: Option<MetadataSource<'_>>,
    ) -> Value {
        let timestamp = record.timestamp_millis().unwrap_or_default();
        if let Event::CustomMessage { message: custom }
        | Event::CustomMessageDelivered {
            message: custom, ..
        } = &record.event
        {
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
                Event::BashExecution { .. } => {
                    shell_message(records, record, metadata, &self.verified_pi_conversions)
                        .expect("shell record")
                }
                Event::BranchSummary { summary, from, .. } => {
                    json!({"role":"branchSummary","summary":summary,"fromId":from.map(|id| id.to_string()),"timestamp":timestamp})
                }
                Event::CompactionSummaryMessage {
                    summary,
                    tokens_before,
                } => {
                    json!({"role":"compactionSummary","summary":summary,"tokensBefore":tokens_before,"timestamp":timestamp})
                }
                _ => {
                    json!({"role":"user","content":super::rpc_events::user_content(content, &[]),"timestamp":timestamp})
                }
            },
            Message::UserWithImages { content, images } => {
                json!({"role":"user","content":super::rpc_events::user_content(content, images),"timestamp":timestamp})
            }
            Message::Assistant {
                content,
                calls,
                replay,
            } => {
                let (request, usage, has_calls) = match &record.event {
                    Event::Assistant {
                        request_id,
                        usage,
                        invocations,
                        ..
                    } => (*request_id, *usage, !invocations.is_empty()),
                    _ => (None, None, !calls.is_empty()),
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
                if !edited
                    && api == "anthropic-messages"
                    && let Some(projected) = replay.as_deref().and_then(|replay| {
                        anthropic::blocks(
                            replay,
                            content,
                            finalized.and_then(|entry| entry.metadata.reasoning.as_deref()),
                            calls,
                        )
                    })
                {
                    blocks = projected;
                }
                if !edited
                    && api == "anthropic-messages"
                    && let Some(projected) = finalized
                        .filter(|entry| entry.failure.is_none())
                        .and_then(|entry| {
                            metadata.and_then(|metadata| metadata.get(entry.record_id))
                        })
                        .and_then(|metadata| anthropic::display_blocks(metadata, content, calls))
                {
                    blocks = projected;
                }
                let stop = pi_rs::native_assistant_stop_reason(
                    finalized.and_then(|entry| entry.metadata.finish_reason.as_deref()),
                    finalized.and_then(|entry| entry.failure),
                    has_calls,
                );
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
                if !edited
                    && let Some(effort) = replay.as_deref().and_then(|replay| match api {
                        "openai-responses" => replay.openai_effort(),
                        "anthropic-messages" => replay.anthropic_effort(),
                        _ => None,
                    })
                {
                    value["providerThinkingLevel"] = json!(effort);
                }
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
                    super::rpc_events::tool_event_result_value(content, outcome, *is_structured);
                if matches!(
                    outcome,
                    Some(pi_rs::Outcome::TextResultWithStructuredContent { .. })
                ) {
                    value
                        .as_object_mut()
                        .expect("result object")
                        .remove("structuredContent");
                }
                value["role"] = json!("toolResult");
                value["toolCallId"] = json!(call_id);
                value["toolName"] = json!(name);
                value["timestamp"] = json!(timestamp);
                value["isError"] = json!(outcome.map_or(*is_error, super::rpc_events::tool_failed));
                if let Some(duration_ms) = super::rpc_events::tool_duration_ms(
                    metadata.and_then(|source| source.get(record.id)),
                ) {
                    value["durationMs"] = json!(duration_ms);
                }
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
        Event::CustomMessage { .. } | Event::CustomMessageDelivered { .. } => {
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
    use_projected_content: bool,
) -> Option<Value> {
    let mut message = if source["type"] == "custom_message" {
        let mut custom = json!({
            "role":"custom","customType":source["customType"],
            "content":source.get("content").filter(|value| !value.is_null()).cloned().unwrap_or_else(|| json!([])),
            "display":source["display"],"timestamp":timestamp
        });
        if let Some(details) = source.get("details") {
            custom["details"] = details.clone();
        }
        custom
    } else {
        source.get("message")?.clone()
    };
    if use_projected_content || (message["role"] == "assistant" && message["content"].is_null()) {
        message["content"] = projected["content"].clone();
    }
    Some(message)
}

fn shell_message(
    records: &[Record],
    record: &Record,
    metadata: Option<MetadataSource<'_>>,
    verified_pi_conversions: &BTreeMap<u64, u64>,
) -> Option<Value> {
    let Event::BashExecution { execution } = &record.event else {
        return None;
    };
    let mut value = serde_json::to_value(execution).ok()?;
    value["role"] = json!("bashExecution");
    value["timestamp"] = json!(record.timestamp_millis().unwrap_or_default());
    let error = metadata
        .and_then(|source| source.get(record.id))
        .and_then(|value| value["fullOutputError"].as_str())
        .or_else(|| {
            let parent = records.get(record.parent?.checked_sub(1)? as usize)?;
            // A Pi shell entry converts into exactly one adjacent record.
            // Later native children can share the same parent.
            if parent.id.checked_add(1) != Some(record.id)
                || verified_pi_conversions.get(&parent.id) != Some(&1)
            {
                return None;
            }
            match &parent.event {
                Event::SourceEntry { format, entry }
                    if format == "pi" && entry["message"]["role"] == "bashExecution" =>
                {
                    entry["message"]["fullOutputError"].as_str()
                }
                _ => None,
            }
        });
    if let Some(error) = execution.output_save_error(error) {
        value["fullOutputError"] = json!(error);
    }
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
    fn imported_shell_save_error_is_only_projected_on_its_verified_conversion() {
        let mut fixture = tempfile::NamedTempFile::new().unwrap();
        for entry in [
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{
                "role":"bashExecution","command":"fixture","output":"tail","exitCode":19,
                "truncated":true,"fullOutputError":"disk full"
            }}),
        ] {
            writeln!(fixture, "{entry}").unwrap();
        }
        let mut session = super::super::session_import::read(fixture.path()).unwrap();
        let imported = session.head();
        let source = session.records().last().unwrap().parent.unwrap();
        let (messages, _) = snapshot(&session);
        let result = messages
            .iter()
            .find(|message| message["role"] == "bashExecution")
            .unwrap();
        assert_eq!(result["output"], "tail");
        assert_eq!(result["exitCode"], 19);
        assert_eq!(result["fullOutputError"], "disk full");
        assert!(result["fullOutputPath"].is_null());

        session.checkout(source).unwrap();
        for exclude_from_context in [false, true] {
            session
                .append_local_bash_result(pi_rs::BashExecution {
                    command: "native fixture".into(),
                    output: "native tail".into(),
                    exit_code: Some(23),
                    cancelled: false,
                    truncated: true,
                    full_output_path: None,
                    exclude_from_context,
                })
                .unwrap();
            assert_eq!(session.records().last().unwrap().parent, Some(source));
            let (messages, _) = snapshot(&session);
            let [message] = messages.as_slice() else {
                panic!("expected only the native shell result, got {messages:?}");
            };
            assert_eq!(message["command"], "native fixture");
            assert_eq!(message["output"], "native tail");
            assert!(message.get("fullOutputError").is_none());
            session.checkout(source).unwrap();
        }

        session.checkout(imported).unwrap();
        let (messages, _) = snapshot(&session);
        assert_eq!(messages[0]["fullOutputError"], "disk full");
    }

    #[test]
    fn native_assistant_stop_is_preserved_when_an_edit_empties_the_reply() {
        for (has_calls, stop, selected) in [(false, "stop", None), (true, "toolUse", Some("older"))]
        {
            let initial = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
            let mut records = initial.records().to_vec();
            let mut append = |event| {
                let parent = records.last().unwrap();
                let id = parent.id + 1;
                records.push(Record {
                    id,
                    parent: Some(parent.id),
                    version: parent.version,
                    timestamp: None,
                    run_id: None,
                    event,
                });
                id
            };
            append(Event::Assistant {
                content: "older".into(),
                invocations: vec![],
                usage: None,
                request_id: None,
            });
            let request_id = Uuid::new_v4();
            let operation_id = Uuid::new_v4();
            append(Event::ModelFinished {
                request_id,
                metadata: ModelMetadata {
                    finish_reason: Some("vendor-complete".into()),
                    ..Default::default()
                },
                usage: UsageReport::Unconfirmed,
                failure: None,
                partial: String::new(),
                reply: None,
            });
            let target = append(Event::Assistant {
                content: if has_calls { "" } else { "newer" }.into(),
                invocations: has_calls
                    .then(|| Invocation {
                        operation_id,
                        call: pi_rs::ToolCall {
                            id: "call".into(),
                            name: "fixture".into(),
                            arguments: "{}".into(),
                        },
                    })
                    .into_iter()
                    .collect(),
                usage: None,
                request_id: Some(request_id),
            });
            if has_calls {
                append(Event::ToolStarted {
                    operation_id,
                    tool_execution: None,
                });
                append(Event::ToolFinished {
                    operation_id,
                    outcome: pi_rs::Outcome::Succeeded("result".into()),
                    states: Default::default(),
                });
            }
            let mut session = Session::from_records(records).unwrap();
            let (before, _) = snapshot(&session);
            assert_eq!(before[1]["stopReason"], stop);
            if has_calls {
                assert_eq!(before[1]["content"][0]["type"], "toolCall");
                assert_eq!(session.last_assistant_text().as_deref(), selected);
            }
            session
                .edit_context(target, Some(pi_rs::ContextContent::Text(" ".into())))
                .unwrap();
            assert_eq!(snapshot(&session).0[1]["stopReason"], stop);
            assert_eq!(session.last_assistant_text().as_deref(), selected);
            session
                .edit_context(target, Some(pi_rs::ContextContent::Blocks(vec![])))
                .unwrap();
            let (visible, native) = snapshot(&session);
            assert_eq!(native.len(), 1);
            assert_eq!(visible.len(), 2);
            assert_eq!(count(&session), visible.len());
            assert_eq!(visible[1]["stopReason"], stop);
            assert_eq!(visible[1]["content"], json!([]));
            assert_eq!(session.last_assistant_text().as_deref(), selected);
        }
    }

    #[test]
    fn imported_assistant_stop_remains_verbatim_after_an_edit() {
        let mut fixture = tempfile::NamedTempFile::new().unwrap();
        for entry in [
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"a","parentId":null,"message":{
                "role":"assistant","stopReason":"stop","content":[{"type":"text","text":"older"}]
            }}),
            json!({"type":"message","id":"b","parentId":"a","message":{
                "role":"assistant","stopReason":"vendor-complete","content":[{"type":"text","text":"newer"}]
            }}),
            json!({"type":"context_edit","id":"c","parentId":"b","targetId":"b","replacement":{"content":[]}}),
        ] {
            writeln!(fixture, "{entry}").unwrap();
        }
        let session = super::super::session_import::read(fixture.path()).unwrap();
        let (visible, native) = snapshot(&session);
        assert_eq!(native.len(), 1);
        assert_eq!(visible.len(), 2);
        assert_eq!(visible[1]["stopReason"], "vendor-complete");
        assert_eq!(visible[1]["content"], json!([]));
        assert_eq!(session.last_assistant_text().as_deref(), Some("older"));
    }

    #[test]
    fn imported_empty_stop_keeps_repaired_content_in_rpc() {
        let call = json!({"type":"toolCall","id":"call","name":"fixture","arguments":{}});
        let thinking = json!({"type":"thinking","thinking":"original","thinkingSignature":"fixture-signature"});
        for (content, expected) in [
            (Value::Null, json!([])),
            (json!([thinking, call]), json!([thinking])),
        ] {
            let mut fixture = tempfile::NamedTempFile::new().unwrap();
            let mut entries = vec![
                json!({"type":"session","version":3}),
                json!({"type":"message","id":"a","parentId":null,"message":{
                    "role":"assistant","stopReason":"stop","content":[{"type":"text","text":"older"}]
                }}),
                json!({"type":"message","id":"u","parentId":"a","message":{
                    "role":"user","content":"question"
                }}),
                json!({"type":"message","id":"b","parentId":"u","message":{
                    "role":"assistant","stopReason":"stop","content":content
                }}),
            ];
            if content.is_null() {
                entries.push(json!({"type":"context_edit","id":"e","parentId":"b","targetId":"u","replacement":null}));
            } else {
                entries.push(json!({"type":"message","id":"r","parentId":"b","message":{
                    "role":"toolResult","toolCallId":"call","toolName":"fixture","isError":false,
                    "content":[{"type":"text","text":"result"}]
                }}));
                entries.push(json!({"type":"context_edit","id":"e","parentId":"r","targetId":"r","replacement":null}));
            }
            for entry in &entries {
                writeln!(fixture, "{entry}").unwrap();
            }
            let session = super::super::session_import::read(fixture.path()).unwrap();
            let (visible, native) = snapshot(&session);
            assert_eq!(visible.len(), if content.is_null() { 2 } else { 3 });
            assert_eq!(count(&session), visible.len());
            assert_eq!(native.len() + 1, visible.len());
            assert_eq!(visible.last().unwrap()["stopReason"], "stop");
            assert_eq!(visible.last().unwrap()["content"], expected);
            assert_eq!(session.last_assistant_text(), None);
            assert!(session.records().iter().any(|record| matches!(
                &record.event, Event::SourceEntry { entry, .. } if entry == &entries[3]
            )));
        }
    }

    #[test]
    fn last_assistant_text_skips_nonfinal_messages_without_visible_text() {
        for (reason, content, expected) in [
            (
                "error",
                json!([{"type":"thinking","thinking":"diagnostic"},{"type":"text","text":" \n"}]),
                Some("Previous answer"),
            ),
            (
                "aborted",
                json!([{"type":"thinking","thinking":"diagnostic"}]),
                Some("Previous answer"),
            ),
            (
                "toolUse",
                json!([{"type":"toolCall","id":"one","name":"read","arguments":{"path":"example.txt"}}]),
                Some("Previous answer"),
            ),
            (
                "length",
                json!([{"type":"text","text":"\u{feff}\u{a0}"}]),
                Some("Previous answer"),
            ),
            (
                "aborted",
                json!([{"type":"text","text":" New "},{"type":"text","text":"partial "}]),
                Some("New partial"),
            ),
            ("stop", json!([]), None),
        ] {
            let mut fixture = tempfile::NamedTempFile::new().unwrap();
            writeln!(fixture, "{}", json!({"type":"session","version":3})).unwrap();
            for (id, parent, reason, blocks) in [
                (
                    "first",
                    None,
                    "stop",
                    json!([{"type":"text","text":"Previous answer"}]),
                ),
                ("latest", Some("first"), reason, content),
            ] {
                writeln!(
                    fixture,
                    "{}",
                    json!({
                        "type":"message","id":id,"parentId":parent,
                        "message":{"role":"assistant","content":blocks,"stopReason":reason}
                    })
                )
                .unwrap();
            }
            let session = super::super::session_import::read(fixture.path()).unwrap();
            assert_eq!(
                session.last_assistant_text().as_deref(),
                expected,
                "{reason}"
            );
        }
    }

    #[test]
    fn failed_and_cancelled_turn_partials_are_display_only_and_not_duplicated() {
        assert_turn_partial(
            pi_rs::ModelFailure::Cancelled,
            "pi-rs:aborted-turn-partial",
            "aborted",
        );
        assert_turn_partial(
            pi_rs::ModelFailure::Failed,
            "pi-rs:failed-turn-partial",
            "error",
        );
        assert_turn_partial(
            pi_rs::ModelFailure::Retryable,
            "pi-rs:failed-turn-partial",
            "error",
        );
    }

    #[test]
    fn superseded_partial_round_trips_without_changing_model_context() {
        use super::super::rpc_entries::Entries;

        let mut initial = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        initial
            .append_user_with_images("first".into(), vec![], None)
            .unwrap();
        let mut records = initial.records().to_vec();
        let mut append = |event| {
            let parent = records.last().unwrap();
            records.push(Record {
                id: parent.id + 1,
                parent: Some(parent.id),
                version: parent.version,
                timestamp: None,
                run_id: None,
                event,
            });
        };
        append(Event::Assistant {
            content: String::new(),
            invocations: vec![],
            usage: None,
            request_id: None,
        });
        let operation_id = Uuid::new_v4();
        append(Event::Assistant {
            content: String::new(),
            invocations: vec![Invocation {
                operation_id,
                call: pi_rs::ToolCall {
                    id: "fixture-call".into(),
                    name: "fixture".into(),
                    arguments: "{}".into(),
                },
            }],
            usage: None,
            request_id: None,
        });
        append(Event::ToolStarted {
            operation_id,
            tool_execution: None,
        });
        append(Event::ToolFinished {
            operation_id,
            outcome: pi_rs::Outcome::Succeeded("fixture result".into()),
            states: Default::default(),
        });
        append(Event::Assistant {
            content: "measured answer".into(),
            invocations: vec![],
            usage: Some(Usage {
                input_tokens: 83,
                output_tokens: 7,
            }),
            request_id: None,
        });
        append(Event::User {
            content: "second".into(),
            input_id: None,
        });
        let request_id = Uuid::new_v4();
        append(Event::SourceEntry {
            format: "pi-rs:failed-turn-partial".into(),
            entry: json!({"requestId":request_id}),
        });
        append(Event::ModelFinished {
            request_id,
            metadata: ModelMetadata::default(),
            usage: UsageReport::Unconfirmed,
            failure: Some(pi_rs::ModelFailure::Retryable),
            partial: "partial answer".into(),
            reply: None,
        });
        let mut explicit = Session::from_records(records.clone()).unwrap();
        let partial = explicit.head();
        explicit.edit_context(partial, None).unwrap();
        records.push(Record {
            id: partial + 1,
            parent: Some(partial),
            version: records.last().unwrap().version,
            timestamp: None,
            run_id: None,
            event: Event::SourceEntry {
                format: "pi-rs:retry-scheduled".into(),
                entry: json!({"requestId":request_id}),
            },
        });
        let mut other_run = records.last().unwrap().clone();
        other_run.run_id = Some(Uuid::nil());
        assert_eq!(pi_rs::superseded_turn_partial(&records, &other_run), None);
        let scheduled = Session::from_records(records).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let round_trip = |session: &Session, superseded: bool| {
            let path = directory.path().join("share.jsonl");
            let body = Entries::new(session)
                .write_share_branch(session, directory.path())
                .unwrap();
            let edit: Value = serde_json::from_slice(
                body.trim_ascii_end()
                    .rsplit(|byte| *byte == b'\n')
                    .next()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(edit["type"], "context_edit");
            assert!(edit["replacement"].is_null());
            assert_eq!(
                edit.get(pi_rs::pi_import::SUPERSEDED_PARTIAL_FIELD),
                superseded.then_some(&Value::Bool(true))
            );
            std::fs::write(&path, body).unwrap();
            super::super::session_import::read(&path).unwrap()
        };

        let expected = snapshot(&scheduled).1;
        let mut imported = round_trip(&scheduled, true);
        for _ in 0..2 {
            let (visible, native) = snapshot(&imported);
            assert_eq!(native, expected);
            assert_eq!(native.len(), 6);
            assert!(
                matches!(&native[1], Message::Assistant { content, calls, .. } if content.is_empty() && calls.is_empty())
            );
            assert!(
                matches!(&native[3], Message::Tool { call_id, .. } if call_id == "fixture-call")
            );
            assert!(
                !visible
                    .iter()
                    .any(|message| message["stopReason"] == "error")
            );
            assert_eq!(
                imported.last_assistant_text().as_deref(),
                Some("measured answer")
            );
            let estimate = imported.context_usage_estimate();
            assert_eq!(estimate.usage_tokens, 90);
            assert!(estimate.last_usage_record.is_some());
            imported = round_trip(&imported, true);
        }

        let imported = round_trip(&explicit, false);
        assert_eq!(
            imported.last_assistant_text().as_deref(),
            Some("measured answer")
        );
        assert_eq!(imported.context_usage_estimate().last_usage_record, None);
        assert_eq!(snapshot(&imported).1.len(), 5);

        let bytes = Entries::new(&scheduled)
            .write_share_branch(&scheduled, directory.path())
            .unwrap();
        let mut entries: Vec<Value> = bytes
            .trim_ascii_end()
            .split(|byte| *byte == b'\n')
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        let measured = entries
            .iter()
            .find(|entry| entry["message"]["usage"]["input"] == 83)
            .unwrap()["id"]
            .clone();
        entries.last_mut().unwrap()["targetId"] = measured;
        let path = directory.path().join("flagged-edit-to-success.jsonl");
        let mut file = std::fs::File::create(&path).unwrap();
        for entry in entries {
            writeln!(file, "{entry}").unwrap();
        }
        let imported = super::super::session_import::read(&path).unwrap();
        assert_eq!(imported.context_usage_estimate().last_usage_record, None);
        assert_eq!(
            imported.last_assistant_text().as_deref(),
            Some("partial answer")
        );
    }

    #[test]
    fn superseded_partial_import_requires_nearest_completion_on_its_branch() {
        let reply = |id, parent, stop, text| {
            json!({"type":"message","id":id,"parentId":parent,"message":{
                "role":"assistant","stopReason":stop,"content":[{"type":"text","text":text}]
            }})
        };
        let mut measured = reply("measured", "early", "stop", "measured answer");
        measured["message"]["usage"] = json!({"input":83,"output":7});
        let history = [
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"prompt","parentId":null,"message":{
                "role":"user","content":"question"
            }}),
            reply("early", "prompt", "error", "older partial"),
            measured,
            reply("retry", "measured", "error", "retry partial"),
            json!({"type":"label","id":"label","parentId":"retry","targetId":"retry","label":"note"}),
            reply("sibling", "measured", "error", "sibling partial"),
        ];
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.jsonl");
        for (parent, target, replacement, expected_usage, expected_assistants) in [
            (
                "label",
                "retry",
                Some(Value::Null),
                90,
                &["older partial", "measured answer"][..],
            ),
            (
                "measured",
                "early",
                Some(Value::Null),
                0,
                &["measured answer"],
            ),
            (
                "label",
                "sibling",
                Some(Value::Null),
                0,
                &["older partial", "measured answer", "retry partial"],
            ),
            (
                "label",
                "retry",
                None,
                0,
                &["older partial", "measured answer"],
            ),
        ] {
            let mut edit = json!({"type":"context_edit","id":"edit","parentId":parent,"targetId":target,
                (pi_rs::pi_import::SUPERSEDED_PARTIAL_FIELD):true});
            if let Some(replacement) = replacement {
                edit["replacement"] = replacement;
            }
            let mut file = std::fs::File::create(&path).unwrap();
            for entry in history.iter().chain(std::iter::once(&edit)) {
                writeln!(file, "{entry}").unwrap();
            }
            let imported = super::super::session_import::read(&path).unwrap();
            let estimate = imported.context_usage_estimate();
            assert_eq!(estimate.usage_tokens, expected_usage, "{edit}");
            assert_eq!(
                estimate.last_usage_record.is_some(),
                expected_usage > 0,
                "{edit}"
            );
            let assistants: Vec<_> = imported
                .current_messages()
                .into_iter()
                .filter_map(|message| match message {
                    Message::Assistant { content, .. } => Some(content),
                    _ => None,
                })
                .collect();
            assert_eq!(assistants, expected_assistants, "{edit}");
        }
    }

    #[test]
    fn repeated_superseded_partial_edits_preserve_the_measured_anchor() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("repeated-edits.jsonl");
        let mut file = std::fs::File::create(&path).unwrap();
        for entry in [
            json!({"type":"session","version":3}),
            json!({"type":"message","id":"prompt","parentId":null,
                "message":{"role":"user","content":"question"}}),
            json!({"type":"message","id":"measured","parentId":"prompt","message":{
                "role":"assistant","stopReason":"stop","usage":{"input":83,"output":7},
                "content":[{"type":"text","text":"measured answer"}]
            }}),
            json!({"type":"message","id":"partial","parentId":"measured","message":{
                "role":"assistant","stopReason":"error",
                "content":[{"type":"text","text":"retry partial"}]
            }}),
        ] {
            writeln!(file, "{entry}").unwrap();
        }
        let mut parent = "partial".to_owned();
        for index in 0..4_000 {
            let id = format!("edit{index}");
            let edit = json!({"type":"context_edit","id":id,"parentId":parent,
                "targetId":"partial","replacement":null,
                (pi_rs::pi_import::SUPERSEDED_PARTIAL_FIELD):true});
            writeln!(file, "{edit}").unwrap();
            parent = id;
        }
        drop(file);
        let imported = super::super::session_import::read(&path).unwrap();
        let estimate = imported.context_usage_estimate();
        assert_eq!(estimate.usage_tokens, 90);
        assert!(estimate.last_usage_record.is_some());
        assert_eq!(
            imported.last_assistant_text().as_deref(),
            Some("measured answer")
        );
        assert_eq!(imported.current_messages().len(), 2);
    }

    #[test]
    fn repeated_native_omissions_follow_the_parent_branch_and_run() {
        use super::super::rpc_entries::Entries;

        let initial = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        let mut records = initial.records().to_vec();
        let run = Uuid::from_u128(1);
        let other_run = Uuid::from_u128(2);
        let request = Uuid::from_u128(3);
        let newer_request = Uuid::from_u128(4);
        let mut append = |run_id, event| {
            let last = records.last().unwrap();
            let id = last.id + 1;
            let parent = match last.event {
                Event::Checkout { target } => target,
                _ => last.id,
            };
            records.push(Record {
                id,
                parent: Some(parent),
                version: last.version,
                timestamp: None,
                run_id: Some(run_id),
                event,
            });
            id
        };
        let marker = |format: &str, request_id| Event::SourceEntry {
            format: format.into(),
            entry: json!({"requestId":request_id}),
        };
        let failed = |request_id, text: &str| Event::ModelFinished {
            request_id,
            metadata: ModelMetadata::default(),
            usage: UsageReport::Unconfirmed,
            failure: Some(pi_rs::ModelFailure::Retryable),
            partial: text.into(),
            reply: None,
        };
        append(run, marker("pi-rs:failed-turn-partial", request));
        let partial = append(run, failed(request, "original partial"));
        let valid: Vec<_> = (0..2_048)
            .map(|index| {
                let format = if index % 2 == 0 {
                    "pi-rs:retry-scheduled"
                } else {
                    "pi-rs:overflow-recovery"
                };
                append(run, marker(format, request))
            })
            .collect();

        append(run, Event::Checkout { target: partial });
        append(run, marker("pi-rs:failed-turn-partial", newer_request));
        append(run, failed(newer_request, "newer partial"));
        let stale = append(run, marker("pi-rs:retry-scheduled", request));

        append(run, Event::Checkout { target: partial });
        append(other_run, marker("fixture:run-boundary", request));
        let fenced = append(run, marker("pi-rs:retry-scheduled", request));

        append(run, Event::Checkout { target: partial });
        let resumed = append(run, marker("pi-rs:retry-scheduled", request));
        let mut session = Session::from_records(records).unwrap();
        assert_eq!(session.last_assistant_text(), None);
        assert!(snapshot(&session).0.is_empty());

        let entries = Entries::new(&session).after(None).unwrap();
        let edits: Vec<_> = entries["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["type"] == "context_edit")
            .collect();
        assert_eq!(edits.len(), valid.len() + 1);
        for (edit, id) in edits.iter().zip(valid.iter().copied().chain([resumed])) {
            assert_eq!(edit["id"], id.to_string());
            assert_eq!(edit["targetId"], partial.to_string());
            assert_eq!(edit[pi_rs::pi_import::SUPERSEDED_PARTIAL_FIELD], true);
        }

        let directory = tempfile::tempdir().unwrap();
        let share = Entries::new(&session)
            .write_share_branch(&session, directory.path())
            .unwrap();
        let entry: Value = serde_json::from_slice(
            share
                .trim_ascii_end()
                .rsplit(|byte| *byte == b'\n')
                .next()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(entry["id"], resumed.to_string());
        assert_eq!(entry["targetId"], partial.to_string());

        session.checkout(*valid.last().unwrap()).unwrap();
        assert_eq!(session.last_assistant_text(), None);
        assert!(snapshot(&session).0.is_empty());
        for (head, expected) in [(stale, "newer partial"), (fenced, "original partial")] {
            session.checkout(head).unwrap();
            assert_eq!(session.last_assistant_text().as_deref(), Some(expected));
            assert_eq!(
                snapshot(&session).0.last().unwrap()["content"][0]["text"],
                expected
            );
        }
    }

    fn assert_turn_partial(failure: pi_rs::ModelFailure, marker: &str, stop: &str) {
        let mut initial = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        initial
            .append_user_with_images("question".into(), vec![], None)
            .unwrap();
        let request_id = Uuid::new_v4();
        let mut records = initial.records().to_vec();
        let mut append = |event| {
            let parent = records.last().unwrap();
            records.push(Record {
                id: parent.id + 1,
                parent: Some(parent.id),
                version: parent.version,
                timestamp: None,
                run_id: None,
                event,
            });
        };
        append(Event::ModelRequested {
            model: "fixture".into(),
            messages: 1,
            tools: 0,
            max_output_tokens: 64,
            request_id: Some(request_id),
            context_head: None,
            context_sha256: None,
            context_transformed: None,
            context_configuration_sha256: None,
            tool_decision: None,
        });
        append(Event::SourceEntry {
            format: marker.into(),
            entry: json!({"requestId":request_id}),
        });
        append(Event::ModelFinished {
            request_id,
            metadata: ModelMetadata::default(),
            usage: UsageReport::Unconfirmed,
            failure: Some(failure),
            partial: "partial answer".into(),
            reply: None,
        });
        let session = Session::from_records(records.clone()).unwrap();
        let (visible, native) = snapshot(&session);
        assert_eq!(visible.len(), 2);
        assert_eq!(count(&session), visible.len());
        assert_eq!(
            visible[1]["content"],
            json!([{"type":"text","text":"partial answer"}])
        );
        assert_eq!(visible[1]["stopReason"], stop);
        assert_eq!(
            session.last_assistant_text().as_deref(),
            Some("partial answer")
        );
        assert_eq!(native.len(), 1);
        if failure == pi_rs::ModelFailure::Retryable {
            let mut scheduled_records = records.clone();
            let target = session.head();
            scheduled_records.push(Record {
                id: target + 1,
                parent: Some(target),
                version: records.last().unwrap().version,
                timestamp: None,
                run_id: None,
                event: Event::SourceEntry {
                    format: "pi-rs:retry-scheduled".into(),
                    entry: json!({"requestId": request_id}),
                },
            });
            let scheduled = Session::from_records(scheduled_records).unwrap();
            assert_eq!(snapshot(&scheduled).0.len(), 1);
            let wire = super::super::rpc_entries::Entries::new(&scheduled)
                .after(None)
                .unwrap();
            let entries = wire["entries"].as_array().unwrap();
            assert_eq!(entries[entries.len() - 2]["message"], visible[1]);
            assert_eq!(entries.last().unwrap()["type"], "context_edit");
            assert_eq!(entries.last().unwrap()["targetId"], target.to_string());
        }
        let mut reopened = Session::from_records(session.records().to_vec()).unwrap();
        assert_eq!(snapshot(&reopened).0, visible);
        let target = reopened.head();
        reopened
            .edit_context(target, Some(pi_rs::ContextContent::Text("changed".into())))
            .unwrap();
        assert_eq!(snapshot(&reopened).0[1]["content"][0]["text"], "changed");
        assert_eq!(reopened.last_assistant_text().as_deref(), Some("changed"));
        assert_eq!(reopened.current_messages().len(), 1);
        let mut reopened = Session::from_records(reopened.records().to_vec()).unwrap();
        assert_eq!(snapshot(&reopened).0[1]["content"][0]["text"], "changed");
        reopened.edit_context(target, None).unwrap();
        assert_eq!(snapshot(&reopened).0.len(), 1);
        assert_eq!(reopened.last_assistant_text(), None);
        assert_eq!(count(&reopened), 1);
        let reopened = Session::from_records(reopened.records().to_vec()).unwrap();
        assert_eq!(snapshot(&reopened).0.len(), 1);
        let parent = records.last().unwrap();
        records.push(Record {
            id: parent.id + 1,
            parent: Some(parent.id),
            version: parent.version,
            timestamp: None,
            run_id: None,
            event: Event::Assistant {
                content: "partial answer".into(),
                invocations: vec![],
                usage: None,
                request_id: Some(request_id),
            },
        });
        let session = Session::from_records(records).unwrap();
        let (visible, _) = snapshot(&session);
        assert_eq!(visible.len(), 2);
        assert_eq!(count(&session), visible.len());
    }

    #[test]
    fn unmarked_cancelled_summary_is_not_visible() {
        let mut initial = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        initial
            .append_user_with_images("question".into(), vec![], None)
            .unwrap();
        let mut records = initial.records().to_vec();
        let parent = records.last().unwrap();
        records.push(Record {
            id: parent.id + 1,
            parent: Some(parent.id),
            version: parent.version,
            timestamp: None,
            run_id: None,
            event: Event::ModelFinished {
                request_id: Uuid::new_v4(),
                metadata: ModelMetadata::default(),
                usage: UsageReport::Unconfirmed,
                failure: Some(pi_rs::ModelFailure::Cancelled),
                partial: "private summary".into(),
                reply: None,
            },
        });
        let mut session = Session::from_records(records).unwrap();
        let (visible, _) = snapshot(&session);
        assert_eq!(visible.len(), 1);
        assert_eq!(count(&session), 1);
        assert_eq!(session.last_assistant_text(), None);
        let target = session.head();
        assert!(session.edit_context(target, None).is_err());
        assert!(
            session
                .edit_context(target, Some(pi_rs::ContextContent::Text("exposed".into())))
                .is_err()
        );
    }

    #[test]
    fn native_user_history_uses_blocks_and_pi_imports_keep_original_content() {
        let mut session = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        session
            .append_user_with_images("hello".into(), vec![], None)
            .unwrap();
        session
            .append_user_with_images(
                "".into(),
                vec![pi_rs::ImageContent {
                    data: "aW1hZ2U=".into(),
                    mime_type: "image/png".into(),
                }],
                None,
            )
            .unwrap();
        let (visible, native) = snapshot(&session);
        assert_eq!(
            visible[0]["content"],
            json!([{ "type":"text", "text":"hello" }])
        );
        assert_eq!(
            visible[1]["content"],
            json!([{ "type":"text", "text":"" }, { "type":"image", "data":"aW1hZ2U=", "mimeType":"image/png" }])
        );
        assert!(matches!(&native[0], Message::User { content } if content == "hello"));

        let mut fixture = tempfile::NamedTempFile::new().unwrap();
        let imported = [
            json!({"role":"user", "content":"original string", "timestamp":1}),
            json!({"role":"user", "content":[{"type":"image", "data":"aW1hZ2U=", "mimeType":"image/png"}, {"type":"text", "text":"after"}], "timestamp":2}),
        ];
        writeln!(fixture, "{}", json!({"type":"session", "version":3})).unwrap();
        for (index, message) in imported.iter().enumerate() {
            writeln!(fixture, "{}", json!({"type":"message", "id":format!("u{index}"), "parentId": if index == 0 { None } else { Some("u0") }, "message":message})).unwrap();
        }
        let imported_session = super::super::session_import::read(fixture.path()).unwrap();
        let (visible, _) = snapshot(&imported_session);
        assert_eq!(visible, imported);
    }

    #[test]
    fn reconstructed_native_tool_message_preserves_multimodal_order() {
        let blocks = json!([
            {"type":"text","text":"before"},
            {"type":"image","mimeType":"image/png","data":"aW1hZ2U="},
            {"type":"text","text":"after"}
        ]);
        let outcome = pi_rs::Outcome::StructuredWithDetails {
            data: json!({"content":blocks,"structuredContent":{"answer":42},"duration":3}),
            attachments: vec![],
            details: json!({"source":"fixture"}),
        };
        let record = Record {
            timestamp: None,
            run_id: None,
            version: 1,
            id: 1,
            parent: None,
            event: Event::ToolFinished {
                operation_id: Uuid::new_v4(),
                outcome: outcome.clone(),
                states: Default::default(),
            },
        };
        let message = Message::Tool {
            call_id: "call".into(),
            content: outcome.tool_content(),
            is_error: false,
            is_structured: Some(true),
        };
        let value = Projection::default().message(
            std::slice::from_ref(&record),
            &record,
            &message,
            false,
            None,
        );
        assert_eq!(value["content"], blocks);
        assert_eq!(value["nativeData"]["duration"], 3);
        assert_eq!(value["structuredContent"], json!({"answer":42}));
        assert_eq!(value["details"]["source"], "fixture");
        assert!(value.get("durationMs").is_none());

        let metadata = BTreeMap::from([
            (record.id, json!({"durationMs":0})),
            (record.id + 1, json!({"durationMs":500})),
        ]);
        let value = Projection::default().message(
            std::slice::from_ref(&record),
            &record,
            &message,
            false,
            Some(MetadataSource::Snapshot(&metadata)),
        );
        assert_eq!(value["durationMs"], 0);
    }

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
                tool_decision: None,
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
