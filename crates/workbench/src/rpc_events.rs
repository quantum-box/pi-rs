//! Pi-compatible message and tool events layered over the native journal stream.
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use pi_rs::{
    Event, Invocation, ModelFailure, ModelMetadata, Notification, Outcome, Record, ToolCall,
    ToolCallDelta, Usage, UsageReport,
};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use super::Cli;

#[derive(Default)]
pub(super) struct Stream {
    pub messages: Vec<Value>,
    assistant: Value,
    request_id: Option<Uuid>,
    started: bool,
    ended: bool,
    turn: bool,
    block: Option<&'static str>,
    metadata: ModelMetadata,
    tool_results: Vec<Value>,
    calls: HashMap<Uuid, (Invocation, Option<String>)>,
    result_metadata: HashMap<Uuid, Value>,
    pending_tools: HashMap<u64, PendingTool>,
    aborted: bool,
    summary: bool,
}

struct PendingTool {
    content_index: usize,
    arguments: super::StreamingJson,
}

impl Stream {
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    pub fn aborted(&self) -> bool {
        self.aborted
    }

    pub fn partial(&self, request_id: &Value) -> Option<&Value> {
        let id = request_id.as_str()?.parse::<Uuid>().ok()?;
        (self.request_id == Some(id) && self.started && !self.ended).then_some(&self.assistant)
    }

    pub fn observe(&mut self, event: &Value, args: &Cli) -> Vec<Value> {
        let mut events = Vec::new();
        match event["type"].as_str() {
            Some("internal_tool_result_metadata") => {
                if let Some(id) = event["operationId"].as_str().and_then(|id| id.parse().ok())
                    && let Some(content) = event.get("structuredContent")
                {
                    self.result_metadata.insert(id, content.clone());
                }
            }
            Some("summary_request_start") => {
                self.finish_turn(&mut events);
                self.summary = true;
            }
            Some("record") => {
                if let Ok(record) = Record::deserialize(&event["record"]) {
                    self.record(record.event, args, &mut events);
                }
            }
            Some("text_delta" | "reasoning_delta") => {
                if let Some(delta) = event["delta"].as_str()
                    && !self.assistant.is_null()
                {
                    let kind = if event["type"] == "text_delta" {
                        "text"
                    } else {
                        "thinking"
                    };
                    self.delta(kind, delta, &mut events);
                }
            }
            Some("tool_call_delta") if !self.assistant.is_null() && !self.ended => {
                if let Ok(delta) = ToolCallDelta::deserialize(&event["toolCallDelta"]) {
                    self.tool_delta(delta, &mut events);
                }
            }
            _ => {}
        }
        events
    }

    pub fn notify(&mut self, event: &Notification<'_>, args: &Cli) -> Vec<Value> {
        let mut events = Vec::new();
        match event {
            Notification::SummaryRequest => {
                self.finish_turn(&mut events);
                self.summary = true;
            }
            Notification::Committed(record) => {
                if let Event::ToolFinished { operation_id, .. } = &record.event
                    && let Some(content) = pi_rs::tool_result_event_metadata(*operation_id)
                {
                    self.result_metadata.insert(*operation_id, content);
                }
                self.record(record.event.clone(), args, &mut events)
            }
            Notification::Text(delta) if !self.assistant.is_null() => {
                self.delta("text", delta, &mut events);
            }
            Notification::Reasoning(delta) if !self.assistant.is_null() => {
                self.delta("thinking", delta, &mut events);
            }
            Notification::ToolProgress(event) => {
                events.push(serde_json::to_value(event).expect("tool progress"));
            }
            Notification::ToolCallDelta(delta) if !self.assistant.is_null() && !self.ended => {
                self.tool_delta((*delta).clone(), &mut events);
            }
            _ => {}
        }
        events
    }

    pub fn settle(&mut self, aborted: bool) -> Vec<Value> {
        self.aborted |= aborted;
        let mut events = Vec::new();
        if !self.assistant.is_null() && !self.ended {
            self.assistant["stopReason"] = json!(if self.aborted { "aborted" } else { "error" });
            self.finish_message(None, &mut events);
        }
        self.finish_turn(&mut events);
        events
    }

    fn record(&mut self, event: Event, args: &Cli, events: &mut Vec<Value>) {
        match event {
            Event::User { content, .. } => {
                self.user(json!(content), events);
            }
            Event::UserWithImages {
                content, images, ..
            } => {
                let mut blocks = Vec::new();
                if !content.is_empty() {
                    blocks.push(json!({"type":"text","text":content}));
                }
                blocks.extend(images.into_iter().map(
                    |image| json!({"type":"image","data":image.data,"mimeType":image.mime_type}),
                ));
                self.user(json!(blocks), events);
            }
            Event::ModelRequested { .. } if self.summary => {}
            Event::ModelRequested {
                model, request_id, ..
            } => {
                // The journal closes a logical turn after its final answer.
                // Pi emits a turn for every assistant response and tool batch.
                self.finish_turn(events);
                self.begin_turn(events);
                self.started = false;
                self.ended = false;
                self.request_id = request_id;
                self.metadata = ModelMetadata::default();
                self.assistant = json!({
                    "role":"assistant", "content":[],
                    "api": args.model_profiles.model_adapter(&args.provider, &args.model).api(),
                    "provider":args.provider.id(), "model":model,
                    "thinkingLevel":args.thinking.as_deref().unwrap_or("off"),
                    "timestamp":timestamp(), "stopReason":"stop",
                    "usage": usage_value(Usage::default(), &self.metadata),
                });
            }
            Event::ModelFinished { .. } if self.summary => self.summary = false,
            Event::ModelFinished {
                metadata,
                usage,
                failure,
                ..
            } => {
                self.metadata = metadata;
                if let Some(failure) = failure
                    && !self.assistant.is_null()
                {
                    self.aborted |= failure == ModelFailure::Cancelled;
                    self.assistant["stopReason"] =
                        json!(if self.aborted { "aborted" } else { "error" });
                    self.finish_message(None, events);
                    self.finish_turn(events);
                } else if let UsageReport::Measured { usage } = usage
                    && !self.assistant.is_null()
                {
                    self.assistant["usage"] = usage_value(usage, &self.metadata);
                }
            }
            Event::Assistant {
                content,
                invocations,
                usage,
                ..
            } if !self.assistant.is_null() => {
                self.start_message(events);
                if let Some(usage) = usage {
                    self.assistant["usage"] = usage_value(usage, &self.metadata);
                }
                if let Some(id) = &self.metadata.provider_request_id {
                    self.assistant["responseId"] = json!(id);
                }
                if let Some(identity) = &self.metadata.resolved_model {
                    self.assistant["responseModel"] = json!(identity.model);
                }
                if !self.metadata.diagnostics.is_empty() {
                    self.assistant["diagnostics"] = json!(self.metadata.diagnostics);
                }
                self.assistant["stopReason"] = json!(if self.metadata.finish_reason.as_deref()
                    == Some("length")
                {
                    "length"
                } else if invocations.is_empty() {
                    "stop"
                } else {
                    "toolUse"
                });
                self.close_block(events);
                let blocks = self.assistant["content"]
                    .as_array_mut()
                    .expect("assistant blocks");
                if let Some(reasoning) = &self.metadata.reasoning
                    && !blocks.iter().any(|block| block["type"] == "thinking")
                {
                    let index = if self.pending_tools.is_empty() {
                        0
                    } else {
                        blocks.len()
                    };
                    blocks.insert(index, json!({"type":"thinking","thinking":reasoning}));
                }
                if !content.is_empty() && !blocks.iter().any(|block| block["type"] == "text") {
                    blocks.push(json!({"type":"text","text":content}));
                }
                let mut finalized = HashSet::new();
                for invocation in invocations {
                    let call = tool_call_value(&invocation.call);
                    let blocks = self.assistant["content"]
                        .as_array_mut()
                        .expect("assistant blocks");
                    let pending = self
                        .pending_tools
                        .values()
                        .find(|pending| blocks[pending.content_index]["id"] == invocation.call.id);
                    let index = if let Some(pending) = pending {
                        blocks[pending.content_index] = call.clone();
                        finalized.insert(pending.content_index);
                        pending.content_index
                    } else {
                        let index = blocks.len();
                        blocks.push(call.clone());
                        self.update(
                            json!({"type":"toolcall_start","contentIndex":index}),
                            events,
                        );
                        index
                    };
                    self.update(
                        json!({"type":"toolcall_end","contentIndex":index,"toolCall":call}),
                        events,
                    );
                    self.calls
                        .insert(invocation.operation_id, (invocation, None));
                }
                let incomplete: HashSet<_> = self
                    .pending_tools
                    .drain()
                    .map(|(_, pending)| pending.content_index)
                    .filter(|index| !finalized.contains(index))
                    .collect();
                if !incomplete.is_empty() {
                    let mut index = 0;
                    self.assistant["content"]
                        .as_array_mut()
                        .expect("assistant blocks")
                        .retain(|_| {
                            let keep = !incomplete.contains(&index);
                            index += 1;
                            keep
                        });
                }
                self.finish_message(Some(content), events);
            }
            Event::ToolPlanned {
                invocation,
                parent_operation,
            } => {
                if invocation.call.name == super::operation_models::TOOL {
                    return;
                }
                let parent = self
                    .calls
                    .get(&parent_operation)
                    .map(|(parent, _)| parent.call.id.clone());
                self.calls
                    .insert(invocation.operation_id, (invocation, parent));
            }
            Event::ToolStarted { operation_id } => {
                if let Some((invocation, parent)) = self.calls.get(&operation_id) {
                    let mut event = json!({
                        "type":"tool_execution_start", "toolCallId":invocation.call.id,
                        "toolName":invocation.call.name, "args":tool_call_value(&invocation.call)["arguments"],
                    });
                    if let Some(parent) = &parent {
                        event["parentToolCallId"] = json!(parent);
                    }
                    events.push(event);
                }
            }
            Event::ToolFinished {
                operation_id,
                outcome,
                ..
            }
            | Event::ToolReconciled {
                operation_id,
                outcome,
            } => {
                if let Some((invocation, parent)) = self.calls.remove(&operation_id) {
                    let is_error = tool_failed(&outcome);
                    let mut result = tool_event_result_value(
                        &outcome.tool_content(),
                        Some(&outcome),
                        Some(outcome.is_structured()),
                    );
                    let sidecar = self.result_metadata.remove(&operation_id);
                    if let Some(content) = &sidecar {
                        result["structuredContent"] = content.clone();
                    }
                    let mut event = json!({
                        "type":"tool_execution_end","toolCallId":invocation.call.id,
                        "toolName":invocation.call.name,"result":result,"isError":is_error,
                    });
                    if let Some(parent) = &parent {
                        event["parentToolCallId"] = json!(parent);
                    }
                    events.push(event);
                    if parent.is_none() {
                        let mut message = result;
                        if sidecar.is_some()
                            || matches!(outcome, Outcome::TextResultWithStructuredContent { .. })
                        {
                            message
                                .as_object_mut()
                                .expect("result object")
                                .remove("structuredContent");
                        }
                        message["role"] = json!("toolResult");
                        message["toolCallId"] = json!(invocation.call.id);
                        message["toolName"] = json!(invocation.call.name);
                        message["isError"] = json!(is_error);
                        message["timestamp"] = json!(timestamp());
                        self.message(message.clone(), events);
                        self.tool_results.push(message);
                    }
                }
            }
            Event::TurnFinished { .. } => self.finish_turn(events),
            _ => {}
        }
    }

    fn begin_turn(&mut self, events: &mut Vec<Value>) {
        if !self.turn {
            self.turn = true;
            events.push(json!({"type":"turn_start"}));
        }
    }

    fn finish_turn(&mut self, events: &mut Vec<Value>) {
        if self.turn && !self.assistant.is_null() {
            self.turn = false;
            events.push(
                json!({"type":"turn_end","message":self.assistant,"toolResults":self.tool_results}),
            );
            self.tool_results.clear();
            self.pending_tools.clear();
            self.assistant = Value::Null;
            self.request_id = None;
        }
    }

    fn user(&mut self, content: Value, events: &mut Vec<Value>) {
        if self.ended {
            self.finish_turn(events);
        }
        self.begin_turn(events);
        self.message(
            json!({"role":"user","content":content,"timestamp":timestamp()}),
            events,
        );
    }

    fn message(&mut self, message: Value, events: &mut Vec<Value>) {
        events.push(json!({"type":"message_start","message":message}));
        events.push(json!({"type":"message_end","message":message}));
        self.messages.push(message);
    }

    fn start_message(&mut self, events: &mut Vec<Value>) {
        if !self.started {
            self.started = true;
            events.push(json!({"type":"message_start","message":self.assistant}));
        }
    }

    fn finish_message(&mut self, content: Option<String>, events: &mut Vec<Value>) {
        self.start_message(events);
        self.close_block(events);
        if let Some(content) = content {
            let blocks = self.assistant["content"]
                .as_array_mut()
                .expect("assistant blocks");
            let mut text = blocks.iter_mut().filter(|block| block["type"] == "text");
            if let Some(first) = text.next()
                && text.next().is_none()
            {
                first["text"] = json!(content);
            }
        }
        events.push(json!({"type":"message_end","message":self.assistant}));
        self.messages.push(self.assistant.clone());
        self.ended = true;
    }

    fn update(&self, mut update: Value, events: &mut Vec<Value>) {
        update["partial"] = self.assistant.clone();
        events.push(json!({"type":"message_update","message":self.assistant,"assistantMessageEvent":update}));
    }

    fn delta(&mut self, kind: &'static str, delta: &str, events: &mut Vec<Value>) {
        self.start_message(events);
        if self.block != Some(kind) {
            self.close_block(events);
            let blocks = self.assistant["content"]
                .as_array_mut()
                .expect("assistant blocks");
            let index = blocks.len();
            blocks.push(json!({"type":kind,(kind):""}));
            self.block = Some(kind);
            self.update(
                json!({"type":format!("{kind}_start"),"contentIndex":index}),
                events,
            );
        }
        let blocks = self.assistant["content"]
            .as_array_mut()
            .expect("assistant blocks");
        let index = blocks.len() - 1;
        if let Value::String(content) = &mut blocks[index][kind] {
            content.push_str(delta);
        }
        self.update(
            json!({"type":format!("{kind}_delta"),"contentIndex":index,"delta":delta}),
            events,
        );
    }

    fn tool_delta(&mut self, delta: ToolCallDelta, events: &mut Vec<Value>) {
        self.start_message(events);
        self.close_block(events);
        let blocks = self.assistant["content"]
            .as_array_mut()
            .expect("assistant blocks");
        let next = blocks.len();
        let pending = self.pending_tools.entry(delta.index).or_insert_with(|| {
            blocks.push(json!({"type":"toolCall","id":"","name":"","arguments":{}}));
            PendingTool {
                content_index: next,
                arguments: super::StreamingJson::default(),
            }
        });
        let index = pending.content_index;
        let block = &mut blocks[index];
        for (field, fragment) in [("id", &delta.id), ("name", &delta.name)] {
            if !fragment.is_empty()
                && let Value::String(value) = &mut block[field]
            {
                value.push_str(fragment);
            }
        }
        if index == next {
            self.update(
                json!({"type":"toolcall_start","contentIndex":index}),
                events,
            );
        }
        if !delta.arguments.is_empty() {
            let pending = self
                .pending_tools
                .get_mut(&delta.index)
                .expect("open tool call");
            let arguments = pending.arguments.append(&delta.arguments);
            if arguments.is_object() {
                self.assistant["content"][index]["arguments"] = arguments;
            }
            self.update(
                json!({"type":"toolcall_delta","contentIndex":index,"delta":delta.arguments}),
                events,
            );
        }
    }

    fn close_block(&mut self, events: &mut Vec<Value>) {
        if let Some(kind) = self.block.take() {
            let index = self.assistant["content"]
                .as_array()
                .expect("assistant blocks")
                .len()
                - 1;
            self.update(json!({"type":format!("{kind}_end"),"contentIndex":index,"content":self.assistant["content"][index][kind]}), events);
        }
    }
}

pub(super) fn tool_call_value(call: &ToolCall) -> Value {
    let arguments = pi_rs::parse_tool_arguments(&call.arguments);
    let mut value = json!({"type":"toolCall","id":call.id,"name":call.name,"arguments":arguments.as_ref().cloned().unwrap_or_else(|_| json!({}))});
    if let Err(error) = arguments {
        value["argumentsError"] = json!(error);
    }
    value
}

pub(super) fn tool_failed(outcome: &Outcome) -> bool {
    outcome.is_error()
}

pub(super) fn tool_result_value(
    text: &str,
    outcome: Option<&Outcome>,
    is_structured: Option<bool>,
) -> Value {
    let (text, images) = if pi_rs::tool_content_is_structured(text, is_structured) {
        pi_rs::extract_tool_images(text.to_owned())
    } else {
        (text.to_owned(), Vec::new())
    };
    let mut content = vec![json!({"type":"text","text":text})];
    content.extend(
        images
            .into_iter()
            .map(|image| json!({"type":"image","data":image.data,"mimeType":image.mime_type})),
    );
    let mut result = json!({"content":content});
    if let Some(
        Outcome::SucceededWithDetails { details, .. }
        | Outcome::StructuredWithDetails { details, .. },
    ) = outcome
    {
        result["details"] = details.clone();
    }
    if let Some(
        Outcome::TextResult {
            details: Some(details),
            ..
        }
        | Outcome::TextResultWithStructuredContent {
            details: Some(details),
            ..
        },
    ) = outcome
    {
        result["details"] = details.clone();
    }
    if let Some(Outcome::TextResultWithStructuredContent {
        structured_content, ..
    }) = outcome
    {
        result["structuredContent"] = structured_content.clone();
    }
    result
}

/// Project native content-block results as Pi events without changing the
/// separate model-facing native tool envelope.
pub(super) fn tool_event_result_value(
    text: &str,
    outcome: Option<&Outcome>,
    is_structured: Option<bool>,
) -> Value {
    let mut result = tool_result_value(text, outcome, is_structured);
    if !pi_rs::tool_content_is_structured(text, is_structured) {
        return result;
    }
    let Ok(envelope) = serde_json::from_str::<Value>(text) else {
        return result;
    };
    let Some(data) = envelope.get("data").and_then(Value::as_object) else {
        return result;
    };
    let mut content = Vec::new();
    let mut rejected = Vec::new();
    let blocks = data.get("content").and_then(Value::as_array);
    for (index, block) in blocks.into_iter().flatten().enumerate() {
        let valid = match block["type"].as_str() {
            Some("text") => block["text"].is_string(),
            Some("image") => {
                block["data"]
                    .as_str()
                    .is_some_and(|data| data.len() <= 8 * 1024 * 1024)
                    && block["mimeType"].is_string()
            }
            _ => false,
        };
        if valid {
            content.push(block.clone());
        } else {
            rejected.push(json!({"index":index,"block":block}));
        }
    }
    if let Some(blocks) = blocks
        && (!content.is_empty() || blocks.is_empty())
    {
        result["content"] = json!(content);
    }
    if let Some(structured_content) = data.get("structuredContent") {
        result["structuredContent"] = structured_content.clone();
    }
    if !rejected.is_empty() {
        result["nativeRejectedBlocks"] = json!(rejected);
    }
    // Preserve native fields that used to be present in the text envelope.
    let mut native = data.clone();
    native.remove("content");
    if let Some(attachments) = envelope
        .get("attachments")
        .filter(|value| value.as_array().is_some_and(|values| !values.is_empty()))
    {
        result["nativeAttachments"] = attachments.clone();
    }
    if !native.is_empty() {
        result["nativeData"] = Value::Object(native);
    }
    if let Some(fields) = envelope.as_object() {
        let mut extra = fields.clone();
        extra.remove("data");
        extra.remove("attachments");
        if !extra.is_empty() {
            result["nativeEnvelope"] = Value::Object(extra);
        }
    }
    result
}

pub(super) fn usage_value(usage: Usage, metadata: &ModelMetadata) -> Value {
    let read = metadata.cached_input_tokens.unwrap_or_default();
    let write = metadata.cache_creation_input_tokens.unwrap_or_default();
    let breakdown = metadata.cost_breakdown.clone().unwrap_or_default();
    let mut value = json!({
        "input":usage.input_tokens.saturating_sub(read).saturating_sub(write),
        "output":usage.output_tokens,"cacheRead":read,"cacheWrite":write,
        "totalTokens":usage.input_tokens.saturating_add(usage.output_tokens),
        "cost":{
            "input":breakdown.input,"output":breakdown.output,
            "cacheRead":breakdown.cache_read,"cacheWrite":breakdown.cache_write,
            "total":metadata.cost_usd.as_ref().and_then(serde_json::Number::as_f64).unwrap_or_default()
        },
    });
    if let Some(long) = metadata.cache_write_1h_tokens {
        value["cacheWrite1h"] = json!(long);
    }
    if let Some(from_provider) = metadata.cost_from_provider {
        value["cost"]["totalFromProvider"] = json!(from_provider);
    }
    value
}

fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod streaming_tool_tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn structured_read_metadata_is_correlated_and_not_added_to_messages() {
        let operation_id = Uuid::new_v4();
        let other_id = Uuid::new_v4();
        let mut stream = Stream::default();
        let args = Cli::parse_from(["pi"]);
        stream.calls.insert(
            operation_id,
            (
                Invocation {
                    operation_id,
                    call: ToolCall {
                        id: "read-call".into(),
                        name: "read".into(),
                        arguments: "{}".into(),
                    },
                },
                None,
            ),
        );
        let metadata = json!("page\n\n[1 more line]");
        stream.result_metadata.insert(other_id, json!("other"));
        assert!(stream.observe(&json!({"type":"internal_tool_result_metadata", "operationId":operation_id, "structuredContent":metadata}), &args).is_empty());
        let mut events = Vec::new();
        stream.record(
            Event::ToolFinished {
                operation_id,
                outcome: Outcome::Succeeded(metadata.as_str().unwrap().into()),
                states: Default::default(),
            },
            &args,
            &mut events,
        );
        let end = events
            .iter()
            .find(|event| event["type"] == "tool_execution_end")
            .unwrap();
        assert_eq!(end["result"]["structuredContent"], metadata);
        let message = events
            .iter()
            .find(|event| event["type"] == "message_end")
            .unwrap();
        assert!(message["message"].get("structuredContent").is_none());
        assert_eq!(stream.result_metadata.get(&other_id), Some(&json!("other")));
        assert!(!stream.result_metadata.contains_key(&operation_id));
    }

    #[test]
    fn image_read_sidecar_appears_only_in_execution_event() {
        let operation_id = Uuid::new_v4();
        let mut stream = Stream::default();
        let args = Cli::parse_from(["pi"]);
        stream.calls.insert(
            operation_id,
            (
                Invocation {
                    operation_id,
                    call: ToolCall {
                        id: "image-read".into(),
                        name: "read".into(),
                        arguments: "{}".into(),
                    },
                },
                None,
            ),
        );
        let blocks = json!([{ "type":"text", "text":"image note" }, { "type":"image", "mimeType":"image/png", "data":"aW1hZ2U=" }]);
        let structured =
            json!({"type":"image", "mimeType":"image/png", "data":"aW1hZ2U=", "note":"image note"});
        stream.observe(&json!({"type":"internal_tool_result_metadata", "operationId":operation_id, "structuredContent":structured}), &args);
        let mut events = Vec::new();
        stream.record(
            Event::ToolFinished {
                operation_id,
                outcome: Outcome::Structured {
                    data: json!({"content":blocks}),
                    attachments: vec![],
                },
                states: Default::default(),
            },
            &args,
            &mut events,
        );
        let end = events
            .iter()
            .find(|event| event["type"] == "tool_execution_end")
            .unwrap();
        assert_eq!(end["result"]["content"], blocks);
        assert_eq!(end["result"]["structuredContent"], structured);
        let message = events
            .iter()
            .find(|event| event["type"] == "message_end")
            .unwrap();
        assert_eq!(message["message"]["content"], blocks);
        assert!(message["message"].get("structuredContent").is_none());
    }

    #[test]
    fn multimodal_tool_events_preserve_order_and_native_metadata() {
        let blocks = json!([
            {"type":"text","text":"before"},
            {"type":"image","mimeType":"image/png","data":"aW1hZ2U="},
            {"type":"text","text":"after"},
            {"type":"image","mimeType":"image/jpeg","data":"cGhvdG8="}
        ]);
        let outcome = Outcome::StructuredWithDetails {
            data: json!({"content":blocks,"structuredContent":{"answer":42},"wall_time_seconds":1.25}),
            attachments: vec![pi_rs::AttachmentRef {
                id: "asset".into(),
                media_type: "image/png".into(),
            }],
            details: json!({"code":42}),
        };
        let operation_id = Uuid::new_v4();
        let invocation = Invocation {
            operation_id,
            call: ToolCall {
                id: "call".into(),
                name: "read".into(),
                arguments: "{}".into(),
            },
        };
        let mut stream = Stream::default();
        stream.calls.insert(operation_id, (invocation, None));
        let mut events = Vec::new();
        stream.record(
            Event::ToolFinished {
                operation_id,
                outcome: outcome.clone(),
                states: Default::default(),
            },
            &Cli::parse_from(["pi"]),
            &mut events,
        );
        let end = events
            .iter()
            .find(|event| event["type"] == "tool_execution_end")
            .unwrap();
        let message = events
            .iter()
            .find(|event| event["type"] == "message_end")
            .unwrap();
        assert_eq!(end["result"]["content"], blocks);
        assert_eq!(message["message"]["content"], blocks);
        assert_eq!(end["result"]["details"], json!({"code":42}));
        assert_eq!(end["result"]["structuredContent"], json!({"answer":42}));
        assert_eq!(end["result"]["nativeData"]["wall_time_seconds"], 1.25);
        assert_eq!(end["result"]["nativeAttachments"][0]["id"], "asset");
        assert_eq!(
            tool_event_result_value(&outcome.tool_content(), None, Some(true))["content"],
            blocks
        );
        assert_eq!(
            tool_event_result_value(&outcome.tool_content(), None, None)["content"],
            blocks
        );
        // Provider-facing native content remains the original envelope.
        assert!(tool_result_value(&outcome.tool_content(), Some(&outcome), Some(true))["content"][0]["text"].as_str().unwrap().contains("[image attached]"));
    }

    #[test]
    fn multimodal_projection_keeps_plain_and_unknown_structured_data() {
        let plain = r#"{ "data": { "content": [{"type":"image","mimeType":"image/png","data":"aW1hZ2U="}] } }"#;
        assert_eq!(
            tool_event_result_value(plain, None, Some(false)),
            json!({"content":[{"type":"text","text":plain}]})
        );
        let outcome = Outcome::Structured {
            data: json!({"content":[{"type":"custom","value":1}]}),
            attachments: vec![],
        };
        let result = tool_event_result_value(&outcome.tool_content(), Some(&outcome), Some(true));
        assert_eq!(
            result["content"],
            tool_result_value(&outcome.tool_content(), Some(&outcome), Some(true))["content"]
        );
        assert_eq!(result["nativeRejectedBlocks"][0]["block"]["type"], "custom");
    }

    #[test]
    fn multimodal_projection_keeps_valid_blocks_around_rejected_images() {
        let oversized = "A".repeat(8 * 1024 * 1024 + 1);
        let rejected_mime = json!({"type":"image","mimeType":"image/tiff","data":"dGlmZg=="});
        let rejected_size = json!({"type":"image","mimeType":"image/png","data":oversized});
        let supported = json!({"type":"image","mimeType":"image/png","data":"aW1hZ2U="});
        let outcome = Outcome::StructuredWithDetails {
            data: json!({"content":[{"type":"text","text":"before"}, rejected_mime, supported, rejected_size, {"type":"text","text":"after"}], "nativeInfo":"kept"}),
            attachments: vec![],
            details: json!({"code":42}),
        };
        let result = tool_event_result_value(&outcome.tool_content(), Some(&outcome), Some(true));
        assert_eq!(
            result["content"],
            json!([{"type":"text","text":"before"}, rejected_mime, supported, {"type":"text","text":"after"}])
        );
        assert_eq!(result["nativeRejectedBlocks"][0]["index"], 3);
        assert_eq!(result["nativeRejectedBlocks"][0]["block"], rejected_size);
        assert_eq!(result["nativeData"]["nativeInfo"], "kept");
        assert_eq!(result["details"]["code"], 42);
        let operation_id = Uuid::new_v4();
        let mut stream = Stream::default();
        stream.calls.insert(
            operation_id,
            (
                Invocation {
                    operation_id,
                    call: ToolCall {
                        id: "call".into(),
                        name: "read".into(),
                        arguments: "{}".into(),
                    },
                },
                None,
            ),
        );
        let mut events = Vec::new();
        stream.record(
            Event::ToolFinished {
                operation_id,
                outcome,
                states: Default::default(),
            },
            &Cli::parse_from(["pi"]),
            &mut events,
        );
        assert_eq!(
            events
                .iter()
                .find(|event| event["type"] == "tool_execution_end")
                .unwrap()["result"],
            result
        );
        let message = &events
            .iter()
            .find(|event| event["type"] == "message_end")
            .unwrap()["message"];
        assert_eq!(message["content"], result["content"]);
        assert_eq!(
            message["nativeRejectedBlocks"],
            result["nativeRejectedBlocks"]
        );
    }

    #[test]
    fn multimodal_legacy_envelope_preserves_structured_content_and_extra_fields() {
        let envelope = json!({
            "data": {
                "content": [
                    {"type":"text","text":"before"},
                    {"type":"image","mimeType":"image/tiff","data":"dGlmZg=="},
                    {"type":"image","mimeType":"image/png","data":"aW1hZ2U="},
                    {"type":"text","text":"after"}
                ],
                "structuredContent": {"answer":42},
                "otherData": true
            },
            "attachments": [],
            "extra": {"origin":"legacy"}
        });
        let result = tool_event_result_value(&envelope.to_string(), None, None);
        assert_eq!(result["structuredContent"], json!({"answer":42}));
        assert_eq!(
            result["nativeData"]["structuredContent"],
            json!({"answer":42})
        );
        assert_eq!(result["nativeData"]["otherData"], true);
        assert_eq!(
            result["nativeEnvelope"],
            json!({"extra":{"origin":"legacy"}})
        );
        assert_eq!(
            result["content"],
            json!([
                {"type":"text","text":"before"},
                {"type":"image","mimeType":"image/tiff","data":"dGlmZg=="},
                {"type":"image","mimeType":"image/png","data":"aW1hZ2U="},
                {"type":"text","text":"after"}
            ])
        );
        assert!(result.get("nativeRejectedBlocks").is_none());
    }

    #[test]
    fn structured_content_survives_missing_or_unprojectable_content() {
        for content in [None, Some(json!([{"type":"custom","value":1}]))] {
            let mut data = json!({"structuredContent":{"answer":42}});
            if let Some(content) = content {
                data["content"] = content;
            }
            let outcome = Outcome::Structured {
                data,
                attachments: vec![],
            };
            let result =
                tool_event_result_value(&outcome.tool_content(), Some(&outcome), Some(true));
            assert_eq!(result["structuredContent"], json!({"answer":42}));
        }
        let image = json!({"type":"image","mimeType":"image/tiff","data":"dGlmZg=="});
        let outcome = Outcome::Structured {
            data: json!({"content":[image],"structuredContent":{"answer":42}}),
            attachments: vec![],
        };
        let result = tool_event_result_value(&outcome.tool_content(), Some(&outcome), Some(true));
        assert_eq!(result["content"], json!([image]));
        assert_eq!(result["structuredContent"], json!({"answer":42}));
    }

    #[test]
    fn patched_result_projects_verbatim_content_details_and_error() {
        let result = Outcome::TextResult {
            output: "exact".into(),
            details: Some(json!({"code": 42})),
            is_error: true,
        };
        assert!(tool_failed(&result));
        assert_eq!(
            tool_result_value(
                &result.tool_content(),
                Some(&result),
                Some(result.is_structured())
            ),
            json!({"content":[{"type":"text","text":"exact"}], "details":{"code":42}})
        );
        let success = Outcome::TextResult {
            output: "recovered".into(),
            details: None,
            is_error: false,
        };
        assert!(!tool_failed(&success));
    }

    #[test]
    fn structured_text_result_projects_event_fields_without_changing_model_text() {
        let outcome = Outcome::TextResultWithStructuredContent {
            output: "bad\n\nCommand exited with code 7".into(),
            details: Some(json!({"source":"shell"})),
            structured_content: json!({"output":"bad","exit_code":7,"truncated":false,"wall_time_seconds":0.1}),
            is_error: true,
        };
        let result = tool_event_result_value(&outcome.tool_content(), Some(&outcome), Some(false));
        assert_eq!(result["content"][0]["text"], outcome.tool_content());
        assert_eq!(result["structuredContent"]["exit_code"], 7);
        assert_eq!(result["details"]["source"], "shell");
        assert!(tool_failed(&outcome));
        let operation_id = Uuid::new_v4();
        let mut stream = Stream::default();
        stream.calls.insert(
            operation_id,
            (
                Invocation {
                    operation_id,
                    call: ToolCall {
                        id: "call".into(),
                        name: "bash".into(),
                        arguments: "{}".into(),
                    },
                },
                None,
            ),
        );
        let mut events = Vec::new();
        stream.record(
            Event::ToolFinished {
                operation_id,
                outcome,
                states: Default::default(),
            },
            &Cli::parse_from(["pi"]),
            &mut events,
        );
        let end = events
            .iter()
            .find(|event| event["type"] == "tool_execution_end")
            .unwrap();
        assert_eq!(end["result"]["structuredContent"]["exit_code"], 7);
        assert_eq!(end["isError"], true);
        let message = &events
            .iter()
            .find(|event| event["type"] == "message_end")
            .unwrap()["message"];
        assert!(message.get("structuredContent").is_none());
        assert_eq!(
            message["content"][0]["text"],
            "bad\n\nCommand exited with code 7"
        );
    }

    #[test]
    fn plain_json_result_is_verbatim_in_rpc() {
        let text = r#"{ "data": { "content": [{"type":"image","mimeType":"image/png","data":"aW1hZ2U="}] } }"#;
        let outcome = Outcome::TextResult {
            output: text.into(),
            details: None,
            is_error: true,
        };
        assert_eq!(
            tool_result_value(text, Some(&outcome), Some(outcome.is_structured())),
            json!({"content":[{"type":"text","text":text}]})
        );
    }

    #[test]
    fn tool_progress_is_forwarded_as_pi_event_in_json_mode() {
        let event = pi_rs::LifecycleEvent::ToolExecutionUpdate {
            tool_call_id: "child".into(),
            tool_name: "bash".into(),
            args: json!({"command":"echo hi"}),
            partial_result: json!({"content":[{"type":"text","text":"hi"}]}),
            parent_tool_call_id: Some("parent".into()),
        };
        let mut stream = Stream::default();
        let args = Cli::parse_from(["pi-rs"]);
        let events = stream.notify(&Notification::ToolProgress(&event), &args);
        assert_eq!(
            events,
            vec![
                json!({"type":"tool_execution_update", "toolCallId":"child", "toolName":"bash", "args":{"command":"echo hi"}, "partialResult":{"content":[{"type":"text","text":"hi"}]}, "parentToolCallId":"parent"})
            ]
        );
    }

    #[test]
    fn json_trace_emits_progress_once_and_worker_trace_still_forwards() {
        let event = pi_rs::LifecycleEvent::ToolExecutionUpdate {
            tool_call_id: "call".into(),
            tool_name: "bash".into(),
            args: json!({}),
            partial_result: json!({"content":[]}),
            parent_tool_call_id: None,
        };
        let args = Cli::parse_from(["pi"]);
        for trace in [false, true] {
            let mut stream = Stream::default();
            let mut output = stream.notify(&Notification::ToolProgress(&event), &args);
            output.extend(super::super::trace_tool_progress(&event, trace, true));
            assert_eq!(output.len(), 1);
            assert_eq!(output[0]["type"], "tool_execution_update");
        }
        let worker_event = super::super::trace_tool_progress(&event, true, false).unwrap();
        assert_eq!(worker_event["type"], "tool_execution_update");
        assert!(super::super::trace_tool_progress(&event, false, false).is_none());
    }

    #[test]
    fn completed_tool_call_reports_repaired_and_invalid_arguments() {
        let call = |arguments: &str| ToolCall {
            id: "id".into(),
            name: "read".into(),
            arguments: arguments.into(),
        };
        let repaired = tool_call_value(&call(r#"{"path":"\unknown"}"#));
        assert_eq!(repaired["arguments"]["path"], r"\unknown");
        assert!(repaired.get("argumentsError").is_none());
        let raw = r#"{"path":"partial"#;
        let invalid = tool_call_value(&call(raw));
        assert_eq!(invalid["arguments"], json!({}));
        assert!(invalid["argumentsError"].as_str().unwrap().ends_with(raw));
        let empty = tool_call_value(&call(""));
        assert_eq!(empty["arguments"], json!({}));
        assert!(empty.get("argumentsError").is_none());
    }

    #[test]
    fn tool_deltas_expose_partial_arguments_for_each_call() {
        let mut stream = Stream {
            assistant: json!({"role":"assistant","content":[]}),
            ..Stream::default()
        };
        let mut events = Vec::new();
        let delta = |index, id: &str, name: &str, arguments: &str| ToolCallDelta {
            index,
            id: id.into(),
            name: name.into(),
            arguments: arguments.into(),
        };
        stream.tool_delta(delta(0, "call-a", "read", "{\"path\":\"fo"), &mut events);
        assert_eq!(
            stream.assistant["content"][0]["arguments"],
            json!({"path":"fo"})
        );
        assert_eq!(
            events.last().unwrap()["assistantMessageEvent"]["partial"]["content"][0]["arguments"],
            json!({"path":"fo"})
        );
        stream.tool_delta(
            delta(1, "call-b", "write", "{\"content\":\"xy"),
            &mut events,
        );
        stream.tool_delta(delta(0, "", "", "o.txt\"}"), &mut events);
        assert_eq!(
            stream.assistant["content"][0]["arguments"],
            json!({"path":"foo.txt"})
        );
        assert_eq!(
            stream.assistant["content"][1]["arguments"],
            json!({"content":"xy"})
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event["assistantMessageEvent"]["type"] == "toolcall_start")
                .count(),
            2
        );
    }
}
