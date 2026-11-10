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
    pending_tools: HashMap<u64, PendingTool>,
    aborted: bool,
    summary: bool,
}

struct PendingTool {
    content_index: usize,
    arguments: String,
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
            Notification::Committed(record) => self.record(record.event.clone(), args, &mut events),
            Notification::Text(delta) if !self.assistant.is_null() => {
                self.delta("text", delta, &mut events);
            }
            Notification::Reasoning(delta) if !self.assistant.is_null() => {
                self.delta("thinking", delta, &mut events);
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
                    "api": args.provider.api(),
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
                    let result = tool_result_value(&outcome.tool_content(), Some(&outcome));
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
                arguments: String::new(),
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
            pending.arguments.push_str(&delta.arguments);
            if let Ok(arguments @ Value::Object(_)) = serde_json::from_str(&pending.arguments) {
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
    let arguments = serde_json::from_str::<Value>(&call.arguments)
        .ok()
        .filter(Value::is_object);
    let mut value = json!({"type":"toolCall","id":call.id,"name":call.name,"arguments":arguments.as_ref().cloned().unwrap_or_else(|| json!({}))});
    if arguments.is_none() {
        value["argumentsError"] = json!("Tool arguments are not a JSON object");
    }
    value
}

pub(super) fn tool_failed(outcome: &Outcome) -> bool {
    matches!(
        outcome,
        Outcome::Failed(_) | Outcome::Skipped(_) | Outcome::Unknown(_)
    ) || matches!(outcome, Outcome::Structured { data, .. } if data["isError"] == true)
}

pub(super) fn tool_result_value(text: &str, outcome: Option<&Outcome>) -> Value {
    let (text, images) = pi_rs::extract_tool_images(text.to_owned());
    let mut content = vec![json!({"type":"text","text":text})];
    content.extend(
        images
            .into_iter()
            .map(|image| json!({"type":"image","data":image.data,"mimeType":image.mime_type})),
    );
    let mut result = json!({"content":content});
    if let Some(Outcome::SucceededWithDetails { details, .. }) = outcome {
        result["details"] = details.clone();
    }
    result
}

pub(super) fn usage_value(usage: Usage, metadata: &ModelMetadata) -> Value {
    let read = metadata.cached_input_tokens.unwrap_or_default();
    let write = metadata.cache_creation_input_tokens.unwrap_or_default();
    json!({
        "input":usage.input_tokens.saturating_sub(read).saturating_sub(write),
        "output":usage.output_tokens,"cacheRead":read,"cacheWrite":write,
        "totalTokens":usage.input_tokens.saturating_add(usage.output_tokens),
        "cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":metadata.cost_usd.as_ref().and_then(serde_json::Number::as_f64).unwrap_or_default()},
    })
}

fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
