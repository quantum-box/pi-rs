//! Amazon Bedrock ConverseStream adapter.
use aws_sdk_bedrockruntime::{
    Client,
    types::{
        self, ContentBlock, ContentBlockDelta, ContentBlockStart, ConversationRole,
        ConverseStreamOutput,
    },
};
use aws_smithy_types::{Document, Number};
use pi_rs_core::{
    Error, Message, Model, ModelEvent, ModelMetadata, ModelRequest, ModelStream, Result, ToolCall,
    ToolCallDelta, Usage,
};
use serde_json::Value;
use std::collections::BTreeMap;

/// A client configured with the AWS SDK's region and refreshing credential provider chain.
#[derive(Clone)]
pub struct Bedrock {
    client: std::sync::Arc<tokio::sync::OnceCell<Client>>,
    unsupported: Option<String>,
}
impl Bedrock {
    /// Resolve the region and standard AWS credential chain lazily.
    pub fn from_env() -> Self {
        Self {
            client: std::sync::Arc::new(tokio::sync::OnceCell::new()),
            unsupported: None,
        }
    }
    /// Use a caller-configured SDK client, including custom profile or endpoint settings.
    pub fn new(client: Client) -> Self {
        Self {
            client: std::sync::Arc::new(tokio::sync::OnceCell::new_with(Some(client))),
            unsupported: None,
        }
    }
    /// Mark unsupported host settings so they fail explicitly when used.
    pub fn unsupported(mut self, reason: impl Into<String>) -> Self {
        self.unsupported = Some(reason.into());
        self
    }
}
fn doc(v: Value) -> Document {
    match v {
        Value::Null => Document::Null,
        Value::Bool(x) => Document::Bool(x),
        Value::String(x) => Document::String(x),
        Value::Number(x) => Document::Number(if let Some(n) = x.as_i64() {
            if n < 0 {
                Number::NegInt(n)
            } else {
                Number::PosInt(n as u64)
            }
        } else if let Some(n) = x.as_u64() {
            Number::PosInt(n)
        } else {
            Number::Float(x.as_f64().unwrap_or_default())
        }),
        Value::Array(xs) => Document::Array(xs.into_iter().map(doc).collect()),
        Value::Object(xs) => Document::Object(xs.into_iter().map(|(k, v)| (k, doc(v))).collect()),
    }
}
fn build_error(e: impl std::fmt::Display) -> Error {
    Error::Config(format!("invalid Bedrock request: {e}"))
}

#[async_trait::async_trait]
impl Model for Bedrock {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        if let Some(reason) = &self.unsupported {
            return Err(Error::Config(reason.clone()));
        }
        if request.max_output_tokens == 0 || request.model.contains(":prompt/") {
            return Err(Error::Config("Bedrock requires a positive max output token limit and does not support managed prompt ARNs".into()));
        }
        let mut system = Vec::new();
        let mut messages: Vec<types::Message> = Vec::new();
        for message in request.messages {
            let (role, blocks) = match message {
                Message::System { content } => {
                    system.push(types::SystemContentBlock::Text(content));
                    continue;
                }
                Message::User { content } => {
                    (ConversationRole::User, vec![ContentBlock::Text(content)])
                }
                Message::UserWithImages { .. } => {
                    return Err(Error::Config(
                        "Bedrock image input is not supported yet".into(),
                    ));
                }
                Message::Assistant { content, calls, .. } => {
                    let mut blocks = Vec::new();
                    if !content.is_empty() {
                        blocks.push(ContentBlock::Text(content));
                    }
                    for call in calls {
                        let input: Value =
                            serde_json::from_str(&call.arguments).map_err(build_error)?;
                        blocks.push(ContentBlock::ToolUse(
                            types::ToolUseBlock::builder()
                                .tool_use_id(call.id)
                                .name(call.name)
                                .input(doc(input))
                                .build()
                                .map_err(build_error)?,
                        ));
                    }
                    (ConversationRole::Assistant, blocks)
                }
                Message::Tool {
                    call_id,
                    content,
                    is_error,
                    ..
                } => (
                    ConversationRole::User,
                    vec![ContentBlock::ToolResult(
                        types::ToolResultBlock::builder()
                            .tool_use_id(call_id)
                            .content(types::ToolResultContentBlock::Text(content))
                            .status(if is_error {
                                types::ToolResultStatus::Error
                            } else {
                                types::ToolResultStatus::Success
                            })
                            .build()
                            .map_err(build_error)?,
                    )],
                ),
            };
            if blocks.is_empty() {
                continue;
            }
            if let Some(last) = messages.last_mut()
                && last.role == role
            {
                last.content.extend(blocks);
                continue;
            }
            messages.push(
                types::Message::builder()
                    .role(role)
                    .set_content(Some(blocks))
                    .build()
                    .map_err(build_error)?,
            );
        }
        let tools: Vec<_> = request
            .tools
            .into_iter()
            .map(|t| {
                Ok(types::Tool::ToolSpec(
                    types::ToolSpecification::builder()
                        .name(t.name)
                        .description(t.description)
                        .input_schema(types::ToolInputSchema::Json(doc(t.parameters)))
                        .build()
                        .map_err(build_error)?,
                ))
            })
            .collect::<Result<_>>()?;
        let client = self
            .client
            .get_or_init(|| async {
                Client::new(&aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await)
            })
            .await;
        let mut command = client
            .converse_stream()
            .model_id(request.model)
            .set_messages(Some(messages))
            .set_system((!system.is_empty()).then_some(system))
            .inference_config(
                types::InferenceConfiguration::builder()
                    .max_tokens(i32::try_from(request.max_output_tokens).map_err(build_error)?)
                    .build(),
            );
        if !tools.is_empty() {
            command = command.tool_config(
                types::ToolConfiguration::builder()
                    .set_tools(Some(tools))
                    .build()
                    .map_err(build_error)?,
            );
        }
        let mut response = command.send().await.map_err(|e| {
            let retryable = e.as_service_error().is_some_and(|e| {
                e.is_throttling_exception()
                    || e.is_model_timeout_exception()
                    || e.is_service_unavailable_exception()
                    || e.is_internal_server_exception()
            }) || matches!(
                &e,
                aws_sdk_bedrockruntime::error::SdkError::TimeoutError(_)
                    | aws_sdk_bedrockruntime::error::SdkError::DispatchFailure(_)
            );
            if retryable {
                Error::RetryableModel(format!("Bedrock request failed: {e}"))
            } else {
                Error::Model(format!("Bedrock request failed: {e}"))
            }
        })?;
        Ok(Box::pin(async_stream::try_stream! {
            let mut calls: BTreeMap<i32, ToolCall> = BTreeMap::new();
            let mut usage=None; let mut stop=None; let mut cache_read=None; let mut cache_write=None; let mut received=0usize;
            let mut open_tools = std::collections::BTreeSet::new();
            while let Some(event)=response.stream.recv().await.map_err(|e| Error::RetryableModel(format!("Bedrock stream failed: {e}")))? {
                match event {
                    ConverseStreamOutput::ContentBlockStart(event) => if let Some(ContentBlockStart::ToolUse(tool))=event.start {
                        let index=event.content_block_index;
                        if index < 0 || calls.contains_key(&index) { Err(Error::Model("invalid Bedrock tool index".into()))?; }
                        open_tools.insert(index);
                        let call=ToolCall {id:tool.tool_use_id().into(), name:tool.name().into(), arguments:String::new()};
                        yield ModelEvent::ToolCallDelta(ToolCallDelta {index:index as u64,id:call.id.clone(),name:call.name.clone(),arguments:String::new()}); calls.insert(index,call);
                    },
                    ConverseStreamOutput::ContentBlockDelta(event) => match event.delta {
                        Some(ContentBlockDelta::Text(text)) => { received=received.saturating_add(text.len()); if received > 4 * 1024 * 1024 { Err(Error::Model("Bedrock stream exceeds size limit".into()))?; } yield ModelEvent::Text(text); },
                        Some(ContentBlockDelta::ToolUse(delta)) => { let index=event.content_block_index; if !open_tools.contains(&index) { Err(Error::Model("Bedrock tool delta without open block".into()))?; } received=received.saturating_add(delta.input().len()); if received > 4 * 1024 * 1024 { Err(Error::Model("Bedrock stream exceeds size limit".into()))?; } let call=calls.get_mut(&index).ok_or_else(|| Error::Model("Bedrock tool delta without start".into()))?; call.arguments.push_str(delta.input()); yield ModelEvent::ToolCallDelta(ToolCallDelta {index:index as u64,id:String::new(),name:String::new(),arguments:delta.input().into()}); },
                        _ => Err(Error::Model("unsupported Bedrock content delta".into()))?,
                    },
                    ConverseStreamOutput::MessageStop(event) => { let reason=event.stop_reason().as_str().to_owned(); if !matches!(reason.as_str(),"end_turn"|"stop_sequence"|"tool_use"|"max_tokens"|"model_context_window_exceeded") { Err(Error::Model(format!("Bedrock stopped with: {reason}")))?; } stop=Some(reason); },
                    ConverseStreamOutput::Metadata(event) => if let Some(u)=event.usage {
                        cache_read=u.cache_read_input_tokens().map(|n| n.max(0) as u64); cache_write=u.cache_write_input_tokens().map(|n| n.max(0) as u64);
                        usage=Some(Usage {input_tokens:(u.input_tokens().max(0) as u64).saturating_add(cache_read.unwrap_or(0)).saturating_add(cache_write.unwrap_or(0)),output_tokens:u.output_tokens().max(0) as u64});
                    },
                    ConverseStreamOutput::ContentBlockStop(event) => { open_tools.remove(&event.content_block_index); },
                    ConverseStreamOutput::MessageStart(_) => {},
                    _ => Err(Error::Model("unsupported Bedrock stream event".into()))?,
                }
            }
            let reason=stop.ok_or_else(|| Error::RetryableModel("Bedrock stream ended before messageStop".into()))?;
            if !open_tools.is_empty() || (!calls.is_empty() && reason != "tool_use") || (calls.is_empty() && reason == "tool_use") { Err(Error::Model("incomplete Bedrock tool response".into()))?; }
            let finish_reason = match reason.as_str() { "end_turn" | "stop_sequence" => "stop", "tool_use" => "tool_calls", "max_tokens" | "model_context_window_exceeded" => "length", _ => unreachable!() };
            yield ModelEvent::Metadata(ModelMetadata {finish_reason:Some(finish_reason.into()), cached_input_tokens:cache_read, cache_creation_input_tokens:cache_write, provider_api:Some(pi_rs_core::ModelApi::BedrockConverseStream), ..Default::default()});
            yield ModelEvent::Complete { calls:calls.into_values().collect(), usage };
        }))
    }
}
