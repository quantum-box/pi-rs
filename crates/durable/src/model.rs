//! Provider-neutral generation adapter. Persistence and retry policy belong to the caller.
//!
//! Progress is ephemeral: only a successful `CompletedGeneration` is suitable for
//! committing. In particular, tool-call deltas must never be executed. A `length`
//! finish reason means the caller must fail the calls rather than execute them.

use std::{collections::HashSet, future::Future};

use futures_util::StreamExt;
use pi_rs_core::{
    CancellationToken, Error, Message, Model, ModelEvent, ModelMetadata, ModelRequest, ModelStream,
    Result, ToolCall, ToolCallDelta, ToolSpec, Usage,
};
use serde::{Deserialize, Serialize};

/// Construct a provider request from already prepared core conversation and tool definitions.
pub fn model_request(
    model: impl Into<String>,
    messages: Vec<Message>,
    tools: Vec<ToolSpec>,
    max_output_tokens: u32,
) -> ModelRequest {
    ModelRequest {
        model: model.into(),
        messages,
        tools,
        max_output_tokens,
    }
}

/// Transient display updates; these do not constitute a completed generation.
#[derive(Clone, Debug)]
pub enum GenerationProgress {
    Text(String),
    Reasoning(String),
    ToolCallDelta(ToolCallDelta),
}

/// The authoritative result of a completed stream, suitable for durable storage.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CompletedGeneration {
    pub content: String,
    pub calls: Vec<ToolCall>,
    pub usage: Option<Usage>,
    pub metadata: ModelMetadata,
}

impl CompletedGeneration {
    /// A truncated response's tool arguments cannot safely be executed.
    pub fn is_truncated(&self) -> bool {
        self.metadata.finish_reason.as_deref() == Some("length")
    }

    /// Preserve provider replay data when appending this response to model context.
    pub fn assistant_message(&self) -> Message {
        Message::Assistant {
            content: self.content.clone(),
            calls: self.calls.clone(),
            replay: self.metadata.replay.clone(),
        }
    }
}

/// Open a normal model stream and collect it. For correlated/session-aware calls,
/// use `collect_generation` with the future returned by `Model::stream_in_session`.
pub async fn generate<M, F>(
    model: &M,
    request: ModelRequest,
    cancel: &CancellationToken,
    progress: F,
) -> Result<CompletedGeneration>
where
    M: Model + ?Sized,
    F: FnMut(GenerationProgress),
{
    collect_generation(model.stream(request), cancel, progress).await
}

/// Collect through the authoritative completion event. Cancellation wins races
/// both while opening and while reading; dropping the future drops the stream.
/// Provider errors retain their original classification for the caller's retry policy.
/// The synchronous progress callback should return promptly and must not persist deltas
/// as completed output. No usage settlement or tool execution is performed here.
pub async fn collect_generation<O, F>(
    opening: O,
    cancel: &CancellationToken,
    mut progress: F,
) -> Result<CompletedGeneration>
where
    O: Future<Output = Result<ModelStream>>,
    F: FnMut(GenerationProgress),
{
    let mut stream = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(Error::Cancelled),
        result = opening => result?,
    };
    let mut content = String::new();
    let mut metadata = ModelMetadata::default();
    const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
    loop {
        let event = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            event = stream.next() => event,
        };
        match event {
            None => return Err(Error::Model("stream ended before completion".into())),
            Some(Err(error)) => return Err(error),
            Some(Ok(ModelEvent::Text(delta))) => {
                if delta.len() > MAX_RESPONSE_BYTES.saturating_sub(content.len()) {
                    return Err(Error::Model("response exceeds size limit".into()));
                }
                content.push_str(&delta);
                progress(GenerationProgress::Text(delta));
            }
            Some(Ok(ModelEvent::Reasoning(delta))) => {
                progress(GenerationProgress::Reasoning(delta))
            }
            Some(Ok(ModelEvent::ToolCallDelta(delta))) => {
                progress(GenerationProgress::ToolCallDelta(delta))
            }
            Some(Ok(ModelEvent::Metadata(update))) => metadata = update,
            Some(Ok(ModelEvent::Complete { calls, usage })) => {
                if calls.len() > 64 {
                    return Err(Error::Model("too many tool calls".into()));
                }
                let mut ids = HashSet::new();
                if calls
                    .iter()
                    .any(|call| call.id.is_empty() || call.name.is_empty() || !ids.insert(&call.id))
                {
                    return Err(Error::Model(
                        "invalid or duplicate tool call identifier".into(),
                    ));
                }
                return Ok(CompletedGeneration {
                    content,
                    calls,
                    usage,
                    metadata,
                });
            }
        }
    }
}

#[cfg(test)]
#[path = "model_tests.rs"]
mod tests;
