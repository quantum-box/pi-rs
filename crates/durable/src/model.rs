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
use uuid::Uuid;

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

/// Caller-provided identity and exact input for one model invocation.
/// Persist the intent before invoking the model when recovery requires it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct GenerationRequest {
    pub request_id: Uuid,
    pub session_id: Uuid,
    pub request: ModelRequest,
}

/// A completed invocation, including the identity and input needed for recovery.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct GenerationResult {
    pub request: GenerationRequest,
    pub generation: CompletedGeneration,
}

/// Invoke the existing session-aware model interface with caller-provided IDs.
///
/// The caller owns intent and checkpoint persistence, retries, usage settlement,
/// and tool execution. An error or cancellation does not return partial output;
/// it does not establish whether the remote provider executed the request.
pub async fn generate_in_session<M, F>(
    model: &M,
    request: GenerationRequest,
    cancel: &CancellationToken,
    progress: F,
) -> Result<GenerationResult>
where
    M: Model + ?Sized,
    F: FnMut(GenerationProgress),
{
    let generation = collect_generation(
        model.stream_in_session(
            request.request_id,
            request.session_id,
            request.request.clone(),
        ),
        cancel,
        progress,
    )
    .await?;
    Ok(GenerationResult {
        request,
        generation,
    })
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

#[cfg(test)]
mod session_adapter_tests {
    use super::*;
    use futures_util::{StreamExt, stream};
    use std::sync::Mutex;

    struct TestModel {
        seen: Mutex<Vec<GenerationRequest>>,
    }

    #[async_trait::async_trait]
    impl Model for TestModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            panic!("session-aware interface must be used")
        }

        async fn stream_in_session(
            &self,
            request_id: Uuid,
            session_id: Uuid,
            request: ModelRequest,
        ) -> Result<ModelStream> {
            self.seen.lock().unwrap().push(GenerationRequest {
                request_id,
                session_id,
                request,
            });
            Ok(stream::iter(vec![
                Ok(ModelEvent::Text("answer".into())),
                Ok(ModelEvent::Metadata(ModelMetadata {
                    provider_request_id: Some("provider-id".into()),
                    finish_reason: Some("tool_calls".into()),
                    ..Default::default()
                })),
                Ok(ModelEvent::Complete {
                    calls: vec![ToolCall {
                        id: "tool-id".into(),
                        name: "read".into(),
                        arguments: "{\"path\":\"a\"}".into(),
                    }],
                    usage: Some(Usage {
                        input_tokens: 5,
                        output_tokens: 7,
                    }),
                }),
            ])
            .boxed())
        }
    }

    #[tokio::test]
    async fn session_identity_input_and_completed_output_roundtrip() {
        let model = TestModel {
            seen: Mutex::new(Vec::new()),
        };
        let request = GenerationRequest {
            request_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            request: model_request(
                "model",
                vec![Message::User {
                    content: "hi".into(),
                }],
                vec![],
                42,
            ),
        };
        let result =
            generate_in_session(&model, request.clone(), &CancellationToken::new(), |_| {})
                .await
                .unwrap();
        assert_eq!(*model.seen.lock().unwrap(), vec![request.clone()]);
        assert_eq!(result.request, request);
        assert_eq!(result.generation.content, "answer");
        assert_eq!(result.generation.calls[0].id, "tool-id");
        assert_eq!(result.generation.calls[0].arguments, "{\"path\":\"a\"}");
        assert_eq!(result.generation.usage.unwrap().output_tokens, 7);
        assert_eq!(
            result.generation.metadata.provider_request_id.as_deref(),
            Some("provider-id")
        );
        let bytes = serde_json::to_vec(&result).unwrap();
        assert_eq!(
            serde_json::from_slice::<GenerationResult>(&bytes).unwrap(),
            result
        );
    }

    #[tokio::test]
    async fn cancelled_call_does_not_open_stream() {
        let model = TestModel {
            seen: Mutex::new(Vec::new()),
        };
        let cancel = CancellationToken::new();
        cancel.cancel();
        let request = GenerationRequest {
            request_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            request: model_request("model", vec![], vec![], 1),
        };
        assert!(matches!(
            generate_in_session(&model, request, &cancel, |_| {}).await,
            Err(Error::Cancelled)
        ));
        assert!(model.seen.lock().unwrap().is_empty());
    }
}
