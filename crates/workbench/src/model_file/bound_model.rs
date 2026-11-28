use std::sync::Arc;

use pi_rs::{
    DetailedModelStream, EnrichedModelStream, Error, Message, Model, ModelRequest, ModelStream,
    PreparedToolDispatch, Result, ToolDeclaration, ToolDispatchContext, ToolTranscriptChange,
};
use uuid::Uuid;

/// A configured adapter cannot send its model-specific settings under another ID.
pub(super) struct BoundModel<'a> {
    model: Arc<str>,
    client: Arc<dyn Model + 'a>,
}

impl<'a> BoundModel<'a> {
    pub(super) fn new(model: &str, client: Arc<dyn Model + 'a>) -> Self {
        Self {
            model: model.into(),
            client,
        }
    }

    fn check(&self, request: &ModelRequest) -> Result<()> {
        if request.model == *self.model {
            Ok(())
        } else {
            Err(Error::Config(
                "request model does not match the granted thinking binding".into(),
            ))
        }
    }
}

#[async_trait::async_trait]
impl Model for BoundModel<'_> {
    fn supports_mid_conversation_system_messages(&self, model: &str) -> bool {
        model == &*self.model && self.client.supports_mid_conversation_system_messages(model)
    }

    fn uses_tool_transcript(&self, model: &str) -> bool {
        model == &*self.model && self.client.uses_tool_transcript(model)
    }

    fn context_identity(&self) -> Option<String> {
        self.client.context_identity().and_then(|identity| {
            pi_rs::protected_model_fingerprint(&("file-bound-model-v1", &*self.model, identity))
        })
    }

    fn can_reuse_measured_context(&self, request: &ModelRequest) -> bool {
        request.model == *self.model && self.client.can_reuse_measured_context(request)
    }

    async fn prepare_context<'a>(
        &'a self,
        request: &ModelRequest,
    ) -> Result<Option<Arc<dyn Model + 'a>>> {
        self.check(request)?;
        Ok(self.client.prepare_context(request).await?.map(|client| {
            Arc::new(BoundModel {
                model: self.model.clone(),
                client,
            }) as Arc<dyn Model + 'a>
        }))
    }

    async fn prepare_context_with_tool_transcript<'a>(
        &'a self,
        request: &ModelRequest,
        transcript: &[ToolTranscriptChange],
    ) -> Result<Option<Arc<dyn Model + 'a>>> {
        self.check(request)?;
        Ok(self
            .client
            .prepare_context_with_tool_transcript(request, transcript)
            .await?
            .map(|client| {
                Arc::new(BoundModel {
                    model: self.model.clone(),
                    client,
                }) as Arc<dyn Model + 'a>
            }))
    }

    async fn prepare_tool_dispatch<'a>(
        &'a self,
        request: &ModelRequest,
        declarations: &[ToolDeclaration],
        context: &ToolDispatchContext,
    ) -> Result<Option<Box<dyn PreparedToolDispatch + 'a>>> {
        self.check(request)?;
        self.client
            .prepare_tool_dispatch(request, declarations, context)
            .await
    }

    fn estimate_message_tokens(&self, request: &ModelRequest, message: &Message) -> u64 {
        self.client.estimate_message_tokens(request, message)
    }

    fn estimate_request_tokens(&self, request: &ModelRequest) -> u64 {
        self.client.estimate_request_tokens(request)
    }

    async fn context_window(&self, model: &str) -> Option<u32> {
        if model == &*self.model {
            self.client.context_window(model).await
        } else {
            None
        }
    }

    fn minimum_output_tokens(&self) -> u32 {
        self.client.minimum_output_tokens()
    }

    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        self.check(&request)?;
        self.client.stream(request).await
    }

    async fn stream_in_session(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.check(&request)?;
        self.client
            .stream_in_session(request_id, session_id, request)
            .await
    }

    async fn stream_in_session_with_identity(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.check(&request)?;
        self.client
            .stream_in_session_with_identity(request_id, session_id, external_id, request)
            .await
    }

    async fn stream_for_summary(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.check(&request)?;
        self.client
            .stream_for_summary(request_id, session_id, request)
            .await
    }

    async fn stream_enriched_in_session_with_identity(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<EnrichedModelStream> {
        self.check(&request)?;
        self.client
            .stream_enriched_in_session_with_identity(request_id, session_id, external_id, request)
            .await
    }

    async fn stream_enriched_for_summary(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<EnrichedModelStream> {
        self.check(&request)?;
        self.client
            .stream_enriched_for_summary(request_id, session_id, request)
            .await
    }

    async fn stream_detailed_in_session_with_identity(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<DetailedModelStream> {
        self.check(&request)?;
        self.client
            .stream_detailed_in_session_with_identity(request_id, session_id, external_id, request)
            .await
    }

    async fn stream_detailed_for_summary(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<DetailedModelStream> {
        self.check(&request)?;
        self.client
            .stream_detailed_for_summary(request_id, session_id, request)
            .await
    }

    async fn stream_detailed_with_tool_declarations(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        external_id: &str,
        request: ModelRequest,
        declarations: &[ToolDeclaration],
    ) -> Result<DetailedModelStream> {
        self.check(&request)?;
        self.client
            .stream_detailed_with_tool_declarations(
                request_id,
                session_id,
                external_id,
                request,
                declarations,
            )
            .await
    }

    async fn stream_detailed_summary_with_tool_declarations(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
        declarations: &[ToolDeclaration],
    ) -> Result<DetailedModelStream> {
        self.check(&request)?;
        self.client
            .stream_detailed_summary_with_tool_declarations(
                request_id,
                session_id,
                request,
                declarations,
            )
            .await
    }
}
