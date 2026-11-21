//! Apply workbench request policy and catalog usage estimates.
use futures_util::StreamExt;
use pi_rs::{
    EnrichedModelEvent, EnrichedModelStream, Message, Model, ModelEvent, ModelMetadata,
    ModelRequest, ModelStream, Result,
};
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{
    model_profiles::ChatPrice,
    providers::{Client, stream_with_declarations},
};

pub(super) const BLOCKED_IMAGE: &str = "Image reading is disabled.";
const NON_VISION_USER_IMAGE: &str = "(image omitted: model does not support images)";
const NON_VISION_TOOL_IMAGE: &str = "(tool image omitted: model does not support images)";

#[derive(Clone)]
pub(super) struct WorkbenchModel {
    pub strict_policy: Option<std::sync::Arc<FinalStrictPolicy>>,
    pub client: Client,
    pub block_images: bool,
    pub supports_images: Option<bool>,
    pub session_affinity: &'static [&'static str],
    pub price: Option<ChatPrice>,
}

#[derive(Clone)]
pub(super) struct FinalStrictPolicy {
    pub profiles: super::model_profiles::Profiles,
    pub provider: super::providers::Provider,
    pub selected_model: String,
    pub explicit_endpoint: bool,
    pub bound_support: Option<bool>,
}

impl FinalStrictPolicy {
    fn baseline(&self) -> Result<bool> {
        match self.bound_support {
            Some(value) => Ok(value),
            None => self.support(&self.selected_model),
        }
    }
    fn support(&self, model: &str) -> Result<bool> {
        let metadata = self.profiles.model(&self.provider, model);
        let adapter = self.profiles.model_adapter(&self.provider, model);
        if adapter
            != self
                .profiles
                .model_adapter(&self.provider, &self.selected_model)
        {
            return Err(pi_rs::Error::Config(
                "final request model uses a different API route".into(),
            ));
        }
        if !self.explicit_endpoint {
            let endpoint = |name: &str| {
                self.profiles
                    .model_endpoint(&self.provider, name)
                    .unwrap_or_else(|| self.profiles.endpoint(&self.provider))
            };
            if endpoint(model) != endpoint(&self.selected_model) {
                return Err(pi_rs::Error::Config(
                    "final request model uses a different endpoint route".into(),
                ));
            }
        }
        if model != self.selected_model
            && !self.profiles.has_model_definition(&self.provider, model)
        {
            return Ok(false);
        }
        Ok(
            if matches!(
                adapter,
                super::providers::Adapter::OpenAiResponses
                    | super::providers::Adapter::AzureResponses
                    | super::providers::Adapter::CodexResponses
            ) {
                self.profiles.responses_strict_tool_support(
                    &self.provider,
                    adapter,
                    metadata.as_ref(),
                )
            } else {
                self.profiles
                    .chat_compatibility(&self.provider, metadata.as_ref())
                    .supports_strict_mode()
                    .unwrap_or(adapter == super::providers::Adapter::MistralConversations)
            },
        )
    }
}

#[async_trait::async_trait]
impl Model for WorkbenchModel {
    fn minimum_output_tokens(&self) -> u32 {
        self.client.minimum_output_tokens()
    }

    async fn stream_detailed_with_tool_declarations(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        external_id: &str,
        request: ModelRequest,
        declarations: &[pi_rs::ToolDeclaration],
    ) -> Result<pi_rs::DetailedModelStream> {
        stream_with_declarations(
            self,
            request,
            declarations,
            pi_rs::ToolDispatchContext {
                request_id,
                session_id,
                external_id: external_id.into(),
                summary: false,
            },
        )
        .await
    }

    async fn stream_detailed_summary_with_tool_declarations(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
        declarations: &[pi_rs::ToolDeclaration],
    ) -> Result<pi_rs::DetailedModelStream> {
        stream_with_declarations(
            self,
            request,
            declarations,
            pi_rs::ToolDispatchContext {
                request_id,
                session_id,
                external_id: session_id.to_string(),
                summary: true,
            },
        )
        .await
    }

    async fn prepare_tool_dispatch<'a>(
        &'a self,
        request: &ModelRequest,
        declarations: &[pi_rs::ToolDeclaration],
        context: &pi_rs::ToolDispatchContext,
    ) -> Result<Option<Box<dyn pi_rs::PreparedToolDispatch + 'a>>> {
        if !self.client.supports_prepared_tool_dispatch() {
            pi_rs::validate_legacy_tool_declarations(request, declarations)?;
            return Ok(None);
        }
        let (client, headers) = if context.summary {
            (self.client.clone(), HeaderMap::new())
        } else {
            self.affinity_headers(&context.external_id).await?
        };
        let client = self.final_client(client, &request.model).await?;
        let filtered = self.filtered(request.clone());
        let Some(inner) = client
            .prepare_tool_dispatch_with_headers(&filtered, declarations, context, &headers, true)
            .await?
        else {
            pi_rs::validate_legacy_tool_declarations(request, declarations)?;
            return Ok(None);
        };
        pi_rs::validate_prepared_tool_dispatch(inner.as_ref(), context, &filtered, declarations)?;
        let snapshot = pi_rs::ToolDecisionSnapshot::capture_dispatch(
            context,
            request,
            inner.dispatch_request(),
            declarations,
            inner.effective_tools(),
        )
        .map_err(|error| pi_rs::Error::Config(error.to_string()))?;
        Ok(Some(Box::new(PreparedWorkbenchToolDispatch {
            inner,
            owner: self.clone(),
            snapshot,
        })))
    }

    async fn prepare_context<'a>(
        &'a self,
        request: &ModelRequest,
    ) -> Result<Option<std::sync::Arc<dyn Model + 'a>>> {
        let support = self
            .strict_policy
            .as_ref()
            .map(|policy| policy.support(&request.model))
            .transpose()?;
        let Some((client, identity)) = self.client.prepare_for_context(&request.model).await?
        else {
            return Ok(None);
        };
        let changed_support = match (&self.strict_policy, support) {
            (Some(policy), Some(final_support)) => {
                (policy.baseline()? != final_support).then_some(final_support)
            }
            _ => None,
        };
        let client = match changed_support {
            Some(supported) => client.with_final_strict_support(supported).await?,
            None => client,
        };
        let identity = identity.and_then(|identity| {
            let identity = match changed_support {
                Some(supported) => {
                    pi_rs::fingerprint_model_context(&("final-strict-v1", identity, supported))?
                }
                None => identity,
            };
            pi_rs::fingerprint_model_context(&(
                "workbench-policy-v2",
                identity,
                self.block_images,
                self.supports_images,
                self.session_affinity,
            ))
        });
        Ok(Some(std::sync::Arc::new(PreparedWorkbenchModel {
            bound_model: self.strict_policy.as_ref().map(|_| request.model.clone()),
            inner: WorkbenchModel {
                strict_policy: self.strict_policy.as_ref().map(|policy| {
                    let mut policy = (**policy).clone();
                    policy.bound_support = support;
                    std::sync::Arc::new(policy)
                }),
                client,
                block_images: self.block_images,
                supports_images: self.supports_images,
                session_affinity: self.session_affinity,
                price: self.price.clone(),
            },
            identity,
        })))
    }
    fn context_identity(&self) -> Option<String> {
        pi_rs::fingerprint_model_context(&(
            "workbench-policy-v2",
            self.client.context_identity()?,
            self.block_images,
            self.supports_images,
            self.session_affinity,
        ))
    }

    fn estimate_request_tokens(&self, request: &ModelRequest) -> u64 {
        self.client
            .estimate_request_tokens(&self.filtered(request.clone()))
    }
    fn estimate_message_tokens(&self, request: &ModelRequest, message: &Message) -> u64 {
        let mut one = request.clone();
        one.messages = vec![message.clone()];
        let filtered = self.filtered(one);
        filtered
            .messages
            .iter()
            .map(|m| self.client.estimate_message_tokens(&filtered, m))
            .fold(0_u64, u64::saturating_add)
    }

    async fn context_window(&self, model: &str) -> Option<u32> {
        self.client.context_window(model).await
    }

    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let client = self
            .final_client(self.client.clone(), &request.model)
            .await?;
        let stream = client.stream(self.filtered(request)).await?;
        Ok(self.estimate_usage(stream))
    }

    async fn stream_in_session(
        &self,
        _request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.stream_in_session_with_identity(
            _request_id,
            session_id,
            &session_id.to_string(),
            request,
        )
        .await
    }

    async fn stream_in_session_with_identity(
        &self,
        _request_id: Uuid,
        _session_id: Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        let (client, headers) = self.affinity_headers(external_id).await?;
        let client = self.final_client(client, &request.model).await?;
        let stream = client
            .stream_with_headers(self.filtered(request), &headers, false, Some(external_id))
            .await?;
        Ok(self.estimate_usage(stream))
    }
    async fn stream_for_summary(
        &self,
        _request_id: Uuid,
        _session_id: Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        let client = self
            .final_client(self.client.clone(), &request.model)
            .await?;
        let stream = client
            .stream_with_headers(self.filtered(request), &HeaderMap::new(), true, None)
            .await?;
        Ok(self.estimate_usage(stream))
    }
    async fn stream_enriched_in_session_with_identity(
        &self,
        _request_id: Uuid,
        _session_id: Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<EnrichedModelStream> {
        let (client, headers) = self.affinity_headers(external_id).await?;
        let client = self.final_client(client, &request.model).await?;
        let stream = client
            .stream_enriched_with_headers(
                self.filtered(request),
                &headers,
                false,
                Some(external_id),
            )
            .await?;
        Ok(self.estimate_enriched_usage(stream))
    }

    async fn stream_detailed_in_session_with_identity(
        &self,
        _request_id: Uuid,
        _session_id: Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<pi_rs::DetailedModelStream> {
        let (client, headers) = self.affinity_headers(external_id).await?;
        let client = self.final_client(client, &request.model).await?;
        let stream = client
            .stream_detailed_with_headers(
                self.filtered(request),
                &headers,
                false,
                Some(external_id),
            )
            .await?;
        Ok(self.estimate_detailed_usage(stream))
    }
    async fn stream_detailed_for_summary(
        &self,
        _request_id: Uuid,
        _session_id: Uuid,
        request: ModelRequest,
    ) -> Result<pi_rs::DetailedModelStream> {
        let client = self
            .final_client(self.client.clone(), &request.model)
            .await?;
        let stream = client
            .stream_detailed_with_headers(self.filtered(request), &HeaderMap::new(), true, None)
            .await?;
        Ok(self.estimate_detailed_usage(stream))
    }

    async fn stream_enriched_for_summary(
        &self,
        _request_id: Uuid,
        _session_id: Uuid,
        request: ModelRequest,
    ) -> Result<EnrichedModelStream> {
        let client = self
            .final_client(self.client.clone(), &request.model)
            .await?;
        let stream = client
            .stream_enriched_with_headers(self.filtered(request), &HeaderMap::new(), true, None)
            .await?;
        Ok(self.estimate_enriched_usage(stream))
    }
}

struct PreparedWorkbenchModel {
    bound_model: Option<String>,
    inner: WorkbenchModel,
    identity: Option<String>,
}

impl PreparedWorkbenchModel {
    fn check_model(&self, model: &str) -> Result<()> {
        if self
            .bound_model
            .as_deref()
            .is_some_and(|bound| bound != model)
        {
            return Err(pi_rs::Error::Config(
                "prepared context is bound to a different request model".into(),
            ));
        }
        Ok(())
    }
}

struct PreparedWorkbenchToolDispatch {
    inner: Box<dyn pi_rs::PreparedToolDispatch>,
    owner: WorkbenchModel,
    snapshot: pi_rs::ToolDecisionSnapshot,
}

#[async_trait::async_trait]
impl pi_rs::PreparedToolDispatch for PreparedWorkbenchToolDispatch {
    fn effective_tools(&self) -> &[pi_rs::EffectiveToolDeclaration] {
        self.inner.effective_tools()
    }

    fn decision_snapshot(&self) -> &pi_rs::ToolDecisionSnapshot {
        &self.snapshot
    }

    fn dispatch_request(&self) -> &ModelRequest {
        self.inner.dispatch_request()
    }

    fn reservation_request(&self) -> &ModelRequest {
        self.inner.reservation_request()
    }

    async fn dispatch(self: Box<Self>) -> Result<pi_rs::DetailedModelStream> {
        Ok(self
            .owner
            .estimate_detailed_usage(self.inner.dispatch().await?))
    }
}

#[async_trait::async_trait]
impl Model for PreparedWorkbenchModel {
    fn minimum_output_tokens(&self) -> u32 {
        self.inner.minimum_output_tokens()
    }

    async fn prepare_context<'a>(
        &'a self,
        request: &ModelRequest,
    ) -> Result<Option<std::sync::Arc<dyn Model + 'a>>> {
        self.inner.prepare_context(request).await
    }

    async fn stream_detailed_with_tool_declarations(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        external_id: &str,
        request: ModelRequest,
        declarations: &[pi_rs::ToolDeclaration],
    ) -> Result<pi_rs::DetailedModelStream> {
        self.check_model(&request.model)?;
        stream_with_declarations(
            self,
            request,
            declarations,
            pi_rs::ToolDispatchContext {
                request_id,
                session_id,
                external_id: external_id.into(),
                summary: false,
            },
        )
        .await
    }

    async fn stream_detailed_summary_with_tool_declarations(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
        declarations: &[pi_rs::ToolDeclaration],
    ) -> Result<pi_rs::DetailedModelStream> {
        self.check_model(&request.model)?;
        stream_with_declarations(
            self,
            request,
            declarations,
            pi_rs::ToolDispatchContext {
                request_id,
                session_id,
                external_id: session_id.to_string(),
                summary: true,
            },
        )
        .await
    }

    async fn prepare_tool_dispatch<'a>(
        &'a self,
        request: &ModelRequest,
        declarations: &[pi_rs::ToolDeclaration],
        context: &pi_rs::ToolDispatchContext,
    ) -> Result<Option<Box<dyn pi_rs::PreparedToolDispatch + 'a>>> {
        self.check_model(&request.model)?;
        self.inner
            .prepare_tool_dispatch(request, declarations, context)
            .await
    }

    fn context_identity(&self) -> Option<String> {
        self.identity.clone()
    }
    fn estimate_request_tokens(&self, request: &ModelRequest) -> u64 {
        if self.check_model(&request.model).is_err() {
            return u64::MAX;
        }
        self.inner.estimate_request_tokens(request)
    }
    fn estimate_message_tokens(&self, request: &ModelRequest, message: &Message) -> u64 {
        if self.check_model(&request.model).is_err() {
            return u64::MAX;
        }
        self.inner.estimate_message_tokens(request, message)
    }
    async fn context_window(&self, model: &str) -> Option<u32> {
        if self.check_model(model).is_err() {
            return None;
        }
        self.inner.context_window(model).await
    }
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        self.check_model(&request.model)?;
        self.inner.stream(request).await
    }
    async fn stream_in_session(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.check_model(&request.model)?;
        self.inner
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
        self.check_model(&request.model)?;
        self.inner
            .stream_in_session_with_identity(request_id, session_id, external_id, request)
            .await
    }
    async fn stream_for_summary(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.check_model(&request.model)?;
        self.inner
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
        self.check_model(&request.model)?;
        self.inner
            .stream_enriched_in_session_with_identity(request_id, session_id, external_id, request)
            .await
    }

    async fn stream_detailed_in_session_with_identity(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<pi_rs::DetailedModelStream> {
        self.check_model(&request.model)?;
        self.inner
            .stream_detailed_in_session_with_identity(request_id, session_id, external_id, request)
            .await
    }
    async fn stream_detailed_for_summary(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<pi_rs::DetailedModelStream> {
        self.check_model(&request.model)?;
        self.inner
            .stream_detailed_for_summary(request_id, session_id, request)
            .await
    }

    async fn stream_enriched_for_summary(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<EnrichedModelStream> {
        self.check_model(&request.model)?;
        self.inner
            .stream_enriched_for_summary(request_id, session_id, request)
            .await
    }
}

impl WorkbenchModel {
    async fn final_client(&self, client: Client, model: &str) -> Result<Client> {
        match &self.strict_policy {
            Some(policy) => {
                let support = policy.support(model)?;
                if support != policy.baseline()? {
                    client.with_final_strict_support(support).await
                } else {
                    Ok(client)
                }
            }
            _ => Ok(client),
        }
    }
    async fn affinity_headers(&self, external_id: &str) -> Result<(Client, HeaderMap)> {
        let mut headers = HeaderMap::new();
        let (client, configured) = if !self.session_affinity.is_empty() && !external_id.is_empty() {
            self.client.resolve_affinity_headers().await?
        } else {
            (self.client.clone(), HeaderMap::new())
        };
        if !external_id.is_empty()
            && self
                .session_affinity
                .iter()
                .any(|name| !configured.contains_key(*name))
        {
            // Fetch's Headers converts JS strings to ByteString (Latin-1),
            // then strips HTTP whitespace at the edges before validation.
            // Reject unrepresentable or invalid bytes rather than dispatching
            // a request with a different or missing affinity identity.
            let bytes = external_id
                .chars()
                .map(|ch| u8::try_from(u32::from(ch)))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| pi_rs::Error::Config("Invalid session affinity ID".into()))?;
            let whitespace = |byte: &u8| matches!(byte, b' ' | b'\t' | b'\r' | b'\n');
            let start = bytes
                .iter()
                .position(|byte| !whitespace(byte))
                .unwrap_or(bytes.len());
            let end = bytes
                .iter()
                .rposition(|byte| !whitespace(byte))
                .map_or(start, |index| index + 1);
            let trimmed = &bytes[start..end];
            let value = HeaderValue::from_bytes(trimmed)
                .map_err(|_| pi_rs::Error::Config("Invalid session affinity ID".into()))?;
            for &name in self.session_affinity {
                if !configured.contains_key(name) {
                    headers.insert(name, value.clone());
                }
            }
        }
        Ok((client, headers))
    }

    fn estimate_detailed_usage(
        &self,
        mut stream: pi_rs::DetailedModelStream,
    ) -> pi_rs::DetailedModelStream {
        let Some(price) = self.price.clone() else {
            return stream;
        };
        Box::pin(async_stream::try_stream! {
            let mut metadata = ModelMetadata::default();
            while let Some(event) = stream.next().await {
                let event = event?;
                if let EnrichedModelEvent::Legacy(ModelEvent::Metadata(current)) = &event.event { metadata.clone_from(current); }
                if let EnrichedModelEvent::Legacy(ModelEvent::Complete { usage: Some(usage), .. }) = &event.event
                    && metadata.cost_breakdown.is_none()
                    && let Some(breakdown) = price.estimate(*usage, &metadata)
                    && let Some(total) = breakdown.total()
                {
                    metadata.cost_breakdown = Some(breakdown);
                    if metadata.cost_usd.is_none() { metadata.cost_usd = Some(total); metadata.cost_from_provider = Some(false); }
                    yield pi_rs::DetailedModelEvent::from(EnrichedModelEvent::Legacy(ModelEvent::Metadata(metadata.clone())));
                }
                yield event;
            }
        })
    }

    fn estimate_enriched_usage(&self, mut stream: EnrichedModelStream) -> EnrichedModelStream {
        let Some(price) = self.price.clone() else {
            return stream;
        };
        Box::pin(async_stream::try_stream! {
            let mut metadata = ModelMetadata::default();
            while let Some(event) = stream.next().await {
                let event = event?;
                if let EnrichedModelEvent::Legacy(ModelEvent::Metadata(current)) = &event {
                    metadata.clone_from(current);
                }
                if let EnrichedModelEvent::Legacy(ModelEvent::Complete { usage: Some(usage), .. }) = &event
                    && metadata.cost_breakdown.is_none()
                    && let Some(breakdown) = price.estimate(*usage, &metadata)
                    && let Some(total) = breakdown.total()
                {
                    metadata.cost_breakdown = Some(breakdown);
                    if metadata.cost_usd.is_none() {
                        metadata.cost_usd = Some(total);
                        metadata.cost_from_provider = Some(false);
                    }
                    yield EnrichedModelEvent::Legacy(ModelEvent::Metadata(metadata.clone()));
                }
                yield event;
            }
        })
    }

    fn estimate_usage(&self, mut stream: ModelStream) -> ModelStream {
        let Some(price) = self.price.clone() else {
            return stream;
        };
        Box::pin(async_stream::try_stream! {
            let mut metadata = ModelMetadata::default();
            while let Some(event) = stream.next().await {
                let event = event?;
                if let ModelEvent::Metadata(current) = &event {
                    metadata.clone_from(current);
                }
                if let ModelEvent::Complete { usage: Some(usage), .. } = &event
                    && metadata.cost_breakdown.is_none()
                    && let Some(breakdown) = price.estimate(*usage, &metadata)
                    && let Some(total) = breakdown.total()
                {
                    metadata.cost_breakdown = Some(breakdown);
                    if metadata.cost_usd.is_none() {
                        metadata.cost_usd = Some(total);
                        metadata.cost_from_provider = Some(false);
                    }
                    yield ModelEvent::Metadata(metadata.clone());
                }
                yield event;
            }
        })
    }

    fn filtered(&self, mut request: ModelRequest) -> ModelRequest {
        if self.block_images {
            remove_images(&mut request.messages, BLOCKED_IMAGE, BLOCKED_IMAGE);
        } else if self.supports_images == Some(false) {
            remove_images(
                &mut request.messages,
                NON_VISION_USER_IMAGE,
                NON_VISION_TOOL_IMAGE,
            );
        }
        request
    }
}

fn remove_images(messages: &mut [Message], user_placeholder: &str, tool_placeholder: &str) {
    for message in messages {
        match message {
            Message::UserWithImages { content, images } if !images.is_empty() => {
                let mut content = std::mem::take(content);
                if !content.is_empty() {
                    content.push_str("\n\n");
                }
                content.push_str(user_placeholder);
                *message = Message::User { content };
            }
            Message::Tool {
                content,
                is_structured,
                ..
            } => {
                if pi_rs::tool_content_is_structured(content, *is_structured)
                    && let Some(filtered) = remove_tool_images(content, tool_placeholder)
                {
                    *content = filtered;
                }
            }
            _ => {}
        }
    }
}

fn remove_tool_images(content: &str, placeholder: &str) -> Option<String> {
    let mut result: Value = serde_json::from_str(content).ok()?;
    let blocks = result.get_mut("data")?.get_mut("content")?.as_array_mut()?;
    let mut previous_placeholder = false;
    let mut changed = false;
    blocks.retain_mut(|block| {
        if block["type"] == "image" {
            changed = true;
            if previous_placeholder {
                return false;
            }
            *block = json!({"type":"text","text":placeholder});
        }
        previous_placeholder = block["type"] == "text" && block["text"] == placeholder;
        true
    });
    changed.then(|| result.to_string())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn enriched_usage_estimation_preserves_reasoning_events() {
        use futures_util::StreamExt;
        let model = super::WorkbenchModel {
            strict_policy: None,
            client: super::Client::Google(
                pi_rs::google::Google::new("https://example.com", "key").unwrap(),
            ),
            block_images: false,
            supports_images: None,
            session_affinity: &[],
            price: Some(
                serde_json::from_value(serde_json::json!({"input": 2.0, "output": 4.0})).unwrap(),
            ),
        };
        let events = vec![
            Ok(pi_rs::EnrichedModelEvent::ReasoningStart { index: 3 }),
            Ok(pi_rs::EnrichedModelEvent::Legacy(
                pi_rs::ModelEvent::Complete {
                    calls: vec![],
                    usage: Some(pi_rs::Usage {
                        input_tokens: 100,
                        output_tokens: 50,
                    }),
                },
            )),
        ];
        let events: Vec<_> = model
            .estimate_enriched_usage(Box::pin(futures_util::stream::iter(events)))
            .collect()
            .await;
        assert!(matches!(
            &events[0],
            Ok(pi_rs::EnrichedModelEvent::ReasoningStart { index: 3 })
        ));
        assert!(
            matches!(&events[1], Ok(pi_rs::EnrichedModelEvent::Legacy(pi_rs::ModelEvent::Metadata(metadata))) if metadata.cost_breakdown.is_some() && metadata.cost_from_provider == Some(false))
        );
        assert!(matches!(
            &events[2],
            Ok(pi_rs::EnrichedModelEvent::Legacy(
                pi_rs::ModelEvent::Complete { .. }
            ))
        ));
        let events = vec![
            Ok(pi_rs::DetailedModelEvent::redacted_start(3)),
            Ok(pi_rs::DetailedModelEvent::from(
                pi_rs::EnrichedModelEvent::Legacy(pi_rs::ModelEvent::Complete {
                    calls: vec![],
                    usage: Some(pi_rs::Usage {
                        input_tokens: 100,
                        output_tokens: 50,
                    }),
                }),
            )),
        ];
        let events: Vec<_> = model
            .estimate_detailed_usage(Box::pin(futures_util::stream::iter(events)))
            .collect()
            .await;
        assert!(events[0].as_ref().unwrap().is_redacted_start());
        assert!(
            matches!(&events[1].as_ref().unwrap().event, pi_rs::EnrichedModelEvent::Legacy(pi_rs::ModelEvent::Metadata(metadata)) if metadata.cost_breakdown.is_some())
        );
    }
    use super::*;

    #[tokio::test]
    async fn responses_session_cache_key_uses_external_identity_and_skips_summaries() {
        use std::io::{Read, Write};
        for (identity, summary, expected) in [
            (
                "session-one".to_owned(),
                false,
                Some("session-one".to_owned()),
            ),
            (
                format!("{}tail", "猫".repeat(64)),
                false,
                Some("猫".repeat(64)),
            ),
            (String::new(), false, None),
            ("session-one".to_owned(), true, None),
        ] {
            for enriched in [false, true] {
                let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let address = listener.local_addr().unwrap();
                listener.set_nonblocking(true).unwrap();
                let server = std::thread::spawn(move || {
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                    let mut socket = loop {
                        match listener.accept() {
                            Ok((socket, _)) => break socket,
                            Err(error)
                                if error.kind() == std::io::ErrorKind::WouldBlock
                                    && std::time::Instant::now() < deadline =>
                            {
                                std::thread::sleep(std::time::Duration::from_millis(10))
                            }
                            Err(error) => panic!("fixture accept failed: {error}"),
                        }
                    };
                    socket.set_nonblocking(false).unwrap();
                    socket
                        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                        .unwrap();
                    let mut bytes = Vec::new();
                    let mut buffer = [0; 4096];
                    let end = loop {
                        let n = socket.read(&mut buffer).unwrap();
                        assert!(n > 0);
                        bytes.extend_from_slice(&buffer[..n]);
                        if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos + 4;
                        }
                    };
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse().unwrap())
                        })
                        .unwrap();
                    while bytes.len() < end + length {
                        let n = socket.read(&mut buffer).unwrap();
                        assert!(n > 0);
                        bytes.extend_from_slice(&buffer[..n]);
                    }
                    let body: serde_json::Value =
                        serde_json::from_slice(&bytes[end..end + length]).unwrap();
                    socket
                        .write_all(
                            b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .unwrap();
                    body
                });
                let model = WorkbenchModel {
                    strict_policy: None,
                    client: Client::OpenAi(
                        pi_rs::openai::OpenAiResponses::new_api(
                            &format!("http://{address}/responses"),
                            "fixture".into(),
                        )
                        .unwrap(),
                    ),
                    block_images: false,
                    supports_images: None,
                    session_affinity: &[],
                    price: None,
                };
                let request = ModelRequest {
                    model: "fixture".into(),
                    messages: vec![],
                    tools: vec![],
                    max_output_tokens: 1,
                };
                if enriched && summary {
                    let _ = model
                        .stream_enriched_for_summary(Uuid::new_v4(), Uuid::new_v4(), request)
                        .await;
                } else if enriched {
                    let _ = model
                        .stream_enriched_in_session_with_identity(
                            Uuid::new_v4(),
                            Uuid::new_v4(),
                            &identity,
                            request,
                        )
                        .await;
                } else if summary {
                    let _ = model
                        .stream_for_summary(Uuid::new_v4(), Uuid::new_v4(), request)
                        .await;
                } else {
                    let _ = model
                        .stream_in_session_with_identity(
                            Uuid::new_v4(),
                            Uuid::new_v4(),
                            &identity,
                            request,
                        )
                        .await;
                }
                let body = server.join().unwrap();
                assert_eq!(
                    body.get("prompt_cache_key").and_then(|v| v.as_str()),
                    expected.as_deref()
                );
            }
        }
    }

    #[tokio::test]
    async fn responses_session_headers_respect_explicit_values_and_summary_boundary() {
        use std::io::{Read, Write};
        for (summary, explicit, enriched) in [
            (false, false, false),
            (false, true, false),
            (true, false, false),
            (false, false, true),
            (false, true, true),
            (true, false, true),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                    let mut block = [0; 2048];
                    let count = socket.read(&mut block).unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&block[..count]);
                }
                socket
                    .write_all(
                        b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                String::from_utf8(bytes).unwrap().to_ascii_lowercase()
            });
            let mut headers = HeaderMap::new();
            if explicit {
                headers.insert("session-id", "explicit".parse().unwrap());
            }
            let model = WorkbenchModel {
                strict_policy: None,
                client: Client::OpenAi(
                    pi_rs::openai::OpenAiResponses::new_api(
                        &format!("http://{address}/responses"),
                        "fixture".into(),
                    )
                    .unwrap()
                    .with_headers(headers),
                ),
                block_images: false,
                supports_images: None,
                session_affinity: &["session-id", "x-client-request-id"],
                price: None,
            };
            let request = ModelRequest {
                model: "fixture".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 1,
            };
            match (summary, enriched) {
                (true, true) => {
                    let _ = model
                        .stream_enriched_for_summary(Uuid::new_v4(), Uuid::new_v4(), request)
                        .await;
                }
                (true, false) => {
                    let _ = model
                        .stream_for_summary(Uuid::new_v4(), Uuid::new_v4(), request)
                        .await;
                }
                (false, true) => {
                    let _ = model
                        .stream_enriched_in_session_with_identity(
                            Uuid::new_v4(),
                            Uuid::new_v4(),
                            "session-one",
                            request,
                        )
                        .await;
                }
                (false, false) => {
                    let _ = model
                        .stream_in_session_with_identity(
                            Uuid::new_v4(),
                            Uuid::new_v4(),
                            "session-one",
                            request,
                        )
                        .await;
                }
            }
            let headers = server.join().unwrap();
            assert_eq!(
                headers.contains("x-client-request-id: session-one\r\n"),
                !summary
            );
            assert_eq!(
                headers.contains("session-id: session-one\r\n"),
                !summary && !explicit
            );
            assert_eq!(headers.contains("session-id: explicit\r\n"), explicit);
        }
    }

    #[tokio::test]
    async fn external_identity_is_used_for_affinity_header() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        for (identity, affinity, expected_header) in [
            ("custom-id.1", true, Some(&b"custom-id.1"[..])),
            ("", true, None),
            ("café", true, Some(&b"caf\xe9"[..])),
            ("\u{80}ÿ", true, Some(&b"\x80\xff"[..])),
            (" \tcafé\r\n", true, Some(&b"caf\xe9"[..])),
            ("a\tb", true, Some(&b"a\tb"[..])),
            (" \t ", true, Some(&b""[..])),
            ("日本", false, None),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("import.jsonl");
            std::fs::write(
                &path,
                format!(
                    "{}\n",
                    serde_json::json!({"type":"session", "version":3, "id": identity})
                ),
            )
            .unwrap();
            let session = crate::session_import::read(&path).unwrap();
            assert_eq!(session.external_session_id(), identity);
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();
            let server = std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && std::time::Instant::now() < deadline =>
                        {
                            std::thread::sleep(std::time::Duration::from_millis(10))
                        }
                        Err(error) => panic!("fixture accept failed: {error}"),
                    }
                };
                // Accepted sockets can inherit the listener's nonblocking mode
                // (notably on macOS); read_timeout requires a blocking socket.
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                    let mut block = [0; 1024];
                    let count = socket.read(&mut block).unwrap();
                    assert_ne!(count, 0);
                    bytes.extend_from_slice(&block[..count]);
                }
                socket
                    .write_all(
                        b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                bytes
            });
            let model = WorkbenchModel {
                strict_policy: None,
                client: Client::Ollama(
                    pi_rs::ollama::Ollama::new(
                        &format!("http://{address}"),
                        Some("fixture-key".into()),
                    )
                    .unwrap()
                    .with_native_probes(false),
                ),
                block_images: false,
                supports_images: None,
                session_affinity: if affinity { &["x-session-id"] } else { &[] },
                price: None,
            };
            let request = ModelRequest {
                model: "fixture".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 1,
            };
            let _ = model
                .stream_in_session_with_identity(
                    Uuid::new_v4(),
                    session.session_id(),
                    &session.external_session_id(),
                    request,
                )
                .await;
            let headers = server.join().unwrap();
            let value = headers.split(|byte| *byte == b'\n').find_map(|line| {
                let colon = line.iter().position(|byte| *byte == b':')?;
                if !line[..colon].eq_ignore_ascii_case(b"x-session-id") {
                    return None;
                }
                let value = line[colon + 1..]
                    .strip_suffix(b"\r")
                    .unwrap_or(&line[colon + 1..]);
                Some(value.strip_prefix(b" ").unwrap_or(value).to_vec())
            });
            assert_eq!(value.as_deref(), expected_header, "identity {identity:?}");
        }
    }

    #[tokio::test]
    async fn unrepresentable_affinity_identity_fails_before_dispatch() {
        use std::net::TcpListener;
        for identity in [
            "日本",
            "\u{100}",
            "🤖",
            "unsafe\r\nx-injected: yes",
            "invalid\0id",
            "bad\x7fid",
            "\x0cid",
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let model = WorkbenchModel {
                strict_policy: None,
                client: Client::Ollama(
                    pi_rs::ollama::Ollama::new(
                        &format!("http://{address}"),
                        Some("fixture-key".into()),
                    )
                    .unwrap()
                    .with_native_probes(false),
                ),
                block_images: false,
                supports_images: None,
                session_affinity: &["x-session-id"],
                price: None,
            };
            let request = ModelRequest {
                model: "fixture".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 1,
            };
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                model.stream_in_session_with_identity(
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    identity,
                    request,
                ),
            )
            .await
            .expect("invalid ID must fail before network access");
            assert!(matches!(result, Err(pi_rs::Error::Config(_))));
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
    }

    #[tokio::test]
    async fn explicit_affinity_headers_override_before_default_validation() {
        use super::super::providers::{Adapter, Provider};
        use std::io::{Read, Write};
        use std::net::TcpListener;
        const MULTI: &[&str] = &["session_id", "x-client-request-id", "x-session-affinity"];
        for (deferred, names, explicit, identity, dispatch) in [
            (
                false,
                MULTI,
                vec![
                    ("SESSION_ID", "s"),
                    ("X-CLIENT-REQUEST-ID", "r"),
                    ("X-SESSION-AFFINITY", "a"),
                ],
                "日本",
                true,
            ),
            (false, MULTI, vec![("SESSION_ID", "s")], "日本", false),
            (false, MULTI, vec![("SESSION_ID", "s")], "café", true),
            (
                false,
                &["x-session-id"][..],
                vec![("X-SESSION-ID", "override")],
                "日本",
                true,
            ),
            (
                true,
                &["x-session-id"][..],
                vec![("x-session-id", "override")],
                "日本",
                true,
            ),
        ] {
            let listener = std::sync::Arc::new(TcpListener::bind("127.0.0.1:0").unwrap());
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let server = if dispatch {
                let listener = listener.clone();
                Some(std::thread::spawn(move || {
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                    let mut socket = loop {
                        match listener.accept() {
                            Ok((socket, _)) => break socket,
                            Err(error)
                                if error.kind() == std::io::ErrorKind::WouldBlock
                                    && std::time::Instant::now() < deadline =>
                            {
                                std::thread::sleep(std::time::Duration::from_millis(10))
                            }
                            Err(error) => panic!("fixture accept failed: {error}"),
                        }
                    };
                    socket.set_nonblocking(false).unwrap();
                    socket
                        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                        .unwrap();
                    let mut bytes = Vec::new();
                    while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                        let mut block = [0; 1024];
                        let count = socket.read(&mut block).unwrap();
                        assert_ne!(count, 0);
                        bytes.extend_from_slice(&block[..count]);
                    }
                    socket
                        .write_all(
                            b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .unwrap();
                    bytes
                }))
            } else {
                None
            };
            let client = if deferred {
                let environment = serde_json::json!({"AFFINITY_OVERRIDE":"override"});
                let headers = super::super::model_profiles::RequestHeaders::for_test(
                    "x-session-id",
                    "${AFFINITY_OVERRIDE}",
                )
                .with_environment(environment.as_object());
                Client::deferred(
                    Provider::Ollama,
                    Adapter::ChatCompletions,
                    &format!("http://{address}"),
                    None,
                    headers,
                )
                .with_context_probes(false)
            } else {
                let mut headers = HeaderMap::new();
                for (name, value) in &explicit {
                    headers.insert(
                        reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                        HeaderValue::from_str(value).unwrap(),
                    );
                }
                Client::Ollama(
                    pi_rs::ollama::Ollama::new(&format!("http://{address}"), None)
                        .unwrap()
                        .with_native_probes(false)
                        .with_headers(headers),
                )
            };
            let model = WorkbenchModel {
                strict_policy: None,
                client,
                block_images: false,
                supports_images: None,
                session_affinity: names,
                price: None,
            };
            let request = ModelRequest {
                model: "fixture".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 1,
            };
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                model.stream_in_session_with_identity(
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    identity,
                    request,
                ),
            )
            .await
            .expect("fixture timed out");
            if let Some(server) = server {
                assert!(
                    !matches!(result, Err(pi_rs::Error::Config(_))),
                    "explicit headers should allow dispatch"
                );
                let bytes = server.join().unwrap();
                let get = |name: &str| {
                    bytes.split(|byte| *byte == b'\n').find_map(|line| {
                        let colon = line.iter().position(|byte| *byte == b':')?;
                        if !line[..colon].eq_ignore_ascii_case(name.as_bytes()) {
                            return None;
                        }
                        let value = line[colon + 1..]
                            .strip_suffix(b"\r")
                            .unwrap_or(&line[colon + 1..]);
                        Some(value.strip_prefix(b" ").unwrap_or(value).to_vec())
                    })
                };
                for (name, value) in &explicit {
                    assert_eq!(get(name).as_deref(), Some(value.as_bytes()));
                }
                if identity == "café" {
                    assert_eq!(get("x-client-request-id").as_deref(), Some(&b"caf\xe9"[..]));
                    assert_eq!(get("x-session-affinity").as_deref(), Some(&b"caf\xe9"[..]));
                }
            } else {
                assert!(matches!(result, Err(pi_rs::Error::Config(_))));
                assert_eq!(
                    listener.accept().unwrap_err().kind(),
                    std::io::ErrorKind::WouldBlock
                );
            }
        }
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn effective_context_identity_includes_image_policy_and_adapter() {
        let make = |endpoint: &str, block_images, supports_images| WorkbenchModel {
            strict_policy: None,
            client: Client::Google(pi_rs::google::Google::new(endpoint, "fixture-key").unwrap()),
            block_images,
            supports_images,
            session_affinity: &[],
            price: None,
        };
        let original = make("https://example.com/v1", false, Some(true));
        let blocked = make("https://example.com/v1", true, Some(true));
        let nonvision = make("https://example.com/v1", false, Some(false));
        let other = make("https://other.example.com/v1", false, Some(true));
        assert!(original.context_identity().is_some());
        assert_ne!(original.context_identity(), blocked.context_identity());
        assert_ne!(original.context_identity(), nonvision.context_identity());
        assert_ne!(original.context_identity(), other.context_identity());
        let request = ModelRequest {
            model: "fixture".into(),
            tools: vec![],
            max_output_tokens: 1,
            messages: vec![Message::UserWithImages {
                content: "look".into(),
                images: vec![pi_rs::ImageContent {
                    mime_type: "image/png".into(),
                    data: "AAAA".into(),
                }],
            }],
        };
        assert_ne!(
            original.filtered(request.clone()).messages,
            blocked.filtered(request).messages
        );
    }

    #[test]
    fn image_policy_replaces_only_model_visible_image_blocks() {
        let original = json!({"data":{"content":[
            {"type":"text","text":"before"},
            {"type":"image","mimeType":"image/png","data":"private-one"},
            {"type":"image","data":"private-two"},
            {"type":"text","text":"after"},
        ]}});
        let mut messages = vec![
            Message::UserWithImages {
                content: "prompt".into(),
                images: vec![pi_rs::ImageContent {
                    data: "private-user".into(),
                    mime_type: "image/png".into(),
                }],
            },
            Message::Tool {
                is_structured: Some(true),
                is_error: false,
                call_id: "call".into(),
                content: original.to_string(),
            },
        ];
        remove_images(&mut messages, BLOCKED_IMAGE, BLOCKED_IMAGE);
        assert_eq!(
            messages[0],
            Message::User {
                content: format!("prompt\n\n{BLOCKED_IMAGE}")
            }
        );
        let Message::Tool { content, .. } = &messages[1] else {
            panic!("expected tool message")
        };
        assert_eq!(
            serde_json::from_str::<Value>(content).unwrap()["data"]["content"],
            json!([{"type":"text","text":"before"},{"type":"text","text":BLOCKED_IMAGE},{"type":"text","text":"after"}]),
        );
    }

    #[test]
    fn plain_json_tool_text_is_not_changed_by_image_policy() {
        let content = r#"{ "data": { "content": [{"type":"image","mimeType":"image/png","data":"private"}] } }"#;
        let mut messages = vec![Message::Tool {
            call_id: "id".into(),
            content: content.into(),
            is_error: false,
            is_structured: Some(false),
        }];
        remove_images(&mut messages, BLOCKED_IMAGE, BLOCKED_IMAGE);
        assert!(matches!(&messages[0], Message::Tool { content: actual, .. } if actual == content));
        let legacy =
            serde_json::from_value(json!({"role":"tool", "call_id":"id", "content":content}))
                .unwrap();
        let mut messages = vec![legacy];
        remove_images(&mut messages, BLOCKED_IMAGE, BLOCKED_IMAGE);
        let Message::Tool {
            content: filtered,
            is_structured: None,
            ..
        } = &messages[0]
        else {
            panic!("legacy tool message missing")
        };
        assert!(!filtered.contains("private"));
        assert!(filtered.contains(BLOCKED_IMAGE));
    }

    #[test]
    fn non_vision_models_receive_reference_placeholders_for_user_and_tool_images() {
        let mut messages = vec![
            Message::UserWithImages {
                content: "look".into(),
                images: vec![pi_rs::ImageContent { data: "private".into(), mime_type: "image/png".into() }],
            },
            Message::Tool {
                is_structured: Some(true),
                is_error: false, call_id: "call".into(),
                content: json!({"data":{"content":[{"type":"text","text":"before"},{"type":"image","data":"private"},{"type":"image","data":"private"},{"type":"text","text":"after"}]}}).to_string(),
            },
        ];
        remove_images(&mut messages, NON_VISION_USER_IMAGE, NON_VISION_TOOL_IMAGE);
        assert_eq!(
            messages[0],
            Message::User {
                content: format!("look\n\n{NON_VISION_USER_IMAGE}")
            }
        );
        let Message::Tool { content, .. } = &messages[1] else {
            panic!("expected tool result")
        };
        assert_eq!(
            serde_json::from_str::<Value>(content).unwrap()["data"]["content"],
            json!([
                {"type":"text","text":"before"},
                {"type":"text","text":NON_VISION_TOOL_IMAGE},
                {"type":"text","text":"after"}
            ])
        );
    }
}

#[cfg(test)]
mod final_strict_policy_tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn chatgpt_responses_prepares_strict_tools_unless_profile_disables_them() {
        use clap::Parser;

        let endpoint = "http://127.0.0.1:1234/v1/responses";
        let mut args = crate::Cli::parse_from(["pi"]);
        args.model_profiles = crate::model_profiles::Profiles::parse(&json!({"providers":{
            "openai-chatgpt": { "api":"openai-responses", "baseUrl":endpoint,
                "models":[{"id":"default"},{"id":"off","compat":{"supportsStrictMode":false}}]}
        }}))
        .unwrap();
        args.provider = args.model_profiles.provider("openai-chatgpt").unwrap();
        args.model = "default".into();
        args.endpoint = endpoint.into();
        args.api_key = Some("fixture".into());
        let model = crate::runtime::model_client(&args, false, false).unwrap();
        let tool = pi_rs::ToolSpec {
            name: "pick".into(),
            description: String::new(),
            parameters: json!({"type":"object","properties":{"name":{"type":"string"}}}),
        };
        let declaration = pi_rs::ToolDeclaration {
            spec: tool.clone(),
            constrained_sampling: Some(pi_rs::ToolConstraint::JsonSchema {
                strict: pi_rs::StrictToolPolicy::Prefer,
            }),
        };
        let context = pi_rs::ToolDispatchContext {
            request_id: Uuid::nil(),
            session_id: Uuid::nil(),
            external_id: "fixture".into(),
            summary: false,
        };
        for (id, strict) in [("default", true), ("off", false), ("unknown", false)] {
            let request = ModelRequest {
                model: id.into(),
                messages: Vec::new(),
                tools: vec![tool.clone()],
                max_output_tokens: 10,
            };
            let prepared = model
                .prepare_tool_dispatch(&request, std::slice::from_ref(&declaration), &context)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                matches!(
                    prepared.effective_tools()[0].sampling,
                    pi_rs::EffectiveToolSampling::StrictJsonSchema { .. }
                ),
                strict,
                "{id}"
            );
        }
    }

    #[test]
    fn codex_final_model_policy_uses_the_responses_strict_default() {
        let profiles = super::super::model_profiles::Profiles::parse(&json!({
            "providers":{"openai-codex":{
                "models":[{"id":"without-strict","compat":{"supportsStrictMode":false}}]
            }}
        }))
        .unwrap();
        let policy = FinalStrictPolicy {
            provider: profiles.provider("openai-codex").unwrap(),
            profiles,
            selected_model: "gpt-6.1-sol".into(),
            explicit_endpoint: false,
            bound_support: None,
        };
        assert!(policy.baseline().unwrap());
        assert!(!policy.support("without-strict").unwrap());
        assert!(!policy.support("unknown").unwrap());
    }

    #[test]
    fn rewritten_model_must_use_same_route() {
        let profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"fixture":{
            "api":"openai-completions", "baseUrl":"http://127.0.0.1:1234/v1", "compat":{"supportsStrictMode":true},
            "models":[{"id":"first"},{"id":"other-route","baseUrl":"http://127.0.0.1:5678/v1"},{"id":"other-api","api":"openai-responses"}],
            "modelOverrides":{"partial":{"compat":{"supportsReasoningEffort":true}},"partial-true":{"compat":{"supportsStrictMode":true}}}
        }}})).unwrap();
        let policy = FinalStrictPolicy {
            provider: profiles.provider("fixture").unwrap(),
            profiles,
            selected_model: "first".into(),
            explicit_endpoint: false,
            bound_support: None,
        };
        assert!(policy.support("first").unwrap());
        assert!(policy.support("other-route").is_err());
        assert!(policy.support("other-api").is_err());
        assert!(!policy.support("unknown").unwrap());
        assert!(policy.profiles.model(&policy.provider, "partial").is_some());
        assert!(!policy.support("partial").unwrap());
        assert!(!policy.support("partial-true").unwrap());
        let other = FinalStrictPolicy {
            selected_model: "other-route".into(),
            ..policy.clone()
        };
        assert!(other.support("unknown").is_err());
        assert!(other.support("partial").is_err());
        let explicit = FinalStrictPolicy {
            explicit_endpoint: true,
            ..policy
        };
        assert!(explicit.support("other-route").unwrap());
        assert!(explicit.support("other-api").is_err());
    }
}

#[cfg(test)]
mod final_stream_policy_tests {
    use super::*;
    use crate::model_auth::Credential;
    use crate::model_profiles::RequestHeaders;
    use crate::providers::{Adapter, Client};
    use clap::Parser;
    use std::{
        io::{Read, Write},
        sync::Arc,
    };

    #[tokio::test]
    async fn final_model_applies_to_all_direct_streams() {
        for deferred in [false, true] {
            for prepared in [false, true] {
                for target in ["yes", "no"] {
                    for mode in 0..8 {
                        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                        let endpoint = format!(
                            "http://{}/v1/chat/completions",
                            listener.local_addr().unwrap()
                        );
                        let mut args = crate::Cli::parse_from(["pi"]);
                        args.model_profiles = crate::model_profiles::Profiles::parse(&json!({"providers":{"fixture":{"api":"openai-completions","baseUrl":endpoint,"models":[{"id":"yes","compat":{"supportsStrictMode":true}},{"id":"no","compat":{"supportsStrictMode":false}}]}}})).unwrap();
                        args.provider = args.model_profiles.provider("fixture").unwrap();
                        args.model = if target == "yes" { "no" } else { "yes" }.into();
                        let client = if deferred {
                            Client::deferred(
                                args.provider.clone(),
                                Adapter::ChatCompletions,
                                &endpoint,
                                Some(Credential::Literal("fixture".into())),
                                RequestHeaders::default(),
                            )
                        } else {
                            Client::Ollama(
                                pi_rs::ollama::Ollama::new(&endpoint, Some("fixture".into()))
                                    .unwrap()
                                    .with_native_probes(false),
                            )
                        }
                        .with_model_options(&args, false)
                        .unwrap();
                        let model = WorkbenchModel {
                            strict_policy: Some(Arc::new(FinalStrictPolicy {
                                profiles: args.model_profiles.clone(),
                                provider: args.provider.clone(),
                                selected_model: args.model.clone(),
                                explicit_endpoint: false,
                                bound_support: None,
                            })),
                            client,
                            block_images: false,
                            supports_images: None,
                            session_affinity: &[],
                            price: None,
                        };
                        let request = ModelRequest {
                            model: target.into(),
                            messages: vec![],
                            tools: vec![pi_rs::ToolSpec {
                                name: "t".into(),
                                description: String::new(),
                                parameters: json!({"type":"object"}),
                            }],
                            max_output_tokens: 10,
                        };
                        let resolved = if prepared {
                            model.prepare_context(&request).await.unwrap()
                        } else {
                            None
                        };
                        let active: &dyn Model = resolved.as_deref().unwrap_or(&model);
                        let server = std::thread::spawn(move || {
                            let (mut stream, _) = listener.accept().unwrap();
                            stream
                                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                                .unwrap();
                            let mut bytes = Vec::new();
                            let (offset, len) = loop {
                                let mut buf = [0; 4096];
                                let n = stream.read(&mut buf).unwrap();
                                assert!(n > 0);
                                bytes.extend_from_slice(&buf[..n]);
                                if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                                    let head = String::from_utf8_lossy(&bytes[..pos]);
                                    let len = head
                                        .lines()
                                        .find_map(|line| {
                                            line.to_ascii_lowercase()
                                                .strip_prefix("content-length:")
                                                .map(|x| x.trim().parse::<usize>().unwrap())
                                        })
                                        .unwrap();
                                    break (pos + 4, len);
                                }
                            };
                            while bytes.len() < offset + len {
                                let mut buf = [0; 4096];
                                let n = stream.read(&mut buf).unwrap();
                                assert!(n > 0);
                                bytes.extend_from_slice(&buf[..n]);
                            }
                            let body: Value =
                                serde_json::from_slice(&bytes[offset..offset + len]).unwrap();
                            stream.write_all(b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                            body
                        });
                        let id = Uuid::new_v4();
                        match mode {
                            0 => {
                                let _ = active.stream(request).await;
                            }
                            1 => {
                                let _ = active.stream_in_session(id, id, request).await;
                            }
                            2 => {
                                let _ = active
                                    .stream_in_session_with_identity(id, id, "s", request)
                                    .await;
                            }
                            3 => {
                                let _ = active.stream_for_summary(id, id, request).await;
                            }
                            4 => {
                                let _ = active
                                    .stream_enriched_in_session_with_identity(id, id, "s", request)
                                    .await;
                            }
                            5 => {
                                let _ = active
                                    .stream_detailed_in_session_with_identity(id, id, "s", request)
                                    .await;
                            }
                            6 => {
                                let _ = active.stream_detailed_for_summary(id, id, request).await;
                            }
                            7 => {
                                let _ = active.stream_enriched_for_summary(id, id, request).await;
                            }
                            _ => unreachable!(),
                        }
                        let body = server.join().unwrap();
                        assert_eq!(body["model"], target);
                        assert_eq!(
                            body["tools"][0]["function"]
                                .get("strict")
                                .and_then(Value::as_bool),
                            if target == "yes" { Some(false) } else { None },
                            "mode={mode} deferred={deferred} prepared={prepared}"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod measured_anchor_policy_tests {
    use super::*;
    use clap::Parser;
    use std::{
        io::{Read, Write},
        sync::Arc,
    };

    #[tokio::test]
    async fn unchanged_capability_preserves_legacy_identity_and_measured_anchor() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let server = std::thread::spawn(move || {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut data = Vec::new();
                let (start, len) = loop {
                    let mut block = [0; 4096];
                    let n = socket.read(&mut block).unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&block[..n]);
                    if let Some(pos) = data.windows(4).position(|x| x == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&data[..pos]);
                        let len = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|x| x.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        break (pos + 4, len);
                    }
                };
                while data.len() < start + len {
                    let mut block = [0; 4096];
                    let n = socket.read(&mut block).unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&block[..n]);
                }
                let events = concat!(
                    "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"},\"finish_reason\":null}]}\n\n",
                    "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\n",
                    "data: [DONE]\n\n"
                );
                write!(socket,"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",events.len(),events).unwrap();
            }
        });
        let mut args = crate::Cli::parse_from(["pi"]);
        args.model_profiles=crate::model_profiles::Profiles::parse(&json!({"providers":{"fixture":{"api":"openai-completions","baseUrl":endpoint,"models":[{"id":"m","compat":{"supportsStrictMode":true}}]}}})).unwrap();
        args.provider = args.model_profiles.provider("fixture").unwrap();
        args.model = "m".into();
        let client = Client::Ollama(
            pi_rs::ollama::Ollama::new(&endpoint, Some("fixture".into()))
                .unwrap()
                .with_native_probes(false),
        )
        .with_model_options(&args, false)
        .unwrap();
        let policy = Arc::new(FinalStrictPolicy {
            profiles: args.model_profiles.clone(),
            provider: args.provider.clone(),
            selected_model: "m".into(),
            explicit_endpoint: false,
            bound_support: None,
        });
        let model = WorkbenchModel {
            strict_policy: Some(policy),
            client,
            block_images: false,
            supports_images: None,
            session_affinity: &[],
            price: None,
        };
        let request = ModelRequest {
            model: "m".into(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 10,
        };
        let (_, old_identity) = model
            .client
            .prepare_for_context("m")
            .await
            .unwrap()
            .unwrap();
        let expected = pi_rs::fingerprint_model_context(&(
            "workbench-policy-v2",
            old_identity.unwrap(),
            false,
            Option::<bool>::None,
            &[] as &[&str],
        ));
        let resolved = model.prepare_context(&request).await.unwrap().unwrap();
        assert_eq!(resolved.context_identity(), expected);
        let mut no_policy = model.clone();
        no_policy.strict_policy = None;
        assert_eq!(
            no_policy
                .prepare_context(&request)
                .await
                .unwrap()
                .unwrap()
                .context_identity(),
            expected
        );
        let registry = pi_rs::Registry::default();
        let mut session = pi_rs::Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        session.auto_compaction_control().set_enabled(false);
        let system = "long system ".repeat(1000);
        for window in [10000, 100] {
            let agent = pi_rs::Agent {
                model: &model,
                model_name: "m",
                registry: &registry,
                system_prompt: &system,
                limits: pi_rs::Limits {
                    context_window: Some(window),
                    output_tokens_per_request: 10,
                    ..Default::default()
                },
            };
            let (_, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
            assert_eq!(
                agent
                    .run(
                        &mut session,
                        Some("x".into()),
                        &mut inputs,
                        &pi_rs::CancellationToken::new(),
                        &mut |_| {}
                    )
                    .await
                    .unwrap(),
                "done"
            );
        }
        server.join().unwrap();
    }
}

#[cfg(test)]
mod route_binding_tests {
    use super::*;
    use crate::model_auth::Credential;
    use crate::model_profiles::RequestHeaders;
    use crate::providers::{Adapter, Client};
    use clap::Parser;
    use std::sync::Arc;

    #[tokio::test]
    async fn route_mismatch_and_prepared_identity_are_fail_closed() {
        for api in ["openai-completions", "openai-responses"] {
            for deferred in [false, true] {
                let mut args = crate::Cli::parse_from(["pi"]);
                args.model_profiles = crate::model_profiles::Profiles::parse(&json!({"providers":{"fixture":{
                    "api":api,"baseUrl":"http://127.0.0.1:9/v1","compat":{"supportsStrictMode":true},
                    "models":[{"id":"a","compat":{"supportsStrictMode":true}},{"id":"b","compat":{"supportsStrictMode":false}},{"id":"other-route","baseUrl":"http://127.0.0.1:10/v1"},{"id":"other-api","api":if api=="openai-responses" {"openai-completions"} else {"openai-responses"}}],
                    "modelOverrides":{"partial":{"reasoning":false}}
                }}})).unwrap();
                args.provider = args.model_profiles.provider("fixture").unwrap();
                args.model = "a".into();
                let adapter = if api == "openai-responses" {
                    Adapter::OpenAiResponses
                } else {
                    Adapter::ChatCompletions
                };
                let endpoint = if api == "openai-responses" {
                    "http://127.0.0.1:9/v1/responses"
                } else {
                    "http://127.0.0.1:9/v1/chat/completions"
                };
                let client = if deferred {
                    Client::deferred(
                        args.provider.clone(),
                        adapter,
                        endpoint,
                        Some(Credential::Literal("fixture".into())),
                        RequestHeaders::default(),
                    )
                } else if api == "openai-responses" {
                    Client::OpenAi(
                        pi_rs::openai::OpenAiResponses::new_api(endpoint, "fixture".into())
                            .unwrap(),
                    )
                } else {
                    Client::Ollama(
                        pi_rs::ollama::Ollama::new(endpoint, Some("fixture".into()))
                            .unwrap()
                            .with_native_probes(false),
                    )
                }
                .with_model_options(&args, false)
                .unwrap();
                let model = WorkbenchModel {
                    strict_policy: Some(Arc::new(FinalStrictPolicy {
                        profiles: args.model_profiles.clone(),
                        provider: args.provider.clone(),
                        selected_model: "a".into(),
                        explicit_endpoint: false,
                        bound_support: None,
                    })),
                    client,
                    block_images: false,
                    supports_images: None,
                    session_affinity: &[],
                    price: None,
                };
                let spec = pi_rs::ToolSpec {
                    name: "t".into(),
                    description: String::new(),
                    parameters: json!({"type":"object"}),
                };
                let make = |name: &str| ModelRequest {
                    model: name.into(),
                    messages: vec![],
                    tools: vec![spec.clone()],
                    max_output_tokens: 10,
                };
                let context = pi_rs::ToolDispatchContext {
                    request_id: Uuid::new_v4(),
                    session_id: Uuid::new_v4(),
                    external_id: "s".into(),
                    summary: false,
                };
                let declaration = pi_rs::ToolDeclaration {
                    spec: spec.clone(),
                    constrained_sampling: Some(pi_rs::ToolConstraint::JsonSchema {
                        strict: pi_rs::StrictToolPolicy::Prefer,
                    }),
                };
                for name in ["other-route", "other-api"] {
                    let request = make(name);
                    assert!(model.prepare_context(&request).await.is_err());
                    let error = model
                        .prepare_tool_dispatch(
                            &request,
                            std::slice::from_ref(&declaration),
                            &context,
                        )
                        .await
                        .err()
                        .unwrap();
                    assert!(error.to_string().contains("route"));
                    let error = model.stream(request).await.err().unwrap();
                    assert!(error.to_string().contains("route"));
                }
                for name in ["unknown", "partial"] {
                    let request = make(name);
                    let dispatch = model
                        .prepare_tool_dispatch(
                            &request,
                            std::slice::from_ref(&declaration),
                            &context,
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(matches!(
                        dispatch.effective_tools()[0].sampling,
                        pi_rs::EffectiveToolSampling::Unconstrained
                    ));
                }
                let prepared = model.prepare_context(&make("a")).await.unwrap().unwrap();
                let identity = prepared.context_identity();
                assert!(identity.is_some());
                for name in ["other-route", "other-api"] {
                    let wrong = make(name);
                    assert!(
                        prepared
                            .prepare_tool_dispatch(
                                &wrong,
                                std::slice::from_ref(&declaration),
                                &context
                            )
                            .await
                            .is_err()
                    );
                    assert!(prepared.stream(wrong).await.is_err());
                }
                let b = make("b");
                assert!(
                    prepared
                        .prepare_tool_dispatch(&b, std::slice::from_ref(&declaration), &context)
                        .await
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("bound")
                );
                assert!(
                    prepared
                        .stream(b.clone())
                        .await
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("bound")
                );
                assert_eq!(prepared.estimate_request_tokens(&b), u64::MAX);
                assert!(prepared.context_window("b").await.is_none());
                assert_eq!(prepared.context_identity(), identity);
                let prepared_b = model.prepare_context(&b).await.unwrap().unwrap();
                assert_ne!(prepared_b.context_identity(), identity);
                // A summary may use the selected model after a turn hook
                // prepared the main request for a different model.
                let summary_request = make("a");
                let summary = prepared_b
                    .prepare_context(&summary_request)
                    .await
                    .unwrap()
                    .unwrap();
                let summary_context = pi_rs::ToolDispatchContext {
                    summary: true,
                    ..context.clone()
                };
                assert!(
                    summary
                        .prepare_tool_dispatch(
                            &summary_request,
                            std::slice::from_ref(&declaration),
                            &summary_context,
                        )
                        .await
                        .is_ok()
                );
                assert!(
                    model
                        .prepare_tool_dispatch(&b, std::slice::from_ref(&declaration), &context)
                        .await
                        .is_ok()
                );
            }
        }
    }
}
