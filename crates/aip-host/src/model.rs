use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use pi_rs_core::{
    EnrichedModelStream, Error, Event, Model, ModelMetadata, ModelRequest, ModelStream, Record,
    Result, UsageReport,
};
use uuid::Uuid;

use crate::RunScope;

#[async_trait::async_trait]
pub trait BudgetPort: Send + Sync {
    /// Reserve on the existing parent/child budget before any provider dispatch.
    async fn reserve(
        &self,
        scope: &RunScope,
        request_id: Uuid,
        request: &ModelRequest,
    ) -> Result<()>;
    /// Idempotent by (scope, request ID). Unconfirmed usage must remain
    /// reconcilable and may be upgraded to measured usage.
    async fn settle(&self, scope: &RunScope, request_id: Uuid, usage: &UsageReport) -> Result<()>;
}

pub(crate) struct BudgetedModel<'a> {
    pub scope: &'a RunScope,
    pub inner: &'a dyn Model,
    pub budget: &'a dyn BudgetPort,
    pub recorded_usage: Mutex<BTreeSet<Uuid>>,
}

struct PreparedBudgetDispatch<'a> {
    inner: Box<dyn pi_rs_core::PreparedToolDispatch + 'a>,
    scope: &'a RunScope,
    budget: &'a dyn BudgetPort,
    request: ModelRequest,
    request_id: Uuid,
}

#[async_trait::async_trait]
impl pi_rs_core::PreparedToolDispatch for PreparedBudgetDispatch<'_> {
    fn effective_tools(&self) -> &[pi_rs_core::EffectiveToolDeclaration] {
        self.inner.effective_tools()
    }

    fn decision_snapshot(&self) -> &pi_rs_core::ToolDecisionSnapshot {
        self.inner.decision_snapshot()
    }

    fn dispatch_request(&self) -> &ModelRequest {
        self.inner.dispatch_request()
    }

    fn reservation_request(&self) -> &ModelRequest {
        self.inner.reservation_request()
    }

    async fn dispatch(self: Box<Self>) -> Result<pi_rs_core::DetailedModelStream> {
        self.budget
            .reserve(self.scope, self.request_id, &self.request)
            .await?;
        self.inner.dispatch().await
    }
}

#[async_trait::async_trait]
impl Model for BudgetedModel<'_> {
    async fn prepare_tool_dispatch<'a>(
        &'a self,
        request: &ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
        context: &pi_rs_core::ToolDispatchContext,
    ) -> Result<Option<Box<dyn pi_rs_core::PreparedToolDispatch + 'a>>> {
        let Some(inner) = self
            .inner
            .prepare_tool_dispatch(request, declarations, context)
            .await?
        else {
            pi_rs_core::validate_legacy_tool_declarations(request, declarations)?;
            return Ok(None);
        };
        pi_rs_core::validate_prepared_tool_dispatch(
            inner.as_ref(),
            context,
            request,
            declarations,
        )?;
        let request = inner.reservation_request().clone();
        Ok(Some(Box::new(PreparedBudgetDispatch {
            inner,
            scope: self.scope,
            budget: self.budget,
            request,
            request_id: context.request_id,
        })))
    }

    async fn prepare_context<'a>(
        &'a self,
        request: &ModelRequest,
    ) -> Result<Option<Arc<dyn Model + 'a>>> {
        Ok(self.inner.prepare_context(request).await?.map(|inner| {
            Arc::new(PreparedBudgetedModel {
                parent: self,
                inner,
            }) as Arc<dyn Model + 'a>
        }))
    }

    fn context_identity(&self) -> Option<String> {
        self.inner.context_identity()
    }
    fn estimate_message_tokens(
        &self,
        request: &ModelRequest,
        message: &pi_rs_core::Message,
    ) -> u64 {
        self.inner.estimate_message_tokens(request, message)
    }
    fn estimate_request_tokens(&self, request: &ModelRequest) -> u64 {
        self.inner.estimate_request_tokens(request)
    }

    async fn context_window(&self, model: &str) -> Option<u32> {
        self.inner.context_window(model).await
    }
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        Err(Error::Config(
            "AIP model requests require a durable request ID".into(),
        ))
    }

    async fn stream_with_id(&self, request_id: Uuid, request: ModelRequest) -> Result<ModelStream> {
        self.budget
            .reserve(self.scope, request_id, &request)
            .await?;
        self.inner.stream_with_id(request_id, request).await
    }

    async fn stream_in_session(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.budget
            .reserve(self.scope, request_id, &request)
            .await?;
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
        self.budget
            .reserve(self.scope, request_id, &request)
            .await?;
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
        self.budget
            .reserve(self.scope, request_id, &request)
            .await?;
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
        self.budget
            .reserve(self.scope, request_id, &request)
            .await?;
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
    ) -> Result<pi_rs_core::DetailedModelStream> {
        self.budget
            .reserve(self.scope, request_id, &request)
            .await?;
        self.inner
            .stream_detailed_in_session_with_identity(request_id, session_id, external_id, request)
            .await
    }
    async fn stream_detailed_for_summary(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<pi_rs_core::DetailedModelStream> {
        self.budget
            .reserve(self.scope, request_id, &request)
            .await?;
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
        self.budget
            .reserve(self.scope, request_id, &request)
            .await?;
        self.inner
            .stream_enriched_for_summary(request_id, session_id, request)
            .await
    }

    async fn record_usage(&self, request_id: Uuid, usage: &UsageReport) -> Result<()> {
        self.budget.settle(self.scope, request_id, usage).await
    }

    async fn record_usage_with_metadata(
        &self,
        request_id: Uuid,
        usage: &UsageReport,
        metadata: &ModelMetadata,
    ) -> Result<()> {
        self.record_usage(request_id, usage).await?;
        if self
            .recorded_usage
            .lock()
            .expect("usage lock poisoned")
            .contains(&request_id)
        {
            return Ok(());
        }
        self.inner
            .record_usage_with_metadata(request_id, usage, metadata)
            .await?;
        self.recorded_usage
            .lock()
            .expect("usage lock poisoned")
            .insert(request_id);
        Ok(())
    }
}

struct PreparedBudgetedModel<'a, 'b> {
    parent: &'a BudgetedModel<'b>,
    inner: Arc<dyn Model + 'a>,
}

#[async_trait::async_trait]
impl Model for PreparedBudgetedModel<'_, '_> {
    async fn prepare_tool_dispatch<'a>(
        &'a self,
        request: &ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
        context: &pi_rs_core::ToolDispatchContext,
    ) -> Result<Option<Box<dyn pi_rs_core::PreparedToolDispatch + 'a>>> {
        let Some(inner) = self
            .inner
            .prepare_tool_dispatch(request, declarations, context)
            .await?
        else {
            pi_rs_core::validate_legacy_tool_declarations(request, declarations)?;
            return Ok(None);
        };
        pi_rs_core::validate_prepared_tool_dispatch(
            inner.as_ref(),
            context,
            request,
            declarations,
        )?;
        let request = inner.reservation_request().clone();
        Ok(Some(Box::new(PreparedBudgetDispatch {
            inner,
            scope: self.parent.scope,
            budget: self.parent.budget,
            request,
            request_id: context.request_id,
        })))
    }

    fn context_identity(&self) -> Option<String> {
        self.inner.context_identity()
    }
    fn estimate_message_tokens(
        &self,
        request: &ModelRequest,
        message: &pi_rs_core::Message,
    ) -> u64 {
        self.inner.estimate_message_tokens(request, message)
    }
    fn estimate_request_tokens(&self, request: &ModelRequest) -> u64 {
        self.inner.estimate_request_tokens(request)
    }
    async fn context_window(&self, model: &str) -> Option<u32> {
        self.inner.context_window(model).await
    }
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        Err(Error::Config(
            "AIP model requests require a durable request ID".into(),
        ))
    }
    async fn stream_with_id(&self, request_id: Uuid, request: ModelRequest) -> Result<ModelStream> {
        self.parent
            .budget
            .reserve(self.parent.scope, request_id, &request)
            .await?;
        self.inner.stream_with_id(request_id, request).await
    }
    async fn stream_in_session(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.parent
            .budget
            .reserve(self.parent.scope, request_id, &request)
            .await?;
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
        self.parent
            .budget
            .reserve(self.parent.scope, request_id, &request)
            .await?;
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
        self.parent
            .budget
            .reserve(self.parent.scope, request_id, &request)
            .await?;
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
        self.parent
            .budget
            .reserve(self.parent.scope, request_id, &request)
            .await?;
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
    ) -> Result<pi_rs_core::DetailedModelStream> {
        self.parent
            .budget
            .reserve(self.parent.scope, request_id, &request)
            .await?;
        self.inner
            .stream_detailed_in_session_with_identity(request_id, session_id, external_id, request)
            .await
    }
    async fn stream_detailed_for_summary(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<pi_rs_core::DetailedModelStream> {
        self.parent
            .budget
            .reserve(self.parent.scope, request_id, &request)
            .await?;
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
        self.parent
            .budget
            .reserve(self.parent.scope, request_id, &request)
            .await?;
        self.inner
            .stream_enriched_for_summary(request_id, session_id, request)
            .await
    }

    async fn record_usage(&self, request_id: Uuid, usage: &UsageReport) -> Result<()> {
        self.parent
            .budget
            .settle(self.parent.scope, request_id, usage)
            .await
    }
    async fn record_usage_with_metadata(
        &self,
        request_id: Uuid,
        usage: &UsageReport,
        metadata: &ModelMetadata,
    ) -> Result<()> {
        self.record_usage(request_id, usage).await?;
        if self
            .parent
            .recorded_usage
            .lock()
            .expect("usage lock poisoned")
            .contains(&request_id)
        {
            return Ok(());
        }
        self.inner
            .record_usage_with_metadata(request_id, usage, metadata)
            .await?;
        self.parent
            .recorded_usage
            .lock()
            .expect("usage lock poisoned")
            .insert(request_id);
        Ok(())
    }
}

impl BudgetedModel<'_> {
    pub async fn settle_session(&self, records: &[Record], recover: bool) -> Result<()> {
        let mut requests = BTreeMap::new();
        let mut finished = None;
        // Use every record of this AIP run, including a branch the host left
        // after a settled attempt. A model call remains billable after checkout.
        for record in records
            .iter()
            .filter(|record| record.run_id == Some(self.scope.journal_run_id))
        {
            match &record.event {
                Event::ModelRequested {
                    request_id: Some(id),
                    ..
                } => {
                    finished = None;
                    requests.insert(*id, (&UsageReport::Unconfirmed, None));
                }
                Event::ModelFinished {
                    request_id,
                    usage,
                    metadata,
                    ..
                } => {
                    finished = Some((*request_id, record.id));
                    requests.insert(*request_id, (usage, Some(metadata)));
                }
                Event::Assistant {
                    request_id: Some(request),
                    ..
                } => {
                    if let Some((_, metadata)) = requests.get_mut(request) {
                        *metadata = None;
                    }
                    finished = None;
                }
                Event::Assistant { .. } | Event::Compacted { usage: Some(_), .. }
                    if finished.is_some_and(|(_, id)| record.parent == Some(id)) =>
                {
                    // A response attached to this measurement proves the hook returned.
                    if let Some((_, metadata)) = finished
                        .take()
                        .and_then(|(request, _)| requests.get_mut(&request))
                    {
                        *metadata = None;
                    }
                }
                _ => {}
            }
        }
        for (request_id, (usage, metadata)) in requests {
            match metadata.filter(|_| recover) {
                Some(metadata) => {
                    self.record_usage_with_metadata(request_id, usage, metadata)
                        .await?
                }
                None => self.record_usage(request_id, usage).await?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod external_identity_tests {
    use super::*;

    #[derive(Default)]
    struct Budget {
        reservations: Mutex<Vec<Uuid>>,
        settlements: Mutex<Vec<(Uuid, UsageReport)>>,
        reservation_requests: Mutex<Vec<ModelRequest>>,
    }
    #[async_trait::async_trait]
    impl BudgetPort for Budget {
        async fn reserve(&self, _: &RunScope, id: Uuid, request: &ModelRequest) -> Result<()> {
            self.reservations.lock().unwrap().push(id);
            self.reservation_requests
                .lock()
                .unwrap()
                .push(request.clone());
            Ok(())
        }
        async fn settle(&self, _: &RunScope, id: Uuid, usage: &UsageReport) -> Result<()> {
            self.settlements.lock().unwrap().push((id, usage.clone()));
            Ok(())
        }
    }
    #[derive(Clone, Default)]
    struct Inner {
        identities: Arc<Mutex<Vec<(Uuid, Uuid, String)>>>,
        receipts: Arc<Mutex<Vec<Uuid>>>,
        enriched: Arc<Mutex<Vec<(Uuid, Uuid, String)>>>,
    }

    struct TestPreparedDispatch {
        context: pi_rs_core::ToolDispatchContext,
        calls: Arc<Mutex<Vec<(Uuid, Uuid, String)>>>,
        request: ModelRequest,
        dispatch_request: ModelRequest,
        effective: Vec<pi_rs_core::EffectiveToolDeclaration>,
        snapshot: pi_rs_core::ToolDecisionSnapshot,
    }

    struct InvalidNoneModel;
    #[async_trait::async_trait]
    impl Model for InvalidNoneModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            panic!("must not dispatch")
        }
        async fn prepare_tool_dispatch<'a>(
            &'a self,
            _: &ModelRequest,
            _: &[pi_rs_core::ToolDeclaration],
            _: &pi_rs_core::ToolDispatchContext,
        ) -> Result<Option<Box<dyn pi_rs_core::PreparedToolDispatch + 'a>>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn budget_wrappers_reject_required_constraint_when_inner_returns_none() {
        let scope = RunScope {
            tenant: "t".into(),
            actor: "a".into(),
            workspace: "w".into(),
            journal_run_id: Uuid::new_v4(),
            aip_run: "r".into(),
            parent_run: None,
            audit_id: "audit".into(),
        };
        let budget = Budget::default();
        let inner = InvalidNoneModel;
        let wrapper = BudgetedModel {
            scope: &scope,
            inner: &inner,
            budget: &budget,
            recorded_usage: Mutex::default(),
        };
        let prepared = PreparedBudgetedModel {
            parent: &wrapper,
            inner: Arc::new(InvalidNoneModel),
        };
        let spec = pi_rs_core::ToolSpec {
            name: "read".into(),
            description: String::new(),
            parameters: serde_json::json!({"type":"object"}),
        };
        let request = ModelRequest {
            model: "m".into(),
            messages: vec![],
            tools: vec![spec.clone()],
            max_output_tokens: 1,
        };
        let declaration = pi_rs_core::ToolDeclaration {
            spec,
            constrained_sampling: Some(pi_rs_core::ToolConstraint::JsonSchema {
                strict: pi_rs_core::StrictToolPolicy::Require,
            }),
        };
        let context = pi_rs_core::ToolDispatchContext {
            request_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            external_id: "s".into(),
            summary: false,
        };
        for model in [&wrapper as &dyn Model, &prepared as &dyn Model] {
            assert!(
                model
                    .prepare_tool_dispatch(&request, std::slice::from_ref(&declaration), &context)
                    .await
                    .is_err()
            );
        }
        assert!(budget.reservations.lock().unwrap().is_empty());
    }
    #[async_trait::async_trait]
    impl pi_rs_core::PreparedToolDispatch for TestPreparedDispatch {
        fn effective_tools(&self) -> &[pi_rs_core::EffectiveToolDeclaration] {
            &self.effective
        }
        fn decision_snapshot(&self) -> &pi_rs_core::ToolDecisionSnapshot {
            &self.snapshot
        }
        fn dispatch_request(&self) -> &ModelRequest {
            &self.dispatch_request
        }
        fn reservation_request(&self) -> &ModelRequest {
            &self.request
        }
        async fn dispatch(self: Box<Self>) -> Result<pi_rs_core::DetailedModelStream> {
            self.calls.lock().unwrap().push((
                self.context.request_id,
                self.context.session_id,
                if self.context.summary {
                    "summary".into()
                } else {
                    self.context.external_id.clone()
                },
            ));
            Ok(Box::pin(futures_util::stream::empty()))
        }
    }
    #[async_trait::async_trait]
    impl Model for Inner {
        async fn prepare_tool_dispatch<'a>(
            &'a self,
            request: &ModelRequest,
            declarations: &[pi_rs_core::ToolDeclaration],
            context: &pi_rs_core::ToolDispatchContext,
        ) -> Result<Option<Box<dyn pi_rs_core::PreparedToolDispatch + 'a>>> {
            let mut reservation = request.clone();
            if let Some(tool) = reservation.tools.first_mut() {
                tool.parameters["required"] = serde_json::json!(["path"]);
            }
            let effective: Vec<_> = request
                .tools
                .iter()
                .map(|spec| pi_rs_core::EffectiveToolDeclaration {
                    spec: spec.clone(),
                    sampling: pi_rs_core::EffectiveToolSampling::Unconstrained,
                })
                .collect();
            let snapshot = pi_rs_core::ToolDecisionSnapshot::capture(
                context,
                request,
                declarations,
                &effective,
            )
            .map_err(|error| Error::Config(error.to_string()))?;
            Ok(Some(Box::new(TestPreparedDispatch {
                context: context.clone(),
                calls: self.enriched.clone(),
                request: reservation,
                dispatch_request: request.clone(),
                effective,
                snapshot,
            })))
        }
        async fn prepare_context<'a>(
            &'a self,
            _: &ModelRequest,
        ) -> Result<Option<Arc<dyn Model + 'a>>> {
            Ok(Some(Arc::new(self.clone())))
        }
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            panic!("external identity was discarded")
        }
        async fn stream_in_session_with_identity(
            &self,
            request_id: Uuid,
            session_id: Uuid,
            external_id: &str,
            _: ModelRequest,
        ) -> Result<ModelStream> {
            self.identities
                .lock()
                .unwrap()
                .push((request_id, session_id, external_id.into()));
            Ok(Box::pin(futures_util::stream::empty()))
        }
        async fn stream_enriched_in_session_with_identity(
            &self,
            request_id: Uuid,
            session_id: Uuid,
            external_id: &str,
            _: ModelRequest,
        ) -> Result<EnrichedModelStream> {
            self.enriched
                .lock()
                .unwrap()
                .push((request_id, session_id, external_id.into()));
            Ok(Box::pin(futures_util::stream::once(async {
                Ok(pi_rs_core::EnrichedModelEvent::ReasoningStart { index: 7 })
            })))
        }
        async fn stream_enriched_for_summary(
            &self,
            request_id: Uuid,
            session_id: Uuid,
            _: ModelRequest,
        ) -> Result<EnrichedModelStream> {
            self.enriched
                .lock()
                .unwrap()
                .push((request_id, session_id, "summary".into()));
            Ok(Box::pin(futures_util::stream::once(async {
                Ok(pi_rs_core::EnrichedModelEvent::ReasoningStart { index: 8 })
            })))
        }
        async fn stream_detailed_in_session_with_identity(
            &self,
            request_id: Uuid,
            session_id: Uuid,
            external_id: &str,
            _: ModelRequest,
        ) -> Result<pi_rs_core::DetailedModelStream> {
            self.enriched
                .lock()
                .unwrap()
                .push((request_id, session_id, external_id.into()));
            Ok(Box::pin(futures_util::stream::once(async {
                Ok(pi_rs_core::DetailedModelEvent::redacted_start(7))
            })))
        }
        async fn stream_detailed_for_summary(
            &self,
            request_id: Uuid,
            session_id: Uuid,
            _: ModelRequest,
        ) -> Result<pi_rs_core::DetailedModelStream> {
            self.enriched
                .lock()
                .unwrap()
                .push((request_id, session_id, "summary".into()));
            Ok(Box::pin(futures_util::stream::once(async {
                Ok(pi_rs_core::DetailedModelEvent::redacted_start(8))
            })))
        }
        async fn record_usage_with_metadata(
            &self,
            request_id: Uuid,
            _: &UsageReport,
            _: &ModelMetadata,
        ) -> Result<()> {
            self.receipts.lock().unwrap().push(request_id);
            Ok(())
        }
    }

    #[tokio::test]
    async fn prepared_tool_dispatch_reserves_at_dispatch_for_both_wrappers() {
        let scope = RunScope {
            tenant: "t".into(),
            actor: "a".into(),
            workspace: "w".into(),
            journal_run_id: Uuid::new_v4(),
            aip_run: "r".into(),
            parent_run: None,
            audit_id: "audit".into(),
        };
        let budget = Budget::default();
        let inner = Inner::default();
        let wrapper = BudgetedModel {
            scope: &scope,
            inner: &inner,
            budget: &budget,
            recorded_usage: Mutex::default(),
        };
        let request = ModelRequest {
            model: "m".into(),
            messages: vec![],
            tools: vec![pi_rs_core::ToolSpec {
                name: "read".into(),
                description: String::new(),
                parameters: serde_json::json!({"type":"object","properties":{"path":{"type":"string"}}}),
            }],
            max_output_tokens: 1,
        };
        let prepared_model = wrapper.prepare_context(&request).await.unwrap().unwrap();
        let declarations = [pi_rs_core::ToolDeclaration {
            spec: request.tools[0].clone(),
            constrained_sampling: None,
        }];
        let mut ids = Vec::new();
        for (model, summary) in [
            (&wrapper as &dyn Model, false),
            (prepared_model.as_ref(), true),
        ] {
            let id = Uuid::new_v4();
            let context = pi_rs_core::ToolDispatchContext {
                request_id: id,
                session_id: Uuid::nil(),
                external_id: "fixture".into(),
                summary,
            };
            let dispatch = model
                .prepare_tool_dispatch(&request, &declarations, &context)
                .await
                .unwrap()
                .unwrap();
            assert!(
                dispatch
                    .decision_snapshot()
                    .validate(
                        &context,
                        &request,
                        dispatch.dispatch_request(),
                        &declarations
                    )
                    .is_ok()
            );
            assert_eq!(
                budget.reservations.lock().unwrap().as_slice(),
                ids.as_slice()
            );
            let _ = dispatch.dispatch().await.unwrap();
            ids.push(id);
        }
        assert_eq!(*budget.reservations.lock().unwrap(), ids);
        assert!(
            budget
                .reservation_requests
                .lock()
                .unwrap()
                .iter()
                .all(|request| request.tools[0].parameters["required"]
                    == serde_json::json!(["path"]))
        );
        assert_eq!(
            inner
                .enriched
                .lock()
                .unwrap()
                .iter()
                .map(|entry| entry.2.as_str())
                .collect::<Vec<_>>(),
            ["fixture", "summary"]
        );
    }
    #[tokio::test]
    async fn prepared_and_unprepared_preserve_identity_and_budget() {
        let scope = RunScope {
            tenant: "t".into(),
            actor: "a".into(),
            workspace: "w".into(),
            journal_run_id: Uuid::new_v4(),
            aip_run: "r".into(),
            parent_run: None,
            audit_id: "audit".into(),
        };
        let budget = Budget::default();
        let inner = Inner::default();
        let wrapper = BudgetedModel {
            scope: &scope,
            inner: &inner,
            budget: &budget,
            recorded_usage: Mutex::default(),
        };
        let request = ModelRequest {
            model: "model".into(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 1,
        };
        let session_id = Uuid::new_v4();
        let usage = UsageReport::Measured {
            usage: pi_rs_core::Usage {
                input_tokens: 4,
                output_tokens: 2,
            },
        };
        let unprepared_id = Uuid::new_v4();
        let _ = wrapper
            .stream_in_session_with_identity(
                unprepared_id,
                session_id,
                "legacy-id",
                request.clone(),
            )
            .await
            .unwrap();
        wrapper
            .record_usage_with_metadata(unprepared_id, &usage, &ModelMetadata::default())
            .await
            .unwrap();
        let prepared = wrapper.prepare_context(&request).await.unwrap().unwrap();
        let prepared_id = Uuid::new_v4();
        let _ = prepared
            .stream_in_session_with_identity(prepared_id, session_id, "legacy-id", request)
            .await
            .unwrap();
        prepared
            .record_usage_with_metadata(prepared_id, &usage, &ModelMetadata::default())
            .await
            .unwrap();
        assert_eq!(
            *budget.reservations.lock().unwrap(),
            vec![unprepared_id, prepared_id]
        );
        assert_eq!(
            *budget.settlements.lock().unwrap(),
            vec![(unprepared_id, usage.clone()), (prepared_id, usage)]
        );
        assert_eq!(
            *inner.identities.lock().unwrap(),
            vec![
                (unprepared_id, session_id, "legacy-id".into()),
                (prepared_id, session_id, "legacy-id".into())
            ]
        );
        assert_eq!(
            *inner.receipts.lock().unwrap(),
            vec![unprepared_id, prepared_id]
        );
    }
    #[tokio::test]
    async fn enriched_streams_reserve_and_forward_for_prepared_and_unprepared_models() {
        use futures_util::StreamExt;
        let scope = RunScope {
            tenant: "t".into(),
            actor: "a".into(),
            workspace: "w".into(),
            journal_run_id: Uuid::new_v4(),
            aip_run: "r".into(),
            parent_run: None,
            audit_id: "audit".into(),
        };
        let budget = Budget::default();
        let inner = Inner::default();
        let wrapper = BudgetedModel {
            scope: &scope,
            inner: &inner,
            budget: &budget,
            recorded_usage: Mutex::default(),
        };
        let request = ModelRequest {
            model: "model".into(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 1,
        };
        let prepared = wrapper.prepare_context(&request).await.unwrap().unwrap();
        let session = Uuid::new_v4();
        let mut expected = Vec::new();
        for model in [&wrapper as &dyn Model, prepared.as_ref()] {
            let id = Uuid::new_v4();
            let mut stream = model
                .stream_enriched_in_session_with_identity(id, session, "external", request.clone())
                .await
                .unwrap();
            assert!(matches!(
                stream.next().await.unwrap().unwrap(),
                pi_rs_core::EnrichedModelEvent::ReasoningStart { index: 7 }
            ));
            expected.push((id, session, "external".to_string()));
            let id = Uuid::new_v4();
            let mut stream = model
                .stream_enriched_for_summary(id, session, request.clone())
                .await
                .unwrap();
            assert!(matches!(
                stream.next().await.unwrap().unwrap(),
                pi_rs_core::EnrichedModelEvent::ReasoningStart { index: 8 }
            ));
            expected.push((id, session, "summary".to_string()));
            let id = Uuid::new_v4();
            let mut stream = model
                .stream_detailed_in_session_with_identity(id, session, "external", request.clone())
                .await
                .unwrap();
            assert!(stream.next().await.unwrap().unwrap().is_redacted_start());
            expected.push((id, session, "external".to_string()));
            let id = Uuid::new_v4();
            let mut stream = model
                .stream_detailed_for_summary(id, session, request.clone())
                .await
                .unwrap();
            assert!(stream.next().await.unwrap().unwrap().is_redacted_start());
            expected.push((id, session, "summary".to_string()));
        }
        assert_eq!(
            *budget.reservations.lock().unwrap(),
            expected.iter().map(|entry| entry.0).collect::<Vec<_>>()
        );
        assert_eq!(*inner.enriched.lock().unwrap(), expected);
    }
}
