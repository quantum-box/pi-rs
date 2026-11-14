use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use pi_rs_core::{
    Error, Event, Model, ModelMetadata, ModelRequest, ModelStream, Record, Result, UsageReport,
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

#[async_trait::async_trait]
impl Model for BudgetedModel<'_> {
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
    }
    #[async_trait::async_trait]
    impl BudgetPort for Budget {
        async fn reserve(&self, _: &RunScope, id: Uuid, _: &ModelRequest) -> Result<()> {
            self.reservations.lock().unwrap().push(id);
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
    }
    #[async_trait::async_trait]
    impl Model for Inner {
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
}
