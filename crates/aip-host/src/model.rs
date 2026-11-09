use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

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
