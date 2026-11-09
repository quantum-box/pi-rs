use std::collections::BTreeMap;

use pi_rs_core::{Error, Event, Model, ModelRequest, ModelStream, Result, Session, UsageReport};
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
}

impl BudgetedModel<'_> {
    pub async fn settle_session(&self, session: &Session) -> Result<()> {
        let mut requests = BTreeMap::new();
        // Use every record of this AIP run, including a branch the host left
        // after a settled attempt. A model call remains billable after checkout.
        for record in session
            .records()
            .iter()
            .filter(|record| record.run_id == Some(self.scope.journal_run_id))
        {
            match &record.event {
                Event::ModelRequested {
                    request_id: Some(id),
                    ..
                } => {
                    requests.insert(*id, &UsageReport::Unconfirmed);
                }
                Event::ModelFinished {
                    request_id, usage, ..
                } => {
                    requests.insert(*request_id, usage);
                }
                _ => {}
            }
        }
        for (request_id, usage) in requests {
            self.budget.settle(self.scope, request_id, usage).await?;
        }
        Ok(())
    }
}
