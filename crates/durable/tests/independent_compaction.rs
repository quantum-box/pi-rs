use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use futures_util::{StreamExt, stream};
use pi_rs_core::{Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::recovery::UncertainPolicy;
use pi_rs_durable::runtime::{
    Config, IndependentCompactionStatus, Options, Scheduler, SummaryProviderIdentity,
};

#[derive(Default)]
struct ModelStub {
    summaries: AtomicUsize,
}

#[async_trait::async_trait]
impl Model for ModelStub {
    async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
    async fn stream_for_summary(
        &self,
        _: uuid::Uuid,
        _: uuid::Uuid,
        _: ModelRequest,
    ) -> pi_rs_core::Result<ModelStream> {
        self.summaries.fetch_add(1, Ordering::SeqCst);
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("checkpoint".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}

#[tokio::test]
async fn public_api_submits_and_places_a_summary() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(ModelStub::default());
    let options = Options {
        model: model.clone(),
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let provider = SummaryProviderIdentity {
        model: "example:model:v1".into(),
        settlement: "example:ledger:v1".into(),
    };
    let mut scheduler = Scheduler::open_with_summary_recovery(
        dir.path().join("db"),
        options,
        Default::default(),
        None,
        provider,
    )
    .unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 64,
        })
        .unwrap();
    scheduler
        .submit(conversation, None, "first".into())
        .unwrap();
    scheduler.wait(conversation).await.unwrap();
    scheduler
        .submit(conversation, None, "second".into())
        .unwrap();
    scheduler.wait(conversation).await.unwrap();
    let operation = uuid::Uuid::new_v4();
    assert!(
        scheduler
            .request_idle_independent_compaction(conversation, operation, 0)
            .unwrap()
    );
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(model.summaries.load(Ordering::SeqCst), 1);
    assert!(
        scheduler
            .request_idle_independent_compaction(conversation, operation, 0)
            .is_err()
    );
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    let conversation = scheduler.conversation(conversation).unwrap();
    assert!(conversation.messages.iter().any(|message| matches!(message, pi_rs_core::Message::User { content } if content.contains("<summary>\ncheckpoint"))));
    scheduler.shutdown().await.unwrap();
}

struct BusyModel {
    gate_next: std::sync::atomic::AtomicBool,
    started: tokio::sync::Semaphore,
    release: Arc<tokio::sync::Semaphore>,
}
impl Default for BusyModel {
    fn default() -> Self {
        Self {
            gate_next: false.into(),
            started: tokio::sync::Semaphore::new(0),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
}
#[async_trait::async_trait]
impl Model for BusyModel {
    async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        if self.gate_next.swap(false, Ordering::SeqCst) {
            self.started.add_permits(1);
            let release = self.release.clone();
            return Ok(stream::once(async move {
                release.acquire().await.unwrap().forget();
                Ok(ModelEvent::Text("busy answer".into()))
            })
            .chain(stream::iter(vec![Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            })]))
            .boxed());
        }
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
    async fn stream_for_summary(
        &self,
        _: uuid::Uuid,
        _: uuid::Uuid,
        _: ModelRequest,
    ) -> pi_rs_core::Result<ModelStream> {
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("busy checkpoint".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
fn busy_options(model: Arc<BusyModel>) -> Options {
    Options {
        model,
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn busy_provider() -> SummaryProviderIdentity {
    SummaryProviderIdentity {
        model: "busy:model:v1".into(),
        settlement: "busy:ledger:v1".into(),
    }
}
async fn busy_fixture(
    path: &std::path::Path,
    model: Arc<BusyModel>,
) -> (Scheduler, uuid::Uuid, uuid::Uuid) {
    let mut scheduler = Scheduler::open_with_summary_recovery(
        path,
        busy_options(model.clone()),
        Default::default(),
        None,
        busy_provider(),
    )
    .unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 64,
        })
        .unwrap();
    for input in ["first", "second"] {
        scheduler.submit(conversation, None, input.into()).unwrap();
        scheduler.wait(conversation).await.unwrap();
    }
    model.gate_next.store(true, Ordering::SeqCst);
    scheduler
        .submit(conversation, None, "third".into())
        .unwrap();
    model.started.acquire().await.unwrap().forget();
    let operation = uuid::Uuid::new_v4();
    assert!(
        scheduler
            .request_busy_independent_compaction(conversation, operation, 0)
            .unwrap()
    );
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Running
    );
    (scheduler, conversation, operation)
}
#[tokio::test]
async fn busy_summary_waits_for_boundary_then_places() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        busy_fixture(&dir.path().join("db"), model.clone()).await;
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    let c = scheduler.conversation(conversation).unwrap();
    assert!(c.messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("<summary>\nbusy checkpoint"))));
    assert!(c.messages.iter().any(
        |m| matches!(m, pi_rs_core::Message::Assistant { content, .. } if content == "busy answer")
    ));
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn queued_input_makes_busy_summary_stale() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        busy_fixture(&dir.path().join("db"), model.clone()).await;
    scheduler
        .submit(conversation, None, "queued".into())
        .unwrap();
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Stale
    );
    let c = scheduler.conversation(conversation).unwrap();
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m, pi_rs_core::Message::User { content } if content == "queued"))
    );
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn pending_busy_summary_recovers_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(BusyModel::default());
    let (scheduler, conversation, operation) = busy_fixture(&path, model.clone()).await;
    scheduler.shutdown().await.unwrap();
    let mut scheduler = Scheduler::open_with_summary_recovery(
        &path,
        busy_options(model),
        Default::default(),
        None,
        busy_provider(),
    )
    .unwrap();
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn queued_reset_makes_busy_summary_stale() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        busy_fixture(&dir.path().join("db"), model.clone()).await;
    scheduler
        .reset(conversation, Some("reset boundary".into()))
        .unwrap();
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Stale
    );
    let c = scheduler.conversation(conversation).unwrap();
    assert!(!c.messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))));
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn conversation_abort_cancels_pending_busy_summary() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        busy_fixture(&dir.path().join("db"), model).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        scheduler.abort_and_wait(conversation),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Aborted
    );
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn application_write_makes_busy_summary_stale() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        busy_fixture(&dir.path().join("db"), model.clone()).await;
    let write = scheduler
        .submit_write(conversation, None, "note".into(), None)
        .unwrap();
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Stale
    );
    assert!(
        scheduler
            .conversation(conversation)
            .unwrap()
            .application_writes
            .iter()
            .any(|item| item.id == write && item.placed)
    );
    scheduler.shutdown().await.unwrap();
}
