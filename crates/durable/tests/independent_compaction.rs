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
    gate_summary: std::sync::atomic::AtomicBool,
    summary_started: tokio::sync::Semaphore,
    summary_release: Arc<tokio::sync::Semaphore>,
    started: tokio::sync::Semaphore,
    release: Arc<tokio::sync::Semaphore>,
}
impl Default for BusyModel {
    fn default() -> Self {
        Self {
            gate_next: false.into(),
            gate_summary: false.into(),
            summary_started: tokio::sync::Semaphore::new(0),
            summary_release: Arc::new(tokio::sync::Semaphore::new(0)),
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
        if self.gate_summary.swap(false, Ordering::SeqCst) {
            self.summary_started.add_permits(1);
            let release = self.summary_release.clone();
            return Ok(stream::once(async move {
                release.acquire().await.unwrap().forget();
                Ok(ModelEvent::Text("busy checkpoint".into()))
            })
            .chain(stream::iter(vec![Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            })]))
            .boxed());
        }
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
async fn queued_input_follows_busy_summary() {
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
        IndependentCompactionStatus::Applied
    );
    let c = scheduler.conversation(conversation).unwrap();
    let summary = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))).unwrap();
    let queued = c
        .messages
        .iter()
        .position(|m| matches!(m, pi_rs_core::Message::User { content } if content == "queued"))
        .unwrap();
    assert!(summary < queued);
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
async fn queued_reset_follows_busy_summary() {
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
        IndependentCompactionStatus::Applied
    );
    let c = scheduler.conversation(conversation).unwrap();
    assert!(!c.messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))));
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn conversation_abort_preserves_pending_busy_summary() {
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
        IndependentCompactionStatus::Applied
    );
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn application_write_follows_busy_summary() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        busy_fixture(&dir.path().join("db"), model.clone()).await;
    let write = scheduler
        .submit_write(conversation, None, "note".into(), None)
        .unwrap();
    assert!(
        scheduler
            .conversation(conversation)
            .unwrap()
            .application_writes
            .iter()
            .any(|item| item.id == write && !item.placed)
    );
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
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

#[tokio::test]
async fn pending_summary_and_passive_writes_survive_abort() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        busy_fixture(&dir.path().join("db"), model).await;
    let write = scheduler
        .submit_write(conversation, None, "note".into(), None)
        .unwrap();
    scheduler.abort_and_wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
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
#[tokio::test]
async fn ordered_pending_work_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) = busy_fixture(&path, model.clone()).await;
    scheduler
        .submit(conversation, None, "queued".into())
        .unwrap();
    let write = scheduler
        .submit_write(conversation, None, "note".into(), None)
        .unwrap();
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
    let c = scheduler.conversation(conversation).unwrap();
    let summary = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))).unwrap();
    let queued = c
        .messages
        .iter()
        .position(|m| matches!(m, pi_rs_core::Message::User { content } if content == "queued"))
        .unwrap();
    assert!(summary < queued);
    assert!(
        c.application_writes
            .iter()
            .any(|item| item.id == write && item.placed)
    );
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn summary_precedes_reset_and_reset_dependent_write() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        busy_fixture(&dir.path().join("db"), model.clone()).await;
    let reset = scheduler
        .reset(conversation, Some("after reset".into()))
        .unwrap();
    let write = scheduler
        .submit_write(conversation, None, "note".into(), None)
        .unwrap();
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    let c = scheduler.conversation(conversation).unwrap();
    assert!(c.resets.iter().any(|item| item.id == reset));
    assert!(
        c.application_writes
            .iter()
            .any(|item| item.id == write && item.placed && item.after_reset == Some(reset))
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_summary_abort_withdraws_pending_write() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        busy_fixture(&dir.path().join("db"), model.clone()).await;
    let write = scheduler
        .submit_write(conversation, None, "note".into(), None)
        .unwrap();
    assert_eq!(
        scheduler
            .abort_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Aborted
    );
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    let c = scheduler.conversation(conversation).unwrap();
    assert!(!c.messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))));
    assert!(
        c.application_writes
            .iter()
            .any(|item| item.id == write && item.placed)
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn admission_before_summary_completion_is_stale() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let mut scheduler = Scheduler::open_with_summary_recovery(
        dir.path().join("db"),
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
    model.gate_summary.store(true, Ordering::SeqCst);
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
    model.summary_started.acquire().await.unwrap().forget();
    scheduler
        .submit(conversation, None, "earlier queued".into())
        .unwrap();
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Stale
    );
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    assert!(scheduler.conversation(conversation).unwrap().messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content == "earlier queued")));
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_preserves_reset_and_its_following_write() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        busy_fixture(&dir.path().join("db"), model).await;
    let reset = scheduler
        .reset(conversation, Some("after reset".into()))
        .unwrap();
    let write = scheduler
        .submit_write(conversation, None, "note".into(), None)
        .unwrap();
    scheduler.abort_and_wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert!(
        scheduler
            .conversation(conversation)
            .unwrap()
            .application_writes
            .iter()
            .any(|item| item.id == write && !item.withdrawn)
    );
    scheduler
        .submit(conversation, None, "continue".into())
        .unwrap();
    scheduler.wait(conversation).await.unwrap();
    let c = scheduler.conversation(conversation).unwrap();
    assert!(c.resets.iter().any(|item| item.id == reset));
    assert!(
        c.application_writes
            .iter()
            .any(|item| item.id == write && item.placed && item.after_reset == Some(reset))
    );
    scheduler.shutdown().await.unwrap();
}
