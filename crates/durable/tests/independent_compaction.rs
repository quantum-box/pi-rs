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
async fn queued_input_before_summary_completion_follows_summary() {
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
        IndependentCompactionStatus::Running
    );
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
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

async fn pending_summary_fixture(
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
    (scheduler, conversation, operation)
}
#[tokio::test]
async fn earlier_application_write_is_preserved_and_summary_places() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        pending_summary_fixture(&dir.path().join("db"), model.clone()).await;
    let write = scheduler
        .submit_write(conversation, None, "early".into(), None)
        .unwrap();
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Running
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
async fn earlier_reset_makes_summary_stale_and_later_write_follows() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        pending_summary_fixture(&dir.path().join("db"), model.clone()).await;
    let reset = scheduler
        .reset(conversation, Some("reset first".into()))
        .unwrap();
    let early = scheduler
        .submit_write(conversation, None, "early".into(), None)
        .unwrap();
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Running
    );
    let later = scheduler
        .submit_write(conversation, None, "later".into(), None)
        .unwrap();
    let before_boundary = scheduler.history_head(conversation).unwrap();
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Stale
    );
    let page = scheduler
        .history_through(
            conversation,
            before_boundary,
            scheduler.history_head(conversation).unwrap(),
            std::num::NonZeroUsize::new(100).unwrap(),
        )
        .unwrap();
    let reset_event = page
        .records
        .iter()
        .find(|record| {
            record.value["resets"]
                .as_array()
                .is_some_and(|items| items.iter().any(|r| r["id"] == reset.to_string()))
        })
        .unwrap();
    let writes = reset_event.value["application_writes"].as_array().unwrap();
    assert!(
        writes
            .iter()
            .any(|w| w["id"] == early.to_string() && w["placed"] == true)
    );
    assert!(
        writes
            .iter()
            .any(|w| w["id"] == later.to_string() && w["placed"] == false)
    );
    let c = scheduler.conversation(conversation).unwrap();
    assert!(c.resets.iter().any(|r| r.id == reset));
    assert!(
        c.application_writes
            .iter()
            .any(|w| w.id == early && w.placed && w.after_reset == Some(reset))
    );
    assert!(
        c.application_writes
            .iter()
            .any(|w| w.id == later && w.placed && w.after_reset == Some(reset))
    );
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn earlier_input_and_pending_summary_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        pending_summary_fixture(&path, model.clone()).await;
    scheduler
        .submit(conversation, None, "queued early".into())
        .unwrap();
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Running
    );
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
    let input = c
        .messages
        .iter()
        .position(
            |m| matches!(m, pi_rs_core::Message::User { content } if content == "queued early"),
        )
        .unwrap();
    assert!(summary < input);
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn abort_before_summary_completion_cancels_operation() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        pending_summary_fixture(&dir.path().join("db"), model).await;
    scheduler
        .submit(conversation, None, "queued early".into())
        .unwrap();
    scheduler.abort_and_wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Aborted
    );
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn model_progress_before_summary_completion_places_at_idle() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        pending_summary_fixture(&dir.path().join("db"), model.clone()).await;
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Applied
    );
    let c = scheduler.conversation(conversation).unwrap();
    assert!(c.messages.iter().any(
        |m| matches!(m, pi_rs_core::Message::Assistant { content, .. } if content == "busy answer")
    ));
    assert!(c.messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))));
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn interrupted_summary_with_earlier_input_places_after_model_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        pending_summary_fixture(&path, model.clone()).await;
    scheduler
        .submit(conversation, None, "queued early".into())
        .unwrap();
    scheduler.shutdown().await.unwrap();
    model.gate_next.store(true, Ordering::SeqCst);
    model.gate_summary.store(true, Ordering::SeqCst);
    let mut scheduler = Scheduler::open_with_summary_recovery(
        &path,
        busy_options(model.clone()),
        Default::default(),
        None,
        busy_provider(),
    )
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), model.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        model.summary_started.acquire(),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Running
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
            .messages
            .iter()
            .any(
                |m| matches!(m, pi_rs_core::Message::User { content } if content == "queued early")
            )
    );
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn earlier_reset_order_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        pending_summary_fixture(&path, model.clone()).await;
    let reset = scheduler
        .reset(conversation, Some("reset first".into()))
        .unwrap();
    let early = scheduler
        .submit_write(conversation, None, "early".into(), None)
        .unwrap();
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Running
    );
    let later = scheduler
        .submit_write(conversation, None, "later".into(), None)
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
        IndependentCompactionStatus::Stale
    );
    let c = scheduler.conversation(conversation).unwrap();
    assert!(c.resets.iter().any(|r| r.id == reset));
    assert!(
        c.application_writes
            .iter()
            .any(|w| w.id == early && w.placed)
    );
    assert!(
        c.application_writes
            .iter()
            .any(|w| w.id == later && w.placed)
    );
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn preceding_reset_waits_for_after_run_hook() {
    use pi_rs_durable::extension::{Context, Extension, GenerationDecision, GenerationEvent, Hook};
    struct CheckAfterRun(Arc<std::sync::atomic::AtomicBool>);
    #[async_trait::async_trait]
    impl Hook for CheckAfterRun {
        async fn generation(
            &self,
            event: GenerationEvent,
            _: &serde_json::Value,
            ctx: &Context<'_>,
        ) -> Result<GenerationDecision, String> {
            if event == GenerationEvent::AfterRun
                && ctx.snapshot["messages"].to_string().contains("busy answer")
            {
                self.0.store(
                    !ctx.snapshot["messages"].to_string().contains("reset first"),
                    Ordering::SeqCst,
                );
            }
            Ok(GenerationDecision::Pass)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut scheduler = Scheduler::open_with_summary_recovery(
        dir.path().join("db"),
        busy_options(model.clone()),
        Default::default(),
        None,
        busy_provider(),
    )
    .unwrap();
    scheduler
        .install_extension(Extension {
            name: "after-run-check".into(),
            hooks: vec![Arc::new(CheckAfterRun(seen.clone()))],
            ..Default::default()
        })
        .unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 64,
        })
        .unwrap();
    scheduler
        .select_extensions(conversation, vec!["after-run-check".into()])
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
        .reset(conversation, Some("reset first".into()))
        .unwrap();
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Running
    );
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    assert!(seen.load(Ordering::SeqCst));
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Stale
    );
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn abort_preserves_summary_after_earlier_reset_admission() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        pending_summary_fixture(&dir.path().join("db"), model.clone()).await;
    let reset = scheduler
        .reset(conversation, Some("reset first".into()))
        .unwrap();
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Running
    );
    scheduler.abort_and_wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Stale
    );
    assert!(
        scheduler
            .conversation(conversation)
            .unwrap()
            .resets
            .iter()
            .any(|item| item.id == reset)
    );
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn completed_summary_waits_for_next_generation_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        pending_summary_fixture(&dir.path().join("db"), model.clone()).await;
    scheduler
        .submit(conversation, None, "next input".into())
        .unwrap();
    model.gate_next.store(true, Ordering::SeqCst);
    model.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), model.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Running
    );
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    let c = scheduler.conversation(conversation).unwrap();
    let summary = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))).unwrap();
    let next = c
        .messages
        .iter()
        .position(|m| matches!(m, pi_rs_core::Message::User { content } if content == "next input"))
        .unwrap();
    assert!(summary < next);
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn reset_placed_during_summary_makes_result_stale() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        pending_summary_fixture(&dir.path().join("db"), model.clone()).await;
    let reset = scheduler
        .reset(conversation, Some("changed context".into()))
        .unwrap();
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Stale
    );
    let c = scheduler.conversation(conversation).unwrap();
    assert!(c.resets.iter().any(|r| r.id == reset));
    assert!(!c.messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))));
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn unfinished_summary_recovers_after_conversation_becomes_idle() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(BusyModel::default());
    let (mut scheduler, conversation, operation) =
        pending_summary_fixture(&path, model.clone()).await;
    model.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    scheduler.shutdown().await.unwrap();
    let mut scheduler = Scheduler::open_with_summary_recovery(
        &path,
        busy_options(model),
        Default::default(),
        None,
        busy_provider(),
    )
    .unwrap();
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert!(scheduler.conversation(conversation).unwrap().messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))));
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn summary_waits_for_after_run_hook() {
    use pi_rs_durable::extension::{Context, Extension, GenerationDecision, GenerationEvent, Hook};
    struct GateAfterRun {
        started: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
        calls: std::sync::atomic::AtomicUsize,
    }
    #[async_trait::async_trait]
    impl Hook for GateAfterRun {
        async fn generation(
            &self,
            event: GenerationEvent,
            _: &serde_json::Value,
            ctx: &Context<'_>,
        ) -> Result<GenerationDecision, String> {
            if event == GenerationEvent::AfterRun
                && ctx.snapshot["messages"].to_string().contains("busy answer")
            {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.started.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            Ok(GenerationDecision::Pass)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let hook = Arc::new(GateAfterRun {
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let mut scheduler = Scheduler::open_with_summary_recovery(
        dir.path().join("db"),
        busy_options(model.clone()),
        Default::default(),
        None,
        busy_provider(),
    )
    .unwrap();
    scheduler
        .install_extension(Extension {
            name: "after-run-gate".into(),
            hooks: vec![hook.clone()],
            ..Default::default()
        })
        .unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 64,
        })
        .unwrap();
    scheduler
        .select_extensions(conversation, vec!["after-run-gate".into()])
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
    model.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), hook.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Running
    );
    assert!(!scheduler.conversation(conversation).unwrap().messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))));
    scheduler
        .submit(conversation, None, "queued after run".into())
        .unwrap();
    scheduler
        .reset(conversation, Some("after-run reset".into()))
        .unwrap();
    hook.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    let c = scheduler.conversation(conversation).unwrap();
    assert!(c.messages.iter().any(
        |m| matches!(m, pi_rs_core::Message::User { content } if content == "after-run reset")
    ));
    assert!(c.messages.iter().any(
        |m| matches!(m, pi_rs_core::Message::User { content } if content == "queued after run")
    ));
    scheduler.shutdown().await.unwrap();
}

struct YieldGate {
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    calls: AtomicUsize,
    effects: AtomicUsize,
    after_run_saw_summary: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl pi_rs_durable::extension::Hook for YieldGate {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        value: &serde_json::Value,
        ctx: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision, GenerationEvent};
        if event == GenerationEvent::AfterRun
            && value["answer"] == "busy answer"
            && ctx.snapshot["messages"]
                .to_string()
                .contains("busy checkpoint")
        {
            self.after_run_saw_summary.store(true, Ordering::SeqCst);
        }
        if event == GenerationEvent::OnYield && value["answer"] == "busy answer" {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if ctx.memo.get("effect").await?.is_none() {
                ctx.memo
                    .get_or_insert("effect", serde_json::json!(true))
                    .await?;
                self.effects.fetch_add(1, Ordering::SeqCst);
            }
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            return Ok(GenerationDecision::Continue(serde_json::json!("follow-on")));
        }
        Ok(GenerationDecision::Pass)
    }
}
fn yield_registry(hook: Arc<YieldGate>) -> pi_rs_durable::extension::Registry {
    let mut registry = pi_rs_durable::extension::Registry::default();
    registry
        .install(pi_rs_durable::extension::Extension {
            name: "yield-gate".into(),
            hooks: vec![hook],
            ..Default::default()
        })
        .unwrap();
    registry
}
async fn yield_hook_fixture(
    path: &std::path::Path,
    model: Arc<BusyModel>,
    hook: Arc<YieldGate>,
    earlier_reset: bool,
) -> (Scheduler, uuid::Uuid, uuid::Uuid) {
    let mut scheduler = Scheduler::open_with_summary_recovery(
        path,
        busy_options(model.clone()),
        yield_registry(hook.clone()),
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
    scheduler
        .select_extensions(conversation, vec!["yield-gate".into()])
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
    model.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), hook.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    if earlier_reset {
        scheduler
            .reset(conversation, Some("reset before summary".into()))
            .unwrap();
    }
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Running
    );
    (scheduler, conversation, operation)
}
fn yield_gate() -> Arc<YieldGate> {
    Arc::new(YieldGate {
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        calls: AtomicUsize::new(0),
        effects: AtomicUsize::new(0),
        after_run_saw_summary: std::sync::atomic::AtomicBool::new(false),
    })
}
#[tokio::test]
async fn summary_during_on_yield_preserves_one_hook_decision_and_inbox_order() {
    for mode in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(BusyModel::default());
        let hook = yield_gate();
        let (mut scheduler, conversation, operation) =
            yield_hook_fixture(&dir.path().join("db"), model, hook.clone(), false).await;
        if mode == 1 {
            scheduler
                .submit(conversation, None, "queued".into())
                .unwrap();
        }
        if mode == 2 {
            scheduler
                .reset(conversation, Some("reset after".into()))
                .unwrap();
        }
        hook.release.add_permits(1);
        scheduler.wait(conversation).await.unwrap();
        assert_eq!(
            scheduler.independent_compaction_status(operation).unwrap(),
            IndependentCompactionStatus::Applied
        );
        assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
        assert_eq!(hook.effects.load(Ordering::SeqCst), 1);
        if mode != 0 {
            assert!(!hook.after_run_saw_summary.load(Ordering::SeqCst));
        }
        let c = scheduler.conversation(conversation).unwrap();
        let follow = c
            .messages
            .iter()
            .any(|m| matches!(m, pi_rs_core::Message::User { content } if content == "follow-on"));
        assert_eq!(follow, mode == 0);
        if mode == 1 {
            let summary = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))).unwrap();
            let queued = c
                .messages
                .iter()
                .position(
                    |m| matches!(m, pi_rs_core::Message::User { content } if content == "queued"),
                )
                .unwrap();
            assert!(summary < queued);
        }
        if mode == 2 {
            assert!(c.messages.iter().any(
                |m| matches!(m, pi_rs_core::Message::User { content } if content == "reset after")
            ));
        }
        scheduler.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn summary_and_memoized_on_yield_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(BusyModel::default());
    let hook = yield_gate();
    let (scheduler, conversation, operation) =
        yield_hook_fixture(&path, model.clone(), hook.clone(), false).await;
    scheduler.shutdown().await.unwrap();
    hook.release.add_permits(1);
    let mut scheduler = Scheduler::open_with_summary_recovery(
        &path,
        busy_options(model),
        yield_registry(hook.clone()),
        None,
        busy_provider(),
    )
    .unwrap();
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(hook.effects.load(Ordering::SeqCst), 1);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 2);
    let c = scheduler.conversation(conversation).unwrap();
    let summary = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))).unwrap();
    let follow = c
        .messages
        .iter()
        .position(|m| matches!(m, pi_rs_core::Message::User { content } if content == "follow-on"))
        .unwrap();
    assert!(summary < follow);
    scheduler.shutdown().await.unwrap();
}

struct AfterRunReplayGate {
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    calls: AtomicUsize,
    effects: AtomicUsize,
}
#[async_trait::async_trait]
impl pi_rs_durable::extension::Hook for AfterRunReplayGate {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        value: &serde_json::Value,
        ctx: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision, GenerationEvent};
        if event == GenerationEvent::AfterRun && value["answer"] == "busy answer" {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if ctx.memo.get("effect").await?.is_none() {
                ctx.memo
                    .get_or_insert("effect", serde_json::json!(true))
                    .await?;
                self.effects.fetch_add(1, Ordering::SeqCst);
            }
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        Ok(GenerationDecision::Pass)
    }
}
fn after_run_registry(hook: Arc<AfterRunReplayGate>) -> pi_rs_durable::extension::Registry {
    let mut registry = pi_rs_durable::extension::Registry::default();
    registry
        .install(pi_rs_durable::extension::Extension {
            name: "after-run-replay".into(),
            hooks: vec![hook],
            ..Default::default()
        })
        .unwrap();
    registry
}
#[tokio::test]
async fn summary_and_memoized_after_run_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(BusyModel::default());
    let hook = Arc::new(AfterRunReplayGate {
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        calls: AtomicUsize::new(0),
        effects: AtomicUsize::new(0),
    });
    let mut scheduler = Scheduler::open_with_summary_recovery(
        &path,
        busy_options(model.clone()),
        after_run_registry(hook.clone()),
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
    scheduler
        .select_extensions(conversation, vec!["after-run-replay".into()])
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
    model.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), hook.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Running
    );
    scheduler
        .submit(conversation, None, "queued".into())
        .unwrap();
    scheduler.shutdown().await.unwrap();
    hook.release.add_permits(1);
    let mut scheduler = Scheduler::open_with_summary_recovery(
        &path,
        busy_options(model),
        after_run_registry(hook.clone()),
        None,
        busy_provider(),
    )
    .unwrap();
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(hook.effects.load(Ordering::SeqCst), 1);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 2);
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
async fn earlier_reset_precedes_summary_during_on_yield() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(BusyModel::default());
    let hook = yield_gate();
    let (mut scheduler, conversation, operation) =
        yield_hook_fixture(&dir.path().join("db"), model, hook.clone(), true).await;
    hook.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Stale
    );
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    let c = scheduler.conversation(conversation).unwrap();
    assert!(c.messages.iter().any(
        |m| matches!(m, pi_rs_core::Message::User { content } if content == "reset before summary")
    ));
    assert!(
        !c.messages
            .iter()
            .any(|m| matches!(m, pi_rs_core::Message::User { content } if content == "follow-on"))
    );
    scheduler.shutdown().await.unwrap();
}

struct ParkedLifecycleHook {
    event: pi_rs_durable::extension::GenerationEvent,
    calls: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl pi_rs_durable::extension::Hook for ParkedLifecycleHook {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        value: &serde_json::Value,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision, HookTask};
        if event == self.event
            && (event == pi_rs_durable::extension::GenerationEvent::AfterTools
                || value["answer"] == "busy answer")
        {
            self.calls.fetch_add(1, Ordering::SeqCst);
            return Ok(GenerationDecision::Tasks(vec![HookTask {
                name: "parked-job".into(),
                input: serde_json::Value::Null,
            }]));
        }
        Ok(GenerationDecision::Pass)
    }
}
fn parked_registries(
    event: pi_rs_durable::extension::GenerationEvent,
    calls: Arc<AtomicUsize>,
    handler: Arc<dyn pi_rs_durable::task_scheduler::Handler>,
) -> (
    pi_rs_durable::extension::Registry,
    pi_rs_durable::task_scheduler::Registry,
) {
    let mut extensions = pi_rs_durable::extension::Registry::default();
    extensions
        .install(pi_rs_durable::extension::Extension {
            name: "parked-hook".into(),
            hooks: vec![Arc::new(ParkedLifecycleHook { event, calls })],
            tasks: vec![("parked-job".into(), handler.clone())],
            ..Default::default()
        })
        .unwrap();
    let mut tasks = pi_rs_durable::task_scheduler::Registry::default();
    assert!(tasks.register_shared("parked-job", true, handler));
    (extensions, tasks)
}
fn parked_handler(
    _event: pi_rs_durable::extension::GenerationEvent,
) -> Arc<dyn pi_rs_durable::task_scheduler::Handler> {
    use pi_rs_durable::task::{Checkpoint, Task, WaitFor};
    use pi_rs_durable::task_scheduler::Step;
    Arc::new(move |task: &Task| {
        if task.delivered_signal().is_some() {
            Ok(Step::Succeed(
                serde_json::json!({"continue":"child follow"}),
            ))
        } else {
            Ok(Step::Wait {
                checkpoint: Checkpoint {
                    phase: "waiting".into(),
                    data: task.checkpoint().data.clone(),
                },
                condition: WaitFor::Signal("go".into()),
                children: vec![],
            })
        }
    })
}
async fn parked_fixture(
    path: &std::path::Path,
    event: pi_rs_durable::extension::GenerationEvent,
    model: Arc<BusyModel>,
    calls: Arc<AtomicUsize>,
    handler: Arc<dyn pi_rs_durable::task_scheduler::Handler>,
    prior: u8,
) -> (
    Scheduler,
    pi_rs_durable::task_driver::Driver,
    uuid::Uuid,
    uuid::Uuid,
    uuid::Uuid,
) {
    let (extensions, tasks) = parked_registries(event, calls, handler);
    let (mut scheduler, driver) = Scheduler::open_with_summary_recovery_and_tasks(
        path,
        busy_options(model.clone()),
        extensions,
        None,
        busy_provider(),
        tasks,
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
        .select_extensions(conversation, vec!["parked-hook".into()])
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
    model.release.add_permits(1);
    let child = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let value =
                serde_json::to_value(scheduler.conversation(conversation).unwrap()).unwrap();
            let phase = if event == pi_rs_durable::extension::GenerationEvent::OnYield {
                "YieldTasksWaiting"
            } else {
                "HookTasksWaiting"
            };
            if let Some(id) = value["phase"][phase]["children"][0].as_str() {
                let id = uuid::Uuid::parse_str(id).unwrap();
                if driver.get(id).unwrap().is_some_and(|task| {
                    matches!(task.state(), pi_rs_durable::task::TaskState::Waiting(_))
                }) {
                    break id;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    if prior == 1 {
        scheduler
            .submit(conversation, None, "queued before".into())
            .unwrap();
    }
    if prior == 2 {
        scheduler
            .reset(conversation, Some("reset before summary".into()))
            .unwrap();
    }
    if prior == 3 {
        return (scheduler, driver, conversation, operation, child);
    }
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Running
    );
    (scheduler, driver, conversation, operation, child)
}
fn wake_parked(driver: &pi_rs_durable::task_driver::Driver, child: uuid::Uuid) {
    driver
        .signal(&pi_rs_durable::task_store::Signal::new(
            uuid::Uuid::new_v4(),
            child,
            "go".into(),
            serde_json::Value::Null,
        ))
        .unwrap();
}
#[tokio::test]
async fn summaries_wait_for_parked_lifecycle_tasks_and_order_queued_inputs() {
    use pi_rs_durable::extension::GenerationEvent;
    for event in [GenerationEvent::OnYield, GenerationEvent::AfterRun] {
        for queued in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let model = Arc::new(BusyModel::default());
            let calls = Arc::new(AtomicUsize::new(0));
            let handler = parked_handler(event);
            let (mut scheduler, driver, conversation, operation, child) = parked_fixture(
                &dir.path().join("db"),
                event,
                model,
                calls.clone(),
                handler,
                0,
            )
            .await;
            if queued {
                scheduler
                    .submit(conversation, None, "queued".into())
                    .unwrap();
            }
            wake_parked(&driver, child);
            scheduler.wait(conversation).await.unwrap();
            assert_eq!(
                scheduler.independent_compaction_status(operation).unwrap(),
                IndependentCompactionStatus::Applied
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let c = scheduler.conversation(conversation).unwrap();
            let summary = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))).unwrap();
            if event == GenerationEvent::OnYield && !queued {
                let follow = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::User { content } if content == "child follow")).unwrap();
                assert!(summary < follow);
            } else {
                assert!(!c.messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content == "child follow")));
            }
            if queued {
                let input = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::User { content } if content == "queued")).unwrap();
                assert!(summary < input);
            }
            scheduler.shutdown().await.unwrap();
            driver.shutdown().unwrap();
        }
    }
}
#[tokio::test]
async fn parked_lifecycle_summary_and_child_recover_together() {
    use pi_rs_durable::extension::GenerationEvent;
    for event in [GenerationEvent::OnYield, GenerationEvent::AfterRun] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(BusyModel::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = parked_handler(event);
        let (mut scheduler, driver, conversation, operation, child) = parked_fixture(
            &path,
            event,
            model.clone(),
            calls.clone(),
            handler.clone(),
            0,
        )
        .await;
        scheduler
            .submit(conversation, None, "queued".into())
            .unwrap();
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
        let (extensions, tasks) = parked_registries(event, calls.clone(), handler);
        let (mut scheduler, driver) = Scheduler::open_with_summary_recovery_and_tasks(
            &path,
            busy_options(model),
            extensions,
            None,
            busy_provider(),
            tasks,
        )
        .unwrap();
        wake_parked(&driver, child);
        scheduler.wait(conversation).await.unwrap();
        assert_eq!(
            scheduler.independent_compaction_status(operation).unwrap(),
            IndependentCompactionStatus::Applied
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let c = scheduler.conversation(conversation).unwrap();
        let summary = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))).unwrap();
        let queued = c
            .messages
            .iter()
            .position(|m| matches!(m, pi_rs_core::Message::User { content } if content == "queued"))
            .unwrap();
        assert!(summary < queued);
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}
#[tokio::test]
async fn parked_lifecycle_abort_preserves_admitted_summary() {
    use pi_rs_durable::extension::GenerationEvent;
    for event in [GenerationEvent::OnYield, GenerationEvent::AfterRun] {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(BusyModel::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut scheduler, driver, conversation, operation, child) = parked_fixture(
            &dir.path().join("db"),
            event,
            model,
            calls.clone(),
            parked_handler(event),
            0,
        )
        .await;
        scheduler
            .submit(conversation, None, "withdraw me".into())
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            scheduler.abort_and_wait(conversation),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            scheduler.independent_compaction_status(operation).unwrap(),
            IndependentCompactionStatus::Applied
        );
        assert!(driver.get(child).unwrap().unwrap().is_terminal());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!scheduler.conversation(conversation).unwrap().messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content == "withdraw me")));
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}
#[tokio::test]
async fn parked_lifecycle_reset_follows_admitted_summary() {
    use pi_rs_durable::extension::GenerationEvent;
    for event in [GenerationEvent::OnYield, GenerationEvent::AfterRun] {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(BusyModel::default());
        let (mut scheduler, driver, conversation, operation, child) = parked_fixture(
            &dir.path().join("db"),
            event,
            model,
            Arc::new(AtomicUsize::new(0)),
            parked_handler(event),
            0,
        )
        .await;
        let reset = scheduler
            .reset(conversation, Some("reset after".into()))
            .unwrap();
        wake_parked(&driver, child);
        scheduler.wait(conversation).await.unwrap();
        assert_eq!(
            scheduler.independent_compaction_status(operation).unwrap(),
            IndependentCompactionStatus::Applied
        );
        let c = scheduler.conversation(conversation).unwrap();
        assert!(c.resets.iter().any(|r| r.id == reset));
        assert!(!c.messages.iter().any(
            |m| matches!(m, pi_rs_core::Message::User { content } if content == "child follow")
        ));
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}
#[tokio::test]
async fn parked_summary_kill_child() {
    let Ok(dir) = std::env::var("PI_PARKED_SUMMARY_KILL_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let event = pi_rs_durable::extension::GenerationEvent::OnYield;
    let (mut scheduler, _driver, conversation, operation, child) = parked_fixture(
        &dir.join("db"),
        event,
        Arc::new(BusyModel::default()),
        Arc::new(AtomicUsize::new(0)),
        parked_handler(event),
        0,
    )
    .await;
    scheduler
        .submit(conversation, None, "queued".into())
        .unwrap();
    std::fs::write(
        dir.join("ready"),
        format!("{conversation} {operation} {child}"),
    )
    .unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn killed_parked_summary_recovers_without_reinvoking_hook() {
    use pi_rs_durable::extension::GenerationEvent;
    let dir = tempfile::tempdir().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("parked_summary_kill_child")
        .arg("--nocapture")
        .env("PI_PARKED_SUMMARY_KILL_DIR", dir.path())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let ready = dir.path().join("ready");
    let found = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if let Ok(value) = std::fs::read_to_string(&ready) {
                break value;
            }
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited before ready: {status}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    if found.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let ids = found.unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    let ids: Vec<uuid::Uuid> = ids
        .split_whitespace()
        .map(|s| uuid::Uuid::parse_str(s).unwrap())
        .collect();
    let calls = Arc::new(AtomicUsize::new(0));
    let event = GenerationEvent::OnYield;
    let (extensions, tasks) = parked_registries(event, calls.clone(), parked_handler(event));
    let (mut scheduler, driver) = Scheduler::open_with_summary_recovery_and_tasks(
        dir.path().join("db"),
        busy_options(Arc::new(BusyModel::default())),
        extensions,
        None,
        busy_provider(),
        tasks,
    )
    .unwrap();
    wake_parked(&driver, ids[2]);
    scheduler.wait(ids[0]).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(ids[1]).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let c = scheduler.conversation(ids[0]).unwrap();
    let summary = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))).unwrap();
    let queued = c
        .messages
        .iter()
        .position(|m| matches!(m, pi_rs_core::Message::User { content } if content == "queued"))
        .unwrap();
    assert!(summary < queued);
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn reset_before_summary_while_hook_task_parked_is_stale() {
    use pi_rs_durable::extension::GenerationEvent;
    for event in [GenerationEvent::OnYield, GenerationEvent::AfterRun] {
        let dir = tempfile::tempdir().unwrap();
        let (mut scheduler, driver, conversation, operation, child) = parked_fixture(
            &dir.path().join("db"),
            event,
            Arc::new(BusyModel::default()),
            Arc::new(AtomicUsize::new(0)),
            parked_handler(event),
            2,
        )
        .await;
        wake_parked(&driver, child);
        scheduler.wait(conversation).await.unwrap();
        assert_eq!(
            scheduler.independent_compaction_status(operation).unwrap(),
            IndependentCompactionStatus::Stale
        );
        let c = scheduler.conversation(conversation).unwrap();
        assert!(c.messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content == "reset before summary")));
        assert!(!c.messages.iter().any(
            |m| matches!(m, pi_rs_core::Message::User { content } if content == "child follow")
        ));
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}

#[tokio::test]
async fn input_before_summary_while_hook_task_parked_follows_write() {
    use pi_rs_durable::extension::GenerationEvent;
    for event in [GenerationEvent::OnYield, GenerationEvent::AfterRun] {
        let dir = tempfile::tempdir().unwrap();
        let (mut scheduler, driver, conversation, operation, child) = parked_fixture(
            &dir.path().join("db"),
            event,
            Arc::new(BusyModel::default()),
            Arc::new(AtomicUsize::new(0)),
            parked_handler(event),
            1,
        )
        .await;
        wake_parked(&driver, child);
        scheduler.wait(conversation).await.unwrap();
        assert_eq!(
            scheduler.independent_compaction_status(operation).unwrap(),
            IndependentCompactionStatus::Applied
        );
        let c = scheduler.conversation(conversation).unwrap();
        let summary = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("busy checkpoint"))).unwrap();
        let input = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::User { content } if content == "queued before")).unwrap();
        assert!(summary < input);
        assert!(!c.messages.iter().any(
            |m| matches!(m, pi_rs_core::Message::User { content } if content == "child follow")
        ));
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}

#[tokio::test]
async fn abort_before_summary_admission_cancels_parked_task_and_summary() {
    use pi_rs_durable::extension::GenerationEvent;
    for event in [GenerationEvent::OnYield, GenerationEvent::AfterRun] {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut scheduler, driver, conversation, operation, child) = parked_fixture(
            &dir.path().join("db"),
            event,
            Arc::new(BusyModel::default()),
            calls.clone(),
            parked_handler(event),
            3,
        )
        .await;
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            scheduler.abort_and_wait(conversation),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            scheduler.independent_compaction_status(operation).unwrap(),
            IndependentCompactionStatus::Aborted
        );
        assert!(driver.get(child).unwrap().unwrap().is_terminal());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}
#[tokio::test]
async fn aborted_parked_hook_task_leaves_summary_for_boundary() {
    use pi_rs_durable::extension::GenerationEvent;
    for event in [GenerationEvent::OnYield, GenerationEvent::AfterRun] {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut scheduler, driver, conversation, operation, child) = parked_fixture(
            &dir.path().join("db"),
            event,
            Arc::new(BusyModel::default()),
            calls.clone(),
            parked_handler(event),
            0,
        )
        .await;
        driver.request_abort(child).unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            scheduler.wait(conversation),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(driver.get(child).unwrap().unwrap().is_terminal());
        assert_eq!(
            scheduler.independent_compaction_status(operation).unwrap(),
            IndependentCompactionStatus::Applied
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!scheduler.conversation(conversation).unwrap().messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content == "child follow")));
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}

struct ToolRoundModel {
    gate_next: std::sync::atomic::AtomicBool,
    started: tokio::sync::Semaphore,
    release: Arc<tokio::sync::Semaphore>,
    summary_started: tokio::sync::Semaphore,
    summary_release: Arc<tokio::sync::Semaphore>,
    requests: std::sync::Mutex<Vec<ModelRequest>>,
}
impl Default for ToolRoundModel {
    fn default() -> Self {
        Self {
            gate_next: false.into(),
            started: tokio::sync::Semaphore::new(0),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
            summary_started: tokio::sync::Semaphore::new(0),
            summary_release: Arc::new(tokio::sync::Semaphore::new(0)),
            requests: std::sync::Mutex::new(Vec::new()),
        }
    }
}
#[async_trait::async_trait]
impl Model for ToolRoundModel {
    async fn stream(&self, request: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        self.requests.lock().unwrap().push(request);
        if self.gate_next.swap(false, Ordering::SeqCst) {
            self.started.add_permits(1);
            let release = self.release.clone();
            return Ok(stream::once(async move {
                release.acquire().await.unwrap().forget();
                Ok(ModelEvent::Text("tool answer".into()))
            })
            .chain(stream::iter(vec![Ok(ModelEvent::Complete {
                calls: vec![pi_rs_core::ToolCall {
                    id: "call-1".into(),
                    name: "round-tool".into(),
                    arguments: "{}".into(),
                }],
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
        self.summary_started.add_permits(1);
        self.summary_release.acquire().await.unwrap().forget();
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("round checkpoint".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
struct RoundTool {
    calls: Arc<AtomicUsize>,
    handoff: bool,
}
#[async_trait::async_trait]
impl pi_rs_durable::keyed_tool::KeyedTool for RoundTool {
    fn spec(&self) -> pi_rs_core::ToolSpec {
        pi_rs_core::ToolSpec {
            name: "round-tool".into(),
            description: String::new(),
            parameters: serde_json::json!({"type":"object","properties":{}}),
        }
    }
    async fn invoke(
        &self,
        _: serde_json::Value,
        _: pi_rs_core::ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        _: pi_rs_durable::keyed_tool::KeyedHost,
    ) -> pi_rs_core::ToolReply {
        unreachable!()
    }
    async fn invoke_controlled(
        &self,
        _: serde_json::Value,
        _: pi_rs_core::ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        _: pi_rs_durable::keyed_tool::KeyedHost,
    ) -> (pi_rs_core::ToolReply, pi_rs_durable::reset::ToolControl) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        (
            pi_rs_core::Outcome::Succeeded("tool result".into()).into(),
            pi_rs_durable::reset::ToolControl {
                handoff: self.handoff.then(|| "round handoff".into()),
                ..Default::default()
            },
        )
    }
}
fn round_options(
    model: Arc<ToolRoundModel>,
    tool_calls: Arc<AtomicUsize>,
    handoff: bool,
) -> Options {
    let tool = Arc::new(pi_rs_durable::keyed_tool::KeyedToolAdapter::new(
        RoundTool {
            calls: tool_calls,
            handoff,
        },
    ));
    Options {
        model,
        tools: vec![tool.register(pi_rs_durable::recovery::ReplayPolicy::Safe)],
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn aftertools_handler(terminate: bool) -> Arc<dyn pi_rs_durable::task_scheduler::Handler> {
    use pi_rs_durable::task::{Checkpoint, Task, WaitFor};
    use pi_rs_durable::task_scheduler::Step;
    Arc::new(move |task: &Task| {
        if task.delivered_signal().is_some() {
            Ok(Step::Succeed(if terminate {
                serde_json::json!({"terminate":true})
            } else {
                serde_json::Value::Null
            }))
        } else {
            Ok(Step::Wait {
                checkpoint: Checkpoint {
                    phase: "waiting".into(),
                    data: task.checkpoint().data.clone(),
                },
                condition: WaitFor::Signal("go".into()),
                children: vec![],
            })
        }
    })
}
async fn aftertools_fixture(
    path: &std::path::Path,
    model: Arc<ToolRoundModel>,
    tool_calls: Arc<AtomicUsize>,
    hook_calls: Arc<AtomicUsize>,
    handler: Arc<dyn pi_rs_durable::task_scheduler::Handler>,
    handoff: bool,
    prior: u8,
) -> (
    Scheduler,
    pi_rs_durable::task_driver::Driver,
    uuid::Uuid,
    uuid::Uuid,
    uuid::Uuid,
) {
    use pi_rs_durable::extension::GenerationEvent;
    let (extensions, tasks) = parked_registries(GenerationEvent::AfterTools, hook_calls, handler);
    let (mut scheduler, driver) = Scheduler::open_with_summary_recovery_and_tasks(
        path,
        round_options(model.clone(), tool_calls, handoff),
        extensions,
        None,
        busy_provider(),
        tasks,
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
        .select_extensions(conversation, vec!["parked-hook".into()])
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
    model.summary_started.acquire().await.unwrap().forget();
    model.release.add_permits(1);
    let child = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let value =
                serde_json::to_value(scheduler.conversation(conversation).unwrap()).unwrap();
            if let Some(id) = value["phase"]["HookTasksWaiting"]["children"][0].as_str() {
                let id = uuid::Uuid::parse_str(id).unwrap();
                if driver.get(id).unwrap().is_some_and(|task| {
                    matches!(task.state(), pi_rs_durable::task::TaskState::Waiting(_))
                }) {
                    break id;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    if prior == 1 {
        scheduler
            .submit(conversation, None, "queued before".into())
            .unwrap();
    }
    if prior == 2 {
        scheduler
            .reset(conversation, Some("reset before".into()))
            .unwrap();
    }
    if prior != 3 {
        model.summary_release.add_permits(1);
        assert_eq!(
            scheduler
                .wait_independent_compaction(operation)
                .await
                .unwrap(),
            IndependentCompactionStatus::Running
        );
    }
    (scheduler, driver, conversation, operation, child)
}
#[tokio::test]
async fn aftertools_summary_precedes_steer_and_preserves_followup_boundary() {
    use pi_rs_durable::state::InputMode;
    for mode in [InputMode::Steer, InputMode::FollowUp] {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(ToolRoundModel::default());
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let hooks = Arc::new(AtomicUsize::new(0));
        let (mut scheduler, driver, conversation, operation, child) = aftertools_fixture(
            &dir.path().join("db"),
            model.clone(),
            tool_calls.clone(),
            hooks.clone(),
            aftertools_handler(false),
            false,
            0,
        )
        .await;
        scheduler
            .submit_with_mode(conversation, None, "queued".into(), mode)
            .unwrap();
        wake_parked(&driver, child);
        scheduler.wait(conversation).await.unwrap();
        assert_eq!(
            scheduler.independent_compaction_status(operation).unwrap(),
            IndependentCompactionStatus::Applied
        );
        assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
        assert_eq!(hooks.load(Ordering::SeqCst), 1);
        {
            let requests = model.requests.lock().unwrap();
            assert!(requests[3].messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("round checkpoint"))));
            assert!(requests[3].messages.iter().any(
            |m| matches!(m, pi_rs_core::Message::Tool { content, .. } if content == "tool result")
        ));
            assert_eq!(
                requests[3].messages.iter().any(
                    |m| matches!(m, pi_rs_core::Message::User { content } if content == "queued")
                ),
                mode == InputMode::Steer
            );
            if mode == InputMode::FollowUp {
                assert!(requests[4].messages.iter().any(
                    |m| matches!(m, pi_rs_core::Message::User { content } if content == "queued")
                ));
            }
        }
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}
#[tokio::test]
async fn aftertools_termination_places_summary_without_another_model_call() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(ToolRoundModel::default());
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let hooks = Arc::new(AtomicUsize::new(0));
    let (mut scheduler, driver, conversation, operation, child) = aftertools_fixture(
        &dir.path().join("db"),
        model.clone(),
        tool_calls.clone(),
        hooks.clone(),
        aftertools_handler(true),
        false,
        0,
    )
    .await;
    wake_parked(&driver, child);
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(model.requests.lock().unwrap().len(), 3);
    assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(hooks.load(Ordering::SeqCst), 1);
    let c = scheduler.conversation(conversation).unwrap();
    assert!(c.messages.iter().any(
        |m| matches!(m, pi_rs_core::Message::Tool { content, .. } if content == "tool result")
    ));
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}
#[tokio::test]
async fn aftertools_handoff_precedes_and_stales_summary() {
    for terminate in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(ToolRoundModel::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut scheduler, driver, conversation, operation, child) = aftertools_fixture(
            &dir.path().join("db"),
            model.clone(),
            calls.clone(),
            Arc::new(AtomicUsize::new(0)),
            aftertools_handler(terminate),
            true,
            0,
        )
        .await;
        wake_parked(&driver, child);
        scheduler.wait(conversation).await.unwrap();
        assert_eq!(
            scheduler.independent_compaction_status(operation).unwrap(),
            IndependentCompactionStatus::Stale
        );
        let c = scheduler.conversation(conversation).unwrap();
        assert!(c.messages.iter().any(
            |m| matches!(m, pi_rs_core::Message::User { content } if content == "round handoff")
        ));
        assert!(!c.messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("round checkpoint"))));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            model.requests.lock().unwrap().len(),
            if terminate { 3 } else { 4 }
        );
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}
#[tokio::test]
async fn aftertools_reset_order_is_durable() {
    for prior in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let (mut scheduler, driver, conversation, operation, child) = aftertools_fixture(
            &dir.path().join("db"),
            Arc::new(ToolRoundModel::default()),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            aftertools_handler(false),
            false,
            if prior { 2 } else { 0 },
        )
        .await;
        if !prior {
            scheduler
                .reset(conversation, Some("reset after".into()))
                .unwrap();
        }
        wake_parked(&driver, child);
        scheduler.wait(conversation).await.unwrap();
        assert_eq!(
            scheduler.independent_compaction_status(operation).unwrap(),
            if prior {
                IndependentCompactionStatus::Stale
            } else {
                IndependentCompactionStatus::Applied
            }
        );
        let c = scheduler.conversation(conversation).unwrap();
        assert!(c.messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content == if prior { "reset before" } else { "reset after" })));
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}
#[tokio::test]
async fn aftertools_abort_distinguishes_before_and_after_summary_admission() {
    for admitted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut scheduler, driver, conversation, operation, child) = aftertools_fixture(
            &dir.path().join("db"),
            Arc::new(ToolRoundModel::default()),
            calls.clone(),
            Arc::new(AtomicUsize::new(0)),
            aftertools_handler(false),
            false,
            if admitted { 0 } else { 3 },
        )
        .await;
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            scheduler.abort_and_wait(conversation),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            scheduler.independent_compaction_status(operation).unwrap(),
            if admitted {
                IndependentCompactionStatus::Applied
            } else {
                IndependentCompactionStatus::Aborted
            }
        );
        assert!(driver.get(child).unwrap().unwrap().is_terminal());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}
#[tokio::test]
async fn aftertools_parked_summary_recovers_without_repeating_tool_or_hook() {
    use pi_rs_durable::extension::GenerationEvent;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(ToolRoundModel::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let hooks = Arc::new(AtomicUsize::new(0));
    let handler = aftertools_handler(false);
    let (scheduler, driver, conversation, operation, child) = aftertools_fixture(
        &path,
        model.clone(),
        calls.clone(),
        hooks.clone(),
        handler.clone(),
        false,
        1,
    )
    .await;
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
    let (extensions, tasks) =
        parked_registries(GenerationEvent::AfterTools, hooks.clone(), handler);
    let (mut scheduler, driver) = Scheduler::open_with_summary_recovery_and_tasks(
        &path,
        round_options(model, calls.clone(), false),
        extensions,
        None,
        busy_provider(),
        tasks,
    )
    .unwrap();
    wake_parked(&driver, child);
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(hooks.load(Ordering::SeqCst), 1);
    let c = scheduler.conversation(conversation).unwrap();
    assert!(
        c.messages.iter().any(
            |m| matches!(m, pi_rs_core::Message::User { content } if content == "queued before")
        )
    );
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}
#[tokio::test]
async fn aftertools_summary_kill_child() {
    let Ok(dir) = std::env::var("PI_AFTERTOOLS_SUMMARY_KILL_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let (mut scheduler, _driver, conversation, operation, child) = aftertools_fixture(
        &dir.join("db"),
        Arc::new(ToolRoundModel::default()),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        aftertools_handler(false),
        false,
        0,
    )
    .await;
    scheduler
        .submit(conversation, None, "queued".into())
        .unwrap();
    std::fs::write(
        dir.join("ready"),
        format!("{conversation} {operation} {child}"),
    )
    .unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn killed_aftertools_summary_recovers_without_repeating_tool_or_hook() {
    use pi_rs_durable::extension::GenerationEvent;
    let dir = tempfile::tempdir().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("aftertools_summary_kill_child")
        .arg("--nocapture")
        .env("PI_AFTERTOOLS_SUMMARY_KILL_DIR", dir.path())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let ready = dir.path().join("ready");
    let found = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if let Ok(value) = std::fs::read_to_string(&ready) {
                break value;
            }
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited before ready: {status}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    if found.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let ids = found.unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    let ids: Vec<uuid::Uuid> = ids
        .split_whitespace()
        .map(|s| uuid::Uuid::parse_str(s).unwrap())
        .collect();
    let calls = Arc::new(AtomicUsize::new(0));
    let hooks = Arc::new(AtomicUsize::new(0));
    let (extensions, tasks) = parked_registries(
        GenerationEvent::AfterTools,
        hooks.clone(),
        aftertools_handler(false),
    );
    let (mut scheduler, driver) = Scheduler::open_with_summary_recovery_and_tasks(
        dir.path().join("db"),
        round_options(Arc::new(ToolRoundModel::default()), calls.clone(), false),
        extensions,
        None,
        busy_provider(),
        tasks,
    )
    .unwrap();
    wake_parked(&driver, ids[2]);
    scheduler.wait(ids[0]).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(ids[1]).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(hooks.load(Ordering::SeqCst), 0);
    let c = scheduler.conversation(ids[0]).unwrap();
    assert!(c.messages.iter().any(
        |m| matches!(m, pi_rs_core::Message::Tool { content, .. } if content == "tool result")
    ));
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m, pi_rs_core::Message::User { content } if content == "queued"))
    );
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}
#[tokio::test]
async fn aborted_aftertools_child_leaves_summary_for_tool_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(ToolRoundModel::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let (mut scheduler, driver, conversation, operation, child) = aftertools_fixture(
        &dir.path().join("db"),
        model.clone(),
        calls.clone(),
        Arc::new(AtomicUsize::new(0)),
        aftertools_handler(false),
        false,
        0,
    )
    .await;
    driver.request_abort(child).unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        scheduler.wait(conversation),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(driver.get(child).unwrap().unwrap().is_terminal());
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(model.requests.lock().unwrap().len(), 4);
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}

struct DirectAfterToolsGate {
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    calls: AtomicUsize,
    effects: AtomicUsize,
    make_task: bool,
}
#[async_trait::async_trait]
impl pi_rs_durable::extension::Hook for DirectAfterToolsGate {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        _: &serde_json::Value,
        ctx: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision, GenerationEvent, HookTask};
        if event == GenerationEvent::AfterTools {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if ctx.memo.get("effect").await?.is_none() {
                ctx.memo
                    .get_or_insert("effect", serde_json::json!(true))
                    .await?;
                self.effects.fetch_add(1, Ordering::SeqCst);
            }
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            return Ok(if self.make_task {
                GenerationDecision::Tasks(vec![HookTask {
                    name: "direct-job".into(),
                    input: serde_json::json!({"selected":"exact"}),
                }])
            } else {
                GenerationDecision::Pass
            });
        }
        Ok(GenerationDecision::Pass)
    }
}
fn direct_aftertools_registries(
    hook: Arc<DirectAfterToolsGate>,
    handler: Arc<dyn pi_rs_durable::task_scheduler::Handler>,
) -> (
    pi_rs_durable::extension::Registry,
    pi_rs_durable::task_scheduler::Registry,
) {
    let mut extensions = pi_rs_durable::extension::Registry::default();
    extensions
        .install(pi_rs_durable::extension::Extension {
            name: "direct-aftertools".into(),
            hooks: vec![hook],
            tasks: vec![("direct-job".into(), handler.clone())],
            ..Default::default()
        })
        .unwrap();
    let mut tasks = pi_rs_durable::task_scheduler::Registry::default();
    assert!(tasks.register_shared("direct-job", true, handler));
    (extensions, tasks)
}
fn direct_aftertools_gate(make_task: bool) -> Arc<DirectAfterToolsGate> {
    Arc::new(DirectAfterToolsGate {
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        calls: AtomicUsize::new(0),
        effects: AtomicUsize::new(0),
        make_task,
    })
}
async fn direct_aftertools_fixture(
    path: &std::path::Path,
    model: Arc<ToolRoundModel>,
    tool_calls: Arc<AtomicUsize>,
    hook: Arc<DirectAfterToolsGate>,
    handler: Arc<dyn pi_rs_durable::task_scheduler::Handler>,
    handoff: bool,
) -> (
    Scheduler,
    pi_rs_durable::task_driver::Driver,
    uuid::Uuid,
    uuid::Uuid,
) {
    let (extensions, tasks) = direct_aftertools_registries(hook.clone(), handler);
    let (mut scheduler, driver) = Scheduler::open_with_summary_recovery_and_tasks(
        path,
        round_options(model.clone(), tool_calls, handoff),
        extensions,
        None,
        busy_provider(),
        tasks,
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
        .select_extensions(conversation, vec!["direct-aftertools".into()])
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
    model.summary_started.acquire().await.unwrap().forget();
    model.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), hook.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    (scheduler, driver, conversation, operation)
}
async fn admit_direct_summary(
    scheduler: &mut Scheduler,
    model: &ToolRoundModel,
    operation: uuid::Uuid,
) {
    model.summary_release.add_permits(1);
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Running
    );
}
async fn direct_aftertools_child(
    scheduler: &Scheduler,
    driver: &pi_rs_durable::task_driver::Driver,
    conversation: uuid::Uuid,
) -> uuid::Uuid {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let state =
                serde_json::to_value(scheduler.conversation(conversation).unwrap()).unwrap();
            if let Some(raw) = state["phase"]["HookTasksWaiting"]["children"][0].as_str() {
                let id = uuid::Uuid::parse_str(raw).unwrap();
                if let Some(task) = driver.get(id).unwrap()
                    && matches!(task.state(), pi_rs_durable::task::TaskState::Waiting(_))
                {
                    assert_eq!(
                        task.checkpoint().data,
                        serde_json::json!({"selected":"exact"})
                    );
                    break id;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn direct_aftertools_summary_preserves_completed_pass_and_order() {
    use pi_rs_durable::state::InputMode;
    for mode in [InputMode::Steer, InputMode::FollowUp] {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(ToolRoundModel::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let hook = direct_aftertools_gate(false);
        let (mut scheduler, driver, conversation, operation) = direct_aftertools_fixture(
            &dir.path().join("db"),
            model.clone(),
            calls.clone(),
            hook.clone(),
            aftertools_handler(false),
            false,
        )
        .await;
        scheduler
            .submit_with_mode(conversation, None, "queued".into(), mode)
            .unwrap();
        admit_direct_summary(&mut scheduler, &model, operation).await;
        hook.release.add_permits(1);
        scheduler.wait(conversation).await.unwrap();
        assert_eq!(
            scheduler.independent_compaction_status(operation).unwrap(),
            IndependentCompactionStatus::Applied
        );
        assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
        assert_eq!(hook.effects.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        {
            let requests = model.requests.lock().unwrap();
            assert!(requests[3].messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("round checkpoint"))));
            assert!(requests[3].messages.iter().any(|m| matches!(m, pi_rs_core::Message::Tool { content, .. } if content == "tool result")));
            assert_eq!(
                requests[3].messages.iter().any(
                    |m| matches!(m, pi_rs_core::Message::User { content } if content == "queued")
                ),
                mode == InputMode::Steer
            );
            if mode == InputMode::FollowUp {
                assert!(requests[4].messages.iter().any(
                    |m| matches!(m, pi_rs_core::Message::User { content } if content == "queued")
                ));
            }
        }
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}
#[tokio::test]
async fn direct_aftertools_completed_task_decision_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(ToolRoundModel::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let hook = direct_aftertools_gate(true);
    let handler = aftertools_handler(true);
    let (mut scheduler, driver, conversation, operation) = direct_aftertools_fixture(
        &path,
        model.clone(),
        calls.clone(),
        hook.clone(),
        handler.clone(),
        false,
    )
    .await;
    admit_direct_summary(&mut scheduler, &model, operation).await;
    hook.release.add_permits(1);
    let child = direct_aftertools_child(&scheduler, &driver, conversation).await;
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
    let (extensions, tasks) = direct_aftertools_registries(hook.clone(), handler);
    let (mut scheduler, driver) = Scheduler::open_with_summary_recovery_and_tasks(
        &path,
        round_options(model.clone(), calls.clone(), false),
        extensions,
        None,
        busy_provider(),
        tasks,
    )
    .unwrap();
    wake_parked(&driver, child);
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(model.requests.lock().unwrap().len(), 3);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    assert_eq!(hook.effects.load(Ordering::SeqCst), 1);
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}
#[tokio::test]
async fn direct_aftertools_handoff_stales_admitted_summary() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(ToolRoundModel::default());
    let hook = direct_aftertools_gate(false);
    let (mut scheduler, driver, conversation, operation) = direct_aftertools_fixture(
        &dir.path().join("db"),
        model.clone(),
        Arc::new(AtomicUsize::new(0)),
        hook.clone(),
        aftertools_handler(false),
        true,
    )
    .await;
    admit_direct_summary(&mut scheduler, &model, operation).await;
    hook.release.add_permits(1);
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Stale
    );
    assert!(
        scheduler
            .conversation(conversation)
            .unwrap()
            .messages
            .iter()
            .any(
                |m| matches!(m, pi_rs_core::Message::User { content } if content == "round handoff")
            )
    );
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}
#[tokio::test]
async fn direct_aftertools_abort_preserves_admitted_summary() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(ToolRoundModel::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let hook = direct_aftertools_gate(false);
    let (mut scheduler, driver, conversation, operation) = direct_aftertools_fixture(
        &dir.path().join("db"),
        model.clone(),
        calls.clone(),
        hook.clone(),
        aftertools_handler(false),
        false,
    )
    .await;
    admit_direct_summary(&mut scheduler, &model, operation).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        scheduler.abort_and_wait(conversation),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}
#[tokio::test]
async fn interrupted_direct_aftertools_hook_replays_with_memo() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(ToolRoundModel::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let hook = direct_aftertools_gate(false);
    let handler = aftertools_handler(false);
    let (mut scheduler, driver, conversation, operation) = direct_aftertools_fixture(
        &path,
        model.clone(),
        calls.clone(),
        hook.clone(),
        handler.clone(),
        false,
    )
    .await;
    admit_direct_summary(&mut scheduler, &model, operation).await;
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
    hook.release.add_permits(1);
    let (extensions, tasks) = direct_aftertools_registries(hook.clone(), handler);
    let (mut scheduler, driver) = Scheduler::open_with_summary_recovery_and_tasks(
        &path,
        round_options(model, calls.clone(), false),
        extensions,
        None,
        busy_provider(),
        tasks,
    )
    .unwrap();
    scheduler.wait(conversation).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 2);
    assert_eq!(hook.effects.load(Ordering::SeqCst), 1);
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}
#[tokio::test]
async fn direct_aftertools_reset_order_is_preserved() {
    for prior in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(ToolRoundModel::default());
        let hook = direct_aftertools_gate(false);
        let (mut scheduler, driver, conversation, operation) = direct_aftertools_fixture(
            &dir.path().join("db"),
            model.clone(),
            Arc::new(AtomicUsize::new(0)),
            hook.clone(),
            aftertools_handler(false),
            false,
        )
        .await;
        if prior {
            scheduler
                .reset(conversation, Some("reset before".into()))
                .unwrap();
        }
        admit_direct_summary(&mut scheduler, &model, operation).await;
        if !prior {
            scheduler
                .reset(conversation, Some("reset after".into()))
                .unwrap();
        }
        hook.release.add_permits(1);
        scheduler.wait(conversation).await.unwrap();
        assert_eq!(
            scheduler.independent_compaction_status(operation).unwrap(),
            if prior {
                IndependentCompactionStatus::Stale
            } else {
                IndependentCompactionStatus::Applied
            }
        );
        assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
        let c = scheduler.conversation(conversation).unwrap();
        assert!(c.messages.iter().any(|m| matches!(m, pi_rs_core::Message::User { content } if content == if prior { "reset before" } else { "reset after" })));
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}
#[tokio::test]
async fn direct_aftertools_kill_child() {
    let Ok(dir) = std::env::var("PI_DIRECT_AFTERTOOLS_KILL_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let model = Arc::new(ToolRoundModel::default());
    let hook = direct_aftertools_gate(false);
    let (mut scheduler, _driver, conversation, operation) = direct_aftertools_fixture(
        &dir.join("db"),
        model.clone(),
        Arc::new(AtomicUsize::new(0)),
        hook,
        aftertools_handler(false),
        false,
    )
    .await;
    scheduler
        .submit(conversation, None, "queued".into())
        .unwrap();
    admit_direct_summary(&mut scheduler, &model, operation).await;
    std::fs::write(dir.join("ready"), format!("{conversation} {operation}")).unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn killed_direct_aftertools_hook_replays_memo_without_repeating_tool() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("direct_aftertools_kill_child")
        .arg("--nocapture")
        .env("PI_DIRECT_AFTERTOOLS_KILL_DIR", dir.path())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let ready = dir.path().join("ready");
    let found = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if let Ok(value) = std::fs::read_to_string(&ready) {
                break value;
            }
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited before ready: {status}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    if found.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let ids = found.unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    let ids: Vec<uuid::Uuid> = ids
        .split_whitespace()
        .map(|s| uuid::Uuid::parse_str(s).unwrap())
        .collect();
    let calls = Arc::new(AtomicUsize::new(0));
    let hook = direct_aftertools_gate(false);
    hook.release.add_permits(1);
    let (extensions, tasks) = direct_aftertools_registries(hook.clone(), aftertools_handler(false));
    let (mut scheduler, driver) = Scheduler::open_with_summary_recovery_and_tasks(
        dir.path().join("db"),
        round_options(Arc::new(ToolRoundModel::default()), calls.clone(), false),
        extensions,
        None,
        busy_provider(),
        tasks,
    )
    .unwrap();
    scheduler.wait(ids[0]).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(ids[1]).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    assert_eq!(hook.effects.load(Ordering::SeqCst), 0);
    let c = scheduler.conversation(ids[0]).unwrap();
    let summary = c.messages.iter().position(|m| matches!(m, pi_rs_core::Message::User { content } if content.contains("round checkpoint"))).unwrap();
    let queued = c
        .messages
        .iter()
        .position(|m| matches!(m, pi_rs_core::Message::User { content } if content == "queued"))
        .unwrap();
    assert!(summary < queued);
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}
#[tokio::test]
async fn direct_aftertools_abort_before_summary_admission_cancels_worker() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(ToolRoundModel::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let hook = direct_aftertools_gate(false);
    let (mut scheduler, driver, conversation, operation) = direct_aftertools_fixture(
        &dir.path().join("db"),
        model,
        calls.clone(),
        hook.clone(),
        aftertools_handler(false),
        false,
    )
    .await;
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        scheduler.abort_and_wait(conversation),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Aborted
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}
