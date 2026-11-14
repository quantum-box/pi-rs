use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{Error, Message, Model, ModelEvent, ModelRequest, ModelStream, UsageReport};
use pi_rs_durable::{
    recovery::UncertainPolicy,
    runtime::{Config, Options, Scheduler, Status},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use uuid::Uuid;

struct TestModel {
    calls: AtomicUsize,
    failures: usize,
    error: &'static str,
    settled: Mutex<Vec<Uuid>>,
    summaries: AtomicUsize,
}
#[async_trait]
impl Model for TestModel {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        if request.messages.iter().any(|m| matches!(m, Message::System { content } if content.starts_with("You are a context summarization"))) {
            self.summaries.fetch_add(1, Ordering::SeqCst);
            return Ok(stream::iter(vec![Ok(ModelEvent::Text("summary".into())), Ok(ModelEvent::Complete { calls: vec![], usage: None })]).boxed());
        }
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n < self.failures {
            return Err(Error::Model(self.error.into()));
        }
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("ok".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
    async fn record_usage(&self, id: Uuid, report: &UsageReport) -> Result<(), Error> {
        if matches!(report, UsageReport::Unconfirmed) {
            self.settled.lock().unwrap().push(id);
        }
        Ok(())
    }
}
fn model(failures: usize, error: &'static str) -> Arc<TestModel> {
    Arc::new(TestModel {
        calls: AtomicUsize::new(0),
        failures,
        error,
        settled: Mutex::new(vec![]),
        summaries: AtomicUsize::new(0),
    })
}
fn options(model: Arc<TestModel>) -> Options {
    Options {
        model,
        tools: vec![],
        restart_limit: 1,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: "system".into(),
        max_output_tokens: 100,
    }
}

#[tokio::test]
async fn retry_survives_shutdown_and_recovers_in_background() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let m = model(1, "503 overloaded");
    let mut s = Scheduler::open(&path, options(m.clone())).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "hi".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while m.settled.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    s.shutdown().await.unwrap();
    let mut s = Scheduler::open(&path, options(m.clone())).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(4), s.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        s.conversation(id).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
    assert_eq!(m.calls.load(Ordering::SeqCst), 2);
    assert_eq!(m.settled.lock().unwrap().len(), 2); // failed request and successful request without usage
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn permanent_failure_is_settled_and_not_retried() {
    let dir = tempfile::tempdir().unwrap();
    let m = model(10, "invalid api key");
    let mut s = Scheduler::open(dir.path().join("db"), options(m.clone())).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "hi".into()).unwrap();
    s.wait(id).await.unwrap();
    assert!(matches!(
        s.conversation(id).unwrap().submissions[0].status,
        Status::Unanswered { .. }
    ));
    assert_eq!(m.calls.load(Ordering::SeqCst), 1);
    assert_eq!(m.settled.lock().unwrap().len(), 1);
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn overflow_compacts_once_and_preserves_latest_input() {
    let dir = tempfile::tempdir().unwrap();
    let m = model(0, "context length exceeded");
    let mut s = Scheduler::open(dir.path().join("db"), options(m.clone())).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "old input".into()).unwrap();
    s.wait(id).await.unwrap();
    // Force the next call to overflow exactly once.
    let m2 = model(2, "context length exceeded");
    s.shutdown().await.unwrap();
    // Sharing call count is unnecessary: one old turn is already persisted.
    let mut s = Scheduler::open(dir.path().join("db"), options(m2.clone())).unwrap();
    s.submit(id, None, "latest input".into()).unwrap();
    s.wait(id).await.unwrap();
    let c = s.conversation(id).unwrap();
    assert!(matches!(c.submissions[1].status, Status::Unanswered { .. }));
    assert_eq!(m2.summaries.load(Ordering::SeqCst), 1);
    assert_eq!(m2.calls.load(Ordering::SeqCst), 2);
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m, Message::User { content } if content == "latest input"))
    );
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn retry_budget_survives_reopen_and_is_independent_of_restart_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let m = model(99, "503 overloaded");
    let mut s = Scheduler::open(&path, options(m.clone())).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "hi".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while m.calls.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        while m.settled.lock().unwrap().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    s.shutdown().await.unwrap();
    let mut s = Scheduler::open(&path, options(m.clone())).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(15), s.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.calls.load(Ordering::SeqCst), 4);
    assert_eq!(m.settled.lock().unwrap().len(), 4);
    assert!(matches!(
        s.conversation(id).unwrap().submissions[0].status,
        Status::Unanswered { .. }
    ));
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn overflow_compaction_can_resume_successfully() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let first = model(0, "");
    let mut s = Scheduler::open(&path, options(first)).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "previous".into()).unwrap();
    s.wait(id).await.unwrap();
    s.shutdown().await.unwrap();
    let next = model(1, "context length exceeded");
    let mut s = Scheduler::open(&path, options(next.clone())).unwrap();
    s.submit(id, None, "current".into()).unwrap();
    s.wait(id).await.unwrap();
    let c = s.conversation(id).unwrap();
    assert!(matches!(c.submissions[1].status, Status::Done { .. }));
    assert_eq!(next.summaries.load(Ordering::SeqCst), 1);
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m, Message::User { content } if content == "current"))
    );
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m, Message::User { content } if content.contains("summary")))
    );
    s.shutdown().await.unwrap();
}

struct OverflowHook(Arc<Mutex<Vec<pi_rs_durable::extension::CompactionReason>>>);
#[async_trait]
impl pi_rs_durable::extension::Hook for OverflowHook {
    async fn before_compact(
        &self,
        input: &pi_rs_durable::extension::CompactionInput,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::BeforeCompact, String> {
        self.0.lock().unwrap().push(input.reason);
        Ok(pi_rs_durable::extension::BeforeCompact::Summary(
            "overflow hook".into(),
        ))
    }
}
#[tokio::test]
async fn overflow_compaction_uses_hook_summary() {
    use pi_rs_core::CancellationToken;
    use pi_rs_durable::{
        extension::{CompactionReason, Extension},
        runtime::Harness,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open_paused(&path, options(model(0, ""))).unwrap();
        id = h.create_conversation(config()).unwrap();
        h.submit_and_run(id, None, "previous".into(), &cancel)
            .await
            .unwrap();
    }
    let next = model(1, "context length exceeded");
    let seen = Arc::new(Mutex::new(vec![]));
    let mut h = Harness::open_paused(&path, options(next.clone())).unwrap();
    h.install_extension(Extension {
        name: "overflow".into(),
        hooks: vec![Arc::new(OverflowHook(seen.clone()))],
        ..Default::default()
    })
    .unwrap();
    h.select_extensions(id, vec!["overflow".into()]).unwrap();
    h.submit_and_run(id, None, "current".into(), &cancel)
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![CompactionReason::Overflow]);
    assert_eq!(next.summaries.load(Ordering::SeqCst), 0);
    assert!(matches!(
        h.conversation(id).unwrap().submissions[1].status,
        Status::Done { .. }
    ));
}
