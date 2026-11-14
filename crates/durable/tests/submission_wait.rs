use async_trait::async_trait;
use futures_util::{StreamExt, poll, stream};
use pi_rs_core::{
    CancellationToken, Error as ModelError, Model, ModelEvent, ModelRequest, ModelStream,
};
use pi_rs_durable::{
    recovery::UncertainPolicy,
    runtime::{
        Config, Error, Options, Scheduler, SubmissionKind, SubmissionSettlement, WithdrawResult,
    },
};
use std::{sync::Arc, task::Poll, time::Duration};
use tokio::sync::Notify;
use uuid::Uuid;

struct Gate {
    started: Arc<Notify>,
    release: Arc<Notify>,
}
#[async_trait]
impl Model for Gate {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
fn options(started: Arc<Notify>, release: Arc<Notify>) -> Options {
    Options {
        model: Arc::new(Gate { started, release }),
        tools: vec![],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 100,
    }
}
async fn wait(s: &Scheduler, c: Uuid, id: Uuid) -> Result<SubmissionSettlement, Error> {
    tokio::time::timeout(
        Duration::from_secs(5),
        s.wait_for_submission(c, id, &CancellationToken::new()),
    )
    .await
    .unwrap()
}
#[tokio::test]
async fn active_queued_cancellation_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut s = Scheduler::open(&path, options(started.clone(), release.clone())).unwrap();
    let c = s.create_conversation(config()).unwrap();
    let first = s.submit(c, None, "first".into()).unwrap();
    started.notified().await;
    let second = s.submit(c, None, "second".into()).unwrap();
    let cancel = CancellationToken::new();
    let mut pending = Box::pin(s.wait_for_submission(c, second, &cancel));
    assert!(matches!(poll!(&mut pending), Poll::Pending));
    cancel.cancel();
    assert!(matches!(pending.await, Err(Error::WaitCancelled)));
    release.notify_one();
    started.notified().await;
    assert!(
        matches!(wait(&s,c,first).await.unwrap(), SubmissionSettlement::Input { answer, answer_id: Some(_) } if answer == "answer")
    );
    let token = CancellationToken::new();
    let mut pending = Box::pin(s.wait_for_submission(c, second, &token));
    assert!(matches!(poll!(&mut pending), Poll::Pending));
    drop(pending);
    release.notify_one();
    assert!(matches!(
        wait(&s, c, second).await.unwrap(),
        SubmissionSettlement::Input { .. }
    ));
    s.shutdown().await.unwrap();
    let s = Scheduler::open(&path, options(started, release)).unwrap();
    assert!(matches!(
        wait(&s, c, first).await.unwrap(),
        SubmissionSettlement::Input { .. }
    ));
    assert!(matches!(
        wait(&s, c, Uuid::new_v4()).await,
        Err(Error::SubmissionNotFound)
    ));
    assert!(matches!(
        wait(&s, Uuid::new_v4(), first).await,
        Err(Error::NotFound)
    ));
    s.shutdown().await.unwrap();
}
#[tokio::test]
async fn write_and_empty_continue_settle() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Scheduler::open(
        dir.path().join("db"),
        options(Arc::new(Notify::new()), Arc::new(Notify::new())),
    )
    .unwrap();
    let c = s.create_conversation(config()).unwrap();
    let w = s.submit_write(c, None, "app.note".into(), None).unwrap();
    assert_eq!(wait(&s, c, w).await.unwrap(), SubmissionSettlement::Write);
    let continuation = s.submit_continue(c, None).unwrap();
    assert_eq!(
        wait(&s, c, continuation).await.unwrap(),
        SubmissionSettlement::Unanswered {
            kind: SubmissionKind::Continue,
            reason: "nothing_to_answer".into()
        }
    );
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn queued_write_waits_until_reset_is_placed() {
    let dir = tempfile::tempdir().unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut s = Scheduler::open(
        dir.path().join("db"),
        options(started.clone(), release.clone()),
    )
    .unwrap();
    let c = s.create_conversation(config()).unwrap();
    s.submit(c, None, "first".into()).unwrap();
    started.notified().await;
    s.reset(c, None).unwrap();
    let w = s.submit_write(c, None, "app.note".into(), None).unwrap();
    let cancel = CancellationToken::new();
    let mut pending = Box::pin(s.wait_for_submission(c, w, &cancel));
    assert!(matches!(poll!(&mut pending), Poll::Pending));
    release.notify_one();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), &mut pending)
            .await
            .unwrap()
            .unwrap(),
        SubmissionSettlement::Write
    );
    drop(pending);
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn withdrawn_write_settles_across_reopen_and_other_writes_follow_resets() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut s = Scheduler::open(&path, options(started.clone(), release.clone())).unwrap();
    let c = s.create_conversation(config()).unwrap();
    let immediate = s
        .submit_write(c, None, "app.immediate".into(), None)
        .unwrap();
    assert_eq!(
        s.withdraw_submission(c, immediate).unwrap(),
        WithdrawResult::Settled
    );
    s.submit(c, None, "first".into()).unwrap();
    started.notified().await;
    let first = s.reset(c, None).unwrap();
    let withdrawn = s
        .submit_write(c, Some("key".into()), "app.withdrawn".into(), None)
        .unwrap();
    let second = s.reset(c, None).unwrap();
    let kept = s.submit_write(c, None, "app.kept".into(), None).unwrap();
    assert_eq!(
        s.withdraw_submission(c, withdrawn).unwrap(),
        WithdrawResult::Aborted
    );
    assert_eq!(
        wait(&s, c, withdrawn).await.unwrap(),
        SubmissionSettlement::WriteUnanswered {
            reason: "aborted".into()
        }
    );
    s.shutdown().await.unwrap();
    let s = Scheduler::open(&path, options(started.clone(), release.clone())).unwrap();
    assert_eq!(
        wait(&s, c, withdrawn).await.unwrap(),
        SubmissionSettlement::WriteUnanswered {
            reason: "aborted".into()
        }
    );
    started.notified().await;
    release.notify_one();
    assert_eq!(
        wait(&s, c, kept).await.unwrap(),
        SubmissionSettlement::Write
    );
    let state = s.conversation(c).unwrap();
    let w = state
        .application_writes
        .iter()
        .find(|w| w.id == withdrawn)
        .unwrap();
    assert!(w.withdrawn && !w.placed && w.after_reset == Some(first));
    let w = state
        .application_writes
        .iter()
        .find(|w| w.id == kept)
        .unwrap();
    assert!(w.placed && !w.withdrawn && w.after_reset == Some(second));
    s.shutdown().await.unwrap();
}

struct PanicModel;
#[async_trait]
impl Model for PanicModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
        panic!("model panic")
    }
}
#[tokio::test]
async fn worker_panic_fails_pending_wait() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = options(Arc::new(Notify::new()), Arc::new(Notify::new()));
    opts.model = Arc::new(PanicModel);
    let mut s = Scheduler::open(dir.path().join("db"), opts).unwrap();
    let c = s.create_conversation(config()).unwrap();
    let id = s.submit(c, None, "first".into()).unwrap();
    assert!(
        matches!(wait(&s,c,id).await, Err(Error::Invalid(message)) if message.contains("worker panicked"))
    );
    assert!(s.shutdown().await.is_err());
}

#[tokio::test]
async fn recovers_pending_submission_and_waits_for_its_answer() {
    use pi_rs_durable::runtime::Harness;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (c, id) = {
        let mut h = Harness::open_paused(&path, options(started.clone(), release.clone())).unwrap();
        let c = h.create_conversation(config()).unwrap();
        let id = h.submit(c, None, "recover".into()).unwrap();
        (c, id)
    };
    let s = Scheduler::open(&path, options(started.clone(), release.clone())).unwrap();
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    let token = CancellationToken::new();
    let mut pending = Box::pin(s.wait_for_submission(c, id, &token));
    assert!(matches!(poll!(&mut pending), Poll::Pending));
    release.notify_one();
    assert!(
        matches!(tokio::time::timeout(Duration::from_secs(5), &mut pending).await.unwrap().unwrap(), SubmissionSettlement::Input { answer, .. } if answer == "answer")
    );
    drop(pending);
    s.shutdown().await.unwrap();
}
