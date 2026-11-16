use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{CancellationToken, Error, Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::{
    recovery::UncertainPolicy,
    runtime::{AfterFailedRun, Config, Harness, Options, Status},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct FailOnce(AtomicUsize);
#[async_trait]
impl Model for FailOnce {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(Error::Model("invalid request".into()));
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
}
fn options(model: Arc<FailOnce>) -> Options {
    Options {
        model,
        tools: vec![],
        restart_limit: 3,
        max_turns: 5,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
#[tokio::test]
async fn policies_survive_reopen_and_preserve_request_identity() {
    for policy in [
        AfterFailedRun::Keep,
        AfterFailedRun::Continue,
        AfterFailedRun::Withdraw,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(FailOnce(AtomicUsize::new(0)));
        let cancel = CancellationToken::new();
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        let id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        if policy != AfterFailedRun::Keep {
            h.set_after_failed_run(id, policy).unwrap();
        }
        let first = h.submit(id, Some("first".into()), "first".into()).unwrap();
        let queued = h
            .submit(id, Some("queued".into()), "queued".into())
            .unwrap();
        assert_eq!(
            h.submit(id, Some("queued".into()), "different".into())
                .unwrap(),
            queued
        );
        drop(h);
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        assert_eq!(h.conversation(id).unwrap().after_failed_run, policy);
        h.resume_conversation(id, &cancel).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert!(matches!(
            c.submissions.iter().find(|s| s.id == first).unwrap().status,
            Status::Unanswered { .. }
        ));
        assert!(c.run_failure.is_some());
        let status = &c
            .submissions
            .iter()
            .find(|s| s.id == queued)
            .unwrap()
            .status;
        match policy {
            AfterFailedRun::Keep => assert_eq!(*status, Status::Queued),
            AfterFailedRun::Continue => assert!(matches!(status, Status::Done { .. })),
            AfterFailedRun::Withdraw => assert_eq!(
                *status,
                Status::Unanswered {
                    reason: "run_failed".into()
                }
            ),
        }
        drop(h);
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        assert_eq!(h.conversation(id).unwrap().after_failed_run, policy);
        assert_eq!(
            h.submit(id, Some("queued".into()), "duplicate".into())
                .unwrap(),
            queued
        );
        if policy == AfterFailedRun::Keep {
            assert_eq!(
                h.conversation(id)
                    .unwrap()
                    .submissions
                    .iter()
                    .find(|s| s.id == queued)
                    .unwrap()
                    .status,
                Status::Queued
            );
            h.submit(id, Some("third".into()), "third".into()).unwrap();
            h.resume_conversation(id, &cancel).await.unwrap();
            assert!(matches!(
                h.conversation(id)
                    .unwrap()
                    .submissions
                    .iter()
                    .find(|s| s.id == queued)
                    .unwrap()
                    .status,
                Status::Done { .. }
            ));
        }
    }
}

struct GatedFailure {
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}
#[async_trait]
impl Model for GatedFailure {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        self.started.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        Err(Error::Model("invalid request".into()))
    }
}
#[tokio::test]
async fn setting_and_admission_during_model_call_apply_to_failure() {
    use pi_rs_durable::runtime::Scheduler;
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(GatedFailure {
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    let opts = Options {
        model: model.clone(),
        tools: vec![],
        restart_limit: 3,
        max_turns: 5,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut s = Scheduler::open(dir.path().join("db"), opts).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.submit(id, None, "first".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), model.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let queued = s
        .submit(id, Some("queued".into()), "queued".into())
        .unwrap();
    s.set_after_failed_run(id, AfterFailedRun::Withdraw)
        .unwrap();
    model.release.add_permits(1);
    s.wait(id).await.unwrap();
    assert_eq!(
        s.conversation(id)
            .unwrap()
            .submissions
            .iter()
            .find(|s| s.id == queued)
            .unwrap()
            .status,
        Status::Unanswered {
            reason: "run_failed".into()
        }
    );
}

struct AbortedIo;
#[async_trait]
impl Model for AbortedIo {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        Err(std::io::Error::other("aborted").into())
    }
}

#[tokio::test]
async fn provider_error_named_aborted_is_still_a_failed_run() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        model: Arc::new(AbortedIo),
        tools: vec![],
        restart_limit: 3,
        max_turns: 5,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut h = Harness::open_paused(dir.path().join("db"), opts).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.set_after_failed_run(id, AfterFailedRun::Withdraw)
        .unwrap();
    h.submit(id, None, "first".into()).unwrap();
    let queued = h.submit(id, None, "queued".into()).unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        h.conversation(id)
            .unwrap()
            .submissions
            .iter()
            .find(|s| s.id == queued)
            .unwrap()
            .status,
        Status::Unanswered {
            reason: "run_failed".into()
        }
    );
}

#[tokio::test]
async fn failure_clears_when_later_scheduler_run_starts_after_reopen() {
    use pi_rs_durable::runtime::Scheduler;
    for continue_run in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(FailOnce(AtomicUsize::new(0)));
        let mut scheduler = Scheduler::open(&path, options(model.clone())).unwrap();
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        let first = scheduler.submit(id, None, "first".into()).unwrap();
        scheduler.wait(id).await.unwrap();
        assert_eq!(
            scheduler
                .conversation(id)
                .unwrap()
                .run_failure
                .unwrap()
                .inputs,
            vec![first]
        );
        scheduler.shutdown().await.unwrap();
        let mut scheduler = Scheduler::open(&path, options(model.clone())).unwrap();
        scheduler
            .set_after_failed_run(id, AfterFailedRun::Keep)
            .unwrap();
        assert!(scheduler.conversation(id).unwrap().run_failure.is_some());
        if continue_run {
            scheduler.submit_continue(id, None).unwrap();
        } else {
            scheduler.submit(id, None, "later".into()).unwrap();
        }
        scheduler.wait(id).await.unwrap();
        assert!(scheduler.conversation(id).unwrap().run_failure.is_none());
        scheduler.shutdown().await.unwrap();
        let scheduler = Scheduler::open(&path, options(model)).unwrap();
        assert!(scheduler.conversation(id).unwrap().run_failure.is_none());
        scheduler.shutdown().await.unwrap();
    }
}

struct GatedOnce {
    calls: AtomicUsize,
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

#[async_trait]
impl Model for GatedOnce {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            return Ok(stream::iter(vec![Ok(ModelEvent::Complete {
                calls: vec![pi_rs_core::ToolCall {
                    id: "call".into(),
                    name: "missing".into(),
                    arguments: "{}".into(),
                }],
                usage: None,
            })])
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
}

#[tokio::test]
async fn reset_start_in_failure_commit_preserves_failure_after_reopen() {
    use pi_rs_durable::runtime::Scheduler;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(GatedOnce {
        calls: AtomicUsize::new(0),
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    let opts = || Options {
        model: model.clone(),
        tools: vec![],
        restart_limit: 3,
        max_turns: 5,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut s = Scheduler::open(&path, opts()).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let first = s.submit(id, None, "first".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), model.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    s.reset(id, Some("reset".into())).unwrap();
    s.submit(id, None, "next".into()).unwrap();
    model.release.add_permits(1);
    s.wait(id).await.unwrap();
    assert_eq!(
        s.conversation(id).unwrap().run_failure.unwrap().inputs,
        vec![first]
    );
    s.shutdown().await.unwrap();
    let s = Scheduler::open(&path, opts()).unwrap();
    assert_eq!(
        s.conversation(id).unwrap().run_failure.unwrap().inputs,
        vec![first]
    );
    s.shutdown().await.unwrap();
}
