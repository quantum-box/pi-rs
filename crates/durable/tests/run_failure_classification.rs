use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{Error, Model, ModelEvent, ModelRequest, ModelStream, ToolCall};
use pi_rs_durable::{
    recovery::UncertainPolicy,
    runtime::{AfterFailedRun, Config, Options, Scheduler, Status},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Semaphore;

struct Fixture {
    calls: AtomicUsize,
    started: Semaphore,
    release: Semaphore,
    mode: &'static str,
}
#[async_trait]
impl Model for Fixture {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.mode == "blocked"
            || (self.mode == "auto" && n == 1)
            || (self.mode == "reset" && n == 0)
        {
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        if self.mode == "error" || (self.mode == "auto" && n == 0) {
            return Err(Error::Model("reset".into()));
        }
        let calls = if self.mode == "turn" || (self.mode == "reset" && n == 0) {
            vec![ToolCall {
                id: "call".into(),
                name: "missing".into(),
                arguments: "{}".into(),
            }]
        } else {
            vec![]
        };
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("ok".into())),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])
        .boxed())
    }
}
fn model(mode: &'static str) -> Arc<Fixture> {
    Arc::new(Fixture {
        calls: AtomicUsize::new(0),
        started: Semaphore::new(0),
        release: Semaphore::new(0),
        mode,
    })
}
fn options(model: Arc<Fixture>, limit: u32, turns: u32) -> Options {
    Options {
        model,
        tools: vec![],
        restart_limit: limit,
        max_turns: turns,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: "".into(),
        max_output_tokens: 100,
    }
}
async fn started(model: &Fixture) {
    tokio::time::timeout(std::time::Duration::from_secs(10), model.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}
fn check(
    s: &Scheduler,
    id: uuid::Uuid,
    sid: uuid::Uuid,
    reason: &str,
    detail: Option<&str>,
    status_reason: &str,
) {
    let c = s.conversation(id).unwrap();
    let f = c.run_failure.unwrap();
    assert_eq!(f.inputs, vec![sid]);
    assert_eq!(f.reason, reason);
    assert_eq!(f.detail.as_deref(), detail);
    assert_eq!(
        c.submissions.iter().find(|v| v.id == sid).unwrap().status,
        Status::Unanswered {
            reason: status_reason.into()
        }
    );
}

#[tokio::test]
async fn classifications_survive_scheduler_reopen() {
    for mode in ["error", "abort", "reset", "turn", "nothing", "restart"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let m = model(match mode {
            "abort" | "restart" => "blocked",
            x => x,
        });
        let opts = || options(m.clone(), 1, if mode == "turn" { 1 } else { 5 });
        let mut s = Scheduler::open(&path, opts()).unwrap();
        let id = s.create_conversation(config()).unwrap();
        let sid = if mode == "nothing" {
            s.submit_continue(id, None).unwrap()
        } else {
            s.submit(id, None, "input".into()).unwrap()
        };
        if mode == "abort" {
            started(&m).await;
            s.abort_and_wait_withdrawn(id).await.unwrap();
        } else if mode == "reset" {
            started(&m).await;
            s.reset(id, None).unwrap();
            m.release.add_permits(1);
        } else if mode == "restart" {
            started(&m).await;
            s.shutdown().await.unwrap();
            s = Scheduler::open(&path, opts()).unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(10), s.wait(id))
            .await
            .unwrap()
            .unwrap();
        let (reason, detail, status) = match mode {
            "error" => ("model_error", Some("model: reset"), "model: reset"),
            "abort" => ("aborted", None, "aborted"),
            "reset" => ("reset", None, "reset"),
            "turn" => ("turn_limit", Some("1 turn"), "turn limit"),
            "nothing" => ("nothing_to_answer", None, "nothing_to_answer"),
            "restart" => (
                "restart_limit",
                Some("The generation was interrupted once in its model request phase"),
                "model restart limit",
            ),
            _ => unreachable!(),
        };
        check(&s, id, sid, reason, detail, status);
        s.shutdown().await.unwrap();
        let s = Scheduler::open(&path, opts()).unwrap();
        check(&s, id, sid, reason, detail, status);
        s.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn automatic_continuation_retains_failure_until_explicit_run() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let m = model("auto");
    // Queue both inputs before scheduling to make the failure boundary deterministic.
    let mut h =
        pi_rs_durable::runtime::Harness::open_paused(&path, options(m.clone(), 3, 5)).unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    let first = h.submit(id, None, "first".into()).unwrap();
    h.submit(id, None, "second".into()).unwrap();
    drop(h);
    let s = Scheduler::open(&path, options(m.clone(), 3, 5)).unwrap();
    started(&m).await;
    check(
        &s,
        id,
        first,
        "model_error",
        Some("model: reset"),
        "model: reset",
    );
    s.shutdown().await.unwrap();
    let mut s = Scheduler::open(&path, options(m.clone(), 3, 5)).unwrap();
    // Reopen recovers the interrupted request; the next call answers.
    s.wait(id).await.unwrap();
    check(
        &s,
        id,
        first,
        "model_error",
        Some("model: reset"),
        "model: reset",
    );
    s.submit(id, None, "explicit".into()).unwrap();
    s.wait(id).await.unwrap();
    assert!(s.conversation(id).unwrap().run_failure.is_none());
    s.shutdown().await.unwrap();
    let s = Scheduler::open(&path, options(m, 3, 5)).unwrap();
    assert!(s.conversation(id).unwrap().run_failure.is_none());
    s.shutdown().await.unwrap();
}
