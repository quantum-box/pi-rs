use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    CancellationToken, Error, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool,
    ToolCall, ToolContext, ToolSpec,
};
use pi_rs_durable::{
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, Harness, Options, RegisteredTool, Status},
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
struct ModelStub;
#[async_trait]
impl Model for ModelStub {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let tool_done = request
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }));
        let event = if tool_done {
            ModelEvent::Text("done".into())
        } else {
            ModelEvent::ToolCallDelta(pi_rs_core::ToolCallDelta {
                index: 0,
                id: String::new(),
                name: String::new(),
                arguments: String::new(),
            })
        };
        let calls = if tool_done {
            vec![]
        } else {
            vec![ToolCall {
                id: "call".into(),
                name: "job".into(),
                arguments: "{}".into(),
            }]
        };
        Ok(stream::iter(vec![
            Ok(event),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])
        .boxed())
    }
}
struct Job {
    dispatched: AtomicUsize,
    polled: AtomicUsize,
}
#[async_trait]
impl Tool for Job {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        self.dispatched.fetch_add(1, Ordering::SeqCst);
        Outcome::Pending {
            reference: "external-1".into(),
        }
    }
    async fn poll(&self, reference: &str, _: ToolContext) -> Option<Outcome> {
        assert_eq!(reference, "external-1");
        if self.polled.fetch_add(1, Ordering::SeqCst) == 0 {
            Some(Outcome::Pending {
                reference: "external-1".into(),
            })
        } else {
            Some(Outcome::Succeeded("complete".into()))
        }
    }
}
fn options(job: Arc<Job>) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![RegisteredTool {
            tool: job,
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
#[tokio::test]
async fn accepted_job_is_polled_after_reopen_without_redispatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let job = Arc::new(Job {
        dispatched: AtomicUsize::new(0),
        polled: AtomicUsize::new(0),
    });
    let id;
    let attempt;
    {
        let mut h = Harness::open(&path, options(job.clone()), &cancel)
            .await
            .unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        h.submit_and_run(id, Some("job".into()), "work".into(), &cancel)
            .await
            .unwrap();
        attempt = h.conversation(id).unwrap().tool_attempt().unwrap();
    }
    let paused = Harness::open_paused(&path, options(job.clone())).unwrap();
    assert_eq!(
        paused.conversation(id).unwrap().tool_attempt(),
        Some(attempt)
    );
    assert_eq!(job.dispatched.load(Ordering::SeqCst), 1);
    drop(paused);
    let h = Harness::open(&path, options(job.clone()), &cancel)
        .await
        .unwrap();
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "done".into()
        }
    );
    assert_eq!(job.dispatched.load(Ordering::SeqCst), 1);
    assert_eq!(job.polled.load(Ordering::SeqCst), 2);
}

struct BlockingJob {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl Tool for BlockingJob {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        Outcome::Pending {
            reference: "external-1".into(),
        }
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        self.started.notify_one();
        self.release.notified().await;
        Some(Outcome::Succeeded("stale".into()))
    }
}
#[tokio::test]
async fn concurrent_poll_cannot_overwrite_reconciled_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let job = Arc::new(BlockingJob {
        started: started.clone(),
        release: release.clone(),
    });
    let opts = Options {
        model: Arc::new(ModelStub),
        tools: vec![RegisteredTool {
            tool: job,
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut s = pi_rs_durable::runtime::Scheduler::open(dir.path().join("db"), opts).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    let (op, _) = s.blocked(id).unwrap().unwrap();
    s.reconcile(id, op, Outcome::Succeeded("confirmed".into()))
        .unwrap();
    release.notify_one();
    let _ = s.wait(id).await;
    s.wait(id).await.unwrap();
    let c = s.conversation(id).unwrap();
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m,Message::Tool{content,..} if content=="confirmed"))
    );
    assert!(
        !c.messages
            .iter()
            .any(|m| matches!(m,Message::Tool{content,..} if content=="stale"))
    );
    s.shutdown().await.unwrap();
}
struct EventuallyDone {
    polls: AtomicUsize,
}
#[async_trait]
impl Tool for EventuallyDone {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "job".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        Outcome::Pending {
            reference: "job".into(),
        }
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
            Some(Outcome::Pending {
                reference: "job".into(),
            })
        } else {
            Some(Outcome::Succeeded("finished".into()))
        }
    }
}
#[tokio::test]
async fn scheduler_polls_pending_jobs_until_complete() {
    let dir = tempfile::tempdir().unwrap();
    let job = Arc::new(EventuallyDone {
        polls: AtomicUsize::new(0),
    });
    let opts = Options {
        model: Arc::new(ModelStub),
        tools: vec![RegisteredTool {
            tool: job.clone(),
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut s = pi_rs_durable::runtime::Scheduler::open(dir.path().join("db"), opts).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), s.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.polls.load(Ordering::SeqCst), 2);
    assert_eq!(
        s.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "done".into()
        }
    );
    s.shutdown().await.unwrap();
}
