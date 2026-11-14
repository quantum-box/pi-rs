use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    Error, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool, ToolCall,
    ToolContext, ToolSpec,
};
use pi_rs_durable::{
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, InputMode, Options, RegisteredTool, Scheduler, Status},
    subagent::ReportedInput,
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
struct FakeModel;
#[async_trait]
impl Model for FakeModel {
    async fn stream(&self, req: ModelRequest) -> Result<ModelStream, Error> {
        let parent = req.model == "parent";
        let tool = req
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }));
        let calls = if parent && !tool {
            vec![ToolCall {
                id: "job-call".into(),
                name: "job".into(),
                arguments: "{}".into(),
            }]
        } else {
            vec![]
        };
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text(
                if parent {
                    "parent answer"
                } else {
                    "child answer"
                }
                .into(),
            )),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])
        .boxed())
    }
}
struct PendingJob(AtomicUsize);
#[async_trait]
impl Tool for PendingJob {
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
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Pending {
            reference: "job-1".into(),
        }
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            Some(Outcome::Pending {
                reference: "job-1".into(),
            })
        } else {
            Some(Outcome::Succeeded("complete".into()))
        }
    }
}
#[tokio::test]
async fn parent_reporter_continues_polling_pending_work() {
    let dir = tempfile::tempdir().unwrap();
    let job = Arc::new(PendingJob(AtomicUsize::new(0)));
    let options = Options {
        model: Arc::new(FakeModel),
        tools: vec![RegisteredTool {
            tool: job.clone(),
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut s = Scheduler::open(dir.path().join("db"), options).unwrap();
    let parent = s
        .create_conversation(Config {
            model: "parent".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.submit_subagent_reported(
        parent,
        "child",
        Config {
            model: "child".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        },
        ReportedInput {
            history: false,
            request_id: "request".into(),
            content: "task".into(),
            mode: InputMode::FollowUp,
        },
    )
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        loop {
            if s.conversation(parent)
                .unwrap()
                .submissions
                .iter()
                .any(|x| matches!(x.status, Status::Done { .. }))
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(job.0.load(Ordering::SeqCst), 2);
    s.shutdown().await.unwrap();
}

struct BlockingModel {
    blocked: &'static str,
    started: tokio::sync::Semaphore,
    calls: AtomicUsize,
}
#[async_trait]
impl Model for BlockingModel {
    async fn stream(&self, req: ModelRequest) -> Result<ModelStream, Error> {
        if req.model == self.blocked {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.add_permits(1);
            std::future::pending::<()>().await;
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

// Insert the intent after startup recovery has gone idle, so only the reporter
// can dispatch the child and its resulting parent follow-up.
async fn abort_reporter_drive(target: &'static str) {
    use durable_store::{Check, Expected, Mutation, Store};
    use pi_rs_durable::runtime::Harness;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(BlockingModel {
        blocked: target,
        started: tokio::sync::Semaphore::new(0),
        calls: AtomicUsize::new(0),
    });
    let options = || Options {
        model: model.clone(),
        tools: vec![],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let config = |name: &str| Config {
        model: name.into(),
        system_prompt: "".into(),
        max_output_tokens: 100,
    };
    let (parent, link) = {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let parent = h.create_conversation(config("parent")).unwrap();
        let link = h.ensure_subagent(parent, "child", config("child")).unwrap();
        (parent, link)
    };
    let mut s = Scheduler::open(&path, options()).unwrap();
    s.wait(parent).await.unwrap();
    s.wait(link.child).await.unwrap();
    let key = format!("runtime/subagent-report/{}/request", link.child).into_bytes();
    let record = json!({"link": link, "request_id":"request", "content":"task", "mode":"FollowUp", "delivered":false});
    let db = Store::open(&path).unwrap();
    db.commit(
        &[Check {
            key: key.clone(),
            expected: Expected::Missing,
        }],
        &[Mutation::Put {
            key,
            value: serde_json::to_vec(&record).unwrap(),
        }],
    )
    .unwrap();
    drop(db);
    tokio::time::timeout(Duration::from_secs(5), model.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let id = if target == "child" {
        link.child
    } else {
        parent
    };
    tokio::time::timeout(Duration::from_secs(5), s.abort_and_wait(id))
        .await
        .unwrap()
        .unwrap();
    assert!(
        s.conversation(id)
            .unwrap()
            .submissions
            .iter()
            .any(|x| matches!(&x.status, Status::Unanswered { reason } if reason == "aborted"))
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    s.shutdown().await.unwrap();
    let mut s = Scheduler::open(&path, options()).unwrap();
    s.wait(id).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        model.calls.load(Ordering::SeqCst),
        1,
        "aborted work replayed after restart"
    );
    assert!(s.reporter_error().is_none(), "{:?}", s.reporter_error());
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_reporter_driven_child_and_restart() {
    abort_reporter_drive("child").await;
}
#[tokio::test]
async fn abort_reporter_driven_parent_and_restart() {
    abort_reporter_drive("parent").await;
}
