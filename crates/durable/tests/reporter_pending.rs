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
