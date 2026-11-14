use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    Error, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool, ToolCall,
    ToolContext, ToolSpec,
};
use pi_rs_durable::{
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, InputMode, Options, RegisteredTool, Scheduler, Status},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Semaphore;

struct TestModel {
    requests: Mutex<Vec<Vec<Message>>>,
    started: Semaphore,
    release: Semaphore,
}
#[async_trait]
impl Model for TestModel {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let n = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request.messages);
            requests.len()
        };
        self.started.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        let calls = if n == 1 {
            (0..2)
                .map(|i| ToolCall {
                    id: format!("t{i}"),
                    name: "test".into(),
                    arguments: "{}".into(),
                })
                .collect()
        } else {
            vec![]
        };
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text(format!("answer{n}"))),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])
        .boxed())
    }
}
struct TestTool {
    count: AtomicUsize,
    started: Semaphore,
    release: Semaphore,
}
#[async_trait]
impl Tool for TestTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "test".into(),
            description: "test".into(),
            parameters: serde_json::json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        let n = self.count.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        Outcome::Succeeded("ok".into())
    }
}
fn opts(model: Arc<TestModel>, tool: Arc<TestTool>) -> Options {
    Options {
        model,
        tools: vec![RegisteredTool {
            tool,
            replay: ReplayPolicy::Safe,
        }],
        restart_limit: 3,
        max_turns: 10,
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
fn setup() -> (Arc<TestModel>, Arc<TestTool>) {
    (
        Arc::new(TestModel {
            requests: Mutex::new(vec![]),
            started: Semaphore::new(0),
            release: Semaphore::new(0),
        }),
        Arc::new(TestTool {
            count: AtomicUsize::new(0),
            started: Semaphore::new(0),
            release: Semaphore::new(0),
        }),
    )
}
async fn acquire(s: &Semaphore) {
    tokio::time::timeout(std::time::Duration::from_secs(10), s.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}
fn users(m: &[Message]) -> Vec<String> {
    m.iter()
        .filter_map(|m| {
            if let Message::User { content } = m {
                Some(content.clone())
            } else {
                None
            }
        })
        .collect()
}

#[tokio::test]
async fn reset_during_tool_round_places_before_queued_inputs_and_preserves_history() {
    let dir = tempfile::tempdir().unwrap();
    let (model, tool) = setup();
    let mut s = Scheduler::open(dir.path().join("db"), opts(model.clone(), tool.clone())).unwrap();
    let id = s.create_conversation(config()).unwrap();
    let initial = s.submit(id, None, "initial".into()).unwrap();
    acquire(&model.started).await;
    model.release.add_permits(1);
    acquire(&tool.started).await;
    let follow = s.submit(id, None, "follow".into()).unwrap();
    let reset = s.reset(id, Some("handoff".into())).unwrap();
    let steer = s
        .submit_with_mode(id, None, "steer".into(), InputMode::Steer)
        .unwrap();
    assert!(s.conversation(id).unwrap().resets.is_empty());
    tool.release.add_permits(1);
    acquire(&model.started).await;
    assert_eq!(tool.count.load(Ordering::SeqCst), 2);
    assert_eq!(
        users(&model.requests.lock().unwrap()[1]),
        ["handoff", "follow", "steer"]
    );
    let c = s.conversation(id).unwrap();
    assert_eq!(c.resets[0].id, reset);
    assert_eq!(
        c.submissions
            .iter()
            .find(|x| x.id == initial)
            .unwrap()
            .status,
        Status::Unanswered {
            reason: "reset".into()
        }
    );
    let page = s
        .history(
            id,
            pi_rs_durable::observer::Cursor(0),
            std::num::NonZeroUsize::new(1000).unwrap(),
        )
        .unwrap();
    assert!(
        page.records
            .iter()
            .any(|record| record.value.to_string().contains("initial"))
    );
    model.release.add_permits(1);
    s.wait(id).await.unwrap();
    let c = s.conversation(id).unwrap();
    for id in [follow, steer] {
        assert!(matches!(
            c.submissions.iter().find(|x| x.id == id).unwrap().status,
            Status::Done { .. }
        ));
    }
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn reset_survives_restart_during_tool_round() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (model, tool) = setup();
    let mut s = Scheduler::open(&path, opts(model.clone(), tool.clone())).unwrap();
    let id = s.create_conversation(config()).unwrap();
    let initial = s.submit(id, None, "initial".into()).unwrap();
    acquire(&model.started).await;
    model.release.add_permits(1);
    acquire(&tool.started).await;
    let reset = s.reset(id, Some("after crash".into())).unwrap();
    let queued = s.submit(id, None, "next".into()).unwrap();
    s.shutdown().await.unwrap();
    tool.release.add_permits(2);
    let mut s = Scheduler::open(&path, opts(model.clone(), tool.clone())).unwrap();
    acquire(&model.started).await;
    assert_eq!(
        users(&model.requests.lock().unwrap()[1]),
        ["after crash", "next"]
    );
    let c = s.conversation(id).unwrap();
    assert_eq!(c.resets.iter().filter(|r| r.id == reset).count(), 1);
    assert_eq!(
        c.submissions
            .iter()
            .find(|x| x.id == initial)
            .unwrap()
            .status,
        Status::Unanswered {
            reason: "reset".into()
        }
    );
    model.release.add_permits(1);
    s.wait(id).await.unwrap();
    assert!(matches!(
        s.conversation(id)
            .unwrap()
            .submissions
            .iter()
            .find(|x| x.id == queued)
            .unwrap()
            .status,
        Status::Done { .. }
    ));
    s.shutdown().await.unwrap();
}
