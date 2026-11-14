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
async fn steering_waits_for_all_tools_and_drains_one_at_a_time_before_followup() {
    let dir = tempfile::tempdir().unwrap();
    let (model, tool) = setup();
    let mut s = Scheduler::open(dir.path().join("db"), opts(model.clone(), tool.clone())).unwrap();
    let id = s.create_conversation(config()).unwrap();
    let initial = s.submit(id, None, "initial".into()).unwrap();
    acquire(&model.started).await;
    let follow = s
        .submit(id, Some("follow".into()), "follow".into())
        .unwrap();
    let a = s
        .submit_with_mode(id, Some("a".into()), "steer-a".into(), InputMode::Steer)
        .unwrap();
    let b = s
        .submit_with_mode(id, Some("b".into()), "steer-b".into(), InputMode::Steer)
        .unwrap();
    assert_eq!(
        a,
        s.submit_with_mode(
            id,
            Some("a".into()),
            "different".into(),
            InputMode::FollowUp
        )
        .unwrap()
    );
    assert_eq!(s.conversation(id).unwrap().submissions.len(), 4);
    model.release.add_permits(1);
    acquire(&tool.started).await;
    assert_eq!(
        s.conversation(id)
            .unwrap()
            .submissions
            .iter()
            .find(|x| x.id == a)
            .unwrap()
            .status,
        Status::Queued
    );
    tool.release.add_permits(1);
    acquire(&model.started).await;
    assert_eq!(tool.count.load(Ordering::SeqCst), 2);
    {
        let r = model.requests.lock().unwrap();
        assert_eq!(users(&r[1]), ["initial", "steer-a"]);
        assert_eq!(
            r[1].iter()
                .filter(|m| matches!(m, Message::Tool { .. }))
                .count(),
            2
        );
    }
    model.release.add_permits(1);
    acquire(&model.started).await;
    assert_eq!(
        users(&model.requests.lock().unwrap()[2]),
        ["initial", "steer-a", "steer-b"]
    );
    model.release.add_permits(1);
    acquire(&model.started).await;
    assert_eq!(
        users(&model.requests.lock().unwrap()[3]),
        ["initial", "steer-a", "steer-b", "follow"]
    );
    model.release.add_permits(1);
    s.wait(id).await.unwrap();
    let c = s.conversation(id).unwrap();
    for sid in [initial, a, b] {
        assert_eq!(
            c.submissions.iter().find(|x| x.id == sid).unwrap().status,
            Status::Done {
                answer: "answer3".into()
            }
        );
    }
    assert_eq!(
        c.submissions
            .iter()
            .find(|x| x.id == follow)
            .unwrap()
            .status,
        Status::Done {
            answer: "answer4".into()
        }
    );
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn queued_steering_survives_restart_during_model() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (model, tool) = setup();
    let mut s = Scheduler::open(&path, opts(model.clone(), tool.clone())).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "initial".into()).unwrap();
    acquire(&model.started).await;
    let steer = s
        .submit_with_mode(
            id,
            Some("steer".into()),
            "persisted".into(),
            InputMode::Steer,
        )
        .unwrap();
    assert!(matches!(
        s.conversation(id).unwrap().submissions[1].status,
        Status::Queued
    ));
    s.shutdown().await.unwrap();
    let mut s = Scheduler::open(&path, opts(model.clone(), tool)).unwrap();
    acquire(&model.started).await;
    model.release.add_permits(1);
    acquire(&model.started).await;
    assert_eq!(
        users(&model.requests.lock().unwrap()[2]),
        ["initial", "persisted"]
    );
    model.release.add_permits(1);
    s.wait(id).await.unwrap();
    assert!(matches!(
        s.conversation(id)
            .unwrap()
            .submissions
            .iter()
            .find(|x| x.id == steer)
            .unwrap()
            .status,
        Status::Done { .. }
    ));
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn admissions_racing_execution_remain_unique_and_are_all_processed() {
    let dir = tempfile::tempdir().unwrap();
    let (model, tool) = setup();
    tool.release.add_permits(1);
    let mut s = Scheduler::open(dir.path().join("db"), opts(model.clone(), tool)).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "initial".into()).unwrap();
    acquire(&model.started).await;
    model.release.add_permits(64);
    for i in 0..12 {
        let key = format!("key-{i}");
        let first = s
            .submit(id, Some(key.clone()), format!("input-{i}"))
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(
            first,
            s.submit_with_mode(id, Some(key), "duplicate".into(), InputMode::Steer)
                .unwrap()
        );
        tokio::task::yield_now().await;
    }
    s.wait(id).await.unwrap();
    let c = s.conversation(id).unwrap();
    assert_eq!(c.submissions.len(), 13);
    assert!(
        c.submissions
            .iter()
            .all(|s| matches!(s.status, Status::Done { .. }))
    );
    assert_eq!(
        c.messages
            .iter()
            .filter(|m| matches!(m, Message::User { .. }))
            .count(),
        13
    );
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn old_conversation_snapshot_without_mode_map_deserializes() {
    let dir = tempfile::tempdir().unwrap();
    let (model, tool) = setup();
    let mut s = Scheduler::open(dir.path().join("db"), opts(model, tool)).unwrap();
    let id = s.create_conversation(config()).unwrap();
    let mut value = serde_json::to_value(s.conversation(id).unwrap()).unwrap();
    value.as_object_mut().unwrap().remove("input_modes");
    let restored: pi_rs_durable::runtime::Conversation = serde_json::from_value(value).unwrap();
    assert_eq!(restored.id, id);
    s.shutdown().await.unwrap();
}
