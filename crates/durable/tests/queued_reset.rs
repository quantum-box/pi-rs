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

#[tokio::test]
async fn application_writes_are_visible_while_busy_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (model, tool) = setup();
    let mut s = Scheduler::open(&path, opts(model.clone(), tool.clone())).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "initial".into()).unwrap();
    acquire(&model.started).await;
    let write = s
        .submit_write(
            id,
            Some("key".into()),
            "app.note".into(),
            Some(serde_json::json!({"value": 1})),
        )
        .unwrap();
    assert_eq!(
        s.submit_write(id, Some("key".into()), "app.other".into(), None)
            .unwrap(),
        write
    );
    let c = s.conversation(id).unwrap();
    assert!(c.application_writes[0].placed);
    assert_eq!(
        c.application_writes[0].data,
        Some(serde_json::json!({"value":1}))
    );
    s.shutdown().await.unwrap();
    let s = Scheduler::open(&path, opts(model.clone(), tool.clone())).unwrap();
    let c = s.conversation(id).unwrap();
    assert_eq!(c.application_writes.len(), 1);
    assert_eq!(c.application_writes[0].id, write);
    assert!(c.application_writes[0].placed);
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn writes_follow_queued_resets_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (model, tool) = setup();
    let mut s = Scheduler::open(&path, opts(model.clone(), tool.clone())).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "initial".into()).unwrap();
    acquire(&model.started).await;
    let first = s.reset(id, None).unwrap();
    let a = s.submit_write(id, None, "app.a".into(), None).unwrap();
    let second = s.reset(id, None).unwrap();
    let b = s.submit_write(id, None, "app.b".into(), None).unwrap();
    let c = s.conversation(id).unwrap();
    assert!(c.application_writes.iter().all(|w| !w.placed));
    assert_eq!(c.application_writes[0].after_reset, Some(first));
    assert_eq!(c.application_writes[1].after_reset, Some(second));
    s.shutdown().await.unwrap();
    let mut s = Scheduler::open(&path, opts(model.clone(), tool.clone())).unwrap();
    acquire(&model.started).await;
    model.release.add_permits(1);
    s.wait(id).await.unwrap();
    let c = s.conversation(id).unwrap();
    assert_eq!(
        c.resets.iter().map(|r| r.id).collect::<Vec<_>>(),
        [first, second]
    );
    assert_eq!(
        c.application_writes
            .iter()
            .map(|w| (w.id, w.placed, w.after_reset))
            .collect::<Vec<_>>(),
        [(a, true, Some(first)), (b, true, Some(second))]
    );
    s.shutdown().await.unwrap();
}

struct HandoffTool(Arc<TestTool>);
#[async_trait]
impl pi_rs_durable::keyed_tool::KeyedTool for HandoffTool {
    fn spec(&self) -> ToolSpec {
        self.0.spec()
    }
    async fn invoke(
        &self,
        _: serde_json::Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        _: pi_rs_durable::keyed_tool::KeyedHost,
    ) -> pi_rs_core::ToolReply {
        unreachable!()
    }
    async fn invoke_controlled(
        &self,
        args: serde_json::Value,
        ctx: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        _: pi_rs_durable::keyed_tool::KeyedHost,
    ) -> (pi_rs_core::ToolReply, pi_rs_durable::reset::ToolControl) {
        let result = self.0.invoke(args, ctx).await;
        (
            result.into(),
            pi_rs_durable::reset::ToolControl {
                add_tools: Vec::new(),
                handoff: Some("tool handoff".into()),
                terminate: false,
            },
        )
    }
}
#[tokio::test]
async fn tool_handoff_precedes_queued_reset_and_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let (model, tool) = setup();
    let adapter = Arc::new(pi_rs_durable::keyed_tool::KeyedToolAdapter::new(
        HandoffTool(tool.clone()),
    ));
    let mut options = opts(model.clone(), tool.clone());
    options.tools = vec![adapter.register(ReplayPolicy::Safe)];
    let mut s = Scheduler::open(dir.path().join("db"), options).unwrap();
    let id = s.create_conversation(config()).unwrap();
    let initial = s.submit(id, None, "initial".into()).unwrap();
    acquire(&model.started).await;
    model.release.add_permits(1);
    acquire(&tool.started).await;
    s.submit(id, None, "follow".into()).unwrap();
    s.reset(id, Some("queued reset".into())).unwrap();
    s.submit_with_mode(id, None, "steer".into(), InputMode::Steer)
        .unwrap();
    tool.release.add_permits(1);
    acquire(&model.started).await;
    let c = s.conversation(id).unwrap();
    assert_eq!(
        c.resets
            .iter()
            .map(|r| r.handoff.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("tool handoff"), Some("queued reset")]
    );
    assert_eq!(
        users(&model.requests.lock().unwrap()[1]),
        ["queued reset", "follow", "steer"]
    );
    assert!(matches!(
        c.submissions
            .iter()
            .find(|x| x.id == initial)
            .unwrap()
            .status,
        Status::Unanswered { .. }
    ));
    model.release.add_permits(1);
    s.wait(id).await.unwrap();
}

#[tokio::test]
async fn tool_handoff_places_steer_in_new_context_before_followup() {
    let dir = tempfile::tempdir().unwrap();
    let (model, tool) = setup();
    let adapter = Arc::new(pi_rs_durable::keyed_tool::KeyedToolAdapter::new(
        HandoffTool(tool.clone()),
    ));
    let mut options = opts(model.clone(), tool.clone());
    options.tools = vec![adapter.register(ReplayPolicy::Safe)];
    let mut s = Scheduler::open(dir.path().join("db"), options).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "initial".into()).unwrap();
    acquire(&model.started).await;
    model.release.add_permits(1);
    acquire(&tool.started).await;
    s.submit(id, None, "follow".into()).unwrap();
    s.submit_with_mode(id, None, "steer".into(), InputMode::Steer)
        .unwrap();
    tool.release.add_permits(1);
    acquire(&model.started).await;
    assert_eq!(
        users(&model.requests.lock().unwrap()[1]),
        ["tool handoff", "steer"]
    );
    model.release.add_permits(1);
    acquire(&model.started).await;
    assert_eq!(
        users(&model.requests.lock().unwrap()[2]),
        ["tool handoff", "steer", "follow"]
    );
    model.release.add_permits(1);
    s.wait(id).await.unwrap();
}

struct BlockAdapter {
    model: Arc<TestModel>,
    requests: Mutex<Vec<pi_rs_durable::model::DurableRequest>>,
}
#[async_trait]
impl pi_rs_durable::model::ProviderAdapter for BlockAdapter {
    fn id(&self) -> &str {
        "block-reset-test/v1"
    }
    fn supports_durable_blocks(&self) -> bool {
        true
    }
    async fn prepare(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
    ) -> Result<serde_json::Value, Error> {
        Ok(serde_json::json!({}))
    }
    fn validate(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
        _: &serde_json::Value,
    ) -> Result<(), Error> {
        Ok(())
    }
    async fn prepare_durable(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        request: &pi_rs_durable::model::DurableRequest,
    ) -> Result<serde_json::Value, Error> {
        self.requests.lock().unwrap().push(request.clone());
        Ok(serde_json::json!({}))
    }
    fn validate_durable(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &pi_rs_durable::model::DurableRequest,
        _: &serde_json::Value,
    ) -> Result<(), Error> {
        Ok(())
    }
    async fn dispatch(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        request: &ModelRequest,
        _: serde_json::Value,
    ) -> Result<ModelStream, Error> {
        self.model.stream(request.clone()).await
    }
}

#[tokio::test]
async fn block_handoff_and_queued_reset_preserve_history_and_clear_provider_blocks() {
    use pi_rs_durable::{extension::Registry, runtime::Harness};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (model, tool) = setup();
    let tool_adapter = Arc::new(pi_rs_durable::keyed_tool::KeyedToolAdapter::new(
        HandoffTool(tool.clone()),
    ));
    let mut options = opts(model.clone(), tool.clone());
    options.tools = vec![tool_adapter.register(ReplayPolicy::Safe)];
    let provider = Arc::new(BlockAdapter {
        model: model.clone(),
        requests: Mutex::new(vec![]),
    });
    let mut h = Harness::open_paused(&path, options.clone()).unwrap();
    h.install_provider_adapter(provider.clone()).unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.select_provider_adapter(id, true).unwrap();
    let blocks = vec![
        serde_json::json!({"type":"text", "text":"private"}),
        serde_json::json!({"type":"image", "data":"aGk=", "mimeType":"image/png"}),
    ];
    let occurrence = h.append_user_blocks(id, blocks.clone()).unwrap();
    drop(h);
    let mut s = Scheduler::open_with_provider_adapter(
        &path,
        options.clone(),
        Registry::default(),
        provider.clone(),
    )
    .unwrap();
    let initial = s.submit(id, None, "initial".into()).unwrap();
    acquire(&model.started).await;
    assert_eq!(
        provider.requests.lock().unwrap()[0].blocks[&1].message_id,
        occurrence
    );
    model.release.add_permits(1);
    acquire(&tool.started).await;
    let reset = s.reset(id, Some("queued reset".into())).unwrap();
    s.submit_with_mode(id, None, "steer".into(), InputMode::Steer)
        .unwrap();
    tool.release.add_permits(1);
    acquire(&model.started).await;
    {
        let requests = provider.requests.lock().unwrap();
        assert!(requests[1].blocks.is_empty());
        assert_eq!(users(&requests[1].request.messages), ["queued reset", "steer"]);
    }
    let c = s.conversation(id).unwrap();
    assert!(c.content_blocks.is_empty());
    assert_eq!(
        c.resets
            .iter()
            .map(|r| r.handoff.as_deref())
            .collect::<Vec<_>>(),
        [Some("tool handoff"), Some("queued reset")]
    );
    assert_eq!(c.resets[1].id, reset);
    assert!(matches!(
        c.submissions
            .iter()
            .find(|x| x.id == initial)
            .unwrap()
            .status,
        Status::Unanswered { .. }
    ));
    model.release.add_permits(1);
    s.wait(id).await.unwrap();
    let historical = s.fork_by_entry_id(id, occurrence).unwrap();
    assert_eq!(
        s.conversation(historical).unwrap().content_blocks[&0].blocks,
        blocks
    );
    s.shutdown().await.unwrap();
    let h = Harness::open_paused(&path, options).unwrap();
    assert!(h.conversation(id).unwrap().content_blocks.is_empty());
    assert_eq!(
        h.conversation(historical).unwrap().content_blocks[&0].message_id,
        occurrence
    );
    assert_eq!(
        h.conversation(historical).unwrap().content_blocks[&0].blocks,
        blocks
    );
}

#[tokio::test]
async fn block_reset_places_queued_text_atomically_after_new_context() {
    use pi_rs_durable::{extension::Registry, runtime::Harness};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (model, tool) = setup();
    let options = opts(model.clone(), tool.clone());
    let provider = Arc::new(BlockAdapter {
        model: model.clone(),
        requests: Mutex::new(vec![]),
    });
    let mut h = Harness::open_paused(&path, options.clone()).unwrap();
    h.install_provider_adapter(provider.clone()).unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.select_provider_adapter(id, true).unwrap();
    let occurrence = h
        .append_user_blocks(id, vec![serde_json::json!({"type":"text", "text":"old"})])
        .unwrap();
    drop(h);
    let mut s = Scheduler::open_with_provider_adapter(
        &path,
        options,
        Registry::default(),
        provider.clone(),
    )
    .unwrap();
    let initial = s.submit(id, None, "initial".into()).unwrap();
    acquire(&model.started).await;
    model.release.add_permits(1);
    acquire(&tool.started).await;
    let marker = s.reset(id, Some("reset note".into())).unwrap();
    s.submit_with_mode(id, None, "queued steer".into(), InputMode::Steer)
        .unwrap();
    tool.release.add_permits(1);
    acquire(&model.started).await;
    {
        let requests = provider.requests.lock().unwrap();
        assert!(requests[1].blocks.is_empty());
        assert_eq!(users(&requests[1].request.messages), ["reset note", "queued steer"]);
    }
    let c = s.conversation(id).unwrap();
    assert!(c.content_blocks.is_empty());
    assert_eq!(c.resets.last().unwrap().id, marker);
    assert!(matches!(
        c.submissions
            .iter()
            .find(|x| x.id == initial)
            .unwrap()
            .status,
        Status::Unanswered { .. }
    ));
    model.release.add_permits(1);
    s.wait(id).await.unwrap();
    let historical = s.fork_by_entry_id(id, occurrence).unwrap();
    assert_eq!(
        s.conversation(historical).unwrap().content_blocks[&0].message_id,
        occurrence
    );
    s.shutdown().await.unwrap();
}
