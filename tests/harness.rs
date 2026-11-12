use std::collections::{BTreeMap, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use futures_util::stream;
use pi_rs::*;
use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::sync::mpsc;

#[derive(Default)]
struct Script {
    replies: Mutex<VecDeque<Vec<ModelEvent>>>,
    requests: Mutex<Vec<ModelRequest>>,
    concurrent_start: Option<tokio::sync::Barrier>,
    context_window: AtomicUsize,
}

struct CompactionObserver(Arc<Mutex<Vec<serde_json::Value>>>);

struct SettlementObserver(Arc<Mutex<Vec<serde_json::Value>>>);

#[async_trait::async_trait]
impl Extension for SettlementObserver {
    fn id(&self) -> &str {
        "settlement-observer"
    }
    fn handles_lifecycle(&self) -> bool {
        true
    }
    async fn lifecycle(
        &self,
        event: &LifecycleEvent,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        if matches!(
            event,
            LifecycleEvent::AgentStart { .. }
                | LifecycleEvent::AgentEnd { .. }
                | LifecycleEvent::AgentSettled { .. }
        ) {
            self.0
                .lock()
                .unwrap()
                .push(serde_json::to_value(event).unwrap());
        }
        Ok(())
    }
}

#[tokio::test]
async fn extension_observes_agent_settled_after_success_and_cancellation() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(SettlementObserver(events.clone())))
        .unwrap();
    let tool = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: true,
        effects: Mutex::new(BTreeMap::new()),
    });
    registry.register(Arc::new(Tools(tool))).unwrap();
    let model = Script::new(vec![answer("done"), tool_reply(&[r#"{"id":"x"}"#])]);
    let agent = agent(&model, &registry);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel::<String>(1);
    agent
        .run(
            &mut session,
            Some("hello".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    let mut observer = |event: Notification<'_>| {
        if matches!(event, Notification::Committed(record) if matches!(record.event, Event::Assistant { .. }))
        {
            cancel.cancel();
        }
    };
    let result = agent
        .run(
            &mut session,
            Some("cancel".into()),
            &mut inputs,
            &cancel,
            &mut observer,
        )
        .await;
    assert!(matches!(result, Err(Error::Cancelled)));
    let events = events.lock().unwrap();
    let types: Vec<_> = events
        .iter()
        .map(|event| event["type"].as_str().unwrap())
        .collect();
    assert_eq!(
        types,
        [
            "agent_start",
            "agent_end",
            "agent_settled",
            "agent_start",
            "agent_end",
            "agent_settled"
        ]
    );
    assert_eq!(events[2]["aborted"], false);
    assert_eq!(events[5]["aborted"], true);
}

#[async_trait::async_trait]
impl Extension for CompactionObserver {
    fn id(&self) -> &str {
        "compaction-observer"
    }
    fn handles_lifecycle(&self) -> bool {
        true
    }
    fn handles_session_lifecycle(&self) -> bool {
        true
    }
    async fn lifecycle(
        &self,
        event: &LifecycleEvent,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        if matches!(
            event,
            LifecycleEvent::SessionCompact { .. } | LifecycleEvent::SessionCompactFailed { .. }
        ) {
            self.0
                .lock()
                .unwrap()
                .push(serde_json::to_value(event).unwrap());
        }
        Ok(())
    }
}

#[tokio::test]
async fn extension_observes_committed_and_failed_manual_compaction() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(CompactionObserver(events.clone())))
        .unwrap();
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let model = Script::new(vec![answer("original"), answer("summary")]);
    let agent = agent(&model, &registry);
    let cancel = CancellationToken::new();
    let (_, mut inputs) = mpsc::channel(1);
    assert!(
        agent
            .summarize(&mut session, 0, &cancel, &mut ignore)
            .await
            .is_err()
    );
    agent
        .run(
            &mut session,
            Some("question".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    agent
        .summarize(&mut session, 0, &cancel, &mut ignore)
        .await
        .unwrap();
    let events = events.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["type"], "session_compact_failed");
    assert_eq!(events[0]["reason"], "manual");
    assert_eq!(events[0]["aborted"], false);
    assert!(events[0]["errorMessage"].as_str().is_some());
    assert_eq!(events[1]["type"], "session_compact");
    assert_eq!(events[1]["reason"], "manual");
    assert_eq!(events[1]["fromExtension"], false);
    assert_eq!(events[1]["compactionEntry"]["event"]["summary"], "summary");
}

impl Script {
    fn new(replies: Vec<Vec<ModelEvent>>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            ..Self::default()
        }
    }
}

#[async_trait::async_trait]
impl Model for Script {
    async fn context_window(&self, _: &str) -> Option<u32> {
        u32::try_from(self.context_window.load(Ordering::SeqCst))
            .ok()
            .filter(|window| *window > 0)
    }
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let request_number = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request);
            requests.len()
        };
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected model request");
        if request_number <= 2
            && let Some(barrier) = &self.concurrent_start
        {
            barrier.wait().await;
        }
        Ok(Box::pin(stream::iter(reply.into_iter().map(Ok))))
    }
}

struct Fixture {
    called: AtomicUsize,
    hold: bool,
    effects: Mutex<BTreeMap<uuid::Uuid, String>>,
}

#[async_trait::async_trait]
impl Tool for Fixture {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "lookup".into(),
            description: "Fixture lookup".into(),
            parameters: json!({"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}),
        }
    }

    async fn invoke(&self, args: Value, context: ToolContext) -> Outcome {
        self.called.fetch_add(1, Ordering::SeqCst);
        let effect = format!("found {}", args["id"].as_str().unwrap());
        self.effects
            .lock()
            .unwrap()
            .insert(context.operation_id, effect.clone());
        if self.hold {
            std::future::pending::<()>().await;
        }
        Outcome::Succeeded(effect)
    }
}

struct Tools(Arc<Fixture>);
impl Extension for Tools {
    fn id(&self) -> &str {
        "test-tools"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![self.0.clone()]
    }
}

struct Policy;
#[async_trait::async_trait]
impl Extension for Policy {
    fn id(&self) -> &str {
        "test-policy"
    }

    async fn before_tool(
        &self,
        _: &str,
        args: &Value,
        _: &ToolContext,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<Decision> {
        Ok(if args["id"] == "denied" {
            Decision::Deny("host denied".into())
        } else {
            Decision::Allow
        })
    }
}

fn agent<'a>(model: &'a dyn Model, registry: &'a Registry) -> Agent<'a> {
    Agent {
        model,
        model_name: "fixture",
        registry,
        system_prompt: "fixture prompt",
        limits: Limits::default(),
    }
}

fn tool_reply(args: &[&str]) -> Vec<ModelEvent> {
    vec![ModelEvent::Complete {
        calls: args
            .iter()
            .enumerate()
            .map(|(index, arguments)| ToolCall {
                id: format!("call-{index}"),
                name: "lookup".into(),
                arguments: (*arguments).into(),
            })
            .collect(),
        usage: Some(Usage {
            input_tokens: 20,
            output_tokens: 8,
        }),
    }]
}

fn unmetered_answer(value: &str) -> Vec<ModelEvent> {
    let mut reply = answer(value);
    if let Some(ModelEvent::Complete { usage, .. }) = reply.last_mut() {
        *usage = None;
    }
    reply
}

fn answer(value: &str) -> Vec<ModelEvent> {
    vec![
        ModelEvent::Text(value.into()),
        ModelEvent::Complete {
            calls: vec![],
            usage: Some(Usage {
                input_tokens: 20,
                output_tokens: 8,
            }),
        },
    ]
}

fn ignore(_: Notification<'_>) {}

#[tokio::test]
async fn sibling_progress_is_delivered_while_nested_tool_runs() {
    struct Parent {
        started: Arc<tokio::sync::Notify>,
        observed: Arc<tokio::sync::Notify>,
    }
    struct Slow {
        observed: Arc<tokio::sync::Notify>,
    }
    #[async_trait::async_trait]
    impl Tool for Parent {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "lookup".into(),
                description: "parent".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
        async fn invoke_with_host(
            &self,
            args: Value,
            _: ToolContext,
            _: Option<&ExtensionState>,
            host: Arc<dyn ToolHost>,
        ) -> Outcome {
            if args["id"] == "nested" {
                host.call("slow", json!({})).await
            } else {
                if tokio::time::timeout(Duration::from_secs(5), self.started.notified())
                    .await
                    .is_err()
                {
                    return Outcome::Failed("nested tool did not start".into());
                }
                host.report_progress(json!({"text":"working"}))
                    .await
                    .unwrap();
                Outcome::Succeeded("sibling done".into())
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Slow {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "slow".into(),
                description: "nested".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            if tokio::time::timeout(Duration::from_secs(5), self.observed.notified())
                .await
                .is_err()
            {
                return Outcome::Failed("sibling progress was not delivered".into());
            }
            Outcome::Succeeded("nested done".into())
        }
    }
    struct Hooks {
        parent: Arc<Parent>,
        slow: Arc<Slow>,
        events: Arc<Mutex<Vec<Value>>>,
        callback_lock: tokio::sync::Mutex<()>,
    }
    #[async_trait::async_trait]
    impl Extension for Hooks {
        fn id(&self) -> &str {
            "progress-nested"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![self.parent.clone(), self.slow.clone()]
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_tool_execution_lifecycle(&self) -> bool {
            true
        }
        fn handles_tool_execution_updates(&self) -> bool {
            true
        }
        async fn lifecycle(
            &self,
            event: &LifecycleEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<()> {
            let _guard = self.callback_lock.lock().await;
            if matches!(event, LifecycleEvent::ToolExecutionStart { tool_name, .. } if tool_name == "slow")
            {
                self.parent.started.notify_one();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if let LifecycleEvent::ToolExecutionUpdate { .. } = event {
                self.parent.observed.notify_one();
            }
            self.events
                .lock()
                .unwrap()
                .push(serde_json::to_value(event).unwrap());
            Ok(())
        }
    }
    let started = Arc::new(tokio::sync::Notify::new());
    let observed = Arc::new(tokio::sync::Notify::new());
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Hooks {
            parent: Arc::new(Parent {
                started: started.clone(),
                observed: observed.clone(),
            }),
            slow: Arc::new(Slow { observed }),
            events: events.clone(),
            callback_lock: tokio::sync::Mutex::new(()),
        }))
        .unwrap();
    let model = Script::new(vec![
        tool_reply(&[r#"{"id":"nested"}"#, r#"{"id":"sibling"}"#]),
        answer("done"),
    ]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel::<String>(1);
    agent(&model, &registry)
        .run(
            &mut session,
            Some("hello".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await
        .unwrap();
    let events = events.lock().unwrap();
    assert!(
        events.iter().any(
            |event| event["type"] == "tool_execution_update" && event["toolCallId"] == "call-1"
        )
    );
    assert!(
        events
            .iter()
            .any(|event| event["type"] == "tool_execution_end"
                && event["toolName"] == "slow"
                && event["isError"] == false)
    );
}

#[tokio::test]
async fn extensions_receive_tool_progress_before_tool_completion() {
    struct ProgressTool(Arc<tokio::sync::Notify>);
    #[async_trait::async_trait]
    impl Tool for ProgressTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "lookup".into(),
                description: "progress fixture".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
        async fn invoke_with_host(
            &self,
            _: Value,
            _: ToolContext,
            _: Option<&ExtensionState>,
            host: Arc<dyn ToolHost>,
        ) -> Outcome {
            host.report_progress(json!({"content":[{"type":"text","text":"halfway"}]}))
                .await
                .unwrap();
            if tokio::time::timeout(Duration::from_secs(3), self.0.notified())
                .await
                .is_err()
            {
                return Outcome::Failed(
                    "progress hook did not run while the tool was active".into(),
                );
            }
            Outcome::Succeeded("done".into())
        }
    }
    struct ProgressExtension {
        tool: Arc<ProgressTool>,
        events: Arc<Mutex<Vec<Value>>>,
    }
    #[async_trait::async_trait]
    impl Extension for ProgressExtension {
        fn id(&self) -> &str {
            "progress"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![self.tool.clone()]
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_tool_execution_lifecycle(&self) -> bool {
            true
        }
        fn handles_tool_execution_updates(&self) -> bool {
            true
        }
        async fn lifecycle(
            &self,
            event: &LifecycleEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<()> {
            if let LifecycleEvent::ToolExecutionUpdate { .. } = event {
                self.tool.0.notify_one();
            }
            self.events
                .lock()
                .unwrap()
                .push(serde_json::to_value(event).unwrap());
            Ok(())
        }
    }
    struct LegacyObserver(Arc<Mutex<Vec<Value>>>);
    #[async_trait::async_trait]
    impl Extension for LegacyObserver {
        fn id(&self) -> &str {
            "legacy-observer"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_tool_execution_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle(
            &self,
            event: &LifecycleEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<()> {
            self.0
                .lock()
                .unwrap()
                .push(serde_json::to_value(event).unwrap());
            Ok(())
        }
    }
    let events = Arc::new(Mutex::new(Vec::new()));
    let legacy_events = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(LegacyObserver(legacy_events.clone())))
        .unwrap();
    registry
        .register(Arc::new(ProgressExtension {
            tool: Arc::new(ProgressTool(Arc::new(tokio::sync::Notify::new()))),
            events: events.clone(),
        }))
        .unwrap();
    let model = Script::new(vec![tool_reply(&[r#"{"id":"x"}"#]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel::<String>(1);
    agent(&model, &registry)
        .run(
            &mut session,
            Some("hello".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await
        .unwrap();
    let events = events.lock().unwrap();
    let tool_events: Vec<_> = events
        .iter()
        .filter(|event| {
            event["type"]
                .as_str()
                .is_some_and(|kind| kind.starts_with("tool_execution_"))
        })
        .collect();
    assert_eq!(tool_events.len(), 3);
    assert_eq!(tool_events[0]["type"], "tool_execution_start");
    assert_eq!(tool_events[1]["type"], "tool_execution_update");
    assert_eq!(tool_events[1]["toolCallId"], "call-0");
    assert_eq!(tool_events[1]["args"]["id"], "x");
    assert_eq!(
        tool_events[1]["partialResult"]["content"][0]["text"],
        "halfway"
    );
    assert_eq!(tool_events[2]["type"], "tool_execution_end");
    assert_eq!(tool_events[2]["isError"], false);
    let legacy = legacy_events.lock().unwrap();
    assert!(
        !legacy
            .iter()
            .any(|event| event["type"] == "tool_execution_update")
    );
    assert!(
        legacy
            .iter()
            .any(|event| event["type"] == "tool_execution_end")
    );
}

#[tokio::test]
async fn extension_observes_tool_start_and_transformed_end_including_denials() {
    struct ToolObserver(Arc<Mutex<Vec<Value>>>);
    #[async_trait::async_trait]
    impl Extension for ToolObserver {
        fn id(&self) -> &str {
            "tool-observer"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_tool_execution_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle(
            &self,
            event: &LifecycleEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<()> {
            self.0
                .lock()
                .unwrap()
                .push(serde_json::to_value(event).unwrap());
            Ok(())
        }
    }
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(ToolObserver(events.clone())))
        .unwrap();
    registry.register(Arc::new(Policy)).unwrap();
    registry
        .register(Arc::new(Tools(Arc::new(Fixture {
            called: AtomicUsize::new(0),
            hold: false,
            effects: Mutex::new(BTreeMap::new()),
        }))))
        .unwrap();
    let model = Script::new(vec![
        tool_reply(&[r#"{"id":"ok"}"#, r#"{"id":"denied"}"#]),
        answer("done"),
    ]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel::<String>(1);
    agent(&model, &registry)
        .run(
            &mut session,
            Some("hello".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await
        .unwrap();
    let events = events.lock().unwrap();
    let events: Vec<_> = events
        .iter()
        .filter(|event| {
            event["type"]
                .as_str()
                .is_some_and(|kind| kind.starts_with("tool_execution_"))
        })
        .collect();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0]["type"], "tool_execution_start");
    assert_eq!(events[0]["toolCallId"], "call-0");
    assert_eq!(events[0]["args"]["id"], "ok");
    assert_eq!(events[1]["type"], "tool_execution_end");
    assert_eq!(events[1]["isError"], false);
    assert_eq!(events[1]["result"]["status"], "succeeded");
    assert!(events[1]["durationMs"].is_number());
    assert_eq!(events[2]["args"]["id"], "denied");
    assert_eq!(events[3]["isError"], true);
    assert_eq!(events[3]["result"]["status"], "skipped");
    assert!(events[3].get("durationMs").is_none());
}

#[tokio::test]
async fn tool_execution_end_observes_the_journaled_size_limited_outcome() {
    struct Hook(Arc<Mutex<Vec<Value>>>);
    #[async_trait::async_trait]
    impl Extension for Hook {
        fn id(&self) -> &str {
            "bounded-observer"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_tool_execution_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle(
            &self,
            event: &LifecycleEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<()> {
            if matches!(event, LifecycleEvent::ToolExecutionEnd { .. }) {
                self.0
                    .lock()
                    .unwrap()
                    .push(serde_json::to_value(event).unwrap());
            }
            Ok(())
        }
    }
    struct LargeTool;
    #[async_trait::async_trait]
    impl Tool for LargeTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "lookup".into(),
                description: "large".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn output_limit(&self) -> usize {
            8
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            Outcome::Succeeded("excessive output".into())
        }
    }
    struct ToolExtension;
    impl Extension for ToolExtension {
        fn id(&self) -> &str {
            "large-tool"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(LargeTool)]
        }
    }
    let observed = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::default();
    registry.register(Arc::new(Hook(observed.clone()))).unwrap();
    registry.register(Arc::new(ToolExtension)).unwrap();
    let model = Script::new(vec![tool_reply(&[r#"{}"#]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel::<String>(1);
    agent(&model, &registry)
        .run(
            &mut session,
            Some("hello".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await
        .unwrap();
    let observed = observed.lock().unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0]["result"]["status"], "truncated");
    assert_eq!(observed[0]["result"]["detail"]["original_bytes"], 16);
}

#[tokio::test]
async fn context_with_system_runs_after_context_hooks_in_registration_order() {
    struct Hook(&'static str);
    #[async_trait::async_trait]
    impl Extension for Hook {
        fn id(&self) -> &str {
            self.0
        }
        async fn before_request(
            &self,
            request: &mut ModelRequest,
            _: RequestPurpose,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<()> {
            request.messages.push(Message::User {
                content: format!("context-{}", self.0),
            });
            Ok(())
        }
        fn handles_context_with_system(&self) -> bool {
            true
        }
        async fn context_with_system(
            &self,
            messages: &mut Vec<Message>,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<()> {
            assert!(matches!(messages.first(), Some(Message::System { .. })));
            assert!(messages.iter().any(
                |message| matches!(message, Message::User { content } if content == "context-b")
            ));
            messages.push(Message::User {
                content: format!("system-{}", self.0),
            });
            Ok(())
        }
    }
    let model = Script::new(vec![answer("done")]);
    let mut registry = Registry::default();
    registry.register(Arc::new(Hook("a"))).unwrap();
    registry.register(Arc::new(Hook("b"))).unwrap();
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel::<String>(1);
    agent(&model, &registry)
        .run(
            &mut session,
            Some("hello".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await
        .unwrap();
    let requests = model.requests.lock().unwrap();
    let contents: Vec<_> = requests[0]
        .messages
        .iter()
        .filter_map(|message| match message {
            Message::User { content } => Some(content.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        contents,
        ["hello", "context-a", "context-b", "system-a", "system-b"]
    );
    assert_eq!(
        session
            .conversation_at(session.head())
            .unwrap()
            .iter()
            .filter(|message| matches!(message, Message::User { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn truncated_response_never_executes_even_valid_tool_arguments() {
    let mut first = vec![ModelEvent::Metadata(ModelMetadata {
        finish_reason: Some("length".into()),
        ..Default::default()
    })];
    first.extend(tool_reply(&[r#"{"id":"42"}"#]));
    let model = Script::new(vec![first, answer("done")]);
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: false,
        effects: Mutex::default(),
    });
    let mut registry = Registry::default();
    registry.register(Arc::new(Tools(fixture.clone()))).unwrap();
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel::<String>(1);
    agent(&model, &registry)
        .run(
            &mut session,
            Some("check".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await
        .unwrap();
    assert_eq!(fixture.called.load(Ordering::SeqCst), 0);
    assert!(session.operations().iter().any(|(_, state)| matches!(state, OperationState::Finished(Outcome::Failed(message)) if message.contains("not executed") && message.contains("output token limit"))));
    assert!(model.requests.lock().unwrap()[1].messages.iter().any(|message| matches!(message, Message::Tool { content, .. } if content.contains("not executed"))));
}

#[tokio::test]
async fn result_hooks_transform_executed_tools_before_model_and_journal() {
    struct Hook;
    #[async_trait::async_trait]
    impl Extension for Hook {
        fn id(&self) -> &str {
            "result-hook"
        }
        fn handles_tool_result(&self) -> bool {
            true
        }
        async fn tool_result(
            &self,
            _: &ToolCall,
            outcome: &Outcome,
            _: &ToolContext,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<Option<Outcome>> {
            assert_eq!(outcome, &Outcome::Succeeded("found 42".into()));
            Ok(Some(Outcome::Succeeded("redacted".into())))
        }
    }
    let model = Script::new(vec![
        tool_reply(&[r#"{"id":"denied"}"#, r#"{"id":"42"}"#]),
        answer("done"),
    ]);
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: false,
        effects: Mutex::default(),
    });
    let mut registry = Registry::default();
    registry.register(Arc::new(Policy)).unwrap();
    registry.register(Arc::new(Tools(fixture.clone()))).unwrap();
    registry.register(Arc::new(Hook)).unwrap();
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel::<String>(1);
    agent(&model, &registry)
        .run(
            &mut session,
            Some("check".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await
        .unwrap();
    assert_eq!(fixture.called.load(Ordering::SeqCst), 1);
    assert!(session.operations().iter().any(|(_, state)| matches!(state, OperationState::Finished(Outcome::Succeeded(text)) if text == "redacted")));
    assert!(
        model.requests.lock().unwrap()[1].messages.iter().any(
            |message| matches!(message, Message::Tool { content, .. } if content == "redacted")
        )
    );
}

#[tokio::test]
async fn completed_tool_arguments_repair_string_escapes_and_keep_raw_journal() {
    let raw = r#"{"id":"\url{x}"}"#;
    let invalid = r#"{"id":"unfinished"#;
    let model = Script::new(vec![tool_reply(&[raw, invalid]), answer("done")]);
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: false,
        effects: Mutex::default(),
    });
    let mut registry = Registry::default();
    registry.register(Arc::new(Tools(fixture.clone()))).unwrap();
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel::<String>(1);
    agent(&model, &registry)
        .run(
            &mut session,
            Some("check".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await
        .unwrap();
    assert_eq!(fixture.called.load(Ordering::SeqCst), 1);
    assert!(
        fixture
            .effects
            .lock()
            .unwrap()
            .values()
            .any(|value| value == r"found \url{x}")
    );
    assert!(session.operations().iter().any(|(_, state)| matches!(state, OperationState::Finished(Outcome::Skipped(error)) if error.contains("Received arguments:") && error.ends_with(invalid))));
    assert!(session.records().iter().any(|record| matches!(&record.event, Event::Assistant { invocations, .. } if invocations[0].call.arguments == raw)));
    let requests = model.requests.lock().unwrap();
    assert!(requests[1].messages.iter().any(|message| matches!(message, Message::Assistant { calls, .. } if serde_json::from_str::<Value>(&calls[0].arguments).unwrap()["id"] == r"\url{x}" && calls[1].arguments == "{}")));
}

#[tokio::test]
async fn validation_policy_and_queued_input_keep_complete_tool_groups() {
    let dir = tempdir().unwrap();
    let mut session = Session::open(dir.path().join("session")).unwrap();
    let model = Script::new(vec![
        tool_reply(&[
            "{bad",
            r#"{"id":1}"#,
            r#"{"id":"denied"}"#,
            r#"{"id":"42"}"#,
        ]),
        answer("done"),
    ]);
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: false,
        effects: Mutex::default(),
    });
    let mut registry = Registry::default();
    registry.register(Arc::new(Policy)).unwrap();
    registry.register(Arc::new(Tools(fixture.clone()))).unwrap();
    let (sender, mut inputs) = mpsc::channel(4);
    let mut observer = |event: Notification<'_>| {
        if matches!(
            event,
            Notification::Committed(Record {
                event: Event::ToolStarted { .. },
                ..
            })
        ) {
            sender
                .try_send("steer after the tool batch".into())
                .unwrap();
        }
    };
    let agent = agent(&model, &registry);
    assert_eq!(
        agent
            .run(
                &mut session,
                Some("first".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut observer
            )
            .await
            .unwrap(),
        "done"
    );
    assert_eq!(fixture.called.load(Ordering::SeqCst), 1);
    assert_eq!(
        session
            .operations()
            .iter()
            .filter(|(_, state)| matches!(state, OperationState::Finished(Outcome::Skipped(_))))
            .count(),
        3
    );
    let requests = model.requests.lock().unwrap();
    let messages = &requests[1].messages;
    assert!(
        matches!(&messages[2], Message::Assistant { calls, .. } if calls.len() == 4 && calls[0].arguments == "{}")
    );
    assert!(matches!(&messages[6], Message::Tool { content, .. } if content == "found 42"));
    assert!(
        matches!(&messages[7], Message::User { content } if content == "steer after the tool batch")
    );
}

#[tokio::test]
async fn interrupted_dispatch_is_never_replayed_and_can_be_reconciled() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("session");
    let mut session = Session::open(&path).unwrap();
    let model = Script::new(vec![
        tool_reply(&[r#"{"id":"first"}"#, r#"{"id":"second"}"#]),
        answer("recovered"),
    ]);
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: true,
        effects: Mutex::default(),
    });
    let mut registry = Registry::default();
    registry.register(Arc::new(Tools(fixture.clone()))).unwrap();
    let agent = agent(&model, &registry);
    let (_, mut inputs) = mpsc::channel(4);
    let cancel = CancellationToken::new();

    // Drop the run after the tool begins, with no opportunity to record a result.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            agent.run(
                &mut session,
                Some("lookup".into()),
                &mut inputs,
                &cancel,
                &mut ignore,
            )
        )
        .await
        .is_err()
    );
    assert_eq!(fixture.called.load(Ordering::SeqCst), 1);
    assert_eq!(session.operations()[0].1, OperationState::Started);
    assert_eq!(session.operations()[1].1, OperationState::Planned);
    drop(session);

    let mut session = Session::open(&path).unwrap();
    let err = agent
        .run(&mut session, None, &mut inputs, &cancel, &mut ignore)
        .await
        .unwrap_err();
    let Error::Reconciliation(operation_id) = err else {
        panic!("{err}")
    };
    assert!(matches!(
        session.operations()[0].1,
        OperationState::Finished(Outcome::Unknown(_))
    ));
    assert!(matches!(
        session.operations()[1].1,
        OperationState::Finished(Outcome::Skipped(_))
    ));
    assert!(session.checkout(1).is_err());
    let confirmed = fixture
        .effects
        .lock()
        .unwrap()
        .get(&operation_id)
        .unwrap()
        .clone();
    session
        .reconcile(operation_id, Outcome::Succeeded(confirmed))
        .unwrap();
    let result = agent
        .run(&mut session, None, &mut inputs, &cancel, &mut ignore)
        .await
        .unwrap();
    assert_eq!(result, "recovered");
    assert_eq!(
        agent
            .run(&mut session, None, &mut inputs, &cancel, &mut ignore)
            .await
            .unwrap(),
        result
    );
    assert_eq!(fixture.called.load(Ordering::SeqCst), 1);
    assert_eq!(model.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn cancellation_after_dispatch_leaves_unknown_result() {
    let dir = tempdir().unwrap();
    let mut session = Session::open(dir.path().join("session")).unwrap();
    let model = Script::new(vec![tool_reply(&[r#"{"id":"42"}"#])]);
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: true,
        effects: Mutex::default(),
    });
    let mut registry = Registry::default();
    registry.register(Arc::new(Tools(fixture.clone()))).unwrap();
    let agent = agent(&model, &registry);
    let (_, mut inputs) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let observer = &mut |event: Notification<'_>| {
        if matches!(
            event,
            Notification::Committed(Record {
                event: Event::ToolStarted { .. },
                ..
            })
        ) {
            let signal = signal.clone();
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                signal.cancel();
            });
        }
    };
    let error = agent
        .run(
            &mut session,
            Some("lookup".into()),
            &mut inputs,
            &cancel,
            observer,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Reconciliation(_)));
    assert!(matches!(
        session.operations()[0].1,
        OperationState::Finished(Outcome::Unknown(_))
    ));
    assert_eq!(fixture.called.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn independent_sessions_run_together_and_checkout_restores_context() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("one");
    let mut first = Session::open(&path).unwrap();
    assert!(Session::open(&path).is_err());
    let mut second = Session::open(dir.path().join("two")).unwrap();
    let mut model = Script::new(vec![
        answer("one"),
        answer("two"),
        answer("continued"),
        answer("fork"),
    ]);
    model.concurrent_start = Some(tokio::sync::Barrier::new(2));
    let registry = Registry::default();
    let agent = agent(&model, &registry);
    let (_, mut queue1) = mpsc::channel(1);
    let (_, mut queue2) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let mut observer_one = ignore;
    let mut observer_two = ignore;
    let (a, b) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            agent.run(
                &mut first,
                Some("private original".into()),
                &mut queue1,
                &cancel,
                &mut observer_one
            ),
            agent.run(
                &mut second,
                Some("other session".into()),
                &mut queue2,
                &cancel,
                &mut observer_two
            ),
        )
    })
    .await
    .expect("both independent model requests should overlap");
    assert_eq!(a.unwrap(), "one");
    assert_eq!(b.unwrap(), "two");
    let original = first.head();
    agent
        .compact(
            &mut first,
            Compaction {
                summary: "key fact 42".into(),
                keep_turns: 0,
            },
            &mut ignore,
        )
        .unwrap();
    drop(first);
    let mut first = Session::open(&path).unwrap();
    agent
        .run(
            &mut first,
            Some("use the summary".into()),
            &mut queue1,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    first.checkout(original).unwrap();
    drop(first);
    let mut first = Session::open(&path).unwrap();
    assert_eq!(first.head(), original);
    agent
        .run(
            &mut first,
            Some("follow original".into()),
            &mut queue1,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    let requests = model.requests.lock().unwrap();
    let compacted = serde_json::to_string(&requests[2]).unwrap();
    let forked = serde_json::to_string(&requests[3]).unwrap();
    assert!(compacted.contains("key fact 42") && !compacted.contains("private original"));
    assert!(forked.contains("private original") && !forked.contains("key fact 42"));
    assert!(!compacted.contains("other session") && !forked.contains("other session"));
}

#[tokio::test]
async fn incomplete_stream_commits_neither_partial_reply_nor_tool_and_recovers_torn_tail() {
    use std::io::Write;
    let dir = tempdir().unwrap();
    let path = dir.path().join("session");
    let mut session = Session::open(&path).unwrap();
    let model = Script::new(vec![
        vec![ModelEvent::Text("partial text".into())],
        answer("after reconnect"),
    ]);
    let registry = Registry::default();
    let agent = agent(&model, &registry);
    let (_, mut inputs) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    assert!(matches!(
        agent
            .run(
                &mut session,
                Some("original".into()),
                &mut inputs,
                &cancel,
                &mut ignore
            )
            .await,
        Err(Error::Model(_))
    ));
    assert!(
        !session
            .records()
            .iter()
            .any(|record| matches!(record.event, Event::Assistant { .. }))
    );
    let head = session.head();
    drop(session);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"{\"partial\":")
        .unwrap();
    let mut session = Session::open(&path).unwrap();
    assert_eq!(session.head(), head);
    assert_eq!(
        agent
            .run(&mut session, None, &mut inputs, &cancel, &mut ignore)
            .await
            .unwrap(),
        "after reconnect"
    );
    drop(session);
    assert!(Session::open(&path).is_ok());
}

#[test]
fn project_instructions_are_ordered_and_skills_are_explicit() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join(".pi/skills/first")).unwrap();
    std::fs::create_dir(root.join("nested")).unwrap();
    std::fs::write(root.join("AGENTS.md"), "root instructions").unwrap();
    std::fs::write(root.join("nested/AGENTS.md"), "nested instructions").unwrap();
    std::fs::write(root.join(".pi/skills/first/SKILL.md"), "skill text").unwrap();
    let out = project_context(root, root.join("nested"), &["first".into()]).unwrap();
    assert!(out.find("root instructions").unwrap() < out.find("nested instructions").unwrap());
    assert!(out.ends_with("skill text"));
    assert!(
        !project_context(root, root, &[])
            .unwrap()
            .contains("skill text")
    );
    assert!(project_context(root, root, &["../first".into()]).is_err());
}

#[tokio::test]
async fn truncated_summary_does_not_compact_history() {
    let mut partial = vec![ModelEvent::Metadata(ModelMetadata {
        finish_reason: Some("length".into()),
        ..Default::default()
    })];
    partial.extend(answer("unfinished summary"));
    let model = Script::new(vec![answer("original"), partial]);
    let registry = Registry::default();
    let agent = agent(&model, &registry);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel::<String>(1);
    let cancel = CancellationToken::new();
    agent
        .run(
            &mut session,
            Some("question".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    let error = agent
        .summarize(&mut session, 0, &cancel, &mut ignore)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("output token limit"));
    assert!(
        !session
            .records()
            .iter()
            .any(|record| matches!(record.event, Event::Compacted { .. }))
    );
    assert!(
        session
            .conversation_at(session.head())
            .unwrap()
            .iter()
            .any(|message| matches!(message, Message::User { content } if content == "question"))
    );
}

#[tokio::test]
async fn model_summary_uses_no_tools_and_preserves_the_prior_branch() {
    let dir = tempdir().unwrap();
    let mut session = Session::open(dir.path().join("session")).unwrap();
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: false,
        effects: Mutex::default(),
    });
    let mut registry = Registry::default();
    registry.register(Arc::new(Tools(fixture.clone()))).unwrap();
    let model = Script::new(vec![
        answer("initial answer"),
        answer("key fact 42"),
        answer("42"),
    ]);
    let agent = agent(&model, &registry);
    let (_, mut inputs) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    agent
        .run(
            &mut session,
            Some("private original".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    agent
        .summarize(&mut session, 0, &cancel, &mut ignore)
        .await
        .unwrap();
    agent
        .run(
            &mut session,
            Some("What was the key fact?".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    let requests = model.requests.lock().unwrap();
    assert!(requests[1].tools.is_empty());
    assert!(requests[1].messages.iter().any(
        |message| matches!(message, Message::User { content } if content == "private original")
    ));
    assert!(requests[2].messages.iter().any(
        |message| matches!(message, Message::User { content } if content.contains("key fact 42"))
    ));
    assert!(!requests[2].messages.iter().any(
        |message| matches!(message, Message::User { content } if content == "private original")
    ));
    assert!(session.records().iter().any(
        |record| matches!(&record.event, Event::User { content, .. } if content == "private original")
    ));
    assert_eq!(fixture.called.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn ollama_sse_requires_end_marker_even_after_finish_reason() {
    use futures_util::StreamExt;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        server.local_addr().unwrap()
    );
    let serve = std::thread::spawn(move || {
        let (mut socket, _) = server.accept().unwrap();
        let mut reader = BufReader::new(&socket);
        loop {
            let mut header = String::new();
            reader.read_line(&mut header).unwrap();
            if header == "\r\n" || header.is_empty() {
                break;
            }
        }
        let body = concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        );
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}", body.len()).unwrap();
    });
    let model = ollama::Ollama::new(&endpoint, None).unwrap();
    let mut stream = model
        .stream(ModelRequest {
            model: "fixture".into(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 32,
        })
        .await
        .unwrap();
    assert!(
        matches!(stream.next().await.unwrap().unwrap(), ModelEvent::Text(value) if value == "partial")
    );
    assert!(
        matches!(stream.next().await.unwrap(), Ok(ModelEvent::Metadata(metadata)) if metadata.finish_reason.as_deref() == Some("stop"))
    );
    assert!(matches!(
        stream.next().await.unwrap(),
        Err(Error::RetryableModel(_))
    ));
    assert!(stream.next().await.is_none());
    serve.join().unwrap();
}

#[tokio::test]
async fn tool_timeout_blocks_until_reconciliation() {
    let dir = tempdir().unwrap();
    let mut session = Session::open(dir.path().join("session")).unwrap();
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: true,
        effects: Mutex::default(),
    });
    let mut registry = Registry::default();
    registry.register(Arc::new(Tools(fixture.clone()))).unwrap();
    let model = Script::new(vec![tool_reply(&[r#"{"id":"42"}"#])]);
    let mut agent = agent(&model, &registry);
    agent.limits.tool_timeout = Duration::from_millis(20);
    let (_, mut inputs) = mpsc::channel(1);
    let error = agent
        .run(
            &mut session,
            Some("lookup".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Reconciliation(_)));
    assert!(
        matches!(&session.operations()[0].1, OperationState::Finished(Outcome::Unknown(reason)) if reason == "tool timed out")
    );
    assert_eq!(fixture.called.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn durable_queue_distinguishes_followups_and_deduplicates_deliveries() {
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let model = Script::new(vec![
        tool_reply(&[r#"{"id":"42"}"#]),
        answer("first"),
        answer("second"),
    ]);
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: false,
        effects: Mutex::default(),
    });
    let mut registry = Registry::default();
    registry.register(Arc::new(Tools(fixture))).unwrap();
    let agent = agent(&model, &registry);
    let (send, mut input) = mpsc::channel(8);
    let follow = QueuedInput {
        id: uuid::Uuid::new_v4(),
        kind: InputKind::FollowUp,
        content: "do the next task".into(),
    };
    send.try_send(follow.clone()).unwrap();
    send.try_send(follow.clone()).unwrap();
    let run_id = uuid::Uuid::new_v4();
    let cancel = CancellationToken::new();
    let result = agent
        .run_correlated(
            &mut session,
            run_id,
            Some("initial".into()),
            &mut input,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    assert_eq!(result, "second");
    {
        let requests = model.requests.lock().unwrap();
        assert!(
            !serde_json::to_string(&requests[1])
                .unwrap()
                .contains(&follow.content)
        );
        assert!(
            serde_json::to_string(&requests[2])
                .unwrap()
                .contains(&follow.content)
        );
    }
    let matches = session.records().iter().filter(|record| matches!(record.event, Event::InputQueued { ref input } if input.id == follow.id)).count();
    assert_eq!(matches, 1);
    assert!(
        matches!(agent.run_correlated(&mut session, run_id, Some("initial".into()), &mut input, &cancel, &mut ignore).await, Err(Error::DuplicateRun(id)) if id == run_id)
    );
    assert_eq!(
        agent
            .run_correlated(&mut session, run_id, None, &mut input, &cancel, &mut ignore)
            .await
            .unwrap(),
        "second"
    );
}

struct ParallelFixture {
    barrier: tokio::sync::Barrier,
}

#[async_trait::async_trait]
impl Tool for ParallelFixture {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "lookup".into(),
            description: "Read fixture".into(),
            parameters: json!({"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}),
        }
    }
    fn execution(&self) -> ToolExecution {
        ToolExecution::ParallelRead
    }

    async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
        self.barrier.wait().await;
        let id = args["id"].as_str().unwrap();
        if id == "slow" {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Outcome::Succeeded(id.into())
    }
}

struct ParallelExtension;
impl Extension for ParallelExtension {
    fn id(&self) -> &str {
        "parallel-fixture"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(ParallelFixture {
            barrier: tokio::sync::Barrier::new(2),
        })]
    }
}

#[tokio::test]
async fn parallel_read_results_keep_their_original_call_ids() {
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let mut registry = Registry::default();
    registry.register(Arc::new(ParallelExtension)).unwrap();
    let model = Script::new(vec![
        tool_reply(&[r#"{"id":"slow"}"#, r#"{"id":"fast"}"#]),
        answer("done"),
    ]);
    let (_, mut inputs) = mpsc::channel(1);
    let mut harness = agent(&model, &registry);
    harness.limits.tool_timeout = Duration::from_secs(2);
    harness
        .run(
            &mut session,
            Some("read both".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await
        .unwrap();
    let requests = model.requests.lock().unwrap();
    assert!(requests[1].messages.contains(&Message::Tool {
        call_id: "call-0".into(),
        content: "slow".into()
    }));
    assert!(requests[1].messages.contains(&Message::Tool {
        call_id: "call-1".into(),
        content: "fast".into()
    }));
}

#[tokio::test]
async fn measured_context_tracks_trailing_input_edits_compaction_and_checkout() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("measured.jsonl");
    let mut session = Session::open(&path).unwrap();
    let model = Script::new(vec![answer("reply"), answer("next reply")]);
    let registry = Registry::default();
    let harness = agent(&model, &registry);
    let (_, mut inputs) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    harness
        .run(
            &mut session,
            Some("first".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    let first_head = session.head();
    assert_eq!(session.context_usage_estimate().usage_tokens, 28);
    session
        .append_user_with_images("tail".into(), vec![], None)
        .unwrap();
    let estimate = session.context_usage_estimate();
    assert_eq!(estimate.tokens, 29);
    assert_eq!(estimate.trailing_tokens, 1);
    let user = session
        .records()
        .iter()
        .find(|record| matches!(record.event, Event::User { .. }))
        .unwrap()
        .id;
    session
        .edit_context(user, Some(ContextContent::Text("x".repeat(200))))
        .unwrap();
    let edited = session.context_usage_estimate();
    assert_eq!(edited.last_usage_record, None);
    assert_eq!(
        edited.tokens,
        session
            .conversation_at(session.head())
            .unwrap()
            .iter()
            .map(estimate_message_tokens)
            .sum::<u64>()
    );
    harness
        .run(&mut session, None, &mut inputs, &cancel, &mut ignore)
        .await
        .unwrap();
    assert_eq!(session.context_usage_estimate().tokens, 28);
    harness
        .compact(
            &mut session,
            Compaction {
                summary: "summary".into(),
                keep_turns: 0,
            },
            &mut ignore,
        )
        .unwrap();
    assert_eq!(session.context_usage_estimate().tokens, 2);
    assert_eq!(session.context_usage_estimate().last_usage_record, None);
    session.checkout(first_head).unwrap();
    assert_eq!(session.context_usage_estimate().tokens, 28);
    drop(session);
    let mut session = Session::open(&path).unwrap();
    assert_eq!(session.context_usage_estimate().tokens, 28);
    session.select_model("other", None).unwrap();
    assert_eq!(session.context_usage_estimate().last_usage_record, None);
}

#[tokio::test]
async fn zero_reported_usage_falls_back_to_visible_content() {
    let mut reply = answer("visible reply");
    if let Some(ModelEvent::Complete { usage, .. }) = reply.last_mut() {
        *usage = Some(Usage::default());
    }
    let model = Script::new(vec![reply]);
    let registry = Registry::default();
    let harness = agent(&model, &registry);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    harness
        .run(
            &mut session,
            Some("first".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await
        .unwrap();
    let estimate = session.context_usage_estimate();
    assert_eq!(estimate.last_usage_record, None);
    assert_eq!(
        estimate.tokens,
        session
            .conversation_at(session.head())
            .unwrap()
            .iter()
            .map(estimate_message_tokens)
            .sum::<u64>()
    );
}

#[tokio::test]
async fn measured_context_triggers_reserve_even_when_text_estimate_is_small() {
    let mut first = answer("short");
    if let Some(ModelEvent::Complete { usage, .. }) = first.last_mut() {
        *usage = Some(Usage {
            input_tokens: 1500,
            output_tokens: 20,
        });
    }
    let model = Script::new(vec![first, answer("summary"), answer("done")]);
    let registry = Registry::default();
    let mut harness = agent(&model, &registry);
    harness.limits.output_tokens_per_request = 128;
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    harness
        .run(
            &mut session,
            Some("first".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    harness.limits.context_window = Some(2000);
    session
        .auto_compaction_control()
        .set_token_settings(CompactionTokens {
            reserve_tokens: Some(1000),
            keep_recent_tokens: Some(1),
        });
    harness
        .run(
            &mut session,
            Some("next".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    assert_eq!(model.requests.lock().unwrap().len(), 3);
    assert!(
        session
            .records()
            .iter()
            .any(|record| matches!(record.event, Event::Compacted { .. }))
    );
}

#[tokio::test]
async fn measured_context_uses_raw_prompt_counts_without_adding_cache_twice() {
    for (prompt, expected) in [(None, 28), (Some(0), 28), (Some(30), 38)] {
        let mut reply = answer("reply");
        reply.insert(
            0,
            ModelEvent::Metadata(ModelMetadata {
                cached_input_tokens: Some(7),
                cache_creation_input_tokens: Some(4),
                provider_prompt_tokens: prompt,
                ..ModelMetadata::default()
            }),
        );
        let model = Script::new(vec![reply]);
        let registry = Registry::default();
        let harness = agent(&model, &registry);
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let (_, mut inputs) = mpsc::channel(1);
        harness
            .run(
                &mut session,
                Some("first".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut ignore,
            )
            .await
            .unwrap();
        assert_eq!(session.context_usage_estimate().tokens, expected);
    }
}

#[tokio::test]
async fn automatic_compaction_can_be_disabled_and_reenabled_without_removing_the_context_limit() {
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let model = Script::new(vec![
        unmetered_answer(&"z".repeat(1500)),
        unmetered_answer("older facts"),
        unmetered_answer("continued"),
    ]);
    let registry = Registry::default();
    let mut harness = agent(&model, &registry);
    harness.limits.output_tokens_per_request = 768;
    let (_, mut inputs) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    harness
        .run(
            &mut session,
            Some("old question".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    harness.limits.context_window = Some(1100);
    let control = session.auto_compaction_control();
    control.set_enabled(false);
    let result = harness
        .run(
            &mut session,
            Some("This is the exact latest request.".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await;
    assert!(
        matches!(&result, Err(Error::Model(message)) if message.contains("automatic compaction is disabled")),
        "{result:?}"
    );
    assert_eq!(model.requests.lock().unwrap().len(), 1);
    assert!(
        !session
            .records()
            .iter()
            .any(|record| matches!(record.event, Event::Compacted { .. }))
    );
    control.set_enabled(true);
    harness
        .run(&mut session, None, &mut inputs, &cancel, &mut ignore)
        .await
        .unwrap();
    let requests = model.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[2].messages.contains(&Message::User {
        content: "This is the exact latest request.".into()
    }));
    assert!(
        session
            .records()
            .iter()
            .any(|record| matches!(record.event, Event::Compacted { .. }))
    );
}

#[tokio::test]
async fn configured_reserve_compacts_early_and_preserves_recent_messages() {
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let model = Script::new(vec![
        unmetered_answer(&"a".repeat(1500)),
        unmetered_answer(&"b".repeat(1500)),
        unmetered_answer("summary of first"),
        unmetered_answer("latest answer"),
    ]);
    let registry = Registry::default();
    let mut harness = agent(&model, &registry);
    harness.limits.output_tokens_per_request = 128;
    let (_, mut inputs) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    for prompt in ["first question", "second question"] {
        harness
            .run(
                &mut session,
                Some(prompt.into()),
                &mut inputs,
                &cancel,
                &mut ignore,
            )
            .await
            .unwrap();
    }
    harness.limits.context_window = Some(2000);
    session
        .auto_compaction_control()
        .set_token_settings(CompactionTokens {
            reserve_tokens: Some(1500),
            keep_recent_tokens: Some(300),
        });
    harness
        .run(
            &mut session,
            Some("latest question".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    let requests = model.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests[2].messages.contains(&Message::User {
        content: "first question".into()
    }));
    assert!(requests[2].messages.contains(&Message::User {
        content: "second question".into()
    }));
    assert!(!requests[3].messages.contains(&Message::User {
        content: "second question".into()
    }));
    assert!(requests[3].messages.iter().any(|message| matches!(message, Message::Assistant { content, .. } if content == &"b".repeat(1500))));
    assert!(requests[3].messages.contains(&Message::User {
        content: "latest question".into()
    }));
    assert!(!requests[3].messages.contains(&Message::User {
        content: "first question".into()
    }));
}

#[tokio::test]
async fn split_turn_retention_preserves_tool_pairs_and_projects_late_edits() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("split.jsonl");
    let mut session = Session::open(&path).unwrap();
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: false,
        effects: Mutex::default(),
    });
    let mut registry = Registry::default();
    registry.register(Arc::new(Tools(fixture))).unwrap();
    let model = Script::new(vec![
        tool_reply(&[r#"{"id":"kept"}"#]),
        answer("remove this final reply"),
        answer("summary of revised input"),
    ]);
    let harness = agent(&model, &registry);
    let (_, mut inputs) = mpsc::channel::<AgentInput>(1);
    let cancel = CancellationToken::new();
    harness
        .run_correlated(
            &mut session,
            uuid::Uuid::new_v4(),
            Some("original private input".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    let mut final_cut = Session::from_records(session.records().to_vec()).unwrap();
    final_cut
        .auto_compaction_control()
        .set_token_settings(CompactionTokens {
            reserve_tokens: None,
            keep_recent_tokens: Some(1),
        });
    harness
        .compact_configured(&mut final_cut, "supplied summary".into(), &mut ignore)
        .unwrap();
    let retained_final = final_cut.conversation_at(final_cut.head()).unwrap();
    assert_eq!(retained_final.len(), 2);
    assert!(
        matches!(&retained_final[1], Message::Assistant { content, calls, .. } if content == "remove this final reply" && calls.is_empty())
    );
    let user = session
        .records()
        .iter()
        .find(|record| matches!(record.event, Event::User { .. }))
        .unwrap()
        .id;
    let last_assistant = session
        .records()
        .iter()
        .rev()
        .find(|record| matches!(record.event, Event::Assistant { .. }))
        .unwrap()
        .id;
    session
        .edit_context(user, Some(ContextContent::Text("revised input".into())))
        .unwrap();
    session.edit_context(last_assistant, None).unwrap();
    session
        .auto_compaction_control()
        .set_token_settings(CompactionTokens {
            reserve_tokens: None,
            keep_recent_tokens: Some(1),
        });
    harness
        .summarize_configured(&mut session, &cancel, &mut ignore, Some(&mut inputs), None)
        .await
        .unwrap();
    let requests = model.requests.lock().unwrap();
    assert!(requests[2].tools.is_empty());
    assert!(requests[2].messages.contains(&Message::User {
        content: "revised input".into()
    }));
    assert!(
        !serde_json::to_string(&requests[2])
            .unwrap()
            .contains("original private input")
    );
    assert!(
        !requests[2]
            .messages
            .iter()
            .any(|message| matches!(message, Message::Tool { .. }))
    );
    drop(requests);
    let retained = session.conversation_at(session.head()).unwrap();
    assert!(
        matches!(&retained[0], Message::User { content } if content.starts_with("Prior conversation summary:"))
    );
    assert!(
        matches!(&retained[1], Message::Assistant { calls, .. } if calls.len() == 1),
        "{retained:?}"
    );
    assert!(matches!(&retained[2], Message::Tool { content, .. } if content == "found kept"));
    assert_eq!(retained.len(), 3);
    drop(session);
    let reopened = Session::open(path).unwrap();
    assert_eq!(reopened.conversation_at(reopened.head()).unwrap(), retained);
    assert!(reopened.records().iter().any(|record| matches!(&record.event, Event::User { content, .. } if content == "original private input")));
}

#[tokio::test]
async fn bounded_automatic_compaction_leaves_the_latest_input_unchanged() {
    for discover in [false, true] {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let model = Script::new(vec![
            unmetered_answer(&"z".repeat(1500)),
            unmetered_answer("older facts"),
            unmetered_answer("continued"),
        ]);
        let registry = Registry::default();
        let mut harness = agent(&model, &registry);
        harness.limits.output_tokens_per_request = 768;
        let (_, mut inputs) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        harness
            .run(
                &mut session,
                Some("old question".into()),
                &mut inputs,
                &cancel,
                &mut ignore,
            )
            .await
            .unwrap();
        if discover {
            model.context_window.store(1100, Ordering::SeqCst);
        } else {
            model.context_window.store(1, Ordering::SeqCst);
            harness.limits.context_window = Some(1100);
        }
        let latest = "This is the exact latest request.";
        harness
            .run(
                &mut session,
                Some(latest.into()),
                &mut inputs,
                &cancel,
                &mut ignore,
            )
            .await
            .unwrap();
        let requests = model.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(
            !serde_json::to_string(&requests[1])
                .unwrap()
                .contains(latest)
        );
        assert!(requests[2].messages.contains(&Message::User {
            content: latest.into()
        }));
        assert!(
            serde_json::to_string(&requests[2])
                .unwrap()
                .contains("older facts")
        );
    }
}

#[test]
fn project_skill_resolution_validates_version_and_scope() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let nested = root.join("nested");
    for dir in [root, nested.as_path()] {
        std::fs::create_dir_all(dir.join(".pi/skills/review")).unwrap();
        std::fs::write(
            dir.join(".pi/skills/review/SKILL.md"),
            dir.to_string_lossy().as_bytes(),
        )
        .unwrap();
    }
    let resources = ProjectResources::open(root, &nested).unwrap();
    assert_eq!(resources.skills().unwrap(), ["review"]);
    let skill = resources.skill("review", None).unwrap();
    assert_eq!(skill.id, "nested/.pi/skills/review/SKILL.md");
    std::fs::write(nested.join(".pi/skills/review/SKILL.md"), "new version").unwrap();
    assert!(resources.skill("review", Some(&skill.version)).is_err());
    let unrelated = tempdir().unwrap();
    assert!(ProjectResources::open(root, unrelated.path()).is_err());
}

#[test]
fn v1_journal_is_preserved_when_v2_appends() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("legacy.jsonl");
    let session_id = uuid::Uuid::new_v4();
    let original = format!(
        "{}\n{}",
        json!({"version":1,"id":1,"parent":null,"event":{"type":"created","session_id":session_id}}),
        format_args!(
            "{}\n",
            json!({"version":1,"id":2,"parent":1,"event":{"type":"user","content":"legacy input"}})
        )
    );
    std::fs::write(&path, &original).unwrap();
    let mut session = Session::open(&path).unwrap();
    assert_eq!(session.session_id(), session_id);
    session.checkout(1).unwrap();
    assert_eq!(session.records().last().unwrap().version, 2);
    drop(session);
    assert!(
        std::fs::read_to_string(&path)
            .unwrap()
            .starts_with(&original)
    );
    assert_eq!(Session::open(&path).unwrap().head(), 1);
}

#[tokio::test]
async fn review_reads_selected_branch_and_returns_findings_to_the_parent() {
    use pi_rs_review::{Review, ReviewContext};

    let mut parent = Session::with_store(MemoryStore::default()).unwrap();
    let mut child = Session::with_store(MemoryStore::default()).unwrap();
    let model = Script::new(vec![
        answer("recorded"),
        answer("detour"),
        answer("dry run finding"),
        answer("parent received finding"),
    ]);
    let mut registry = Registry::default();
    registry.register(Arc::new(ReviewContext)).unwrap();
    let harness = agent(&model, &registry);
    let cancel = CancellationToken::new();
    let (_, mut inputs) = mpsc::channel(1);
    harness
        .run(
            &mut parent,
            Some("original dry run authorization".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    let anchor = parent.head();
    harness
        .run(
            &mut parent,
            Some("hypothetical production detour".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    let review = Review {
        task: "review authorization",
        from_record: Some(anchor),
        allowed_tools: Default::default(),
        output_budget: 100,
        timeout: Duration::from_secs(2),
        resume: false,
    };
    assert_eq!(
        review
            .run(&harness, &mut parent, &mut child, &cancel, &mut ignore)
            .await
            .unwrap(),
        "dry run finding"
    );
    assert!(
        parent.extension_states()["branch-review"]
            .data
            .get(child.session_id().to_string())
            .is_some()
    );
    harness
        .run(
            &mut parent,
            Some("continue after review".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    let requests = model.requests.lock().unwrap();
    let child_context = serde_json::to_string(&requests[2]).unwrap();
    assert!(child_context.contains("original dry run authorization"));
    assert!(!child_context.contains("hypothetical production detour"));
    let parent_context = serde_json::to_string(&requests[3]).unwrap();
    assert!(
        parent_context.contains("dry run finding")
            && parent_context.contains("hypothetical production detour")
    );
}

struct Depends(&'static str, &'static [&'static str]);
impl Extension for Depends {
    fn id(&self) -> &str {
        self.0
    }
    fn dependencies(&self) -> &[&str] {
        self.1
    }
}

#[test]
fn extension_profiles_reject_cycles_and_disabled_dependencies() {
    assert!(
        Registry::compose(
            vec![
                Arc::new(Depends("a", &["b"])),
                Arc::new(Depends("b", &["a"]))
            ],
            &Default::default()
        )
        .is_err()
    );
    assert!(
        Registry::compose(
            vec![Arc::new(Depends("a", &["b"])), Arc::new(Depends("b", &[]))],
            &["b".into()].into()
        )
        .is_err()
    );
    let fixture = Arc::new(Fixture {
        called: AtomicUsize::new(0),
        hold: false,
        effects: Mutex::default(),
    });
    let registry = Registry::compose(
        vec![Arc::new(Tools(fixture))],
        &["test-tools".into()].into(),
    )
    .unwrap();
    assert!(!registry.has_tool("lookup"));
}

#[tokio::test]
async fn interrupted_compaction_preserves_prior_branch_and_pending_context() {
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let model = Script::new(vec![
        answer("original answer"),
        vec![ModelEvent::Text("partial summary".into())],
        answer("continued"),
    ]);
    let registry = Registry::default();
    let agent = agent(&model, &registry);
    let (_, mut inputs) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    agent
        .run(
            &mut session,
            Some("retain the prior task".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    assert!(
        agent
            .summarize(&mut session, 0, &cancel, &mut ignore)
            .await
            .is_err()
    );
    assert!(
        !session
            .records()
            .iter()
            .any(|record| matches!(record.event, Event::Compacted { .. }))
    );
    assert!(session.records().iter().any(|record| matches!(&record.event,
        Event::ModelFinished { partial, usage: UsageReport::Unconfirmed, failure: Some(_), .. } if partial == "partial summary")));
    agent
        .run(
            &mut session,
            Some("continue".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    let requests = model.requests.lock().unwrap();
    let context = serde_json::to_string(&requests[2]).unwrap();
    assert!(context.contains("retain the prior task"));
    assert!(!context.contains("partial summary"));
}

struct RetryScript {
    requests: Mutex<Vec<ModelRequest>>,
    failures: usize,
    settlement_failure: bool,
}
#[async_trait::async_trait]
impl Model for RetryScript {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let mut requests = self.requests.lock().unwrap();
        requests.push(request);
        if requests.len() <= self.failures {
            return Ok(Box::pin(stream::iter(vec![
                Ok(ModelEvent::Text("partial".into())),
                Err(Error::RetryableModel("temporary connection failure".into())),
            ])));
        }
        Ok(Box::pin(stream::iter(
            answer("recovered").into_iter().map(Ok),
        )))
    }
    async fn record_usage(&self, _: uuid::Uuid, _: &UsageReport) -> Result<()> {
        if self.settlement_failure {
            Err(Error::RetryableModel("settlement unavailable".into()))
        } else {
            Ok(())
        }
    }
}

fn enable_fast_retries(session: &Session, max_retries: u32) {
    session.retry_control().configure(RetrySettings {
        enabled: true,
        max_retries,
        base_delay_ms: 1,
        max_agent_delay_ms: 4,
    });
}

#[tokio::test]
async fn retries_failed_model_attempts_without_replaying_partial_text() {
    let model = RetryScript {
        requests: Mutex::new(vec![]),
        failures: 2,
        settlement_failure: false,
    };
    let registry = Registry::default();
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    enable_fast_retries(&session, 3);
    let mut events = Vec::new();
    let (sender, mut inputs) = mpsc::channel::<String>(8);
    let mut observe = |event: Notification<'_>| {
        if let Notification::Retry(event) = event {
            events.push(event);
        }
    };
    assert_eq!(
        agent(&model, &registry)
            .run(
                &mut session,
                Some("hello".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut observe
            )
            .await
            .unwrap(),
        "recovered"
    );
    drop(sender);
    assert_eq!(model.requests.lock().unwrap().len(), 3);
    assert_eq!(events.len(), 3);
    assert!(matches!(
        events.last(),
        Some(RetryEvent::End {
            success: true,
            attempt: 2,
            ..
        })
    ));
    let messages = session.conversation_at(session.head()).unwrap();
    assert!(!format!("{messages:?}").contains("partial"));
    assert_eq!(
        session
            .records()
            .iter()
            .filter(|record| matches!(
                record.event,
                Event::ModelFinished {
                    failure: Some(ModelFailure::Retryable),
                    ..
                }
            ))
            .count(),
        2
    );
}

#[tokio::test]
async fn retry_limits_and_request_budget_both_bound_attempts() {
    for (retries, requests, expected) in [(0, 12, 1), (2, 12, 3), (5, 2, 2)] {
        let model = RetryScript {
            requests: Mutex::new(vec![]),
            failures: 10,
            settlement_failure: false,
        };
        let registry = Registry::default();
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        enable_fast_retries(&session, retries);
        let mut harness = agent(&model, &registry);
        harness.limits.model_requests = requests;
        let (_sender, mut inputs) = mpsc::channel::<String>(1);
        let mut events = Vec::new();
        let result = harness
            .run(
                &mut session,
                Some("hello".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |event| {
                    if let Notification::Retry(event) = event {
                        events.push(event)
                    }
                },
            )
            .await;
        assert!(result.is_err());
        assert_eq!(model.requests.lock().unwrap().len(), expected);
        if expected > 1 {
            assert!(matches!(
                events.last(),
                Some(RetryEvent::End { success: false, .. })
            ));
        }
    }
}

#[tokio::test]
async fn retry_wait_is_abortable_and_keeps_queued_inputs() {
    for abort in [false, true] {
        let model = RetryScript {
            requests: Mutex::new(vec![]),
            failures: 1,
            settlement_failure: false,
        };
        let registry = Registry::default();
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        enable_fast_retries(&session, 3);
        let control = session.retry_control();
        let (sender, mut inputs) = mpsc::channel::<String>(4);
        let mut events = Vec::new();
        let result = agent(&model, &registry)
            .run(
                &mut session,
                Some("hello".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |event| {
                    if let Notification::Retry(event) = event {
                        if matches!(event, RetryEvent::Start { .. }) {
                            assert!(control.is_retrying());
                            sender.try_send("queued".into()).unwrap();
                            if abort {
                                control.abort();
                            }
                        }
                        events.push(event);
                    }
                },
            )
            .await;
        assert!(!control.is_retrying());
        if abort {
            assert!(matches!(result, Err(Error::Cancelled)));
            assert_eq!(model.requests.lock().unwrap().len(), 1);
            assert!(
                matches!(events.last(),Some(RetryEvent::End {success:false,final_error:Some(message),..}) if message=="Retry cancelled")
            );
        } else {
            assert!(result.is_ok());
            assert!(
                format!(
                    "{:?}",
                    model.requests.lock().unwrap().last().unwrap().messages
                )
                .contains("queued")
            );
        }
    }
}

#[tokio::test]
async fn completed_reply_settlement_errors_do_not_retry_the_provider() {
    let model = RetryScript {
        requests: Mutex::new(vec![]),
        failures: 0,
        settlement_failure: true,
    };
    let registry = Registry::default();
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    enable_fast_retries(&session, 3);
    let (_sender, mut inputs) = mpsc::channel::<String>(1);
    assert!(
        agent(&model, &registry)
            .run(
                &mut session,
                Some("hello".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |_| {}
            )
            .await
            .is_err()
    );
    assert_eq!(model.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn failed_reply_settlement_errors_do_not_retry_the_provider() {
    let model = RetryScript {
        requests: Mutex::new(vec![]),
        failures: 10,
        settlement_failure: true,
    };
    let registry = Registry::default();
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    enable_fast_retries(&session, 3);
    let (_sender, mut inputs) = mpsc::channel::<String>(1);
    let mut retries = 0;
    let result = agent(&model, &registry)
        .run(
            &mut session,
            Some("hello".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut |event| {
                if matches!(event, Notification::Retry(_)) {
                    retries += 1
                }
            },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(model.requests.lock().unwrap().len(), 1);
    assert_eq!(retries, 0);
}

struct SlashCommands(&'static str, &'static [&'static str]);
impl Extension for SlashCommands {
    fn id(&self) -> &str {
        self.0
    }
    fn commands(&self) -> &[&str] {
        self.1
    }
    fn command_description(&self, _: &str) -> Option<&str> {
        Some("Set test state")
    }
    fn command(
        &self,
        name: &str,
        arguments: &str,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<ExtensionState> {
        Ok(ExtensionState {
            version: 1,
            data: json!({"name":name,"arguments":arguments}),
        })
    }
}

#[test]
fn discovered_command_names_match_pi_collisions_and_invoke_the_correct_owner() {
    let mut registry = Registry::default();
    registry
        .register(Arc::new(SlashCommands("first", &["set", "set:1"])))
        .unwrap();
    registry
        .register(Arc::new(SlashCommands("second", &["set", "other"])))
        .unwrap();
    let commands = registry.registered_commands();
    assert_eq!(
        commands
            .iter()
            .map(|command| command.invocation_name.as_str())
            .collect::<Vec<_>>(),
        ["set:1", "set:1:2", "set:2", "other"]
    );
    assert_eq!(commands[2].description.as_deref(), Some("Set test state"));
    let model = Script::new(vec![]);
    let harness = agent(&model, &registry);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    assert!(
        harness
            .try_extension_command(&mut session, "/set:2  hello world", &mut ignore)
            .unwrap()
    );
    assert!(!session.extension_states().contains_key("first"));
    assert_eq!(
        session.extension_states()["second"].data,
        json!({"name":"set","arguments":"hello world"})
    );
    assert!(
        !harness
            .try_extension_command(&mut session, "/unknown hello", &mut ignore)
            .unwrap()
    );
    assert!(
        !harness
            .try_extension_command(&mut session, "ordinary text", &mut ignore)
            .unwrap()
    );
    assert!(model.requests.lock().unwrap().is_empty());
    let filtered = Registry::compose(
        vec![
            Arc::new(SlashCommands("first", &["set"])),
            Arc::new(SlashCommands("second", &["set"])),
        ],
        &["first".into()].into(),
    )
    .unwrap();
    assert_eq!(filtered.registered_commands()[0].invocation_name, "set");
}

struct CatalogTool {
    name: &'static str,
    calls: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl Tool for CatalogTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name.into(),
            description: self.name.into(),
            parameters: json!({"type":"object","properties":{},"additionalProperties":false}),
        }
    }
    fn execution(&self) -> ToolExecution {
        ToolExecution::ParallelRead
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Outcome::Succeeded(self.name.into())
    }
}
struct CatalogExtension {
    name: &'static str,
    calls: Arc<AtomicUsize>,
}
impl Extension for CatalogExtension {
    fn id(&self) -> &str {
        "catalog"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(CatalogTool {
            name: self.name,
            calls: self.calls.clone(),
        })]
    }
}
struct ChangingCatalog {
    refreshes: AtomicUsize,
    calls: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl RegistryRefresher for ChangingCatalog {
    async fn refresh(&self, current: &Registry) -> Result<Option<RegistryUpdate>> {
        if self.calls.load(Ordering::SeqCst) == 0
            || self.refreshes.fetch_add(1, Ordering::SeqCst) != 0
        {
            return Ok(None);
        }
        let mut registry = current.clone();
        registry.replace(Arc::new(CatalogExtension {
            name: "new_tool",
            calls: self.calls.clone(),
        }))?;
        registry.set_active_tools(["new_tool".into()])?;
        Ok(Some(RegistryUpdate {
            registry,
            system_prompt: Some("updated catalog".into()),
        }))
    }
}
#[tokio::test]
async fn tool_catalog_refresh_changes_advertising_and_dispatch_inside_one_run() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(CatalogExtension {
            name: "old_tool",
            calls: calls.clone(),
        }))
        .unwrap();
    registry.set_refresher(Arc::new(ChangingCatalog {
        refreshes: AtomicUsize::new(0),
        calls: calls.clone(),
    }));
    let call = |name: &str| {
        vec![ModelEvent::Complete {
            calls: vec![ToolCall {
                id: name.into(),
                name: name.into(),
                arguments: "{}".into(),
            }],
            usage: Some(Usage {
                input_tokens: 1,
                output_tokens: 1,
            }),
        }]
    };
    let model = Script::new(vec![call("old_tool"), call("new_tool"), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_sender, mut inputs) = mpsc::channel::<String>(1);
    assert_eq!(
        agent(&model, &registry)
            .run(
                &mut session,
                Some("refresh".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut ignore
            )
            .await
            .unwrap(),
        "done"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    {
        let requests = model.requests.lock().unwrap();
        assert_eq!(requests[0].tools[0].name, "old_tool");
        assert_eq!(requests[1].tools.len(), 1);
        assert_eq!(requests[1].tools[0].name, "new_tool");
        assert!(
            matches!(&requests[1].messages[0], Message::System {content} if content=="updated catalog")
        );
    }
    let read_only = registry
        .read_only_subset(&["old_tool".into()].into())
        .unwrap();
    assert!(read_only.refreshed().await.unwrap().is_none());
}

struct RemovingCatalog(AtomicUsize);
#[async_trait::async_trait]
impl RegistryRefresher for RemovingCatalog {
    async fn refresh(&self, current: &Registry) -> Result<Option<RegistryUpdate>> {
        if self.0.fetch_add(1, Ordering::SeqCst) != 1 {
            return Ok(None);
        }
        let mut registry = current.clone();
        registry.retain_tools(|tool| tool.name != "old_tool");
        Ok(Some(RegistryUpdate {
            registry,
            system_prompt: None,
        }))
    }
}
#[tokio::test]
async fn removed_tools_are_not_dispatched_after_the_model_returns() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(CatalogExtension {
            name: "old_tool",
            calls: calls.clone(),
        }))
        .unwrap();
    registry.set_refresher(Arc::new(RemovingCatalog(AtomicUsize::new(0))));
    let model = Script::new(vec![
        vec![ModelEvent::Complete {
            calls: vec![ToolCall {
                id: "old".into(),
                name: "old_tool".into(),
                arguments: "{}".into(),
            }],
            usage: None,
        }],
        answer("done"),
    ]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_sender, mut inputs) = mpsc::channel::<String>(1);
    agent(&model, &registry)
        .run(
            &mut session,
            Some("remove it".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(model.requests.lock().unwrap()[1].tools.is_empty());
    assert!(matches!(
        &session.operations()[0].1,
        OperationState::Finished(Outcome::Skipped(_))
    ));
}

struct HangingCatalog;
#[async_trait::async_trait]
impl RegistryRefresher for HangingCatalog {
    async fn refresh(&self, _: &Registry) -> Result<Option<RegistryUpdate>> {
        std::future::pending().await
    }
}
#[tokio::test]
async fn catalog_refresh_can_be_cancelled_before_a_model_request() {
    let mut registry = Registry::default();
    registry.set_refresher(Arc::new(HangingCatalog));
    let model = Script::new(vec![]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_sender, mut inputs) = mpsc::channel::<String>(1);
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        signal.cancel();
    });
    assert!(matches!(
        agent(&model, &registry)
            .run(
                &mut session,
                Some("hello".into()),
                &mut inputs,
                &cancel,
                &mut ignore
            )
            .await,
        Err(Error::Cancelled)
    ));
    assert!(model.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancelled_catalog_refresh_settles_unstarted_tools() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(CatalogExtension {
            name: "old_tool",
            calls: calls.clone(),
        }))
        .unwrap();
    let model = Script::new(vec![vec![ModelEvent::Complete {
        calls: vec![ToolCall {
            id: "old".into(),
            name: "old_tool".into(),
            arguments: "{}".into(),
        }],
        usage: None,
    }]]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_sender, mut inputs) = mpsc::channel::<String>(1);
    let cancel = CancellationToken::new();
    let result=agent(&model,&registry).run(&mut session,Some("hello".into()),&mut inputs,&cancel,&mut |event| {
        if matches!(event,Notification::Committed(record) if matches!(record.event,Event::Assistant {..})) {cancel.cancel();}
    }).await;
    assert!(matches!(result, Err(Error::Cancelled)));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        session
            .operations()
            .iter()
            .all(|(_, state)| matches!(state, OperationState::Finished(Outcome::Skipped(_))))
    );
}
