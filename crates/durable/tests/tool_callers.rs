use futures_util::{StreamExt, stream};
use pi_rs_core::{
    CancellationToken, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool,
    ToolCall, ToolContext, ToolSpec,
};
use pi_rs_durable::{
    extension::{Context, Extension, GenerationDecision, GenerationEvent, Hook},
    keyed_tool::{KeyedHost, KeyedTool, KeyedToolAdapter},
    recovery::{ReplayPolicy, UncertainPolicy},
    reset::ToolControl,
    runtime::{Config, Harness, Options, RegisteredTool, ToolCallers},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct TestModel {
    first: &'static str,
    requests: Mutex<Vec<Vec<String>>>,
    attempts: AtomicUsize,
}
#[async_trait::async_trait]
impl Model for TestModel {
    async fn stream(&self, request: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        self.requests
            .lock()
            .unwrap()
            .push(request.tools.iter().map(|s| s.name.clone()).collect());
        let calls = if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            vec![ToolCall {
                id: "one".into(),
                name: self.first.into(),
                arguments: "{}".into(),
            }]
        } else {
            vec![]
        };
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])
        .boxed())
    }
}
struct Child(&'static str, Arc<AtomicUsize>);
#[async_trait::async_trait]
impl Tool for Child {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.0.into(),
            description: String::new(),
            parameters: serde_json::json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        self.1.fetch_add(1, Ordering::SeqCst);
        Outcome::Succeeded("child ran".into())
    }
}
struct Parent {
    target: &'static str,
    seen: Arc<Mutex<Vec<String>>>,
    add: bool,
}
#[async_trait::async_trait]
impl KeyedTool for Parent {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "parent".into(),
            description: String::new(),
            parameters: serde_json::json!({"type":"object"}),
        }
    }
    async fn invoke(
        &self,
        _: serde_json::Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        _: KeyedHost,
    ) -> pi_rs_core::ToolReply {
        unreachable!()
    }
    async fn invoke_controlled(
        &self,
        _: serde_json::Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        host: KeyedHost,
    ) -> (pi_rs_core::ToolReply, ToolControl) {
        *self.seen.lock().unwrap() = host
            .ordinary()
            .tools()
            .iter()
            .map(|s| s.name.clone())
            .collect();
        let result = host
            .call_keyed("child", self.target, serde_json::json!({}))
            .await;
        (
            Outcome::Succeeded(format!("{result:?}")).into(),
            ToolControl {
                add_tools: if self.add {
                    vec!["hidden".into()]
                } else {
                    vec![]
                },
                ..Default::default()
            },
        )
    }
}
struct SnapshotHook(Arc<Mutex<Vec<serde_json::Value>>>);
#[async_trait::async_trait]
impl Hook for SnapshotHook {
    async fn generation(
        &self,
        event: GenerationEvent,
        _: &serde_json::Value,
        context: &Context<'_>,
    ) -> Result<GenerationDecision, String> {
        if event == GenerationEvent::BeforeRequest {
            self.0
                .lock()
                .unwrap()
                .push(context.snapshot["tool_filters"]["callers"].clone());
        }
        Ok(GenerationDecision::Pass)
    }
}
fn options(
    model: Arc<TestModel>,
    target: &'static str,
    seen: Arc<Mutex<Vec<String>>>,
    effects: Arc<AtomicUsize>,
) -> Options {
    let mut tools: Vec<RegisteredTool> = ["hidden", "modelonly", "neither"]
        .into_iter()
        .map(|name| RegisteredTool {
            tool: Arc::new(Child(name, effects.clone())),
            replay: ReplayPolicy::Safe,
        })
        .collect();
    tools.push(
        Arc::new(KeyedToolAdapter::new(Parent {
            target,
            seen,
            add: true,
        }))
        .register(ReplayPolicy::Safe),
    );
    Options {
        model,
        tools,
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 64,
    }
}
fn model(first: &'static str) -> Arc<TestModel> {
    Arc::new(TestModel {
        first,
        requests: Mutex::new(vec![]),
        attempts: AtomicUsize::new(0),
    })
}
fn configure(h: &mut Harness, id: uuid::Uuid) {
    h.set_tool_callers(id, "parent".into(), Some(ToolCallers::ModelOnly))
        .unwrap();
    h.set_tool_callers(id, "modelonly".into(), Some(ToolCallers::ModelOnly))
        .unwrap();
    h.set_tool_callers(id, "hidden".into(), Some(ToolCallers::ToolsOnly))
        .unwrap();
    h.set_tool_callers(id, "neither".into(), Some(ToolCallers::Neither))
        .unwrap();
}

#[tokio::test]
async fn caller_permissions_filter_model_and_nested_context_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let m = model("parent");
    let seen = Arc::new(Mutex::new(vec![]));
    let effects = Arc::new(AtomicUsize::new(0));
    let snapshots = Arc::new(Mutex::new(vec![]));
    let id;
    {
        let mut h = Harness::open_paused(
            &path,
            options(m.clone(), "hidden", seen.clone(), effects.clone()),
        )
        .unwrap();
        h.install_extension(Extension {
            name: "observe".into(),
            hooks: vec![Arc::new(SnapshotHook(snapshots.clone()))],
            ..Default::default()
        })
        .unwrap();
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["observe".into()]).unwrap();
        configure(&mut h, id);
        h.set_deferred_tools(id, vec!["hidden".into()]).unwrap();
        h.select_model_tools(
            id,
            Some(vec![
                "hidden".into(),
                "parent".into(),
                "modelonly".into(),
                "neither".into(),
            ]),
        )
        .unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            *m.requests.lock().unwrap(),
            vec![vec!["parent", "modelonly"], vec!["parent", "modelonly"]]
        );
        assert_eq!(*seen.lock().unwrap(), vec!["hidden"]);
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert_eq!(
            snapshots.lock().unwrap()[0]["hidden"],
            serde_json::json!("tools_only")
        );
        assert!(
            h.conversation(id)
                .unwrap()
                .tool_filters
                .model_add
                .is_empty()
        );
        let fork = h.fork(id).unwrap();
        assert_eq!(
            h.conversation(fork).unwrap().tool_filters.callers,
            h.conversation(id).unwrap().tool_filters.callers
        );
    }
    let mut h = Harness::open_paused(
        &path,
        options(m.clone(), "hidden", seen.clone(), effects.clone()),
    )
    .unwrap();
    h.install_extension(Extension {
        name: "observe".into(),
        hooks: vec![Arc::new(SnapshotHook(snapshots.clone()))],
        ..Default::default()
    })
    .unwrap();
    h.submit_and_run(id, None, "again".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        m.requests.lock().unwrap().last().unwrap(),
        &vec!["parent", "modelonly"]
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    h.select_model_tools_patch(id, vec![], vec!["hidden".into()])
        .unwrap();
    h.submit_and_run(
        id,
        None,
        "cannot load hidden".into(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        m.requests.lock().unwrap().last().unwrap(),
        &vec!["modelonly", "parent"]
    );
    h.select_model_tools(
        id,
        Some(vec!["hidden".into(), "parent".into(), "modelonly".into()]),
    )
    .unwrap();
    h.set_tool_callers(id, "hidden".into(), None).unwrap();
    h.submit_and_run(
        id,
        None,
        "default restored".into(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        m.requests.lock().unwrap().last().unwrap(),
        &vec!["hidden", "parent", "modelonly"]
    );
}

#[tokio::test]
async fn unoffered_model_call_cannot_bypass_caller_policy() {
    let dir = tempfile::tempdir().unwrap();
    let m = model("hidden");
    let seen = Arc::new(Mutex::new(vec![]));
    let effects = Arc::new(AtomicUsize::new(0));
    let mut h = Harness::open_paused(
        dir.path().join("db"),
        options(m.clone(), "hidden", seen, effects.clone()),
    )
    .unwrap();
    let id = h.create_conversation(config()).unwrap();
    configure(&mut h, id);
    h.select_model_tools(id, Some(vec!["hidden".into(), "parent".into()]))
        .unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(m.requests.lock().unwrap()[0], vec!["parent"]);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(h.conversation(id).unwrap().messages.iter().any(|message| matches!(message, Message::Tool { content, .. } if content.contains("unavailable"))));
}

#[tokio::test]
async fn denied_nested_call_never_executes_and_requires_reconciliation() {
    let dir = tempfile::tempdir().unwrap();
    let m = model("parent");
    let seen = Arc::new(Mutex::new(vec![]));
    let effects = Arc::new(AtomicUsize::new(0));
    let mut h = Harness::open_paused(
        dir.path().join("db"),
        options(m, "modelonly", seen.clone(), effects.clone()),
    )
    .unwrap();
    let id = h.create_conversation(config()).unwrap();
    configure(&mut h, id);
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), vec!["hidden"]);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(h.blocked(id).unwrap().is_some());
}

#[test]
fn invalid_caller_selection_is_rejected_without_changing_state() {
    let dir = tempfile::tempdir().unwrap();
    let m = model("parent");
    let mut h = Harness::open_paused(
        dir.path().join("db"),
        options(
            m,
            "hidden",
            Arc::new(Mutex::new(vec![])),
            Arc::new(AtomicUsize::new(0)),
        ),
    )
    .unwrap();
    let id = h.create_conversation(config()).unwrap();
    assert!(
        h.set_tool_callers(id, "".into(), Some(ToolCallers::Both))
            .is_err()
    );
    assert!(
        h.set_tool_callers(id, "missing".into(), Some(ToolCallers::Both))
            .is_err()
    );
    assert!(serde_json::from_str::<ToolCallers>("\"root\"").is_err());
    assert!(h.conversation(id).unwrap().tool_filters.callers.is_empty());
    h.submit(id, None, "queued".into()).unwrap();
    assert!(
        h.set_tool_callers(id, "hidden".into(), Some(ToolCallers::ToolsOnly))
            .is_err()
    );
    assert!(h.conversation(id).unwrap().tool_filters.callers.is_empty());
}

#[tokio::test]
async fn scheduler_enforces_persisted_caller_policy_after_reopen() {
    use pi_rs_durable::runtime::Scheduler;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let m = model("parent");
    let seen = Arc::new(Mutex::new(vec![]));
    let effects = Arc::new(AtomicUsize::new(0));
    let id;
    {
        let mut scheduler = Scheduler::open(
            &path,
            options(m.clone(), "hidden", seen.clone(), effects.clone()),
        )
        .unwrap();
        id = scheduler.create_conversation(config()).unwrap();
        scheduler
            .set_tool_callers(id, "parent".into(), Some(ToolCallers::ModelOnly))
            .unwrap();
        scheduler
            .set_tool_callers(id, "hidden".into(), Some(ToolCallers::ToolsOnly))
            .unwrap();
        scheduler.submit(id, None, "go".into()).unwrap();
        scheduler.wait(id).await.unwrap();
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert!(!m.requests.lock().unwrap()[0].contains(&"hidden".into()));
        scheduler.shutdown().await.unwrap();
    }
    let mut scheduler =
        Scheduler::open(&path, options(m.clone(), "hidden", seen, effects.clone())).unwrap();
    scheduler.submit(id, None, "again".into()).unwrap();
    scheduler.wait(id).await.unwrap();
    assert!(
        !m.requests
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .contains(&"hidden".into())
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    scheduler.shutdown().await.unwrap();
}
