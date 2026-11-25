use futures_util::{StreamExt, stream};
use pi_rs_core::{
    CancellationToken, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool,
    ToolCall, ToolContext, ToolSpec,
};
use pi_rs_durable::{
    extension::{Context, Extension, GenerationDecision, GenerationEvent, Hook},
    keyed_tool::{KeyedHost, KeyedTool, KeyedToolAdapter},
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, Harness, Options, RegisteredTool, Scheduler, ToolCallers},
    tool_search,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct Script {
    requests: Mutex<Vec<Vec<String>>>,
    steps: Mutex<Vec<(String, String)>>,
}
#[async_trait::async_trait]
impl Model for Script {
    async fn stream(&self, request: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        self.requests
            .lock()
            .unwrap()
            .push(request.tools.iter().map(|s| s.name.clone()).collect());
        let call = self
            .steps
            .lock()
            .unwrap()
            .pop()
            .map(|(name, arguments)| ToolCall {
                id: uuid::Uuid::new_v4().to_string(),
                name,
                arguments,
            });
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete {
                calls: call.into_iter().collect(),
                usage: None,
            }),
        ])
        .boxed())
    }
}
struct Candidate {
    name: &'static str,
    description: &'static str,
    effects: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl Tool for Candidate {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name.into(),
            description: self.description.into(),
            parameters: serde_json::json!({"type":"object","properties":{"image":{"$ref":"#/$defs/image"}},"$defs":{"image":{"description":"apricot screenshot image"}}}),
        }
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        self.effects.fetch_add(1, Ordering::SeqCst);
        Outcome::Succeeded("candidate ran".into())
    }
}
fn model(steps: Vec<(&str, &str)>) -> Arc<Script> {
    Arc::new(Script {
        requests: Mutex::new(vec![]),
        steps: Mutex::new(
            steps
                .into_iter()
                .rev()
                .map(|(a, b)| (a.into(), b.into()))
                .collect(),
        ),
    })
}
fn options(model: Arc<Script>, effects: Arc<AtomicUsize>) -> Options {
    Options {
        model,
        tools: [
            ("screen", "capture window screenshot"),
            ("hidden", "secret screenshot"),
            ("removed", "removed screenshot"),
            ("file", "transfer document"),
        ]
        .into_iter()
        .map(|(name, description)| RegisteredTool {
            tool: Arc::new(Candidate {
                name,
                description,
                effects: effects.clone(),
            }),
            replay: ReplayPolicy::Safe,
        })
        .collect(),
        restart_limit: 3,
        max_turns: 6,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 128,
    }
}
struct Observe {
    before: Arc<Mutex<Vec<serde_json::Value>>>,
    after: Arc<Mutex<Vec<String>>>,
    cancel: Option<CancellationToken>,
}
#[async_trait::async_trait]
impl Hook for Observe {
    async fn generation(
        &self,
        event: GenerationEvent,
        _: &serde_json::Value,
        c: &Context<'_>,
    ) -> Result<GenerationDecision, String> {
        if event == GenerationEvent::BeforeRequest {
            self.before
                .lock()
                .unwrap()
                .push(c.snapshot["tool_filters"].clone());
        }
        Ok(GenerationDecision::Pass)
    }
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &serde_json::Value,
        _: &Context<'_>,
    ) -> Result<Option<serde_json::Value>, String> {
        if call.name == "tool_search" {
            self.after
                .lock()
                .unwrap()
                .push(result.as_str().unwrap_or_default().into());
            if let Some(cancel) = &self.cancel {
                cancel.cancel();
            }
        }
        Ok(None)
    }
}
fn install(
    h: &Harness,
    before: Arc<Mutex<Vec<serde_json::Value>>>,
    after: Arc<Mutex<Vec<String>>>,
    cancel: Option<CancellationToken>,
) {
    h.install_extension(tool_search::extension()).unwrap();
    h.install_extension(Extension {
        name: "observe".into(),
        hooks: vec![Arc::new(Observe {
            before,
            after,
            cancel,
        })],
        ..Default::default()
    })
    .unwrap();
}
fn select(h: &mut Harness, id: uuid::Uuid) {
    h.select_extensions(id, vec!["tool-search".into(), "observe".into()])
        .unwrap();
    h.set_deferred_tools(
        id,
        vec![
            "screen".into(),
            "hidden".into(),
            "removed".into(),
            "file".into(),
        ],
    )
    .unwrap();
    h.set_tool_callers(id, "hidden".into(), Some(ToolCallers::ToolsOnly))
        .unwrap();
    h.select_tools_except(id, vec!["removed".into()]).unwrap();
    h.select_model_tools(id, Some(vec!["tool_search".into()]))
        .unwrap();
}
#[tokio::test]
async fn search_loads_only_permitted_selected_candidates_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let m = model(vec![
        ("tool_search", r#"{"query":"screenshot","limit":1}"#),
        ("screen", "{}"),
    ]);
    let effects = Arc::new(AtomicUsize::new(0));
    let before = Arc::new(Mutex::new(vec![]));
    let after = Arc::new(Mutex::new(vec![]));
    let id;
    {
        let mut h = Harness::open_paused(&path, options(m.clone(), effects.clone())).unwrap();
        install(&h, before.clone(), after.clone(), None);
        id = h.create_conversation(config()).unwrap();
        select(&mut h, id);
        h.submit_and_run(
            id,
            None,
            "find screenshot".into(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            *m.requests.lock().unwrap(),
            vec![
                vec!["tool_search"],
                vec!["tool_search", "screen"],
                vec!["tool_search", "screen"]
            ]
        );
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert!(after.lock().unwrap()[0].contains("screen"));
        assert!(!after.lock().unwrap()[0].contains("hidden"));
        assert!(!after.lock().unwrap()[0].contains("removed"));
        assert_eq!(
            before.lock().unwrap()[0]["model_add"],
            serde_json::json!([])
        );
        assert_eq!(
            h.conversation(id).unwrap().model_tools,
            Some(vec!["tool_search".into(), "screen".into()])
        );
    }
    let mut h = Harness::open_paused(&path, options(m.clone(), effects.clone())).unwrap();
    install(&h, before, after, None);
    h.submit_and_run(id, None, "again".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        m.requests.lock().unwrap().last().unwrap(),
        &vec!["tool_search", "screen"]
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn search_control_is_durable_before_after_tool_hook_and_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let m = model(vec![("tool_search", r#"{"query":"apricot"}"#)]);
    let effects = Arc::new(AtomicUsize::new(0));
    let before = Arc::new(Mutex::new(vec![]));
    let after = Arc::new(Mutex::new(vec![]));
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open_paused(&path, options(m.clone(), effects.clone())).unwrap();
        install(&h, before.clone(), after.clone(), Some(cancel.clone()));
        id = h.create_conversation(config()).unwrap();
        select(&mut h, id);
        assert!(
            h.submit_and_run(id, None, "go".into(), &cancel)
                .await
                .is_err()
        );
        assert_eq!(m.requests.lock().unwrap().len(), 1);
    }
    let mut h = Harness::open_paused(&path, options(m.clone(), effects)).unwrap();
    install(&h, before, after.clone(), None);
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(m.requests.lock().unwrap().len(), 2);
    assert_eq!(
        m.requests.lock().unwrap()[1],
        vec!["tool_search", "file", "screen"]
    );
    assert_eq!(
        h.conversation(id).unwrap().model_tools,
        Some(vec!["tool_search".into(), "file".into(), "screen".into()])
    );
}
#[tokio::test]
async fn invalid_and_empty_searches_do_not_load_any_tool() {
    for arguments in [
        r#"{"query":" "}"#,
        r#"{"query":"screenshot","limit":0}"#,
        r#"{"query":"screenshot","limit":129}"#,
        r#"{"query":"unmatched"}"#,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let m = model(vec![("tool_search", arguments)]);
        let effects = Arc::new(AtomicUsize::new(0));
        let mut h =
            Harness::open_paused(dir.path().join("db"), options(m.clone(), effects)).unwrap();
        install(&h, Arc::default(), Arc::default(), None);
        let id = h.create_conversation(config()).unwrap();
        select(&mut h, id);
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            h.conversation(id).unwrap().model_tools,
            Some(vec!["tool_search".into()])
        );
        assert_eq!(
            m.requests.lock().unwrap().last().unwrap(),
            &vec!["tool_search"]
        );
    }
}

#[tokio::test]
async fn search_tool_respects_model_caller_restriction() {
    let dir = tempfile::tempdir().unwrap();
    let m = model(vec![("tool_search", r#"{"query":"screenshot"}"#)]);
    let mut h =
        Harness::open_paused(dir.path().join("db"), options(m.clone(), Arc::default())).unwrap();
    install(&h, Arc::default(), Arc::default(), None);
    let id = h.create_conversation(config()).unwrap();
    select(&mut h, id);
    h.set_tool_callers(id, "tool_search".into(), Some(ToolCallers::ToolsOnly))
        .unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert!(m.requests.lock().unwrap()[0].is_empty());
    assert_eq!(
        h.conversation(id).unwrap().model_tools,
        Some(vec!["tool_search".into()])
    );
    assert!(h.conversation(id).unwrap().messages.iter().any(|message| matches!(message, Message::Tool { content, .. } if content.contains("unavailable"))));
}
struct Probe(Arc<Mutex<Vec<String>>>);
#[async_trait::async_trait]
impl KeyedTool for Probe {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "probe".into(),
            description: String::new(),
            parameters: serde_json::json!({"type":"object"}),
        }
    }
    async fn invoke(
        &self,
        _: serde_json::Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        host: KeyedHost,
    ) -> pi_rs_core::ToolReply {
        *self.0.lock().unwrap() = host
            .ordinary()
            .tools()
            .iter()
            .map(|tool| tool.name.clone())
            .collect();
        let _ = host
            .call_keyed(
                "search",
                "tool_search",
                serde_json::json!({"query":"screenshot"}),
            )
            .await;
        Outcome::Succeeded("probe".into()).into()
    }
}
#[tokio::test]
async fn search_is_never_exposed_to_nested_callers() {
    let dir = tempfile::tempdir().unwrap();
    let m = model(vec![("probe", "{}")]);
    let visible = Arc::new(Mutex::new(vec![]));
    let mut opts = options(m, Arc::default());
    opts.tools
        .push(Arc::new(KeyedToolAdapter::new(Probe(visible.clone()))).register(ReplayPolicy::Safe));
    let mut h = Harness::open_paused(dir.path().join("db"), opts).unwrap();
    install(&h, Arc::default(), Arc::default(), None);
    let id = h.create_conversation(config()).unwrap();
    select(&mut h, id);
    h.select_model_tools(id, Some(vec!["probe".into(), "tool_search".into()]))
        .unwrap();
    h.set_tool_callers(id, "tool_search".into(), Some(ToolCallers::Both))
        .unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert!(!visible.lock().unwrap().contains(&"tool_search".into()));
    assert!(h.blocked(id).unwrap().is_some());
    assert_eq!(
        h.conversation(id).unwrap().model_tools,
        Some(vec!["probe".into(), "tool_search".into()])
    );
}
#[tokio::test]
async fn scheduler_search_loads_tool_for_next_model_request() {
    let dir = tempfile::tempdir().unwrap();
    let m = model(vec![("tool_search", r#"{"query":"screen","limit":1}"#)]);
    let effects = Arc::new(AtomicUsize::new(0));
    let mut s = Scheduler::open(dir.path().join("db"), options(m.clone(), effects)).unwrap();
    s.install_extension(tool_search::extension()).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.select_extensions(id, vec!["tool-search".into()]).unwrap();
    s.set_deferred_tools(
        id,
        vec![
            "screen".into(),
            "hidden".into(),
            "removed".into(),
            "file".into(),
        ],
    )
    .unwrap();
    s.select_model_tools(id, Some(vec!["tool_search".into()]))
        .unwrap();
    s.submit(id, None, "go".into()).unwrap();
    s.wait(id).await.unwrap();
    assert_eq!(m.requests.lock().unwrap()[0], vec!["tool_search"]);
    assert_eq!(m.requests.lock().unwrap()[1], vec!["tool_search", "screen"]);
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn search_can_load_model_removed_deferred_tool_through_patch() {
    let dir = tempfile::tempdir().unwrap();
    let m = model(vec![("tool_search", r#"{"query":"screen"}"#)]);
    let mut h =
        Harness::open_paused(dir.path().join("db"), options(m.clone(), Arc::default())).unwrap();
    install(&h, Arc::default(), Arc::default(), None);
    let id = h.create_conversation(config()).unwrap();
    select(&mut h, id);
    h.select_model_tools_patch(id, vec!["screen".into()], vec![])
        .unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(m.requests.lock().unwrap()[0], vec!["tool_search"]);
    assert_eq!(m.requests.lock().unwrap()[1], vec!["tool_search", "screen"]);
    let c = h.conversation(id).unwrap();
    assert!(c.tool_filters.model_excluded.is_empty());
    assert_eq!(c.tool_filters.model_add, vec!["screen"]);
}

#[tokio::test]
async fn integer_valued_numeric_limit_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let m = model(vec![("tool_search", r#"{"query":"screen","limit":1.0}"#)]);
    let mut h =
        Harness::open_paused(dir.path().join("db"), options(m.clone(), Arc::default())).unwrap();
    install(&h, Arc::default(), Arc::default(), None);
    let id = h.create_conversation(config()).unwrap();
    select(&mut h, id);
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(m.requests.lock().unwrap()[1], vec!["tool_search", "screen"]);
}
