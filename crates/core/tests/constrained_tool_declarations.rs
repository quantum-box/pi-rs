use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use pi_rs_core::{
    ContextChange, ContextRequest, DetailedModelEvent, DetailedModelStream,
    EffectiveToolDeclaration, EffectiveToolSampling, Event, Extension, ExtensionResult,
    ExtensionState, Model, ModelEvent, ModelRequest, ModelStream, Outcome, PreparedToolDispatch,
    RecordedToolDecision, Registry, Result, SessionBuilder, StrictToolPolicy, Tool, ToolConstraint,
    ToolContext, ToolDecisionSnapshot, ToolDeclaration, ToolDispatchContext, ToolSpec,
};
use serde_json::{Value, json};

struct ConstrainedTool(StrictToolPolicy);
#[async_trait::async_trait]
impl Tool for ConstrainedTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "constrained".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    fn constrained_sampling(&self) -> Option<ToolConstraint> {
        Some(ToolConstraint::JsonSchema { strict: self.0 })
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Succeeded(String::new())
    }
}
struct ConstrainedExtension(StrictToolPolicy);
impl Extension for ConstrainedExtension {
    fn id(&self) -> &str {
        "constrained"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(ConstrainedTool(self.0))]
    }
}

struct ChangeTool(bool);
impl Extension for ChangeTool {
    fn id(&self) -> &str {
        "change-tool"
    }
    fn context(
        &self,
        request: &mut ContextRequest<'_>,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<ContextChange> {
        if self.0 {
            request.tools_mut().clear();
        } else {
            request.tools_mut()[0].parameters =
                json!({"type":"object", "properties":{"x":{"type":"string"}}});
        }
        Ok(ContextChange::Changed)
    }
}
struct CountingModel(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl Model for CountingModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(Box::pin(futures_util::stream::iter(vec![
            Ok(ModelEvent::Text("done".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])))
    }
}

struct PreparedFallbackModel(Arc<AtomicUsize>, Option<Arc<AtomicBool>>);
struct FallbackDispatch {
    calls: Arc<AtomicUsize>,
    committed: Option<Arc<AtomicBool>>,
    request: ModelRequest,
    snapshot: ToolDecisionSnapshot,
    effective: Vec<EffectiveToolDeclaration>,
}
#[async_trait::async_trait]
impl PreparedToolDispatch for FallbackDispatch {
    fn effective_tools(&self) -> &[EffectiveToolDeclaration] {
        &self.effective
    }
    fn decision_snapshot(&self) -> &ToolDecisionSnapshot {
        &self.snapshot
    }
    fn dispatch_request(&self) -> &ModelRequest {
        &self.request
    }
    fn reservation_request(&self) -> &ModelRequest {
        &self.request
    }
    async fn dispatch(self: Box<Self>) -> Result<DetailedModelStream> {
        if let Some(committed) = &self.committed {
            assert!(
                committed.load(Ordering::SeqCst),
                "dispatch before record commit"
            );
        }
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(Box::pin(futures_util::stream::iter(vec![
            Ok(DetailedModelEvent::from(
                pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Text("done".into())),
            )),
            Ok(DetailedModelEvent::from(
                pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            )),
        ])))
    }
}
#[async_trait::async_trait]
impl Model for PreparedFallbackModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        panic!("prepared dispatch must be used")
    }
    async fn prepare_tool_dispatch<'a>(
        &'a self,
        request: &ModelRequest,
        declarations: &[ToolDeclaration],
        context: &ToolDispatchContext,
    ) -> Result<Option<Box<dyn PreparedToolDispatch + 'a>>> {
        let effective = declarations
            .iter()
            .map(|declaration| EffectiveToolDeclaration {
                spec: declaration.spec.clone(),
                sampling: EffectiveToolSampling::Unconstrained,
            })
            .collect::<Vec<_>>();
        let snapshot = ToolDecisionSnapshot::capture(context, request, declarations, &effective)
            .map_err(|error| pi_rs_core::Error::Config(error.to_string()))?;
        Ok(Some(Box::new(FallbackDispatch {
            calls: self.0.clone(),
            committed: self.1.clone(),
            request: request.clone(),
            snapshot,
            effective,
        })))
    }
}

#[tokio::test]
async fn preferred_prepared_fallback_is_dispatched_and_journaled() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(ConstrainedExtension(StrictToolPolicy::Prefer)))
        .unwrap();
    let mut session = SessionBuilder::new(
        Arc::new(PreparedFallbackModel(calls.clone(), None)),
        "fixture",
    )
    .registry(registry)
    .in_memory()
    .unwrap();
    assert_eq!(session.prompt("test").await.unwrap(), "done");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    let journal = session.journal().unwrap();
    let request_id = journal
        .records()
        .iter()
        .find_map(|record| match &record.event {
            Event::ModelRequested { request_id, .. } => *request_id,
            _ => None,
        })
        .unwrap();
    assert!(matches!(
        journal.active_tool_decision(request_id).unwrap(),
        RecordedToolDecision::Current(_)
    ));
}

#[tokio::test]
async fn prepared_dispatch_waits_for_atomic_decision_commit() {
    use pi_rs_core::{
        Agent, CancellationToken, Limits, MemoryStore, Record, Session, SessionStore,
    };

    struct ObservedStore {
        inner: MemoryStore,
        committed: Arc<AtomicBool>,
    }
    impl SessionStore for ObservedStore {
        fn load(&mut self) -> Result<Vec<Record>> {
            self.inner.load()
        }
        fn append(&mut self, record: &Record) -> Result<()> {
            self.inner.append(record)
        }
        fn supports_record_metadata(&self) -> bool {
            true
        }
        fn load_record_metadata(&mut self) -> Result<std::collections::BTreeMap<u64, Value>> {
            self.inner.load_record_metadata()
        }
        fn append_with_metadata(
            &mut self,
            record: &Record,
            metadata: Option<&Value>,
        ) -> Result<()> {
            self.inner.append_with_metadata(record, metadata)?;
            if metadata.is_some_and(|value| value.get("toolDecision").is_some()) {
                self.committed.store(true, Ordering::SeqCst);
            }
            Ok(())
        }
    }
    let committed = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let model = PreparedFallbackModel(calls.clone(), Some(committed.clone()));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(ConstrainedExtension(StrictToolPolicy::Prefer)))
        .unwrap();
    let mut session = Session::with_store(ObservedStore {
        inner: MemoryStore::default(),
        committed,
    })
    .unwrap();
    let agent = Agent {
        model: &model,
        model_name: "fixture",
        registry: &registry,
        system_prompt: "",
        limits: Limits::default(),
    };
    let (_sender, mut receiver) = tokio::sync::mpsc::channel(1);
    assert_eq!(
        agent
            .run(
                &mut session,
                Some("test".into()),
                &mut receiver,
                &CancellationToken::new(),
                &mut |_| {}
            )
            .await
            .unwrap(),
        "done"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn required_constraint_fails_before_legacy_model_dispatch() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(ConstrainedExtension(StrictToolPolicy::Require)))
        .unwrap();
    let mut session = SessionBuilder::new(Arc::new(CountingModel(calls.clone())), "fixture")
        .registry(registry)
        .in_memory()
        .unwrap();
    let error = session.prompt("test").await.unwrap_err().to_string();
    assert!(
        error.contains("requires JSON-schema constrained sampling"),
        "{error}"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn preferred_constraint_allows_legacy_fallback() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(ConstrainedExtension(StrictToolPolicy::Prefer)))
        .unwrap();
    let mut session = SessionBuilder::new(Arc::new(CountingModel(calls.clone())), "fixture")
        .registry(registry)
        .in_memory()
        .unwrap();
    assert_eq!(session.prompt("test").await.unwrap(), "done");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn required_constraint_uses_post_hook_tool_list_and_identity() {
    for remove in [true, false] {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::default();
        registry
            .register(Arc::new(ConstrainedExtension(StrictToolPolicy::Require)))
            .unwrap();
        registry.register(Arc::new(ChangeTool(remove))).unwrap();
        let mut session = SessionBuilder::new(Arc::new(CountingModel(calls.clone())), "fixture")
            .registry(registry)
            .in_memory()
            .unwrap();
        let result = session.prompt("test").await;
        if remove {
            assert_eq!(result.unwrap(), "done");
            assert_eq!(calls.load(Ordering::Relaxed), 1);
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("requires JSON-schema")
            );
            assert_eq!(calls.load(Ordering::Relaxed), 0);
        }
    }
}

struct StrictTool(Arc<std::sync::Mutex<Option<Value>>>);
#[async_trait::async_trait]
impl Tool for StrictTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "strict_read".into(),
            description: String::new(),
            parameters: json!({"type":"object","properties":{"path":{"type":"string"},"limit":{"type":"integer"}},"required":["path"]}),
        }
    }
    fn constrained_sampling(&self) -> Option<ToolConstraint> {
        Some(ToolConstraint::JsonSchema {
            strict: StrictToolPolicy::Require,
        })
    }
    async fn invoke(&self, arguments: Value, _: ToolContext) -> Outcome {
        *self.0.lock().unwrap() = Some(arguments);
        Outcome::Succeeded("ok".into())
    }
}
struct StrictExtension(Arc<std::sync::Mutex<Option<Value>>>);
impl Extension for StrictExtension {
    fn id(&self) -> &str {
        "strict"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(StrictTool(self.0.clone()))]
    }
}
struct StrictModel(Arc<AtomicUsize>);
struct StrictDispatch {
    calls: Arc<AtomicUsize>,
    request: ModelRequest,
    snapshot: ToolDecisionSnapshot,
    effective: Vec<EffectiveToolDeclaration>,
}
#[async_trait::async_trait]
impl PreparedToolDispatch for StrictDispatch {
    fn effective_tools(&self) -> &[EffectiveToolDeclaration] {
        &self.effective
    }
    fn decision_snapshot(&self) -> &ToolDecisionSnapshot {
        &self.snapshot
    }
    fn dispatch_request(&self) -> &ModelRequest {
        &self.request
    }
    fn reservation_request(&self) -> &ModelRequest {
        &self.request
    }
    async fn dispatch(self: Box<Self>) -> Result<DetailedModelStream> {
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        let calls = if first {
            vec![pi_rs_core::ToolCall {
                id: "call".into(),
                name: "strict_read".into(),
                arguments: r#"{"path":"file","limit":null}"#.into(),
            }]
        } else {
            vec![]
        };
        let mut events = Vec::new();
        if !first {
            events.push(Ok(DetailedModelEvent::from(
                pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Text("done".into())),
            )));
        }
        events.push(Ok(DetailedModelEvent::from(
            pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Complete { calls, usage: None }),
        )));
        Ok(Box::pin(futures_util::stream::iter(events)))
    }
}
#[async_trait::async_trait]
impl Model for StrictModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        panic!("strict dispatch must be prepared")
    }
    async fn prepare_tool_dispatch<'a>(
        &'a self,
        request: &ModelRequest,
        declarations: &[ToolDeclaration],
        context: &ToolDispatchContext,
    ) -> Result<Option<Box<dyn PreparedToolDispatch + 'a>>> {
        let effective = declarations
            .iter()
            .map(|declaration| EffectiveToolDeclaration {
                spec: declaration.spec.clone(),
                sampling: EffectiveToolSampling::StrictJsonSchema {
                    original_schema: declaration.spec.parameters.clone(),
                    normalization_version: pi_rs_core::STRICT_TOOL_NORMALIZATION_VERSION,
                },
            })
            .collect::<Vec<_>>();
        let snapshot = ToolDecisionSnapshot::capture(context, request, declarations, &effective)
            .map_err(|error| pi_rs_core::Error::Config(error.to_string()))?;
        Ok(Some(Box::new(StrictDispatch {
            calls: self.0.clone(),
            request: request.clone(),
            snapshot,
            effective,
        })))
    }
}

#[tokio::test]
async fn strict_tool_execution_records_decision_and_normalizes_a_copy() {
    let captured = Arc::new(std::sync::Mutex::new(None));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(StrictExtension(captured.clone())))
        .unwrap();
    let mut session = SessionBuilder::new(Arc::new(StrictModel(calls.clone())), "fixture")
        .registry(registry)
        .in_memory()
        .unwrap();
    assert_eq!(session.prompt("read").await.unwrap(), "done");
    assert_eq!(*captured.lock().unwrap(), Some(json!({"path":"file"})));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let journal = session.journal().unwrap();
    assert!(journal.records().iter().any(|record| matches!(
        &record.event,
        Event::ToolStarted {
            tool_execution: Some(_),
            ..
        }
    )));
    let raw = journal
        .records()
        .iter()
        .find_map(|record| match &record.event {
            Event::Assistant { invocations, .. } => invocations.first(),
            _ => None,
        })
        .unwrap();
    assert!(raw.call.arguments.contains("null"));
}

struct StrictPendingTool(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl Tool for StrictPendingTool {
    fn spec(&self) -> ToolSpec {
        StrictTool(Arc::new(std::sync::Mutex::new(None))).spec()
    }
    fn constrained_sampling(&self) -> Option<ToolConstraint> {
        Some(ToolConstraint::JsonSchema {
            strict: StrictToolPolicy::Require,
        })
    }
    fn can_poll(&self) -> bool {
        true
    }
    fn recovery_identity(&self) -> Option<String> {
        Some("strict-pending-backend-v1".into())
    }
    async fn invoke(&self, arguments: Value, _: ToolContext) -> Outcome {
        assert_eq!(arguments, json!({"path":"authorized"}));
        Outcome::Pending {
            reference: "job".into(),
        }
    }
    async fn poll(&self, reference: &str, _: ToolContext) -> Option<Outcome> {
        assert_eq!(reference, "job");
        self.0.fetch_add(1, Ordering::SeqCst);
        Some(Outcome::Succeeded("ok".into()))
    }
}
struct StrictPendingExtension(Arc<AtomicUsize>);
impl Extension for StrictPendingExtension {
    fn id(&self) -> &str {
        "strict-pending"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(StrictPendingTool(self.0.clone()))]
    }
}
struct AuthorizeAndObserve(Arc<std::sync::Mutex<Option<Value>>>);
#[async_trait::async_trait]
impl Extension for AuthorizeAndObserve {
    fn id(&self) -> &str {
        "authorize"
    }
    fn handles_tool_call(&self) -> bool {
        true
    }
    async fn tool_call(
        &self,
        _: &str,
        args: &mut Value,
        _: &ToolContext,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<pi_rs_core::Decision> {
        assert_eq!(*args, json!({"path":"file"}));
        args["path"] = json!("authorized");
        Ok(pi_rs_core::Decision::Allow)
    }
    async fn before_tool(
        &self,
        _: &str,
        args: &Value,
        _: &ToolContext,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<pi_rs_core::Decision> {
        *self.0.lock().unwrap() = Some(args.clone());
        Ok(pi_rs_core::Decision::Allow)
    }
}

#[tokio::test]
async fn strict_pending_poll_uses_post_hook_authorized_arguments() {
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(std::sync::Mutex::new(None));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(StrictPendingExtension(polls.clone())))
        .unwrap();
    registry
        .register(Arc::new(AuthorizeAndObserve(observed.clone())))
        .unwrap();
    let mut session = SessionBuilder::new(
        Arc::new(StrictModel(Arc::new(AtomicUsize::new(0)))),
        "fixture",
    )
    .registry(registry)
    .in_memory()
    .unwrap();
    assert_eq!(session.prompt("read").await.unwrap(), "done");
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(
        *observed.lock().unwrap(),
        Some(json!({"path":"authorized"}))
    );
}

struct RestartTool {
    polls: AtomicUsize,
    preparations: AtomicUsize,
    started: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl Tool for RestartTool {
    fn spec(&self) -> ToolSpec {
        StrictTool(Arc::new(std::sync::Mutex::new(None))).spec()
    }
    fn constrained_sampling(&self) -> Option<ToolConstraint> {
        Some(ToolConstraint::JsonSchema {
            strict: StrictToolPolicy::Require,
        })
    }
    fn can_poll(&self) -> bool {
        true
    }
    fn recovery_identity(&self) -> Option<String> {
        Some("restart-backend-v1".into())
    }
    async fn prepare_arguments(&self, args: Value) -> ExtensionResult<Value> {
        self.preparations.fetch_add(1, Ordering::SeqCst);
        Ok(args)
    }
    async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
        assert_eq!(args, json!({"path":"authorized"}));
        Outcome::Pending {
            reference: "job".into(),
        }
    }
    async fn poll(&self, _: &str, context: ToolContext) -> Option<Outcome> {
        if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.started.notify_one();
            context.cancellation.cancelled().await;
            None
        } else {
            Some(Outcome::Succeeded("ok".into()))
        }
    }
}
struct RestartExtension(Arc<RestartTool>);
impl Extension for RestartExtension {
    fn id(&self) -> &str {
        "restart"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![self.0.clone()]
    }
}

#[tokio::test]
async fn strict_pending_reopens_with_original_authorized_arguments() {
    let tool = Arc::new(RestartTool {
        polls: AtomicUsize::new(0),
        preparations: AtomicUsize::new(0),
        started: tokio::sync::Notify::new(),
    });
    let observed = Arc::new(std::sync::Mutex::new(None));
    let model = Arc::new(StrictModel(Arc::new(AtomicUsize::new(0))));
    let make_registry = || {
        let mut registry = Registry::default();
        registry
            .register(Arc::new(RestartExtension(tool.clone())))
            .unwrap();
        registry
            .register(Arc::new(AuthorizeAndObserve(observed.clone())))
            .unwrap();
        registry
    };
    let mut session = SessionBuilder::new(model.clone(), "fixture")
        .registry(make_registry())
        .in_memory()
        .unwrap();
    let control = session.controls();
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(session.prompt("read"), async {
            tool.started.notified().await;
            control.abort();
        })
    })
    .await
    .unwrap();
    assert!(matches!(result, Err(pi_rs_core::Error::Cancelled)));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("strict-pending.jsonl");
    session.export_jsonl(&path).unwrap();
    session.close().await.unwrap();
    drop(session);
    *observed.lock().unwrap() = None;
    let mut session = SessionBuilder::new(model, "fixture")
        .registry(make_registry())
        .open(path)
        .unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(10), session.resume())
            .await
            .unwrap()
            .unwrap(),
        "done"
    );
    assert_eq!(tool.preparations.load(Ordering::SeqCst), 1);
    assert_eq!(tool.polls.load(Ordering::SeqCst), 2);
    assert_eq!(
        *observed.lock().unwrap(),
        Some(json!({"path":"authorized"}))
    );
}

struct ProjectedModel {
    prepares: Arc<AtomicUsize>,
    dispatches: Arc<AtomicUsize>,
    large: Arc<AtomicBool>,
    strict: Arc<AtomicBool>,
    large_prompt: Arc<AtomicBool>,
    estimates: Arc<AtomicUsize>,
}
struct ProjectedDispatch {
    inner: FallbackDispatch,
    reservation: ModelRequest,
}
#[async_trait::async_trait]
impl PreparedToolDispatch for ProjectedDispatch {
    fn effective_tools(&self) -> &[EffectiveToolDeclaration] {
        &self.inner.effective
    }
    fn decision_snapshot(&self) -> &ToolDecisionSnapshot {
        &self.inner.snapshot
    }
    fn dispatch_request(&self) -> &ModelRequest {
        &self.inner.request
    }
    fn reservation_request(&self) -> &ModelRequest {
        &self.reservation
    }
    async fn dispatch(self: Box<Self>) -> Result<DetailedModelStream> {
        self.inner.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(futures_util::stream::iter(vec![
            Ok(DetailedModelEvent::from(
                pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Text("done".into())),
            )),
            Ok(DetailedModelEvent::from(
                pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Complete {
                    calls: vec![],
                    usage: Some(pi_rs_core::Usage {
                        input_tokens: 1,
                        output_tokens: 1,
                    }),
                }),
            )),
        ])))
    }
}
#[async_trait::async_trait]
impl Model for ProjectedModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        panic!("prepared dispatch required")
    }
    fn context_identity(&self) -> Option<String> {
        Some("projection-test".into())
    }
    fn estimate_request_tokens(&self, request: &ModelRequest) -> u64 {
        self.estimates.fetch_add(1, Ordering::SeqCst);
        if request
            .tools
            .first()
            .is_some_and(|t| t.description == "large-projection")
            || request.messages.iter().any(|m| matches!(m, pi_rs_core::Message::System { content } if content == "large-effective-prompt"))
        {
            1000
        } else {
            1
        }
    }
    async fn prepare_tool_dispatch<'a>(
        &'a self,
        request: &ModelRequest,
        declarations: &[ToolDeclaration],
        context: &ToolDispatchContext,
    ) -> Result<Option<Box<dyn PreparedToolDispatch + 'a>>> {
        self.prepares.fetch_add(1, Ordering::SeqCst);
        let effective = declarations
            .iter()
            .map(|d| EffectiveToolDeclaration {
                spec: d.spec.clone(),
                sampling: if self.strict.load(Ordering::SeqCst) {
                    EffectiveToolSampling::StrictJsonSchema {
                        original_schema: d.spec.parameters.clone(),
                        normalization_version: pi_rs_core::STRICT_TOOL_NORMALIZATION_VERSION,
                    }
                } else {
                    EffectiveToolSampling::Unconstrained
                },
            })
            .collect::<Vec<_>>();
        let mut dispatch_request = request.clone();
        if self.large_prompt.load(Ordering::SeqCst) {
            dispatch_request.messages.insert(
                0,
                pi_rs_core::Message::System {
                    content: "large-effective-prompt".into(),
                },
            );
        }
        let snapshot = ToolDecisionSnapshot::capture_dispatch(
            context,
            request,
            &dispatch_request,
            declarations,
            &effective,
        )
        .map_err(|e| pi_rs_core::Error::Config(e.to_string()))?;
        let mut reservation = dispatch_request.clone();
        if self.large.load(Ordering::SeqCst) {
            reservation.tools[0].description = "large-projection".into();
        }
        Ok(Some(Box::new(ProjectedDispatch {
            inner: FallbackDispatch {
                calls: self.dispatches.clone(),
                committed: None,
                request: dispatch_request,
                snapshot,
                effective,
            },
            reservation,
        })))
    }
}

#[tokio::test]
async fn prepared_projection_controls_budget_and_is_prepared_once() {
    let prepares = Arc::new(AtomicUsize::new(0));
    let dispatches = Arc::new(AtomicUsize::new(0));
    let large = Arc::new(AtomicBool::new(true));
    let model = Arc::new(ProjectedModel {
        prepares: prepares.clone(),
        dispatches: dispatches.clone(),
        large: large.clone(),
        strict: Arc::new(AtomicBool::new(false)),
        large_prompt: Arc::new(AtomicBool::new(false)),
        estimates: Arc::new(AtomicUsize::new(0)),
    });
    let mut registry = Registry::default();
    registry
        .register(Arc::new(ConstrainedExtension(StrictToolPolicy::Prefer)))
        .unwrap();
    let mut session = SessionBuilder::new(model, "fixture")
        .registry(registry)
        .limits(pi_rs_core::Limits {
            context_window: Some(100),
            output_tokens_per_request: 1,
            ..Default::default()
        })
        .in_memory()
        .unwrap();
    assert!(session.prompt("oversize").await.is_err());
    assert_eq!(prepares.load(Ordering::SeqCst), 1);
    assert_eq!(dispatches.load(Ordering::SeqCst), 0);
    assert!(
        !session
            .journal()
            .unwrap()
            .records()
            .iter()
            .any(|r| matches!(r.event, Event::ModelRequested { .. }))
    );
}

#[tokio::test]
async fn prepared_projection_changes_configuration_identity() {
    let prepares = Arc::new(AtomicUsize::new(0));
    let dispatches = Arc::new(AtomicUsize::new(0));
    let large = Arc::new(AtomicBool::new(false));
    let strict = Arc::new(AtomicBool::new(false));
    let model = Arc::new(ProjectedModel {
        prepares: prepares.clone(),
        dispatches: dispatches.clone(),
        large: large.clone(),
        strict: strict.clone(),
        large_prompt: Arc::new(AtomicBool::new(false)),
        estimates: Arc::new(AtomicUsize::new(0)),
    });
    let mut registry = Registry::default();
    registry
        .register(Arc::new(ConstrainedExtension(StrictToolPolicy::Prefer)))
        .unwrap();
    let mut session = SessionBuilder::new(model, "fixture")
        .registry(registry)
        .limits(pi_rs_core::Limits {
            context_window: Some(5000),
            output_tokens_per_request: 1,
            ..Default::default()
        })
        .in_memory()
        .unwrap();
    assert_eq!(session.prompt("first").await.unwrap(), "done");
    large.store(true, Ordering::SeqCst);
    assert_eq!(session.prompt("second").await.unwrap(), "done");
    strict.store(true, Ordering::SeqCst);
    assert_eq!(session.prompt("third").await.unwrap(), "done");
    let hashes: Vec<_> = session
        .journal()
        .unwrap()
        .records()
        .iter()
        .filter_map(|r| match &r.event {
            Event::ModelRequested {
                context_configuration_sha256: Some(hash),
                ..
            } => Some(hash.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(hashes.len(), 3);
    assert_ne!(hashes[0], hashes[1]);
    assert_ne!(hashes[1], hashes[2]);
    assert_eq!(prepares.load(Ordering::SeqCst), 3);
    assert_eq!(dispatches.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn changed_effective_system_prompt_invalidates_measured_budget_anchor() {
    let prepares = Arc::new(AtomicUsize::new(0));
    let dispatches = Arc::new(AtomicUsize::new(0));
    let estimates = Arc::new(AtomicUsize::new(0));
    let large_prompt = Arc::new(AtomicBool::new(false));
    let model = Arc::new(ProjectedModel {
        prepares,
        dispatches: dispatches.clone(),
        large: Arc::new(AtomicBool::new(false)),
        strict: Arc::new(AtomicBool::new(false)),
        large_prompt: large_prompt.clone(),
        estimates: estimates.clone(),
    });
    let mut registry = Registry::default();
    registry
        .register(Arc::new(ConstrainedExtension(StrictToolPolicy::Prefer)))
        .unwrap();
    let mut session = SessionBuilder::new(model, "fixture")
        .registry(registry)
        .limits(pi_rs_core::Limits {
            context_window: Some(100),
            output_tokens_per_request: 1,
            ..Default::default()
        })
        .in_memory()
        .unwrap();
    session
        .controls()
        .set_auto_compaction_enabled(false)
        .unwrap();
    assert_eq!(session.prompt("first").await.unwrap(), "done");
    let after_first = estimates.load(Ordering::SeqCst);
    assert_eq!(session.prompt("second").await.unwrap(), "done");
    assert_eq!(
        estimates.load(Ordering::SeqCst),
        after_first,
        "unchanged configuration should reuse measured usage"
    );
    large_prompt.store(true, Ordering::SeqCst);
    assert!(session.prompt("third").await.is_err());
    assert!(
        estimates.load(Ordering::SeqCst) > after_first,
        "changed effective prompt must be estimated again"
    );
    assert_eq!(
        dispatches.load(Ordering::SeqCst),
        2,
        "oversized effective prompt must not dispatch"
    );
}
