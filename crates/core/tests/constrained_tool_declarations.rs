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
