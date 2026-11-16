use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use pi_rs_core::{
    ContextChange, ContextRequest, Extension, ExtensionResult, ExtensionState, Model, ModelEvent,
    ModelRequest, ModelStream, Outcome, Registry, Result, SessionBuilder, StrictToolPolicy, Tool,
    ToolConstraint, ToolContext, ToolSpec,
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
