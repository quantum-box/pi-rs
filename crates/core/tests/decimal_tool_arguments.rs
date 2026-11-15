use std::sync::{Arc, Mutex};

use pi_rs_core::{
    Extension, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Registry, Result,
    SessionBuilder, Tool, ToolCall, ToolContext, ToolSpec,
};
use serde_json::{Value, json};

struct Capture(Arc<Mutex<Option<Value>>>);
#[async_trait::async_trait]
impl Tool for Capture {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "capture".into(),
            description: "capture numeric argument".into(),
            parameters: json!({"type":"object","properties":{"x":{"type":"number"}},"required":["x"]}),
        }
    }
    async fn invoke(&self, arguments: Value, _: ToolContext) -> Outcome {
        *self.0.lock().unwrap() = Some(arguments);
        Outcome::Succeeded("ok".into())
    }
}
struct CaptureExtension(Arc<Mutex<Option<Value>>>);
impl Extension for CaptureExtension {
    fn id(&self) -> &str {
        "capture"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(Capture(self.0.clone()))]
    }
}
struct Script(Mutex<usize>);
#[async_trait::async_trait]
impl Model for Script {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        let mut calls = self.0.lock().unwrap();
        *calls += 1;
        let events = if *calls == 1 {
            vec![ModelEvent::Complete {
                calls: vec![ToolCall {
                    id: "id".into(),
                    name: "capture".into(),
                    arguments: r#"{"x":34450847700860466777e-23}"#.into(),
                }],
                usage: None,
            }]
        } else {
            vec![
                ModelEvent::Text("done".into()),
                ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                },
            ]
        };
        Ok(Box::pin(futures_util::stream::iter(
            events.into_iter().map(Ok),
        )))
    }
}
#[tokio::test]
async fn tool_execution_receives_correctly_rounded_decimal() {
    let captured = Arc::new(Mutex::new(None));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(CaptureExtension(captured.clone())))
        .unwrap();
    let mut session = SessionBuilder::new(Arc::new(Script(Mutex::new(0))), "fixture")
        .registry(registry)
        .in_memory()
        .unwrap();
    assert_eq!(session.prompt("test").await.unwrap(), "done");
    let captured = captured.lock().unwrap();
    let number = captured.as_ref().unwrap()["x"].as_f64().unwrap();
    assert_eq!(number.to_bits(), 0.00034450847700860464_f64.to_bits());
}

#[test]
fn decimal_parsing_is_precise_but_native_large_integers_remain_exact() {
    let value: Value =
        serde_json::from_str(r#"{"x":34450847700860466777e-23,"i":9007199254740993}"#).unwrap();
    assert_eq!(
        value["x"].as_f64().unwrap().to_bits(),
        0.00034450847700860464_f64.to_bits()
    );
    assert_eq!(value["i"].as_u64(), Some(9_007_199_254_740_993));
}
