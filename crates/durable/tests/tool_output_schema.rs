use futures_util::{StreamExt, stream};
use pi_rs_core::{
    CancellationToken, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool,
    ToolCall, ToolContext, ToolSpec,
};
use pi_rs_durable::{
    extension::{Context, Extension, Hook},
    keyed_tool::{KeyedHost, KeyedTool, KeyedToolAdapter},
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, Harness, Options, RegisteredTool, Scheduler},
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
struct OneModel {
    requests: Mutex<Vec<ModelRequest>>,
    calls: AtomicUsize,
    name: &'static str,
}
#[async_trait::async_trait]
impl Model for OneModel {
    async fn stream(&self, r: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        self.requests.lock().unwrap().push(r);
        let calls = if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            vec![ToolCall {
                id: "call".into(),
                name: self.name.into(),
                arguments: "{}".into(),
            }]
        } else {
            vec![]
        };
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("done".into())),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])
        .boxed())
    }
}
fn model(name: &'static str) -> Arc<OneModel> {
    Arc::new(OneModel {
        requests: Mutex::new(vec![]),
        calls: AtomicUsize::new(0),
        name,
    })
}
fn spec(name: &str) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        description: "test".into(),
        parameters: json!({"type":"object"}),
    }
}
struct Reply {
    mode: u8,
    calls: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl Tool for Reply {
    fn spec(&self) -> ToolSpec {
        spec("job")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            0 => Outcome::Structured {
                data: json!({"value":7, "secret":"schema-secret"}),
                attachments: vec![],
            },
            1 => Outcome::Structured {
                data: json!({"value":"bad"}),
                attachments: vec![],
            },
            2 => Outcome::Succeeded("plain".into()),
            3 => Outcome::Failed("failure".into()),
            5 => Outcome::Structured {
                data: json!({"value":7,"large":"x".repeat(70_000)}),
                attachments: vec![],
            },
            6 => Outcome::Structured {
                data: Value::Null,
                attachments: vec![],
            },
            _ => Outcome::SucceededWithDetails {
                output: "private output".into(),
                details: json!({"secret":"metadata-secret"}),
            },
        }
    }
}

#[tokio::test]
async fn null_structured_output_remains_present_through_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = model("job");
    let calls = Arc::new(AtomicUsize::new(0));
    let id;
    {
        let mut s = Scheduler::open(&path, opts(model.clone(), 6, calls.clone())).unwrap();
        id = s.create_conversation(config()).unwrap();
        s.set_tool_output_schema(id, "job".into(), Some(json!({"type":"null"})))
            .unwrap();
        s.submit(id, None, "go".into()).unwrap();
        s.wait(id).await.unwrap();
        assert_eq!(
            s.conversation(id).unwrap().tool_results[0].structured_output,
            Some(Value::Null)
        );
        s.shutdown().await.unwrap();
    }
    let s = Scheduler::open(&path, opts(model, 6, calls.clone())).unwrap();
    let result = &s.conversation(id).unwrap().tool_results[0];
    assert_eq!(result.structured_output, Some(Value::Null));
    assert!(
        !result
            .diagnostics
            .iter()
            .any(|d| d.code.as_deref() == Some("invalid_structured_output"))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn oversized_structured_metadata_reports_limit_without_false_schema_failure() {
    let dir = tempfile::tempdir().unwrap();
    let m = model("job");
    let calls = Arc::new(AtomicUsize::new(0));
    let mut s = Scheduler::open(dir.path().join("db"), opts(m, 5, calls.clone())).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.set_tool_output_schema(id, "job".into(), Some(schema()))
        .unwrap();
    s.submit(id, None, "go".into()).unwrap();
    s.wait(id).await.unwrap();
    let c = s.conversation(id).unwrap();
    let result = &c.tool_results[0];
    assert!(result.metadata_truncated && result.structured_output.is_none());
    assert!(
        result
            .diagnostics
            .iter()
            .any(|d| d.code.as_deref() == Some("metadata_truncated"))
    );
    assert!(
        !result
            .diagnostics
            .iter()
            .any(|d| d.code.as_deref() == Some("invalid_structured_output"))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    s.shutdown().await.unwrap();
}
fn opts(m: Arc<OneModel>, mode: u8, calls: Arc<AtomicUsize>) -> Options {
    Options {
        model: m,
        tools: vec![RegisteredTool {
            tool: Arc::new(Reply { mode, calls }),
            replay: ReplayPolicy::Unsafe,
        }],
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
fn schema() -> Value {
    json!({"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]})
}
#[tokio::test]
async fn validates_model_results_without_leaking_structured_output_and_survives_reopen() {
    for mode in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let m = model("job");
        let calls = Arc::new(AtomicUsize::new(0));
        let id;
        {
            let mut s = Scheduler::open(&path, opts(m.clone(), mode, calls.clone())).unwrap();
            id = s.create_conversation(config()).unwrap();
            s.set_tool_output_schema(id, "job".into(), Some(schema()))
                .unwrap();
            s.submit(id, None, "go".into()).unwrap();
            s.wait(id).await.unwrap();
            let c = s.conversation(id).unwrap();
            let result = &c.tool_results[0];
            match mode {
                0 => assert_eq!(result.structured_output.as_ref().unwrap()["value"], 7),
                1 | 2 => {
                    assert!(result.structured_output.is_none());
                    assert!(
                        result
                            .diagnostics
                            .iter()
                            .any(|d| d.code.as_deref() == Some("invalid_structured_output"))
                    );
                    assert!(!result.is_error);
                }
                _ => {
                    assert!(result.is_error);
                    assert!(
                        !result
                            .diagnostics
                            .iter()
                            .any(|d| d.code.as_deref() == Some("invalid_structured_output"))
                    );
                }
            }
            if mode == 0 || mode == 1 {
                assert!(
                    m.requests
                        .lock()
                        .unwrap()
                        .iter()
                        .flat_map(|r| &r.messages)
                        .any(|v| matches!(v,Message::Tool { content,.. } if (mode == 0 && content.is_empty()) || (mode == 1 && content.contains("structured output missing or does not match the configured schema"))))
                );
            }
            assert!(
                !m.requests
                    .lock()
                    .unwrap()
                    .iter()
                    .flat_map(|r| &r.messages)
                    .any(|v| format!("{v:?}").contains("schema-secret"))
            );
            s.shutdown().await.unwrap();
        }
        let s = Scheduler::open(&path, opts(m, mode, calls.clone())).unwrap();
        assert_eq!(
            s.conversation(id).unwrap().tool_filters.output_schemas["job"],
            schema()
        );
        assert_eq!(s.conversation(id).unwrap().tool_results.len(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        s.shutdown().await.unwrap();
    }
}
struct Redactor {
    cancel: Option<CancellationToken>,
}
#[async_trait::async_trait]
impl Hook for Redactor {
    async fn after_tool(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
        }
        Ok(Some(json!("REDACTED")))
    }
}
fn install(h: &Harness, cancel: Option<CancellationToken>) {
    h.install_extension(Extension {
        name: "redact".into(),
        hooks: vec![Arc::new(Redactor { cancel })],
        ..Default::default()
    })
    .unwrap();
}
#[tokio::test]
async fn redaction_hides_pending_and_final_metadata_across_hook_interruption() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let m = model("job");
    let calls = Arc::new(AtomicUsize::new(0));
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open_paused(&path, opts(m.clone(), 4, calls.clone())).unwrap();
        install(&h, Some(cancel.clone()));
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["redact".into()]).unwrap();
        assert!(
            h.submit_and_run(id, None, "go".into(), &cancel)
                .await
                .is_err()
        );
    }
    let mut h = Harness::open_paused(&path, opts(m.clone(), 4, calls.clone())).unwrap();
    install(&h, None);
    let before = serde_json::to_string(&h.conversation(id).unwrap()).unwrap();
    assert!(!before.contains("metadata-secret") && !before.contains("private output"));
    assert!(
        !h.history(
            id,
            pi_rs_durable::observer::Cursor(0),
            std::num::NonZeroUsize::new(1000).unwrap()
        )
        .unwrap()
        .records
        .iter()
        .any(|r| r.value.to_string().contains("metadata-secret")
            || r.value.to_string().contains("private output"))
    );
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(c.tool_results[0].details.is_none());
    assert!(
        c.tool_results[0]
            .diagnostics
            .iter()
            .any(|d| d.code.as_deref() == Some("output_redacted"))
    );
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m,Message::Tool { content,.. } if content=="REDACTED"))
    );
    assert!(
        !h.history(
            id,
            pi_rs_durable::observer::Cursor(0),
            std::num::NonZeroUsize::new(1000).unwrap()
        )
        .unwrap()
        .records
        .iter()
        .any(|r| r.value.to_string().contains("metadata-secret")
            || r.value.to_string().contains("private output"))
    );
}
#[test]
fn invalid_schema_and_unknown_tool_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut h =
        Harness::open_paused(dir.path().join("db"), opts(model("job"), 0, Arc::default())).unwrap();
    let id = h.create_conversation(config()).unwrap();
    assert!(
        h.set_tool_output_schema(id, "missing".into(), Some(schema()))
            .is_err()
    );
    assert!(
        h.set_tool_output_schema(id, "job".into(), Some(json!({"type":5})))
            .is_err()
    );
    assert!(
        h.set_tool_output_schema(
            id,
            "job".into(),
            Some(json!({"$ref":"https://example.invalid/schema"}))
        )
        .is_err()
    );
    assert!(
        h.conversation(id)
            .unwrap()
            .tool_filters
            .output_schemas
            .is_empty()
    );
}

struct Parent(Arc<Mutex<Vec<Outcome>>>);
#[async_trait::async_trait]
impl KeyedTool for Parent {
    fn spec(&self) -> ToolSpec {
        spec("parent")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        host: KeyedHost,
    ) -> pi_rs_core::ToolReply {
        let result = host.call_keyed("child", "job", json!({})).await;
        self.0.lock().unwrap().push(result);
        Outcome::Succeeded("parent done".into()).into()
    }
}
#[tokio::test]
async fn nested_results_validate_before_return_and_do_not_repeat_after_reopen() {
    for mode in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let m = model("parent");
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::new(Mutex::new(vec![]));
        let id;
        let options = || {
            let mut o = opts(m.clone(), mode, calls.clone());
            o.tools.push(
                Arc::new(KeyedToolAdapter::new(Parent(observed.clone())))
                    .register(ReplayPolicy::Unsafe),
            );
            o
        };
        {
            let mut s = Scheduler::open(&path, options()).unwrap();
            id = s.create_conversation(config()).unwrap();
            s.set_tool_output_schema(id, "job".into(), Some(schema()))
                .unwrap();
            s.submit(id, None, "go".into()).unwrap();
            s.wait(id).await.unwrap();
            {
                let seen = observed.lock().unwrap();
                match mode {
                    0 => {
                        assert!(
                            matches!(&seen[0],Outcome::Structured { data,.. } if data["value"]==7)
                        )
                    }
                    1 | 2 => assert!(
                        matches!(&seen[0],Outcome::Failed(reason) if reason.contains("invalid_structured_output"))
                    ),
                    _ => assert!(matches!(&seen[0],Outcome::Failed(reason) if reason=="failure")),
                }
            }
            s.shutdown().await.unwrap();
        }
        let s = Scheduler::open(&path, options()).unwrap();
        assert_eq!(s.conversation(id).unwrap().tool_results.len(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        s.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn output_replacement_withholds_schema_bearing_structured_value() {
    let dir = tempfile::tempdir().unwrap();
    let model = model("job");
    let calls = Arc::new(AtomicUsize::new(0));
    let mut h = Harness::open_paused(dir.path().join("db"), opts(model, 0, calls.clone())).unwrap();
    install(&h, None);
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["redact".into()]).unwrap();
    h.set_tool_output_schema(id, "job".into(), Some(schema()))
        .unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert!(c.tool_results[0].structured_output.is_none());
    assert!(c.tool_results[0].redacted);
    assert!(
        c.tool_results[0]
            .diagnostics
            .iter()
            .any(|d| d.code.as_deref() == Some("output_redacted"))
    );
    assert!(
        !c.tool_results[0]
            .diagnostics
            .iter()
            .any(|d| d.code.as_deref() == Some("invalid_structured_output"))
    );
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m, Message::Tool { content, .. } if content == "REDACTED"))
    );
    assert!(
        !h.history(
            id,
            pi_rs_durable::observer::Cursor(0),
            std::num::NonZeroUsize::new(1000).unwrap()
        )
        .unwrap()
        .records
        .iter()
        .any(|r| r.value.to_string().contains("schema-secret"))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn abort_before_hook_completion_withholds_pending_output_and_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = model("job");
    let calls = Arc::new(AtomicUsize::new(0));
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open_paused(&path, opts(model.clone(), 4, calls.clone())).unwrap();
        install(&h, Some(cancel.clone()));
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["redact".into()]).unwrap();
        assert!(
            h.submit_and_run(id, None, "go".into(), &cancel)
                .await
                .is_err()
        );
    }
    let mut h = Harness::open_paused(&path, opts(model, 4, calls.clone())).unwrap();
    install(&h, None);
    h.request_abort(id).unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert!(c.tool_results[0].redacted && c.tool_results[0].details.is_none());
    assert!(
        c.tool_results[0]
            .diagnostics
            .iter()
            .any(|d| d.code.as_deref() == Some("hook_not_completed"))
    );
    assert!(!format!("{:?}", c.messages).contains("private output"));
    assert!(
        !h.history(
            id,
            pi_rs_durable::observer::Cursor(0),
            std::num::NonZeroUsize::new(1000).unwrap()
        )
        .unwrap()
        .records
        .iter()
        .any(|r| r.value.to_string().contains("metadata-secret")
            || r.value.to_string().contains("private output"))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
