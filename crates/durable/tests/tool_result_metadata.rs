use futures_util::{StreamExt, stream};
use pi_rs_core::{
    CancellationToken, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool,
    ToolCall, ToolContext, ToolSpec,
};
use pi_rs_durable::{
    extension::{Context, Extension, Hook},
    keyed_tool::{KeyedHost, KeyedTool, KeyedToolAdapter},
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{
        Config, DiagnosticSeverity, Harness, Options, RegisteredTool, Scheduler, ToolDiagnostic,
    },
    tool_search,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct Script {
    steps: Mutex<Vec<(&'static str, &'static str)>>,
    requests: Mutex<Vec<ModelRequest>>,
}
#[async_trait::async_trait]
impl Model for Script {
    async fn stream(&self, request: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        self.requests.lock().unwrap().push(request);
        let calls = self
            .steps
            .lock()
            .unwrap()
            .pop()
            .map(|(name, args)| ToolCall {
                id: format!("id-{name}"),
                name: name.into(),
                arguments: args.into(),
            })
            .into_iter()
            .collect();
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])
        .boxed())
    }
}
fn spec(name: &str) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        description: "window screenshot".into(),
        parameters: serde_json::json!({"type":"object"}),
    }
}
struct Detailed(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl KeyedTool for Detailed {
    fn spec(&self) -> ToolSpec {
        spec("detailed")
    }
    async fn invoke(
        &self,
        _: serde_json::Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        host: KeyedHost,
    ) -> pi_rs_core::ToolReply {
        self.0.fetch_add(1, Ordering::SeqCst);
        host.diagnostic(ToolDiagnostic {
            severity: DiagnosticSeverity::Warn,
            message: "private warning".into(),
            code: Some("warn_code".into()),
        })
        .unwrap();
        Outcome::SucceededWithDetails {
            output: "visible output".into(),
            details: serde_json::json!({"secret":"private details"}),
        }
        .into()
    }
}
struct Plain(&'static str);
#[async_trait::async_trait]
impl Tool for Plain {
    fn spec(&self) -> ToolSpec {
        spec(self.0)
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        match self.0 {
            "structured" => Outcome::Structured {
                data: serde_json::json!({"count":42}),
                attachments: vec![pi_rs_core::AttachmentRef {
                    id: "asset".into(),
                    media_type: "text/plain".into(),
                }],
            },
            "failed" => Outcome::Failed("deliberate failure".into()),
            _ => Outcome::Succeeded("ok".into()),
        }
    }
}
struct Observe {
    snapshots: Arc<Mutex<Vec<serde_json::Value>>>,
    cancel: Option<CancellationToken>,
}
#[async_trait::async_trait]
impl Hook for Observe {
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &serde_json::Value,
        ctx: &Context<'_>,
    ) -> Result<Option<serde_json::Value>, String> {
        self.snapshots
            .lock()
            .unwrap()
            .push(ctx.snapshot["tool_results"].clone());
        if call.name == "detailed"
            && let Some(cancel) = &self.cancel
        {
            cancel.cancel();
        }
        Ok(None)
    }
}
fn opts(model: Arc<Script>, effects: Arc<AtomicUsize>) -> Options {
    let mut tools: Vec<RegisteredTool> = ["structured", "failed", "screen"]
        .into_iter()
        .map(|name| RegisteredTool {
            tool: Arc::new(Plain(name)),
            replay: ReplayPolicy::Safe,
        })
        .collect();
    tools.push(Arc::new(KeyedToolAdapter::new(Detailed(effects))).register(ReplayPolicy::Unsafe));
    Options {
        model,
        tools,
        restart_limit: 3,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
fn script() -> Arc<Script> {
    Arc::new(Script {
        steps: Mutex::new(vec![
            ("tool_search", r#"{"query":"screen"}"#),
            ("failed", "{}"),
            ("structured", "{}"),
            ("detailed", "{}"),
        ]),
        requests: Mutex::new(vec![]),
    })
}
fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 128,
    }
}
fn install(
    h: &Harness,
    snapshots: Arc<Mutex<Vec<serde_json::Value>>>,
    cancel: Option<CancellationToken>,
) {
    h.install_extension(tool_search::extension()).unwrap();
    h.install_extension(Extension {
        name: "observe".into(),
        hooks: vec![Arc::new(Observe { snapshots, cancel })],
        ..Default::default()
    })
    .unwrap();
}
#[tokio::test]
async fn result_details_diagnostics_and_search_survive_hook_interruption_history_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = script();
    let effects = Arc::new(AtomicUsize::new(0));
    let snapshots = Arc::new(Mutex::new(vec![]));
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open_paused(&path, opts(model.clone(), effects.clone())).unwrap();
        install(&h, snapshots.clone(), Some(cancel.clone()));
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["tool-search".into(), "observe".into()])
            .unwrap();
        h.set_deferred_tools(id, vec!["screen".into()]).unwrap();
        assert!(
            h.submit_and_run(id, None, "go".into(), &cancel)
                .await
                .is_err()
        );
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert!(snapshots.lock().unwrap()[0].is_null());
    }
    let mut h = Harness::open_paused(&path, opts(model.clone(), effects.clone())).unwrap();
    install(&h, snapshots.clone(), None);
    let prior = h.conversation(id).unwrap();
    assert!(prior.tool_results.is_empty());
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let c = h.conversation(id).unwrap();
    assert_eq!(c.tool_results.len(), 4);
    assert_eq!(
        c.tool_results[0].details.as_ref().unwrap()["secret"],
        "private details"
    );
    assert_eq!(
        c.tool_results[0].diagnostics[0].code.as_deref(),
        Some("warn_code")
    );
    assert_eq!(
        c.tool_results[1].structured_output.as_ref().unwrap()["count"],
        42
    );
    assert_eq!(c.tool_results[1].attachments[0].id, "asset");
    assert!(c.tool_results[2].is_error);
    assert_eq!(
        c.tool_results[2].diagnostics[0].message,
        "deliberate failure"
    );
    assert_eq!(
        c.tool_results[3].details.as_ref().unwrap()["loaded"],
        serde_json::json!(["screen"])
    );
    assert_eq!(c.tool_results[3].call_id, "id-tool_search");
    let requests = model.requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .flat_map(|r| &r.messages)
            .any(|m| matches!(m, Message::Tool { content, .. } if content == "visible output"))
    );
    assert!(
        !requests
            .iter()
            .flat_map(|r| &r.messages)
            .any(|m| format!("{m:?}").contains("private details")
                || format!("{m:?}").contains("private warning"))
    );
    drop(requests);
    let history = h
        .history(
            id,
            pi_rs_durable::observer::Cursor(0),
            std::num::NonZeroUsize::new(1000).unwrap(),
        )
        .unwrap();
    assert!(history.records.iter().any(|event| {
        event.value["tool_results"]
            .to_string()
            .contains("private details")
    }));
    let fork = h.fork(id).unwrap();
    assert_eq!(h.conversation(fork).unwrap().tool_results, c.tool_results);
    h.reset(id, Some("new context".into())).unwrap();
    assert_eq!(h.conversation(id).unwrap().tool_results, c.tool_results);
}
#[tokio::test]
async fn scheduler_exposes_result_metadata_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(Script {
        steps: Mutex::new(vec![("detailed", "{}")]),
        requests: Mutex::new(vec![]),
    });
    let effects = Arc::new(AtomicUsize::new(0));
    let id;
    {
        let mut s = Scheduler::open(&path, opts(model.clone(), effects.clone())).unwrap();
        id = s.create_conversation(config()).unwrap();
        s.submit(id, None, "go".into()).unwrap();
        s.wait(id).await.unwrap();
        assert_eq!(
            s.conversation(id).unwrap().tool_results[0].diagnostics[0].message,
            "private warning"
        );
        s.shutdown().await.unwrap();
    }
    let s = Scheduler::open(&path, opts(model, effects.clone())).unwrap();
    assert_eq!(
        s.conversation(id).unwrap().tool_results[0]
            .details
            .as_ref()
            .unwrap()["secret"],
        "private details"
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    s.shutdown().await.unwrap();
}

struct Pending(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl Tool for Pending {
    fn spec(&self) -> ToolSpec {
        spec("pending")
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        self.0.fetch_add(1, Ordering::SeqCst);
        Outcome::Pending {
            reference: "job".into(),
        }
    }
}
#[tokio::test]
async fn reconciled_details_are_committed_once_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(Script {
        steps: Mutex::new(vec![("pending", "{}")]),
        requests: Mutex::new(vec![]),
    });
    let effects = Arc::new(AtomicUsize::new(0));
    let opts = || Options {
        model: model.clone(),
        tools: vec![RegisteredTool {
            tool: Arc::new(Pending(effects.clone())),
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let id;
    {
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        id = h.create_conversation(config()).unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        let operation = h.blocked(id).unwrap().unwrap().0;
        h.reconcile(
            id,
            operation,
            Outcome::SucceededWithDetails {
                output: "confirmed".into(),
                details: serde_json::json!({"receipt":7}),
            },
        )
        .unwrap();
    }
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(c.tool_results.len(), 1);
    assert_eq!(c.tool_results[0].details.as_ref().unwrap()["receipt"], 7);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
}

struct Oversized(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl Tool for Oversized {
    fn spec(&self) -> ToolSpec {
        spec("oversized")
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        self.0.fetch_add(1, Ordering::SeqCst);
        Outcome::SucceededWithDetails {
            output: "kept output".into(),
            details: serde_json::json!({"large":"x".repeat(70_000)}),
        }
    }
}
#[tokio::test]
async fn oversized_metadata_records_warning_without_repeating_tool() {
    let dir = tempfile::tempdir().unwrap();
    let effects = Arc::new(AtomicUsize::new(0));
    let model = Arc::new(Script {
        steps: Mutex::new(vec![("oversized", "{}")]),
        requests: Mutex::new(vec![]),
    });
    let options = Options {
        model,
        tools: vec![RegisteredTool {
            tool: Arc::new(Oversized(effects.clone())),
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open_paused(dir.path().join("db"), options).unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert!(c.tool_results[0].details.is_none());
    assert_eq!(
        c.tool_results[0].diagnostics[0].code.as_deref(),
        Some("metadata_truncated")
    );
    assert!(c.messages.iter().any(
        |message| matches!(message, Message::Tool { content, .. } if content == "kept output")
    ));
    assert_eq!(effects.load(Ordering::SeqCst), 1);
}
