use futures_util::{StreamExt, stream};
use pi_rs_core::{
    CancellationToken, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, ToolCall,
    ToolContext, ToolSpec,
};
use pi_rs_durable::{
    extension::{Context, Extension, Hook},
    keyed_tool::{KeyedHost, KeyedTool, KeyedToolAdapter},
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, DiagnosticSeverity, Harness, Options, ToolDiagnostic},
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct FakeModel(&'static str);
#[async_trait::async_trait]
impl Model for FakeModel {
    async fn stream(&self, req: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        let done = req
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }));
        let calls = if done {
            vec![]
        } else {
            vec![ToolCall {
                id: "call".into(),
                name: self.0.into(),
                arguments: "{}".into(),
            }]
        };
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("done".into())),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])
        .boxed())
    }
}
fn spec(name: &str) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        description: String::new(),
        parameters: json!({"type":"object"}),
    }
}
fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 64,
    }
}
fn diagnostic(message: &str) -> ToolDiagnostic {
    ToolDiagnostic {
        severity: DiagnosticSeverity::Warn,
        message: message.into(),
        code: Some("checkpoint".into()),
    }
}

struct ReplayDiagnostics {
    name: &'static str,
    count: Arc<AtomicUsize>,
    cancel: CancellationToken,
}
#[async_trait::async_trait]
impl KeyedTool for ReplayDiagnostics {
    fn spec(&self) -> ToolSpec {
        spec(self.name)
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        host: KeyedHost,
    ) -> pi_rs_core::ToolReply {
        let attempt = self.count.fetch_add(1, Ordering::SeqCst);
        host.diagnostic_keyed("event-1", diagnostic("repeated"))
            .unwrap();
        host.diagnostic_keyed("event-1", diagnostic("repeated"))
            .unwrap();
        let mut concurrent = Vec::new();
        for _ in 0..4 {
            let host = host.clone();
            concurrent.push(tokio::spawn(async move {
                host.diagnostic_keyed("race", diagnostic("repeated"))
            }));
        }
        for result in concurrent {
            result.await.unwrap().unwrap();
        }
        host.diagnostic_keyed("event-2", diagnostic("repeated"))
            .unwrap();
        assert!(
            host.diagnostic_keyed("event-1", diagnostic("conflict"))
                .is_err()
        );
        assert!(host.diagnostic_keyed("", diagnostic("invalid")).is_err());
        assert!(
            host.diagnostic_keyed(&"x".repeat(129), diagnostic("invalid"))
                .is_err()
        );
        host.diagnostic(diagnostic("unkeyed")).unwrap();
        if attempt == 0 {
            self.cancel.cancel();
            std::future::pending().await
        } else {
            host.diagnostic_keyed("event-3", diagnostic("repeated"))
                .unwrap();
            host.diagnostic(diagnostic("repeated")).unwrap();
            host.diagnostic(diagnostic("new")).unwrap();
            Outcome::Succeeded("done".into()).into()
        }
    }
}
struct ReplayParent;
#[async_trait::async_trait]
impl KeyedTool for ReplayParent {
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
        let result = host
            .call_keyed_with_diagnostics("child", "child", json!({}))
            .await;
        for diagnostic in result.diagnostics {
            host.diagnostic(diagnostic).unwrap();
        }
        result.outcome.into()
    }
}
#[tokio::test]
async fn stable_diagnostic_ids_deduplicate_recovery_but_keep_new_identical_events() {
    for nested in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let count = Arc::new(AtomicUsize::new(0));
        let cancel = CancellationToken::new();
        let options = |token: CancellationToken| {
            let child = Arc::new(KeyedToolAdapter::new(ReplayDiagnostics {
                name: if nested { "child" } else { "job" },
                count: count.clone(),
                cancel: token,
            }))
            .register(ReplayPolicy::Safe);
            let mut tools = vec![child];
            if nested {
                tools.push(
                    Arc::new(KeyedToolAdapter::new(ReplayParent)).register(ReplayPolicy::Safe),
                );
            }
            Options {
                model: Arc::new(FakeModel(if nested { "parent" } else { "job" })),
                tools,
                restart_limit: 4,
                max_turns: 4,
                uncertain_policy: UncertainPolicy::RequireReconciliation,
            }
        };
        let id;
        {
            let mut h = Harness::open_paused(&path, options(cancel.clone())).unwrap();
            id = h.create_conversation(config()).unwrap();
            assert!(
                h.submit_and_run(id, None, "go".into(), &cancel)
                    .await
                    .is_err()
            );
        }
        let mut h = Harness::open(
            &path,
            options(CancellationToken::new()),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        h.resume_conversation(id, &CancellationToken::new())
            .await
            .unwrap();
        let c = h.conversation(id).unwrap();
        let messages: Vec<_> = c.tool_results[0]
            .diagnostics
            .iter()
            .map(|d| d.message.as_str())
            .collect();
        assert_eq!(
            messages,
            [
                "repeated", "repeated", "repeated", "unkeyed", "unkeyed", "repeated", "repeated",
                "new"
            ]
        );
        assert_eq!(count.load(Ordering::SeqCst), 2);
        let expected = c.tool_results[0].diagnostics.clone();
        let fork = h.fork(id).unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options(CancellationToken::new())).unwrap();
        assert_eq!(
            h.conversation(id).unwrap().tool_results[0].diagnostics,
            expected
        );
        assert_eq!(
            h.conversation(fork).unwrap().tool_results[0].diagnostics,
            expected
        );
    }
}
struct Unsettled {
    mode: u8,
    count: Arc<AtomicUsize>,
    cancel: CancellationToken,
}
#[async_trait::async_trait]
impl KeyedTool for Unsettled {
    fn spec(&self) -> ToolSpec {
        spec("job")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        host: KeyedHost,
    ) -> pi_rs_core::ToolReply {
        self.count.fetch_add(1, Ordering::SeqCst);
        host.diagnostic(diagnostic("private checkpoint")).unwrap();
        match self.mode {
            0 => Outcome::Pending {
                reference: "external".into(),
            }
            .into(),
            1 => Outcome::Unknown("uncertain".into()).into(),
            _ => {
                self.cancel.cancel();
                std::future::pending().await
            }
        }
    }
}
struct Redact;
#[async_trait::async_trait]
impl Hook for Redact {
    async fn after_tool(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        Ok(Some(json!("REDACTED")))
    }
}
fn opts(mode: u8, count: Arc<AtomicUsize>, cancel: CancellationToken) -> Options {
    Options {
        model: Arc::new(FakeModel("job")),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(Unsettled {
                mode,
                count,
                cancel,
            }))
            .register(ReplayPolicy::Unsafe),
        ],
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
#[tokio::test]
async fn pending_unknown_and_interrupted_diagnostics_survive_reopen_and_fork() {
    for mode in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let count = Arc::new(AtomicUsize::new(0));
        let cancel = CancellationToken::new();
        let id;
        {
            let mut h =
                Harness::open_paused(&path, opts(mode, count.clone(), cancel.clone())).unwrap();
            id = h.create_conversation(config()).unwrap();
            let result = h.submit_and_run(id, None, "go".into(), &cancel).await;
            if mode == 2 {
                assert!(result.is_err());
            } else {
                result.unwrap();
            }
        }
        let mut h = Harness::open(
            &path,
            opts(mode, count.clone(), CancellationToken::new()),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        let op = h.blocked(id).unwrap().unwrap().0;
        assert!(h.conversation(id).unwrap().tool_results.is_empty());
        h.reconcile(id, op, Outcome::Succeeded("confirmed".into()))
            .unwrap();
        h.resume_conversation(id, &CancellationToken::new())
            .await
            .unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(c.tool_results.len(), 1);
        assert_eq!(
            c.tool_results[0].diagnostics[0].message,
            "private checkpoint"
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let fork = h.fork(id).unwrap();
        assert_eq!(h.conversation(fork).unwrap().tool_results, c.tool_results);
        let hist = h
            .history(
                id,
                pi_rs_durable::observer::Cursor(0),
                std::num::NonZeroUsize::new(1000).unwrap(),
            )
            .unwrap();
        assert!(hist.records.iter().any(|event| {
            event.value["tool_results"]
                .to_string()
                .contains("private checkpoint")
        }));
    }
}
#[tokio::test]
async fn reconciled_diagnostics_are_withheld_by_legacy_redaction_hook() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let count = Arc::new(AtomicUsize::new(0));
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open_paused(&path, opts(0, count.clone(), cancel.clone())).unwrap();
        h.install_extension(Extension {
            name: "redact".into(),
            hooks: vec![Arc::new(Redact)],
            ..Default::default()
        })
        .unwrap();
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["redact".into()]).unwrap();
        h.submit_and_run(id, None, "go".into(), &cancel)
            .await
            .unwrap();
    }
    let mut h = Harness::open_paused(&path, opts(0, count.clone(), cancel)).unwrap();
    h.install_extension(Extension {
        name: "redact".into(),
        hooks: vec![Arc::new(Redact)],
        ..Default::default()
    })
    .unwrap();
    let op = h.blocked(id).unwrap().unwrap().0;
    h.reconcile(id, op, Outcome::Succeeded("raw".into()))
        .unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert!(c.tool_results[0].redacted);
    assert!(!format!("{:?}", c.tool_results).contains("private checkpoint"));
    assert!(
        !h.history(
            id,
            pi_rs_durable::observer::Cursor(0),
            std::num::NonZeroUsize::new(1000).unwrap()
        )
        .unwrap()
        .records
        .iter()
        .any(|event| event.value.to_string().contains("private checkpoint"))
    );
}

struct Child(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl KeyedTool for Child {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        host: KeyedHost,
    ) -> pi_rs_core::ToolReply {
        self.0.fetch_add(1, Ordering::SeqCst);
        host.diagnostic(diagnostic("nested checkpoint")).unwrap();
        Outcome::Succeeded("child result".into()).into()
    }
}
struct Parent {
    calls: Arc<AtomicUsize>,
    cancel: CancellationToken,
}
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
        let result = host
            .call_keyed_with_diagnostics("child-key", "child", json!({}))
            .await;
        assert!(matches!(result.outcome,Outcome::Succeeded(ref v) if v=="child result"));
        assert_eq!(result.diagnostics.len(), 1);
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.cancel.cancel();
            std::future::pending().await
        } else {
            host.diagnostic(result.diagnostics[0].clone()).unwrap();
            Outcome::Succeeded("parent result".into()).into()
        }
    }
}
#[tokio::test]
async fn nested_diagnostics_reattach_after_parent_interruption_without_child_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let parent = Arc::new(AtomicUsize::new(0));
    let child = Arc::new(AtomicUsize::new(0));
    let cancel = CancellationToken::new();
    let id;
    let options = |token: CancellationToken| Options {
        model: Arc::new(FakeModel("parent")),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(Parent {
                calls: parent.clone(),
                cancel: token,
            }))
            .register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(Child(child.clone()))).register(ReplayPolicy::Unsafe),
        ],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    {
        let mut h = Harness::open_paused(&path, options(cancel.clone())).unwrap();
        id = h.create_conversation(config()).unwrap();
        assert!(
            h.submit_and_run(id, None, "go".into(), &cancel)
                .await
                .is_err()
        );
        assert_eq!(child.load(Ordering::SeqCst), 1);
    }
    let mut h = Harness::open(
        &path,
        options(CancellationToken::new()),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(
        c.tool_results[0].diagnostics[0].message,
        "nested checkpoint"
    );
    assert_eq!(parent.load(Ordering::SeqCst), 2);
    assert_eq!(child.load(Ordering::SeqCst), 1);
    let fork = h.fork(id).unwrap();
    assert_eq!(h.conversation(fork).unwrap().tool_results, c.tool_results);
}

struct Structured(Arc<AtomicUsize>);

struct NestedRewrite {
    cancel: Option<CancellationToken>,
}

struct NestedBeforeChild(Arc<std::sync::Mutex<Vec<Value>>>);
#[async_trait::async_trait]
impl KeyedTool for NestedBeforeChild {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "child".into(),
            description: String::new(),
            parameters: json!({"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]}),
        }
    }
    async fn invoke(
        &self,
        args: Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        _: KeyedHost,
    ) -> pi_rs_core::ToolReply {
        self.0.lock().unwrap().push(args);
        Outcome::Succeeded("child done".into()).into()
    }
}
struct NestedBeforeParent(Arc<std::sync::Mutex<Vec<pi_rs_durable::nested::KeyedResult>>>);
#[async_trait::async_trait]
impl KeyedTool for NestedBeforeParent {
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
        for _ in 0..2 {
            let result = host
                .call_keyed_with_diagnostics("same", "child", json!({"n":1}))
                .await;
            self.0.lock().unwrap().push(result);
        }
        Outcome::Succeeded("parent done".into()).into()
    }
}
struct NestedBeforeHook {
    mode: &'static str,
    before: Arc<AtomicUsize>,
    after: Arc<std::sync::Mutex<Vec<Value>>>,
}
#[async_trait::async_trait]
impl Hook for NestedBeforeHook {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &Context<'_>,
    ) -> Result<pi_rs_durable::extension::BeforeTool, String> {
        use pi_rs_durable::extension::BeforeTool;
        if call.name != "child" {
            return Ok(BeforeTool::Pass);
        }
        self.before.fetch_add(1, Ordering::SeqCst);
        if self.mode == "error" {
            return Err("hook failure".into());
        }
        Ok(match self.mode {
            "rewrite" => BeforeTool::Rewrite(json!({"n":9})),
            "invalid" => BeforeTool::Rewrite(json!({"n":"bad"})),
            "block" => BeforeTool::Block("policy".into()),
            "sleep" => BeforeTool::Sleep(pi_rs_durable::extension::Sleep {
                signals: vec!["wait".into()],
                until_millis: None,
            }),
            _ => BeforeTool::Pass,
        })
    }
    async fn after_tool_result(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &pi_rs_durable::extension::ToolResult,
        _: &Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolResult>, String> {
        if call.name != "child" {
            return Ok(None);
        }
        self.after.lock().unwrap().push(call.arguments.clone());
        if self.mode == "block" || self.mode == "invalid" || self.mode == "error" {
            assert!(result.is_error);
            assert_eq!(
                result.diagnostics[0].code.as_deref(),
                Some(if self.mode == "block" || self.mode == "error" {
                    "blocked"
                } else {
                    "invalid_arguments"
                })
            );
        }
        Ok(None)
    }
}
#[tokio::test]
async fn nested_before_tool_rewrites_or_rejects_once_and_after_tool_observes_decision() {
    for mode in ["pass", "rewrite", "block", "invalid", "error"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let invoked = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let before = Arc::new(AtomicUsize::new(0));
        let after = Arc::new(std::sync::Mutex::new(Vec::new()));
        let options = || Options {
            model: Arc::new(FakeModel("parent")),
            tools: vec![
                Arc::new(KeyedToolAdapter::new(NestedBeforeParent(observed.clone())))
                    .register(ReplayPolicy::Safe),
                Arc::new(KeyedToolAdapter::new(NestedBeforeChild(invoked.clone())))
                    .register(ReplayPolicy::Unsafe),
            ],
            restart_limit: 4,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        };
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.install_extension(Extension {
            name: "nested-before".into(),
            hooks: vec![Arc::new(NestedBeforeHook {
                mode,
                before: before.clone(),
                after: after.clone(),
            })],
            ..Default::default()
        })
        .unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["nested-before".into()])
            .unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(before.load(Ordering::SeqCst), 1, "{mode}");
        assert_eq!(after.lock().unwrap().len(), 1, "{mode}");
        if mode == "error" {
            assert!(
                h.extension_reports()
                    .unwrap()
                    .iter()
                    .any(|batch| batch.phase == "before_tool")
            );
        }
        let expected_args = match mode {
            "rewrite" => json!({"n":9}),
            "invalid" => json!({"n":"bad"}),
            _ => json!({"n":1}),
        };
        assert_eq!(after.lock().unwrap()[0], expected_args);
        let results = observed.lock().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], results[1]);
        if mode == "block" || mode == "invalid" || mode == "error" {
            assert!(invoked.lock().unwrap().is_empty());
            assert!(matches!(results[0].outcome, Outcome::Failed(_)));
        } else {
            assert_eq!(*invoked.lock().unwrap(), vec![expected_args]);
            assert_eq!(results[0].outcome, Outcome::Succeeded("child done".into()));
        }
        drop(results);
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        assert_eq!(h.conversation(id).unwrap().submissions.len(), 1);
    }
}

struct CancelNestedBefore(CancellationToken);
#[async_trait::async_trait]
impl Hook for CancelNestedBefore {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &Context<'_>,
    ) -> Result<pi_rs_durable::extension::BeforeTool, String> {
        if call.name == "child" {
            self.0.cancel();
            std::future::pending::<()>().await;
        }
        Ok(pi_rs_durable::extension::BeforeTool::Pass)
    }
}
#[tokio::test]
async fn interrupted_nested_before_tool_never_dispatches_unsafe_child() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let invoked = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let options = || Options {
        model: Arc::new(FakeModel("parent")),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(NestedBeforeParent(observed.clone())))
                .register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(NestedBeforeChild(invoked.clone())))
                .register(ReplayPolicy::Unsafe),
        ],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.install_extension(Extension {
            name: "cancel-before".into(),
            hooks: vec![Arc::new(CancelNestedBefore(cancel.clone()))],
            ..Default::default()
        })
        .unwrap();
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["cancel-before".into()])
            .unwrap();
        assert!(
            h.submit_and_run(id, None, "go".into(), &cancel)
                .await
                .is_err()
        );
    }
    let h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.blocked(id).unwrap().is_some());
    assert!(invoked.lock().unwrap().is_empty());
}

#[tokio::test]
async fn nested_before_tool_sleep_fails_closed_before_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let invoked = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let options = Options {
        model: Arc::new(FakeModel("parent")),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(NestedBeforeParent(observed)))
                .register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(NestedBeforeChild(invoked.clone())))
                .register(ReplayPolicy::Unsafe),
        ],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open_paused(dir.path().join("db"), options).unwrap();
    h.install_extension(Extension {
        name: "sleep".into(),
        hooks: vec![Arc::new(NestedBeforeHook {
            mode: "sleep",
            before: Arc::new(AtomicUsize::new(0)),
            after: Arc::default(),
        })],
        ..Default::default()
    })
    .unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["sleep".into()]).unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert!(h.blocked(id).unwrap().is_some());
    assert!(invoked.lock().unwrap().is_empty());
}
#[async_trait::async_trait]
impl Hook for NestedRewrite {
    async fn after_tool_result(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &pi_rs_durable::extension::ToolResult,
        _: &Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolResult>, String> {
        if call.name != "child" {
            return Ok(None);
        }
        assert_eq!(result.diagnostics[0].message, "nested checkpoint");
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
            std::future::pending::<()>().await;
        }
        let mut safe = result.clone();
        safe.output = "rewritten child".into();
        safe.diagnostics = vec![diagnostic("safe child")];
        Ok(Some(safe))
    }
}
struct CaptureRewritten;
#[async_trait::async_trait]
impl KeyedTool for CaptureRewritten {
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
        let result = host
            .call_keyed_with_diagnostics("child-key", "child", json!({}))
            .await;
        assert_eq!(result.outcome, Outcome::Succeeded("rewritten child".into()));
        assert_eq!(result.diagnostics, vec![diagnostic("safe child")]);
        for diagnostic in result.diagnostics {
            host.diagnostic(diagnostic).unwrap();
        }
        result.outcome.into()
    }
}
#[tokio::test]
async fn interrupted_nested_result_hook_is_blocked_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let count = Arc::new(AtomicUsize::new(0));
    let options = || Options {
        model: Arc::new(FakeModel("parent")),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(CaptureRewritten)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(Child(count.clone()))).register(ReplayPolicy::Unsafe),
        ],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.install_extension(Extension {
            name: "nested-rewrite".into(),
            hooks: vec![Arc::new(NestedRewrite {
                cancel: Some(cancel.clone()),
            })],
            ..Default::default()
        })
        .unwrap();
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["nested-rewrite".into()])
            .unwrap();
        assert!(
            h.submit_and_run(id, None, "go".into(), &cancel)
                .await
                .is_err()
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_extension(Extension {
        name: "nested-rewrite".into(),
        hooks: vec![Arc::new(NestedRewrite { cancel: None })],
        ..Default::default()
    })
    .unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(h.blocked(id).unwrap().is_some());
    assert!(h.conversation(id).unwrap().tool_results.is_empty());
}

#[tokio::test]
async fn nested_result_hook_rewrites_before_parent_observes_result() {
    let dir = tempfile::tempdir().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let options = Options {
        model: Arc::new(FakeModel("parent")),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(CaptureRewritten)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(Child(count.clone()))).register(ReplayPolicy::Unsafe),
        ],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open_paused(dir.path().join("db"), options).unwrap();
    h.install_extension(Extension {
        name: "nested-rewrite".into(),
        hooks: vec![Arc::new(NestedRewrite { cancel: None })],
        ..Default::default()
    })
    .unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["nested-rewrite".into()])
        .unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.conversation(id).unwrap().tool_results[0].diagnostics,
        vec![diagnostic("safe child")]
    );
}

struct PollChild;
#[async_trait::async_trait]
impl pi_rs_core::Tool for PollChild {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Pending {
            reference: "job".into(),
        }
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        Some(Outcome::Succeeded("raw".into()))
    }
}
struct PollRewrite;
#[async_trait::async_trait]
impl Hook for PollRewrite {
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        Ok((call.name == "child").then(|| json!("safe")))
    }
}
struct PollParent;
#[async_trait::async_trait]
impl KeyedTool for PollParent {
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
        let result = host
            .call_keyed_with_diagnostics("child", "child", json!({}))
            .await;
        assert_eq!(result.outcome, Outcome::Succeeded("safe".into()));
        assert_eq!(
            result.diagnostics[0].code.as_deref(),
            Some("output_redacted")
        );
        Outcome::Succeeded("parent".into()).into()
    }
}
#[tokio::test]
async fn polled_nested_result_runs_legacy_hook_before_parent() {
    let dir = tempfile::tempdir().unwrap();
    let options = Options {
        model: Arc::new(FakeModel("parent")),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(PollParent)).register(ReplayPolicy::Safe),
            pi_rs_durable::runtime::RegisteredTool {
                tool: Arc::new(PollChild),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open_paused(dir.path().join("db"), options).unwrap();
    h.install_extension(Extension {
        name: "poll-rewrite".into(),
        hooks: vec![Arc::new(PollRewrite)],
        ..Default::default()
    })
    .unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["poll-rewrite".into()])
        .unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert!(h.blocked(id).unwrap().is_none());
}
#[async_trait::async_trait]
impl KeyedTool for Structured {
    fn spec(&self) -> ToolSpec {
        spec("job")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        host: KeyedHost,
    ) -> pi_rs_core::ToolReply {
        self.0.fetch_add(1, Ordering::SeqCst);
        host.diagnostic(diagnostic("raw diagnostic secret"))
            .unwrap();
        Outcome::Structured {
            data: json!({"secret":"raw structured secret"}),
            attachments: vec![],
        }
        .into()
    }
}
struct Rewrite {
    cancel: Option<CancellationToken>,
    invalid: bool,
}
#[async_trait::async_trait]
impl Hook for Rewrite {
    async fn after_tool_result(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        result: &pi_rs_durable::extension::ToolResult,
        _: &Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolResult>, String> {
        assert_eq!(
            result.structured_output.as_ref().unwrap()["secret"],
            "raw structured secret"
        );
        let mut result = result.clone();
        result.output = "safe output".into();
        result.structured_output = Some(if self.invalid {
            json!({"safe":"wrong"})
        } else {
            json!({"safe":7})
        });
        result.details = Some(json!({"redacted":true}));
        result.diagnostics = vec![diagnostic("safe diagnostic")];
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
        }
        Ok(Some(result))
    }
}
#[tokio::test]
async fn explicit_result_rewrite_is_validated_and_survives_hook_interruption() {
    for invalid in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let count = Arc::new(AtomicUsize::new(0));
        let cancel = CancellationToken::new();
        let id;
        let options = || Options {
            model: Arc::new(FakeModel("job")),
            tools: vec![
                Arc::new(KeyedToolAdapter::new(Structured(count.clone())))
                    .register(ReplayPolicy::Unsafe),
            ],
            restart_limit: 3,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        };
        {
            let mut h = Harness::open_paused(&path, options()).unwrap();
            h.install_extension(Extension {
                name: "rewrite".into(),
                hooks: vec![Arc::new(Rewrite {
                    cancel: Some(cancel.clone()),
                    invalid,
                })],
                ..Default::default()
            })
            .unwrap();
            id = h.create_conversation(config()).unwrap();
            h.select_extensions(id, vec!["rewrite".into()]).unwrap();
            h.set_tool_output_schema(id,"job".into(),Some(json!({"type":"object","properties":{"safe":{"type":"integer"}},"required":["safe"]}))).unwrap();
            assert!(
                h.submit_and_run(id, None, "go".into(), &cancel)
                    .await
                    .is_err()
            );
        }
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.install_extension(Extension {
            name: "rewrite".into(),
            hooks: vec![Arc::new(Rewrite {
                cancel: None,
                invalid,
            })],
            ..Default::default()
        })
        .unwrap();
        assert!(h.conversation(id).unwrap().tool_results.is_empty());
        h.resume_conversation(id, &CancellationToken::new())
            .await
            .unwrap();
        let c = h.conversation(id).unwrap();
        let result = &c.tool_results[0];
        if invalid {
            assert!(result.structured_output.is_none());
            assert!(
                result
                    .diagnostics
                    .iter()
                    .any(|d| d.code.as_deref() == Some("invalid_structured_output"))
            );
        } else {
            assert_eq!(result.structured_output.as_ref().unwrap()["safe"], 7);
        }
        assert_eq!(result.details.as_ref().unwrap()["redacted"], true);
        assert_eq!(result.diagnostics[0].message, "safe diagnostic");
        assert!(
            c.messages
                .iter()
                .any(|m| matches!(m,Message::Tool { content,.. } if content.starts_with("safe output\n<harness>\n") && content.contains("safe diagnostic")))
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
            .any(|r| r.value.to_string().contains("raw structured secret")
                || r.value.to_string().contains("raw diagnostic secret"))
        );
        let fork = h.fork(id).unwrap();
        assert_eq!(h.conversation(fork).unwrap().tool_results, c.tool_results);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}

struct SavedHost(Arc<std::sync::Mutex<Option<KeyedHost>>>);
#[async_trait::async_trait]
impl KeyedTool for SavedHost {
    fn spec(&self) -> ToolSpec {
        spec("job")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        host: KeyedHost,
    ) -> pi_rs_core::ToolReply {
        host.diagnostic(diagnostic("in time")).unwrap();
        *self.0.lock().unwrap() = Some(host);
        Outcome::Succeeded("done".into()).into()
    }
}
#[tokio::test]
async fn completed_attempt_rejects_late_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let saved = Arc::new(std::sync::Mutex::new(None));
    let options = Options {
        model: Arc::new(FakeModel("job")),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(SavedHost(saved.clone())))
                .register(ReplayPolicy::Unsafe),
        ],
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open_paused(dir.path().join("db"), options).unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert!(
        saved
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .diagnostic(diagnostic("too late"))
            .is_err()
    );
    let c = h.conversation(id).unwrap();
    assert_eq!(c.tool_results[0].diagnostics.len(), 1);
    assert_eq!(c.tool_results[0].diagnostics[0].message, "in time");
}

struct SavedNestedParent(Arc<std::sync::Mutex<Option<KeyedHost>>>);
#[async_trait::async_trait]
impl KeyedTool for SavedNestedParent {
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
        let child = host
            .call_keyed_with_diagnostics("child-key", "child", json!({}))
            .await;
        assert_eq!(child.diagnostics.len(), 1);
        *self.0.lock().unwrap() = Some(host);
        Outcome::Succeeded("done".into()).into()
    }
}
#[tokio::test]
async fn sealed_parent_cannot_read_nested_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let saved = Arc::new(std::sync::Mutex::new(None));
    let child_count = Arc::new(AtomicUsize::new(0));
    let options = Options {
        model: Arc::new(FakeModel("parent")),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(SavedNestedParent(saved.clone())))
                .register(ReplayPolicy::Unsafe),
            Arc::new(KeyedToolAdapter::new(Child(child_count.clone())))
                .register(ReplayPolicy::Unsafe),
        ],
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open_paused(dir.path().join("db"), options).unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    let host = saved.lock().unwrap().as_ref().unwrap().clone();
    let later = host
        .call_keyed_with_diagnostics("child-key", "child", json!({}))
        .await;
    assert!(matches!(later.outcome, Outcome::Unknown(_)));
    assert!(later.diagnostics.is_empty());
    assert_eq!(child_count.load(Ordering::SeqCst), 1);
}

struct InvalidChild;
#[async_trait::async_trait]
impl KeyedTool for InvalidChild {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        _: KeyedHost,
    ) -> pi_rs_core::ToolReply {
        Outcome::Structured {
            data: json!("invalid"),
            attachments: vec![],
        }
        .into()
    }
}
struct CaptureChild(Arc<std::sync::Mutex<Option<pi_rs_durable::nested::KeyedResult>>>);
#[async_trait::async_trait]
impl KeyedTool for CaptureChild {
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
        let result = host
            .call_keyed_with_diagnostics("child-key", "child", json!({}))
            .await;
        *self.0.lock().unwrap() = Some(result);
        Outcome::Succeeded("done".into()).into()
    }
}
#[tokio::test]
async fn nested_schema_failure_includes_error_diagnostic() {
    let dir = tempfile::tempdir().unwrap();
    let captured = Arc::new(std::sync::Mutex::new(None));
    let options = Options {
        model: Arc::new(FakeModel("parent")),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(CaptureChild(captured.clone())))
                .register(ReplayPolicy::Unsafe),
            Arc::new(KeyedToolAdapter::new(InvalidChild)).register(ReplayPolicy::Unsafe),
        ],
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open_paused(dir.path().join("db"), options).unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.set_tool_output_schema(id, "child".into(), Some(json!({"type":"integer"})))
        .unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    let result = captured.lock().unwrap();
    let result = result.as_ref().unwrap();
    assert!(matches!(result.outcome, Outcome::Failed(_)));
    assert_eq!(
        result.diagnostics[0].code.as_deref(),
        Some("invalid_structured_output")
    );
    assert_eq!(result.diagnostics[0].severity, DiagnosticSeverity::Error);
}

#[tokio::test]
async fn nested_block_redaction_rewrites_failure_outcome_and_fallback_after_reopen() {
    use pi_rs_durable::extension::{ToolBlockResult, ToolCall as HookCall};
    use std::sync::Mutex;
    struct ModelCapture(Arc<Mutex<Vec<ModelRequest>>>);
    #[async_trait::async_trait]
    impl Model for ModelCapture {
        async fn stream(&self, request: ModelRequest) -> pi_rs_core::Result<ModelStream> {
            self.0.lock().unwrap().push(request.clone());
            FakeModel("parent").stream(request).await
        }
    }
    struct Child(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl KeyedTool for Child {
        fn spec(&self) -> ToolSpec {
            spec("child")
        }
        async fn invoke(
            &self,
            _: Value,
            _: ToolContext,
            _: Option<&pi_rs_core::ExtensionState>,
            _: KeyedHost,
        ) -> pi_rs_core::ToolReply {
            self.0.fetch_add(1, Ordering::SeqCst);
            Outcome::Failed("child secret".into()).into()
        }
    }
    struct Parent;
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
            for _ in 0..2 {
                let child = host.call_keyed_with_blocks("key", "child", json!({})).await;
                assert_eq!(child.outcome, Outcome::Failed("redacted".into()));
                assert_eq!(
                    child.output_blocks,
                    Some(vec![json!({"type":"text","text":"redacted"})])
                );
                assert_eq!(child.diagnostics.len(), 1);
                assert_eq!(child.diagnostics[0].message, "redacted");
                assert_eq!(child.diagnostics[0].code.as_deref(), Some("tool_error"));
            }
            let child = host.call_keyed_with_blocks("key", "child", json!({})).await;
            for diagnostic in child.diagnostics {
                host.diagnostic(diagnostic).unwrap();
            }
            child.outcome.into()
        }
    }
    struct Redact;
    #[async_trait::async_trait]
    impl Hook for Redact {
        fn supports_runtime_tool_blocks(&self) -> bool {
            true
        }
        async fn after_tool_blocks(
            &self,
            call: &HookCall,
            result: &ToolBlockResult,
            nested: bool,
            _: &Context<'_>,
        ) -> Result<Option<ToolBlockResult>, String> {
            if !nested || call.name != "child" {
                return Ok(None);
            }
            let mut value = result.clone();
            value.output = vec![json!({"type":"text","text":"redacted"})];
            Ok(Some(value))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let options = || Options {
        model: Arc::new(ModelCapture(requests.clone())),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(Parent)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(Child(calls.clone()))).register(ReplayPolicy::Unsafe),
        ],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_extension(Extension {
        name: "redact".into(),
        hooks: vec![Arc::new(Redact)],
        ..Default::default()
    })
    .unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["redact".into()]).unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let requests = requests.lock().unwrap();
    let actual = requests
        .iter()
        .flat_map(|r| &r.messages)
        .find_map(|m| match m {
            Message::Tool { content, .. } => Some(content),
            _ => None,
        })
        .unwrap();
    assert!(actual.contains("redacted"));
    assert!(!actual.contains("child secret"));
    drop(requests);
    let before = h.conversation(id).unwrap();
    assert!(!format!("{before:?}").contains("child secret"));
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(id).unwrap().messages, before.messages);
    assert!(!format!("{:?}", h.conversation(id).unwrap()).contains("child secret"));
    drop(h);
    let store = durable_store::Store::open(&path).unwrap();
    for prefix in [b"runtime/".as_slice(), b"nested/".as_slice()] {
        let entries = store.list(prefix).unwrap();
        if prefix == b"nested/" {
            assert!(!entries.is_empty());
        }
        for (_, entry) in entries {
            assert!(!String::from_utf8_lossy(&entry.value).contains("child secret"));
        }
    }
}

#[tokio::test]
async fn nested_structured_block_projection_repeated_reads_and_reopen() {
    use pi_rs_durable::extension::{ToolBlockResult, ToolCall as HookCall};
    struct StructuredChild(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl KeyedTool for StructuredChild {
        fn spec(&self) -> ToolSpec {
            spec("child")
        }
        async fn invoke(
            &self,
            _: Value,
            _: ToolContext,
            _: Option<&pi_rs_core::ExtensionState>,
            _: KeyedHost,
        ) -> pi_rs_core::ToolReply {
            self.0.fetch_add(1, Ordering::SeqCst);
            Outcome::Structured {
                data: json!({"n": 7}),
                attachments: vec![],
            }
            .into()
        }
    }
    struct ParentReplay {
        calls: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl KeyedTool for ParentReplay {
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
            for _ in 0..2 {
                let result = host.call_keyed_with_blocks("key", "child", json!({})).await;
                assert_eq!(result.outcome, Outcome::Succeeded("safe".into()));
                assert_eq!(
                    result.output_blocks,
                    Some(vec![json!({"type":"text","text":"safe"})])
                );
                assert_eq!(result.diagnostics, vec![diagnostic("preserved")]);
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            Outcome::Succeeded("parent safe".into()).into()
        }
    }
    struct Block;
    #[async_trait::async_trait]
    impl Hook for Block {
        fn supports_runtime_tool_blocks(&self) -> bool {
            true
        }
        async fn after_tool_blocks(
            &self,
            call: &HookCall,
            result: &ToolBlockResult,
            nested: bool,
            _: &Context<'_>,
        ) -> Result<Option<ToolBlockResult>, String> {
            if !nested || call.name != "child" {
                return Ok(None);
            }
            let mut value = result.clone();
            value.output = vec![json!({"type":"text","text":"safe"})];
            value.diagnostics.push(diagnostic("preserved"));
            Ok(Some(value))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let parent = Arc::new(AtomicUsize::new(0));
    let child = Arc::new(AtomicUsize::new(0));
    let cancel = CancellationToken::new();
    let options = |_token: CancellationToken| Options {
        model: Arc::new(FakeModel("parent")),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(ParentReplay {
                calls: parent.clone(),
            }))
            .register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(StructuredChild(child.clone())))
                .register(ReplayPolicy::Unsafe),
        ],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let extension = || Extension {
        name: "block".into(),
        hooks: vec![Arc::new(Block)],
        ..Default::default()
    };
    let id;
    {
        let mut h = Harness::open_paused(&path, options(cancel.clone())).unwrap();
        h.install_extension(extension()).unwrap();
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["block".into()]).unwrap();
        h.set_tool_output_schema(
            id,
            "child".into(),
            Some(json!({"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]})),
        )
        .unwrap();
        h.submit_and_run(id, None, "go".into(), &cancel)
            .await
            .unwrap();
    }
    let h = Harness::open_paused(&path, options(CancellationToken::new())).unwrap();
    assert_eq!(child.load(Ordering::SeqCst), 1);
    assert_eq!(parent.load(Ordering::SeqCst), 1);
    assert!(
        h.conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { content, .. } if content == "parent safe"))
    );
    drop(h);
    let store = durable_store::Store::open(&path).unwrap();
    let records = store.list(b"nested/").unwrap();
    assert!(
        records.iter().any(
            |(_, entry)| String::from_utf8_lossy(&entry.value).contains("projected_structured")
        )
    );
}

#[tokio::test]
async fn nested_gate_failure_diagnostics_are_redacted_before_parent_model_request() {
    use pi_rs_durable::extension::{BeforeTool, ToolBlockResult, ToolCall as HookCall, ToolResult};
    use std::sync::Mutex;
    struct Capture(Arc<Mutex<Vec<ModelRequest>>>);
    #[async_trait::async_trait]
    impl Model for Capture {
        async fn stream(&self, req: ModelRequest) -> pi_rs_core::Result<ModelStream> {
            self.0.lock().unwrap().push(req.clone());
            FakeModel("parent").stream(req).await
        }
    }
    struct Never;
    #[async_trait::async_trait]
    impl KeyedTool for Never {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "child".into(),
                description: String::new(),
                parameters: json!({"type":"object","properties":{"secret":{"type":"integer"}},"required":["secret"]}),
            }
        }
        async fn invoke(
            &self,
            _: Value,
            _: ToolContext,
            _: Option<&pi_rs_core::ExtensionState>,
            _: KeyedHost,
        ) -> pi_rs_core::ToolReply {
            panic!("must not dispatch")
        }
    }
    struct ParentTool;
    #[async_trait::async_trait]
    impl KeyedTool for ParentTool {
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
            let args = json!({"secret": 1});
            for _ in 0..2 {
                let child = host
                    .call_keyed_with_blocks("key", "child", args.clone())
                    .await;
                assert_eq!(child.outcome, Outcome::Failed("safe".into()));
                assert!(!format!("{child:?}").contains("private"));
                for diagnostic in child.diagnostics {
                    host.diagnostic(diagnostic).unwrap();
                }
            }
            Outcome::Succeeded("parent safe".into()).into()
        }
    }
    struct Redact {
        blocked: bool,
        rich: bool,
    }
    #[async_trait::async_trait]
    impl Hook for Redact {
        async fn before_tool(
            &self,
            call: &HookCall,
            _: &Context<'_>,
        ) -> Result<BeforeTool, String> {
            Ok(if call.name != "child" {
                BeforeTool::Pass
            } else if self.blocked {
                BeforeTool::Block("private-block".into())
            } else {
                BeforeTool::Rewrite(json!({"secret":"private-invalid"}))
            })
        }
        async fn after_tool_result(
            &self,
            call: &HookCall,
            result: &ToolResult,
            _: &Context<'_>,
        ) -> Result<Option<ToolResult>, String> {
            if call.name != "child" || !self.rich {
                return Ok(None);
            }
            let mut result = result.clone();
            result.output = "safe".into();
            Ok(Some(result))
        }
        fn supports_runtime_tool_blocks(&self) -> bool {
            !self.rich
        }
        async fn after_tool_blocks(
            &self,
            call: &HookCall,
            result: &ToolBlockResult,
            nested: bool,
            _: &Context<'_>,
        ) -> Result<Option<ToolBlockResult>, String> {
            if !nested || call.name != "child" {
                return Ok(None);
            }
            let mut result = result.clone();
            result.output = vec![json!({"type":"text","text":"safe"})];
            Ok(Some(result))
        }
    }
    for (blocked, rich) in [(true, true), (true, false), (false, true), (false, false)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let options = || Options {
            model: Arc::new(Capture(requests.clone())),
            tools: vec![
                Arc::new(KeyedToolAdapter::new(ParentTool)).register(ReplayPolicy::Safe),
                Arc::new(KeyedToolAdapter::new(Never)).register(ReplayPolicy::Unsafe),
            ],
            restart_limit: 4,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        };
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.install_extension(Extension {
            name: "redact".into(),
            hooks: vec![Arc::new(Redact { blocked, rich })],
            ..Default::default()
        })
        .unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["redact".into()]).unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        let requests = requests.lock().unwrap();
        let actual = requests
            .iter()
            .flat_map(|r| &r.messages)
            .find_map(|m| match m {
                Message::Tool { content, .. } => Some(content),
                _ => None,
            })
            .unwrap();
        assert!(actual.contains("parent safe"));
        assert!(!actual.contains("private"));
        drop(requests);
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        assert!(!format!("{:?}", h.conversation(id).unwrap()).contains("private"));
    }
}
