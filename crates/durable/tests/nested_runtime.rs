use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    CancellationToken, Error, ExtensionState, Message, Model, ModelEvent, ModelRequest,
    ModelStream, Outcome, Tool, ToolCall, ToolContext, ToolHost, ToolSpec,
};
use pi_rs_durable::{
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, Harness, Options, RegisteredTool, Status},
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
struct FakeModel;
#[async_trait]
impl Model for FakeModel {
    async fn stream(&self, req: ModelRequest) -> Result<ModelStream, Error> {
        let done = req
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }));
        let calls = if done {
            vec![]
        } else {
            vec![ToolCall {
                id: "p".into(),
                name: "parent".into(),
                arguments: "{}".into(),
            }]
        };
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text(if done { "done" } else { "" }.into())),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])
        .boxed())
    }
}
struct Parent;
#[async_trait]
impl Tool for Parent {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "parent".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Failed("host required".into())
    }
    async fn invoke_with_host(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> Outcome {
        let _ignored = host.call("child", json!({})).await;
        Outcome::Succeeded("parent says ok".into())
    }
}
struct Child {
    count: AtomicUsize,
    unknown: bool,
}
#[async_trait]
impl Tool for Child {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "child".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        self.count.fetch_add(1, Ordering::SeqCst);
        if self.unknown {
            Outcome::Unknown("lost".into())
        } else {
            Outcome::Succeeded("done".into())
        }
    }
}
#[tokio::test]
async fn parent_cannot_hide_uncertain_nested_effect() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let child = Arc::new(Child {
        count: AtomicUsize::new(0),
        unknown: true,
    });
    let opts = Options {
        model: Arc::new(FakeModel),
        tools: vec![
            RegisteredTool {
                tool: Arc::new(Parent),
                replay: ReplayPolicy::Safe,
            },
            RegisteredTool {
                tool: child.clone(),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open(dir.path().join("db"), opts, &cancel)
        .await
        .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.submit_and_run(id, None, "work".into(), &cancel)
        .await
        .unwrap();
    assert!(h.blocked(id).unwrap().is_some());
    assert_eq!(child.count.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Running
    );
}
#[tokio::test]
async fn known_nested_result_allows_parent_to_finish() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let child = Arc::new(Child {
        count: AtomicUsize::new(0),
        unknown: false,
    });
    let opts = Options {
        model: Arc::new(FakeModel),
        tools: vec![
            RegisteredTool {
                tool: Arc::new(Parent),
                replay: ReplayPolicy::Safe,
            },
            RegisteredTool {
                tool: child.clone(),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open(dir.path().join("db"), opts, &cancel)
        .await
        .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.submit_and_run(id, None, "work".into(), &cancel)
        .await
        .unwrap();
    assert!(h.blocked(id).unwrap().is_none());
    assert_eq!(child.count.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "done".into()
        }
    );
}
struct InterruptingParent {
    executions: AtomicUsize,
    cancel: CancellationToken,
}
#[async_trait]
impl Tool for InterruptingParent {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "parent".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Failed("host required".into())
    }
    async fn invoke_with_host(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> Outcome {
        let result = host.call("child", json!({})).await;
        if self.executions.fetch_add(1, Ordering::SeqCst) == 0 {
            self.cancel.cancel();
            std::future::pending().await
        } else {
            result
        }
    }
}
#[tokio::test]
async fn parent_replay_reuses_committed_child_result() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let parent = Arc::new(InterruptingParent {
        executions: AtomicUsize::new(0),
        cancel: cancel.clone(),
    });
    let child = Arc::new(Child {
        count: AtomicUsize::new(0),
        unknown: false,
    });
    let opts = || Options {
        model: Arc::new(FakeModel),
        tools: vec![
            RegisteredTool {
                tool: parent.clone(),
                replay: ReplayPolicy::Safe,
            },
            RegisteredTool {
                tool: child.clone(),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let id;
    {
        let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        assert!(
            h.submit_and_run(id, None, "work".into(), &cancel)
                .await
                .is_err()
        );
    }
    let h = Harness::open(&path, opts(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "done".into()
        }
    );
    assert_eq!(child.count.load(Ordering::SeqCst), 1);
    assert_eq!(parent.executions.load(Ordering::SeqCst), 2);
}

struct DetachedParent {
    started: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl Tool for DetachedParent {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "parent".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Failed("host required".into())
    }
    async fn invoke_with_host(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> Outcome {
        tokio::spawn(async move {
            let _ = host.call("child", json!({})).await;
        });
        self.started.notified().await;
        Outcome::Succeeded("ignored detached child".into())
    }
}
struct WaitingChild {
    started: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl Tool for WaitingChild {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "child".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        self.started.notify_one();
        std::future::pending().await
    }
}
#[tokio::test]
async fn detached_child_is_stopped_before_parent_can_be_reconciled() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let opts = Options {
        model: Arc::new(FakeModel),
        tools: vec![
            RegisteredTool {
                tool: Arc::new(DetachedParent {
                    started: started.clone(),
                }),
                replay: ReplayPolicy::Safe,
            },
            RegisteredTool {
                tool: Arc::new(WaitingChild { started }),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open(dir.path().join("db"), opts, &cancel)
        .await
        .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        h.submit_and_run(id, None, "work".into(), &cancel),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(h.blocked(id).unwrap().is_some());
    assert!(!cancel.is_cancelled());
}
struct PanickingParent {
    started: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl Tool for PanickingParent {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "parent".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Failed("host required".into())
    }
    async fn invoke_with_host(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> Outcome {
        tokio::spawn(async move {
            let _ = host.call("child", json!({})).await;
        });
        self.started.notified().await;
        panic!("parent failed after starting nested call")
    }
}
#[tokio::test]
async fn panicking_parent_cannot_leave_nested_call_running() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let opts = Options {
        model: Arc::new(FakeModel),
        tools: vec![
            RegisteredTool {
                tool: Arc::new(PanickingParent {
                    started: started.clone(),
                }),
                replay: ReplayPolicy::Safe,
            },
            RegisteredTool {
                tool: Arc::new(WaitingChild { started }),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open(dir.path().join("db"), opts, &cancel)
        .await
        .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        h.submit_and_run(id, None, "work".into(), &cancel),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(h.blocked(id).unwrap().is_some());
}

struct KeyedParent {
    executions: AtomicUsize,
    cancel: CancellationToken,
    change: bool,
}
#[async_trait]
impl pi_rs_durable::keyed_tool::KeyedTool for KeyedParent {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "parent".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: pi_rs_durable::keyed_tool::KeyedHost,
    ) -> pi_rs_core::ToolReply {
        let n = self.executions.fetch_add(1, Ordering::SeqCst);
        let keys = if n == 0 {
            ["first", "second"]
        } else {
            ["second", "first"]
        };
        for key in keys {
            let value = if self.change && n > 0 && key == "first" {
                99
            } else if key == "first" {
                1
            } else {
                2
            };
            let _ = host.call_keyed(key, "child", json!({"value":value})).await;
        }
        if n == 0 {
            self.cancel.cancel();
            std::future::pending::<()>().await;
        }
        Outcome::Succeeded("parent".into()).into()
    }
}

async fn keyed_reopen(change: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let parent = Arc::new(pi_rs_durable::keyed_tool::KeyedToolAdapter::new(
        KeyedParent {
            executions: AtomicUsize::new(0),
            cancel: cancel.clone(),
            change,
        },
    ));
    let child = Arc::new(Child {
        count: AtomicUsize::new(0),
        unknown: false,
    });
    let opts = || Options {
        model: Arc::new(FakeModel),
        tools: vec![
            parent.register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: child.clone(),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let id;
    {
        let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        assert!(
            h.submit_and_run(id, None, "work".into(), &cancel)
                .await
                .is_err()
        );
    }
    let h = Harness::open(&path, opts(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(child.count.load(Ordering::SeqCst), 2);
    assert_eq!(parent.0.executions.load(Ordering::SeqCst), 2);
    if change {
        assert!(h.blocked(id).unwrap().is_some());
    } else {
        assert_eq!(
            h.conversation(id).unwrap().submissions[0].status,
            Status::Done {
                answer: "done".into()
            }
        );
    }
}

#[tokio::test]
async fn keyed_runtime_reorders_after_reopen_without_reexecuting_children() {
    keyed_reopen(false).await;
}
#[tokio::test]
async fn keyed_runtime_changed_arguments_block_even_if_ignored() {
    keyed_reopen(true).await;
}

#[tokio::test]
async fn keyed_adapter_fails_closed_without_durable_dispatch() {
    struct EmptyHost;
    #[async_trait]
    impl ToolHost for EmptyHost {
        fn tools(&self) -> Vec<ToolSpec> {
            vec![]
        }
        async fn call(&self, _: &str, _: Value) -> Outcome {
            panic!("must not dispatch")
        }
    }
    let tool = pi_rs_durable::keyed_tool::KeyedToolAdapter::new(KeyedParent {
        executions: AtomicUsize::new(0),
        cancel: CancellationToken::new(),
        change: false,
    });
    let result = tool
        .invoke_transaction(
            json!({}),
            ToolContext {
                operation_id: uuid::Uuid::new_v4(),
                cancellation: CancellationToken::new(),
            },
            None,
            Arc::new(EmptyHost),
        )
        .await;
    assert!(matches!(result.outcome, Outcome::Skipped(_)));
    assert_eq!(tool.0.executions.load(Ordering::SeqCst), 0);
}

// A normal tool must not be able to turn its ambient task-local scope into a
// keyed invocation by calling an adapter directly instead of dispatching it.
struct BorrowingParent {
    adapter: Arc<pi_rs_durable::keyed_tool::KeyedToolAdapter<KeyedParent>>,
}
#[async_trait]
impl Tool for BorrowingParent {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "parent".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
    async fn invoke_transaction(
        &self,
        args: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> pi_rs_core::ToolReply {
        self.adapter
            .invoke_transaction(args, context, state, host)
            .await
    }
}
#[tokio::test]
async fn ordinary_tool_cannot_borrow_keyed_adapter_scope() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let adapter = Arc::new(pi_rs_durable::keyed_tool::KeyedToolAdapter::new(
        KeyedParent {
            executions: AtomicUsize::new(0),
            cancel: cancel.clone(),
            change: false,
        },
    ));
    let child = Arc::new(Child {
        count: AtomicUsize::new(0),
        unknown: false,
    });
    let opts = Options {
        model: Arc::new(FakeModel),
        tools: vec![
            RegisteredTool {
                tool: Arc::new(BorrowingParent {
                    adapter: adapter.clone(),
                }),
                replay: ReplayPolicy::Safe,
            },
            RegisteredTool {
                tool: child.clone(),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open(dir.path().join("db"), opts, &cancel)
        .await
        .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.submit_and_run(id, None, "work".into(), &cancel)
        .await
        .unwrap();
    assert_eq!(adapter.0.executions.load(Ordering::SeqCst), 0);
    assert_eq!(child.count.load(Ordering::SeqCst), 0);
    assert!(matches!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
}

// A wrapper's first field shares its data address with the enclosing tool.
#[repr(C)]
struct EmbeddedParent(pi_rs_durable::keyed_tool::KeyedToolAdapter<KeyedParent>);
#[async_trait]
impl Tool for EmbeddedParent {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "parent".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
    async fn invoke_transaction(
        &self,
        args: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> pi_rs_core::ToolReply {
        self.0.invoke_transaction(args, context, state, host).await
    }
}
#[tokio::test]
async fn embedded_adapter_cannot_borrow_enclosing_tool_scope() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let parent = Arc::new(EmbeddedParent(
        pi_rs_durable::keyed_tool::KeyedToolAdapter::new(KeyedParent {
            executions: AtomicUsize::new(0),
            cancel: cancel.clone(),
            change: false,
        }),
    ));
    let child = Arc::new(Child {
        count: AtomicUsize::new(0),
        unknown: false,
    });
    let opts = Options {
        model: Arc::new(FakeModel),
        tools: vec![
            RegisteredTool {
                tool: parent.clone(),
                replay: ReplayPolicy::Safe,
            },
            RegisteredTool {
                tool: child.clone(),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open(dir.path().join("db"), opts, &cancel)
        .await
        .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.submit_and_run(id, None, "work".into(), &cancel)
        .await
        .unwrap();
    assert_eq!(parent.0.0.executions.load(Ordering::SeqCst), 0);
    assert_eq!(child.count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn model_selection_survives_reopen_and_keeps_nested_tools_callable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let child = Arc::new(Child {
        count: AtomicUsize::new(0),
        unknown: false,
    });
    let options = || Options {
        model: Arc::new(FakeModel),
        tools: vec![
            RegisteredTool {
                tool: Arc::new(Parent),
                replay: ReplayPolicy::Safe,
            },
            RegisteredTool {
                tool: child.clone(),
                replay: ReplayPolicy::Safe,
            },
        ],
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_tools(id, Some(vec!["parent".into(), "child".into()]))
        .unwrap();
    h.select_model_tools(id, Some(vec!["parent".into(), "missing".into()]))
        .unwrap();
    let fork = h.fork(id).unwrap();
    assert_eq!(
        h.conversation(fork).unwrap().model_tools,
        h.conversation(id).unwrap().model_tools
    );
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        h.conversation(id).unwrap().model_tools,
        Some(vec!["parent".into(), "missing".into()])
    );
    h.submit_and_run(id, None, "work".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(child.count.load(Ordering::SeqCst), 1);
}

struct PanicAfterEffect(Arc<AtomicUsize>);
#[async_trait]
impl Tool for PanicAfterEffect {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "parent".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        struct SensitivePayload;
        impl Drop for SensitivePayload {
            fn drop(&mut self) {
                panic!("secret payload destructor");
            }
        }
        self.0.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(SensitivePayload)
    }
}

#[tokio::test]
async fn tool_panic_report_is_redacted_deduplicated_and_never_redispatches() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let cancel = CancellationToken::new();
    let count = Arc::new(AtomicUsize::new(0));
    let opts = Options {
        model: Arc::new(FakeModel),
        tools: vec![RegisteredTool {
            tool: Arc::new(PanicAfterEffect(count.clone())),
            replay: ReplayPolicy::Safe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut h = Harness::open(&db, opts.clone(), &cancel).await.unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.submit_and_run(id, None, "work".into(), &cancel)
        .await
        .unwrap();
    let operation = h.blocked(id).unwrap().unwrap().0;
    let reports = h.extension_reports().unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].phase, "tool_execution_panic");
    assert!(!serde_json::to_string(&reports).unwrap().contains("secret"));
    let (sent, recv) = tokio::sync::oneshot::channel();
    let mut sent = Some(sent);
    let mut dispatcher = h
        .dispatch_extension_reports(0, std::time::Duration::from_millis(1), move |_| {
            sent.take().unwrap().send(()).unwrap();
            async { Err("listener failure".into()) }
        })
        .unwrap();
    recv.await.unwrap();
    assert!(dispatcher.close().await.is_err());
    drop(dispatcher);
    drop(h);
    let mut h = Harness::open(&db, opts, &cancel).await.unwrap();
    h.resume_conversation(id, &cancel).await.unwrap();
    assert_eq!(h.extension_reports().unwrap(), reports);
    assert_eq!(
        h.subscribe_extension_reports()
            .unwrap()
            .read(0, 10)
            .unwrap()
            .len(),
        1
    );
    h.reconcile(id, operation, Outcome::Succeeded("confirmed".into()))
        .unwrap();
    h.resume_conversation(id, &cancel).await.unwrap();
    assert_eq!(h.extension_reports().unwrap(), reports);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

struct CancelThenPanic {
    cancel: CancellationToken,
    count: Arc<AtomicUsize>,
}
#[async_trait]
impl Tool for CancelThenPanic {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "parent".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.cancel.cancel();
        panic!("sensitive panic payload")
    }
}

#[tokio::test]
async fn cancellation_after_panic_preserves_report_and_prevents_replay() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let cancel = CancellationToken::new();
    let count = Arc::new(AtomicUsize::new(0));
    let opts = Options {
        model: Arc::new(FakeModel),
        tools: vec![RegisteredTool {
            tool: Arc::new(CancelThenPanic {
                cancel: cancel.clone(),
                count: count.clone(),
            }),
            replay: ReplayPolicy::Safe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut h = Harness::open(&db, opts.clone(), &cancel).await.unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    assert!(
        h.submit_and_run(id, None, "work".into(), &cancel)
            .await
            .is_err()
    );
    drop(h);
    let fresh = CancellationToken::new();
    let mut h = Harness::open(&db, opts, &fresh).await.unwrap();
    assert!(h.blocked(id).unwrap().is_some());
    h.resume_conversation(id, &fresh).await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let reports = h.extension_reports().unwrap();
    assert_eq!(reports.len(), 1);
    assert!(
        !serde_json::to_string(&reports)
            .unwrap()
            .contains("sensitive")
    );
}

struct PendingNested {
    invoked: AtomicUsize,
    polled: AtomicUsize,
}
#[async_trait]
impl Tool for PendingNested {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "child".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        self.invoked.fetch_add(1, Ordering::SeqCst);
        Outcome::Pending {
            reference: "child-job".into(),
        }
    }
    async fn poll(&self, reference: &str, _: ToolContext) -> Option<Outcome> {
        if self.polled.fetch_add(1, Ordering::SeqCst) == 0 {
            assert_eq!(reference, "child-job");
            Some(Outcome::Pending {
                reference: "child-job-next".into(),
            })
        } else {
            assert_eq!(reference, "child-job-next");
            Some(Outcome::Succeeded("finished".into()))
        }
    }
}

#[tokio::test]
async fn nested_pending_is_polled_to_completion_without_redispatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let child = Arc::new(PendingNested {
        invoked: AtomicUsize::new(0),
        polled: AtomicUsize::new(0),
    });
    let options = || Options {
        model: Arc::new(FakeModel),
        tools: vec![
            RegisteredTool {
                tool: Arc::new(Parent),
                replay: ReplayPolicy::Safe,
            },
            RegisteredTool {
                tool: child.clone(),
                replay: ReplayPolicy::Safe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let id;
    {
        let mut h = Harness::open(&path, options(), &cancel).await.unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        h.submit_and_run(id, None, "work".into(), &cancel)
            .await
            .unwrap();
        assert!(h.blocked(id).unwrap().is_none());
    }
    {
        let mut h = Harness::open(&path, options(), &cancel).await.unwrap();
        h.resume_conversation(id, &cancel).await.unwrap();
        assert!(h.blocked(id).unwrap().is_none());
    }
    assert_eq!(child.invoked.load(Ordering::SeqCst), 1);
    assert_eq!(child.polled.load(Ordering::SeqCst), 2);
}

struct ProcessPending {
    root: std::path::PathBuf,
    worker: bool,
}
#[async_trait]
impl Tool for ProcessPending {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "child".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn invoke(&self, _: Value, ctx: ToolContext) -> Outcome {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("invocations"))
            .unwrap();
        writeln!(file, "{}", ctx.operation_id).unwrap();
        Outcome::Pending {
            reference: "process-job".into(),
        }
    }
    async fn poll(&self, reference: &str, ctx: ToolContext) -> Option<Outcome> {
        assert_eq!(reference, "process-job");
        std::fs::write(self.root.join("poll-id"), ctx.operation_id.to_string()).unwrap();
        if self.worker {
            std::future::pending().await
        } else {
            Some(Outcome::Succeeded("done".into()))
        }
    }
}
fn process_options(root: &std::path::Path, worker: bool) -> Options {
    Options {
        model: Arc::new(FakeModel),
        tools: vec![
            RegisteredTool {
                tool: Arc::new(Parent),
                replay: ReplayPolicy::Safe,
            },
            RegisteredTool {
                tool: Arc::new(ProcessPending {
                    root: root.into(),
                    worker,
                }),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
#[tokio::test]
async fn nested_poll_kill_worker() {
    let Some(root) = std::env::var_os("PI_NESTED_POLL_WORKER") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let cancel = CancellationToken::new();
    let mut h = Harness::open(root.join("db"), process_options(&root, true), &cancel)
        .await
        .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    std::fs::write(root.join("id"), id.to_string()).unwrap();
    h.submit_and_run(id, None, "work".into(), &cancel)
        .await
        .unwrap();
}
#[tokio::test]
async fn killed_nested_poll_reattaches_to_unsafe_job() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut worker = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "nested_poll_kill_worker"])
            .env("PI_NESTED_POLL_WORKER", root)
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while !root.join("poll-id").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("worker never reached polling");
    worker.0.kill().unwrap();
    worker.0.wait().unwrap();
    let id = std::fs::read_to_string(root.join("id"))
        .unwrap()
        .parse()
        .unwrap();
    let cancel = CancellationToken::new();
    let mut h = Harness::open(root.join("db"), process_options(root, false), &cancel)
        .await
        .unwrap();
    h.resume_conversation(id, &cancel).await.unwrap();
    assert!(h.blocked(id).unwrap().is_none());
    let invocations = std::fs::read_to_string(root.join("invocations")).unwrap();
    assert_eq!(invocations.lines().count(), 1);
    assert_eq!(
        invocations.trim(),
        std::fs::read_to_string(root.join("poll-id")).unwrap()
    );
}
