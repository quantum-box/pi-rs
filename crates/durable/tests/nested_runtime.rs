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
