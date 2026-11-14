use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    CancellationToken, Error, ExtensionState, Message, Model, ModelEvent, ModelRequest,
    ModelStream, Outcome, Tool, ToolCall, ToolContext, ToolHost, ToolSpec,
};
use pi_rs_durable::{
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, Harness, InputMode, Options, RegisteredTool, Scheduler, Status},
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Semaphore;

fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: "".into(),
        max_output_tokens: 100,
    }
}
fn opts(model: Arc<dyn Model>, tools: Vec<RegisteredTool>) -> Options {
    Options {
        model,
        tools,
        restart_limit: 3,
        max_turns: 5,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
async fn wait_permit(s: &Semaphore) {
    tokio::time::timeout(Duration::from_secs(5), s.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

struct ModelBarrier {
    started: Semaphore,
    release: Semaphore,
}
#[async_trait]
impl Model for ModelBarrier {
    async fn stream(&self, req: ModelRequest) -> Result<ModelStream, Error> {
        if req
            .messages
            .iter()
            .any(|m| matches!(m, Message::User { content } if content == "hold"))
        {
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("ok".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
#[tokio::test]
async fn abort_only_one_conversation_and_withdraw_existing_queue() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(ModelBarrier {
        started: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let mut s = Scheduler::open(dir.path().join("db"), opts(model.clone(), vec![])).unwrap();
    let a = s.create_conversation(config()).unwrap();
    let b = s.create_conversation(config()).unwrap();
    s.submit(a, None, "hold".into()).unwrap();
    wait_permit(&model.started).await;
    s.submit_with_mode(a, None, "steer".into(), InputMode::Steer)
        .unwrap();
    s.submit(a, None, "follow".into()).unwrap();
    s.submit(b, None, "other".into()).unwrap();
    s.abort(a).unwrap();
    tokio::time::timeout(Duration::from_secs(5), s.wait(a))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), s.wait(b))
        .await
        .unwrap()
        .unwrap();
    let a_state = s.conversation(a).unwrap();
    assert!(a_state.submissions.iter().all(|x| x.status
        == Status::Unanswered {
            reason: "aborted".into()
        }));
    assert!(!a_state.messages.iter().any(
        |m| matches!(m, Message::User { content } if content == "steer" || content == "follow")
    ));
    assert!(matches!(
        s.conversation(b).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
    s.shutdown().await.unwrap();
}

struct CallModel;
#[async_trait]
impl Model for CallModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        Ok(stream::iter(vec![Ok(ModelEvent::Complete {
            calls: vec![
                ToolCall {
                    id: "one".into(),
                    name: "parent".into(),
                    arguments: "{}".into(),
                },
                ToolCall {
                    id: "two".into(),
                    name: "parent".into(),
                    arguments: "{}".into(),
                },
            ],
            usage: None,
        })])
        .boxed())
    }
}
struct WaitingTool {
    started: Arc<Semaphore>,
    count: AtomicUsize,
}
#[async_trait]
impl Tool for WaitingTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "parent".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.started.add_permits(1);
        std::future::pending().await
    }
}
#[tokio::test]
async fn abort_dispatched_tool_blocks_replay_across_restart_even_when_safe() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let tool = Arc::new(WaitingTool {
        started: Arc::new(Semaphore::new(0)),
        count: AtomicUsize::new(0),
    });
    let options = || {
        opts(
            Arc::new(CallModel),
            vec![RegisteredTool {
                tool: tool.clone(),
                replay: ReplayPolicy::Safe,
            }],
        )
    };
    let mut s = Scheduler::open(&path, options()).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "work".into()).unwrap();
    wait_permit(&tool.started).await;
    s.abort(id).unwrap();
    tokio::time::timeout(Duration::from_secs(5), s.wait(id))
        .await
        .unwrap()
        .unwrap();
    let (operation, reason) = s.blocked(id).unwrap().unwrap();
    assert!(reason.contains("external outcome"));
    assert_eq!(
        s.conversation(id).unwrap().submissions[0].status,
        Status::Unanswered {
            reason: "aborted".into()
        }
    );
    s.shutdown().await.unwrap();
    let mut h = Harness::open(&path, options(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(tool.count.load(Ordering::SeqCst), 1);
    assert_eq!(h.blocked(id).unwrap().unwrap().0, operation);
    h.reconcile(id, operation, Outcome::Succeeded("confirmed".into()))
        .unwrap();
    h.resume(&CancellationToken::new()).await.unwrap();
    assert_eq!(tool.count.load(Ordering::SeqCst), 1);
    let state = h.conversation(id).unwrap();
    assert!(state.messages.iter().any(|m| matches!(m, Message::Tool { call_id, content } if call_id == "two" && content.contains("did not start"))));
}

struct Parent {
    child_started: Arc<Semaphore>,
}
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
        tokio::spawn(async move {
            let _ = host.call("child", json!({})).await;
        });
        self.child_started.acquire().await.unwrap().forget();
        std::future::pending().await
    }
}
struct Child {
    started: Arc<Semaphore>,
    observed: Arc<Semaphore>,
}
struct DropSignal(Arc<Semaphore>);
impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
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
        let _guard = DropSignal(self.observed.clone());
        self.started.add_permits(2);
        std::future::pending().await
    }
}
#[tokio::test]
async fn abort_waits_for_detached_nested_call_to_stop() {
    let dir = tempfile::tempdir().unwrap();
    let started = Arc::new(Semaphore::new(0));
    let stopped = Arc::new(Semaphore::new(0));
    let tools = vec![
        RegisteredTool {
            tool: Arc::new(Parent {
                child_started: started.clone(),
            }),
            replay: ReplayPolicy::Safe,
        },
        RegisteredTool {
            tool: Arc::new(Child {
                started: started.clone(),
                observed: stopped.clone(),
            }),
            replay: ReplayPolicy::Unsafe,
        },
    ];
    let mut s = Scheduler::open(dir.path().join("db"), opts(Arc::new(CallModel), tools)).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "work".into()).unwrap();
    wait_permit(&started).await;
    s.abort(id).unwrap();
    tokio::time::timeout(Duration::from_secs(5), s.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert!(stopped.try_acquire().is_ok());
    let operation = s.blocked(id).unwrap().unwrap().0;
    assert!(
        s.reconcile(
            id,
            operation,
            Outcome::Succeeded("parent reported success".into())
        )
        .is_err()
    );
    assert!(s.blocked(id).unwrap().is_some());
    s.shutdown().await.unwrap();
}

struct SettlementBarrier {
    started: Semaphore,
    settling: Semaphore,
    release: Semaphore,
}
#[async_trait]
impl Model for SettlementBarrier {
    async fn stream(&self, req: ModelRequest) -> Result<ModelStream, Error> {
        if req
            .messages
            .iter()
            .any(|m| matches!(m, Message::User { content } if content == "first"))
            && !req
                .messages
                .iter()
                .any(|m| matches!(m, Message::User { content } if content == "later"))
        {
            self.started.add_permits(1);
            std::future::pending().await
        }
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("later done".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
    async fn record_usage(&self, _: uuid::Uuid, _: &pi_rs_core::UsageReport) -> Result<(), Error> {
        self.settling.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        Ok(())
    }
}
#[tokio::test]
async fn submission_after_abort_mark_is_not_withdrawn() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(SettlementBarrier {
        started: Semaphore::new(0),
        settling: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let mut s = Scheduler::open(dir.path().join("db"), opts(model.clone(), vec![])).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "first".into()).unwrap();
    wait_permit(&model.started).await;
    s.abort(id).unwrap();
    wait_permit(&model.settling).await;
    s.submit(id, None, "later".into()).unwrap();
    model.release.add_permits(2);
    tokio::time::timeout(Duration::from_secs(5), s.wait(id))
        .await
        .unwrap()
        .unwrap();
    let state = s.conversation(id).unwrap();
    assert_eq!(
        state.submissions[0].status,
        Status::Unanswered {
            reason: "aborted".into()
        }
    );
    assert!(matches!(state.submissions[1].status, Status::Done { .. }));
    s.shutdown().await.unwrap();
}

struct PartialModel {
    checkpointed: Arc<Semaphore>,
}
#[async_trait]
impl Model for PartialModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        let started = self.checkpointed.clone();
        Ok(
            stream::unfold((0u8, started), |(step, started)| async move {
                if step == 0 {
                    Some((Ok(ModelEvent::Text("partial".into())), (1, started)))
                } else {
                    started.add_permits(1);
                    std::future::pending().await
                }
            })
            .boxed(),
        )
    }
}
#[tokio::test]
async fn abort_preserves_committed_partial_for_display_only() {
    let dir = tempfile::tempdir().unwrap();
    let checkpointed = Arc::new(Semaphore::new(0));
    let mut s = Scheduler::open(
        dir.path().join("db"),
        opts(
            Arc::new(PartialModel {
                checkpointed: checkpointed.clone(),
            }),
            vec![],
        ),
    )
    .unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "work".into()).unwrap();
    wait_permit(&checkpointed).await;
    s.abort_and_wait(id).await.unwrap();
    let state = s.conversation(id).unwrap();
    assert_eq!(state.aborted_generations.len(), 1);
    assert_eq!(state.aborted_generations[0].text, "partial");
    assert!(
        !state
            .messages
            .iter()
            .any(|m| matches!(m, Message::Assistant { content, .. } if content == "partial"))
    );
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_withdraws_inputs_but_retains_queued_reset() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(ModelBarrier {
        started: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let mut s = Scheduler::open(dir.path().join("db"), opts(model.clone(), vec![])).unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "hold".into()).unwrap();
    wait_permit(&model.started).await;
    let reset = s.reset(id, Some("handoff".into())).unwrap();
    s.submit(id, None, "withdraw".into()).unwrap();
    s.abort_and_wait(id).await.unwrap();
    let state = s.conversation(id).unwrap();
    assert_eq!(state.resets.len(), 1);
    assert_eq!(state.resets[0].id, reset);
    assert_eq!(
        state.submissions[1].status,
        Status::Unanswered {
            reason: "aborted".into()
        }
    );
    s.shutdown().await.unwrap();
}
