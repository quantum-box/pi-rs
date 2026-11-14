use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    CancellationToken, Error, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool,
    ToolCall, ToolContext, ToolSpec,
};
use pi_rs_durable::{
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, Harness, Options, RegisteredTool, Status},
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct FakeModel {
    calls: AtomicUsize,
    fail_first: bool,
}
#[async_trait]
impl Model for FakeModel {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_first && n == 0 {
            return Err(Error::Model("connection lost".into()));
        }
        let tool_result = request
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }));
        let events = if tool_result {
            vec![
                Ok(ModelEvent::Text("finished".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ]
        } else {
            vec![Ok(ModelEvent::Complete {
                calls: vec![ToolCall {
                    id: "call-1".into(),
                    name: "test".into(),
                    arguments: "{}".into(),
                }],
                usage: None,
            })]
        };
        Ok(stream::iter(events).boxed())
    }
}
struct FakeTool {
    calls: AtomicUsize,
    interrupt: bool,
    cancel: Option<CancellationToken>,
}
#[async_trait]
impl Tool for FakeTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "test".into(),
            description: "test".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _args: serde_json::Value, _ctx: ToolContext) -> Outcome {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.interrupt && n == 0 {
            self.cancel.as_ref().unwrap().cancel();
            std::future::pending().await
        }
        Outcome::Succeeded("ok".into())
    }
}
fn options(model: Arc<FakeModel>, tool: Arc<FakeTool>, replay: ReplayPolicy) -> Options {
    Options {
        model,
        tools: vec![RegisteredTool { tool, replay }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn config() -> Config {
    Config {
        model: "fake".into(),
        system_prompt: "system".into(),
        max_output_tokens: 100,
    }
}

#[tokio::test]
async fn executes_model_tool_and_final_answer_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.sqlite");
    let model = Arc::new(FakeModel {
        calls: AtomicUsize::new(0),
        fail_first: false,
    });
    let tool = Arc::new(FakeTool {
        calls: AtomicUsize::new(0),
        interrupt: false,
        cancel: None,
    });
    let cancel = CancellationToken::new();
    let mut h = Harness::open(
        &path,
        options(model.clone(), tool.clone(), ReplayPolicy::Unsafe),
        &cancel,
    )
    .await
    .unwrap();
    let id = h.create_conversation(config()).unwrap();
    let sid = h
        .submit_and_run(id, Some("job".into()), "work".into(), &cancel)
        .await
        .unwrap();
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "finished".into()
        }
    );
    assert_eq!(
        h.submit_and_run(id, Some("job".into()), "again".into(), &cancel)
            .await
            .unwrap(),
        sid
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reopen_retries_model_interrupted_before_response() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.sqlite");
    let model = Arc::new(FakeModel {
        calls: AtomicUsize::new(0),
        fail_first: true,
    });
    let tool = Arc::new(FakeTool {
        calls: AtomicUsize::new(0),
        interrupt: false,
        cancel: None,
    });
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open(
            &path,
            options(model.clone(), tool.clone(), ReplayPolicy::Unsafe),
            &cancel,
        )
        .await
        .unwrap();
        id = h.create_conversation(config()).unwrap();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                h.submit_and_run(id, Some("job".into()), "work".into(), &cancel)
            )
            .await
            .is_err()
        );
    }
    let h = Harness::open(
        &path,
        options(model.clone(), tool.clone(), ReplayPolicy::Unsafe),
        &cancel,
    )
    .await
    .unwrap();
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "finished".into()
        }
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn interrupted_tool_respects_replay_policy_on_reopen() {
    for policy in [ReplayPolicy::Safe, ReplayPolicy::Unsafe] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.sqlite");
        let model = Arc::new(FakeModel {
            calls: AtomicUsize::new(0),
            fail_first: false,
        });
        let outer_cancel = CancellationToken::new();
        let tool = Arc::new(FakeTool {
            calls: AtomicUsize::new(0),
            interrupt: true,
            cancel: Some(outer_cancel.clone()),
        });
        let id;
        {
            let cancel = outer_cancel.clone();
            let mut h = Harness::open(&path, options(model.clone(), tool.clone(), policy), &cancel)
                .await
                .unwrap();
            id = h.create_conversation(config()).unwrap();
            assert!(
                h.submit_and_run(id, None, "work".into(), &cancel)
                    .await
                    .is_err()
            );
        }
        let h = Harness::open(
            &path,
            options(model.clone(), tool.clone(), policy),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            h.conversation(id).unwrap().submissions[0].status,
            Status::Done {
                answer: "finished".into()
            }
        );
        assert_eq!(
            tool.calls.load(Ordering::SeqCst),
            if policy == ReplayPolicy::Safe { 2 } else { 1 }
        );
    }
}

struct CrashTool {
    marker: std::path::PathBuf,
}
#[async_trait]
impl Tool for CrashTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "test".into(),
            description: "test".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _args: serde_json::Value, _ctx: ToolContext) -> Outcome {
        std::fs::write(&self.marker, b"dispatched").unwrap();
        std::future::pending().await
    }
}
#[tokio::test]
async fn child_crash_during_dispatched_tool() {
    let Ok(root) = std::env::var("PI_DURABLE_CRASH_ROOT") else {
        return;
    };
    let path = std::path::Path::new(&root).join("crash.sqlite");
    let marker = std::path::Path::new(&root).join("marker");
    let model = Arc::new(FakeModel {
        calls: AtomicUsize::new(0),
        fail_first: false,
    });
    let tool = Arc::new(CrashTool { marker });
    let options = Options {
        model,
        tools: vec![RegisteredTool {
            tool,
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let cancel = CancellationToken::new();
    let mut h = Harness::open(&path, options, &cancel).await.unwrap();
    let id = h.create_conversation(config()).unwrap();
    std::fs::write(std::path::Path::new(&root).join("id"), id.to_string()).unwrap();
    h.submit_and_run(id, Some("job".into()), "work".into(), &cancel)
        .await
        .unwrap();
}
#[tokio::test]
async fn killed_process_recovers_unsafe_call_without_reexecution() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("child_crash_during_dispatched_tool")
        .env("PI_DURABLE_CRASH_ROOT", root)
        .spawn()
        .unwrap();
    let marker = root.join("marker");
    let timeout = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !marker.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    if timeout.is_err() {
        let _ = child.kill();
        let _ = child.wait();
        panic!("child never reached tool dispatch");
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let id = std::fs::read_to_string(root.join("id"))
        .unwrap()
        .parse()
        .unwrap();
    let model = Arc::new(FakeModel {
        calls: AtomicUsize::new(0),
        fail_first: false,
    });
    let tool = Arc::new(FakeTool {
        calls: AtomicUsize::new(0),
        interrupt: false,
        cancel: None,
    });
    let h = Harness::open(
        root.join("crash.sqlite"),
        options(model, tool.clone(), ReplayPolicy::Unsafe),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "finished".into()
        }
    );
}

struct HangingTool {
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl Tool for HangingTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "test".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _args: serde_json::Value, _ctx: ToolContext) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
    }
}
#[tokio::test]
async fn dropping_resume_cannot_replay_unsafe_tool_in_same_harness() {
    let dir = tempfile::tempdir().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let model = Arc::new(FakeModel {
        calls: AtomicUsize::new(0),
        fail_first: false,
    });
    let opts = Options {
        model,
        tools: vec![RegisteredTool {
            tool: Arc::new(HangingTool {
                calls: count.clone(),
            }),
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let cancel = CancellationToken::new();
    let mut h = Harness::open(dir.path().join("db"), opts, &cancel)
        .await
        .unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.submit(id, None, "work".into()).unwrap();
    {
        let run = h.resume(&cancel);
        tokio::pin!(run);
        tokio::select! { result=&mut run => panic!("finished early: {result:?}"), _=async { while count.load(Ordering::SeqCst)==0 { tokio::task::yield_now().await; } } => {} }
    }
    assert!(h.resume(&cancel).await.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn reconcile_policy_blocks_until_authoritative_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.sqlite");
    let model = Arc::new(FakeModel {
        calls: AtomicUsize::new(0),
        fail_first: false,
    });
    let cancel = CancellationToken::new();
    let tool = Arc::new(FakeTool {
        calls: AtomicUsize::new(0),
        interrupt: true,
        cancel: Some(cancel.clone()),
    });
    let mut opts = options(model.clone(), tool.clone(), ReplayPolicy::Unsafe);
    opts.uncertain_policy = UncertainPolicy::RequireReconciliation;
    let id;
    {
        let mut h = Harness::open(&path, opts, &cancel).await.unwrap();
        id = h.create_conversation(config()).unwrap();
        assert!(
            h.submit_and_run(id, None, "work".into(), &cancel)
                .await
                .is_err()
        );
    }
    let mut opts = options(model, tool.clone(), ReplayPolicy::Unsafe);
    opts.uncertain_policy = UncertainPolicy::RequireReconciliation;
    let fresh = CancellationToken::new();
    let mut h = Harness::open(&path, opts, &fresh).await.unwrap();
    let (operation_id, _) = h.blocked(id).unwrap().expect("operation must be blocked");
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    h.reconcile(id, operation_id, Outcome::Succeeded("confirmed".into()))
        .unwrap();
    h.resume(&fresh).await.unwrap();
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "finished".into()
        }
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
}

struct AccountingModel {
    inner: FakeModel,
    unconfirmed: Arc<AtomicUsize>,
}
#[async_trait]
impl Model for AccountingModel {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        if self.inner.calls.load(Ordering::SeqCst) > 0 {
            assert!(self.unconfirmed.load(Ordering::SeqCst) > 0);
        }
        self.inner.stream(request).await
    }
    async fn record_usage(
        &self,
        _id: uuid::Uuid,
        usage: &pi_rs_core::UsageReport,
    ) -> Result<(), Error> {
        if matches!(usage, pi_rs_core::UsageReport::Unconfirmed) {
            self.unconfirmed.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}
#[tokio::test]
async fn interrupted_model_is_settled_before_retry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.sqlite");
    let unconfirmed = Arc::new(AtomicUsize::new(0));
    let model = Arc::new(AccountingModel {
        inner: FakeModel {
            calls: AtomicUsize::new(0),
            fail_first: true,
        },
        unconfirmed: unconfirmed.clone(),
    });
    let tool = Arc::new(FakeTool {
        calls: AtomicUsize::new(0),
        interrupt: false,
        cancel: None,
    });
    let opts = || Options {
        model: model.clone(),
        tools: vec![RegisteredTool {
            tool: tool.clone(),
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
        id = h.create_conversation(config()).unwrap();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                h.submit_and_run(id, None, "work".into(), &cancel)
            )
            .await
            .is_err()
        );
    }
    let h = Harness::open(&path, opts(), &cancel).await.unwrap();
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "finished".into()
        }
    );
    // The successful model responses have no usage too; this test checks at
    // least the abandoned request was settled as unconfirmed before retry.
    assert!(unconfirmed.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn repeated_paused_open_does_not_consume_tool_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.sqlite");
    let cancel = CancellationToken::new();
    let model = Arc::new(FakeModel {
        calls: AtomicUsize::new(0),
        fail_first: false,
    });
    let tool = Arc::new(FakeTool {
        calls: AtomicUsize::new(0),
        interrupt: true,
        cancel: Some(cancel.clone()),
    });
    let id;
    {
        let mut h = Harness::open(
            &path,
            options(model.clone(), tool.clone(), ReplayPolicy::Safe),
            &cancel,
        )
        .await
        .unwrap();
        id = h.create_conversation(config()).unwrap();
        assert!(
            h.submit_and_run(id, None, "work".into(), &cancel)
                .await
                .is_err()
        );
    }
    for _ in 0..7 {
        let h = Harness::open_paused(
            &path,
            options(model.clone(), tool.clone(), ReplayPolicy::Safe),
        )
        .unwrap();
        drop(h);
    }
    let h = Harness::open(
        &path,
        options(model, tool.clone(), ReplayPolicy::Safe),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(tool.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "finished".into()
        }
    );
}

#[tokio::test]
async fn paused_owner_can_run_another_conversation_after_failure() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.sqlite");
    let model = Arc::new(FakeModel {
        calls: AtomicUsize::new(0),
        fail_first: true,
    });
    let tool = Arc::new(FakeTool {
        calls: AtomicUsize::new(0),
        interrupt: false,
        cancel: None,
    });
    let cancel = CancellationToken::new();
    let first;
    let second;
    {
        let mut h = Harness::open(
            &path,
            options(model.clone(), tool.clone(), ReplayPolicy::Unsafe),
            &cancel,
        )
        .await
        .unwrap();
        first = h.create_conversation(config()).unwrap();
        second = h.create_conversation(config()).unwrap();
        h.submit(first, None, "first".into()).unwrap();
        h.submit(second, None, "second".into()).unwrap();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                h.resume_conversation(first, &cancel)
            )
            .await
            .is_err()
        );
    }
    let mut h = Harness::open_paused(&path, options(model, tool, ReplayPolicy::Unsafe)).unwrap();
    h.resume_conversation(second, &cancel).await.unwrap();
    assert_eq!(
        h.conversation(second).unwrap().submissions[0].status,
        Status::Done {
            answer: "finished".into()
        }
    );
    assert_eq!(
        h.conversation(first).unwrap().submissions[0].status,
        Status::Running
    );
}

#[tokio::test]
async fn background_scheduler_retries_interrupted_model_without_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(FakeModel {
        calls: AtomicUsize::new(0),
        fail_first: true,
    });
    let tool = Arc::new(FakeTool {
        calls: AtomicUsize::new(0),
        interrupt: false,
        cancel: None,
    });
    let mut s = pi_rs_durable::runtime::Scheduler::open(
        dir.path().join("db"),
        options(model.clone(), tool.clone(), ReplayPolicy::Unsafe),
    )
    .unwrap();
    let id = s.create_conversation(config()).unwrap();
    s.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(8), s.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        s.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "finished".into()
        }
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    s.shutdown().await.unwrap();
}
