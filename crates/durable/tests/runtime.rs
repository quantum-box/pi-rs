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
async fn view_pages_stay_on_captured_transcript_and_show_submission_state() {
    let dir = tempfile::tempdir().unwrap();
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
        dir.path().join("db"),
        options(model, tool, ReplayPolicy::Unsafe),
        &cancel,
    )
    .await
    .unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.submit_and_run(id, None, "one".into(), &cancel)
        .await
        .unwrap();
    let expected = h.conversation(id).unwrap().messages;
    assert!(expected.len() > 1);
    let limit = std::num::NonZeroUsize::new(1).unwrap();
    let view = h.view(id, limit).unwrap();
    assert!(view.state.messages.is_empty());
    assert_eq!(view.state.submissions.len(), 1);
    let mut actual = view.entries;
    let mut older = view.older;
    h.reset(id, Some("new context".into())).unwrap();
    while let Some(cursor) = older {
        let page = h.view_page(id, cursor, limit).unwrap();
        assert!(page.entries.len() <= 1);
        actual.extend(page.entries);
        older = page.older;
    }
    actual.reverse();
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    let fresh = h.view(id, limit).unwrap();
    assert_eq!(fresh.state.resets.len(), 1);
}

#[tokio::test]
async fn positioned_messages_survive_reset_and_reopen_and_are_conversation_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
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
    let other = h.create_conversation(config()).unwrap();
    h.submit_and_run(id, None, "one".into(), &cancel)
        .await
        .unwrap();
    let expected = h.conversation(id).unwrap().messages;
    let limit = std::num::NonZeroUsize::new(2).unwrap();
    let mut page = h.view_positioned(id, limit).unwrap();
    let mut positions = Vec::new();
    loop {
        positions.extend(
            page.entries
                .into_iter()
                .map(|e| (e.message, serde_json::to_string(&e.position).unwrap())),
        );
        match page.older {
            Some(next) => page = h.view_positioned_page(id, next, limit).unwrap(),
            None => break,
        }
    }
    assert_eq!(positions.len(), expected.len());
    assert_eq!(
        positions.iter().map(|p| &p.0).collect::<Vec<_>>(),
        expected.iter().rev().collect::<Vec<_>>()
    );
    h.reset(id, Some("new context".into())).unwrap();
    drop(h);
    let h = Harness::open(&path, options(model, tool, ReplayPolicy::Unsafe), &cancel)
        .await
        .unwrap();
    for (index, (message, encoded)) in positions.iter().enumerate() {
        let position = serde_json::from_str(encoded).unwrap();
        let page = h
            .view_positioned_page(id, position, std::num::NonZeroUsize::new(1).unwrap())
            .unwrap();
        assert_eq!(&page.entries[0].message, message, "position {index}");
        assert!(
            h.view_positioned_page(other, serde_json::from_str(encoded).unwrap(), limit)
                .is_err()
        );
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
            replay: if std::env::var_os("PI_DURABLE_CRASH_SAFE").is_some() {
                ReplayPolicy::Safe
            } else {
                ReplayPolicy::Unsafe
            },
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

#[tokio::test]
async fn killed_unsafe_tool_requires_reconciliation_even_if_now_replay_safe() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("child_crash_during_dispatched_tool")
        .env("PI_DURABLE_CRASH_ROOT", root)
        .env_remove("PI_DURABLE_CRASH_SAFE")
        .spawn()
        .unwrap();
    let ready = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !root.join("marker").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    child.kill().unwrap();
    let status = child.wait().unwrap();
    ready.expect("child never reached tool dispatch");
    assert!(!status.success());

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
    let opts = || {
        let mut opts = options(model.clone(), tool.clone(), ReplayPolicy::Safe);
        opts.uncertain_policy = UncertainPolicy::RequireReconciliation;
        opts
    };
    let cancel = CancellationToken::new();
    let path = root.join("crash.sqlite");
    let h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let (operation, _) = h.blocked(id).unwrap().expect("must await reconciliation");
    assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert!(
        !h.conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }))
    );
    drop(h);

    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    assert_eq!(h.blocked(id).unwrap().unwrap().0, operation);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    h.reconcile(id, operation, Outcome::Succeeded("confirmed effect".into()))
        .unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    assert!(h.blocked(id).unwrap().is_none());
    h.resume(&cancel).await.unwrap();
    let conversation = h.conversation(id).unwrap();
    assert_eq!(
        conversation.submissions[0].status,
        Status::Done {
            answer: "finished".into()
        }
    );
    let results: Vec<_> = conversation
        .messages
        .iter()
        .filter(|m| matches!(m, Message::Tool { .. }))
        .collect();
    assert_eq!(results.len(), 1);
    assert!(
        serde_json::to_string(results[0])
            .unwrap()
            .contains("confirmed effect")
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
    drop(h);

    let h = Harness::open(&path, opts(), &cancel).await.unwrap();
    assert!(h.blocked(id).unwrap().is_none());
    assert_eq!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done {
            answer: "finished".into()
        }
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn killed_process_replay_gets_new_persisted_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("child_crash_during_dispatched_tool")
        .env("PI_DURABLE_CRASH_ROOT", root)
        .env("PI_DURABLE_CRASH_SAFE", "1")
        .spawn()
        .unwrap();
    let ready = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !root.join("marker").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    child.kill().unwrap();
    child.wait().unwrap();
    ready.expect("child never dispatched");
    let id: uuid::Uuid = std::fs::read_to_string(root.join("id"))
        .unwrap()
        .parse()
        .unwrap();
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
    let path = root.join("crash.sqlite");
    // Simulate a database written before attempt discovery existed, while
    // retaining its previously persisted current attempt and progress.
    {
        let store = durable_store::Store::open(&path).unwrap();
        let mut prefix = b"durable/tool-attempts/".to_vec();
        prefix.extend_from_slice(id.as_bytes());
        prefix.push(b'/');
        let mut head = prefix.clone();
        head.push(b'h');
        let mut record = prefix;
        record.push(b'r');
        record.extend_from_slice(&1_u64.to_be_bytes());
        store
            .commit(
                &[],
                &[
                    durable_store::Mutation::Delete { key: head },
                    durable_store::Mutation::Delete { key: record },
                ],
            )
            .unwrap();
    }
    let mut h = Harness::open_paused(
        &path,
        options(model.clone(), tool.clone(), ReplayPolicy::Safe),
    )
    .unwrap();
    let first = h.conversation(id).unwrap().tool_attempt().unwrap();
    let page_size = std::num::NonZeroUsize::new(1).unwrap();
    assert!(
        h.tool_attempts(id, pi_rs_durable::observer::Cursor(0), page_size)
            .unwrap()
            .records
            .is_empty()
    );
    assert!(h.resume(&cancel).await.is_err());
    drop(h);
    let h = Harness::open_paused(
        &path,
        options(model.clone(), tool.clone(), ReplayPolicy::Safe),
    )
    .unwrap();
    let second = h.conversation(id).unwrap().tool_attempt().unwrap();
    let discovered = h
        .tool_attempts(id, pi_rs_durable::observer::Cursor(0), page_size)
        .unwrap();
    assert_eq!(discovered.records.len(), 1);
    assert_eq!(discovered.records[0].value["attempt"], json!(second));
    assert_eq!(first.operation_id, second.operation_id);
    assert_ne!(first.attempt_id, second.attempt_id);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    drop(h);
    let h = Harness::open(
        &path,
        options(model, tool.clone(), ReplayPolicy::Safe),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(tool.calls.load(Ordering::SeqCst), 2);
    assert!(matches!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
    drop(h);
    let _h = Harness::open_paused(
        &path,
        options(
            Arc::new(FakeModel {
                calls: AtomicUsize::new(0),
                fail_first: false,
            }),
            tool.clone(),
            ReplayPolicy::Safe,
        ),
    )
    .unwrap();
    assert_eq!(tool.calls.load(Ordering::SeqCst), 2);
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

#[tokio::test]
async fn continue_after_turn_limit_does_not_repeat_tool_effect() {
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
    let mut opts = options(model.clone(), tool.clone(), ReplayPolicy::Unsafe);
    opts.max_turns = 1;
    let mut h = Harness::open_paused(&path, opts).unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.submit_and_run(id, None, "work".into(), &cancel)
        .await
        .unwrap();
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    let continuation = h.submit_continue(id, Some("resume".into())).unwrap();
    drop(h);
    let mut h = Harness::open(
        &path,
        options(model.clone(), tool.clone(), ReplayPolicy::Unsafe),
        &cancel,
    )
    .await
    .unwrap();
    assert_eq!(
        h.submit_continue(id, Some("resume".into())).unwrap(),
        continuation
    );
    h.resume(&cancel).await.unwrap();
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        h.conversation(id)
            .unwrap()
            .submissions
            .iter()
            .find(|s| s.id == continuation)
            .unwrap()
            .status,
        Status::Done {
            answer: "finished".into()
        }
    );
}

struct ProgressTool {
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    host: std::sync::Mutex<Option<Arc<dyn pi_rs_core::ToolHost>>>,
    fail: bool,
}
#[async_trait]
impl Tool for ProgressTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "test".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
    async fn invoke_with_host(
        &self,
        _: serde_json::Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        host: Arc<dyn pi_rs_core::ToolHost>,
    ) -> Outcome {
        host.report_progress(json!({"text":"working"}))
            .await
            .unwrap();
        *self.host.lock().unwrap() = Some(host);
        self.started.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        if self.fail {
            Outcome::Failed("failed".into())
        } else {
            Outcome::Succeeded("ok".into())
        }
    }
}

#[tokio::test]
async fn runtime_tool_progress_is_visible_and_abort_fences_late_callbacks_after_reopen() {
    use pi_rs_durable::{runtime::Scheduler, tool_progress::State};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let tool = Arc::new(ProgressTool {
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        host: std::sync::Mutex::new(None),
        fail: false,
    });
    let opts = || Options {
        model: Arc::new(FakeModel {
            calls: AtomicUsize::new(0),
            fail_first: false,
        }),
        tools: vec![RegisteredTool {
            tool: tool.clone(),
            replay: ReplayPolicy::Safe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut scheduler = Scheduler::open(&path, opts()).unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    let other = scheduler.create_conversation(config()).unwrap();
    let history_start = scheduler.progress_history_from_now(id).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), tool.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let attempt = scheduler.conversation(id).unwrap().tool_attempt().unwrap();
    let limit = std::num::NonZeroUsize::new(1).unwrap();
    let discovered = scheduler
        .tool_attempts(id, pi_rs_durable::observer::Cursor(0), limit)
        .unwrap();
    assert_eq!(discovered.records.len(), 1);
    assert_eq!(discovered.records[0].value["attempt"], json!(attempt));
    assert!(
        scheduler
            .tool_attempts(other, pi_rs_durable::observer::Cursor(0), limit)
            .unwrap()
            .records
            .is_empty()
    );
    assert_eq!(
        scheduler.view(id, limit).unwrap().tool_progress,
        Some(State::Running(Some(json!({"text":"working"}))))
    );
    let first = scheduler
        .tool_progress_events(
            attempt.attempt_id,
            pi_rs_durable::observer::Cursor(0),
            limit,
        )
        .unwrap();
    assert_eq!(first.records.len(), 1);
    assert_eq!(
        first.records[0].value["attempt"]["attempt_id"],
        json!(attempt.attempt_id)
    );
    assert_eq!(
        first.records[0].value["attempt"]["operation_id"],
        json!(attempt.operation_id)
    );
    assert_eq!(
        first.records[0].value["state"],
        json!({"state":"running","partial":{"text":"working"}})
    );
    let live_host = tool.host.lock().unwrap().as_ref().unwrap().clone();
    live_host
        .report_progress(json!({"text":"later"}))
        .await
        .unwrap();
    drop(live_host);
    let second = scheduler
        .tool_progress_events(attempt.attempt_id, first.next, limit)
        .unwrap();
    assert_eq!(
        second.records[0].value["state"],
        json!({"state":"running","partial":{"text":"later"}})
    );
    let mut history_cursor = history_start.clone();
    let mut history_records = Vec::new();
    loop {
        let page = scheduler
            .progress_history(&history_cursor, std::num::NonZeroUsize::MIN)
            .unwrap();
        assert!(page.records.len() <= 1);
        history_cursor = page.next;
        if page.records.is_empty() {
            break;
        }
        history_records.extend(page.records);
    }
    assert!(history_records.iter().any(|frame| frame.changes.iter().any(|change| matches!(change,
        pi_rs_durable::runtime::ProgressHistoryChange::Tool { attempt: seen, state: State::Running(Some(value)), .. }
        if *seen == attempt && *value == json!({"text":"working"})
    ))));
    assert!(history_records.iter().any(|frame| frame.changes.iter().any(|change| matches!(change,
        pi_rs_durable::runtime::ProgressHistoryChange::Tool { attempt: seen, state: State::Running(Some(value)), .. }
        if *seen == attempt && *value == json!({"text":"later"})
    ))));
    let error = scheduler.abort_and_wait(id).await.unwrap_err();
    assert!(error.to_string().contains("abort cleanup remains pending"));
    let host = tool.host.lock().unwrap().take().unwrap();
    host.report_progress(json!("late")).await.unwrap();
    drop(host);
    scheduler.shutdown().await.unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    drop(store);
    let reopened = Scheduler::open(&path, opts()).unwrap();
    assert!(
        reopened
            .tool_attempts(id, discovered.next, limit)
            .unwrap()
            .records
            .is_empty()
    );
    assert_eq!(
        reopened
            .tool_attempts(id, pi_rs_durable::observer::Cursor(0), limit)
            .unwrap()
            .records[0]
            .value["attempt"],
        json!(attempt)
    );
    let history_terminal = reopened
        .progress_history(&history_cursor, std::num::NonZeroUsize::new(256).unwrap())
        .unwrap();
    assert!(history_terminal.records.iter().any(|frame| frame.changes.iter().any(|change| matches!(change,
        pi_rs_durable::runtime::ProgressHistoryChange::Tool { attempt: seen, state: State::Aborted(Some(value)), .. }
        if *seen == attempt && *value == json!({"text":"later"})
    ))));
    let terminal = reopened
        .tool_progress_events(attempt.attempt_id, second.next, limit)
        .unwrap();
    assert_eq!(terminal.records.len(), 1);
    assert_eq!(
        terminal.records[0].value["state"],
        json!({"state":"aborted","partial":{"text":"later"}})
    );
    assert!(
        reopened
            .tool_progress_events(attempt.attempt_id, terminal.next, limit)
            .unwrap()
            .records
            .is_empty()
    );
    reopened.shutdown().await.unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let snapshot =
        pi_rs_durable::tool_progress::Snapshot::load(&store, attempt.attempt_id).unwrap();
    assert_eq!(
        snapshot.state(),
        &State::Aborted(Some(json!({"text":"later"})))
    );
}

#[tokio::test]
async fn replay_seals_old_attempt_and_completion_seals_fresh_progress() {
    use pi_rs_durable::{
        runtime::Scheduler,
        tool_progress::{Snapshot, State},
    };
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let tool = Arc::new(ProgressTool {
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        host: std::sync::Mutex::new(None),
        fail: false,
    });
    let opts = || Options {
        model: Arc::new(FakeModel {
            calls: AtomicUsize::new(0),
            fail_first: false,
        }),
        tools: vec![RegisteredTool {
            tool: tool.clone(),
            replay: ReplayPolicy::Safe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut scheduler = Scheduler::open(&path, opts()).unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), tool.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let first = scheduler.conversation(id).unwrap().tool_attempt().unwrap();
    // Shutdown interrupts the invocation without recording an abort request.
    scheduler.shutdown().await.unwrap();
    let late = tool.host.lock().unwrap().take().unwrap();
    late.report_progress(json!("late after shutdown"))
        .await
        .unwrap();
    drop(late);
    let mut scheduler = Scheduler::open(&path, opts()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), tool.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let second = scheduler.conversation(id).unwrap().tool_attempt().unwrap();
    let limit = std::num::NonZeroUsize::new(10).unwrap();
    let one = std::num::NonZeroUsize::new(1).unwrap();
    let page = scheduler
        .tool_attempts(id, pi_rs_durable::observer::Cursor(0), one)
        .unwrap();
    assert_eq!(page.records[0].value["attempt"], json!(first));
    assert!(page.has_more);
    let page = scheduler.tool_attempts(id, page.next, one).unwrap();
    assert_eq!(page.records[0].value["attempt"], json!(second));
    assert!(!page.has_more);
    assert!(
        scheduler
            .tool_attempts(id, pi_rs_durable::observer::Cursor(3), one)
            .is_err()
    );
    let old = scheduler
        .tool_progress_events(first.attempt_id, pi_rs_durable::observer::Cursor(0), limit)
        .unwrap();
    assert_eq!(old.records.len(), 2);
    assert_eq!(old.records[0].value["state"]["state"], "running");
    assert_eq!(old.records[1].value["state"]["state"], "aborted");
    let fresh = scheduler
        .tool_progress_events(second.attempt_id, pi_rs_durable::observer::Cursor(0), limit)
        .unwrap();
    assert_eq!(fresh.records.len(), 1);
    assert_eq!(
        fresh.records[0].value["attempt"]["attempt_id"],
        json!(second.attempt_id)
    );
    assert_ne!(first.attempt_id, second.attempt_id);
    assert_eq!(first.operation_id, second.operation_id);
    tool.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    let late = tool.host.lock().unwrap().take().unwrap();
    late.report_progress(json!("late after completion"))
        .await
        .unwrap();
    drop(late);
    let done = scheduler
        .tool_progress_events(second.attempt_id, fresh.next, limit)
        .unwrap();
    assert_eq!(done.records.len(), 1);
    assert_eq!(done.records[0].value["state"], json!({"state":"completed"}));
    assert_eq!(
        done.records[0].value["retained_partial"],
        json!({"text":"working"})
    );
    assert!(
        scheduler
            .tool_progress_events(first.attempt_id, old.next, limit)
            .unwrap()
            .records
            .is_empty()
    );
    scheduler.shutdown().await.unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    assert_eq!(
        Snapshot::load(&store, first.attempt_id).unwrap().state(),
        &State::Aborted(Some(json!({"text":"working"})))
    );
    let completed = Snapshot::load(&store, second.attempt_id).unwrap();
    assert_eq!(completed.state(), &State::Completed);
    assert_eq!(
        completed.retained_partial(),
        Some(&json!({"text":"working"}))
    );
}

#[tokio::test]
async fn failed_tool_retains_null_partial_in_snapshot_and_ordered_events() {
    use pi_rs_durable::{
        observer::Cursor,
        runtime::Scheduler,
        tool_progress::{Snapshot, State},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let tool = Arc::new(ProgressTool {
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        host: std::sync::Mutex::new(None),
        fail: true,
    });
    let opts = Options {
        model: Arc::new(FakeModel {
            calls: AtomicUsize::new(0),
            fail_first: false,
        }),
        tools: vec![RegisteredTool {
            tool: tool.clone(),
            replay: ReplayPolicy::Safe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut scheduler = Scheduler::open(&path, opts).unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), tool.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let attempt = scheduler.conversation(id).unwrap().tool_attempt().unwrap();
    let host = tool.host.lock().unwrap().as_ref().unwrap().clone();
    host.report_progress(serde_json::Value::Null).await.unwrap();
    tool.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(10), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    host.report_progress(json!("late")).await.unwrap();
    drop(host);
    drop(tool.host.lock().unwrap().take());
    let events = scheduler
        .tool_progress_events(
            attempt.attempt_id,
            Cursor(0),
            std::num::NonZeroUsize::new(10).unwrap(),
        )
        .unwrap();
    assert_eq!(events.records.len(), 3);
    assert_eq!(events.records[1].value["partial_is_null"], true);
    assert_eq!(events.records[2].value["state"]["state"], "completed");
    assert_eq!(events.records[2].value["partial_is_null"], true);
    scheduler.shutdown().await.unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let snapshot = Snapshot::load(&store, attempt.attempt_id).unwrap();
    assert_eq!(snapshot.state(), &State::Completed);
    assert_eq!(snapshot.retained_partial(), Some(&serde_json::Value::Null));
}

struct StreamedFixture {
    calls: AtomicUsize,
    mode: &'static str,
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    saved: std::sync::Mutex<Option<pi_rs_durable::keyed_tool::StreamOutput>>,
}
#[async_trait]
impl pi_rs_durable::keyed_tool::StreamedTool for StreamedFixture {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "test".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(
        &self,
        _: serde_json::Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        _: pi_rs_durable::keyed_tool::KeyedHost,
        output: pi_rs_durable::keyed_tool::StreamOutput,
    ) -> pi_rs_durable::keyed_tool::StreamedReply {
        use pi_rs_durable::keyed_tool::{MAX_STREAMED_OUTPUT_BYTES, StreamedReply};
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.mode != "no_stream" {
            output
                .append(if n == 0 { "first" } else { "second" })
                .await
                .unwrap();
        }
        if self.mode == "chunks" {
            output.append("é").await.unwrap();
        }
        *self.saved.lock().unwrap() = Some(output.clone());
        if self.mode == "replay" {
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        if self.mode == "oversize" {
            assert!(
                output
                    .append(&"x".repeat(MAX_STREAMED_OUTPUT_BYTES))
                    .await
                    .is_err()
            );
        }
        if self.mode == "failure" {
            return StreamedReply::Explicit(Outcome::Failed("explicit failure".into()).into());
        }
        StreamedReply::Success {
            output: if self.mode == "empty" {
                Some(String::new())
            } else if self.mode == "override" {
                Some("explicit".into())
            } else {
                None
            },
            state: None,
        }
    }
}
fn stream_options(tool: Arc<StreamedFixture>) -> Options {
    use pi_rs_durable::keyed_tool::{KeyedToolAdapter, StreamedToolAdapter};
    let adapter = Arc::new(KeyedToolAdapter::new(StreamedToolAdapter(
        SharedStreamedFixture(tool),
    )));
    Options {
        model: Arc::new(FakeModel {
            calls: AtomicUsize::new(0),
            fail_first: false,
        }),
        tools: vec![adapter.register(ReplayPolicy::Safe)],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
// Keep the fixture reusable when a scheduler is reopened.
struct SharedStreamedFixture(Arc<StreamedFixture>);
#[async_trait]
impl pi_rs_durable::keyed_tool::StreamedTool for SharedStreamedFixture {
    fn spec(&self) -> ToolSpec {
        self.0.spec()
    }
    async fn invoke(
        &self,
        a: serde_json::Value,
        c: ToolContext,
        s: Option<&pi_rs_core::ExtensionState>,
        h: pi_rs_durable::keyed_tool::KeyedHost,
        o: pi_rs_durable::keyed_tool::StreamOutput,
    ) -> pi_rs_durable::keyed_tool::StreamedReply {
        self.0.invoke(a, c, s, h, o).await
    }
}
fn stream_fixture(mode: &'static str) -> Arc<StreamedFixture> {
    Arc::new(StreamedFixture {
        calls: AtomicUsize::new(0),
        mode,
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        saved: std::sync::Mutex::new(None),
    })
}
#[tokio::test]
async fn streamed_tool_completion_empty_failure_limit_and_reopen() {
    use pi_rs_durable::runtime::Scheduler;
    for (mode, expected) in [
        ("fallback", Some("first")),
        ("no_stream", Some("")),
        ("chunks", Some("firsté")),
        ("override", Some("explicit")),
        ("empty", Some("")),
        ("failure", Some("Tool failed: explicit failure")),
        ("oversize", None),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let tool = stream_fixture(mode);
        let mut scheduler = Scheduler::open(&path, stream_options(tool.clone())).unwrap();
        let id = scheduler.create_conversation(config()).unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        if mode != "oversize" {
            tokio::time::timeout(std::time::Duration::from_secs(10), scheduler.wait(id))
                .await
                .unwrap()
                .unwrap();
        } else {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    if scheduler.blocked(id).unwrap().is_some() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        let records = scheduler
            .tool_attempts(
                id,
                pi_rs_durable::observer::Cursor(0),
                std::num::NonZeroUsize::new(10).unwrap(),
            )
            .unwrap();
        let attempt: pi_rs_durable::runtime::ToolAttempt =
            serde_json::from_value(records.records[0].value["attempt"].clone()).unwrap();
        let saved = tool.saved.lock().unwrap().take().unwrap();
        assert!(saved.append("late").await.is_err());
        if let Some(expected) = expected {
            assert!(
                scheduler
                    .conversation(id)
                    .unwrap()
                    .messages
                    .iter()
                    .any(|m| matches!(m, Message::Tool { content, .. } if content == expected))
            );
        }
        scheduler.shutdown().await.unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        assert_eq!(
            pi_rs_durable::streamed_output::load(&store, attempt.attempt_id)
                .unwrap()
                .as_deref(),
            match mode {
                "no_stream" => None,
                "chunks" => Some("firsté"),
                _ => Some("first"),
            }
        );
        drop(store);
        let reopened = Scheduler::open(&path, stream_options(tool.clone())).unwrap();
        if let Some(expected) = expected {
            assert!(
                reopened
                    .conversation(id)
                    .unwrap()
                    .messages
                    .iter()
                    .any(|m| matches!(m, Message::Tool { content, .. } if content == expected))
            );
        }
        reopened.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn streamed_tool_replay_is_fenced_by_attempt_and_reopened_result() {
    use pi_rs_durable::{observer::Cursor, runtime::Scheduler};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let tool = stream_fixture("replay");
    let mut scheduler = Scheduler::open(&path, stream_options(tool.clone())).unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), tool.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let first = scheduler.conversation(id).unwrap().tool_attempt().unwrap();
    let old = tool.saved.lock().unwrap().take().unwrap();
    scheduler.shutdown().await.unwrap();
    assert!(old.append("stale").await.is_err());
    let mut scheduler = Scheduler::open(&path, stream_options(tool.clone())).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), tool.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let second = scheduler.conversation(id).unwrap().tool_attempt().unwrap();
    assert_ne!(first.attempt_id, second.attempt_id);
    assert_eq!(first.operation_id, second.operation_id);
    assert!(old.append("stale again").await.is_err());
    tool.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(10), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { content, .. } if content == "second"))
    );
    assert_eq!(
        scheduler
            .tool_attempts(id, Cursor(0), std::num::NonZeroUsize::new(10).unwrap())
            .unwrap()
            .records
            .len(),
        2
    );
    scheduler.shutdown().await.unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    assert_eq!(
        pi_rs_durable::streamed_output::load(&store, first.attempt_id)
            .unwrap()
            .as_deref(),
        Some("first")
    );
    assert_eq!(
        pi_rs_durable::streamed_output::load(&store, second.attempt_id)
            .unwrap()
            .as_deref(),
        Some("second")
    );
    drop(store);
    let reopened = Scheduler::open(&path, stream_options(tool)).unwrap();
    assert!(
        reopened
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { content, .. } if content == "second"))
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn interrupted_unsafe_stream_is_not_promoted_or_replayed() {
    use pi_rs_durable::runtime::Scheduler;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let tool = stream_fixture("replay");
    let options = || {
        let mut options = stream_options(tool.clone());
        options.tools[0].replay = ReplayPolicy::Unsafe;
        options
    };
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), tool.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let attempt = scheduler.conversation(id).unwrap().tool_attempt().unwrap();
    scheduler.shutdown().await.unwrap();
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { content, .. } if !content.contains("first")))
    );
    scheduler.shutdown().await.unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    assert_eq!(
        pi_rs_durable::streamed_output::load(&store, attempt.attempt_id)
            .unwrap()
            .as_deref(),
        Some("first")
    );
}

#[test]
fn runtime_block_process_child() {
    let Ok(path) = std::env::var("PI_RS_BLOCK_CRASH_DB") else {
        return;
    };
    let model = Arc::new(FakeModel {
        calls: AtomicUsize::new(0),
        fail_first: false,
    });
    let tool = Arc::new(FakeTool {
        calls: AtomicUsize::new(0),
        interrupt: false,
        cancel: None,
    });
    let mut h = Harness::open_paused(&path, options(model, tool, ReplayPolicy::Unsafe)).unwrap();
    let id = h.create_conversation(config()).unwrap();
    let occurrence = h
        .append_user_blocks(
            id,
            vec![json!({"type":"text","text":"saved","textSignature":"opaque"})],
        )
        .unwrap();
    std::fs::write(format!("{path}.ids"), format!("{id}\n{occurrence}")).unwrap();
    std::process::exit(0);
}

#[test]
fn runtime_block_commit_survives_process_exit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "runtime_block_process_child", "--nocapture"])
        .env("PI_RS_BLOCK_CRASH_DB", &path)
        .status()
        .unwrap();
    assert!(status.success());
    let ids = std::fs::read_to_string(format!("{}.ids", path.display())).unwrap();
    let mut ids = ids.lines();
    let id = ids.next().unwrap().parse().unwrap();
    let occurrence = ids.next().unwrap();
    let model = Arc::new(FakeModel {
        calls: AtomicUsize::new(0),
        fail_first: false,
    });
    let tool = Arc::new(FakeTool {
        calls: AtomicUsize::new(0),
        interrupt: false,
        cancel: None,
    });
    let mut h = Harness::open_paused(&path, options(model, tool, ReplayPolicy::Unsafe)).unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(c.content_blocks[&0].message_id.to_string(), occurrence);
    assert_eq!(
        c.content_blocks[&0].blocks,
        vec![json!({"type":"text","text":"saved","textSignature":"opaque"})]
    );
    let fork = h.fork(id).unwrap();
    assert_eq!(
        h.conversation(fork).unwrap().content_blocks,
        c.content_blocks
    );
    assert_eq!(h.request_manual_compaction(id, 0, None).unwrap(), None);
    drop(h);
    let model = Arc::new(FakeModel {
        calls: AtomicUsize::new(0),
        fail_first: false,
    });
    let tool = Arc::new(FakeTool {
        calls: AtomicUsize::new(0),
        interrupt: false,
        cancel: None,
    });
    let h = Harness::open_paused(&path, options(model, tool, ReplayPolicy::Unsafe)).unwrap();
    assert_eq!(
        h.conversation(fork).unwrap().content_blocks,
        c.content_blocks
    );
}
