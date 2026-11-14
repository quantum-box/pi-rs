use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{CancellationToken, Error, Message, Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::{
    observer::Cursor,
    recovery::UncertainPolicy,
    runtime::{Config, Harness, Options},
};
use std::{num::NonZeroUsize, sync::Arc};
struct ModelStub;
#[async_trait]
impl Model for ModelStub {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let summary=request.messages.iter().any(|m|matches!(m,Message::System{content} if content.starts_with("You are a context summarization")));
        let text = if summary {
            "summary of prior work"
        } else {
            "answer"
        };
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text(text.into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
fn opts() -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![],
        restart_limit: 5,
        max_turns: 5,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}

#[tokio::test]
async fn manual_compaction_rejects_an_earlier_pending_abort() {
    use pi_rs_durable::runtime::ManualCompactionStatus;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        h.submit_and_run(id, None, "old".into(), &cancel)
            .await
            .unwrap();
        h.submit_and_run(id, None, "new".into(), &cancel)
            .await
            .unwrap();
        h.request_abort(id).unwrap();
        assert!(h.request_manual_compaction(id, 0, None).is_err());
        assert!(h.conversation(id).unwrap().manual_compactions.is_empty());
    }
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    assert!(h.request_manual_compaction(id, 0, None).is_err());
    h.resume(&cancel).await.unwrap();
    h.continue_queued(id).unwrap();
    let op = h.request_manual_compaction(id, 0, None).unwrap().unwrap();
    h.resume(&cancel).await.unwrap();
    let records = h.conversation(id).unwrap().manual_compactions;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id, op);
    assert_eq!(records[0].status, ManualCompactionStatus::Applied);
}

#[tokio::test]
async fn manual_compaction_model_error_remains_running_until_abort_after_reopen() {
    use pi_rs_durable::runtime::ManualCompactionStatus;
    struct FailingSummary;
    #[async_trait]
    impl Model for FailingSummary {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
            if request.messages.iter().any(|m| matches!(m, Message::System { content } if content.starts_with("You are a context summarization"))) {
                return Err(Error::Model("summary failed".into()));
            }
            ModelStub.stream(request).await
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let options = || Options {
        model: Arc::new(FailingSummary),
        ..opts()
    };
    let id;
    let op;
    {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        h.submit_and_run(id, None, "old".into(), &cancel)
            .await
            .unwrap();
        h.submit_and_run(id, None, "new".into(), &cancel)
            .await
            .unwrap();
        op = h.request_manual_compaction(id, 0, None).unwrap().unwrap();
        assert!(h.resume(&cancel).await.is_err());
    }
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        h.conversation(id).unwrap().manual_compactions[0].status,
        ManualCompactionStatus::Running
    );
    h.request_abort(id).unwrap();
    h.resume(&cancel).await.unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let record = &h.conversation(id).unwrap().manual_compactions[0];
    assert_eq!(record.id, op);
    assert_eq!(record.status, ManualCompactionStatus::Aborted);
}

#[tokio::test]
async fn bounded_conversation_history_reopens_and_forks_at_captured_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let id;
    let boundary;
    let first;
    {
        let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        h.submit_and_run(id, None, "before boundary".into(), &cancel)
            .await
            .unwrap();
        boundary = h.history_head(id).unwrap();
        first = h
            .history_through(id, Cursor(0), boundary, NonZeroUsize::MIN)
            .unwrap();
        assert!(first.has_more);
        h.submit_and_run(id, None, "after boundary".into(), &cancel)
            .await
            .unwrap();
    }
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let mut cursor = first.next;
    let mut records = first.records;
    loop {
        let page = h
            .history_through(id, cursor, boundary, NonZeroUsize::MIN)
            .unwrap();
        cursor = page.next;
        records.extend(page.records);
        if !page.has_more {
            break;
        }
    }
    assert_eq!(cursor, boundary);
    assert_eq!(
        records,
        h.history(id, Cursor(0), NonZeroUsize::new(records.len()).unwrap())
            .unwrap()
            .records
    );
    let last = &records.last().unwrap().value;
    assert!(last["messages"].to_string().contains("before boundary"));
    assert!(!last.to_string().contains("after boundary"));
    assert!(last.get("_transcript").is_none());
    let child = h.fork_at(id, boundary).unwrap();
    let child_head = h.history_head(child).unwrap();
    assert_eq!(child_head, Cursor(1));
    let child_history = h
        .history_through(child, Cursor(0), child_head, NonZeroUsize::MIN)
        .unwrap();
    assert_eq!(child_history.next, child_head);
    assert!(!child_history.has_more);
    assert!(
        child_history.records[0]
            .value
            .to_string()
            .contains("before boundary")
    );
    assert!(
        !child_history.records[0]
            .value
            .to_string()
            .contains("after boundary")
    );
    assert!(
        h.history_through(child, Cursor(0), boundary, NonZeroUsize::MIN)
            .is_err()
    );
    let empty = h
        .history_through(child, Cursor(0), Cursor(0), NonZeroUsize::MIN)
        .unwrap();
    assert!(empty.records.is_empty());
    assert_eq!(empty.next, Cursor(0));
    assert!(!empty.has_more);
    assert!(
        h.conversation(child)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::User { content } if content == "before boundary"))
    );
    assert!(
        !h.conversation(child)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::User { content } if content == "after boundary"))
    );
    let live = h
        .watch_history(
            id,
            cursor,
            NonZeroUsize::new(100).unwrap(),
            std::time::Duration::from_millis(1),
        )
        .await
        .unwrap();
    assert!(
        live.records
            .iter()
            .any(|r| r.value.to_string().contains("after boundary"))
    );
    assert!(
        h.history_through(id, Cursor(boundary.0 + 1), boundary, NonZeroUsize::MIN)
            .is_err()
    );
    assert!(
        h.history_through(
            id,
            Cursor(0),
            Cursor(h.history_head(id).unwrap().0 + 1),
            NonZeroUsize::MIN
        )
        .is_err()
    );
    assert!(
        h.history_through(child, Cursor(0), Cursor(u64::MAX), NonZeroUsize::MIN)
            .is_err()
    );
    assert!(h.history_head(uuid::Uuid::new_v4()).is_err());
}
#[tokio::test]
async fn compaction_is_durable_and_old_snapshots_remain_observable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        h.submit_and_run(id, None, "first question".into(), &cancel)
            .await
            .unwrap();
        h.submit_and_run(id, None, "latest question".into(), &cancel)
            .await
            .unwrap();
        assert!(h.request_compaction(id, 0).unwrap());
        h.resume(&cancel).await.unwrap();
    }
    let h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let c = h.conversation(id).unwrap();
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m,Message::User{content} if content=="latest question"))
    );
    assert!(
        c.messages.iter().any(
            |m| matches!(m,Message::User{content} if content.contains("summary of prior work"))
        )
    );
    let page = h
        .history(id, Cursor(0), NonZeroUsize::new(1000).unwrap())
        .unwrap();
    assert!(
        page.records
            .iter()
            .any(|r| r.value.to_string().contains("first question"))
    );
}

#[tokio::test]
async fn manual_compaction_instructions_survive_reopen() {
    use pi_rs_durable::extension::{BeforeCompact, CompactionInput, Context, Hook};
    struct Capture(Arc<std::sync::Mutex<Vec<Option<String>>>>);
    #[async_trait]
    impl Hook for Capture {
        async fn before_compact(
            &self,
            input: &CompactionInput,
            _: &Context<'_>,
        ) -> Result<BeforeCompact, String> {
            self.0.lock().unwrap().push(input.instructions.clone());
            Ok(BeforeCompact::Pass)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let id;
    {
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        h.install_extension(compact_extension(vec![Arc::new(Capture(seen.clone()))]))
            .unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        h.submit_and_run(id, None, "old".into(), &cancel)
            .await
            .unwrap();
        h.submit_and_run(id, None, "latest".into(), &cancel)
            .await
            .unwrap();
        h.select_extensions(id, vec!["compact".into()]).unwrap();
        assert!(
            h.request_compaction_with_instructions(id, 0, Some("keep test names"))
                .unwrap()
        );
    }
    {
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        h.install_extension(compact_extension(vec![Arc::new(Capture(seen.clone()))]))
            .unwrap();
        h.resume(&cancel).await.unwrap();
        assert!(h.conversation(id).unwrap().messages.iter().any(
            |m| matches!(m, Message::User { content } if content.contains("summary of prior work"))
        ));
    }
    assert_eq!(*seen.lock().unwrap(), vec![Some("keep test names".into())]);
}

#[tokio::test]
async fn manual_compaction_retry_preserves_request_and_context() {
    use pi_rs_durable::extension::{BeforeCompact, CompactionInput, Context, Hook};
    use std::sync::Mutex;

    struct CaptureHook(Arc<Mutex<Vec<CompactionInput>>>);
    #[async_trait]
    impl Hook for CaptureHook {
        async fn before_compact(
            &self,
            input: &CompactionInput,
            _: &Context<'_>,
        ) -> Result<BeforeCompact, String> {
            self.0.lock().unwrap().push(input.clone());
            Ok(BeforeCompact::Pass)
        }
    }
    struct RetryModel(Arc<Mutex<Vec<ModelRequest>>>);
    #[async_trait]
    impl Model for RetryModel {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
            if request.messages.iter().any(|m| matches!(m, Message::System { content } if content.starts_with("You are a context summarization"))) {
                let mut requests = self.0.lock().unwrap();
                requests.push(request);
                if requests.len() == 1 {
                    return Ok(stream::pending().boxed());
                }
                return Ok(stream::iter(vec![
                    Ok(ModelEvent::Text("recovered summary".into())),
                    Ok(ModelEvent::Complete { calls: vec![], usage: None }),
                ]).boxed());
            }
            ModelStub.stream(request).await
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let hooks = Arc::new(Mutex::new(Vec::new()));
    let options = || Options {
        model: Arc::new(RetryModel(requests.clone())),
        ..opts()
    };
    let id;
    let operation;
    {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.install_extension(compact_extension(vec![Arc::new(CaptureHook(
            hooks.clone(),
        ))]))
        .unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "system".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        h.select_extensions(id, vec!["compact".into()]).unwrap();
        h.submit_and_run(id, None, "old".into(), &cancel)
            .await
            .unwrap();
        h.submit_and_run(id, None, "latest".into(), &cancel)
            .await
            .unwrap();
        operation = h
            .request_manual_compaction(id, 0, Some("preserve names"))
            .unwrap()
            .unwrap();
        let cursor = h.history_head(id).unwrap();
        let fork = h.fork_at(id, cursor).unwrap();
        assert!(h.conversation(fork).unwrap().manual_compactions.is_empty());
        let stop = CancellationToken::new();
        let trigger = stop.clone();
        let captured = requests.clone();
        tokio::spawn(async move {
            while captured.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
            trigger.cancel();
        });
        assert!(h.resume(&stop).await.is_err());
    }
    {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        assert_eq!(
            h.conversation(id).unwrap().manual_compactions[0],
            pi_rs_durable::runtime::ManualCompaction {
                id: operation,
                status: pi_rs_durable::runtime::ManualCompactionStatus::Running
            }
        );
        h.install_extension(compact_extension(vec![Arc::new(CaptureHook(
            hooks.clone(),
        ))]))
        .unwrap();
        h.resume(&cancel).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(
            c.manual_compactions[0].status,
            pi_rs_durable::runtime::ManualCompactionStatus::Applied
        );
        let messages = c.messages;
        assert!(
            matches!(&messages[0], Message::User { content } if content.contains("recovered summary"))
        );
        assert!(matches!(&messages[1], Message::User { content } if content == "latest"));
        let fork = h.fork(id).unwrap();
        assert!(h.conversation(fork).unwrap().manual_compactions.is_empty());
    }
    {
        let h = Harness::open_paused(&path, options()).unwrap();
        assert_eq!(
            h.conversation(id).unwrap().manual_compactions[0],
            pi_rs_durable::runtime::ManualCompaction {
                id: operation,
                status: pi_rs_durable::runtime::ManualCompactionStatus::Applied
            }
        );
    }
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
    assert!(
        matches!(&requests[0].messages[1], Message::User { content } if content.contains("preserve names") && content.contains("old") && !content.contains("latest"))
    );
    let hooks = hooks.lock().unwrap();
    assert_eq!(hooks.len(), 1);
    assert_eq!(hooks[0].instructions.as_deref(), Some("preserve names"));
    assert!(matches!(&hooks[0].messages[0], Message::User { content } if content == "old"));
}

#[tokio::test]
async fn manual_compaction_restart_limit_has_terminal_status() {
    use pi_rs_durable::runtime::ManualCompactionStatus;
    struct PendingSummary(Arc<tokio::sync::Semaphore>);
    #[async_trait]
    impl Model for PendingSummary {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
            if request.messages.iter().any(|m| matches!(m, Message::System { content } if content.starts_with("You are a context summarization"))) {
                self.0.add_permits(1);
                Ok(stream::pending().boxed())
            } else { ModelStub.stream(request).await }
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let options = || Options {
        model: Arc::new(PendingSummary(started.clone())),
        restart_limit: 1,
        ..opts()
    };
    let cancel = CancellationToken::new();
    let id;
    let operation;
    {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        h.submit_and_run(id, None, "old".into(), &cancel)
            .await
            .unwrap();
        h.submit_and_run(id, None, "new".into(), &cancel)
            .await
            .unwrap();
        operation = h.request_manual_compaction(id, 0, None).unwrap().unwrap();
        let stop = CancellationToken::new();
        let trigger = stop.clone();
        let signal = started.clone();
        tokio::spawn(async move {
            signal.acquire().await.unwrap().forget();
            trigger.cancel();
        });
        assert!(h.resume(&stop).await.is_err());
    }
    {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.resume(&cancel).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(c.manual_compactions[0].id, operation);
        assert_eq!(
            c.manual_compactions[0].status,
            ManualCompactionStatus::RestartLimit
        );
        assert!(
            c.messages
                .iter()
                .any(|m| matches!(m, Message::User { content } if content == "old"))
        );
    }
}

#[tokio::test]
async fn ordered_compaction_crash_child() {
    let Ok(path) = std::env::var("PI_ORDERED_COMPACTION_DB") else {
        return;
    };
    struct WaitingModel(String);
    #[async_trait]
    impl Model for WaitingModel {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
            if request.messages.iter().any(|m| matches!(m, Message::System { content } if content.starts_with("You are a context summarization"))) {
                std::fs::write(format!("{}.running", self.0), b"running").unwrap();
                Ok(stream::pending().boxed())
            } else { ModelStub.stream(request).await }
        }
    }
    let mut h = Harness::open_paused(
        &path,
        Options {
            model: Arc::new(WaitingModel(path.clone())),
            ..opts()
        },
    )
    .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    let cancel = CancellationToken::new();
    h.submit_and_run(id, None, "old".into(), &cancel)
        .await
        .unwrap();
    h.submit_and_run(id, None, "new".into(), &cancel)
        .await
        .unwrap();
    let operation = h.request_manual_compaction(id, 0, None).unwrap().unwrap();
    let reset = h.reset(id, Some("after crash".into())).unwrap();
    std::fs::write(format!("{path}.ids"), format!("{id} {operation} {reset}")).unwrap();
    h.resume(&cancel).await.unwrap();
}

#[tokio::test]
async fn reset_before_summary_survives_process_death_and_recovery() {
    use pi_rs_durable::runtime::ManualCompactionStatus;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let running = dir.path().join("db.running");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "ordered_compaction_crash_child"])
        .env("PI_ORDERED_COMPACTION_DB", &path)
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !running.exists() {
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("summary did not start")
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let ids = std::fs::read_to_string(dir.path().join("db.ids")).unwrap();
    let mut ids = ids
        .split_whitespace()
        .map(|s| uuid::Uuid::parse_str(s).unwrap());
    let id = ids.next().unwrap();
    let operation = ids.next().unwrap();
    let reset = ids.next().unwrap();
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    h.resume(&CancellationToken::new()).await.unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(c.manual_compactions[0].id, operation);
    assert_eq!(
        c.manual_compactions[0].status,
        ManualCompactionStatus::Stale
    );
    assert!(c.resets.iter().any(|r| r.id == reset));
    assert_eq!(
        c.messages,
        vec![Message::User {
            content: "after crash".into()
        }]
    );
}

#[tokio::test]
async fn reset_queued_during_manual_summary_precedes_its_placement() {
    use pi_rs_durable::runtime::{ManualCompactionStatus, Scheduler};
    use tokio::sync::Semaphore;
    struct GatedSummary {
        started: Arc<Semaphore>,
        release: Arc<Semaphore>,
    }
    #[async_trait]
    impl Model for GatedSummary {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
            if request.messages.iter().any(|m| matches!(m, Message::System { content } if content.starts_with("You are a context summarization"))) {
                self.started.add_permits(1);
                let release = self.release.clone();
                Ok(stream::once(async move { release.acquire().await.unwrap().forget(); Ok(ModelEvent::Text("checkpoint".into())) }).chain(stream::iter(vec![Ok(ModelEvent::Complete { calls: vec![], usage: None })])).boxed())
            } else { ModelStub.stream(request).await }
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        h.submit_and_run(id, None, "old".into(), &cancel)
            .await
            .unwrap();
        h.submit_and_run(id, None, "new".into(), &cancel)
            .await
            .unwrap();
    }
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let mut s = Scheduler::open(
        &path,
        Options {
            model: Arc::new(GatedSummary {
                started: started.clone(),
                release: release.clone(),
            }),
            ..opts()
        },
    )
    .unwrap();
    let operation = s.request_manual_compaction(id, 0, None).unwrap().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let reset = s.reset(id, Some("handoff".into())).unwrap();
    release.add_permits(1);
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        s.wait_for_idle(id, &cancel),
    )
    .await
    .unwrap()
    .unwrap();
    let c = s.conversation(id).unwrap();
    assert_eq!(c.manual_compactions[0].id, operation);
    assert_eq!(
        c.manual_compactions[0].status,
        ManualCompactionStatus::Stale
    );
    assert!(c.resets.iter().any(|r| r.id == reset));
    assert_eq!(
        c.messages,
        vec![Message::User {
            content: "handoff".into()
        }]
    );
    s.shutdown().await.unwrap();
    let h = Harness::open_paused(&path, opts()).unwrap();
    assert_eq!(
        h.conversation(id).unwrap().manual_compactions[0].status,
        ManualCompactionStatus::Stale
    );
}

#[tokio::test]
async fn manual_compaction_abort_during_summary_survives_reopen() {
    use pi_rs_durable::runtime::{ManualCompactionStatus, Scheduler};
    use tokio::sync::Semaphore;
    struct PendingSummary(Arc<Semaphore>);
    #[async_trait]
    impl Model for PendingSummary {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
            if request.messages.iter().any(|m| matches!(m, Message::System { content } if content.starts_with("You are a context summarization"))) {
                self.0.add_permits(1);
                Ok(stream::pending().boxed())
            } else {
                ModelStub.stream(request).await
            }
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        h.submit_and_run(id, None, "old".into(), &cancel)
            .await
            .unwrap();
        h.submit_and_run(id, None, "new".into(), &cancel)
            .await
            .unwrap();
    }
    let started = Arc::new(Semaphore::new(0));
    let options = Options {
        model: Arc::new(PendingSummary(started.clone())),
        ..opts()
    };
    let mut scheduler = Scheduler::open(&path, options).unwrap();
    let operation = scheduler
        .request_manual_compaction(id, 0, None)
        .unwrap()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert_eq!(
        scheduler.conversation(id).unwrap().manual_compactions[0].status,
        ManualCompactionStatus::Running
    );
    let queued = scheduler
        .submit(id, None, "queued during summary".into())
        .unwrap();
    scheduler.abort(id).unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        scheduler.wait_for_idle(id, &cancel),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        scheduler.conversation(id).unwrap().manual_compactions[0].status,
        ManualCompactionStatus::Aborted
    );
    scheduler.shutdown().await.unwrap();
    let h = Harness::open_paused(&path, opts()).unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(c.manual_compactions[0].id, operation);
    assert_eq!(
        c.manual_compactions[0].status,
        ManualCompactionStatus::Aborted
    );
    assert!(c.submissions.iter().any(|s| s.id == queued && matches!(&s.status, pi_rs_durable::runtime::Status::Unanswered { reason } if reason == "aborted")));
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m, Message::User { content } if content == "old"))
    );
}

#[tokio::test]
async fn fork_copies_settled_context_but_not_submissions() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let mut h = Harness::open(dir.path().join("db"), opts(), &cancel)
        .await
        .unwrap();
    let parent = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    h.submit_and_run(parent, Some("request".into()), "original".into(), &cancel)
        .await
        .unwrap();
    let fork = h.fork(parent).unwrap();
    assert!(h.conversation(fork).unwrap().submissions.is_empty());
    assert_eq!(
        h.conversation(fork).unwrap().messages,
        h.conversation(parent).unwrap().messages
    );
    h.submit_and_run(fork, Some("request".into()), "forked".into(), &cancel)
        .await
        .unwrap();
    assert_eq!(h.conversation(parent).unwrap().submissions.len(), 1);
    assert_eq!(h.conversation(fork).unwrap().submissions.len(), 1);
    assert_ne!(
        h.conversation(fork).unwrap().messages,
        h.conversation(parent).unwrap().messages
    );
}

#[tokio::test]
async fn forks_keep_application_entries_at_the_selected_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let mut h = Harness::open(dir.path().join("db"), opts(), &cancel)
        .await
        .unwrap();
    let parent = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    h.submit_write(
        parent,
        Some("first".into()),
        "app.note".into(),
        Some(serde_json::json!(1)),
    )
    .unwrap();
    let cursor = h
        .history(parent, Cursor(0), NonZeroUsize::new(100).unwrap())
        .unwrap()
        .next;
    h.submit_write(
        parent,
        Some("second".into()),
        "app.note".into(),
        Some(serde_json::json!(2)),
    )
    .unwrap();
    let historical = h.fork_at(parent, cursor).unwrap();
    let current = h.fork(parent).unwrap();
    let old = h.conversation(historical).unwrap();
    assert_eq!(old.application_writes.len(), 1);
    assert_eq!(old.application_writes[0].data, Some(serde_json::json!(1)));
    assert!(old.application_writes[0].request_id.is_none());
    assert_eq!(h.conversation(current).unwrap().application_writes.len(), 2);
    h.submit_write(historical, Some("first".into()), "app.new".into(), None)
        .unwrap();
    assert_eq!(
        h.conversation(historical).unwrap().application_writes.len(),
        2
    );
}

#[tokio::test]
async fn scheduler_runs_atomically_admitted_document_input() {
    use pi_rs_durable::{
        document::{Address, Documents},
        runtime::{AdmissionOptions, Scheduler, Status},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let address = Address {
        scope: "test".into(),
        name: "counter".into(),
        key: "".into(),
    };
    let mut s = Scheduler::open(&path, opts()).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    s.submit_with_documents(
        id,
        None,
        "hello".into(),
        AdmissionOptions::default(),
        |docs| {
            assert!(docs.get(&address)?.is_none());
            docs.put(
                &address,
                serde_json::json!({"n":1}).as_object().unwrap().clone(),
            )
        },
    )
    .unwrap();
    s.wait(id).await.unwrap();
    assert!(matches!(
        s.conversation(id).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
    assert_eq!(s.document(&address).unwrap().unwrap().value["n"], 1);
    s.shutdown().await.unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    assert_eq!(
        Documents::new(&store).get(&address).unwrap().unwrap().value["n"],
        1
    );
}

#[tokio::test]
async fn historical_fork_uses_selected_settled_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let mut h = Harness::open(dir.path().join("db"), opts(), &cancel)
        .await
        .unwrap();
    let parent = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    h.submit_and_run(parent, None, "first".into(), &cancel)
        .await
        .unwrap();
    let cursor = h
        .history(parent, Cursor(0), NonZeroUsize::new(1000).unwrap())
        .unwrap()
        .next;
    h.submit_and_run(parent, None, "second".into(), &cancel)
        .await
        .unwrap();
    let fork = h.fork_at(parent, cursor).unwrap();
    let c = h.conversation(fork).unwrap();
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m,Message::User{content} if content=="first"))
    );
    assert!(
        !c.messages
            .iter()
            .any(|m| matches!(m,Message::User{content} if content=="second"))
    );
    assert!(c.submissions.is_empty());
}

struct AutoModel {
    summaries: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl Model for AutoModel {
    async fn context_window(&self, _: &str) -> Option<u32> {
        Some(20_000)
    }
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let summary=request.messages.iter().any(|m|matches!(m,Message::System{content} if content.starts_with("You are a context summarization")));
        if summary {
            self.summaries
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        let output = if summary {
            "older work summary"
        } else {
            "answer"
        };
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text(output.into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
#[tokio::test]
async fn automatically_compacts_before_request_when_context_fills() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let model = Arc::new(AutoModel {
        summaries: std::sync::atomic::AtomicUsize::new(0),
    });
    let options = Options {
        model: model.clone(),
        tools: vec![],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut h = Harness::open(dir.path().join("db"), options, &cancel)
        .await
        .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    h.submit_and_run(id, None, "x".repeat(90_000), &cancel)
        .await
        .unwrap();
    h.submit_and_run(id, None, "latest".into(), &cancel)
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(model.summaries.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m,Message::User{content} if content.contains("older work summary")))
    );
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m,Message::User{content} if content=="latest"))
    );
}
struct SmallModel {
    summaries: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl Model for SmallModel {
    async fn context_window(&self, _: &str) -> Option<u32> {
        Some(4096)
    }
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let summary=request.messages.iter().any(|m|matches!(m,Message::System{content} if content.starts_with("You are a context summarization")));
        if summary {
            self.summaries
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text(
                if summary { "short summary" } else { "answer" }.into(),
            )),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
#[tokio::test]
async fn automatic_compaction_scales_retention_to_small_context() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let model = Arc::new(SmallModel {
        summaries: std::sync::atomic::AtomicUsize::new(0),
    });
    let options = Options {
        model: model.clone(),
        tools: vec![],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut h = Harness::open(dir.path().join("db"), options, &cancel)
        .await
        .unwrap();
    let id = h
        .create_conversation(Config {
            model: "small".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    h.submit_and_run(id, None, "x".repeat(14_000), &cancel)
        .await
        .unwrap();
    h.submit_and_run(id, None, "latest".into(), &cancel)
        .await
        .unwrap();
    assert_eq!(model.summaries.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(
        h.conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m,Message::User{content} if content.contains("short summary")))
    );
}

#[tokio::test]
async fn idle_reset_preserves_observed_history_and_uses_handoff() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let path = dir.path().join("db");
    let id;
    {
        let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        h.submit_and_run(id, None, "old task".into(), &cancel)
            .await
            .unwrap();
        h.reset(id, Some("new handoff".into())).unwrap();
    }
    let h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(
        c.messages,
        vec![Message::User {
            content: "new handoff".into()
        }]
    );
    assert_eq!(c.resets.len(), 1);
    let page = h
        .history(id, Cursor(0), NonZeroUsize::new(1000).unwrap())
        .unwrap();
    assert!(
        page.records
            .iter()
            .any(|r| r.value.to_string().contains("old task"))
    );
}
struct CountingModel(std::sync::atomic::AtomicUsize);
#[async_trait]
impl Model for CountingModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
#[tokio::test]
async fn input_is_placed_before_second_follow_up_is_admitted() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let model = Arc::new(CountingModel(std::sync::atomic::AtomicUsize::new(0)));
    let options = Options {
        model: model.clone(),
        tools: vec![],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut h = Harness::open(dir.path().join("db"), options, &cancel)
        .await
        .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.submit(id, None, "first".into()).unwrap();
    h.submit(id, None, "followup".into()).unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(
        c.submissions[0].status,
        pi_rs_durable::runtime::Status::Running
    );
    assert_eq!(
        c.submissions[1].status,
        pi_rs_durable::runtime::Status::Queued
    );
    h.resume(&cancel).await.unwrap();
    assert_eq!(model.0.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn mid_run_cut_survives_reopen_and_does_not_inherit_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let (parent, child, selected);
    {
        let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
        parent = h
            .create_conversation(Config {
                model: "historical".into(),
                system_prompt: "system".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        let reset = h.reset(parent, Some("handoff".into())).unwrap();
        h.submit_write(parent, Some("old-write".into()), "app.note".into(), None)
            .unwrap();
        h.submit_and_run(
            parent,
            Some("parent-request".into()),
            "mid-run question".into(),
            &cancel,
        )
        .await
        .unwrap();
        let page = h
            .history(parent, Cursor(0), NonZeroUsize::new(1000).unwrap())
            .unwrap();
        let record = page
            .records
            .iter()
            .find(|r| {
                let messages = r.value["messages"].as_array().unwrap();
                messages
                    .iter()
                    .any(|m| m.to_string().contains("mid-run question"))
                    && !messages.iter().any(|m| m.to_string().contains("answer"))
                    && r.value["submissions"].to_string().contains("Running")
            })
            .expect("active cut after user placement");
        selected =
            serde_json::from_value::<Vec<Message>>(record.value["messages"].clone()).unwrap();
        child = h.fork_at(parent, record.cursor).unwrap();
        let c = h.conversation(child).unwrap();
        assert_eq!(c.messages, selected);
        assert_eq!(c.config.model, "historical");
        assert!(c.submissions.is_empty());
        assert_eq!(c.resets.last().unwrap().id, reset);
        assert_eq!(c.application_writes[0].after_reset, Some(reset));
        assert!(c.application_writes[0].request_id.is_none());
    }
    {
        let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
        assert_eq!(h.conversation(child).unwrap().messages, selected);
        assert!(h.conversation(child).unwrap().submissions.is_empty());
        h.submit_and_run(
            child,
            Some("parent-request".into()),
            "child question".into(),
            &cancel,
        )
        .await
        .unwrap();
        assert!(
            !h.conversation(parent)
                .unwrap()
                .messages
                .iter()
                .any(|m| matches!(m, Message::User { content } if content == "child question"))
        );
        let grandchild = h
            .fork_at(
                child,
                h.history(child, Cursor(0), NonZeroUsize::new(1000).unwrap())
                    .unwrap()
                    .next,
            )
            .unwrap();
        assert_eq!(
            h.conversation(grandchild).unwrap().messages,
            h.conversation(child).unwrap().messages
        );
    }
}

#[tokio::test]
async fn fork_drops_queued_reset_and_unplaced_write() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let mut h = Harness::open(dir.path().join("db"), opts(), &cancel)
        .await
        .unwrap();
    let parent = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 128,
        })
        .unwrap();
    h.submit(parent, None, "active".into()).unwrap();
    h.reset(parent, Some("queued handoff".into())).unwrap();
    h.submit_write(parent, None, "app.pending".into(), None)
        .unwrap();
    h.submit(parent, None, "queued input".into()).unwrap();
    let cursor = h
        .history(parent, Cursor(0), NonZeroUsize::new(100).unwrap())
        .unwrap()
        .next;
    let child = h.fork_at(parent, cursor).unwrap();
    let c = h.conversation(child).unwrap();
    assert!(c.submissions.is_empty());
    assert!(c.resets.is_empty());
    assert!(c.application_writes.is_empty());
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m, Message::User { content } if content == "active"))
    );
    assert!(!c.messages.iter().any(|m| matches!(m, Message::User { content } if content == "queued input" || content == "queued handoff")));
    h.submit_write(child, None, "app.child".into(), None)
        .unwrap();
    assert!(h.conversation(child).unwrap().application_writes[0].placed);
}

struct CompactHook {
    decision: pi_rs_durable::extension::BeforeCompact,
    seen: Arc<std::sync::Mutex<Vec<pi_rs_durable::extension::CompactionReason>>>,
    wait: bool,
    fail: bool,
}
#[async_trait]
impl pi_rs_durable::extension::Hook for CompactHook {
    async fn before_compact(
        &self,
        input: &pi_rs_durable::extension::CompactionInput,
        ctx: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::BeforeCompact, String> {
        assert!(!input.messages.is_empty());
        self.seen.lock().unwrap().push(input.reason);
        let value = ctx
            .memo
            .get_or_insert(
                "compact-test",
                serde_json::json!(if self.fail { 99 } else { 42 }),
            )
            .await?;
        assert_eq!(value, serde_json::json!(42));
        if self.wait {
            std::future::pending::<()>().await;
        }
        if self.fail {
            return Err("hook error".into());
        }
        Ok(self.decision.clone())
    }
}
fn compact_extension(
    hooks: Vec<Arc<dyn pi_rs_durable::extension::Hook>>,
) -> pi_rs_durable::extension::Extension {
    pi_rs_durable::extension::Extension {
        name: "compact".into(),
        hooks,
        ..Default::default()
    }
}
fn hook(
    decision: pi_rs_durable::extension::BeforeCompact,
    seen: &Arc<std::sync::Mutex<Vec<pi_rs_durable::extension::CompactionReason>>>,
    wait: bool,
    fail: bool,
) -> Arc<dyn pi_rs_durable::extension::Hook> {
    Arc::new(CompactHook {
        decision,
        seen: seen.clone(),
        wait,
        fail,
    })
}
#[tokio::test]
async fn before_compact_replays_memo_after_cancellation_and_first_decision_wins() {
    use pi_rs_durable::extension::{BeforeCompact, CompactionReason};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let seen = Arc::new(std::sync::Mutex::new(vec![]));
    let id;
    {
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        h.install_extension(compact_extension(vec![hook(
            BeforeCompact::Pass,
            &seen,
            true,
            false,
        )]))
        .unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        h.select_extensions(id, vec!["compact".into()]).unwrap();
        h.submit_and_run(id, None, "old".into(), &cancel)
            .await
            .unwrap();
        h.submit_and_run(id, None, "latest".into(), &cancel)
            .await
            .unwrap();
        assert!(h.request_compaction(id, 0).unwrap());
        let stop = CancellationToken::new();
        let trigger = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            trigger.cancel();
        });
        assert!(h.resume(&stop).await.is_err());
    }
    {
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        h.install_extension(compact_extension(vec![
            hook(BeforeCompact::Pass, &seen, false, true),
            hook(
                BeforeCompact::Summary("hook checkpoint".into()),
                &seen,
                false,
                false,
            ),
            hook(BeforeCompact::Decline, &seen, false, false),
        ]))
        .unwrap();
        h.resume(&cancel).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert!(c.messages.iter().any(
            |m| matches!(m, Message::User { content } if content.contains("hook checkpoint"))
        ));
        assert!(
            h.extension_reports()
                .unwrap()
                .iter()
                .any(|batch| batch.conversation_id == id
                    && batch.phase == "before_compact"
                    && !batch.reports.is_empty())
        );
    }
    assert_eq!(*seen.lock().unwrap(), vec![CompactionReason::Manual; 3]);
}

#[tokio::test]
async fn before_compact_decline_preserves_messages() {
    use pi_rs_durable::extension::BeforeCompact;
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let seen = Arc::new(std::sync::Mutex::new(vec![]));
    let mut h = Harness::open_paused(dir.path().join("db"), opts()).unwrap();
    h.install_extension(compact_extension(vec![hook(
        BeforeCompact::Decline,
        &seen,
        false,
        false,
    )]))
    .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    h.select_extensions(id, vec!["compact".into()]).unwrap();
    h.submit_and_run(id, None, "old".into(), &cancel)
        .await
        .unwrap();
    assert_eq!(h.request_manual_compaction(id, 0, None).unwrap(), None);
    assert!(h.conversation(id).unwrap().manual_compactions.is_empty());
    h.submit_and_run(id, None, "latest".into(), &cancel)
        .await
        .unwrap();
    let before = h.conversation(id).unwrap().messages;
    let operation = h.request_manual_compaction(id, 0, None).unwrap().unwrap();
    h.resume(&cancel).await.unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(c.messages, before);
    assert_eq!(c.manual_compactions[0].id, operation);
    assert_eq!(
        c.manual_compactions[0].status,
        pi_rs_durable::runtime::ManualCompactionStatus::Declined
    );
}

#[tokio::test]
async fn before_compact_runs_for_automatic_threshold() {
    use pi_rs_durable::extension::{BeforeCompact, CompactionReason};
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let model = Arc::new(SmallModel {
        summaries: std::sync::atomic::AtomicUsize::new(0),
    });
    let options = Options {
        model: model.clone(),
        tools: vec![],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let seen = Arc::new(std::sync::Mutex::new(vec![]));
    let mut h = Harness::open_paused(dir.path().join("db"), options).unwrap();
    h.install_extension(compact_extension(vec![hook(
        BeforeCompact::Summary("automatic hook summary".into()),
        &seen,
        false,
        false,
    )]))
    .unwrap();
    let id = h
        .create_conversation(Config {
            model: "small".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    h.select_extensions(id, vec!["compact".into()]).unwrap();
    h.submit_and_run(id, None, "x".repeat(14_000), &cancel)
        .await
        .unwrap();
    h.submit_and_run(id, None, "latest".into(), &cancel)
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![CompactionReason::Threshold]);
    assert_eq!(model.summaries.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(h.conversation(id).unwrap().messages.iter().any(
        |m| matches!(m, Message::User { content } if content.contains("automatic hook summary"))
    ));
}

#[tokio::test]
async fn document_forks_follow_committed_cuts_and_survive_reopen() {
    use pi_rs_durable::{document::Address, runtime::AdmissionOptions};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let (parent, old_child, new_child, shared);
    {
        let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
        parent = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        let a = Address {
            scope: parent.to_string(),
            name: "note".into(),
            key: "x".into(),
        };
        shared = Address {
            scope: "shared".into(),
            name: "shared-only".into(),
            key: "x".into(),
        };
        h.submit_with_documents(
            parent,
            None,
            "first".into(),
            AdmissionOptions::default(),
            |docs| {
                docs.get(&a)?;
                docs.get(&shared)?;
                docs.put(&a, serde_json::json!({"n": 1}).as_object().unwrap().clone())?;
                docs.put(
                    &shared,
                    serde_json::json!({"n": 9}).as_object().unwrap().clone(),
                )
            },
        )
        .unwrap();
        let cut = h
            .history(parent, Cursor(0), NonZeroUsize::new(100).unwrap())
            .unwrap()
            .next;
        h.submit_with_documents(
            parent,
            None,
            "second".into(),
            AdmissionOptions::default(),
            |docs| {
                docs.get(&a)?;
                docs.put(&a, serde_json::json!({"n": 2}).as_object().unwrap().clone())
            },
        )
        .unwrap();
        assert_eq!(
            h.document_at_cut(parent, cut, &a).unwrap().unwrap().value["n"],
            1
        );
        h.resume(&cancel).await.unwrap();
        let current_child = h.fork(parent).unwrap();
        assert_eq!(
            h.document(&Address {
                scope: current_child.to_string(),
                ..a.clone()
            })
            .unwrap()
            .unwrap()
            .value["n"],
            2
        );
        old_child = h.fork_at(parent, cut).unwrap();
        let child_address = Address {
            scope: old_child.to_string(),
            ..a.clone()
        };
        assert_eq!(h.document(&child_address).unwrap().unwrap().value["n"], 1);
        let old_version = h.document(&child_address).unwrap().unwrap().version;
        assert_ne!(old_version, h.document(&a).unwrap().unwrap().version);
        h.submit_with_documents(
            old_child,
            None,
            "child change".into(),
            AdmissionOptions::default(),
            |docs| {
                docs.get(&child_address)?;
                docs.put(
                    &child_address,
                    serde_json::json!({"n": 7}).as_object().unwrap().clone(),
                )
            },
        )
        .unwrap();
        assert_eq!(h.document(&a).unwrap().unwrap().value["n"], 2);
        h.submit_with_documents(
            parent,
            None,
            "delete".into(),
            AdmissionOptions::default(),
            |docs| {
                docs.get(&a)?;
                docs.delete(&a)
            },
        )
        .unwrap();
        let deleted_cut = h
            .history(parent, Cursor(0), NonZeroUsize::new(100).unwrap())
            .unwrap()
            .next;
        new_child = h.fork_at(parent, deleted_cut).unwrap();
        assert!(
            h.document(&Address {
                scope: new_child.to_string(),
                ..a
            })
            .unwrap()
            .is_none()
        );
    }
    let h = Harness::open_paused(&path, opts()).unwrap();
    assert_eq!(
        h.document(&Address {
            scope: old_child.to_string(),
            name: "note".into(),
            key: "x".into()
        })
        .unwrap()
        .unwrap()
        .value["n"],
        7
    );
    assert!(
        h.document(&Address {
            scope: new_child.to_string(),
            name: "note".into(),
            key: "x".into()
        })
        .unwrap()
        .is_none()
    );
    assert_eq!(h.document(&shared).unwrap().unwrap().value["n"], 9);
    assert!(
        h.document(&Address {
            scope: old_child.to_string(),
            name: "shared-only".into(),
            key: "x".into()
        })
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn task_entries_follow_proven_fork_cuts_and_descendants() {
    use durable_store::Store;
    use pi_rs_durable::{
        application_entry,
        document::TaskDocuments,
        task::{
            Checkpoint, Task, TaskOwner,
            scheduler::{Handler, Registry, Scheduler as TaskScheduler, Step},
        },
    };
    use serde_json::json;
    use uuid::Uuid;
    struct Append;
    impl Handler for Append {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            task: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.append_entry("app.note", task.checkpoint().data.clone())
                .unwrap();
            Ok(Step::Succeed(json!(null)))
        }
    }
    fn append(path: &std::path::Path, conversation: Uuid, value: i32) -> Uuid {
        let id = Uuid::new_v4();
        let task = Task::new(
            id,
            TaskOwner::Conversation(conversation),
            Checkpoint {
                phase: "start".into(),
                data: json!(value),
            },
            3,
        )
        .unwrap()
        .registered("append".into(), true);
        let mut registry = Registry::default();
        registry.register("append", true, Append);
        let mut scheduler = TaskScheduler::open(path, registry).unwrap();
        scheduler.submit(&task).unwrap();
        scheduler.tick(0).unwrap();
        id
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let parent = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    drop(h);
    let first_author = append(&path, parent, 1);
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    h.reset(parent, None).unwrap();
    let cut = h
        .history(parent, Cursor(0), NonZeroUsize::new(100).unwrap())
        .unwrap()
        .next;
    drop(h);
    append(&path, parent, 2);
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let old = h.fork_at(parent, cut).unwrap();
    let current = h.fork(parent).unwrap();
    let db = Store::open(&path).unwrap();
    let original = application_entry::list(&db, parent).unwrap();
    assert_eq!(original.len(), 2);
    assert_eq!(original[0].author_task_id, first_author);
    assert_eq!(application_entry::list(&db, old).unwrap(), original[..1]);
    assert_eq!(application_entry::list(&db, current).unwrap(), original);
    drop(h);
    append(&path, old, 3);
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let descendant = h.fork(old).unwrap();
    let old_entries = application_entry::list(&db, old).unwrap();
    assert_eq!(old_entries.len(), 2);
    assert_eq!(old_entries[1].conversation_id, old);
    assert_eq!(
        application_entry::list(&db, descendant).unwrap(),
        old_entries
    );
    assert_eq!(application_entry::list(&db, parent).unwrap(), original);
    drop(db);
    drop(h);
    let db = Store::open(&path).unwrap();
    assert_eq!(
        application_entry::list(&db, descendant).unwrap(),
        old_entries
    );
}

#[tokio::test]
async fn document_fork_policies_are_persisted_and_follow_historical_cuts() {
    use durable_store::{Expected, Store};
    use pi_rs_durable::{
        document::{Address, Documents, ForkPolicy, Transaction},
        runtime::AdmissionOptions,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let parent;
    let cut;
    let address;
    {
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        parent = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        address = Address {
            scope: parent.to_string(),
            name: "policy".into(),
            key: "one".into(),
        };
        let db = Store::open(&path).unwrap();
        let mut tx = Transaction::new();
        tx.put(
            &address,
            Expected::Missing,
            &serde_json::json!({"n": 1}).as_object().unwrap().clone(),
        )
        .unwrap();
        tx.set_fork_policy(
            &address,
            Expected::Missing,
            &ForkPolicy::Initial {
                value: serde_json::json!({"n": 0}).as_object().unwrap().clone(),
            },
        )
        .unwrap();
        tx.commit(&db).unwrap();
        h.submit_with_documents(
            parent,
            None,
            "first".into(),
            AdmissionOptions::default(),
            |_| Ok(()),
        )
        .unwrap();
        cut = h
            .history(parent, Cursor(0), NonZeroUsize::new(100).unwrap())
            .unwrap()
            .next;
        let version = Documents::new(&db)
            .fork_policy(&address)
            .unwrap()
            .unwrap()
            .version;
        let mut tx = Transaction::new();
        tx.set_fork_policy(&address, Expected::Version(version), &ForkPolicy::Copy)
            .unwrap();
        tx.commit(&db).unwrap();
        h.submit_with_documents(
            parent,
            None,
            "second".into(),
            AdmissionOptions::default(),
            |docs| {
                docs.get(&address)?;
                docs.put(
                    &address,
                    serde_json::json!({"n": 2}).as_object().unwrap().clone(),
                )
            },
        )
        .unwrap();
    }
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    h.resume(&cancel).await.unwrap();
    let old = h.fork_at(parent, cut).unwrap();
    let current = h.fork(parent).unwrap();
    let old_address = Address {
        scope: old.to_string(),
        ..address.clone()
    };
    let current_address = Address {
        scope: current.to_string(),
        ..address.clone()
    };
    assert_eq!(h.document(&old_address).unwrap().unwrap().value["n"], 0);
    assert_eq!(h.document(&current_address).unwrap().unwrap().value["n"], 2);
    let db = Store::open(&path).unwrap();
    assert!(matches!(
        Documents::new(&db)
            .fork_policy(&old_address)
            .unwrap()
            .unwrap()
            .policy,
        ForkPolicy::Initial { .. }
    ));
    assert_eq!(
        Documents::new(&db)
            .fork_policy(&current_address)
            .unwrap()
            .unwrap()
            .policy,
        ForkPolicy::Copy
    );
    let parent_policy_version = Documents::new(&db)
        .fork_policy(&address)
        .unwrap()
        .unwrap()
        .version;
    let mut tx = Transaction::new();
    tx.set_fork_policy(
        &old_address,
        Expected::Version(
            Documents::new(&db)
                .fork_policy(&old_address)
                .unwrap()
                .unwrap()
                .version,
        ),
        &ForkPolicy::Copy,
    )
    .unwrap();
    tx.commit(&db).unwrap();
    assert_eq!(
        Documents::new(&db)
            .fork_policy(&address)
            .unwrap()
            .unwrap()
            .policy,
        ForkPolicy::Copy
    );
    assert_eq!(
        Documents::new(&db)
            .fork_policy(&address)
            .unwrap()
            .unwrap()
            .version,
        parent_policy_version
    );
    assert_eq!(h.document(&address).unwrap().unwrap().value["n"], 2);
    drop(h);
    let reopened = Harness::open_paused(&path, opts()).unwrap();
    assert_eq!(
        reopened.document(&old_address).unwrap().unwrap().value["n"],
        0
    );
    assert_eq!(
        Documents::new(&db)
            .fork_policy(&old_address)
            .unwrap()
            .unwrap()
            .policy,
        ForkPolicy::Copy
    );
}

#[tokio::test]
async fn fork_inherits_policies_for_absent_documents_at_the_cut() {
    use durable_store::{Expected, Store};
    use pi_rs_durable::{
        document::{Address, Documents, ForkPolicy, Transaction},
        runtime::AdmissionOptions,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let parent = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    let deleted = Address {
        scope: parent.to_string(),
        name: "deleted".into(),
        key: "x".into(),
    };
    let never = Address {
        name: "never".into(),
        ..deleted.clone()
    };
    let shared = Address {
        scope: "shared".into(),
        name: "shared".into(),
        key: "x".into(),
    };
    let db = Store::open(&path).unwrap();
    let initial = ForkPolicy::Initial {
        value: serde_json::json!({"n": 42}).as_object().unwrap().clone(),
    };
    let mut tx = Transaction::new();
    tx.put(
        &deleted,
        Expected::Missing,
        &serde_json::json!({"n": 1}).as_object().unwrap().clone(),
    )
    .unwrap();
    for address in [&deleted, &never, &shared] {
        tx.set_fork_policy(address, Expected::Missing, &initial)
            .unwrap();
    }
    tx.commit(&db).unwrap();
    let mut tx = Transaction::new();
    tx.delete(
        &deleted,
        Documents::new(&db).get(&deleted).unwrap().unwrap().version,
    )
    .unwrap();
    tx.commit(&db).unwrap();
    h.submit_with_documents(
        parent,
        None,
        "cut".into(),
        AdmissionOptions::default(),
        |_| Ok(()),
    )
    .unwrap();
    let cut = h
        .history(parent, Cursor(0), NonZeroUsize::new(100).unwrap())
        .unwrap()
        .next;
    let version = Documents::new(&db)
        .fork_policy(&deleted)
        .unwrap()
        .unwrap()
        .version;
    let mut tx = Transaction::new();
    tx.set_fork_policy(&deleted, Expected::Version(version), &ForkPolicy::Copy)
        .unwrap();
    tx.commit(&db).unwrap();
    h.resume(&cancel).await.unwrap();
    let child = h.fork_at(parent, cut).unwrap();
    let docs = Documents::new(&db);
    for address in [&deleted, &never] {
        let child_address = Address {
            scope: child.to_string(),
            ..address.clone()
        };
        assert!(docs.get(&child_address).unwrap().is_none());
        assert_eq!(
            docs.fork_policy(&child_address).unwrap().unwrap().policy,
            initial
        );
    }
    let shared_child = Address {
        scope: child.to_string(),
        ..shared.clone()
    };
    assert!(docs.fork_policy(&shared_child).unwrap().is_none());
    let child_address = Address {
        scope: child.to_string(),
        ..deleted.clone()
    };
    let mut tx = Transaction::new();
    tx.put(
        &child_address,
        Expected::Missing,
        &serde_json::json!({"n": 7}).as_object().unwrap().clone(),
    )
    .unwrap();
    tx.commit(&db).unwrap();
    let grandchild = h.fork(child).unwrap();
    let descendant = Address {
        scope: grandchild.to_string(),
        ..deleted
    };
    assert_eq!(docs.get(&descendant).unwrap().unwrap().value["n"], 42);
}

#[tokio::test]
async fn unified_history_pages_survive_reopen_and_show_fork_snapshots() {
    use pi_rs_durable::{
        document::TaskDocuments,
        task::{
            Checkpoint, Task, TaskOwner,
            scheduler::{Handler, Registry, Step},
        },
    };
    struct WriteEntry;
    impl Handler for WriteEntry {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.append_entry("app.note", serde_json::json!("task entry"))
                .unwrap();
            Ok(Step::Succeed(serde_json::Value::Null))
        }
    }
    use uuid::Uuid;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let parent = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    h.submit_and_run(parent, None, "hello unified".into(), &cancel)
        .await
        .unwrap();
    let task_id = Uuid::new_v4();
    let task = Task::new(
        task_id,
        TaskOwner::Conversation(parent),
        Checkpoint {
            phase: "start".into(),
            data: serde_json::json!({}),
        },
        3,
    )
    .unwrap()
    .registered("writer".into(), true);
    let mut registry = Registry::default();
    registry.register("writer", true, WriteEntry);
    let mut scheduler = h.attach_tasks(registry).unwrap();
    scheduler.submit(&task).unwrap();
    scheduler.tick(0).unwrap();
    drop(scheduler);
    let store = durable_store::Store::open(&path).unwrap();
    let app = pi_rs_durable::application_entry::list(&store, parent)
        .unwrap()
        .remove(0);
    assert_eq!(app.author_task_id, task_id);
    let captured = h.unified_history_start(parent, false).unwrap();
    let first = h.unified_history(&captured, NonZeroUsize::MIN).unwrap();
    assert_eq!(first.commits.len(), 1);
    assert!(first.has_more);
    let saved = serde_json::to_string(&first.next).unwrap();
    h.submit_write(
        parent,
        None,
        "app.later".into(),
        Some(serde_json::json!("after capture")),
    )
    .unwrap();
    let child = h.fork(parent).unwrap();
    drop(h);
    drop(store);
    let h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let mut cursor = serde_json::from_str(&saved).unwrap();
    let mut frames = first.commits;
    loop {
        let page = h.unified_history(&cursor, NonZeroUsize::MIN).unwrap();
        frames.extend(page.commits);
        cursor = page.next;
        if !page.has_more {
            break;
        }
    }
    assert!(
        frames
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
    );
    assert!(
        frames
            .iter()
            .flat_map(|f| &f.snapshots)
            .any(|s| s.value.to_string().contains("hello unified"))
    );
    assert_eq!(
        frames.last().unwrap().applications.as_ref().unwrap(),
        &vec![app.clone()]
    );
    assert!(
        !frames
            .iter()
            .flat_map(|f| &f.snapshots)
            .any(|s| s.value.to_string().contains("after capture"))
    );
    let child_page = h
        .unified_history(
            &h.unified_history_start(child, false).unwrap(),
            NonZeroUsize::new(10).unwrap(),
        )
        .unwrap();
    assert!(child_page.commits.iter().any(|f| {
        f.applications
            .as_ref()
            .is_some_and(|v| v == &vec![app.clone()])
    }));
    assert!(
        child_page
            .commits
            .iter()
            .flat_map(|f| &f.snapshots)
            .any(|s| s.value.to_string().contains("hello unified"))
    );
    assert!(
        h.unified_history(&cursor, NonZeroUsize::new(256).unwrap())
            .is_err()
    );
}

#[tokio::test]
async fn unified_history_requires_explicit_legacy_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    {
        let db = rusqlite::Connection::open(&path).unwrap();
        db.execute_batch("CREATE TABLE durable_store_meta (id INTEGER PRIMARY KEY, version INTEGER NOT NULL); INSERT INTO durable_store_meta VALUES (1, 1); CREATE TABLE durable_store_entries (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL, version INTEGER NOT NULL) WITHOUT ROWID; INSERT INTO durable_store_entries VALUES (X'78', X'79', 1);").unwrap();
    }
    let cancel = CancellationToken::new();
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    assert!(h.unified_history_start(id, false).is_err());
    let head = h.history_head(id).unwrap();
    assert_ne!(head, Cursor(0));
    let history = h
        .history_through(id, Cursor(0), head, NonZeroUsize::MIN)
        .unwrap();
    assert_eq!(history.next, head);
    assert!(!history.has_more);
    let cursor = h.unified_history_start(id, true).unwrap();
    assert!(
        !h.unified_history(&cursor, NonZeroUsize::MIN)
            .unwrap()
            .commits
            .is_empty()
    );
}
#[tokio::test]
async fn current_policy_uses_latest_value_and_policy_at_historical_fork_after_reopen() {
    use durable_store::{Expected, Store};
    use pi_rs_durable::{
        document::{Address, Documents, ForkPolicy, Transaction},
        runtime::AdmissionOptions,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let parent = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    let addr = Address {
        scope: parent.to_string(),
        name: "current".into(),
        key: "".into(),
    };
    let absent = Address {
        name: "absent".into(),
        ..addr.clone()
    };
    let deleted = Address {
        name: "deleted".into(),
        ..addr.clone()
    };
    let db = Store::open(&path).unwrap();
    let mut tx = Transaction::new();
    for a in [&addr, &absent, &deleted] {
        tx.set_fork_policy(a, Expected::Missing, &ForkPolicy::Current)
            .unwrap();
    }
    for a in [&addr, &deleted] {
        tx.put(
            a,
            Expected::Missing,
            &serde_json::json!({"n": 1}).as_object().unwrap().clone(),
        )
        .unwrap();
    }
    tx.commit(&db).unwrap();
    h.submit_with_documents(
        parent,
        None,
        "cut".into(),
        AdmissionOptions::default(),
        |_| Ok(()),
    )
    .unwrap();
    let cut = h
        .history(parent, Cursor(0), NonZeroUsize::new(100).unwrap())
        .unwrap()
        .next;
    let docs = Documents::new(&db);
    let mut tx = Transaction::new();
    tx.put(
        &addr,
        Expected::Version(docs.get(&addr).unwrap().unwrap().version),
        &serde_json::json!({"n": 2}).as_object().unwrap().clone(),
    )
    .unwrap();
    tx.set_fork_policy(
        &addr,
        Expected::Version(docs.fork_policy(&addr).unwrap().unwrap().version),
        &ForkPolicy::Copy,
    )
    .unwrap();
    tx.delete(&deleted, docs.get(&deleted).unwrap().unwrap().version)
        .unwrap();
    tx.commit(&db).unwrap();
    h.resume(&cancel).await.unwrap();
    let child = h.fork_at(parent, cut).unwrap();
    drop(h);
    drop(db);
    let db = Store::open(&path).unwrap();
    let docs = Documents::new(&db);
    let child_addr = |a: &Address| Address {
        scope: child.to_string(),
        ..a.clone()
    };
    assert_eq!(docs.get(&child_addr(&addr)).unwrap().unwrap().value["n"], 2);
    assert_eq!(
        docs.fork_policy(&child_addr(&addr))
            .unwrap()
            .unwrap()
            .policy,
        ForkPolicy::Copy
    );
    for a in [&absent, &deleted] {
        assert!(docs.get(&child_addr(a)).unwrap().is_none());
        assert_eq!(
            docs.fork_policy(&child_addr(a)).unwrap().unwrap().policy,
            ForkPolicy::Current
        );
    }
}

#[tokio::test]
async fn application_entry_fork_uses_atomic_entry_commit_and_reopens() {
    use durable_store::{Expected, Store};
    use pi_rs_durable::{
        application_entry,
        document::{Address, Documents, TaskDocuments, Transaction},
        task::{
            Checkpoint, Task, TaskOwner,
            scheduler::{Handler, Registry, Step},
        },
        task_store::TaskStore,
    };
    use serde_json::json;
    use uuid::Uuid;
    struct WritePair(Address);
    impl Handler for WritePair {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.get(&self.0).unwrap();
            docs.append_entry("app.first", json!(1)).unwrap();
            docs.put(&self.0, json!({"n": 7}).as_object().unwrap().clone())
                .unwrap();
            docs.append_entry("app.second", json!(2)).unwrap();
            Ok(Step::Succeed(json!(null)))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let parent = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    h.submit_and_run(parent, None, "before task".into(), &cancel)
        .await
        .unwrap();
    let expected_messages = h.conversation(parent).unwrap().messages;
    let address = Address {
        scope: parent.to_string(),
        name: "note".into(),
        key: "".into(),
    };
    let mut registry = Registry::default();
    registry.register("pair", true, WritePair(address.clone()));
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(parent),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("pair".into(), true);
    let mut scheduler = h.attach_tasks(registry).unwrap();
    scheduler.submit(&task).unwrap();
    scheduler.tick(0).unwrap();
    drop(scheduler);
    let db = Store::open(&path).unwrap();
    let entries = application_entry::list(&db, parent).unwrap();
    assert_eq!(entries.len(), 2);
    h.submit_and_run(parent, None, "after task".into(), &cancel)
        .await
        .unwrap();
    let mut tx = Transaction::new();
    tx.put(
        &address,
        Expected::Version(Documents::new(&db).get(&address).unwrap().unwrap().version),
        &json!({"n": 9}).as_object().unwrap().clone(),
    )
    .unwrap();
    tx.commit(&db).unwrap();
    assert!(h.fork_at_application_entry(parent, Uuid::new_v4()).is_err());
    let child = h.fork_at_application_entry(parent, entries[0].id).unwrap();
    let child_address = Address {
        scope: child.to_string(),
        ..address.clone()
    };
    assert_eq!(application_entry::list(&db, child).unwrap(), entries[..1]);
    assert_eq!(h.document(&child_address).unwrap().unwrap().value["n"], 7);
    assert_eq!(h.conversation(child).unwrap().messages, expected_messages);
    assert!(
        TaskStore::new(&db)
            .list()
            .unwrap()
            .iter()
            .all(|record| { record.task.owner() != &TaskOwner::Conversation(child) })
    );
    let inherited = h.fork_at_application_entry(child, entries[0].id).unwrap();
    assert_eq!(
        application_entry::list(&db, inherited).unwrap(),
        entries[..1]
    );
    assert_eq!(
        h.document(&Address {
            scope: inherited.to_string(),
            ..address.clone()
        })
        .unwrap()
        .unwrap()
        .value["n"],
        7
    );
    drop(h);
    drop(db);
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    let db = Store::open(&path).unwrap();
    assert_eq!(application_entry::list(&db, child).unwrap(), entries[..1]);
    assert_eq!(h.document(&child_address).unwrap().unwrap().value["n"], 7);
    h.submit_and_run(child, None, "continue child".into(), &cancel)
        .await
        .unwrap();
    assert_eq!(application_entry::list(&db, child).unwrap(), entries[..1]);
    assert_eq!(application_entry::list(&db, parent).unwrap(), entries);
    let second = h.fork_at_application_entry(parent, entries[1].id).unwrap();
    assert_eq!(application_entry::list(&db, second).unwrap(), entries);
    assert_eq!(
        h.document(&Address {
            scope: second.to_string(),
            ..address.clone()
        })
        .unwrap()
        .unwrap()
        .value["n"],
        7
    );

    // Race independent document commits against fork preparation. A fence
    // conflict may exhaust bounded retries; every successful fork must still
    // contain the exact historical document and truncated log.
    let gate = Arc::new(std::sync::Barrier::new(2));
    let writer_gate = gate.clone();
    let writer_path = path.clone();
    let writer = std::thread::spawn(move || {
        let db = Store::open(writer_path).unwrap();
        let other = Address {
            scope: "unrelated".into(),
            name: "race".into(),
            key: "".into(),
        };
        writer_gate.wait();
        for n in 0..24 {
            let current = Documents::new(&db).get(&other).unwrap();
            let expected = current.map_or(Expected::Missing, |doc| Expected::Version(doc.version));
            let mut tx = Transaction::new();
            tx.put(
                &other,
                expected,
                &json!({"n": n}).as_object().unwrap().clone(),
            )
            .unwrap();
            tx.commit(&db).unwrap();
            std::thread::yield_now();
        }
    });
    gate.wait();
    for _ in 0..8 {
        if let Ok(raced) = h.fork_at_application_entry(parent, entries[0].id) {
            assert_eq!(application_entry::list(&db, raced).unwrap(), entries[..1]);
            assert_eq!(
                h.document(&Address {
                    scope: raced.to_string(),
                    ..address.clone()
                })
                .unwrap()
                .unwrap()
                .value["n"],
                7
            );
        }
    }
    writer.join().unwrap();
    let after_race = h.fork_at_application_entry(parent, entries[0].id).unwrap();
    assert_eq!(
        application_entry::list(&db, after_race).unwrap(),
        entries[..1]
    );
    drop(h);
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    assert_eq!(
        h.document(&Address {
            scope: after_race.to_string(),
            ..address.clone()
        })
        .unwrap()
        .unwrap()
        .value["n"],
        7
    );

    // A simultaneous runtime mutation with no corresponding immutable fork
    // snapshot makes the entry's runtime state unprovable.
    let runtime_key = format!("runtime/conversation/{parent}").into_bytes();
    let app_key = format!("durable/application-entries/{parent}").into_bytes();
    let runtime = db.get(&runtime_key).unwrap().unwrap();
    let mut corrupted = entries.clone();
    let mut extra = entries[0].clone();
    extra.id = Uuid::new_v4();
    corrupted.push(extra.clone());
    db.commit(
        &[],
        &[
            durable_store::Mutation::Put {
                key: runtime_key,
                value: runtime.value,
            },
            durable_store::Mutation::Put {
                key: app_key,
                value: serde_json::to_vec(&corrupted).unwrap(),
            },
        ],
    )
    .unwrap();
    assert!(h.fork_at_application_entry(parent, extra.id).is_err());

    // Even a matching runtime snapshot cannot establish the order of a
    // runtime write and application entry in the same atomic commit.
    let runtime_key = format!("runtime/conversation/{parent}").into_bytes();
    let app_key = format!("durable/application-entries/{parent}").into_bytes();
    let runtime = db.get(&runtime_key).unwrap().unwrap();
    let prefix = format!("runtime/fork/{parent}/");
    let snapshots = db.list(prefix.as_bytes()).unwrap();
    let snapshot = &snapshots.last().unwrap().1;
    let snapshot_key = format!("{prefix}{:020}", u64::MAX).into_bytes();
    let mut simultaneous = corrupted;
    let mut last = extra;
    last.id = Uuid::new_v4();
    simultaneous.push(last.clone());
    db.commit(
        &[],
        &[
            durable_store::Mutation::Put {
                key: runtime_key,
                value: runtime.value,
            },
            durable_store::Mutation::Put {
                key: snapshot_key,
                value: snapshot.value.clone(),
            },
            durable_store::Mutation::Put {
                key: app_key,
                value: serde_json::to_vec(&simultaneous).unwrap(),
            },
        ],
    )
    .unwrap();
    assert!(h.fork_at_application_entry(parent, last.id).is_err());
}

#[tokio::test]
async fn inherited_application_forks_use_origin_and_immediate_parent_after_reopen() {
    use durable_store::{Expected, Store};
    use pi_rs_durable::{
        application_entry,
        document::{Address, Documents, ForkPolicy, TaskDocuments, Transaction},
        task::{
            Checkpoint, Task, TaskOwner,
            scheduler::{Handler, Registry, Step},
        },
    };
    use serde_json::json;
    use uuid::Uuid;
    struct Append;
    impl Handler for Append {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.append_entry("app.local", json!(1)).unwrap();
            Ok(Step::Succeed(json!(null)))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let parent = h
        .create_conversation(Config {
            model: "origin".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    let historical = Address {
        scope: parent.to_string(),
        name: "historical".into(),
        key: "".into(),
    };
    let current = Address {
        name: "current".into(),
        ..historical.clone()
    };
    let current2 = Address {
        name: "current2".into(),
        ..historical.clone()
    };
    let db = Store::open(&path).unwrap();
    let mut tx = Transaction::new();
    for address in [&historical, &current, &current2] {
        tx.put(
            address,
            Expected::Missing,
            &json!({"n": 1}).as_object().unwrap().clone(),
        )
        .unwrap();
    }
    tx.set_fork_policy(&current, Expected::Missing, &ForkPolicy::Current)
        .unwrap();
    tx.set_fork_policy(&current2, Expected::Missing, &ForkPolicy::Current)
        .unwrap();
    tx.commit(&db).unwrap();
    let mut registry = Registry::default();
    registry.register("app", true, Append);
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(parent),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("app".into(), true);
    let mut tasks = h.attach_tasks(registry).unwrap();
    tasks.submit(&task).unwrap();
    tasks.tick(0).unwrap();
    drop(tasks);
    let entry = application_entry::list(&db, parent).unwrap().remove(0);
    let child = h.fork_at_application_entry(parent, entry.id).unwrap();
    h.submit_and_run(child, None, "child-only context".into(), &cancel)
        .await
        .unwrap();
    let child_hist = Address {
        scope: child.to_string(),
        ..historical.clone()
    };
    let child_current = Address {
        scope: child.to_string(),
        ..current.clone()
    };
    let child_current2 = Address {
        scope: child.to_string(),
        ..current2.clone()
    };
    let mut tx = Transaction::new();
    for address in [&child_hist, &child_current, &child_current2] {
        let version = Documents::new(&db).get(address).unwrap().unwrap().version;
        tx.put(
            address,
            Expected::Version(version),
            &json!({"n": 2}).as_object().unwrap().clone(),
        )
        .unwrap();
    }
    tx.commit(&db).unwrap();
    let mut registry = Registry::default();
    registry.register("app", true, Append);
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(child),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("app".into(), true);
    let mut tasks = h.attach_tasks(registry).unwrap();
    tasks.submit(&task).unwrap();
    tasks.tick(0).unwrap();
    drop(tasks);
    let child_entries = application_entry::list(&db, child).unwrap();
    assert_eq!(child_entries.len(), 2);
    let own = h
        .fork_at_application_entry(child, child_entries[1].id)
        .unwrap();
    assert_eq!(application_entry::list(&db, own).unwrap(), child_entries);
    assert!(
        h.conversation(own)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::User { content } if content == "child-only context"))
    );
    assert_eq!(
        h.document(&Address {
            scope: own.to_string(),
            ..historical.clone()
        })
        .unwrap()
        .unwrap()
        .value["n"],
        2
    );
    let inherited = h.fork_at_application_entry(child, entry.id).unwrap();
    assert_eq!(
        application_entry::list(&db, inherited).unwrap(),
        vec![entry.clone()]
    );
    assert!(
        h.conversation(inherited)
            .unwrap()
            .messages
            .iter()
            .all(|m| !matches!(m, Message::User { content } if content == "child-only context"))
    );
    assert_eq!(
        h.document(&Address {
            scope: inherited.to_string(),
            ..historical.clone()
        })
        .unwrap()
        .unwrap()
        .value["n"],
        1
    );
    assert_eq!(
        h.document(&Address {
            scope: inherited.to_string(),
            ..current.clone()
        })
        .unwrap()
        .unwrap()
        .value["n"],
        2
    );
    // Both current-policy values must come from one fenced parent revision,
    // even as another connection atomically changes them during preparation.
    let gate = Arc::new(std::sync::Barrier::new(2));
    let writer_gate = gate.clone();
    let writer_path = path.clone();
    let a = child_current.clone();
    let b = child_current2.clone();
    let writer = std::thread::spawn(move || {
        let db = Store::open(writer_path).unwrap();
        writer_gate.wait();
        for n in 3..25 {
            let mut tx = Transaction::new();
            for address in [&a, &b] {
                let version = Documents::new(&db).get(address).unwrap().unwrap().version;
                tx.put(
                    address,
                    Expected::Version(version),
                    &json!({"n": n}).as_object().unwrap().clone(),
                )
                .unwrap();
            }
            tx.commit(&db).unwrap();
            std::thread::yield_now();
        }
    });
    gate.wait();
    for _ in 0..10 {
        let result = h.fork_at_application_entry(child, entry.id);
        if let Err(ref error) = result {
            assert!(
                matches!(error, pi_rs_durable::runtime::Error::Storage(durable_store::Error::Conflict { key }) if key == b"durable/document-fork-revision")
            );
        }
        if let Ok(raced) = result {
            let a = h
                .document(&Address {
                    scope: raced.to_string(),
                    ..current.clone()
                })
                .unwrap()
                .unwrap()
                .value["n"]
                .clone();
            let b = h
                .document(&Address {
                    scope: raced.to_string(),
                    ..current2.clone()
                })
                .unwrap()
                .unwrap()
                .value["n"]
                .clone();
            assert_eq!(a, b);
            assert_eq!(
                h.document(&Address {
                    scope: raced.to_string(),
                    ..historical.clone()
                })
                .unwrap()
                .unwrap()
                .value["n"],
                1
            );
            assert_eq!(
                application_entry::list(&db, raced).unwrap(),
                vec![entry.clone()]
            );
        }
    }
    writer.join().unwrap();
    // Appends to the immediate parent's log cannot enlarge the selected
    // inherited prefix, regardless of whether they win the race.
    let mut registry = Registry::default();
    registry.register("app", true, Append);
    let mut tasks = h.attach_tasks(registry).unwrap();
    let gate = Arc::new(std::sync::Barrier::new(2));
    let task_gate = gate.clone();
    let task_writer = std::thread::spawn(move || {
        task_gate.wait();
        for _ in 0..8 {
            let task = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(child),
                Checkpoint {
                    phase: "start".into(),
                    data: json!(null),
                },
                3,
            )
            .unwrap()
            .registered("app".into(), true);
            tasks.submit(&task).unwrap();
            tasks.tick(0).unwrap();
        }
    });
    gate.wait();
    for _ in 0..8 {
        let result = h.fork_at_application_entry(child, entry.id);
        if let Err(ref error) = result {
            assert!(
                matches!(error, pi_rs_durable::runtime::Error::Storage(durable_store::Error::Conflict { key }) if key == b"durable/document-fork-revision")
            );
        }
        if let Ok(raced) = result {
            assert_eq!(
                application_entry::list(&db, raced).unwrap(),
                vec![entry.clone()]
            );
        }
    }
    task_writer.join().unwrap();
    let sibling = h
        .create_conversation(Config {
            model: "unrelated".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    assert!(h.fork_at_application_entry(sibling, entry.id).is_err());
    // A foreign prefix inserted after creation has no fork provenance.
    let forged_key = format!("durable/application-entries/{sibling}").into_bytes();
    db.commit(
        &[],
        &[durable_store::Mutation::Put {
            key: forged_key,
            value: serde_json::to_vec(&vec![entry.clone()]).unwrap(),
        }],
    )
    .unwrap();
    assert!(h.fork_at_application_entry(sibling, entry.id).is_err());
    assert!(
        h.fork_at_application_entry(inherited, child_entries[1].id)
            .is_err()
    );
    drop(h);
    drop(db);
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    let db = Store::open(&path).unwrap();
    let grandchild = h.fork_at_application_entry(inherited, entry.id).unwrap();
    assert_eq!(
        application_entry::list(&db, grandchild).unwrap(),
        vec![entry]
    );
    assert_eq!(
        h.document(&Address {
            scope: grandchild.to_string(),
            ..historical
        })
        .unwrap()
        .unwrap()
        .value["n"],
        1
    );
    assert_eq!(
        h.document(&Address {
            scope: grandchild.to_string(),
            ..current
        })
        .unwrap()
        .unwrap()
        .value["n"],
        2
    );
}

#[tokio::test]
async fn local_entry_cannot_hide_an_unproven_inherited_prefix() {
    use durable_store::{Mutation, Store};
    use pi_rs_durable::{
        application_entry::{self, ApplicationEntry},
        document::TaskDocuments,
        task::{
            Checkpoint, Task, TaskOwner,
            scheduler::{Handler, Registry, Step},
        },
    };
    use serde_json::json;
    use uuid::Uuid;
    struct Append;
    impl Handler for Append {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.append_entry("app.local", json!(1)).unwrap();
            Ok(Step::Succeed(json!(null)))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let parent = h
        .create_conversation(Config {
            model: "origin".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    let db = Store::open(&path).unwrap();
    let bogus = ApplicationEntry {
        id: Uuid::new_v4(),
        conversation_id: Uuid::new_v4(),
        author_task_id: Uuid::new_v4(),
        kind: "app.forged".into(),
        data: json!(null),
    };
    db.commit(
        &[],
        &[Mutation::Put {
            key: format!("durable/application-entries/{parent}").into_bytes(),
            value: serde_json::to_vec(&vec![bogus]).unwrap(),
        }],
    )
    .unwrap();
    let child = h.fork(parent).unwrap();
    let mut registry = Registry::default();
    registry.register("app", true, Append);
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(child),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("app".into(), true);
    let mut tasks = h.attach_tasks(registry).unwrap();
    tasks.submit(&task).unwrap();
    tasks.tick(0).unwrap();
    drop(tasks);
    let entries = application_entry::list(&db, child).unwrap();
    assert_eq!(entries.len(), 2);
    assert!(h.fork_at_application_entry(child, entries[1].id).is_err());
    drop(h);
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    assert!(h.fork_at_application_entry(child, entries[1].id).is_err());
}

#[tokio::test]
async fn inherited_entries_require_atomic_parent_lineage() {
    use durable_store::{Mutation, Store};
    use pi_rs_durable::{
        application_entry,
        document::TaskDocuments,
        task::{
            Checkpoint, Task, TaskOwner,
            scheduler::{Handler, Registry, Step},
        },
    };
    use serde_json::json;
    use uuid::Uuid;
    struct Append;
    impl Handler for Append {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.append_entry("app.local", json!(1)).unwrap();
            Ok(Step::Succeed(json!(null)))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let config = Config {
        model: "origin".into(),
        system_prompt: "".into(),
        max_output_tokens: 128,
    };
    let origin = h.create_conversation(config.clone()).unwrap();
    let unrelated = h.create_conversation(config).unwrap();
    let db = Store::open(&path).unwrap();
    let mut registry = Registry::default();
    registry.register("app", true, Append);
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(origin),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("app".into(), true);
    let mut tasks = h.attach_tasks(registry).unwrap();
    tasks.submit(&task).unwrap();
    tasks.tick(0).unwrap();
    drop(tasks);
    let entries = application_entry::list(&db, origin).unwrap();
    let child = h.fork_at_application_entry(origin, entries[0].id).unwrap();
    let grandchild = h.fork_at_application_entry(child, entries[0].id).unwrap();
    assert_eq!(application_entry::list(&db, grandchild).unwrap(), entries);
    let replay = h
        .unified_history(
            &h.unified_history_start(grandchild, false).unwrap(),
            std::num::NonZeroUsize::new(10).unwrap(),
        )
        .unwrap();
    assert!(replay.commits.iter().any(|frame| {
        frame.applications.as_ref() == Some(&entries) && !frame.snapshots.is_empty()
    }));
    // Simulate an unrelated conversation created atomically with a copied,
    // origin-matching prefix and a lineage record claiming the unrelated parent.
    let forged = Uuid::new_v4();
    let runtime_key = format!("runtime/conversation/{child}");
    let mut runtime: serde_json::Value =
        serde_json::from_slice(&db.get(runtime_key.as_bytes()).unwrap().unwrap().value).unwrap();
    runtime["id"] = json!(forged);
    runtime["transcript"] = json!(null);
    let first = db
        .first_commit_for_key(runtime_key.as_bytes())
        .unwrap()
        .unwrap();
    let (_, snapshot_key) = db
        .runtime_snapshot_at(
            runtime_key.as_bytes(),
            format!("runtime/fork/{child}/").as_bytes(),
            first,
        )
        .unwrap()
        .unwrap();
    let snapshot = db.get(&snapshot_key).unwrap().unwrap().value;
    let suffix = std::str::from_utf8(&snapshot_key)
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap();
    let revision = db.document_snapshot().unwrap();
    // Even if two creation records claim each other as parents, at least one
    // link points to a prefix that did not exist at its recorded revision.
    let cycle_a = Uuid::new_v4();
    let cycle_b = Uuid::new_v4();
    for (id, parent) in [(cycle_a, cycle_b), (cycle_b, cycle_a)] {
        runtime["id"] = json!(id);
        let parent_revision = db.document_snapshot().unwrap();
        db.commit(
            &[],
            &[
                Mutation::Put {
                    key: format!("runtime/conversation/{id}").into_bytes(),
                    value: serde_json::to_vec(&runtime).unwrap(),
                },
                Mutation::Put {
                    key: format!("runtime/fork/{id}/{suffix}").into_bytes(),
                    value: snapshot.clone(),
                },
                Mutation::Put {
                    key: format!("durable/application-entries/{id}").into_bytes(),
                    value: serde_json::to_vec(&entries).unwrap(),
                },
                Mutation::Put {
                    key: format!("durable/application-lineage/{id}").into_bytes(),
                    value: serde_json::to_vec(
                        &json!({"parent": parent, "parent_revision": parent_revision}),
                    )
                    .unwrap(),
                },
            ],
        )
        .unwrap();
    }
    for id in [cycle_a, cycle_b] {
        assert!(h.conversation(id).is_ok());
        assert!(matches!(
            h.fork_at_application_entry(id, entries[0].id),
            Err(pi_rs_durable::runtime::Error::Fork(
                pi_rs_durable::fork::Error::InvalidSnapshot
            ))
        ));
    }
    runtime["id"] = json!(forged);
    db.commit(
        &[],
        &[
            Mutation::Put {
                key: format!("runtime/conversation/{forged}").into_bytes(),
                value: serde_json::to_vec(&runtime).unwrap(),
            },
            Mutation::Put {
                key: format!("runtime/fork/{forged}/{suffix}").into_bytes(),
                value: snapshot,
            },
            Mutation::Put {
                key: format!("durable/application-entries/{forged}").into_bytes(),
                value: serde_json::to_vec(&entries).unwrap(),
            },
            Mutation::Put {
                key: format!("durable/application-lineage/{forged}").into_bytes(),
                value: serde_json::to_vec(
                    &json!({"parent": unrelated, "parent_revision": revision}),
                )
                .unwrap(),
            },
        ],
    )
    .unwrap();
    assert!(h.conversation(forged).is_ok());
    assert!(matches!(
        h.fork_at_application_entry(forged, entries[0].id),
        Err(pi_rs_durable::runtime::Error::Fork(
            pi_rs_durable::fork::Error::InvalidSnapshot
        ))
    ));
    // A missing record or one written later cannot authenticate creation,
    // even if the late record names the real origin and its prefix matches.
    let forged_lineage = format!("durable/application-lineage/{forged}").into_bytes();
    db.commit(
        &[],
        &[Mutation::Delete {
            key: forged_lineage.clone(),
        }],
    )
    .unwrap();
    assert!(h.fork_at_application_entry(forged, entries[0].id).is_err());
    db.commit(
        &[],
        &[Mutation::Put {
            key: forged_lineage,
            value: serde_json::to_vec(&json!({"parent": origin, "parent_revision": revision}))
                .unwrap(),
        }],
    )
    .unwrap();
    assert!(h.fork_at_application_entry(forged, entries[0].id).is_err());
    drop(h);
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    let after_reopen = h
        .fork_at_application_entry(grandchild, entries[0].id)
        .unwrap();
    assert_eq!(application_entry::list(&db, after_reopen).unwrap(), entries);
    // Legacy forks have an atomically installed prefix but no lineage record.
    db.commit(
        &[],
        &[Mutation::Delete {
            key: format!("durable/application-lineage/{child}").into_bytes(),
        }],
    )
    .unwrap();
    assert!(h.fork_at_application_entry(child, entries[0].id).is_err());
    assert!(
        h.fork_at_application_entry(grandchild, entries[0].id)
            .is_err()
    );
    drop(h);
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    assert!(h.fork_at_application_entry(forged, entries[0].id).is_err());
    assert!(h.fork_at_application_entry(child, entries[0].id).is_err());
    // Original local entries are still usable in the absence of lineage.
    assert!(h.fork_at_application_entry(origin, entries[0].id).is_ok());
}

#[tokio::test]
async fn task_ordered_runtime_checkpoint_forks_reopen_and_retries_races() {
    use pi_rs_durable::{
        application_entry,
        document::{Address, TaskDocuments},
        task::{
            Checkpoint, Task, TaskOwner,
            scheduler::{Handler, Registry, Step},
        },
    };
    use serde_json::json;
    use uuid::Uuid;
    struct Writer {
        address: Address,
        ready: Option<std::sync::mpsc::Sender<()>>,
        resume: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    }
    impl Handler for Writer {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.get(&self.address).unwrap();
            docs.append_entry("app.before", json!(1)).unwrap();
            docs.checkpoint_runtime().unwrap();
            docs.append_entry("app.after", json!(2)).unwrap();
            docs.put(&self.address, json!({"n": 7}).as_object().unwrap().clone())
                .unwrap();
            if let Some(rx) = self.resume.lock().unwrap().take() {
                self.ready.as_ref().unwrap().send(()).unwrap();
                rx.recv().unwrap();
            }
            Ok(Step::Succeed(json!(null)))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    h.submit_and_run(id, None, "original".into(), &cancel)
        .await
        .unwrap();
    let address = Address {
        scope: id.to_string(),
        name: "cut".into(),
        key: "".into(),
    };
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let mut registry = Registry::default();
    registry.register(
        "ordered",
        true,
        Writer {
            address: address.clone(),
            ready: Some(ready_tx),
            resume: std::sync::Mutex::new(Some(resume_rx)),
        },
    );
    let mut scheduler = h.attach_tasks(registry).unwrap();
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("ordered".into(), true);
    scheduler.submit(&task).unwrap();
    let writer = std::thread::spawn(move || {
        scheduler.tick(0).unwrap();
        scheduler
    });
    ready_rx.recv().unwrap();
    h.submit_write(id, None, "app.race".into(), None).unwrap();
    resume_tx.send(()).unwrap();
    let mut scheduler = writer.join().unwrap();
    let db = durable_store::Store::open(&path).unwrap();
    assert!(application_entry::list(&db, id).unwrap().is_empty());
    assert!(
        db.list(format!("durable/application-ordered-cut/{id}/").as_bytes())
            .unwrap()
            .is_empty()
    );
    assert!(h.document(&address).unwrap().is_none());
    scheduler.tick(0).unwrap();
    drop(scheduler);
    let entries = application_entry::list(&db, id).unwrap();
    assert_eq!(entries.len(), 2);
    let first = h.fork_at_application_entry(id, entries[0].id).unwrap();
    let second = h.fork_at_application_entry(id, entries[1].id).unwrap();
    assert_eq!(application_entry::list(&db, first).unwrap(), entries[..1]);
    assert_eq!(application_entry::list(&db, second).unwrap(), entries);
    for child in [first, second] {
        assert_eq!(
            h.document(&Address {
                scope: child.to_string(),
                ..address.clone()
            })
            .unwrap()
            .unwrap()
            .value["n"],
            7
        );
        assert_eq!(
            h.conversation(child).unwrap().messages,
            h.conversation(id).unwrap().messages
        );
    }
    drop(h);
    drop(db);
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    let db = durable_store::Store::open(&path).unwrap();
    let grandchild = h.fork_at_application_entry(first, entries[0].id).unwrap();
    assert_eq!(
        application_entry::list(&db, grandchild).unwrap(),
        entries[..1]
    );
    assert_eq!(
        h.document(&Address {
            scope: grandchild.to_string(),
            ..address
        })
        .unwrap()
        .unwrap()
        .value["n"],
        7
    );
    // An untracked or missing proof cannot authorize a same-commit cut.
    let prefix = format!("durable/application-ordered-cut/{id}/");
    let proofs = db.list(prefix.as_bytes()).unwrap();
    assert_eq!(proofs.len(), 1);
    db.commit(
        &[],
        &[durable_store::Mutation::Delete {
            key: proofs[0].0.clone(),
        }],
    )
    .unwrap();
    assert!(h.fork_at_application_entry(id, entries[0].id).is_err());
    drop(h);
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    assert!(h.fork_at_application_entry(id, entries[1].id).is_err());
}

#[tokio::test]
async fn ordered_cut_rejects_forged_batch_start_and_stale_runtime_version() {
    use durable_store::{Mutation, Store};
    use pi_rs_durable::{application_entry::ApplicationEntry, observer::Observer};
    use serde_json::json;
    use uuid::Uuid;

    for stale_runtime in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        let id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        let db = Store::open(&path).unwrap();
        let runtime_key = format!("runtime/conversation/{id}").into_bytes();
        let log_key = format!("durable/application-entries/{id}").into_bytes();
        let snapshot_prefix = format!("runtime/fork/{id}/");
        let root = db.get(&runtime_key).unwrap().unwrap();
        let seq = db
            .commit_for_version(&runtime_key, root.version)
            .unwrap()
            .unwrap();
        let (_, snapshot_key) = db
            .runtime_snapshot_at(&runtime_key, snapshot_prefix.as_bytes(), seq)
            .unwrap()
            .unwrap();
        let snapshot = db.get(&snapshot_key).unwrap().unwrap().value;
        let entry = |kind: &str| ApplicationEntry {
            id: Uuid::new_v4(),
            conversation_id: id,
            author_task_id: Uuid::new_v4(),
            kind: kind.into(),
            data: json!(null),
        };
        let first = entry("app.first");
        db.commit(
            &[],
            &[Mutation::Put {
                key: log_key.clone(),
                value: serde_json::to_vec(&vec![first.clone()]).unwrap(),
            }],
        )
        .unwrap();
        let observer = Observer::new(&db, id);
        if stale_runtime {
            let append = observer.prepare_append(&json!({})).unwrap();
            let mut mutations = append.mutations;
            mutations.extend([
                Mutation::Put {
                    key: runtime_key.clone(),
                    value: root.value.clone(),
                },
                Mutation::Put {
                    key: format!("{snapshot_prefix}{:020}", append.cursor.0).into_bytes(),
                    value: snapshot.clone(),
                },
            ]);
            db.commit(&append.checks, &mutations).unwrap();
        }
        let second = entry("app.second");
        let entries = vec![first, second.clone()];
        let first_index = if stale_runtime { 1 } else { 0 };
        let append = observer.prepare_append(&json!({})).unwrap();
        let proof = json!({"version": 1, "previous_runtime_version": root.version,
            "first_index": first_index, "checkpoint_index": 2,
            "entry_ids": entries[first_index..].iter().map(|e| e.id).collect::<Vec<_>>()});
        let mut mutations = append.mutations;
        mutations.extend([
            Mutation::Put {
                key: runtime_key.clone(),
                value: root.value.clone(),
            },
            Mutation::Put {
                key: format!("{snapshot_prefix}{:020}", append.cursor.0).into_bytes(),
                value: snapshot,
            },
            Mutation::Put {
                key: log_key,
                value: serde_json::to_vec(&entries).unwrap(),
            },
            Mutation::Put {
                key: format!(
                    "durable/application-ordered-cut/{id}/{:020}",
                    append.cursor.0
                )
                .into_bytes(),
                value: serde_json::to_vec(&proof).unwrap(),
            },
        ]);
        db.commit(&append.checks, &mutations).unwrap();
        assert!(h.fork_at_application_entry(id, second.id).is_err());
        drop(h);
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        assert!(h.fork_at_application_entry(id, second.id).is_err());
    }
}

#[tokio::test]
async fn task_ordered_configuration_change_forks_reopen_and_retries_races() {
    use pi_rs_durable::{
        application_entry,
        document::{Address, TaskDocuments},
        task::{
            Checkpoint, Task, TaskOwner,
            scheduler::{Handler, Registry, Step},
        },
    };
    use serde_json::json;
    use uuid::Uuid;
    struct Writer {
        address: Address,
        ready: Option<std::sync::mpsc::Sender<()>>,
        resume: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    }
    impl Handler for Writer {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.get(&self.address).unwrap();
            docs.append_entry("app.before", json!(1)).unwrap();
            docs.configure_runtime_at_checkpoint(Config {
                model: "changed".into(),
                system_prompt: "new".into(),
                max_output_tokens: 64,
            })
            .unwrap();
            docs.append_entry("app.after", json!(2)).unwrap();
            docs.put(&self.address, json!({"n": 7}).as_object().unwrap().clone())
                .unwrap();
            if let Some(rx) = self.resume.lock().unwrap().take() {
                self.ready.as_ref().unwrap().send(()).unwrap();
                rx.recv().unwrap();
            }
            Ok(Step::Succeed(json!(null)))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    h.submit_and_run(id, None, "original".into(), &cancel)
        .await
        .unwrap();
    let address = Address {
        scope: id.to_string(),
        name: "cut".into(),
        key: "".into(),
    };
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let mut registry = Registry::default();
    registry.register(
        "ordered",
        true,
        Writer {
            address: address.clone(),
            ready: Some(ready_tx),
            resume: std::sync::Mutex::new(Some(resume_rx)),
        },
    );
    let mut scheduler = h.attach_tasks(registry).unwrap();
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("ordered".into(), true);
    scheduler.submit(&task).unwrap();
    let writer = std::thread::spawn(move || {
        scheduler.tick(0).unwrap();
        scheduler
    });
    ready_rx.recv().unwrap();
    h.submit_write(id, None, "app.race".into(), None).unwrap();
    resume_tx.send(()).unwrap();
    let mut scheduler = writer.join().unwrap();
    let db = durable_store::Store::open(&path).unwrap();
    assert!(application_entry::list(&db, id).unwrap().is_empty());
    assert!(
        db.list(format!("durable/application-ordered-cut/{id}/").as_bytes())
            .unwrap()
            .is_empty()
    );
    assert!(h.document(&address).unwrap().is_none());
    assert_eq!(h.conversation(id).unwrap().config.model, "test");
    scheduler.tick(0).unwrap();
    drop(scheduler);
    assert_eq!(h.conversation(id).unwrap().config.model, "changed");
    let entries = application_entry::list(&db, id).unwrap();
    assert_eq!(entries.len(), 2);
    let first = h.fork_at_application_entry(id, entries[0].id).unwrap();
    let second = h.fork_at_application_entry(id, entries[1].id).unwrap();
    assert_eq!(application_entry::list(&db, first).unwrap(), entries[..1]);
    assert_eq!(application_entry::list(&db, second).unwrap(), entries);
    for child in [first, second] {
        assert_eq!(h.conversation(child).unwrap().config.model, "changed");
        assert_eq!(
            h.document(&Address {
                scope: child.to_string(),
                ..address.clone()
            })
            .unwrap()
            .unwrap()
            .value["n"],
            7
        );
        assert_eq!(
            h.conversation(child).unwrap().messages,
            h.conversation(id).unwrap().messages
        );
    }
    drop(h);
    drop(db);
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    let db = durable_store::Store::open(&path).unwrap();
    let grandchild = h.fork_at_application_entry(first, entries[0].id).unwrap();
    assert_eq!(h.conversation(grandchild).unwrap().config.model, "changed");
    assert_eq!(
        application_entry::list(&db, grandchild).unwrap(),
        entries[..1]
    );
    assert_eq!(
        h.document(&Address {
            scope: grandchild.to_string(),
            ..address
        })
        .unwrap()
        .unwrap()
        .value["n"],
        7
    );
    // An untracked or missing proof cannot authorize a same-commit cut.
    let prefix = format!("durable/application-ordered-cut/{id}/");
    let proofs = db.list(prefix.as_bytes()).unwrap();
    assert_eq!(proofs.len(), 1);
    db.commit(
        &[],
        &[durable_store::Mutation::Delete {
            key: proofs[0].0.clone(),
        }],
    )
    .unwrap();
    assert!(h.fork_at_application_entry(id, entries[0].id).is_err());
    drop(h);
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    assert!(h.fork_at_application_entry(id, entries[1].id).is_err());
}

#[test]
fn ordered_config_process_child() {
    use pi_rs_durable::{
        document::TaskDocuments,
        task::{
            Checkpoint, Task, TaskOwner,
            scheduler::{Handler, Registry, Step},
        },
    };
    use serde_json::json;
    use uuid::Uuid;
    let Ok(path) = std::env::var("PI_ORDERED_CONFIG_DB") else {
        return;
    };
    struct Writer;
    impl Handler for Writer {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.append_entry("app.before", json!(1)).unwrap();
            docs.configure_runtime_at_checkpoint(Config {
                model: "after".into(),
                system_prompt: "new".into(),
                max_output_tokens: 64,
            })
            .unwrap();
            docs.append_entry("app.after", json!(2)).unwrap();
            if std::env::var_os("PI_ORDERED_CONFIG_BEFORE").is_some() {
                std::process::exit(0);
            }
            Ok(Step::Succeed(json!(null)))
        }
    }
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "before".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("ordered".into(), true);
    let mut registry = Registry::default();
    registry.register("ordered", true, Writer);
    let mut scheduler = h.attach_tasks(registry).unwrap();
    scheduler.submit(&task).unwrap();
    std::fs::write(format!("{path}.id"), id.to_string()).unwrap();
    scheduler.tick(0).unwrap();
    std::process::exit(0);
}

#[test]
fn ordered_config_commit_survives_process_loss() {
    use pi_rs_durable::application_entry;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "ordered_config_process_child"])
        .env("PI_ORDERED_CONFIG_DB", &path)
        .status()
        .unwrap();
    assert!(status.success());
    let id =
        uuid::Uuid::parse_str(&std::fs::read_to_string(format!("{}.id", path.display())).unwrap())
            .unwrap();
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    let db = durable_store::Store::open(&path).unwrap();
    let entries = application_entry::list(&db, id).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(h.conversation(id).unwrap().config.model, "after");
    for (i, entry) in entries.iter().enumerate() {
        let child = h.fork_at_application_entry(id, entry.id).unwrap();
        assert_eq!(h.conversation(child).unwrap().config.model, "after");
        assert_eq!(application_entry::list(&db, child).unwrap(), entries[..=i]);
    }
}

#[test]
fn ordered_config_staging_is_lost_on_process_loss() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "ordered_config_process_child"])
        .env("PI_ORDERED_CONFIG_DB", &path)
        .env("PI_ORDERED_CONFIG_BEFORE", "1")
        .status()
        .unwrap();
    assert!(status.success());
    let id =
        uuid::Uuid::parse_str(&std::fs::read_to_string(format!("{}.id", path.display())).unwrap())
            .unwrap();
    let h = Harness::open_paused(&path, opts()).unwrap();
    let db = durable_store::Store::open(&path).unwrap();
    assert_eq!(h.conversation(id).unwrap().config.model, "before");
    assert!(
        pi_rs_durable::application_entry::list(&db, id)
            .unwrap()
            .is_empty()
    );
    assert!(
        db.list(format!("durable/application-ordered-cut/{id}/").as_bytes())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn ordered_config_rejects_active_conversations() {
    use pi_rs_durable::{
        application_entry,
        document::TaskDocuments,
        task::{
            Checkpoint, Task, TaskOwner,
            scheduler::{Handler, Registry, Step},
        },
    };
    use serde_json::json;
    use uuid::Uuid;
    struct Writer;
    impl Handler for Writer {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.append_entry("app.before", json!(1)).unwrap();
            docs.configure_runtime_at_checkpoint(Config {
                model: "after".into(),
                system_prompt: "".into(),
                max_output_tokens: 64,
            })
            .unwrap();
            Ok(Step::Succeed(json!(null)))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "before".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    h.submit(id, None, "active".into()).unwrap();
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("ordered".into(), true);
    let mut registry = Registry::default();
    registry.register("ordered", true, Writer);
    let mut scheduler = h.attach_tasks(registry).unwrap();
    scheduler.submit(&task).unwrap();
    assert!(scheduler.tick(0).is_err());
    let db = durable_store::Store::open(&path).unwrap();
    assert!(application_entry::list(&db, id).unwrap().is_empty());
    assert_eq!(h.conversation(id).unwrap().config.model, "before");
}

#[test]
fn ordered_config_active_race_discards_staged_effects() {
    use pi_rs_durable::{
        application_entry,
        document::TaskDocuments,
        task::{Checkpoint, Task, TaskOwner, scheduler::{Handler, Registry, Step}},
    };
    use serde_json::json;
    use std::sync::{Mutex, mpsc};
    use uuid::Uuid;

    struct Writer {
        ready: mpsc::Sender<()>,
        resume: Mutex<mpsc::Receiver<()>>,
    }
    impl Handler for Writer {
        fn run(&self, _: &Task) -> Result<Step, String> { unreachable!() }
        fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
            docs.append_entry("app.before", json!(1)).unwrap();
            docs.configure_runtime_at_checkpoint(Config {
                model: "after".into(), system_prompt: "".into(), max_output_tokens: 64,
            }).unwrap();
            self.ready.send(()).unwrap();
            self.resume.lock().unwrap().recv().unwrap();
            Ok(Step::Succeed(json!(null)))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    let id = h.create_conversation(Config {
        model: "before".into(), system_prompt: "".into(), max_output_tokens: 128,
    }).unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let mut registry = Registry::default();
    registry.register("writer", true, Writer { ready: ready_tx, resume: Mutex::new(resume_rx) });
    let mut scheduler = h.attach_tasks(registry).unwrap();
    let task = Task::new(Uuid::new_v4(), TaskOwner::Conversation(id), Checkpoint {
        phase: "start".into(), data: json!(null),
    }, 3).unwrap().registered("writer".into(), true);
    let task_id = task.id();
    scheduler.submit(&task).unwrap();
    let thread = std::thread::spawn(move || { scheduler.tick(0).unwrap(); scheduler });
    ready_rx.recv().unwrap();
    h.submit(id, None, "new input".into()).unwrap();
    resume_tx.send(()).unwrap();
    let scheduler = thread.join().unwrap();
    assert_eq!(scheduler.get(task_id).unwrap().unwrap().state(), &pi_rs_durable::task::TaskState::Ready);
    assert_eq!(h.conversation(id).unwrap().config.model, "before");
    let db = durable_store::Store::open(&path).unwrap();
    assert!(application_entry::list(&db, id).unwrap().is_empty());
    assert!(db.list(format!("durable/application-ordered-cut/{id}/").as_bytes()).unwrap().is_empty());
}

#[test]
fn successive_ordered_config_changes_keep_each_commit_in_forks() {
    use pi_rs_durable::{application_entry, document::TaskDocuments, task::{
        Checkpoint, Task, TaskOwner, scheduler::{Handler, Registry, Step},
    }};
    use serde_json::json;
    use uuid::Uuid;
    struct Writer;
    impl Handler for Writer {
        fn run(&self, _: &Task) -> Result<Step, String> { unreachable!() }
        fn run_with_documents(&self, task: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
            let model = task.checkpoint().data.as_str().unwrap();
            docs.append_entry("app.before", json!(model)).unwrap();
            docs.configure_runtime_at_checkpoint(Config {
                model: model.into(), system_prompt: model.into(), max_output_tokens: 64,
            }).unwrap();
            docs.append_entry("app.after", json!(model)).unwrap();
            Ok(Step::Succeed(json!(null)))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    let id = h.create_conversation(Config {
        model: "initial".into(), system_prompt: "".into(), max_output_tokens: 128,
    }).unwrap();
    let mut registry = Registry::default();
    registry.register("writer", true, Writer);
    let mut scheduler = h.attach_tasks(registry).unwrap();
    for model in ["first", "second"] {
        let task = Task::new(Uuid::new_v4(), TaskOwner::Conversation(id), Checkpoint {
            phase: "start".into(), data: json!(model),
        }, 3).unwrap().registered("writer".into(), true);
        scheduler.submit(&task).unwrap();
        scheduler.tick(0).unwrap();
    }
    drop(scheduler);
    drop(h);
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    let db = durable_store::Store::open(&path).unwrap();
    let entries = application_entry::list(&db, id).unwrap();
    assert_eq!(entries.len(), 4);
    assert_eq!(h.conversation(id).unwrap().config.model, "second");
    for (i, entry) in entries.iter().enumerate() {
        let fork = h.fork_at_application_entry(id, entry.id).unwrap();
        assert_eq!(h.conversation(fork).unwrap().config.model, if i < 2 { "first" } else { "second" });
        assert_eq!(application_entry::list(&db, fork).unwrap(), entries[..=i]);
    }
}

#[test]
fn ordered_config_task_abort_discards_staged_effects() {
    use pi_rs_durable::{
        application_entry,
        document::TaskDocuments,
        task::{Checkpoint, Task, TaskOwner, scheduler::{Handler, Registry, Step}},
    };
    use serde_json::json;
    use std::sync::{Mutex, mpsc};
    use uuid::Uuid;

    struct Writer {
        ready: mpsc::Sender<()>,
        resume: Mutex<mpsc::Receiver<()>>,
    }
    impl Handler for Writer {
        fn run(&self, _: &Task) -> Result<Step, String> { unreachable!() }
        fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
            docs.append_entry("app.before", json!(1)).unwrap();
            docs.configure_runtime_at_checkpoint(Config {
                model: "after".into(), system_prompt: "".into(), max_output_tokens: 64,
            }).unwrap();
            self.ready.send(()).unwrap();
            self.resume.lock().unwrap().recv().unwrap();
            Ok(Step::Succeed(json!(null)))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    let id = h.create_conversation(Config {
        model: "before".into(), system_prompt: "".into(), max_output_tokens: 128,
    }).unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let mut registry = Registry::default();
    registry.register("writer", true, Writer { ready: ready_tx, resume: Mutex::new(resume_rx) });
    let mut scheduler = h.attach_tasks(registry).unwrap();
    let task = Task::new(Uuid::new_v4(), TaskOwner::Conversation(id), Checkpoint {
        phase: "start".into(), data: json!(null),
    }, 3).unwrap().registered("writer".into(), true);
    let task_id = task.id();
    scheduler.submit(&task).unwrap();
    let thread = std::thread::spawn(move || { scheduler.tick(0).unwrap(); scheduler });
    ready_rx.recv().unwrap();
    h.request_abort_task(task_id).unwrap();
    resume_tx.send(()).unwrap();
    let scheduler = thread.join().unwrap();
    assert_eq!(scheduler.get(task_id).unwrap().unwrap().state(), &pi_rs_durable::task::TaskState::Ready);
    assert!(scheduler.get(task_id).unwrap().unwrap().abort_requested());
    assert_eq!(h.conversation(id).unwrap().config.model, "before");
    let db = durable_store::Store::open(&path).unwrap();
    assert!(application_entry::list(&db, id).unwrap().is_empty());
    assert!(db.list(format!("durable/application-ordered-cut/{id}/").as_bytes()).unwrap().is_empty());
}
