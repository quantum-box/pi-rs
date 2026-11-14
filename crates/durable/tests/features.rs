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
    h.submit_and_run(id, None, "latest".into(), &cancel)
        .await
        .unwrap();
    let before = h.conversation(id).unwrap().messages;
    assert!(h.request_compaction(id, 0).unwrap());
    h.resume(&cancel).await.unwrap();
    assert_eq!(h.conversation(id).unwrap().messages, before);
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
