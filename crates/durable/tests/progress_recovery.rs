use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{CancellationToken, Error, Message, Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::{
    recovery::UncertainPolicy,
    runtime::{Config, Harness, Options, Status},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
struct PartialModel {
    calls: AtomicUsize,
    reached: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl Model for PartialModel {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(!request.messages.iter().any(|m|matches!(m,Message::Assistant{content,..} if content.contains("uncommitted partial"))));
        if attempt == 0 {
            let reached = self.reached.clone();
            let events = stream::unfold((0, reached), |(n, reached)| async move {
                if n == 0 {
                    Some((
                        Ok(ModelEvent::Text("uncommitted partial".into())),
                        (1, reached),
                    ))
                } else {
                    reached.notify_one();
                    std::future::pending().await
                }
            });
            Ok(events.boxed())
        } else {
            Ok(stream::iter(vec![
                Ok(ModelEvent::Text("finished".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])
            .boxed())
        }
    }
}
fn opts(model: Arc<PartialModel>) -> Options {
    Options {
        model,
        tools: vec![],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
#[tokio::test]
async fn committed_partial_becomes_aborted_display_entry_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let reached = Arc::new(tokio::sync::Notify::new());
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: reached.clone(),
    });
    let id;
    {
        let mut h = Harness::open(&path, opts(model.clone()), &cancel)
            .await
            .unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        h.submit(id, None, "work".into()).unwrap();
        {
            let run = h.resume(&cancel);
            tokio::pin!(run);
            tokio::select! {r=&mut run=>panic!("unexpected completion {r:?}"),_=reached.notified()=>{}}
        }
    }
    let h = Harness::open(&path, opts(model.clone()), &cancel)
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    let view = h
        .view(id, std::num::NonZeroUsize::new(50).unwrap())
        .unwrap();
    assert_eq!(
        view.state.aborted_generations[0].text,
        "uncommitted partial"
    );
    assert_eq!(c.aborted_generations.len(), 1);
    assert_eq!(c.aborted_generations[0].text, "uncommitted partial");
    assert_eq!(
        c.submissions[0].status,
        Status::Done {
            answer: "finished".into()
        }
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn view_shows_committed_live_partial() {
    let dir = tempfile::tempdir().unwrap();
    let reached = Arc::new(tokio::sync::Notify::new());
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: reached.clone(),
    });
    let mut scheduler =
        pi_rs_durable::runtime::Scheduler::open(dir.path().join("db"), opts(model)).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let mut watch = scheduler
        .watch_view(
            id,
            std::num::NonZeroUsize::new(50).unwrap(),
            std::time::Duration::from_millis(2),
        )
        .unwrap();
    assert!(watch.initial.entries.is_empty());
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    let view = scheduler
        .view(id, std::num::NonZeroUsize::new(50).unwrap())
        .unwrap();
    assert!(
        matches!(view.live, Some(pi_rs_durable::progress::State::Running(ref partial)) if partial.text == "uncommitted partial")
    );
    let watched = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let value = watch.next().await.unwrap().unwrap();
            if matches!(value.live, Some(pi_rs_durable::progress::State::Running(ref partial)) if partial.text == "uncommitted partial") { break value; }
        }
    }).await.unwrap();
    assert!(
        watched
            .entries
            .iter()
            .any(|m| matches!(m, Message::User { .. }))
    );
    scheduler.abort_and_wait(id).await.unwrap();
    let ended = tokio::time::timeout(std::time::Duration::from_secs(2), watch.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!ended.state.aborted_generations.is_empty());
    watch.close();
    assert!(watch.next().await.unwrap().is_none());
    scheduler.shutdown().await.unwrap();
}
struct EscapedModel;
#[async_trait]
impl Model for EscapedModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        let text = "\\".repeat(600_000);
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text(text)),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
#[tokio::test]
async fn oversized_escaped_progress_does_not_cancel_a_valid_answer() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let options = Options {
        model: Arc::new(EscapedModel),
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
    h.submit_and_run(id, None, "work".into(), &cancel)
        .await
        .unwrap();
    assert!(
        matches!(&h.conversation(id).unwrap().submissions[0].status,Status::Done{answer} if answer.len()==600_000)
    );
}

#[tokio::test]
async fn view_watch_coalesces_and_cancelled_wait_can_resume() {
    use std::{num::NonZeroUsize, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let cancel = CancellationToken::new();
    let mut h = Harness::open(dir.path().join("db"), opts(model), &cancel)
        .await
        .unwrap();
    let config = Config {
        model: "test".into(),
        system_prompt: "".into(),
        max_output_tokens: 100,
    };
    let id = h.create_conversation(config.clone()).unwrap();
    let other = h.create_conversation(config).unwrap();
    let mut watch = h
        .watch_view(id, NonZeroUsize::new(1).unwrap(), Duration::from_millis(2))
        .unwrap();
    assert!(watch.initial.entries.is_empty());
    assert!(h.watch_view(id, NonZeroUsize::MIN, Duration::ZERO).is_err());
    h.submit(other, None, "other".into()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), watch.next())
            .await
            .is_err()
    );
    h.submit(id, None, "one".into()).unwrap();
    h.submit(id, None, "two".into()).unwrap();
    let latest = tokio::time::timeout(Duration::from_secs(2), watch.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(latest.state.submissions.len(), 2);
    assert!(latest.entries.len() <= 1);
    h.submit(id, None, "three".into()).unwrap();
    let next = tokio::time::timeout(Duration::from_secs(2), watch.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(next.state.submissions.len(), 3);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), watch.next())
            .await
            .is_err()
    );
    assert_eq!(watch.end_reason(), None);
    let mut aborted = h
        .watch_view(id, NonZeroUsize::MIN, Duration::from_millis(2))
        .unwrap();
    let stop = CancellationToken::new();
    let trigger = stop.clone();
    let wait = aborted.next_with_cancel(&stop);
    let cancel_wait = async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        trigger.cancel();
    };
    let (result, ()) = tokio::join!(wait, cancel_wait);
    assert!(result.unwrap().is_none());
    assert_eq!(
        aborted.end_reason(),
        Some(&pi_rs_durable::runtime::ViewWatchEnd::Aborted)
    );
    aborted.close();
    assert_eq!(
        aborted.end_reason(),
        Some(&pi_rs_durable::runtime::ViewWatchEnd::Aborted)
    );
    watch.close();
    watch.close();
    assert_eq!(
        watch.end_reason(),
        Some(&pi_rs_durable::runtime::ViewWatchEnd::Stopped)
    );
    assert!(watch.next().await.unwrap().is_none());
}

#[tokio::test]
async fn committed_view_frames_replay_each_commit_and_resume_after_reopen() {
    use pi_rs_durable::runtime::ViewWatchEnd;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let id;
    let saved;
    {
        let mut h = Harness::open_paused(&path, opts(model.clone())).unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        let mut watch = h
            .watch_committed_view(id, None, Duration::from_millis(1))
            .unwrap();
        let mut other = h
            .watch_committed_view(id, None, Duration::from_millis(1))
            .unwrap();
        assert_eq!(
            watch.initial.ops,
            vec![serde_json::json!(["r", watch.initial.value])]
        );
        h.submit(id, None, "one".into()).unwrap();
        h.submit(id, None, "two".into()).unwrap();
        h.submit(id, None, "three".into()).unwrap();
        let first = tokio::time::timeout(Duration::from_secs(2), watch.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(2), watch.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(first.value["submissions"].as_array().unwrap().len(), 1);
        assert_eq!(second.value["submissions"].as_array().unwrap().len(), 2);
        assert_eq!(second.ops, vec![serde_json::json!(["r", second.value])]);
        assert_eq!(other.next().await.unwrap().unwrap(), first);
        saved = second.cursor.clone();
        let encoded = serde_json::to_vec(&saved).unwrap();
        assert_eq!(
            serde_json::from_slice::<pi_rs_durable::runtime::CommittedViewCursor>(&encoded)
                .unwrap(),
            saved
        );
        watch.close();
        assert_eq!(watch.end_reason(), Some(&ViewWatchEnd::Stopped));
        other.abort();
        other.close();
        assert_eq!(other.end_reason(), Some(&ViewWatchEnd::Aborted));
    }
    let h = Harness::open_paused(&path, opts(model)).unwrap();
    let mut watch = h
        .watch_committed_view(id, Some(saved.clone()), Duration::from_millis(1))
        .unwrap();
    assert_eq!(watch.initial.cursor, saved);
    let third = tokio::time::timeout(Duration::from_secs(2), watch.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(third.value["submissions"].as_array().unwrap().len(), 3);
    assert_eq!(third.cursor.position, saved.position + 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), watch.next())
            .await
            .is_err()
    );
    assert_eq!(watch.cursor(), &third.cursor);
    assert_eq!(watch.end_reason(), None);
    let mut wrong = saved.clone();
    wrong.conversation = uuid::Uuid::new_v4();
    assert!(
        h.watch_committed_view(id, Some(wrong), Duration::from_millis(1))
            .is_err()
    );
    let token = CancellationToken::new();
    token.cancel();
    assert!(watch.next_with_cancel(&token).await.unwrap().is_none());
    assert_eq!(watch.end_reason(), Some(&ViewWatchEnd::Aborted));
    assert_eq!(watch.cursor(), &third.cursor);
}

#[tokio::test]
async fn committed_view_rejects_unproven_observer_records() {
    use pi_rs_durable::{observer::Observer, runtime::ViewWatchEnd};
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let mut h = Harness::open_paused(&path, opts(model)).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let mut watch = h
        .watch_committed_view(id, None, Duration::from_millis(1))
        .unwrap();
    let initial = watch.cursor().clone();
    let store = durable_store::Store::open(&path).unwrap();
    let append = Observer::new(&store, id)
        .prepare_append(&serde_json::json!({"forged":true}))
        .unwrap();
    store.commit(&append.checks, &append.mutations).unwrap();
    assert!(watch.next().await.is_err());
    assert!(matches!(watch.end_reason(), Some(ViewWatchEnd::Failed(_))));
    assert_eq!(watch.cursor(), &initial);
    assert!(
        h.watch_committed_view(id, None, Duration::from_millis(1))
            .is_err()
    );
}

#[tokio::test]
async fn committed_view_keeps_ordered_block_occurrence() {
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let mut h = Harness::open_paused(dir.path().join("db"), opts(model)).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let mut watch = h
        .watch_committed_view(id, None, Duration::from_millis(1))
        .unwrap();
    let blocks = vec![
        serde_json::json!({"type":"text","text":"first"}),
        serde_json::json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
        serde_json::json!({"type":"text","text":"last"}),
    ];
    let occurrence = h.append_user_blocks(id, blocks.clone()).unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(2), watch.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        frame.value["content_blocks"]["0"]["message_id"],
        serde_json::json!(occurrence)
    );
    assert_eq!(
        frame.value["content_blocks"]["0"]["blocks"],
        serde_json::json!(blocks)
    );
}

#[tokio::test]
async fn committed_view_rejects_snapshot_from_different_transaction() {
    use pi_rs_durable::observer::Observer;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let mut h = Harness::open_paused(&path, opts(model)).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let mut watch = h
        .watch_committed_view(id, None, Duration::from_millis(1))
        .unwrap();
    let cursor = watch.cursor().clone();
    let store = durable_store::Store::open(&path).unwrap();
    let previous = format!("runtime/fork/{id}/{:020}", cursor.position);
    let fork = store.get(previous.as_bytes()).unwrap().unwrap();
    let append = Observer::new(&store, id)
        .prepare_append(&serde_json::json!({}))
        .unwrap();
    let next = append.cursor.0;
    store.commit(&append.checks, &append.mutations).unwrap();
    let forged = format!("runtime/fork/{id}/{next:020}");
    store
        .commit(
            &[],
            &[durable_store::Mutation::Put {
                key: forged.into_bytes(),
                value: fork.value,
            }],
        )
        .unwrap();
    assert!(
        watch
            .next()
            .await
            .unwrap_err()
            .to_string()
            .contains("unproven committed view transaction")
    );
    assert_eq!(watch.cursor(), &cursor);
}

#[tokio::test]
async fn progress_history_has_atomic_forward_boundary_and_reopens() {
    use pi_rs_durable::{
        progress::State,
        runtime::{ProgressHistoryChange, Scheduler},
    };
    use std::{num::NonZeroUsize, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history.db");
    let reached = Arc::new(tokio::sync::Notify::new());
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: reached.clone(),
    });
    let mut scheduler = Scheduler::open(&path, opts(model.clone())).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let start = scheduler.progress_history_from_now(id).unwrap();
    assert_eq!(start.version, 1);
    assert_eq!(start.position, 0);
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    let first = tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.watch_progress_history(&start, NonZeroUsize::MIN, Duration::from_millis(1)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(first.records.len(), 1);
    let request = match &first.records[0].changes[..] {
        [ProgressHistoryChange::Generation { request_id, state }] => {
            assert!(
                matches!(state.as_ref(), State::Running(partial) if partial.text == "uncommitted partial")
            );
            *request_id
        }
        other => panic!("unexpected progress {other:?}"),
    };
    let reader = durable_store::Store::open(&path).unwrap();
    let snapshot_key = format!("durable/generation-progress/{request}");
    let snapshot = reader.get(snapshot_key.as_bytes()).unwrap().unwrap();
    let mut event_key = b"durable/progress-history/".to_vec();
    event_key.extend_from_slice(id.as_bytes());
    event_key.extend_from_slice(b"/r");
    event_key.extend_from_slice(&first.next.position.to_be_bytes());
    let event = reader.get(&event_key).unwrap().unwrap();
    assert_eq!(
        Some(first.records[0].commit_sequence),
        reader
            .commit_for_version(&event_key, event.version)
            .unwrap()
    );
    assert_eq!(
        reader
            .commit_for_version(&event_key, event.version)
            .unwrap(),
        reader
            .commit_for_version(snapshot_key.as_bytes(), snapshot.version)
            .unwrap()
    );
    let saved = first.next.clone();
    let encoded = serde_json::to_vec(&saved).unwrap();
    assert_eq!(
        serde_json::from_slice::<pi_rs_durable::runtime::ProgressHistoryCursor>(&encoded).unwrap(),
        saved
    );
    assert!(
        scheduler
            .progress_history(&start, NonZeroUsize::new(257).unwrap())
            .is_err()
    );
    let other = scheduler
        .progress_history(&start, NonZeroUsize::MIN)
        .unwrap();
    assert_eq!(other, first);
    scheduler.abort_and_wait(id).await.unwrap();
    scheduler.shutdown().await.unwrap();
    drop(reader);
    let h = Harness::open_paused(&path, opts(model)).unwrap();
    let terminal = h.progress_history(&saved, NonZeroUsize::MIN).unwrap();
    assert_eq!(terminal.records.len(), 1);
    assert!(
        matches!(&terminal.records[0].changes[..], [ProgressHistoryChange::Generation { request_id, state }] if *request_id == request && matches!(state.as_ref(), State::Aborted(partial) if partial.text == "uncommitted partial"))
    );
    assert!(
        h.progress_history(&terminal.next, NonZeroUsize::MIN)
            .unwrap()
            .records
            .is_empty()
    );
    assert_eq!(h.progress_history_from_now(id).unwrap(), terminal.next);
    let mut invalid = saved.clone();
    invalid.version = 2;
    assert!(h.progress_history(&invalid, NonZeroUsize::MIN).is_err());
}

#[tokio::test]
async fn merged_progress_watch_pages_atomic_commits_and_reopens() {
    use pi_rs_durable::{
        progress::State,
        runtime::{ProgressHistoryChange, Scheduler},
    };
    use std::{num::NonZeroUsize, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("merged.db");
    let reached = Arc::new(tokio::sync::Notify::new());
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: reached.clone(),
    });
    let mut s = Scheduler::open(&path, opts(model.clone())).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let start = s.merged_progress_from_now(id).unwrap();
    let encoded = serde_json::to_vec(&start).unwrap();
    assert_eq!(
        serde_json::from_slice::<pi_rs_durable::runtime::MergedProgressCursor>(&encoded).unwrap(),
        start
    );
    s.submit(id, None, "work".into()).unwrap();
    let (first, reached_result) = tokio::join!(
        s.watch_merged_progress(&start, NonZeroUsize::MIN, Duration::from_millis(1)),
        tokio::time::timeout(Duration::from_secs(5), reached.notified())
    );
    reached_result.unwrap();
    let first = first.unwrap();
    assert_eq!(first.frames.len(), 1);
    let second_reader = s.merged_progress(&start, NonZeroUsize::MIN).unwrap();
    assert_eq!(first.frames, second_reader.frames);
    assert_eq!(first.next, second_reader.next);
    let mut cursor = first.next;
    let mut frames = first.frames;
    loop {
        let page = s.merged_progress(&cursor, NonZeroUsize::MIN).unwrap();
        assert!(page.frames.len() <= 1);
        cursor = page.next;
        if page.frames.is_empty() {
            break;
        }
        frames.extend(page.frames);
    }
    assert!(
        frames
            .windows(2)
            .all(|pair| pair[0].commit_sequence < pair[1].commit_sequence)
    );
    assert!(frames.iter().any(|frame| frame.observation.is_some()));
    assert!(frames.iter().any(|frame| frame.progress.as_ref().is_some_and(|p| p.changes.iter().any(|c| matches!(c, ProgressHistoryChange::Generation { state, .. } if matches!(state.as_ref(), State::Running(partial) if partial.text == "uncommitted partial"))))));
    let saved = cursor.clone();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            s.watch_merged_progress(&cursor, NonZeroUsize::MIN, Duration::from_millis(1))
        )
        .await
        .is_err()
    );
    s.abort_and_wait(id).await.unwrap();
    s.shutdown().await.unwrap();
    let h = Harness::open_paused(&path, opts(model)).unwrap();
    let mut cursor = saved;
    let mut after = Vec::new();
    loop {
        let page = h.merged_progress(&cursor, NonZeroUsize::MIN).unwrap();
        cursor = page.next;
        if page.frames.is_empty() {
            break;
        }
        after.extend(page.frames);
    }
    assert!(
        after
            .windows(2)
            .all(|pair| pair[0].commit_sequence < pair[1].commit_sequence)
    );
    let terminal = after.iter().find(|frame| frame.progress.as_ref().is_some_and(|p| p.changes.iter().any(|c| matches!(c, ProgressHistoryChange::Generation { state, .. } if matches!(state.as_ref(), State::Aborted(_)))))).unwrap();
    assert!(
        terminal.observation.is_some(),
        "atomic abort progress must not be split"
    );
    assert_eq!(
        terminal.progress.as_ref().unwrap().commit_sequence,
        terminal.commit_sequence
    );
    let mut invalid = serde_json::to_value(&cursor).unwrap();
    invalid["version"] = serde_json::json!(3);
    let invalid = serde_json::from_value(invalid).unwrap();
    assert!(h.merged_progress(&invalid, NonZeroUsize::MIN).is_err());
    let mut skipped = serde_json::to_value(&cursor).unwrap();
    skipped["progress_position"] = serde_json::json!(0);
    let skipped = serde_json::from_value(skipped).unwrap();
    assert!(h.merged_progress(&skipped, NonZeroUsize::MIN).is_err());
}

#[tokio::test]
async fn merged_progress_rejects_unproven_observation_without_advancing() {
    use std::{num::NonZeroUsize, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let mut h = Harness::open_paused(&path, opts(model)).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let start = h.merged_progress_from_now(id).unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let append = pi_rs_durable::observer::Observer::new(&store, id)
        .prepare_append(&serde_json::json!({"forged":true}))
        .unwrap();
    store.commit(&append.checks, &append.mutations).unwrap();
    assert!(h.merged_progress(&start, NonZeroUsize::MIN).is_err());
    assert!(
        h.watch_merged_progress(&start, NonZeroUsize::MIN, Duration::from_millis(1))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn merged_watch_groups_documents_and_replays_deletion_after_reopen() {
    use pi_rs_durable::{
        document::{Address, Documents, Transaction},
        runtime::AdmissionOptions,
    };
    use std::num::NonZeroUsize;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("docs.db");
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let id;
    let address;
    let saved;
    {
        let mut h = Harness::open_paused(&path, opts(model.clone())).unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        address = Address {
            scope: id.to_string(),
            name: "note".into(),
            key: "a".into(),
        };
        let second = Address {
            key: "b".into(),
            ..address.clone()
        };
        let other = Address {
            scope: "shared".into(),
            ..address.clone()
        };
        let start = h.merged_progress_from_now(id).unwrap();
        let mut legacy_json = serde_json::to_value(&start).unwrap();
        legacy_json["version"] = serde_json::json!(1);
        legacy_json
            .as_object_mut()
            .unwrap()
            .remove("document_position");
        let legacy = serde_json::from_value(legacy_json).unwrap();
        h.submit_with_documents(
            id,
            None,
            "work".into(),
            AdmissionOptions::default(),
            |docs| {
                for item in [&address, &second, &other] {
                    docs.get(item)?;
                }
                for item in [&address, &second, &other] {
                    docs.put(
                        item,
                        serde_json::json!({"value": 1}).as_object().unwrap().clone(),
                    )?;
                }
                Ok(())
            },
        )
        .unwrap();
        let page = h.merged_progress(&start, NonZeroUsize::MIN).unwrap();
        assert_eq!(page.frames.len(), 1);
        let frame = &page.frames[0];
        assert!(frame.observation.is_some());
        assert_eq!(frame.documents.len(), 2);
        assert!(
            frame
                .documents
                .iter()
                .all(|change| change.document.as_ref().unwrap().value["value"] == 1)
        );
        assert_eq!(h.merged_progress(&start, NonZeroUsize::MIN).unwrap(), page);
        let old_page = h.merged_progress(&legacy, NonZeroUsize::MIN).unwrap();
        assert!(old_page.frames[0].documents.is_empty());
        assert!(
            h.merged_progress(&start, NonZeroUsize::new(257).unwrap())
                .is_err()
        );
        saved = page.next;
        let store = durable_store::Store::open(&path).unwrap();
        let version = Documents::new(&store)
            .get(&address)
            .unwrap()
            .unwrap()
            .version;
        let mut tx = Transaction::new();
        tx.delete(&address, version).unwrap();
        tx.commit(&store).unwrap();
    }
    let h = Harness::open_paused(&path, opts(model)).unwrap();
    let encoded = serde_json::to_vec(&saved).unwrap();
    let restored = serde_json::from_slice(&encoded).unwrap();
    let page = h.merged_progress(&restored, NonZeroUsize::MIN).unwrap();
    assert_eq!(page.frames.len(), 1);
    assert!(page.frames[0].observation.is_none());
    assert_eq!(page.frames[0].documents.len(), 1);
    assert_eq!(page.frames[0].documents[0].address, address);
    assert!(page.frames[0].documents[0].document.is_none());
    assert!(
        h.merged_progress(&page.next, NonZeroUsize::MIN)
            .unwrap()
            .frames
            .is_empty()
    );
}

#[test]
fn merged_document_watch_readers_do_not_skip_concurrent_commits() {
    use pi_rs_durable::document::{Address, Documents, Transaction};
    use std::num::NonZeroUsize;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("concurrent.db");
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let mut h = Harness::open_paused(&path, opts(model)).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let address = Address {
        scope: id.to_string(),
        name: "note".into(),
        key: "".into(),
    };
    let initial = h.merged_progress_from_now(id).unwrap();
    let writer_path = path.clone();
    let writer_address = address.clone();
    let thread = std::thread::spawn(move || {
        let store = durable_store::Store::open(writer_path).unwrap();
        for n in 0..30 {
            let expected = Documents::new(&store)
                .get(&writer_address)
                .unwrap()
                .map_or(durable_store::Expected::Missing, |doc| {
                    durable_store::Expected::Version(doc.version)
                });
            let mut tx = Transaction::new();
            tx.put(
                &writer_address,
                expected,
                serde_json::json!({"n":n}).as_object().unwrap(),
            )
            .unwrap();
            tx.commit(&store).unwrap();
        }
    });
    let mut cursor = initial.clone();
    let mut frames = Vec::new();
    loop {
        let page = h.merged_progress(&cursor, NonZeroUsize::MIN).unwrap();
        cursor = page.next;
        if page.frames.is_empty() {
            if thread.is_finished() {
                break;
            }
            std::thread::yield_now();
        } else {
            frames.extend(page.frames);
        }
    }
    thread.join().unwrap();
    assert_eq!(frames.len(), 30);
    for (index, frame) in frames.iter().enumerate() {
        assert_eq!(frame.documents.len(), 1);
        assert_eq!(
            frame.documents[0].document.as_ref().unwrap().value["n"],
            index
        );
    }
    let again = h
        .merged_progress(&initial, NonZeroUsize::new(64).unwrap())
        .unwrap();
    assert_eq!(again.frames, frames);
    assert_eq!(again.next, cursor);
}

#[test]
fn merged_document_frame_limit_fails_without_consuming_cursor() {
    use pi_rs_durable::document::{Address, Transaction};
    use std::num::NonZeroUsize;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("large.db");
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let mut h = Harness::open_paused(&path, opts(model)).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let cursor = h.merged_progress_from_now(id).unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let mut tx = Transaction::new();
    for index in 0..257 {
        let address = Address {
            scope: id.to_string(),
            name: "limit".into(),
            key: index.to_string(),
        };
        tx.put(
            &address,
            durable_store::Expected::Missing,
            serde_json::json!({"v":index}).as_object().unwrap(),
        )
        .unwrap();
    }
    tx.commit(&store).unwrap();
    for _ in 0..2 {
        let error = h.merged_progress(&cursor, NonZeroUsize::MIN).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("document commit exceeds merged watch limits")
        );
    }
}

#[tokio::test]
async fn merged_watch_handle_delivers_progress_documents_and_reopens() {
    use pi_rs_durable::{
        document::{Address, Documents, Transaction},
        progress::State,
        runtime::{AdmissionOptions, ProgressHistoryChange, Scheduler, ViewWatchEnd},
    };
    use std::{num::NonZeroUsize, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("handle.db");
    let reached = Arc::new(tokio::sync::Notify::new());
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: reached.clone(),
    });
    let mut s = Scheduler::open(&path, opts(model.clone())).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let address = Address {
        scope: id.to_string(),
        name: "note".into(),
        key: "".into(),
    };
    assert!(
        s.watch_merged_progress_handle(
            id,
            None,
            NonZeroUsize::new(257).unwrap(),
            Duration::from_millis(1)
        )
        .is_err()
    );
    assert!(
        s.watch_merged_progress_handle(id, None, NonZeroUsize::MIN, Duration::ZERO)
            .is_err()
    );
    let mut watch = s
        .watch_merged_progress_handle(id, None, NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap();
    let start = watch.cursor().clone();
    let mut other = s
        .watch_merged_progress_handle(
            id,
            Some(start.clone()),
            NonZeroUsize::MIN,
            Duration::from_millis(1),
        )
        .unwrap();
    let mut legacy = serde_json::to_value(&start).unwrap();
    legacy["version"] = serde_json::json!(1);
    legacy.as_object_mut().unwrap().remove("document_position");
    let legacy = serde_json::from_value(legacy).unwrap();
    let mut old = s
        .watch_merged_progress_handle(
            id,
            Some(legacy),
            NonZeroUsize::MIN,
            Duration::from_millis(1),
        )
        .unwrap();
    s.submit_with_documents(
        id,
        None,
        "work".into(),
        AdmissionOptions::default(),
        |docs| {
            docs.get(&address)?;
            docs.put(
                &address,
                serde_json::json!({"value":1}).as_object().unwrap().clone(),
            )
        },
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    let first = watch.next().await.unwrap().unwrap();
    let first_other = other.next().await.unwrap().unwrap();
    assert_eq!(first.frames, first_other.frames);
    assert!(first.frames[0].observation.is_some());
    assert_eq!(first.frames[0].documents[0].address, address);
    assert!(
        old.next().await.unwrap().unwrap().frames[0]
            .documents
            .is_empty()
    );
    loop {
        let page = tokio::time::timeout(Duration::from_secs(5), watch.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if page.frames.iter().any(|frame| frame.progress.as_ref().is_some_and(|p| p.changes.iter().any(|change| matches!(change, ProgressHistoryChange::Generation { state, .. } if matches!(state.as_ref(), State::Running(_)))))) { break; }
    }
    let before_abort = watch.cursor().clone();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), watch.next())
            .await
            .is_err()
    );
    assert_eq!(watch.cursor(), &before_abort);
    assert_eq!(watch.end_reason(), None);
    s.abort_and_wait(id).await.unwrap();
    loop {
        let page = tokio::time::timeout(Duration::from_secs(5), watch.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Some(terminal) = page.frames.iter().find(|frame| frame.progress.as_ref().is_some_and(|p| p.changes.iter().any(|change| matches!(change, ProgressHistoryChange::Generation { state, .. } if matches!(state.as_ref(), State::Aborted(_)))))) {
            assert!(terminal.observation.is_some());
            break;
        }
    }
    let saved = watch.cursor().clone();
    watch.close();
    watch.abort();
    assert_eq!(watch.end_reason(), Some(&ViewWatchEnd::Stopped));
    assert!(watch.next().await.unwrap().is_none());
    other.close();
    old.close();
    s.shutdown().await.unwrap();
    let h = Harness::open_paused(&path, opts(model)).unwrap();
    let mut resumed = h
        .watch_merged_progress_handle(id, Some(saved), NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let version = Documents::new(&store)
        .get(&address)
        .unwrap()
        .unwrap()
        .version;
    let mut tx = Transaction::new();
    tx.delete(&address, version).unwrap();
    tx.commit(&store).unwrap();
    loop {
        let page = tokio::time::timeout(Duration::from_secs(5), resumed.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Some(change) = page
            .frames
            .iter()
            .flat_map(|frame| &frame.documents)
            .find(|change| change.address == address)
        {
            assert!(change.document.is_none());
            break;
        }
    }
    let cursor = resumed.cursor().clone();
    let token = CancellationToken::new();
    token.cancel();
    assert!(resumed.next_with_cancel(&token).await.unwrap().is_none());
    assert_eq!(resumed.end_reason(), Some(&ViewWatchEnd::Aborted));
    assert_eq!(resumed.cursor(), &cursor);
}

#[tokio::test]
async fn merged_watch_handle_failure_is_terminal_without_cursor_advance() {
    use pi_rs_durable::{observer::Observer, runtime::ViewWatchEnd};
    use std::{num::NonZeroUsize, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let mut h = Harness::open_paused(&path, opts(model)).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let mut watch = h
        .watch_merged_progress_handle(id, None, NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap();
    let cursor = watch.cursor().clone();
    let mut listener = h
        .watch_merged_progress_handle(
            id,
            Some(cursor.clone()),
            NonZeroUsize::MIN,
            Duration::from_millis(1),
        )
        .unwrap()
        .into_listener()
        .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let append = Observer::new(&store, id)
        .prepare_append(&serde_json::json!({"forged":true}))
        .unwrap();
    store.commit(&append.checks, &append.mutations).unwrap();
    assert!(watch.next().await.is_err());
    assert!(matches!(watch.end_reason(), Some(ViewWatchEnd::Failed(_))));
    assert_eq!(watch.cursor(), &cursor);
    watch.close();
    assert!(matches!(watch.end_reason(), Some(ViewWatchEnd::Failed(_))));
    assert!(watch.next().await.unwrap().is_none());
    let end = listener
        .run(
            &pi_rs_durable::runtime::MergedListenerSignals::default(),
            |_| async { panic!("invalid frame delivered") },
        )
        .await;
    assert!(matches!(
        end,
        pi_rs_durable::runtime::MergedListenerEnd::Failed(_)
    ));
    assert_eq!(listener.cursor(), &cursor);
}

#[tokio::test]
async fn merged_listener_source_close_waits_for_ack_and_replays_later_commits() {
    use pi_rs_durable::{
        document::Address,
        runtime::{AdmissionOptions, MergedListenerEnd, MergedListenerSignals},
    };
    use std::{num::NonZeroUsize, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("listener.db");
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let mut h = Harness::open_paused(&path, opts(model.clone())).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let address = Address {
        scope: id.to_string(),
        name: "note".into(),
        key: "".into(),
    };
    let mut listener = h
        .watch_merged_progress_handle(id, None, NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap()
        .into_listener()
        .unwrap();
    let start = listener.cursor().clone();
    h.submit_with_documents(
        id,
        None,
        "one".into(),
        AdmissionOptions::default(),
        |docs| {
            docs.get(&address)?;
            docs.put(
                &address,
                serde_json::json!({"n":1}).as_object().unwrap().clone(),
            )
        },
    )
    .unwrap();
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let signals = MergedListenerSignals::default();
    {
        let entered2 = entered.clone();
        let release2 = release.clone();
        let seen2 = seen.clone();
        let run = listener.run(&signals, move |page| {
            let entered = entered2.clone();
            let release = release2.clone();
            let seen = seen2.clone();
            async move {
                seen.lock().unwrap().push(page);
                entered.add_permits(1);
                release.acquire().await.unwrap().forget();
                Ok(())
            }
        });
        tokio::pin!(run);
        tokio::select! { result = &mut run => panic!("ended early: {result:?}"), permit = entered.acquire() => { permit.unwrap().forget(); } }
        h.submit(id, None, "two".into()).unwrap();
        signals.source_closed.cancel();
        release.add_permits(1);
        assert_eq!(run.await, MergedListenerEnd::Ended);
    }
    let saved = listener.cursor().clone();
    {
        let pages = seen.lock().unwrap();
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].frames.len(), 1);
        assert!(pages[0].frames[0].observation.is_some());
        assert_eq!(pages[0].frames[0].documents[0].address, address);
        assert_ne!(saved, start);
        assert_eq!(saved, pages[0].next);
    }
    assert_eq!(listener.end_reason(), Some(&MergedListenerEnd::Ended));
    let mut legacy_value = serde_json::to_value(&start).unwrap();
    legacy_value["version"] = serde_json::json!(1);
    legacy_value
        .as_object_mut()
        .unwrap()
        .remove("document_position");
    let legacy_cursor = serde_json::from_value(legacy_value).unwrap();
    let mut legacy = h
        .watch_merged_progress_handle(
            id,
            Some(legacy_cursor),
            NonZeroUsize::MIN,
            Duration::from_millis(1),
        )
        .unwrap()
        .into_listener()
        .unwrap();
    let legacy_signals = MergedListenerSignals::default();
    let legacy_stop = legacy_signals.stop.clone();
    let legacy_seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let legacy_capture = legacy_seen.clone();
    assert_eq!(
        legacy
            .run(&legacy_signals, move |page| {
                let stop = legacy_stop.clone();
                let capture = legacy_capture.clone();
                async move {
                    capture.lock().unwrap().push(page);
                    stop.cancel();
                    Ok(())
                }
            })
            .await,
        MergedListenerEnd::Stopped
    );
    assert!(
        legacy_seen.lock().unwrap()[0].frames[0]
            .documents
            .is_empty()
    );
    drop(legacy);
    drop(listener);
    drop(h);
    let h = Harness::open_paused(&path, opts(model)).unwrap();
    let mut resumed = h
        .watch_merged_progress_handle(id, Some(saved), NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap();
    let next = resumed.next().await.unwrap().unwrap();
    assert_eq!(
        next.frames[0].observation.as_ref().unwrap().value["submissions"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn merged_listener_failure_does_not_ack_and_abort_is_terminal() {
    use pi_rs_durable::runtime::{MergedListenerEnd, MergedListenerSignals};
    use std::{num::NonZeroUsize, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let mut h = Harness::open_paused(dir.path().join("db"), opts(model)).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let mut listener = h
        .watch_merged_progress_handle(id, None, NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap()
        .into_listener()
        .unwrap();
    let start = listener.cursor().clone();
    h.submit(id, None, "one".into()).unwrap();
    let signals = MergedListenerSignals::default();
    let end = listener
        .run(&signals, |_| async { Err("rejected".to_owned()) })
        .await;
    assert_eq!(end, MergedListenerEnd::ListenerError("rejected".into()));
    assert_eq!(listener.cursor(), &start);
    signals.stop.cancel();
    assert_eq!(listener.run(&signals, |_| async { Ok(()) }).await, end);
    let mut panicking = h
        .watch_merged_progress_handle(
            id,
            Some(start.clone()),
            NonZeroUsize::MIN,
            Duration::from_millis(1),
        )
        .unwrap()
        .into_listener()
        .unwrap();
    assert_eq!(
        panicking
            .run(&MergedListenerSignals::default(), |_| async {
                panic!("listener failure")
            })
            .await,
        MergedListenerEnd::ListenerError("listener panicked".into())
    );
    assert_eq!(panicking.cursor(), &start);
    let mut replay = h
        .watch_merged_progress_handle(
            id,
            Some(start.clone()),
            NonZeroUsize::MIN,
            Duration::from_millis(1),
        )
        .unwrap()
        .into_listener()
        .unwrap();
    let abort = MergedListenerSignals::default();
    abort.abort.cancel();
    assert_eq!(
        replay
            .run(&abort, |_| async { panic!("cancelled listener called") })
            .await,
        MergedListenerEnd::Aborted
    );
    assert_eq!(replay.cursor(), &start);
    let mut pull = h
        .watch_merged_progress_handle(id, Some(start), NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap();
    assert_eq!(
        pull.next().await.unwrap().unwrap().frames[0]
            .observation
            .as_ref()
            .unwrap()
            .value["submissions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn merged_listener_delivers_model_progress_and_atomic_abort() {
    use pi_rs_durable::{
        progress::State,
        runtime::{MergedListenerEnd, MergedListenerSignals, ProgressHistoryChange, Scheduler},
    };
    use std::{num::NonZeroUsize, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let reached = Arc::new(tokio::sync::Notify::new());
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: reached.clone(),
    });
    let mut scheduler = Scheduler::open(dir.path().join("db"), opts(model)).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let mut listener = scheduler
        .watch_merged_progress_handle(id, None, NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap()
        .into_listener()
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    scheduler.abort_and_wait(id).await.unwrap();
    let signals = MergedListenerSignals::default();
    let stop = signals.stop.clone();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let capture = seen.clone();
    let end = tokio::time::timeout(Duration::from_secs(5), listener.run(&signals, move |page| {
        let capture = capture.clone(); let stop = stop.clone();
        async move {
            if page.frames.iter().any(|frame| frame.progress.as_ref().is_some_and(|p| p.changes.iter().any(|change| matches!(change, ProgressHistoryChange::Generation { state, .. } if matches!(state.as_ref(), State::Aborted(_)))))) { stop.cancel(); }
            capture.lock().unwrap().push(page);
            Ok(())
        }
    })).await.unwrap();
    assert_eq!(end, MergedListenerEnd::Stopped);
    {
        let pages = seen.lock().unwrap();
        assert!(pages.iter().flat_map(|p| &p.frames).any(|frame| frame.progress.as_ref().is_some_and(|p| p.changes.iter().any(|change| matches!(change, ProgressHistoryChange::Generation { state, .. } if matches!(state.as_ref(), State::Running(_)))))));
        let terminal = pages.iter().flat_map(|p| &p.frames).find(|frame| frame.progress.as_ref().is_some_and(|p| p.changes.iter().any(|change| matches!(change, ProgressHistoryChange::Generation { state, .. } if matches!(state.as_ref(), State::Aborted(_)))))).unwrap();
        assert!(terminal.observation.is_some());
        assert_eq!(listener.cursor(), &pages.last().unwrap().next);
    }
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn merged_listener_cancelled_callback_replays_unacknowledged_page() {
    use pi_rs_durable::runtime::{MergedListenerEnd, MergedListenerSignals};
    use std::{num::NonZeroUsize, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let mut h = Harness::open_paused(dir.path().join("db"), opts(model)).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let mut listener = h
        .watch_merged_progress_handle(id, None, NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap()
        .into_listener()
        .unwrap();
    let start = listener.cursor().clone();
    h.submit(id, None, "one".into()).unwrap();
    let signals = MergedListenerSignals::default();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            listener.run(&signals, |_| async {
                std::future::pending::<Result<(), String>>().await
            })
        )
        .await
        .is_err()
    );
    assert_eq!(listener.cursor(), &start);
    assert_eq!(listener.end_reason(), None);
    let stop = signals.stop.clone();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let capture = seen.clone();
    assert_eq!(
        listener
            .run(&signals, move |page| {
                let stop = stop.clone();
                let capture = capture.clone();
                async move {
                    capture.lock().unwrap().push(page);
                    stop.cancel();
                    Ok(())
                }
            })
            .await,
        MergedListenerEnd::Stopped
    );
    let pages = seen.lock().unwrap();
    assert_eq!(pages.len(), 1);
    assert_eq!(
        pages[0].frames[0].observation.as_ref().unwrap().value["submissions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(listener.cursor(), &pages[0].next);
}

#[tokio::test]
async fn merged_watch_drains_shutdown_commits_then_releases_owner() {
    use pi_rs_durable::{
        progress::State,
        runtime::{ProgressHistoryChange, Scheduler, ViewWatchEnd},
    };
    use std::{num::NonZeroUsize, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shutdown.db");
    let reached = Arc::new(tokio::sync::Notify::new());
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: reached.clone(),
    });
    let mut scheduler = Scheduler::open(&path, opts(model.clone())).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let mut watch = scheduler
        .watch_merged_progress_handle(id, None, NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    scheduler.shutdown().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), watch.source_closed())
            .await
            .unwrap()
            .unwrap()
            > 0
    );
    let mut frames = Vec::new();
    while let Some(page) = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .unwrap()
        .unwrap()
    {
        frames.extend(page.frames);
    }
    assert_eq!(watch.end_reason(), Some(&ViewWatchEnd::Ended));
    assert!(frames.iter().any(|f| f.progress.as_ref().is_some_and(|p| p.changes.iter().any(|c| matches!(c, ProgressHistoryChange::Generation { state, .. } if matches!(state.as_ref(), State::Running(_)))))));
    let saved = watch.cursor().clone();
    let h = Harness::open_paused(&path, opts(model)).unwrap();
    let mut resumed = h
        .watch_merged_progress_handle(id, Some(saved), NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap();
    loop {
        match tokio::time::timeout(Duration::from_millis(20), resumed.next()).await {
            Ok(Ok(Some(_))) => continue,
            Err(_) => break,
            other => panic!("reopened source ended unexpectedly: {other:?}"),
        }
    }
    assert_eq!(resumed.end_reason(), None);
}

struct LifetimeGateModel {
    started: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}
#[async_trait]
impl Model for LifetimeGateModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        let started = self.started.clone();
        let release = self.release.clone();
        Ok(stream::once(async move {
            started.add_permits(1);
            release.acquire().await.unwrap().forget();
            Ok(ModelEvent::Text("finished".into()))
        })
        .chain(stream::iter(vec![Ok(ModelEvent::Complete {
            calls: vec![],
            usage: None,
        })]))
        .boxed())
    }
}
#[tokio::test]
async fn merged_watch_waits_for_worker_after_scheduler_drop() {
    use pi_rs_durable::runtime::{Scheduler, ViewWatchEnd};
    use std::{num::NonZeroUsize, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("drop.db");
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let options = || Options {
        model: Arc::new(LifetimeGateModel {
            started: started.clone(),
            release: release.clone(),
        }),
        tools: vec![],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let mut watch = scheduler
        .watch_merged_progress_handle(
            id,
            None,
            NonZeroUsize::new(256).unwrap(),
            Duration::from_millis(1),
        )
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    drop(scheduler);
    loop {
        match tokio::time::timeout(Duration::from_millis(50), watch.next()).await {
            Ok(Ok(Some(_))) => continue,
            Err(_) => break,
            other => panic!("source ended while worker was active: {other:?}"),
        }
    }
    assert_eq!(watch.end_reason(), None);
    assert!(
        tokio::time::timeout(Duration::from_millis(15), watch.source_closed())
            .await
            .is_err()
    );
    release.add_permits(1);
    let mut frames = Vec::new();
    while let Some(page) = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .unwrap()
        .unwrap()
    {
        frames.extend(page.frames);
    }
    assert_eq!(watch.end_reason(), Some(&ViewWatchEnd::Ended));
    assert!(frames.iter().any(|f| {
        f.observation
            .as_ref()
            .is_some_and(|o| o.value["submissions"].to_string().contains("finished"))
    }));
    let reopened = Harness::open_paused(&path, options()).unwrap();
    assert!(reopened.conversation(id).is_ok());
}

#[tokio::test]
async fn merged_watch_close_boundary_excludes_later_external_commits() {
    use pi_rs_durable::{
        document::{Address, Transaction},
        runtime::ViewWatchEnd,
    };
    use std::{num::NonZeroUsize, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("boundary.db");
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let mut h = Harness::open_paused(&path, opts(model.clone())).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let mut watch = h
        .watch_merged_progress_handle(id, None, NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap();
    h.submit(id, None, "before".into()).unwrap();
    drop(h);
    let store = durable_store::Store::open(&path).unwrap();
    let address = Address {
        scope: id.to_string(),
        name: "after".into(),
        key: "".into(),
    };
    let mut tx = Transaction::new();
    tx.put(
        &address,
        durable_store::Expected::Missing,
        serde_json::json!({"n":1}).as_object().unwrap(),
    )
    .unwrap();
    tx.commit(&store).unwrap();
    let mut seen = Vec::new();
    while let Some(page) = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .unwrap()
        .unwrap()
    {
        seen.extend(page.frames);
    }
    assert_eq!(watch.end_reason(), Some(&ViewWatchEnd::Ended));
    assert!(seen.iter().any(|frame| frame.observation.is_some()));
    assert!(seen.iter().all(|frame| frame.documents.is_empty()));
    let saved = watch.cursor().clone();
    drop(store);
    let h = Harness::open_paused(&path, opts(model)).unwrap();
    let mut resumed = h
        .watch_merged_progress_handle(id, Some(saved), NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap();
    loop {
        let page = tokio::time::timeout(Duration::from_secs(5), resumed.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if page.frames.iter().any(|frame| {
            frame
                .documents
                .iter()
                .any(|change| change.address == address)
        }) {
            break;
        }
    }
}

#[tokio::test]
async fn merged_listener_drains_to_automatic_close_after_inflight_ack() {
    use pi_rs_durable::runtime::{MergedListenerEnd, MergedListenerSignals};
    use std::{num::NonZeroUsize, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("listener-close.db");
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let mut h = Harness::open_paused(&path, opts(model.clone())).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let mut listener = h
        .watch_merged_progress_handle(id, None, NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap()
        .into_listener()
        .unwrap();
    h.submit(id, None, "one".into()).unwrap();
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let pages = Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let signals = MergedListenerSignals::default();
    {
        let started = entered.clone();
        let gate = release.clone();
        let seen = pages.clone();
        let count = calls.clone();
        let run = listener.run(&signals, move |page| {
            let started = started.clone();
            let gate = gate.clone();
            let seen = seen.clone();
            let count = count.clone();
            async move {
                let first = count.fetch_add(1, Ordering::SeqCst) == 0;
                if first {
                    started.add_permits(1);
                    gate.acquire().await.unwrap().forget();
                }
                seen.lock().unwrap().push(page);
                Ok(())
            }
        });
        tokio::pin!(run);
        tokio::select! { result = &mut run => panic!("early end: {result:?}"), permit = entered.acquire() => permit.unwrap().forget() }
        h.submit(id, None, "two".into()).unwrap();
        drop(h);
        release.add_permits(1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), &mut run)
                .await
                .unwrap(),
            MergedListenerEnd::Ended
        );
    }
    let saved = listener.cursor().clone();
    {
        let seen = pages.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen.last().unwrap().next, saved);
    }
    assert_eq!(listener.end_reason(), Some(&MergedListenerEnd::Ended));
    assert!(listener.source_closed().await.unwrap() > 0);
    drop(listener);
    let h = Harness::open_paused(&path, opts(model)).unwrap();
    let mut watch = h
        .watch_merged_progress_handle(id, Some(saved), NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), watch.next())
            .await
            .is_err()
    );
}

fn apply_derived_ops(mut value: serde_json::Value, ops: &[serde_json::Value]) -> serde_json::Value {
    for op in ops {
        let op = op.as_array().unwrap();
        match op[0].as_str().unwrap() {
            "r" => value = op[1].clone(),
            "s" => {
                value
                    .as_object_mut()
                    .unwrap()
                    .insert(op[1][0].as_str().unwrap().into(), op[2].clone());
            }
            "d" => {
                value
                    .as_object_mut()
                    .unwrap()
                    .remove(op[1][0].as_str().unwrap());
            }
            "p" => {
                let array = value[op[1][0].as_str().unwrap()].as_array_mut().unwrap();
                let start = op[2].as_u64().unwrap() as usize;
                let count = op[3].as_u64().unwrap() as usize;
                array.splice(start..start + count, op[4].as_array().unwrap().clone());
            }
            other => panic!("unexpected operation {other}"),
        }
    }
    value
}

#[tokio::test]
async fn merged_incremental_watch_applies_atomic_frames_and_reopens() {
    use pi_rs_durable::{
        document::{Address, Documents, Transaction},
        runtime::{AdmissionOptions, ViewWatchEnd},
    };
    use std::{num::NonZeroUsize, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ops.db");
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let mut h = Harness::open_paused(&path, opts(model.clone())).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let address = Address {
        scope: id.to_string(),
        name: "note".into(),
        key: "".into(),
    };
    let mut watch = h
        .watch_merged_progress_handle(id, None, NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap();
    let initial_cursor = watch.cursor().clone();
    h.submit_with_documents(
        id,
        None,
        "x".repeat(2000),
        AdmissionOptions::default(),
        |docs| {
            docs.get(&address)?;
            docs.put(
                &address,
                serde_json::json!({"stable":"z".repeat(2000),"items":[1],"old":1})
                    .as_object()
                    .unwrap()
                    .clone(),
            )
        },
    )
    .unwrap();
    let replay = h
        .merged_progress_operations(&initial_cursor, NonZeroUsize::MIN)
        .unwrap();
    assert_eq!(watch.cursor(), &initial_cursor);
    let first = watch.next_operations().await.unwrap().unwrap();
    assert_eq!(first, replay);
    let frame = &first.frames[0];
    assert!(frame.frame.observation.is_some());
    assert_eq!(frame.documents.len(), 1);
    assert_eq!(frame.documents[0].ops[0][0], "r");
    let store = durable_store::Store::open(&path).unwrap();
    let version = Documents::new(&store)
        .get(&address)
        .unwrap()
        .unwrap()
        .version;
    let mut tx = Transaction::new();
    tx.put(
        &address,
        durable_store::Expected::Version(version),
        serde_json::json!({"stable":"z".repeat(2000),"items":[1,2],"new":true})
            .as_object()
            .unwrap(),
    )
    .unwrap();
    tx.commit(&store).unwrap();
    h.submit(id, None, "second".into()).unwrap();
    let mut found_doc = false;
    let mut found_observation = false;
    for _ in 0..2 {
        let page = tokio::time::timeout(Duration::from_secs(5), watch.next_operations())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        for item in &page.frames {
            if let Some(observation) = &item.frame.observation
                && let Some(previous) = &item.observation_previous
            {
                assert_eq!(
                    apply_derived_ops(previous.clone(), &observation.ops),
                    observation.value
                );
                found_observation |= observation.ops.iter().any(|op| op[0] != "r");
            }
            for (patch, change) in item.documents.iter().zip(&item.frame.documents) {
                let previous = patch.previous.as_ref().unwrap();
                let after = change.document.as_ref().unwrap();
                assert_eq!(
                    apply_derived_ops(serde_json::json!(previous.value), &patch.ops),
                    serde_json::json!(after.value)
                );
                assert!(patch.ops.iter().any(|op| op[0] == "p"));
                found_doc = true;
            }
        }
    }
    assert!(found_doc && found_observation);
    let saved = watch.cursor().clone();
    drop(h);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), watch.next_operations())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    assert_eq!(watch.end_reason(), Some(&ViewWatchEnd::Ended));
    drop(store);
    let h = Harness::open_paused(&path, opts(model)).unwrap();
    let mut resumed = h
        .watch_merged_progress_handle(id, Some(saved), NonZeroUsize::MIN, Duration::from_millis(1))
        .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let version = Documents::new(&store)
        .get(&address)
        .unwrap()
        .unwrap()
        .version;
    let mut tx = Transaction::new();
    tx.delete(&address, version).unwrap();
    tx.commit(&store).unwrap();
    loop {
        let page = tokio::time::timeout(Duration::from_secs(5), resumed.next_operations())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Some(patch) = page
            .frames
            .iter()
            .flat_map(|frame| &frame.documents)
            .find(|patch| patch.address == address)
        {
            assert!(patch.retired && patch.ops.is_empty());
            break;
        }
    }
}
