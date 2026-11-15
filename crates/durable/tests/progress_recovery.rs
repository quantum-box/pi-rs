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
    invalid["version"] = serde_json::json!(2);
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
