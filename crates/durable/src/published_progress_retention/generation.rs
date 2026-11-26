//! Expiry for published running model partials. The terminal checkpoint is
//! retained as the writer fence and as evidence for expiry.
use super::{PAGE_ROWS, enable_at, enabled_at, parse_payload_key, payload_key, read_payload};
use crate::{
    observer::{Append, Cursor, Observer},
    progress::{self, MAX_PROGRESS_BYTES, PartialGeneration, State},
    progress_retention::{ReportExpiry, expire_after_terminal},
    runtime::{Error, ProgressHistoryRecord},
};
use durable_store::{Check, Entry, Expected, Mutation, Store};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const ENABLED: &[u8] = b"runtime/published-generation-retention/v1";
const PREFIX: &[u8] = b"runtime/generation-partial-payload/";
const EXPIRED: &[u8] = b"runtime/expired-generation-partial/";
const KEY_BYTES: usize = PREFIX.len() + 36 + 1 + 20;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Reference {
    pub request_id: Uuid,
    digest: [u8; 32],
}

fn invalid() -> Error {
    Error::Invalid("invalid published generation partial".into())
}

pub(crate) fn enable(store: &Store) -> Result<(), Error> {
    enable_at(store, ENABLED)
}

pub(crate) fn enabled(store: &Store) -> Result<bool, Error> {
    enabled_at(store, ENABLED)
}

pub(crate) fn unconfigured_check() -> Check {
    Check {
        key: ENABLED.to_vec(),
        expected: Expected::Missing,
    }
}

pub(crate) fn prepare(
    store: &Store,
    conversation: Uuid,
    request_id: Uuid,
    partial: &PartialGeneration,
) -> Result<Append, Error> {
    let payload = serde_json::to_vec(partial)?;
    if request_id.is_nil() || payload.len() > MAX_PROGRESS_BYTES {
        return Err(invalid());
    }
    let reference = Reference {
        request_id,
        digest: Sha256::digest(&payload).into(),
    };
    let frame = ProgressHistoryRecord::published_generation(conversation, reference);
    let mut append = Observer::progress_history(store, conversation)
        .prepare_append(&serde_json::to_value(frame)?)?;
    let key = payload_key(PREFIX, conversation, append.cursor.0);
    append.checks.push(Check {
        key: key.clone(),
        expected: Expected::Missing,
    });
    append.mutations.push(Mutation::Put {
        key,
        value: payload,
    });
    Ok(append)
}

pub(crate) fn read(
    store: &Store,
    conversation: Uuid,
    position: u64,
    commit: u64,
    reference: &Reference,
) -> Result<Option<PartialGeneration>, Error> {
    read_payload(
        store,
        &payload_key(PREFIX, conversation, position),
        &payload_key(EXPIRED, conversation, position),
        MAX_PROGRESS_BYTES,
        commit,
        &reference.digest,
    )?
    .map(|value| serde_json::from_slice(&value).map_err(Error::from))
    .transpose()
}

pub(crate) fn sweep(
    store: &Store,
    after: &mut Vec<u8>,
    now_ms: u64,
    retain_ms: u64,
) -> Result<(), Error> {
    let page =
        store.prefix_page_bounded(PREFIX, after, PAGE_ROWS, KEY_BYTES, MAX_PROGRESS_BYTES)?;
    let next = page
        .last()
        .filter(|_| page.len() == PAGE_ROWS)
        .map(|(key, _)| key.clone());
    for (key, payload) in page {
        if let Err(error) = expire_one(store, key, payload, now_ms, retain_ms)
            && !matches!(error, Error::Storage(durable_store::Error::Conflict { .. }))
        {
            return Err(error);
        }
    }
    *after = next.unwrap_or_default();
    Ok(())
}

fn expire_one(
    store: &Store,
    key: Vec<u8>,
    payload: Entry,
    now_ms: u64,
    retain_ms: u64,
) -> Result<(), Error> {
    let (conversation, position) = parse_payload_key(PREFIX, &key)?;
    let observer = Observer::progress_history(store, conversation);
    let frame_key = observer.record_key(Cursor(position));
    let frame = store.get_bounded(&frame_key, 4096)?.ok_or_else(invalid)?;
    let record: ProgressHistoryRecord = serde_json::from_slice(&frame.value)?;
    let reference = record.generation_reference(conversation)?;
    let original_commit = store
        .commit_for_version(&frame_key, frame.version)?
        .ok_or_else(invalid)?;
    if store.commit_for_version(&key, payload.version)? != Some(original_commit)
        || reference.digest != <[u8; 32]>::from(Sha256::digest(&payload.value))
    {
        return Err(invalid());
    }
    let Some((terminal_key, terminal)) =
        terminal_for_publication(store, reference.request_id, original_commit)?
    else {
        return Ok(());
    };
    let marker = payload_key(EXPIRED, conversation, position);
    expire_after_terminal(
        store,
        terminal_key,
        terminal.version,
        ReportExpiry {
            key,
            payload,
            checks: vec![
                Check {
                    key: frame_key,
                    expected: Expected::Version(frame.version),
                },
                Check {
                    key: marker.clone(),
                    expected: Expected::Missing,
                },
            ],
            mutations: vec![Mutation::Put {
                key: marker,
                value: reference.digest.to_vec(),
            }],
        },
        now_ms,
        retain_ms,
    )
}

pub(super) fn terminal_for_publication(
    store: &Store,
    request_id: Uuid,
    original_commit: u64,
) -> Result<Option<(Vec<u8>, Entry)>, Error> {
    let key = progress::key(request_id);
    let Some(terminal) = store.get_bounded(&key, MAX_PROGRESS_BYTES)? else {
        return Ok(None);
    };
    if matches!(
        serde_json::from_slice::<State>(&terminal.value)?,
        State::Running(_)
    ) || store
        .commit_for_version(&key, terminal.version)?
        .is_none_or(|commit| commit <= original_commit)
    {
        return Ok(None);
    }
    Ok(Some((key, terminal)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::published_progress_retention::inline::{self, Scope};
    use crate::{
        model::GenerationProgress,
        recovery::UncertainPolicy,
        runtime::{
            Config, Harness, Options, ProgressHistoryChange, Scheduler, prepare_progress_history,
        },
    };
    use pi_rs_core::{Model, ModelEvent, ModelRequest, ModelStream};
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    struct UnusedModel;

    #[async_trait::async_trait]
    impl Model for UnusedModel {
        async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<ModelStream> {
            panic!("expiry must not call the provider")
        }
    }

    fn options() -> Options {
        Options {
            model: Arc::new(UnusedModel),
            tools: Vec::new(),
            restart_limit: 2,
            max_turns: 1,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        }
    }

    struct ReplyModel;

    #[async_trait::async_trait]
    impl Model for ReplyModel {
        async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<ModelStream> {
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Text("complete reply".into())),
                Ok(ModelEvent::Complete {
                    calls: Vec::new(),
                    usage: None,
                }),
            ])))
        }
    }

    #[tokio::test]
    async fn scheduler_expires_a_real_model_partial_and_keeps_the_settled_reply() {
        verify_scheduler_retention(false).await;
        verify_scheduler_retention(true).await;
    }

    async fn verify_scheduler_retention(inline_history: bool) {
        let dir = tempfile::tempdir().unwrap();
        let mut scheduler = Scheduler::open(
            dir.path().join("db"),
            Options {
                model: Arc::new(ReplyModel),
                ..options()
            },
        )
        .unwrap();
        let retain_for = Duration::from_millis(10);
        let interval = Duration::from_millis(5);
        if inline_history {
            scheduler
                .start_inline_generation_retention(retain_for, interval)
                .unwrap();
        } else {
            scheduler
                .start_published_generation_retention(retain_for, interval)
                .unwrap();
        }
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: String::new(),
                max_output_tokens: 10,
            })
            .unwrap();
        let start = scheduler.progress_history_from_now(id).unwrap();
        scheduler.submit(id, None, "prompt".into()).unwrap();
        scheduler.wait(id).await.unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let page = scheduler
                    .progress_history(&start, NonZeroUsize::new(4).unwrap())
                    .unwrap();
                if let [partial, terminal] = page.records.as_slice()
                    && let [ProgressHistoryChange::ExpiredGeneration { request_id }] =
                        partial.changes.as_slice()
                {
                    assert!(matches!(&terminal.changes[0],
                        ProgressHistoryChange::Generation { request_id: completed, state }
                        if request_id == completed && matches!(state.as_ref(), State::Completed)
                    ));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            matches!(scheduler.conversation(id).unwrap().messages.last(),
                Some(pi_rs_core::Message::Assistant { content, .. }) if content == "complete reply"
            )
        );
        scheduler.shutdown().await.unwrap();
    }

    #[test]
    fn expires_only_opted_in_running_partials_after_terminal_observation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("durable.sqlite");
        let mut harness = Harness::open_paused(&path, options()).unwrap();
        let id = harness
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "retained context".into(),
                max_output_tokens: 5,
            })
            .unwrap();
        let start = harness.progress_history_from_now(id).unwrap();
        let merged_start = harness.merged_progress_from_now(id).unwrap();
        let request_id = Uuid::new_v4();
        let store = harness.owner.store();
        let publish = |store: &Store, state: State, mut checkpoint: progress::Prepared| {
            let append = prepare_progress_history(
                store,
                id,
                vec![ProgressHistoryChange::Generation {
                    request_id,
                    state: Box::new(state),
                }],
            )
            .unwrap();
            checkpoint.checks.extend(append.checks);
            checkpoint.mutations.extend(append.mutations);
            store
                .commit(&checkpoint.checks, &checkpoint.mutations)
                .unwrap();
        };
        let mut snapshot = progress::Snapshot::load(store, request_id).unwrap();
        snapshot
            .push(&GenerationProgress::Text("legacy".into()))
            .unwrap();
        publish(
            store,
            snapshot.state().clone(),
            snapshot.prepare_checkpoint().unwrap(),
        );
        let mut snapshot = progress::Snapshot::load(store, request_id).unwrap();
        snapshot
            .push(&GenerationProgress::Text(" running".into()))
            .unwrap();
        let raced = prepare_progress_history(
            store,
            id,
            vec![ProgressHistoryChange::Generation {
                request_id,
                state: Box::new(snapshot.state().clone()),
            }],
        )
        .unwrap();
        enable(store).unwrap();
        assert!(matches!(
            store.commit(&raced.checks, &raced.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        publish(
            store,
            snapshot.state().clone(),
            snapshot.prepare_checkpoint().unwrap(),
        );

        let partials = harness
            .progress_history(&start, NonZeroUsize::new(2).unwrap())
            .unwrap();
        let published_frame = &partials.records[1];
        let payload = payload_key(PREFIX, id, published_frame.cursor.position);
        let inline_key = Observer::progress_history(store, id)
            .record_key(Cursor(partials.records[0].cursor.position));
        let mut after_inline = Vec::new();
        let mut after = Vec::new();
        sweep(store, &mut after, 1_000, 20).unwrap();
        inline::sweep(store, &mut after_inline, Scope::Generation, 1_000, 20).unwrap();
        assert!(
            store
                .get(&crate::progress_retention::observation_key(&inline_key))
                .unwrap()
                .is_none()
        );
        assert!(store.get(&payload).unwrap().is_some());
        assert!(
            store
                .get(&crate::progress_retention::observation_key(&payload))
                .unwrap()
                .is_none()
        );
        let snapshot = progress::Snapshot::load(store, request_id).unwrap();
        let State::Running(partial) = snapshot.state() else {
            panic!("running snapshot")
        };
        publish(
            store,
            State::Aborted(partial.clone()),
            snapshot.prepare_abort().unwrap(),
        );
        sweep(store, &mut after, 2_000, 20).unwrap();

        let fork = harness.fork(id).unwrap();
        let capture = harness.unified_history_start(id, false).unwrap();
        drop(harness);
        let harness = Harness::open_paused(&path, options()).unwrap();
        let store = harness.owner.store();
        sweep(store, &mut after, 2_019, 20).unwrap();
        assert!(store.get(&payload).unwrap().is_some());
        sweep(store, &mut after, 2_020, 20).unwrap();
        assert!(store.get(&payload).unwrap().is_none());
        assert!(matches!(
            progress::Snapshot::load(store, request_id)
                .unwrap()
                .prepare_checkpoint(),
            Err(progress::Error::Finished)
        ));
        let history = harness
            .progress_history(&start, NonZeroUsize::new(3).unwrap())
            .unwrap();
        assert_eq!(history.records[0], partials.records[0]);
        let expired = &history.records[1];
        assert_eq!(expired.cursor, published_frame.cursor);
        assert_eq!(expired.commit_sequence, published_frame.commit_sequence);
        assert_eq!(
            expired.changes,
            [ProgressHistoryChange::ExpiredGeneration { request_id }]
        );
        assert!(matches!(&history.records[2].changes[0],
            ProgressHistoryChange::Generation { state, .. }
            if matches!(state.as_ref(), State::Aborted(partial) if partial.text == "legacy running")
        ));
        assert!(
            harness
                .merged_progress(&merged_start, NonZeroUsize::new(16).unwrap())
                .unwrap()
                .frames
                .iter()
                .any(|frame| frame.progress.as_ref() == Some(expired))
        );
        assert!(
            harness
                .unified_history(&capture, NonZeroUsize::new(16).unwrap())
                .unwrap()
                .commits
                .iter()
                .flat_map(|commit| &commit.progress)
                .any(|frame| frame == expired)
        );
        assert_eq!(
            harness.conversation(fork).unwrap().config.system_prompt,
            "retained context"
        );

        let first = &history.records[0];
        let original = store.get(&inline_key).unwrap().unwrap();
        let terminal = history.records[2].clone();
        inline::sweep(store, &mut after_inline, Scope::Generation, 3_000, 20).unwrap();
        drop(harness);
        let harness = Harness::open_paused(path, options()).unwrap();
        let store = harness.owner.store();
        inline::sweep(store, &mut after_inline, Scope::Generation, 3_019, 20).unwrap();
        assert_eq!(store.get(&inline_key).unwrap().unwrap(), original);
        inline::sweep(store, &mut after_inline, Scope::Generation, 3_020, 20).unwrap();
        let replacement = store.get(&inline_key).unwrap().unwrap();
        assert_eq!(replacement.version, original.version);
        assert!(replacement.value.len() < original.value.len());
        let terminal_entry = store.get(&progress::key(request_id)).unwrap().unwrap();
        assert!(matches!(
            store.observe_inline_generation_progress(
                &inline_key,
                &original,
                &terminal_entry,
                None,
                3_021
            ),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(
            store
                .get(&crate::progress_retention::observation_key(&inline_key))
                .unwrap()
                .is_none()
        );

        let history = harness
            .progress_history(&start, NonZeroUsize::new(3).unwrap())
            .unwrap();
        let retired = &history.records[0];
        assert_eq!(retired.cursor, first.cursor);
        assert_eq!(retired.commit_sequence, first.commit_sequence);
        assert_eq!(
            retired.changes,
            [ProgressHistoryChange::ExpiredGeneration { request_id }]
        );
        assert_eq!(history.records[2], terminal);
        assert!(
            harness
                .merged_progress(&merged_start, NonZeroUsize::new(16).unwrap())
                .unwrap()
                .frames
                .iter()
                .any(|frame| frame.progress.as_ref() == Some(retired))
        );
        assert!(
            harness
                .unified_history(&capture, NonZeroUsize::new(16).unwrap())
                .unwrap()
                .commits
                .iter()
                .flat_map(|commit| &commit.progress)
                .any(|frame| frame == retired)
        );
    }
}
