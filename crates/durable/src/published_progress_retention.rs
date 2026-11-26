//! Retention for the payloads of opted-in checkpointable progress frames.
//! Frames are immutable; expiring a payload leaves an authenticated marker at
//! its locator and preserves the frame's cursor and original commit sequence.
use durable_store::{Check, Entry, Expected, Mutation, Store};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    async_task_tool::{DurableToolProgress, MAX_CHECKPOINTABLE_PROGRESS_BYTES},
    observer::{Cursor, Observer},
    progress_retention::{ReportExpiry, expire_report},
    runtime::{Error, ProgressHistoryRecord},
};

const ENABLED: &[u8] = b"runtime/published-progress-retention/v1";
const PREFIX: &[u8] = b"runtime/progress-payload/";
const EXPIRED: &[u8] = b"runtime/expired-progress-payload/";
const KEY_BYTES: usize = PREFIX.len() + 36 + 1 + 20;
const PAGE_ROWS: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Reference {
    pub task: Uuid,
    pub attempt: Uuid,
    digest: [u8; 32],
}

fn invalid() -> Error {
    Error::Invalid("invalid published checkpointable progress payload".into())
}

pub(crate) fn enabled(store: &Store) -> Result<bool, Error> {
    match store.get_bounded(ENABLED, 1)? {
        None => Ok(false),
        Some(entry) if entry.value == b"1" => Ok(true),
        Some(_) => Err(invalid()),
    }
}

pub(crate) fn enable(store: &Store) -> Result<(), Error> {
    if !enabled(store)? {
        match store.commit(
            &[Check {
                key: ENABLED.to_vec(),
                expected: Expected::Missing,
            }],
            &[Mutation::Put {
                key: ENABLED.to_vec(),
                value: b"1".to_vec(),
            }],
        ) {
            Ok(()) => (),
            Err(durable_store::Error::Conflict { .. }) if enabled(store)? => (),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

pub(crate) fn unconfigured_check() -> Check {
    Check {
        key: ENABLED.to_vec(),
        expected: Expected::Missing,
    }
}

impl Reference {
    pub(crate) fn new(task: Uuid, attempt: Uuid, payload: &[u8]) -> Result<Self, Error> {
        if task.is_nil() || attempt.is_nil() || payload.len() > MAX_CHECKPOINTABLE_PROGRESS_BYTES {
            return Err(invalid());
        }
        Ok(Self {
            task,
            attempt,
            digest: Sha256::digest(payload).into(),
        })
    }

    fn matches(&self, value: &[u8]) -> bool {
        let digest: [u8; 32] = Sha256::digest(value).into();
        digest == self.digest
    }
}

fn expired_key(payload: &[u8]) -> Vec<u8> {
    [EXPIRED, &payload[PREFIX.len()..]].concat()
}

pub(crate) fn key(conversation: Uuid, position: u64) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.extend_from_slice(format!("{conversation}/{position:020}").as_bytes());
    key
}

fn parse_key(value: &[u8]) -> Result<(Uuid, u64), Error> {
    let (conversation, position) = value
        .strip_prefix(PREFIX)
        .and_then(|suffix| std::str::from_utf8(suffix).ok())
        .and_then(|suffix| suffix.split_once('/'))
        .ok_or_else(invalid)?;
    let conversation = Uuid::parse_str(conversation).map_err(|_| invalid())?;
    let position = position.parse().map_err(|_| invalid())?;
    if conversation.is_nil() || position == 0 || key(conversation, position) != value {
        return Err(invalid());
    }
    Ok((conversation, position))
}

/// Expiry atomically removes the payload and writes its digest marker.
pub(crate) fn read(
    store: &Store,
    conversation: Uuid,
    position: u64,
    commit: u64,
    reference: &Reference,
) -> Result<Option<DurableToolProgress>, Error> {
    let key = key(conversation, position);
    if let Some(payload) = store.get_bounded(&key, MAX_CHECKPOINTABLE_PROGRESS_BYTES)? {
        if store.commit_for_version(&key, payload.version)? != Some(commit)
            || !reference.matches(&payload.value)
        {
            return Err(invalid());
        }
        return Ok(Some(serde_json::from_slice(&payload.value)?));
    }
    let marker = expired_key(&key);
    let entry = store.get_bounded(&marker, 32)?.ok_or_else(invalid)?;
    if entry.value != reference.digest
        || store
            .commit_for_version(&marker, entry.version)?
            .is_none_or(|sequence| sequence <= commit)
    {
        return Err(invalid());
    }
    Ok(None)
}

pub(crate) fn sweep(
    store: &Store,
    after: &mut Vec<u8>,
    now_ms: u64,
    retain_ms: u64,
) -> Result<(), Error> {
    let page = store.prefix_page_bounded(
        PREFIX,
        after,
        PAGE_ROWS,
        KEY_BYTES,
        MAX_CHECKPOINTABLE_PROGRESS_BYTES,
    )?;
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
    let (conversation, position) = parse_key(&key)?;
    let frame_key = Observer::progress_history(store, conversation).record_key(Cursor(position));
    let frame = store.get_bounded(&frame_key, 4096)?.ok_or_else(invalid)?;
    let record: ProgressHistoryRecord = serde_json::from_slice(&frame.value)?;
    let reference = record.checkpointable_reference(conversation)?;
    let original_commit = store
        .commit_for_version(&frame_key, frame.version)?
        .ok_or_else(invalid)?;
    if store.commit_for_version(&key, payload.version)? != Some(original_commit)
        || !reference.matches(&payload.value)
    {
        return Err(invalid());
    }
    let marker = expired_key(&key);
    expire_report(
        store,
        reference.task,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        recovery::UncertainPolicy,
        runtime::{
            Config, Harness, Options, ProgressHistoryChange,
            prepare_checkpointable_progress_history,
        },
        task::{Checkpoint, Task, TaskOwner},
        task_store::{Save, TaskStore},
    };
    use pi_rs_core::{Model, ModelRequest, ModelStream};
    use std::{num::NonZeroUsize, sync::Arc};

    struct UnusedModel;

    #[async_trait::async_trait]
    impl Model for UnusedModel {
        async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<ModelStream> {
            panic!("retention must not run the model")
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

    #[test]
    fn published_progress_expires_across_reopen_without_breaking_cursors_or_forks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("durable.sqlite");
        let mut harness = Harness::open_paused(&path, options()).unwrap();
        let conversation = harness
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "retained context".into(),
                max_output_tokens: 5,
            })
            .unwrap();
        let store = harness.owner.store();
        let task = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(conversation),
            Checkpoint {
                phase: "initial".into(),
                data: serde_json::Value::Null,
            },
            2,
        )
        .unwrap()
        .into_task_tool();
        let task_id = task.id();
        TaskStore::new(store).save(&[Save::Create(&task)]).unwrap();
        let attempt = Uuid::new_v4();
        let report = |text: &str| DurableToolProgress {
            text: text.into(),
            ..Default::default()
        };
        let commit = |harness: &Harness, progress: &DurableToolProgress| {
            let store = harness.owner.store();
            let batch = prepare_checkpointable_progress_history(
                store,
                conversation,
                task_id,
                attempt,
                progress,
            )
            .unwrap();
            store.commit(&batch.checks, &batch.mutations).unwrap();
        };

        let start = harness.progress_history_from_now(conversation).unwrap();
        commit(&harness, &report("legacy progress"));
        let after_legacy = harness.progress_history_from_now(conversation).unwrap();
        let legacy_racer = prepare_checkpointable_progress_history(
            store,
            conversation,
            task_id,
            attempt,
            &report("must retry"),
        )
        .unwrap();

        enable(store).unwrap();
        assert!(matches!(
            store.commit(&legacy_racer.checks, &legacy_racer.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        let merged_start = harness.merged_progress_from_now(conversation).unwrap();
        let scratch = report(&"payload ".repeat(2048));
        commit(&harness, &scratch);
        let live = harness
            .progress_history(&after_legacy, NonZeroUsize::MIN)
            .unwrap();
        let publication = &live.records[0];
        let position = publication.cursor.position;
        let publication_commit = publication.commit_sequence;
        assert!(
            matches!(&publication.changes[0], ProgressHistoryChange::CheckpointableTool { progress, .. } if progress.as_ref() == &scratch)
        );

        let store = harness.owner.store();
        let mut after = Vec::new();
        sweep(store, &mut after, 1_000, 20).unwrap();
        let payload = key(conversation, position);
        assert!(store.get(&payload).unwrap().unwrap().value.len() > 16_000);
        let tasks = TaskStore::new(store);
        let current = tasks.load(task_id).unwrap().unwrap();
        tasks
            .update(&current, |task| {
                task.start()?;
                task.succeed(serde_json::json!("retained outcome"))
            })
            .unwrap();
        sweep(store, &mut after, 2_000, 20).unwrap();

        let fork = harness.fork(conversation).unwrap();
        let archived = harness.unified_history_start(conversation, false).unwrap();
        drop(harness);
        let harness = Harness::open_paused(path, options()).unwrap();
        let store = harness.owner.store();
        sweep(store, &mut after, 2_019, 20).unwrap();
        assert!(store.get(&payload).unwrap().unwrap().value.len() > 16_000);
        sweep(store, &mut after, 2_020, 20).unwrap();
        assert!(store.get(&payload).unwrap().is_none());
        let marker = expired_key(&payload);
        assert_eq!(store.get(&marker).unwrap().unwrap().value.len(), 32);

        let restored = harness
            .progress_history(&start, NonZeroUsize::new(2).unwrap())
            .unwrap();
        assert_eq!(restored.next, live.next);
        assert_eq!(restored.records[1].commit_sequence, publication_commit);
        assert!(
            matches!(&restored.records[0].changes[0], ProgressHistoryChange::CheckpointableTool { progress, .. } if progress.text == "legacy progress")
        );
        assert_eq!(
            restored.records[1].changes,
            [ProgressHistoryChange::ExpiredCheckpointableTool {
                task: task_id,
                attempt
            }]
        );
        let merged = harness
            .merged_progress(&merged_start, NonZeroUsize::new(16).unwrap())
            .unwrap();
        assert!(
            merged
                .frames
                .iter()
                .filter_map(|frame| frame.progress.as_ref())
                .any(|frame| frame == &restored.records[1])
        );
        let unified = harness
            .unified_history(&archived, NonZeroUsize::new(16).unwrap())
            .unwrap();
        assert!(
            unified
                .commits
                .iter()
                .flat_map(|commit| &commit.progress)
                .any(|frame| frame == &restored.records[1])
        );
        assert_eq!(
            harness.conversation(fork).unwrap().config.system_prompt,
            "retained context"
        );
        assert!(
            TaskStore::new(store)
                .load(task_id)
                .unwrap()
                .unwrap()
                .task
                .is_terminal()
        );

        let expired = store.get(&marker).unwrap().unwrap();
        store
            .commit(
                &[Check {
                    key: marker.clone(),
                    expected: Expected::Version(expired.version),
                }],
                &[Mutation::Delete { key: marker }],
            )
            .unwrap();
        assert!(
            harness
                .progress_history(&after_legacy, NonZeroUsize::MIN)
                .is_err(),
            "missing payload without a committed marker is corruption"
        );
    }
}
