//! Expiry for attempt-local tool checkpoints. Published history is not eligible.
use durable_store::{Check, Entry, Expected, Mutation, Store};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    async_task_tool::{
        CHECKPOINTABLE_PROGRESS_PREFIX, MAX_CHECKPOINTABLE_PROGRESS_BYTES, durable_progress_key,
    },
    runtime::Error,
    task_store::{TaskStore, key as task_key},
};

const PAGE_ROWS: usize = 16;
const MARKER_BYTES: usize = 256;

#[derive(Deserialize, Serialize)]
pub(crate) struct TerminalObservation {
    pub task_version: u64,
    pub progress_version: u64,
    pub observed_ms: u64,
}

fn parse_progress_key(key: &[u8]) -> Result<(Uuid, Uuid), Error> {
    let failure = || Error::Invalid("invalid checkpointable progress key".into());
    let (task, attempt) = key
        .strip_prefix(CHECKPOINTABLE_PROGRESS_PREFIX.as_bytes())
        .and_then(|suffix| std::str::from_utf8(suffix).ok())
        .and_then(|suffix| suffix.split_once('/'))
        .ok_or_else(failure)?;
    let task = Uuid::parse_str(task).map_err(|_| failure())?;
    let attempt = Uuid::parse_str(attempt).map_err(|_| failure())?;
    if durable_progress_key(task, attempt) != key {
        return Err(failure());
    }
    Ok((task, attempt))
}

pub(crate) fn observation_key(progress_key: &[u8]) -> Vec<u8> {
    [b"runtime/retention/".as_slice(), progress_key].concat()
}

pub(crate) fn sweep(
    store: &Store,
    after: &mut Vec<u8>,
    now_ms: u64,
    retain_ms: u64,
) -> Result<(), Error> {
    let prefix = CHECKPOINTABLE_PROGRESS_PREFIX.as_bytes();
    let page = store.prefix_page_bounded(
        prefix,
        after,
        PAGE_ROWS,
        prefix.len() + 73,
        MAX_CHECKPOINTABLE_PROGRESS_BYTES,
    )?;
    let next = page
        .last()
        .filter(|_| page.len() == PAGE_ROWS)
        .map(|(key, _)| key.clone());
    for (key, progress) in page {
        if let Err(error) = expire_one(store, key, progress, now_ms, retain_ms)
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
    progress: Entry,
    now_ms: u64,
    retain_ms: u64,
) -> Result<(), Error> {
    let (id, _) = parse_progress_key(&key)?;
    expire_report(
        store,
        id,
        ReportExpiry {
            key,
            payload: progress,
            checks: Vec::new(),
            mutations: Vec::new(),
        },
        now_ms,
        retain_ms,
    )
}

pub(crate) struct ReportExpiry {
    pub key: Vec<u8>,
    pub payload: Entry,
    pub checks: Vec<Check>,
    pub mutations: Vec<Mutation>,
}

pub(crate) fn expire_report(
    store: &Store,
    id: Uuid,
    expiry: ReportExpiry,
    now_ms: u64,
    retain_ms: u64,
) -> Result<(), Error> {
    let Some(task) = TaskStore::new(store)
        .load(id)
        .map_err(crate::task_scheduler::Error::from)?
        .filter(|task| task.task.is_task_tool() && task.task.is_terminal())
    else {
        return Ok(());
    };
    expire_after_terminal(store, task_key(id), task.version, expiry, now_ms, retain_ms)
}

pub(crate) fn expire_after_terminal(
    store: &Store,
    terminal_key: Vec<u8>,
    terminal_version: u64,
    expiry: ReportExpiry,
    now_ms: u64,
    retain_ms: u64,
) -> Result<(), Error> {
    let ReportExpiry {
        key,
        payload: progress,
        mut checks,
        mut mutations,
    } = expiry;
    let marker = observation_key(&key);
    let prior = store.get_bounded(&marker, MARKER_BYTES)?;
    let observation = prior
        .as_ref()
        .map(|entry| serde_json::from_slice::<TerminalObservation>(&entry.value))
        .transpose()?;
    checks.extend([
        Check {
            key: terminal_key,
            expected: Expected::Version(terminal_version),
        },
        Check {
            key: key.clone(),
            expected: Expected::Version(progress.version),
        },
        Check {
            key: marker.clone(),
            expected: prior
                .as_ref()
                .map_or(Expected::Missing, |e| Expected::Version(e.version)),
        },
    ]);
    if let Some(observation) = observation.filter(|entry| {
        entry.task_version == terminal_version && entry.progress_version == progress.version
    }) {
        if now_ms.saturating_sub(observation.observed_ms) >= retain_ms {
            mutations.push(Mutation::Delete { key });
            mutations.push(Mutation::Delete { key: marker });
            store.commit(&checks, &mutations)?;
        }
    } else {
        let value = serde_json::to_vec(&TerminalObservation {
            task_version: terminal_version,
            progress_version: progress.version,
            observed_ms: now_ms,
        })?;
        store.commit(&checks, &[Mutation::Put { key: marker, value }])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{Checkpoint, Task, TaskOwner};

    #[test]
    fn expiry_is_persistent_and_fenced_to_the_observed_versions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("durable.db");
        let id = Uuid::new_v4();
        let attempt = Uuid::new_v4();
        let progress = durable_progress_key(id, attempt);
        let publication = b"durable/progress-history/published".to_vec();
        let mut task = Task::new(
            id,
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "fixture".into(),
                data: serde_json::Value::Null,
            },
            3,
        )
        .unwrap()
        .into_task_tool();
        let task_key = task_key(id);
        let store = Store::open(&path).unwrap();
        let put = |store: &Store, key: &[u8], value: Vec<u8>| {
            store
                .commit(
                    &[],
                    &[Mutation::Put {
                        key: key.to_vec(),
                        value,
                    }],
                )
                .unwrap();
        };
        put(&store, &task_key, serde_json::to_vec(&task).unwrap());
        put(&store, &progress, b"running output".to_vec());
        put(&store, &publication, b"published output".to_vec());
        let mut after = Vec::new();
        sweep(&store, &mut after, 0, 10).unwrap();
        assert!(store.get(&observation_key(&progress)).unwrap().is_none());
        task.start().unwrap();
        task.succeed(serde_json::Value::Null).unwrap();
        put(&store, &task_key, serde_json::to_vec(&task).unwrap());
        sweep(&store, &mut after, 100, 10).unwrap();
        drop(store);

        let store = Store::open(&path).unwrap();
        sweep(&store, &mut after, 109, 10).unwrap();
        assert!(store.get(&progress).unwrap().is_some());
        put(&store, &progress, b"newer output".to_vec());
        sweep(&store, &mut after, 110, 10).unwrap();
        assert!(store.get(&progress).unwrap().is_some());
        sweep(&store, &mut after, 120, 10).unwrap();
        assert!(store.get(&progress).unwrap().is_none());
        assert!(store.get(&observation_key(&progress)).unwrap().is_none());
        assert!(store.get(&task_key).unwrap().is_some());
        assert_eq!(
            store.get(&publication).unwrap().unwrap().value,
            b"published output"
        );
    }
}
