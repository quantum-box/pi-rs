//! Pull-based delivery of committed extension report batches.

use crate::{
    observer::{Cursor, Observer},
    runtime::{Error, ExtensionReportBatch},
};
use durable_store::{Check, Expected, Mutation, Store, owner::OwnedStore};
use futures_util::FutureExt;
use std::{
    future::Future,
    num::NonZeroUsize,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use uuid::Uuid;

const REPORTS: &[u8] = b"runtime/extension-report/";
const MARKERS: &[u8] = b"runtime/extension-report-index/";
// Reserved exclusively for this log, including future versions.
const STREAM: Uuid = Uuid::from_u128(0x8765bb7ab37448fd8e9226e84f27d8b1);
const MAX_PAGE: usize = 256;

fn marker(key: &[u8]) -> Vec<u8> {
    let mut result = MARKERS.to_vec();
    result.extend_from_slice(&key[REPORTS.len()..]);
    result
}

/// Index a legacy record or atomically commit and index a new one.
/// A report identity is immutable; the first committed batch wins.
pub(crate) fn index(
    store: &Store,
    key: Vec<u8>,
    batch: &ExtensionReportBatch,
    legacy: bool,
) -> Result<(), Error> {
    let mark = marker(&key);
    loop {
        if store.get(&mark)?.is_some() {
            return Ok(());
        }
        let existing = store.get(&key)?;
        if !legacy && let Some(entry) = &existing {
            // Old databases may contain this identity without an index.
            let old: ExtensionReportBatch = serde_json::from_slice(&entry.value)?;
            return index(store, key, &old, true);
        }
        // Migration must append the exact version guarded below, not the value
        // from an earlier page (or from a previous failed attempt).
        let indexed = if legacy {
            let entry = existing
                .as_ref()
                .ok_or_else(|| Error::Invalid("report disappeared during indexing".into()))?;
            serde_json::from_slice::<ExtensionReportBatch>(&entry.value)?
        } else {
            batch.clone()
        };
        let value = serde_json::to_value(&indexed)?;
        let mut append = Observer::new(store, STREAM).prepare_append(&value)?;
        append.checks.push(Check {
            key: mark.clone(),
            expected: Expected::Missing,
        });
        append.mutations.push(Mutation::Put {
            key: mark.clone(),
            value: vec![],
        });
        if legacy {
            let Some(entry) = existing else {
                return Err(Error::Invalid("report disappeared during indexing".into()));
            };
            append.checks.push(Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            });
        } else {
            append.checks.push(Check {
                key: key.clone(),
                expected: Expected::Missing,
            });
            append.mutations.push(Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(batch)?,
            });
        }
        match store.commit(&append.checks, &append.mutations) {
            Ok(()) => return Ok(()),
            Err(durable_store::Error::Conflict { .. }) => {
                // An occupied next record with an unchanged head is an orphan,
                // not contention. Retrying forever would hang the writer.
                let head = &append.checks[0];
                let record = &append.checks[1];
                if store.get(&record.key)?.is_some() {
                    let current = store.get(&head.key)?;
                    let unchanged = match (&head.expected, current) {
                        (Expected::Missing, None) => true,
                        (Expected::Version(version), Some(entry)) => *version == entry.version,
                        _ => false,
                    };
                    if unchanged {
                        return Err(crate::observer::Error::Corrupt.into());
                    }
                }
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// A batch and its exclusive, database-local processing cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    pub cursor: u64,
    pub batch: ExtensionReportBatch,
}

/// A pull subscription; no task, queue, or callback runs on the writer's path.
/// Drop it to stop waiting. Persist a cursor only after processing its batch.
pub struct Subscription<'a> {
    store: &'a Store,
}
impl<'a> Subscription<'a> {
    pub(crate) fn open(store: &'a Store) -> Result<Self, Error> {
        // Upgrade records from before the index existed, in bounded pages.
        // Concurrent new writers index atomically, including keys before `after`.
        let mut after = REPORTS.to_vec();
        loop {
            let (_, entries) = store.page_with_metadata(b"", REPORTS, &after, MAX_PAGE)?;
            if entries.is_empty() {
                break;
            }
            for (key, entry) in entries {
                after = key.clone();
                let batch = serde_json::from_slice(&entry.value)?;
                index(store, key, &batch, true)?;
            }
        }
        Ok(Self { store })
    }
    /// Read at most `limit` (1..=256) committed batches after `cursor`.
    pub fn read(&self, cursor: u64, limit: usize) -> Result<Vec<Delivery>, Error> {
        if !(1..=MAX_PAGE).contains(&limit) {
            return Err(Error::Invalid("report page limit must be 1..=256".into()));
        }
        let page = Observer::new(self.store, STREAM)
            .read(Cursor(cursor), NonZeroUsize::new(limit).unwrap())?;
        page.records
            .into_iter()
            .map(|r| {
                Ok(Delivery {
                    cursor: r.cursor.0,
                    batch: serde_json::from_value(r.value)?,
                })
            })
            .collect()
    }
    /// Wait until a page exists; dropping the future cancels the wait.
    pub async fn next(
        &self,
        cursor: u64,
        limit: usize,
        poll: Duration,
    ) -> Result<Vec<Delivery>, Error> {
        if poll.is_zero() {
            return Err(Error::Invalid(
                "report poll interval must be nonzero".into(),
            ));
        }
        loop {
            let page = self.read(cursor, limit)?;
            if !page.is_empty() {
                return Ok(page);
            }
            tokio::time::sleep(poll).await;
        }
    }
}

/// Terminal delivery failure. `cursor` is the last successfully processed batch.
#[derive(Clone, Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("report read failed after cursor {cursor}: {source}")]
    Read { cursor: u64, source: Arc<Error> },
    #[error("report listener failed after cursor {cursor}: {message}")]
    Listener { cursor: u64, message: String },
    #[error("report listener panicked after cursor {cursor}")]
    Panic { cursor: u64 },
    #[error("report worker failed after cursor {cursor}: {message}")]
    Worker { cursor: u64, message: String },
}

/// Host-owned worker. Call `close` and await it before shutting down the host.
/// Dropping without closing aborts the task; this is not an awaited shutdown.
#[must_use]
pub struct Dispatcher {
    stop: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<u64, DispatchError>>,
    cursor: Arc<AtomicU64>,
    result: Option<Result<u64, DispatchError>>,
}

impl Dispatcher {
    pub(crate) fn start<F, Fut>(
        owner: Arc<OwnedStore>,
        cursor: u64,
        poll: Duration,
        mut listener: F,
    ) -> Result<Self, Error>
    where
        F: FnMut(Delivery) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        if poll.is_zero() {
            return Err(Error::Invalid(
                "report poll interval must be nonzero".into(),
            ));
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Invalid("report dispatcher requires a Tokio runtime".into()))?;
        // Setup and cursor validation happen before the worker can call user code.
        let sub = Subscription::open(owner.store())?;
        sub.read(cursor, 1)?;
        let (stop, mut stopped) = tokio::sync::watch::channel(false);
        let acknowledged = Arc::new(AtomicU64::new(cursor));
        let progress = acknowledged.clone();
        let task = runtime.spawn(async move {
            let sub = Subscription {
                store: owner.store(),
            };
            let mut cursor = cursor;
            loop {
                let page = tokio::select! {
                    biased;
                    _ = stopped.changed() => return Ok(cursor),
                    result = sub.next(cursor, 1, poll) => result.map_err(|source| DispatchError::Read { cursor, source: Arc::new(source) })?,
                };
                if *stopped.borrow() {
                    return Ok(cursor);
                }
                let delivery = page
                    .into_iter()
                    .next()
                    .expect("next returns a nonempty page");
                let next = delivery.cursor;
                // Catch both construction and polling panics. No store or runtime lock
                // is held here; listener completion is the acknowledgment boundary.
                match AssertUnwindSafe(async { listener(delivery).await })
                    .catch_unwind()
                    .await
                {
                    Ok(Ok(())) => {
                        cursor = next;
                        progress.store(cursor, Ordering::Release);
                    }
                    Ok(Err(message)) => return Err(DispatchError::Listener { cursor, message }),
                    Err(_) => return Err(DispatchError::Panic { cursor }),
                }
            }
        });
        Ok(Self {
            stop,
            task,
            cursor: acknowledged,
            result: None,
        })
    }

    /// Request stop and wait for any callback already selected for delivery. Such a
    /// callback may start after the stop request. Return the last acknowledged cursor
    /// or terminal failure. Safe to cancel and await again;
    /// subsequent calls return the same result.
    pub async fn close(&mut self) -> Result<u64, DispatchError> {
        if let Some(result) = &self.result {
            return result.clone();
        }
        let _ = self.stop.send(true);
        let result = match (&mut self.task).await {
            Ok(result) => result,
            Err(error) => Err(DispatchError::Worker {
                cursor: self.cursor.load(Ordering::Acquire),
                message: error.to_string(),
            }),
        };
        self.result = Some(result.clone());
        result
    }
}
impl Drop for Dispatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    fn batch(n: usize) -> ExtensionReportBatch {
        ExtensionReportBatch {
            conversation_id: Uuid::new_v4(),
            operation_id: Uuid::new_v4(),
            phase: format!("phase-{n}"),
            reports: vec![crate::extension::Report {
                extension: "test".into(),
                operation: "hook".into(),
                error: format!("error-{n}"),
            }],
        }
    }
    fn key(b: &ExtensionReportBatch) -> Vec<u8> {
        format!(
            "runtime/extension-report/{}/{}/{}",
            b.conversation_id, b.operation_id, b.phase
        )
        .into_bytes()
    }
    #[cfg(unix)]
    #[test]
    fn dispatcher_without_runtime_returns_error_and_releases_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reports.db");
        let owner = Arc::new(OwnedStore::open(&path).unwrap());
        let result = Dispatcher::start(owner, 0, Duration::from_secs(1), |_| async { Ok(()) });
        assert!(
            matches!(result, Err(Error::Invalid(message)) if message.contains("Tokio runtime"))
        );
        OwnedStore::open(&path).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dispatcher_close_is_repeatable_and_releases_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reports.db");
        let owner = Arc::new(OwnedStore::open(&path).unwrap());
        let b = batch(1);
        index(owner.store(), key(&b), &b, false).unwrap();
        let (started, received) = tokio::sync::oneshot::channel();
        let mut started = Some(started);
        let mut dispatcher = Dispatcher::start(owner, 0, Duration::from_secs(1), move |_| {
            started.take().unwrap().send(()).unwrap();
            async { Err("failure".into()) }
        })
        .unwrap();
        received.await.unwrap();
        for _ in 0..2 {
            assert!(matches!(dispatcher.close().await,
                Err(DispatchError::Listener { cursor: 0, ref message }) if message == "failure"));
        }
        // The handle may stay alive after close without retaining the store owner.
        let owner = Arc::new(OwnedStore::open(&path).unwrap());
        let (sent, received) = tokio::sync::oneshot::channel();
        let mut sent = Some(sent);
        let mut replay = Dispatcher::start(owner, 0, Duration::from_secs(1), move |_| {
            sent.take().unwrap().send(()).unwrap();
            async { Ok(()) }
        })
        .unwrap();
        received.await.unwrap();
        assert_eq!(replay.close().await.unwrap(), 1);
        assert_eq!(replay.close().await.unwrap(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dispatcher_reentrant_failure_and_restart_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reports.db");
        let owner = Arc::new(OwnedStore::open(&path).unwrap());
        let first = batch(1);
        index(owner.store(), key(&first), &first, false).unwrap();
        let callback_owner = owner.clone();
        let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
        let mut dispatcher = Dispatcher::start(
            owner.clone(),
            0,
            Duration::from_millis(1),
            move |delivery| {
                let owner = callback_owner.clone();
                let sent = sent.clone();
                async move {
                    let extra = batch(2);
                    index(owner.store(), key(&extra), &extra, false).unwrap();
                    sent.send(delivery.cursor).unwrap();
                    Err("retry me".to_string())
                }
            },
        )
        .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), received.recv())
                .await
                .unwrap(),
            Some(1)
        );
        assert!(matches!(
            dispatcher.close().await,
            Err(DispatchError::Listener { cursor: 0, .. })
        ));
        drop(dispatcher);
        drop(owner);
        let owner = Arc::new(OwnedStore::open(&path).unwrap());
        let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
        let mut dispatcher = Dispatcher::start(
            owner.clone(),
            0,
            Duration::from_millis(1),
            move |delivery| {
                let sent = sent.clone();
                async move {
                    sent.send(delivery.cursor).unwrap();
                    Ok(())
                }
            },
        )
        .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), received.recv())
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), received.recv())
                .await
                .unwrap(),
            Some(2)
        );
        assert_eq!(dispatcher.close().await.unwrap(), 2);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dispatcher_close_waits_for_callback_and_leaves_later_reports() {
        let dir = tempfile::tempdir().unwrap();
        let owner = Arc::new(OwnedStore::open(dir.path().join("reports.db")).unwrap());
        let b = batch(1);
        index(owner.store(), key(&b), &b, false).unwrap();
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        let mut started = Some(started);
        let mut release_rx = Some(release_rx);
        let mut dispatcher =
            Dispatcher::start(owner.clone(), 0, Duration::from_millis(1), move |_| {
                let started = started.take().unwrap();
                let release_rx = release_rx.take().unwrap();
                async move {
                    started.send(()).unwrap();
                    release_rx.await.unwrap();
                    Ok(())
                }
            })
            .unwrap();
        started_rx.await.unwrap();
        let later = batch(2);
        index(owner.store(), key(&later), &later, false).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), dispatcher.close())
                .await
                .is_err()
        );
        release.send(()).unwrap();
        assert_eq!(dispatcher.close().await.unwrap(), 1);
        assert_eq!(
            Subscription::open(owner.store())
                .unwrap()
                .read(1, 1)
                .unwrap()[0]
                .batch,
            later
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dispatcher_checkpoint_resume_and_idle_close() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reports.db");
        let checkpoint = dir.path().join("cursor");
        let owner = Arc::new(OwnedStore::open(&path).unwrap());
        let first = batch(1);
        index(owner.store(), key(&first), &first, false).unwrap();
        let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
        let checkpoint_path = checkpoint.clone();
        let mut dispatcher = Dispatcher::start(
            owner.clone(),
            0,
            Duration::from_millis(1),
            move |delivery| {
                let sent = sent.clone();
                let checkpoint_path = checkpoint_path.clone();
                async move {
                    std::fs::write(checkpoint_path, delivery.cursor.to_string()).unwrap();
                    sent.send(delivery.cursor).unwrap();
                    Ok(())
                }
            },
        )
        .unwrap();
        assert_eq!(received.recv().await, Some(1));
        assert_eq!(dispatcher.close().await.unwrap(), 1);
        drop(dispatcher);
        drop(owner);
        let cursor = std::fs::read_to_string(checkpoint)
            .unwrap()
            .parse()
            .unwrap();
        let owner = Arc::new(OwnedStore::open(&path).unwrap());
        let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
        let mut dispatcher = Dispatcher::start(
            owner.clone(),
            cursor,
            Duration::from_millis(1),
            move |delivery| {
                let sent = sent.clone();
                async move {
                    sent.send(delivery.cursor).unwrap();
                    Ok(())
                }
            },
        )
        .unwrap();
        let second = batch(2);
        index(owner.store(), key(&second), &second, false).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), received.recv())
                .await
                .unwrap(),
            Some(2)
        );
        assert_eq!(dispatcher.close().await.unwrap(), 2);
        let third = batch(3);
        index(owner.store(), key(&third), &third, false).unwrap();
        assert!(received.recv().await.is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dispatcher_panic_preserves_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let owner = Arc::new(OwnedStore::open(dir.path().join("reports.db")).unwrap());
        let b = batch(1);
        index(owner.store(), key(&b), &b, false).unwrap();
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let mut started = Some(started);
        let mut dispatcher = Dispatcher::start(owner, 0, Duration::from_millis(1), move |_| {
            started.take().unwrap().send(()).unwrap();
            async move {
                panic!("listener panic");
                #[allow(unreachable_code)]
                Ok(())
            }
        })
        .unwrap();
        started_rx.await.unwrap();
        assert!(matches!(
            dispatcher.close().await,
            Err(DispatchError::Panic { cursor: 0 })
        ));
    }

    #[test]
    fn concurrent_commits_reentrancy_and_failed_consumer() {
        let store = Arc::new(Store::memory().unwrap());
        let barrier = Arc::new(Barrier::new(9));
        let mut threads = vec![];
        for n in 0..8 {
            let (store, barrier) = (store.clone(), barrier.clone());
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                let b = batch(n);
                index(&store, key(&b), &b, false).unwrap();
            }));
        }
        barrier.wait();
        for t in threads {
            t.join().unwrap();
        }
        let sub = Subscription::open(&store).unwrap();
        let first = sub.read(0, 1).unwrap();
        // Simulate a listener error: do not save the cursor; reconnect replays it.
        assert_eq!(sub.read(0, 1).unwrap(), first);
        // A consumer can synchronously cause another report without a callback lock.
        let extra = batch(9);
        index(&store, key(&extra), &extra, false).unwrap();
        let mut cursor = 0;
        let mut phases = std::collections::HashSet::new();
        loop {
            let page = sub.read(cursor, 2).unwrap();
            if page.is_empty() {
                break;
            }
            for delivery in page {
                cursor = delivery.cursor;
                assert!(phases.insert(delivery.batch.phase));
            }
        }
        assert_eq!(phases.len(), 9);
        assert!(sub.read(cursor + 1, 1).is_err());
        assert!(sub.read(cursor, 257).is_err());
    }
    #[test]
    fn concurrent_duplicate_writers_commit_one_identical_delivery() {
        let store = Arc::new(Store::memory().unwrap());
        let barrier = Arc::new(Barrier::new(3));
        let first = batch(1);
        let mut second = first.clone();
        second.reports[0].error = "second".into();
        let handles: Vec<_> = [first.clone(), second.clone()]
            .into_iter()
            .map(|b| {
                let store = store.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    index(&store, key(&b), &b, false).unwrap();
                })
            })
            .collect();
        barrier.wait();
        for handle in handles {
            handle.join().unwrap();
        }
        let stored: ExtensionReportBatch =
            serde_json::from_slice(&store.get(&key(&first)).unwrap().unwrap().value).unwrap();
        assert!(stored == first || stored == second);
        let sub = Subscription::open(&store).unwrap();
        let page = sub.read(0, 10).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].batch, stored);
        assert_eq!(page[0].cursor, 1);
    }

    #[test]
    fn migration_uses_current_checked_value_and_duplicate_keeps_first() {
        let store = Store::memory().unwrap();
        let old = batch(1);
        let mut replacement = old.clone();
        replacement.reports[0].error = "replacement".into();
        let key = key(&old);
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: key.clone(),
                    value: serde_json::to_vec(&old).unwrap(),
                }],
            )
            .unwrap();
        // Simulate a migration page captured before a concurrent replacement.
        let (_, page) = store
            .page_with_metadata(b"", REPORTS, REPORTS, MAX_PAGE)
            .unwrap();
        let stale: ExtensionReportBatch = serde_json::from_slice(&page[0].1.value).unwrap();
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: key.clone(),
                    value: serde_json::to_vec(&replacement).unwrap(),
                }],
            )
            .unwrap();
        index(&store, key.clone(), &stale, true).unwrap();
        index(&store, key, &old, false).unwrap();
        let sub = Subscription::open(&store).unwrap();
        let deliveries = sub.read(0, 10).unwrap();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].batch, replacement);
        assert_eq!(deliveries[0].cursor, 1);
        assert!(sub.read(1, 10).unwrap().is_empty());
    }

    #[test]
    fn orphaned_next_record_is_reported_as_corrupt() {
        let store = Store::memory().unwrap();
        let mut orphan = b"durable/observer/".to_vec();
        orphan.extend_from_slice(STREAM.as_bytes());
        orphan.extend_from_slice(b"/r");
        orphan.extend_from_slice(&1u64.to_be_bytes());
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: orphan,
                    value: b"null".to_vec(),
                }],
            )
            .unwrap();
        let b = batch(1);
        assert!(matches!(
            index(&store, key(&b), &b, false),
            Err(Error::Observer(crate::observer::Error::Corrupt))
        ));
        assert!(store.get(&key(&b)).unwrap().is_none());
    }

    #[test]
    fn malformed_legacy_record_fails_without_appending() {
        let store = Store::memory().unwrap();
        let b = batch(1);
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: key(&b),
                    value: b"{".to_vec(),
                }],
            )
            .unwrap();
        assert!(matches!(Subscription::open(&store), Err(Error::Json(_))));
        assert_eq!(Observer::new(&store, STREAM).head().unwrap(), Cursor(0));
        assert!(store.get(&marker(&key(&b))).unwrap().is_none());
    }

    #[test]
    fn migration_and_new_writes_preserve_delivery_cursors() {
        let store = Store::memory().unwrap();
        let legacy = batch(1);
        let newer = batch(2);
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: key(&legacy),
                    value: serde_json::to_vec(&legacy).unwrap(),
                }],
            )
            .unwrap();
        // A new write can index an existing identity before migration reaches it.
        let mut duplicate = legacy.clone();
        duplicate.reports[0].error = "later".into();
        index(&store, key(&legacy), &duplicate, false).unwrap();
        index(&store, key(&newer), &newer, false).unwrap();
        let sub = Subscription::open(&store).unwrap();
        let first = sub.read(0, 1).unwrap();
        assert_eq!(first[0].batch, legacy);
        assert_eq!(first[0].cursor, 1);
        let second = sub.read(first[0].cursor, 1).unwrap();
        assert_eq!(second[0].batch, newer);
        assert_eq!(second[0].cursor, 2);
        assert!(sub.read(2, 1).unwrap().is_empty());
    }

    #[tokio::test]
    async fn legacy_reconnect_and_cancelled_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reports.db");
        let legacy = batch(0);
        let cursor;
        {
            let store = Store::open(&path).unwrap();
            store
                .commit(
                    &[],
                    &[Mutation::Put {
                        key: key(&legacy),
                        value: serde_json::to_vec(&legacy).unwrap(),
                    }],
                )
                .unwrap();
            let sub = Subscription::open(&store).unwrap();
            let first = sub.read(0, 1).unwrap();
            assert_eq!(first[0].batch, legacy);
            cursor = first[0].cursor;
            // Cancellation of a pending wait needs no worker or shutdown handshake.
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(10),
                    sub.next(cursor, 1, Duration::from_millis(1))
                )
                .await
                .is_err()
            );
        }
        {
            let store = Store::open(&path).unwrap();
            let b = batch(1);
            index(&store, key(&b), &b, false).unwrap();
            let sub = Subscription::open(&store).unwrap();
            assert_eq!(sub.read(0, 1).unwrap()[0].batch, legacy);
            let next = sub.next(cursor, 1, Duration::from_millis(1)).await.unwrap();
            assert_eq!(next[0].batch, b);
            assert_eq!(sub.read(next[0].cursor, 1).unwrap().len(), 0);
        }
    }
}
