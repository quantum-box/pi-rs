//! Pull-based delivery of committed extension report batches.

use crate::{
    observer::{Cursor, Observer},
    runtime::{Error, ExtensionReportBatch},
};
use durable_store::{Check, Expected, Mutation, Store};
use std::{num::NonZeroUsize, time::Duration};
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
