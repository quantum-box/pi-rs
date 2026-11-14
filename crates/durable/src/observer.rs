//! Durable, append-only observation log, independent of the runtime.
//!
//! Only records explicitly appended here are retained: arbitrary JSON values,
//! in per-stream commit order. This is not a transcript of existing runtime
//! activity. The host can atomically commit an append with its own state changes
//! using `prepare_append` and `Store::commit`. All writers must use this API and
//! reserve its `durable/observer/` namespace; deleting or modifying its keys
//! invalidates the guarantees. No retention or compaction is performed.
//!
//! A cursor is an exclusive sequence number scoped to a stream. Each read is a
//! store snapshot, not a snapshot shared across pages; later appends can appear
//! on later pages. Committed records are immutable and never skipped under the
//! namespace contract. Consumers persist a cursor only after processing records;
//! a crash before that may replay records, so delivery is at least once, not
//! exactly once. A cursor beyond the current head is rejected.
//!
//! Watch is polling, including across processes and reconnects; it has no
//! in-memory subscriber queue, notifications, or backpressure on writers. Slow
//! readers resume from their cursor and retain all records, at the cost of
//! unbounded disk growth. Reads fetch at most the requested page plus one
//! lookahead from the store. Store operations are
//! synchronous and can block the calling async executor thread.

use std::{num::NonZeroUsize, time::Duration};

use durable_store::{Check, Expected, Mutation, Store};
use serde_json::Value;
use uuid::Uuid;

const PREFIX: &[u8] = b"durable/observer/";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("invalid or inconsistent observer history")]
    Corrupt,
    #[error("cursor is beyond the current stream head or replay boundary")]
    FutureCursor,
    #[error("observer sequence exhausted")]
    SequenceExhausted,
    #[error("poll interval must be nonzero")]
    ZeroPollInterval,
}

/// Sequence zero means the beginning; all other values identify a record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cursor(pub u64);

#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub cursor: Cursor,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Page {
    pub records: Vec<Record>,
    /// Cursor after the last returned record, or the input cursor if empty.
    pub next: Cursor,
    /// Whether this snapshot contains more records after `next`.
    pub has_more: bool,
}

/// Checks and mutations to add to a host's atomic state transition.
/// On a conflict, discard this value and prepare again from the new head.
/// A commit storage error can have an unknown outcome: inspect the store before
/// retrying, particularly if the host transition is not idempotent.
pub struct Append {
    pub checks: Vec<Check>,
    pub mutations: Vec<Mutation>,
    pub cursor: Cursor,
}

impl Append {
    /// Set the payload after preparing the sequence, before committing the batch.
    pub(crate) fn set_value(&mut self, value: &Value) -> Result<(), Error> {
        match self.mutations.first_mut() {
            Some(Mutation::Put { value: stored, .. }) => *stored = serde_json::to_vec(value)?,
            _ => return Err(Error::Corrupt),
        }
        Ok(())
    }
}

pub struct Observer<'a> {
    store: &'a Store,
    stream: Uuid,
}

impl<'a> Observer<'a> {
    pub fn new(store: &'a Store, stream: Uuid) -> Self {
        Self { store, stream }
    }

    /// Capture the current committed head for a finite reconnect catch-up.
    /// Persist this stream-scoped cursor together with the processing cursor if
    /// the same catch-up must survive reopening.
    pub fn head(&self) -> Result<Cursor, Error> {
        Ok(Cursor(
            self.store
                .get(&head_key(self.stream))?
                .map(|entry| decode_head(&entry.value))
                .transpose()?
                .unwrap_or(0),
        ))
    }

    /// Read complete records after `after`, stopping at the captured `through`
    /// cursor even if later records have been committed. `has_more` describes
    /// this bounded range; once false, resume `watch` from `next`.
    pub fn read_through(
        &self,
        after: Cursor,
        through: Cursor,
        limit: NonZeroUsize,
    ) -> Result<Page, Error> {
        if after.0 > through.0 {
            return Err(Error::FutureCursor);
        }
        if through.0 > self.head()?.0 {
            return Err(Error::FutureCursor);
        }
        // Never fetch records beyond the captured boundary, except read's
        // single lookahead. This also avoids converting an unbounded u64 to usize.
        let remaining = through.0 - after.0;
        if remaining == 0 {
            return Ok(Page {
                records: Vec::new(),
                next: after,
                has_more: false,
            });
        }
        let count = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(limit.get());
        let mut page = self.read(
            after,
            NonZeroUsize::new(count).expect("remaining is nonzero"),
        )?;
        page.has_more = page.next.0 < through.0;
        Ok(page)
    }

    /// Prepare one record. Commit these checks and mutations together, without
    /// modifying them, optionally alongside unrelated host checks and mutations.
    pub fn prepare_append(&self, value: &Value) -> Result<Append, Error> {
        let head_key = head_key(self.stream);
        let head = self.store.get(&head_key)?;
        let sequence = match &head {
            Some(entry) => decode_head(&entry.value)?,
            None => 0,
        }
        .checked_add(1)
        .ok_or(Error::SequenceExhausted)?;
        let record_key = record_key(self.stream, sequence);
        Ok(Append {
            checks: vec![
                Check {
                    key: head_key.clone(),
                    expected: head.map_or(Expected::Missing, |e| Expected::Version(e.version)),
                },
                Check {
                    key: record_key.clone(),
                    expected: Expected::Missing,
                },
            ],
            mutations: vec![
                Mutation::Put {
                    key: record_key,
                    value: serde_json::to_vec(value)?,
                },
                Mutation::Put {
                    key: head_key,
                    value: sequence.to_be_bytes().to_vec(),
                },
            ],
            cursor: Cursor(sequence),
        })
    }

    /// Read up to `limit` records strictly after `after` from one snapshot.
    pub fn read(&self, after: Cursor, limit: NonZeroUsize) -> Result<Page, Error> {
        let mut prefix = stream_prefix(self.stream);
        prefix.push(b'r');
        let (head, entries) = self.store.page_with_metadata(
            &head_key(self.stream),
            &prefix,
            &record_key(self.stream, after.0),
            limit.get().saturating_add(1),
        )?;
        let head = head
            .as_ref()
            .map(|entry| decode_head(&entry.value))
            .transpose()?
            .unwrap_or(0);
        if after.0 > head {
            return Err(Error::FutureCursor);
        }
        let mut records = Vec::new();
        let mut expected = after.0;
        let mut has_more = false;
        for (key, entry) in entries {
            let suffix = &key[prefix.len()..];
            if suffix.len() != 8 {
                return Err(Error::Corrupt);
            }
            let seq = u64::from_be_bytes(suffix.try_into().map_err(|_| Error::Corrupt)?);
            expected = expected.checked_add(1).ok_or(Error::SequenceExhausted)?;
            if seq != expected || seq > head {
                return Err(Error::Corrupt);
            }
            if records.len() < limit.get() {
                records.push(Record {
                    cursor: Cursor(seq),
                    value: serde_json::from_slice(&entry.value)?,
                });
            } else {
                has_more = true;
            }
        }
        if expected < head && !has_more {
            return Err(Error::Corrupt);
        }
        let next = records.last().map_or(after, |r| r.cursor);
        Ok(Page {
            records,
            next,
            has_more,
        })
    }

    /// Repeatedly read until a nonempty page is available. Dropping the future
    /// cancels the wait. Reconnect by calling again with the last processed cursor.
    pub async fn watch(
        &self,
        after: Cursor,
        limit: NonZeroUsize,
        poll: Duration,
    ) -> Result<Page, Error> {
        if poll.is_zero() {
            return Err(Error::ZeroPollInterval);
        }
        loop {
            let page = self.read(after, limit)?;
            if !page.records.is_empty() {
                return Ok(page);
            }
            tokio::time::sleep(poll).await;
        }
    }
}

fn stream_prefix(id: Uuid) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.extend_from_slice(id.as_bytes());
    key.push(b'/');
    key
}
fn head_key(id: Uuid) -> Vec<u8> {
    let mut key = stream_prefix(id);
    key.push(b'h');
    key
}
fn record_key(id: Uuid, sequence: u64) -> Vec<u8> {
    let mut key = stream_prefix(id);
    key.push(b'r');
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}
fn decode_head(value: &[u8]) -> Result<u64, Error> {
    let n = u64::from_be_bytes(value.try_into().map_err(|_| Error::Corrupt)?);
    if n == 0 {
        return Err(Error::Corrupt);
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{Arc, Barrier};

    fn append(observer: &Observer<'_>, value: Value) -> Cursor {
        let a = observer.prepare_append(&value).unwrap();
        observer.store.commit(&a.checks, &a.mutations).unwrap();
        a.cursor
    }
    fn limit(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    #[test]
    fn pages_reopen_and_stream_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("observer.db");
        let id = Uuid::from_u128(1);
        {
            let db = Store::open(&path).unwrap();
            let o = Observer::new(&db, id);
            for n in 0..3 {
                append(&o, json!(n));
            }
            append(&Observer::new(&db, Uuid::from_u128(2)), json!("other"));
            let p = o.read(Cursor(0), limit(2)).unwrap();
            assert_eq!(
                p.records
                    .iter()
                    .map(|r| r.value.clone())
                    .collect::<Vec<_>>(),
                vec![json!(0), json!(1)]
            );
            assert_eq!(p.next, Cursor(2));
            assert!(p.has_more);
        }
        let db = Store::open(&path).unwrap();
        let o = Observer::new(&db, id);
        let p = o.read(Cursor(2), limit(2)).unwrap();
        assert_eq!(p.records[0].value, json!(2));
        assert_eq!(p.next, Cursor(3));
        assert!(!p.has_more);
        assert!(o.read(p.next, limit(1)).unwrap().records.is_empty());
        assert!(matches!(
            o.read(Cursor(4), limit(1)),
            Err(Error::FutureCursor)
        ));
    }

    #[tokio::test]
    async fn bounded_replay_survives_reopen_and_hands_off_to_watch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replay.db");
        let id = Uuid::from_u128(7);
        let boundary;
        let first;
        {
            let db = Store::open(&path).unwrap();
            let o = Observer::new(&db, id);
            assert_eq!(o.head().unwrap(), Cursor(0));
            assert!(
                !o.read_through(Cursor(0), Cursor(0), limit(1))
                    .unwrap()
                    .has_more
            );
            for n in 1..=3 {
                append(&o, json!(n));
            }
            boundary = o.head().unwrap();
            first = o.read_through(Cursor(0), boundary, limit(1)).unwrap();
            assert_eq!(first.records[0].value, json!(1));
            assert!(first.has_more);
            append(&o, json!(4));
        }
        let db = Store::open(&path).unwrap();
        let o = Observer::new(&db, id);
        append(&o, json!(5));
        let rest = o.read_through(first.next, boundary, limit(10)).unwrap();
        assert_eq!(
            rest.records
                .iter()
                .map(|r| r.value.clone())
                .collect::<Vec<_>>(),
            vec![json!(2), json!(3)]
        );
        assert_eq!(rest.next, boundary);
        assert!(!rest.has_more);
        let live = o
            .watch(rest.next, limit(10), Duration::from_millis(1))
            .await
            .unwrap();
        assert_eq!(
            live.records
                .iter()
                .map(|r| r.value.clone())
                .collect::<Vec<_>>(),
            vec![json!(4), json!(5)]
        );
        assert!(matches!(
            o.read_through(Cursor(4), boundary, limit(1)),
            Err(Error::FutureCursor)
        ));
        assert!(matches!(
            o.read_through(Cursor(0), Cursor(6), limit(1)),
            Err(Error::FutureCursor)
        ));
    }

    #[test]
    fn concurrent_append_conflicts_and_host_batch_is_atomic() {
        let db = Arc::new(Store::memory().unwrap());
        let barrier = Arc::new(Barrier::new(2));
        let threads: Vec<_> = (0..2)
            .map(|n| {
                let db = db.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let o = Observer::new(&db, Uuid::nil());
                    let mut a = o.prepare_append(&json!(n)).unwrap();
                    a.mutations.push(Mutation::Put {
                        key: format!("host/{n}").into_bytes(),
                        value: vec![1],
                    });
                    barrier.wait();
                    db.commit(&a.checks, &a.mutations).is_ok()
                })
            })
            .collect();
        let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|&&v| v).count(), 1);
        for (n, success) in results.iter().enumerate() {
            assert_eq!(
                db.get(format!("host/{n}").as_bytes()).unwrap().is_some(),
                *success
            );
        }
        let o = Observer::new(&db, Uuid::nil());
        assert_eq!(o.read(Cursor(0), limit(5)).unwrap().records.len(), 1);
        append(&o, json!("retry"));
        assert_eq!(o.read(Cursor(0), limit(5)).unwrap().records.len(), 2);
    }

    #[tokio::test]
    async fn watch_polls_and_reconnects_without_losing_slow_reader_history() {
        let db = Arc::new(Store::memory().unwrap());
        let writer = db.clone();
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let o = Observer::new(&writer, Uuid::nil());
            for n in 0..3 {
                append(&o, json!(n));
            }
        });
        let o = Observer::new(&db, Uuid::nil());
        let first = tokio::time::timeout(
            Duration::from_secs(2),
            o.watch(Cursor(0), limit(1), Duration::from_millis(2)),
        )
        .await
        .unwrap()
        .unwrap();
        task.await.unwrap();
        let rest = o
            .watch(first.next, limit(5), Duration::from_millis(2))
            .await
            .unwrap();
        assert_eq!(
            rest.records
                .iter()
                .map(|r| r.value.clone())
                .collect::<Vec<_>>(),
            vec![json!(1), json!(2)]
        );
        assert!(matches!(
            o.watch(rest.next, limit(1), Duration::ZERO).await,
            Err(Error::ZeroPollInterval)
        ));
    }

    #[test]
    fn detects_missing_record() {
        let db = Store::memory().unwrap();
        let o = Observer::new(&db, Uuid::nil());
        append(&o, json!(1));
        db.commit(
            &[],
            &[Mutation::Delete {
                key: record_key(Uuid::nil(), 1),
            }],
        )
        .unwrap();
        assert!(matches!(o.read(Cursor(0), limit(1)), Err(Error::Corrupt)));
    }
}
