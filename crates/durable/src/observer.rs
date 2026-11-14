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
    #[error("wait duration exceeds the clock range")]
    WaitTooLong,
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

/// Result of a bounded polling wait. A timeout does not close the stream.
#[derive(Clone, Debug, PartialEq)]
pub enum WaitOutcome {
    Records(Page),
    TimedOut { cursor: Cursor },
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
    prefix: &'static [u8],
}

impl<'a> Observer<'a> {
    pub fn new(store: &'a Store, stream: Uuid) -> Self {
        Self {
            store,
            stream,
            prefix: PREFIX,
        }
    }

    pub(crate) fn tool_progress(store: &'a Store, stream: Uuid) -> Self {
        Self {
            store,
            stream,
            prefix: b"durable/tool-progress-events/",
        }
    }

    pub(crate) fn tool_attempts(store: &'a Store, conversation: Uuid) -> Self {
        Self {
            store,
            stream: conversation,
            prefix: b"durable/tool-attempts/",
        }
    }

    /// Capture the current committed head for a finite reconnect catch-up.
    /// Persist this stream-scoped cursor together with the processing cursor if
    /// the same catch-up must survive reopening.
    pub fn head(&self) -> Result<Cursor, Error> {
        Ok(Cursor(
            self.store
                .get(&head_key_with(self.prefix, self.stream))?
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
        let head_key = head_key_with(self.prefix, self.stream);
        let head = self.store.get(&head_key)?;
        let sequence = match &head {
            Some(entry) => decode_head(&entry.value)?,
            None => 0,
        }
        .checked_add(1)
        .ok_or(Error::SequenceExhausted)?;
        let record_key = record_key_with(self.prefix, self.stream, sequence);
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
        let mut prefix = stream_prefix_with(self.prefix, self.stream);
        prefix.push(b'r');
        let (head, entries) = self.store.page_with_metadata(
            &head_key_with(self.prefix, self.stream),
            &prefix,
            &record_key_with(self.prefix, self.stream, after.0),
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

    /// Poll for records for at most `wait`, returning the unchanged cursor on
    /// timeout. Dropping the future cancels the wait without changing a cursor.
    /// Synchronous store reads cannot be preempted by the deadline.
    pub async fn watch_for(
        &self,
        after: Cursor,
        limit: NonZeroUsize,
        poll: Duration,
        wait: Duration,
    ) -> Result<WaitOutcome, Error> {
        if poll.is_zero() {
            return Err(Error::ZeroPollInterval);
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(wait)
            .ok_or(Error::WaitTooLong)?;
        loop {
            let page = self.read(after, limit)?;
            if !page.records.is_empty() {
                return Ok(WaitOutcome::Records(page));
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(WaitOutcome::TimedOut { cursor: after });
            }
            tokio::time::sleep(
                poll.min(deadline.saturating_duration_since(tokio::time::Instant::now())),
            )
            .await;
        }
    }
}

fn stream_prefix_with(prefix: &[u8], id: Uuid) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(id.as_bytes());
    key.push(b'/');
    key
}
fn head_key_with(prefix: &[u8], id: Uuid) -> Vec<u8> {
    let mut key = stream_prefix_with(prefix, id);
    key.push(b'h');
    key
}
fn record_key_with(prefix: &[u8], id: Uuid, sequence: u64) -> Vec<u8> {
    let mut key = stream_prefix_with(prefix, id);
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
    fn bounded_pages_keep_the_captured_head_while_another_writer_commits() {
        let db = Arc::new(Store::memory().unwrap());
        let id = Uuid::new_v4();
        let observer = Observer::new(&db, id);
        for n in 1..=4 {
            append(&observer, json!(n));
        }
        let boundary = observer.head().unwrap();
        let rendezvous = Arc::new(Barrier::new(2));
        let writer = {
            let db = Arc::clone(&db);
            let rendezvous = Arc::clone(&rendezvous);
            std::thread::spawn(move || {
                let observer = Observer::new(&db, id);
                for n in 5..=8 {
                    rendezvous.wait();
                    append(&observer, json!(n));
                    rendezvous.wait();
                }
            })
        };
        let mut cursor = Cursor(0);
        for n in 1..=4 {
            rendezvous.wait();
            let page = observer.read_through(cursor, boundary, limit(1)).unwrap();
            rendezvous.wait();
            assert_eq!(page.records.len(), 1);
            assert_eq!(page.records[0].value, json!(n));
            assert_eq!(page.next, Cursor(n));
            assert_eq!(page.has_more, n < 4);
            cursor = page.next;
        }
        writer.join().unwrap();
        assert_eq!(cursor, boundary);
        let live = observer.read(cursor, limit(10)).unwrap();
        assert_eq!(
            live.records
                .iter()
                .map(|r| r.value.clone())
                .collect::<Vec<_>>(),
            vec![json!(5), json!(6), json!(7), json!(8)]
        );
        assert_eq!(live.next, Cursor(8));
        assert!(!live.has_more);
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

    #[tokio::test]
    async fn bounded_wait_timeout_and_reopen_preserve_cursor_and_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wait.db");
        let id = Uuid::new_v4();
        let cursor;
        {
            let db = Store::open(&path).unwrap();
            let observer = Observer::new(&db, id);
            append(&observer, json!(1));
            append(&observer, json!(2));
            let WaitOutcome::Records(first) = observer
                .watch_for(Cursor(0), limit(1), Duration::from_millis(1), Duration::ZERO)
                .await
                .unwrap()
            else {
                panic!("expected records")
            };
            assert_eq!(first.records[0].value, json!(1));
            cursor = first.next;
        }
        let db = Store::open(&path).unwrap();
        let observer = Observer::new(&db, id);
        let WaitOutcome::Records(second) = observer
            .watch_for(cursor, limit(1), Duration::from_millis(1), Duration::ZERO)
            .await
            .unwrap()
        else {
            panic!("expected records after reopening")
        };
        assert_eq!(second.records[0].value, json!(2));
        assert_eq!(
            observer
                .watch_for(second.next, limit(1), Duration::from_secs(1), Duration::from_millis(5))
                .await
                .unwrap(),
            WaitOutcome::TimedOut { cursor: second.next }
        );
        append(&observer, json!(3));
        let WaitOutcome::Records(third) = observer
            .watch_for(second.next, limit(1), Duration::from_millis(1), Duration::ZERO)
            .await
            .unwrap()
        else {
            panic!("expected subsequent record")
        };
        assert_eq!(third.records[0].value, json!(3));
        assert!(matches!(
            observer.watch_for(Cursor(4), limit(1), Duration::from_millis(1), Duration::ZERO).await,
            Err(Error::FutureCursor)
        ));
        assert!(matches!(
            observer.watch_for(third.next, limit(1), Duration::ZERO, Duration::ZERO).await,
            Err(Error::ZeroPollInterval)
        ));
    }

    #[tokio::test]
    async fn bounded_wait_observes_later_commit() {
        let db = Arc::new(Store::memory().unwrap());
        let writer = Arc::clone(&db);
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            append(&Observer::new(&writer, Uuid::nil()), json!("new"));
        });
        let observer = Observer::new(&db, Uuid::nil());
        let outcome = observer
            .watch_for(Cursor(0), limit(1), Duration::from_millis(2), Duration::from_secs(2))
            .await
            .unwrap();
        task.await.unwrap();
        let WaitOutcome::Records(page) = outcome else {
            panic!("expected the later commit")
        };
        assert_eq!(page.next, Cursor(1));
        assert_eq!(page.records[0].value, json!("new"));
    }

    #[test]
    fn detects_missing_record() {
        let db = Store::memory().unwrap();
        let o = Observer::new(&db, Uuid::nil());
        append(&o, json!(1));
        db.commit(
            &[],
            &[Mutation::Delete {
                key: record_key_with(PREFIX, Uuid::nil(), 1),
            }],
        )
        .unwrap();
        assert!(matches!(o.read(Cursor(0), limit(1)), Err(Error::Corrupt)));
    }

    #[test]
    fn progress_streams_are_isolated_and_detect_a_missing_first_event() {
        let db = Store::memory().unwrap();
        let id = Uuid::from_u128(1);
        let progress = Observer::tool_progress(&db, id);
        let other = Observer::tool_progress(&db, Uuid::from_u128(2));
        let history = Observer::new(&db, id);
        append(&history, json!("history"));
        append(&progress, json!("first"));
        append(&progress, json!("second"));
        append(&other, json!("other"));
        let first = progress.read(Cursor(0), limit(1)).unwrap();
        assert_eq!(first.records[0].value, json!("first"));
        assert!(first.has_more);
        assert_eq!(
            progress.read(first.next, limit(1)).unwrap().records[0].value,
            json!("second")
        );
        assert_eq!(
            history.read(Cursor(0), limit(2)).unwrap().records[0].value,
            json!("history")
        );
        assert_eq!(
            other.read(Cursor(0), limit(2)).unwrap().records[0].value,
            json!("other")
        );
        assert!(matches!(
            other.read(Cursor(2), limit(1)),
            Err(Error::FutureCursor)
        ));
        db.commit(
            &[],
            &[Mutation::Delete {
                key: record_key_with(b"durable/tool-progress-events/", id, 1),
            }],
        )
        .unwrap();
        assert!(matches!(
            progress.read(Cursor(0), limit(1)),
            Err(Error::Corrupt)
        ));
    }

    #[test]
    fn stale_progress_and_terminal_batches_cannot_overtake_each_other() {
        use crate::tool_progress::{Snapshot, State};
        for terminal_first in [false, true] {
            let db = Store::memory().unwrap();
            let id = Uuid::new_v4();
            let observer = Observer::tool_progress(&db, id);
            let mut running = Snapshot::load(&db, id).unwrap();
            running.update(json!("partial")).unwrap();
            let mut update = running.prepare_checkpoint().unwrap();
            let event = observer.prepare_append(&json!(running.state())).unwrap();
            update.checks.extend(event.checks);
            update.mutations.extend(event.mutations);
            let snapshot = Snapshot::load(&db, id).unwrap();
            let mut terminal = snapshot.prepare_abort().unwrap();
            let event = observer
                .prepare_append(&json!(State::Aborted(None)))
                .unwrap();
            terminal.checks.extend(event.checks);
            terminal.mutations.extend(event.mutations);
            let (winner, loser) = if terminal_first {
                (&terminal, &update)
            } else {
                (&update, &terminal)
            };
            db.commit(&winner.checks, &winner.mutations).unwrap();
            assert!(matches!(
                db.commit(&loser.checks, &loser.mutations),
                Err(durable_store::Error::Conflict { .. })
            ));
            if !terminal_first {
                let snapshot = Snapshot::load(&db, id).unwrap();
                let mut retry = snapshot.prepare_abort().unwrap();
                let event = observer
                    .prepare_append(&json!(State::Aborted(Some(json!("partial")))))
                    .unwrap();
                retry.checks.extend(event.checks);
                retry.mutations.extend(event.mutations);
                db.commit(&retry.checks, &retry.mutations).unwrap();
            }
            let page = observer.read(Cursor(0), limit(3)).unwrap();
            assert_eq!(page.records.len(), if terminal_first { 1 } else { 2 });
            assert_eq!(page.records.last().unwrap().value["state"], "aborted");
            assert!(matches!(
                Snapshot::load(&db, id).unwrap().state(),
                State::Aborted(_)
            ));
        }
    }
}
