//! Losslessly compact aged fork evidence without changing its logical version.
use crate::{
    transcript_packing::{unpack, HEADER, MAGIC, MAX_LIMIT},
    Check, Entry, Error, Expected, Mutation, Store,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const PREFIX: &[u8] = b"runtime/fork/";
const FORK_KEY_BYTES: usize = PREFIX.len() + 36 + 1 + 20;
const WINDOW: &[u8] = b"runtime/fork-snapshot-packing/window/v1";
const SWEEP: &[u8] = b"runtime/fork-snapshot-packing/sweep/v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkSnapshotPackingCursor {
    #[serde(default, skip_serializing_if = "is_zero")]
    since_version: u64,
    through_version: u64,
    after: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForkSnapshotPackingPage {
    pub examined: usize,
    pub packed: usize,
    pub reclaimed_bytes: usize,
    pub next: Option<ForkSnapshotPackingCursor>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Window {
    eligible: u64,
    pending: u64,
    observed_ms: u64,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Sweep {
    completed: u64,
    cursor: Option<ForkSnapshotPackingCursor>,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn canonical_key(key: &[u8]) -> bool {
    let Some(suffix) = key.strip_prefix(PREFIX) else {
        return false;
    };
    let Ok(text) = std::str::from_utf8(suffix) else {
        return false;
    };
    let Some((session, observation)) = text.split_once('/') else {
        return false;
    };
    let Ok(id) = Uuid::parse_str(session) else {
        return false;
    };
    let Ok(number) = observation.parse::<u64>() else {
        return false;
    };
    !id.is_nil()
        && id.to_string() == session
        && number != 0
        && observation.len() == 20
        && format!("{number:020}") == observation
}

/// Decode a packed fork snapshot across the ordinary Store read surfaces.
pub(crate) fn unpack_entry(
    key: &[u8],
    mut entry: Entry,
    limit: Option<usize>,
) -> Result<Entry, Error> {
    if !entry.value.starts_with(MAGIC) || !canonical_key(key) {
        return Ok(entry);
    }
    if let Some((limit, header)) = limit.zip(entry.value.get(MAGIC.len()..HEADER)) {
        let size = u32::from_be_bytes(header[..4].try_into().map_err(|_| Error::InvalidSnapshot)?);
        if size as usize > limit {
            return Err(Error::DocumentFrameTooLarge);
        }
    }
    entry.value = unpack(entry.value, limit)?;
    Ok(entry)
}

pub(crate) fn unpack_keyed_entry(
    (key, entry): (Vec<u8>, Entry),
    limit: Option<usize>,
) -> Result<(Vec<u8>, Entry), Error> {
    let entry = unpack_entry(&key, entry, limit)?;
    Ok((key, entry))
}

impl Store {
    /// Return a finite pass over fork snapshots observed at least `retain_ms`
    /// earlier. Observations survive reopen. The delay is conservative:
    /// snapshots written between observations wait for the next aging window.
    pub fn fork_snapshot_packing_due(
        &self,
        now_ms: u64,
        retain_ms: u64,
    ) -> Result<Option<ForkSnapshotPackingCursor>, Error> {
        if retain_ms == 0 {
            return Err(Error::InvalidSnapshot);
        }
        loop {
            let previous = self.get_bounded(WINDOW, 256)?;
            let boundary = self.put_version()?;
            let mut window = if let Some(entry) = &previous {
                let window: Window =
                    serde_json::from_slice(&entry.value).map_err(|_| Error::InvalidSnapshot)?;
                if window.eligible > window.pending || window.pending > boundary {
                    return Err(Error::InvalidSnapshot);
                }
                if now_ms.saturating_sub(window.observed_ms) < retain_ms {
                    return Ok(packing_cursor(window.eligible));
                }
                window
            } else {
                Window {
                    eligible: 0,
                    pending: 0,
                    observed_ms: now_ms,
                }
            };
            window.eligible = window.pending;
            window.pending = boundary;
            window.observed_ms = now_ms;
            let value = serde_json::to_vec(&window).map_err(|_| Error::InvalidSnapshot)?;
            match self.commit(
                &[Check {
                    key: WINDOW.to_vec(),
                    expected: previous
                        .as_ref()
                        .map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
                }],
                &[Mutation::Put {
                    key: WINDOW.to_vec(),
                    value,
                }],
            ) {
                Ok(()) => return Ok(packing_cursor(window.eligible)),
                Err(Error::Conflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
    }

    /// Pack aged canonical fork snapshots in bounded pages. Payload bytes are
    /// decoded on reads, and their original versions and commit evidence remain.
    pub fn pack_fork_snapshot_history(
        &self,
        cursor: &ForkSnapshotPackingCursor,
        limit: usize,
    ) -> Result<ForkSnapshotPackingPage, Error> {
        let window = self
            .get_bounded(WINDOW, 256)?
            .ok_or(Error::InvalidSnapshot)?;
        let window: Window =
            serde_json::from_slice(&window.value).map_err(|_| Error::InvalidSnapshot)?;
        if !(1..=MAX_LIMIT).contains(&limit)
            || cursor.since_version >= cursor.through_version
            || cursor.through_version > window.eligible
            || cursor.through_version > self.put_version()?
            || cursor
                .after
                .as_ref()
                .is_some_and(|key| key.len() != FORK_KEY_BYTES || !key.starts_with(PREFIX))
        {
            return Err(Error::InvalidSnapshot);
        }
        let page = self.packing_candidates(
            PREFIX,
            cursor.after.as_deref().unwrap_or(PREFIX),
            Some((cursor.since_version, cursor.through_version)),
            Some(FORK_KEY_BYTES),
            limit,
        )?;
        let mut packed = 0;
        let mut reclaimed_bytes = 0;
        for (key, version) in &page {
            if !canonical_key(key) || self.document_cut_for_version(key, *version)?.is_none() {
                continue;
            }
            if let Some(saved) = self.pack_stored_entry(key, *version)? {
                packed += 1;
                reclaimed_bytes += saved;
            }
        }
        let next =
            page.last()
                .filter(|_| page.len() == limit)
                .map(|(key, _)| ForkSnapshotPackingCursor {
                    since_version: cursor.since_version,
                    through_version: cursor.through_version,
                    after: Some(key.clone()),
                });
        Ok(ForkSnapshotPackingPage {
            examined: page.len(),
            packed,
            reclaimed_bytes,
            next,
        })
    }

    /// Run one page of the background sweep and persist its continuation.
    /// Packing the same page again after an interruption is safe.
    pub fn sweep_fork_snapshot_history(
        &self,
        now_ms: u64,
        retain_ms: u64,
        limit: usize,
    ) -> Result<(), Error> {
        if retain_ms == 0 || !(1..=MAX_LIMIT).contains(&limit) {
            return Err(Error::InvalidSnapshot);
        }
        let checkpoint = self.get_bounded(SWEEP, 1024)?;
        let mut sweep: Sweep = checkpoint
            .as_ref()
            .map(|entry| serde_json::from_slice(&entry.value))
            .transpose()
            .map_err(|_| Error::InvalidSnapshot)?
            .unwrap_or_default();
        if sweep.completed > self.put_version()? {
            return Err(Error::InvalidSnapshot);
        }
        let cursor = match sweep.cursor.take() {
            Some(cursor) if cursor.since_version == sweep.completed => cursor,
            Some(_) => return Err(Error::InvalidSnapshot),
            None => {
                let Some(mut cursor) = self.fork_snapshot_packing_due(now_ms, retain_ms)? else {
                    return Ok(());
                };
                if cursor.through_version <= sweep.completed {
                    return Ok(());
                }
                cursor.since_version = sweep.completed;
                cursor
            }
        };
        let page = self.pack_fork_snapshot_history(&cursor, limit)?;
        sweep.cursor = page.next;
        if sweep.cursor.is_none() {
            sweep.completed = cursor.through_version;
        }
        let value = serde_json::to_vec(&sweep).map_err(|_| Error::InvalidSnapshot)?;
        match self.commit(
            &[Check {
                key: SWEEP.to_vec(),
                expected: checkpoint
                    .map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
            }],
            &[Mutation::Put {
                key: SWEEP.to_vec(),
                value,
            }],
        ) {
            Ok(()) | Err(Error::Conflict { .. }) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

fn packing_cursor(through_version: u64) -> Option<ForkSnapshotPackingCursor> {
    (through_version != 0).then_some(ForkSnapshotPackingCursor {
        since_version: 0,
        through_version,
        after: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn background_sweep_resumes_after_reopen_and_skips_completed_versions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("forks.sqlite");
        let key = |position: usize| {
            format!("runtime/fork/{}/{position:020}", Uuid::from_u128(1)).into_bytes()
        };
        let bytes = vec![b'x'; 4096];
        let put = |store: &Store, position| {
            store
                .commit(
                    &[],
                    &[Mutation::Put {
                        key: key(position),
                        value: bytes.clone(),
                    }],
                )
                .unwrap();
        };
        let checkpoint = |store: &Store| -> Sweep {
            serde_json::from_slice(&store.get(SWEEP).unwrap().unwrap().value).unwrap()
        };
        let is_packed = |store: &Store, position| {
            let value: Vec<u8> = store
                .connection
                .lock()
                .unwrap()
                .query_row(
                    "SELECT value FROM durable_store_entries WHERE key=?1",
                    [key(position)],
                    |row| row.get(0),
                )
                .unwrap();
            value.starts_with(MAGIC)
        };

        let store = Store::open(&path).unwrap();
        put(&store, 2);
        put(&store, 3);
        // Invalid keys cannot inflate the persisted cursor beyond its read bound.
        let malformed = [PREFIX, &[b'!'; 2048]].concat();
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: malformed,
                    value: bytes.clone(),
                }],
            )
            .unwrap();
        let through = store.put_version().unwrap();
        let evidence = store.get(&key(3)).unwrap().unwrap();
        let cut = store
            .document_cut_for_version(&key(3), evidence.version)
            .unwrap();
        store.sweep_fork_snapshot_history(100, 20, 1).unwrap();
        // This key sorts before the progress cursor but belongs to the next window.
        put(&store, 1);
        store.sweep_fork_snapshot_history(120, 20, 1).unwrap();
        assert_eq!(checkpoint(&store).cursor.unwrap().after, Some(key(2)));
        assert!(is_packed(&store, 2));
        assert!(!is_packed(&store, 1));
        assert!(!is_packed(&store, 3));
        // Simulate interruption after the payload update and before its checkpoint.
        let pending = checkpoint(&store).cursor.unwrap();
        assert_eq!(
            store
                .pack_fork_snapshot_history(&pending, 1)
                .unwrap()
                .packed,
            1
        );
        assert_eq!(checkpoint(&store).cursor, Some(pending));
        drop(store);

        let store = Store::open(&path).unwrap();
        store.sweep_fork_snapshot_history(121, 20, 1).unwrap();
        assert_eq!(checkpoint(&store).cursor.unwrap().after, Some(key(3)));
        assert!(is_packed(&store, 3));
        assert!(!is_packed(&store, 1));
        let entry = store.get(&key(3)).unwrap().unwrap();
        assert_eq!(entry.version, evidence.version);
        assert_eq!(entry.value, evidence.value);
        assert_eq!(
            store
                .document_cut_for_version(&key(3), entry.version)
                .unwrap(),
            cut
        );

        store.sweep_fork_snapshot_history(122, 20, 1).unwrap();
        assert_eq!(checkpoint(&store).completed, through);
        assert!(checkpoint(&store).cursor.is_none());
        let checkpoint_version = store.get(SWEEP).unwrap().unwrap().version;
        store.sweep_fork_snapshot_history(123, 20, 1).unwrap();
        assert_eq!(
            store.get(SWEEP).unwrap().unwrap().version,
            checkpoint_version
        );

        store.sweep_fork_snapshot_history(140, 20, 1).unwrap();
        let cursor = checkpoint(&store).cursor.unwrap();
        assert_eq!(cursor.since_version, through);
        assert_eq!(cursor.after, Some(key(1)));
        assert!(is_packed(&store, 1));
        store.sweep_fork_snapshot_history(141, 20, 1).unwrap();
        assert!(checkpoint(&store).cursor.is_none());
    }

    #[test]
    fn newer_fork_snapshots_do_not_extend_an_aging_pass() {
        let store = Store::memory().unwrap();
        let conversation = Uuid::from_u128(1);
        let append = |position| {
            store
                .commit(
                    &[],
                    &[Mutation::Put {
                        key: format!("runtime/fork/{conversation}/{position:020}").into_bytes(),
                        value: vec![b'x'; 4096],
                    }],
                )
                .unwrap();
        };

        append(1);
        append(2);
        assert!(store.fork_snapshot_packing_due(100, 20).unwrap().is_none());
        for position in 3..=7 {
            append(position);
        }

        let cursor = store.fork_snapshot_packing_due(120, 20).unwrap().unwrap();
        let first = store.pack_fork_snapshot_history(&cursor, 2).unwrap();
        assert_eq!((first.examined, first.packed), (2, 2));
        append(8);
        let tail = store
            .pack_fork_snapshot_history(&first.next.unwrap(), 2)
            .unwrap();
        assert_eq!(tail.examined, 0);
        assert!(tail.next.is_none());

        let next_window = store.fork_snapshot_packing_due(140, 20).unwrap().unwrap();
        let later = store.pack_fork_snapshot_history(&next_window, 16).unwrap();
        assert_eq!((later.examined, later.packed), (7, 5));
        assert!(later.next.is_none());
    }
}
