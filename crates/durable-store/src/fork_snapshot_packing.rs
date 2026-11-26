//! Losslessly compact aged fork evidence without changing its logical version.
use crate::{
    transcript_packing::{unpack, HEADER, MAGIC, MAX_LIMIT},
    Check, Entry, Error, Expected, Mutation, Store,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const PREFIX: &[u8] = b"runtime/fork/";
const WINDOW: &[u8] = b"runtime/fork-snapshot-packing/window/v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkSnapshotPackingCursor {
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
            || cursor.through_version == 0
            || cursor.through_version > window.eligible
            || cursor.through_version > self.put_version()?
            || cursor
                .after
                .as_ref()
                .is_some_and(|key| !key.starts_with(PREFIX))
        {
            return Err(Error::InvalidSnapshot);
        }
        let page = self.packing_candidates(
            PREFIX,
            cursor.after.as_deref().unwrap_or(PREFIX),
            Some(cursor.through_version),
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
}

fn packing_cursor(through_version: u64) -> Option<ForkSnapshotPackingCursor> {
    (through_version != 0).then_some(ForkSnapshotPackingCursor {
        through_version,
        after: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
