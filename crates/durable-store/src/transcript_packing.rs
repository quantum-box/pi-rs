//! Optional lossless packing of the runtime's immutable transcript entries.
//! Heads, entry IDs and commit metadata are outside this representation change.

use std::io::{Read, Write};

use flate2::{bufread::ZlibDecoder, write::ZlibEncoder, Compression};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{prefix_end, read_limit, Check, Entry, Error, Expected, Mutation, Store};

const PREFIX: &[u8] = b"durable/transcript/entry/";
pub(crate) const MAGIC: &[u8] = b"\x89PI-TRANSCRIPT\0\x01";
pub(crate) const HEADER: usize = MAGIC.len() + 4 + 32;
const MIN_BYTES: usize = 1024;
const MAX_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAX_LIMIT: usize = 64;
const WINDOW: &[u8] = b"runtime/transcript-packing/window/v1";
const SWEEP: &[u8] = b"runtime/transcript-packing/sweep/v1";

/// A finite database-local pass. The boundary excludes writes after capture.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackingCursor {
    through_version: u64,
    after: Option<Uuid>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackingPage {
    /// Number of transcript keys visited, including skipped entries.
    pub examined: usize,
    pub packed: usize,
    pub reclaimed_bytes: usize,
    /// Continue the same pass, or start a new one to include later writes.
    pub next: Option<PackingCursor>,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Sweep {
    completed: u64,
    cursor: Option<PackingCursor>,
}

fn key_for(id: Uuid) -> Vec<u8> {
    [PREFIX, id.as_bytes()].concat()
}

fn entry_id(key: &[u8]) -> Result<Uuid, Error> {
    let suffix = key.strip_prefix(PREFIX).ok_or(Error::InvalidSnapshot)?;
    Uuid::from_slice(suffix).map_err(|_| Error::InvalidSnapshot)
}

fn packed_bytes(value: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    if !(MIN_BYTES..=MAX_BYTES).contains(&value.len()) || value.starts_with(MAGIC) {
        return Ok(None);
    }
    let mut encoded = Vec::with_capacity(HEADER);
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&(value.len() as u32).to_be_bytes());
    encoded.extend_from_slice(&Sha256::digest(value));
    let mut encoder = ZlibEncoder::new(encoded, Compression::fast());
    encoder
        .write_all(value)
        .map_err(|_| Error::InvalidSnapshot)?;
    let packed = encoder.finish().map_err(|_| Error::InvalidSnapshot)?;
    Ok((packed.len() + 32 <= value.len()).then_some(packed))
}

pub(crate) fn unpack(value: Vec<u8>, max_bytes: Option<usize>) -> Result<Vec<u8>, Error> {
    if !value.starts_with(MAGIC) {
        return Ok(value);
    }
    let body = value.get(HEADER..).ok_or(Error::InvalidSnapshot)?;
    let (size, expected_digest) = value[MAGIC.len()..HEADER].split_at(4);
    let size = u32::from_be_bytes(size.try_into().map_err(|_| Error::InvalidSnapshot)?) as usize;
    if !(MIN_BYTES..=MAX_BYTES).contains(&size) || max_bytes.is_some_and(|limit| size > limit) {
        return Err(Error::InvalidSnapshot);
    }
    let mut decoded = Vec::with_capacity(size);
    let mut reader = ZlibDecoder::new(body);
    (&mut reader)
        .take(size as u64 + 1)
        .read_to_end(&mut decoded)
        .map_err(|_| Error::InvalidSnapshot)?;
    if decoded.len() != size
        || reader.total_in() != body.len() as u64
        || Sha256::digest(&decoded).as_slice() != expected_digest
    {
        return Err(Error::InvalidSnapshot);
    }
    Ok(decoded)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Window {
    eligible: u64,
    pending: u64,
    observed_ms: u64,
}

pub(crate) fn eligible_packing_boundary(value: &[u8]) -> Result<u64, Error> {
    let window: Window = serde_json::from_slice(value).map_err(|_| Error::InvalidSnapshot)?;
    Ok(window.eligible)
}

impl Store {
    pub(crate) fn aged_packing_boundary(
        &self,
        key: &[u8],
        now_ms: u64,
        retain_ms: u64,
    ) -> Result<u64, Error> {
        if retain_ms == 0 {
            return Err(Error::InvalidSnapshot);
        }
        loop {
            let previous = self.get_bounded(key, 256)?;
            let boundary = self.put_version()?;
            let mut window = if let Some(entry) = &previous {
                let window: Window =
                    serde_json::from_slice(&entry.value).map_err(|_| Error::InvalidSnapshot)?;
                if window.eligible > window.pending || window.pending > boundary {
                    return Err(Error::InvalidSnapshot);
                }
                if now_ms.saturating_sub(window.observed_ms) < retain_ms {
                    return Ok(window.eligible);
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
                    key: key.to_vec(),
                    expected: previous
                        .as_ref()
                        .map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
                }],
                &[Mutation::Put {
                    key: key.to_vec(),
                    value,
                }],
            ) {
                Ok(()) => return Ok(window.eligible),
                Err(Error::Conflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
    }

    /// Read the logical bytes of one transcript entry, including packed entries.
    /// Apply the same byte ceiling to the original representation.
    pub fn get_transcript_entry(
        &self,
        key: &[u8],
        max_bytes: Option<usize>,
    ) -> Result<Option<Entry>, Error> {
        entry_id(key)?;
        let max_bytes = max_bytes.into_iter().chain(read_limit(key)).min();
        let stored = match max_bytes {
            Some(limit) => self.get_bounded(key, limit)?,
            None => self.get(key)?,
        };
        stored
            .map(|entry| {
                Ok(Entry {
                    value: unpack(entry.value, max_bytes)?,
                    version: entry.version,
                })
            })
            .transpose()
    }

    pub fn transcript_packing_start(&self) -> Result<PackingCursor, Error> {
        Ok(PackingCursor {
            through_version: self.put_version()?,
            after: None,
        })
    }

    /// Reclaim storage used by eligible immutable entries while retaining their
    /// exact logical bytes and transaction evidence. This explicitly opts into
    /// the packed format, which requires a compatible runtime to read.
    pub fn pack_transcript_history(
        &self,
        cursor: &PackingCursor,
        limit: usize,
    ) -> Result<PackingPage, Error> {
        self.pack_transcript_page(cursor, None, limit)
    }

    /// Pack one page of entries that have aged past the persisted boundary.
    /// A page interrupted before its checkpoint can be packed again safely.
    pub fn sweep_transcript_history(
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
        let through = self.aged_packing_boundary(WINDOW, now_ms, retain_ms)?;
        if sweep.completed > through {
            return Err(Error::InvalidSnapshot);
        }
        let cursor = match sweep.cursor.take() {
            Some(cursor)
                if cursor.through_version > sweep.completed
                    && cursor.through_version <= through =>
            {
                cursor
            }
            Some(_) => return Err(Error::InvalidSnapshot),
            None if through == sweep.completed => return Ok(()),
            None => PackingCursor {
                through_version: through,
                after: None,
            },
        };
        let page = self.pack_transcript_page(&cursor, Some(sweep.completed), limit)?;
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

    fn pack_transcript_page(
        &self,
        cursor: &PackingCursor,
        since_version: Option<u64>,
        limit: usize,
    ) -> Result<PackingPage, Error> {
        if !(1..=MAX_LIMIT).contains(&limit) || cursor.through_version > self.put_version()? {
            return Err(Error::InvalidSnapshot);
        }
        let after = cursor.after.map_or_else(|| PREFIX.to_vec(), key_for);
        let page = self.packing_candidates(
            PREFIX,
            &after,
            since_version.map(|since| (since, cursor.through_version)),
            since_version.map(|_| PREFIX.len() + 16),
            limit,
        )?;
        let next = page
            .last()
            .filter(|_| page.len() == limit)
            .map(|(key, _)| -> Result<PackingCursor, Error> {
                Ok(PackingCursor {
                    through_version: cursor.through_version,
                    after: Some(entry_id(key)?),
                })
            })
            .transpose()?;
        let mut packed = 0;
        let mut reclaimed_bytes = 0;
        for (key, version) in &page {
            entry_id(key)?;
            if *version > cursor.through_version {
                continue;
            }
            if let Some(saved) = self.pack_stored_entry(key, *version)? {
                packed += 1;
                reclaimed_bytes += saved;
            }
        }
        Ok(PackingPage {
            examined: page.len(),
            packed,
            reclaimed_bytes,
            next,
        })
    }

    pub(crate) fn packing_candidates(
        &self,
        prefix: &[u8],
        after: &[u8],
        versions: Option<(u64, u64)>,
        exact_key_bytes: Option<usize>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, u64)>, Error> {
        let end = prefix_end(prefix).ok_or(Error::InvalidSnapshot)?;
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let mut stmt = conn.prepare(
            "SELECT key, version FROM durable_store_entries
             WHERE key>?1 AND key<?2 AND (?3 IS NULL OR (version>?3 AND version<=?4))
               AND (?5 IS NULL OR length(key)=?5) ORDER BY key LIMIT ?6",
        )?;
        let (since, through) = versions.unzip();
        let rows = stmt.query_map(
            params![after, end, since, through, exact_key_bytes, limit],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, u64>(1)?)),
        )?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub(crate) fn pack_stored_entry(
        &self,
        key: &[u8],
        version: u64,
    ) -> Result<Option<usize>, Error> {
        let raw: Option<Vec<u8>> = {
            let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
            conn.query_row(
                "SELECT value FROM durable_store_entries
                 WHERE key=?1 AND version=?2 AND length(value) BETWEEN ?3 AND ?4
                   AND substr(value, 1, ?5)<>?6",
                params![key, version, MIN_BYTES, MAX_BYTES, MAGIC.len(), MAGIC],
                |row| row.get(0),
            )
            .optional()?
        };
        let Some(raw) = raw else { return Ok(None) };
        let Some(representation) = packed_bytes(&raw)? else {
            return Ok(None);
        };
        if unpack(representation.clone(), Some(raw.len()))? != raw {
            return Err(Error::InvalidSnapshot);
        }
        // Another worker can pack this same logical version after our read.
        let saved = raw.len() - representation.len();
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        #[cfg(feature = "experimental-graph-revision")]
        let _protocol = self.graph_protocol_write_scope();
        let updated = conn.execute(
            "UPDATE durable_store_entries SET value=?1
             WHERE key=?2 AND version=?3 AND value=?4",
            params![representation, key, version, raw],
        )?;
        Ok((updated == 1).then_some(saved))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aged_transcript_sweep_resumes_and_preserves_logical_entries() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("transcript.sqlite");
        let text = b"transcript content".repeat(512);
        let key = |id| key_for(Uuid::from_u128(id));
        let put = |store: &Store, id| {
            store
                .commit(
                    &[],
                    &[Mutation::Put {
                        key: key(id),
                        value: text.clone(),
                    }],
                )
                .unwrap();
        };
        let stored = |store: &Store, id| store.get(&key(id)).unwrap().unwrap();
        let checkpoint = |store: &Store| -> Sweep {
            serde_json::from_slice(&store.get(SWEEP).unwrap().unwrap().value).unwrap()
        };
        let store = Store::open(&path).unwrap();
        put(&store, 2);
        put(&store, 3);
        let through = store.put_version().unwrap();
        let before = stored(&store, 3);
        let sequence = store.commit_for_version(&key(3), before.version).unwrap();
        store.sweep_transcript_history(100, 20, 1).unwrap();
        put(&store, 1);
        store.sweep_transcript_history(99, 20, 1).unwrap();
        store.sweep_transcript_history(119, 20, 1).unwrap();
        assert_eq!(stored(&store, 2).value, text);
        store.sweep_transcript_history(120, 20, 1).unwrap();
        assert!(stored(&store, 2).value.starts_with(MAGIC));
        let saved = checkpoint(&store).cursor.unwrap();
        assert_eq!(saved.after, Some(Uuid::from_u128(2)));
        assert_eq!(saved.through_version, through);
        assert_eq!(stored(&store, 3), before);
        assert_eq!(stored(&store, 1).value, text);
        // The row may be packed before the page checkpoint is committed.
        assert_eq!(
            store
                .pack_transcript_page(&saved, Some(0), 1)
                .unwrap()
                .packed,
            1
        );
        drop(store);

        let store = Store::open(&path).unwrap();
        store.sweep_transcript_history(121, 20, 1).unwrap();
        assert_eq!(
            checkpoint(&store).cursor.unwrap().after,
            Some(Uuid::from_u128(3))
        );
        assert_eq!(
            store.get_transcript_entry(&key(3), None).unwrap().unwrap(),
            before
        );
        assert_eq!(
            store.commit_for_version(&key(3), before.version).unwrap(),
            sequence
        );
        store.sweep_transcript_history(122, 20, 1).unwrap();
        assert_eq!(checkpoint(&store).completed, through);
        assert!(checkpoint(&store).cursor.is_none());
        assert_eq!(stored(&store, 1).value, text);
        store.sweep_transcript_history(140, 20, 1).unwrap();
        assert!(stored(&store, 1).value.starts_with(MAGIC));
    }

    #[test]
    fn packing_page_advances_past_ineligible_keys_and_later_writes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("packing.sqlite")).unwrap();
        let text = b"transcript fixture".repeat(400);
        let put = |id, value| crate::Mutation::Put {
            key: key_for(Uuid::from_u128(id)),
            value,
        };
        store
            .commit(
                &[],
                &[
                    put(1, b"too short".to_vec()),
                    put(2, packed_bytes(&text).unwrap().unwrap()),
                    put(3, text.clone()),
                ],
            )
            .unwrap();
        let mut cursor = store.transcript_packing_start().unwrap();
        store.commit(&[], &[put(4, text)]).unwrap();

        for (id, packed) in [(1, 0), (2, 0), (3, 1), (4, 0)] {
            let page = store.pack_transcript_history(&cursor, 1).unwrap();
            assert_eq!((page.examined, page.packed), (1, packed));
            cursor = page.next.unwrap();
            assert_eq!(cursor.after, Some(Uuid::from_u128(id)));
        }
        let end = store.pack_transcript_history(&cursor, 1).unwrap();
        assert_eq!(end.examined, 0);
        assert!(end.next.is_none());
    }

    #[cfg(all(feature = "experimental-graph-revision", unix))]
    #[test]
    fn packing_preserves_v4_write_guard_and_commit_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("packed-v4.sqlite");
        let store = Store::open(&path).unwrap();
        let key = key_for(Uuid::from_u128(1));
        let value = b"transcript fixture".repeat(400);
        store
            .commit(
                &[],
                &[crate::Mutation::Put {
                    key: key.clone(),
                    value: value.clone(),
                }],
            )
            .unwrap();
        let version = store.get(&key).unwrap().unwrap().version;
        let commit = store.commit_for_version(&key, version).unwrap();
        let cursor = store.transcript_packing_start().unwrap();
        assert_eq!(store.pack_transcript_history(&cursor, 1).unwrap().packed, 1);
        let logical = store.get_transcript_entry(&key, None).unwrap().unwrap();
        assert_eq!(logical, Entry { value, version });
        assert_eq!(store.commit_for_version(&key, version).unwrap(), commit);
        let connection = rusqlite::Connection::open(path).unwrap();
        assert!(connection
            .execute(
                "UPDATE durable_store_entries SET value=X'00' WHERE key=?1",
                [&key]
            )
            .is_err());
    }

    #[test]
    fn packed_transcript_enforces_declared_length_and_checks_digest() {
        let raw = b"stored transcript".repeat(400);
        let packed = packed_bytes(&raw).unwrap().unwrap();
        assert_eq!(unpack(packed.clone(), Some(raw.len())).unwrap(), raw);
        assert!(unpack(packed.clone(), Some(raw.len() - 1)).is_err());

        let mut digest_corrupted = packed.clone();
        digest_corrupted[MAGIC.len() + 4] ^= 1;
        assert!(unpack(digest_corrupted, None).is_err());

        let mut trailing = packed;
        trailing.push(0);
        assert!(unpack(trailing, None).is_err());
    }
}
