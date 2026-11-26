//! Optional lossless packing of the runtime's immutable transcript entries.
//! Heads, entry IDs and commit metadata are outside this representation change.

use std::io::{Read, Write};

use flate2::{bufread::ZlibDecoder, write::ZlibEncoder, Compression};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{prefix_end, read_limit, Entry, Error, Store};

const PREFIX: &[u8] = b"durable/transcript/entry/";
const MAGIC: &[u8] = b"\x89PI-TRANSCRIPT\0\x01";
const HEADER: usize = MAGIC.len() + 4 + 32;
const MIN_BYTES: usize = 1024;
const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_LIMIT: usize = 64;

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

fn unpack(value: Vec<u8>, max_bytes: Option<usize>) -> Result<Vec<u8>, Error> {
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

impl Store {
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
        if !(1..=MAX_LIMIT).contains(&limit) || cursor.through_version > self.put_version()? {
            return Err(Error::InvalidSnapshot);
        }
        let after = cursor.after.map_or_else(|| PREFIX.to_vec(), key_for);
        let end = prefix_end(PREFIX).ok_or(Error::InvalidSnapshot)?;
        let page = {
            let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
            let mut stmt = conn.prepare(
                "SELECT key, version FROM durable_store_entries
                 WHERE key>?1 AND key<?2
                 ORDER BY key LIMIT ?3",
            )?;
            let rows = stmt.query_map(params![after, end, limit], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, u64>(1)?))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
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
            let Some(raw) = raw else { continue };
            let Some(representation) = packed_bytes(&raw)? else {
                continue;
            };
            if unpack(representation.clone(), Some(raw.len()))? != raw {
                return Err(Error::InvalidSnapshot);
            }
            // The CAS includes raw bytes because another packer may have used
            // this same logical version. No logical read has been changed.
            let saved = raw.len() - representation.len();
            let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
            #[cfg(feature = "experimental-graph-revision")]
            let _protocol = self.graph_protocol_write_scope();
            if conn.execute(
                "UPDATE durable_store_entries SET value=?1
                 WHERE key=?2 AND version=?3 AND value=?4",
                params![representation, key, version, raw],
            )? == 1
            {
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
