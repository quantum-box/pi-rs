//! Incremental durable transcript storage used by the runtime.
//!
//! Reserve `durable/transcript/` for this module. Entries are immutable and
//! heads may only be changed with these operations. A fork shares its parent's
//! captured head; an interior runtime fork may copy a verified prefix projection. Reads walk
//! newest-first from a captured head and perform at most `limit` entry reads.
//! Each page is stable across subsequent appends and forks under this namespace
//! contract. Storage and payload sizes are otherwise unbounded.
//!
//! The public fork operation captures the current head; the runtime also forks
//! trusted historical cursors. Forward pagination, deletion, garbage collection, cross-database references, and
//! concurrent writers bypassing this API are not supported. Corruption checks
//! validate the portions visited, not the entire database; they cannot detect a
//! valid-looking replacement by an external writer. Store operations are blocking.
//! A storage error during commit can have an unknown outcome: inspect the head
//! before retrying. Callers must retry conflicts with freshly prepared batches.

use durable_store::{Check, Expected, Mutation, Store};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::num::NonZeroUsize;
use uuid::Uuid;

const PREFIX: &[u8] = b"durable/transcript/";
const DISPLAY_FORK_RECORD_LIMIT: usize = 8 * 1024 * 1024;

thread_local! {
    static BOUNDED_FORK_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    #[cfg(test)]
    pub(crate) static TEST_UNBOUNDED_ENTRY_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    #[cfg(test)]
    pub(crate) static TEST_ENTRY_DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Restrict transcript entry reads for the entire synchronous display fork,
/// including nested hydration and reset proof reconstruction.
pub(crate) struct BoundedForkReads;
impl BoundedForkReads {
    pub(crate) fn enter() -> Self {
        BOUNDED_FORK_READS.with(|depth| depth.set(depth.get() + 1));
        Self
    }
}
impl Drop for BoundedForkReads {
    fn drop(&mut self) {
        BOUNDED_FORK_READS.with(|depth| depth.set(depth.get() - 1));
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error("invalid or inconsistent transcript data")]
    Corrupt,
    #[error("transcript does not exist")]
    Missing,
    #[error("ordered transcript content blocks are unsupported by this operation")]
    UnsupportedContentBlocks,
    #[error("transcript sequence exhausted")]
    SequenceExhausted,
    #[error("cursor belongs to another transcript")]
    WrongTranscript,
    #[error("a transcript cannot fork itself")]
    SelfFork,
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Link {
    id: Uuid,
    sequence: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Head {
    format: u8,
    tip: Option<Link>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    format: u8,
    id: Uuid,
    sequence: u64,
    previous: Option<Link>,
    value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_blocks: Option<Vec<MessageContentBlocks>>,
}

/// Ordered content for one user or tool-message occurrence in a runtime transcript change.
/// The message identity is validated against the corresponding record value.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MessageContentBlocks {
    pub message_id: Uuid,
    pub blocks: Vec<Value>,
}

/// An opaque in-process position in a captured transcript snapshot.
/// Serialized cursors are intended for trusted, durable host records.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    transcript: Uuid,
    next: Option<Link>,
}

impl Cursor {
    pub(crate) fn transcript_id(&self) -> Uuid {
        self.transcript
    }

    pub(crate) fn entry_identity(&self) -> Option<(Uuid, u64)> {
        self.next.map(|link| (link.id, link.sequence))
    }
}
#[derive(Debug)]
pub struct Record {
    pub sequence: u64,
    pub value: Value,
    pub content_blocks: Option<Vec<MessageContentBlocks>>,
}
#[derive(Debug)]
pub struct Page {
    pub records: Vec<Record>,
    pub next: Cursor,
    pub has_more: bool,
}

/// A snapshot expected at an intermediate transcript position. The expected
/// state must be derived independently by the caller from a trusted baseline.
#[derive(Clone, Debug)]
pub struct AtomicSnapshotProof {
    pub cursor: Cursor,
    pub snapshot_key: Vec<u8>,
    pub expected_snapshot: Value,
}

/// Add all checks and mutations intact to a single host transaction.
/// Do not commit multiple independently prepared operations for the same head
/// in one batch: checks are evaluated before any mutation.
pub struct Batch {
    pub checks: Vec<Check>,
    pub mutations: Vec<Mutation>,
}

pub struct Transcript<'a> {
    store: &'a Store,
    id: Uuid,
}
impl<'a> Transcript<'a> {
    pub fn new(store: &'a Store, id: Uuid) -> Self {
        Self { store, id }
    }

    /// Locate and validate the exact entry addressed by a cursor. This does
    /// not prove that the entry is reachable or authorize a fork.
    pub(crate) fn entry_evidence(&self, cursor: &Cursor) -> Result<(Vec<u8>, u64), Error> {
        if cursor.transcript != self.id {
            return Err(Error::WrongTranscript);
        }
        let link = cursor.next.ok_or(Error::Corrupt)?;
        let key = entry_key(link.id);
        let stored = self
            .store
            .get_bounded(&key, DISPLAY_FORK_RECORD_LIMIT)?
            .ok_or(Error::Corrupt)?;
        decode_entry(link, &stored.value)?;
        Ok((key, stored.version))
    }

    /// Capture the versions of every source entry before a host reads a fork
    /// seed. The host must commit these checks with its child publication.
    pub(crate) fn fork_source_checks(&self, cursor: &Cursor) -> Result<Vec<Check>, Error> {
        if cursor.transcript != self.id {
            return Err(Error::WrongTranscript);
        }
        let mut next = cursor.next;
        let mut checks = Vec::new();
        while let Some(link) = next {
            let key = entry_key(link.id);
            let stored = self
                .store
                .get_bounded(&key, DISPLAY_FORK_RECORD_LIMIT)?
                .ok_or(Error::Corrupt)?;
            let entry = decode_entry(link, &stored.value)?;
            checks.push(Check {
                key,
                expected: Expected::Version(stored.version),
            });
            next = entry.previous;
        }
        Ok(checks)
    }

    /// Create an empty transcript, failing atomically if its ID already exists.
    pub fn prepare_create(&self) -> Result<Batch, Error> {
        let key = head_key(self.id);
        Ok(Batch {
            checks: vec![Check {
                key: key.clone(),
                expected: Expected::Missing,
            }],
            mutations: vec![Mutation::Put {
                key,
                value: encode(&Head {
                    format: 1,
                    tip: None,
                })?,
            }],
        })
    }

    /// Prepare an append against the observed head. The head check and the
    /// entry's missing check prevent lost updates and accidental overwrites.
    pub fn prepare_append(&self, value: &Value) -> Result<Batch, Error> {
        self.prepare_write(value, false).map(|(batch, _)| batch)
    }

    /// Prepare a first record or an append and return its immutable position.
    pub(crate) fn prepare_write(
        &self,
        value: &Value,
        create: bool,
    ) -> Result<(Batch, Cursor), Error> {
        self.prepare_write_with_blocks(value, None, create)
    }

    /// Prepare an append carrying lossless ordered block arrays. A nonempty
    /// collection is required; individual message arrays may be empty.
    pub fn prepare_append_with_blocks(
        &self,
        value: &Value,
        blocks: Vec<MessageContentBlocks>,
    ) -> Result<Batch, Error> {
        self.prepare_write_with_blocks(value, Some(blocks), false)
            .map(|(batch, _)| batch)
    }

    pub(crate) fn prepare_write_with_blocks(
        &self,
        value: &Value,
        blocks: Option<Vec<MessageContentBlocks>>,
        create: bool,
    ) -> Result<(Batch, Cursor), Error> {
        let (batch, mut positions) = self.prepare_writes(&[(value.clone(), blocks)], create)?;
        Ok((batch, positions.pop().expect("one transcript record")))
    }

    /// Prepare ordered immutable entries under one checked head update.
    /// Positions correspond to the input records and can be used to fork at
    /// an intermediate entry after the batch commits. An empty batch is invalid.
    pub fn prepare_append_batch(
        &self,
        records: &[(Value, Option<Vec<MessageContentBlocks>>)],
    ) -> Result<(Batch, Vec<Cursor>), Error> {
        self.prepare_writes(records, false)
    }

    fn prepare_writes(
        &self,
        records: &[(Value, Option<Vec<MessageContentBlocks>>)],
        create: bool,
    ) -> Result<(Batch, Vec<Cursor>), Error> {
        if records.is_empty()
            || records.iter().any(|(value, blocks)| {
                blocks
                    .as_ref()
                    .is_some_and(|blocks| !valid_block_bindings(value, blocks))
            })
        {
            return Err(Error::Corrupt);
        }
        let (head, expected) = if create {
            (
                Head {
                    format: 1,
                    tip: None,
                },
                Expected::Missing,
            )
        } else {
            let (head, version) = self.load_head()?;
            (head, Expected::Version(version))
        };
        if let Some(tip) = head.tip {
            self.load_entry(tip)?;
        }
        let head_key = head_key(self.id);
        let mut checks = vec![Check {
            key: head_key.clone(),
            expected,
        }];
        let mut mutations = Vec::with_capacity(records.len() + 1);
        let mut positions = Vec::with_capacity(records.len());
        let mut previous = head.tip;
        let mut keys = std::collections::HashSet::new();
        for (value, blocks) in records {
            let sequence = previous
                .map_or(Some(1), |link| link.sequence.checked_add(1))
                .ok_or(Error::SequenceExhausted)?;
            let tip = Link {
                id: Uuid::new_v4(),
                sequence,
            };
            let key = entry_key(tip.id);
            if !keys.insert(tip.id) {
                return Err(Error::Corrupt);
            }
            let entry = Entry {
                format: if blocks.is_some() { 2 } else { 1 },
                id: tip.id,
                sequence,
                previous,
                value: value.clone(),
                content_blocks: blocks.clone(),
            };
            checks.push(Check {
                key: key.clone(),
                expected: Expected::Missing,
            });
            mutations.push(Mutation::Put {
                key,
                value: encode(&entry)?,
            });
            positions.push(Cursor {
                transcript: self.id,
                next: Some(tip),
            });
            previous = Some(tip);
        }
        mutations.push(Mutation::Put {
            key: head_key,
            value: encode(&Head {
                format: 1,
                tip: previous,
            })?,
        });
        Ok((Batch { checks, mutations }, positions))
    }

    /// Verify that a nonempty contiguous append chain and an application-owned
    /// anchor were written in the same store commit. The caller must validate
    /// the anchor's contents, namespace, and relationship to application state.
    /// A later append to this transcript does not invalidate the proof.
    pub fn verify_atomic_append(
        &self,
        anchor_key: &[u8],
        prior: &Cursor,
        positions: &[Cursor],
    ) -> Result<(), Error> {
        self.verify_atomic_chain(anchor_key, prior, positions)
            .map(|_| ())
    }

    /// Return checks for a verified chain so the caller can fence all of its
    /// evidence in the transaction that publishes the dependent result.
    #[allow(dead_code)] // Used by the future authenticated fork commit.
    pub(crate) fn atomic_append_checks(
        &self,
        anchor_key: &[u8],
        prior: &Cursor,
        positions: &[Cursor],
    ) -> Result<Vec<Check>, Error> {
        let (commit, anchor_version) = self.verify_atomic_chain(anchor_key, prior, positions)?;
        let mut checks = vec![Check {
            key: anchor_key.to_vec(),
            expected: Expected::Version(anchor_version),
        }];
        for position in positions {
            let (key, version) = self.entry_evidence(position)?;
            if self.store.commit_for_version(&key, version)? != Some(commit) {
                return Err(Error::Corrupt);
            }
            checks.push(Check {
                key,
                expected: Expected::Version(version),
            });
        }
        self.store.commit(&checks, &[])?;
        Ok(checks)
    }

    fn verify_atomic_chain(
        &self,
        anchor_key: &[u8],
        prior: &Cursor,
        positions: &[Cursor],
    ) -> Result<(u64, u64), Error> {
        if prior.transcript != self.id || positions.is_empty() {
            return Err(Error::Corrupt);
        }
        let anchor = self.store.get(anchor_key)?.ok_or(Error::Corrupt)?;
        let commit = self
            .store
            .commit_for_version(anchor_key, anchor.version)?
            .ok_or(Error::Corrupt)?;
        let mut previous = prior.next;
        for position in positions {
            if position.transcript != self.id {
                return Err(Error::Corrupt);
            }
            let link = position.next.ok_or(Error::Corrupt)?;
            let key = entry_key(link.id);
            let (entry, stored) = self.load_entry_with_version(link)?;
            if entry.previous != previous
                || self.store.commit_for_version(&key, stored.version)? != Some(commit)
            {
                return Err(Error::Corrupt);
            }
            previous = Some(link);
        }
        Ok((commit, anchor.version))
    }

    /// Verify exact independently derived snapshots and their cursor bindings
    /// were committed together with the ordered entries and anchor. The caller
    /// also checks the anchor's captured document revision. The caller must
    /// independently derive states and the expected revision, authenticate the
    /// anchor, and preserve immutability of these storage namespaces.
    pub fn verify_atomic_snapshots(
        &self,
        anchor_key: &[u8],
        prior: &Cursor,
        proofs: &[AtomicSnapshotProof],
        document_revision: u64,
    ) -> Result<(), Error> {
        if self.store.document_cut(anchor_key)? != Some(document_revision) {
            return Err(Error::Corrupt);
        }
        let cursors: Vec<_> = proofs.iter().map(|proof| proof.cursor.clone()).collect();
        let (commit, anchor_version) = self.verify_atomic_chain(anchor_key, prior, &cursors)?;
        let mut keys = std::collections::HashSet::new();
        for proof in proofs {
            if proof.snapshot_key.as_slice() == anchor_key || !keys.insert(&proof.snapshot_key) {
                return Err(Error::Corrupt);
            }
            let snapshot = self.store.get(&proof.snapshot_key)?.ok_or(Error::Corrupt)?;
            let value: Value = decode(&snapshot.value)?;
            let bound_cursor: Cursor =
                serde_json::from_value(value.get("_transcript").cloned().ok_or(Error::Corrupt)?)
                    .map_err(|_| Error::Corrupt)?;
            if value != proof.expected_snapshot
                || bound_cursor != proof.cursor
                || self
                    .store
                    .commit_for_version(&proof.snapshot_key, snapshot.version)?
                    != Some(commit)
            {
                return Err(Error::Corrupt);
            }
        }
        if self
            .store
            .get(anchor_key)?
            .is_none_or(|entry| entry.version != anchor_version)
        {
            return Err(Error::Corrupt);
        }
        Ok(())
    }

    /// Fork the current head. A simultaneous source append makes this batch
    /// conflict, so the caller can choose the new head explicitly on retry.
    /// Scans and validates the captured immutable chain, including block bindings.
    pub fn prepare_fork(&self, child: Uuid) -> Result<Batch, Error> {
        if child == self.id {
            return Err(Error::SelfFork);
        }
        let (head, version) = self.load_head()?;
        self.validate_chain(head.tip)?;
        let child_key = head_key(child);
        Ok(Batch {
            checks: vec![
                Check {
                    key: head_key(self.id),
                    expected: Expected::Version(version),
                },
                Check {
                    key: child_key.clone(),
                    expected: Expected::Missing,
                },
            ],
            mutations: vec![Mutation::Put {
                key: child_key,
                value: encode(&head)?,
            }],
        })
    }

    /// Fork a previously captured immutable position for a host-owned child.
    pub(crate) fn prepare_fork_at(
        &self,
        cursor: &Cursor,
        child: Uuid,
    ) -> Result<(Batch, Cursor), Error> {
        if cursor.transcript != self.id {
            return Err(Error::WrongTranscript);
        }
        if child == self.id {
            return Err(Error::SelfFork);
        }
        self.validate_chain(cursor.next)?;
        let key = head_key(child);
        Ok((
            Batch {
                checks: vec![Check {
                    key: key.clone(),
                    expected: Expected::Missing,
                }],
                mutations: vec![Mutation::Put {
                    key,
                    value: encode(&Head {
                        format: 1,
                        tip: cursor.next,
                    })?,
                }],
            },
            Cursor {
                transcript: child,
                next: cursor.next,
            },
        ))
    }

    /// Atomically fork an immutable source and append a validated projection.
    pub(crate) fn prepare_fork_projection(
        &self,
        cursor: &Cursor,
        child: Uuid,
        value: &Value,
        blocks: Vec<MessageContentBlocks>,
    ) -> Result<(Batch, Cursor), Error> {
        if cursor.transcript != self.id {
            return Err(Error::WrongTranscript);
        }
        if child == self.id {
            return Err(Error::SelfFork);
        }
        if value.get("ForkCut").is_none()
            || (!blocks.is_empty() && !valid_block_bindings(value, &blocks))
        {
            return Err(Error::Corrupt);
        }
        self.validate_chain(cursor.next)?;
        let sequence = cursor
            .next
            .and_then(|tip| tip.sequence.checked_add(1))
            .ok_or(Error::SequenceExhausted)?;
        let tip = Link {
            id: Uuid::new_v4(),
            sequence,
        };
        let entry = Entry {
            format: if blocks.is_empty() { 1 } else { 2 },
            id: tip.id,
            sequence,
            previous: cursor.next,
            value: value.clone(),
            content_blocks: if blocks.is_empty() {
                None
            } else {
                Some(blocks)
            },
        };
        let head_key = head_key(child);
        let entry_key = entry_key(tip.id);
        Ok((
            Batch {
                checks: vec![
                    Check {
                        key: head_key.clone(),
                        expected: Expected::Missing,
                    },
                    Check {
                        key: entry_key.clone(),
                        expected: Expected::Missing,
                    },
                ],
                mutations: vec![
                    Mutation::Put {
                        key: entry_key,
                        value: encode(&entry)?,
                    },
                    Mutation::Put {
                        key: head_key,
                        value: encode(&Head {
                            format: 1,
                            tip: Some(tip),
                        })?,
                    },
                ],
            },
            Cursor {
                transcript: child,
                next: Some(tip),
            },
        ))
    }

    /// Capture a stable position; later writes do not change this snapshot.
    pub fn snapshot(&self) -> Result<Cursor, Error> {
        let (head, _) = self.load_head()?;
        Ok(Cursor {
            transcript: self.id,
            next: head.tip,
        })
    }

    /// Read newest-first, returning no more than `limit` records. Pages may
    /// cross any number of fork boundaries without loading an ancestry table.
    /// Rejects records with blocks; use `read_with_blocks` for lossless access.
    pub fn read(&self, cursor: &Cursor, limit: NonZeroUsize) -> Result<Page, Error> {
        let page = self.read_with_blocks(cursor, limit)?;
        if page
            .records
            .iter()
            .any(|record| record.content_blocks.is_some())
        {
            return Err(Error::UnsupportedContentBlocks);
        }
        Ok(page)
    }

    /// Read losslessly, including versioned ordered content blocks.
    pub fn read_with_blocks(&self, cursor: &Cursor, limit: NonZeroUsize) -> Result<Page, Error> {
        if cursor.transcript != self.id {
            return Err(Error::WrongTranscript);
        }
        let mut next = cursor.next;
        let mut records = Vec::new();
        while records.len() < limit.get() {
            let Some(link) = next else { break };
            let entry = self.load_entry(link)?;
            next = entry.previous;
            records.push(Record {
                sequence: entry.sequence,
                value: entry.value,
                content_blocks: entry.content_blocks,
            });
        }
        Ok(Page {
            records,
            next: Cursor {
                transcript: self.id,
                next,
            },
            has_more: next.is_some(),
        })
    }

    fn validate_chain(&self, mut link: Option<Link>) -> Result<(), Error> {
        while let Some(current) = link {
            link = self.load_entry(current)?.previous;
        }
        Ok(())
    }

    fn load_head(&self) -> Result<(Head, u64), Error> {
        let stored = self.store.get(&head_key(self.id))?.ok_or(Error::Missing)?;
        let head: Head = decode(&stored.value)?;
        if head.format != 1 || head.tip.is_some_and(|tip| tip.sequence == 0) {
            return Err(Error::Corrupt);
        }
        Ok((head, stored.version))
    }
    fn load_entry(&self, link: Link) -> Result<Entry, Error> {
        self.load_entry_with_version(link).map(|(entry, _)| entry)
    }
    fn load_entry_with_version(&self, link: Link) -> Result<(Entry, durable_store::Entry), Error> {
        let key = entry_key(link.id);
        let bounded = BOUNDED_FORK_READS.with(|depth| depth.get() != 0);
        #[cfg(test)]
        if !bounded {
            TEST_UNBOUNDED_ENTRY_READS.with(|count| count.set(count.get() + 1));
        }
        let stored = if bounded {
            self.store.get_bounded(&key, DISPLAY_FORK_RECORD_LIMIT)?
        } else {
            self.store.get(&key)?
        }
        .ok_or(Error::Corrupt)?;
        #[cfg(test)]
        TEST_ENTRY_DECODES.with(|count| count.set(count.get() + 1));
        let entry = decode_entry(link, &stored.value)?;
        Ok((entry, stored))
    }
}

fn decode_entry(link: Link, value: &[u8]) -> Result<Entry, Error> {
    let entry: Entry = decode(value)?;
    if !match (entry.format, entry.content_blocks.as_ref()) {
        (1, None) => true,
        (2, Some(blocks)) => valid_block_bindings(&entry.value, blocks),
        _ => false,
    } || entry.id != link.id
        || entry.sequence != link.sequence
        || match entry.previous {
            None => entry.sequence != 1,
            Some(prev) => {
                prev.id == entry.id || prev.sequence.checked_add(1) != Some(entry.sequence)
            }
        }
    {
        return Err(Error::Corrupt);
    }
    Ok(entry)
}

fn valid_block_bindings(value: &Value, blocks: &[MessageContentBlocks]) -> bool {
    if blocks.is_empty() {
        return false;
    }
    let Some(object) = value.as_object().filter(|object| object.len() == 1) else {
        return false;
    };
    let Some((kind, data)) = object.iter().next() else {
        return false;
    };
    let Some(messages) = data.get("messages").and_then(Value::as_array) else {
        return false;
    };
    let ids: Vec<&Value> = if kind == "ForkCut" {
        let Some(histories) = data.get("histories").and_then(Value::as_array) else {
            return false;
        };
        if histories.len() != messages.len() {
            return false;
        }
        let Some(ids) = histories
            .iter()
            .map(|history| history.get("id"))
            .collect::<Option<Vec<_>>>()
        else {
            return false;
        };
        ids
    } else {
        if !matches!(
            kind.as_str(),
            "AppendV2" | "AppendV3" | "AppendV4" | "ReplaceV2"
        ) {
            return false;
        }
        let Some(ids) = data.get("ids").and_then(Value::as_array) else {
            return false;
        };
        if ids.len() != messages.len() {
            return false;
        }
        ids.iter().collect()
    };
    let mut occurrences = std::collections::HashMap::new();
    for (id, message) in ids.into_iter().zip(messages) {
        let Some(id) = id.as_str().and_then(|id| Uuid::parse_str(id).ok()) else {
            return false;
        };
        if id.is_nil() || occurrences.insert(id, message).is_some() {
            return false;
        }
    }
    let mut seen = std::collections::HashSet::new();
    blocks.iter().all(|block| {
        seen.insert(block.message_id)
            && crate::content::UserContent::from_yield(Value::Array(block.blocks.clone()), false)
                .is_some()
            && occurrences.get(&block.message_id).is_some_and(|message| {
                message.get("content").and_then(Value::as_str) == Some("")
                    && message.as_object().is_some_and(|object| {
                        match message.get("role").and_then(Value::as_str) {
                            Some("user") => object.len() == 2,
                            Some("tool") => {
                                object.len() == 3
                                    && message
                                        .get("call_id")
                                        .and_then(Value::as_str)
                                        .is_some_and(|s| !s.is_empty())
                            }
                            _ => false,
                        }
                    })
            })
    })
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    Ok(serde_json::to_vec(value)?)
}
fn decode<T: for<'de> Deserialize<'de>>(value: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(value).map_err(|_| Error::Corrupt)
}
fn head_key(id: Uuid) -> Vec<u8> {
    let mut k = PREFIX.to_vec();
    k.extend_from_slice(b"head/");
    k.extend_from_slice(id.as_bytes());
    k
}
fn entry_key(id: Uuid) -> Vec<u8> {
    let mut k = PREFIX.to_vec();
    k.extend_from_slice(b"entry/");
    k.extend_from_slice(id.as_bytes());
    k
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{Arc, Barrier};

    fn commit(db: &Store, batch: Batch) {
        db.commit(&batch.checks, &batch.mutations).unwrap();
    }
    fn create(db: &Store, id: Uuid) {
        commit(db, Transcript::new(db, id).prepare_create().unwrap());
    }
    fn append(db: &Store, id: Uuid, value: Value) {
        commit(db, Transcript::new(db, id).prepare_append(&value).unwrap());
    }
    fn read_all(t: &Transcript<'_>, mut cursor: Cursor, size: usize) -> Vec<Value> {
        let mut result = Vec::new();
        loop {
            let page = t.read(&cursor, NonZeroUsize::new(size).unwrap()).unwrap();
            result.extend(page.records.into_iter().map(|r| {
                assert!(r.sequence > 0);
                r.value
            }));
            cursor = page.next;
            if !page.has_more {
                return result;
            }
        }
    }

    #[test]
    fn atomic_snapshot_proofs_bind_exact_state_cursor_and_transaction() {
        let db = Store::memory().unwrap();
        let id = Uuid::new_v4();
        create(&db, id);
        let t = Transcript::new(&db, id);
        let prior = t.snapshot().unwrap();
        let (mut batch, cursors) = t
            .prepare_append_batch(&[(json!({"reset":1}), None), (json!({"reset":2}), None)])
            .unwrap();
        let document = crate::document::Address {
            scope: "scope".into(),
            name: "note".into(),
            key: "".into(),
        }
        .storage_key()
        .unwrap();
        batch.mutations.push(Mutation::Put {
            key: document.clone(),
            value: br#"{"version":1}"#.to_vec(),
        });
        let anchor = format!("runtime/fork/{id}/00000000000000000001").into_bytes();
        batch.mutations.push(Mutation::Put {
            key: anchor.clone(),
            value: b"runtime commit".to_vec(),
        });
        let proofs: Vec<_> = cursors.iter().enumerate().map(|(index, cursor)| AtomicSnapshotProof {
            cursor: cursor.clone(), snapshot_key: format!("test/snapshot/{index}").into_bytes(),
            expected_snapshot: json!({"_transcript":cursor,"resets":index+1,"documents":{"doc":"value"}}),
        }).collect();
        for proof in &proofs {
            batch.mutations.push(Mutation::Put {
                key: proof.snapshot_key.clone(),
                value: serde_json::to_vec(&proof.expected_snapshot).unwrap(),
            });
        }
        commit(&db, batch);
        let revision = db.document_cut(&anchor).unwrap().unwrap();
        t.verify_atomic_snapshots(&anchor, &prior, &proofs, revision)
            .unwrap();
        db.commit(
            &[],
            &[Mutation::Put {
                key: document.clone(),
                value: br#"{"version":2}"#.to_vec(),
            }],
        )
        .unwrap();
        t.verify_atomic_snapshots(&anchor, &prior, &proofs, revision)
            .unwrap();
        let historical = db.document_at(&document, revision).unwrap().unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&historical.value).unwrap(),
            json!({"version":1})
        );
        assert!(matches!(
            t.verify_atomic_snapshots(&anchor, &prior, &proofs, revision + 1),
            Err(Error::Corrupt)
        ));
        let mut forged = proofs.clone();
        forged[0].expected_snapshot["documents"]["doc"] = json!("changed");
        assert!(matches!(
            t.verify_atomic_snapshots(&anchor, &prior, &forged, revision),
            Err(Error::Corrupt)
        ));
        let mut forged = proofs.clone();
        forged[0].cursor = cursors[1].clone();
        assert!(matches!(
            t.verify_atomic_snapshots(&anchor, &prior, &forged, revision),
            Err(Error::Corrupt)
        ));
        let mut forged = proofs.clone();
        forged[1].snapshot_key = forged[0].snapshot_key.clone();
        assert!(matches!(
            t.verify_atomic_snapshots(&anchor, &prior, &forged, revision),
            Err(Error::Corrupt)
        ));
        let snapshot = db.get(&proofs[0].snapshot_key).unwrap().unwrap();
        db.commit(
            &[],
            &[Mutation::Put {
                key: proofs[0].snapshot_key.clone(),
                value: snapshot.value,
            }],
        )
        .unwrap();
        assert!(matches!(
            t.verify_atomic_snapshots(&anchor, &prior, &proofs, revision),
            Err(Error::Corrupt)
        ));
    }

    #[test]
    fn atomic_append_checks_fence_intermediate_rewrite() {
        let db = Store::memory().unwrap();
        let id = Uuid::new_v4();
        create(&db, id);
        let t = Transcript::new(&db, id);
        let prior = t.snapshot().unwrap();
        let (mut batch, cursors) = t
            .prepare_append_batch(&[(json!({"a":1}), None), (json!({"b":2}), None)])
            .unwrap();
        let anchor = b"proof/anchor".to_vec();
        batch.mutations.push(Mutation::Put {
            key: anchor.clone(),
            value: vec![],
        });
        commit(&db, batch);
        let checks = t.atomic_append_checks(&anchor, &prior, &cursors).unwrap();
        let (key, _) = t.entry_evidence(&cursors[0]).unwrap();
        let value = db.get(&key).unwrap().unwrap().value;
        db.commit(&[], &[Mutation::Put { key, value }]).unwrap();
        let fork = b"proof/fork".to_vec();
        assert!(matches!(
            db.commit(
                &checks,
                &[Mutation::Put {
                    key: fork.clone(),
                    value: vec![]
                }]
            ),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(db.get(&fork).unwrap().is_none());
    }

    #[test]
    fn atomic_append_proof_checks_anchor_chain_and_commit() {
        let db = Store::memory().unwrap();
        let id = Uuid::new_v4();
        create(&db, id);
        let t = Transcript::new(&db, id);
        let prior = t.snapshot().unwrap();
        let (mut batch, cursors) = t
            .prepare_append_batch(&[(json!({"first":1}), None), (json!({"second":2}), None)])
            .unwrap();
        let anchor = b"test/proof".to_vec();
        batch.checks.push(Check {
            key: anchor.clone(),
            expected: Expected::Missing,
        });
        batch.mutations.push(Mutation::Put {
            key: anchor.clone(),
            value: b"anchor".to_vec(),
        });
        commit(&db, batch);
        t.verify_atomic_append(&anchor, &prior, &cursors).unwrap();
        append(&db, id, json!({"later":3}));
        t.verify_atomic_append(&anchor, &prior, &cursors).unwrap();
        assert!(matches!(
            t.verify_atomic_append(&anchor, &prior, &[]),
            Err(Error::Corrupt)
        ));
        assert!(matches!(
            t.verify_atomic_append(&anchor, &prior, &[cursors[1].clone(), cursors[0].clone()]),
            Err(Error::Corrupt)
        ));
        assert!(matches!(
            t.verify_atomic_append(&anchor, &cursors[0], &cursors),
            Err(Error::Corrupt)
        ));
        assert!(matches!(
            t.verify_atomic_append(b"test/missing", &prior, &cursors),
            Err(Error::Corrupt)
        ));
        let mut foreign = cursors[0].clone();
        foreign.transcript = Uuid::new_v4();
        assert!(matches!(
            t.verify_atomic_append(&anchor, &prior, &[foreign]),
            Err(Error::Corrupt)
        ));
        db.commit(
            &[],
            &[Mutation::Put {
                key: anchor.clone(),
                value: b"replacement".to_vec(),
            }],
        )
        .unwrap();
        assert!(matches!(
            t.verify_atomic_append(&anchor, &prior, &cursors),
            Err(Error::Corrupt)
        ));
    }

    #[test]
    fn atomic_append_proof_rejects_forged_or_missing_entry() {
        let db = Store::memory().unwrap();
        let id = Uuid::new_v4();
        create(&db, id);
        let t = Transcript::new(&db, id);
        let prior = t.snapshot().unwrap();
        let (mut batch, cursors) = t.prepare_append_batch(&[(json!({"one":1}), None)]).unwrap();
        let anchor = b"test/proof".to_vec();
        batch.mutations.push(Mutation::Put {
            key: anchor.clone(),
            value: vec![],
        });
        commit(&db, batch);
        let key = entry_key(cursors[0].next.unwrap().id);
        let entry = db.get(&key).unwrap().unwrap();
        db.commit(
            &[],
            &[Mutation::Put {
                key: key.clone(),
                value: entry.value,
            }],
        )
        .unwrap();
        assert!(matches!(
            t.verify_atomic_append(&anchor, &prior, &cursors),
            Err(Error::Corrupt)
        ));
        db.commit(&[], &[Mutation::Delete { key }]).unwrap();
        assert!(matches!(
            t.verify_atomic_append(&anchor, &prior, &cursors),
            Err(Error::Corrupt)
        ));
    }

    #[test]
    fn ordered_batch_is_atomic_and_forkable_at_each_block_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let id = Uuid::new_v4();
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let child = Uuid::new_v4();
        let binding = |message_id, text: &str| MessageContentBlocks {
            message_id,
            blocks: vec![json!({"type":"text", "text":text})],
        };
        let record = |message_id| json!({"ReplaceV2":{"ids":[message_id],"messages":[{"role":"user","content":""}],"commit":1,"origin":id}});
        let records = vec![
            (record(first), Some(vec![binding(first, "first")])),
            (record(second), Some(vec![binding(second, "second")])),
        ];
        let db = Store::open(&path).unwrap();
        create(&db, id);
        let t = Transcript::new(&db, id);
        let before = t.snapshot().unwrap();
        let (batch, cursors) = t.prepare_append_batch(&records).unwrap();
        assert!(
            t.read_with_blocks(&before, NonZeroUsize::MIN)
                .unwrap()
                .records
                .is_empty()
        );
        assert!(matches!(
            t.read_with_blocks(&cursors[0], NonZeroUsize::MIN),
            Err(Error::Corrupt)
        ));
        commit(&db, batch);
        let (fork, _) = t.prepare_fork_at(&cursors[0], child).unwrap();
        commit(&db, fork);
        drop(db);
        let db = Store::open(&path).unwrap();
        let t = Transcript::new(&db, id);
        let page = t
            .read_with_blocks(&t.snapshot().unwrap(), NonZeroUsize::new(8).unwrap())
            .unwrap();
        assert_eq!(
            page.records.iter().map(|r| r.sequence).collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert_eq!(page.records[0].content_blocks, records[1].1);
        assert_eq!(page.records[1].content_blocks, records[0].1);
        let fork = Transcript::new(&db, child);
        let page = fork
            .read_with_blocks(&fork.snapshot().unwrap(), NonZeroUsize::new(8).unwrap())
            .unwrap();
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].content_blocks, records[0].1);
    }

    #[test]
    fn ordered_batch_rejects_invalid_records_and_stale_head_without_partial_entries() {
        let db = Store::memory().unwrap();
        let id = Uuid::new_v4();
        create(&db, id);
        let t = Transcript::new(&db, id);
        assert!(matches!(t.prepare_append_batch(&[]), Err(Error::Corrupt)));
        let invalid = vec![(json!({"ok": true}), None), (json!(null), Some(vec![]))];
        assert!(matches!(
            t.prepare_append_batch(&invalid),
            Err(Error::Corrupt)
        ));
        let (stale, positions) = t
            .prepare_append_batch(&[(json!({"a":1}), None), (json!({"b":2}), None)])
            .unwrap();
        append(&db, id, json!({"winner": true}));
        assert!(matches!(
            db.commit(&stale.checks, &stale.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        for cursor in positions {
            assert!(matches!(
                t.read_with_blocks(&cursor, NonZeroUsize::MIN),
                Err(Error::Corrupt)
            ));
        }
        let page = t
            .read_with_blocks(&t.snapshot().unwrap(), NonZeroUsize::new(8).unwrap())
            .unwrap();
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].value, json!({"winner": true}));
    }

    #[test]
    fn versioned_blocks_survive_append_fork_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let id = Uuid::new_v4();
        let message_id = Uuid::new_v4();
        let blocks = vec![
            json!({"type":"text","text":"a","textSignature":"opaque"}),
            json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
            json!({"type":"text","text":"after"}),
        ];
        let db = Store::open(&path).unwrap();
        create(&db, id);
        append(&db, id, json!({"legacy": true}));
        let transcript = Transcript::new(&db, id);
        let content = vec![MessageContentBlocks {
            message_id,
            blocks: blocks.clone(),
        }];
        commit(
            &db,
            transcript
                .prepare_append_with_blocks(&json!({"AppendV2":{"ids":[message_id],"messages":[{"role":"user","content":""}]}}), content.clone())
                .unwrap(),
        );
        let blocked_snapshot = transcript.snapshot().unwrap();
        append(&db, id, json!({"later": true}));
        let full_child = Uuid::new_v4();
        let historical_child = Uuid::new_v4();
        commit(&db, transcript.prepare_fork(full_child).unwrap());
        let (batch, _) = transcript
            .prepare_fork_at(&blocked_snapshot, historical_child)
            .unwrap();
        commit(&db, batch);
        drop(db);

        let db = Store::open(&path).unwrap();
        let child = Transcript::new(&db, historical_child);
        let inherited = child
            .read_with_blocks(&child.snapshot().unwrap(), NonZeroUsize::new(8).unwrap())
            .unwrap();
        assert_eq!(inherited.records[0].content_blocks, Some(content.clone()));
        let full = Transcript::new(&db, full_child);
        assert_eq!(
            full.read_with_blocks(&full.snapshot().unwrap(), NonZeroUsize::new(8).unwrap())
                .unwrap()
                .records[1]
                .content_blocks,
            Some(content.clone())
        );
        let transcript = Transcript::new(&db, id);
        let page = transcript
            .read_with_blocks(
                &transcript.snapshot().unwrap(),
                NonZeroUsize::new(8).unwrap(),
            )
            .unwrap();
        assert_eq!(page.records.len(), 3);
        assert!(page.records[0].content_blocks.is_none());
        assert_eq!(page.records[1].content_blocks, Some(content));
        assert_eq!(page.records[2].value, json!({"legacy": true}));
        assert!(page.records[2].content_blocks.is_none());
        assert!(matches!(
            transcript.read(
                &transcript.snapshot().unwrap(),
                NonZeroUsize::new(8).unwrap()
            ),
            Err(Error::UnsupportedContentBlocks)
        ));
    }

    #[test]
    fn tool_blocks_require_empty_bound_tool_with_call_id() {
        let db = Store::memory().unwrap();
        let id = Uuid::new_v4();
        create(&db, id);
        let transcript = Transcript::new(&db, id);
        let message_id = Uuid::new_v4();
        let binding = MessageContentBlocks {
            message_id,
            blocks: vec![json!({"type":"image", "data":"aGk=", "mimeType":"image/png"})],
        };
        for message in [
            json!({"role":"tool", "call_id":"", "content":""}),
            json!({"role":"tool", "call_id":"c", "content":"hidden"}),
            json!({"role":"tool", "call_id":"c", "content":"", "extra":1}),
        ] {
            let value = json!({"AppendV2":{"ids":[message_id], "messages":[message]}});
            assert!(matches!(
                transcript.prepare_append_with_blocks(&value, vec![binding.clone()]),
                Err(Error::Corrupt)
            ));
        }
        let value = json!({"AppendV2":{"ids":[message_id], "messages":[{"role":"tool", "call_id":"c", "content":""}]}});
        commit(
            &db,
            transcript
                .prepare_append_with_blocks(&value, vec![binding.clone()])
                .unwrap(),
        );
        let page = transcript
            .read_with_blocks(&transcript.snapshot().unwrap(), NonZeroUsize::MIN)
            .unwrap();
        assert_eq!(page.records[0].content_blocks, Some(vec![binding]));
    }

    #[test]
    fn empty_content_array_and_invalid_metadata() {
        let db = Store::memory().unwrap();
        let id = Uuid::new_v4();
        create(&db, id);
        let transcript = Transcript::new(&db, id);
        assert!(matches!(
            transcript.prepare_append_with_blocks(&json!(null), vec![]),
            Err(Error::Corrupt)
        ));
        let invalid = MessageContentBlocks {
            message_id: Uuid::nil(),
            blocks: vec![],
        };
        assert!(matches!(
            transcript.prepare_append_with_blocks(&json!(null), vec![invalid]),
            Err(Error::Corrupt)
        ));
        let empty = MessageContentBlocks {
            message_id: Uuid::new_v4(),
            blocks: vec![],
        };
        let change = json!({"AppendV2": {"ids": [empty.message_id], "messages": [{"role":"user","content":""}]}});
        assert!(matches!(
            transcript.prepare_append_with_blocks(&change, vec![empty.clone(), empty.clone()]),
            Err(Error::Corrupt)
        ));
        commit(
            &db,
            transcript
                .prepare_append_with_blocks(&change, vec![empty.clone()])
                .unwrap(),
        );
        let page = transcript
            .read_with_blocks(&transcript.snapshot().unwrap(), NonZeroUsize::MIN)
            .unwrap();
        assert_eq!(page.records[0].content_blocks, Some(vec![empty]));
    }
    #[test]
    fn blocks_must_bind_to_one_user_occurrence_on_write_and_read() {
        let db = Store::memory().unwrap();
        let id = Uuid::new_v4();
        create(&db, id);
        let transcript = Transcript::new(&db, id);
        let message_id = Uuid::new_v4();
        let other_id = Uuid::new_v4();
        let content = vec![MessageContentBlocks {
            message_id,
            blocks: vec![],
        }];
        let good =
            json!({"AppendV2": {"ids": [message_id], "messages": [{"role":"user","content":""}]}});
        let wrong_id =
            json!({"AppendV2": {"ids": [other_id], "messages": [{"role":"user","content":""}]}});
        let wrong_role = json!({"AppendV2": {"ids": [message_id], "messages": [{"role":"assistant","content":""}]}});
        let repeated = json!({"AppendV2": {"ids": [message_id, message_id], "messages": [{"role":"user"}, {"role":"user"}]}});
        for value in [wrong_id, wrong_role, repeated, json!({"ids":[message_id]})] {
            assert!(matches!(
                transcript.prepare_append_with_blocks(&value, content.clone()),
                Err(Error::Corrupt)
            ));
        }
        commit(
            &db,
            transcript
                .prepare_append_with_blocks(&good, content)
                .unwrap(),
        );
        let cursor = transcript.snapshot().unwrap();
        let link = cursor.next.unwrap();
        let mut entry = transcript.load_entry(link).unwrap();
        entry.value["AppendV2"]["ids"][0] = json!(other_id);
        db.commit(
            &[],
            &[Mutation::Put {
                key: entry_key(link.id),
                value: encode(&entry).unwrap(),
            }],
        )
        .unwrap();
        assert!(matches!(
            transcript.read_with_blocks(&cursor, NonZeroUsize::MIN),
            Err(Error::Corrupt)
        ));
        assert!(matches!(
            transcript.prepare_fork(Uuid::new_v4()),
            Err(Error::Corrupt)
        ));
        assert!(matches!(
            transcript.prepare_fork_at(&cursor, Uuid::new_v4()),
            Err(Error::Corrupt)
        ));
    }

    #[test]
    fn rejects_conflicting_placeholders_and_malformed_blocks_on_write_and_read() {
        let db = Store::memory().unwrap();
        let id = Uuid::new_v4();
        create(&db, id);
        let transcript = Transcript::new(&db, id);
        let message_id = Uuid::new_v4();
        let good =
            json!({"AppendV2": {"ids": [message_id], "messages": [{"role":"user","content":""}]}});
        let binding = MessageContentBlocks {
            message_id,
            blocks: vec![json!({"type":"text", "text":"kept"})],
        };
        for message in [
            json!({"role":"user", "content":"hidden"}),
            json!({"role":"user_with_images", "content":"", "images":[]}),
            json!({"role":"user", "content":"", "extra":"hidden"}),
        ] {
            let mut value = good.clone();
            value["AppendV2"]["messages"][0] = message;
            assert!(matches!(
                transcript.prepare_append_with_blocks(&value, vec![binding.clone()]),
                Err(Error::Corrupt)
            ));
        }
        let invalid = MessageContentBlocks {
            message_id,
            blocks: vec![json!({"type":"text"})],
        };
        assert!(matches!(
            transcript.prepare_append_with_blocks(&good, vec![invalid.clone()]),
            Err(Error::Corrupt)
        ));
        commit(
            &db,
            transcript
                .prepare_append_with_blocks(&good, vec![binding])
                .unwrap(),
        );
        let cursor = transcript.snapshot().unwrap();
        let link = cursor.next.unwrap();
        let mut entry = transcript.load_entry(link).unwrap();
        entry.content_blocks = Some(vec![invalid]);
        db.commit(
            &[],
            &[Mutation::Put {
                key: entry_key(link.id),
                value: encode(&entry).unwrap(),
            }],
        )
        .unwrap();
        assert!(matches!(
            transcript.read_with_blocks(&cursor, NonZeroUsize::MIN),
            Err(Error::Corrupt)
        ));
        entry.content_blocks.as_mut().unwrap()[0].blocks = vec![];
        entry.value["AppendV2"]["messages"][0]["content"] = json!("hidden");
        db.commit(
            &[],
            &[Mutation::Put {
                key: entry_key(link.id),
                value: encode(&entry).unwrap(),
            }],
        )
        .unwrap();
        assert!(matches!(
            transcript.read_with_blocks(&cursor, NonZeroUsize::MIN),
            Err(Error::Corrupt)
        ));
    }

    #[test]
    fn record_version_and_content_must_agree() {
        let db = Store::memory().unwrap();
        let id = Uuid::new_v4();
        create(&db, id);
        append(&db, id, json!("legacy"));
        let transcript = Transcript::new(&db, id);
        let cursor = transcript.snapshot().unwrap();
        let link = cursor.next.unwrap();
        let mut entry = transcript.load_entry(link).unwrap();
        assert_eq!(entry.format, 1);
        entry.content_blocks = Some(vec![MessageContentBlocks {
            message_id: Uuid::new_v4(),
            blocks: vec![],
        }]);
        db.commit(
            &[],
            &[Mutation::Put {
                key: entry_key(link.id),
                value: encode(&entry).unwrap(),
            }],
        )
        .unwrap();
        assert!(matches!(
            transcript.read_with_blocks(&cursor, NonZeroUsize::MIN),
            Err(Error::Corrupt)
        ));
        entry.format = 2;
        entry.content_blocks = None;
        db.commit(
            &[],
            &[Mutation::Put {
                key: entry_key(link.id),
                value: encode(&entry).unwrap(),
            }],
        )
        .unwrap();
        assert!(matches!(
            transcript.read_with_blocks(&cursor, NonZeroUsize::MIN),
            Err(Error::Corrupt)
        ));
    }

    #[test]
    fn pages_forks_snapshots_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();
        let db = Store::open(&path).unwrap();
        create(&db, a);
        for n in 0..100 {
            append(&db, a, json!(n));
        }
        let source = Transcript::new(&db, a);
        let snapshot = source.snapshot().unwrap();
        let fork = source.prepare_fork(b).unwrap();
        assert_eq!(fork.mutations.len(), 1);
        commit(&db, fork);
        append(&db, a, json!("parent"));
        append(&db, b, json!("child"));
        commit(&db, Transcript::new(&db, b).prepare_fork(c).unwrap());
        append(&db, c, json!("grandchild"));
        assert_eq!(
            read_all(&source, snapshot, 7),
            (0..100).rev().map(|n| json!(n)).collect::<Vec<_>>()
        );
        let wrong = source.snapshot().unwrap();
        assert!(matches!(
            Transcript::new(&db, b).read(&wrong, NonZeroUsize::MIN),
            Err(Error::WrongTranscript)
        ));
        drop(db);
        let db = Store::open(&path).unwrap();
        let t = Transcript::new(&db, c);
        let all = read_all(&t, t.snapshot().unwrap(), 1);
        assert_eq!(all.len(), 102);
        assert_eq!(&all[..3], &[json!("grandchild"), json!("child"), json!(99)]);
        assert_eq!(db.list(&[PREFIX, b"entry/"].concat()).unwrap().len(), 103);
    }

    #[test]
    fn concurrent_writers_retry_without_loss_and_batch_is_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let id = Uuid::new_v4();
        let db = Store::open(&path).unwrap();
        create(&db, id);
        let barrier = Arc::new(Barrier::new(4));
        let threads: Vec<_> = (0..4)
            .map(|writer| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let db = Store::open(path).unwrap();
                    let t = Transcript::new(&db, id);
                    for n in 0..25 {
                        let mut first = true;
                        loop {
                            let mut batch = t.prepare_append(&json!([writer, n])).unwrap();
                            let key = format!("host/{writer}/{n}").into_bytes();
                            batch.checks.push(Check {
                                key: key.clone(),
                                expected: Expected::Missing,
                            });
                            batch.mutations.push(Mutation::Put {
                                key,
                                value: vec![1],
                            });
                            if n == 0 && first {
                                barrier.wait();
                                first = false;
                            }
                            match db.commit(&batch.checks, &batch.mutations) {
                                Ok(()) => break,
                                Err(durable_store::Error::Conflict { .. }) => continue,
                                Err(e) => panic!("{e}"),
                            }
                        }
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let t = Transcript::new(&db, id);
        let entries = read_all(&t, t.snapshot().unwrap(), 3);
        assert_eq!(entries.len(), 100);
        for writer in 0..4 {
            for n in 0..25 {
                assert_eq!(
                    entries.iter().filter(|v| **v == json!([writer, n])).count(),
                    1
                );
                assert!(
                    db.get(format!("host/{writer}/{n}").as_bytes())
                        .unwrap()
                        .is_some()
                );
            }
        }
    }

    #[test]
    fn stale_append_and_fork_fail_without_partial_writes() {
        let db = Store::memory().unwrap();
        let id = Uuid::new_v4();
        let child = Uuid::new_v4();
        create(&db, id);
        let t = Transcript::new(&db, id);
        let mut stale = t.prepare_append(&json!(1)).unwrap();
        stale.mutations.push(Mutation::Put {
            key: b"host".to_vec(),
            value: vec![1],
        });
        let fork = t.prepare_fork(child).unwrap();
        append(&db, id, json!(2));
        assert!(matches!(
            db.commit(&stale.checks, &stale.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(db.get(b"host").unwrap().is_none());
        assert!(matches!(
            db.commit(&fork.checks, &fork.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(matches!(
            Transcript::new(&db, child).snapshot(),
            Err(Error::Missing)
        ));
        assert!(matches!(t.prepare_fork(id), Err(Error::SelfFork)));
        commit(&db, t.prepare_fork(child).unwrap());
        let duplicate = t.prepare_fork(child).unwrap();
        assert!(matches!(
            db.commit(&duplicate.checks, &duplicate.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
    }

    #[test]
    fn entry_collision_and_broken_link_fail_closed() {
        let db = Store::memory().unwrap();
        let id = Uuid::new_v4();
        create(&db, id);
        let t = Transcript::new(&db, id);
        let batch = t.prepare_append(&json!(1)).unwrap();
        assert_eq!(batch.checks.len(), 2);
        assert_eq!(batch.mutations.len(), 2);
        let key = batch.checks[1].key.clone();
        db.commit(
            &[],
            &[Mutation::Put {
                key: key.clone(),
                value: b"occupied".to_vec(),
            }],
        )
        .unwrap();
        assert!(matches!(
            db.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(t.snapshot().unwrap().next.is_none());
        assert_eq!(db.get(&key).unwrap().unwrap().value, b"occupied");
        append(&db, id, json!(1));
        append(&db, id, json!(2));
        let cursor = t.snapshot().unwrap();
        let tip = cursor.next.unwrap();
        let mut entry = t.load_entry(tip).unwrap();
        entry.previous.as_mut().unwrap().sequence = 0;
        db.commit(
            &[],
            &[Mutation::Put {
                key: entry_key(tip.id),
                value: encode(&entry).unwrap(),
            }],
        )
        .unwrap();
        assert!(matches!(
            t.read(&cursor, NonZeroUsize::MIN),
            Err(Error::Corrupt)
        ));
    }

    #[test]
    fn corruption_detected_on_visit_and_invalid_head_rejected() {
        let db = Store::memory().unwrap();
        let id = Uuid::new_v4();
        create(&db, id);
        append(&db, id, json!(1));
        append(&db, id, json!(2));
        let t = Transcript::new(&db, id);
        let cursor = t.snapshot().unwrap();
        let tip = cursor.next.unwrap();
        let entry = t.load_entry(tip).unwrap();
        let prior = entry.previous.unwrap();
        db.commit(
            &[],
            &[Mutation::Delete {
                key: entry_key(prior.id),
            }],
        )
        .unwrap();
        let page = t.read(&cursor, NonZeroUsize::MIN).unwrap();
        assert!(page.has_more);
        assert!(matches!(
            t.read(&page.next, NonZeroUsize::MIN),
            Err(Error::Corrupt)
        ));
        db.commit(
            &[],
            &[Mutation::Put {
                key: entry_key(tip.id),
                value: br#"{}"#.to_vec(),
            }],
        )
        .unwrap();
        assert!(matches!(t.prepare_append(&json!(3)), Err(Error::Corrupt)));
        assert!(matches!(
            t.prepare_fork(Uuid::new_v4()),
            Err(Error::Corrupt)
        ));
        db.commit(
            &[],
            &[Mutation::Put {
                key: head_key(id),
                value: br#"{"format":2,"tip":null}"#.to_vec(),
            }],
        )
        .unwrap();
        assert!(matches!(t.snapshot(), Err(Error::Corrupt)));
    }
}
