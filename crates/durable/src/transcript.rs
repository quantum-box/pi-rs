//! Incremental durable transcript storage used by the runtime.
//!
//! Reserve `durable/transcript/` for this module. Entries are immutable and
//! heads may only be changed with these operations. A fork shares its parent's
//! captured head; neither forking nor appending copies history. Reads walk
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

/// Ordered content for one user-message occurrence in a runtime transcript change.
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

    fn prepare_write_with_blocks(
        &self,
        value: &Value,
        blocks: Option<Vec<MessageContentBlocks>>,
        create: bool,
    ) -> Result<(Batch, Cursor), Error> {
        if blocks
            .as_ref()
            .is_some_and(|blocks| !valid_block_bindings(value, blocks))
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
        let sequence = head
            .tip
            .map_or(Some(1), |t| t.sequence.checked_add(1))
            .ok_or(Error::SequenceExhausted)?;
        let tip = Link {
            id: Uuid::new_v4(),
            sequence,
        };
        let entry = Entry {
            format: if blocks.is_some() { 2 } else { 1 },
            id: tip.id,
            sequence,
            previous: head.tip,
            value: value.clone(),
            content_blocks: blocks,
        };
        let hk = head_key(self.id);
        let ek = entry_key(tip.id);
        Ok((
            Batch {
                checks: vec![
                    Check {
                        key: hk.clone(),
                        expected,
                    },
                    Check {
                        key: ek.clone(),
                        expected: Expected::Missing,
                    },
                ],
                mutations: vec![
                    Mutation::Put {
                        key: ek,
                        value: encode(&entry)?,
                    },
                    Mutation::Put {
                        key: hk,
                        value: encode(&Head {
                            format: 1,
                            tip: Some(tip),
                        })?,
                    },
                ],
            },
            Cursor {
                transcript: self.id,
                next: Some(tip),
            },
        ))
    }

    /// Fork the current head. A simultaneous source append makes this batch
    /// conflict, so the caller can choose the new head explicitly on retry.
    /// Scans the captured chain and rejects unsupported block-bearing records.
    pub fn prepare_fork(&self, child: Uuid) -> Result<Batch, Error> {
        if child == self.id {
            return Err(Error::SelfFork);
        }
        let (head, version) = self.load_head()?;
        self.reject_blocks_in_chain(head.tip)?;
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
        self.reject_blocks_in_chain(cursor.next)?;
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

    fn reject_blocks_in_chain(&self, mut link: Option<Link>) -> Result<(), Error> {
        while let Some(current) = link {
            let entry = self.load_entry(current)?;
            if entry.content_blocks.is_some() {
                return Err(Error::UnsupportedContentBlocks);
            }
            link = entry.previous;
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
        let stored = self.store.get(&entry_key(link.id))?.ok_or(Error::Corrupt)?;
        let entry: Entry = decode(&stored.value)?;
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
    if !matches!(
        kind.as_str(),
        "AppendV2" | "AppendV3" | "AppendV4" | "ReplaceV2"
    ) {
        return false;
    }
    let (Some(ids), Some(messages)) = (
        data.get("ids").and_then(Value::as_array),
        data.get("messages").and_then(Value::as_array),
    ) else {
        return false;
    };
    if ids.len() != messages.len() {
        return false;
    }
    let mut occurrences = std::collections::HashMap::new();
    for (id, message) in ids.iter().zip(messages) {
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
            && occurrences.get(&block.message_id).is_some_and(|message| {
                matches!(
                    message.get("role").and_then(Value::as_str),
                    Some("user" | "user_with_images")
                )
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
    fn versioned_blocks_survive_append_and_reopen_and_forks_fail_closed() {
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
                .prepare_append_with_blocks(&json!({"AppendV2":{"ids":[message_id],"messages":[{"role":"user","content":"placeholder"}]}}), content.clone())
                .unwrap(),
        );
        let blocked_snapshot = transcript.snapshot().unwrap();
        append(&db, id, json!({"later": true}));
        assert!(matches!(
            transcript.prepare_fork(Uuid::new_v4()),
            Err(Error::UnsupportedContentBlocks)
        ));
        assert!(matches!(
            transcript.prepare_fork_at(&blocked_snapshot, Uuid::new_v4()),
            Err(Error::UnsupportedContentBlocks)
        ));
        drop(db);

        let db = Store::open(&path).unwrap();
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
