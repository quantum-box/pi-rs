//! Optimistic JSON document storage and checked batch composition.
//!
//! Documents are whole JSON objects. Reads are snapshots, and every write or
//! delete requires the version read (or an explicit missing precondition).
//! A batch is inert until committed; all conditions see the pre-commit state.

use crate::task_store::OwnedBatch;
use durable_store::{Check, Expected, Mutation, Store};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};

const PREFIX: &[u8] = b"durable/document/";
const POLICY_PREFIX: &[u8] = b"durable/document-policy/";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("stored document is not a JSON object")]
    InvalidDocument,
    #[error("a batch has duplicate checks or writes, or an unchecked write")]
    InvalidBatch,
    #[error("document reads must precede all writes")]
    ReadAfterWrite,
    #[error("document must be read before writing, and written at most once")]
    UnreadOrWritten,
    #[error("duplicate task in admission")]
    DuplicateTask,
    #[error(
        "application entries require a directly conversation-owned task and a nonempty non-pi. kind"
    )]
    InvalidApplicationEntry,
}

/// An application-defined address. Components are encoded unambiguously;
/// the scope is a host convention, not an enforced ownership relationship.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Address {
    pub scope: String,
    pub name: String,
    pub key: String,
}
impl Address {
    fn storage_key(&self) -> Result<Vec<u8>, Error> {
        let mut result = PREFIX.to_vec();
        result.extend(serde_json::to_vec(&[&self.scope, &self.name, &self.key])?);
        Ok(result)
    }
    fn policy_key(&self) -> Result<Vec<u8>, Error> {
        let mut key = POLICY_PREFIX.to_vec();
        key.extend(serde_json::to_vec(&[&self.scope, &self.name, &self.key])?);
        Ok(key)
    }
    fn from_storage_key(key: &[u8]) -> Result<Self, Error> {
        let [scope, name, key] =
            serde_json::from_slice(key.strip_prefix(PREFIX).ok_or(Error::InvalidDocument)?)?;
        Ok(Self { scope, name, key })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Document {
    pub value: Map<String, Value>,
    pub version: u64,
}

/// Fork behavior for a document address. Missing policies retain legacy copy behavior.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ForkPolicy {
    /// Copy the value at the selected boundary.
    Copy,
    /// Start each fork with this stored initial JSON object.
    Initial { value: Map<String, Value> },
}

#[derive(Clone, Debug, PartialEq)]
pub struct DocumentForkPolicy {
    pub policy: ForkPolicy,
    pub version: u64,
}

/// A database-local committed document state, including deletion.
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentRevision {
    pub revision: u64,
    pub document: Option<Document>,
}

/// Final document states written by one atomic store commit.
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentCommit {
    pub revision: u64,
    pub changes: Vec<(Address, Option<Document>)>,
}

/// Pull-based stream of complete document commits across the database.
pub struct DocumentCommitWatch<'a> {
    store: &'a Store,
    cursor: u64,
}
impl DocumentCommitWatch<'_> {
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Return up to `limit` complete commits (1..=256). Errors do not advance the cursor.
    pub fn poll(&mut self, limit: usize) -> Result<Vec<DocumentCommit>, Error> {
        let frames = self.store.document_commits(self.cursor, limit)?;
        let result = frames
            .into_iter()
            .map(|(revision, changes)| {
                Ok(DocumentCommit {
                    revision,
                    changes: changes
                        .into_iter()
                        .map(|(key, entry)| {
                            Ok((Address::from_storage_key(&key)?, Documents::decode(entry)?))
                        })
                        .collect::<Result<Vec<_>, Error>>()?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        if let Some(last) = result.last() {
            self.cursor = last.revision;
        }
        Ok(result)
    }
}

/// Pull-based watch. Persist the cursor only after consuming its entire page.
pub struct DocumentWatch<'a> {
    store: &'a Store,
    key: Vec<u8>,
    cursor: u64,
}
impl DocumentWatch<'_> {
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Return at most `limit` changes (1..=256), without waiting.
    /// On error the cursor is unchanged.
    pub fn poll(&mut self, limit: usize) -> Result<Vec<DocumentRevision>, Error> {
        let changes = self.store.document_changes(&self.key, self.cursor, limit)?;
        let result = changes
            .into_iter()
            .map(|(revision, entry)| {
                Ok(DocumentRevision {
                    revision,
                    document: Documents::decode(entry)?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        if let Some(last) = result.last() {
            self.cursor = last.revision;
        }
        Ok(result)
    }
}

pub struct Documents<'a> {
    store: &'a Store,
}
impl<'a> Documents<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }
    /// Capture all live documents and attach a commit stream at the same boundary.
    pub fn watch_commits(&self) -> Result<(DocumentCommit, DocumentCommitWatch<'a>), Error> {
        let revision = self.store.document_snapshot()?;
        let changes = self
            .store
            .documents_at(revision)?
            .into_iter()
            .map(|(key, entry)| Ok((Address::from_storage_key(&key)?, Self::decode(Some(entry))?)))
            .collect::<Result<Vec<_>, Error>>()?;
        Ok((
            DocumentCommit { revision, changes },
            DocumentCommitWatch {
                store: self.store,
                cursor: revision,
            },
        ))
    }

    /// Resume after a fully consumed commit in the same database.
    pub fn resume_commit_watch(&self, cursor: u64) -> Result<DocumentCommitWatch<'a>, Error> {
        if cursor > self.store.document_snapshot()? {
            return Err(durable_store::Error::InvalidSnapshot.into());
        }
        Ok(DocumentCommitWatch {
            store: self.store,
            cursor,
        })
    }
    /// Attach at a consistent immutable boundary, even if a writer commits
    /// between capturing the boundary and reading the initial document.
    pub fn watch(&self, address: &Address) -> Result<(DocumentRevision, DocumentWatch<'a>), Error> {
        let key = address.storage_key()?;
        let revision = self.store.document_snapshot()?;
        let document = Self::decode(self.store.document_at(&key, revision)?)?;
        Ok((
            DocumentRevision { revision, document },
            DocumentWatch {
                store: self.store,
                key,
                cursor: revision,
            },
        ))
    }

    /// Resume after a previously consumed cursor in the same database.
    pub fn resume_watch(&self, address: &Address, cursor: u64) -> Result<DocumentWatch<'a>, Error> {
        let key = address.storage_key()?;
        // Validate the boundary without consuming changes.
        self.store.document_at(&key, cursor)?;
        Ok(DocumentWatch {
            store: self.store,
            key,
            cursor,
        })
    }
    /// Capture a store-wide document boundary, reusable after reopen.
    pub fn snapshot(&self) -> Result<u64, Error> {
        Ok(self.store.document_snapshot()?)
    }
    /// Prepare copies of one conversation scope at an immutable boundary.
    pub(crate) fn fork_scope(
        &self,
        parent: uuid::Uuid,
        child: uuid::Uuid,
        revision: u64,
    ) -> Result<OwnedBatch, Error> {
        let mut batch = OwnedBatch {
            checks: Vec::new(),
            mutations: Vec::new(),
        };
        let source = crate::application_entry::key(parent);
        if let Some(entry) = self.store.application_entries_at(&source, revision)? {
            // Validate before copying; the original IDs and authors remain immutable.
            let _: Vec<crate::application_entry::ApplicationEntry> =
                serde_json::from_slice(&entry.value)?;
            let target = crate::application_entry::key(child);
            batch.checks.push(Check {
                key: target.clone(),
                expected: Expected::Missing,
            });
            batch.mutations.push(Mutation::Put {
                key: target,
                value: entry.value,
            });
        }
        for (key, entry) in self.store.documents_at(revision)? {
            let parts: [String; 3] =
                serde_json::from_slice(key.strip_prefix(PREFIX).ok_or(Error::InvalidDocument)?)?;
            if parts[0] != parent.to_string() {
                continue;
            }
            // Policy and value are read at the same immutable cut.
            Self::decode(Some(entry.clone()))?;
            let source_address = Address::from_storage_key(&key)?;
            let policy_entry = self
                .store
                .document_at(&source_address.policy_key()?, revision)?;
            let policy = policy_entry
                .as_ref()
                .map(|e| serde_json::from_slice::<ForkPolicy>(&e.value))
                .transpose()?;
            let target = Address {
                scope: child.to_string(),
                name: parts[1].clone(),
                key: parts[2].clone(),
            }
            .storage_key()?;
            let value = match policy {
                Some(ForkPolicy::Initial { value }) => serde_json::to_vec(&value)?,
                _ => entry.value,
            };
            batch.checks.push(Check {
                key: target.clone(),
                expected: Expected::Missing,
            });
            batch.mutations.push(Mutation::Put { key: target, value });
            if let Some(policy_entry) = policy_entry {
                let child_address = Address {
                    scope: child.to_string(),
                    ..source_address
                };
                let key = child_address.policy_key()?;
                batch.checks.push(Check {
                    key: key.clone(),
                    expected: Expected::Missing,
                });
                batch.mutations.push(Mutation::Put {
                    key,
                    value: policy_entry.value,
                });
            }
        }
        Ok(batch)
    }
    /// Read one address at a previously captured document boundary.
    pub fn get_at(&self, address: &Address, snapshot: u64) -> Result<Option<Document>, Error> {
        Self::decode(self.store.document_at(&address.storage_key()?, snapshot)?)
    }
    /// Read an explicitly configured policy and its CAS version; absence means copy.
    pub fn fork_policy(&self, address: &Address) -> Result<Option<DocumentForkPolicy>, Error> {
        self.store
            .get(&address.policy_key()?)?
            .map(|entry| {
                Ok(DocumentForkPolicy {
                    policy: serde_json::from_slice(&entry.value)?,
                    version: entry.version,
                })
            })
            .transpose()
    }
    pub fn get(&self, address: &Address) -> Result<Option<Document>, Error> {
        Self::decode(self.store.get(&address.storage_key()?)?)
    }
    fn decode(entry: Option<durable_store::Entry>) -> Result<Option<Document>, Error> {
        entry
            .map(|entry| {
                let value = serde_json::from_slice::<Value>(&entry.value)?;
                let Value::Object(value) = value else {
                    return Err(Error::InvalidDocument);
                };
                Ok(Document {
                    value,
                    version: entry.version,
                })
            })
            .transpose()
    }
}

/// An all-or-nothing collection of checked writes. Duplicate keys are rejected
/// across components, including duplicate checks; this prevents silently
/// overriding a participant's write or condition. Read-only checks are allowed.
#[derive(Default)]
pub struct Transaction {
    checks: Vec<Check>,
    mutations: Vec<Mutation>,
    checked: HashSet<Vec<u8>>,
    written: HashSet<Vec<u8>>,
}
impl Transaction {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn put(
        &mut self,
        address: &Address,
        expected: Expected,
        value: &Map<String, Value>,
    ) -> Result<(), Error> {
        let key = address.storage_key()?;
        self.append(OwnedBatch {
            checks: vec![Check {
                key: key.clone(),
                expected,
            }],
            mutations: vec![Mutation::Put {
                key,
                value: serde_json::to_vec(value)?,
            }],
        })
    }
    /// Set a policy using its own CAS version; can be composed atomically with document writes.
    pub fn set_fork_policy(
        &mut self,
        address: &Address,
        expected: Expected,
        policy: &ForkPolicy,
    ) -> Result<(), Error> {
        let key = address.policy_key()?;
        self.append(OwnedBatch {
            checks: vec![Check {
                key: key.clone(),
                expected,
            }],
            mutations: vec![Mutation::Put {
                key,
                value: serde_json::to_vec(policy)?,
            }],
        })
    }
    pub fn delete(&mut self, address: &Address, version: u64) -> Result<(), Error> {
        let key = address.storage_key()?;
        self.append(OwnedBatch {
            checks: vec![Check {
                key: key.clone(),
                expected: Expected::Version(version),
            }],
            mutations: vec![Mutation::Delete { key }],
        })
    }
    /// Compose a prepared task or host batch. Every mutated key must be checked
    /// in this component; no state is added if validation fails. Never use raw
    /// host batches to bypass the document API's JSON or namespace invariants.
    pub fn append(&mut self, batch: OwnedBatch) -> Result<(), Error> {
        let mut checks = self.checked.clone();
        let mut writes = self.written.clone();
        let mut local_checks = HashSet::new();
        for check in &batch.checks {
            if !checks.insert(check.key.clone()) {
                return Err(Error::InvalidBatch);
            }
            local_checks.insert(check.key.clone());
        }
        for mutation in &batch.mutations {
            let key = match mutation {
                Mutation::Put { key, .. } | Mutation::Delete { key } => key,
            };
            if !local_checks.contains(key) || !writes.insert(key.clone()) {
                return Err(Error::InvalidBatch);
            }
        }
        self.checked = checks;
        self.written = writes;
        self.checks.extend(batch.checks);
        self.mutations.extend(batch.mutations);
        Ok(())
    }
    pub fn commit(self, store: &Store) -> Result<(), Error> {
        store.commit(&self.checks, &self.mutations)?;
        Ok(())
    }
}

/// Checked document operations for one admission attempt. Reads are cached and
/// validated at commit, including missing and read-only documents. Once any
/// write is staged, further reads are rejected. A conflict reruns the caller.
pub struct AdmissionDocuments<'a> {
    store: &'a Store,
    reads: HashMap<Address, Option<Document>>,
    writes: HashMap<Address, Option<Map<String, Value>>>,
    tasks: Vec<crate::task::Task>,
}

/// Checked document operations at one invocation-wide immutable revision.
/// Staged values are visible to later reads. A staged new document has version
/// zero; existing staged documents retain their original version until commit.
pub struct TaskDocuments<'a> {
    store: &'a Store,
    revision: u64,
    reads: HashMap<Address, Option<Document>>,
    writes: HashMap<Address, Option<Map<String, Value>>>,
    task: crate::task::Task,
    entries: Option<(Option<u64>, Vec<crate::application_entry::ApplicationEntry>)>,
}
impl<'a> TaskDocuments<'a> {
    pub(crate) fn new(store: &'a Store, task: &crate::task::Task) -> Result<Self, Error> {
        Ok(Self {
            store,
            task: task.clone(),
            entries: None,
            revision: store.document_snapshot()?,
            reads: HashMap::new(),
            writes: HashMap::new(),
        })
    }
    pub fn get(&mut self, address: &Address) -> Result<Option<Document>, Error> {
        if let Some(value) = self.writes.get(address) {
            return Ok(value.as_ref().map(|value| Document {
                value: value.clone(),
                version: self.reads[address].as_ref().map_or(0, |doc| doc.version),
            }));
        }
        if let Some(value) = self.reads.get(address) {
            return Ok(value.clone());
        }
        let value = Documents::new(self.store).get_at(address, self.revision)?;
        self.reads.insert(address.clone(), value.clone());
        Ok(value)
    }
    pub fn put(&mut self, address: &Address, value: Map<String, Value>) -> Result<(), Error> {
        self.write(address, Some(value))
    }
    pub fn delete(&mut self, address: &Address) -> Result<(), Error> {
        self.write(address, None)
    }
    fn write(&mut self, address: &Address, value: Option<Map<String, Value>>) -> Result<(), Error> {
        if !self.reads.contains_key(address) {
            return Err(Error::UnreadOrWritten);
        }
        self.writes.insert(address.clone(), value);
        Ok(())
    }
    /// Stage an immutable application record with this task as its author.
    /// Only directly conversation-owned tasks are supported. The returned ID is
    /// provisional until the step commits and can change on replay.
    pub fn append_entry(&mut self, kind: &str, data: Value) -> Result<uuid::Uuid, Error> {
        let crate::task::TaskOwner::Conversation(conversation) = self.task.owner() else {
            return Err(Error::InvalidApplicationEntry);
        };
        if kind.is_empty() || kind.starts_with("pi.") {
            return Err(Error::InvalidApplicationEntry);
        }
        if self.entries.is_none() {
            let record = self
                .store
                .get(&crate::application_entry::key(*conversation))?;
            let version = record.as_ref().map(|r| r.version);
            let entries = record
                .map(|r| serde_json::from_slice(&r.value))
                .transpose()?
                .unwrap_or_default();
            self.entries = Some((version, entries));
        }
        let id = uuid::Uuid::new_v4();
        self.entries
            .as_mut()
            .unwrap()
            .1
            .push(crate::application_entry::ApplicationEntry {
                id,
                conversation_id: *conversation,
                author_task_id: self.task.id(),
                kind: kind.into(),
                data,
            });
        Ok(id)
    }
    pub(crate) fn into_batch(self) -> Result<OwnedBatch, Error> {
        let mut batch = OwnedBatch {
            checks: Vec::new(),
            mutations: Vec::new(),
        };
        for address in self.reads.keys() {
            let key = address.storage_key()?;
            batch.checks.push(Check {
                key: key.clone(),
                expected: Expected::DocumentUnchangedSince(self.revision),
            });
            if let Some(value) = self.writes.get(address) {
                batch.mutations.push(match value {
                    Some(value) => Mutation::Put {
                        key,
                        value: serde_json::to_vec(value)?,
                    },
                    None => Mutation::Delete { key },
                });
            }
        }
        if let Some((version, entries)) = self.entries {
            let crate::task::TaskOwner::Conversation(conversation) = self.task.owner() else {
                return Err(Error::InvalidApplicationEntry);
            };
            let key = crate::application_entry::key(*conversation);
            batch.checks.push(Check {
                key: key.clone(),
                expected: version.map_or(Expected::Missing, Expected::Version),
            });
            batch.mutations.push(Mutation::Put {
                key,
                value: serde_json::to_vec(&entries)?,
            });
        }
        Ok(batch)
    }
}
impl<'a> AdmissionDocuments<'a> {
    pub(crate) fn new(store: &'a Store) -> Self {
        Self {
            store,
            reads: HashMap::new(),
            writes: HashMap::new(),
            tasks: Vec::new(),
        }
    }
    pub fn get(&mut self, address: &Address) -> Result<Option<Document>, Error> {
        if !self.writes.is_empty() {
            return Err(Error::ReadAfterWrite);
        }
        if let Some(value) = self.reads.get(address) {
            return Ok(value.clone());
        }
        let value = Documents::new(self.store).get(address)?;
        self.reads.insert(address.clone(), value.clone());
        Ok(value)
    }
    pub fn put(&mut self, address: &Address, value: Map<String, Value>) -> Result<(), Error> {
        self.write(address, Some(value))
    }
    pub fn delete(&mut self, address: &Address) -> Result<(), Error> {
        self.write(address, None)
    }
    /// Stage a conversation-owned registered task for the admission commit.
    pub fn create_task(&mut self, task: crate::task::Task) -> Result<(), Error> {
        if self.tasks.iter().any(|existing| existing.id() == task.id()) {
            return Err(Error::DuplicateTask);
        }
        self.tasks.push(task);
        Ok(())
    }
    pub(crate) fn take_tasks(&mut self) -> Vec<crate::task::Task> {
        std::mem::take(&mut self.tasks)
    }
    fn write(&mut self, address: &Address, value: Option<Map<String, Value>>) -> Result<(), Error> {
        if !self.reads.contains_key(address) || self.writes.contains_key(address) {
            return Err(Error::UnreadOrWritten);
        }
        self.writes.insert(address.clone(), value);
        Ok(())
    }
    pub(crate) fn into_batch(self) -> Result<OwnedBatch, Error> {
        let mut batch = OwnedBatch {
            checks: Vec::new(),
            mutations: Vec::new(),
        };
        for (address, document) in self.reads {
            let key = address.storage_key()?;
            batch.checks.push(Check {
                key: key.clone(),
                expected: document.map_or(Expected::Missing, |d| Expected::Version(d.version)),
            });
            if let Some(value) = self.writes.get(&address) {
                batch.mutations.push(match value {
                    Some(value) => Mutation::Put {
                        key,
                        value: serde_json::to_vec(value)?,
                    },
                    None => Mutation::Delete { key },
                });
            }
        }
        Ok(batch)
    }
}
