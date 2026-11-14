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
}

#[derive(Clone, Debug, PartialEq)]
pub struct Document {
    pub value: Map<String, Value>,
    pub version: u64,
}

pub struct Documents<'a> {
    store: &'a Store,
}
impl<'a> Documents<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
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
        for (key, entry) in self.store.documents_at(revision)? {
            let parts: [String; 3] =
                serde_json::from_slice(key.strip_prefix(PREFIX).ok_or(Error::InvalidDocument)?)?;
            if parts[0] != parent.to_string() {
                continue;
            }
            // Validate the document before copying; preserve JSON, allocate a new CAS version.
            Self::decode(Some(entry.clone()))?;
            let target = Address {
                scope: child.to_string(),
                name: parts[1].clone(),
                key: parts[2].clone(),
            }
            .storage_key()?;
            batch.checks.push(Check {
                key: target.clone(),
                expected: Expected::Missing,
            });
            batch.mutations.push(Mutation::Put {
                key: target,
                value: entry.value,
            });
        }
        Ok(batch)
    }
    /// Read one address at a previously captured document boundary.
    pub fn get_at(&self, address: &Address, snapshot: u64) -> Result<Option<Document>, Error> {
        Self::decode(self.store.document_at(&address.storage_key()?, snapshot)?)
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
