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
    #[error(transparent)]
    Signal(#[from] crate::task_store::Error),
    #[error("self-delivery or a repeated typed signal key cannot be staged")]
    SignalComposition,
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
    #[error("application entries require a valid active task ancestry and a nonempty non-pi. kind")]
    InvalidApplicationEntry,
    #[error("application entry head does not match the expected ID")]
    ApplicationEntryHeadMismatch,
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
    /// Copy the latest committed value and policy when the fork commits.
    Current,
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

/// One committed document operation frame. The store retains complete states,
/// so puts are represented by a Chord root replacement, not a reconstructed diff.
/// A deletion is a retirement with no replacement and no live document.
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentOperationFrame {
    pub revision: u64,
    pub document: Option<Document>,
    pub ops: Vec<Value>,
    pub retired: bool,
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

    /// Return one operation frame per committed state. A put is a root replacement;
    /// a delete is a retirement. On error the cursor is unchanged.
    pub fn poll_operations(&mut self, limit: usize) -> Result<Vec<DocumentOperationFrame>, Error> {
        Ok(self
            .poll(limit)?
            .into_iter()
            .map(|change| {
                let ops = change
                    .document
                    .as_ref()
                    .map(|document| vec![serde_json::json!(["r", document.value])])
                    .unwrap_or_default();
                DocumentOperationFrame {
                    revision: change.revision,
                    retired: change.document.is_none(),
                    document: change.document,
                    ops,
                }
            })
            .collect())
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
        self.fork_scope_from(parent, parent, child, revision)
    }

    /// Historical values come from the entry owner; current-policy values come
    /// from the immediate parent. Both are covered by the same revision fence.
    pub(crate) fn fork_scope_from(
        &self,
        parent: uuid::Uuid,
        current_parent: uuid::Uuid,
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
        // A global revision fence also detects addresses created or deleted while
        // preparing the copy. The caller retries this batch on a fence conflict.
        let current = self.store.document_snapshot()?;
        let lineage_key = crate::application_entry::lineage_key(child);
        batch.checks.push(Check {
            key: lineage_key.clone(),
            expected: Expected::Missing,
        });
        batch.mutations.push(Mutation::Put {
            key: lineage_key,
            value: serde_json::to_vec(&crate::application_entry::Lineage {
                parent: current_parent,
                parent_revision: current,
            })?,
        });
        batch.checks.push(Check {
            key: b"durable/document-fork-revision".to_vec(),
            expected: Expected::DocumentRevision(current),
        });
        let historical_docs: HashMap<_, _> =
            self.store.documents_at(revision)?.into_iter().collect();
        let historical_policies: HashMap<_, _> = self
            .store
            .document_policies_at(revision)?
            .into_iter()
            .collect();
        let current_docs: HashMap<_, _> = self.store.documents_at(current)?.into_iter().collect();
        let current_policies: HashMap<_, _> = self
            .store
            .document_policies_at(current)?
            .into_iter()
            .collect();
        // Normalize both scopes to the historical owner's address; matching
        // names and family keys identify the same logical document.
        let mut addresses = HashSet::new();
        for (key, scope) in historical_docs
            .keys()
            .map(|k| (k, parent))
            .chain(current_docs.keys().map(|k| (k, current_parent)))
        {
            let mut address = Address::from_storage_key(key)?;
            if address.scope == scope.to_string() {
                address.scope = parent.to_string();
                addresses.insert(address);
            }
        }
        for (key, scope) in historical_policies
            .keys()
            .map(|k| (k, parent))
            .chain(current_policies.keys().map(|k| (k, current_parent)))
        {
            let [found_scope, name, key]: [String; 3] = serde_json::from_slice(
                key.strip_prefix(POLICY_PREFIX)
                    .ok_or(Error::InvalidDocument)?,
            )?;
            if found_scope == scope.to_string() {
                addresses.insert(Address {
                    scope: parent.to_string(),
                    name,
                    key,
                });
            }
        }
        for address in addresses {
            let doc_key = address.storage_key()?;
            let policy_key = address.policy_key()?;
            let current_address = Address {
                scope: current_parent.to_string(),
                ..address.clone()
            };
            let current_doc_key = current_address.storage_key()?;
            let current_policy_key = current_address.policy_key()?;
            let old_policy = historical_policies.get(&policy_key);
            let latest_policy = current_policies.get(&current_policy_key);
            let parse =
                |entry: Option<&durable_store::Entry>| -> Result<Option<ForkPolicy>, Error> {
                    entry
                        .map(|e| serde_json::from_slice(&e.value).map_err(Error::from))
                        .transpose()
                };
            let old = parse(old_policy)?;
            let latest = parse(latest_policy)?;
            // Existing historical addresses keep their cut policy; addresses first
            // introduced later participate only when their latest policy is Current.
            let use_current = matches!(old, Some(ForkPolicy::Current))
                || (old_policy.is_none()
                    && !historical_docs.contains_key(&doc_key)
                    && matches!(latest, Some(ForkPolicy::Current)));
            let (doc, policy_entry, policy) = if use_current {
                (current_docs.get(&current_doc_key), latest_policy, latest)
            } else {
                (historical_docs.get(&doc_key), old_policy, old)
            };
            let target_address = Address {
                scope: child.to_string(),
                name: address.name,
                key: address.key,
            };
            if let Some(entry) = doc {
                Self::decode(Some(entry.clone()))?;
                let value = if !use_current {
                    match policy {
                        Some(ForkPolicy::Initial { value }) => serde_json::to_vec(&value)?,
                        _ => entry.value.clone(),
                    }
                } else {
                    entry.value.clone()
                };
                let target = target_address.storage_key()?;
                batch.checks.push(Check {
                    key: target.clone(),
                    expected: Expected::Missing,
                });
                batch.mutations.push(Mutation::Put { key: target, value });
            }
            if let Some(entry) = policy_entry {
                let target = target_address.policy_key()?;
                batch.checks.push(Check {
                    key: target.clone(),
                    expected: Expected::Missing,
                });
                batch.mutations.push(Mutation::Put {
                    key: target,
                    value: entry.value.clone(),
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

/// An all-or-nothing collection of checked writes. Identical checks may be
/// shared, but writes must be unique and ownership insertions must not
/// invalidate another component's view of the graph.
#[derive(Default)]
pub struct Transaction {
    checks: Vec<Check>,
    mutations: Vec<Mutation>,
    checked: HashSet<Vec<u8>>,
    written: HashSet<Vec<u8>>,
}

// Independently prepared task observations can race. Compose their fences
// before appending batches so disagreement becomes a normal commit conflict.
pub(crate) fn reconcile_task_checks(documents: &mut OwnedBatch, graph: &mut OwnedBatch) {
    for other in &mut graph.checks {
        if !other.key.starts_with(crate::task_store::PREFIX) {
            continue;
        }
        if let Some(check) = documents.checks.iter_mut().find(|c| c.key == other.key) {
            let expected = match (&check.expected, &other.expected) {
                (Expected::Version(a), Expected::VersionBefore(b, deadline))
                | (Expected::VersionBefore(b, deadline), Expected::Version(a))
                    if a == b =>
                {
                    Expected::VersionBefore(*a, *deadline)
                }
                (Expected::VersionBefore(a, x), Expected::VersionBefore(b, y)) if a == b => {
                    Expected::VersionBefore(*a, (*x).min(*y))
                }
                (a, b) if a == b => a.clone(),
                _ => Expected::VersionBefore(0, std::time::Instant::now()),
            };
            check.expected = expected.clone();
            other.expected = expected;
        }
    }
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
        let mut unique = Vec::new();
        for check in &batch.checks {
            if checks.insert(check.key.clone()) {
                unique.push(check.clone());
            } else if !self
                .checks
                .iter()
                .chain(unique.iter())
                .any(|existing| existing == check)
            {
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
        // Independently prepared graph changes can each observe a missing
        // owner and together introduce a cycle. Checks see the pre-commit
        // state, so identical checks alone cannot detect this composition.
        let graph_key = |key: &[u8]| {
            key.starts_with(crate::task_store::PREFIX)
                || key.starts_with(crate::ownership_edge::PREFIX)
        };
        if batch.checks.iter().any(|check| {
            graph_key(&check.key)
                && check.expected == Expected::Missing
                && ((self.checked.contains(&check.key)
                    && writes.contains(&check.key)
                    && !self.written.contains(&check.key))
                    || self.written.contains(&check.key))
        }) {
            return Err(Error::InvalidBatch);
        }
        self.checked = checks;
        self.written = writes;
        self.checks.extend(unique);
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

type PendingEntries = (
    uuid::Uuid,
    Option<u64>,
    Vec<crate::application_entry::ApplicationEntry>,
    Vec<Check>,
    bool, // whether an entry was staged
);

/// Checked document operations at one invocation-wide immutable revision.
/// Staged values are visible to later reads. A staged new document has version
/// zero; existing staged documents retain their original version until commit.
pub struct TaskDocuments<'a> {
    store: &'a Store,
    revision: u64,
    reads: HashMap<Address, Option<Document>>,
    writes: HashMap<Address, Option<Map<String, Value>>>,
    task: crate::task::Task,
    entries: Option<PendingEntries>,
    entry_initial_len: usize,
    runtime_cut: Option<usize>,
    runtime_config: Option<crate::runtime::Config>,
    signal: OwnedBatch,
    signal_keys: HashSet<(uuid::Uuid, String, String)>,
    child_reads: HashMap<uuid::Uuid, Option<crate::task_store::StoredTask>>,
    pub(crate) keyed_children: Vec<(String, crate::task::Task, crate::keyed_child::Prepared)>,
}
impl<'a> TaskDocuments<'a> {
    pub(crate) fn belongs_to(&self, store: &Store, task: &crate::task::Task) -> bool {
        std::ptr::eq(self.store, store) && &self.task == task
    }

    pub(crate) fn new(store: &'a Store, task: &crate::task::Task) -> Result<Self, Error> {
        Ok(Self {
            store,
            task: task.clone(),
            entries: None,
            entry_initial_len: 0,
            runtime_cut: None,
            runtime_config: None,
            signal: OwnedBatch {
                checks: Vec::new(),
                mutations: Vec::new(),
            },
            signal_keys: HashSet::new(),
            child_reads: HashMap::new(),
            keyed_children: Vec::new(),
            revision: store.document_snapshot()?,
            reads: HashMap::new(),
            writes: HashMap::new(),
        })
    }
    /// Stage a keyed direct child for this invocation. Nothing is persisted until
    /// the scheduler commits the handler's transition and documents. The returned
    /// ID is stable for an existing matching key; candidate IDs need not match.
    /// A key may be staged once per invocation. Handler errors discard the stage.
    pub fn stage_keyed_child(
        &mut self,
        key: &str,
        tool: &str,
        arguments: &Value,
        child: crate::task::Task,
    ) -> Result<uuid::Uuid, String> {
        if self
            .keyed_children
            .iter()
            .any(|(existing, _, _)| existing == key)
        {
            return Err("key already staged in this invocation".into());
        }
        let parent = crate::task_store::TaskStore::new(self.store)
            .load(self.task.id())
            .map_err(|e| e.to_string())?
            .ok_or("parent missing")?;
        if parent.task != self.task {
            return Err("parent changed during invocation".into());
        }
        let prepared =
            crate::keyed_child::prepare(self.store, &parent, key, tool, arguments, &child)
                .map_err(|e| e.to_string())?;
        let id = prepared.child;
        self.keyed_children.push((key.into(), child, prepared));
        Ok(id)
    }

    /// Read a direct owned child and fence the handler's whole transition on
    /// that version. Repeated reads use the same snapshot. This does not read
    /// children staged for creation in the current uncommitted invocation.
    pub fn get_owned_child(&mut self, id: uuid::Uuid) -> Result<crate::task::Task, String> {
        if !self.child_reads.contains_key(&id) {
            let child = crate::task_store::TaskStore::new(self.store)
                .load(id)
                .map_err(|e| e.to_string())?;
            let expected = child
                .as_ref()
                .map_or(Expected::Missing, |child| Expected::Version(child.version));
            let key = crate::task_store::key(id);
            if let Some(check) = self.signal.checks.iter_mut().find(|c| c.key == key) {
                let compatible = match (&check.expected, &expected) {
                    (Expected::Version(a), Expected::Version(b))
                    | (Expected::VersionBefore(a, _), Expected::Version(b)) => a == b,
                    (Expected::Missing, Expected::Missing) => true,
                    _ => false,
                };
                if !compatible {
                    check.expected = Expected::VersionBefore(0, std::time::Instant::now());
                }
            } else {
                self.signal.checks.push(Check { key, expected });
            }
            self.child_reads.insert(id, child);
        }
        let child = self.child_reads[&id]
            .as_ref()
            .ok_or("owned child missing")?;
        if child.task.owner() != &crate::task::TaskOwner::Task(self.task.id()) {
            return Err("task is not a direct child".into());
        }
        Ok(child.task.clone())
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
    /// Stage distinct typed deliveries before document or entry writes. The returned
    /// outcome is provisional until this task's checkpoint commits. A conflict
    /// replays only registered replay-safe handlers. Supply a fresh clock value
    /// on replay; elapsed monotonic time is checked at the storage commit boundary.
    /// Clock jumps after staging are not observed. Self-delivery is excluded.
    pub fn signal_at(
        &mut self,
        task_id: uuid::Uuid,
        token: &crate::task_store::SignalToken,
        key: &str,
        value: Value,
        now_millis: i64,
    ) -> Result<crate::task_store::SignalResult, Error> {
        if !self.writes.is_empty() || self.entries.as_ref().is_some_and(|entries| entries.4) {
            return Err(Error::ReadAfterWrite);
        }
        let identity = (task_id, token.name().to_owned(), key.to_owned());
        if task_id == self.task.id() || self.signal_keys.contains(&identity) {
            return Err(Error::SignalComposition);
        }
        let (result, batch) = crate::task_store::TaskStore::new(self.store).prepare_signal_at(
            task_id,
            token,
            key,
            value,
            || now_millis,
        )?;
        // Only the target and its revision may be shared between deliveries.
        // Other overlaps remain errors when the complete transaction is composed.
        let shared = [
            crate::task_store::key(task_id),
            crate::task_store::signal_revision_key(task_id),
        ];
        for check in batch.checks {
            if shared.contains(&check.key)
                && let Some(existing) = self.signal.checks.iter_mut().find(|c| c.key == check.key)
            {
                existing.expected = match (&existing.expected, &check.expected) {
                    (Expected::VersionBefore(a, x), Expected::VersionBefore(b, y)) if a == b => {
                        Expected::VersionBefore(*a, (*x).min(*y))
                    }
                    (Expected::Version(a), Expected::VersionBefore(b, x))
                    | (Expected::VersionBefore(b, x), Expected::Version(a))
                        if a == b =>
                    {
                        Expected::VersionBefore(*a, *x)
                    }
                    (a, b) if a == b => a.clone(),
                    // Observations disagreed during staging. Force a commit conflict
                    // so the normal replay-safe retry policy handles the race.
                    _ => Expected::VersionBefore(0, std::time::Instant::now()),
                };
                continue;
            }
            self.signal.checks.push(check);
        }
        for mutation in batch.mutations {
            if let Mutation::Put { key, .. } = &mutation
                && *key == shared[1]
                && self
                    .signal
                    .mutations
                    .iter()
                    .any(|m| matches!(m, Mutation::Put { key: k, .. } if k == key))
            {
                continue;
            }
            self.signal.mutations.push(mutation);
        }
        self.signal_keys.insert(identity);
        Ok(result)
    }

    /// Stage an immutable application record with this task as its author.
    /// The root conversation and every task ancestor must exist and not be aborting. The returned ID is
    /// provisional until the step commits and can change on replay.
    pub fn append_entry(&mut self, kind: &str, data: Value) -> Result<uuid::Uuid, Error> {
        self.append_entry_inner(None, kind, data)
    }

    /// Stage one unchanged runtime checkpoint after the entries already staged.
    /// At least one entry must precede it. Further entries may be appended; all
    /// effects commit with the task step. The runtime version and observer head
    /// are fenced, so a concurrent runtime change causes the whole step to retry.
    pub fn checkpoint_runtime(&mut self) -> Result<(), Error> {
        let Some((_, _, entries, _, true)) = &self.entries else {
            return Err(Error::InvalidApplicationEntry);
        };
        if self.runtime_cut.is_some() {
            return Err(Error::InvalidApplicationEntry);
        }
        self.runtime_cut = Some(entries.len());
        Ok(())
    }

    /// Stage an ordered checkpoint that changes configuration for an idle root
    /// conversation. The new configuration is the end-of-commit agent state,
    /// including for entries before the checkpoint. Model context is unchanged.
    pub fn configure_runtime_at_checkpoint(
        &mut self,
        config: crate::runtime::Config,
    ) -> Result<(), Error> {
        if config.model.is_empty() || config.max_output_tokens == 0 {
            return Err(Error::InvalidApplicationEntry);
        }
        self.checkpoint_runtime()?;
        self.runtime_config = Some(config);
        Ok(())
    }

    /// Append only if the application log's last entry matches `expected`.
    /// `None` expects an empty log. Earlier entries staged in this invocation
    /// are included. Even a mismatch retains a read dependency on the observed
    /// log and subsequent checks use that observation. The log version is fenced
    /// at commit; IDs are provisional and may change on replay.
    pub fn append_entry_if_head(
        &mut self,
        expected: Option<uuid::Uuid>,
        kind: &str,
        data: Value,
    ) -> Result<uuid::Uuid, Error> {
        self.append_entry_inner(Some(expected), kind, data)
    }

    fn append_entry_inner(
        &mut self,
        expected: Option<Option<uuid::Uuid>>,
        kind: &str,
        data: Value,
    ) -> Result<uuid::Uuid, Error> {
        if kind.is_empty() || kind.starts_with("pi.") {
            return Err(Error::InvalidApplicationEntry);
        }
        if self.entries.is_none() {
            let mut owner = self.task.owner().clone();
            let mut seen = HashSet::from([self.task.id()]);
            let mut checks = Vec::new();
            let conversation = loop {
                match owner {
                    crate::task::TaskOwner::Conversation(id) => break id,
                    crate::task::TaskOwner::Task(id) => {
                        if !seen.insert(id) {
                            return Err(Error::InvalidApplicationEntry);
                        }
                        let key = crate::task_store::key(id);
                        let record = self
                            .store
                            .get(&key)?
                            .ok_or(Error::InvalidApplicationEntry)?;
                        let ancestor: crate::task::Task = serde_json::from_slice(&record.value)?;
                        if ancestor.id() != id
                            || ancestor.is_terminal()
                            || ancestor.abort_requested()
                        {
                            return Err(Error::InvalidApplicationEntry);
                        }
                        checks.push(Check {
                            key,
                            expected: Expected::Version(record.version),
                        });
                        owner = ancestor.owner().clone();
                    }
                }
            };
            let key = format!("runtime/conversation/{conversation}").into_bytes();
            let root = self
                .store
                .get(&key)?
                .ok_or(Error::InvalidApplicationEntry)?;
            let state: crate::runtime::Conversation = serde_json::from_slice(&root.value)?;
            if state.id != conversation || state.abort_requested {
                return Err(Error::InvalidApplicationEntry);
            }
            checks.push(Check {
                key,
                expected: Expected::Version(root.version),
            });
            let record = self
                .store
                .get(&crate::application_entry::key(conversation))?;
            let version = record.as_ref().map(|r| r.version);
            let entries = record
                .map(|r| serde_json::from_slice(&r.value))
                .transpose()?
                .unwrap_or_default();
            let entries: Vec<crate::application_entry::ApplicationEntry> = entries;
            self.entry_initial_len = entries.len();
            self.entries = Some((conversation, version, entries, checks, false));
        }
        if expected.is_some_and(|head| {
            self.entries
                .as_ref()
                .unwrap()
                .2
                .last()
                .map(|entry| entry.id)
                != head
        }) {
            return Err(Error::ApplicationEntryHeadMismatch);
        }
        let id = uuid::Uuid::new_v4();
        let conversation = self.entries.as_ref().unwrap().0;
        self.entries.as_mut().unwrap().4 = true;
        self.entries
            .as_mut()
            .unwrap()
            .2
            .push(crate::application_entry::ApplicationEntry {
                id,
                conversation_id: conversation,
                author_task_id: self.task.id(),
                kind: kind.into(),
                data,
            });
        Ok(id)
    }
    pub(crate) fn into_batch(self) -> Result<OwnedBatch, Error> {
        // Only the scheduler can validate and commit staged keyed children.
        if !self.keyed_children.is_empty() {
            return Err(Error::InvalidBatch);
        }
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
        batch.checks.extend(self.signal.checks);
        batch.mutations.extend(self.signal.mutations);
        if let Some((conversation, version, entries, checks, dirty)) = self.entries {
            if let Some(index) = self.runtime_cut {
                let cut = crate::runtime::prepare_ordered_task_checkpoint(
                    self.store,
                    conversation,
                    &checks,
                    self.entry_initial_len,
                    index,
                    &entries,
                    self.runtime_config.as_ref(),
                )?;
                batch.checks.extend(cut.checks);
                batch.mutations.extend(cut.mutations);
            }
            batch.checks.extend(checks);
            let key = crate::application_entry::key(conversation);
            batch.checks.push(Check {
                key: key.clone(),
                expected: version.map_or(Expected::Missing, Expected::Version),
            });
            if dirty {
                batch.mutations.push(Mutation::Put {
                    key,
                    value: serde_json::to_vec(&entries)?,
                });
            }
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

#[cfg(test)]
mod nested_entry_tests {
    use super::*;
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_store::{Save, TaskStore};
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn racing_child_observations_conflict_and_preserve_deadlines() {
        let db = Store::memory().unwrap();
        let store = TaskStore::new(&db);
        let parent = task(1, TaskOwner::Conversation(Uuid::from_u128(9)));
        let child = task(2, TaskOwner::Task(parent.id()));
        store
            .save(&[Save::Create(&parent), Save::Create(&child)])
            .unwrap();
        let mut docs = TaskDocuments::new(&db, &parent).unwrap();
        docs.get_owned_child(child.id()).unwrap();
        let old = store.load(child.id()).unwrap().unwrap();
        store.update(&old, |t| t.start()).unwrap();
        let new = store.load(child.id()).unwrap().unwrap();
        let mut document_batch = docs.into_batch().unwrap();
        let mut graph = OwnedBatch {
            checks: vec![Check {
                key: crate::task_store::key(child.id()),
                expected: Expected::Version(new.version),
            }],
            mutations: vec![],
        };
        reconcile_task_checks(&mut document_batch, &mut graph);
        let mut tx = Transaction::new();
        tx.append(document_batch).unwrap();
        tx.append(graph).unwrap();
        assert!(matches!(
            tx.commit(&db),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        let key = crate::task_store::key(child.id());
        let mut document_batch = OwnedBatch {
            checks: vec![Check {
                key: key.clone(),
                expected: Expected::VersionBefore(new.version, deadline),
            }],
            mutations: vec![],
        };
        let mut graph = OwnedBatch {
            checks: vec![Check {
                key,
                expected: Expected::Version(new.version),
            }],
            mutations: vec![],
        };
        reconcile_task_checks(&mut document_batch, &mut graph);
        assert_eq!(
            graph.checks[0].expected,
            Expected::VersionBefore(new.version, deadline)
        );
        let mut tx = Transaction::new();
        tx.append(document_batch).unwrap();
        tx.append(graph).unwrap();
        tx.commit(&db).unwrap();
    }

    #[test]
    fn owned_child_read_is_cached_fenced_and_rejects_foreign_tasks() {
        let db = Store::memory().unwrap();
        let store = TaskStore::new(&db);
        let parent = task(1, TaskOwner::Conversation(Uuid::from_u128(9)));
        let child = task(2, TaskOwner::Task(parent.id()));
        let foreign = task(3, TaskOwner::Conversation(Uuid::from_u128(9)));
        store
            .save(&[
                Save::Create(&parent),
                Save::Create(&child),
                Save::Create(&foreign),
            ])
            .unwrap();
        let mut docs = TaskDocuments::new(&db, &parent).unwrap();
        assert!(docs.get_owned_child(foreign.id()).is_err());
        assert!(docs.get_owned_child(Uuid::from_u128(99)).is_err());
        let first = docs.get_owned_child(child.id()).unwrap();
        let address = Address {
            scope: "test".into(),
            name: "result".into(),
            key: "".into(),
        };
        docs.get(&address).unwrap();
        docs.put(
            &address,
            json!({"committed": true}).as_object().unwrap().clone(),
        )
        .unwrap();
        let stored = store.load(child.id()).unwrap().unwrap();
        store.update(&stored, |t| t.start()).unwrap();
        assert_eq!(docs.get_owned_child(child.id()).unwrap(), first);
        let mut transaction = Transaction::new();
        transaction.append(docs.into_batch().unwrap()).unwrap();
        assert!(matches!(
            transaction.commit(&db),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert!(Documents::new(&db).get(&address).unwrap().is_none());
    }

    #[test]
    fn handled_missing_child_read_is_fenced_against_creation() {
        let db = Store::memory().unwrap();
        let store = TaskStore::new(&db);
        let parent = task(1, TaskOwner::Conversation(Uuid::from_u128(9)));
        store.save(&[Save::Create(&parent)]).unwrap();
        let mut docs = TaskDocuments::new(&db, &parent).unwrap();
        let id = Uuid::from_u128(2);
        assert!(docs.get_owned_child(id).is_err());
        let child = task(2, TaskOwner::Task(parent.id()));
        store.save(&[Save::Create(&child)]).unwrap();
        assert!(docs.get_owned_child(id).is_err());
        let mut tx = Transaction::new();
        tx.append(docs.into_batch().unwrap()).unwrap();
        assert!(matches!(
            tx.commit(&db),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
    }

    #[test]
    fn transaction_coalesces_identical_checks_but_rejects_conflicts() {
        let mut tx = Transaction::new();
        let batch = |expected| OwnedBatch {
            checks: vec![Check {
                key: b"owner".to_vec(),
                expected,
            }],
            mutations: vec![],
        };
        tx.append(batch(Expected::Missing)).unwrap();
        tx.append(batch(Expected::Missing)).unwrap();
        assert!(matches!(
            tx.append(batch(Expected::Version(1))),
            Err(Error::InvalidBatch)
        ));
        tx.commit(&Store::memory().unwrap()).unwrap();
    }

    #[test]
    fn transaction_rejects_independently_prepared_ownership_cycles() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let a = task(1, TaskOwner::Task(Uuid::from_u128(2)));
        let b = task(2, TaskOwner::Task(a.id()));
        for reverse in [false, true] {
            let first = ts
                .prepare_save(&[Save::Create(if reverse { &b } else { &a })])
                .unwrap();
            let second = ts
                .prepare_save(&[Save::Create(if reverse { &a } else { &b })])
                .unwrap();
            let mut tx = Transaction::new();
            tx.append(first).unwrap();
            assert!(matches!(tx.append(second), Err(Error::InvalidBatch)));
            tx.commit(&db).unwrap();
            assert!(
                ts.load(if reverse { b.id() } else { a.id() })
                    .unwrap()
                    .is_some()
            );
            // Start the next ordering against the same empty graph.
            let id = if reverse { b.id() } else { a.id() };
            db.commit(
                &[],
                &[Mutation::Delete {
                    key: crate::task_store::key(id),
                }],
            )
            .unwrap();
        }
    }

    #[test]
    fn transaction_allows_shared_missing_ownership_roots() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let a = task(1, TaskOwner::Task(Uuid::from_u128(3)));
        let b = task(2, TaskOwner::Task(Uuid::from_u128(3)));
        let mut tx = Transaction::new();
        tx.append(ts.prepare_save(&[Save::Create(&a)]).unwrap())
            .unwrap();
        tx.append(ts.prepare_save(&[Save::Create(&b)]).unwrap())
            .unwrap();
        tx.commit(&db).unwrap();
        assert_eq!(ts.list().unwrap().len(), 2);
    }

    #[test]
    fn transaction_rejects_cross_component_missing_edge_in_both_orders() {
        let key = crate::ownership_edge::edge_key(Uuid::from_u128(7));
        for write_first in [false, true] {
            let check = Check {
                key: key.clone(),
                expected: Expected::Missing,
            };
            let read = || OwnedBatch {
                checks: vec![check.clone()],
                mutations: vec![],
            };
            let write = || OwnedBatch {
                checks: vec![check.clone()],
                mutations: vec![Mutation::Put {
                    key: key.clone(),
                    value: vec![],
                }],
            };
            let mut tx = Transaction::new();
            tx.append(if write_first { write() } else { read() })
                .unwrap();
            assert!(matches!(
                tx.append(if write_first { read() } else { write() }),
                Err(Error::InvalidBatch)
            ));
        }
    }

    fn task(id: u128, owner: TaskOwner) -> Task {
        Task::new(
            Uuid::from_u128(id),
            owner,
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
    }

    fn conversation(db: &Store, id: Uuid) {
        let state = crate::runtime::Harness::empty_conversation(
            id,
            crate::runtime::Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 1,
            },
        );
        let key = format!("runtime/conversation/{id}").into_bytes();
        db.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Missing,
            }],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(&state).unwrap(),
            }],
        )
        .unwrap();
    }

    #[test]
    fn application_entry_expected_head_is_atomic_and_includes_staged_entries() {
        for sqlite in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("entries.sqlite");
            let db = if sqlite {
                Store::open(&path).unwrap()
            } else {
                Store::memory().unwrap()
            };
            let id = Uuid::from_u128(123);
            conversation(&db, id);
            let author = task(1, TaskOwner::Conversation(id));
            let mut first = TaskDocuments::new(&db, &author).unwrap();
            let mut stale = TaskDocuments::new(&db, &author).unwrap();
            let addr = Address {
                scope: "test".into(),
                name: "atomic".into(),
                key: "".into(),
            };
            stale.get(&addr).unwrap();
            stale
                .put(&addr, json!({"stale": true}).as_object().unwrap().clone())
                .unwrap();
            stale
                .append_entry_if_head(None, "app.stale", json!(0))
                .unwrap();
            let first_id = first
                .append_entry_if_head(None, "app.first", json!(1))
                .unwrap();
            assert!(matches!(
                first.append_entry_if_head(None, "app.invalid", json!(0)),
                Err(Error::ApplicationEntryHeadMismatch)
            ));
            let second_id = first
                .append_entry_if_head(Some(first_id), "app.second", json!(2))
                .unwrap();
            let batch = first.into_batch().unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
            let batch = stale.into_batch().unwrap();
            assert!(matches!(
                db.commit(&batch.checks, &batch.mutations),
                Err(durable_store::Error::Conflict { .. })
            ));
            assert!(Documents::new(&db).get(&addr).unwrap().is_none());
            let mut mismatch = TaskDocuments::new(&db, &author).unwrap();
            assert!(matches!(
                mismatch.append_entry_if_head(Some(first_id), "app.invalid", json!(0)),
                Err(Error::ApplicationEntryHeadMismatch)
            ));
            let empty = mismatch.into_batch().unwrap();
            assert!(empty.mutations.is_empty());
            let mut next = TaskDocuments::new(&db, &author).unwrap();
            let third_id = next
                .append_entry_if_head(Some(second_id), "app.third", json!(3))
                .unwrap();
            let batch = next.into_batch().unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
            let expected = vec![first_id, second_id, third_id];
            if sqlite {
                drop(db);
                let reopened = Store::open(&path).unwrap();
                let entries = crate::application_entry::list(&reopened, id).unwrap();
                assert_eq!(entries.iter().map(|e| e.id).collect::<Vec<_>>(), expected);
            } else {
                let entries = crate::application_entry::list(&db, id).unwrap();
                assert_eq!(entries.iter().map(|e| e.id).collect::<Vec<_>>(), expected);
            }
        }
    }

    #[test]
    fn handled_head_mismatch_fences_document_writes_and_keeps_snapshot() {
        for sqlite in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = if sqlite {
                Store::open(dir.path().join("db")).unwrap()
            } else {
                Store::memory().unwrap()
            };
            let id = Uuid::from_u128(124);
            conversation(&db, id);
            let author = task(1, TaskOwner::Conversation(id));
            let address = Address {
                scope: "test".into(),
                name: "decision".into(),
                key: "".into(),
            };
            let mut decision = TaskDocuments::new(&db, &author).unwrap();
            decision.get(&address).unwrap();
            assert!(matches!(
                decision.append_entry_if_head(Some(Uuid::from_u128(99)), "app.x", json!(0)),
                Err(Error::ApplicationEntryHeadMismatch)
            ));
            let mut concurrent = TaskDocuments::new(&db, &author).unwrap();
            concurrent
                .append_entry_if_head(None, "app.concurrent", json!(1))
                .unwrap();
            let batch = concurrent.into_batch().unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
            // A second check sees the same transaction-local observation.
            assert!(matches!(
                decision.append_entry_if_head(Some(Uuid::from_u128(99)), "app.x", json!(0)),
                Err(Error::ApplicationEntryHeadMismatch)
            ));
            decision
                .put(
                    &address,
                    json!({"decision": true}).as_object().unwrap().clone(),
                )
                .unwrap();
            let batch = decision.into_batch().unwrap();
            assert!(matches!(
                db.commit(&batch.checks, &batch.mutations),
                Err(durable_store::Error::Conflict { .. })
            ));
            assert!(Documents::new(&db).get(&address).unwrap().is_none());
        }
    }

    #[test]
    fn inherited_head_is_checked_independently_on_each_fork() {
        let db = Store::memory().unwrap();
        let parent = Uuid::from_u128(125);
        let child = Uuid::from_u128(126);
        conversation(&db, parent);
        conversation(&db, child);
        let mut parent_tx =
            TaskDocuments::new(&db, &task(1, TaskOwner::Conversation(parent))).unwrap();
        let inherited = parent_tx
            .append_entry_if_head(None, "app.parent", json!(1))
            .unwrap();
        let batch = parent_tx.into_batch().unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        let docs = Documents::new(&db);
        let batch = docs
            .fork_scope(parent, child, docs.snapshot().unwrap())
            .unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        let mut parent_tx =
            TaskDocuments::new(&db, &task(1, TaskOwner::Conversation(parent))).unwrap();
        parent_tx
            .append_entry_if_head(Some(inherited), "app.parent2", json!(2))
            .unwrap();
        let batch = parent_tx.into_batch().unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        let mut child_tx =
            TaskDocuments::new(&db, &task(2, TaskOwner::Conversation(child))).unwrap();
        child_tx
            .append_entry_if_head(Some(inherited), "app.child", json!(3))
            .unwrap();
        let batch = child_tx.into_batch().unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        assert_eq!(
            crate::application_entry::list(&db, parent).unwrap().len(),
            2
        );
        assert_eq!(crate::application_entry::list(&db, child).unwrap().len(), 2);
    }

    #[test]
    fn handled_mismatch_fences_root_deletion() {
        let db = Store::memory().unwrap();
        let id = Uuid::from_u128(127);
        conversation(&db, id);
        let mut tx = TaskDocuments::new(&db, &task(1, TaskOwner::Conversation(id))).unwrap();
        assert!(matches!(
            tx.append_entry_if_head(Some(Uuid::from_u128(99)), "app.x", json!(0)),
            Err(Error::ApplicationEntryHeadMismatch)
        ));
        db.commit(
            &[],
            &[Mutation::Delete {
                key: format!("runtime/conversation/{id}").into_bytes(),
            }],
        )
        .unwrap();
        let batch = tx.into_batch().unwrap();
        assert!(matches!(
            db.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
    }

    #[test]
    fn root_conversation_fences_both_abort_orderings() {
        let db = Store::memory().unwrap();
        let id = Uuid::from_u128(99);
        let root = task(1, TaskOwner::Conversation(id));
        let author = task(2, TaskOwner::Task(root.id()));
        TaskStore::new(&db)
            .save(&[Save::Create(&root), Save::Create(&author)])
            .unwrap();
        assert!(matches!(
            TaskDocuments::new(&db, &author)
                .unwrap()
                .append_entry("app.test", json!(1)),
            Err(Error::InvalidApplicationEntry)
        ));
        conversation(&db, id);
        let mut docs = TaskDocuments::new(&db, &author).unwrap();
        let addr = Address {
            scope: "s".into(),
            name: "race".into(),
            key: "".into(),
        };
        docs.get(&addr).unwrap();
        docs.put(&addr, json!({"value": 1}).as_object().unwrap().clone())
            .unwrap();
        docs.append_entry("app.test", json!(1)).unwrap();
        let batch = docs.into_batch().unwrap();
        let key = format!("runtime/conversation/{id}").into_bytes();
        let record = db.get(&key).unwrap().unwrap();
        let mut state: crate::runtime::Conversation =
            serde_json::from_slice(&record.value).unwrap();
        state.abort_requested = true;
        let checks = [Check {
            key: key.clone(),
            expected: Expected::Version(record.version),
        }];
        let mutations = [Mutation::Put {
            key,
            value: serde_json::to_vec(&state).unwrap(),
        }];
        db.commit(&checks, &mutations).unwrap();
        assert!(matches!(
            db.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(Documents::new(&db).get(&addr).unwrap().is_none());
        assert!(crate::application_entry::list(&db, id).unwrap().is_empty());
        assert!(matches!(
            TaskDocuments::new(&db, &author)
                .unwrap()
                .append_entry("app.test", json!(1)),
            Err(Error::InvalidApplicationEntry)
        ));

        let other = Uuid::from_u128(100);
        conversation(&db, other);
        let direct = task(4, TaskOwner::Conversation(other));
        let mut docs = TaskDocuments::new(&db, &direct).unwrap();
        docs.append_entry("app.test", json!(2)).unwrap();
        let batch = docs.into_batch().unwrap();
        let key = format!("runtime/conversation/{other}").into_bytes();
        let record = db.get(&key).unwrap().unwrap();
        let mut state: crate::runtime::Conversation =
            serde_json::from_slice(&record.value).unwrap();
        state.abort_requested = true;
        db.commit(&batch.checks, &batch.mutations).unwrap();
        db.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Version(record.version),
            }],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(&state).unwrap(),
            }],
        )
        .unwrap();
        assert_eq!(crate::application_entry::list(&db, other).unwrap().len(), 1);
    }

    #[test]
    fn ancestry_checks_fence_atomic_commit_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Store::open(&path).unwrap();
        let ts = TaskStore::new(&db);
        conversation(&db, Uuid::from_u128(99));
        let root = task(1, TaskOwner::Conversation(Uuid::from_u128(99)));
        let middle = task(2, TaskOwner::Task(root.id()));
        let author = task(3, TaskOwner::Task(middle.id()));
        ts.save(&[
            Save::Create(&root),
            Save::Create(&middle),
            Save::Create(&author),
        ])
        .unwrap();
        let addr = Address {
            scope: "s".into(),
            name: "n".into(),
            key: "k".into(),
        };
        let mut docs = TaskDocuments::new(&db, &author).unwrap();
        docs.get(&addr).unwrap();
        docs.put(&addr, json!({"value": 1}).as_object().unwrap().clone())
            .unwrap();
        docs.append_entry("app.test", json!(1)).unwrap();
        let batch = docs.into_batch().unwrap();
        let snapshot = ts.load(root.id()).unwrap().unwrap();
        ts.update(&snapshot, |t| t.request_abort()).unwrap();
        assert!(matches!(
            db.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        drop(db);
        let db = Store::open(&path).unwrap();
        assert!(Documents::new(&db).get(&addr).unwrap().is_none());
        assert!(
            crate::application_entry::list(&db, Uuid::from_u128(99))
                .unwrap()
                .is_empty()
        );
        let mut docs = TaskDocuments::new(&db, &author).unwrap();
        assert!(matches!(
            docs.append_entry("app.test", json!(1)),
            Err(Error::InvalidApplicationEntry)
        ));
    }

    #[test]
    fn missing_terminal_and_cyclic_ancestors_fail_closed() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let author = task(3, TaskOwner::Task(Uuid::from_u128(2)));
        let mut docs = TaskDocuments::new(&db, &author).unwrap();
        assert!(matches!(
            docs.append_entry("app.test", json!(null)),
            Err(Error::InvalidApplicationEntry)
        ));
        let mut terminal = task(2, TaskOwner::Conversation(Uuid::from_u128(99)));
        terminal.cancel().unwrap();
        ts.save(&[Save::Create(&terminal)]).unwrap();
        let mut docs = TaskDocuments::new(&db, &author).unwrap();
        assert!(matches!(
            docs.append_entry("app.test", json!(null)),
            Err(Error::InvalidApplicationEntry)
        ));
        let cycle_a = task(4, TaskOwner::Task(Uuid::from_u128(5)));
        let cycle_b = task(5, TaskOwner::Task(cycle_a.id()));
        let cyclic_author = task(6, TaskOwner::Task(cycle_a.id()));
        // Simulate a legacy corrupt store, bypassing current admission checks.
        let corrupt = [cycle_a, cycle_b]
            .iter()
            .map(|task| Mutation::Put {
                key: crate::task_store::key(task.id()),
                value: serde_json::to_vec(task).unwrap(),
            })
            .collect::<Vec<_>>();
        db.commit(&[], &corrupt).unwrap();
        let mut docs = TaskDocuments::new(&db, &cyclic_author).unwrap();
        assert!(matches!(
            docs.append_entry("app.test", json!(null)),
            Err(Error::InvalidApplicationEntry)
        ));
    }
    #[test]
    fn process_exit_discards_nested_staging() {
        const ENV: &str = "PI_NESTED_ENTRY_CRASH_DB";
        if let Ok(path) = std::env::var(ENV) {
            let db = Store::open(path).unwrap();
            let author = task(3, TaskOwner::Task(Uuid::from_u128(2)));
            let mut docs = TaskDocuments::new(&db, &author).unwrap();
            let addr = Address {
                scope: "s".into(),
                name: "crash".into(),
                key: "".into(),
            };
            docs.get(&addr).unwrap();
            docs.put(&addr, json!({"value": 1}).as_object().unwrap().clone())
                .unwrap();
            docs.append_entry("app.crash", json!(1)).unwrap();
            std::process::exit(0);
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = Store::open(&path).unwrap();
            conversation(&db, Uuid::from_u128(99));
            let root = task(1, TaskOwner::Conversation(Uuid::from_u128(99)));
            let middle = task(2, TaskOwner::Task(root.id()));
            let author = task(3, TaskOwner::Task(middle.id()));
            TaskStore::new(&db)
                .save(&[
                    Save::Create(&root),
                    Save::Create(&middle),
                    Save::Create(&author),
                ])
                .unwrap();
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("document::nested_entry_tests::process_exit_discards_nested_staging")
            .env(ENV, &path)
            .status()
            .unwrap();
        assert!(status.success());
        let db = Store::open(&path).unwrap();
        let addr = Address {
            scope: "s".into(),
            name: "crash".into(),
            key: "".into(),
        };
        assert!(Documents::new(&db).get(&addr).unwrap().is_none());
        assert!(
            crate::application_entry::list(&db, Uuid::from_u128(99))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            TaskStore::new(&db)
                .load(Uuid::from_u128(3))
                .unwrap()
                .unwrap()
                .task
                .checkpoint()
                .phase,
            "start"
        );
    }
    #[test]
    fn deep_author_commits_to_root_conversation_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let root_id = Uuid::from_u128(99);
        {
            let db = Store::open(&path).unwrap();
            conversation(&db, root_id);
            let root = task(1, TaskOwner::Conversation(root_id));
            let middle = task(2, TaskOwner::Task(root.id()));
            let author = task(3, TaskOwner::Task(middle.id()));
            TaskStore::new(&db)
                .save(&[
                    Save::Create(&root),
                    Save::Create(&middle),
                    Save::Create(&author),
                ])
                .unwrap();
            let mut docs = TaskDocuments::new(&db, &author).unwrap();
            docs.append_entry("app.deep", json!(42)).unwrap();
            let batch = docs.into_batch().unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
        }
        let db = Store::open(&path).unwrap();
        let entries = crate::application_entry::list(&db, root_id).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].conversation_id, root_id);
        assert_eq!(entries[0].author_task_id, Uuid::from_u128(3));
        assert_eq!(entries[0].data, json!(42));
    }
}

#[cfg(test)]
mod fork_current_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn fork_fence_rejects_concurrent_creation_and_atomic_value_policy_change() {
        let db = Store::memory().unwrap();
        let parent = uuid::Uuid::new_v4();
        let child = uuid::Uuid::new_v4();
        let address = Address {
            scope: parent.to_string(),
            name: "a".into(),
            key: "".into(),
        };
        let mut tx = Transaction::new();
        tx.put(
            &address,
            Expected::Missing,
            json!({"n": 1}).as_object().unwrap(),
        )
        .unwrap();
        tx.set_fork_policy(&address, Expected::Missing, &ForkPolicy::Current)
            .unwrap();
        tx.commit(&db).unwrap();
        let docs = Documents::new(&db);
        let cut = docs.snapshot().unwrap();
        let batch = docs.fork_scope(parent, child, cut).unwrap();
        // A writer on another thread commits while the prepared fork is pending.
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let mut tx = Transaction::new();
                    tx.put(
                        &address,
                        Expected::Version(docs.get(&address).unwrap().unwrap().version),
                        json!({"n": 2}).as_object().unwrap(),
                    )
                    .unwrap();
                    tx.set_fork_policy(
                        &address,
                        Expected::Version(docs.fork_policy(&address).unwrap().unwrap().version),
                        &ForkPolicy::Copy,
                    )
                    .unwrap();
                    tx.commit(&db).unwrap();
                })
                .join()
                .unwrap();
        });
        assert!(matches!(
            db.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        let batch = docs.fork_scope(parent, child, cut).unwrap();
        let new_address = Address {
            name: "new".into(),
            ..address.clone()
        };
        let mut tx = Transaction::new();
        tx.set_fork_policy(&new_address, Expected::Missing, &ForkPolicy::Current)
            .unwrap();
        tx.commit(&db).unwrap();
        assert!(matches!(
            db.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        let batch = docs.fork_scope(parent, child, cut).unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        let copied = Address {
            scope: child.to_string(),
            ..address
        };
        assert_eq!(docs.get(&copied).unwrap().unwrap().value["n"], 2);
        assert_eq!(
            docs.fork_policy(&copied).unwrap().unwrap().policy,
            ForkPolicy::Copy
        );
    }
}
