//! Atomic, versioned byte-key storage for durable runtimes.
//!
//! Both backends serialize checks and writes in one transaction. SQLite uses
//! WAL and FULL synchronization; memory storage is intentionally ephemeral.
//! Versions are unique within a database, including across delete/recreate.
//! A storage error during commit can leave its outcome unknown: reopen and read
//! the database before deciding whether to retry a non-idempotent operation.

pub mod controls;
#[cfg(feature = "experimental-graph-revision")]
pub mod graph_revision;
pub mod owner;

use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::{
    fmt,
    path::Path,
    sync::Mutex,
    time::{Duration, Instant},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub value: Vec<u8>,
    pub version: u64,
}
pub type KeyedEntry = (Vec<u8>, Entry);
pub type MetadataPage = (Option<Entry>, Vec<KeyedEntry>);
pub type DocumentChange = (Vec<u8>, Option<Entry>);
pub type DocumentCommit = (u64, Vec<DocumentChange>);
type BoundedHistoryRow = (Option<Vec<u8>>, Option<u64>, Option<u64>);
type BoundedChangeRow = (u64, Option<Vec<u8>>, Option<u64>, Option<u64>);
type ReadLimit = (std::rc::Rc<()>, Vec<Vec<u8>>, usize);

thread_local! {
    static READ_LIMITS: std::cell::RefCell<Vec<ReadLimit>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Synchronously limit individual values read from selected namespaces.
/// Nested scopes use the smallest applicable limit and are restored on drop.
pub struct BoundedReadScope(std::rc::Rc<()>);
impl BoundedReadScope {
    pub fn enter(prefixes: &[&[u8]], max_bytes: usize) -> Self {
        let token = std::rc::Rc::new(());
        READ_LIMITS.with(|limits| {
            limits.borrow_mut().push((
                token.clone(),
                prefixes.iter().map(|prefix| prefix.to_vec()).collect(),
                max_bytes,
            ))
        });
        Self(token)
    }
}
impl Drop for BoundedReadScope {
    fn drop(&mut self) {
        READ_LIMITS.with(|limits| {
            limits
                .borrow_mut()
                .retain(|(token, _, _)| !std::rc::Rc::ptr_eq(token, &self.0));
        });
    }
}
fn read_limit(key: &[u8]) -> Option<usize> {
    READ_LIMITS.with(|limits| {
        limits
            .borrow()
            .iter()
            .filter(|(_, prefixes, _)| prefixes.iter().any(|prefix| key.starts_with(prefix)))
            .map(|(_, _, limit)| *limit)
            .min()
    })
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Check {
    pub key: Vec<u8>,
    pub expected: Expected,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expected {
    Missing,
    /// Key remains absent and has not been put since this global version.
    MissingUnchangedSince(u64),
    /// The same absence history fence with a monotonic deadline.
    MissingUnchangedSinceBefore(u64, Instant),
    /// Key must remain absent and the monotonic deadline must not elapse.
    MissingBefore(Instant),
    /// No put under this byte prefix after the observed database put version.
    /// Pair this with checks on observed keys to fence deletion.
    PrefixUnchangedSince(u64),
    Version(u64),
    /// Version must match and the monotonic deadline must not have elapsed.
    VersionBefore(u64, Instant),
    /// Document key has no mutations after this history boundary.
    DocumentUnchangedSince(u64),
    /// The database-wide document revision is still exactly this boundary.
    DocumentRevision(u64),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug)]
pub enum Error {
    Conflict {
        key: Vec<u8>,
    },
    Storage(rusqlite::Error),
    /// The SQLite writer lock was unavailable before a transaction began.
    WriteLockBusy(rusqlite::Error),
    LockPoisoned,
    VersionExhausted,
    InvalidSnapshot,
    IncompatibleGraphProtocol,
    MigrationOwnerUnavailable(String),
    DocumentFrameTooLarge,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict { key } => write!(f, "condition failed for key {key:?}"),
            Self::Storage(e) => write!(f, "storage error: {e}"),
            Self::WriteLockBusy(e) => write!(f, "writer lock unavailable before transaction: {e}"),
            Self::LockPoisoned => write!(f, "store lock poisoned"),
            Self::VersionExhausted => write!(f, "store version exhausted"),
            Self::MigrationOwnerUnavailable(reason) => write!(
                f,
                "graph protocol migration requires exclusive ownership: {reason}"
            ),
            Self::IncompatibleGraphProtocol => write!(
                f,
                "durable graph protocol is incompatible with this runtime"
            ),
            Self::InvalidSnapshot => write!(
                f,
                "snapshot is newer than document history or lacks tracking evidence"
            ),
            Self::DocumentFrameTooLarge => write!(f, "document commit exceeds merged watch limits"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(e) | Self::WriteLockBusy(e) => Some(e),
            _ => None,
        }
    }
}
impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Self::Storage(e)
    }
}

/// Explicit acquisition of an exact document key in the same transaction as
/// ordinary checked mutations. This is not a key in the generic store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskDocumentAcquisition {
    pub document_key: Vec<u8>,
    pub owner: uuid::Uuid,
}

// The same persisted declarations are expanded in `pi-rs-durable::task`.
// Keep schema validation independent of the runtime crate and its executors.
#[allow(dead_code)]
mod task_record {
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use uuid::Uuid;

    durable_task_schema::declare_signal_key!();
    durable_task_schema::declare_task_schema!(SignalKey);

    pub(super) fn terminal(value: &[u8], owner: &[u8]) -> Option<bool> {
        let task: Task = serde_json::from_slice(value).ok()?;
        if task.id.as_bytes().as_slice() != owner {
            return None;
        }
        Some(task.state.is_terminal_state())
    }

    #[cfg(feature = "experimental-graph-revision")]
    pub(super) fn is_owned_reservation(
        before: &[u8],
        after: &[u8],
        id: Uuid,
        interrupted: Uuid,
    ) -> bool {
        let Ok(mut old) = serde_json::from_slice::<Task>(before) else {
            return false;
        };
        let Ok(new) = serde_json::from_slice::<Task>(after) else {
            return false;
        };
        if old.id != id
            || old.state != TaskState::Ready
            || !old.abort_requested
            || old.woken_interrupted_attempt != Some(interrupted)
            || old.restarts > 0 && old.restarts >= old.restart_limit
            || new.attempt_id.is_none_or(|id| id.is_nil())
        {
            return false;
        }
        old.state = TaskState::Running;
        old.attempt_id = new.attempt_id;
        old.retired_attempt_id = None;
        old == new
    }

    #[cfg(feature = "experimental-graph-revision")]
    pub(super) fn is_owned_child_reservation(before: &[u8], after: &[u8], id: Uuid) -> bool {
        let Ok(mut old) = serde_json::from_slice::<Task>(before) else {
            return false;
        };
        let Ok(new) = serde_json::from_slice::<Task>(after) else {
            return false;
        };
        if old.id != id
            || old.state != TaskState::Ready
            || !old.abort_requested
            || old.restarts > 0 && old.restarts >= old.restart_limit
            || new.attempt_id.is_none_or(|id| id.is_nil())
        {
            return false;
        }
        old.state = TaskState::Running;
        old.attempt_id = new.attempt_id;
        old.retired_attempt_id = None;
        old == new
    }

    #[cfg(feature = "experimental-graph-revision")]
    pub(super) fn is_owned_graph_root(value: &[u8], id: Uuid, interrupted: Uuid) -> bool {
        let Ok(task) = serde_json::from_slice::<Task>(value) else {
            return false;
        };
        !interrupted.is_nil()
            && task.id == id
            && task.state == TaskState::Ready
            && task.abort_requested
            && task.woken_interrupted_attempt == Some(interrupted)
    }
}

// Prototype for a single direct owned-conversation child. It is deliberately
// not wired into the protocol until child callback and receipt authentication
// can consume its evidence.
#[cfg(feature = "experimental-graph-revision")]
#[allow(dead_code)]
fn validate_owned_child_reservation(
    tx: &rusqlite::Transaction<'_>,
    mutations: &[Mutation],
) -> Result<(), Error> {
    const PREFIX: &[u8] = b"runtime/woken-owned-child-reservation/v1/";
    let Some((key, bytes)) = mutations.iter().find_map(|m| match m {
        Mutation::Put { key, value } if key.starts_with(PREFIX) => Some((key, value)),
        _ => None,
    }) else {
        return Err(Error::InvalidSnapshot);
    };
    let fail = || Error::Conflict { key: key.clone() };
    if mutations.len() != 2 {
        return Err(fail());
    }
    let suffix = std::str::from_utf8(&key[PREFIX.len()..]).map_err(|_| fail())?;
    let Some((root_text, child_text)) = suffix.split_once('/') else {
        return Err(fail());
    };
    let root = uuid::Uuid::parse_str(root_text).map_err(|_| fail())?;
    let child = uuid::Uuid::parse_str(child_text).map_err(|_| fail())?;
    if suffix != format!("{root}/{child}") || root == child {
        return Err(fail());
    }
    let marker: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| fail())?;
    if marker.as_object().is_none_or(|object| object.len() != 6)
        || marker["version"] != 1
        || marker["root"] != root.to_string()
        || marker["child"] != child.to_string()
    {
        return Err(fail());
    }
    let child_key = [b"durable/task/".as_slice(), child.as_bytes()].concat();
    let after = mutations
        .iter()
        .find_map(|m| match m {
            Mutation::Put { key, value } if key == &child_key => Some(value),
            _ => None,
        })
        .ok_or_else(fail)?;
    let (before, child_version): (Vec<u8>, u64) = tx
        .query_row(
            "SELECT value,version FROM durable_store_entries WHERE key=?1",
            [&child_key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?
        .ok_or_else(fail)?;
    let current: serde_json::Value = serde_json::from_slice(&before).map_err(|_| fail())?;
    if current["background"] == true
        || marker["ready_version"].as_u64() != Some(child_version)
        || marker["running"]
            != serde_json::from_slice::<serde_json::Value>(after).map_err(|_| fail())?
        || !task_record::is_owned_child_reservation(&before, after, child)
    {
        return Err(fail());
    }
    let conversation = current["owner"]["Conversation"]
        .as_str()
        .and_then(|v| uuid::Uuid::parse_str(v).ok())
        .ok_or_else(fail)?;
    let graph_key = format!("runtime/woken-owned-graph/v1/{root}").into_bytes();
    let (graph_bytes, graph_version): (Vec<u8>, u64) = tx
        .query_row(
            "SELECT value,version FROM durable_store_entries WHERE key=?1",
            [&graph_key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?
        .ok_or_else(fail)?;
    let graph: serde_json::Value = serde_json::from_slice(&graph_bytes).map_err(|_| fail())?;
    if graph["version"] != 1
        || graph["root"] != root.to_string()
        || marker["graph_version"].as_u64() != Some(graph_version)
    {
        return Err(fail());
    }
    let root_key = [b"durable/task/".as_slice(), root.as_bytes()].concat();
    let (root_bytes, root_version): (Vec<u8>, u64) = tx
        .query_row(
            "SELECT value,version FROM durable_store_entries WHERE key=?1",
            [&root_key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?
        .ok_or_else(fail)?;
    let root_task: serde_json::Value = serde_json::from_slice(&root_bytes).map_err(|_| fail())?;
    let interrupted = graph["interrupted_attempt"]
        .as_str()
        .and_then(|v| uuid::Uuid::parse_str(v).ok())
        .ok_or_else(fail)?;
    let same_commit: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM durable_commit_versions a JOIN durable_commit_versions b ON a.sequence=b.sequence WHERE a.key=?1 AND a.version=?2 AND b.key=?3 AND b.version=?4)", rusqlite::params![&root_key,root_version,&graph_key,graph_version], |r| r.get(0))?;
    if !same_commit || !task_record::is_owned_graph_root(&root_bytes, root, interrupted) {
        return Err(fail());
    }
    let marked = graph["tasks"]
        .as_array()
        .and_then(|tasks| {
            let mut matches = tasks.iter().filter(|task| task["id"] == child.to_string());
            let task = matches.next()?;
            matches.next().is_none().then_some(task)
        })
        .ok_or_else(fail)?;
    for field in [
        "id",
        "owner",
        "kind",
        "definition_version",
        "input",
        "replay_safe",
        "background",
        "task_tool",
        "retain_task_children_on_success",
    ] {
        if marked.get(field) != current.get(field) {
            return Err(fail());
        }
    }
    let edge = graph["edges"]
        .as_array()
        .and_then(|edges| {
            let mut matches = edges
                .iter()
                .filter(|edge| edge["edge"]["child"] == conversation.to_string());
            let edge = matches.next()?;
            matches.next().is_none().then_some(edge)
        })
        .ok_or_else(fail)?;
    if edge["edge"]["task"] != root.to_string()
        || edge["edge"]["mode"] != "Foreground"
        || edge["edge"]["task_owner"] != root_task["owner"]
    {
        return Err(fail());
    }
    let edge_key = [
        b"durable/conversation-owner/".as_slice(),
        conversation.as_bytes(),
    ]
    .concat();
    let (edge_bytes, edge_version): (Vec<u8>, u64) = tx
        .query_row(
            "SELECT value,version FROM durable_store_entries WHERE key=?1",
            [&edge_key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?
        .ok_or_else(fail)?;
    if edge["edge_version"].as_u64() != Some(edge_version)
        || edge["edge"]
            != serde_json::from_slice::<serde_json::Value>(&edge_bytes).map_err(|_| fail())?
    {
        return Err(fail());
    }
    let marker_exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM durable_store_entries WHERE key=?1)",
        [key],
        |r| r.get(0),
    )?;
    if marker_exists {
        return Err(fail());
    }
    let boundary = graph["structure"]["boundary"].as_u64().ok_or_else(fail)?;
    let (current_version, legacy): (u64,u64) = tx.query_row("SELECT s.version,c.legacy_version FROM durable_store_meta s JOIN durable_commit_meta c ON s.id=c.id WHERE s.id=1", [], |r| Ok((r.get(0)?,r.get(1)?)))?;
    if boundary < legacy || boundary > current_version {
        return Err(fail());
    }
    for (revision_key, field) in [
        (
            graph_revision::task_structure_revision(root),
            "root_revision",
        ),
        (
            graph_revision::global_structure_revision(),
            "global_revision",
        ),
    ] {
        let expected = &graph["structure"][field];
        let version: Option<u64> = tx
            .query_row(
                "SELECT version FROM durable_store_entries WHERE key=?1",
                [&revision_key],
                |r| r.get(0),
            )
            .optional()?;
        if expected.as_u64() != version || (!expected.is_null() && expected.as_u64().is_none()) {
            return Err(fail());
        }
        if version.is_none() {
            let changed: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM durable_commit_versions WHERE key=?1 AND version>?2)",
                rusqlite::params![&revision_key, boundary],
                |r| r.get(0),
            )?;
            if changed {
                return Err(fail());
            }
        }
    }
    Ok(())
}

// Protocol v3 validates the entire reservation write set under the SQLite
// write lock and prevents older writers from bypassing this validation.
#[cfg(feature = "experimental-graph-revision")]
fn validate_owned_reservation(
    tx: &rusqlite::Transaction<'_>,
    mutations: &[Mutation],
) -> Result<(), Error> {
    const PREFIX: &[u8] = b"runtime/woken-owned-reservation/v1/";
    const CHILD_PREFIX: &[u8] = b"runtime/woken-owned-child-reservation/";
    if let Some(key) = mutations.iter().find_map(|mutation| match mutation {
        Mutation::Put { key, .. } | Mutation::Delete { key } if key.starts_with(CHILD_PREFIX) => {
            Some(key)
        }
        _ => None,
    }) {
        // The child path remains disabled until its graph and identity proof
        // can be consumed at every callback boundary.
        return Err(Error::Conflict { key: key.clone() });
    }
    let markers: Vec<_> = mutations
        .iter()
        .filter_map(|m| match m {
            Mutation::Put { key, value } if key.starts_with(PREFIX) => Some((key, value)),
            Mutation::Delete { key } if key.starts_with(PREFIX) => Some((key, key)),
            _ => None,
        })
        .collect();
    if markers.is_empty() {
        return Ok(());
    }
    let fail = || Error::Conflict {
        key: markers[0].0.clone(),
    };
    if markers.len() != 1 || mutations.len() != 2 {
        return Err(fail());
    }
    let (marker_key, marker_bytes) = markers[0];
    let id = std::str::from_utf8(&marker_key[PREFIX.len()..])
        .ok()
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
        .ok_or_else(fail)?;
    if marker_key != format!("runtime/woken-owned-reservation/v1/{id}").as_bytes() {
        return Err(fail());
    }
    let marker: serde_json::Value = serde_json::from_slice(marker_bytes).map_err(|_| fail())?;
    if marker.as_object().is_none_or(|v| v.len() != 4) || marker["version"] != 1 {
        return Err(fail());
    }
    let task_key = [b"durable/task/".as_slice(), id.as_bytes()].concat();
    let after = mutations
        .iter()
        .find_map(|m| match m {
            Mutation::Put { key, value } if key == &task_key => Some(value),
            _ => None,
        })
        .ok_or_else(fail)?;
    let (before, ready_version): (Vec<u8>, u64) = tx
        .query_row(
            "SELECT value,version FROM durable_store_entries WHERE key=?1",
            [&task_key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?
        .ok_or_else(fail)?;
    let graph_key = format!("runtime/woken-owned-graph/v1/{id}").into_bytes();
    let (graph_bytes, graph_version): (Vec<u8>, u64) = tx
        .query_row(
            "SELECT value,version FROM durable_store_entries WHERE key=?1",
            [&graph_key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?
        .ok_or_else(fail)?;
    let graph: serde_json::Value = serde_json::from_slice(&graph_bytes).map_err(|_| fail())?;
    let same_commit: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM durable_commit_versions a JOIN durable_commit_versions b ON a.sequence=b.sequence WHERE a.key=?1 AND a.version=?2 AND b.key=?3 AND b.version=?4)", rusqlite::params![&task_key, ready_version, &graph_key, graph_version], |r| r.get(0))?;
    let interrupted = graph["interrupted_attempt"]
        .as_str()
        .and_then(|v| uuid::Uuid::parse_str(v).ok())
        .ok_or_else(fail)?;
    if !same_commit
        || graph["version"] != 1
        || graph["root"] != id.to_string()
        || marker["graph_version"].as_u64() != Some(graph_version)
        || marker["ready_version"].as_u64() != Some(ready_version)
        || marker["running"]
            != serde_json::from_slice::<serde_json::Value>(after).map_err(|_| fail())?
        || !task_record::is_owned_reservation(&before, after, id, interrupted)
    {
        return Err(fail());
    }
    let exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM durable_store_entries WHERE key=?1)",
        [marker_key],
        |r| r.get(0),
    )?;
    if exists {
        return Err(fail());
    }
    let boundary = graph["structure"]["boundary"].as_u64().ok_or_else(fail)?;
    let current: u64 = tx.query_row(
        "SELECT version FROM durable_store_meta WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    let legacy: u64 = tx.query_row(
        "SELECT legacy_version FROM durable_commit_meta WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    if boundary < legacy || boundary > current {
        return Err(fail());
    }
    for (key, field) in [
        (graph_revision::task_structure_revision(id), "root_revision"),
        (
            graph_revision::global_structure_revision(),
            "global_revision",
        ),
    ] {
        let expected = &graph["structure"][field];
        let version: Option<u64> = tx
            .query_row(
                "SELECT version FROM durable_store_entries WHERE key=?1",
                [&key],
                |r| r.get(0),
            )
            .optional()?;
        if expected.as_u64() != version || (!expected.is_null() && expected.as_u64().is_none()) {
            return Err(fail());
        }
        if version.is_none() {
            let changed: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM durable_commit_versions WHERE key=?1 AND version>?2)",
                rusqlite::params![&key, boundary],
                |r| r.get(0),
            )?;
            if changed {
                return Err(fail());
            }
        }
    }
    Ok(())
}

#[cfg(all(test, feature = "experimental-graph-revision"))]
mod owned_reservation_validation_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn direct_owned_child_reservation_checks_graph_and_entire_write_set() {
        let store = Store::memory().unwrap();
        let root = uuid::Uuid::from_u128(10);
        let child = uuid::Uuid::from_u128(11);
        let outer = uuid::Uuid::from_u128(12);
        let conversation = uuid::Uuid::from_u128(13);
        let interrupted = uuid::Uuid::from_u128(14);
        let root_key = [b"durable/task/".as_slice(), root.as_bytes()].concat();
        let child_key = [b"durable/task/".as_slice(), child.as_bytes()].concat();
        let edge_key = [
            b"durable/conversation-owner/".as_slice(),
            conversation.as_bytes(),
        ]
        .concat();
        let graph_key = format!("runtime/woken-owned-graph/v1/{root}").into_bytes();
        let marker_key =
            format!("runtime/woken-owned-child-reservation/v1/{root}/{child}").into_bytes();
        let mut root_task = json!({"id":root,"owner":{"Conversation":outer},"checkpoint":{"phase":"wait","data":null},"state":"Running","restarts":0,"restart_limit":4,"abort_requested":true,"woken_interrupted_attempt":interrupted});
        let mut child_task = json!({"id":child,"owner":{"Conversation":conversation},"checkpoint":{"phase":"parked","data":null},"state":"Ready","restarts":0,"restart_limit":4,"abort_requested":true,"kind":"unsafe-child","replay_safe":false});
        let edge = json!({"task":root,"task_owner":{"Conversation":outer},"child":conversation,"mode":"Foreground"});
        store
            .commit(
                &[],
                &[
                    Mutation::Put {
                        key: root_key.clone(),
                        value: serde_json::to_vec(&root_task).unwrap(),
                    },
                    Mutation::Put {
                        key: child_key.clone(),
                        value: serde_json::to_vec(&child_task).unwrap(),
                    },
                    Mutation::Put {
                        key: edge_key.clone(),
                        value: serde_json::to_vec(&edge).unwrap(),
                    },
                ],
            )
            .unwrap();
        let root_before = store.get(&root_key).unwrap().unwrap();
        let child_before = store.get(&child_key).unwrap().unwrap();
        let edge_version = store.get(&edge_key).unwrap().unwrap().version;
        let root_revision = store
            .get(&graph_revision::task_structure_revision(root))
            .unwrap()
            .map(|entry| entry.version);
        let global_revision = store
            .get(&graph_revision::global_structure_revision())
            .unwrap()
            .map(|entry| entry.version);
        let boundary = store.put_version().unwrap();
        let graph = json!({"version":1,"root":root,"interrupted_attempt":interrupted,"tasks":[root_task,child_task],"edges":[{"edge":edge,"edge_version":edge_version}],"structure":{"boundary":boundary,"root_revision":root_revision,"global_revision":global_revision}});
        root_task["state"] = json!("Ready");
        store
            .commit(
                &[Check {
                    key: root_key.clone(),
                    expected: Expected::Version(root_before.version),
                }],
                &[
                    Mutation::Put {
                        key: root_key,
                        value: serde_json::to_vec(&root_task).unwrap(),
                    },
                    Mutation::Put {
                        key: graph_key.clone(),
                        value: serde_json::to_vec(&graph).unwrap(),
                    },
                ],
            )
            .unwrap();
        let graph_version = store.get(&graph_key).unwrap().unwrap().version;
        child_task["state"] = json!("Running");
        child_task["attempt_id"] = json!(uuid::Uuid::from_u128(15));
        let marker = json!({"version":1,"root":root,"child":child,"graph_version":graph_version,"ready_version":child_before.version,"running":child_task});
        let mut writes = vec![
            Mutation::Put {
                key: child_key.clone(),
                value: serde_json::to_vec(&child_task).unwrap(),
            },
            Mutation::Put {
                key: marker_key,
                value: serde_json::to_vec(&marker).unwrap(),
            },
        ];
        let mut connection = store.connection.lock().unwrap();
        let tx = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        assert!(validate_owned_child_reservation(&tx, &writes).is_ok());
        let mut altered = child_task.clone();
        altered["abort_requested"] = json!(false);
        writes[0] = Mutation::Put {
            key: child_key.clone(),
            value: serde_json::to_vec(&altered).unwrap(),
        };
        assert!(validate_owned_child_reservation(&tx, &writes).is_err());
        let mut forged_marker = marker.clone();
        forged_marker["running"] = altered;
        let marker_key = match &writes[1] {
            Mutation::Put { key, .. } => key.clone(),
            _ => unreachable!(),
        };
        writes[1] = Mutation::Put {
            key: marker_key.clone(),
            value: serde_json::to_vec(&forged_marker).unwrap(),
        };
        assert!(validate_owned_child_reservation(&tx, &writes).is_err());
        writes[1] = Mutation::Put {
            key: marker_key,
            value: serde_json::to_vec(&marker).unwrap(),
        };
        writes[0] = Mutation::Put {
            key: child_key,
            value: serde_json::to_vec(&child_task).unwrap(),
        };
        writes.push(Mutation::Delete { key: edge_key });
        assert!(validate_owned_child_reservation(&tx, &writes).is_err());
    }

    #[test]
    fn reservation_rejects_wrong_task_and_mixed_graph_mutation() {
        let store = Store::memory().unwrap();
        assert!(matches!(
            store.commit(
                &[],
                &[Mutation::Put {
                    key: b"runtime/woken-owned-child-reservation/v1/forged".to_vec(),
                    value: b"forged".to_vec(),
                }]
            ),
            Err(Error::Conflict { .. })
        ));
        let id = uuid::Uuid::from_u128(1);
        let interrupted = uuid::Uuid::from_u128(2);
        let task_key = [b"durable/task/".as_slice(), id.as_bytes()].concat();
        let graph_key = format!("runtime/woken-owned-graph/v1/{id}").into_bytes();
        let marker_key = format!("runtime/woken-owned-reservation/v1/{id}").into_bytes();
        let mut task = json!({"id": id, "owner": {"Conversation": uuid::Uuid::from_u128(3)}, "checkpoint": {"phase":"wait", "data":null}, "state":"Running", "restarts":0, "restart_limit":4, "abort_requested":true, "woken_interrupted_attempt":interrupted});
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: task_key.clone(),
                    value: serde_json::to_vec(&task).unwrap(),
                }],
            )
            .unwrap();
        let task_entry = store.get(&task_key).unwrap().unwrap();
        let root_revision = store
            .get(&graph_revision::task_structure_revision(id))
            .unwrap()
            .map(|e| e.version);
        let global_revision = store
            .get(&graph_revision::global_structure_revision())
            .unwrap()
            .map(|e| e.version);
        let boundary = store.put_version().unwrap();
        let graph = json!({"version":1, "root":id, "interrupted_attempt":interrupted, "structure":{"boundary":boundary,"root_revision":root_revision,"global_revision":global_revision}});
        task["state"] = json!("Ready");
        store
            .commit(
                &[Check {
                    key: task_key.clone(),
                    expected: Expected::Version(task_entry.version),
                }],
                &[
                    Mutation::Put {
                        key: task_key.clone(),
                        value: serde_json::to_vec(&task).unwrap(),
                    },
                    Mutation::Put {
                        key: graph_key.clone(),
                        value: serde_json::to_vec(&graph).unwrap(),
                    },
                ],
            )
            .unwrap();
        let ready_version = store.get(&task_key).unwrap().unwrap().version;
        let graph_version = store.get(&graph_key).unwrap().unwrap().version;
        task["state"] = json!("Running");
        task["attempt_id"] = json!(uuid::Uuid::from_u128(4));
        let marker = json!({"version":1,"graph_version":graph_version,"ready_version":ready_version,"running":task});
        let mut writes = vec![
            Mutation::Put {
                key: task_key.clone(),
                value: serde_json::to_vec(&task).unwrap(),
            },
            Mutation::Put {
                key: marker_key,
                value: serde_json::to_vec(&marker).unwrap(),
            },
        ];
        let mut connection = store.connection.lock().unwrap();
        let tx = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        assert!(validate_owned_reservation(&tx, &writes).is_ok());
        let mut bad = task.clone();
        bad["abort_requested"] = json!(false);
        writes[0] = Mutation::Put {
            key: task_key.clone(),
            value: serde_json::to_vec(&bad).unwrap(),
        };
        assert!(validate_owned_reservation(&tx, &writes).is_err());
        let mut forged_marker = marker.clone();
        forged_marker["running"] = bad;
        let marker_key = match &writes[1] {
            Mutation::Put { key, .. } => key.clone(),
            _ => unreachable!(),
        };
        writes[1] = Mutation::Put {
            key: marker_key.clone(),
            value: serde_json::to_vec(&forged_marker).unwrap(),
        };
        assert!(validate_owned_reservation(&tx, &writes).is_err());
        writes[0] = Mutation::Put {
            key: task_key.clone(),
            value: serde_json::to_vec(&task).unwrap(),
        };
        writes[1] = Mutation::Put {
            key: marker_key,
            value: serde_json::to_vec(&marker).unwrap(),
        };
        writes.push(Mutation::Delete {
            key: b"durable/task/other".to_vec(),
        });
        assert!(validate_owned_reservation(&tx, &writes).is_err());
        writes.pop();
        drop(tx);
        drop(connection);
        let mut mixed = writes.clone();
        mixed.push(Mutation::Delete {
            key: b"durable/task/other".to_vec(),
        });
        assert!(matches!(
            store.commit(&[], &mixed),
            Err(Error::Conflict { .. })
        ));
        assert_eq!(
            store.get(&task_key).unwrap().unwrap().version,
            ready_version
        );
        let mut forged = writes.clone();
        forged[0] = Mutation::Put {
            key: task_key.clone(),
            value: serde_json::to_vec(&forged_marker["running"]).unwrap(),
        };
        let marker_key = match &forged[1] {
            Mutation::Put { key, .. } => key.clone(),
            _ => unreachable!(),
        };
        forged[1] = Mutation::Put {
            key: marker_key.clone(),
            value: serde_json::to_vec(&forged_marker).unwrap(),
        };
        assert!(matches!(
            store.commit(&[], &forged),
            Err(Error::Conflict { .. })
        ));
        assert!(store.get(&marker_key).unwrap().is_none());
        assert_eq!(
            store.get(&task_key).unwrap().unwrap().version,
            ready_version
        );
        let child = uuid::Uuid::from_u128(5);
        let child_key = [b"durable/task/".as_slice(), child.as_bytes()].concat();
        let child_value = json!({"id":child,"owner":{"Task":id},"checkpoint":{"phase":"initial","data":null},"state":"Ready","restarts":0,"restart_limit":4});
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: child_key.clone(),
                    value: serde_json::to_vec(&child_value).unwrap(),
                }],
            )
            .unwrap();
        let child_entry = store.get(&child_key).unwrap().unwrap();
        store
            .commit(
                &[Check {
                    key: child_key.clone(),
                    expected: Expected::Version(child_entry.version),
                }],
                &[Mutation::Delete { key: child_key }],
            )
            .unwrap();
        let mut connection = store.connection.lock().unwrap();
        let tx = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        assert!(validate_owned_reservation(&tx, &writes).is_err());
    }
}

/// A single connection, safe to share between threads. Different instances
/// opening the same database coordinate through SQLite's write lock.
pub struct Store {
    connection: Mutex<Connection>,
    #[cfg(feature = "experimental-graph-revision")]
    graph_protocol_active: std::sync::Arc<std::sync::atomic::AtomicBool>,
    commits: tokio::sync::watch::Sender<u64>,
}

/// Coalesced commit notifications from one Store instance.
pub struct CommitUpdates {
    receiver: tokio::sync::watch::Receiver<u64>,
}
impl CommitUpdates {
    /// Return the latest observed commit sequence and mark it observed.
    pub fn current(&mut self) -> u64 {
        *self.receiver.borrow_and_update()
    }

    /// Wait for a later local commit. Intermediate commits can coalesce.
    pub async fn changed(&mut self) -> Result<u64, tokio::sync::watch::error::RecvError> {
        self.receiver.changed().await?;
        Ok(self.current())
    }
}
impl Store {
    pub fn memory() -> Result<Self, Error> {
        Self::initialize(Connection::open_in_memory()?, false, true)
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        #[cfg(all(feature = "experimental-graph-revision", unix))]
        {
            let version = if path.exists() {
                let conn =
                    Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
                let protected: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='durable_graph_protocol')", [], |row| row.get(0))?;
                if protected {
                    conn.query_row(
                        "SELECT version FROM durable_graph_protocol WHERE id=1",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()?
                } else {
                    None
                }
            } else {
                None
            };
            if version.is_none() || matches!(version, Some(1 | 2)) {
                let (db, _lock) = owner::acquire_owner_lock(path)
                    .map_err(|error| Error::MigrationOwnerUnavailable(error.to_string()))?;
                return Self::initialize(Connection::open(db)?, true, true);
            }
        }
        Self::initialize(Connection::open(path)?, true, false)
    }
    pub(crate) fn open_owned(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::initialize(Connection::open(path)?, true, true)
    }
    fn initialize(
        conn: Connection,
        disk: bool,
        _migration_authorized: bool,
    ) -> Result<Self, Error> {
        #[cfg(feature = "experimental-graph-revision")]
        let graph_protocol_active = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        #[cfg(feature = "experimental-graph-revision")]
        {
            let active = graph_protocol_active.clone();
            conn.create_scalar_function(
                "pi_graph_protocol_v3",
                0,
                rusqlite::functions::FunctionFlags::SQLITE_UTF8,
                move |_| Ok(i64::from(active.load(std::sync::atomic::Ordering::Acquire))),
            )?;
        }
        conn.busy_timeout(Duration::from_secs(5))?;
        let protected: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='durable_graph_protocol')", [], |row| row.get(0))?;
        #[cfg(not(feature = "experimental-graph-revision"))]
        if protected {
            return Err(Error::IncompatibleGraphProtocol);
        }
        #[cfg(feature = "experimental-graph-revision")]
        if disk && !protected && !_migration_authorized {
            return Err(Error::IncompatibleGraphProtocol);
        }
        #[cfg(feature = "experimental-graph-revision")]
        let mut migrate_old = false;
        #[cfg(feature = "experimental-graph-revision")]
        if protected {
            let version: Option<i64> = conn
                .query_row(
                    "SELECT version FROM durable_graph_protocol WHERE id=1",
                    [],
                    |row| row.get(0),
                )
                .optional()?;
            if !matches!(version, Some(1 | 2 | 3))
                || (matches!(version, Some(1 | 2)) && disk && !_migration_authorized)
            {
                return Err(Error::IncompatibleGraphProtocol);
            }
            migrate_old = matches!(version, Some(1 | 2));
            let guard_name = match version {
                Some(1) => "pi_graph_protocol() != 1",
                Some(2) => "pi_graph_protocol_v2() != 1",
                Some(3) => "pi_graph_protocol_v3() != 1",
                _ => unreachable!(),
            };
            for (name, action) in [
                ("durable_graph_protocol_insert", "INSERT"),
                ("durable_graph_protocol_update", "UPDATE"),
                ("durable_graph_protocol_delete", "DELETE"),
            ] {
                let sql: Option<String> = conn.query_row("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?1 AND tbl_name='durable_store_entries'", [name], |row| row.get(0)).optional()?;
                let expected = format!("BEFORE {action} ON durable_store_entries");
                if !sql.as_deref().is_some_and(|sql| {
                    sql.contains(&expected)
                        && sql.contains(guard_name)
                        && sql.contains("RAISE(ABORT, 'graph protocol required')")
                }) {
                    return Err(Error::IncompatibleGraphProtocol);
                }
            }
        }
        if disk {
            let mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
            if !mode.eq_ignore_ascii_case("wal") {
                return Err(Error::Storage(rusqlite::Error::InvalidQuery));
            }
        }
        conn.execute_batch("PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS durable_store_meta (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL CHECK(version>=0));
            INSERT OR IGNORE INTO durable_store_meta(id, version) VALUES(1, 0);
            CREATE TABLE IF NOT EXISTS durable_store_entries (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL, version INTEGER NOT NULL CHECK(version>0)) WITHOUT ROWID;")?;
        conn.execute_batch("CREATE TABLE IF NOT EXISTS durable_task_document_owners (document_key BLOB PRIMARY KEY NOT NULL, owner BLOB NOT NULL) WITHOUT ROWID;")?;
        // The version boundary distinguishes pre-migration writes, whose commit
        // grouping is unknowable, from all subsequently recorded puts.
        conn.execute_batch("BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS durable_commit_meta (id INTEGER PRIMARY KEY CHECK(id=1), sequence INTEGER NOT NULL CHECK(sequence>=0), legacy_version INTEGER NOT NULL CHECK(legacy_version>=0));
            INSERT OR IGNORE INTO durable_commit_meta SELECT 1,0,version FROM durable_store_meta WHERE id=1;
            CREATE TABLE IF NOT EXISTS durable_commit_versions (key BLOB NOT NULL, version INTEGER NOT NULL CHECK(version>0), sequence INTEGER NOT NULL CHECK(sequence>0), PRIMARY KEY(key,version)) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS durable_commit_versions_by_version ON durable_commit_versions(version,key);
            COMMIT;")?;
        // Seed existing live documents once. Earlier overwritten/deleted values cannot
        // be recovered. Both the seed and its marker are committed atomically.
        conn.execute_batch("BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS durable_document_history_meta (id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL CHECK(revision>=0));
            CREATE TABLE IF NOT EXISTS durable_document_history (key BLOB NOT NULL, revision INTEGER NOT NULL, value BLOB, version INTEGER, PRIMARY KEY(key, revision)) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS durable_document_history_by_revision ON durable_document_history(revision, key);
            CREATE TABLE IF NOT EXISTS durable_document_cuts (key BLOB PRIMARY KEY NOT NULL, revision INTEGER NOT NULL) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS durable_document_cut_versions (key BLOB NOT NULL, version INTEGER NOT NULL, revision INTEGER NOT NULL, PRIMARY KEY(key,version)) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS durable_document_fence_versions (key BLOB NOT NULL, version INTEGER NOT NULL, revision INTEGER NOT NULL, PRIMARY KEY(key,version)) WITHOUT ROWID;
            INSERT INTO durable_document_history(key, revision, value, version)
                SELECT key, 0, value, version FROM durable_store_entries
                WHERE substr(key, 1, 17)=CAST('durable/document/' AS BLOB)
                AND NOT EXISTS (SELECT 1 FROM durable_document_history_meta WHERE id=1);
            INSERT OR IGNORE INTO durable_document_history_meta(id, revision) VALUES(1, 0);
            COMMIT;")?;
        // Establish a boundary after all legacy cuts. Legacy live logs are seeded
        // here, but earlier states cannot be reconstructed.
        conn.execute_batch("BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS durable_application_history_meta (id INTEGER PRIMARY KEY CHECK(id=1), first_revision INTEGER NOT NULL);
            UPDATE durable_document_history_meta SET revision=revision+1 WHERE id=1 AND NOT EXISTS (SELECT 1 FROM durable_application_history_meta WHERE id=1);
            INSERT INTO durable_document_history(key,revision,value,version)
                SELECT key,(SELECT revision FROM durable_document_history_meta WHERE id=1),value,version FROM durable_store_entries
                WHERE substr(key,1,28)=CAST('durable/application-entries/' AS BLOB)
                  AND NOT EXISTS (SELECT 1 FROM durable_application_history_meta WHERE id=1);
            INSERT OR IGNORE INTO durable_application_history_meta(id,first_revision) SELECT 1,revision FROM durable_document_history_meta WHERE id=1;
            COMMIT;")?;
        // Forward-only proof of which store transaction wrote each document
        // revision, including delete-only transactions. Do not backfill.
        conn.execute_batch("BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS durable_document_commit_meta (id INTEGER PRIMARY KEY CHECK(id=1), boundary_revision INTEGER NOT NULL, boundary_sequence INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS durable_document_commit_sequences (revision INTEGER PRIMARY KEY, sequence INTEGER NOT NULL UNIQUE);
            INSERT OR IGNORE INTO durable_document_commit_meta SELECT 1, (SELECT revision FROM durable_document_history_meta WHERE id=1), (SELECT sequence FROM durable_commit_meta WHERE id=1);
            COMMIT;")?;
        #[cfg(feature = "experimental-graph-revision")]
        if !protected || migrate_old {
            conn.execute_batch("BEGIN IMMEDIATE;")?;
            let marker_prefix = b"runtime/woken-owned-reservation/v1/";
            let child_marker_prefix = b"runtime/woken-owned-child-reservation/";
            let old_marker: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM durable_store_entries WHERE substr(key,1,?1)=?2 OR substr(key,1,?3)=?4)",
                rusqlite::params![marker_prefix.len() as i64, marker_prefix, child_marker_prefix.len() as i64, child_marker_prefix],
                |row| row.get(0),
            )?;
            if old_marker {
                conn.execute_batch("ROLLBACK;")?;
                return Err(Error::IncompatibleGraphProtocol);
            }
            conn.execute_batch("
            DROP TRIGGER IF EXISTS durable_graph_protocol_insert;
            DROP TRIGGER IF EXISTS durable_graph_protocol_update;
            DROP TRIGGER IF EXISTS durable_graph_protocol_delete;
            DROP TABLE IF EXISTS durable_graph_protocol;
            CREATE TABLE durable_graph_protocol (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL CHECK(version=3));
            INSERT INTO durable_graph_protocol(id,version) VALUES(1,3);
            CREATE TRIGGER durable_graph_protocol_insert BEFORE INSERT ON durable_store_entries BEGIN SELECT CASE WHEN pi_graph_protocol_v3() != 1 THEN RAISE(ABORT, 'graph protocol required') END; END;
            CREATE TRIGGER durable_graph_protocol_update BEFORE UPDATE ON durable_store_entries BEGIN SELECT CASE WHEN pi_graph_protocol_v3() != 1 THEN RAISE(ABORT, 'graph protocol required') END; END;
            CREATE TRIGGER durable_graph_protocol_delete BEFORE DELETE ON durable_store_entries BEGIN SELECT CASE WHEN pi_graph_protocol_v3() != 1 THEN RAISE(ABORT, 'graph protocol required') END; END;
            COMMIT;")?;
        }
        let sequence = conn.query_row(
            "SELECT sequence FROM durable_commit_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        let (commits, _) = tokio::sync::watch::channel(sequence);
        Ok(Self {
            connection: Mutex::new(conn),
            #[cfg(feature = "experimental-graph-revision")]
            graph_protocol_active,
            commits,
        })
    }
    /// Read explicit ownership metadata; ordinary byte keys are not interpreted.
    pub fn task_document_owner(&self, document_key: &[u8]) -> Result<Option<uuid::Uuid>, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let owner: Option<Vec<u8>> = conn
            .query_row(
                "SELECT owner FROM durable_task_document_owners WHERE document_key=?1",
                [document_key],
                |r| r.get(0),
            )
            .optional()?;
        owner
            .map(|owner| uuid::Uuid::from_slice(&owner).map_err(|_| Error::InvalidSnapshot))
            .transpose()
    }
    /// Explicitly migrate caller-identified ownership records written by the
    /// experimental byte-key implementation. The caller must first stop all
    /// older writers. Ordinary lookalike keys are never migrated implicitly.
    /// All listed records migrate atomically, and their byte keys remain intact.
    pub fn migrate_legacy_task_documents(
        &self,
        acquisitions: &[TaskDocumentAcquisition],
    ) -> Result<(), Error> {
        if acquisitions.is_empty() {
            return Ok(());
        }
        let mut conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut seen = std::collections::HashSet::new();
        for item in acquisitions {
            let doc = &item.document_key;
            if !doc.starts_with(b"durable/document/") || !seen.insert(doc.clone()) {
                return Err(Error::InvalidSnapshot);
            }
            let mut legacy = b"durable/task-document-owner/".to_vec();
            legacy.extend_from_slice(doc);
            let old: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT value FROM durable_store_entries WHERE key=?1",
                    [&legacy],
                    |r| r.get(0),
                )
                .optional()?;
            if old.as_deref() != Some(item.owner.as_bytes().as_slice()) {
                return Err(Error::Conflict { key: legacy });
            }
            let owned: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM durable_task_document_owners WHERE document_key=?1)",
                [doc],
                |r| r.get(0),
            )?;
            if owned {
                return Err(Error::Conflict { key: doc.clone() });
            }
            let mut task_key = b"durable/task/".to_vec();
            task_key.extend_from_slice(item.owner.as_bytes());
            let task: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT value FROM durable_store_entries WHERE key=?1",
                    [&task_key],
                    |r| r.get(0),
                )
                .optional()?;
            let state = task
                .as_deref()
                .and_then(|v| task_record::terminal(v, item.owner.as_bytes()))
                .ok_or_else(|| Error::Conflict {
                    key: task_key.clone(),
                })?;
            let exists: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM durable_store_entries WHERE key=?1)",
                [doc],
                |r| r.get(0),
            )?;
            if state && exists {
                return Err(Error::Conflict { key: doc.clone() });
            }
        }
        for item in acquisitions {
            tx.execute(
                "INSERT INTO durable_task_document_owners(document_key,owner) VALUES(?1,?2)",
                params![&item.document_key, item.owner.as_bytes().as_slice()],
            )?;
        }
        let previous: i64 = tx.query_row(
            "SELECT sequence FROM durable_commit_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        let sequence = previous.checked_add(1).ok_or(Error::VersionExhausted)?;
        tx.execute(
            "UPDATE durable_commit_meta SET sequence=?1 WHERE id=1",
            [sequence],
        )?;
        tx.commit()?;
        self.commits.send_replace(sequence as u64);
        Ok(())
    }

    /// Subscribe at a store sequence without racing a commit on this instance.
    /// Writes through another connection do not generate notifications here.
    pub fn subscribe_commits(&self) -> Result<CommitUpdates, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let sequence = conn.query_row(
            "SELECT sequence FROM durable_commit_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        let previous = *self.commits.borrow();
        if previous != sequence {
            self.commits.send_replace(sequence);
        }
        Ok(CommitUpdates {
            receiver: self.commits.subscribe(),
        })
    }
    /// Current store-wide sequence. Zero is the migration boundary; only
    /// successful commits containing mutations advance it, once per batch.
    pub fn commit_sequence(&self) -> Result<u64, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn.query_row(
            "SELECT sequence FROM durable_commit_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?)
    }

    /// Current database-wide put version, including versions before commit tracking.
    pub fn put_version(&self) -> Result<u64, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn.query_row(
            "SELECT version FROM durable_store_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?)
    }

    /// Highest per-put version allocated before commit tracking was installed.
    /// Its historical commit grouping cannot be reconstructed.
    pub fn legacy_version_boundary(&self) -> Result<u64, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn.query_row(
            "SELECT legacy_version FROM durable_commit_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?)
    }

    /// Migration boundary after which document revision/commit links are recorded.
    pub fn document_commit_boundary(&self) -> Result<(u64, u64), Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn.query_row("SELECT boundary_revision,boundary_sequence FROM durable_document_commit_meta WHERE id=1", [], |r| Ok((r.get(0)?, r.get(1)?)))?)
    }
    /// Commit recorded for a document revision, including deletion-only revisions.
    pub fn document_commit_sequence(&self, revision: u64) -> Result<Option<u64>, Error> {
        if revision > i64::MAX as u64 {
            return Ok(None);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn
            .query_row(
                "SELECT sequence FROM durable_document_commit_sequences WHERE revision=?1",
                [revision],
                |r| r.get(0),
            )
            .optional()?)
    }
    /// Document revision written by a tracked store transaction, if any.
    pub fn document_revision_for_commit(&self, sequence: u64) -> Result<Option<u64>, Error> {
        if sequence > i64::MAX as u64 {
            return Ok(None);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn
            .query_row(
                "SELECT revision FROM durable_document_commit_sequences WHERE sequence=?1",
                [sequence],
                |r| r.get(0),
            )
            .optional()?)
    }
    /// Revision of the latest retained history row at a given boundary.
    pub fn document_last_revision(&self, key: &[u8], through: u64) -> Result<Option<u64>, Error> {
        if !key.starts_with(b"durable/document/") || through > i64::MAX as u64 {
            return Err(Error::InvalidSnapshot);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn.query_row(
            "SELECT MAX(revision) FROM durable_document_history WHERE key=?1 AND revision<=?2",
            params![key, through],
            |r| r.get(0),
        )?)
    }
    /// Next complete scoped document revision after a forward-only cursor.
    /// Missing tracking for a matching revision is an error, not a gap.
    pub fn next_scoped_document_commit(
        &self,
        prefix: &[u8],
        after: u64,
    ) -> Result<Option<(u64, u64, Vec<DocumentChange>)>, Error> {
        if !prefix.starts_with(b"durable/document/") || after > i64::MAX as u64 {
            return Err(Error::InvalidSnapshot);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let current: u64 = conn.query_row(
            "SELECT revision FROM durable_document_history_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if after > current {
            return Err(Error::InvalidSnapshot);
        }
        let revision: Option<u64> = conn.query_row("SELECT MIN(revision) FROM durable_document_history WHERE substr(key,1,?1)=?2 AND revision>?3", params![prefix.len() as i64, prefix, after], |r| r.get(0))?;
        let Some(revision) = revision else {
            return Ok(None);
        };
        let sequence: u64 = conn
            .query_row(
                "SELECT sequence FROM durable_document_commit_sequences WHERE revision=?1",
                [revision],
                |r| r.get(0),
            )
            .optional()?
            .ok_or(Error::InvalidSnapshot)?;
        let mut stmt = conn.prepare("SELECT key,value,version,length(key),coalesce(length(value),0) FROM durable_document_history WHERE revision=?1 AND substr(key,1,?2)=?3 ORDER BY key LIMIT 257")?;
        let mut rows = stmt.query(params![revision, prefix.len() as i64, prefix])?;
        let mut changes = Vec::new();
        let mut bytes = 0usize;
        while let Some(row) = rows.next()? {
            if changes.len() == 256 {
                return Err(Error::DocumentFrameTooLarge);
            }
            let key_bytes: usize = row.get(3)?;
            let value_bytes: usize = row.get(4)?;
            bytes = bytes
                .checked_add(key_bytes)
                .and_then(|n| n.checked_add(value_bytes))
                .ok_or(Error::DocumentFrameTooLarge)?;
            if bytes > 8 * 1024 * 1024 {
                return Err(Error::DocumentFrameTooLarge);
            }
            let entry = match (
                row.get::<_, Option<Vec<u8>>>(1)?,
                row.get::<_, Option<u64>>(2)?,
            ) {
                (Some(value), Some(version)) => Some(Entry { value, version }),
                (None, None) => None,
                _ => return Err(Error::InvalidSnapshot),
            };
            let key: Vec<u8> = row.get(0)?;
            if let Some(entry) = &entry {
                let committed: Option<u64> = conn
                    .query_row(
                        "SELECT sequence FROM durable_commit_versions WHERE key=?1 AND version=?2",
                        params![&key, entry.version],
                        |r| r.get(0),
                    )
                    .optional()?;
                if committed != Some(sequence) {
                    return Err(Error::InvalidSnapshot);
                }
            }
            changes.push((key, entry));
        }
        Ok(Some((revision, sequence, changes)))
    }

    /// Commit containing this put, including puts later overwritten or deleted.
    /// None means the key/version is unknown or predates commit tracking.
    pub fn commit_for_version(&self, key: &[u8], version: u64) -> Result<Option<u64>, Error> {
        // SQLite versions are signed 64-bit integers; larger values cannot
        // have been allocated and must not fail parameter conversion.
        if version > i64::MAX as u64 {
            return Ok(None);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn
            .query_row(
                "SELECT sequence FROM durable_commit_versions WHERE key=?1 AND version=?2",
                params![key, version],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Read relevant immutable observer records and application-log snapshots
    /// in complete store commit frames. The namespace contract forbids raw
    /// mutation or deletion of these records.
    pub fn conversation_commits(
        &self,
        observer_prefix: &[u8],
        application_key: &[u8],
        after: u64,
        through: u64,
        limit: usize,
    ) -> Result<Vec<(u64, Vec<KeyedEntry>)>, Error> {
        if limit == 0 || limit > 256 || after > through || through > i64::MAX as u64 {
            return Err(Error::InvalidSnapshot);
        }
        let mut conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let current: u64 = tx.query_row(
            "SELECT sequence FROM durable_commit_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if through > current {
            return Err(Error::InvalidSnapshot);
        }
        let sql = "WITH records AS (
            SELECT c.sequence,e.key,e.value,e.version FROM durable_store_entries e
            JOIN durable_commit_versions c ON c.key=e.key AND c.version=e.version
            WHERE substr(e.key,1,?1)=?2
            UNION ALL
            SELECT c.sequence,h.key,h.value,h.version FROM durable_document_history h
            JOIN durable_commit_versions c ON c.key=h.key AND c.version=h.version
            WHERE h.key=?3 AND h.value IS NOT NULL
        ), chosen AS (SELECT DISTINCT sequence FROM records WHERE sequence>?4 AND sequence<=?5 ORDER BY sequence LIMIT ?6)
        SELECT sequence,key,value,version FROM records WHERE sequence IN chosen ORDER BY sequence,key";
        let mut stmt = tx.prepare(sql)?;
        let mut rows = stmt.query(params![
            observer_prefix.len() as i64,
            observer_prefix,
            application_key,
            after,
            through,
            limit as i64
        ])?;
        let mut result: Vec<(u64, Vec<KeyedEntry>)> = Vec::new();
        while let Some(row) = rows.next()? {
            let sequence = row.get(0)?;
            if result.last().is_none_or(|frame| frame.0 != sequence) {
                result.push((sequence, Vec::new()));
            }
            result.last_mut().unwrap().1.push((
                row.get(1)?,
                Entry {
                    value: row.get(2)?,
                    version: row.get(3)?,
                },
            ));
        }
        Ok(result)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Entry>, Error> {
        if let Some(limit) = read_limit(key) {
            return self.get_bounded(key, limit);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn
            .query_row(
                "SELECT value, version FROM durable_store_entries WHERE key=?1",
                [key],
                |r| {
                    Ok(Entry {
                        value: r.get(0)?,
                        version: r.get(1)?,
                    })
                },
            )
            .optional()?)
    }
    /// Read an entry without materializing a value above `max_bytes`.
    pub fn get_bounded(&self, key: &[u8], max_bytes: usize) -> Result<Option<Entry>, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let cap = i64::try_from(max_bytes).map_err(|_| Error::InvalidSnapshot)?;
        let row: Option<(Option<Vec<u8>>, u64, u64)> = conn.query_row(
            "SELECT CASE WHEN length(value)<=?2 THEN value ELSE NULL END,version,length(value) FROM durable_store_entries WHERE key=?1",
            params![key, cap], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).optional()?;
        match row {
            None => Ok(None),
            Some((Some(value), version, _)) => Ok(Some(Entry { value, version })),
            Some(_) => Err(Error::DocumentFrameTooLarge),
        }
    }
    /// Returns a consistent snapshot in lexicographic byte order.
    pub fn list(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Entry)>, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let mut stmt = conn.prepare("SELECT key, value, version FROM durable_store_entries WHERE substr(key, 1, ?1)=?2 ORDER BY key")?;
        let rows = stmt.query_map(params![prefix.len() as i64, prefix], |r| {
            Ok((
                r.get(0)?,
                Entry {
                    value: r.get(1)?,
                    version: r.get(2)?,
                },
            ))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Read at most `limit` keys without allocating their values. Oversized
    /// keys fail closed instead of being copied out of SQLite.
    pub fn prefix_keys_bounded(
        &self,
        prefix: &[u8],
        limit: usize,
        max_key_bytes: usize,
    ) -> Result<Vec<Vec<u8>>, Error> {
        if limit == 0 || limit > 256 || max_key_bytes < prefix.len() {
            return Err(Error::InvalidSnapshot);
        }
        let limit = i64::try_from(limit).map_err(|_| Error::InvalidSnapshot)?;
        let cap = i64::try_from(max_key_bytes).map_err(|_| Error::InvalidSnapshot)?;
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let mut stmt = conn.prepare("SELECT CASE WHEN length(key)<=?3 THEN key ELSE NULL END FROM durable_store_entries WHERE substr(key,1,?1)=?2 ORDER BY key LIMIT ?4")?;
        let rows = stmt.query_map(params![prefix.len() as i64, prefix, cap, limit], |r| {
            r.get::<_, Option<Vec<u8>>>(0)
        })?;
        rows.map(|row| row?.ok_or(Error::InvalidSnapshot)).collect()
    }
    /// Read a bounded lexicographic page without allocating oversized keys or values.
    /// `after` is exclusive. A full page may require a final empty read.
    pub fn prefix_page_bounded(
        &self,
        prefix: &[u8],
        after: &[u8],
        limit: usize,
        max_key_bytes: usize,
        max_value_bytes: usize,
    ) -> Result<Vec<(Vec<u8>, Entry)>, Error> {
        if limit == 0 || limit > 256 || max_key_bytes < prefix.len() || max_value_bytes == 0 {
            return Err(Error::InvalidSnapshot);
        }
        let limit = i64::try_from(limit).map_err(|_| Error::InvalidSnapshot)?;
        let key_cap = i64::try_from(max_key_bytes).map_err(|_| Error::InvalidSnapshot)?;
        let value_cap = i64::try_from(max_value_bytes).map_err(|_| Error::InvalidSnapshot)?;
        let end = prefix_end(prefix);
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let query = if end.is_some() {
            "SELECT CASE WHEN length(key)<=?5 THEN key END, CASE WHEN length(value)<=?6 THEN value END, version FROM durable_store_entries WHERE key>=?1 AND key>?2 AND key<?3 ORDER BY key LIMIT ?4"
        } else {
            "SELECT CASE WHEN length(key)<=?5 THEN key END, CASE WHEN length(value)<=?6 THEN value END, version FROM durable_store_entries WHERE key>=?1 AND key>?2 AND ?3 IS NULL ORDER BY key LIMIT ?4"
        };
        let mut stmt = conn.prepare(query)?;
        let rows = stmt.query_map(
            params![prefix, after, end, limit, key_cap, value_cap],
            |r| {
                Ok((
                    r.get::<_, Option<Vec<u8>>>(0)?,
                    r.get::<_, Option<Vec<u8>>>(1)?,
                    r.get::<_, u64>(2)?,
                ))
            },
        )?;
        rows.map(|row| {
            let (key, value, version) = row?;
            Ok((
                key.ok_or(Error::InvalidSnapshot)?,
                Entry {
                    value: value.ok_or(Error::InvalidSnapshot)?,
                    version,
                },
            ))
        })
        .collect()
    }

    /// Read multiple key ranges from one SQLite snapshot.
    pub fn list_prefixes(&self, prefixes: &[&[u8]]) -> Result<Vec<(Vec<u8>, Entry)>, Error> {
        let mut conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let mut result = Vec::new();
        for prefix in prefixes {
            let mut stmt = tx.prepare("SELECT key, value, version FROM durable_store_entries WHERE substr(key, 1, ?1)=?2 ORDER BY key")?;
            let rows = stmt.query_map(params![prefix.len() as i64, prefix], |r| {
                Ok((
                    r.get(0)?,
                    Entry {
                        value: r.get(1)?,
                        version: r.get(2)?,
                    },
                ))
            })?;
            result.extend(rows.collect::<Result<Vec<_>, _>>()?);
        }
        tx.commit()?;
        Ok(result)
    }
    /// Read a metadata key and a bounded, ordered page from one SQLite snapshot.
    /// `after` is exclusive and must be a key in or before the prefix range.
    /// Pages across calls may observe newer commits.
    pub fn page_with_metadata(
        &self,
        metadata_key: &[u8],
        prefix: &[u8],
        after: &[u8],
        limit: usize,
    ) -> Result<MetadataPage, Error> {
        let value_limit = read_limit(prefix);
        let metadata_limit = read_limit(metadata_key);
        let mut conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let metadata: Option<(Option<Vec<u8>>, u64)> = tx
            .query_row(
                "SELECT CASE WHEN ?2 IS NULL OR length(value)<=?2 THEN value ELSE NULL END, version FROM durable_store_entries WHERE key=?1",
                params![metadata_key, metadata_limit.map(|v| i64::try_from(v).unwrap_or(i64::MAX))],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let metadata = metadata
            .map(|(value, version)| {
                Ok::<_, Error>(Entry {
                    value: value.ok_or(Error::DocumentFrameTooLarge)?,
                    version,
                })
            })
            .transpose()?;
        let end = prefix_end(prefix);
        let rows = {
            let query = if end.is_some() {
                "SELECT CASE WHEN ?5 IS NULL OR length(key)<=?5 THEN key ELSE NULL END, CASE WHEN ?5 IS NULL OR length(value)<=?5 THEN value ELSE NULL END, version FROM durable_store_entries WHERE key>=?1 AND key>?2 AND key<?3 ORDER BY key LIMIT ?4"
            } else {
                "SELECT CASE WHEN ?5 IS NULL OR length(key)<=?5 THEN key ELSE NULL END, CASE WHEN ?5 IS NULL OR length(value)<=?5 THEN value ELSE NULL END, version FROM durable_store_entries WHERE key>=?1 AND key>?2 AND ?3 IS NULL ORDER BY key LIMIT ?4"
            };
            let mut stmt = tx.prepare(query)?;
            let limit = i64::try_from(limit).unwrap_or(i64::MAX);
            let cap = value_limit.map(|v| i64::try_from(v).unwrap_or(i64::MAX));
            let rows = stmt.query_map(params![prefix, after, end, limit, cap], |r| {
                Ok((
                    r.get::<_, Option<Vec<u8>>>(0)?,
                    r.get::<_, Option<Vec<u8>>>(1)?,
                    r.get(2)?,
                ))
            })?;
            rows.map(|row| {
                let (key, value, version) = row?;
                Ok((
                    key.ok_or(Error::DocumentFrameTooLarge)?,
                    Entry {
                        value: value.ok_or(Error::DocumentFrameTooLarge)?,
                        version,
                    },
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?
        };
        tx.commit()?;
        Ok((metadata, rows))
    }
    /// Capture a document history boundary. Every document mutation in one
    /// commit shares a boundary; subsequent reads at it are immutable.
    pub fn document_snapshot(&self) -> Result<u64, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn.query_row(
            "SELECT revision FROM durable_document_history_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?)
    }

    /// Read a document as of a captured history boundary. This does not accept
    /// arbitrary store keys and does not promise history from before migration.
    pub fn document_at(&self, key: &[u8], revision: u64) -> Result<Option<Entry>, Error> {
        if !key.starts_with(b"durable/document/") && !key.starts_with(b"durable/document-policy/") {
            return Err(Error::InvalidSnapshot);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let current: u64 = conn.query_row(
            "SELECT revision FROM durable_document_history_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if revision > current {
            return Err(Error::InvalidSnapshot);
        }
        let row: Option<(Option<Vec<u8>>, Option<u64>)> = conn.query_row(
            "SELECT value, version FROM durable_document_history WHERE key=?1 AND revision<=?2 ORDER BY revision DESC LIMIT 1",
            params![key, revision], |r| Ok((r.get(0)?, r.get(1)?)),
        ).optional()?;
        match row {
            Some((Some(value), Some(version))) => Ok(Some(Entry { value, version })),
            Some((None, None)) | None => Ok(None),
            _ => Err(Error::Storage(rusqlite::Error::InvalidQuery)),
        }
    }

    /// Read an immutable document snapshot without allocating an oversized blob.
    pub fn document_at_bounded(
        &self,
        key: &[u8],
        revision: u64,
        max_bytes: usize,
    ) -> Result<Option<Entry>, Error> {
        if !key.starts_with(b"durable/document/") {
            return Err(Error::InvalidSnapshot);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let current: u64 = conn.query_row(
            "SELECT revision FROM durable_document_history_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if revision > current {
            return Err(Error::InvalidSnapshot);
        }
        let cap = i64::try_from(max_bytes).map_err(|_| Error::InvalidSnapshot)?;
        let row: Option<BoundedHistoryRow> = conn.query_row(
            "SELECT CASE WHEN length(value)<=?3 THEN value ELSE NULL END,version,length(value) FROM durable_document_history WHERE key=?1 AND revision<=?2 ORDER BY revision DESC LIMIT 1",
            params![key,revision,cap], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
        ).optional()?;
        match row {
            None | Some((None, None, None)) => Ok(None),
            Some((Some(value), Some(version), Some(_))) => Ok(Some(Entry { value, version })),
            Some((None, Some(_), Some(_))) => Err(Error::DocumentFrameTooLarge),
            _ => Err(Error::InvalidSnapshot),
        }
    }

    /// Fetch at most one document change, checking its size inside SQLite.
    pub fn next_document_change_bounded(
        &self,
        key: &[u8],
        after: u64,
        max_bytes: usize,
    ) -> Result<Option<(u64, Option<Entry>)>, Error> {
        if !key.starts_with(b"durable/document/") {
            return Err(Error::InvalidSnapshot);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let current: u64 = conn.query_row(
            "SELECT revision FROM durable_document_history_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if after > current {
            return Err(Error::InvalidSnapshot);
        }
        let cap = i64::try_from(max_bytes).map_err(|_| Error::InvalidSnapshot)?;
        let row: Option<BoundedChangeRow> = conn.query_row(
            "SELECT revision,CASE WHEN length(value)<=?3 THEN value ELSE NULL END,version,length(value) FROM durable_document_history WHERE key=?1 AND revision>?2 ORDER BY revision LIMIT 1",
            params![key,after,cap], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)),
        ).optional()?;
        match row {
            None => Ok(None),
            Some((rev, None, None, None)) => Ok(Some((rev, None))),
            Some((rev, Some(value), Some(version), Some(_))) => {
                Ok(Some((rev, Some(Entry { value, version }))))
            }
            Some((_, None, Some(_), Some(_))) => Err(Error::DocumentFrameTooLarge),
            _ => Err(Error::InvalidSnapshot),
        }
    }

    /// Ordered committed states for one document after a database-local revision.
    /// A delete is represented by None. The page contains at most 256 states.
    pub fn document_changes(
        &self,
        key: &[u8],
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, Option<Entry>)>, Error> {
        if !key.starts_with(b"durable/document/") || limit == 0 || limit > 256 {
            return Err(Error::InvalidSnapshot);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let current: u64 = conn.query_row(
            "SELECT revision FROM durable_document_history_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if after > current {
            return Err(Error::InvalidSnapshot);
        }
        let mut stmt = conn.prepare("SELECT revision,value,version FROM durable_document_history WHERE key=?1 AND revision>?2 ORDER BY revision LIMIT ?3")?;
        let rows = stmt.query_map(params![key, after, limit as i64], |r| {
            Ok((
                r.get::<_, u64>(0)?,
                r.get::<_, Option<Vec<u8>>>(1)?,
                r.get::<_, Option<u64>>(2)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (revision, value, version) = row?;
            let entry = match (value, version) {
                (Some(value), Some(version)) => Some(Entry { value, version }),
                (None, None) => None,
                _ => return Err(Error::Storage(rusqlite::Error::InvalidQuery)),
            };
            result.push((revision, entry));
        }
        Ok(result)
    }

    /// Complete document commits after a revision, bounded by commit count.
    /// Each frame contains every changed document, ordered by storage key.
    pub fn document_commits(&self, after: u64, limit: usize) -> Result<Vec<DocumentCommit>, Error> {
        if limit == 0 || limit > 256 {
            return Err(Error::InvalidSnapshot);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let current: u64 = conn.query_row(
            "SELECT revision FROM durable_document_history_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if after > current {
            return Err(Error::InvalidSnapshot);
        }
        let mut stmt = conn.prepare("SELECT revision,key,value,version FROM durable_document_history WHERE substr(key,1,17)=CAST('durable/document/' AS BLOB) AND revision IN (SELECT DISTINCT revision FROM durable_document_history WHERE substr(key,1,17)=CAST('durable/document/' AS BLOB) AND revision>?1 ORDER BY revision LIMIT ?2) ORDER BY revision,key")?;
        let mut rows = stmt.query(params![after, limit as i64])?;
        let mut commits: Vec<DocumentCommit> = Vec::new();
        while let Some(row) = rows.next()? {
            let revision: u64 = row.get(0)?;
            let key = row.get(1)?;
            let entry = match (
                row.get::<_, Option<Vec<u8>>>(2)?,
                row.get::<_, Option<u64>>(3)?,
            ) {
                (Some(value), Some(version)) => Some(Entry { value, version }),
                (None, None) => None,
                _ => return Err(Error::Storage(rusqlite::Error::InvalidQuery)),
            };
            if commits.last().is_none_or(|frame| frame.0 != revision) {
                commits.push((revision, Vec::new()));
            }
            commits.last_mut().unwrap().1.push((key, entry));
        }
        Ok(commits)
    }

    /// Document revision recorded atomically with a conversation fork record.
    pub fn document_cut(&self, key: &[u8]) -> Result<Option<u64>, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn
            .query_row(
                "SELECT revision FROM durable_document_cuts WHERE key=?1",
                [key],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Document cut captured for this exact snapshot version. Evidence is
    /// forward-only; older snapshots are not assigned inferred versions.
    pub fn document_cut_for_version(&self, key: &[u8], version: u64) -> Result<Option<u64>, Error> {
        if version > i64::MAX as u64 {
            return Ok(None);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn
            .query_row(
                "SELECT revision FROM durable_document_cut_versions WHERE key=?1 AND version=?2",
                params![key, version],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// The document revision explicitly checked by the snapshot's commit.
    /// Absence means no tracked fence; legacy records are not backfilled.
    pub fn document_fence_for_version(
        &self,
        key: &[u8],
        version: u64,
    ) -> Result<Option<u64>, Error> {
        if version > i64::MAX as u64 {
            return Ok(None);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn
            .query_row(
                "SELECT revision FROM durable_document_fence_versions WHERE key=?1 AND version=?2",
                params![key, version],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Bounded, immutable application snapshots in commit order. Missing commit
    /// metadata (including migration seeds) is returned as `None` and must not
    /// be treated as a reconstructible entry boundary.
    pub fn application_history(
        &self,
        key: &[u8],
        limit: usize,
    ) -> Result<Vec<(u64, Option<u64>, Entry)>, Error> {
        if !key.starts_with(b"durable/application-entries/") || limit == 0 || limit > 257 {
            return Err(Error::InvalidSnapshot);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let mut stmt = conn.prepare("SELECT h.revision,c.sequence,h.value,h.version FROM durable_document_history h LEFT JOIN durable_commit_versions c ON c.key=h.key AND c.version=h.version WHERE h.key=?1 ORDER BY h.revision LIMIT ?2")?;
        let mut rows = stmt.query(params![key, limit as i64])?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            let value: Option<Vec<u8>> = row.get(2)?;
            let version: Option<u64> = row.get(3)?;
            let (Some(value), Some(version)) = (value, version) else {
                return Err(Error::InvalidSnapshot);
            };
            result.push((row.get(0)?, row.get(1)?, Entry { value, version }));
        }
        Ok(result)
    }

    /// First tracked put of a key, used to prove a copied log was installed
    /// atomically with conversation creation.
    pub fn first_commit_for_key(&self, key: &[u8]) -> Result<Option<u64>, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn.query_row(
            "SELECT MIN(sequence) FROM durable_commit_versions WHERE key=?1",
            [key],
            |r| r.get(0),
        )?)
    }

    /// First tracked put of a key strictly after a store commit sequence.
    pub fn next_commit_for_key(&self, key: &[u8], after: u64) -> Result<Option<u64>, Error> {
        if after > i64::MAX as u64 {
            return Err(Error::InvalidSnapshot);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn.query_row(
            "SELECT MIN(sequence) FROM durable_commit_versions WHERE key=?1 AND sequence>?2",
            params![key, after],
            |r| r.get(0),
        )?)
    }

    /// Find the immutable fork snapshot from the last tracked runtime write at
    /// or before `sequence`. A write lacking a matching snapshot fails closed.
    pub fn runtime_snapshot_at(
        &self,
        runtime_key: &[u8],
        snapshot_prefix: &[u8],
        sequence: u64,
    ) -> Result<Option<(u64, Vec<u8>)>, Error> {
        if sequence > i64::MAX as u64 {
            return Err(Error::InvalidSnapshot);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let runtime_sequence: Option<u64> = conn.query_row(
            "SELECT MAX(sequence) FROM durable_commit_versions WHERE key=?1 AND sequence<=?2",
            params![runtime_key, sequence],
            |r| r.get(0),
        )?;
        let Some(runtime_sequence) = runtime_sequence else {
            return Ok(None);
        };
        let mut stmt = conn.prepare("SELECT e.key FROM durable_store_entries e JOIN durable_commit_versions c ON c.key=e.key AND c.version=e.version WHERE substr(e.key,1,?1)=?2 AND c.sequence=?3 LIMIT 2")?;
        let keys = stmt
            .query_map(
                params![
                    snapshot_prefix.len() as i64,
                    snapshot_prefix,
                    runtime_sequence
                ],
                |r| r.get::<_, Vec<u8>>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        if keys.len() != 1 {
            return Ok(None);
        }
        Ok(keys.into_iter().next().map(|key| (runtime_sequence, key)))
    }

    /// Find a unique canonical, twenty-digit snapshot key written with the
    /// specified runtime root commit. Other same-commit snapshot keys are
    /// ignored. The caller must authenticate the matching observer and payload.
    pub fn runtime_canonical_snapshot_at(
        &self,
        runtime_key: &[u8],
        snapshot_prefix: &[u8],
        sequence: u64,
    ) -> Result<Option<Vec<u8>>, Error> {
        if sequence == 0 || sequence > i64::MAX as u64 || snapshot_prefix.is_empty() {
            return Ok(None);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let mut stmt = conn.prepare("SELECT e.key FROM durable_store_entries e JOIN durable_commit_versions c ON c.key=e.key AND c.version=e.version JOIN durable_commit_versions r ON r.key=?1 AND r.sequence=c.sequence WHERE c.sequence=?2 AND substr(e.key,1,?3)=?4 AND length(e.key)=?5")?;
        let rows = stmt.query_map(
            params![
                runtime_key,
                sequence,
                snapshot_prefix.len() as i64,
                snapshot_prefix,
                snapshot_prefix.len() as i64 + 20
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )?;
        let mut found = None;
        for row in rows {
            let key = row?;
            let suffix = &key[snapshot_prefix.len()..];
            let Ok(text) = std::str::from_utf8(suffix) else {
                continue;
            };
            let Ok(observation) = text.parse::<u64>() else {
                continue;
            };
            if observation == 0 || format!("{observation:020}").as_bytes() != suffix {
                continue;
            }
            if found.replace(key).is_some() {
                return Ok(None);
            }
        }
        Ok(found)
    }

    /// Verify that an explicitly selected, currently retained snapshot and a
    /// runtime root were put in the same tracked commit. This does not prove
    /// that the snapshot is authorized by a manifest or that its contents are
    /// correct; the caller must verify those relationships separately. Later
    /// root updates are allowed, but rewriting the selected snapshot invalidates
    /// the proof. The prefix must be the caller's trusted snapshot namespace.
    pub fn runtime_snapshot_exact_at(
        &self,
        runtime_key: &[u8],
        snapshot_prefix: &[u8],
        snapshot_key: &[u8],
        sequence: u64,
    ) -> Result<bool, Error> {
        if sequence > i64::MAX as u64 {
            return Err(Error::InvalidSnapshot);
        }
        if sequence == 0
            || runtime_key.is_empty()
            || snapshot_prefix.is_empty()
            || snapshot_key == runtime_key
            || !snapshot_key.starts_with(snapshot_prefix)
        {
            return Ok(false);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM durable_commit_versions r JOIN durable_store_entries s ON s.key=?2 JOIN durable_commit_versions c ON c.key=s.key AND c.version=s.version WHERE r.key=?1 AND r.sequence=?3 AND c.sequence=r.sequence)",
            params![runtime_key, snapshot_key, sequence],
            |row| row.get(0),
        )?)
    }

    /// Read an application log at a proven history boundary. Older cuts are
    /// rejected because pre-migration log changes were not recorded.
    pub fn application_entries_at(
        &self,
        key: &[u8],
        revision: u64,
    ) -> Result<Option<Entry>, Error> {
        if !key.starts_with(b"durable/application-entries/") {
            return Err(Error::InvalidSnapshot);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let first: u64 = conn.query_row(
            "SELECT first_revision FROM durable_application_history_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        let current: u64 = conn.query_row(
            "SELECT revision FROM durable_document_history_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if revision < first || revision > current {
            return Err(Error::InvalidSnapshot);
        }
        let cap = read_limit(key).map(|v| i64::try_from(v).unwrap_or(i64::MAX));
        let row: Option<(Option<Vec<u8>>, Option<u64>)> = conn.query_row(
            "SELECT CASE WHEN ?3 IS NULL OR length(value)<=?3 THEN value ELSE NULL END,version FROM durable_document_history WHERE key=?1 AND revision<=?2 ORDER BY revision DESC LIMIT 1",
            params![key, revision, cap], |r| Ok((r.get(0)?, r.get(1)?)),
        ).optional()?;
        match row {
            Some((Some(value), Some(version))) => Ok(Some(Entry { value, version })),
            Some((None, None)) | None => Ok(None),
            Some((None, Some(_))) if cap.is_some() => Err(Error::DocumentFrameTooLarge),
            _ => Err(Error::Storage(rusqlite::Error::InvalidQuery)),
        }
    }

    /// Enumerate live document entries at an immutable revision.
    pub fn documents_at(&self, revision: u64) -> Result<Vec<KeyedEntry>, Error> {
        self.entries_at_prefix(revision, b"durable/document/")
    }

    /// Enumerate live fork policies at an immutable revision, including policies
    /// whose document is absent.
    pub fn document_policies_at(&self, revision: u64) -> Result<Vec<KeyedEntry>, Error> {
        self.entries_at_prefix(revision, b"durable/document-policy/")
    }

    /// Enumerate a specific document or fork-policy key prefix at a revision.
    pub fn scoped_documents_at(
        &self,
        revision: u64,
        prefix: &[u8],
    ) -> Result<Vec<KeyedEntry>, Error> {
        if !prefix.starts_with(b"durable/document/")
            && !prefix.starts_with(b"durable/document-policy/")
        {
            return Err(Error::InvalidSnapshot);
        }
        self.entries_at_prefix(revision, prefix)
    }

    fn entries_at_prefix(&self, revision: u64, prefix: &[u8]) -> Result<Vec<KeyedEntry>, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let current: u64 = conn.query_row(
            "SELECT revision FROM durable_document_history_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if revision > current {
            return Err(Error::InvalidSnapshot);
        }
        let cap = read_limit(prefix);
        let sql_cap = cap.map(|v| i64::try_from(v).unwrap_or(i64::MAX));
        let mut stmt = conn.prepare("SELECT CASE WHEN ?3 IS NULL OR length(h.key)<=?3 THEN h.key ELSE NULL END,CASE WHEN ?3 IS NULL OR length(h.value)<=?3 THEN h.value ELSE NULL END,h.version FROM durable_document_history h WHERE substr(h.key,1,length(?2))=?2 AND h.revision=(SELECT max(x.revision) FROM durable_document_history x WHERE x.key=h.key AND x.revision<=?1) AND h.value IS NOT NULL ORDER BY h.key")?;
        let rows = stmt.query_map(params![revision, prefix, sql_cap], |r| {
            Ok((
                r.get::<_, Option<Vec<u8>>>(0)?,
                r.get::<_, Option<Vec<u8>>>(1)?,
                r.get(2)?,
            ))
        })?;
        let mut total = 0usize;
        let mut result = Vec::new();
        for row in rows {
            let (key, value, version) = row?;
            let key = key.ok_or(Error::DocumentFrameTooLarge)?;
            let value = value.ok_or(Error::DocumentFrameTooLarge)?;
            total = total.saturating_add(key.len()).saturating_add(value.len());
            if cap.is_some_and(|cap| total > cap) {
                return Err(Error::DocumentFrameTooLarge);
            }
            result.push((key, Entry { value, version }));
        }
        Ok(result)
    }

    /// All checks are evaluated against the pre-commit state; mutations apply
    /// in order. A failed check rolls back the complete batch.
    pub fn commit(&self, checks: &[Check], mutations: &[Mutation]) -> Result<(), Error> {
        self.commit_inner(checks, mutations, &[], None)
    }

    /// Commit the original atomic batch only if no put under the supplied
    /// prefixes occurred after `boundary`. Deletes alone are not tracked.
    /// The boundary must be obtained from `put_version` after migration.
    pub fn commit_with_prefix_put_fence(
        &self,
        checks: &[Check],
        mutations: &[Mutation],
        prefixes: &[&[u8]],
        boundary: u64,
    ) -> Result<(), Error> {
        self.commit_inner(checks, mutations, &[], Some((prefixes, boundary)))
    }

    /// Commit explicit document acquisitions with ordinary checked mutations.
    pub fn commit_with_task_documents(
        &self,
        checks: &[Check],
        mutations: &[Mutation],
        acquisitions: &[TaskDocumentAcquisition],
    ) -> Result<(), Error> {
        self.commit_inner(checks, mutations, acquisitions, None)
    }
    /// As above, with a graph put fence for scheduler finalization.
    pub fn commit_task_documents_with_prefix_put_fence(
        &self,
        checks: &[Check],
        mutations: &[Mutation],
        acquisitions: &[TaskDocumentAcquisition],
        prefixes: &[&[u8]],
        boundary: u64,
    ) -> Result<(), Error> {
        self.commit_inner(checks, mutations, acquisitions, Some((prefixes, boundary)))
    }

    fn commit_inner(
        &self,
        checks: &[Check],
        mutations: &[Mutation],
        acquisitions: &[TaskDocumentAcquisition],
        prefix_fence: Option<(&[&[u8]], u64)>,
    ) -> Result<(), Error> {
        let mut conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        #[cfg(feature = "experimental-graph-revision")]
        let _protocol = {
            struct Reset<'a>(&'a std::sync::atomic::AtomicBool);
            impl Drop for Reset<'_> {
                fn drop(&mut self) {
                    self.0.store(false, std::sync::atomic::Ordering::Release);
                }
            }
            self.graph_protocol_active
                .store(true, std::sync::atomic::Ordering::Release);
            Reset(&self.graph_protocol_active)
        };
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| match &error {
                rusqlite::Error::SqliteFailure(code, _)
                    if matches!(
                        code.code,
                        rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                    ) =>
                {
                    Error::WriteLockBusy(error)
                }
                _ => Error::Storage(error),
            })?;
        #[cfg(feature = "experimental-graph-revision")]
        validate_owned_reservation(&tx, mutations)?;
        if let Some((prefixes, boundary)) = prefix_fence {
            let current: u64 = tx.query_row(
                "SELECT version FROM durable_store_meta WHERE id=1",
                [],
                |r| r.get(0),
            )?;
            let legacy: u64 = tx.query_row(
                "SELECT legacy_version FROM durable_commit_meta WHERE id=1",
                [],
                |r| r.get(0),
            )?;
            if boundary > current || boundary < legacy {
                return Err(Error::InvalidSnapshot);
            }
            for prefix in prefixes {
                let changed: bool = if let Some(end) = prefix_end(prefix) {
                    tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM durable_commit_versions INDEXED BY durable_commit_versions_by_version WHERE key>=?1 AND key<?2 AND version>?3)",
                        params![prefix, end, boundary], |r| r.get(0),
                    )?
                } else {
                    tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM durable_commit_versions INDEXED BY durable_commit_versions_by_version WHERE key>=?1 AND version>?2)",
                        params![prefix, boundary], |r| r.get(0),
                    )?
                };
                if changed {
                    return Err(Error::Conflict {
                        key: prefix.to_vec(),
                    });
                }
            }
        }
        for check in checks {
            let version: Option<u64> = tx
                .query_row(
                    "SELECT version FROM durable_store_entries WHERE key=?1",
                    [&check.key],
                    |r| r.get(0),
                )
                .optional()?;
            let matches = match check.expected {
                Expected::Missing => version.is_none(),
                Expected::MissingUnchangedSince(boundary)
                | Expected::MissingUnchangedSinceBefore(boundary, _) => {
                    let current: u64 = tx.query_row(
                        "SELECT version FROM durable_store_meta WHERE id=1",
                        [],
                        |r| r.get(0),
                    )?;
                    let legacy: u64 = tx.query_row(
                        "SELECT legacy_version FROM durable_commit_meta WHERE id=1",
                        [],
                        |r| r.get(0),
                    )?;
                    if boundary > current || boundary < legacy {
                        return Err(Error::InvalidSnapshot);
                    }
                    let changed: bool = tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM durable_commit_versions WHERE key=?1 AND version>?2)",
                        params![check.key, boundary],
                        |r| r.get(0),
                    )?;
                    let before = match check.expected {
                        Expected::MissingUnchangedSinceBefore(_, deadline) => {
                            Instant::now() < deadline
                        }
                        _ => true,
                    };
                    version.is_none() && !changed && before
                }
                Expected::MissingBefore(deadline) => version.is_none() && Instant::now() < deadline,
                Expected::PrefixUnchangedSince(boundary) => {
                    let current: u64 = tx.query_row(
                        "SELECT version FROM durable_store_meta WHERE id=1",
                        [],
                        |r| r.get(0),
                    )?;
                    let legacy: u64 = tx.query_row(
                        "SELECT legacy_version FROM durable_commit_meta WHERE id=1",
                        [],
                        |r| r.get(0),
                    )?;
                    if boundary > current || boundary < legacy {
                        return Err(Error::InvalidSnapshot);
                    }
                    let changed: bool = if let Some(end) = prefix_end(&check.key) {
                        tx.query_row("SELECT EXISTS(SELECT 1 FROM durable_commit_versions INDEXED BY durable_commit_versions_by_version WHERE key>=?1 AND key<?2 AND version>?3)", params![check.key, end, boundary], |r| r.get(0))?
                    } else {
                        tx.query_row("SELECT EXISTS(SELECT 1 FROM durable_commit_versions INDEXED BY durable_commit_versions_by_version WHERE key>=?1 AND version>?2)", params![check.key, boundary], |r| r.get(0))?
                    };
                    !changed
                }
                Expected::Version(v) => version == Some(v),
                Expected::DocumentRevision(revision) => {
                    let current: u64 = tx.query_row(
                        "SELECT revision FROM durable_document_history_meta WHERE id=1",
                        [],
                        |r| r.get(0),
                    )?;
                    current == revision
                }
                Expected::VersionBefore(v, deadline) => {
                    version == Some(v) && Instant::now() < deadline
                }
                Expected::DocumentUnchangedSince(revision) => {
                    if !check.key.starts_with(b"durable/document/") {
                        return Err(Error::InvalidSnapshot);
                    }
                    let current: u64 = tx.query_row(
                        "SELECT revision FROM durable_document_history_meta WHERE id=1",
                        [],
                        |r| r.get(0),
                    )?;
                    if revision > current {
                        return Err(Error::InvalidSnapshot);
                    }
                    let changed: bool = tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM durable_document_history WHERE key=?1 AND revision>?2)",
                        params![check.key, revision], |r| r.get(0),
                    )?;
                    !changed
                }
            };
            if !matches {
                return Err(Error::Conflict {
                    key: check.key.clone(),
                });
            }
        }
        // Ownership is explicit metadata keyed by the exact document key. No
        // existing scope string is interpreted as a task scope. All validation,
        // candidate resolution, and retirement happen under this write lock.
        let mut owners = std::collections::HashMap::<Vec<u8>, Vec<u8>>::new();
        {
            let mut stmt =
                tx.prepare("SELECT document_key,owner FROM durable_task_document_owners")?;
            let rows = stmt.query_map([], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?))
            })?;
            for row in rows {
                let (doc, owner) = row?;
                if !doc.starts_with(b"durable/document/") || owner.len() != 16 {
                    return Err(Error::InvalidSnapshot);
                }
                owners.insert(doc, owner);
            }
        }
        for acquisition in acquisitions {
            let doc = &acquisition.document_key;
            if !doc.starts_with(b"durable/document/") {
                return Err(Error::InvalidSnapshot);
            }
            if owners.contains_key(doc) {
                return Err(Error::Conflict { key: doc.clone() });
            }
            let exists: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM durable_store_entries WHERE key=?1)",
                [doc],
                |r| r.get(0),
            )?;
            if exists {
                return Err(Error::Conflict { key: doc.clone() });
            }
            owners.insert(doc.clone(), acquisition.owner.as_bytes().to_vec());
        }
        let mut task_candidates = std::collections::HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
        for mutation in mutations {
            let (key, value) = match mutation {
                Mutation::Put { key, value } => (key, Some(value.clone())),
                Mutation::Delete { key } => (key, None),
            };
            if let Some(id) = key.strip_prefix(b"durable/task/") {
                if id.len() == 16 && owners.values().any(|owner| owner.as_slice() == id) {
                    task_candidates.insert(id.to_vec(), value);
                }
            }
        }
        // Only touched owners can affect this commit. A corrupt, unrelated
        // legacy task must not prevent independent store operations.
        let mut affected = std::collections::HashSet::<Vec<u8>>::new();
        affected.extend(
            acquisitions
                .iter()
                .map(|item| item.owner.as_bytes().to_vec()),
        );
        affected.extend(task_candidates.keys().cloned());
        for mutation in mutations {
            let key = match mutation {
                Mutation::Put { key, .. } | Mutation::Delete { key } => key,
            };
            let policy_document = key.strip_prefix(b"durable/document-policy/").map(|suffix| {
                let mut document = b"durable/document/".to_vec();
                document.extend_from_slice(suffix);
                document
            });
            if let Some(owner) = owners
                .get(key)
                .or_else(|| policy_document.as_ref().and_then(|doc| owners.get(doc)))
            {
                affected.insert(owner.clone());
            }
        }
        let mut states = std::collections::HashMap::<Vec<u8>, bool>::new();
        for owner in &affected {
            let mut key = b"durable/task/".to_vec();
            key.extend_from_slice(owner);
            let value = match task_candidates.get(owner) {
                Some(value) => value.clone(),
                None => tx
                    .query_row(
                        "SELECT value FROM durable_store_entries WHERE key=?1",
                        [&key],
                        |r| r.get(0),
                    )
                    .optional()?,
            };
            let state = value
                .as_deref()
                .and_then(|value| task_record::terminal(value, owner))
                .ok_or_else(|| Error::Conflict { key: key.clone() })?;
            states.insert(owner.clone(), state);
        }
        // Once a task with acquired documents is terminal, a raw task write
        // cannot resurrect it and thereby reopen its document addresses.
        let mut settling = std::collections::HashSet::new();
        for (owner, candidate) in &task_candidates {
            let mut task_key = b"durable/task/".to_vec();
            task_key.extend_from_slice(owner);
            let previous: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT value FROM durable_store_entries WHERE key=?1",
                    [&task_key],
                    |r| r.get(0),
                )
                .optional()?;
            let previous_terminal =
                match previous.as_deref() {
                    Some(value) => Some(task_record::terminal(value, owner).ok_or_else(|| {
                        Error::Conflict {
                            key: task_key.clone(),
                        }
                    })?),
                    None => None,
                };
            if candidate
                .as_deref()
                .and_then(|value| task_record::terminal(value, owner))
                == Some(true)
                && (previous.is_none() || previous_terminal == Some(false))
            {
                settling.insert(owner.clone());
            }
            if previous_terminal == Some(true)
                && candidate
                    .as_deref()
                    .and_then(|value| task_record::terminal(value, owner))
                    != Some(true)
            {
                return Err(Error::Conflict { key: task_key });
            }
        }
        for acquisition in acquisitions {
            let owner = acquisition.owner.as_bytes().as_slice();
            if states[owner] && !settling.contains(owner) {
                return Err(Error::Conflict {
                    key: acquisition.document_key.clone(),
                });
            }
        }
        let mut effective = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            let key = match mutation {
                Mutation::Put { key, .. } | Mutation::Delete { key } => key,
            };
            let policy_document = key.strip_prefix(b"durable/document-policy/").map(|suffix| {
                let mut document = b"durable/document/".to_vec();
                document.extend_from_slice(suffix);
                document
            });
            if let Some(owner) = owners
                .get(key)
                .or_else(|| policy_document.as_ref().and_then(|doc| owners.get(doc)))
            {
                if states[owner] && !settling.contains(owner) {
                    return Err(Error::Conflict { key: key.clone() });
                }
                // Keep the mutation in order; a retirement appended below
                // makes the final document state absent in this transaction.
            }
            effective.push(mutation.clone());
        }
        for (doc, owner) in &owners {
            if task_candidates.contains_key(owner) && states.get(owner) == Some(&true) {
                let exists: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM durable_store_entries WHERE key=?1)",
                    [doc],
                    |r| r.get(0),
                )?;
                if exists
                    || mutations
                        .iter()
                        .any(|mutation| matches!(mutation, Mutation::Put { key, .. } if key == doc))
                {
                    effective.push(Mutation::Delete { key: doc.clone() });
                }
            }
        }
        #[cfg(feature = "experimental-graph-revision")]
        {
            let revisions = graph_revision::affected(&tx, &effective)?;
            effective.extend(revisions.into_iter().map(|key| Mutation::Put {
                key,
                value: vec![1],
            }));
        }
        let mutations = effective.as_slice();
        for acquisition in acquisitions {
            tx.execute(
                "INSERT INTO durable_task_document_owners(document_key,owner) VALUES(?1,?2)",
                params![
                    &acquisition.document_key,
                    acquisition.owner.as_bytes().as_slice()
                ],
            )?;
        }
        let sequence = if mutations.is_empty() && acquisitions.is_empty() {
            None
        } else {
            let previous: i64 = tx.query_row(
                "SELECT sequence FROM durable_commit_meta WHERE id=1",
                [],
                |r| r.get(0),
            )?;
            let next = previous.checked_add(1).ok_or(Error::VersionExhausted)?;
            tx.execute(
                "UPDATE durable_commit_meta SET sequence=?1 WHERE id=1",
                [next],
            )?;
            Some(next)
        };
        let document_changed = mutations.iter().any(|mutation| match mutation {
            Mutation::Put { key, .. } | Mutation::Delete { key } => {
                key.starts_with(b"durable/document/")
                    || key.starts_with(b"durable/document-policy/")
                    || key.starts_with(b"durable/application-entries/")
            }
        });
        let revision = if document_changed {
            let previous: i64 = tx.query_row(
                "SELECT revision FROM durable_document_history_meta WHERE id=1",
                [],
                |r| r.get(0),
            )?;
            let next = previous.checked_add(1).ok_or(Error::VersionExhausted)?;
            tx.execute(
                "UPDATE durable_document_history_meta SET revision=?1 WHERE id=1",
                [next],
            )?;
            Some(next)
        } else {
            None
        };
        if let (Some(revision), Some(sequence)) = (revision, sequence) {
            tx.execute(
                "INSERT INTO durable_document_commit_sequences(revision,sequence) VALUES(?1,?2)",
                params![revision, sequence],
            )?;
        }
        for mutation in mutations {
            match mutation {
                Mutation::Put { key, value } => {
                    let version: i64 = tx.query_row(
                        "SELECT version FROM durable_store_meta WHERE id=1",
                        [],
                        |r| r.get(0),
                    )?;
                    let version = version.checked_add(1).ok_or(Error::VersionExhausted)?;
                    tx.execute(
                        "UPDATE durable_store_meta SET version=?1 WHERE id=1",
                        [version],
                    )?;
                    tx.execute("INSERT INTO durable_store_entries(key,value,version) VALUES(?1,?2,?3) ON CONFLICT(key) DO UPDATE SET value=excluded.value,version=excluded.version", params![key, value, version])?;
                    tx.execute("INSERT INTO durable_commit_versions(key,version,sequence) VALUES(?1,?2,?3)", params![key, version, sequence])?;
                }
                Mutation::Delete { key } => {
                    tx.execute("DELETE FROM durable_store_entries WHERE key=?1", [key])?;
                }
            }
            let key = match mutation {
                Mutation::Put { key, .. } | Mutation::Delete { key } => key,
            };
            if key.starts_with(b"durable/document/")
                || key.starts_with(b"durable/document-policy/")
                || key.starts_with(b"durable/application-entries/")
            {
                // Upsert accommodates raw batches that mutate the same key more
                // than once: the snapshot represents the final committed state.
                tx.execute("INSERT INTO durable_document_history(key,revision,value,version) VALUES(?1,?2,(SELECT value FROM durable_store_entries WHERE key=?1),(SELECT version FROM durable_store_entries WHERE key=?1)) ON CONFLICT(key,revision) DO UPDATE SET value=excluded.value,version=excluded.version", params![key, revision])?;
            }
        }
        // Capture the post-mutation revision in the same transaction as every
        // conversation snapshot, even when the document batch is appended later.
        let document_fence = checks.iter().find_map(|check| match check.expected {
            Expected::DocumentRevision(revision) => Some(revision),
            _ => None,
        });
        for mutation in mutations {
            if let Mutation::Put { key, .. } = mutation {
                if key.starts_with(b"runtime/fork/") {
                    tx.execute("INSERT OR IGNORE INTO durable_document_cuts(key,revision) SELECT ?1,revision FROM durable_document_history_meta WHERE id=1", [key])?;
                    tx.execute("INSERT OR IGNORE INTO durable_document_cut_versions(key,version,revision) SELECT ?1,e.version,m.revision FROM durable_store_entries e CROSS JOIN durable_document_history_meta m WHERE e.key=?1 AND m.id=1", [key])?;
                    if let Some(revision) = document_fence {
                        tx.execute("INSERT OR IGNORE INTO durable_document_fence_versions(key,version,revision) SELECT ?1,version,?2 FROM durable_store_entries WHERE key=?1", params![key, revision])?;
                    }
                }
            }
        }
        // Recheck time after assembling writes, while still holding the write lock.
        // A failure drops the transaction and rolls back every mutation.
        for check in checks {
            if let Expected::VersionBefore(_, deadline)
            | Expected::MissingBefore(deadline)
            | Expected::MissingUnchangedSinceBefore(_, deadline) = check.expected
            {
                if Instant::now() >= deadline {
                    return Err(Error::Conflict {
                        key: check.key.clone(),
                    });
                }
            }
        }
        tx.commit()?;
        if let Some(sequence) = sequence {
            self.commits.send_replace(sequence as u64);
        }
        Ok(())
    }
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(byte) = end.pop() {
        if byte != u8::MAX {
            end.push(byte + 1);
            return Some(end);
        }
    }
    None
}

#[cfg(test)]
mod document_history_tests {
    use super::*;

    #[test]
    fn scoped_read_limits_cover_pages_and_restore_out_of_order() {
        let db = Store::memory().unwrap();
        db.commit(
            &[],
            &[
                Mutation::Put {
                    key: b"test/head".to_vec(),
                    value: b"1234".to_vec(),
                },
                Mutation::Put {
                    key: b"test/record".to_vec(),
                    value: b"12345".to_vec(),
                },
            ],
        )
        .unwrap();
        let outer = BoundedReadScope::enter(&[b"test/"], 6);
        let inner = BoundedReadScope::enter(&[b"test/"], 4);
        drop(outer);
        assert!(matches!(
            db.get(b"test/record"),
            Err(Error::DocumentFrameTooLarge)
        ));
        assert!(matches!(
            db.page_with_metadata(b"test/head", b"test/r", b"test/", 2),
            Err(Error::DocumentFrameTooLarge)
        ));
        drop(inner);
        assert_eq!(db.get(b"test/record").unwrap().unwrap().value, b"12345");
        assert_eq!(
            db.page_with_metadata(b"test/head", b"test/r", b"test/", 2)
                .unwrap()
                .1
                .len(),
            1
        );
    }

    #[test]
    fn missing_unchanged_since_detects_exact_key_aba_and_deadline() {
        let db = Store::memory().unwrap();
        let boundary = db.put_version().unwrap();
        let key = b"durable/task/observed".to_vec();
        let other = b"durable/task/other".to_vec();
        let check = Check {
            key: key.clone(),
            expected: Expected::MissingUnchangedSince(boundary),
        };
        db.commit(
            &[],
            &[Mutation::Put {
                key: other,
                value: vec![1],
            }],
        )
        .unwrap();
        db.commit(std::slice::from_ref(&check), &[]).unwrap();
        db.commit(
            &[],
            &[Mutation::Put {
                key: key.clone(),
                value: vec![1],
            }],
        )
        .unwrap();
        db.commit(&[], &[Mutation::Delete { key: key.clone() }])
            .unwrap();
        assert!(matches!(
            db.commit(std::slice::from_ref(&check), &[]),
            Err(Error::Conflict { .. })
        ));
        let current = db.put_version().unwrap();
        let expired = Check {
            key: key.clone(),
            expected: Expected::MissingUnchangedSinceBefore(current, Instant::now()),
        };
        assert!(matches!(
            db.commit(&[expired], &[]),
            Err(Error::Conflict { .. })
        ));
        let future = Check {
            key,
            expected: Expected::MissingUnchangedSince(current + 1),
        };
        assert!(matches!(
            db.commit(&[future], &[]),
            Err(Error::InvalidSnapshot)
        ));
    }

    #[test]
    fn missing_unchanged_check_detects_same_commit_aba() {
        let db = Store::memory().unwrap();
        let key = vec![0, 255, 0];
        let boundary = db.put_version().unwrap();
        db.commit(
            &[],
            &[Mutation::Put {
                key: b"other".to_vec(),
                value: vec![],
            }],
        )
        .unwrap();
        let check = Check {
            key: key.clone(),
            expected: Expected::MissingUnchangedSince(boundary),
        };
        db.commit(std::slice::from_ref(&check), &[]).unwrap();
        db.commit(
            &[],
            &[
                Mutation::Put {
                    key: key.clone(),
                    value: vec![1],
                },
                Mutation::Delete { key: key.clone() },
            ],
        )
        .unwrap();
        assert!(matches!(db.commit(&[check], &[]), Err(Error::Conflict { key: k }) if k == key));
    }

    #[test]
    fn next_key_commit_skips_unrelated_writes_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let store = Store::open(&path).unwrap();
        let put = |key: &[u8]| Mutation::Put {
            key: key.to_vec(),
            value: vec![1],
        };
        store.commit(&[], &[put(b"root")]).unwrap();
        let first = store
            .commit_for_version(b"root", store.get(b"root").unwrap().unwrap().version)
            .unwrap()
            .unwrap();
        store.commit(&[], &[put(b"other")]).unwrap();
        store.commit(&[], &[put(b"root")]).unwrap();
        let second = store
            .commit_for_version(b"root", store.get(b"root").unwrap().unwrap().version)
            .unwrap()
            .unwrap();
        assert_eq!(
            store.next_commit_for_key(b"root", first).unwrap(),
            Some(second)
        );
        assert_eq!(store.next_commit_for_key(b"root", second).unwrap(), None);
        drop(store);
        let reopened = Store::open(&path).unwrap();
        assert_eq!(
            reopened.next_commit_for_key(b"root", first).unwrap(),
            Some(second)
        );
        assert!(matches!(
            reopened.next_commit_for_key(b"root", u64::MAX),
            Err(Error::InvalidSnapshot)
        ));
    }

    #[cfg(not(feature = "experimental-graph-revision"))]
    #[test]
    fn disabled_runtime_rejects_protected_database_before_migration() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("protected.sqlite");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE durable_graph_protocol(id INTEGER PRIMARY KEY, version INTEGER NOT NULL); INSERT INTO durable_graph_protocol VALUES(1,1);").unwrap();
        drop(conn);
        assert!(matches!(
            Store::open(&path),
            Err(Error::IncompatibleGraphProtocol)
        ));
        let conn = rusqlite::Connection::open(&path).unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='durable_store_entries'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "incompatible open must not migrate the database");
    }

    #[test]
    fn prefix_put_fence_is_atomic_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let store = Store::open(&path).unwrap();
        let boundary = store.put_version().unwrap();
        let prefixes: &[&[u8]] = &[b"tasks/"];
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: b"other/key".to_vec(),
                    value: vec![1],
                }],
            )
            .unwrap();
        store
            .commit_with_prefix_put_fence(&[], &[], prefixes, boundary)
            .unwrap();
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: b"tasks/child".to_vec(),
                    value: vec![2],
                }],
            )
            .unwrap();
        store
            .commit(
                &[],
                &[Mutation::Delete {
                    key: b"tasks/child".to_vec(),
                }],
            )
            .unwrap();
        assert!(matches!(
            store.commit_with_prefix_put_fence(
                &[],
                &[Mutation::Put {
                    key: b"unrelated".to_vec(),
                    value: vec![3]
                }],
                prefixes,
                boundary
            ),
            Err(Error::Conflict { .. })
        ));
        assert!(store.get(b"unrelated").unwrap().is_none());
        drop(store);
        let reopened = Store::open(&path).unwrap();
        assert!(matches!(
            reopened.commit_with_prefix_put_fence(&[], &[], prefixes, boundary),
            Err(Error::Conflict { .. })
        ));
        assert!(matches!(
            reopened.commit_with_prefix_put_fence(&[], &[], prefixes, u64::MAX),
            Err(Error::InvalidSnapshot)
        ));
    }

    #[test]
    fn document_commit_tracking_fences_missing_delete_evidence() {
        let store = Store::memory().unwrap();
        let (boundary, _) = store.document_commit_boundary().unwrap();
        let key = b"durable/document/example".to_vec();
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: key.clone(),
                    value: b"{}".to_vec(),
                }],
            )
            .unwrap();
        let (revision, sequence, puts) = store
            .next_scoped_document_commit(&key, boundary)
            .unwrap()
            .unwrap();
        assert_eq!(puts.len(), 1);
        assert_eq!(
            store.document_commit_sequence(revision).unwrap(),
            Some(sequence)
        );
        store
            .commit(&[], &[Mutation::Delete { key: key.clone() }])
            .unwrap();
        let (deleted, delete_sequence, changes) = store
            .next_scoped_document_commit(&key, revision)
            .unwrap()
            .unwrap();
        assert!(delete_sequence > sequence);
        assert_eq!(changes, vec![(key.clone(), None)]);
        store
            .connection
            .lock()
            .unwrap()
            .execute(
                "DELETE FROM durable_document_commit_sequences WHERE revision=?1",
                [deleted],
            )
            .unwrap();
        assert!(matches!(
            store.next_scoped_document_commit(&key, revision),
            Err(Error::InvalidSnapshot)
        ));
    }

    #[test]
    fn oversized_document_commit_fails_without_partial_frame() {
        let store = Store::memory().unwrap();
        let (boundary, _) = store.document_commit_boundary().unwrap();
        let writes: Vec<_> = (0..257)
            .map(|i| Mutation::Put {
                key: format!("durable/document/{i:03}").into_bytes(),
                value: b"{}".to_vec(),
            })
            .collect();
        store.commit(&[], &writes).unwrap();
        assert!(matches!(
            store.next_scoped_document_commit(b"durable/document/", boundary),
            Err(Error::DocumentFrameTooLarge)
        ));
        let store = Store::memory().unwrap();
        let (boundary, _) = store.document_commit_boundary().unwrap();
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: b"durable/document/large".to_vec(),
                    value: vec![b'x'; 8 * 1024 * 1024],
                }],
            )
            .unwrap();
        assert!(matches!(
            store.next_scoped_document_commit(b"durable/document/", boundary),
            Err(Error::DocumentFrameTooLarge)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn exact_snapshot_kill_child() {
        use std::io::Write;
        let Ok(path) = std::env::var("PI_EXACT_SNAPSHOT_KILL_DB") else {
            return;
        };
        let db = Store::open(path).unwrap();
        let root = b"runtime/conversation/kill".to_vec();
        db.commit(
            &[],
            &[
                Mutation::Put {
                    key: root.clone(),
                    value: b"root".to_vec(),
                },
                Mutation::Put {
                    key: b"runtime/fork/kill/first".to_vec(),
                    value: b"first".to_vec(),
                },
                Mutation::Put {
                    key: b"runtime/fork/kill/second".to_vec(),
                    value: b"second".to_vec(),
                },
            ],
        )
        .unwrap();
        let version = db.get(&root).unwrap().unwrap().version;
        let sequence = db.commit_for_version(&root, version).unwrap().unwrap();
        println!("EXACT_SNAPSHOT_COMMITTED {sequence}");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }

    #[cfg(unix)]
    #[test]
    fn explicit_runtime_snapshots_survive_process_kill() {
        use std::io::{BufRead, BufReader};
        use std::process::{Command, Stdio};
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "document_history_tests::exact_snapshot_kill_child",
                "--nocapture",
            ])
            .env("PI_EXACT_SNAPSHOT_KILL_DB", &path)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some((_, sequence)) = line.split_once("EXACT_SNAPSHOT_COMMITTED ") {
                    let _ = tx.send(sequence.to_string());
                    break;
                }
            }
        });
        let sequence = rx.recv_timeout(Duration::from_secs(20));
        child.kill().unwrap();
        child.wait().unwrap();
        let sequence = sequence.unwrap().parse::<u64>().unwrap();
        let db = Store::open(&path).unwrap();
        for key in [
            b"runtime/fork/kill/first".as_slice(),
            b"runtime/fork/kill/second".as_slice(),
        ] {
            assert!(db
                .runtime_snapshot_exact_at(
                    b"runtime/conversation/kill",
                    b"runtime/fork/kill/",
                    key,
                    sequence
                )
                .unwrap());
        }
    }

    #[test]
    fn canonical_runtime_snapshot_selects_only_one_exact_commit_key() {
        let dir = tempfile::tempdir().unwrap();
        let db = Store::open(dir.path().join("db")).unwrap();
        let root = b"runtime/conversation/a";
        let prefix = b"runtime/fork/a/";
        let canonical = b"runtime/fork/a/00000000000000000007";
        let other = b"runtime/fork/a/intermediate";
        db.commit(
            &[],
            &[
                Mutation::Put {
                    key: root.to_vec(),
                    value: vec![],
                },
                Mutation::Put {
                    key: canonical.to_vec(),
                    value: vec![],
                },
                Mutation::Put {
                    key: other.to_vec(),
                    value: vec![],
                },
            ],
        )
        .unwrap();
        let v = db.get(root).unwrap().unwrap().version;
        let sequence = db.commit_for_version(root, v).unwrap().unwrap();
        assert_eq!(
            db.runtime_canonical_snapshot_at(root, prefix, sequence)
                .unwrap(),
            Some(canonical.to_vec())
        );
        let duplicate = b"runtime/fork/a/00000000000000000008";
        db.commit(
            &[],
            &[
                Mutation::Put {
                    key: root.to_vec(),
                    value: vec![],
                },
                Mutation::Put {
                    key: canonical.to_vec(),
                    value: vec![],
                },
                Mutation::Put {
                    key: duplicate.to_vec(),
                    value: vec![],
                },
            ],
        )
        .unwrap();
        let new_version = db.get(root).unwrap().unwrap().version;
        let ambiguous = db.commit_for_version(root, new_version).unwrap().unwrap();
        assert_eq!(
            db.runtime_canonical_snapshot_at(root, prefix, ambiguous)
                .unwrap(),
            None
        );
        db.commit(
            &[],
            &[Mutation::Put {
                key: canonical.to_vec(),
                value: b"tampered".to_vec(),
            }],
        )
        .unwrap();
        assert_eq!(
            db.runtime_canonical_snapshot_at(root, prefix, sequence)
                .unwrap(),
            None
        );
    }

    #[test]
    fn explicit_runtime_snapshot_requires_the_same_tracked_root_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let root = b"runtime/conversation/example".to_vec();
        let other_root = b"runtime/conversation/other".to_vec();
        let prefix = b"runtime/fork/example/";
        let first = b"runtime/fork/example/first".to_vec();
        let second = b"runtime/fork/example/second".to_vec();
        let doc = b"durable/document/example".to_vec();
        let db = Store::open(&path).unwrap();
        db.commit(
            &[],
            &[
                Mutation::Put {
                    key: root.clone(),
                    value: b"root".to_vec(),
                },
                Mutation::Put {
                    key: first.clone(),
                    value: b"first".to_vec(),
                },
                Mutation::Put {
                    key: second.clone(),
                    value: b"second".to_vec(),
                },
                Mutation::Put {
                    key: doc.clone(),
                    value: b"before".to_vec(),
                },
            ],
        )
        .unwrap();
        let root_version = db.get(&root).unwrap().unwrap().version;
        let sequence = db.commit_for_version(&root, root_version).unwrap().unwrap();
        assert!(db
            .runtime_snapshot_at(&root, prefix, sequence)
            .unwrap()
            .is_none());
        assert!(db
            .runtime_snapshot_exact_at(&root, prefix, &first, sequence)
            .unwrap());
        assert!(db
            .runtime_snapshot_exact_at(&root, prefix, &second, sequence)
            .unwrap());
        assert!(!db
            .runtime_snapshot_exact_at(&other_root, prefix, &first, sequence)
            .unwrap());
        assert!(!db
            .runtime_snapshot_exact_at(&root, b"other/", &first, sequence)
            .unwrap());
        assert!(!db
            .runtime_snapshot_exact_at(&root, prefix, &first, 0)
            .unwrap());
        let revision = db.document_cut(&first).unwrap().unwrap();
        assert_eq!(db.document_cut(&second).unwrap(), Some(revision));
        db.commit(
            &[],
            &[
                Mutation::Put {
                    key: root.clone(),
                    value: b"later".to_vec(),
                },
                Mutation::Put {
                    key: doc.clone(),
                    value: b"after".to_vec(),
                },
            ],
        )
        .unwrap();
        drop(db);
        let db = Store::open(&path).unwrap();
        assert!(db
            .runtime_snapshot_exact_at(&root, prefix, &first, sequence)
            .unwrap());
        assert_eq!(
            db.document_at(&doc, revision).unwrap().unwrap().value,
            b"before"
        );
        db.commit(
            &[],
            &[Mutation::Put {
                key: first.clone(),
                value: b"first".to_vec(),
            }],
        )
        .unwrap();
        assert!(!db
            .runtime_snapshot_exact_at(&root, prefix, &first, sequence)
            .unwrap());
        assert!(db
            .runtime_snapshot_exact_at(&root, prefix, &second, sequence)
            .unwrap());
        db.commit(
            &[],
            &[Mutation::Delete {
                key: second.clone(),
            }],
        )
        .unwrap();
        assert!(!db
            .runtime_snapshot_exact_at(&root, prefix, &second, sequence)
            .unwrap());
    }

    #[test]
    fn application_revisions_preserve_document_watches_and_conflicts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared.db");
        let reader = Store::open(&path).unwrap();
        let writer = Store::open(&path).unwrap();
        let doc = b"durable/document/example".to_vec();
        let log = b"durable/application-entries/example".to_vec();
        let cut = b"runtime/fork/example/1".to_vec();
        let start = reader.document_snapshot().unwrap();
        writer
            .commit(
                &[],
                &[Mutation::Put {
                    key: log.clone(),
                    value: b"one".to_vec(),
                }],
            )
            .unwrap();
        let gap = reader.document_snapshot().unwrap();
        assert!(gap > start);
        assert!(reader.document_commits(start, 1).unwrap().is_empty());
        writer
            .commit(
                &[Check {
                    key: doc.clone(),
                    expected: Expected::DocumentUnchangedSince(start),
                }],
                &[
                    Mutation::Put {
                        key: cut.clone(),
                        value: vec![],
                    },
                    Mutation::Put {
                        key: log.clone(),
                        value: b"two".to_vec(),
                    },
                    Mutation::Put {
                        key: doc.clone(),
                        value: b"document".to_vec(),
                    },
                ],
            )
            .unwrap();
        let revision = reader.document_cut(&cut).unwrap().unwrap();
        assert_eq!(
            reader
                .application_entries_at(&log, gap)
                .unwrap()
                .unwrap()
                .value,
            b"one"
        );
        assert_eq!(
            reader
                .application_entries_at(&log, revision)
                .unwrap()
                .unwrap()
                .value,
            b"two"
        );
        assert_eq!(
            reader.document_at(&doc, revision).unwrap().unwrap().value,
            b"document"
        );
        let commits = reader.document_commits(start, 1).unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].0, revision);
        assert_eq!(commits[0].1.len(), 1);
        assert_eq!(commits[0].1[0].0, doc);
        assert!(matches!(
            reader.commit(
                &[Check {
                    key: doc,
                    expected: Expected::DocumentUnchangedSince(gap)
                }],
                &[]
            ),
            Err(Error::Conflict { .. })
        ));
    }

    #[test]
    fn migrates_existing_live_documents_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        let key = b"durable/document/legacy";
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch("CREATE TABLE durable_store_meta (id INTEGER PRIMARY KEY, version INTEGER NOT NULL); INSERT INTO durable_store_meta VALUES(1, 7); CREATE TABLE durable_store_entries (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL, version INTEGER NOT NULL) WITHOUT ROWID;").unwrap();
            conn.execute(
                "INSERT INTO durable_store_entries VALUES (?1, ?2, 7)",
                params![key, b"legacy".as_slice()],
            )
            .unwrap();
        }
        let db = Store::open(&path).unwrap();
        assert_eq!(db.put_version().unwrap(), 7);
        assert!(matches!(
            db.commit_with_prefix_put_fence(&[], &[], &[b"tasks/"], 6),
            Err(Error::InvalidSnapshot)
        ));
        db.commit_with_prefix_put_fence(&[], &[], &[b"tasks/"], 7)
            .unwrap();
        assert_eq!(db.document_snapshot().unwrap(), 1);
        assert_eq!(db.document_at(key, 0).unwrap().unwrap().version, 7);
        db.commit(
            &[Check {
                key: key.to_vec(),
                expected: Expected::Version(7),
            }],
            &[Mutation::Delete { key: key.to_vec() }],
        )
        .unwrap();
        drop(db);
        let db = Store::open(&path).unwrap();
        assert_eq!(db.document_at(key, 0).unwrap().unwrap().value, b"legacy");
        assert!(db.document_at(key, 2).unwrap().is_none());
    }
    #[test]
    fn legacy_application_cuts_fail_closed_and_live_log_is_seeded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        let key = b"durable/application-entries/example";
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch("CREATE TABLE durable_store_meta (id INTEGER PRIMARY KEY, version INTEGER NOT NULL); INSERT INTO durable_store_meta VALUES(1, 7); CREATE TABLE durable_store_entries (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL, version INTEGER NOT NULL) WITHOUT ROWID;").unwrap();
            conn.execute(
                "INSERT INTO durable_store_entries VALUES (?1, ?2, 7)",
                params![key, b"legacy".as_slice()],
            )
            .unwrap();
        }
        let db = Store::open(&path).unwrap();
        assert!(matches!(
            db.application_entries_at(key, 0),
            Err(Error::InvalidSnapshot)
        ));
        assert_eq!(
            db.application_entries_at(key, 1).unwrap().unwrap().value,
            b"legacy"
        );
        drop(db);
        let db = Store::open(&path).unwrap();
        assert_eq!(
            db.application_entries_at(key, 1).unwrap().unwrap().value,
            b"legacy"
        );
    }
    #[test]
    fn bounded_document_and_entry_reads_enforce_exact_size_and_retirement() {
        let db = Store::memory().unwrap();
        let key = b"durable/document/test".to_vec();
        let marker = b"marker".to_vec();
        db.commit(
            &[],
            &[
                Mutation::Put {
                    key: key.clone(),
                    value: b"12345".to_vec(),
                },
                Mutation::Put {
                    key: marker.clone(),
                    value: b"12345".to_vec(),
                },
            ],
        )
        .unwrap();
        let rev = db.document_snapshot().unwrap();
        assert!(matches!(
            db.get_bounded(&marker, 4),
            Err(Error::DocumentFrameTooLarge)
        ));
        assert_eq!(db.get_bounded(&marker, 5).unwrap().unwrap().value, b"12345");
        assert!(matches!(
            db.document_at_bounded(&key, rev, 4),
            Err(Error::DocumentFrameTooLarge)
        ));
        assert_eq!(
            db.document_at_bounded(&key, rev, 5).unwrap().unwrap().value,
            b"12345"
        );
        assert!(matches!(
            db.next_document_change_bounded(&key, 0, 4),
            Err(Error::DocumentFrameTooLarge)
        ));
        assert_eq!(
            db.next_document_change_bounded(&key, 0, 5)
                .unwrap()
                .unwrap()
                .0,
            rev
        );
        db.commit(&[], &[Mutation::Delete { key: key.clone() }])
            .unwrap();
        let (deleted, value) = db
            .next_document_change_bounded(&key, rev, 0)
            .unwrap()
            .unwrap();
        assert!(value.is_none());
        assert!(db.document_at_bounded(&key, deleted, 0).unwrap().is_none());
    }
}
