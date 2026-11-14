//! Durable task-to-conversation ownership records. These are storage primitives;
//! they do not schedule, abort, or otherwise manage either endpoint.
use crate::{
    runtime::Conversation,
    task::{TaskOwner, TaskState},
    task_store::{self, TaskStore},
};
use durable_store::{Check, Entry, Expected, Mutation, Store};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const PREFIX: &[u8] = b"durable/conversation-owner/";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mode {
    Foreground,
    Background,
}

/// An immutable edge. Each child conversation can have at most one owning task.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    pub task: Uuid,
    pub task_owner: TaskOwner,
    pub child: Uuid,
    pub mode: Mode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredEdge {
    pub edge: Edge,
    pub version: u64,
}

/// Include all checks and mutations unchanged in one `Store::commit`. Do not
/// add conflicting writes to these keys or replace either endpoint with a
/// different identity or ownership in the same transaction. A caller that
/// traverses edges for abort must also advance the task record version in this
/// transaction: an edge write alone does not invalidate a traversal that has
/// already read the task and not yet observed the edge. On an uncertain
/// commit outcome, reopen and inspect the records before retrying.
pub struct PreparedEdge {
    pub checks: Vec<Check>,
    pub mutations: Vec<Mutation>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Task(#[from] task_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("missing or invalid task, owner, child, or edge")]
    Invalid,
}

pub(crate) fn edge_key(child: Uuid) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.extend_from_slice(child.as_bytes());
    key
}
fn child_key(child: Uuid) -> Vec<u8> {
    format!("runtime/conversation/{child}").into_bytes()
}
fn decode(key: &[u8], entry: Entry) -> Result<StoredEdge, Error> {
    let edge: Edge = serde_json::from_slice(&entry.value)?;
    if edge_key(edge.child) != key || edge.task_owner == TaskOwner::Conversation(edge.child) {
        return Err(Error::Invalid);
    }
    Ok(StoredEdge {
        edge,
        version: entry.version,
    })
}

pub struct EdgeStore<'a> {
    store: &'a Store,
}
impl<'a> EdgeStore<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }
    pub fn get(&self, child: Uuid) -> Result<Option<StoredEdge>, Error> {
        let key = edge_key(child);
        self.store
            .get(&key)?
            .map(|entry| decode(&key, entry))
            .transpose()
    }
    /// Snapshot of a task's children; concurrent writes may immediately make it stale.
    pub fn children(&self, task: Uuid) -> Result<Vec<StoredEdge>, Error> {
        self.store
            .list(PREFIX)?
            .into_iter()
            .map(|(k, e)| decode(&k, e))
            .filter_map(|r| match r {
                Ok(e) if e.edge.task != task => None,
                other => Some(other),
            })
            .collect()
    }
    /// Prepare creation against existing task and child snapshots. The caller's
    /// expected owner is checked against the task record. The child must already
    /// exist; its actual identity and both endpoint versions are checked. A
    /// concurrent change or another owner of this child makes commit conflict.
    /// This only rejects a direct conversation self-edge, not longer cycles.
    pub fn prepare_create(&self, edge: &Edge) -> Result<PreparedEdge, Error> {
        let task = TaskStore::new(self.store)
            .load(edge.task)?
            .ok_or(Error::Invalid)?;
        if task.task.owner() != &edge.task_owner
            || task.task.is_terminal()
            || task.task.abort_requested()
            || edge.task_owner == TaskOwner::Conversation(edge.child)
            || !matches!(
                task.task.state(),
                TaskState::Ready | TaskState::Running | TaskState::Waiting(_)
            )
        {
            return Err(Error::Invalid);
        }
        let child_key = child_key(edge.child);
        let child_entry = self.store.get(&child_key)?.ok_or(Error::Invalid)?;
        let child: Conversation = serde_json::from_slice(&child_entry.value)?;
        if child.id != edge.child {
            return Err(Error::Invalid);
        }
        let key = edge_key(edge.child);
        Ok(PreparedEdge {
            checks: vec![
                Check {
                    key: task_store::key(edge.task),
                    expected: Expected::Version(task.version),
                },
                Check {
                    key: child_key,
                    expected: Expected::Version(child_entry.version),
                },
                Check {
                    key: key.clone(),
                    expected: Expected::Missing,
                },
            ],
            mutations: vec![Mutation::Put {
                key,
                value: serde_json::to_vec(edge)?,
            }],
        })
    }
}
