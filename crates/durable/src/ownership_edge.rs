//! Durable task-to-conversation ownership records. These are storage primitives;
//! they do not schedule, abort, or otherwise manage either endpoint.
use crate::{
    runtime::Conversation,
    task::{TaskOwner, TaskState},
    task_store::{self, TaskStore},
};
use durable_store::{Check, Entry, Expected, Mutation, Store};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use uuid::Uuid;

pub(crate) const PREFIX: &[u8] = b"durable/conversation-owner/";

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
/// different identity or ownership in the same transaction. Do not compose
/// another ownership edge or modify ownership records in the same transaction:
/// checks see the pre-commit state. Re-prepare after a conflict. The mutations
/// advance the task version to fence abort traversals. On an uncertain commit
/// outcome, reopen and inspect the records before retrying.
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
pub(crate) fn decode(key: &[u8], entry: Entry) -> Result<StoredEdge, Error> {
    let edge: Edge = serde_json::from_slice(&entry.value)?;
    // Existing stores may contain a self-cycle. Admission rejects creating one,
    // but cleanup must be able to read and drain it.
    if edge_key(edge.child) != key {
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
    /// The entire owner chain, including background edges, is fenced at commit.
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
        let mut checks = owner_chain_checks(self.store, edge.task, edge.child)?;
        checks.extend([
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
        ]);
        Ok(PreparedEdge {
            checks,
            mutations: vec![
                Mutation::Put {
                    key: task_store::key(edge.task),
                    value: serde_json::to_vec(&task.task)?,
                },
                Mutation::Put {
                    key,
                    value: serde_json::to_vec(edge)?,
                },
            ],
        })
    }
}

// A legacy store can contain cycles even though current admission rejects them.
// Capture the ownership graph and pending states in a single store snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum CleanupNode {
    Task(Uuid),
    Conversation(Uuid),
}

pub(crate) struct CleanupGraph {
    children: std::collections::HashMap<CleanupNode, Vec<CleanupNode>>,
    active: HashSet<CleanupNode>,
}

impl CleanupGraph {
    pub(crate) fn load(store: &Store) -> Result<Self, Error> {
        use std::collections::HashMap;
        let entries =
            store.list_prefixes(&[task_store::PREFIX, PREFIX, b"runtime/conversation/"])?;
        let mut tasks = HashMap::new();
        let mut edges = Vec::new();
        let mut active = HashSet::new();
        for (key, entry) in entries {
            if key.starts_with(task_store::PREFIX) {
                let task = task_store::decode(&key, entry)?;
                if !task.task.is_terminal() {
                    active.insert(CleanupNode::Task(task.task.id()));
                }
                tasks.insert(task.task.id(), task.task);
            } else if key.starts_with(PREFIX) {
                edges.push(decode(&key, entry)?.edge);
            } else {
                let conversation: Conversation = serde_json::from_slice(&entry.value)?;
                if conversation.abort_requested {
                    active.insert(CleanupNode::Conversation(conversation.id));
                }
            }
        }
        let mut children: HashMap<CleanupNode, Vec<CleanupNode>> = HashMap::new();
        for task in tasks.values().filter(|task| !task.is_background()) {
            let parent = match task.owner() {
                TaskOwner::Conversation(id) => CleanupNode::Conversation(*id),
                TaskOwner::Task(id) => CleanupNode::Task(*id),
            };
            children
                .entry(parent)
                .or_default()
                .push(CleanupNode::Task(task.id()));
        }
        for edge in edges {
            let owner = tasks.get(&edge.task).ok_or(Error::Invalid)?;
            if edge.task_owner != *owner.owner() {
                return Err(Error::Invalid);
            }
            if edge.mode == Mode::Foreground {
                children
                    .entry(CleanupNode::Task(edge.task))
                    .or_default()
                    .push(CleanupNode::Conversation(edge.child));
            }
        }
        Ok(Self { children, active })
    }

    fn reaches(&self, start: CleanupNode, target: CleanupNode) -> bool {
        let mut pending = vec![start];
        let mut seen = HashSet::new();
        while let Some(node) = pending.pop() {
            if !self.active.contains(&node) {
                continue;
            }
            if node == target {
                return true;
            }
            if seen.insert(node)
                && let Some(children) = self.children.get(&node)
            {
                pending.extend(children.iter().copied());
            }
        }
        false
    }

    // Break a cycle only after every reachable dependency outside its strongly
    // connected component has settled. Otherwise an owner could overtake an
    // ordinary child just because another child participates in a cycle.
    fn can_relax(&self, parent: CleanupNode, child: CleanupNode) -> bool {
        if !self.reaches(child, parent) {
            return false;
        }
        let mut pending = vec![child];
        let mut seen = HashSet::new();
        while let Some(node) = pending.pop() {
            if !self.active.contains(&node) || !seen.insert(node) {
                continue;
            }
            if !self.reaches(node, parent) {
                return false;
            }
            if let Some(children) = self.children.get(&node) {
                pending.extend(children.iter().copied());
            }
        }
        true
    }

    // Keep conversation -> task waits, so abort_and_wait still joins all tasks.
    pub(crate) fn cyclic_conversation_edge(&self, task: Uuid, child: Uuid) -> bool {
        self.can_relax(CleanupNode::Task(task), CleanupNode::Conversation(child))
    }

    // A task-only cycle has no conversation edge to relax. UUID ordering breaks
    // its back edges deterministically; background dependencies are never cut.
    pub(crate) fn cyclic_task_edge(&self, parent: Uuid, child: Uuid) -> bool {
        child <= parent && self.can_relax(CleanupNode::Task(parent), CleanupNode::Task(child))
    }
}

/// Fail closed until foreground conversation cleanup can be joined by its owner.
pub(crate) fn reject_foreground_children(
    store: &Store,
    tasks: impl IntoIterator<Item = Uuid>,
) -> Result<bool, Error> {
    let edges = EdgeStore::new(store);
    for task in tasks {
        if edges
            .children(task)?
            .iter()
            .any(|edge| edge.edge.mode == Mode::Foreground)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Follow owners upwards. A missing conversation edge is a root and must be
/// checked as missing, so a concurrent edge insertion invalidates the result.
pub(crate) fn owner_chain_checks(
    store: &Store,
    task: Uuid,
    child: Uuid,
) -> Result<Vec<Check>, Error> {
    owner_chain_from(store, TaskOwner::Task(task), child)
}

pub(crate) fn owner_chain_from(
    store: &Store,
    mut next: TaskOwner,
    child: Uuid,
) -> Result<Vec<Check>, Error> {
    let mut checks = Vec::new();
    let mut seen_tasks = HashSet::new();
    let mut seen_conversations = HashSet::new();
    loop {
        match next {
            TaskOwner::Task(id) => {
                if !seen_tasks.insert(id) {
                    return Err(Error::Invalid);
                }
                let stored = TaskStore::new(store).load(id)?.ok_or(Error::Invalid)?;
                checks.push(Check {
                    key: task_store::key(id),
                    expected: Expected::Version(stored.version),
                });
                next = stored.task.owner().clone();
            }
            TaskOwner::Conversation(id) => {
                if id == child || !seen_conversations.insert(id) {
                    return Err(Error::Invalid);
                }
                let key = edge_key(id);
                match store.get(&key)? {
                    Some(entry) => {
                        let stored = decode(&key, entry)?;
                        checks.push(Check {
                            key,
                            expected: Expected::Version(stored.version),
                        });
                        let owner = TaskStore::new(store)
                            .load(stored.edge.task)?
                            .ok_or(Error::Invalid)?;
                        if owner.task.owner() != &stored.edge.task_owner {
                            return Err(Error::Invalid);
                        }
                        checks.push(Check {
                            key: task_store::key(stored.edge.task),
                            expected: Expected::Version(owner.version),
                        });
                        next = TaskOwner::Task(stored.edge.task);
                    }
                    None => {
                        checks.push(Check {
                            key,
                            expected: Expected::Missing,
                        });
                        return Ok(checks);
                    }
                }
            }
        }
    }
}
