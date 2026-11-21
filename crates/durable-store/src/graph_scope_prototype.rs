//! Experimental conservative reachability over all observed graph values.
//! The feature is disabled by default while migration and driver checks are evaluated.
use std::collections::{HashMap, HashSet, VecDeque};
use uuid::Uuid;

const TASK: &[u8] = b"durable/task/";
const EDGE: &[u8] = b"durable/conversation-owner/";
const CONVERSATION: &[u8] = b"runtime/conversation/";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Node {
    Task(Uuid),
    Conversation(Uuid),
}

pub(crate) struct Change {
    pub(crate) key: Vec<u8>,
    pub(crate) before: Option<Vec<u8>>,
    pub(crate) after: Option<Vec<u8>>,
}

fn id(key: &[u8], prefix: &[u8], binary: bool) -> Result<Uuid, ()> {
    let suffix = key.strip_prefix(prefix).ok_or(())?;
    let id = if binary {
        Uuid::from_slice(suffix).map_err(|_| ())?
    } else {
        Uuid::parse_str(std::str::from_utf8(suffix).map_err(|_| ())?).map_err(|_| ())?
    };
    if id.is_nil() {
        return Err(());
    }
    Ok(id)
}
fn value_uuid(value: &serde_json::Value) -> Result<Uuid, ()> {
    let id = Uuid::parse_str(value.as_str().ok_or(())?).map_err(|_| ())?;
    if id.is_nil() {
        return Err(());
    }
    Ok(id)
}
fn owner(value: &serde_json::Value) -> Result<Node, ()> {
    let map = value.as_object().ok_or(())?;
    if map.len() != 1 {
        return Err(());
    }
    if let Some(id) = map.get("Task") {
        return Ok(Node::Task(value_uuid(id)?));
    }
    if let Some(id) = map.get("Conversation") {
        return Ok(Node::Conversation(value_uuid(id)?));
    }
    Err(())
}
fn observe(
    key: &[u8],
    value: &[u8],
    edges: &mut HashMap<Node, HashSet<Node>>,
    root: Uuid,
    parent_conversations: &mut HashSet<Node>,
) -> Result<(), ()> {
    let value: serde_json::Value = serde_json::from_slice(value).map_err(|_| ())?;
    if key.starts_with(TASK) {
        let task = id(key, TASK, true)?;
        if value_uuid(&value["id"])? != task {
            return Err(());
        }
        let parent = owner(&value["owner"])?;
        if task == root {
            if let Node::Conversation(_) = parent {
                parent_conversations.insert(parent);
            }
        }
        edges.entry(parent).or_default().insert(Node::Task(task));
    } else if key.starts_with(EDGE) {
        let child = id(key, EDGE, true)?;
        if value_uuid(&value["child"])? != child {
            return Err(());
        }
        let task = value_uuid(&value["task"])?;
        let _owner = owner(&value["task_owner"])?;
        if value["mode"] != "Foreground" && value["mode"] != "Background" {
            return Err(());
        }
        // Including background edges is conservative.
        edges
            .entry(Node::Task(task))
            .or_default()
            .insert(Node::Conversation(child));
    } else if key.starts_with(CONVERSATION) {
        let conversation = id(key, CONVERSATION, false)?;
        if value_uuid(&value["id"])? != conversation {
            return Err(());
        }
    } else {
        return Err(());
    }
    Ok(())
}
pub(crate) fn changed_scope(
    root: Uuid,
    current: &[(Vec<u8>, Vec<u8>)],
    changes: &[Change],
) -> Result<bool, ()> {
    if root.is_nil()
        || !current
            .iter()
            .any(|(key, _)| key.as_slice() == [TASK, root.as_bytes()].concat().as_slice())
    {
        return Err(());
    }
    let mut edges = HashMap::new();
    let mut parents = HashSet::new();
    for (key, value) in current {
        observe(key, value, &mut edges, root, &mut parents)?;
    }
    for change in changes {
        if change.before.is_none() && change.after.is_none() {
            return Err(());
        }
        for value in [&change.before, &change.after].into_iter().flatten() {
            observe(&change.key, value, &mut edges, root, &mut parents)?;
        }
    }
    let mut reached = HashSet::new();
    let mut pending = VecDeque::from([Node::Task(root)]);
    while let Some(node) = pending.pop_front() {
        if reached.insert(node) {
            pending.extend(edges.get(&node).into_iter().flatten().copied());
        }
    }
    // Ancestors themselves must remain stable, but their unrelated siblings
    // do not become descendants of this callback root.
    let mut reverse: HashMap<Node, HashSet<Node>> = HashMap::new();
    for (parent, children) in &edges {
        for child in children {
            reverse.entry(*child).or_default().insert(*parent);
        }
    }
    let mut ancestors = HashSet::new();
    let mut pending = VecDeque::from([Node::Task(root)]);
    while let Some(node) = pending.pop_front() {
        if ancestors.insert(node) {
            pending.extend(reverse.get(&node).into_iter().flatten().copied());
        }
    }
    reached.extend(ancestors);
    reached.extend(parents);
    for change in changes {
        let node = if change.key.starts_with(TASK) {
            Node::Task(id(&change.key, TASK, true)?)
        } else if change.key.starts_with(EDGE) {
            Node::Conversation(id(&change.key, EDGE, true)?)
        } else if change.key.starts_with(CONVERSATION) {
            Node::Conversation(id(&change.key, CONVERSATION, false)?)
        } else {
            return Err(());
        };
        if reached.contains(&node) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn task(id: Uuid, owner: Node) -> (Vec<u8>, Vec<u8>) {
        let owner = match owner {
            Node::Task(x) => json!({"Task": x}),
            Node::Conversation(x) => json!({"Conversation": x}),
        };
        (
            [TASK, id.as_bytes()].concat(),
            serde_json::to_vec(&json!({"id":id,"owner":owner})).unwrap(),
        )
    }
    fn change(pair: &(Vec<u8>, Vec<u8>), before: bool, after: bool) -> Change {
        Change {
            key: pair.0.clone(),
            before: before.then(|| pair.1.clone()),
            after: after.then(|| pair.1.clone()),
        }
    }
    #[test]
    fn observes_insert_delete_and_reparent_aba() {
        let root = Uuid::from_u128(1);
        let child = Uuid::from_u128(2);
        let other = Uuid::from_u128(3);
        let conv = Uuid::from_u128(4);
        let base = task(root, Node::Conversation(conv));
        let owned = task(child, Node::Task(root));
        let outside = task(child, Node::Task(other));
        assert_eq!(
            changed_scope(root, &[base.clone()], &[change(&owned, false, true)]),
            Ok(true)
        );
        assert_eq!(
            changed_scope(root, &[base.clone()], &[change(&owned, true, false)]),
            Ok(true)
        );
        let aba = vec![
            Change {
                key: owned.0.clone(),
                before: Some(outside.1.clone()),
                after: Some(owned.1.clone()),
            },
            Change {
                key: owned.0.clone(),
                before: Some(owned.1.clone()),
                after: Some(outside.1.clone()),
            },
        ];
        assert_eq!(
            changed_scope(root, &[base.clone(), outside.clone()], &aba),
            Ok(true)
        );
        assert_eq!(
            changed_scope(root, &[base], &[change(&outside, false, true)]),
            Ok(false)
        );
    }
    #[test]
    fn follows_owned_conversations_and_ignores_other_roots() {
        let root = Uuid::from_u128(1);
        let owner = Uuid::from_u128(2);
        let conv = Uuid::from_u128(3);
        let child = Uuid::from_u128(4);
        let other = Uuid::from_u128(5);
        let root_task = task(root, Node::Conversation(owner));
        let edge=([EDGE,conv.as_bytes()].concat(),serde_json::to_vec(&json!({"task":root,"task_owner":{"Conversation":owner},"child":conv,"mode":"Foreground"})).unwrap());
        let child_task = task(child, Node::Conversation(conv));
        let unrelated = task(other, Node::Conversation(Uuid::from_u128(6)));
        assert_eq!(
            changed_scope(
                root,
                &[root_task.clone(), edge.clone()],
                &[change(&child_task, false, true)]
            ),
            Ok(true)
        );
        assert_eq!(
            changed_scope(root, &[root_task, edge], &[change(&unrelated, false, true)]),
            Ok(false)
        );
    }
    #[test]
    fn nested_root_fences_ancestors_but_not_siblings() {
        let parent = Uuid::from_u128(1);
        let root = Uuid::from_u128(2);
        let sibling = Uuid::from_u128(3);
        let conversation = Uuid::from_u128(4);
        let parent_task = task(parent, Node::Conversation(conversation));
        let root_task = task(root, Node::Task(parent));
        let sibling_task = task(sibling, Node::Task(parent));
        let current = vec![parent_task.clone(), root_task, sibling_task.clone()];
        assert_eq!(
            changed_scope(root, &current, &[change(&parent_task, true, true)]),
            Ok(true)
        );
        assert_eq!(
            changed_scope(root, &current, &[change(&sibling_task, true, true)]),
            Ok(false)
        );
    }
    #[test]
    fn malformed_values_fail_closed() {
        let root = Uuid::from_u128(1);
        let bad = Change {
            key: [TASK, Uuid::from_u128(2).as_bytes()].concat(),
            before: None,
            after: Some(b"bad".to_vec()),
        };
        let base = task(root, Node::Conversation(Uuid::from_u128(3)));
        assert!(changed_scope(root, &[base], &[bad]).is_err());
    }
}
