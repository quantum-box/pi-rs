//! Experimental ownership-scoped revision derivation. Not enabled in production.
use crate::{Error, Mutation};
use rusqlite::{OptionalExtension, Transaction};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

const TASK: &[u8] = b"durable/task/";
const EDGE: &[u8] = b"durable/conversation-owner/";
const CONVERSATION: &[u8] = b"runtime/conversation/";
const REVISION: &[u8] = b"durable/experimental-graph-revision/v1/";
const STRUCTURE: &[u8] = b"durable/experimental-graph-structure/v1/";

pub fn task_structure_revision(id: Uuid) -> Vec<u8> {
    [STRUCTURE, b"task/", id.as_bytes()].concat()
}
pub fn conversation_structure_revision(id: Uuid) -> Vec<u8> {
    [STRUCTURE, b"conversation/", id.as_bytes()].concat()
}
pub fn global_structure_revision() -> Vec<u8> {
    [STRUCTURE, b"global"].concat()
}

pub fn task_revision(id: Uuid) -> Vec<u8> {
    [REVISION, b"task/", id.as_bytes()].concat()
}
pub fn conversation_revision(id: Uuid) -> Vec<u8> {
    [REVISION, b"conversation/", id.as_bytes()].concat()
}
pub fn global_revision() -> Vec<u8> {
    [REVISION, b"global"].concat()
}
fn task_key(id: Uuid) -> Vec<u8> {
    [TASK, id.as_bytes()].concat()
}
fn edge_key(id: Uuid) -> Vec<u8> {
    [EDGE, id.as_bytes()].concat()
}

type Overlay = HashMap<Vec<u8>, Option<Vec<u8>>>;
fn read(
    tx: &Transaction<'_>,
    key: &[u8],
    overlay: Option<&Overlay>,
) -> Result<Option<Vec<u8>>, Error> {
    if let Some(value) = overlay.and_then(|m| m.get(key)) {
        return Ok(value.clone());
    }
    Ok(tx
        .query_row(
            "SELECT value FROM durable_store_entries WHERE key=?1",
            [key],
            |r| r.get(0),
        )
        .optional()?)
}
fn uuid_field(value: &serde_json::Value, name: &str) -> Option<Uuid> {
    Uuid::parse_str(value.get(name)?.as_str()?).ok()
}
fn walk_task(
    tx: &Transaction<'_>,
    id: Uuid,
    overlay: Option<&Overlay>,
    revisions: &mut HashSet<Vec<u8>>,
) -> Result<bool, Error> {
    let mut next = id;
    let mut seen = HashSet::new();
    loop {
        if !seen.insert(next) {
            return Ok(false);
        }
        revisions.insert(task_revision(next));
        let Some(bytes) = read(tx, &task_key(next), overlay)? else {
            return Ok(false);
        };
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
            return Ok(false);
        };
        if uuid_field(&value, "id") != Some(next) {
            return Ok(false);
        }
        let Some(owner) = value.get("owner") else {
            return Ok(false);
        };
        if let Some(parent) = owner
            .get("Task")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
        {
            next = parent;
        } else if let Some(conversation) = owner
            .get("Conversation")
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
        {
            revisions.insert(conversation_revision(conversation));
            let Some(edge) = read(tx, &edge_key(conversation), overlay)? else {
                return Ok(true);
            };
            let Ok(edge) = serde_json::from_slice::<serde_json::Value>(&edge) else {
                return Ok(false);
            };
            if uuid_field(&edge, "child") != Some(conversation) {
                return Ok(false);
            }
            let Some(parent) = uuid_field(&edge, "task") else {
                return Ok(false);
            };
            next = parent;
        } else {
            return Ok(false);
        }
    }
}
fn walk_conversation(
    tx: &Transaction<'_>,
    id: Uuid,
    overlay: Option<&Overlay>,
    revisions: &mut HashSet<Vec<u8>>,
) -> Result<bool, Error> {
    revisions.insert(conversation_revision(id));
    let Some(edge) = read(tx, &edge_key(id), overlay)? else {
        return Ok(true);
    };
    let Ok(edge) = serde_json::from_slice::<serde_json::Value>(&edge) else {
        return Ok(false);
    };
    if uuid_field(&edge, "child") != Some(id) {
        return Ok(false);
    }
    match uuid_field(&edge, "task") {
        Some(task) => walk_task(tx, task, overlay, revisions),
        None => Ok(false),
    }
}
/// Enumerate affected ancestors in both the pre- and post-batch graph.
pub(crate) fn affected(
    tx: &Transaction<'_>,
    mutations: &[Mutation],
) -> Result<Vec<Vec<u8>>, Error> {
    let mut structural = Vec::new();
    for mutation in mutations {
        let (key, next) = match mutation {
            Mutation::Put { key, value } => (key, Some(value.as_slice())),
            Mutation::Delete { key } => (key, None),
        };
        let changed = if key.starts_with(EDGE) || key.starts_with(STRUCTURE) {
            true
        } else if key.starts_with(TASK) {
            let old = read(tx, key, None)?;
            match (old.as_deref(), next) {
                (Some(old), Some(next)) => {
                    match (
                        serde_json::from_slice::<serde_json::Value>(old),
                        serde_json::from_slice::<serde_json::Value>(next),
                    ) {
                        (Ok(old), Ok(next)) if old.is_object() && next.is_object() => [
                            "id",
                            "owner",
                            "kind",
                            "definition_version",
                            "background",
                            "task_tool",
                        ]
                        .iter()
                        .any(|field| old.get(*field) != next.get(*field)),
                        _ => true,
                    }
                }
                (None, None) => false,
                _ => true,
            }
        } else if key.starts_with(CONVERSATION) {
            read(tx, key, None)?.is_some() != next.is_some()
        } else {
            false
        };
        if changed {
            structural.push(mutation.clone());
        }
    }
    let mut result = affected_graph(tx, mutations)?;
    let mut structure = affected_graph(tx, &structural)?;
    for key in &mut structure {
        if let Some(suffix) = key.strip_prefix(REVISION) {
            *key = [STRUCTURE, suffix].concat();
        }
    }
    if mutations.iter().any(|m| match m {
        Mutation::Put { key, .. } | Mutation::Delete { key } => key.starts_with(STRUCTURE),
    }) {
        structure.push(global_structure_revision());
    }
    result.extend(structure);
    Ok(result)
}

fn affected_graph(tx: &Transaction<'_>, mutations: &[Mutation]) -> Result<Vec<Vec<u8>>, Error> {
    let mut overlay = Overlay::new();
    for mutation in mutations {
        let key = match mutation {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key,
        };
        if !key.starts_with(TASK) && !key.starts_with(EDGE) && !key.starts_with(CONVERSATION) {
            continue;
        }
        match mutation {
            Mutation::Put { key, value } => {
                overlay.insert(key.clone(), Some(value.clone()));
            }
            Mutation::Delete { key } => {
                overlay.insert(key.clone(), None);
            }
        }
    }
    let mut revisions = HashSet::new();
    let mut uncertain = false;
    for mutation in mutations {
        let key = match mutation {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key,
        };
        if let Some(id) = key.strip_prefix(TASK) {
            let Ok(id) = Uuid::from_slice(id) else {
                uncertain = true;
                continue;
            };
            for after in [false, true] {
                let o = after.then_some(&overlay);
                if read(tx, key, o)?.is_some() && !walk_task(tx, id, o, &mut revisions)? {
                    uncertain = true;
                }
            }
        } else if let Some(id) = key.strip_prefix(EDGE) {
            let Ok(id) = Uuid::from_slice(id) else {
                uncertain = true;
                continue;
            };
            for after in [false, true] {
                if !walk_conversation(tx, id, after.then_some(&overlay), &mut revisions)? {
                    uncertain = true;
                }
            }
        } else if let Some(id) = key.strip_prefix(CONVERSATION) {
            let Ok(id) = std::str::from_utf8(id)
                .ok()
                .and_then(|s| Uuid::parse_str(s).ok())
                .ok_or(())
            else {
                uncertain = true;
                continue;
            };
            for after in [false, true] {
                if !walk_conversation(tx, id, after.then_some(&overlay), &mut revisions)? {
                    uncertain = true;
                }
            }
        } else if key.starts_with(REVISION) {
            uncertain = true;
        }
    }
    if uncertain {
        revisions.insert(global_revision());
    }
    Ok(revisions.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;
    use serde_json::json;

    fn next_uuid() -> Uuid {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Uuid::from_u128(NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u128)
    }
    fn task(id: Uuid, owner: serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(&json!({"id":id, "owner":owner, "background":false})).unwrap()
    }
    fn put(key: Vec<u8>, value: Vec<u8>) -> Mutation {
        Mutation::Put { key, value }
    }
    fn version(store: &Store, key: &[u8]) -> Option<u64> {
        store.get(key).unwrap().map(|entry| entry.version)
    }

    #[test]
    fn structure_revision_ignores_state_but_detects_child_aba() {
        let store = Store::memory().unwrap();
        let root = next_uuid();
        let child = next_uuid();
        let conversation = next_uuid();
        let key = task_key(root);
        let initial = json!({"id":root,"owner":{"Conversation":conversation},"state":"Ready","kind":"worker","definition_version":1});
        store
            .commit(
                &[],
                &[put(key.clone(), serde_json::to_vec(&initial).unwrap())],
            )
            .unwrap();
        let revision = task_structure_revision(root);
        let start = version(&store, &revision).unwrap();
        let mut next = initial.clone();
        next["state"] = json!("Running");
        store
            .commit(&[], &[put(key, serde_json::to_vec(&next).unwrap())])
            .unwrap();
        assert_eq!(version(&store, &revision), Some(start));
        let unrelated = next_uuid();
        store
            .commit(
                &[],
                &[put(
                    task_key(unrelated),
                    task(unrelated, json!({"Conversation":next_uuid()})),
                )],
            )
            .unwrap();
        assert_eq!(version(&store, &revision), Some(start));
        let child_key = task_key(child);
        store
            .commit(
                &[],
                &[put(child_key.clone(), task(child, json!({"Task":root})))],
            )
            .unwrap();
        store
            .commit(&[], &[Mutation::Delete { key: child_key }])
            .unwrap();
        assert_ne!(version(&store, &revision), Some(start));
    }

    #[test]
    fn missing_guard_rejects_reopen_without_repairing_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("damaged.sqlite");
        drop(Store::open(&path).unwrap());
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("DROP TRIGGER durable_graph_protocol_update")
            .unwrap();
        drop(conn);
        assert!(matches!(
            Store::open(&path),
            Err(Error::IncompatibleGraphProtocol)
        ));
        let conn = rusqlite::Connection::open(&path).unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='durable_graph_protocol_update'", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn unknown_protocol_version_rejects_before_schema_migration() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unknown.sqlite");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE durable_graph_protocol(id INTEGER PRIMARY KEY, version INTEGER NOT NULL); INSERT INTO durable_graph_protocol VALUES(1,2);").unwrap();
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
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
        let version: i64 = conn
            .query_row(
                "SELECT version FROM durable_graph_protocol WHERE id=1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(version, 2);
    }

    #[cfg(unix)]
    #[test]
    fn migration_requires_exclusive_scheduler_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owned.sqlite");
        let (_canonical, old_lock) = crate::owner::acquire_owner_lock(&path).unwrap();
        assert!(matches!(
            Store::open(&path),
            Err(Error::MigrationOwnerUnavailable(_))
        ));
        assert!(
            !path.exists(),
            "failed migration must not create the database"
        );
        drop(old_lock);
        let store = Store::open(&path).unwrap();
        assert!(store.get(&global_revision()).unwrap().is_none());
    }

    #[test]
    fn persisted_protocol_fences_legacy_connections_and_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.sqlite");
        let legacy = rusqlite::Connection::open(&path).unwrap();
        legacy.execute_batch("CREATE TABLE durable_store_entries (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL, version INTEGER NOT NULL) WITHOUT ROWID;").unwrap();
        let mut prepared = legacy
            .prepare("INSERT INTO durable_store_entries(key,value,version) VALUES(?1,?2,1)")
            .unwrap();
        let store = Store::open(&path).unwrap();
        let id = next_uuid();
        let key = task_key(id);
        let bytes = task(id, json!({"Conversation":next_uuid()}));
        let old_write = prepared.execute(rusqlite::params![&key, &bytes]);
        assert!(
            old_write.is_err(),
            "legacy connection must not bypass the graph protocol"
        );
        store.commit(&[], &[put(key.clone(), bytes)]).unwrap();
        drop(store);
        drop(prepared);
        drop(legacy);
        let reopened = Store::open(&path).unwrap();
        assert!(reopened.get(&key).unwrap().is_some());
        let other = rusqlite::Connection::open(&path).unwrap();
        assert!(other
            .execute("DELETE FROM durable_store_entries WHERE key=?1", [&key])
            .is_err());
    }

    #[test]
    fn raw_task_lifecycle_advances_only_affected_ancestry() {
        let store = Store::memory().unwrap();
        let a = next_uuid();
        let b = next_uuid();
        let c = next_uuid();
        let ca = next_uuid();
        let cb = next_uuid();
        store
            .commit(
                &[],
                &[
                    put(task_key(a), task(a, json!({"Conversation":ca}))),
                    put(task_key(b), task(b, json!({"Conversation":cb}))),
                ],
            )
            .unwrap();
        let av = version(&store, &task_revision(a)).unwrap();
        let bv = version(&store, &task_revision(b)).unwrap();
        let global = version(&store, &global_revision());
        store
            .commit(&[], &[put(task_key(c), task(c, json!({"Task":a})))])
            .unwrap();
        assert_ne!(version(&store, &task_revision(a)), Some(av));
        assert_eq!(version(&store, &task_revision(b)), Some(bv));
        assert_eq!(version(&store, &global_revision()), global);
        let av2 = version(&store, &task_revision(a));
        store
            .commit(&[], &[Mutation::Delete { key: task_key(c) }])
            .unwrap();
        assert_ne!(version(&store, &task_revision(a)), av2);
        assert_eq!(version(&store, &task_revision(b)), Some(bv));
    }

    #[test]
    fn reparent_touches_old_and_new_owners_and_invalid_values_fence_globally() {
        let store = Store::memory().unwrap();
        let a = next_uuid();
        let b = next_uuid();
        let c = next_uuid();
        store
            .commit(
                &[],
                &[
                    put(task_key(a), task(a, json!({"Conversation":next_uuid()}))),
                    put(task_key(b), task(b, json!({"Conversation":next_uuid()}))),
                    put(task_key(c), task(c, json!({"Task":a}))),
                ],
            )
            .unwrap();
        let av = version(&store, &task_revision(a));
        let bv = version(&store, &task_revision(b));
        store
            .commit(&[], &[put(task_key(c), task(c, json!({"Task":b})))])
            .unwrap();
        assert_ne!(version(&store, &task_revision(a)), av);
        assert_ne!(version(&store, &task_revision(b)), bv);
        let before = version(&store, &global_revision());
        store
            .commit(&[], &[put(task_key(c), b"invalid".to_vec())])
            .unwrap();
        assert_ne!(version(&store, &global_revision()), before);
    }
}
