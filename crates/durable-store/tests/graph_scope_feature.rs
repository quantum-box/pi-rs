#![cfg(feature = "experimental-graph-scope")]

#[test]
fn new_feature_database_only_installs_bounded_v2_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("feature.sqlite");
    let _store = durable_store::Store::open(&path).unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    let old: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name LIKE 'durable_graph_mutation_%')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let new: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='durable_graph_history_v2_prune')", [], |r| r.get(0)).unwrap();
    assert!(!old);
    assert!(new);
}
