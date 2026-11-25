#![cfg(not(feature = "experimental-graph-scope"))]

#[test]
fn default_open_does_not_install_graph_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("default.sqlite");
    let _store = durable_store::Store::open(&path).unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    let installed: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name LIKE 'durable_graph_mutation_%' OR name LIKE 'durable_graph_history_v2%')",
        [],
        |row| row.get(0),
    ).unwrap();
    assert!(!installed);
}
