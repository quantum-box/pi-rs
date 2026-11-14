use durable_store::{Check, Error, Expected, Mutation, Store};
use std::sync::{Arc, Barrier};

fn put(key: &[u8], value: &[u8]) -> Mutation {
    Mutation::Put {
        key: key.into(),
        value: value.into(),
    }
}
fn check(key: &[u8], expected: Expected) -> Check {
    Check {
        key: key.into(),
        expected,
    }
}
fn exercise(store: &Store) {
    store
        .commit(
            &[check(b"a", Expected::Missing)],
            &[
                put(b"a", b"one"),
                put(b"a\xff", b"two"),
                put(b"b", b"three"),
            ],
        )
        .unwrap();
    let first = store.get(b"a").unwrap().unwrap();
    assert_eq!(
        store
            .list(b"a")
            .unwrap()
            .iter()
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>(),
        vec![b"a".to_vec(), b"a\xff".to_vec()]
    );
    assert_eq!(store.list(b"").unwrap().len(), 3);
    assert!(matches!(
        store.commit(
            &[check(b"a", Expected::Version(first.version + 1))],
            &[put(b"b", b"bad"), Mutation::Delete { key: b"a".into() }]
        ),
        Err(Error::Conflict { .. })
    ));
    assert_eq!(store.get(b"b").unwrap().unwrap().value, b"three");
    assert_eq!(store.get(b"a").unwrap(), Some(first.clone()));
    store
        .commit(
            &[check(b"a", Expected::Version(first.version))],
            &[Mutation::Delete { key: b"a".into() }, put(b"a", b"new")],
        )
        .unwrap();
    assert!(store.get(b"a").unwrap().unwrap().version > first.version);
    assert!(matches!(
        store.commit(&[check(b"a", Expected::Version(first.version))], &[]),
        Err(Error::Conflict { .. })
    ));
}
#[test]
fn memory_semantics() {
    exercise(&Store::memory().unwrap());
}
#[test]
fn disk_semantics_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    {
        let store = Store::open(&path).unwrap();
        exercise(&store);
    }
    let store = Store::open(&path).unwrap();
    assert_eq!(store.get(b"a").unwrap().unwrap().value, b"new");
    assert_eq!(store.list(b"").unwrap().len(), 3);
}
#[test]
fn uncommitted_sqlite_transaction_is_rolled_back_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    {
        let store = Store::open(&path).unwrap();
        store.commit(&[], &[put(b"stable", b"yes")]).unwrap();
    }
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE; UPDATE durable_store_entries SET value=x'6e6f' WHERE key=x'737461626c65'; INSERT INTO durable_store_entries VALUES(x'6c6f7374',x'79',100);").unwrap();
        // Simulate a connection disappearing before COMMIT.
    }
    let store = Store::open(&path).unwrap();
    assert_eq!(store.get(b"stable").unwrap().unwrap().value, b"yes");
    assert!(store.get(b"lost").unwrap().is_none());
}
#[test]
fn concurrent_connections_compare_and_swap_only_one_wins() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let a = Store::open(&path).unwrap();
    let b = Store::open(&path).unwrap();
    a.commit(&[], &[put(b"counter", b"0")]).unwrap();
    let version = a.get(b"counter").unwrap().unwrap().version;
    let barrier = Arc::new(Barrier::new(2));
    let threads: Vec<_> = [a, b]
        .into_iter()
        .enumerate()
        .map(|(i, store)| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store.commit(
                    &[check(b"counter", Expected::Version(version))],
                    &[put(b"counter", &[i as u8]), put(&[i as u8], b"winner")],
                )
            })
        })
        .collect();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(Error::Conflict { .. })))
            .count(),
        1
    );
    let reopened = Store::open(&path).unwrap();
    assert_eq!(reopened.list(b"").unwrap().len(), 2);
}

#[test]
fn crash_recovery_child() {
    let Ok(path) = std::env::var("DURABLE_STORE_CRASH_DB") else {
        return;
    };
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch("BEGIN IMMEDIATE; UPDATE durable_store_entries SET value=x'62726f6b656e' WHERE key=x'737461626c65'; INSERT INTO durable_store_entries VALUES(x'6c6f7374',x'79',100);").unwrap();
    std::process::exit(0);
}
#[test]
fn process_exit_before_commit_recovers_original_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    {
        let store = Store::open(&path).unwrap();
        store.commit(&[], &[put(b"stable", b"yes")]).unwrap();
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("crash_recovery_child")
        .env("DURABLE_STORE_CRASH_DB", &path)
        .status()
        .unwrap();
    assert!(status.success());
    let store = Store::open(&path).unwrap();
    assert_eq!(store.get(b"stable").unwrap().unwrap().value, b"yes");
    assert!(store.get(b"lost").unwrap().is_none());
}
