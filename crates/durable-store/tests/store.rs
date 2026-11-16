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

#[tokio::test]
async fn commit_updates_follow_committed_batches_and_reopen() {
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let store = Store::open(&path).unwrap();
    let mut updates = store.subscribe_commits().unwrap();
    let initial = updates.current();
    store.commit(&[], &[put(b"a", b"one")]).unwrap();
    assert_eq!(updates.changed().await.unwrap(), initial + 1);
    assert!(store
        .commit(&[check(b"a", Expected::Missing)], &[put(b"b", b"bad")])
        .is_err());
    store.commit(&[], &[]).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), updates.changed())
            .await
            .is_err()
    );
    store
        .commit(&[], &[Mutation::Delete { key: b"a".to_vec() }])
        .unwrap();
    let deleted = updates.changed().await.unwrap();
    assert_eq!(deleted, initial + 2);
    drop(store);
    assert!(updates.changed().await.is_err());
    let reopened = Store::open(&path).unwrap();
    let mut resumed = reopened.subscribe_commits().unwrap();
    assert_eq!(resumed.current(), deleted);
    reopened.commit(&[], &[put(b"c", b"ok")]).unwrap();
    assert_eq!(resumed.changed().await.unwrap(), deleted + 1);
}

#[tokio::test]
async fn commit_subscription_starts_from_current_cross_connection_state() {
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let first = Store::open(&path).unwrap();
    let second = Store::open(&path).unwrap();
    let mut existing = first.subscribe_commits().unwrap();
    let before = existing.current();
    second.commit(&[], &[put(b"other", b"value")]).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), existing.changed())
            .await
            .is_err()
    );
    let mut updates = first.subscribe_commits().unwrap();
    assert_eq!(updates.current(), second.commit_sequence().unwrap());
    assert!(updates.current() > before);
    assert_eq!(existing.changed().await.unwrap(), updates.current());
    first.commit(&[], &[put(b"local", b"value")]).unwrap();
    assert_eq!(
        updates.changed().await.unwrap(),
        first.commit_sequence().unwrap()
    );
}

#[tokio::test]
async fn commit_subscription_cannot_miss_a_concurrent_local_commit() {
    use std::time::Duration;
    let store = Arc::new(Store::memory().unwrap());
    for n in 0_u32..32 {
        let boundary = Arc::new(Barrier::new(2));
        let worker_store = store.clone();
        let worker_boundary = boundary.clone();
        let worker = std::thread::spawn(move || {
            worker_boundary.wait();
            worker_store
                .commit(&[], &[put(b"race", &n.to_le_bytes())])
                .unwrap();
        });
        boundary.wait();
        let mut updates = store.subscribe_commits().unwrap();
        let observed = updates.current();
        worker.join().unwrap();
        let committed = store.commit_sequence().unwrap();
        if observed != committed {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), updates.changed())
                    .await
                    .unwrap()
                    .unwrap(),
                committed
            );
        }
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

#[test]
fn bounded_page_respects_binary_prefix_and_cursor() {
    let store = Store::memory().unwrap();
    store
        .commit(
            &[],
            &[
                put(b"head", b"three"),
                put(b"a\x00", b"zero"),
                put(b"a\xff", b"ff"),
                put(b"b", b"other"),
            ],
        )
        .unwrap();
    let (head, page) = store.page_with_metadata(b"head", b"a", b"a", 1).unwrap();
    assert_eq!(head.unwrap().value, b"three");
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].0, b"a\x00");
    let (_, next) = store
        .page_with_metadata(b"head", b"a", &page[0].0, 2)
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].0, b"a\xff");
    let (_, none) = store
        .page_with_metadata(b"head", b"a", &next[0].0, 2)
        .unwrap();
    assert!(none.is_empty());
}

#[test]
fn commit_sequences_track_batches_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sequence.db");
    let store = Store::open(&path).unwrap();
    assert_eq!(store.legacy_version_boundary().unwrap(), 0);
    assert_eq!(store.commit_sequence().unwrap(), 0);
    store
        .commit(&[], &[put(b"a", b"1"), put(b"a", b"2"), put(b"b", b"3")])
        .unwrap();
    let a = store.get(b"a").unwrap().unwrap();
    let b = store.get(b"b").unwrap().unwrap();
    assert_eq!(
        store.commit_for_version(b"a", a.version - 1).unwrap(),
        Some(1)
    );
    assert_eq!(store.commit_for_version(b"a", a.version).unwrap(), Some(1));
    assert_eq!(store.commit_for_version(b"b", b.version).unwrap(), Some(1));
    assert_eq!(store.commit_for_version(b"b", a.version).unwrap(), None);
    store
        .commit(&[check(b"a", Expected::Version(a.version))], &[])
        .unwrap();
    assert!(matches!(
        store.commit(&[check(b"a", Expected::Missing)], &[put(b"x", b"no")]),
        Err(Error::Conflict { .. })
    ));
    assert_eq!(store.commit_sequence().unwrap(), 1);
    store
        .commit(&[], &[Mutation::Delete { key: b"a".into() }])
        .unwrap();
    assert_eq!(store.commit_sequence().unwrap(), 2);
    store.commit(&[], &[put(b"a", b"new")]).unwrap();
    let new = store.get(b"a").unwrap().unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    assert_eq!(store.commit_sequence().unwrap(), 3);
    assert_eq!(store.commit_for_version(b"a", a.version).unwrap(), Some(1));
    assert_eq!(
        store.commit_for_version(b"a", new.version).unwrap(),
        Some(3)
    );
}

#[test]
fn legacy_versions_have_an_explicit_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("CREATE TABLE durable_store_meta (id INTEGER PRIMARY KEY, version INTEGER NOT NULL); INSERT INTO durable_store_meta VALUES(1, 7); CREATE TABLE durable_store_entries (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL, version INTEGER NOT NULL) WITHOUT ROWID; INSERT INTO durable_store_entries VALUES(x'61',x'62',7);").unwrap();
    drop(conn);
    let store = Store::open(&path).unwrap();
    assert_eq!(store.legacy_version_boundary().unwrap(), 7);
    assert!(matches!(
        store.commit(&[check(b"missing", Expected::MissingUnchangedSince(6))], &[]),
        Err(Error::InvalidSnapshot)
    ));
    store.commit(&[check(b"missing", Expected::MissingUnchangedSince(7))], &[]).unwrap();
    assert_eq!(store.commit_for_version(b"a", 7).unwrap(), None);
    store.commit(&[], &[put(b"a", b"new")]).unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    assert_eq!(store.legacy_version_boundary().unwrap(), 7);
    assert_eq!(store.commit_for_version(b"a", 8).unwrap(), Some(1));
}

#[test]
fn concurrent_writers_allocate_distinct_commit_sequences() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("concurrent.db");
    let stores = [Store::open(&path).unwrap(), Store::open(&path).unwrap()];
    let barrier = Arc::new(Barrier::new(2));
    let threads: Vec<_> = stores
        .into_iter()
        .enumerate()
        .map(|(i, store)| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                for n in 0..30 {
                    store.commit(&[], &[put(&[i as u8, n], b"value")]).unwrap();
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let store = Store::open(&path).unwrap();
    assert_eq!(store.commit_sequence().unwrap(), 60);
    let mut sequences: Vec<_> = store
        .list(b"")
        .unwrap()
        .into_iter()
        .map(|(key, entry)| {
            store
                .commit_for_version(&key, entry.version)
                .unwrap()
                .unwrap()
        })
        .collect();
    sequences.sort_unstable();
    assert_eq!(sequences, (1..=60).collect::<Vec<_>>());
}

#[test]
fn failed_write_rolls_back_sequence_and_version_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rollback.db");
    let store = Store::open(&path).unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("CREATE TRIGGER reject_second BEFORE INSERT ON durable_commit_versions WHEN NEW.key=x'62' BEGIN SELECT RAISE(ABORT, 'rejected'); END;").unwrap();
    assert!(store
        .commit(&[], &[put(b"a", b"1"), put(b"b", b"2")])
        .is_err());
    assert_eq!(store.commit_sequence().unwrap(), 0);
    assert_eq!(store.commit_for_version(b"a", 1).unwrap(), None);
    assert!(store.get(b"a").unwrap().is_none());
    conn.execute_batch("DROP TRIGGER reject_second;").unwrap();
    store.commit(&[], &[put(b"a", b"ok")]).unwrap();
    assert_eq!(store.commit_for_version(b"a", 1).unwrap(), Some(1));
    assert_eq!(store.commit_for_version(b"a", u64::MAX).unwrap(), None);
}

#[test]
fn migration_failure_rolls_back_and_retries_with_legacy_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("migration.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("CREATE TABLE durable_store_meta (id INTEGER PRIMARY KEY, version INTEGER NOT NULL); INSERT INTO durable_store_meta VALUES(1, 7); CREATE TABLE durable_store_entries (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL, version INTEGER NOT NULL) WITHOUT ROWID; INSERT INTO durable_store_entries VALUES(x'61',x'62',7); CREATE TABLE durable_commit_meta (id INTEGER PRIMARY KEY, sequence INTEGER NOT NULL, legacy_version INTEGER NOT NULL); CREATE TRIGGER reject_migration BEFORE INSERT ON durable_commit_meta BEGIN SELECT RAISE(ABORT, 'fail migration'); END;").unwrap();
    assert!(Store::open(&path).is_err());
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='durable_commit_versions'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
    conn.execute_batch("DROP TRIGGER reject_migration").unwrap();
    drop(conn);
    let store = Store::open(&path).unwrap();
    assert_eq!(store.legacy_version_boundary().unwrap(), 7);
    assert_eq!(store.commit_sequence().unwrap(), 0);
    assert_eq!(store.get(b"a").unwrap().unwrap().version, 7);
}

#[test]
fn concurrent_opens_migrate_once_and_writers_share_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("open.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE durable_store_meta (id INTEGER PRIMARY KEY, version INTEGER NOT NULL); INSERT INTO durable_store_meta VALUES(1, 7); CREATE TABLE durable_store_entries (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL, version INTEGER NOT NULL) WITHOUT ROWID;").unwrap();
    drop(conn);
    let barrier = Arc::new(Barrier::new(2));
    let threads: Vec<_> = (0..2)
        .map(|i| {
            let path = path.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let store = Store::open(path).unwrap();
                store.commit(&[], &[put(&[i], b"value")]).unwrap();
                assert_eq!(store.legacy_version_boundary().unwrap(), 7);
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let store = Store::open(path).unwrap();
    assert_eq!(store.commit_sequence().unwrap(), 2);
    let mut sequences: Vec<_> = store
        .list(b"")
        .unwrap()
        .into_iter()
        .map(|(key, entry)| {
            store
                .commit_for_version(&key, entry.version)
                .unwrap()
                .unwrap()
        })
        .collect();
    sequences.sort();
    assert_eq!(sequences, [1, 2]);
}

#[test]
fn exhausted_sequence_rejects_mutations_but_not_checks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("exhausted.db");
    let store = Store::open(&path).unwrap();
    store.commit(&[], &[put(b"a", b"old")]).unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute("UPDATE durable_commit_meta SET sequence=?1", [i64::MAX])
        .unwrap();
    store.commit(&[], &[]).unwrap();
    store
        .commit(&[check(b"a", Expected::Version(1))], &[])
        .unwrap();
    assert!(matches!(
        store.commit(&[check(b"a", Expected::Missing)], &[]),
        Err(Error::Conflict { .. })
    ));
    assert!(matches!(
        store.commit(&[], &[Mutation::Delete { key: b"a".into() }]),
        Err(Error::VersionExhausted)
    ));
    assert_eq!(store.get(b"a").unwrap().unwrap().value, b"old");
    assert_eq!(store.commit_sequence().unwrap(), i64::MAX as u64);
    assert_eq!(store.commit_for_version(b"a", 1).unwrap(), Some(1));
}

#[test]
fn version_exhaustion_rolls_back_sequence_and_earlier_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("versions.db");
    let store = Store::open(&path).unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute("UPDATE durable_store_meta SET version=?1", [i64::MAX - 1])
        .unwrap();
    assert!(matches!(
        store.commit(&[], &[put(b"a", b"one"), put(b"b", b"two")]),
        Err(Error::VersionExhausted)
    ));
    assert_eq!(store.commit_sequence().unwrap(), 0);
    assert!(store.get(b"a").unwrap().is_none());
    assert_eq!(
        store.commit_for_version(b"a", i64::MAX as u64).unwrap(),
        None
    );
    store.commit(&[], &[put(b"a", b"one")]).unwrap();
    assert_eq!(
        store.commit_for_version(b"a", i64::MAX as u64).unwrap(),
        Some(1)
    );
}

#[test]
fn repeated_mutations_and_missing_deletes_are_one_commit_each() {
    let store = Store::memory().unwrap();
    store
        .commit(
            &[],
            &[
                put(b"a", b"one"),
                Mutation::Delete { key: b"a".into() },
                put(b"a", b"two"),
                Mutation::Delete { key: b"a".into() },
            ],
        )
        .unwrap();
    assert!(store.get(b"a").unwrap().is_none());
    assert_eq!(store.commit_for_version(b"a", 1).unwrap(), Some(1));
    assert_eq!(store.commit_for_version(b"a", 2).unwrap(), Some(1));
    store
        .commit(&[], &[Mutation::Delete { key: b"a".into() }])
        .unwrap();
    assert_eq!(store.commit_sequence().unwrap(), 2);
    store.commit(&[], &[put(b"a", b"three")]).unwrap();
    assert_eq!(store.commit_for_version(b"a", 3).unwrap(), Some(3));
}

#[test]
fn sequence_crash_child() {
    let Ok(path) = std::env::var("DURABLE_SEQUENCE_CRASH_DB") else {
        return;
    };
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch("BEGIN IMMEDIATE; UPDATE durable_commit_meta SET sequence=2; UPDATE durable_store_meta SET version=2; INSERT INTO durable_store_entries VALUES(x'62',x'76',2); INSERT INTO durable_commit_versions VALUES(x'62',2,2);").unwrap();
    std::process::exit(0);
}

#[test]
fn process_exit_rolls_back_sequence_and_mappings() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("crash.db");
    let store = Store::open(&path).unwrap();
    store.commit(&[], &[put(b"a", b"stable")]).unwrap();
    drop(store);
    assert!(std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("sequence_crash_child")
        .env("DURABLE_SEQUENCE_CRASH_DB", &path)
        .status()
        .unwrap()
        .success());
    let store = Store::open(&path).unwrap();
    assert_eq!(store.commit_sequence().unwrap(), 1);
    assert_eq!(store.commit_for_version(b"a", 1).unwrap(), Some(1));
    assert_eq!(store.commit_for_version(b"b", 2).unwrap(), None);
    assert!(store.get(b"b").unwrap().is_none());
    store.commit(&[], &[put(b"b", b"recovered")]).unwrap();
    assert_eq!(store.commit_for_version(b"b", 2).unwrap(), Some(2));
}

#[test]
fn arbitrary_legacy_owner_prefix_and_ordered_task_mutations_survive_reopen() {
    use durable_store::{Mutation, Store};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("arbitrary.db");
    let legacy = b"durable/task-document-owner/not-metadata".to_vec();
    let task = [b"durable/task/".as_slice(), &[42; 16]].concat();
    let lookalike = b"durable/task-document-owner/durable/document/[\"s\",\"n\",\"k\"]".to_vec();
    // Simulate a database written before the ownership table existed.
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE durable_store_meta (id INTEGER PRIMARY KEY, version INTEGER NOT NULL); INSERT INTO durable_store_meta VALUES (1, 2); CREATE TABLE durable_store_entries (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL, version INTEGER NOT NULL) WITHOUT ROWID;").unwrap();
        conn.execute(
            "INSERT INTO durable_store_entries VALUES (?1,?2,1)",
            rusqlite::params![&legacy, b"opaque".as_slice()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO durable_store_entries VALUES (?1,?2,2)",
            rusqlite::params![&lookalike, vec![42u8; 16]],
        )
        .unwrap();
    }
    let store = Store::open(&path).unwrap();
    store
        .commit(
            &[],
            &[
                Mutation::Put {
                    key: task.clone(),
                    value: b"first".to_vec(),
                },
                Mutation::Delete { key: task.clone() },
                Mutation::Put {
                    key: task.clone(),
                    value: b"last".to_vec(),
                },
            ],
        )
        .unwrap();
    let version = store.get(&task).unwrap().unwrap().version;
    drop(store);
    let store = Store::open(&path).unwrap();
    assert_eq!(store.get(&legacy).unwrap().unwrap().value, b"opaque");
    assert_eq!(store.get(&lookalike).unwrap().unwrap().value, vec![42; 16]);
    let entry = store.get(&task).unwrap().unwrap();
    assert_eq!(entry.value, b"last");
    assert_eq!(entry.version, version);
    store
        .commit(
            &[],
            &[Mutation::Delete {
                key: legacy.clone(),
            }],
        )
        .unwrap();
    assert!(store.get(&legacy).unwrap().is_none());
}
