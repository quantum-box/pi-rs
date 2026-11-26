//! Rehearse migration from a standard or v3 store to v4 on independent SQLite files.

#[cfg(unix)]
mod unix {
    use std::{error::Error, fs, os::unix::fs::DirBuilderExt, path::Path, time::Instant};

    use durable_store::{Check, Expected, Mutation, Store};
    use rusqlite::{functions::FunctionFlags, params, Connection, OpenFlags, OptionalExtension};
    use serde::Serialize;
    use sha2::{Digest, Sha256};

    type Result<T> = std::result::Result<T, Box<dyn Error>>;

    #[derive(Debug, Default, PartialEq, Eq, Serialize)]
    struct Counts {
        rows: u64,
        conversations: u64,
        tasks: u64,
        documents: u64,
    }

    #[derive(Serialize)]
    pub(super) struct Report {
        source_graph_protocol: Option<i64>,
        cutoff_rows: Counts,
        backup_bytes: u64,
        backup_ms: u128,
        migrate_and_reopen_ms: u128,
        restore_and_verify_ms: u128,
    }

    fn open_read_only(path: &Path) -> Result<Connection> {
        Ok(Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?)
    }

    fn protocol(connection: &Connection) -> Result<Option<i64>> {
        let has_protocol: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='durable_graph_protocol')",
            [],
            |row| row.get(0),
        )?;
        if !has_protocol {
            return Ok(None);
        }
        Ok(Some(connection.query_row(
            "SELECT version FROM durable_graph_protocol WHERE id=1",
            [],
            |row| row.get(0),
        )?))
    }

    fn check_integrity(connection: &Connection) -> Result<()> {
        let mut statement = connection.prepare("PRAGMA integrity_check")?;
        let mut rows = statement.query([])?;
        let result: String = rows
            .next()?
            .ok_or("missing integrity check result")?
            .get(0)?;
        if result != "ok" || rows.next()?.is_some() {
            return Err("SQLite integrity check failed".into());
        }
        Ok(())
    }

    // Compare the whole storage cut without exposing keys or payloads in the report.
    fn fingerprint(connection: &Connection) -> Result<(Counts, [u8; 32])> {
        let mut statement = connection
            .prepare("SELECT key,value,version FROM durable_store_entries ORDER BY key")?;
        let mut rows = statement.query([])?;
        let mut counts = Counts::default();
        let mut digest = Sha256::new();
        let put_version: u64 = connection.query_row(
            "SELECT version FROM durable_store_meta WHERE id=1",
            [],
            |row| row.get(0),
        )?;
        digest.update(put_version.to_be_bytes());
        while let Some(row) = rows.next()? {
            let key = row.get_ref(0)?.as_blob()?;
            let value = row.get_ref(1)?.as_blob()?;
            let version: u64 = row.get(2)?;
            counts.rows += 1;
            counts.conversations += u64::from(key.starts_with(b"runtime/conversation/"));
            counts.tasks += u64::from(key.starts_with(b"durable/task/"));
            counts.documents += u64::from(key.starts_with(b"durable/document/"));
            for bytes in [key, value] {
                digest.update((bytes.len() as u64).to_be_bytes());
                digest.update(bytes);
            }
            digest.update(version.to_be_bytes());
        }
        Ok((counts, digest.finalize().into()))
    }

    // An older source may not have all these tables. Compare the data in the
    // tables that existed at the cut, allowing initialization to add new ones.
    const HISTORY_TABLES: &[(&str, &str)] = &[
        ("durable_commit_meta", "SELECT id,sequence,legacy_version FROM durable_commit_meta ORDER BY id"),
        ("durable_commit_versions", "SELECT key,version,sequence FROM durable_commit_versions ORDER BY key,version"),
        ("durable_document_history_meta", "SELECT id,revision FROM durable_document_history_meta ORDER BY id"),
        ("durable_document_history", "SELECT key,revision,value,version FROM durable_document_history ORDER BY key,revision"),
        ("durable_document_cuts", "SELECT key,revision FROM durable_document_cuts ORDER BY key"),
        ("durable_document_cut_versions", "SELECT key,version,revision FROM durable_document_cut_versions ORDER BY key,version"),
        ("durable_document_fence_versions", "SELECT key,version,revision FROM durable_document_fence_versions ORDER BY key,version"),
        ("durable_application_history_meta", "SELECT id,first_revision FROM durable_application_history_meta ORDER BY id"),
        ("durable_document_commit_meta", "SELECT id,boundary_revision,boundary_sequence FROM durable_document_commit_meta ORDER BY id"),
        ("durable_document_commit_sequences", "SELECT revision,sequence FROM durable_document_commit_sequences ORDER BY revision"),
        ("durable_task_document_owners", "SELECT document_key,owner FROM durable_task_document_owners ORDER BY document_key"),
    ];

    fn historical_fingerprint(connection: &Connection, query: &str) -> Result<[u8; 32]> {
        use rusqlite::types::ValueRef;

        let mut statement = connection.prepare(query)?;
        let columns = statement.column_count();
        let mut rows = statement.query([])?;
        let mut digest = Sha256::new();
        while let Some(row) = rows.next()? {
            for index in 0..columns {
                match row.get_ref(index)? {
                    ValueRef::Null => digest.update([0]),
                    ValueRef::Integer(value) => {
                        digest.update([1]);
                        digest.update(value.to_be_bytes());
                    }
                    ValueRef::Blob(value) => {
                        digest.update([2]);
                        digest.update((value.len() as u64).to_be_bytes());
                        digest.update(value);
                    }
                    _ => return Err("unexpected Durable history column type".into()),
                }
            }
        }
        Ok(digest.finalize().into())
    }

    #[derive(Clone)]
    pub(super) struct HistoryFingerprint {
        table: &'static str,
        query: &'static str,
        digest: [u8; 32],
    }

    pub(super) fn capture_history(connection: &Connection) -> Result<Vec<HistoryFingerprint>> {
        let mut fingerprints = Vec::new();
        for &(table, query) in HISTORY_TABLES {
            if table_exists(connection, table)? {
                fingerprints.push(HistoryFingerprint {
                    table,
                    query,
                    digest: historical_fingerprint(connection, query)?,
                });
            }
        }
        Ok(fingerprints)
    }

    fn table_exists(connection: &Connection, table: &str) -> Result<bool> {
        Ok(connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
            [table],
            |row| row.get(0),
        )?)
    }

    pub(super) fn expected_migrated_history(
        backup: &Connection,
        history: &[HistoryFingerprint],
    ) -> Result<Vec<HistoryFingerprint>> {
        let mut migrated = history.to_vec();
        let has_boundary = table_exists(backup, "durable_application_history_meta")?
            && backup.query_row(
                "SELECT EXISTS(SELECT 1 FROM durable_application_history_meta WHERE id=1)",
                [],
                |row| row.get::<_, bool>(0),
            )?;
        if has_boundary || !table_exists(backup, "durable_document_history_meta")? {
            return Ok(migrated);
        }

        // The first open adds one boundary and seeds only existing live
        // application logs. Reproduce the expected rows on the read-only
        // backup, so old rows and the new seed must both match in full.
        for item in &mut migrated {
            let query = match item.table {
                "durable_document_history_meta" => "SELECT id,revision+1 FROM durable_document_history_meta ORDER BY id",
                "durable_document_history" => "
                    SELECT key,revision,value,version FROM durable_document_history
                    UNION ALL
                    SELECT key,(SELECT revision+1 FROM durable_document_history_meta WHERE id=1),value,version
                    FROM durable_store_entries
                    WHERE substr(key,1,28)=CAST('durable/application-entries/' AS BLOB)
                    ORDER BY key,revision",
                _ => continue,
            };
            item.digest = historical_fingerprint(backup, query)?;
        }
        let boundary = HistoryFingerprint {
            table: "durable_application_history_meta",
            query: "SELECT id,first_revision FROM durable_application_history_meta ORDER BY id",
            digest: historical_fingerprint(
                backup,
                "SELECT id,revision+1 FROM durable_document_history_meta WHERE id=1",
            )?,
        };
        migrated.retain(|item| item.table != boundary.table);
        migrated.push(boundary);
        Ok(migrated)
    }

    pub(super) fn verify_history(
        connection: &Connection,
        expected: &[HistoryFingerprint],
    ) -> Result<()> {
        for snapshot in expected {
            if historical_fingerprint(connection, snapshot.query)? != snapshot.digest {
                return Err(format!("{} differs from the backup cut", snapshot.table).into());
            }
        }
        Ok(())
    }

    fn open_legacy_connection(path: &Path) -> Result<Connection> {
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        connection.create_scalar_function(
            "pi_graph_protocol_v3",
            0,
            FunctionFlags::SQLITE_UTF8,
            |_| Ok(1i64),
        )?;
        Ok(connection)
    }

    /// Take a consistent input snapshot without opening the input for writing.
    /// Keep the three independent output copies for operator inspection.
    pub(super) fn run(input: &Path, directory: &Path) -> Result<Report> {
        let input = open_read_only(input)?;
        let source_graph_protocol = protocol(&input)?;
        let (backup_name, restored_name) = match source_graph_protocol {
            None => ("backup-default.sqlite", "restored-default.sqlite"),
            Some(3) => ("backup-v3.sqlite", "restored-v3.sqlite"),
            Some(_) => return Err("expected a standard or v3 Durable SQLite source".into()),
        };
        input.query_row(
            "SELECT version FROM durable_store_meta WHERE id=1",
            [],
            |row| row.get::<_, u64>(0),
        )?;
        input.prepare("SELECT key,value,version FROM durable_store_entries")?;
        fs::DirBuilder::new().mode(0o700).create(directory)?;
        let backup = directory.join(backup_name);
        let migrated = directory.join("migrated-v4.sqlite");
        let restored = directory.join(restored_name);

        let started = Instant::now();
        input.execute(
            "VACUUM INTO ?1",
            [backup.to_str().ok_or("backup path must be UTF-8")?],
        )?;
        let backup_ms = started.elapsed().as_millis();
        let backup_bytes = fs::metadata(&backup)?.len();
        let baseline = open_read_only(&backup)?;
        if protocol(&baseline)? != source_graph_protocol {
            return Err("source protocol changed before the backup cut".into());
        }
        check_integrity(&baseline)?;
        let snapshot = fingerprint(&baseline)?;
        let history = capture_history(&baseline)?;
        let migrated_history = expected_migrated_history(&baseline, &history)?;
        drop(baseline);

        fs::copy(&backup, &migrated)?;
        let legacy = open_legacy_connection(&migrated)?;
        let started = Instant::now();
        let store = Store::open(&migrated)?;
        drop(store);
        let store = Store::open(&migrated)?;
        let migrate_and_reopen_ms = started.elapsed().as_millis();
        let migrated_read = open_read_only(&migrated)?;
        if protocol(&migrated_read)? != Some(4) || fingerprint(&migrated_read)? != snapshot {
            return Err("the reopened v4 store differs from the backup cut".into());
        }
        verify_history(&migrated_read, &migrated_history)?;
        check_integrity(&migrated_read)?;
        drop(migrated_read);

        let mut probe =
            format!("verification/graph-rehearsal/{}", store.put_version()?).into_bytes();
        while store.get(&probe)?.is_some() {
            probe.push(b'/');
        }
        // The legacy connection can write the source format and lacks the v4 write gate.
        let rejected = legacy.execute(
            "INSERT INTO durable_store_entries(key,value,version) VALUES (?1,X'31',?2)",
            params![
                probe,
                store
                    .put_version()?
                    .checked_add(1)
                    .ok_or("store version exhausted")?
            ],
        );
        if !matches!(rejected, Err(rusqlite::Error::SqliteFailure(_, Some(ref message))) if message.contains("pi_graph_protocol_v4"))
        {
            return Err("stale legacy write was not rejected by the v4 guard".into());
        }
        drop(legacy);
        store.commit(
            &[Check {
                key: probe.clone(),
                expected: Expected::Missing,
            }],
            &[Mutation::Put {
                key: probe.clone(),
                value: b"after migration".to_vec(),
            }],
        )?;
        drop(store);

        let started = Instant::now();
        fs::copy(&backup, &restored)?;
        let mut restored = open_legacy_connection(&restored)?;
        check_integrity(&restored)?;
        let probe_exists = restored
            .query_row(
                "SELECT 1 FROM durable_store_entries WHERE key=?1",
                [&probe],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if protocol(&restored)? != source_graph_protocol
            || fingerprint(&restored)? != snapshot
            || probe_exists
        {
            return Err("restoration did not return to the backup cut".into());
        }
        verify_history(&restored, &history)?;
        let transaction = restored.transaction()?;
        transaction.execute(
            "INSERT INTO durable_store_entries(key,value,version)
             SELECT ?1,X'31',version+1 FROM durable_store_meta WHERE id=1",
            [&probe],
        )?;
        transaction.rollback()?;
        let restore_and_verify_ms = started.elapsed().as_millis();

        Ok(Report {
            source_graph_protocol,
            cutoff_rows: snapshot.0,
            backup_bytes,
            backup_ms,
            migrate_and_reopen_ms,
            restore_and_verify_ms,
        })
    }
}

#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.as_slice() == ["--help"] {
        println!("Usage: durable-graph-rehearsal SOURCE.sqlite NEW_OUTPUT_DIRECTORY");
        return Ok(());
    }
    let [source, directory] = args.as_slice() else {
        return Err("usage: durable-graph-rehearsal SOURCE.sqlite NEW_OUTPUT_DIRECTORY".into());
    };
    let result = unix::run(source.as_ref(), directory.as_ref())?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

#[cfg(not(unix))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err("Durable graph migration requires Unix store ownership".into())
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::unix::{capture_history, expected_migrated_history, run, verify_history};
    use rusqlite::{params, Connection};

    fn verify_rehearsal(v3: bool, backup_name: &str, restore_name: &str) {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.sqlite");
        let output = dir.path().join("report");
        {
            let connection = Connection::open(&source).unwrap();
            connection.execute_batch(
                "CREATE TABLE durable_store_meta(id INTEGER PRIMARY KEY, version INTEGER NOT NULL);
                 INSERT INTO durable_store_meta VALUES(1,1);
                 CREATE TABLE durable_store_entries (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL, version INTEGER NOT NULL CHECK(version>0)) WITHOUT ROWID;",
            ).unwrap();
            connection
                .execute(
                    "INSERT INTO durable_store_entries VALUES (?1, ?2, 1)",
                    params![b"fixture/original".as_slice(), b"fixture value".as_slice()],
                )
                .unwrap();
            if v3 {
                connection.execute_batch(
                    "CREATE TABLE durable_graph_protocol (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL CHECK(version=3));
                     INSERT INTO durable_graph_protocol VALUES(1,3);",
                ).unwrap();
                for (name, operation) in [
                    ("insert", "INSERT"),
                    ("update", "UPDATE"),
                    ("delete", "DELETE"),
                ] {
                    connection.execute_batch(&format!(
                        "CREATE TRIGGER durable_graph_protocol_{name} BEFORE {operation} ON durable_store_entries
                         BEGIN SELECT CASE WHEN pi_graph_protocol_v3() != 1 THEN RAISE(ABORT, 'graph protocol required') END; END;",
                    )).unwrap();
                }
            }
        }
        let original = std::fs::read(&source).unwrap();
        let report = serde_json::to_value(run(&source, &output).unwrap()).unwrap();
        assert_eq!(report["cutoff_rows"]["rows"], 1);
        assert_eq!(output.metadata().unwrap().permissions().mode() & 0o077, 0);
        assert_eq!(
            report["source_graph_protocol"],
            serde_json::json!(v3.then_some(3)),
        );
        for file in [backup_name, "migrated-v4.sqlite", restore_name] {
            assert!(output.join(file).is_file());
        }
        assert!(run(&source, &output).is_err());
        let rejected = dir.path().join("rejected");
        assert!(run(&output.join("migrated-v4.sqlite"), &rejected).is_err());
        assert!(!rejected.exists());
        assert_eq!(std::fs::read(source).unwrap(), original);
    }

    #[test]
    fn rehearsal_detects_changed_historical_document_values() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE durable_document_history(key BLOB,revision INTEGER,value BLOB,version INTEGER);
             INSERT INTO durable_document_history VALUES(X'01',1,X'02',1),(X'01',2,X'03',2);
             INSERT INTO durable_document_history VALUES(X'04',2,NULL,NULL);",
        )
        .unwrap();
        let expected = capture_history(&db).unwrap();
        assert_eq!(expected.len(), 1);
        verify_history(&db, &expected).unwrap();
        db.execute(
            "UPDATE durable_document_history SET value=X'ff' WHERE revision=1",
            [],
        )
        .unwrap();
        assert!(verify_history(&db, &expected).is_err());
    }

    #[test]
    fn rehearsal_initializes_legacy_application_history_without_losing_old_rows() {
        for has_empty_marker in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("legacy.sqlite");
            let output = dir.path().join("report");
            let connection = Connection::open(&source).unwrap();
            connection.execute_batch("
                CREATE TABLE durable_store_meta(id INTEGER PRIMARY KEY, version INTEGER NOT NULL);
                INSERT INTO durable_store_meta VALUES(1,3);
                CREATE TABLE durable_store_entries (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL, version INTEGER NOT NULL CHECK(version>0)) WITHOUT ROWID;
                INSERT INTO durable_store_entries VALUES(CAST('durable/application-entries/fixture' AS BLOB),X'5b5d',3);
                CREATE TABLE durable_document_history_meta(id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL CHECK(revision>=0));
                INSERT INTO durable_document_history_meta VALUES(1,2);
                CREATE TABLE durable_document_history(key BLOB NOT NULL,revision INTEGER NOT NULL,value BLOB,version INTEGER,PRIMARY KEY(key,revision)) WITHOUT ROWID;
                INSERT INTO durable_document_history VALUES(CAST('durable/document/fixture' AS BLOB),1,X'7b7d',1);
                INSERT INTO durable_document_history VALUES(CAST('durable/document/fixture' AS BLOB),2,NULL,NULL);
            ").unwrap();
            if has_empty_marker {
                connection.execute_batch("
                    CREATE TABLE durable_application_history_meta (id INTEGER PRIMARY KEY CHECK(id=1), first_revision INTEGER NOT NULL);
                ").unwrap();
            }
            drop(connection);
            let original = std::fs::read(&source).unwrap();

            run(&source, &output).unwrap();
            let backup = Connection::open(output.join("backup-default.sqlite")).unwrap();
            let original_history = capture_history(&backup).unwrap();
            let expected = expected_migrated_history(&backup, &original_history).unwrap();
            let migrated = Connection::open(output.join("migrated-v4.sqlite")).unwrap();
            verify_history(&migrated, &expected).unwrap();
            let seed: (i64, Vec<u8>, i64) = migrated.query_row(
                "SELECT revision,value,version FROM durable_document_history WHERE key=CAST('durable/application-entries/fixture' AS BLOB)",
                [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            ).unwrap();
            assert_eq!(seed, (3, b"[]".to_vec(), 3));
            let restored = Connection::open(output.join("restored-default.sqlite")).unwrap();
            verify_history(&restored, &original_history).unwrap();

            migrated
                .execute(
                    "UPDATE durable_document_history SET value=X'ff' WHERE revision=3",
                    [],
                )
                .unwrap();
            assert!(verify_history(&migrated, &expected).is_err());
            migrated
                .execute(
                    "UPDATE durable_document_history SET value=X'5b5d' WHERE revision=3",
                    [],
                )
                .unwrap();
            verify_history(&migrated, &expected).unwrap();
            migrated
                .execute("DELETE FROM durable_document_history WHERE revision=1", [])
                .unwrap();
            assert!(verify_history(&migrated, &expected).is_err());
            assert_eq!(std::fs::read(&source).unwrap(), original);
        }
    }

    #[test]
    fn rehearsal_rejects_an_unrelated_sqlite_database_before_copying_it() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("unrelated.sqlite");
        let output = dir.path().join("report");
        Connection::open(&source)
            .unwrap()
            .execute_batch("CREATE TABLE unrelated (value TEXT)")
            .unwrap();

        assert!(run(&source, &output).is_err());
        assert!(!output.exists());
    }

    #[test]
    fn standard_rehearsal_restores_the_unversioned_source_format() {
        verify_rehearsal(false, "backup-default.sqlite", "restored-default.sqlite");
    }

    #[test]
    fn v3_rehearsal_uses_separate_files_and_rejects_reusing_output() {
        verify_rehearsal(true, "backup-v3.sqlite", "restored-v3.sqlite");
    }
}
