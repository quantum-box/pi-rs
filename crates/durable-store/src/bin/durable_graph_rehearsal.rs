//! Rehearse v3-to-v4 migration and rollback on independent SQLite files.

#[cfg(unix)]
mod unix {
    use std::{error::Error, fs, path::Path, time::Instant};

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

    fn protocol(connection: &Connection) -> Result<i64> {
        Ok(connection.query_row(
            "SELECT version FROM durable_graph_protocol WHERE id=1",
            [],
            |row| row.get(0),
        )?)
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

    fn open_v3_connection(path: &Path) -> Result<Connection> {
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
        if protocol(&input)? != 3 {
            return Err("expected a v3 Durable SQLite source".into());
        }
        fs::create_dir(directory)?;
        let backup = directory.join("backup-v3.sqlite");
        let migrated = directory.join("migrated-v4.sqlite");
        let restored = directory.join("restored-v3.sqlite");

        let started = Instant::now();
        input.execute(
            "VACUUM INTO ?1",
            [backup.to_str().ok_or("backup path must be UTF-8")?],
        )?;
        let backup_ms = started.elapsed().as_millis();
        let backup_bytes = fs::metadata(&backup)?.len();
        let baseline = open_read_only(&backup)?;
        if protocol(&baseline)? != 3 {
            return Err("source protocol changed before the backup cut".into());
        }
        check_integrity(&baseline)?;
        let snapshot = fingerprint(&baseline)?;
        drop(baseline);

        fs::copy(&backup, &migrated)?;
        let legacy = open_v3_connection(&migrated)?;
        let started = Instant::now();
        let store = Store::open(&migrated)?;
        drop(store);
        let store = Store::open(&migrated)?;
        let migrate_and_reopen_ms = started.elapsed().as_millis();
        let migrated_read = open_read_only(&migrated)?;
        if protocol(&migrated_read)? != 4 || fingerprint(&migrated_read)? != snapshot {
            return Err("the reopened v4 store differs from the backup cut".into());
        }
        check_integrity(&migrated_read)?;
        drop(migrated_read);

        let mut probe =
            format!("verification/graph-rehearsal/{}", store.put_version()?).into_bytes();
        while store.get(&probe)?.is_some() {
            probe.push(b'/');
        }
        // A legacy connection has the v3 function but lacks the v4 write gate.
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
            return Err("stale v3 write was not rejected by the v4 guard".into());
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
        let mut restored = open_v3_connection(&restored)?;
        check_integrity(&restored)?;
        let probe_exists = restored
            .query_row(
                "SELECT 1 FROM durable_store_entries WHERE key=?1",
                [&probe],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if protocol(&restored)? != 3 || fingerprint(&restored)? != snapshot || probe_exists {
            return Err("restoration did not return to the backup cut".into());
        }
        let transaction = restored.transaction()?;
        transaction.execute(
            "INSERT INTO durable_store_entries(key,value,version)
             SELECT ?1,X'31',version+1 FROM durable_store_meta WHERE id=1",
            [&probe],
        )?;
        transaction.rollback()?;
        let restore_and_verify_ms = started.elapsed().as_millis();

        Ok(Report {
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
        println!("Usage: durable-graph-rehearsal V3_BACKUP.sqlite NEW_OUTPUT_DIRECTORY");
        return Ok(());
    }
    let [source, directory] = args.as_slice() else {
        return Err("usage: durable-graph-rehearsal V3_BACKUP.sqlite NEW_OUTPUT_DIRECTORY".into());
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
    use super::unix::run;
    use rusqlite::{params, Connection};

    #[test]
    fn rehearsal_uses_separate_files_and_rejects_reusing_output() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.sqlite");
        let output = dir.path().join("report");
        {
            let connection = Connection::open(&source).unwrap();
            connection.execute_batch(
                "CREATE TABLE durable_store_meta(id INTEGER PRIMARY KEY, version INTEGER NOT NULL);
                 INSERT INTO durable_store_meta VALUES(1,1);
                 CREATE TABLE durable_store_entries (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL, version INTEGER NOT NULL CHECK(version>0)) WITHOUT ROWID;
                 CREATE TABLE durable_graph_protocol (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL CHECK(version=3));
                 INSERT INTO durable_graph_protocol VALUES(1,3);",
            ).unwrap();
            connection
                .execute(
                    "INSERT INTO durable_store_entries VALUES (?1, ?2, 1)",
                    params![b"fixture/original".as_slice(), b"fixture value".as_slice()],
                )
                .unwrap();
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
        let original = std::fs::read(&source).unwrap();
        let report = serde_json::to_value(run(&source, &output).unwrap()).unwrap();
        assert_eq!(report["cutoff_rows"]["rows"], 1);
        for file in [
            "backup-v3.sqlite",
            "migrated-v4.sqlite",
            "restored-v3.sqlite",
        ] {
            assert!(output.join(file).is_file());
        }
        assert!(run(&source, &output).is_err());
        let rejected = dir.path().join("rejected");
        assert!(run(&output.join("migrated-v4.sqlite"), &rejected).is_err());
        assert!(!rejected.exists());
        assert_eq!(std::fs::read(source).unwrap(), original);
    }
}
