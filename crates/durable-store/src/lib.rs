//! Atomic, versioned byte-key storage for durable runtimes.
//!
//! Both backends serialize checks and writes in one transaction. SQLite uses
//! WAL and FULL synchronization; memory storage is intentionally ephemeral.
//! Versions are unique within a database, including across delete/recreate.
//! A storage error during commit can leave its outcome unknown: reopen and read
//! the database before deciding whether to retry a non-idempotent operation.

pub mod controls;
pub mod owner;

use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::{fmt, path::Path, sync::Mutex, time::Duration};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub value: Vec<u8>,
    pub version: u64,
}
pub type KeyedEntry = (Vec<u8>, Entry);
pub type MetadataPage = (Option<Entry>, Vec<KeyedEntry>);
pub type DocumentChange = (Vec<u8>, Option<Entry>);
pub type DocumentCommit = (u64, Vec<DocumentChange>);
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Check {
    pub key: Vec<u8>,
    pub expected: Expected,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expected {
    Missing,
    Version(u64),
    /// Document key has no mutations after this history boundary.
    DocumentUnchangedSince(u64),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug)]
pub enum Error {
    Conflict { key: Vec<u8> },
    Storage(rusqlite::Error),
    LockPoisoned,
    VersionExhausted,
    InvalidSnapshot,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict { key } => write!(f, "condition failed for key {key:?}"),
            Self::Storage(e) => write!(f, "storage error: {e}"),
            Self::LockPoisoned => write!(f, "store lock poisoned"),
            Self::VersionExhausted => write!(f, "store version exhausted"),
            Self::InvalidSnapshot => write!(f, "snapshot is newer than document history"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(e) => Some(e),
            _ => None,
        }
    }
}
impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Self::Storage(e)
    }
}

/// A single connection, safe to share between threads. Different instances
/// opening the same database coordinate through SQLite's write lock.
pub struct Store {
    connection: Mutex<Connection>,
}
impl Store {
    pub fn memory() -> Result<Self, Error> {
        Self::initialize(Connection::open_in_memory()?, false)
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::initialize(Connection::open(path)?, true)
    }
    fn initialize(conn: Connection, disk: bool) -> Result<Self, Error> {
        conn.busy_timeout(Duration::from_secs(5))?;
        if disk {
            let mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
            if !mode.eq_ignore_ascii_case("wal") {
                return Err(Error::Storage(rusqlite::Error::InvalidQuery));
            }
        }
        conn.execute_batch("PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS durable_store_meta (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL CHECK(version>=0));
            INSERT OR IGNORE INTO durable_store_meta(id, version) VALUES(1, 0);
            CREATE TABLE IF NOT EXISTS durable_store_entries (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL, version INTEGER NOT NULL CHECK(version>0)) WITHOUT ROWID;")?;
        // Seed existing live documents once. Earlier overwritten/deleted values cannot
        // be recovered. Both the seed and its marker are committed atomically.
        conn.execute_batch("BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS durable_document_history_meta (id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL CHECK(revision>=0));
            CREATE TABLE IF NOT EXISTS durable_document_history (key BLOB NOT NULL, revision INTEGER NOT NULL, value BLOB, version INTEGER, PRIMARY KEY(key, revision)) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS durable_document_history_by_revision ON durable_document_history(revision, key);
            CREATE TABLE IF NOT EXISTS durable_document_cuts (key BLOB PRIMARY KEY NOT NULL, revision INTEGER NOT NULL) WITHOUT ROWID;
            INSERT INTO durable_document_history(key, revision, value, version)
                SELECT key, 0, value, version FROM durable_store_entries
                WHERE substr(key, 1, 17)=CAST('durable/document/' AS BLOB)
                AND NOT EXISTS (SELECT 1 FROM durable_document_history_meta WHERE id=1);
            INSERT OR IGNORE INTO durable_document_history_meta(id, revision) VALUES(1, 0);
            COMMIT;")?;
        Ok(Self {
            connection: Mutex::new(conn),
        })
    }
    pub fn get(&self, key: &[u8]) -> Result<Option<Entry>, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn
            .query_row(
                "SELECT value, version FROM durable_store_entries WHERE key=?1",
                [key],
                |r| {
                    Ok(Entry {
                        value: r.get(0)?,
                        version: r.get(1)?,
                    })
                },
            )
            .optional()?)
    }
    /// Returns a consistent snapshot in lexicographic byte order.
    pub fn list(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Entry)>, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let mut stmt = conn.prepare("SELECT key, value, version FROM durable_store_entries WHERE substr(key, 1, ?1)=?2 ORDER BY key")?;
        let rows = stmt.query_map(params![prefix.len() as i64, prefix], |r| {
            Ok((
                r.get(0)?,
                Entry {
                    value: r.get(1)?,
                    version: r.get(2)?,
                },
            ))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
    /// Read a metadata key and a bounded, ordered page from one SQLite snapshot.
    /// `after` is exclusive and must be a key in or before the prefix range.
    /// Pages across calls may observe newer commits.
    pub fn page_with_metadata(
        &self,
        metadata_key: &[u8],
        prefix: &[u8],
        after: &[u8],
        limit: usize,
    ) -> Result<MetadataPage, Error> {
        let mut conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let metadata = tx
            .query_row(
                "SELECT value, version FROM durable_store_entries WHERE key=?1",
                [metadata_key],
                |r| {
                    Ok(Entry {
                        value: r.get(0)?,
                        version: r.get(1)?,
                    })
                },
            )
            .optional()?;
        let end = prefix_end(prefix);
        let rows = {
            let query = if end.is_some() {
                "SELECT key, value, version FROM durable_store_entries WHERE key>=?1 AND key>?2 AND key<?3 ORDER BY key LIMIT ?4"
            } else {
                "SELECT key, value, version FROM durable_store_entries WHERE key>=?1 AND key>?2 AND ?3 IS NULL ORDER BY key LIMIT ?4"
            };
            let mut stmt = tx.prepare(query)?;
            let limit = i64::try_from(limit).unwrap_or(i64::MAX);
            let rows = stmt.query_map(params![prefix, after, end, limit], |r| {
                Ok((
                    r.get(0)?,
                    Entry {
                        value: r.get(1)?,
                        version: r.get(2)?,
                    },
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        tx.commit()?;
        Ok((metadata, rows))
    }
    /// Capture a document history boundary. Every document mutation in one
    /// commit shares a boundary; subsequent reads at it are immutable.
    pub fn document_snapshot(&self) -> Result<u64, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn.query_row(
            "SELECT revision FROM durable_document_history_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?)
    }

    /// Read a document as of a captured history boundary. This does not accept
    /// arbitrary store keys and does not promise history from before migration.
    pub fn document_at(&self, key: &[u8], revision: u64) -> Result<Option<Entry>, Error> {
        if !key.starts_with(b"durable/document/") {
            return Err(Error::InvalidSnapshot);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let current: u64 = conn.query_row(
            "SELECT revision FROM durable_document_history_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if revision > current {
            return Err(Error::InvalidSnapshot);
        }
        let row: Option<(Option<Vec<u8>>, Option<u64>)> = conn.query_row(
            "SELECT value, version FROM durable_document_history WHERE key=?1 AND revision<=?2 ORDER BY revision DESC LIMIT 1",
            params![key, revision], |r| Ok((r.get(0)?, r.get(1)?)),
        ).optional()?;
        match row {
            Some((Some(value), Some(version))) => Ok(Some(Entry { value, version })),
            Some((None, None)) | None => Ok(None),
            _ => Err(Error::Storage(rusqlite::Error::InvalidQuery)),
        }
    }

    /// Ordered committed states for one document after a database-local revision.
    /// A delete is represented by None. The page contains at most 256 states.
    pub fn document_changes(
        &self,
        key: &[u8],
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, Option<Entry>)>, Error> {
        if !key.starts_with(b"durable/document/") || limit == 0 || limit > 256 {
            return Err(Error::InvalidSnapshot);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let current: u64 = conn.query_row(
            "SELECT revision FROM durable_document_history_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if after > current {
            return Err(Error::InvalidSnapshot);
        }
        let mut stmt = conn.prepare("SELECT revision,value,version FROM durable_document_history WHERE key=?1 AND revision>?2 ORDER BY revision LIMIT ?3")?;
        let rows = stmt.query_map(params![key, after, limit as i64], |r| {
            Ok((
                r.get::<_, u64>(0)?,
                r.get::<_, Option<Vec<u8>>>(1)?,
                r.get::<_, Option<u64>>(2)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (revision, value, version) = row?;
            let entry = match (value, version) {
                (Some(value), Some(version)) => Some(Entry { value, version }),
                (None, None) => None,
                _ => return Err(Error::Storage(rusqlite::Error::InvalidQuery)),
            };
            result.push((revision, entry));
        }
        Ok(result)
    }

    /// Complete document commits after a revision, bounded by commit count.
    /// Each frame contains every changed document, ordered by storage key.
    pub fn document_commits(&self, after: u64, limit: usize) -> Result<Vec<DocumentCommit>, Error> {
        if limit == 0 || limit > 256 {
            return Err(Error::InvalidSnapshot);
        }
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let current: u64 = conn.query_row(
            "SELECT revision FROM durable_document_history_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if after > current {
            return Err(Error::InvalidSnapshot);
        }
        let mut stmt = conn.prepare("SELECT revision,key,value,version FROM durable_document_history WHERE revision IN (SELECT DISTINCT revision FROM durable_document_history WHERE revision>?1 ORDER BY revision LIMIT ?2) ORDER BY revision,key")?;
        let mut rows = stmt.query(params![after, limit as i64])?;
        let mut commits: Vec<DocumentCommit> = Vec::new();
        while let Some(row) = rows.next()? {
            let revision: u64 = row.get(0)?;
            let key = row.get(1)?;
            let entry = match (
                row.get::<_, Option<Vec<u8>>>(2)?,
                row.get::<_, Option<u64>>(3)?,
            ) {
                (Some(value), Some(version)) => Some(Entry { value, version }),
                (None, None) => None,
                _ => return Err(Error::Storage(rusqlite::Error::InvalidQuery)),
            };
            if commits.last().is_none_or(|frame| frame.0 != revision) {
                commits.push((revision, Vec::new()));
            }
            commits.last_mut().unwrap().1.push((key, entry));
        }
        Ok(commits)
    }

    /// Document revision recorded atomically with a conversation fork record.
    pub fn document_cut(&self, key: &[u8]) -> Result<Option<u64>, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(conn
            .query_row(
                "SELECT revision FROM durable_document_cuts WHERE key=?1",
                [key],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Enumerate live document entries at an immutable revision.
    pub fn documents_at(&self, revision: u64) -> Result<Vec<KeyedEntry>, Error> {
        let conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let current: u64 = conn.query_row(
            "SELECT revision FROM durable_document_history_meta WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if revision > current {
            return Err(Error::InvalidSnapshot);
        }
        let mut stmt = conn.prepare("SELECT h.key,h.value,h.version FROM durable_document_history h WHERE h.revision=(SELECT max(x.revision) FROM durable_document_history x WHERE x.key=h.key AND x.revision<=?1) AND h.value IS NOT NULL ORDER BY h.key")?;
        let rows = stmt.query_map([revision], |r| {
            Ok((
                r.get(0)?,
                Entry {
                    value: r.get(1)?,
                    version: r.get(2)?,
                },
            ))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// All checks are evaluated against the pre-commit state; mutations apply
    /// in order. A failed check rolls back the complete batch.
    pub fn commit(&self, checks: &[Check], mutations: &[Mutation]) -> Result<(), Error> {
        let mut conn = self.connection.lock().map_err(|_| Error::LockPoisoned)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for check in checks {
            let version: Option<u64> = tx
                .query_row(
                    "SELECT version FROM durable_store_entries WHERE key=?1",
                    [&check.key],
                    |r| r.get(0),
                )
                .optional()?;
            let matches = match check.expected {
                Expected::Missing => version.is_none(),
                Expected::Version(v) => version == Some(v),
                Expected::DocumentUnchangedSince(revision) => {
                    if !check.key.starts_with(b"durable/document/") {
                        return Err(Error::InvalidSnapshot);
                    }
                    let current: u64 = tx.query_row(
                        "SELECT revision FROM durable_document_history_meta WHERE id=1",
                        [],
                        |r| r.get(0),
                    )?;
                    if revision > current {
                        return Err(Error::InvalidSnapshot);
                    }
                    let changed: bool = tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM durable_document_history WHERE key=?1 AND revision>?2)",
                        params![check.key, revision], |r| r.get(0),
                    )?;
                    !changed
                }
            };
            if !matches {
                return Err(Error::Conflict {
                    key: check.key.clone(),
                });
            }
        }
        let document_changed = mutations.iter().any(|mutation| match mutation {
            Mutation::Put { key, .. } | Mutation::Delete { key } => {
                key.starts_with(b"durable/document/")
            }
        });
        let revision = if document_changed {
            let previous: i64 = tx.query_row(
                "SELECT revision FROM durable_document_history_meta WHERE id=1",
                [],
                |r| r.get(0),
            )?;
            let next = previous.checked_add(1).ok_or(Error::VersionExhausted)?;
            tx.execute(
                "UPDATE durable_document_history_meta SET revision=?1 WHERE id=1",
                [next],
            )?;
            Some(next)
        } else {
            None
        };
        for mutation in mutations {
            match mutation {
                Mutation::Put { key, value } => {
                    let version: i64 = tx.query_row(
                        "SELECT version FROM durable_store_meta WHERE id=1",
                        [],
                        |r| r.get(0),
                    )?;
                    let version = version.checked_add(1).ok_or(Error::VersionExhausted)?;
                    tx.execute(
                        "UPDATE durable_store_meta SET version=?1 WHERE id=1",
                        [version],
                    )?;
                    tx.execute("INSERT INTO durable_store_entries(key,value,version) VALUES(?1,?2,?3) ON CONFLICT(key) DO UPDATE SET value=excluded.value,version=excluded.version", params![key, value, version])?;
                }
                Mutation::Delete { key } => {
                    tx.execute("DELETE FROM durable_store_entries WHERE key=?1", [key])?;
                }
            }
            let key = match mutation {
                Mutation::Put { key, .. } | Mutation::Delete { key } => key,
            };
            if key.starts_with(b"durable/document/") {
                // Upsert accommodates raw batches that mutate the same key more
                // than once: the snapshot represents the final committed state.
                tx.execute("INSERT INTO durable_document_history(key,revision,value,version) VALUES(?1,?2,(SELECT value FROM durable_store_entries WHERE key=?1),(SELECT version FROM durable_store_entries WHERE key=?1)) ON CONFLICT(key,revision) DO UPDATE SET value=excluded.value,version=excluded.version", params![key, revision])?;
            }
        }
        // Capture the post-mutation revision in the same transaction as every
        // conversation snapshot, even when the document batch is appended later.
        for mutation in mutations {
            if let Mutation::Put { key, .. } = mutation {
                if key.starts_with(b"runtime/fork/") {
                    tx.execute("INSERT OR IGNORE INTO durable_document_cuts(key,revision) SELECT ?1,revision FROM durable_document_history_meta WHERE id=1", [key])?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(byte) = end.pop() {
        if byte != u8::MAX {
            end.push(byte + 1);
            return Some(end);
        }
    }
    None
}

#[cfg(test)]
mod document_history_tests {
    use super::*;

    #[test]
    fn migrates_existing_live_documents_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        let key = b"durable/document/legacy";
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch("CREATE TABLE durable_store_meta (id INTEGER PRIMARY KEY, version INTEGER NOT NULL); INSERT INTO durable_store_meta VALUES(1, 7); CREATE TABLE durable_store_entries (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL, version INTEGER NOT NULL) WITHOUT ROWID;").unwrap();
            conn.execute(
                "INSERT INTO durable_store_entries VALUES (?1, ?2, 7)",
                params![key, b"legacy".as_slice()],
            )
            .unwrap();
        }
        let db = Store::open(&path).unwrap();
        assert_eq!(db.document_snapshot().unwrap(), 0);
        assert_eq!(db.document_at(key, 0).unwrap().unwrap().version, 7);
        db.commit(
            &[Check {
                key: key.to_vec(),
                expected: Expected::Version(7),
            }],
            &[Mutation::Delete { key: key.to_vec() }],
        )
        .unwrap();
        drop(db);
        let db = Store::open(&path).unwrap();
        assert_eq!(db.document_at(key, 0).unwrap().unwrap().value, b"legacy");
        assert!(db.document_at(key, 1).unwrap().is_none());
    }
}
