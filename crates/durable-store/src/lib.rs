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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Check {
    pub key: Vec<u8>,
    pub expected: Expected,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expected {
    Missing,
    Version(u64),
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
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict { key } => write!(f, "condition failed for key {key:?}"),
            Self::Storage(e) => write!(f, "storage error: {e}"),
            Self::LockPoisoned => write!(f, "store lock poisoned"),
            Self::VersionExhausted => write!(f, "store version exhausted"),
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
            };
            if !matches {
                return Err(Error::Conflict {
                    key: check.key.clone(),
                });
            }
        }
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
