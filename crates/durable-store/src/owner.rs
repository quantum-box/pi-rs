//! Exclusive scheduler ownership for a local SQLite store.
//!
//! Acquire this guard before opening or recovering the database, and retain it
//! until the scheduler and every operation using the store have stopped. It is
//! independent of SQLite transactions and only coordinates cooperating owners.
//!
//! The parent directory is canonicalized, so relative paths and symlinked
//! directories share an identity. Database and lockfile symlinks and hard links
//! are rejected. The final database path may initially be absent. The adjacent
//! `<database>.scheduler.lock` file is permanent and must never be removed or
//! replaced, even while no owner is running. Database names ending in
//! `.scheduler.lock` are reserved and rejected. Neither the database nor its
//! directories may be renamed, replaced, linked, or unlinked while in use;
//! path validation cannot defend against concurrent filesystem manipulation.
//! Bind mounts, other filesystem aliases, network filesystems, noncooperating
//! writers and live database copies are unsupported. Use a local filesystem
//! with working OS locks. On process death the OS releases the lock, except
//! while a forked child still holds the same open file description. A forked
//! child must not use inherited SQLite or scheduler state before exec.
//!
//! This implementation currently supports Unix only, where hard-link counts
//! are available. Unsupported platforms fail closed.

use crate::Store;
use std::{
    collections::HashMap,
    fmt,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
};
use uuid::Uuid;

#[derive(Debug)]
pub enum OwnerError {
    /// Another scheduler holds the lock.
    Busy,
    /// The path or filesystem does not meet the ownership protocol.
    InvalidPath(&'static str),
    Io(io::Error),
    Store(crate::Error),
}

impl fmt::Display for OwnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy => write!(f, "scheduler ownership is already held"),
            Self::InvalidPath(reason) => write!(f, "invalid scheduler store path: {reason}"),
            Self::Io(e) => write!(f, "scheduler ownership I/O error: {e}"),
            Self::Store(e) => write!(f, "scheduler store error: {e}"),
        }
    }
}
impl std::error::Error for OwnerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Store(e) => Some(e),
            _ => None,
        }
    }
}
impl From<io::Error> for OwnerError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<crate::Error> for OwnerError {
    fn from(e: crate::Error) -> Self {
        Self::Store(e)
    }
}

/// Owns the store and its scheduler lock. The store closes before the lock.
pub struct OwnedStore {
    store: Store,
    _lock: OwnerLock,
    task_gate: crate::controls::PollGate,
    controls: crate::controls::ConversationControls,
    summary_fences: Mutex<HashMap<Uuid, Weak<tokio::sync::Mutex<()>>>>,
}

// Dropped after `store`: explicitly unlock the shared open file description so
// a child temporarily between fork and exec cannot retain an abandoned lock.
struct OwnerLock {
    file: File,
    owner_pid: u32,
}

impl Drop for OwnerLock {
    fn drop(&mut self) {
        if self.owner_pid == std::process::id() {
            let _ = self.file.unlock();
        }
    }
}

/// Exclusive execution of one summary operation, including usage settlement.
/// Retains the database owner until the executor has actually stopped.
pub struct SummaryGuard {
    _guard: tokio::sync::OwnedMutexGuard<()>,
    _owner: Arc<OwnedStore>,
}

#[cfg(all(test, unix))]
type OwnerLockHook = Box<dyn FnOnce(&File, &Path)>;

#[cfg(all(test, unix))]
thread_local! {
    static AFTER_OWNER_LOCK: std::cell::RefCell<Option<OwnerLockHook>> = std::cell::RefCell::new(None);
}

impl OwnedStore {
    /// Try to acquire ownership without waiting, then open the SQLite store.
    #[cfg(unix)]
    pub fn open(path: impl AsRef<Path>) -> Result<Self, OwnerError> {
        let path = path.as_ref();
        let name = path
            .file_name()
            .ok_or(OwnerError::InvalidPath("database needs a file name"))?;
        use std::os::unix::ffi::OsStrExt;
        if name.as_bytes().ends_with(b".scheduler.lock") {
            return Err(OwnerError::InvalidPath(
                "database name uses the reserved lockfile suffix",
            ));
        }
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let db = fs::canonicalize(parent)?.join(name);
        validate_file(&db, true)?;
        let mut lock_name = name.to_os_string();
        lock_name.push(".scheduler.lock");
        let lock_path = db.parent().unwrap().join(lock_name);
        validate_file(&lock_path, true)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        // Check both the path and the open handle before locking. Filesystem
        // replacement races are outside the supported deployment contract.
        validate_file(&lock_path, false)?;
        validate_metadata(&lock.metadata()?)?;
        match lock.try_lock() {
            Ok(()) => (),
            Err(std::fs::TryLockError::WouldBlock) => return Err(OwnerError::Busy),
            Err(std::fs::TryLockError::Error(e)) => return Err(OwnerError::Io(e)),
        }
        let lock = OwnerLock {
            file: lock,
            owner_pid: std::process::id(),
        };
        #[cfg(test)]
        AFTER_OWNER_LOCK.with(|slot| {
            if let Some(hook) = slot.borrow_mut().take() {
                hook(&lock.file, &db);
            }
        });
        validate_file(&db, true)?;
        let store = Store::open(&db)?;
        validate_file(&db, false)?;
        Ok(Self {
            store,
            _lock: lock,
            task_gate: crate::controls::PollGate::default(),
            controls: crate::controls::ConversationControls::default(),
            summary_fences: Mutex::new(HashMap::new()),
        })
    }

    #[cfg(not(unix))]
    pub fn open(_path: impl AsRef<Path>) -> Result<Self, OwnerError> {
        Err(OwnerError::InvalidPath(
            "scheduler ownership is supported only on Unix",
        ))
    }

    /// Serializes task graph decisions without holding a lock during handlers.
    pub fn task_gate(&self) -> &crate::controls::PollGate {
        &self.task_gate
    }

    /// Process-local coordination shared by all users of this owner.
    pub fn conversation_controls(&self) -> &crate::controls::ConversationControls {
        &self.controls
    }

    /// Serialize all executors and recovery for an operation. Hold this guard
    /// through provider invocation, settlement, and all associated commits.
    /// Cancelling a waiter or dropping its guard releases its claim. A caller
    /// must cancel and join an executor before starting replacement work.
    /// This fence coordinates only users of this owner, not direct store writes.
    pub async fn summary_guard(self: &Arc<Self>, id: Uuid) -> SummaryGuard {
        let lock = {
            let mut entries = self
                .summary_fences
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            entries.retain(|_, entry| entry.strong_count() != 0);
            match entries.get(&id).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(tokio::sync::Mutex::new(()));
                    entries.insert(id, Arc::downgrade(&lock));
                    lock
                }
            }
        };
        let guard = lock.lock_owned().await;
        SummaryGuard {
            _guard: guard,
            _owner: self.clone(),
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }
}

#[cfg(unix)]
fn validate_metadata(metadata: &fs::Metadata) -> Result<(), OwnerError> {
    use std::os::unix::fs::MetadataExt;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(OwnerError::InvalidPath(
            "database and lockfile must be regular files with exactly one link",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_file(path: &PathBuf, may_be_missing: bool) -> Result<(), OwnerError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => validate_metadata(&metadata),
        Err(e) if may_be_missing && e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

#[cfg(all(test, unix))]
mod inherited_lock_tests {
    use super::*;

    #[test]
    fn failed_open_releases_lock_while_inherited_descriptor_remains() {
        use std::{cell::RefCell, os::unix::fs::symlink, rc::Rc};
        for fail_validation in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let inherited = Rc::new(RefCell::new(None));
            let retained = inherited.clone();
            AFTER_OWNER_LOCK.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move |lock, db| {
                    *retained.borrow_mut() = Some(lock.try_clone().unwrap());
                    if fail_validation {
                        symlink("/dev/null", db).unwrap();
                    } else {
                        std::fs::write(db, b"not a sqlite database").unwrap();
                    }
                }));
            });
            let result = OwnedStore::open(&path);
            if fail_validation {
                assert!(matches!(result, Err(OwnerError::InvalidPath(_))));
            } else {
                assert!(matches!(result, Err(OwnerError::Store(_))));
            }
            assert!(inherited.borrow().is_some());
            std::fs::remove_file(&path).unwrap();
            let reopened = OwnedStore::open(&path).unwrap();
            drop(reopened);
            drop(inherited);
        }
    }

    #[test]
    fn fork_child_drop_does_not_unlock_live_parent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let owner = OwnedStore::open(&path).unwrap();
        let mut inherited = OwnerLock {
            file: owner._lock.file.try_clone().unwrap(),
            owner_pid: owner._lock.owner_pid,
        };
        // In the fork child do no allocation, locking, I/O through SQLite,
        // or unwinding: just drop the copied descriptor and immediately exit.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            drop(inherited);
            unsafe { libc::_exit(0) }
        }
        let mut child_status = 0;
        assert_eq!(unsafe { libc::waitpid(pid, &mut child_status, 0) }, pid);
        assert!(libc::WIFEXITED(child_status));
        assert_eq!(libc::WEXITSTATUS(child_status), 0);
        assert!(matches!(OwnedStore::open(&path), Err(OwnerError::Busy)));
        // Drop the parent's duplicate without running the ownership wrapper;
        // the real owner retains the lock until its own shutdown.
        inherited.owner_pid = 0;
        drop(inherited);
        drop(owner);
        assert!(OwnedStore::open(&path).is_ok());
    }

    #[test]
    fn shutdown_releases_lock_even_if_an_inherited_descriptor_remains_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let owner = OwnedStore::open(&path).unwrap();
        // A duplicated descriptor models the open file description inherited
        // by a concurrent fork before close-on-exec takes effect.
        let inherited = owner._lock.file.try_clone().unwrap();
        assert!(matches!(OwnedStore::open(&path), Err(OwnerError::Busy)));
        drop(owner);
        let reopened = OwnedStore::open(&path).unwrap();
        drop(reopened);
        drop(inherited);
    }
}
