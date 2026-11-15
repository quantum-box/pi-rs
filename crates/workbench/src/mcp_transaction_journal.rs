//! Experimental, secret-free, single-record MCP OAuth transaction journal.
//!
//! Every operation, including reads and recovery, MUST be called with the same
//! cross-process shared MCP configuration gate held. This module neither owns
//! nor verifies that gate. The containing directory must be private and trusted;
//! non-cooperating writers and storage rollback are out of scope. The parent path
//! and its ancestors must remain trusted; identity checks cannot prevent an
//! adversarial swap-and-restore or detect replacement across process restarts.
//!
//! Record stages are write-ahead *intent*, not proof that an action completed:
//! persist `ConfigInProgress` before config mutation and
//! `CredentialsInProgress` before credential mutation. Persist `Complete` only
//! after both operations are known durable. A crash can occur on either side of
//! each action. This record contains no server identity, URL, credentials, or
//! rollback data, so it cannot decide or perform recovery. Incomplete records
//! must block new work until a separately designed recovery policy resolves them.
//! An absent record alone is not evidence that no transaction ever existed.
//!
//! On Unix, updates sync a temporary file, atomically rename it in the same
//! directory, and sync that directory; removal also syncs the directory. A sync
//! error has an ambiguous outcome: stop the transaction and require explicit
//! recovery. A re-read cannot prove durability and must not resume operations. Filesystem and
//! hardware guarantees vary, and external deletion or snapshot rollback cannot
//! be detected. Windows fails closed until durable directory syncing is designed.
use sha2::{Digest, Sha256};
use std::{
    cell::Cell,
    fs::File,
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

const NAME: &str = "mcp-oauth-transaction.journal";
const MAGIC: &[u8; 8] = b"PIMCPJNL";
const VERSION: u8 = 1;
const PREFIX_LEN: usize = 26;
const RECORD_LEN: usize = PREFIX_LEN + 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stage {
    Prepared,
    ConfigInProgress,
    CredentialsInProgress,
    Complete,
}
impl Stage {
    fn code(self) -> u8 {
        match self {
            Self::Prepared => 1,
            Self::ConfigInProgress => 2,
            Self::CredentialsInProgress => 3,
            Self::Complete => 4,
        }
    }
    fn parse(byte: u8) -> io::Result<Self> {
        match byte {
            1 => Ok(Self::Prepared),
            2 => Ok(Self::ConfigInProgress),
            3 => Ok(Self::CredentialsInProgress),
            4 => Ok(Self::Complete),
            _ => Err(invalid("unknown transaction stage")),
        }
    }
    fn next(self) -> Option<Self> {
        match self {
            Self::Prepared => Some(Self::ConfigInProgress),
            Self::ConfigInProgress => Some(Self::CredentialsInProgress),
            Self::CredentialsInProgress => Some(Self::Complete),
            Self::Complete => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Record {
    id: [u8; 16],
    pub(crate) stage: Stage,
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn conflict(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::AlreadyExists, message)
}

pub(crate) struct Journal {
    path: PathBuf,
    #[cfg(unix)]
    directory: File,
    poisoned: Cell<bool>,
    #[cfg(test)]
    fail_sync: Cell<bool>,
}
impl Journal {
    /// Open and pin the existing private, trusted directory under the shared gate.
    /// Keep its pathname and ancestors stable; replacement across restarts cannot
    /// be detected from this journal alone.
    pub(crate) fn new(directory: &Path) -> io::Result<Self> {
        require_durable_platform()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let handle = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(directory)?;
            let journal = Self {
                path: directory.join(NAME),
                directory: handle,
                poisoned: Cell::new(false),
                #[cfg(test)]
                fail_sync: Cell::new(false),
            };
            journal.check_directory()?;
            Ok(journal)
        }
        #[cfg(not(unix))]
        {
            let _ = directory;
            Err(unsupported())
        }
    }

    #[cfg(unix)]
    fn check_directory(&self) -> io::Result<()> {
        use std::os::unix::fs::MetadataExt;
        let opened = self.directory.metadata()?;
        let path = self
            .path
            .parent()
            .ok_or_else(|| invalid("journal has no parent"))?;
        let current = std::fs::symlink_metadata(path)
            .map_err(|_| invalid("journal directory missing or inaccessible"))?;
        if !opened.is_dir()
            || opened.nlink() == 0
            || !current.file_type().is_dir()
            || opened.dev() != current.dev()
            || opened.ino() != current.ino()
        {
            return Err(invalid("journal directory identity changed"));
        }
        Ok(())
    }
    fn check_ready(&self) -> io::Result<()> {
        require_durable_platform()?;
        if self.poisoned.get() {
            return Err(invalid(
                "journal durability uncertain; stop and require recovery",
            ));
        }
        #[cfg(unix)]
        self.check_directory()?;
        Ok(())
    }
    #[cfg(unix)]
    fn open_at(&self, name: &std::ffi::CStr, flags: i32, mode: u32) -> io::Result<File> {
        use std::os::fd::{AsRawFd, FromRawFd};
        let fd = unsafe {
            libc::openat(
                self.directory.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                mode as libc::c_uint,
            )
        };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(unsafe { File::from_raw_fd(fd) })
        }
    }
    #[cfg(unix)]
    fn sync_directory(&self) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_sync.replace(false) {
            return Err(io::Error::other("injected directory sync failure"));
        }
        self.directory.sync_all()
    }
    /// Missing is distinct from malformed, unreadable, or a replaced directory.
    pub(crate) fn read(&self) -> io::Result<Option<Record>> {
        self.check_ready()?;
        #[cfg(unix)]
        {
            let file = match self.open_at(
                c"mcp-oauth-transaction.journal",
                libc::O_RDONLY | libc::O_NONBLOCK,
                0,
            ) {
                Ok(file) => file,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    self.check_directory()?;
                    return Ok(None);
                }
                Err(e) => return Err(e),
            };
            if !file.metadata()?.is_file() {
                return Err(invalid("journal is not a regular file"));
            }
            let mut bytes = Vec::with_capacity(RECORD_LEN + 1);
            file.take((RECORD_LEN + 1) as u64).read_to_end(&mut bytes)?;
            self.check_directory()?;
            if bytes.len() != RECORD_LEN {
                return Err(invalid("journal has an invalid length"));
            }
            if &bytes[..8] != MAGIC {
                return Err(invalid("invalid journal magic"));
            }
            if bytes[8] != VERSION {
                return Err(invalid("unknown journal version"));
            }
            let stage = Stage::parse(bytes[9])?;
            if Sha256::digest(&bytes[..PREFIX_LEN]).as_slice() != &bytes[PREFIX_LEN..] {
                return Err(invalid("journal checksum mismatch"));
            }
            let mut id = [0u8; 16];
            id.copy_from_slice(&bytes[10..PREFIX_LEN]);
            if id == [0; 16] {
                return Err(invalid("invalid transaction identifier"));
            }
            Ok(Some(Record { id, stage }))
        }
        #[cfg(not(unix))]
        {
            Err(unsupported())
        }
    }
    pub(crate) fn begin(&self) -> io::Result<Record> {
        self.check_ready()?;
        if self.read()?.is_some() {
            return Err(conflict("a transaction journal already exists"));
        }
        let record = Record {
            id: *uuid::Uuid::new_v4().as_bytes(),
            stage: Stage::Prepared,
        };
        self.write(record)?;
        Ok(record)
    }
    pub(crate) fn advance(&self, expected: Record, next: Stage) -> io::Result<Record> {
        self.check_ready()?;
        if expected.stage.next() != Some(next) {
            return Err(invalid("invalid journal stage transition"));
        }
        if self.read()? != Some(expected) {
            return Err(conflict("transaction journal changed"));
        }
        let record = Record {
            stage: next,
            ..expected
        };
        self.write(record)?;
        Ok(record)
    }
    pub(crate) fn clear_complete(&self, expected: Record) -> io::Result<()> {
        self.check_ready()?;
        if expected.stage != Stage::Complete {
            return Err(invalid("cannot clear an incomplete transaction"));
        }
        if self.read()? != Some(expected) {
            return Err(conflict("transaction journal changed"));
        }
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            self.poisoned.set(true);
            if unsafe {
                libc::unlinkat(
                    self.directory.as_raw_fd(),
                    c"mcp-oauth-transaction.journal".as_ptr(),
                    0,
                )
            } != 0
            {
                return Err(io::Error::last_os_error());
            }
            self.sync_directory()?;
            self.check_directory()?;
            self.poisoned.set(false);
            Ok(())
        }
        #[cfg(not(unix))]
        {
            Err(unsupported())
        }
    }
    fn write(&self, record: Record) -> io::Result<()> {
        self.check_ready()?;
        #[cfg(unix)]
        {
            use std::{ffi::CString, os::fd::AsRawFd};
            let mut bytes = Vec::with_capacity(RECORD_LEN);
            bytes.extend_from_slice(MAGIC);
            bytes.push(VERSION);
            bytes.push(record.stage.code());
            bytes.extend_from_slice(&record.id);
            let checksum = Sha256::digest(&bytes);
            bytes.extend_from_slice(&checksum);
            let temp = CString::new(format!(".mcp-journal-{}", uuid::Uuid::new_v4()))
                .expect("UUID has no NUL");
            self.poisoned.set(true);
            let result = (|| {
                let mut file =
                    self.open_at(&temp, libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL, 0o600)?;
                file.write_all(&bytes)?;
                file.sync_all()?;
                self.check_directory()?;
                if unsafe {
                    libc::renameat(
                        self.directory.as_raw_fd(),
                        temp.as_ptr(),
                        self.directory.as_raw_fd(),
                        c"mcp-oauth-transaction.journal".as_ptr(),
                    )
                } != 0
                {
                    return Err(io::Error::last_os_error());
                }
                self.sync_directory()?;
                self.check_directory()?;
                Ok(())
            })();
            if result.is_err() {
                unsafe {
                    libc::unlinkat(self.directory.as_raw_fd(), temp.as_ptr(), 0);
                }
            }
            if result.is_ok() {
                self.poisoned.set(false);
            }
            result
        }
        #[cfg(not(unix))]
        {
            let _ = record;
            Err(unsupported())
        }
    }
}
#[cfg(not(unix))]
fn unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "durable journal unavailable on this platform",
    )
}
#[cfg(unix)]
fn require_durable_platform() -> io::Result<()> {
    Ok(())
}
#[cfg(not(unix))]
fn require_durable_platform() -> io::Result<()> {
    Err(unsupported())
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    #[test]
    fn reads_are_unsupported_without_touching_the_path() {
        let journal = Journal {
            path: PathBuf::from("nonexistent-journal"),
            poisoned: Cell::new(false),
            fail_sync: Cell::new(false),
        };
        assert_eq!(
            journal.read().unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            Journal::new(Path::new("nonexistent-directory"))
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::Unsupported
        );
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    #[test]
    fn directory_removal_and_replacement_are_not_absence() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("journal-dir");
        std::fs::create_dir(&path).unwrap();
        let journal = Journal::new(&path).unwrap();
        assert_eq!(journal.read().unwrap(), None);
        std::fs::rename(&path, root.path().join("moved")).unwrap();
        assert!(journal.read().is_err());
        assert!(journal.begin().is_err());
        std::fs::create_dir(&path).unwrap();
        assert!(journal.read().is_err());
        assert!(journal.begin().is_err());
        assert!(!path.join(NAME).exists());

        let removed = root.path().join("removed");
        std::fs::create_dir(&removed).unwrap();
        let journal = Journal::new(&removed).unwrap();
        std::fs::remove_dir(&removed).unwrap();
        assert!(journal.read().is_err());
        assert!(journal.begin().is_err());
    }

    #[test]
    fn sync_failure_poison_stops_reads_and_writes_even_if_record_visible() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::new(dir.path()).unwrap();
        journal.fail_sync.set(true);
        assert!(journal.begin().is_err());
        assert!(journal.read().is_err());
        assert!(journal.begin().is_err());
        // Observation after reopening does not prove that the rename survived
        // a crash; it must not be interpreted as authorization to continue.
        assert_eq!(
            Journal::new(dir.path())
                .unwrap()
                .read()
                .unwrap()
                .unwrap()
                .stage,
            Stage::Prepared
        );
    }
    #[test]
    fn sync_failure_after_advance_or_clear_stops_instance() {
        for fail_clear in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let journal = Journal::new(dir.path()).unwrap();
            let mut record = journal.begin().unwrap();
            if fail_clear {
                for stage in [
                    Stage::ConfigInProgress,
                    Stage::CredentialsInProgress,
                    Stage::Complete,
                ] {
                    record = journal.advance(record, stage).unwrap();
                }
            }
            journal.fail_sync.set(true);
            let result = if fail_clear {
                journal.clear_complete(record)
            } else {
                journal.advance(record, Stage::ConfigInProgress).map(|_| ())
            };
            assert!(result.is_err());
            assert!(journal.read().is_err());
            assert!(journal.begin().is_err());
        }
    }
    #[test]
    fn restart_keeps_each_intent_and_rejects_skipped_or_stale_changes() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::new(dir.path()).unwrap();
        let prepared = journal.begin().unwrap();
        assert_eq!(
            Journal::new(dir.path()).unwrap().read().unwrap(),
            Some(prepared)
        );
        assert!(journal.begin().is_err());
        assert!(
            journal
                .advance(prepared, Stage::CredentialsInProgress)
                .is_err()
        );
        assert!(journal.clear_complete(prepared).is_err());
        let config = journal.advance(prepared, Stage::ConfigInProgress).unwrap();
        assert_eq!(
            Journal::new(dir.path()).unwrap().read().unwrap(),
            Some(config)
        );
        assert!(journal.advance(prepared, Stage::ConfigInProgress).is_err());
        let credentials = journal
            .advance(config, Stage::CredentialsInProgress)
            .unwrap();
        assert_eq!(
            Journal::new(dir.path()).unwrap().read().unwrap(),
            Some(credentials)
        );
        let complete = journal.advance(credentials, Stage::Complete).unwrap();
        assert_eq!(
            Journal::new(dir.path()).unwrap().read().unwrap(),
            Some(complete)
        );
        journal.clear_complete(complete).unwrap();
        assert_eq!(journal.read().unwrap(), None);
    }
    #[test]
    fn corruption_truncation_unknown_versions_and_trailing_bytes_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::new(dir.path()).unwrap();
        let record = journal.begin().unwrap();
        let bytes = std::fs::read(&journal.path).unwrap();
        for len in 0..bytes.len() {
            std::fs::write(&journal.path, &bytes[..len]).unwrap();
            assert!(journal.read().is_err(), "length {len}");
            assert!(journal.begin().is_err());
        }
        for replacement in [8usize, 9, 10, 30] {
            let mut modified = bytes.clone();
            modified[replacement] ^= 0x7f;
            std::fs::write(&journal.path, modified).unwrap();
            assert!(journal.read().is_err());
            assert!(journal.advance(record, Stage::ConfigInProgress).is_err());
        }
        let mut extra = bytes.clone();
        extra.push(0);
        std::fs::write(&journal.path, extra).unwrap();
        assert!(journal.read().is_err());
    }
    #[test]
    fn cooperative_concurrent_starts_have_exactly_one_winner() {
        let dir = tempfile::tempdir().unwrap();
        let gate_path = dir.path().join("gate.lock");
        let barrier = Arc::new(Barrier::new(9));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let barrier = barrier.clone();
            let path = dir.path().to_path_buf();
            let gate_path = gate_path.clone();
            threads.push(std::thread::spawn(move || {
                let gate = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(gate_path)
                    .unwrap();
                barrier.wait();
                gate.lock().unwrap();
                Journal::new(&path).unwrap().begin().is_ok()
            }));
        }
        barrier.wait();
        let successes = threads
            .into_iter()
            .map(|thread| usize::from(thread.join().unwrap()))
            .sum::<usize>();
        assert_eq!(successes, 1);
        assert_eq!(
            Journal::new(dir.path())
                .unwrap()
                .read()
                .unwrap()
                .unwrap()
                .stage,
            Stage::Prepared
        );
    }
    #[test]
    fn refuse_symlink_to_external_record() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("external");
        std::fs::write(&path, b"untouched").unwrap();
        let journal = Journal::new(dir.path()).unwrap();
        symlink(&path, &journal.path).unwrap();
        assert!(journal.read().is_err());
        assert!(journal.begin().is_err());
        assert_eq!(std::fs::read(path).unwrap(), b"untouched");
    }
}
