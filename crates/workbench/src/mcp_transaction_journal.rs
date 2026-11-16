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
//! each action. This record contains opaque identities and a random transaction
//! ID, but no URL, credentials, raw paths, or rollback data. It cannot perform
//! recovery. Secure opaque identity derivation, effective-auth fingerprint
//! derivation, actual configuration reads, and key management are unimplemented.
//! Callers must not supply secret or low-entropy IDs. Incomplete records must
//! block work until a separately designed recovery policy resolves them. The
//! target's auth fingerprint describes the expected post-commit state.
//! An absent record alone is not evidence that no transaction ever existed.
//!
//! On Unix, updates sync a temporary file, atomically rename it in the same
//! directory, and sync that directory; removal also syncs the directory. A sync
//! error has an ambiguous outcome: stop the transaction and require explicit
//! recovery. A re-read cannot prove durability and must not resume operations.
//! Filesystem and hardware guarantees vary; external deletion or snapshot rollback cannot
//! be detected. Windows fails closed until durable directory syncing is designed.
//! The caller must inject a stable secret record MAC key, independent of keys
//! used for opaque identities. Actual credential/config binding, key durability
//! (including cross-process sync failure), recovery and Windows are unimplemented.
//! Authentication does not prevent deletion, replay or storage rollback.
//! MAC keys are wrapped with `zeroize`; HMAC's internal digest state, compiler
//! copies, registers and swap are not guaranteed to be erased.
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::{
    cell::Cell,
    fs::File,
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
use zeroize::Zeroizing;

const NAME: &str = "mcp-oauth-transaction.journal";
const MAGIC: &[u8; 8] = b"PIMCPJNL";
const VERSION: u8 = 5;
// Header + seven tagged 32-byte IDs + the previous-generation presence byte.
const PREFIX_LEN: usize = 10 + 7 * 33 + 1;
const RECORD_LEN: usize = PREFIX_LEN + 32;
const MAC_DOMAIN: &[u8] = b"pi-rs:mcp-journal-record:v1\0";

/// Caller supplies a stable secret key. Keeping that key durable, private and
/// consistently bound to the real credential store is outside this prototype.
pub(crate) struct RecordAuthenticator(Zeroizing<[u8; 32]>);
impl RecordAuthenticator {
    /// Derive a journal-only key from the identity key. This does not make
    /// provisioning, durability, recovery, or key replacement safe by itself.
    pub(crate) fn from_identity_key(key: &crate::mcp_journal_identity::Key) -> io::Result<Self> {
        let derived = key.derive(
            crate::mcp_journal_identity::Domain::RecordKey,
            &[b"record authentication key"],
        )?;
        Self::new(Zeroizing::new(derived))
    }

    pub(crate) fn new(key: Zeroizing<[u8; 32]>) -> io::Result<Self> {
        if *key == [0; 32] {
            return Err(invalid("invalid record authentication key"));
        }
        Ok(Self(key))
    }
    fn mac(&self, bytes: &[u8]) -> io::Result<Hmac<Sha256>> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0[..])
            .map_err(|_| invalid("invalid record authentication key"))?;
        mac.update(MAC_DOMAIN);
        mac.update(bytes);
        Ok(mac)
    }
    fn sign(&self, bytes: &[u8]) -> io::Result<[u8; 32]> {
        Ok(self.mac(bytes)?.finalize().into_bytes().into())
    }
    fn verify(&self, bytes: &[u8], tag: &[u8]) -> io::Result<()> {
        self.mac(bytes)?
            .verify_slice(tag)
            .map_err(|_| invalid("journal authentication failed"))
    }
}

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
/// Fixed-length opaque identifier. The caller must use privacy-preserving,
/// domain-separated IDs; the journal cannot tell a raw secret from random bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OpaqueId([u8; 32]);
impl OpaqueId {
    pub(crate) fn new(bytes: [u8; 32]) -> io::Result<Self> {
        if bytes == [0; 32] {
            return Err(invalid("zero opaque ID"));
        }
        Ok(Self(bytes))
    }
    pub(crate) fn bytes(self) -> [u8; 32] {
        self.0
    }
}
/// Caller-supplied targets, compared exactly. `previous_generation: None` is
/// permitted only after the caller has verified that no old credential exists.
/// This prototype cannot itself verify that fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Target {
    pub(crate) credential: OpaqueId,
    pub(crate) store: OpaqueId,
    pub(crate) config: OpaqueId,
    /// Fingerprint of the expected effective authentication configuration after commit.
    pub(crate) auth_fingerprint: OpaqueId,
    pub(crate) previous_generation: Option<OpaqueId>,
    pub(crate) new_generation: OpaqueId,
}
/// Stable, privacy-preserving target IDs that can be reconstructed on restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TargetScope {
    pub(crate) credential: OpaqueId,
    pub(crate) store: OpaqueId,
    pub(crate) config: OpaqueId,
}
/// Snapshot returned by inspection; it does not by itself authorize recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Inspection {
    pub(crate) stage: Stage,
    pub(crate) previous_generation: Option<OpaqueId>,
    pub(crate) new_generation: OpaqueId,
    #[cfg(test)]
    record: Record,
}
/// Test-only model of a caller-asserted observation. Production recovery is
/// unavailable until real credential/config verification and both lock
/// lifetimes are bound to a capability by the authentication implementation.
/// This type is not proof: the constructor cannot verify the external inputs.
/// The caller must independently authenticate the store and config identity,
/// read the effective auth settings and actual credential generation, and hold
/// both the shared config gate and the
/// credential lock continuously from observation through clearing. Unknown or
/// missing generations or fingerprints must be passed as None and are rejected.
#[cfg(test)]
pub(crate) struct VerifiedObservation {
    scope: TargetScope,
    generation: OpaqueId,
    auth_fingerprint: OpaqueId,
}
#[cfg(test)]
impl VerifiedObservation {
    pub(crate) fn from_caller_verified(
        scope: TargetScope,
        generation: Option<OpaqueId>,
        auth_fingerprint: Option<OpaqueId>,
    ) -> io::Result<Self> {
        Ok(Self {
            scope,
            generation: generation
                .ok_or_else(|| invalid("actual credential generation is unknown"))?,
            auth_fingerprint: auth_fingerprint
                .ok_or_else(|| invalid("effective authentication fingerprint is unknown"))?,
        })
    }
}
impl Target {
    pub(crate) fn scope(self) -> TargetScope {
        TargetScope {
            credential: self.credential,
            store: self.store,
            config: self.config,
        }
    }
}
/// Combines caller targets with the transaction ID created by `begin`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Identity {
    transaction: OpaqueId,
    target: Target,
}
impl Identity {
    /// Reconstruct a previously issued identity for matching after restart.
    /// Supplying an ID here cannot select the ID generated by a future begin.
    pub(crate) fn from_parts(transaction: OpaqueId, target: Target) -> io::Result<Self> {
        target.validate()?;
        Ok(Self {
            transaction,
            target,
        })
    }
    pub(crate) fn transaction(self) -> OpaqueId {
        self.transaction
    }
    pub(crate) fn target(self) -> Target {
        self.target
    }
}
impl Target {
    fn validate(self) -> io::Result<()> {
        if self.previous_generation == Some(self.new_generation) {
            return Err(invalid("previous and new generations must differ"));
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Record {
    identity: Identity,
    pub(crate) stage: Stage,
}
impl Record {
    /// Retain this identity for later matching, advancing and clearing.
    pub(crate) fn identity(self) -> Identity {
        self.identity
    }
    fn encode(self, auth: &RecordAuthenticator) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(RECORD_LEN);
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION);
        bytes.push(self.stage.code());
        for (kind, id) in [
            (1, self.identity.transaction),
            (2, self.identity.target.credential),
            (3, self.identity.target.store),
            (4, self.identity.target.config),
        ] {
            bytes.push(kind);
            bytes.extend_from_slice(&id.0);
        }
        bytes.push(5);
        bytes.push(u8::from(self.identity.target.previous_generation.is_some()));
        bytes.extend_from_slice(
            &self
                .identity
                .target
                .previous_generation
                .map_or([0; 32], |id| id.0),
        );
        bytes.push(6);
        bytes.extend_from_slice(&self.identity.target.new_generation.0);
        bytes.push(7);
        bytes.extend_from_slice(&self.identity.target.auth_fingerprint.0);
        debug_assert_eq!(bytes.len(), PREFIX_LEN);
        let tag = auth.sign(&bytes)?;
        bytes.extend_from_slice(&tag);
        Ok(bytes)
    }
    fn decode(bytes: &[u8], auth: &RecordAuthenticator) -> io::Result<Self> {
        if bytes.len() != RECORD_LEN {
            return Err(invalid("journal has an invalid length"));
        }
        if &bytes[..8] != MAGIC {
            return Err(invalid("invalid journal magic"));
        }
        if bytes[8] != VERSION {
            return Err(invalid("unknown journal version"));
        }
        auth.verify(&bytes[..PREFIX_LEN], &bytes[PREFIX_LEN..])?;
        let stage = Stage::parse(bytes[9])?;
        let mut offset = 10;
        let mut get = |kind: u8| -> io::Result<OpaqueId> {
            if bytes[offset] != kind {
                return Err(invalid("unknown or misplaced ID kind"));
            }
            offset += 1;
            let id = OpaqueId::new(bytes[offset..offset + 32].try_into().expect("fixed length"))?;
            offset += 32;
            Ok(id)
        };
        let transaction = get(1)?;
        let credential = get(2)?;
        let store = get(3)?;
        let config = get(4)?;
        if bytes[offset] != 5 {
            return Err(invalid("unknown or misplaced generation kind"));
        }
        offset += 1;
        let present = bytes[offset];
        offset += 1;
        let raw: [u8; 32] = bytes[offset..offset + 32].try_into().expect("fixed length");
        offset += 32;
        let previous_generation = match present {
            0 if raw == [0; 32] => None,
            1 => Some(OpaqueId::new(raw)?),
            _ => return Err(invalid("invalid previous generation")),
        };
        if bytes[offset] != 6 {
            return Err(invalid("unknown or misplaced generation kind"));
        }
        offset += 1;
        let new_generation =
            OpaqueId::new(bytes[offset..offset + 32].try_into().expect("fixed length"))?;
        offset += 32;
        if bytes[offset] != 7 {
            return Err(invalid("unknown or misplaced auth fingerprint kind"));
        }
        offset += 1;
        let auth_fingerprint =
            OpaqueId::new(bytes[offset..offset + 32].try_into().expect("fixed length"))?;
        let target = Target {
            credential,
            store,
            config,
            auth_fingerprint,
            previous_generation,
            new_generation,
        };
        target.validate()?;
        Ok(Self {
            stage,
            identity: Identity {
                transaction,
                target,
            },
        })
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn conflict(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::AlreadyExists, message)
}

pub(crate) struct Journal<'a> {
    path: PathBuf,
    auth: &'a RecordAuthenticator,
    #[cfg(unix)]
    directory: File,
    poisoned: Cell<bool>,
    #[cfg(test)]
    fail_sync: Cell<bool>,
}
impl<'a> Journal<'a> {
    /// Open and pin the existing private, trusted directory under the shared gate.
    /// Keep its pathname and ancestors stable; replacement across restarts cannot
    /// be detected from this journal alone.
    pub(crate) fn new(directory: &Path, auth: &'a RecordAuthenticator) -> io::Result<Self> {
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
                auth,
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
            let _ = (directory, auth);
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
    fn read(&self) -> io::Result<Option<Record>> {
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
            Record::decode(&bytes, self.auth).map(Some)
        }
        #[cfg(not(unix))]
        {
            Err(unsupported())
        }
    }
    /// Conservative admission probe for callers holding the shared gate.
    /// Every record, including `Complete`, requires independently verified
    /// reconciliation before unrelated operations may proceed. A corrupt or
    /// unreadable record is an error, never an absent record. `false` alone is
    /// not evidence that initial key provisioning or an earlier transaction
    /// was durable; production admission must establish those facts separately.
    pub(crate) fn needs_reconciliation(&self) -> io::Result<bool> {
        Ok(self.read()?.is_some())
    }
    /// Inspect only after the caller supplies the expected complete identity.
    /// A mismatch is an error, never an absent record or authorization to clear.
    pub(crate) fn read_matching(&self, identity: Identity) -> io::Result<Option<Record>> {
        match self.read()? {
            Some(record) if record.identity != identity => {
                Err(conflict("transaction identity does not match"))
            }
            record => Ok(record),
        }
    }
    /// Inspect the exact expected scope after restart. Generations returned by
    /// this method must still be verified before any recovery action.
    /// An absent record is not proof of durability or authorization to proceed.
    pub(crate) fn inspect_scope(&self, scope: TargetScope) -> io::Result<Option<Inspection>> {
        match self.read()? {
            Some(record) if record.identity.target.scope() != scope => {
                Err(conflict("transaction target does not match"))
            }
            Some(record) => Ok(Some(Inspection {
                stage: record.stage,
                previous_generation: record.identity.target.previous_generation,
                new_generation: record.identity.target.new_generation,
                #[cfg(test)]
                record,
            })),
            None => Ok(None),
        }
    }
    pub(crate) fn begin(&self, target: Target) -> io::Result<Record> {
        self.check_ready()?;
        target.validate()?;
        if self.read()?.is_some() {
            return Err(conflict("a transaction journal already exists"));
        }
        // Two OS-random UUIDv4 values provide 244 random bits. Callers cannot
        // supply or reuse the transaction ID; persistent storage rollback and
        // cryptographically negligible random collisions remain outside scope.
        let mut id = [0u8; 32];
        id[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        id[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        let identity = Identity {
            transaction: OpaqueId::new(id)?,
            target,
        };
        let record = Record {
            identity,
            stage: Stage::Prepared,
        };
        self.write(record)?;
        Ok(record)
    }
    pub(crate) fn advance(
        &self,
        identity: Identity,
        expected: Record,
        next: Stage,
    ) -> io::Result<Record> {
        self.check_ready()?;
        if expected.stage.next() != Some(next) {
            return Err(invalid("invalid journal stage transition"));
        }
        if self.read_matching(identity)? != Some(expected) {
            return Err(conflict("transaction journal changed"));
        }
        let record = Record {
            stage: next,
            ..expected
        };
        self.write(record)?;
        Ok(record)
    }
    pub(crate) fn clear_complete(&self, identity: Identity, expected: Record) -> io::Result<()> {
        self.check_ready()?;
        if expected.stage != Stage::Complete {
            return Err(invalid("cannot clear an incomplete transaction"));
        }
        if self.read_matching(identity)? != Some(expected) {
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
    /// Recovery-only clear of an inspected complete journal. The inspection is
    /// bound to the entire original record (including transaction ID). Re-read
    /// it before removal so stale inspections and changed stages fail closed.
    /// The caller-supplied observation is an assertion, not independently
    /// verified here; freshness depends on the locks described on its type.
    #[cfg(test)]
    pub(crate) fn clear_inspected_complete(
        &self,
        inspection: Inspection,
        observed: VerifiedObservation,
    ) -> io::Result<()> {
        let expected = inspection.record;
        if expected.stage != Stage::Complete {
            return Err(invalid("cannot recover an incomplete transaction"));
        }
        if observed.scope != expected.identity.target.scope()
            || observed.generation != expected.identity.target.new_generation
            || observed.auth_fingerprint != expected.identity.target.auth_fingerprint
        {
            return Err(conflict(
                "verified state does not match complete transaction",
            ));
        }
        self.clear_complete(expected.identity, expected)
    }
    fn write(&self, record: Record) -> io::Result<()> {
        self.check_ready()?;
        #[cfg(unix)]
        {
            use std::{ffi::CString, os::fd::AsRawFd};
            let bytes = record.encode(self.auth)?;
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

#[cfg(test)]
fn test_auth() -> &'static RecordAuthenticator {
    static AUTH: std::sync::OnceLock<RecordAuthenticator> = std::sync::OnceLock::new();
    AUTH.get_or_init(|| RecordAuthenticator::new(Zeroizing::new([0x42; 32])).unwrap())
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    #[test]
    fn reads_are_unsupported_without_touching_the_path() {
        let journal = Journal {
            path: PathBuf::from("nonexistent-journal"),
            auth: test_auth(),
            poisoned: Cell::new(false),
            fail_sync: Cell::new(false),
        };
        assert_eq!(
            journal.read().unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            Journal::new(Path::new("nonexistent-directory"), test_auth())
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
    use sha2::Digest;
    fn id(byte: u8) -> OpaqueId {
        OpaqueId::new([byte; 32]).unwrap()
    }
    fn target() -> Target {
        Target {
            credential: id(2),
            store: id(3),
            config: id(4),
            auth_fingerprint: id(8),
            previous_generation: Some(id(5)),
            new_generation: id(6),
        }
    }
    fn complete(j: &Journal, target: Target) -> Record {
        let mut r = j.begin(target).unwrap();
        for stage in [
            Stage::ConfigInProgress,
            Stage::CredentialsInProgress,
            Stage::Complete,
        ] {
            r = j.advance(r.identity(), r, stage).unwrap();
        }
        r
    }
    #[test]
    fn admission_requires_reconciliation_for_every_stage_and_rejects_corruption() {
        let root = tempfile::tempdir().unwrap();
        let journal = Journal::new(root.path(), test_auth()).unwrap();
        assert!(!journal.needs_reconciliation().unwrap());
        let mut record = journal.begin(target()).unwrap();
        assert!(journal.needs_reconciliation().unwrap());
        let wrong_auth = RecordAuthenticator::new(Zeroizing::new([0x43; 32])).unwrap();
        let wrong_key = Journal::new(root.path(), &wrong_auth).unwrap();
        assert!(wrong_key.needs_reconciliation().is_err());
        for stage in [
            Stage::ConfigInProgress,
            Stage::CredentialsInProgress,
            Stage::Complete,
        ] {
            record = journal.advance(record.identity(), record, stage).unwrap();
            assert!(journal.needs_reconciliation().unwrap());
        }
        journal.clear_complete(record.identity(), record).unwrap();
        assert!(!journal.needs_reconciliation().unwrap());
        std::fs::write(root.path().join(NAME), b"invalid").unwrap();
        assert!(journal.needs_reconciliation().is_err());

        let other = tempfile::tempdir().unwrap();
        let poisoned = Journal::new(other.path(), test_auth()).unwrap();
        poisoned.fail_sync.set(true);
        assert!(poisoned.begin(target()).is_err());
        assert!(poisoned.needs_reconciliation().is_err());
    }
    #[test]
    fn derived_record_authentication_matches_fixed_vector() {
        let master = crate::mcp_journal_identity::Key::fixture([0x42; 32]);
        let other = crate::mcp_journal_identity::Key::fixture([0x43; 32]);
        let auth = RecordAuthenticator::from_identity_key(&master).unwrap();
        let tag = auth.sign(b"canonical record").unwrap();
        assert_eq!(
            tag,
            [
                0x8e, 0x2b, 0x50, 0x11, 0x87, 0x19, 0x15, 0xa2, 0x6e, 0x88, 0xa2, 0xa8, 0x10, 0xdc,
                0xdd, 0x14, 0x77, 0x78, 0x9d, 0xe1, 0x37, 0x8b, 0xa3, 0xa8, 0x83, 0xef, 0xff, 0xaa,
                0x32, 0x0a, 0x4c, 0x71
            ]
        );
        assert!(auth.verify(b"canonical record", &tag).is_ok());
        let mut altered = tag;
        altered[0] ^= 1;
        assert!(auth.verify(b"canonical record", &altered).is_err());
        assert!(auth.verify(b"changed record", &tag).is_err());
        assert!(
            RecordAuthenticator::from_identity_key(&other)
                .unwrap()
                .verify(b"canonical record", &tag)
                .is_err()
        );
    }
    #[test]
    fn identity_key_authenticates_journal_after_reopen() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let store = crate::mcp_journal_identity::KeyStore::open(dir.path()).unwrap();
        let key = store.provision().unwrap();
        let auth = RecordAuthenticator::from_identity_key(&key).unwrap();
        let journal = Journal::new(dir.path(), &auth).unwrap();
        let record = journal.begin(target()).unwrap();
        drop(journal);
        let loaded_key = crate::mcp_journal_identity::KeyStore::open(dir.path())
            .unwrap()
            .load()
            .unwrap();
        let loaded_auth = RecordAuthenticator::from_identity_key(&loaded_key).unwrap();
        let reopened = Journal::new(dir.path(), &loaded_auth).unwrap();
        assert_eq!(
            reopened.read_matching(record.identity()).unwrap(),
            Some(record)
        );
        let other = tempfile::tempdir().unwrap();
        std::fs::set_permissions(other.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let other_key = crate::mcp_journal_identity::KeyStore::open(other.path())
            .unwrap()
            .provision()
            .unwrap();
        let other_auth = RecordAuthenticator::from_identity_key(&other_key).unwrap();
        assert!(
            Journal::new(dir.path(), &other_auth)
                .unwrap()
                .read_matching(record.identity())
                .is_err()
        );
    }
    #[test]
    fn authenticated_record_restart_wrong_key_and_tampering_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::new(dir.path(), test_auth()).unwrap();
        let record = complete(&j, target());
        let valid = std::fs::read(&j.path).unwrap();
        drop(j);
        let other = RecordAuthenticator::new(Zeroizing::new([0x43; 32])).unwrap();
        let wrong = Journal::new(dir.path(), &other).unwrap();
        assert!(wrong.read_matching(record.identity()).is_err());
        assert!(wrong.inspect_scope(target().scope()).is_err());
        assert!(wrong.begin(target()).is_err());
        let mut pretend_prepared = record;
        pretend_prepared.stage = Stage::Prepared;
        assert!(
            wrong
                .advance(record.identity(), pretend_prepared, Stage::ConfigInProgress)
                .is_err()
        );
        assert!(wrong.clear_complete(record.identity(), record).is_err());
        assert_eq!(std::fs::read(&wrong.path).unwrap(), valid);
        drop(wrong);
        let j = Journal::new(dir.path(), test_auth()).unwrap();
        assert_eq!(j.read_matching(record.identity()).unwrap(), Some(record));
        for offset in [9, 11, 44, 77, 110, 144, 177, 210, PREFIX_LEN] {
            let mut changed = valid.clone();
            changed[offset] ^= 1;
            std::fs::write(&j.path, &changed).unwrap();
            assert!(
                j.read_matching(record.identity()).is_err(),
                "offset {offset}"
            );
            assert!(j.begin(target()).is_err());
            assert!(j.clear_complete(record.identity(), record).is_err());
            assert_eq!(std::fs::read(&j.path).unwrap(), changed);
        }
        let mut old = valid.clone();
        old[8] = 4;
        let legacy_checksum = Sha256::digest(&old[..PREFIX_LEN]);
        old[PREFIX_LEN..].copy_from_slice(&legacy_checksum);
        std::fs::write(&j.path, &old).unwrap();
        assert!(j.read_matching(record.identity()).is_err());
        assert!(j.clear_complete(record.identity(), record).is_err());
        assert_eq!(std::fs::read(&j.path).unwrap(), old);
        assert!(RecordAuthenticator::new(Zeroizing::new([0; 32])).is_err());
    }
    #[test]
    fn generated_transaction_ids_prevent_stale_record_reuse() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::new(dir.path(), test_auth()).unwrap();
        let target = target();
        let old = complete(&j, target);
        let old_identity = old.identity();
        j.clear_complete(old_identity, old).unwrap();
        let new = complete(&j, target);
        let new_identity = new.identity();
        let restored = Identity::from_parts(
            OpaqueId::new(new_identity.transaction().bytes()).unwrap(),
            target,
        )
        .unwrap();
        assert_eq!(j.read_matching(restored).unwrap(), Some(new));
        assert_ne!(old_identity.transaction, new_identity.transaction);
        assert!(j.read_matching(old_identity).is_err());
        assert!(j.clear_complete(old_identity, old).is_err());
        assert_eq!(j.read_matching(new_identity).unwrap(), Some(new));
        j.clear_complete(new_identity, new).unwrap();
    }
    #[test]
    fn same_generations_rejected_on_begin_and_decode() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::new(dir.path(), test_auth()).unwrap();
        let mut bad = target();
        bad.previous_generation = Some(bad.new_generation);
        assert!(j.begin(bad).is_err());
        assert!(j.read().unwrap().is_none());
        let valid = j.begin(target()).unwrap();
        let mut forged = valid;
        forged.identity.target = bad;
        std::fs::write(&j.path, forged.encode(test_auth()).unwrap()).unwrap();
        assert!(j.read_matching(valid.identity()).is_err());
        assert!(j.begin(target()).is_err());
        assert!(
            j.advance(valid.identity(), valid, Stage::ConfigInProgress)
                .is_err()
        );
    }
    #[test]
    fn identities_and_first_login_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::new(dir.path(), test_auth()).unwrap();
        for prior in [None, Some(id(5))] {
            let mut target = target();
            target.previous_generation = prior;
            let r = complete(&j, target);
            assert_eq!(
                Journal::new(dir.path(), test_auth())
                    .unwrap()
                    .read_matching(r.identity())
                    .unwrap(),
                Some(r)
            );
            j.clear_complete(r.identity(), r).unwrap();
        }
    }
    #[test]
    fn restart_can_inspect_only_the_exact_target() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::new(dir.path(), test_auth()).unwrap();
        let record = journal.begin(target()).unwrap();
        let reopened = Journal::new(dir.path(), test_auth()).unwrap();
        assert_eq!(
            reopened.inspect_scope(target().scope()).unwrap(),
            Some(Inspection {
                stage: Stage::Prepared,
                previous_generation: target().previous_generation,
                new_generation: target().new_generation,
                record,
            })
        );
        assert_eq!(record.identity().target(), target());
        let mut wrong = target().scope();
        wrong.credential = id(7);
        assert!(reopened.inspect_scope(wrong).is_err());
        wrong = target().scope();
        wrong.store = id(7);
        assert!(reopened.inspect_scope(wrong).is_err());
        wrong = target().scope();
        wrong.config = id(7);
        assert!(reopened.inspect_scope(wrong).is_err());
        assert_eq!(
            reopened.read_matching(record.identity()).unwrap(),
            Some(record)
        );
    }

    #[test]
    fn recovery_clears_complete_after_restart_with_verified_state() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::new(dir.path(), test_auth()).unwrap();
        complete(&j, target());
        drop(j);
        let reopened = Journal::new(dir.path(), test_auth()).unwrap();
        let inspection = reopened.inspect_scope(target().scope()).unwrap().unwrap();
        let observed = VerifiedObservation::from_caller_verified(
            target().scope(),
            Some(target().new_generation),
            Some(target().auth_fingerprint),
        )
        .unwrap();
        reopened
            .clear_inspected_complete(inspection, observed)
            .unwrap();
        assert!(reopened.inspect_scope(target().scope()).unwrap().is_none());
    }
    #[test]
    fn recovery_refuses_incomplete_unknown_or_different_live_state() {
        for stage in [
            Stage::Prepared,
            Stage::ConfigInProgress,
            Stage::CredentialsInProgress,
            Stage::Complete,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let j = Journal::new(dir.path(), test_auth()).unwrap();
            let mut r = j.begin(target()).unwrap();
            for next in [
                Stage::ConfigInProgress,
                Stage::CredentialsInProgress,
                Stage::Complete,
            ] {
                if r.stage == stage {
                    break;
                }
                r = j.advance(r.identity(), r, next).unwrap();
            }
            let original = std::fs::read(&j.path).unwrap();
            let inspection = j.inspect_scope(target().scope()).unwrap().unwrap();
            assert!(
                VerifiedObservation::from_caller_verified(
                    target().scope(),
                    None,
                    Some(target().auth_fingerprint)
                )
                .is_err()
            );
            let mut wrong = target().scope();
            for field in 0..3 {
                match field {
                    0 => wrong.credential = id(7),
                    1 => wrong.store = id(7),
                    _ => wrong.config = id(7),
                }
                let obs = VerifiedObservation::from_caller_verified(
                    wrong,
                    Some(target().new_generation),
                    Some(target().auth_fingerprint),
                )
                .unwrap();
                assert!(j.clear_inspected_complete(inspection, obs).is_err());
                wrong = target().scope();
            }
            let wrong_generation = VerifiedObservation::from_caller_verified(
                target().scope(),
                Some(id(7)),
                Some(target().auth_fingerprint),
            )
            .unwrap();
            assert!(
                j.clear_inspected_complete(inspection, wrong_generation)
                    .is_err()
            );
            if stage != Stage::Complete {
                let obs = VerifiedObservation::from_caller_verified(
                    target().scope(),
                    Some(target().new_generation),
                    Some(target().auth_fingerprint),
                )
                .unwrap();
                assert!(j.clear_inspected_complete(inspection, obs).is_err());
            }
            assert_eq!(std::fs::read(&j.path).unwrap(), original);
        }
    }
    #[test]
    fn recovery_refuses_stale_or_replaced_record() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::new(dir.path(), test_auth()).unwrap();
        let old = complete(&j, target());
        let inspection = j.inspect_scope(target().scope()).unwrap().unwrap();
        j.clear_complete(old.identity(), old).unwrap();
        let new = complete(&j, target());
        assert_ne!(old.identity(), new.identity());
        let before = std::fs::read(&j.path).unwrap();
        let observed = VerifiedObservation::from_caller_verified(
            target().scope(),
            Some(target().new_generation),
            Some(target().auth_fingerprint),
        )
        .unwrap();
        assert!(j.clear_inspected_complete(inspection, observed).is_err());
        assert_eq!(std::fs::read(&j.path).unwrap(), before);
        assert_eq!(j.read_matching(new.identity()).unwrap(), Some(new));
    }
    #[test]
    fn recovery_rejects_changed_or_unknown_effective_auth_in_same_config() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::new(dir.path(), test_auth()).unwrap();
        let record = complete(&j, target());
        let inspection = j.inspect_scope(target().scope()).unwrap().unwrap();
        let before = std::fs::read(&j.path).unwrap();
        assert!(
            VerifiedObservation::from_caller_verified(
                target().scope(),
                Some(target().new_generation),
                None
            )
            .is_err()
        );
        let changed = VerifiedObservation::from_caller_verified(
            target().scope(),
            Some(target().new_generation),
            Some(id(9)),
        )
        .unwrap();
        assert!(j.clear_inspected_complete(inspection, changed).is_err());
        assert_eq!(std::fs::read(&j.path).unwrap(), before);
        let mut different = record.identity();
        different.target.auth_fingerprint = id(9);
        assert!(j.read_matching(different).is_err());
        drop(j);
        let reopened = Journal::new(dir.path(), test_auth()).unwrap();
        let inspection = reopened.inspect_scope(target().scope()).unwrap().unwrap();
        let matching = VerifiedObservation::from_caller_verified(
            target().scope(),
            Some(target().new_generation),
            Some(target().auth_fingerprint),
        )
        .unwrap();
        reopened
            .clear_inspected_complete(inspection, matching)
            .unwrap();
        assert!(reopened.read().unwrap().is_none());
    }

    #[test]
    fn wrong_target_or_generation_cannot_read_advance_or_clear() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::new(dir.path(), test_auth()).unwrap();
        let before = j.begin(target()).unwrap();
        let identity = before.identity();
        let mut wrongs = Vec::new();
        let mut x = identity;
        x.transaction = id(7);
        wrongs.push(x);
        let mut x = identity;
        x.target.credential = id(7);
        wrongs.push(x);
        let mut x = identity;
        x.target.store = id(7);
        wrongs.push(x);
        let mut x = identity;
        x.target.config = id(7);
        wrongs.push(x);
        let mut x = identity;
        x.target.previous_generation = None;
        wrongs.push(x);
        let mut x = identity;
        x.target.previous_generation = Some(id(7));
        wrongs.push(x);
        let mut x = identity;
        x.target.new_generation = id(7);
        wrongs.push(x);
        for wrong in &wrongs {
            assert!(j.read_matching(*wrong).is_err());
            assert!(j.advance(*wrong, before, Stage::ConfigInProgress).is_err());
            assert!(j.begin(wrong.target).is_err());
        }
        let mut r = before;
        for stage in [
            Stage::ConfigInProgress,
            Stage::CredentialsInProgress,
            Stage::Complete,
        ] {
            r = j.advance(identity, r, stage).unwrap();
        }
        for wrong in wrongs {
            assert!(j.clear_complete(wrong, r).is_err());
        }
        assert_eq!(j.read_matching(identity).unwrap(), Some(r));
        j.clear_complete(identity, r).unwrap();
    }
    #[test]
    fn old_truncated_unknown_kind_and_other_invalid_records_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::new(dir.path(), test_auth()).unwrap();
        let record = j.begin(target()).unwrap();
        let identity = record.identity();
        let valid = std::fs::read(&j.path).unwrap();
        for len in 0..valid.len() {
            std::fs::write(&j.path, &valid[..len]).unwrap();
            assert!(j.read_matching(identity).is_err());
            assert!(j.begin(target()).is_err());
        }
        for version in [1, 2, 3, 4, 99] {
            let mut old = valid.clone();
            old[8] = version;
            let tag = test_auth().sign(&old[..PREFIX_LEN]).unwrap();
            old[PREFIX_LEN..].copy_from_slice(&tag);
            std::fs::write(&j.path, old).unwrap();
            assert!(j.read_matching(identity).is_err());
        }
        // Version 3 did not have the tagged auth fingerprint.
        let mut v3 = valid[..209].to_vec();
        v3[8] = 3;
        let checksum = Sha256::digest(&v3);
        v3.extend_from_slice(&checksum);
        std::fs::write(&j.path, v3).unwrap();
        assert!(j.read_matching(identity).is_err());
        for offset in [10, 43, 76, 109, 142, 176, 209] {
            let mut corrupt = valid.clone();
            corrupt[offset] = 99;
            let tag = test_auth().sign(&corrupt[..PREFIX_LEN]).unwrap();
            corrupt[PREFIX_LEN..].copy_from_slice(&tag);
            std::fs::write(&j.path, corrupt).unwrap();
            assert!(j.read_matching(identity).is_err());
            assert!(
                j.advance(identity, record, Stage::ConfigInProgress)
                    .is_err()
            );
        }
        let mut extra = valid.clone();
        extra.push(0);
        std::fs::write(&j.path, extra).unwrap();
        assert!(j.read_matching(identity).is_err());
        assert!(OpaqueId::new([0; 32]).is_err());
    }
    #[test]
    fn shared_gate_serializes_starts() {
        use std::sync::{Arc, Barrier};
        let dir = tempfile::tempdir().unwrap();
        let barrier = Arc::new(Barrier::new(5));
        let mut threads = Vec::new();
        for _ in 0..4 {
            let path = dir.path().to_path_buf();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                let gate = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(path.join("gate"))
                    .unwrap();
                barrier.wait();
                gate.lock().unwrap();
                Journal::new(&path, test_auth())
                    .unwrap()
                    .begin(target())
                    .is_ok()
            }));
        }
        barrier.wait();
        assert_eq!(
            threads
                .into_iter()
                .map(|t| usize::from(t.join().unwrap()))
                .sum::<usize>(),
            1
        );
    }
    #[test]
    fn directory_replacement_and_sync_failure_are_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("journal");
        std::fs::create_dir(&path).unwrap();
        let j = Journal::new(&path, test_auth()).unwrap();
        std::fs::rename(&path, root.path().join("moved")).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(j.read().is_err());
        assert!(j.begin(target()).is_err());
        let j = Journal::new(&path, test_auth()).unwrap();
        j.fail_sync.set(true);
        assert!(j.begin(target()).is_err());
        assert!(j.read().is_err());
    }
    #[test]
    fn sync_failure_during_advance_and_clear_poison_instance() {
        for clear in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let j = Journal::new(dir.path(), test_auth()).unwrap();
            let r = if clear {
                complete(&j, target())
            } else {
                j.begin(target()).unwrap()
            };
            j.fail_sync.set(true);
            if clear {
                assert!(j.clear_complete(r.identity(), r).is_err());
            } else {
                assert!(j.advance(r.identity(), r, Stage::ConfigInProgress).is_err());
            }
            assert!(j.read().is_err());
            assert!(j.begin(target()).is_err());
        }
    }
    #[test]
    fn reject_symlink_journal_and_stale_stage() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let external = dir.path().join("external");
        std::fs::write(&external, b"untouched").unwrap();
        let j = Journal::new(dir.path(), test_auth()).unwrap();
        symlink(&external, &j.path).unwrap();
        assert!(j.begin(target()).is_err());
        assert_eq!(std::fs::read(&external).unwrap(), b"untouched");
        std::fs::remove_file(&j.path).unwrap();
        let old = j.begin(target()).unwrap();
        j.advance(old.identity(), old, Stage::ConfigInProgress)
            .unwrap();
        assert!(
            j.advance(old.identity(), old, Stage::ConfigInProgress)
                .is_err()
        );
    }
}
