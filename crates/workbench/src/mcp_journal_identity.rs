//! Experimental private HMAC key and opaque journal target identifiers.
//!
//! Unconnected prototype. All methods require the caller to hold the same
//! cross-process shared gate, including key provisioning and loading. The key
//! directory and ancestors must be private, trusted and stable; this module
//! verifies the opened directory's owner, mode and identity, but cannot defend
//! against a same-user adversary, hostile ACLs on ancestors, swap-and-restore,
//! snapshot rollback, or noncooperating writers. Never log inputs or key bytes.
//!
//! `provision` requires independent proof that no prior journal, credential or
//! key-dependent identity exists; absence of files is not such proof. Published
//! proof is created only after the key and its directory entry were synced.
//! A visible, valid proof may be accepted after the final proof directory sync
//! failed; after a crash it may disappear, in which case load fails closed.
//! No automatic key/proof regeneration or recovery is implemented. The proof
//! relies on successful sync remaining durable despite later failures, a trusted
//! filesystem/device honoring sync and ordering, stable durable ancestors, and
//! all participants holding the same gate through credential/journal binding.
//! I/O corruption, hostile changes, rollback, and loss of previously durable
//! data are outside this model; a hash does not authenticate an adversary.
//! Existing journal/credential binding, recovery, rotation and integration are
//! NOT implemented. Linux and macOS only; Windows fails closed. On macOS file
//! sync requests F_FULLFSYNC; directory sync and crash guarantees still depend
//! on the filesystem and hardware. This is not a production durability claim.
//! `zeroize` protects explicitly wrapped buffers on drop, including error paths;
//! hmac 0.12 / sha2 0.10 internal digest states, compiler copies, registers and
//! swap are not guaranteed to be erased.
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
use zeroize::Zeroizing;

const KEY_NAME: &str = "mcp-journal-identity.key";
const MAGIC: &[u8; 8] = b"PIJKEY01";
const PROOF_MAGIC: &[u8; 8] = b"PIJPRF01";
const PROOF_DOMAIN: &[u8] = b"pi-rs:mcp-journal-key-proof:v1\0";
const PROOF_LEN: usize = 8 + 32;
const KEY_LEN: usize = 32;
const FILE_LEN: usize = 8 + KEY_LEN + 32;
const CONTEXT: &[u8] = b"pi-rs:mcp-journal-identity:v1";

/// Disjoint namespaces: an identifier derived for one role cannot be used as
/// another. Generation IDs must describe the actual credential generation.
#[derive(Clone, Copy)]
pub(crate) enum Domain {
    Credential,
    Store,
    Config,
    Generation,
    Authentication,
    RecordKey,
}
impl Domain {
    fn tag(self) -> u8 {
        match self {
            Self::Credential => 1,
            Self::Store => 2,
            Self::Config => 3,
            Self::Generation => 4,
            Self::Authentication => 5,
            Self::RecordKey => 6,
        }
    }
}
pub(crate) struct Key {
    bytes: Zeroizing<[u8; KEY_LEN]>,
}
impl Key {
    #[cfg(test)]
    pub(crate) fn fixture(bytes: [u8; 32]) -> Self {
        Self {
            bytes: Zeroizing::new(bytes),
        }
    }

    /// Length-delimited raw components are never retained. Their encoding and
    /// normalization must be agreed by callers; values can be low entropy.
    pub(crate) fn derive(&self, domain: Domain, components: &[&[u8]]) -> io::Result<[u8; 32]> {
        if components.is_empty() {
            return Err(invalid("identity requires components"));
        }
        let count = u64::try_from(components.len()).map_err(|_| invalid("too many components"))?;
        let mut inner = Hmac::<Sha256>::new_from_slice(&self.bytes[..])
            .map_err(|_| invalid("invalid HMAC key"))?;
        inner.update(CONTEXT);
        inner.update(&[domain.tag()]);
        inner.update(&count.to_be_bytes());
        for component in components {
            let len = u64::try_from(component.len()).map_err(|_| invalid("component too long"))?;
            inner.update(&len.to_be_bytes());
            inner.update(component);
        }
        Ok(inner.finalize().into_bytes().into())
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn denied(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailPoint {
    KeyWrite,
    KeySync,
    KeyPublish,
    KeyUnlink,
    KeyDirSync,
    ProofWrite,
    ProofSync,
    ProofPublish,
    ProofUnlink,
    ProofDirSync,
}

pub(crate) struct KeyStore {
    path: PathBuf,
    #[cfg(unix)]
    directory: File,
    poisoned: std::cell::Cell<bool>,
    #[cfg(test)]
    fail_at: std::cell::Cell<Option<FailPoint>>,
}
impl KeyStore {
    pub(crate) fn open(directory: &Path) -> io::Result<Self> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let dir = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(directory)?;
            let store = Self {
                path: directory.to_path_buf(),
                directory: dir,
                poisoned: std::cell::Cell::new(false),
                #[cfg(test)]
                fail_at: std::cell::Cell::new(None),
            };
            store.check_directory()?;
            Ok(store)
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = directory;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "journal key store unavailable",
            ))
        }
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn check_directory(&self) -> io::Result<()> {
        use std::os::unix::fs::MetadataExt;
        let meta = self.directory.metadata()?;
        let current = std::fs::symlink_metadata(&self.path)
            .map_err(|_| denied("key directory unavailable"))?;
        if !meta.is_dir()
            || meta.uid() != unsafe { libc::geteuid() }
            || meta.mode() & 0o7777 != 0o700
            || meta.nlink() == 0
            || !current.file_type().is_dir()
            || current.dev() != meta.dev()
            || current.ino() != meta.ino()
        {
            return Err(denied("unsafe key directory"));
        }
        check_no_acl(&self.directory)?;
        Ok(())
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn file(&self, name: &std::ffi::CStr, flags: i32) -> io::Result<File> {
        use std::os::fd::{AsRawFd, FromRawFd};
        let fd = unsafe {
            libc::openat(
                self.directory.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
                0o600 as libc::c_uint,
            )
        };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(unsafe { File::from_raw_fd(fd) })
        }
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn validate_key_file(&self, file: &File) -> io::Result<()> {
        use std::os::unix::fs::MetadataExt;
        let meta = file.metadata()?;
        if !meta.is_file()
            || meta.uid() != unsafe { libc::geteuid() }
            || meta.mode() & 0o7777 != 0o600
            || meta.nlink() != 1
        {
            return Err(denied("unsafe journal key file"));
        }
        check_no_acl(file)
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn absent(&self, name: &std::ffi::CStr) -> io::Result<()> {
        use std::os::fd::AsRawFd;
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        let result = unsafe {
            libc::fstatat(
                self.directory.as_raw_fd(),
                name.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if result == 0 {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "journal key object already exists",
            ));
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            Ok(())
        } else {
            Err(error)
        }
    }
    // linkat is atomic and refuses an existing destination. Temp and final
    // are in the same trusted directory. A crash between link and unlink
    // leaves nlink=2, which validation rejects until explicit recovery.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn publish(&self, temp: &std::ffi::CStr, name: &std::ffi::CStr) -> io::Result<()> {
        use std::os::fd::AsRawFd;
        let dir = self.directory.as_raw_fd();
        if unsafe { libc::linkat(dir, temp.as_ptr(), dir, name.as_ptr(), 0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        #[cfg(test)]
        self.inject(if name == c"mcp-journal-identity.key" {
            FailPoint::KeyUnlink
        } else {
            FailPoint::ProofUnlink
        })?;
        if unsafe { libc::unlinkat(dir, temp.as_ptr(), 0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn check_named(&self, name: &std::ffi::CStr, opened: &File) -> io::Result<()> {
        use std::os::unix::fs::MetadataExt;
        let current = self.file(name, libc::O_RDONLY)?;
        self.validate_key_file(&current)?;
        let a = opened.metadata()?;
        let b = current.metadata()?;
        if a.dev() != b.dev() || a.ino() != b.ino() {
            return Err(denied("journal key object changed"));
        }
        Ok(())
    }
    #[cfg(test)]
    fn inject(&self, point: FailPoint) -> io::Result<()> {
        if self.fail_at.get() == Some(point) {
            return Err(io::Error::other("injected provisioning failure"));
        }
        Ok(())
    }
    /// Caller independently establishes first-time initialization. Existing
    /// names and leftovers are never overwritten or cleaned automatically.
    pub(crate) fn provision(&self) -> io::Result<Key> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            if self.poisoned.get() {
                return Err(invalid(
                    "key provisioning outcome uncertain; recovery required",
                ));
            }
            self.check_directory()?;
            for name in [
                c"mcp-journal-identity.key",
                c"mcp-journal-identity.proof",
                c"mcp-journal-identity.key.tmp",
                c"mcp-journal-identity.proof.tmp",
            ] {
                self.absent(name)?;
            }
            let mut key = Key {
                bytes: Zeroizing::new([0; KEY_LEN]),
            };
            File::open("/dev/urandom")?.read_exact(&mut key.bytes[..])?;
            if *key.bytes == [0; KEY_LEN] {
                return Err(invalid("invalid random key"));
            }
            let mut file = self.file(
                c"mcp-journal-identity.key.tmp",
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            )?;
            self.poisoned.set(true);
            self.validate_key_file(&file)?;
            let mut digest = Sha256::new();
            digest.update(MAGIC);
            digest.update(&key.bytes[..]);
            file.write_all(MAGIC)?;
            #[cfg(test)]
            self.inject(FailPoint::KeyWrite)?;
            file.write_all(&key.bytes[..])?;
            file.write_all(&digest.finalize())?;
            #[cfg(test)]
            self.inject(FailPoint::KeySync)?;
            sync_file(&file)?;
            #[cfg(test)]
            self.inject(FailPoint::KeyPublish)?;
            self.publish(c"mcp-journal-identity.key.tmp", c"mcp-journal-identity.key")?;
            self.check_named(c"mcp-journal-identity.key", &file)?;
            #[cfg(test)]
            self.inject(FailPoint::KeyDirSync)?;
            self.directory.sync_all()?;
            self.check_directory()?;
            let mut proof = self.file(
                c"mcp-journal-identity.proof.tmp",
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            )?;
            self.validate_key_file(&proof)?;
            proof.write_all(PROOF_MAGIC)?;
            #[cfg(test)]
            self.inject(FailPoint::ProofWrite)?;
            proof.write_all(&proof_digest(&key))?;
            #[cfg(test)]
            self.inject(FailPoint::ProofSync)?;
            sync_file(&proof)?;
            #[cfg(test)]
            self.inject(FailPoint::ProofPublish)?;
            self.publish(
                c"mcp-journal-identity.proof.tmp",
                c"mcp-journal-identity.proof",
            )?;
            self.check_named(c"mcp-journal-identity.proof", &proof)?;
            #[cfg(test)]
            self.inject(FailPoint::ProofDirSync)?;
            self.directory.sync_all()?;
            self.check_named(c"mcp-journal-identity.key", &file)?;
            self.check_named(c"mcp-journal-identity.proof", &proof)?;
            self.check_directory()?;
            self.poisoned.set(false);
            Ok(key)
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "journal key store unavailable",
            ))
        }
    }
    /// A valid proof binds the key to the earlier successful sync sequence.
    /// Missing, malformed, unsafe or mismatched objects fail closed.
    pub(crate) fn load(&self) -> io::Result<Key> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            if self.poisoned.get() {
                return Err(invalid(
                    "key provisioning outcome uncertain; recovery required",
                ));
            }
            self.check_directory()?;
            let file = self.file(c"mcp-journal-identity.key", libc::O_RDONLY)?;
            self.validate_key_file(&file)?;
            let key = read_key(&file)?;
            let mut proof = self.file(c"mcp-journal-identity.proof", libc::O_RDONLY)?;
            self.validate_key_file(&proof)?;
            let mut bytes = [0u8; PROOF_LEN + 1];
            let mut used = 0;
            while used < bytes.len() {
                let n = proof.read(&mut bytes[used..])?;
                if n == 0 {
                    break;
                }
                used += n;
            }
            if used != PROOF_LEN
                || &bytes[..8] != PROOF_MAGIC
                || bytes[8..PROOF_LEN] != proof_digest(&key)
            {
                return Err(invalid("invalid journal key proof"));
            }
            self.check_named(c"mcp-journal-identity.key", &file)?;
            self.check_named(c"mcp-journal-identity.proof", &proof)?;
            self.check_directory()?;
            Ok(key)
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "journal key store unavailable",
            ))
        }
    }
}

fn proof_digest(key: &Key) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(PROOF_DOMAIN);
    digest.update(MAGIC);
    digest.update(&key.bytes[..]);
    digest.finalize().into()
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn sync_file(file: &File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) } == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(target_os = "linux")]
    {
        file.sync_all()
    }
}

fn read_key(mut reader: impl Read) -> io::Result<Key> {
    let mut raw = Zeroizing::new([0u8; FILE_LEN + 1]);
    let mut used = 0;
    while used < raw.len() {
        let n = reader.read(&mut raw[used..])?;
        if n == 0 {
            break;
        }
        used += n;
    }
    if used != FILE_LEN || &raw[..8] != MAGIC {
        return Err(invalid("invalid journal key file"));
    }
    let mut key = Key {
        bytes: Zeroizing::new([0; KEY_LEN]),
    };
    key.bytes.copy_from_slice(&raw[8..40]);
    let digest = Sha256::digest(&raw[..40]);
    if *key.bytes == [0; KEY_LEN] || digest.as_slice() != &raw[40..FILE_LEN] {
        return Err(invalid("invalid journal key checksum"));
    }
    Ok(key)
}
#[cfg(target_os = "linux")]
fn check_no_acl(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    for name in [c"system.posix_acl_access", c"system.posix_acl_default"] {
        let result =
            unsafe { libc::fgetxattr(file.as_raw_fd(), name.as_ptr(), std::ptr::null_mut(), 0) };
        if result >= 0 {
            return Err(denied("journal key object has ACL"));
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ENODATA) {
            return Err(error);
        }
    }
    Ok(())
}
#[cfg(target_os = "macos")]
fn check_no_acl(file: &File) -> io::Result<()> {
    use std::{ffi::c_void, os::fd::AsRawFd};
    unsafe extern "C" {
        fn acl_get_fd_np(fd: i32, kind: i32) -> *mut c_void;
        fn acl_free(acl: *mut c_void) -> i32;
    }
    let acl = unsafe { acl_get_fd_np(file.as_raw_fd(), 0x00000100) };
    if !acl.is_null() {
        unsafe {
            acl_free(acl);
        }
        return Err(denied("journal key object has ACL"));
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ENOENT) {
        Ok(())
    } else {
        Err(error)
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};
    #[test]
    fn hmac_matches_rfc4231_case_one() {
        let rfc_key = Zeroizing::new([0x0bu8; 20]);
        let mut h = Hmac::<Sha256>::new_from_slice(&rfc_key[..]).unwrap();
        h.update(b"Hi There");
        assert_eq!(
            <[u8; 32]>::from(h.finalize().into_bytes()),
            [
                0xb0, 0x34, 0x4c, 0x61, 0xd8, 0xdb, 0x38, 0x53, 0x5c, 0xa8, 0xaf, 0xce, 0xaf, 0x0b,
                0xf1, 0x2b, 0x88, 0x1d, 0xc2, 0x00, 0xc9, 0x83, 0x3d, 0xa7, 0x26, 0xe9, 0x37, 0x6c,
                0x2e, 0x32, 0xcf, 0xf7
            ]
        );
    }
    #[test]
    fn fixed_derivation_is_compatible() {
        let key = Key {
            bytes: Zeroizing::new([0x42; 32]),
        };
        assert_eq!(
            key.derive(Domain::Credential, &[b"ab", b"c"]).unwrap(),
            [
                0xa3, 0x63, 0xf6, 0x0c, 0x7c, 0x57, 0x3a, 0x52, 0x0b, 0x15, 0x04, 0x8a, 0x4f, 0x8b,
                0x94, 0xc5, 0x97, 0xc2, 0x1c, 0xa5, 0x79, 0xcd, 0x95, 0xb4, 0x17, 0x12, 0xef, 0x3e,
                0xfd, 0xbd, 0x19, 0x36,
            ]
        );
    }

    #[test]
    fn partial_read_error_is_propagated() {
        struct FailingReader(bool);
        impl Read for FailingReader {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.0 {
                    return Err(io::Error::other("injected read failure"));
                }
                self.0 = true;
                buf[..16].fill(0x5a);
                Ok(16)
            }
        }
        assert_eq!(
            read_key(FailingReader(false)).err().unwrap().to_string(),
            "injected read failure"
        );
    }
    #[test]
    fn provision_load_domain_separation_and_framing() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let store = KeyStore::open(dir.path()).unwrap();
        assert_eq!(store.load().err().unwrap().kind(), io::ErrorKind::NotFound);
        let key = store.provision().unwrap();
        let a = key.derive(Domain::Credential, &[b"ab", b"c"]).unwrap();
        let loaded = KeyStore::open(dir.path()).unwrap().load().unwrap();
        assert_eq!(
            a,
            loaded.derive(Domain::Credential, &[b"ab", b"c"]).unwrap()
        );
        assert_ne!(
            a,
            loaded.derive(Domain::Credential, &[b"a", b"bc"]).unwrap()
        );
        assert_ne!(a, loaded.derive(Domain::Store, &[b"ab", b"c"]).unwrap());
        assert_ne!(a, loaded.derive(Domain::Config, &[b"ab", b"c"]).unwrap());
        assert_ne!(
            loaded.derive(Domain::Config, &[b"ab", b"c"]).unwrap(),
            loaded
                .derive(Domain::Authentication, &[b"ab", b"c"])
                .unwrap()
        );
        assert_ne!(
            a,
            loaded.derive(Domain::Generation, &[b"ab", b"c"]).unwrap()
        );
        assert!(loaded.derive(Domain::Config, &[]).is_err());
        assert!(store.provision().is_err());
    }
    #[test]
    fn missing_truncated_corrupt_symlink_hardlink_and_permissions_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let store = KeyStore::open(dir.path()).unwrap();
        store.provision().unwrap();
        let path = dir.path().join(KEY_NAME);
        let bytes = std::fs::read(&path).unwrap();
        for size in 0..bytes.len() {
            std::fs::write(&path, &bytes[..size]).unwrap();
            assert!(store.load().is_err());
            assert!(store.provision().is_err());
        }
        let mut bad = bytes.clone();
        bad[10] ^= 1;
        std::fs::write(&path, bad).unwrap();
        assert!(store.load().is_err());
        let mut extra = bytes.clone();
        extra.push(0);
        std::fs::write(&path, extra).unwrap();
        assert!(store.load().is_err());
        std::fs::write(&path, &bytes).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(store.load().is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::hard_link(&path, dir.path().join("hard")).unwrap();
        assert!(store.load().is_err());
        std::fs::remove_file(&path).unwrap();
        symlink(dir.path().join("hard"), &path).unwrap();
        assert!(store.load().is_err());
        assert!(store.provision().is_err());
        std::fs::remove_file(&path).unwrap();
        assert!(store.load().is_err());
    }
    #[test]
    fn replacement_directory_does_not_recreate_key() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("private");
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let store = KeyStore::open(&path).unwrap();
        store.provision().unwrap();
        std::fs::rename(&path, root.path().join("old")).unwrap();
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(store.load().is_err());
        assert!(store.provision().is_err());
        assert!(!path.join(KEY_NAME).exists());
    }
    #[test]
    fn failures_do_not_publish_unproven_keys_across_instances() {
        for point in [
            FailPoint::KeyWrite,
            FailPoint::KeySync,
            FailPoint::KeyPublish,
            FailPoint::KeyUnlink,
            FailPoint::KeyDirSync,
            FailPoint::ProofWrite,
            FailPoint::ProofSync,
            FailPoint::ProofPublish,
            FailPoint::ProofUnlink,
            FailPoint::ProofDirSync,
        ] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            let store = KeyStore::open(dir.path()).unwrap();
            store.fail_at.set(Some(point));
            assert!(store.provision().is_err(), "{point:?}");
            assert!(store.load().is_err());
            let fresh = KeyStore::open(dir.path()).unwrap();
            if point == FailPoint::ProofDirSync {
                assert!(
                    fresh.load().is_ok(),
                    "visible proof follows successful key sync"
                );
            } else {
                assert!(fresh.load().is_err(), "{point:?}");
            }
            assert!(fresh.provision().is_err(), "{point:?}");
            if point == FailPoint::ProofDirSync {
                // Simulate loss of the unsynced proof name on crash.
                std::fs::remove_file(dir.path().join("mcp-journal-identity.proof")).unwrap();
                let after_crash = KeyStore::open(dir.path()).unwrap();
                assert!(after_crash.load().is_err());
                assert!(after_crash.provision().is_err());
            }
        }
    }
    #[test]
    fn failed_provision_is_checked_in_another_process() {
        struct ChildGuard(std::process::Child);
        impl Drop for ChildGuard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        const CHILD: &str = "PI_RS_TEST_KEY_PROOF_CHILD";
        const DIRECTORY: &str = "PI_RS_TEST_KEY_PROOF_DIRECTORY";
        if let Ok(point) = std::env::var(CHILD) {
            let path = PathBuf::from(std::env::var_os(DIRECTORY).unwrap());
            let store = KeyStore::open(&path).unwrap();
            store.fail_at.set(Some(match point.as_str() {
                "key-sync" => FailPoint::KeySync,
                "proof-dir-sync" => FailPoint::ProofDirSync,
                _ => panic!("unexpected failpoint"),
            }));
            assert!(store.provision().is_err());
            // Model an abrupt process end: do not rely on dropping the store.
            std::process::exit(0);
        }
        for (point, visible_proof) in [("key-sync", false), ("proof-dir-sync", true)] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            let mut child = ChildGuard(std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "mcp_journal_identity::tests::failed_provision_is_checked_in_another_process",
                ])
                .env(CHILD, point)
                .env(DIRECTORY, dir.path())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap());
            let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                if let Some(status) = child.0.try_wait().unwrap() {
                    assert!(status.success(), "child failed at {point}: {status}");
                    break;
                }
                if std::time::Instant::now() >= until {
                    panic!("child timed out at {point}");
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            let fresh = KeyStore::open(dir.path()).unwrap();
            assert_eq!(fresh.load().is_ok(), visible_proof, "{point}");
            assert!(fresh.provision().is_err(), "{point}");
        }
    }
    #[test]
    fn proof_missing_corrupt_mismatched_and_existing_names_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let store = KeyStore::open(dir.path()).unwrap();
        store.provision().unwrap();
        let key_path = dir.path().join(KEY_NAME);
        let proof_path = dir.path().join("mcp-journal-identity.proof");
        let key = std::fs::read(&key_path).unwrap();
        let proof = std::fs::read(&proof_path).unwrap();
        for size in 0..proof.len() {
            std::fs::write(&proof_path, &proof[..size]).unwrap();
            assert!(KeyStore::open(dir.path()).unwrap().load().is_err());
        }
        let mut corrupt = proof.clone();
        corrupt[8] ^= 1;
        std::fs::write(&proof_path, corrupt).unwrap();
        assert!(KeyStore::open(dir.path()).unwrap().load().is_err());
        std::fs::write(&proof_path, &proof).unwrap();
        let mut extra = proof.clone();
        extra.push(0);
        std::fs::write(&proof_path, &extra).unwrap();
        assert!(KeyStore::open(dir.path()).unwrap().load().is_err());
        std::fs::write(&proof_path, &proof).unwrap();
        let hard = dir.path().join("proof-hard");
        std::fs::hard_link(&proof_path, &hard).unwrap();
        assert!(KeyStore::open(dir.path()).unwrap().load().is_err());
        std::fs::remove_file(&hard).unwrap();
        std::fs::rename(&proof_path, &hard).unwrap();
        symlink(&hard, &proof_path).unwrap();
        assert!(KeyStore::open(dir.path()).unwrap().load().is_err());
        std::fs::remove_file(&proof_path).unwrap();
        std::fs::rename(&hard, &proof_path).unwrap();
        let other = tempfile::tempdir().unwrap();
        std::fs::set_permissions(other.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        KeyStore::open(other.path()).unwrap().provision().unwrap();
        std::fs::write(
            &key_path,
            std::fs::read(other.path().join(KEY_NAME)).unwrap(),
        )
        .unwrap();
        assert!(KeyStore::open(dir.path()).unwrap().load().is_err());
        std::fs::write(&key_path, &key).unwrap();
        std::fs::remove_file(&proof_path).unwrap();
        assert!(KeyStore::open(dir.path()).unwrap().load().is_err());
        assert!(KeyStore::open(dir.path()).unwrap().provision().is_err());
        assert_eq!(std::fs::read(&key_path).unwrap(), key);
        std::fs::remove_file(&key_path).unwrap();
        std::fs::write(&proof_path, &proof).unwrap();
        assert!(KeyStore::open(dir.path()).unwrap().provision().is_err());
        assert!(!key_path.exists());
        assert_eq!(std::fs::read(&proof_path).unwrap(), proof);
    }
}
