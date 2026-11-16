//! Experimental private HMAC key and opaque journal target identifiers.
//!
//! Unconnected prototype. All methods require the caller to hold the same
//! cross-process shared gate, including key provisioning and loading. The key
//! directory and ancestors must be private, trusted and stable; this module
//! verifies the opened directory's owner, mode and identity, but cannot defend
//! against a same-user adversary, hostile ACLs on ancestors, swap-and-restore,
//! snapshot rollback, or noncooperating writers. Never log inputs or key bytes.
//!
//! `provision` is an explicit, one-time operation ONLY when the caller has
//! independently established that no journal or prior key-dependent identity
//! exists. `load` NEVER creates a missing key. If provisioning, validation or
//! syncing fails, stop and require explicit recovery; never automatically
//! replace a key or assume re-reading proves its earlier durability. A crash may
//! leave a partial file or an uncertain outcome. Key recovery, rotation,
//! cross-OS canonical input encoding, trusted placement, crash-proof key state,
//! and integration with the journal are NOT implemented. Windows fails closed.
//! P1 remains unresolved: after a failed sync, a new instance may accept the
//! uncertain key. Instance-local poison is not a cross-process safeguard.
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
}
impl Domain {
    fn tag(self) -> u8 {
        match self {
            Self::Credential => 1,
            Self::Store => 2,
            Self::Config => 3,
            Self::Generation => 4,
        }
    }
}
pub(crate) struct Key {
    bytes: Zeroizing<[u8; KEY_LEN]>,
}
impl Key {
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

pub(crate) struct KeyStore {
    path: PathBuf,
    #[cfg(unix)]
    directory: File,
    poisoned: std::cell::Cell<bool>,
    #[cfg(test)]
    fail_sync: std::cell::Cell<bool>,
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
                fail_sync: std::cell::Cell::new(false),
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
    fn file(&self, flags: i32) -> io::Result<File> {
        use std::os::fd::{AsRawFd, FromRawFd};
        let fd = unsafe {
            libc::openat(
                self.directory.as_raw_fd(),
                c"mcp-journal-identity.key".as_ptr(),
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
    /// Explicit first-time initialization. Never call merely because load failed.
    /// The caller must establish that no old journal/identity exists. A failed
    /// create leaves any created file in place to prevent implicit regeneration.
    pub(crate) fn provision(&self) -> io::Result<Key> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            if self.poisoned.get() {
                return Err(invalid(
                    "key provisioning outcome uncertain; recovery required",
                ));
            }
            self.check_directory()?;
            let mut key = Key {
                bytes: Zeroizing::new([0; KEY_LEN]),
            };
            File::open("/dev/urandom")?.read_exact(&mut key.bytes[..])?;
            if *key.bytes == [0; KEY_LEN] {
                return Err(invalid("invalid random key"));
            }
            let mut file = self.file(libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL)?;
            self.poisoned.set(true);
            self.validate_key_file(&file)?;
            let mut digest = Sha256::new();
            digest.update(MAGIC);
            digest.update(&key.bytes[..]);
            file.write_all(MAGIC)?;
            file.write_all(&key.bytes[..])?;
            file.write_all(&digest.finalize())?;
            #[cfg(test)]
            if self.fail_sync.replace(false) {
                return Err(io::Error::other("injected key sync failure"));
            }
            file.sync_all()?;
            self.directory.sync_all()?;
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
    /// Open an existing key only; missing, malformed and unsafe are all errors.
    pub(crate) fn load(&self) -> io::Result<Key> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            if self.poisoned.get() {
                return Err(invalid(
                    "key provisioning outcome uncertain; recovery required",
                ));
            }
            self.check_directory()?;
            let file = self.file(libc::O_RDONLY)?;
            self.validate_key_file(&file)?;
            let key = read_key(&file)?;
            self.check_directory()?;
            // Synchronizing an observed file does not prove an earlier failed
            // provisioning completed; the caller must still require recovery.
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
    fn failed_sync_poison_stops_store_and_leaves_no_implicit_reprovision() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let store = KeyStore::open(dir.path()).unwrap();
        store.fail_sync.set(true);
        assert!(store.provision().is_err());
        assert!(store.load().is_err());
        assert!(store.provision().is_err());
        // A fresh instance may observe the file; that observation cannot prove
        // the earlier sync succeeded and is not permission to resume.
        assert!(dir.path().join(KEY_NAME).exists());
    }
}
