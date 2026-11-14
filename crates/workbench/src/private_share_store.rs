//! Private, fail-closed storage for a last-live share snapshot.
//!
//! This storage primitive is not yet connected to standalone sharing. Callers
//! must supply a complete provenance digest and invalidate before any runtime
//! side effects. Other platforms deliberately return unsupported.

use serde::{Deserialize, Serialize};
#[cfg(unix)]
use serde_json::Value;

/// Exact identity of the journal and the environment that produced a snapshot.
/// The provenance digest must cover all inputs relevant to the effective context.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub session_id: uuid::Uuid,
    pub head: u64,
    pub journal_digest: [u8; 32],
    pub provenance_digest: [u8; 32],
}

#[cfg(unix)]
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u32,
    binding: Binding,
    context: Value,
    tag: [u8; 32],
}

#[cfg(unix)]
mod unix {
    use super::{Binding, Envelope, Value};
    use hmac::{Hmac, Mac};
    use serde::{Serialize, de::DeserializeOwned};
    use sha2::Sha256;
    use std::ffi::CString;
    use std::fs::{File, OpenOptions};
    use std::io::{self, Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    use std::path::Path;

    const MAX_CACHE_BYTES: u64 = 8 * 1024 * 1024;

    /// All operations remain relative to a verified private directory handle.
    pub struct Store {
        directory: File,
        key: [u8; 32],
    }

    /// An exclusive generation invalidates any previous snapshot before the
    /// caller is permitted to start the associated live runtime.
    pub struct Generation<'a> {
        store: &'a Store,
        lock: File,
        name: CString,
        binding: Binding,
    }

    fn invalid() -> io::Error {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            "untrusted share cache storage",
        )
    }

    #[cfg(target_os = "macos")]
    fn reject_extended_acl(file: &File) -> io::Result<()> {
        unsafe extern "C" {
            fn acl_get_fd_np(fd: libc::c_int, kind: libc::c_int) -> *mut libc::c_void;
            fn acl_free(acl: *mut libc::c_void) -> libc::c_int;
        }
        // SAFETY: file owns a live descriptor; ACL_TYPE_EXTENDED is 0x100 on Darwin.
        let acl = unsafe { acl_get_fd_np(file.as_raw_fd(), 0x100) };
        if acl.is_null() {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(libc::ENOENT) {
                Ok(())
            } else {
                Err(error)
            };
        }
        // SAFETY: acl_get_fd_np returned an allocated ACL, released exactly once.
        unsafe {
            acl_free(acl);
        }
        Err(invalid())
    }

    #[cfg(target_os = "linux")]
    fn reject_extended_acl(_: &File) -> io::Result<()> {
        // On Linux the ACL mask is represented in group mode bits; the mode
        // checks below deny any effective access outside the owner.
        Ok(())
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    fn reject_extended_acl(_: &File) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "unverified ACL semantics",
        ))
    }

    fn secure_dir(file: &File) -> io::Result<()> {
        let meta = file.metadata()?;
        // SAFETY: geteuid has no arguments or preconditions.
        if !meta.is_dir()
            || meta.uid() != unsafe { libc::geteuid() }
            || meta.mode() & 0o777 != 0o700
        {
            return Err(invalid());
        }
        reject_extended_acl(file)
    }

    fn secure_parent(file: &File) -> io::Result<()> {
        let meta = file.metadata()?;
        // SAFETY: geteuid has no arguments or preconditions.
        if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o022 != 0 {
            return Err(invalid());
        }
        reject_extended_acl(file)
    }

    fn secure_file(file: &File) -> io::Result<()> {
        let meta = file.metadata()?;
        // SAFETY: geteuid has no arguments or preconditions.
        if !meta.is_file()
            || meta.uid() != unsafe { libc::geteuid() }
            || meta.nlink() != 1
            || meta.mode() & 0o077 != 0
        {
            return Err(invalid());
        }
        reject_extended_acl(file)
    }

    fn open_at(
        dir: &File,
        name: &CString,
        flags: libc::c_int,
        mode: libc::mode_t,
    ) -> io::Result<File> {
        // SAFETY: directory is a live directory fd and name is NUL terminated.
        let fd = unsafe {
            libc::openat(
                dir.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                mode as libc::c_uint,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: openat returned a new owned descriptor.
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    fn unlink_at(dir: &File, name: &CString) -> io::Result<()> {
        // SAFETY: directory and NUL-terminated name remain valid for the call.
        if unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), 0) } == 0 {
            Ok(())
        } else {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::NotFound {
                Ok(())
            } else {
                Err(error)
            }
        }
    }

    fn keyed_digest<T: Serialize>(key: &[u8; 32], domain: &str, value: &T) -> io::Result<[u8; 32]> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts 32 bytes");
        mac.update(domain.as_bytes());
        mac.update(&[0]);
        mac.update(&serde_json::to_vec(value)?);
        Ok(mac.finalize().into_bytes().into())
    }

    fn name_for(
        key: &[u8; 32],
        session_path: &Path,
        id: uuid::Uuid,
        suffix: &str,
    ) -> io::Result<CString> {
        let path = session_path.as_os_str().as_bytes();
        let digest = keyed_digest(key, "share-session-v1", &(path, id))?;
        let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        CString::new(format!("{hex}.{suffix}"))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid cache name"))
    }

    fn read_key(directory: &File) -> io::Result<Option<[u8; 32]>> {
        let name = CString::new("key-v1").unwrap();
        let mut file = match open_at(directory, &name, libc::O_RDONLY | libc::O_NONBLOCK, 0) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        secure_file(&file)?;
        if file.metadata()?.len() != 32 {
            return Err(invalid());
        }
        let mut key = [0; 32];
        file.read_exact(&mut key)?;
        Ok(Some(key))
    }

    fn load_or_create_key(directory: &File) -> io::Result<[u8; 32]> {
        let lock_name = CString::new("key-v1.lock").unwrap();
        let lock = open_at(
            directory,
            &lock_name,
            libc::O_RDWR | libc::O_CREAT | libc::O_NONBLOCK,
            0o600,
        )?;
        secure_file(&lock)?;
        lock.lock()?;
        if let Some(key) = read_key(directory)? {
            return Ok(key);
        }
        let mut key = [0; 32];
        key[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        key[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        let temporary = CString::new(format!(".key-{}.tmp", uuid::Uuid::new_v4())).unwrap();
        let target = CString::new("key-v1").unwrap();
        let result = (|| {
            let mut file = open_at(
                directory,
                &temporary,
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                0o600,
            )?;
            secure_file(&file)?;
            file.write_all(&key)?;
            file.sync_all()?;
            // SAFETY: names and directory descriptor are valid.
            if unsafe {
                libc::renameat(
                    directory.as_raw_fd(),
                    temporary.as_ptr(),
                    directory.as_raw_fd(),
                    target.as_ptr(),
                )
            } < 0
            {
                return Err(io::Error::last_os_error());
            }
            directory.sync_all()?;
            Ok(key)
        })();
        let _ = unlink_at(directory, &temporary);
        result
    }

    impl Store {
        /// `parent` must already exist, be user-owned and not writable by others.
        /// The cache directory is created owner-only and verified without
        /// following a symlink. No existing permissions are widened or repaired.
        pub fn open(parent: &Path) -> io::Result<Self> {
            Self::open_inner(parent, true)?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "share cache missing"))
        }

        /// Read-only opening: a legacy session never creates a cache directory.
        pub fn open_existing(parent: &Path) -> io::Result<Option<Self>> {
            Self::open_inner(parent, false)
        }

        fn open_inner(parent: &Path, create: bool) -> io::Result<Option<Self>> {
            let parent = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(parent)?;
            secure_parent(&parent)?;
            let name = CString::new("share-context-v1").unwrap();
            let created = if create {
                // SAFETY: parent and name remain valid for the call.
                let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
                if result < 0 {
                    let error = io::Error::last_os_error();
                    if error.kind() != io::ErrorKind::AlreadyExists {
                        return Err(error);
                    }
                }
                result == 0
            } else {
                false
            };
            let directory = match open_at(&parent, &name, libc::O_RDONLY | libc::O_DIRECTORY, 0) {
                Ok(directory) => directory,
                Err(error) if !create && error.kind() == io::ErrorKind::NotFound => {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            secure_dir(&directory)?;
            if created {
                parent.sync_all()?;
            }
            let key = if create {
                load_or_create_key(&directory)?
            } else {
                match read_key(&directory)? {
                    Some(key) => key,
                    None => return Ok(None),
                }
            };
            Ok(Some(Self { directory, key }))
        }

        /// Keyed fingerprint for complete journal or provenance inputs. The
        /// caller chooses and validates the inputs and a domain-specific label.
        pub fn fingerprint<T: Serialize>(&self, domain: &str, value: &T) -> io::Result<[u8; 32]> {
            keyed_digest(&self.key, domain, value)
        }

        /// Bind to the complete validated journal, not just the head number.
        /// `provenance` must include every effective runtime input; this method
        /// cannot establish freshness of remote or dynamically supplied tools.
        pub fn binding_for_session<T: Serialize>(
            &self,
            session: &pi_rs::Session,
            provenance: &T,
        ) -> io::Result<Binding> {
            Ok(Binding {
                session_id: session.session_id(),
                head: session.head(),
                journal_digest: self.fingerprint("share-journal-v1", &session.records())?,
                provenance_digest: self.fingerprint("share-provenance-v1", provenance)?,
            })
        }

        /// Decode a cached context only when the full journal and supplied
        /// provenance match. Unknown or malformed context schemas are misses.
        pub fn read_session<P: Serialize, C: DeserializeOwned>(
            &self,
            path: &Path,
            session: &pi_rs::Session,
            provenance: &P,
        ) -> io::Result<Option<C>> {
            let binding = self.binding_for_session(session, provenance)?;
            Ok(self
                .read(path, &binding)?
                .and_then(|value| serde_json::from_value(value).ok()))
        }

        fn lock(&self, path: &Path, binding: &Binding, create: bool) -> io::Result<File> {
            let name = name_for(&self.key, path, binding.session_id, "lock")?;
            let file = open_at(
                &self.directory,
                &name,
                libc::O_RDWR | libc::O_NONBLOCK | if create { libc::O_CREAT } else { 0 },
                0o600,
            )?;
            secure_file(&file)?;
            Ok(file)
        }

        /// Invalidates the last generation before runtime preparation. The
        /// exclusive lock remains held until the generation is dropped.
        pub fn begin(&self, path: &Path, binding: Binding) -> io::Result<Generation<'_>> {
            secure_dir(&self.directory)?;
            let lock = self.lock(path, &binding, true)?;
            lock.lock()?;
            let name = name_for(&self.key, path, binding.session_id, "json")?;
            unlink_at(&self.directory, &name)?;
            self.directory.sync_all()?;
            Ok(Generation {
                store: self,
                lock,
                name,
                binding,
            })
        }

        /// Invalidate before polling runtime initialization. Failure to
        /// invalidate prevents the initializer from running at all. Dropping
        /// this future or an initializer error leaves the generation dirty.
        pub async fn begin_and_initialize<F, Fut, T>(
            &self,
            path: &Path,
            binding: Binding,
            initialize: F,
        ) -> pi_rs::Result<(Generation<'_>, T)>
        where
            F: FnOnce() -> Fut,
            Fut: std::future::Future<Output = pi_rs::Result<T>>,
        {
            let generation = self.begin(path, binding)?;
            let runtime = initialize().await?;
            Ok((generation, runtime))
        }

        /// A missing, busy, mismatched, malformed, or old-format entry is a
        /// cache miss. Caller determines how to reconstruct a legacy session.
        pub fn read(&self, path: &Path, binding: &Binding) -> io::Result<Option<Value>> {
            secure_dir(&self.directory)?;
            let lock = match self.lock(path, binding, false) {
                Ok(lock) => lock,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error),
            };
            match lock.try_lock() {
                Ok(()) => (),
                Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
                Err(std::fs::TryLockError::Error(error)) => return Err(error),
            }
            let name = name_for(&self.key, path, binding.session_id, "json")?;
            let file = match open_at(&self.directory, &name, libc::O_RDONLY | libc::O_NONBLOCK, 0) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error),
            };
            secure_file(&file)?;
            if file.metadata()?.len() > MAX_CACHE_BYTES {
                return Ok(None);
            }
            let mut bytes = Vec::new();
            file.take(MAX_CACHE_BYTES + 1).read_to_end(&mut bytes)?;
            if bytes.len() as u64 > MAX_CACHE_BYTES {
                return Ok(None);
            }
            let Ok(envelope) = serde_json::from_slice::<Envelope>(&bytes) else {
                return Ok(None);
            };
            if envelope.version != 1 || &envelope.binding != binding {
                return Ok(None);
            }
            let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("HMAC accepts 32 bytes");
            mac.update(b"share-envelope-v1\0");
            mac.update(&serde_json::to_vec(&(
                name.as_bytes(),
                envelope.version,
                &envelope.binding,
                &envelope.context,
            ))?);
            if mac.verify_slice(&envelope.tag).is_err() {
                return Ok(None);
            }
            Ok(Some(envelope.context))
        }
    }

    impl Generation<'_> {
        /// Serialize a clean live context against the final session and
        /// provenance. The caller must hold the session's exclusive lock and
        /// prove the runtime has settled through publication.
        pub fn publish_session<P: Serialize, C: Serialize>(
            self,
            session: &pi_rs::Session,
            provenance: &P,
            context: &C,
        ) -> io::Result<()> {
            let binding = self.store.binding_for_session(session, provenance)?;
            let context = serde_json::to_value(context)?;
            self.publish(binding, context)
        }

        /// Publish only after the live runtime has settled and the caller has
        /// independently revalidated the complete binding and journal.
        pub fn publish(self, binding: Binding, context: Value) -> io::Result<()> {
            secure_dir(&self.store.directory)?;
            if binding.session_id != self.binding.session_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "session identity changed",
                ));
            }
            let tag = keyed_digest(
                &self.store.key,
                "share-envelope-v1",
                &(self.name.as_bytes(), 1u32, &binding, &context),
            )?;
            let envelope = Envelope {
                version: 1,
                binding,
                context,
                tag,
            };
            let bytes = serde_json::to_vec(&envelope)?;
            if bytes.len() as u64 > MAX_CACHE_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "share cache too large",
                ));
            }
            let temporary = CString::new(format!(".{}.tmp", uuid::Uuid::new_v4())).unwrap();
            let mut renamed = false;
            let result = (|| {
                let mut file = open_at(
                    &self.store.directory,
                    &temporary,
                    libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                    0o600,
                )?;
                secure_file(&file)?;
                file.write_all(&bytes)?;
                file.sync_all()?;
                secure_file(&file)?;
                secure_dir(&self.store.directory)?;
                // SAFETY: both names refer to the same verified directory.
                if unsafe {
                    libc::renameat(
                        self.store.directory.as_raw_fd(),
                        temporary.as_ptr(),
                        self.store.directory.as_raw_fd(),
                        self.name.as_ptr(),
                    )
                } < 0
                {
                    return Err(io::Error::last_os_error());
                }
                renamed = true;
                self.store.directory.sync_all()
            })();
            let _ = unlink_at(&self.store.directory, &temporary);
            if result.is_err() && renamed {
                let _ = unlink_at(&self.store.directory, &self.name);
                let _ = self.store.directory.sync_all();
            }
            let _ = &self.lock;
            result
        }
    }
}

#[cfg(unix)]
pub use unix::{Generation, Store};

#[cfg(windows)]
pub mod windows_private;

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::path::Path;

    fn binding() -> Binding {
        Binding {
            session_id: uuid::Uuid::new_v4(),
            head: 3,
            journal_digest: [1; 32],
            provenance_digest: [2; 32],
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn rejects_extended_acl_even_when_mode_is_private() {
        let root = tempfile::tempdir().unwrap();
        let status = std::process::Command::new("chmod")
            .args(["+a", "everyone allow list,search"])
            .arg(root.path())
            .status()
            .unwrap();
        assert!(status.success());
        assert_eq!(
            std::fs::metadata(root.path()).unwrap().permissions().mode() & 0o022,
            0
        );
        assert!(Store::open(root.path()).is_err());
    }

    #[test]
    fn legacy_lookup_does_not_create_cache_files() {
        let directory = tempfile::tempdir().unwrap();
        assert!(Store::open_existing(directory.path()).unwrap().is_none());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        let store = Store::open(directory.path()).unwrap();
        assert!(
            store
                .read(Path::new("/legacy/session.jsonl"), &binding())
                .unwrap()
                .is_none()
        );
        let before = std::fs::read_dir(directory.path().join("share-context-v1"))
            .unwrap()
            .count();
        assert!(
            store
                .read(Path::new("/legacy/session.jsonl"), &binding())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            std::fs::read_dir(directory.path().join("share-context-v1"))
                .unwrap()
                .count(),
            before
        );
    }

    #[test]
    fn restart_requires_exact_binding_and_a_clean_generation() {
        let directory = tempfile::tempdir().unwrap();
        let path = Path::new("/workspace/session.jsonl");
        let original = binding();
        let expected = serde_json::json!({"systemPrompt":"secret","tools":[{"name":"read"}]});
        let store = Store::open(directory.path()).unwrap();
        assert!(store.read(path, &original).unwrap().is_none());
        let generation = store.begin(path, original.clone()).unwrap();
        assert!(store.read(path, &original).unwrap().is_none());
        generation
            .publish(original.clone(), expected.clone())
            .unwrap();
        drop(store);
        let store = Store::open(directory.path()).unwrap();
        assert_eq!(store.read(path, &original).unwrap(), Some(expected));
        let mut changed = original.clone();
        changed.head += 1;
        assert!(store.read(path, &changed).unwrap().is_none());
        changed = original.clone();
        changed.journal_digest[0] ^= 1;
        assert!(store.read(path, &changed).unwrap().is_none());
        changed = original.clone();
        changed.provenance_digest[0] ^= 1;
        assert!(store.read(path, &changed).unwrap().is_none());
        assert!(
            store
                .read(Path::new("/other/session.jsonl"), &original)
                .unwrap()
                .is_none()
        );
        let generation = store.begin(path, original.clone()).unwrap();
        drop(generation); // crash or unsuccessful shutdown leaves no clean cache
        assert!(store.read(path, &original).unwrap().is_none());
    }

    #[test]
    fn journal_binding_detects_equal_head_content_changes_and_provenance_changes() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        let mut session = pi_rs::Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        session
            .append_user_with_images("first".into(), vec![], None)
            .unwrap();
        let provenance = serde_json::json!({"workspace":"/workspace","tools":["read"]});
        let binding = store.binding_for_session(&session, &provenance).unwrap();
        let mut changed_records = session.records().to_vec();
        match &mut changed_records.last_mut().unwrap().event {
            pi_rs::Event::User { content, .. } => *content = "different".into(),
            _ => panic!("expected user record"),
        }
        let changed = pi_rs::Session::from_records(changed_records).unwrap();
        let changed_binding = store.binding_for_session(&changed, &provenance).unwrap();
        assert_eq!(binding.session_id, changed_binding.session_id);
        assert_eq!(binding.head, changed_binding.head);
        assert_ne!(binding.journal_digest, changed_binding.journal_digest);
        let changed_provenance = serde_json::json!({"workspace":"/workspace","tools":["bash"]});
        let changed_binding = store
            .binding_for_session(&session, &changed_provenance)
            .unwrap();
        assert_eq!(binding.journal_digest, changed_binding.journal_digest);
        assert_ne!(binding.provenance_digest, changed_binding.provenance_digest);
    }

    #[test]
    fn session_round_trip_rejects_journal_and_runtime_changes() {
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Context {
            prompt: String,
            tools: Vec<String>,
        }
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        let path = Path::new("/private/live-session.jsonl");
        let mut session = pi_rs::Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        let provenance = serde_json::json!({"tools":["read"],"extensionGeneration":1});
        let start = store.binding_for_session(&session, &provenance).unwrap();
        let generation = store.begin(path, start).unwrap();
        session
            .append_user_with_images("message".into(), vec![], None)
            .unwrap();
        let expected = Context {
            prompt: "live prompt".into(),
            tools: vec!["read".into()],
        };
        generation
            .publish_session(&session, &provenance, &expected)
            .unwrap();
        assert_eq!(
            store
                .read_session::<_, Context>(path, &session, &provenance)
                .unwrap(),
            Some(expected)
        );
        let changed = serde_json::json!({"tools":["read"],"extensionGeneration":2});
        assert!(
            store
                .read_session::<_, Context>(path, &session, &changed)
                .unwrap()
                .is_none()
        );
        session
            .append_user_with_images("next".into(), vec![], None)
            .unwrap();
        assert!(
            store
                .read_session::<_, Context>(path, &session, &provenance)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn clean_generation_can_publish_a_later_head_but_not_another_session() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        let path = Path::new("/private/session.jsonl");
        let initial = binding();
        let mut final_binding = initial.clone();
        final_binding.head += 1;
        final_binding.journal_digest[0] ^= 1;
        store
            .begin(path, initial.clone())
            .unwrap()
            .publish(final_binding.clone(), serde_json::json!({"prompt":"final"}))
            .unwrap();
        assert!(store.read(path, &initial).unwrap().is_none());
        assert_eq!(
            store.read(path, &final_binding).unwrap(),
            Some(serde_json::json!({"prompt":"final"}))
        );
        let mut other = final_binding.clone();
        other.session_id = uuid::Uuid::new_v4();
        assert!(
            store
                .begin(path, final_binding.clone())
                .unwrap()
                .publish(other, serde_json::json!({}))
                .is_err()
        );
        assert!(store.read(path, &final_binding).unwrap().is_none());
    }

    #[test]
    fn concurrent_generations_serialize_and_new_generation_invalidates_old() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        let path = Path::new("/private/session.jsonl");
        let binding = binding();
        let first = store.begin(path, binding.clone()).unwrap();
        let (attempted, started) = std::sync::mpsc::channel();
        let (acquired, ready) = std::sync::mpsc::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let root_path = root.path().to_owned();
        let second_binding = binding.clone();
        let second = std::thread::spawn(move || {
            let store = Store::open(&root_path).unwrap();
            attempted.send(()).unwrap();
            let generation = store.begin(path, second_binding.clone()).unwrap();
            acquired.send(()).unwrap();
            gate.recv().unwrap();
            generation
                .publish(second_binding, serde_json::json!({"prompt":"new"}))
                .unwrap();
        });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert!(
            ready
                .recv_timeout(std::time::Duration::from_millis(100))
                .is_err()
        );
        first
            .publish(binding.clone(), serde_json::json!({"prompt":"old"}))
            .unwrap();
        ready
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert!(store.read(path, &binding).unwrap().is_none());
        release.send(()).unwrap();
        second.join().unwrap();
        assert_eq!(
            store.read(path, &binding).unwrap(),
            Some(serde_json::json!({"prompt":"new"}))
        );
    }

    #[test]
    fn crashed_live_generation_cannot_restore_the_previous_snapshot() {
        const CHILD: &str = "PI_RS_TEST_SHARE_CACHE_CRASH_CHILD";
        let path = Path::new("/private/crash-session.jsonl");
        let binding = Binding {
            session_id: uuid::Uuid::from_u128(42),
            head: 7,
            journal_digest: [4; 32],
            provenance_digest: [9; 32],
        };
        if let Some(root) = std::env::var_os(CHILD) {
            let root = std::path::PathBuf::from(root);
            let store = Store::open(&root).unwrap();
            let _generation = store.begin(path, binding).unwrap();
            std::fs::write(root.join("child-ready"), b"ready").unwrap();
            loop {
                std::thread::park();
            }
        }
        struct KillOnDrop(std::process::Child);
        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        store
            .begin(path, binding.clone())
            .unwrap()
            .publish(binding.clone(), serde_json::json!({"systemPrompt":"old"}))
            .unwrap();
        let mut child = KillOnDrop(std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "private_share_store::tests::crashed_live_generation_cannot_restore_the_previous_snapshot"])
            .env(CHILD, root.path()).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn().unwrap());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !root.path().join("child-ready").exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before invalidation"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "child did not start in time"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(store.read(path, &binding).unwrap().is_none());
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        assert!(store.read(path, &binding).unwrap().is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_invalidation_never_starts_runtime_initialization() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let root = tempfile::tempdir().unwrap();
        let path = Path::new("/private/session.jsonl");
        let binding = binding();
        let store = Store::open(root.path()).unwrap();
        store
            .begin(path, binding.clone())
            .unwrap()
            .publish(binding.clone(), serde_json::json!({"prompt":"old"}))
            .unwrap();
        let directory = root.path().join("share-context-v1");
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o500)).unwrap();
        let started = AtomicBool::new(false);
        let result = store
            .begin_and_initialize(path, binding.clone(), || async {
                started.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await;
        assert!(result.is_err());
        assert!(!started.load(Ordering::SeqCst));
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            store.read(path, &binding).unwrap(),
            Some(serde_json::json!({"prompt":"old"}))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_runtime_initialization_leaves_generation_invalidated() {
        let root = tempfile::tempdir().unwrap();
        let path = Path::new("/private/session.jsonl");
        let binding = binding();
        let store = Store::open(root.path()).unwrap();
        store
            .begin(path, binding.clone())
            .unwrap()
            .publish(binding.clone(), serde_json::json!({"prompt":"old"}))
            .unwrap();
        let result = store
            .begin_and_initialize(path, binding.clone(), || async {
                Err::<(), _>(pi_rs::Error::Config("fixture startup failed".into()))
            })
            .await;
        assert!(result.is_err());
        assert!(store.read(path, &binding).unwrap().is_none());
    }

    #[test]
    fn unsafe_directory_change_during_generation_prevents_publication() {
        let root = tempfile::tempdir().unwrap();
        let path = Path::new("/private/session.jsonl");
        let binding = binding();
        let store = Store::open(root.path()).unwrap();
        let generation = store.begin(path, binding.clone()).unwrap();
        let directory = root.path().join("share-context-v1");
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            generation
                .publish(binding.clone(), serde_json::json!({"prompt":"secret"}))
                .is_err()
        );
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(store.read(path, &binding).unwrap().is_none());
    }

    #[test]
    fn cache_directory_without_invalidation_permission_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let path = Path::new("/private/session.jsonl");
        let binding = binding();
        let store = Store::open(root.path()).unwrap();
        store
            .begin(path, binding.clone())
            .unwrap()
            .publish(binding.clone(), serde_json::json!({"prompt":"old"}))
            .unwrap();
        let directory = root.path().join("share-context-v1");
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o500)).unwrap();
        assert!(store.read(path, &binding).is_err());
        assert!(Store::open_existing(root.path()).is_err());
        assert!(Store::open(root.path()).is_err());
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn rejects_writable_parent_and_symlink_cache_directory() {
        let root = tempfile::tempdir().unwrap();
        let shared = root.path().join("shared");
        std::fs::create_dir(&shared).unwrap();
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(Store::open(&shared).is_err());
        let target = root.path().join("target");
        std::fs::create_dir(&target).unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o700)).unwrap();
        symlink(&target, root.path().join("share-context-v1")).unwrap();
        assert!(Store::open(root.path()).is_err());
    }

    #[test]
    fn copying_a_valid_envelope_to_another_session_path_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        let binding = binding();
        let first = Path::new("/private/one.jsonl");
        let second = Path::new("/private/two.jsonl");
        store
            .begin(first, binding.clone())
            .unwrap()
            .publish(binding.clone(), serde_json::json!({"prompt":"first"}))
            .unwrap();
        drop(store.begin(second, binding.clone()).unwrap());
        let directory = root.path().join("share-context-v1");
        let entries: Vec<_> = std::fs::read_dir(directory)
            .unwrap()
            .map(Result::unwrap)
            .map(|entry| entry.path())
            .collect();
        let source = entries
            .iter()
            .find(|path| path.extension().is_some_and(|ext| ext == "json"))
            .unwrap();
        let target = entries
            .iter()
            .filter(|path| {
                path.extension().is_some_and(|ext| ext == "lock")
                    && path.file_stem().is_some_and(|stem| stem.len() == 64)
            })
            .map(|path| path.with_extension("json"))
            .find(|path| !path.exists())
            .unwrap();
        std::fs::copy(source, target).unwrap();
        assert!(store.read(second, &binding).unwrap().is_none());
    }

    #[test]
    fn insecure_or_symlinked_key_is_rejected_without_replacement() {
        let root = tempfile::tempdir().unwrap();
        drop(Store::open(root.path()).unwrap());
        let key = root.path().join("share-context-v1/key-v1");
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(Store::open_existing(root.path()).is_err());
        assert!(Store::open(root.path()).is_err());
        std::fs::remove_file(&key).unwrap();
        let target = root.path().join("target");
        std::fs::write(&target, [3u8; 32]).unwrap();
        symlink(&target, &key).unwrap();
        assert!(Store::open_existing(root.path()).is_err());
        assert!(Store::open(root.path()).is_err());
        assert_eq!(std::fs::read(target).unwrap(), [3u8; 32]);
    }

    #[test]
    fn keyed_fingerprints_and_snapshot_authentication_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let other_root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        let other = Store::open(other_root.path()).unwrap();
        let first = store.fingerprint("provenance", &"private input").unwrap();
        assert_eq!(
            first,
            store.fingerprint("provenance", &"private input").unwrap()
        );
        assert_ne!(
            first,
            store.fingerprint("journal", &"private input").unwrap()
        );
        assert_ne!(
            first,
            other.fingerprint("provenance", &"private input").unwrap()
        );
        let path = Path::new("/private/authenticated.jsonl");
        let binding = binding();
        store
            .begin(path, binding.clone())
            .unwrap()
            .publish(binding.clone(), serde_json::json!({"prompt":"original"}))
            .unwrap();
        let directory = root.path().join("share-context-v1");
        let json = std::fs::read_dir(directory)
            .unwrap()
            .map(Result::unwrap)
            .map(|entry| entry.path())
            .find(|path| path.extension().is_some_and(|ext| ext == "json"))
            .unwrap();
        let mut envelope: Value = serde_json::from_slice(&std::fs::read(&json).unwrap()).unwrap();
        envelope["context"]["prompt"] = serde_json::json!("tampered");
        std::fs::write(json, serde_json::to_vec(&envelope).unwrap()).unwrap();
        assert!(store.read(path, &binding).unwrap().is_none());
    }

    #[test]
    fn stored_contents_are_owner_only_and_legacy_data_is_not_reused() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        let binding = binding();
        let path = Path::new("/private/session.jsonl");
        store
            .begin(path, binding.clone())
            .unwrap()
            .publish(binding.clone(), serde_json::json!({"secret":"value"}))
            .unwrap();
        let directory = root.path().join("share-context-v1");
        assert_eq!(
            std::fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let json = std::fs::read_dir(&directory)
            .unwrap()
            .map(Result::unwrap)
            .map(|entry| entry.path())
            .find(|path| path.extension().is_some_and(|ext| ext == "json"))
            .unwrap();
        assert_eq!(
            std::fs::metadata(&json).unwrap().permissions().mode() & 0o777,
            0o600
        );
        std::fs::write(&json, br#"{"version":0,"context":{"secret":"old"}}"#).unwrap();
        assert!(store.read(path, &binding).unwrap().is_none());
        std::fs::remove_file(&json).unwrap();
        let external = root.path().join("external");
        std::fs::write(&external, b"unchanged").unwrap();
        symlink(&external, &json).unwrap();
        assert!(store.read(path, &binding).is_err());
        assert_eq!(std::fs::read(external).unwrap(), b"unchanged");
    }
}
