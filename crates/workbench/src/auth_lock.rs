//! The mkdir/mtime lock used by Pi's proper-lockfile auth storage.
use pi_rs::{Error, Result};
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::{Duration, SystemTime};

const WAIT: Duration = Duration::from_secs(30);

#[cfg(unix)]
type Identity = std::fs::Metadata;
#[cfg(unix)]
fn identity(path: &Path) -> std::io::Result<Identity> {
    std::fs::metadata(path)
}
#[cfg(unix)]
fn same_dir(a: &Identity, b: &Identity) -> bool {
    use std::os::unix::fs::MetadataExt;
    a.dev() == b.dev() && a.ino() == b.ino()
}
#[cfg(windows)]
struct Identity {
    // Keep the handle open so Windows cannot reuse its identity while held.
    _handle: std::fs::File,
    volume: u64,
    id: [u8; 16],
}
#[cfg(windows)]
fn identity(path: &Path) -> std::io::Result<Identity> {
    use std::os::windows::{fs::OpenOptionsExt, io::AsRawHandle};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_ID_INFO, FileIdInfo, GetFileInformationByHandleEx,
    };
    let handle = std::fs::OpenOptions::new()
        .access_mode(0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    let mut info = FILE_ID_INFO::default();
    // The handle is valid and info is a writable buffer of the specified size.
    let ok = unsafe {
        GetFileInformationByHandleEx(
            handle.as_raw_handle() as _,
            FileIdInfo,
            std::ptr::addr_of_mut!(info).cast(),
            std::mem::size_of::<FILE_ID_INFO>() as u32,
        )
    };
    if ok == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(Identity {
        _handle: handle,
        volume: info.VolumeSerialNumber,
        id: info.FileId.Identifier,
    })
}
#[cfg(windows)]
fn same_dir(a: &Identity, b: &Identity) -> bool {
    a.volume == b.volume && a.id == b.id
}
#[cfg(not(any(unix, windows)))]
type Identity = std::fs::Metadata;
#[cfg(not(any(unix, windows)))]
fn identity(path: &Path) -> std::io::Result<Identity> {
    std::fs::metadata(path)
}
#[cfg(not(any(unix, windows)))]
fn same_dir(a: &Identity, b: &Identity) -> bool {
    a.created()
        .ok()
        .zip(b.created().ok())
        .is_some_and(|(a, b)| a == b)
}
fn owns(path: &Path, held: &Identity) -> bool {
    identity(path).is_ok_and(|current| same_dir(held, &current))
}
fn busy() -> Error {
    Error::Config("auth.json lock is busy or compromised. If stale, stop all Pi and pi-rs processes before removing the lock directory beside the resolved credential file".into())
}

pub(super) struct Guard {
    path: PathBuf,
    requested_auth: PathBuf,
    resolved_auth: PathBuf,
    identity: Arc<Identity>,
    compromised: Arc<AtomicBool>,
    stop: Option<mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    // Drop after the Windows directory handle in identity has closed.
    staging: Option<tempfile::TempDir>,
}
impl Guard {
    pub(super) fn check(&self) -> Result<()> {
        if self.compromised.load(Ordering::SeqCst)
            || !owns(&self.path, &self.identity)
            || resolved_auth_path(&self.requested_auth)? != self.resolved_auth
        {
            return Err(busy());
        }
        Ok(())
    }

    pub(super) fn auth_path(&self) -> Result<&Path> {
        self.check()?;
        Ok(&self.resolved_auth)
    }

    pub(super) fn auth_path_for(&self, path: &Path) -> Result<&Path> {
        let target = self.auth_path()?;
        if resolved_auth_path(path)? != target {
            return Err(busy());
        }
        Ok(target)
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        #[cfg(test)]
        if let Some(directory) = std::env::var_os("PI_RS_TEST_PAUSE_AUTH_UNLOCK") {
            let directory = PathBuf::from(directory);
            let _ = std::fs::write(directory.join("unlock-entered"), b"");
            use std::io::Read;
            let _ = std::io::stdin().read(&mut [0]);
        }
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        if owns(&self.path, &self.identity) {
            if let Some(staging) = &self.staging {
                // Keep the marker in place until the directory is off the shared path.
                // A delayed Pi rmdir cannot remove a new holder during cleanup.
                let _ = std::fs::rename(&self.path, staging.path().join("retired"));
            } else {
                let _ = std::fs::remove_dir(&self.path);
            }
        }
    }
}
fn auth_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

/// Resolve existing links while allowing an auth file that has yet to be created.
pub(super) fn resolved_auth_path(path: &Path) -> Result<PathBuf> {
    use std::io::ErrorKind::NotFound;

    let mut path = std::path::absolute(path)?;
    for _ in 0..40 {
        match path.canonicalize() {
            Ok(resolved) => return Ok(resolved),
            Err(error) if error.kind() == NotFound => {}
            Err(error) => return Err(error.into()),
        }
        match std::fs::symlink_metadata(&path) {
            Ok(meta) if meta.is_symlink() => {
                path = auth_parent(&path).join(std::fs::read_link(&path)?);
            }
            // The file appeared during resolution. Recheck its current destination.
            Ok(_) => continue,
            Err(error) if error.kind() == NotFound => {
                let Some(name) = path.file_name() else {
                    return Err(error.into());
                };
                return match auth_parent(&path).canonicalize() {
                    Ok(parent) => Ok(parent.join(name)),
                    Err(error) if error.kind() == NotFound => Ok(path),
                    Err(error) => Err(error.into()),
                };
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(Error::Config("could not resolve auth.json path".into()))
}

pub(super) fn lock_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".lock");
    PathBuf::from(name)
}

// Pi only removes lock directories with rmdir. Publish the marked directory
// atomically so even a stale Pi observation cannot remove a live holder.
fn publish_lock(path: &Path) -> io::Result<Option<tempfile::TempDir>> {
    let staging = tempfile::Builder::new()
        .prefix(".pi-rs-auth-lock-")
        .tempdir_in(auth_parent(path))?;
    let prepared = staging.path().join("prepared");
    std::fs::create_dir(&prepared)?;
    std::fs::File::create_new(prepared.join("held-by-pi-rs"))?;
    match rename_no_replace(&prepared, path) {
        Ok(()) => Ok(Some(staging)),
        Err(error) if error.kind() == ErrorKind::Unsupported => {
            std::fs::create_dir(path)?;
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn rename_no_replace(source: &Path, destination: &Path) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let source = CString::new(source.as_os_str().as_bytes()).map_err(io::Error::from)?;
    let destination = CString::new(destination.as_os_str().as_bytes()).map_err(io::Error::from)?;
    // The two C strings remain valid during the call. The rename must fail if
    // the destination already exists, including an empty Pi lock directory.
    #[cfg(target_os = "macos")]
    let result =
        unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) };
    #[cfg(target_os = "linux")]
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ENOSYS | libc::ENOTSUP | libc::EINVAL) => {
            Err(io::Error::new(ErrorKind::Unsupported, error))
        }
        _ => Err(error),
    }
}

#[cfg(windows)]
fn rename_no_replace(source: &Path, destination: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::MoveFileExW;

    let to_wide = |path: &Path| {
        path.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>()
    };
    let source = to_wide(source);
    let destination_wide = to_wide(destination);
    // Both paths are wide NUL-terminated strings. Zero flags forbids replacement.
    if unsafe { MoveFileExW(source.as_ptr(), destination_wide.as_ptr(), 0) } != 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.kind() == ErrorKind::PermissionDenied && destination.is_dir() {
        return Err(io::Error::new(ErrorKind::AlreadyExists, error));
    }
    Err(error)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn rename_no_replace(_: &Path, _: &Path) -> io::Result<()> {
    Err(ErrorKind::Unsupported.into())
}

fn try_lock(path: &Path) -> Result<Option<Guard>> {
    let requested_auth = std::path::absolute(path)?;
    std::fs::create_dir_all(auth_parent(&requested_auth))?;
    let resolved_auth = resolved_auth_path(&requested_auth)?;
    let lock_path = lock_path(&resolved_auth);
    if lock_path.try_exists()? {
        return Ok(None);
    }
    match publish_lock(&lock_path) {
        Ok(staging) => {
            let identity = Arc::new(identity(&lock_path)?);
            let thread_identity = identity.clone();
            let thread_path = lock_path.clone();
            let compromised = Arc::new(AtomicBool::new(false));
            let flag = compromised.clone();
            let (stop, receiver) = mpsc::channel();
            let thread = std::thread::spawn(move || {
                while matches!(
                    receiver.recv_timeout(Duration::from_secs(2)),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ) {
                    if !owns(&thread_path, &thread_identity)
                        || filetime::set_file_mtime(
                            &thread_path,
                            filetime::FileTime::from_system_time(
                                SystemTime::now() + Duration::from_secs(1),
                            ),
                        )
                        .is_err()
                    {
                        flag.store(true, Ordering::SeqCst);
                        break;
                    }
                }
            });
            let guard = Guard {
                path: lock_path,
                staging,
                requested_auth,
                resolved_auth,
                identity,
                compromised,
                stop: Some(stop),
                thread: Some(thread),
            };
            guard.check()?;
            Ok(Some(guard))
        }
        // Neither acquisition path can replace an existing Pi lock.
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(None),
        Err(e) => Err(e.into()),
    }
}
pub(super) fn acquire(path: &Path) -> Result<Guard> {
    // Synchronous UI and credential paths must never sleep on a Tokio worker.
    // Async callers that need to wait use acquire_async instead.
    if tokio::runtime::Handle::try_current().is_ok() {
        return try_lock(path)?.ok_or_else(busy);
    }
    let deadline = std::time::Instant::now() + WAIT;
    loop {
        if let Some(guard) = try_lock(path)? {
            return Ok(guard);
        }
        if std::time::Instant::now() >= deadline {
            return Err(busy());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
pub(super) async fn acquire_async(path: &Path) -> Result<Guard> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if let Some(guard) = try_lock(path)? {
            return Ok(guard);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(busy());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn auth_guard_rejects_an_alias_retargeted_after_locking() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("credentials.json");
        let alias = dir.path().join("auth.json");
        std::fs::write(&target, "{}").unwrap();
        symlink("credentials.json", &alias).unwrap();
        let guard = try_lock(&alias).unwrap().unwrap();
        assert!(try_lock(&target).unwrap().is_none());

        std::fs::remove_file(&alias).unwrap();
        symlink("replacement.json", &alias).unwrap();
        assert!(guard.auth_path().is_err());
        drop(guard);
        assert!(!lock_path(&target).exists());
        assert!(try_lock(&alias).unwrap().is_some());
    }

    #[test]
    fn pi_rs_does_not_reclaim_stale_or_remove_live_locks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        let lock = dir.path().join("auth.json.lock");
        std::fs::create_dir(&lock).unwrap();
        filetime::set_file_mtime(
            &lock,
            filetime::FileTime::from_system_time(SystemTime::now() - Duration::from_secs(120)),
        )
        .unwrap();
        {
            let stale = identity(&lock).unwrap();
            let waiters: Vec<_> = (0..2)
                .map(|_| {
                    let path = path.clone();
                    std::thread::spawn(move || assert!(try_lock(&path).unwrap().is_none()))
                })
                .collect();
            for waiter in waiters {
                waiter.join().unwrap();
            }
            assert!(owns(&lock, &stale), "stale directory was removed");
        }

        // Simulate Pi reclaiming the stale directory and holding a fresh one.
        std::fs::remove_dir(&lock).unwrap();
        std::fs::create_dir(&lock).unwrap();
        let live = identity(&lock).unwrap();
        let waiters: Vec<_> = (0..2)
            .map(|_| {
                let path = path.clone();
                std::thread::spawn(move || assert!(try_lock(&path).unwrap().is_none()))
            })
            .collect();
        for waiter in waiters {
            waiter.join().unwrap();
        }
        assert!(owns(&lock, &live), "a live replacement was removed");
    }

    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    #[test]
    fn live_auth_lock_stays_nonempty_until_it_leaves_the_shared_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        let lock = lock_path(&path);
        let guard = try_lock(&path).unwrap().unwrap();
        assert!(guard.staging.is_some());
        assert!(std::fs::remove_dir(&lock).is_err());
        guard.check().unwrap();
        drop(guard);
        assert!(!lock.exists());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    #[test]
    fn publishing_a_marked_lock_cannot_replace_an_empty_pi_lock() {
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("auth.json.lock");
        std::fs::create_dir(&lock).unwrap();
        let existing = identity(&lock).unwrap();
        let error = publish_lock(&lock).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::AlreadyExists);
        assert!(owns(&lock, &existing));
        assert_eq!(std::fs::read_dir(&lock).unwrap().count(), 0);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn lock_excludes_other_holders_and_preserves_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        let first = acquire(&path).unwrap();
        assert!(try_lock(&path).unwrap().is_none());
        let lock = dir.path().join("auth.json.lock");
        std::fs::rename(&lock, dir.path().join("old-lock")).unwrap();
        std::fs::create_dir(&lock).unwrap();
        // Windows can assign equal creation times to different directories.
        // Force that collision so the test cannot pass using timestamps as identity.
        #[cfg(windows)]
        {
            use std::os::windows::{
                fs::{MetadataExt, OpenOptionsExt},
                io::AsRawHandle,
            };
            use windows_sys::Win32::{
                Foundation::FILETIME,
                Storage::FileSystem::{
                    FILE_FLAG_BACKUP_SEMANTICS, FILE_WRITE_ATTRIBUTES, SetFileTime,
                },
            };
            let created = std::fs::metadata(dir.path().join("old-lock"))
                .unwrap()
                .creation_time();
            let time = FILETIME {
                dwLowDateTime: created as u32,
                dwHighDateTime: (created >> 32) as u32,
            };
            let replacement = std::fs::OpenOptions::new()
                .access_mode(FILE_WRITE_ATTRIBUTES)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
                .open(&lock)
                .unwrap();
            // Both the handle and the FILETIME pointer remain valid for this call.
            assert_ne!(
                unsafe {
                    SetFileTime(
                        replacement.as_raw_handle() as _,
                        &time,
                        std::ptr::null(),
                        std::ptr::null(),
                    )
                },
                0
            );
            assert_eq!(std::fs::metadata(&lock).unwrap().creation_time(), created);
        }
        assert!(first.check().is_err());
        drop(first);
        assert!(lock.exists());
    }

    #[tokio::test]
    async fn synchronous_runtime_access_fails_fast_and_async_wait_can_be_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        let held = acquire(&path).unwrap();
        let started = std::time::Instant::now();
        assert!(acquire(&path).is_err());
        assert!(started.elapsed() < Duration::from_millis(500));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), acquire_async(&path))
                .await
                .is_err()
        );
        drop(held);
        assert!(acquire_async(&path).await.is_ok());
    }

    // Optional local interoperability check against Pi's pinned npm dependency.
    #[test]
    fn proper_lockfile_interoperability_when_available() {
        use std::io::{BufRead, Write};
        use std::process::{Command, Stdio};
        let Some(module) = std::env::var_os("PI_TEST_PROPER_LOCKFILE") else {
            return;
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        std::fs::write(&path, "{}").unwrap();
        let guard = acquire(&path).unwrap();
        let status = Command::new("node").arg("-e").arg("try { require(process.argv[1]).lockSync(process.argv[2], {realpath:false}); process.exit(1); } catch (e) { process.exit(e.code === 'ELOCKED' ? 0 : 2); }").arg(&module).arg(&path).status().unwrap();
        assert!(status.success());
        drop(guard);
        let mut child = Command::new("node").arg("-e").arg("const release = require(process.argv[1]).lockSync(process.argv[2], {realpath:false}); console.log('locked'); process.stdin.once('data', () => { release(); process.exit(0); });").arg(&module).arg(&path).stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap();
        let mut line = String::new();
        std::io::BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        assert_eq!(line.trim(), "locked");
        assert!(try_lock(&path).unwrap().is_none());
        child.stdin.take().unwrap().write_all(b"release\n").unwrap();
        assert!(child.wait().unwrap().success());
        assert!(try_lock(&path).unwrap().is_some());

        let lock = dir.path().join("auth.json.lock");
        std::fs::create_dir(&lock).unwrap();
        filetime::set_file_mtime(
            &lock,
            filetime::FileTime::from_system_time(SystemTime::now() - Duration::from_secs(120)),
        )
        .unwrap();
        assert!(try_lock(&path).unwrap().is_none());
        let mut child = Command::new("node").arg("-e").arg("const release = require(process.argv[1]).lockSync(process.argv[2], {realpath:false, stale:30000}); console.log('recovered'); process.stdin.once('data', () => { release(); process.exit(0); });").arg(&module).arg(&path).stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap();
        let mut line = String::new();
        std::io::BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        assert_eq!(line.trim(), "recovered");
        assert!(try_lock(&path).unwrap().is_none());
        child.stdin.take().unwrap().write_all(b"release\n").unwrap();
        assert!(child.wait().unwrap().success());
        assert!(try_lock(&path).unwrap().is_some());
    }
}
