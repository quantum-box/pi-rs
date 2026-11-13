//! The mkdir/mtime lock used by Pi's proper-lockfile auth storage.
use pi_rs::{Error, Result};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::{Duration, SystemTime};

const WAIT: Duration = Duration::from_secs(30);
const STALE: Duration = Duration::from_secs(30);

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
    Error::Config("auth.json lock is busy or compromised".into())
}

pub(super) struct Guard {
    path: PathBuf,
    identity: Arc<Identity>,
    compromised: Arc<AtomicBool>,
    stop: Option<mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Guard {
    pub(super) fn check(&self) -> Result<()> {
        if self.compromised.load(Ordering::SeqCst) || !owns(&self.path, &self.identity) {
            return Err(busy());
        }
        Ok(())
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        if owns(&self.path, &self.identity) {
            let _ = std::fs::remove_dir(&self.path);
        }
    }
}
fn try_lock(path: &Path) -> Result<Option<Guard>> {
    let parent = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let mut name = path.as_os_str().to_os_string();
    name.push(".lock");
    let lock_path = PathBuf::from(name);
    match std::fs::create_dir(&lock_path) {
        Ok(()) => {
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
            Ok(Some(Guard {
                path: lock_path,
                identity,
                compromised,
                stop: Some(stop),
                thread: Some(thread),
            }))
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            if std::fs::metadata(&lock_path)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|time| time.elapsed().ok())
                .is_some_and(|age| age > STALE)
            {
                let _ = std::fs::remove_dir(&lock_path);
            }
            Ok(None)
        }
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
    }
}
