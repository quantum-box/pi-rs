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
fn same_dir(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    a.dev() == b.dev() && a.ino() == b.ino()
}
#[cfg(not(unix))]
fn same_dir(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    a.created()
        .ok()
        .zip(b.created().ok())
        .is_some_and(|(a, b)| a == b)
}
fn owns(path: &Path, identity: &std::fs::Metadata) -> bool {
    std::fs::metadata(path).is_ok_and(|current| same_dir(identity, &current))
}
fn busy() -> Error {
    Error::Config("auth.json lock is busy or compromised".into())
}

pub(super) struct Guard {
    path: PathBuf,
    identity: std::fs::Metadata,
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
            let identity = std::fs::metadata(&lock_path)?;
            let thread_identity = std::fs::metadata(&lock_path)?;
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
        assert!(first.check().is_err());
        drop(first);
        assert!(lock.exists());
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
