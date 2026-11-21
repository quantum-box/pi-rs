//! Coordinate CLI OAuth finalization with the existing MCP configuration writers.
use pi_rs::{Error, Result};
use std::fs::{File, OpenOptions, TryLockError};
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub(crate) async fn lock(paths: Vec<PathBuf>, credential_lock: Arc<File>) -> Result<Vec<File>> {
    let files = tokio::task::spawn_blocking(move || open_locks(paths, &credential_lock))
        .await
        .map_err(|_| Error::Config("MCP configuration lock worker stopped".into()))??;
    loop {
        let mut locked = Vec::with_capacity(files.len());
        for file in &files {
            match file.try_lock_shared() {
                Ok(()) => locked.push(file),
                Err(TryLockError::WouldBlock) => break,
                Err(TryLockError::Error(error)) => {
                    release(&locked)?;
                    return Err(error.into());
                }
            }
        }
        if locked.len() == files.len() {
            return Ok(files);
        }
        // Release partial acquisitions before yielding. Paths may alias
        // across projects and agent directories, including via hard links.
        release(&locked)?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn open_locks(paths: Vec<PathBuf>, credential_lock: &File) -> Result<Vec<File>> {
    let credential = same_file::Handle::from_file(credential_lock.try_clone()?)?;
    let mut identities = Vec::with_capacity(paths.len());
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let path = path.with_extension("lock");
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&path)?
            }
            Err(error) => return Err(error.into()),
        };
        let identity = same_file::Handle::from_file(file.try_clone()?)?;
        if identity == credential {
            return Err(Error::Config(
                "MCP configuration and credential locks refer to the same file".into(),
            ));
        }
        if !identities.contains(&identity) {
            identities.push(identity);
            files.push(file);
        }
    }
    Ok(files)
}

fn release(locked: &[&File]) -> Result<()> {
    for file in locked.iter().rev() {
        file.unlock()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn configuration_writer_can_proceed_after_cancelled_login_wait() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("global.json");
        let second = dir.path().join("project.json");
        let credential = Arc::new(File::create(dir.path().join("credential.lock")).unwrap());
        credential.lock().unwrap();
        let writer = File::create(second.with_extension("lock")).unwrap();
        writer.lock().unwrap();

        let waiting = lock(vec![first.clone(), second.clone()], credential.clone());
        assert!(
            tokio::time::timeout(Duration::from_millis(250), waiting)
                .await
                .is_err()
        );

        let probe = OpenOptions::new()
            .read(true)
            .write(true)
            .open(first.with_extension("lock"))
            .unwrap();
        probe.try_lock().unwrap();
        probe.unlock().unwrap();
        writer.unlock().unwrap();

        let held = tokio::time::timeout(
            Duration::from_secs(5),
            lock(vec![first, second], credential),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(writer.try_lock(), Err(TryLockError::WouldBlock)));
        drop(held);
        writer.try_lock().unwrap();
    }

    #[tokio::test]
    async fn detects_configuration_and_credential_lock_aliases() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("mcp.json");
        let credential = Arc::new(File::create(dir.path().join("credential.lock")).unwrap());
        credential.lock().unwrap();
        std::fs::hard_link(
            dir.path().join("credential.lock"),
            config.with_extension("lock"),
        )
        .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), lock(vec![config], credential))
            .await
            .unwrap();
        assert!(matches!(result, Err(Error::Config(message)) if message.contains("same file")));
    }
}
