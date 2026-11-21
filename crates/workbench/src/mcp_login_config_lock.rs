//! Coordinate CLI OAuth finalization with the existing MCP configuration writers.
use pi_rs::{Error, Result};
use std::fs::{File, OpenOptions, TryLockError};
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub(crate) async fn lock(paths: Vec<PathBuf>, credential_lock: Arc<File>) -> Result<Vec<File>> {
    acquire(paths, Some(credential_lock)).await
}

pub async fn lock_edit(path: PathBuf, other_scope: Option<PathBuf>) -> Result<Vec<File>> {
    let paths = std::iter::once(path).chain(other_scope).collect();
    acquire(paths, None).await
}

async fn acquire(paths: Vec<PathBuf>, credential_lock: Option<Arc<File>>) -> Result<Vec<File>> {
    let files = tokio::task::spawn_blocking(move || open_locks(paths, credential_lock.as_deref()))
        .await
        .map_err(|_| Error::Config("MCP configuration lock worker stopped".into()))??;
    loop {
        let mut locked = Vec::with_capacity(files.len());
        for (file, exclusive) in &files {
            let acquired = if *exclusive {
                file.try_lock()
            } else {
                file.try_lock_shared()
            };
            match acquired {
                Ok(()) => locked.push(file),
                Err(TryLockError::WouldBlock) => break,
                Err(TryLockError::Error(error)) => {
                    release(&locked)?;
                    return Err(error.into());
                }
            }
        }
        if locked.len() == files.len() {
            return Ok(files.into_iter().map(|(file, _)| file).collect());
        }
        // Release partial acquisitions before yielding. Paths may alias
        // across projects and agent directories, including via hard links.
        release(&locked)?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn open_locks(paths: Vec<PathBuf>, credential_lock: Option<&File>) -> Result<Vec<(File, bool)>> {
    let credential = credential_lock
        .map(|file| same_file::Handle::from_file(file.try_clone()?))
        .transpose()?;
    let editing = credential.is_none();
    let mut identities = Vec::with_capacity(paths.len());
    let mut files = Vec::with_capacity(paths.len());
    for (index, path) in paths.into_iter().enumerate() {
        let path = path.with_extension("lock");
        let exclusive = editing && index == 0;
        // A global edit should not need to create or write a project
        // directory. When the other scope has a lock, read it shared.
        let optional = editing && !exclusive;
        let open_writer = || {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
        };
        let file = if exclusive {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            open_writer()?
        } else {
            match File::open(&path) {
                Ok(file) => file,
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    if !optional && let Some(parent) = path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    match open_writer() {
                        Ok(file) => file,
                        Err(error)
                            if optional
                                && matches!(
                                    error.kind(),
                                    ErrorKind::NotFound
                                        | ErrorKind::PermissionDenied
                                        | ErrorKind::ReadOnlyFilesystem
                                ) =>
                        {
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                Err(error) => return Err(error.into()),
            }
        };
        let identity = same_file::Handle::from_file(file.try_clone()?)?;
        if credential.as_ref() == Some(&identity) {
            return Err(Error::Config(
                "MCP configuration and credential locks refer to the same file".into(),
            ));
        }
        if !identities.contains(&identity) {
            identities.push(identity);
            files.push((file, exclusive));
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
    async fn edits_share_the_other_scope_lock_without_creating_its_project_directory() {
        let dir = tempfile::tempdir().unwrap();
        let global = dir.path().join("global.json");
        let project_dir = dir.path().join("project/.pi");
        let project = project_dir.join("mcp.json");
        let held = lock_edit(global.clone(), Some(project.clone()))
            .await
            .unwrap();
        assert!(!project_dir.exists());
        let create = lock_edit(project.clone(), Some(global.clone()));
        assert!(
            tokio::time::timeout(Duration::from_millis(250), create)
                .await
                .is_err()
        );
        drop(held);

        assert!(project_dir.exists());
        let writer = File::create(project.with_extension("lock")).unwrap();
        writer.lock().unwrap();
        let waiting = lock_edit(global.clone(), Some(project.clone()));
        assert!(
            tokio::time::timeout(Duration::from_millis(250), waiting)
                .await
                .is_err()
        );
        writer.unlock().unwrap();

        let held = lock_edit(global, Some(project)).await.unwrap();
        assert!(matches!(writer.try_lock(), Err(TryLockError::WouldBlock)));
        drop(held);
        writer.try_lock().unwrap();
    }

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
