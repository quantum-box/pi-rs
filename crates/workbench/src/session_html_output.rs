use std::fs::File;
use std::io::Write;
use std::path::Path;

use pi_rs::{Error, Result};
use tempfile::{NamedTempFile, PersistError};

pub(crate) fn publish(
    document: &str,
    destination: &Path,
    persist: impl FnOnce(NamedTempFile, &Path) -> std::result::Result<File, PersistError>,
) -> Result<()> {
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    #[cfg(unix)]
    let parent_dir = File::open(parent)?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    temporary.write_all(document.as_bytes())?;
    temporary.as_file().sync_all()?;
    persist(temporary, destination).map_err(|error| Error::Io(error.error))?;
    #[cfg(unix)]
    parent_dir.sync_all()?;
    Ok(())
}
