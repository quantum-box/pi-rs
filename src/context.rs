use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::{Error, Result};

const MAX_RESOURCE_BYTES: u64 = 64 * 1024;
const MAX_CONTEXT_BYTES: usize = 256 * 1024;

/// Load trusted project instructions and opt-in skills. A skill name resolves to
/// .pi/skills/<name>/SKILL.md. Canonical paths must stay inside the project.
pub fn project_context(
    root: impl AsRef<Path>,
    cwd: impl AsRef<Path>,
    skills: &[String],
) -> Result<String> {
    let root = root.as_ref().canonicalize()?;
    let cwd = cwd.as_ref().canonicalize()?;
    let relative = cwd
        .strip_prefix(&root)
        .map_err(|_| Error::Config("working directory is outside project".into()))?;
    let mut dirs = vec![root.clone()];
    for part in relative {
        dirs.push(dirs.last().unwrap().join(part));
    }
    let mut output = String::new();
    for dir in dirs {
        let path = dir.join("AGENTS.md");
        match path.symlink_metadata() {
            Ok(_) => append(&root, path, &mut output)?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
    }
    for name in skills {
        if name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(Error::Config("invalid skill name".into()));
        }
        append(
            &root,
            root.join(".pi/skills").join(name).join("SKILL.md"),
            &mut output,
        )?;
    }
    Ok(output)
}

fn append(root: &Path, path: PathBuf, output: &mut String) -> Result<()> {
    let target = path.canonicalize()?;
    if !target.starts_with(root) || !target.is_file() {
        return Err(Error::Config(
            "project resource must be a file within the project".into(),
        ));
    }
    let mut content = String::new();
    File::open(target)?
        .take(MAX_RESOURCE_BYTES + 1)
        .read_to_string(&mut content)?;
    if content.len() as u64 > MAX_RESOURCE_BYTES || output.len() + content.len() > MAX_CONTEXT_BYTES
    {
        return Err(Error::Config(
            "project instructions or skills exceed size limit".into(),
        ));
    }
    let label = path.strip_prefix(root).unwrap_or(&path).display();
    output.push_str(&format!("\n\n# Project resource: {label}\n{content}"));
    Ok(())
}
