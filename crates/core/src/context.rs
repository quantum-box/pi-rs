use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use cap_std::fs::Dir;
use sha2::{Digest, Sha256};

use crate::{Error, Result};

const MAX_RESOURCE_BYTES: u64 = 64 * 1024;
const MAX_CONTEXT_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resource {
    pub id: String,
    pub version: String,
    pub text: String,
}

/// Local resource adapter bound to one project. A host that changes access must
/// check its authorization before loading resources for each request.
pub struct ProjectResources {
    root: Dir,
    ancestors: Vec<PathBuf>,
}

impl ProjectResources {
    pub fn open(root: impl AsRef<Path>, cwd: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().canonicalize()?;
        let cwd = cwd.as_ref().canonicalize()?;
        let relative = cwd
            .strip_prefix(&root)
            .map_err(|_| Error::Config("working directory is outside project".into()))?;
        let mut ancestors: Vec<_> = relative.ancestors().map(Path::to_path_buf).collect();
        ancestors.reverse();
        let root = Dir::open_ambient_dir(root, cap_std::ambient_authority())?;
        Ok(Self { root, ancestors })
    }

    /// Names are listed without loading skill bodies. The closest directory wins.
    pub fn skills(&self) -> Result<Vec<String>> {
        Ok(self.skill_paths()?.into_keys().collect())
    }

    pub fn instructions(&self) -> Result<Vec<Resource>> {
        let mut resources = Vec::new();
        for ancestor in &self.ancestors {
            let path = ancestor.join("AGENTS.md");
            match self.root.symlink_metadata(&path) {
                Ok(_) => resources.push(self.read(&path)?),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(resources)
    }

    pub fn skill(&self, name: &str, expected_version: Option<&str>) -> Result<Resource> {
        if !valid_name(name) {
            return Err(Error::Config("invalid skill name".into()));
        }
        let paths = self.skill_paths()?;
        let path = paths
            .get(name)
            .ok_or_else(|| Error::Config(format!("skill {name} is unavailable")))?;
        let resource = self.read(path)?;
        if expected_version.is_some_and(|version| resource.version != version) {
            return Err(Error::Config(format!(
                "skill {name} changed since selection"
            )));
        }
        Ok(resource)
    }

    pub fn context(&self, skills: &[String]) -> Result<String> {
        let mut resources = self.instructions()?;
        for name in skills {
            resources.push(self.skill(name, None)?);
        }
        let mut output = String::new();
        for resource in resources {
            let entry = format!(
                "\n\n# Project resource: {} (sha256:{})\n{}",
                resource.id, resource.version, resource.text
            );
            if output.len() + entry.len() > MAX_CONTEXT_BYTES {
                return Err(Error::Config(
                    "project instructions or skills exceed size limit".into(),
                ));
            }
            output.push_str(&entry);
        }
        Ok(output)
    }

    fn skill_paths(&self) -> Result<BTreeMap<String, PathBuf>> {
        let mut paths = BTreeMap::new();
        for ancestor in &self.ancestors {
            let directory = ancestor.join(".pi/skills");
            let entries = match self.root.read_dir(&directory) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            for entry in entries {
                let entry = entry?;
                let Ok(name) = entry.file_name().into_string() else {
                    continue;
                };
                if valid_name(&name) {
                    let path = directory.join(&name).join("SKILL.md");
                    match self.root.symlink_metadata(&path) {
                        Ok(_) => {
                            paths.insert(name, path);
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error.into()),
                    }
                }
            }
        }
        Ok(paths)
    }

    fn read(&self, path: &Path) -> Result<Resource> {
        if !self.root.metadata(path)?.is_file() {
            return Err(Error::Config(
                "project resource must be a regular file".into(),
            ));
        }
        let mut text = String::new();
        self.root
            .open(path)?
            .take(MAX_RESOURCE_BYTES + 1)
            .read_to_string(&mut text)?;
        if text.len() as u64 > MAX_RESOURCE_BYTES {
            return Err(Error::Config("project resource exceeds size limit".into()));
        }
        Ok(Resource {
            id: path
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/"),
            version: format!("{:x}", Sha256::digest(text.as_bytes())),
            text,
        })
    }
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
}

pub fn project_context(
    root: impl AsRef<Path>,
    cwd: impl AsRef<Path>,
    skills: &[String],
) -> Result<String> {
    ProjectResources::open(root, cwd)?.context(skills)
}
