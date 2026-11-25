//! Read-only discovery in package directories granted by a native SDK host.
use std::fs::{self, Metadata};
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};

use globset::GlobMatcher;
use pi_rs::{Error, Result};

use crate::package_manifest::{
    has_resource_fields, local_startup_manifest, manifest_pi, resource_glob, resource_patterns,
};

const MAX_PATHS: usize = 128;
const MAX_ENTRIES: usize = 10_000;
const MAX_DEPTH: usize = 64;

/// Paths in host-granted package roots. Executables are candidates for the
/// host to authorize separately through `pi_rs_extensions::NativeExtensionLoader`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PackageResourcePaths {
    pub extension_candidates: Vec<PathBuf>,
    pub skills: Vec<PathBuf>,
    pub prompts: Vec<PathBuf>,
    pub themes: Vec<PathBuf>,
}

impl PackageResourcePaths {
    /// Inspect local package directories in grant order, using the native
    /// manifest precedence. Reads are synchronous. Grants follow symlinks in
    /// exact paths, including the roots and manifests. Recursive scans skip
    /// hidden entries, dependency/build directories, and symlinked directories.
    /// This operation performs filesystem discovery only.
    pub fn discover(directories: &[PathBuf]) -> Result<Self> {
        if directories.len() > MAX_PATHS {
            return Err(Error::Config("too many package directories".into()));
        }
        for directory in directories {
            if !directory.is_absolute()
                || directory
                    .components()
                    .any(|part| part == Component::ParentDir)
            {
                return Err(Error::Config(format!(
                    "package grant must be absolute without parent components: {}",
                    directory.display()
                )));
            }
        }
        let mut paths = Self::default();
        let mut examined = 0;
        for root in directories {
            if !fs::metadata(root)?.is_dir() {
                return Err(Error::Config(format!(
                    "package grant must be a directory: {}",
                    root.display()
                )));
            }
            let (manifest, _) = local_startup_manifest(root)?;
            let pi = manifest_pi(&manifest)?;
            let explicit = has_resource_fields(pi);
            for (kind, output) in [
                (Kind::Extension, &mut paths.extension_candidates),
                (Kind::Skill, &mut paths.skills),
                (Kind::Prompt, &mut paths.prompts),
                (Kind::Theme, &mut paths.themes),
            ] {
                let name = kind.directory();
                let patterns = if explicit {
                    resource_patterns(pi, name)?
                } else {
                    vec![name.into()]
                };
                let candidates = discover_kind(root, kind, &patterns, &mut examined)?;
                for path in candidates {
                    if !output.contains(&path) {
                        if output.len() == MAX_PATHS {
                            return Err(Error::Config(format!("too many package {name}")));
                        }
                        output.push(path);
                    }
                }
            }
        }
        Ok(paths)
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Extension,
    Skill,
    Prompt,
    Theme,
}

impl Kind {
    fn directory(self) -> &'static str {
        match self {
            Self::Extension => "extensions",
            Self::Skill => "skills",
            Self::Prompt => "prompts",
            Self::Theme => "themes",
        }
    }

    fn accepts(self, path: &Path, metadata: &Metadata) -> bool {
        if !metadata.is_file() {
            return false;
        }
        match self {
            Self::Extension => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    metadata.permissions().mode() & 0o111 != 0
                }
                #[cfg(not(unix))]
                path.extension().is_some_and(|ext| ext == "exe")
            }
            Self::Skill => path.file_name().is_some_and(|name| name == "SKILL.md"),
            Self::Prompt => has_suffix(path, b".md"),
            Self::Theme => has_suffix(path, b".json"),
        }
    }
}

fn has_suffix(path: &Path, suffix: &[u8]) -> bool {
    path.file_name()
        .is_some_and(|name| name.as_encoded_bytes().ends_with(suffix))
}

fn discover_kind(
    root: &Path,
    kind: Kind,
    patterns: &[String],
    examined: &mut usize,
) -> Result<Vec<PathBuf>> {
    if patterns.is_empty() {
        return Ok(Vec::new());
    }
    if patterns.len() > MAX_PATHS {
        return Err(Error::Config("too many package resource patterns".into()));
    }
    let mut sources = Vec::new();
    let mut includes = Vec::new();
    let mut excludes = Vec::new();
    let mut force_includes = Vec::new();
    let mut force_excludes = Vec::new();
    for pattern in patterns {
        let (prefix, value) = match pattern.as_bytes().first() {
            Some(b'!' | b'+' | b'-') => (pattern.as_bytes()[0], &pattern[1..]),
            _ => (b' ', pattern.as_str()),
        };
        let value = value.replace('\\', "/");
        let path = Path::new(&value);
        if value.is_empty()
            || value.contains(':')
            || value == "~"
            || value.starts_with("~/")
            || path
                .components()
                .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
        {
            return Err(Error::Config(format!(
                "package resource pattern must be relative without parent components: {pattern:?}"
            )));
        }
        match prefix {
            b'+' => force_includes.push(root.join(path)),
            b'-' => force_excludes.push(root.join(path)),
            b'!' => excludes.push(compile_glob(&value)?),
            _ if value.contains(['*', '?', '[', '{']) => includes.push(compile_glob(&value)?),
            _ => sources.push(root.join(path)),
        }
    }

    let mut candidates = Vec::new();
    if !includes.is_empty() || sources.is_empty() && force_includes.is_empty() {
        scan(root, kind, examined, |path| {
            let relative = path
                .strip_prefix(root)
                .expect("package scan stays in its root");
            if includes.is_empty()
                || relative.ancestors().any(|candidate| {
                    !candidate.as_os_str().is_empty()
                        && includes.iter().any(|glob| glob.is_match(candidate))
                })
            {
                candidates.push(path);
            }
        })?;
    }
    for source in sources {
        if let Some(metadata) = metadata(&source)? {
            if metadata.is_dir() {
                scan(&source, kind, examined, |path| candidates.push(path))?;
            } else if kind.accepts(&source, &metadata) {
                candidates.push(source);
            }
        }
    }
    candidates.retain(|path| {
        let relative = path
            .strip_prefix(root)
            .expect("package path is root-relative");
        !excludes.iter().any(|glob| glob.is_match(relative))
    });
    for path in force_includes {
        if metadata(&path)?.is_some_and(|metadata| kind.accepts(&path, &metadata)) {
            candidates.push(path);
        }
    }
    candidates.retain(|path| !force_excludes.contains(path));
    candidates.sort();
    candidates.dedup();
    Ok(candidates)
}

fn compile_glob(value: &str) -> Result<GlobMatcher> {
    resource_glob(value.trim_start_matches("./"), false)
        .map(|glob| glob.compile_matcher())
        .map_err(|error| {
            Error::Config(format!(
                "invalid package resource pattern {value:?}: {error}"
            ))
        })
}

fn metadata(path: &Path) -> Result<Option<Metadata>> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn scan(
    root: &Path,
    kind: Kind,
    examined: &mut usize,
    mut accept: impl FnMut(PathBuf),
) -> Result<()> {
    for entry in ignore::WalkBuilder::new(root)
        .standard_filters(false)
        .hidden(true)
        .max_depth(Some(MAX_DEPTH + 1))
        .filter_entry(|entry| {
            entry.depth() == 0
                || !matches!(entry.file_name().to_str(), Some("target" | "node_modules"))
        })
        .build()
    {
        let entry =
            entry.map_err(|error| Error::Config(format!("package scan failed: {error}")))?;
        if entry.depth() == 0 {
            continue;
        }
        *examined += 1;
        if *examined > MAX_ENTRIES || entry.depth() > MAX_DEPTH {
            return Err(Error::Config(
                "package directory scan limit exceeded".into(),
            ));
        }
        let path = entry.into_path();
        if metadata(&path)?.is_some_and(|metadata| kind.accepts(&path, &metadata)) {
            accept(path);
        }
    }
    Ok(())
}
