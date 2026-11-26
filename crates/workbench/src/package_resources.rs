//! Read-only discovery in package directories granted by a native SDK host.
use std::collections::BTreeMap;
use std::fs::{self, Metadata};
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};

use globset::GlobMatcher;
use pi_rs::{Error, Result};

use crate::package_manifest::{
    extension_stem_matches, has_resource_fields, is_native_executable, local_startup_manifest,
    manifest_pi, resource_glob, resource_patterns, select_resource_with, skill_directory,
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
    /// A scan can discover a file symlink whose target is outside the package.
    /// File reads through the resulting paths follow those links.
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
                let candidates = discover_kind(root, kind, &patterns, explicit, &mut examined)?;
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
            Self::Extension => is_native_executable(path, metadata),
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

fn has_glob(pattern: &str) -> bool {
    pattern.contains(['*', '?', '[', '{'])
}

fn normalize_pattern(pattern: &str) -> Result<String> {
    let normalized = pattern.replace('\\', "/");
    let value = normalized
        .strip_prefix(['!', '+', '-'])
        .unwrap_or(&normalized);
    if value.is_empty()
        || value.contains(':')
        || value == "~"
        || value.starts_with("~/")
        || Path::new(value)
            .components()
            .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
    {
        return Err(Error::Config(format!(
            "package resource pattern must be relative without parent components: {pattern:?}"
        )));
    }
    Ok(normalized)
}

fn discover_kind(
    root: &Path,
    kind: Kind,
    patterns: &[String],
    explicit: bool,
    examined: &mut usize,
) -> Result<Vec<PathBuf>> {
    if patterns.is_empty() {
        return Ok(Vec::new());
    }
    if patterns.len() > MAX_PATHS {
        return Err(Error::Config("too many package resource patterns".into()));
    }
    let patterns = patterns
        .iter()
        .map(|pattern| normalize_pattern(pattern))
        .collect::<Result<Vec<_>>>()?;
    let mut candidates = if matches!(kind, Kind::Extension) {
        discover_extensions(root, &patterns, explicit, examined)?
    } else {
        discover_file_resources(root, kind, &patterns, explicit, examined)?
    };
    candidates.sort();
    candidates.dedup();
    Ok(candidates)
}

fn discover_extensions(
    root: &Path,
    patterns: &[String],
    explicit: bool,
    examined: &mut usize,
) -> Result<Vec<PathBuf>> {
    let extension_dir = root.join("extensions");
    let mut candidates = Vec::new();
    if metadata(&extension_dir)?.is_some_and(|metadata| metadata.is_dir()) {
        for entry in fs::read_dir(&extension_dir)? {
            count_entry(examined)?;
            let path = entry?.path();
            if metadata(&path)?.is_some_and(|metadata| is_native_executable(&path, &metadata)) {
                candidates.push(path);
            }
        }
    }

    let mut matchers = BTreeMap::new();
    for pattern in patterns {
        if pattern.starts_with(['+', '-']) {
            continue;
        }
        let value = pattern.strip_prefix('!').unwrap_or(pattern);
        matchers.insert(value, compile_glob(value)?);
        if explicit && !pattern.starts_with('!') && !has_glob(value) {
            let path = root.join(value);
            if metadata(&path)?.is_some_and(|metadata| is_native_executable(&path, &metadata)) {
                candidates.push(path);
            }
        }
    }
    candidates.retain(|path| {
        select_resource_with(patterns, |pattern, exact| {
            extension_stem_matches(path, pattern)
                || if exact {
                    path == &root.join(pattern)
                } else {
                    matches_file(path, root, pattern, &matchers[pattern])
                }
        })
    });
    Ok(candidates)
}

fn discover_file_resources(
    root: &Path,
    kind: Kind,
    patterns: &[String],
    explicit: bool,
    examined: &mut usize,
) -> Result<Vec<PathBuf>> {
    let mut sources = Vec::new();
    let mut includes = Vec::new();
    let mut excludes = Vec::new();
    let mut force_includes = Vec::new();
    let mut force_excludes = Vec::new();
    for pattern in patterns {
        if let Some(value) = pattern.strip_prefix('+') {
            force_includes.push(root.join(value));
        } else if let Some(value) = pattern.strip_prefix('-') {
            force_excludes.push(root.join(value));
        } else if let Some(value) = pattern.strip_prefix('!') {
            excludes.push((value, compile_glob(value)?));
        } else if has_glob(pattern) {
            includes.push(compile_glob(pattern)?);
        } else {
            sources.push(root.join(pattern));
        }
    }

    let mut candidates = Vec::new();
    let implicit = includes.is_empty() && sources.is_empty();
    let mut skill_names = Vec::new();
    for source in &sources {
        match metadata(source)? {
            Some(info) if info.is_dir() => {
                scan(source, kind, examined, |path| candidates.push(path))?;
            }
            Some(info) if kind.accepts(source, &info) => candidates.push(source.clone()),
            None if explicit && matches!(kind, Kind::Skill) && source.parent() == Some(root) => {
                if let Some(name) = source.file_name() {
                    skill_names.push(name);
                }
            }
            _ => {}
        }
    }
    if implicit || !includes.is_empty() || !skill_names.is_empty() {
        scan(root, kind, examined, |path| {
            let relative = path
                .strip_prefix(root)
                .expect("package scan stays in its root");
            let skill_name_matches = matches!(kind, Kind::Skill)
                && skill_directory(&path)
                    .and_then(Path::file_name)
                    .is_some_and(|name| {
                        skill_names.contains(&name)
                            || includes.iter().any(|glob| glob.is_match(name))
                    });
            if implicit
                || skill_name_matches
                || relative.ancestors().any(|candidate| {
                    !candidate.as_os_str().is_empty()
                        && includes.iter().any(|glob| glob.is_match(candidate))
                })
            {
                candidates.push(path);
            }
        })?;
    }
    candidates.retain(|path| {
        !excludes.iter().any(|(pattern, glob)| {
            matches_file(path, root, pattern, glob)
                || matches!(kind, Kind::Skill)
                    && skill_directory(path)
                        .is_some_and(|parent| matches_file(parent, root, pattern, glob))
        })
    });
    for mut path in force_includes {
        let Some(mut info) = metadata(&path)? else {
            continue;
        };
        if info.is_dir() && matches!(kind, Kind::Skill) {
            path.push("SKILL.md");
            let Some(index) = metadata(&path)? else {
                continue;
            };
            info = index;
        }
        if kind.accepts(&path, &info) {
            candidates.push(path);
        }
    }
    candidates.retain(|path| {
        !force_excludes.contains(path)
            && !(matches!(kind, Kind::Skill)
                && skill_directory(path)
                    .is_some_and(|parent| force_excludes.iter().any(|target| target == parent)))
    });
    Ok(candidates)
}

fn matches_file(path: &Path, root: &Path, pattern: &str, glob: &GlobMatcher) -> bool {
    let relative = path
        .strip_prefix(root)
        .expect("package path is root-relative");
    glob.is_match(relative)
        || path.file_name().is_some_and(|name| glob.is_match(name))
        || (!has_glob(pattern) && relative.starts_with(pattern.trim_start_matches("./")))
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
        count_entry(examined)?;
        if entry.depth() > MAX_DEPTH {
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

fn count_entry(examined: &mut usize) -> Result<()> {
    *examined += 1;
    if *examined > MAX_ENTRIES {
        return Err(Error::Config(
            "package directory scan limit exceeded".into(),
        ));
    }
    Ok(())
}
