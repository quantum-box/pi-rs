//! Read-only discovery in package directories granted by a native SDK host.
use std::collections::BTreeMap;
use std::fs::{self, Metadata};
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};

use pi_rs::{Error, Result};

use crate::package_manifest::{
    extension_stem_matches, has_resource_fields, is_native_executable, local_startup_manifest,
    manifest_pi, resource_patterns, select_resource_with,
};

use crate::package_pattern::{ResourceMatcher, has_extglob};

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

#[derive(Clone, Copy, PartialEq, Eq)]
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
    pattern.contains(['*', '?', '[', '{']) || has_extglob(pattern)
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

    if explicit {
        let matcher = ManifestMatcher::new(patterns);
        for path in literal_sources(root, patterns) {
            if metadata(&path)?.is_some_and(|metadata| is_native_executable(&path, &metadata)) {
                candidates.push(path);
            }
        }
        candidates.retain(|path| matcher.selects(path, root, Kind::Extension, patterns));
    }
    Ok(candidates)
}

fn literal_sources<'a>(
    root: &'a Path,
    patterns: &'a [String],
) -> impl Iterator<Item = PathBuf> + 'a {
    patterns
        .iter()
        .filter(|pattern| !is_override(pattern) && !has_glob(pattern))
        .map(|pattern| root.join(pattern))
}

fn is_override(pattern: &str) -> bool {
    pattern.starts_with(['!', '+', '-'])
}

struct ManifestMatcher<'a> {
    patterns: BTreeMap<(&'a str, bool), ResourceMatcher<'a>>,
}

impl<'a> ManifestMatcher<'a> {
    fn new(patterns: &'a [String]) -> Self {
        let patterns = patterns
            .iter()
            .map(|pattern| {
                let exact = pattern.starts_with(['+', '-']);
                let value = pattern.strip_prefix(['!', '+', '-']).unwrap_or(pattern);
                let normalized = if exact {
                    value.strip_prefix("./").unwrap_or(value)
                } else {
                    value
                };
                (
                    (value, exact),
                    ResourceMatcher::new(normalized, exact, true),
                )
            })
            .collect();
        Self { patterns }
    }

    fn matches(&self, path: &Path, root: &Path, pattern: &str, exact: bool) -> bool {
        self.patterns[&(pattern, exact)].matches(path, root)
    }

    fn selects(&self, path: &Path, root: &Path, kind: Kind, patterns: &[String]) -> bool {
        select_resource_with(patterns, |pattern, exact| {
            (kind == Kind::Extension && extension_stem_matches(path, pattern))
                || self.matches(path, root, pattern, exact)
        })
    }
}

fn discover_file_resources(
    root: &Path,
    kind: Kind,
    patterns: &[String],
    explicit: bool,
    examined: &mut usize,
) -> Result<Vec<PathBuf>> {
    let matcher = ManifestMatcher::new(patterns);
    let mut candidates = Vec::new();
    // Retain exact sources, including authorized directory symlinks that a
    // recursive scan of the package would skip.
    for source in literal_sources(root, patterns) {
        if let Some(info) = metadata(&source)? {
            if info.is_dir() {
                scan(&source, kind, examined, |path| candidates.push(path))?;
            } else if kind.accepts(&source, &info) {
                candidates.push(source);
            }
        }
    }
    if !explicit {
        return Ok(candidates);
    }

    if kind == Kind::Prompt {
        collect_prompt_matches(root, patterns, &matcher, examined, &mut candidates)?;
    } else if needs_package_scan(patterns) {
        // For skills and themes the native matcher selects files and skill
        // directories, never arbitrary glob-matched ancestors.
        scan(root, kind, examined, |path| {
            if matcher.selects(&path, root, kind, patterns) {
                candidates.push(path);
            }
        })?;
    }

    for value in patterns
        .iter()
        .filter_map(|pattern| pattern.strip_prefix('+'))
    {
        let mut path = root.join(value);
        let Some(mut info) = metadata(&path)? else {
            continue;
        };
        if info.is_dir() && kind == Kind::Skill {
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
    let overrides: Vec<_> = patterns
        .iter()
        .filter(|pattern| is_override(pattern))
        .cloned()
        .collect();
    let selection = if kind == Kind::Prompt {
        &overrides
    } else {
        patterns
    };
    if !selection.is_empty() {
        candidates.retain(|path| matcher.selects(path, root, kind, selection));
    }
    Ok(candidates)
}

fn needs_package_scan(patterns: &[String]) -> bool {
    let mut plain = patterns
        .iter()
        .filter(|pattern| !is_override(pattern))
        .peekable();
    plain.peek().is_none()
        || plain.any(|pattern| has_glob(pattern) || !pattern.trim_start_matches("./").contains('/'))
}

fn collect_prompt_matches(
    root: &Path,
    patterns: &[String],
    matcher: &ManifestMatcher<'_>,
    examined: &mut usize,
    candidates: &mut Vec<PathBuf>,
) -> Result<()> {
    let mut plain = patterns
        .iter()
        .filter(|pattern| !is_override(pattern))
        .peekable();
    let implicit = plain.peek().is_none();
    let scan_patterns: Vec<_> = plain
        .filter(|pattern| {
            has_glob(pattern)
                || (!pattern.trim_start_matches("./").contains('/') && pattern.ends_with(".md"))
        })
        .collect();
    if implicit || !scan_patterns.is_empty() {
        scan(root, Kind::Prompt, examined, |path| {
            let selected = implicit
                || scan_patterns.iter().any(|pattern| {
                    matcher.matches(&path, root, pattern, false)
                    // A qualified prompt glob can resolve to a directory
                    // source. Unqualified globs match file basenames only.
                    || (pattern.trim_start_matches("./").contains('/')
                        && path.ancestors().skip(1).take_while(|parent| *parent != root)
                            .any(|parent| matcher.matches(parent, root, pattern, false)))
                });
            if selected {
                candidates.push(path);
            }
        })?;
    }
    Ok(())
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
