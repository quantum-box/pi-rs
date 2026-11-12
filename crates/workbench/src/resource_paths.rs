//! Resource discovery keeps user and trusted-project paths in their own scopes.
use std::path::{Path, PathBuf};

use pi_rs::{Error, Result};

use super::{Cli, packages};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Extension,
    Skill,
    Prompt,
    Theme,
}
impl Kind {
    fn key(self) -> &'static str {
        match self {
            Self::Extension => "extensions",
            Self::Skill => "skills",
            Self::Prompt => "prompts",
            Self::Theme => "themes",
        }
    }
    fn disabled(self, args: &Cli) -> bool {
        match self {
            Self::Extension => args.no_extensions,
            Self::Skill => args.no_skills,
            Self::Prompt => args.no_prompt_templates,
            Self::Theme => args.no_themes,
        }
    }
    fn accepts(self, path: &Path) -> bool {
        match self {
            Self::Skill => path.file_name().is_some_and(|name| name == "SKILL.md"),
            Self::Prompt => path.extension().is_some_and(|extension| extension == "md"),
            Self::Theme => path
                .extension()
                .is_some_and(|extension| extension == "json"),
            Self::Extension => executable(path),
        }
    }
}
fn executable(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        path.metadata()
            .is_ok_and(|metadata| metadata.permissions().mode() & 0o111 != 0)
    }
    #[cfg(not(unix))]
    {
        path.extension().is_some_and(|extension| extension == "exe")
    }
}
fn has_pattern(value: &str) -> bool {
    value.contains(['*', '?', '[', '{'])
}
pub fn skill_path(args: &Cli, value: &str) -> Option<PathBuf> {
    let workspace = args
        .workspace
        .canonicalize()
        .unwrap_or_else(|_| args.workspace.clone());
    let path = absolute(&workspace, value);
    (args.project.is_none() || path.exists() || has_pattern(value) || value.contains(['/', '\\']))
        .then_some(path)
}
pub fn named_extension(value: &str) -> bool {
    value.starts_with("builtin:") || matches!(value, "notes" | "todo")
}
pub fn builtin_enabled(args: &Cli, name: &str) -> Result<bool> {
    if args.extensions.iter().any(|entry| entry == name) {
        return Ok(true);
    }
    if args.no_extensions {
        return Ok(false);
    }
    let mut enabled = true;
    for (base, entries) in args.settings.resource_layers("extensions")? {
        for entry in entries {
            let Some((prefix, pattern)) = entry
                .chars()
                .next()
                .map(|prefix| (prefix, &entry[prefix.len_utf8()..]))
            else {
                continue;
            };
            if !matches!(prefix, '+' | '-' | '!') {
                continue;
            }
            if packages::resource_matches(Path::new(name), &base, pattern, prefix != '!', false) {
                enabled = prefix == '+';
            }
        }
    }
    Ok(enabled)
}
pub fn resolve(
    args: &Cli,
    kind: Kind,
    packaged: &[PathBuf],
    explicit: &[PathBuf],
    warnings: &mut Vec<String>,
) -> Result<Vec<PathBuf>> {
    let workspace = args.workspace.canonicalize()?;
    let mut paths = Vec::new();
    for path in explicit {
        let path = absolute(&workspace, &path.to_string_lossy());
        if kind == Kind::Prompt && path.is_dir() {
            for found in expand(&path, kind, true, warnings)? {
                if !paths.contains(&found) {
                    paths.push(found);
                }
            }
            continue;
        }
        append(&mut paths, expand(&path, kind, true, warnings)?);
    }
    if kind.disabled(args) {
        return Ok(paths);
    }
    let mut layers = args.settings.resource_layers(kind.key())?;
    let project = workspace.join(".pi");
    let project = project.canonicalize().unwrap_or(project);
    if args.trusted
        && !layers
            .iter()
            .any(|(base, _)| base.canonicalize().as_ref().unwrap_or(base) == &project)
    {
        layers.push((project, Vec::new()));
    }
    for (layer_index, (base, entries)) in layers.into_iter().enumerate().rev() {
        let base = base.canonicalize().unwrap_or(base);
        let entries: Vec<_> = entries
            .into_iter()
            .filter(|entry| {
                kind != Kind::Extension
                    || !named_extension(entry.trim_start_matches(['+', '-', '!']))
            })
            .collect();
        packages::validate_patterns(&entries)?;
        let patterns: Vec<_> = entries
            .iter()
            .filter(|entry| entry.starts_with(['+', '-', '!']))
            .cloned()
            .collect();
        let mut configured = Vec::new();
        for entry in entries
            .iter()
            .filter(|entry| !entry.starts_with(['+', '-', '!']))
        {
            let path = absolute(&base, entry);
            let found = expand(&path, kind, false, warnings)?;
            if found.is_empty() && !path.exists() {
                warnings.push(format!("Resource {} was not found", path.display()));
            }
            configured.extend(found);
        }
        if !patterns.is_empty() {
            configured.retain(|path| packages::selected(path, &base, &patterns));
        }
        append(&mut paths, configured);
        let mut roots = vec![(base.join(kind.key()), base.clone())];
        if kind == Kind::Skill {
            if layer_index == 0 {
                if let Some(home) =
                    std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))
                {
                    let agents = PathBuf::from(home).join(".agents");
                    roots.push((agents.join("skills"), agents));
                }
            } else if args.trusted {
                let user_agents = std::env::var_os("HOME")
                    .or_else(|| std::env::var_os("USERPROFILE"))
                    .map(|home| PathBuf::from(home).join(".agents"));
                for directory in workspace.ancestors() {
                    let agents = directory.join(".agents");
                    if user_agents.as_ref() != Some(&agents) {
                        roots.push((agents.join("skills"), agents));
                    }
                    if directory.join(".git").exists() {
                        break;
                    }
                }
            }
        }
        let mut overrides = vec!["**".to_owned()];
        overrides.extend(
            entries
                .iter()
                .filter(|entry| entry.starts_with(['+', '-', '!']))
                .cloned(),
        );
        for (root, scope) in roots {
            let mut discovered = if kind == Kind::Prompt {
                auto_prompts(&root, warnings)?
            } else {
                collect(&root, kind, false, warnings)?
            };
            discovered.retain(|path| packages::selected(path, &scope, &overrides));
            append(&mut paths, discovered);
        }
    }
    append(
        &mut paths,
        packaged
            .iter()
            .filter(|path| path.is_file())
            .cloned()
            .collect(),
    );
    Ok(paths)
}
// Pi's automatic prompt discovery scans only visible files in the prompt root,
// applying ignore files from that directory. Configured paths use collect instead.
fn auto_prompts(root: &Path, warnings: &mut Vec<String>) -> Result<Vec<PathBuf>> {
    if !root.is_dir() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in ignore::WalkBuilder::new(root)
        .max_depth(Some(1))
        .hidden(true)
        .parents(false)
        .git_global(false)
        .git_exclude(false)
        .require_git(false)
        .add_custom_ignore_filename(".fdignore")
        .follow_links(true)
        .build()
    {
        match entry {
            Ok(entry)
                if entry.depth() == 1
                    && entry.path().is_file()
                    && Kind::Prompt.accepts(entry.path()) =>
            {
                files.push(entry.into_path());
            }
            Err(error) => warnings.push(error.to_string()),
            _ => {}
        }
    }
    files.sort();
    files.dedup();
    Ok(files)
}
fn append(paths: &mut Vec<PathBuf>, additions: Vec<PathBuf>) {
    for path in additions {
        let key = path.canonicalize().unwrap_or_else(|_| path.clone());
        if !paths
            .iter()
            .any(|existing| existing.canonicalize().unwrap_or_else(|_| existing.clone()) == key)
        {
            paths.push(path);
        }
    }
}
fn absolute(base: &Path, value: &str) -> PathBuf {
    if let Some(relative) = value.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))
    {
        return PathBuf::from(home).join(relative);
    }
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}
fn expand(
    path: &Path,
    kind: Kind,
    required: bool,
    warnings: &mut Vec<String>,
) -> Result<Vec<PathBuf>> {
    if path.exists() || !has_pattern(&path.to_string_lossy()) {
        return collect(path, kind, required, warnings);
    }
    let pattern = path.to_string_lossy().replace('\\', "/");
    let matcher = globset::GlobBuilder::new(&pattern)
        .literal_separator(true)
        .backslash_escape(false)
        .build()
        .map_err(|error| Error::Config(format!("invalid resource pattern {pattern:?}: {error}")))?
        .compile_matcher();
    let mut root = PathBuf::new();
    for component in path.components() {
        if has_pattern(&component.as_os_str().to_string_lossy()) {
            break;
        }
        root.push(component);
    }
    let found = collect_matching(&root, kind, false, warnings, Some(&matcher))?;
    if required && found.is_empty() {
        return Err(Error::Config(format!(
            "Resource pattern {pattern:?} matched no files"
        )));
    }
    Ok(found)
}
fn collect(
    path: &Path,
    kind: Kind,
    required: bool,
    warnings: &mut Vec<String>,
) -> Result<Vec<PathBuf>> {
    collect_matching(path, kind, required, warnings, None)
}
fn collect_matching(
    path: &Path,
    kind: Kind,
    required: bool,
    warnings: &mut Vec<String>,
    matcher: Option<&globset::GlobMatcher>,
) -> Result<Vec<PathBuf>> {
    if path.is_file() {
        return Ok(vec![if kind == Kind::Prompt {
            path.to_owned()
        } else {
            path.canonicalize()?
        }]);
    }
    if !path.is_dir() {
        return if required {
            Err(Error::Config(format!(
                "Resource {} was not found",
                path.display()
            )))
        } else {
            Ok(Vec::new())
        };
    }
    if kind == Kind::Skill {
        let mut files: Vec<_> = skill_files(path)
            .into_iter()
            .filter(|file| matches_pattern(file, path, matcher))
            .map(|file| file.canonicalize())
            .collect::<std::io::Result<_>>()?;
        files.sort();
        files.dedup();
        return Ok(files);
    }
    let mut files = Vec::new();
    let mut walker = ignore::WalkBuilder::new(path);
    walker
        .hidden(kind == Kind::Prompt)
        .git_ignore(kind == Kind::Prompt);
    if kind == Kind::Prompt {
        walker
            .parents(false)
            .git_global(false)
            .git_exclude(false)
            .require_git(false)
            .add_custom_ignore_filename(".fdignore");
    }
    for entry in walker
        .follow_links(true)
        .filter_entry(move |entry| {
            entry.depth() == 0
                || if kind == Kind::Prompt {
                    entry.file_name() != "node_modules"
                } else {
                    !matches!(
                        entry.file_name().to_str(),
                        Some(".git" | "target" | "node_modules")
                    )
                }
        })
        .build()
    {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                warnings.push(error.to_string());
                continue;
            }
        };
        if entry.file_type().is_some_and(|kind| kind.is_file())
            && kind.accepts(entry.path())
            && matches_pattern(entry.path(), path, matcher)
        {
            files.push(if kind == Kind::Prompt {
                entry.into_path()
            } else {
                entry.path().canonicalize()?
            });
        }
    }
    files.sort();
    files.dedup();
    Ok(files)
}

fn matches_pattern(path: &Path, root: &Path, matcher: Option<&globset::GlobMatcher>) -> bool {
    matcher.is_none_or(|matcher| {
        path.ancestors()
            .take_while(|ancestor| ancestor.starts_with(root))
            .any(|candidate| matcher.is_match(candidate.to_string_lossy().replace('\\', "/")))
    })
}

pub(super) fn skill_files(root: &Path) -> Vec<PathBuf> {
    if root.is_file() {
        return vec![root.to_owned()];
    }
    let mut files = Vec::new();
    let mut declared_roots = std::collections::BTreeSet::new();
    for entry in ignore::WalkBuilder::new(root)
        .hidden(true)
        .parents(false)
        .git_global(false)
        .git_exclude(false)
        .require_git(false)
        .add_custom_ignore_filename(".fdignore")
        .follow_links(true)
        .filter_entry(|entry| entry.file_name() != "node_modules")
        .build()
        .flatten()
    {
        if !entry.file_type().is_some_and(|kind| kind.is_file()) {
            continue;
        }
        if entry.file_name() == "SKILL.md" {
            let path = entry.into_path();
            declared_roots.insert(path.parent().expect("skill parent directory").to_owned());
            files.push(path);
        } else if entry.depth() == 1 && entry.path().extension().is_some_and(|ext| ext == "md") {
            files.push(entry.into_path());
        }
    }
    files.retain(|path| {
        let mut ancestors = path.ancestors().skip(1);
        if path.file_name().is_some_and(|name| name == "SKILL.md") {
            ancestors.next();
        }
        !ancestors.any(|directory| declared_roots.contains(directory))
    });
    files.sort();
    files
}

#[cfg(test)]
mod glob_tests {
    use super::*;
    #[test]
    fn resource_globs_expand_files_directories_and_literal_metacharacters() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("a/nested")).unwrap();
        std::fs::create_dir_all(root.path().join("b")).unwrap();
        for path in ["a/one.md", "a/nested/deep.md", "b/two.md", "[literal].md"] {
            std::fs::write(root.path().join(path), "text").unwrap();
        }
        let mut warnings = Vec::new();
        let files = expand(
            &root.path().join("{a,b}/*.md"),
            Kind::Prompt,
            true,
            &mut warnings,
        )
        .unwrap();
        assert_eq!(files.len(), 2);
        let files = expand(&root.path().join("a/*"), Kind::Prompt, true, &mut warnings).unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|path| path.ends_with("deep.md")));
        assert_eq!(
            expand(
                &root.path().join("[literal].md"),
                Kind::Prompt,
                true,
                &mut warnings
            )
            .unwrap()
            .len(),
            1
        );
        assert!(
            expand(
                &root.path().join("missing/*.md"),
                Kind::Prompt,
                true,
                &mut warnings
            )
            .is_err()
        );
    }
}
