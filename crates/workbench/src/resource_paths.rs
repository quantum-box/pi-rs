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
        append(
            &mut paths,
            collect(
                &absolute(&workspace, &path.to_string_lossy()),
                kind,
                true,
                warnings,
            )?,
        );
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
            .filter(|entry| entry.starts_with(['+', '-', '!']) || entry.contains(['*', '?']))
            .cloned()
            .collect();
        let mut configured = Vec::new();
        for entry in entries
            .iter()
            .filter(|entry| !entry.starts_with(['+', '-', '!']) && !entry.contains(['*', '?']))
        {
            let path = absolute(&base, entry);
            if !path.exists() {
                warnings.push(format!("Resource {} was not found", path.display()));
                continue;
            }
            append(&mut configured, collect(&path, kind, false, warnings)?);
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
            let mut discovered = collect(&root, kind, false, warnings)?;
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
fn append(paths: &mut Vec<PathBuf>, additions: Vec<PathBuf>) {
    for path in additions {
        if !paths.contains(&path) {
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
fn collect(
    path: &Path,
    kind: Kind,
    required: bool,
    warnings: &mut Vec<String>,
) -> Result<Vec<PathBuf>> {
    if path.is_file() {
        return Ok(vec![path.canonicalize()?]);
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
    let mut files = Vec::new();
    for entry in ignore::WalkBuilder::new(path)
        .hidden(false)
        .git_ignore(false)
        .follow_links(true)
        .filter_entry(|entry| {
            entry.depth() == 0
                || !matches!(
                    entry.file_name().to_str(),
                    Some(".git" | "target" | "node_modules")
                )
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
        if entry.file_type().is_some_and(|kind| kind.is_file()) && kind.accepts(entry.path()) {
            files.push(entry.path().canonicalize()?);
        }
    }
    files.sort();
    files.dedup();
    Ok(files)
}
