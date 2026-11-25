//! Host-granted snapshots of file-based Pi themes.
use std::collections::{BTreeMap, HashSet};
use std::path::{Component, Path, PathBuf};

use crossterm::style::Color;
use pi_rs::{Error, Result, UiThemeInfo};
use serde_json::Value;

const MAX_FILES: usize = 64;
const MAX_DIRECTORY_ENTRIES: usize = 4096;
const MAX_TOTAL_BYTES: usize = 4 * 1024 * 1024;
const MAX_VARIABLE_DEPTH: usize = 128;

/// Exact absolute files the host authorizes this load to read. Paths cannot
/// contain parent components. Symlinks are followed;
/// a grant also authorizes the target and is not a filesystem sandbox.
#[derive(Clone, Debug, Default)]
pub struct ThemeReadGrants {
    pub files: Vec<PathBuf>,
}

/// An ordered, independent snapshot. Loading does not select or apply a theme.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThemeSnapshot {
    themes: Vec<ThemeResource>,
}

/// A resolved file theme. Colors are terminal-independent RGB, ANSI index, or reset values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThemeResource {
    name: String,
    path: PathBuf,
    colors: BTreeMap<String, Color>,
}

impl ThemeSnapshot {
    pub fn themes(&self) -> &[ThemeResource] {
        &self.themes
    }
}

impl ThemeResource {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
    pub fn colors(&self) -> &BTreeMap<String, Color> {
        &self.colors
    }
}

impl ThemeReadGrants {
    /// Read exact file grants first, then direct `.json` files in each granted
    /// directory. Directory order follows the caller's list and entries are
    /// sorted by path. Hidden files and symlinks to regular files are included.
    /// Directory grants must be absolute without parent components and include
    /// symlink targets. Reads are synchronous and any failure rejects the snapshot.
    pub fn load_with_directories(&self, directories: &[PathBuf]) -> Result<ThemeSnapshot> {
        if directories.is_empty() {
            return self.load();
        }
        if directories.len() > MAX_FILES || self.files.len() > MAX_FILES {
            return Err(Error::Config("too many theme paths".into()));
        }
        for directory in directories {
            validate_path(directory, "directory")?;
        }
        for file in &self.files {
            validate_path(file, "file")?;
        }

        let mut files = self.files.clone();
        let mut examined = 0;
        for directory in directories {
            let mut entries = Vec::new();
            for entry in std::fs::read_dir(directory)? {
                examined += 1;
                if examined > MAX_DIRECTORY_ENTRIES {
                    return Err(Error::Config("too many theme directory entries".into()));
                }
                let entry = entry?;
                if !entry.file_name().as_encoded_bytes().ends_with(b".json") {
                    continue;
                }
                let kind = entry.file_type()?;
                let path = entry.path();
                let is_file = if kind.is_symlink() {
                    match std::fs::metadata(&path) {
                        Ok(metadata) => metadata.is_file(),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                        Err(error) => return Err(error.into()),
                    }
                } else {
                    kind.is_file()
                };
                if is_file {
                    if files.len() + entries.len() >= MAX_FILES {
                        return Err(Error::Config("too many theme files".into()));
                    }
                    entries.push(path);
                }
            }
            entries.sort();
            files.extend(entries);
        }
        Self { files }.load()
    }

    /// Read only these files, in order. Any failure rejects the entire snapshot.
    /// Reads are synchronous and should run off a UI thread.
    pub fn load(&self) -> Result<ThemeSnapshot> {
        if self.files.len() > MAX_FILES {
            return Err(Error::Config("too many theme files".into()));
        }
        let mut total = 0usize;
        let mut names = HashSet::new();
        let mut themes = Vec::with_capacity(self.files.len());
        for path in &self.files {
            validate_path(path, "file")?;
            let text = crate::prompt_resources::read_optional(path)?.ok_or_else(|| {
                Error::Config(format!("theme file is missing: {}", path.display()))
            })?;
            total = total
                .checked_add(text.len())
                .ok_or_else(|| Error::Config("theme files exceed size limit".into()))?;
            if total > MAX_TOTAL_BYTES {
                return Err(Error::Config("theme files exceed size limit".into()));
            }
            validate_variable_depth(&text)?;
            let parsed = crate::theme_parser::parse_theme(&text)?;
            if !names.insert(parsed.name.clone()) {
                return Err(Error::Config(format!(
                    "duplicate theme name: {}",
                    parsed.name
                )));
            }
            themes.push(ThemeResource {
                name: parsed.name,
                path: path.clone(),
                colors: parsed.colors,
            });
        }
        Ok(ThemeSnapshot { themes })
    }
}

/// Resolve a presentation catalog from the worker's already resolved theme files.
#[doc(hidden)]
pub fn extension_theme_catalog(paths: &[PathBuf]) -> Result<Vec<UiThemeInfo>> {
    if paths.len() > MAX_DIRECTORY_ENTRIES {
        return Err(Error::Config("too many resolved theme files".into()));
    }
    let mut bytes = 0usize;
    let mut seen = HashSet::from(["system".to_owned(), "dark".to_owned(), "light".to_owned()]);
    let mut themes = vec![
        UiThemeInfo {
            name: "dark".into(),
            path: None,
        },
        UiThemeInfo {
            name: "light".into(),
            path: None,
        },
    ];
    for path in paths {
        let Ok(Some(text)) = crate::prompt_resources::read_optional_resource(path, "theme file")
        else {
            continue;
        };
        bytes += text.len();
        if bytes > MAX_TOTAL_BYTES {
            return Err(Error::Config(
                "theme catalog exceeds its read size limit".into(),
            ));
        }
        if validate_variable_depth(&text).is_err() {
            continue;
        }
        let Ok(theme) = crate::theme_parser::parse_theme(&text) else {
            continue;
        };
        if seen.insert(theme.name.clone()) {
            themes.push(UiThemeInfo {
                name: theme.name,
                path: path.to_str().map(PathBuf::from),
            });
        }
    }
    let locale = crate::locale::select(|name| std::env::var(name).ok());
    let collator = crate::locale::collator(&locale).ok();
    themes.sort_by(|left, right| {
        collator.as_ref().map_or_else(
            || left.name.cmp(&right.name),
            |collator| collator.compare(&left.name, &right.name),
        )
    });
    themes.insert(
        0,
        UiThemeInfo {
            name: "system".into(),
            path: None,
        },
    );
    Ok(themes)
}

fn validate_path(path: &Path, kind: &str) -> Result<()> {
    if !path.is_absolute() || path.components().any(|part| part == Component::ParentDir) {
        return Err(Error::Config(format!(
            "theme {kind} path must be absolute without parent components"
        )));
    }
    Ok(())
}

// Bound variable recursion before calling the parser shared with the TUI.
fn validate_variable_depth(text: &str) -> Result<()> {
    let value: Value = serde_json::from_str(text)?;
    let Some(vars) = value.get("vars").and_then(Value::as_object) else {
        return Ok(());
    };
    for key in vars.keys() {
        let mut next = key.as_str();
        let mut seen = HashSet::new();
        loop {
            if !seen.insert(next) {
                break;
            } // The shared parser reports referenced cycles.
            if seen.len() > MAX_VARIABLE_DEPTH {
                return Err(Error::Config(
                    "theme variable chain exceeds depth limit".into(),
                ));
            }
            let Some(value) = vars.get(next).and_then(Value::as_str) else {
                break;
            };
            if value.is_empty()
                || value.starts_with('#')
                || value.to_ascii_lowercase().starts_with("okhsl(")
                || value.to_ascii_lowercase().starts_with("oklch(")
            {
                break;
            }
            next = value;
        }
    }
    Ok(())
}
