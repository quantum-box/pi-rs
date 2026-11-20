//! Host-granted snapshots of file-based Pi themes.
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;

use crossterm::style::Color;
use pi_rs::{Error, Result};
use serde_json::Value;

const MAX_FILES: usize = 64;
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
            if !path.is_absolute()
                || path
                    .components()
                    .any(|component| matches!(component, std::path::Component::ParentDir))
            {
                return Err(Error::Config(
                    "theme file path must be absolute without parent components".into(),
                ));
            }
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
