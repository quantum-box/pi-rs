//! Host-controlled prompt template snapshots for native embedding.
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use pi_rs::{Error, Result};

const MAX_FILES: usize = 128;
const MAX_TOTAL_BYTES: usize = 4 * 1024 * 1024;

/// Explicitly granted template files. No directory scanning or implicit reads
/// occur. Symlinks are followed; grants are not a filesystem sandbox.
#[derive(Clone, Debug, Default)]
pub struct TemplateReadGrants {
    pub files: Vec<PathBuf>,
}

/// A template snapshot independent of future reads and permission changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromptTemplate {
    pub name: String,
    pub description: String,
    pub argument_hint: Option<String>,
    pub body: String,
    pub path: PathBuf,
}

impl TemplateReadGrants {
    /// Read each granted file in order. Any error rejects the whole snapshot.
    /// Reads are synchronous and must be kept off a UI thread.
    pub fn load(&self) -> Result<Vec<PromptTemplate>> {
        if self.files.len() > MAX_FILES {
            return Err(Error::Config("too many prompt templates".into()));
        }
        let mut seen = HashSet::new();
        let mut total = 0usize;
        let mut templates = Vec::with_capacity(self.files.len());
        for path in &self.files {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix(".md"))
                .filter(|name| !name.is_empty())
                .ok_or_else(|| {
                    Error::Config("prompt template must have a UTF-8 .md filename".into())
                })?;
            if !seen.insert(name.to_owned()) {
                return Err(Error::Config(format!(
                    "duplicate prompt template name: {name}"
                )));
            }
            let text = super::prompt_resources::read_optional(path)?.ok_or_else(|| {
                Error::Config(format!("prompt template is missing: {}", path.display()))
            })?;
            total = total
                .checked_add(text.len())
                .ok_or_else(|| Error::Config("prompt templates exceed size limit".into()))?;
            if total > MAX_TOTAL_BYTES {
                return Err(Error::Config("prompt templates exceed size limit".into()));
            }
            templates.push(parse_template(path, name, &text)?);
        }
        Ok(templates)
    }
}

fn parse_template(path: &Path, name: &str, text: &str) -> Result<PromptTemplate> {
    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    let mut description = None;
    let mut argument_hint = None;
    let body = if let Some(end) = normalized
        .strip_prefix("---")
        .and_then(|rest| rest.find("\n---"))
        .map(|index| index + 3)
    {
        let yaml = normalized.get(4..=end).unwrap_or_default();
        let value: serde_yaml::Value = serde_yaml::from_str(yaml)
            .map_err(|error| Error::Config(format!("invalid template frontmatter: {error}")))?;
        description = value
            .get("description")
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .map(str::to_owned);
        argument_hint = value
            .get("argument-hint")
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .map(str::to_owned);
        normalized[end + 4..].trim_matches(is_js_space).to_owned()
    } else {
        normalized
    };
    let description = description.unwrap_or_else(|| {
        let first = body
            .lines()
            .find(|line| !line.trim_matches(is_js_space).is_empty())
            .unwrap_or_default();
        let mut units = first.encode_utf16();
        let prefix: Vec<_> = units.by_ref().take(60).collect();
        let mut text = String::from_utf16_lossy(&prefix);
        if units.next().is_some() {
            text.push_str("...");
        }
        text
    });
    Ok(PromptTemplate {
        name: name.to_owned(),
        description,
        argument_hint,
        body,
        path: path.to_owned(),
    })
}

fn is_js_space(ch: char) -> bool {
    (ch.is_whitespace() && ch != '\u{0085}') || ch == '\u{feff}'
}
