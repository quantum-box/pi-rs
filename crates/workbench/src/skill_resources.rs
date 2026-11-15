//! Explicit, host-controlled skill metadata snapshots for native SDK hosts.
use pi_rs::{Error, Result};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};

const MAX_SKILL_FILES: usize = 128;
const MAX_TOTAL_BYTES: usize = 4 * 1024 * 1024;

/// Exact skill files that the host currently permits this load to read.
/// Relative paths require an explicit absolute base via `load_with_base`. Symlinks are followed, so this grant is not a filesystem sandbox. No
/// directory scanning, global discovery, packages or settings are accessed.
#[derive(Clone, Debug, Default)]
pub struct SkillReadGrants {
    pub files: Vec<PathBuf>,
}

/// Loaded metadata. Reading the file later is a separate host authorization;
/// snapshots do not grant access to its contents or update after a reload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillResource {
    pub name: String,
    pub description: String,
    pub path: PathBuf,
    pub disable_model_invocation: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillDiagnostic {
    pub path: PathBuf,
    pub message: String,
}

/// Independent snapshot; no existing session is updated when it is reloaded.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SkillResources {
    pub skills: Vec<SkillResource>,
    pub diagnostics: Vec<SkillDiagnostic>,
}

/// Selects the file-reading hint used by Pi's skill prompt section.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkillReader {
    Read,
    Bash,
    Indirect,
}

impl SkillReadGrants {
    /// Load explicitly granted absolute file paths; relative paths require
    /// `load_with_base`. Reads are synchronous and should run off a GUI thread.
    pub fn load(&self) -> Result<SkillResources> {
        self.load_inner(None)
    }

    /// Resolve relative grants against an explicitly supplied absolute base.
    /// Parent components are rejected rather than normalized across symlinks.
    /// Reads follow symlinks and are not a sandbox. Returned paths are absolute
    /// UTF-8 paths which can be reauthorized by the host.
    pub fn load_with_base(&self, base: &Path) -> Result<SkillResources> {
        if !base.is_absolute() {
            return Err(Error::Config("skill base must be absolute".into()));
        }
        self.load_inner(Some(base))
    }

    fn load_inner(&self, base: Option<&Path>) -> Result<SkillResources> {
        if self.files.len() > MAX_SKILL_FILES {
            return Err(Error::Config("too many skill files".into()));
        }
        let mut result = SkillResources::default();
        let mut names = HashMap::<String, PathBuf>::new();
        let mut total = 0usize;
        for grant in &self.files {
            let path = resolve_grant(grant, base)?;
            if !path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".md"))
            {
                return Err(Error::Config(format!(
                    "skill must have a UTF-8 .md filename: {}",
                    path.display()
                )));
            }
            let text = super::prompt_resources::read_optional(&path)
                .map_err(|error| {
                    Error::Config(format!("cannot read skill {}: {error}", path.display()))
                })?
                .ok_or_else(|| Error::Config(format!("skill is missing: {}", path.display())))?;
            total = total
                .checked_add(text.len())
                .ok_or_else(|| Error::Config("skills exceed size limit".into()))?;
            if total > MAX_TOTAL_BYTES {
                return Err(Error::Config("skills exceed size limit".into()));
            }
            let value = frontmatter(&text)
                .map_err(|error| Error::Config(format!("{}: {error}", path.display())))?;
            let declared = path.file_name().is_some_and(|name| name == "SKILL.md");
            let description = value
                .get("description")
                .and_then(|item| item.as_str())
                .filter(|text| !text.trim_matches(is_js_space).is_empty());
            if !declared && description.is_none() {
                continue;
            }
            let name = value
                .get("name")
                .and_then(|item| item.as_str())
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| {
                    path.parent()
                        .and_then(Path::file_name)
                        .and_then(|part| part.to_str())
                        .unwrap_or("")
                });
            let mut warning = |message: String| {
                result.diagnostics.push(SkillDiagnostic {
                    path: path.clone(),
                    message,
                })
            };
            match description {
                None => warning("description is required".into()),
                Some(text) if text.encode_utf16().count() > 1024 => warning(format!(
                    "description exceeds 1024 characters ({})",
                    text.encode_utf16().count()
                )),
                _ => {}
            }
            let name_len = name.encode_utf16().count();
            if name_len > 64 {
                warning(format!("name exceeds 64 characters ({name_len})"));
            }
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            {
                warning(
                    "name contains invalid characters (must be lowercase a-z, 0-9, hyphens only)"
                        .into(),
                );
            }
            if name.starts_with('-') || name.ends_with('-') {
                warning("name must not start or end with a hyphen".into());
            }
            if name.contains("--") {
                warning("name must not contain consecutive hyphens".into());
            }
            let Some(description) = description else {
                continue;
            };
            if let Some(previous) = names.get(name) {
                return Err(Error::Config(format!(
                    "duplicate skill {name} at {} and {}",
                    previous.display(),
                    path.display()
                )));
            }
            names.insert(name.to_owned(), path.clone());
            result.skills.push(SkillResource {
                name: name.to_owned(),
                description: description.to_owned(),
                path,
                disable_model_invocation: value
                    .get("disable-model-invocation")
                    .and_then(|v| v.as_bool())
                    == Some(true),
            });
        }
        Ok(result)
    }
}

fn resolve_grant(path: &Path, base: Option<&Path>) -> Result<PathBuf> {
    if !path.is_absolute()
        && (path.has_root() || matches!(path.components().next(), Some(Component::Prefix(_))))
    {
        return Err(Error::Config(
            "skill path must be fully absolute or relative to the supplied base".into(),
        ));
    }
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        base.ok_or_else(|| Error::Config("relative skill path requires an absolute base".into()))?
            .join(path)
    };
    if !absolute.is_absolute() {
        return Err(Error::Config("skill path must be absolute".into()));
    }
    if absolute
        .components()
        .any(|component| component == Component::ParentDir)
    {
        return Err(Error::Config(
            "skill path must not contain parent components".into(),
        ));
    }
    if absolute.to_str().is_none() {
        return Err(Error::Config("skill path must be UTF-8".into()));
    }
    let mut result = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => unreachable!("parent components rejected"),
            _ => result.push(component.as_os_str()),
        }
    }
    Ok(result)
}

impl SkillResources {
    /// Format the visible skills using Pi's prompt section. This formats paths
    /// for display only; it neither reads files nor changes session state.
    pub fn render(&self, reader: SkillReader) -> Result<String> {
        let visible: Vec<_> = self
            .skills
            .iter()
            .filter(|skill| !skill.disable_model_invocation)
            .collect();
        if visible.is_empty() {
            return Ok(String::new());
        }
        let hint = match reader {
            SkillReader::Read => {
                "Use the read tool to load a skill's file when the task matches its description."
            }
            SkillReader::Bash => {
                "Use bash to load a skill's file when the task matches its description."
            }
            SkillReader::Indirect => "Load a skill's file when the task matches its description.",
        };
        let mut output = format!(
            "\n\nThe following skills provide specialized instructions for specific tasks.\n{hint}\nResolve relative paths against the skill's directory (parent of SKILL.md). Use absolute paths in tool calls.\n\n<available_skills>"
        );
        for skill in visible {
            let path = resolve_grant(&skill.path, None)?;
            let path = path
                .to_str()
                .ok_or_else(|| Error::Config("skill path must be UTF-8".into()))?;
            output.push_str(&format!("\n  <skill>\n    <name>{}</name>\n    <description>{}</description>\n    <location>{}</location>\n  </skill>", escape_xml(&skill.name), escape_xml(&skill.description), escape_xml(path)));
        }
        output.push_str("\n</available_skills>");
        Ok(output)
    }
}

fn frontmatter(text: &str) -> Result<serde_yaml::Value> {
    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    let Some(end) = normalized
        .strip_prefix("---")
        .and_then(|rest| rest.find("\n---"))
        .map(|index| index + 3)
    else {
        return Ok(serde_yaml::Value::Null);
    };
    serde_yaml::from_str(normalized.get(4..=end).unwrap_or_default())
        .map_err(|error| Error::Config(format!("invalid skill frontmatter: {error}")))
}

fn is_js_space(ch: char) -> bool {
    (ch.is_whitespace() && ch != '\u{0085}') || ch == '\u{feff}'
}
fn escape_xml(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
