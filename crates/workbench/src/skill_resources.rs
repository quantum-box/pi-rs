//! Explicit, host-controlled skill metadata snapshots for native SDK hosts.
use pi_rs::{Error, Result};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};

const MAX_SKILL_FILES: usize = 128;
const MAX_SKILL_DIRECTORY_ENTRIES: usize = 4096;
const MAX_SKILL_DIRECTORY_DEPTH: usize = 64;
const MAX_SKILL_IGNORE_LINES: usize = 16_384;
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

    /// Recursively load explicitly granted absolute skill directories. A
    /// directory containing an unignored `SKILL.md` is a skill root and its
    /// descendants are not returned. Only root-level legacy `.md` files are
    /// included. Hidden entries, `node_modules`, and files ignored by local
    /// `.gitignore`, `.ignore`, or `.fdignore` are skipped. Ignore files are
    /// read with file and aggregate limits; malformed rules fail the snapshot.
    /// A directory grant authorizes enumeration and reading ignore files in its
    /// subtree, including through symlinks to locations outside the directory.
    /// Symlinks are followed, and grants are not a filesystem sandbox.
    pub fn load_with_directories(&self, directories: &[PathBuf]) -> Result<SkillResources> {
        self.load_directories_inner(directories, None)
    }

    /// Resolve relative file and directory grants against an explicit absolute
    /// base before scanning. Parent components remain forbidden.
    pub fn load_with_directories_and_base(
        &self,
        directories: &[PathBuf],
        base: &Path,
    ) -> Result<SkillResources> {
        if !base.is_absolute() {
            return Err(Error::Config("skill base must be absolute".into()));
        }
        self.load_directories_inner(directories, Some(base))
    }

    fn load_directories_inner(
        &self,
        directories: &[PathBuf],
        base: Option<&Path>,
    ) -> Result<SkillResources> {
        if directories.len() > MAX_SKILL_FILES || self.files.len() > MAX_SKILL_FILES {
            return Err(Error::Config("too many skill paths".into()));
        }
        let mut files = self.files.clone();
        let mut budget = ScanBudget::default();
        for directory in directories {
            let root = resolve_grant(directory, base)?;
            if !std::fs::metadata(&root)?.is_dir() {
                return Err(Error::Config(format!(
                    "skill directory is not a directory: {}",
                    root.display()
                )));
            }
            let mut candidates = scan_skill_directory(&root, &mut budget)?;
            candidates.sort();
            if files.len().saturating_add(candidates.len()) > MAX_SKILL_FILES {
                return Err(Error::Config("too many skill files".into()));
            }
            files.extend(candidates);
        }
        SkillReadGrants { files }.load_inner(base)
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

#[derive(Default)]
struct ScanBudget {
    entries: usize,
    ignore_bytes: usize,
    ignore_lines: usize,
}

struct IgnoreChain {
    matcher: ignore::gitignore::Gitignore,
    parent: Option<std::sync::Arc<IgnoreChain>>,
}

fn ignored(path: &Path, is_dir: bool, chain: &IgnoreChain) -> bool {
    let mut current = Some(chain);
    while let Some(node) = current {
        match node.matcher.matched(path, is_dir) {
            ignore::Match::Ignore(_) => return true,
            ignore::Match::Whitelist(_) => return false,
            ignore::Match::None => current = node.parent.as_deref(),
        }
    }
    false
}

fn file_kind(path: &Path) -> Result<Option<std::fs::Metadata>> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(Error::Config(format!(
            "cannot inspect {}: {error}",
            path.display()
        ))),
    }
}

fn scan_skill_directory(root: &Path, budget: &mut ScanBudget) -> Result<Vec<PathBuf>> {
    use std::sync::Arc;
    let mut pending: Vec<(PathBuf, Option<Arc<IgnoreChain>>, usize)> =
        vec![(root.to_owned(), None, 0)];
    let mut candidates = Vec::new();
    while let Some((directory, inherited, depth)) = pending.pop() {
        let mut builder = ignore::gitignore::GitignoreBuilder::new(&directory);
        for filename in [".gitignore", ".ignore", ".fdignore"] {
            let path = directory.join(filename);
            let text = super::prompt_resources::read_optional(&path).map_err(|error| {
                Error::Config(format!(
                    "cannot read ignore file {}: {error}",
                    path.display()
                ))
            })?;
            if let Some(text) = text {
                budget.ignore_bytes = budget
                    .ignore_bytes
                    .checked_add(text.len())
                    .ok_or_else(|| Error::Config("skill ignore files exceed size limit".into()))?;
                if budget.ignore_bytes > MAX_TOTAL_BYTES {
                    return Err(Error::Config("skill ignore files exceed size limit".into()));
                }
                for line in text.lines() {
                    budget.ignore_lines += 1;
                    if budget.ignore_lines > MAX_SKILL_IGNORE_LINES {
                        return Err(Error::Config("too many skill ignore rules".into()));
                    }
                    if line.trim().is_empty() || line.trim_start().starts_with('#') {
                        continue;
                    }
                    // Pi prefixes nested ignore rules with their directory relative to
                    // the grant root, so a nested basename is anchored there.
                    let anchored;
                    let pattern = line.strip_prefix('!').unwrap_or(line);
                    let line = if depth > 0
                        && !pattern.starts_with('#')
                        && !pattern.trim_end_matches('/').contains('/')
                        && !pattern.is_empty()
                    {
                        anchored = if let Some(negated) = line.strip_prefix('!') {
                            format!("!/{negated}")
                        } else {
                            format!("/{line}")
                        };
                        anchored.as_str()
                    } else {
                        line
                    };
                    builder
                        .add_line(Some(path.clone()), line)
                        .map_err(|error| {
                            Error::Config(format!(
                                "invalid ignore file {}: {error}",
                                path.display()
                            ))
                        })?;
                }
            }
        }
        let matcher = builder.build().map_err(|error| {
            Error::Config(format!(
                "invalid ignore rules in {}: {error}",
                directory.display()
            ))
        })?;
        let chain = Arc::new(IgnoreChain {
            matcher,
            parent: inherited,
        });
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(&directory)? {
            budget.entries += 1;
            if budget.entries > MAX_SKILL_DIRECTORY_ENTRIES {
                return Err(Error::Config("too many skill directory entries".into()));
            }
            entries.push(entry?);
        }
        entries.sort_by_key(|entry| entry.file_name());
        if let Some(entry) = entries.iter().find(|entry| entry.file_name() == "SKILL.md") {
            let path = entry.path();
            if file_kind(&path)?.is_some_and(|metadata| metadata.is_file())
                && !ignored(&path, false, &chain)
            {
                candidates.push(path);
                if candidates.len() > MAX_SKILL_FILES {
                    return Err(Error::Config("too many skill files".into()));
                }
                continue;
            }
        }
        for entry in entries.into_iter().rev() {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with('.') || name == "node_modules" {
                continue;
            }
            let path = entry.path();
            let Some(metadata) = file_kind(&path)? else {
                continue;
            };
            let is_dir = metadata.is_dir();
            if ignored(&path, is_dir, &chain) {
                continue;
            }
            if is_dir {
                if depth >= MAX_SKILL_DIRECTORY_DEPTH {
                    return Err(Error::Config("skill directory depth limit exceeded".into()));
                }
                pending.push((path, Some(chain.clone()), depth + 1));
            } else if depth == 0
                && metadata.is_file()
                && name.to_str().is_some_and(|value| value.ends_with(".md"))
            {
                candidates.push(path);
                if candidates.len() > MAX_SKILL_FILES {
                    return Err(Error::Config("too many skill files".into()));
                }
            }
        }
    }
    Ok(candidates)
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
