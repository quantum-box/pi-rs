//! Host-controlled loading of Pi's system-prompt resources.
//!
//! Construction does no I/O. Each `load` is a fresh snapshot and requires the
//! caller to supply its current read grants. No session is modified implicitly.
use std::io::Read;
use std::path::{Path, PathBuf};

use pi_rs::{Error, Result, SessionBuilder};

const MAX_PROMPT_BYTES: u64 = 256 * 1024;

/// Directories the host currently permits this load to read. `project` is the
/// working directory (its `.pi` child is used); `global` is the agent directory.
/// A grant authorizes reading the corresponding prompt files, including through
/// filesystem symlinks; it is not a filesystem sandbox.
#[derive(Clone, Debug, Default)]
pub struct PromptReadGrants {
    pub project: Option<PathBuf>,
    pub global: Option<PathBuf>,
}

/// Explicit values replace discovery independently for each prompt category.
/// `Some(vec![])` deliberately suppresses discovery of an appended prompt.
#[derive(Clone, Debug, Default)]
pub struct PromptResourceFactory {
    pub system: Option<String>,
    pub append: Option<Vec<String>>,
}

/// A resolved prompt and, for discovered files, the path used to read it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromptResource {
    pub text: String,
    pub source: Option<PathBuf>,
}

/// Independent snapshot: reloading does not affect an existing session or snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PromptResources {
    pub system: Option<PromptResource>,
    pub append: Vec<PromptResource>,
}

impl PromptResourceFactory {
    /// Read only granted scopes. Project files win over global files. A read or
    /// UTF-8 error fails the whole load; callers can retain their previous snapshot.
    pub fn load(&self, grants: &PromptReadGrants) -> Result<PromptResources> {
        let system = match &self.system {
            Some(text) => Some(literal(text)),
            None => discover(grants, "SYSTEM.md")?,
        };
        let append = match &self.append {
            Some(values) => values.iter().map(literal).collect(),
            None => discover(grants, "APPEND_SYSTEM.md")?.into_iter().collect(),
        };
        Ok(PromptResources { system, append })
    }
}

impl PromptResources {
    /// Compose a caller-selected default with the resolved override and appendices.
    /// The caller owns the default prompt and any additional context formatting.
    pub fn compose(&self, default: &str) -> String {
        let mut result = self.system.as_ref().map_or(default, |p| &p.text).to_owned();
        for prompt in &self.append {
            if !prompt.text.is_empty() {
                result.push_str("\n\n");
                result.push_str(&prompt.text);
            }
        }
        result
    }

    /// Apply this snapshot when creating a session; later loads do not mutate it.
    pub fn apply(&self, builder: SessionBuilder, default: &str) -> SessionBuilder {
        builder.system_prompt(self.compose(default))
    }
}

fn literal(text: &String) -> PromptResource {
    PromptResource {
        text: text.clone(),
        source: None,
    }
}

fn discover(grants: &PromptReadGrants, filename: &str) -> Result<Option<PromptResource>> {
    let paths = grants
        .project
        .as_ref()
        .map(|p| p.join(".pi").join(filename))
        .into_iter()
        .chain(grants.global.as_ref().map(|p| p.join(filename)));
    for path in paths {
        if let Some(text) = read_optional(&path)? {
            return Ok(Some(PromptResource {
                text,
                source: Some(path),
            }));
        }
    }
    Ok(None)
}

fn read_optional(path: &Path) -> Result<Option<String>> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if !file.metadata()?.is_file() {
        return Err(Error::Config(format!(
            "prompt resource is not a regular file: {}",
            path.display()
        )));
    }
    let mut bytes = Vec::new();
    file.take(MAX_PROMPT_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_PROMPT_BYTES {
        return Err(Error::Config(format!(
            "prompt resource exceeds size limit: {}",
            path.display()
        )));
    }
    let text = String::from_utf8(bytes)
        .map_err(|_| Error::Config(format!("prompt resource is not UTF-8: {}", path.display())))?;
    Ok(Some(
        text.strip_prefix('\u{feff}').unwrap_or(&text).to_owned(),
    ))
}
