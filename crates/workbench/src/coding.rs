//! Local coding tools. Hosted callers opt into their own execution profile.
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use pi_rs::{Error, Extension, Outcome, Result, Tool, ToolContext, ToolExecution, ToolSpec};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt};

const MAX_FILE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_OUTPUT_BYTES: usize = 50 * 1024;

pub struct CodingTools {
    root: PathBuf,
    unrestricted: bool,
}

impl CodingTools {
    pub fn new(root: &Path) -> Result<Self> {
        let root = root.canonicalize()?;
        if !root.is_dir() {
            return Err(Error::Config("workspace root must be a directory".into()));
        }
        Ok(Self {
            root,
            unrestricted: false,
        })
    }
    /// Local Pi profile, with paths resolved from the workspace and access to user resources.
    pub fn local(root: &Path) -> Result<Self> {
        let mut tools = Self::new(root)?;
        tools.unrestricted = true;
        Ok(tools)
    }
}

impl Extension for CodingTools {
    fn id(&self) -> &str {
        "local-coding-tools"
    }

    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        [
            Kind::Read,
            Kind::Write,
            Kind::Edit,
            Kind::Bash,
            Kind::Grep,
            Kind::Find,
            Kind::List,
        ]
        .into_iter()
        .map(|kind| {
            Arc::new(CodingTool {
                root: self.root.clone(),
                unrestricted: self.unrestricted,
                kind,
            }) as Arc<dyn Tool>
        })
        .collect()
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Read,
    Write,
    Edit,
    Bash,
    Grep,
    Find,
    List,
}

struct CodingTool {
    root: PathBuf,
    unrestricted: bool,
    kind: Kind,
}

#[async_trait::async_trait]
impl Tool for CodingTool {
    fn spec(&self) -> ToolSpec {
        let path = json!({"type":"string","description":if self.unrestricted { "Absolute path or path relative to the workspace" } else { "Path relative to the workspace, or an absolute path inside it" }});
        let (name, description, properties, required) = match self.kind {
            Kind::Read => (
                "read",
                "Read UTF-8 text with optional one-based line offset and line limit.",
                json!({"path":path,"offset":{"type":"integer","minimum":1},"limit":{"type":"integer","minimum":1}}),
                vec!["path"],
            ),
            Kind::Write => (
                "write",
                "Create or overwrite a UTF-8 file, creating parent directories.",
                json!({"path":path,"content":{"type":"string"}}),
                vec!["path", "content"],
            ),
            Kind::Edit => (
                "edit",
                "Apply unique non-overlapping exact replacements, all matched against the original file.",
                json!({"path":path,"edits":{"type":"array","minItems":1,"items":{"type":"object","properties":{"oldText":{"type":"string","minLength":1},"newText":{"type":"string"}},"required":["oldText","newText"],"additionalProperties":false}}}),
                vec!["path", "edits"],
            ),
            Kind::Bash => (
                "bash",
                "Run a shell command in the workspace and return its bounded output and exit status.",
                json!({"command":{"type":"string"},"timeout":{"type":"integer","minimum":1,"maximum":120}}),
                vec!["command"],
            ),
            Kind::Grep => (
                "grep",
                "Search UTF-8 file contents. Respects ignore files and returns path, line number, and matching text.",
                json!({"pattern":{"type":"string"},"path":path,"glob":{"type":"string"},"ignoreCase":{"type":"boolean"},"literal":{"type":"boolean"},"limit":{"type":"integer","minimum":1,"maximum":1000}}),
                vec!["pattern"],
            ),
            Kind::Find => (
                "find",
                "Find workspace files matching a glob. Respects ignore files.",
                json!({"pattern":{"type":"string"},"path":path,"limit":{"type":"integer","minimum":1,"maximum":1000}}),
                vec!["pattern"],
            ),
            Kind::List => (
                "ls",
                "List the immediate entries in a workspace directory.",
                json!({"path":path,"limit":{"type":"integer","minimum":1,"maximum":1000}}),
                vec![],
            ),
        };
        ToolSpec {
            name: name.into(),
            description: description.into(),
            parameters: json!({"type":"object","properties":properties,"required":required,"additionalProperties":false}),
        }
    }

    fn execution(&self) -> ToolExecution {
        match self.kind {
            Kind::Read | Kind::Grep | Kind::Find | Kind::List => ToolExecution::ParallelRead,
            _ => ToolExecution::Sequential,
        }
    }

    async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
        let result = match self.kind {
            Kind::Read => self.read(&args),
            Kind::Write => self.write(&args),
            Kind::Edit => self.edit(&args),
            Kind::Grep | Kind::Find => self.search(&args),
            Kind::List => self.list(&args),
            Kind::Bash => self.bash(&args).await,
        };
        match result {
            Ok(value) => Outcome::Succeeded(value),
            Err(error) => Outcome::Failed(error.to_string()),
        }
    }
}

impl CodingTool {
    fn path(&self, value: &str) -> Result<PathBuf> {
        let input = Path::new(value);
        if !self.unrestricted
            && input
                .components()
                .any(|part| matches!(part, Component::ParentDir))
        {
            return Err(Error::Config("parent traversal is not supported".into()));
        }
        let path = if input.is_absolute() {
            input.to_path_buf()
        } else {
            self.root.join(input)
        };
        let resolved = path.canonicalize()?;
        if !self.unrestricted && !resolved.starts_with(&self.root) {
            return Err(Error::Config("path is outside the workspace".into()));
        }
        Ok(resolved)
    }

    fn text(&self, path: &Path) -> Result<String> {
        let metadata = path.metadata()?;
        if !metadata.is_file() || metadata.len() > MAX_FILE_BYTES {
            return Err(Error::Config(
                "expected a regular text file up to 4 MiB".into(),
            ));
        }
        Ok(std::fs::read_to_string(path)?)
    }

    fn read(&self, args: &Value) -> Result<String> {
        let path = self.path(string(args, "path")?)?;
        let content = self.text(&path)?;
        let offset = usize::try_from(args["offset"].as_u64().unwrap_or(1).saturating_sub(1))
            .unwrap_or(usize::MAX);
        let limit = usize::try_from(args["limit"].as_u64().unwrap_or(2000)).unwrap_or(usize::MAX);
        let selected = content
            .lines()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>()
            .join("\n");
        Ok(bounded(selected))
    }

    fn write(&self, args: &Value) -> Result<String> {
        let content = string(args, "content")?;
        if content.len() as u64 > MAX_FILE_BYTES {
            return Err(Error::Config("content exceeds 4 MiB".into()));
        }
        let input = Path::new(string(args, "path")?);
        let target = if input.is_absolute() {
            input.to_path_buf()
        } else {
            self.root.join(input)
        };
        if !self.unrestricted
            && (!target.starts_with(&self.root)
                || input
                    .components()
                    .any(|part| matches!(part, Component::ParentDir)))
        {
            return Err(Error::Config("path is outside the workspace".into()));
        }
        // Validate each existing ancestor before creating any missing directory.
        let parent = target
            .parent()
            .ok_or_else(|| Error::Config("file needs a parent".into()))?;
        let mut ancestor = parent;
        while !ancestor.exists() {
            ancestor = ancestor
                .parent()
                .ok_or_else(|| Error::Config("invalid parent".into()))?;
        }
        self.path(&ancestor.to_string_lossy())?;
        std::fs::create_dir_all(parent)?;
        let parent = self.path(&parent.to_string_lossy())?;
        let mut target = parent.join(
            target
                .file_name()
                .ok_or_else(|| Error::Config("file name missing".into()))?,
        );
        if target.symlink_metadata().is_ok() {
            let existing = self.path(&target.to_string_lossy())?;
            if !existing.is_file() {
                return Err(Error::Config("expected a regular file".into()));
            }
            target = existing;
        }
        atomic_write(&target, content.as_bytes())?;
        Ok(format!(
            "Wrote {} bytes to {}",
            content.len(),
            target.strip_prefix(&self.root).unwrap_or(&target).display()
        ))
    }

    fn edit(&self, args: &Value) -> Result<String> {
        let path = self.path(string(args, "path")?)?;
        let original = self.text(&path)?;
        let edits = args["edits"]
            .as_array()
            .ok_or_else(|| Error::Config("edits must be an array".into()))?;
        if edits.is_empty() {
            return Err(Error::Config("at least one edit is required".into()));
        }
        let mut replacements = Vec::new();
        for edit in edits {
            let old = string(edit, "oldText")?;
            let new = string(edit, "newText")?;
            if old.is_empty() {
                return Err(Error::Config("oldText must not be empty".into()));
            }
            let mut matches = original.match_indices(old);
            let (start, _) = matches
                .next()
                .ok_or_else(|| Error::Config("oldText was not found".into()))?;
            if matches.next().is_some() {
                return Err(Error::Config("oldText matches more than once".into()));
            }
            replacements.push((start, start + old.len(), new));
        }
        replacements.sort_by_key(|(start, _, _)| *start);
        if replacements.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            return Err(Error::Config("edits overlap in the original file".into()));
        }
        let mut output = original.clone();
        for (start, end, replacement) in replacements.into_iter().rev() {
            output.replace_range(start..end, replacement);
        }
        if output.len() as u64 > MAX_FILE_BYTES {
            return Err(Error::Config("edited file exceeds 4 MiB".into()));
        }
        if self.text(&path)? != original {
            return Err(Error::Config(
                "file changed while preparing edits; read it again".into(),
            ));
        }
        atomic_write(&path, output.as_bytes())?;
        Ok(format!(
            "Applied {} edits to {}",
            edits.len(),
            args["path"].as_str().unwrap_or_default()
        ))
    }

    fn list(&self, args: &Value) -> Result<String> {
        let path = self.path(args["path"].as_str().unwrap_or("."))?;
        let limit = args["limit"].as_u64().unwrap_or(500).min(1000) as usize;
        let mut entries = std::fs::read_dir(path)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        Ok(bounded(
            entries
                .into_iter()
                .take(limit)
                .map(|entry| {
                    let suffix = if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                        "/"
                    } else {
                        ""
                    };
                    format!("{}{suffix}", entry.file_name().to_string_lossy())
                })
                .collect::<Vec<_>>()
                .join("\n"),
        ))
    }

    fn search(&self, args: &Value) -> Result<String> {
        let base = self.path(args["path"].as_str().unwrap_or("."))?;
        let limit = args["limit"].as_u64().unwrap_or(100).min(1000) as usize;
        let pattern = string(args, "pattern")?;
        let is_find = matches!(self.kind, Kind::Find);
        let glob = if is_find {
            Some(pattern)
        } else {
            args["glob"].as_str()
        }
        .map(globset::Glob::new)
        .transpose()
        .map_err(|error| Error::Config(error.to_string()))?
        .map(|glob| glob.compile_matcher());
        let pattern = if args["literal"] == true {
            regex::escape(pattern)
        } else {
            pattern.to_owned()
        };
        let regex = if is_find {
            None
        } else {
            Some(
                regex::RegexBuilder::new(&pattern)
                    .case_insensitive(args["ignoreCase"] == true)
                    .build()
                    .map_err(|error| Error::Config(error.to_string()))?,
            )
        };
        let mut results = Vec::new();
        for entry in ignore::WalkBuilder::new(&base)
            .hidden(false)
            .build()
            .take(100_000)
        {
            let entry = entry.map_err(|error| Error::Config(error.to_string()))?;
            if !entry.file_type().is_some_and(|kind| kind.is_file()) {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(&base)
                .ok()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(entry.file_name()));
            if glob
                .as_ref()
                .is_some_and(|glob| !glob.is_match(relative) && !glob.is_match(entry.file_name()))
            {
                continue;
            }
            if is_find {
                results.push(relative.display().to_string());
            } else if let Ok(content) = self.text(entry.path()) {
                for (line, text) in content.lines().enumerate() {
                    if regex.as_ref().is_some_and(|regex| regex.is_match(text)) {
                        results.push(format!("{}:{}:{text}", relative.display(), line + 1));
                    }
                    if results.len() >= limit {
                        break;
                    }
                }
            }
            if results.len() >= limit {
                break;
            }
        }
        Ok(bounded(results.join("\n")))
    }

    async fn bash(&self, args: &Value) -> Result<String> {
        let mut command = tokio::process::Command::new("bash");
        command
            .args(["-c", string(args, "command")?])
            .current_dir(&self.root)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let mut process = ProcessGroup::new(command.spawn()?);
        let stdout = process.child.stdout.take().expect("piped stdout");
        let stderr = process.child.stderr.take().expect("piped stderr");
        let timeout = Duration::from_secs(args["timeout"].as_u64().unwrap_or(30).clamp(1, 120));
        let result = tokio::time::timeout(timeout, async {
            tokio::try_join!(process.child.wait(), capture(stdout), capture(stderr))
        })
        .await
        .map_err(|_| Error::Config("command timed out".into()))??;
        process.finished = true;
        let truncation = if result.1.truncated || result.2.truncated {
            "\n[Output truncated at 50 KiB per stream]"
        } else {
            ""
        };
        let output = format!(
            "{}{}{truncation}\nExit status: {}",
            String::from_utf8_lossy(&result.1.bytes),
            String::from_utf8_lossy(&result.2.bytes),
            result.0
        );
        if !result.0.success() {
            return Err(Error::Config(output));
        }
        Ok(output)
    }
}

fn string<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value[name]
        .as_str()
        .ok_or_else(|| Error::Config(format!("{name} must be a string")))
}

fn atomic_write(path: &Path, content: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::Config("file needs a parent".into()))?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    if let Ok(metadata) = path.metadata() {
        file.as_file().set_permissions(metadata.permissions())?;
    }
    file.write_all(content)?;
    file.as_file().sync_all()?;
    file.persist(path).map_err(|error| Error::Io(error.error))?;
    #[cfg(unix)]
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn bounded(mut output: String) -> String {
    if output.len() > MAX_OUTPUT_BYTES {
        let mut end = MAX_OUTPUT_BYTES;
        while !output.is_char_boundary(end) {
            end -= 1;
        }
        output.truncate(end);
        output.push_str("\n[Output truncated at 50 KiB]");
    }
    output
}

struct CapturedOutput {
    bytes: Vec<u8>,
    truncated: bool,
}

async fn capture(mut stream: impl AsyncRead + Unpin) -> std::io::Result<CapturedOutput> {
    let mut output = Vec::new();
    let mut truncated = false;
    let mut chunk = [0; 8192];
    loop {
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            break;
        }
        let remaining = MAX_OUTPUT_BYTES.saturating_sub(output.len());
        truncated |= count > remaining;
        output.extend_from_slice(&chunk[..count.min(remaining)]);
    }
    Ok(CapturedOutput {
        bytes: output,
        truncated,
    })
}

struct ProcessGroup {
    child: tokio::process::Child,
    #[cfg(unix)]
    group: Option<u32>,
    finished: bool,
}
impl ProcessGroup {
    fn new(child: tokio::process::Child) -> Self {
        Self {
            #[cfg(unix)]
            group: child.id(),
            child,
            finished: false,
        }
    }
}
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if !self.finished {
            #[cfg(unix)]
            if let Some(id) = self.group {
                // The child is its own process-group leader, so cancellation
                // also reaches grandchildren such as a shell's sleep process.
                unsafe {
                    libc::kill(-(id as i32), libc::SIGKILL);
                }
            }
            let _ = self.child.start_kill();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_profile_can_read_personal_skills_while_scoped_profile_cannot() {
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path().join("project");
        std::fs::create_dir(&workspace).unwrap();
        let skill = directory.path().join("SKILL.md");
        std::fs::write(&skill, "personal skill").unwrap();
        let scoped = CodingTool {
            root: workspace.clone(),
            unrestricted: false,
            kind: Kind::Read,
        };
        assert!(scoped.path(skill.to_str().unwrap()).is_err());
        let local = CodingTool {
            root: workspace,
            unrestricted: true,
            kind: Kind::Read,
        };
        assert_eq!(
            local.read(&json!({"path":skill})).unwrap(),
            "personal skill"
        );
    }

    #[test]
    fn edits_match_original_and_reject_overlap_without_writing() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("a.txt");
        std::fs::write(&path, "alpha beta gamma").unwrap();
        let tool = CodingTool {
            unrestricted: false,
            root: root.path().canonicalize().unwrap(),
            kind: Kind::Edit,
        };
        tool.edit(&json!({"path":"a.txt","edits":[{"oldText":"alpha","newText":"beta"},{"oldText":"beta","newText":"delta"}]})).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "beta delta gamma");
        assert!(tool.edit(&json!({"path":"a.txt","edits":[{"oldText":"beta delta","newText":"x"},{"oldText":"delta","newText":"y"}]})).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "beta delta gamma");
    }

    #[test]
    fn write_creates_parents_and_read_pages_utf8() {
        let root = tempfile::tempdir().unwrap();
        let tool = CodingTool {
            unrestricted: false,
            root: root.path().canonicalize().unwrap(),
            kind: Kind::Write,
        };
        tool.write(&json!({"path":"src/a.txt","content":"first\n日本語\nlast\n"}))
            .unwrap();
        assert_eq!(
            tool.read(&json!({"path":"src/a.txt","offset":2,"limit":1}))
                .unwrap(),
            "日本語"
        );
        assert!(
            tool.write(&json!({"path":"../escape.txt","content":"bad"}))
                .is_err()
        );
    }

    #[test]
    fn glob_search_and_file_scoped_grep_use_the_correct_matcher() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("main.rs"), "fn main() {}\n").unwrap();
        let tool = CodingTool {
            unrestricted: false,
            root: root.path().canonicalize().unwrap(),
            kind: Kind::Find,
        };
        assert_eq!(
            tool.search(&json!({"pattern":"**/*.rs"})).unwrap(),
            "main.rs"
        );
        let tool = CodingTool {
            unrestricted: false,
            kind: Kind::Grep,
            ..tool
        };
        assert_eq!(
            tool.search(&json!({"pattern":"fn", "path":"main.rs"}))
                .unwrap(),
            "main.rs:1:fn main() {}"
        );
    }

    #[tokio::test]
    async fn shell_drains_both_streams_and_reports_failure() {
        let root = tempfile::tempdir().unwrap();
        let tool = CodingTool {
            unrestricted: false,
            root: root.path().to_path_buf(),
            kind: Kind::Bash,
        };
        let output = tool
            .bash(&json!({"command":"printf out; printf err >&2; exit 7"}))
            .await
            .unwrap_err()
            .to_string();
        assert!(output.contains("outerr"));
        assert!(output.contains('7'));
    }
}
