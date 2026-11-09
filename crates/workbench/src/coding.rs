//! Local coding tools. Hosted callers opt into their own execution profile.
use base64::Engine;
use sha2::{Digest, Sha256};
use std::io::{Cursor, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use pi_rs::{
    Error, Extension, Outcome, Result, Tool, ToolContext, ToolExecution, ToolHost, ToolReply,
    ToolSpec,
};
use serde_json::{Value, json};
use tokio::io::AsyncReadExt;

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
    /// Read a user-selected attachment using the same image limits as the read tool.
    pub fn read_attachment(&self, path: &Path) -> Result<(String, Vec<pi_rs::ImageContent>)> {
        let tool = CodingTool {
            root: self.root.clone(),
            unrestricted: self.unrestricted,
            kind: Kind::Read,
        };
        let name = path
            .to_str()
            .ok_or_else(|| Error::Config("attachment path must be UTF-8".into()))?;
        if let Some(image) = tool.read_image(&json!({"path":name}))? {
            let blocks = image["content"].as_array().expect("image content");
            let images = blocks
                .iter()
                .filter(|block| block["type"] == "image")
                .cloned()
                .map(serde_json::from_value)
                .collect::<std::result::Result<_, _>>()?;
            let hints = blocks
                .iter()
                .filter_map(|block| block["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n");
            Ok((hints, images))
        } else {
            Ok((tool.text(&tool.path(name)?)?, Vec::new()))
        }
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
            if cfg!(windows) {
                Kind::PowerShell
            } else {
                Kind::Bash
            },
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
    PowerShell,
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
                "Read text with optional one-based line offset and line limit, or return a PNG/JPEG/GIF/WebP/BMP/TIFF image to the model.",
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
                json!({"path":path,"oldText":{"type":"string","minLength":1},"newText":{"type":"string"},"edits":{"type":"array","minItems":1,"items":{"type":"object","properties":{"oldText":{"type":"string","minLength":1},"newText":{"type":"string"}},"required":["oldText","newText"],"additionalProperties":false}}}),
                vec!["path"],
            ),
            Kind::Bash | Kind::PowerShell => (
                if matches!(self.kind, Kind::PowerShell) {
                    "powershell"
                } else {
                    "bash"
                },
                "Run a shell command in the workspace. Returns the last 2000 lines or 50 KiB and saves complete output to a temporary file when truncated.",
                json!({"command":{"type":"string"},"timeout":{"type":"number","exclusiveMinimum":0,"maximum":2147483.647}}),
                vec!["command"],
            ),
            Kind::Grep => (
                "grep",
                "Search UTF-8 file contents. Respects ignore files and returns path, line number, and matching text.",
                json!({"pattern":{"type":"string"},"path":path,"glob":{"type":"string"},"ignoreCase":{"type":"boolean"},"literal":{"type":"boolean"},"context":{"type":"integer","minimum":0,"maximum":1000},"limit":{"type":"integer","minimum":1,"maximum":100000}}),
                vec!["pattern"],
            ),
            Kind::Find => (
                "find",
                "Find workspace files matching a glob. Respects ignore files.",
                json!({"pattern":{"type":"string"},"path":path,"limit":{"type":"integer","minimum":1,"maximum":100000}}),
                vec!["pattern"],
            ),
            Kind::List => (
                "ls",
                "List the immediate entries in a workspace directory.",
                json!({"path":path,"limit":{"type":"integer","minimum":1,"maximum":100000}}),
                vec![],
            ),
        };
        let mut parameters = json!({"type":"object","properties":properties,"required":required,"additionalProperties":false});
        if matches!(self.kind, Kind::Edit) {
            parameters["anyOf"] =
                json!([{"required":["edits"]},{"required":["oldText","newText"]}]);
        }
        ToolSpec {
            name: name.into(),
            description: description.into(),
            parameters,
        }
    }

    fn execution(&self) -> ToolExecution {
        match self.kind {
            Kind::Read | Kind::Grep | Kind::Find | Kind::List => ToolExecution::ParallelRead,
            _ => ToolExecution::Sequential,
        }
    }

    async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
        if matches!(self.kind, Kind::Read) {
            match self.read_image(&args) {
                Ok(Some(data)) => {
                    return Outcome::Structured {
                        data,
                        attachments: vec![],
                    };
                }
                Ok(None) => {}
                Err(error) => return Outcome::Failed(error.to_string()),
            }
        }
        let result = match self.kind {
            Kind::Read => self.read(&args),
            Kind::Write => self.write(&args),
            Kind::Edit => self.edit(&args),
            Kind::Grep | Kind::Find => self.search(&args),
            Kind::List => self.list(&args),
            Kind::Bash | Kind::PowerShell => self.bash(&args).await,
        };
        match result {
            Ok(value) => Outcome::Succeeded(value),
            Err(error) => Outcome::Failed(error.to_string()),
        }
    }
    async fn invoke_programmatic(
        &self,
        args: Value,
        context: ToolContext,
        _state: Option<&pi_rs::ExtensionState>,
        _host: Arc<dyn ToolHost>,
    ) -> ToolReply {
        if matches!(self.kind, Kind::Bash | Kind::PowerShell) {
            return match self.shell(&args).await {
                Ok(result) => {
                    let mut data = json!({"output":result.output.script,"truncated":result.output.script_truncated,"exit_code":exit_code(result.status),"wall_time_seconds":result.elapsed});
                    if result.output.script_truncated {
                        data["full_output_path"] = json!(result.output.path);
                    }
                    Outcome::Structured {
                        data,
                        attachments: vec![],
                    }
                    .into()
                }
                Err(error) => Outcome::Failed(error.to_string()).into(),
            };
        }
        let outcome = self.invoke(args, context).await;
        match outcome {
            Outcome::Structured { data, attachments } if matches!(self.kind, Kind::Read) => {
                let image = data["content"]
                    .as_array()
                    .and_then(|blocks| blocks.iter().find(|block| block["type"] == "image"))
                    .cloned()
                    .unwrap_or(data);
                Outcome::Structured {
                    data: image,
                    attachments,
                }
                .into()
            }
            outcome => outcome.into(),
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

    fn read_image(&self, args: &Value) -> Result<Option<Value>> {
        let path = self.path(string(args, "path")?)?;
        if !path.is_file() {
            return Err(Error::Config("expected a regular file".into()));
        }
        let reader = image::ImageReader::open(&path)?.with_guessed_format()?;
        if reader.format().is_none() {
            return Ok(None);
        }
        if path.metadata()?.len() > 20 * 1024 * 1024 {
            return Err(Error::Config("image exceeds 20 MiB".into()));
        }
        let prepared = normalize_image(reader)?;
        Ok(Some(json!({"content":[
            {"type":"text","text":format!("Read image {} ({}×{}, provided at {}×{})",path.display(),prepared.source_width,prepared.source_height,prepared.width,prepared.height)},
            {"type":"image","mimeType":"image/png","data":base64::engine::general_purpose::STANDARD.encode(prepared.png)}
        ]})))
    }

    fn read(&self, args: &Value) -> Result<String> {
        let path = self.path(string(args, "path")?)?;
        let offset = usize::try_from(args["offset"].as_u64().unwrap_or(1).saturating_sub(1))
            .unwrap_or(usize::MAX);
        let limit = args["limit"]
            .as_u64()
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));
        crate::text_page::read(&path, offset, limit)
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
        let _lock = mutation_lock(&target)?;
        atomic_write(&target, content.as_bytes())?;
        Ok(format!(
            "Wrote {} bytes to {}",
            content.len(),
            target.strip_prefix(&self.root).unwrap_or(&target).display()
        ))
    }

    fn edit(&self, args: &Value) -> Result<String> {
        let path = self.path(string(args, "path")?)?;
        let _lock = mutation_lock(&path)?;
        let original = self.text(&path)?;
        let legacy = vec![json!({"oldText":args["oldText"],"newText":args["newText"]})];
        let edits = match args.get("edits") {
            Some(value) => value
                .as_array()
                .ok_or_else(|| Error::Config("edits must be an array".into()))?,
            None => &legacy,
        };
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
        let display_path = args["path"].as_str().unwrap_or_default();
        let diff = similar::TextDiff::from_lines(&original, &output)
            .unified_diff()
            .context_radius(3)
            .header(display_path, display_path)
            .to_string();
        Ok(bounded(format!(
            "Applied {} edits to {display_path}\n{diff}",
            edits.len()
        )))
    }

    fn list(&self, args: &Value) -> Result<String> {
        let path = self.path(args["path"].as_str().unwrap_or("."))?;
        let limit = args["limit"].as_u64().unwrap_or(500).min(100_000) as usize;
        let mut entries = std::fs::read_dir(path)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.file_name().to_string_lossy().to_lowercase());
        let entries: Vec<_> = entries
            .into_iter()
            .filter_map(|entry| {
                let metadata = std::fs::metadata(entry.path()).ok()?;
                Some(format!(
                    "{}{}",
                    entry.file_name().to_string_lossy(),
                    if metadata.is_dir() { "/" } else { "" }
                ))
            })
            .collect();
        if entries.is_empty() {
            return Ok("(empty directory)".into());
        }
        let mut output = bounded(
            entries
                .iter()
                .take(limit)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n"),
        );
        if entries.len() > limit {
            output.push_str(&format!(
                "\n\n[{limit} entries limit reached. Use limit={} for more.]",
                limit.saturating_mul(2)
            ));
        }
        Ok(output)
    }

    fn search(&self, args: &Value) -> Result<String> {
        let base = self.path(args["path"].as_str().unwrap_or("."))?;
        let is_find = matches!(self.kind, Kind::Find);
        let limit = args["limit"]
            .as_u64()
            .unwrap_or(if is_find { 1000 } else { 100 })
            .min(100_000) as usize;
        let pattern = string(args, "pattern")?;
        let glob = if is_find {
            Some(pattern)
        } else {
            args["glob"].as_str()
        }
        .map(globset::Glob::new)
        .transpose()
        .map_err(|error| Error::Config(error.to_string()))?
        .map(|glob| glob.compile_matcher());
        let mut grep = if is_find {
            None
        } else {
            Some(crate::file_search::Grep::new(args, limit)?)
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
            if let Some(grep) = &mut grep {
                if !grep.search(entry.path(), &relative.to_string_lossy())? {
                    break;
                }
            } else {
                results.push(relative.display().to_string());
                if results.len() >= limit {
                    break;
                }
            }
        }
        Ok(match grep {
            Some(grep) => grep.finish(),
            None => {
                if results.is_empty() {
                    return Ok("No files found matching pattern".into());
                }
                let reached = results.len() >= limit;
                let mut output = bounded(results.join("\n"));
                if reached {
                    output.push_str(&format!(
                        "\n\n[{limit} results limit reached. Use limit={} for more.]",
                        limit.saturating_mul(2)
                    ));
                }
                output
            }
        })
    }

    async fn bash(&self, args: &Value) -> Result<String> {
        let result = self.shell(args).await?;
        let output = format!("{}\nExit status: {}", result.output.display, result.status);
        if !result.status.success() {
            return Err(Error::Config(output));
        }
        Ok(output)
    }
    async fn shell(&self, args: &Value) -> Result<ShellResult> {
        let started = std::time::Instant::now();
        let timeout = shell_timeout(args)?;
        let mut command = if matches!(self.kind, Kind::PowerShell) {
            let executable = std::env::var_os("PATH")
                .into_iter()
                .flat_map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
                .map(|path| path.join(if cfg!(windows) { "pwsh.exe" } else { "pwsh" }))
                .find(|path| path.is_file())
                .unwrap_or_else(|| "powershell.exe".into());
            let mut command = tokio::process::Command::new(executable);
            command.args(["-NoLogo", "-NoProfile", "-NonInteractive", "-Command"]).arg(format!("$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new(); {}", string(args, "command")?));
            command
        } else {
            let mut command = tokio::process::Command::new("bash");
            command.args(["-c", string(args, "command")?]);
            command
        };
        command
            .current_dir(&self.root)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let mut process = ProcessGroup::spawn(command)?;
        let mut stdout = process.stdout();
        let mut stderr = process.stderr();
        let deadline = async {
            match timeout {
                Some(timeout) => tokio::time::sleep(timeout).await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(deadline);
        let mut output = crate::shell_output::Accumulator::default();
        let mut stdout_open = true;
        let mut stderr_open = true;
        let mut status = None;
        let mut stdout_chunk = [0; 8192];
        let mut stderr_chunk = [0; 8192];
        let mut drain_deadline = None;
        while status.is_none() || stdout_open || stderr_open {
            tokio::select! {
                _ = &mut deadline => {
                    let output = output.finish()?;
                    return Err(Error::Config(format!("{}\nCommand timed out after {} seconds", output.display, timeout.expect("deadline needs a timeout").as_secs_f64())));
                }
                result = process.wait(), if status.is_none() => {
                    status = Some(result?);
                    process.finished = true;
                    // Descendants may retain inherited pipes after the shell exits.
                    drain_deadline = Some(tokio::time::Instant::now() + Duration::from_millis(250));
                }
                count = stdout.read(&mut stdout_chunk), if stdout_open => {
                    let count = count?;
                    stdout_open = count != 0;
                    output.append(&stdout_chunk[..count])?;
                }
                count = stderr.read(&mut stderr_chunk), if stderr_open => {
                    let count = count?;
                    stderr_open = count != 0;
                    output.append(&stderr_chunk[..count])?;
                }
                _ = async {
                    match drain_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => break,
            }
        }
        Ok(ShellResult {
            output: output.finish()?,
            status: status.expect("shell status observed"),
            elapsed: started.elapsed().as_secs_f64(),
        })
    }
}

pub struct PreparedImage {
    pub png: Vec<u8>,
    pub source_width: u32,
    pub source_height: u32,
    pub width: u32,
    pub height: u32,
}
/// Normalize supported image readers under the shared decoding and output limits.
pub fn normalize_image<R: std::io::BufRead + std::io::Seek>(
    mut reader: image::ImageReader<R>,
) -> Result<PreparedImage> {
    let mut limits = image::Limits::default();
    limits.max_alloc = Some(128 * 1024 * 1024);
    reader.limits(limits);
    let original = reader
        .decode()
        .map_err(|error| Error::Config(format!("cannot decode image: {error}")))?;
    let source_width = original.width();
    let source_height = original.height();
    let mut resized = if source_width > 2000 || source_height > 2000 {
        original.thumbnail(2000, 2000)
    } else {
        original
    };
    let mut png = Vec::new();
    loop {
        png.clear();
        resized
            .write_to(&mut Cursor::new(&mut png), image::ImageFormat::Png)
            .map_err(|error| Error::Config(error.to_string()))?;
        if png.len() <= 512 * 1024 {
            break;
        }
        let width = (resized.width() * 3 / 4).max(1);
        let height = (resized.height() * 3 / 4).max(1);
        if width == resized.width() && height == resized.height() {
            return Err(Error::Config("image cannot fit the output limit".into()));
        }
        resized = resized.thumbnail(width, height);
    }
    Ok(PreparedImage {
        png,
        source_width,
        source_height,
        width: resized.width(),
        height: resized.height(),
    })
}

struct ShellResult {
    output: crate::shell_output::Output,
    status: std::process::ExitStatus,
    elapsed: f64,
}

fn string<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value[name]
        .as_str()
        .ok_or_else(|| Error::Config(format!("{name} must be a string")))
}

fn mutation_lock(path: &Path) -> Result<std::fs::File> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .unwrap_or_default();
    let owner = Sha256::digest(home.as_encoded_bytes());
    let directory = std::env::temp_dir().join(format!("pi-rs-file-locks-{owner:x}"));
    std::fs::create_dir_all(&directory)?;
    let digest = Sha256::digest(path.as_os_str().as_encoded_bytes());
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join(format!("{digest:x}.lock")))?;
    file.lock()?;
    Ok(file)
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
        if let Some(newline) = output[..end].rfind('\n') {
            end = newline;
        }
        output.truncate(end);
        output.push_str("\n[Output truncated at 50 KiB]");
    }
    output
}

fn shell_timeout(args: &Value) -> Result<Option<Duration>> {
    let Some(value) = args.get("timeout") else {
        return Ok(None);
    };
    let seconds = value
        .as_f64()
        .filter(|seconds| seconds.is_finite() && *seconds > 0.0 && *seconds <= 2_147_483.647)
        .ok_or_else(|| {
            Error::Config(
                "timeout must be a positive number no greater than 2147483.647 seconds".into(),
            )
        })?;
    Ok(Some(Duration::from_secs_f64(seconds)))
}

fn exit_code(status: std::process::ExitStatus) -> i32 {
    if let Some(code) = status.code() {
        return code;
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        128 + status.signal().unwrap_or(0)
    }
    #[cfg(not(unix))]
    {
        1
    }
}

#[cfg(not(windows))]
type ShellChild = tokio::process::Child;
#[cfg(windows)]
type ShellChild = Box<dyn process_wrap::tokio::ChildWrapper>;

struct ProcessGroup {
    child: ShellChild,
    #[cfg(unix)]
    group: Option<u32>,
    finished: bool,
}
impl ProcessGroup {
    fn spawn(command: tokio::process::Command) -> std::io::Result<Self> {
        #[cfg(windows)]
        let child = {
            let mut wrapped = process_wrap::tokio::CommandWrap::from(command);
            wrapped.wrap(process_wrap::tokio::JobObject);
            wrapped.wrap(process_wrap::tokio::KillOnDrop);
            wrapped.spawn()?
        };
        #[cfg(not(windows))]
        let child = {
            let mut command = command;
            command.spawn()?
        };
        Ok(Self {
            #[cfg(unix)]
            group: child.id(),
            child,
            finished: false,
        })
    }
    fn stdout(&mut self) -> tokio::process::ChildStdout {
        #[cfg(windows)]
        {
            self.child.stdout().take().expect("piped stdout")
        }
        #[cfg(not(windows))]
        {
            self.child.stdout.take().expect("piped stdout")
        }
    }
    fn stderr(&mut self) -> tokio::process::ChildStderr {
        #[cfg(windows)]
        {
            self.child.stderr().take().expect("piped stderr")
        }
        #[cfg(not(windows))]
        {
            self.child.stderr.take().expect("piped stderr")
        }
    }
    async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.child.wait().await
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
    fn concurrent_disjoint_edits_are_serialized_and_return_diffs() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        std::fs::write(root.join("file.txt"), "alpha beta\n").unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let handles = [("alpha", "A"), ("beta", "B")]
            .into_iter()
            .map(|(old, new)| {
                let root = root.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let tool = CodingTool {
                        root,
                        unrestricted: false,
                        kind: Kind::Edit,
                    };
                    barrier.wait();
                    tool.edit(&json!({"path":"file.txt","oldText":old,"newText":new}))
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        for handle in handles {
            assert!(handle.join().unwrap().contains("@@"));
        }
        assert_eq!(
            std::fs::read_to_string(root.join("file.txt")).unwrap(),
            "A B\n"
        );
    }

    #[test]
    fn image_reads_preserve_alpha_and_emit_bounded_multimodal_content() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sample.png");
        image::RgbaImage::from_pixel(32, 24, image::Rgba([255, 0, 0, 128]))
            .save(&path)
            .unwrap();
        let tool = CodingTool {
            root: directory.path().canonicalize().unwrap(),
            unrestricted: false,
            kind: Kind::Read,
        };
        let value = tool
            .read_image(&json!({"path":"sample.png"}))
            .unwrap()
            .unwrap();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(value["content"][1]["data"].as_str().unwrap())
            .unwrap();
        let image = image::load_from_memory(&bytes).unwrap().to_rgba8();
        assert_eq!(image.dimensions(), (32, 24));
        assert_eq!(image.get_pixel(0, 0).0, [255, 0, 0, 128]);
        assert!(bytes.len() <= 512 * 1024);
    }

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
            "日本語\n\n[2 more lines in file. Use offset=3 to continue.]"
        );
        assert!(
            tool.write(&json!({"path":"../escape.txt","content":"bad"}))
                .is_err()
        );
    }

    #[test]
    fn directory_output_sorts_case_insensitively_and_reports_limits() {
        let root = tempfile::tempdir().unwrap();
        let tool = CodingTool {
            root: root.path().canonicalize().unwrap(),
            unrestricted: false,
            kind: Kind::List,
        };
        assert_eq!(tool.list(&json!({})).unwrap(), "(empty directory)");
        std::fs::write(root.path().join("Zeta"), "").unwrap();
        std::fs::write(root.path().join("alpha"), "").unwrap();
        std::fs::create_dir(root.path().join("Beta")).unwrap();
        assert_eq!(tool.list(&json!({})).unwrap(), "alpha\nBeta/\nZeta");
        let output = tool.list(&json!({"limit":1})).unwrap();
        assert!(output.starts_with("alpha\n"));
        assert!(output.contains("1 entries limit reached"));
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
            "main.rs:1: fn main() {}"
        );
    }

    #[tokio::test]
    async fn shell_timeout_retains_partial_output_and_rejects_invalid_values() {
        let root = tempfile::tempdir().unwrap();
        let tool = CodingTool {
            root: root.path().to_path_buf(),
            unrestricted: false,
            kind: if cfg!(windows) {
                Kind::PowerShell
            } else {
                Kind::Bash
            },
        };
        let (command, timeout) = if cfg!(windows) {
            ("Write-Output before; Start-Sleep -Seconds 20", 5.0)
        } else {
            ("printf before; sleep 5", 0.05)
        };
        let error = tool
            .bash(&json!({"command":command, "timeout":timeout}))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("before"));
        assert!(error.contains(&format!("timed out after {timeout}")));
        assert_eq!(shell_timeout(&json!({})).unwrap(), None);
        for value in [json!(0), json!(-1), json!(null), json!("1"), json!(2147484)] {
            assert!(shell_timeout(&json!({"timeout":value})).is_err());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shell_does_not_wait_for_inherited_background_pipes() {
        let root = tempfile::tempdir().unwrap();
        let tool = CodingTool {
            root: root.path().to_path_buf(),
            unrestricted: false,
            kind: Kind::Bash,
        };
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            tool.shell(&json!({"command":"sleep 1 & printf done"})),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.output.script, "done");
        assert!(result.status.success());
    }

    #[tokio::test]
    async fn shell_drains_both_streams_and_reports_failure() {
        let root = tempfile::tempdir().unwrap();
        let tool = CodingTool {
            unrestricted: false,
            root: root.path().to_path_buf(),
            kind: if cfg!(windows) {
                Kind::PowerShell
            } else {
                Kind::Bash
            },
        };
        let command = if cfg!(windows) {
            "[Console]::Out.Write('out'); [Console]::Error.Write('err'); exit 7"
        } else {
            "printf out; printf err >&2; exit 7"
        };
        let output = tool
            .bash(&json!({"command":command}))
            .await
            .unwrap_err()
            .to_string();
        assert!(output.contains("out"));
        assert!(output.contains("err"));
        assert!(output.contains('7'));
    }
    #[cfg(windows)]
    #[tokio::test]
    async fn windows_default_shell_preserves_unicode_and_kills_child_processes() {
        let root = tempfile::tempdir().unwrap();
        let extension = CodingTools::local(root.path()).unwrap();
        assert!(
            extension
                .tools()
                .iter()
                .any(|tool| tool.spec().name == "powershell")
        );
        assert!(
            !extension
                .tools()
                .iter()
                .any(|tool| tool.spec().name == "bash")
        );
        let tool = CodingTool {
            root: root.path().to_owned(),
            unrestricted: false,
            kind: Kind::PowerShell,
        };
        let output = tool
            .shell(&json!({"command":"Write-Output '日本語'"}))
            .await
            .unwrap();
        assert!(output.output.script.contains("日本語"));
        let ready = root
            .path()
            .join("ready.txt")
            .display()
            .to_string()
            .replace('\'', "''");
        let leaked = root
            .path()
            .join("leaked.txt")
            .display()
            .to_string()
            .replace('\'', "''");
        let script = format!(
            "Set-Content -LiteralPath '{ready}' -Value ready; Start-Sleep -Seconds 10; Set-Content -LiteralPath '{leaked}' -Value leaked"
        );
        let command = format!(
            "Start-Process -FilePath (Get-Process -Id $PID).Path -ArgumentList '-NoProfile','-Command',\"{script}\"; Start-Sleep -Seconds 20"
        );
        assert!(
            tool.shell(&json!({"command":command,"timeout":5}))
                .await
                .is_err()
        );
        assert!(
            root.path().join("ready.txt").exists(),
            "child process must have started"
        );
        tokio::time::sleep(Duration::from_secs(11)).await;
        assert!(
            !root.path().join("leaked.txt").exists(),
            "timed-out descendants must be terminated"
        );
    }
}
