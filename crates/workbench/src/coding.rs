//! Local coding tools. Hosted callers opt into their own execution profile.
use base64::Engine;
use sha2::{Digest, Sha256};
use std::io::{Cursor, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use pi_rs::{
    Error, Extension, Outcome, Result, Tool, ToolContext, ToolExecution, ToolHost, ToolReply,
    ToolSpec,
};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub use crate::shell_config::{ShellConfig, ShellContext, ShellSpawnContext, ShellSpawnHook};

const MAX_FILE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_ORIGINAL_IMAGE_BYTES: usize = 6 * 1024 * 1024;
const MAX_OUTPUT_BYTES: usize = 50 * 1024;

pub struct CodingTools {
    root: PathBuf,
    unrestricted: bool,
    shell: ShellConfig,
    auto_resize_images: bool,
    image_profile: Option<crate::image_profile::ImageResizeProfile>,
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
            shell: ShellConfig::default(),
            auto_resize_images: true,
            image_profile: None,
        })
    }
    /// Local Pi profile, with paths resolved from the workspace and access to user resources.
    pub fn local(root: &Path) -> Result<Self> {
        let mut tools = Self::new(root)?;
        tools.unrestricted = true;
        Ok(tools)
    }
    pub fn with_shell_config(mut self, shell: ShellConfig) -> Self {
        self.shell = shell;
        self
    }

    pub fn with_image_resize(mut self, enabled: bool) -> Self {
        self.auto_resize_images = enabled;
        self
    }

    pub fn with_image_profile(
        mut self,
        profile: Option<crate::image_profile::ImageResizeProfile>,
    ) -> Self {
        self.image_profile = profile;
        self
    }

    /// Execute an explicit user shell command without making a model request.
    pub async fn execute_shell(
        &self,
        command: &str,
        timeout: Option<Duration>,
        cancellation: &pi_rs::CancellationToken,
        progress: &mut (dyn FnMut(&str) + Send),
    ) -> Result<pi_rs::BashExecution> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if !self.unrestricted && command.len() > 1024 * 1024 {
            return Err(Error::Config("shell command exceeds 1048576 bytes".into()));
        }
        let tool = CodingTool {
            root: self.root.clone(),
            unrestricted: self.unrestricted,
            shell: self.shell.clone(),
            kind: if cfg!(windows) && !self.shell.uses_custom_bash() {
                Kind::PowerShell
            } else {
                Kind::Bash
            },
        };
        let mut arguments = json!({"command":command});
        if let Some(timeout) = timeout {
            arguments["timeout"] = json!(timeout.as_secs_f64());
        }
        let result = tool
            .shell_stream(&arguments, cancellation, progress, true)
            .await?;
        let output = if result.timed_out {
            format!(
                "{}\nCommand timed out after {} seconds",
                result.output.display_without_notice,
                timeout.expect("elapsed timeout").as_secs_f64()
            )
        } else {
            result.output.display_without_notice
        };
        Ok(pi_rs::BashExecution {
            command: command.into(),
            output,
            exit_code: result.status.code(),
            cancelled: result.cancelled,
            truncated: result.output.details.is_some(),
            full_output_path: result
                .output
                .path
                .map(|path| path.to_string_lossy().into_owned()),
            exclude_from_context: false,
        })
    }

    /// Run extension-supplied shell operations through the usual user output limits.
    pub async fn execute_shell_operations(
        &self,
        event: &pi_rs::UserBashEvent,
        extension: &dyn pi_rs::Extension,
        state: Option<&pi_rs::ExtensionState>,
        timeout: Option<Duration>,
        cancellation: &pi_rs::CancellationToken,
        progress: &mut (dyn FnMut(&str) + Send),
    ) -> Result<pi_rs::BashExecution> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if !self.unrestricted && event.command.len() > 1024 * 1024 {
            return Err(Error::Config("shell command exceeds 1048576 bytes".into()));
        }
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Vec<u8>>(16);
        let operation_cancel = cancellation.child_token();
        let mut output = crate::shell_output::Accumulator::for_user_shell();
        let mut text = ShellStream::default();
        let mut filter = UserShellFilter::default();
        let mut cancelled = false;
        let mut timed_out = false;
        let mut operation_event = event.clone();
        operation_event.command = self.shell.command_with_prefix(&event.command);
        let exit_code = {
            let operation = extension.execute_user_bash(
                &operation_event,
                state,
                timeout,
                operation_cancel.clone(),
                sender,
            );
            tokio::pin!(operation);
            let deadline = async {
                match timeout {
                    Some(timeout) => tokio::time::sleep(timeout).await,
                    None => std::future::pending().await,
                }
            };
            tokio::pin!(deadline);
            let mut open = true;
            loop {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => { cancelled = true; operation_cancel.cancel(); break None; }
                    _ = &mut deadline => { cancelled = true; timed_out = true; operation_cancel.cancel(); break None; }
                    chunk = receiver.recv(), if open => match chunk {
                        Some(chunk) => append_user_shell_chunk(&mut output, &mut text, &mut filter, &chunk, progress)?,
                        None => open = false,
                    },
                    result = &mut operation => {
                        while let Ok(chunk) = receiver.try_recv() {
                            append_user_shell_chunk(&mut output, &mut text, &mut filter, &chunk, progress)?;
                        }
                        break result.map_err(|message| Error::Extension { id: extension.id().into(), message })?;
                    }
                }
            }
        };
        while let Ok(chunk) = receiver.try_recv() {
            append_user_shell_chunk(&mut output, &mut text, &mut filter, &chunk, progress)?;
        }
        let mut remaining = String::new();
        text.finish(&mut |text| remaining.push_str(&filter.filter(text)));
        remaining.push_str(&filter.finish());
        if !remaining.is_empty() {
            progress(&remaining);
            output.append(remaining.as_bytes())?;
        }
        let result = output.finish()?;
        let output = if timed_out {
            format!(
                "{}\nCommand timed out after {} seconds",
                result.display_without_notice,
                timeout.expect("elapsed timeout").as_secs_f64()
            )
        } else {
            result.display_without_notice
        };
        Ok(pi_rs::BashExecution {
            command: event.command.clone(),
            output,
            exit_code,
            cancelled,
            truncated: result.details.is_some(),
            full_output_path: result.path.map(|path| path.to_string_lossy().into_owned()),
            exclude_from_context: event.exclude_from_context,
        })
    }

    /// Resolve a reader path, including Pi's filename fallbacks and this profile's scope.
    pub fn resolve_read_path(&self, path: &Path) -> Result<PathBuf> {
        CodingTool {
            root: self.root.clone(),
            unrestricted: self.unrestricted,
            shell: self.shell.clone(),
            kind: Kind::Read {
                auto_resize: self.auto_resize_images,
                profile: self.image_profile,
            },
        }
        .path(
            path.to_str()
                .ok_or_else(|| Error::Config("attachment path must be UTF-8".into()))?,
        )
    }

    /// Read a user-selected attachment using the same image limits as the read tool.
    pub fn read_attachment(&self, path: &Path) -> Result<(String, Vec<pi_rs::ImageContent>)> {
        let tool = CodingTool {
            root: self.root.clone(),
            unrestricted: self.unrestricted,
            shell: self.shell.clone(),
            kind: Kind::Read {
                auto_resize: self.auto_resize_images,
                profile: self.image_profile,
            },
        };
        let name = path
            .to_str()
            .ok_or_else(|| Error::Config("attachment path must be UTF-8".into()))?;
        let path = tool.path(name)?;
        if let Some(image) = tool.read_image(&path)? {
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
            Ok((tool.text(&path)?, Vec::new()))
        }
    }
}

fn append_user_shell_chunk(
    output: &mut crate::shell_output::Accumulator,
    decoder: &mut ShellStream,
    filter: &mut UserShellFilter,
    chunk: &[u8],
    progress: &mut (dyn FnMut(&str) + Send),
) -> Result<()> {
    output.record_raw_bytes(chunk.len());
    let mut clean = String::new();
    decoder.push(chunk, &mut |text| clean.push_str(&filter.filter(text)));
    if !clean.is_empty() {
        progress(&clean);
        output.append(clean.as_bytes())?;
    }
    Ok(())
}

impl Extension for CodingTools {
    fn id(&self) -> &str {
        "local-coding-tools"
    }

    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        let mut kinds = vec![
            Kind::Read {
                auto_resize: self.auto_resize_images,
                profile: self.image_profile,
            },
            Kind::Write,
            Kind::Edit,
            Kind::Grep,
            Kind::Find,
            Kind::List,
        ];
        if cfg!(windows) {
            kinds.push(Kind::PowerShell);
        }
        if !cfg!(windows) || self.unrestricted {
            kinds.push(Kind::Bash);
        }
        kinds
            .into_iter()
            .map(|kind| {
                Arc::new(CodingTool {
                    root: self.root.clone(),
                    unrestricted: self.unrestricted,
                    shell: self.shell.clone(),
                    kind,
                }) as Arc<dyn Tool>
            })
            .collect()
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Read {
        auto_resize: bool,
        profile: Option<crate::image_profile::ImageResizeProfile>,
    },
    Write,
    Edit,
    Bash,
    PowerShell,
    Grep,
    Find,
    List,
}

#[derive(Clone)]
struct CodingTool {
    root: PathBuf,
    unrestricted: bool,
    shell: ShellConfig,
    kind: Kind,
}

#[async_trait::async_trait]
impl Tool for CodingTool {
    fn spec(&self) -> ToolSpec {
        let path = json!({"type":"string","description":if self.unrestricted { "Absolute path or path relative to the workspace" } else { "Path relative to the workspace, or an absolute path inside it" }});
        let (name, description, properties, required) = match self.kind {
            Kind::Read { .. } => (
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
            Kind::Edit => {
                let replacement = json!({
                    "type": "object",
                    "properties": {
                        "oldText": {"type":"string", "minLength":1},
                        "newText": {"type":"string"}
                    },
                    "required": ["oldText", "newText"],
                    "additionalProperties": false
                });
                (
                    "edit",
                    "Apply unique non-overlapping replacements against the original file, preserving UTF-8 BOMs and line endings.",
                    json!({
                        "path": path,
                        "edits": {"type":"array", "minItems":1, "items":replacement}
                    }),
                    vec!["path", "edits"],
                )
            }
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
                "Search for files by glob pattern. Returns matching file paths relative to the search directory. Respects .gitignore. Output is truncated to 1000 results or 50KB (whichever is hit first).",
                json!({"pattern":{"type":"string"},"path":path,"limit":{"type":"integer","minimum":1,"maximum":100000}}),
                vec!["pattern"],
            ),
            Kind::List => (
                "ls",
                "List directory contents. Returns entries sorted alphabetically, with '/' suffix for directories. Includes dotfiles. Output is truncated to 500 entries or 50KB (whichever is hit first).",
                json!({"path":path,"limit":{"type":"integer","minimum":1,"maximum":100000}}),
                vec![],
            ),
        };
        let parameters = json!({"type":"object","properties":properties,"required":required,"additionalProperties":false});
        ToolSpec {
            name: name.into(),
            description: description.into(),
            parameters,
        }
    }

    fn execution(&self) -> ToolExecution {
        match self.kind {
            Kind::Read { .. } | Kind::Grep | Kind::Find | Kind::List => ToolExecution::ParallelRead,
            _ => ToolExecution::Sequential,
        }
    }

    fn output_limit(&self) -> usize {
        if self.unrestricted && matches!(self.kind, Kind::Read { .. }) {
            // The reference permits inline images up to a 4.5 MiB base64
            // payload; include room for the surrounding JSON and text.
            8 * 1024 * 1024
        } else {
            1024 * 1024
        }
    }

    async fn prepare_arguments(&self, arguments: Value) -> pi_rs::ExtensionResult<Value> {
        if matches!(self.kind, Kind::Edit) {
            crate::text_edit::prepare_arguments(arguments).map_err(edit_error_text)
        } else {
            Ok(arguments)
        }
    }
    async fn invoke(&self, args: Value, context: ToolContext) -> Outcome {
        let result = match self.kind {
            Kind::Read { .. } | Kind::Grep | Kind::Find | Kind::List => {
                let tool = self.clone();
                let cancellation = context.cancellation.child_token();
                let _guard = cancellation.clone().drop_guard();
                let worker_token = cancellation.clone();
                let worker = tokio::task::spawn_blocking(move || match tool.kind {
                    Kind::Read { .. } => tool.read(&args, &worker_token),
                    Kind::Grep | Kind::Find => tool.search(&args, &worker_token),
                    Kind::List => tool.list(&args, &worker_token),
                    _ => unreachable!("only local read operations run here"),
                });
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => Err(Error::Cancelled),
                    result = worker => result.unwrap_or_else(|error| Err(Error::Config(error.to_string()))),
                }
            }
            Kind::Write | Kind::Edit => {
                let tool = self.clone();
                let cancellation = context.cancellation.child_token();
                let _guard = cancellation.clone().drop_guard();
                tokio::task::spawn_blocking(move || {
                    if matches!(tool.kind, Kind::Write) {
                        tool.write_cancellable(&args, &cancellation)
                            .map(Outcome::Succeeded)
                    } else {
                        tool.edit_cancellable(&args, &cancellation)
                    }
                })
                .await
                .unwrap_or_else(|error| Err(Error::Config(error.to_string())))
            }
            Kind::Bash | Kind::PowerShell => return self.bash(&args, &context.cancellation).await,
        };
        match result {
            Ok(outcome) => outcome,
            Err(Error::Cancelled) => Outcome::Failed("Operation aborted".into()),
            Err(Error::Config(message))
                if self.unrestricted && matches!(self.kind, Kind::Grep | Kind::List) =>
            {
                Outcome::Failed(message)
            }
            Err(error) if matches!(self.kind, Kind::Edit) => {
                Outcome::Failed(edit_error_text(error))
            }
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
            return match self.shell(&args, &context.cancellation).await {
                Ok(result) => {
                    let elapsed = if self.unrestricted {
                        (result.elapsed * 10.0).round() / 10.0
                    } else {
                        result.elapsed
                    };
                    let mut data = json!({"output":result.output.script,"truncated":result.output.script_truncated,"exit_code":exit_code(result.status),"wall_time_seconds":elapsed});
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
            Outcome::Structured { data, attachments } if matches!(self.kind, Kind::Read { .. }) => {
                let mut image = data["content"]
                    .as_array()
                    .and_then(|blocks| blocks.iter().find(|block| block["type"] == "image"))
                    .cloned()
                    .unwrap_or_else(|| data.clone());
                if image["type"] == "image" {
                    let note = data["content"]
                        .as_array()
                        .and_then(|blocks| blocks.iter().find_map(|block| block["text"].as_str()))
                        .unwrap_or_default();
                    image["note"] = json!(note);
                }
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

fn directory_collator(locale: &str) -> Result<icu_collator::CollatorBorrowed<'static>> {
    let locale = locale
        .split(['.', '@'])
        .next()
        .unwrap_or_default()
        .replace('_', "-");
    let locale = locale
        .parse::<icu_locale_core::Locale>()
        .map(icu_collator::CollatorPreferences::from)
        .unwrap_or_default();
    icu_collator::Collator::try_new(locale, Default::default())
        .map_err(|error| Error::Config(format!("Cannot initialize directory collation: {error}")))
}

impl CodingTool {
    fn path(&self, value: &str) -> Result<PathBuf> {
        let input = crate::coding_paths::normalize(value)?;
        if !self.unrestricted
            && input
                .components()
                .any(|part| matches!(part, Component::ParentDir))
        {
            return Err(Error::Config("parent traversal is not supported".into()));
        }
        let path = crate::coding_paths::resolve(&self.root, &input);
        let path = if matches!(self.kind, Kind::Read { .. }) {
            crate::coding_paths::read_variant(path)
        } else {
            path
        };
        let resolved = path.canonicalize().map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound
                && matches!(self.kind, Kind::Grep | Kind::List)
            {
                Error::Config(format!("Path not found: {}", path.display()))
            } else {
                error.into()
            }
        })?;
        if !self.unrestricted && !resolved.starts_with(&self.root) {
            return Err(Error::Config("path is outside the workspace".into()));
        }
        Ok(resolved)
    }

    fn text(&self, path: &Path) -> Result<String> {
        let metadata = path.metadata()?;
        if !metadata.is_file() || (!self.unrestricted && metadata.len() > MAX_FILE_BYTES) {
            return Err(Error::Config(
                "expected a regular text file up to 4 MiB".into(),
            ));
        }
        Ok(std::fs::read_to_string(path)?)
    }

    fn read_image(&self, path: &Path) -> Result<Option<Value>> {
        if !path.is_file() {
            return Err(Error::Config("expected a regular file".into()));
        }
        let reader = image::ImageReader::open(path)?.with_guessed_format()?;
        let Some(format) = reader.format() else {
            return Ok(None);
        };
        if path.metadata()?.len() > 20 * 1024 * 1024 {
            return Err(Error::Config("image exceeds 20 MiB".into()));
        }
        if self.unrestricted
            && let Kind::Read {
                auto_resize: true,
                profile,
            } = self.kind
        {
            let original_mime = format.to_mime_type();
            let prepared = crate::image_profile::normalize(reader, profile.unwrap_or_default())?;
            let mut note = format!("Read image file [{}]", prepared.mime_type);
            if !matches!(
                original_mime,
                "image/png" | "image/jpeg" | "image/gif" | "image/webp"
            ) {
                note.push_str(&format!(
                    "\n[Image converted from {original_mime} to {}.]",
                    prepared.mime_type
                ));
            }
            if prepared.source != prepared.output {
                let x_scale = format!("{:.2}", prepared.source.0 as f64 / prepared.output.0 as f64);
                let y_scale = format!("{:.2}", prepared.source.1 as f64 / prepared.output.1 as f64);
                let mapping = if x_scale == y_scale {
                    format!("Multiply coordinates by {x_scale}")
                } else {
                    format!("Multiply x coordinates by {x_scale} and y coordinates by {y_scale}")
                };
                note.push_str(&format!(
                    "\n[Image: original {}x{}, displayed at {}x{}. {mapping} to map to original image.]",
                    prepared.source.0, prepared.source.1, prepared.output.0, prepared.output.1
                ));
            }
            return Ok(Some(json!({"content":[
                {"type":"text", "text":note},
                {"type":"image", "mimeType":prepared.mime_type, "data":base64::engine::general_purpose::STANDARD.encode(prepared.bytes)}
            ]})));
        }
        let (bytes, mime_type, source, output) = if matches!(
            self.kind,
            Kind::Read {
                auto_resize: false,
                ..
            }
        ) {
            let mime_type = format.to_mime_type();
            if !matches!(
                mime_type,
                "image/png" | "image/jpeg" | "image/gif" | "image/webp"
            ) {
                return Err(Error::Config("unsupported image format".into()));
            }
            let mut bytes = Vec::new();
            reader
                .into_inner()
                .take((MAX_ORIGINAL_IMAGE_BYTES + 1) as u64)
                .read_to_end(&mut bytes)?;
            if bytes.len() > MAX_ORIGINAL_IMAGE_BYTES {
                return Err(Error::Config(
                    "image exceeds the 6 MiB original image limit".into(),
                ));
            }
            let dimensions = image::ImageReader::with_format(Cursor::new(&bytes), format)
                .into_dimensions()
                .map_err(|error| Error::Config(format!("cannot decode image: {error}")))?;
            (bytes, mime_type, dimensions, dimensions)
        } else if let Kind::Read {
            profile: Some(profile),
            ..
        } = self.kind
        {
            let prepared = crate::image_profile::normalize(reader, profile)?;
            (
                prepared.bytes,
                prepared.mime_type,
                prepared.source,
                prepared.output,
            )
        } else {
            let prepared = normalize_image(reader)?;
            (
                prepared.png,
                "image/png",
                (prepared.source_width, prepared.source_height),
                (prepared.width, prepared.height),
            )
        };
        let note = if self.unrestricted {
            format!("Read image file [{mime_type}]")
        } else {
            format!(
                "Read image {} ({}×{}, provided at {}×{})",
                path.display(),
                source.0,
                source.1,
                output.0,
                output.1
            )
        };
        Ok(Some(json!({"content":[
            {"type":"text","text":note},
            {"type":"image","mimeType":mime_type,"data":base64::engine::general_purpose::STANDARD.encode(bytes)}
        ]})))
    }

    fn read(&self, args: &Value, cancellation: &pi_rs::CancellationToken) -> Result<Outcome> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let display_path = string(args, "path")?;
        let path = self.path(display_path)?;
        if let Some(data) = self.read_image(&path)? {
            return Ok(Outcome::Structured {
                data,
                attachments: vec![],
            });
        }
        let offset = usize::try_from(args["offset"].as_u64().unwrap_or(1).saturating_sub(1))
            .unwrap_or(usize::MAX);
        let limit = args["limit"]
            .as_u64()
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));
        crate::text_page::read(&path, display_path, offset, limit, cancellation)
    }

    #[cfg(test)]
    fn write(&self, args: &Value) -> Result<String> {
        self.write_cancellable(args, &pi_rs::CancellationToken::new())
    }

    fn write_cancellable(
        &self,
        args: &Value,
        cancellation: &pi_rs::CancellationToken,
    ) -> Result<String> {
        check_mutation_cancelled(cancellation)?;
        let content = string(args, "content")?;
        if !self.unrestricted && content.len() as u64 > MAX_FILE_BYTES {
            return Err(Error::Config("content exceeds 4 MiB".into()));
        }
        let display_path = string(args, "path")?;
        let input = crate::coding_paths::normalize(display_path)?;
        let target = crate::coding_paths::resolve(&self.root, &input);
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
        check_mutation_cancelled(cancellation)?;
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
        let _lock = mutation_lock(&target, cancellation)?;
        check_mutation_cancelled(cancellation)?;
        atomic_write(&target, content.as_bytes(), cancellation)?;
        check_mutation_cancelled(cancellation)?;
        Ok(format!("Successfully wrote to {display_path}"))
    }

    #[cfg(test)]
    fn edit(&self, args: &Value) -> Result<Outcome> {
        self.edit_cancellable(args, &pi_rs::CancellationToken::new())
    }

    fn edit_cancellable(
        &self,
        args: &Value,
        cancellation: &pi_rs::CancellationToken,
    ) -> Result<Outcome> {
        check_mutation_cancelled(cancellation)?;
        let path = self.path(string(args, "path")?)?;
        let _lock = mutation_lock(&path, cancellation)?;
        check_mutation_cancelled(cancellation)?;
        let original = self.text(&path)?;
        let (output, count) = crate::text_edit::apply(&original, args)?;
        if !self.unrestricted && output.len() as u64 > MAX_FILE_BYTES {
            return Err(Error::Config("edited file exceeds 4 MiB".into()));
        }
        if self.text(&path)? != original {
            return Err(Error::Config(
                "file changed while preparing edits; read it again".into(),
            ));
        }
        check_mutation_cancelled(cancellation)?;
        atomic_write(&path, output.as_bytes(), cancellation)?;
        check_mutation_cancelled(cancellation)?;
        let display_path = args["path"].as_str().unwrap_or_default();
        let original = crate::text_edit::normalized_content(&original);
        let edited = crate::text_edit::normalized_content(&output);
        let diff = similar::TextDiff::from_lines(&original, &edited);
        let first_changed_line = diff
            .ops()
            .iter()
            .find(|op| !matches!(op, similar::DiffOp::Equal { .. }))
            .map(|op| op.new_range().start + 1);
        let digits = original
            .split('\n')
            .count()
            .max(edited.split('\n').count())
            .to_string()
            .len();
        let display = crate::text_edit::numbered_diff(&diff, digits);
        let patch = diff
            .unified_diff()
            .context_radius(4)
            .header(display_path, display_path)
            .to_string();
        Ok(Outcome::SucceededWithDetails {
            output: format!("Successfully replaced {count} block(s) in {display_path}."),
            details: json!({"diff":bounded(display),"patch":bounded(patch),"firstChangedLine":first_changed_line}),
        })
    }

    fn list(&self, args: &Value, cancellation: &pi_rs::CancellationToken) -> Result<Outcome> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let path = self.path(args["path"].as_str().unwrap_or("."))?;
        let limit = args["limit"].as_u64().unwrap_or(500).min(100_000) as usize;
        if !path.is_dir() {
            return Err(Error::Config(format!(
                "Not a directory: {}",
                path.display()
            )));
        }
        let directory = std::fs::read_dir(&path)
            .map_err(|error| Error::Config(format!("Cannot read directory: {error}")))?;
        let mut entries = Vec::new();
        for entry in directory {
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let entry = entry?;
            entries.push((entry.file_name().to_string_lossy().to_lowercase(), entry));
        }
        let locale = ["LC_ALL", "LANG"]
            .into_iter()
            .filter_map(|key| std::env::var(key).ok())
            .find(|value| !value.is_empty())
            .unwrap_or_default();
        let collator = directory_collator(&locale)?;
        entries.sort_by(|(left, _), (right, _)| collator.compare(left, right));
        let mut results = Vec::new();
        let mut entry_limit_reached = false;
        for (_, entry) in entries {
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            if results.len() >= limit {
                entry_limit_reached = true;
                break;
            }
            if let Ok(metadata) = std::fs::metadata(entry.path()) {
                results.push(format!(
                    "{}{}",
                    entry.file_name().to_string_lossy(),
                    if metadata.is_dir() { "/" } else { "" }
                ));
            }
        }
        if results.is_empty() {
            return Ok(Outcome::Succeeded("(empty directory)".into()));
        }
        let mut details = Map::new();
        let mut notices = Vec::new();
        if entry_limit_reached {
            details.insert("entryLimitReached".into(), json!(limit));
            notices.push(format!(
                "{limit} entries limit reached. Use limit={} for more",
                limit.saturating_mul(2)
            ));
        }
        Ok(crate::tool_output::listing(
            results.join("\n"),
            details,
            notices,
        ))
    }

    fn search(&self, args: &Value, cancellation: &pi_rs::CancellationToken) -> Result<Outcome> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let base = self.path(args["path"].as_str().unwrap_or("."))?;
        let is_find = matches!(self.kind, Kind::Find);
        let limit = args["limit"]
            .as_u64()
            .unwrap_or(if is_find { 1000 } else { 100 })
            .min(100_000) as usize;
        let limit = if is_find { limit } else { limit.max(1) };
        let pattern = string(args, "pattern")?;
        let glob = if is_find {
            let pattern = if pattern.contains('/')
                && !pattern.starts_with('/')
                && !pattern.starts_with("**/")
            {
                format!("**/{pattern}")
            } else {
                pattern.to_owned()
            };
            Some(
                globset::GlobBuilder::new(&pattern)
                    .case_insensitive(!pattern.chars().any(char::is_uppercase))
                    .literal_separator(true)
                    .build()
                    .map_err(|error| Error::Config(error.to_string()))?
                    .compile_matcher(),
            )
        } else {
            None
        };
        let mut grep = if is_find {
            None
        } else {
            Some(crate::file_search::Grep::new(args, limit)?)
        };
        let mut results = Vec::new();
        let mut walker = ignore::WalkBuilder::new(&base);
        walker.hidden(false);
        if is_find {
            if !base.is_dir() {
                return Err(Error::Config(format!(
                    "Not a directory: {}",
                    base.display()
                )));
            }
            let inside_git = base.ancestors().any(|path| path.join(".git").exists());
            walker
                .require_git(inside_git)
                .add_custom_ignore_filename(".fdignore")
                .filter_entry(|entry| {
                    entry.depth() == 0
                        || entry.file_name() != ".git"
                        || !entry.file_type().is_some_and(|kind| kind.is_dir())
                })
                .sort_by_file_path(std::path::Path::cmp);
        } else if let Some(glob) = args["glob"].as_str() {
            let mut overrides = ignore::overrides::OverrideBuilder::new(&self.root);
            let overrides = overrides
                .add(glob)
                .and_then(|builder| builder.build())
                .map_err(|error| Error::Config(error.to_string()))?;
            walker.overrides(overrides);
        }
        for entry in walker.build() {
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let entry = entry.map_err(|error| Error::Config(error.to_string()))?;
            if (is_find && entry.depth() == 0)
                || (!is_find && !entry.file_type().is_some_and(|kind| kind.is_file()))
            {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(&base)
                .ok()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(entry.file_name()));
            let match_path = if pattern.contains('/') {
                entry.path()
            } else {
                Path::new(entry.file_name())
            };
            if glob.as_ref().is_some_and(|glob| !glob.is_match(match_path)) {
                continue;
            }
            if let Some(grep) = &mut grep {
                if !grep.search(entry.path(), &relative.to_string_lossy(), cancellation)? {
                    break;
                }
            } else {
                let mut path = relative
                    .to_string_lossy()
                    .replace(std::path::MAIN_SEPARATOR, "/");
                if entry.file_type().is_some_and(|kind| kind.is_dir()) {
                    path.push('/');
                }
                results.push(path);
                if results.len() >= limit {
                    break;
                }
            }
        }
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        Ok(match grep {
            Some(grep) => grep.finish(),
            None => {
                if results.is_empty() {
                    return Ok(Outcome::Succeeded("No files found matching pattern".into()));
                }
                results.sort();
                let mut details = Map::new();
                let mut notices = Vec::new();
                if results.len() >= limit {
                    details.insert("resultLimitReached".into(), json!(limit));
                    notices.push(format!(
                        "{limit} results limit reached. Use limit={} for more, or refine pattern",
                        limit.saturating_mul(2)
                    ));
                }
                crate::tool_output::listing(results.join("\n"), details, notices)
            }
        })
    }

    async fn bash(&self, args: &Value, cancellation: &pi_rs::CancellationToken) -> Outcome {
        let result = match self.shell(args, cancellation).await {
            Ok(result) => result,
            Err(Error::Config(message)) if self.unrestricted => return Outcome::Failed(message),
            Err(error) => return Outcome::Failed(error.to_string()),
        };
        if !self.unrestricted {
            let output = format!("{}\nExit status: {}", result.output.display, result.status);
            return if result.status.success() {
                Outcome::Succeeded(output)
            } else {
                Outcome::Failed(Error::Config(output).to_string())
            };
        }
        let output = if result.output.display.is_empty() {
            "(no output)".into()
        } else {
            result.output.display
        };
        match exit_code(result.status) {
            0 => match result.output.details {
                Some(details) => Outcome::SucceededWithDetails { output, details },
                None => Outcome::Succeeded(output),
            },
            code => Outcome::Failed(shell_status(
                &output,
                &format!("Command exited with code {code}"),
            )),
        }
    }

    async fn shell(
        &self,
        args: &Value,
        cancellation: &pi_rs::CancellationToken,
    ) -> Result<ShellResult> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let result = self
            .shell_stream(args, cancellation, &mut |_| {}, false)
            .await?;
        if result.timed_out {
            let status = format!(
                "Command timed out after {} seconds",
                shell_timeout(args)?.expect("elapsed timeout").as_secs_f64()
            );
            return Err(Error::Config(shell_status(&result.output.display, &status)));
        }
        if result.cancelled {
            return Err(Error::Config(shell_status(
                &result.output.display,
                "Command aborted",
            )));
        }
        Ok(result)
    }
    async fn shell_stream(
        &self,
        args: &Value,
        cancellation: &pi_rs::CancellationToken,
        progress: &mut (dyn FnMut(&str) + Send),
        sanitize_user_output: bool,
    ) -> Result<ShellResult> {
        let started = std::time::Instant::now();
        let timeout = shell_timeout(args)?;
        let crate::shell_config::Launch {
            mut command,
            input,
            cwd,
        } = self.shell.launch(
            &self.root,
            matches!(self.kind, Kind::PowerShell),
            self.unrestricted,
            string(args, "command")?,
            !sanitize_user_output,
        )?;
        command
            .current_dir(cwd)
            .stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let mut process = ProcessGroup::spawn(command)?;
        let input_task = input.map(|input| {
            let mut stdin = process.stdin();
            tokio::spawn(async move {
                let _ = stdin.write_all(input.as_bytes()).await;
            })
        });
        let mut stdout = process.stdout();
        let mut stderr = process.stderr();
        let deadline = async {
            match timeout {
                Some(timeout) => tokio::time::sleep(timeout).await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(deadline);
        let mut output = if sanitize_user_output {
            crate::shell_output::Accumulator::for_user_shell()
        } else {
            crate::shell_output::Accumulator::default()
        };
        let mut stdout_open = true;
        let mut stderr_open = true;
        let mut status = None;
        let mut stdout_chunk = [0; 8192];
        let mut stderr_chunk = [0; 8192];
        let mut drain_deadline = None;
        let mut cancelled = false;
        let mut timed_out = false;
        let mut stdout_text = ShellStream::default();
        let mut stderr_text = ShellStream::default();
        let mut stdout_filter = UserShellFilter::default();
        while status.is_none() || stdout_open || stderr_open {
            tokio::select! {
                _ = cancellation.cancelled(), if !cancelled => {
                    cancelled = true;
                    process.terminate();
                }
                _ = &mut deadline, if !cancelled => {
                    timed_out = true;
                    cancelled = true;
                    process.terminate();
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
                    if sanitize_user_output {
                        output.record_raw_bytes(count);
                        let mut clean = String::new();
                        stdout_text.push(&stdout_chunk[..count], &mut |text| clean.push_str(&stdout_filter.filter(text)));
                        if !clean.is_empty() {
                            progress(&clean);
                            output.append(clean.as_bytes())?;
                        }
                    } else {
                        output.append(&stdout_chunk[..count])?;
                        stdout_text.push(&stdout_chunk[..count], progress);
                    }
                }
                count = stderr.read(&mut stderr_chunk), if stderr_open => {
                    let count = count?;
                    stderr_open = count != 0;
                    if sanitize_user_output {
                        output.record_raw_bytes(count);
                        let mut clean = String::new();
                        stdout_text.push(&stderr_chunk[..count], &mut |text| clean.push_str(&stdout_filter.filter(text)));
                        if !clean.is_empty() {
                            progress(&clean);
                            output.append(clean.as_bytes())?;
                        }
                    } else {
                        output.append(&stderr_chunk[..count])?;
                        stderr_text.push(&stderr_chunk[..count], progress);
                    }
                }
                _ = async {
                    match drain_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => break,
            }
        }
        if let Some(input_task) = input_task {
            input_task.abort();
        }
        if sanitize_user_output {
            let mut remaining = String::new();
            stdout_text.finish(&mut |text| remaining.push_str(&stdout_filter.filter(text)));
            remaining.push_str(&stdout_filter.finish());
            if !remaining.is_empty() {
                progress(&remaining);
                output.append(remaining.as_bytes())?;
            }
        } else {
            stdout_text.finish(progress);
            stderr_text.finish(progress);
        }
        Ok(ShellResult {
            cancelled,
            timed_out,
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
    prepare_image(original)
}

/// Normalize a decoded image under the same limits as file and clipboard input.
pub fn prepare_image(original: image::DynamicImage) -> Result<PreparedImage> {
    prepare_image_with_resize(original, true)
}

/// Encode clipboard pixels with or without resizing.
pub fn prepare_image_with_resize(
    original: image::DynamicImage,
    resize: bool,
) -> Result<PreparedImage> {
    let source_width = original.width();
    let source_height = original.height();
    let mut resized = if resize && (source_width > 2000 || source_height > 2000) {
        original.thumbnail(2000, 2000)
    } else {
        original
    };
    let limit = if resize {
        512 * 1024
    } else {
        MAX_ORIGINAL_IMAGE_BYTES
    };
    let mut png = Vec::new();
    loop {
        png.clear();
        resized
            .write_to(&mut Cursor::new(&mut png), image::ImageFormat::Png)
            .map_err(|error| Error::Config(error.to_string()))?;
        if png.len() <= limit {
            break;
        }
        if !resize {
            return Err(Error::Config(
                "image exceeds the 6 MiB original image limit".into(),
            ));
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
    cancelled: bool,
    timed_out: bool,
    output: crate::shell_output::Output,
    status: std::process::ExitStatus,
    elapsed: f64,
}

struct UserShellFilter {
    pending: String,
    at_start: bool,
}

impl Default for UserShellFilter {
    fn default() -> Self {
        Self {
            pending: String::new(),
            at_start: true,
        }
    }
}

impl UserShellFilter {
    fn filter(&mut self, text: &str) -> String {
        let text = if self.at_start && !text.is_empty() {
            self.at_start = false;
            text.strip_prefix('\u{feff}').unwrap_or(text)
        } else {
            text
        };
        static PENDING: std::sync::OnceLock<fancy_regex::Regex> = std::sync::OnceLock::new();
        let value = format!("{}{text}", self.pending);
        self.pending.clear();
        if value.contains(['\u{1b}', '\u{9b}']) {
            let mut units = 0;
            let mut window = value.len();
            for (index, ch) in value.char_indices().rev() {
                if units + ch.len_utf16() > 256 {
                    break;
                }
                units += ch.len_utf16();
                window = index;
            }
            let pending = PENDING.get_or_init(|| fancy_regex::Regex::new(
                r"(?:\x1b\](?:[^\x07\u009c\x1b]|\x1b(?!\\))*|(?:(?:\x1b\[|\u009b)[<=>]|[\x1b\u009b][\[\]()#;?]*)(?:[0-9]{1,4}(?:[;:][0-9]{0,4})*)?)$"
            ).expect("valid ANSI suffix pattern"));
            if let Ok(Some(found)) = pending.find(&value[window..]) {
                let start = window + found.start();
                self.pending.push_str(&value[start..]);
                return sanitize_user_shell_text(&value[..start]);
            }
        }
        sanitize_user_shell_text(&value)
    }

    fn finish(&mut self) -> String {
        sanitize_user_shell_text(&std::mem::take(&mut self.pending))
    }
}

// ANSI patterns follow Pi's ansi-regex-based implementation, MIT-licensed by Sindre Sorhus.
fn sanitize_user_shell_text(text: &str) -> String {
    static ANSI: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let ansi = ANSI.get_or_init(|| regex::Regex::new(
        r"(?:\x1b\][\s\S]*?(?:\x07|\x1b\\|\u009c))|(?:(?:\x1b\[|\u009b)[<=>]|[\x1b\u009b][\[\]()#;?]*)(?:[0-9]{1,4}(?:[;:][0-9]{0,4})*)?[0-9A-PR-TZcf-nq-uy=><~]"
    ).expect("valid ANSI pattern"));
    ansi.replace_all(text, "")
        .chars()
        .filter(
            |ch| !matches!(*ch, '\u{0}'..='\u{8}' | '\u{b}'..='\u{1f}' | '\u{fff9}'..='\u{fffb}'),
        )
        .collect()
}

#[derive(Default)]
struct ShellStream(Vec<u8>);
impl ShellStream {
    fn push(&mut self, bytes: &[u8], progress: &mut (dyn FnMut(&str) + Send)) {
        self.0.extend_from_slice(bytes);
        while !self.0.is_empty() {
            let (valid, invalid) = match std::str::from_utf8(&self.0) {
                Ok(text) => {
                    progress(text);
                    self.0.clear();
                    return;
                }
                Err(error) => (error.valid_up_to(), error.error_len()),
            };
            if valid > 0 {
                progress(std::str::from_utf8(&self.0[..valid]).expect("valid UTF-8 prefix"));
                self.0.drain(..valid);
            }
            match invalid {
                Some(count) => {
                    progress("�");
                    self.0.drain(..count);
                }
                None => break,
            }
        }
    }
    fn finish(&mut self, progress: &mut (dyn FnMut(&str) + Send)) {
        if !self.0.is_empty() {
            progress(&String::from_utf8_lossy(&self.0));
            self.0.clear();
        }
    }
}

fn string<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value[name]
        .as_str()
        .ok_or_else(|| Error::Config(format!("{name} must be a string")))
}

fn mutation_lock(path: &Path, cancellation: &pi_rs::CancellationToken) -> Result<std::fs::File> {
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
    loop {
        check_mutation_cancelled(cancellation)?;
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) => {
                std::thread::sleep(std::time::Duration::from_millis(20))
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }
    }
}

fn check_mutation_cancelled(cancellation: &pi_rs::CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(Error::Cancelled)
    } else {
        Ok(())
    }
}

fn atomic_write(
    path: &Path,
    content: &[u8],
    cancellation: &pi_rs::CancellationToken,
) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::Config("file needs a parent".into()))?;
    #[cfg(unix)]
    let mut builder = tempfile::Builder::new();
    #[cfg(not(unix))]
    let builder = tempfile::Builder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(std::fs::Permissions::from_mode(0o666));
    }
    let mut file = builder.tempfile_in(parent)?;
    if let Ok(metadata) = path.metadata() {
        file.as_file().set_permissions(metadata.permissions())?;
    }
    file.write_all(content)?;
    file.as_file().sync_all()?;
    check_mutation_cancelled(cancellation)?;
    file.persist(path).map_err(|error| Error::Io(error.error))?;
    #[cfg(unix)]
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn edit_error_text(error: Error) -> String {
    match error {
        Error::Config(message) => message,
        error => error.to_string(),
    }
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

fn shell_status(output: &str, status: &str) -> String {
    if output.is_empty() {
        status.to_owned()
    } else {
        format!("{output}\n\n{status}")
    }
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
    fn terminate(&mut self) {
        #[cfg(unix)]
        if let Some(id) = self.group {
            unsafe {
                libc::kill(-(id as i32), libc::SIGKILL);
            }
        }
        let _ = self.child.start_kill();
    }
    fn stdin(&mut self) -> tokio::process::ChildStdin {
        #[cfg(windows)]
        {
            self.child.stdin().take().expect("piped stdin")
        }
        #[cfg(not(windows))]
        {
            self.child.stdin.take().expect("piped stdin")
        }
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
            self.terminate();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn model_shell_spawn_hook_changes_command_directory_and_environment() {
        let root = tempfile::tempdir().unwrap();
        let subdir = root.path().join("nested");
        std::fs::create_dir(&subdir).unwrap();
        let subdir = subdir.canonicalize().unwrap();
        let target = subdir.clone();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = calls.clone();
        let config = ShellConfig::new(None, Some("export PI_TEST_PREFIX=prefix")).with_spawn_hook(
            Arc::new(move |mut context| {
                observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                assert!(
                    context
                        .command
                        .starts_with("export PI_TEST_PREFIX=prefix\n")
                );
                assert_eq!(
                    context.env.get(std::ffi::OsStr::new("PI_MODEL")).unwrap(),
                    "model-fixture"
                );
                assert!(
                    std::env::split_paths(context.env.get(std::ffi::OsStr::new("PATH")).unwrap())
                        .any(|path| path == crate::agent_dir().join("bin"))
                );
                context.command.push_str("; printf '|rewritten'");
                context.cwd = target.clone();
                context.env.insert("PI_TEST_HOOK".into(), "hooked".into());
                Ok(context)
            }),
        );
        config.set_context(ShellContext {
            id: uuid::Uuid::nil(),
            path: None,
            provider: "fixture".into(),
            model: "model-fixture".into(),
            reasoning_level: Some("low".into()),
        });
        let tool = CodingTool {
            root: root.path().into(),
            unrestricted: true,
            shell: config.clone(),
            kind: Kind::Bash,
        };
        let result = tool.shell(&json!({"command":"printf '%s|%s|%s|%s' \"$PI_TEST_PREFIX\" \"$PI_TEST_HOOK\" \"$PI_MODEL\" \"$PWD\""}), &pi_rs::CancellationToken::new()).await.unwrap();
        assert_eq!(
            result.output.script,
            format!("prefix|hooked|model-fixture|{}|rewritten", subdir.display())
        );
        let user = CodingTools::local(root.path())
            .unwrap()
            .with_shell_config(config);
        let result = user
            .execute_shell(
                "printf '%s' \"$PI_TEST_PREFIX\"",
                None,
                &pi_rs::CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        assert_eq!(result.output, "prefix");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn model_shell_can_disable_session_metadata() {
        let root = tempfile::tempdir().unwrap();
        let config = ShellConfig::default()
            .with_session_environment(false)
            .with_spawn_hook(Arc::new(|context| {
                assert!(
                    !context
                        .env
                        .contains_key(std::ffi::OsStr::new("PI_SESSION_ID"))
                );
                assert!(!context.env.contains_key(std::ffi::OsStr::new("PI_MODEL")));
                Ok(context)
            }));
        config.set_context(ShellContext {
            id: uuid::Uuid::nil(),
            path: None,
            provider: "fixture".into(),
            model: "model-fixture".into(),
            reasoning_level: None,
        });
        let tool = CodingTool {
            root: root.path().into(),
            unrestricted: true,
            shell: config,
            kind: Kind::Bash,
        };
        let result = tool
            .shell(
                &json!({"command":"printf '%s' \"${PI_MODEL-unset}\""}),
                &pi_rs::CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(result.output.script, "unset");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failing_shell_spawn_hook_stops_execution() {
        let root = tempfile::tempdir().unwrap();
        let config = ShellConfig::default()
            .with_spawn_hook(Arc::new(|_| Err(Error::Config("blocked by hook".into()))));
        let tool = CodingTool {
            root: root.path().into(),
            unrestricted: true,
            shell: config,
            kind: Kind::Bash,
        };
        let result = tool
            .shell(
                &json!({"command":"touch should-not-exist"}),
                &pi_rs::CancellationToken::new(),
            )
            .await;
        assert!(matches!(result, Err(Error::Config(message)) if message == "blocked by hook"));
        assert!(!root.path().join("should-not-exist").exists());
    }

    struct StreamingShell;
    #[async_trait::async_trait]
    impl pi_rs::Extension for StreamingShell {
        fn id(&self) -> &str {
            "streaming-shell"
        }
        async fn execute_user_bash(
            &self,
            event: &pi_rs::UserBashEvent,
            _: Option<&pi_rs::ExtensionState>,
            _: Option<Duration>,
            cancel: pi_rs::CancellationToken,
            output: tokio::sync::mpsc::Sender<Vec<u8>>,
        ) -> pi_rs::ExtensionResult<Option<i32>> {
            if event.command == "large" {
                output
                    .send(vec![b'x'; 60_000])
                    .await
                    .map_err(|error| error.to_string())?;
                return Ok(Some(0));
            }
            for chunk in [
                b"\x1b[31m\xe6".as_slice(),
                b"\x97\xa5\xe6\x9c\xac\x1b[0m\r\n".as_slice(),
            ] {
                output
                    .send(chunk.to_vec())
                    .await
                    .map_err(|error| error.to_string())?;
            }
            if event.command == "wait" {
                cancel.cancelled().await;
            }
            Ok(Some(7))
        }
    }

    #[tokio::test]
    async fn user_shell_operations_stream_sanitized_utf8_and_keep_output_on_timeout() {
        let directory = tempfile::tempdir().unwrap();
        let tools = CodingTools::local(directory.path()).unwrap();
        let mut event = pi_rs::UserBashEvent {
            command: "remote".into(),
            exclude_from_context: true,
            cwd: directory.path().into(),
        };
        let mut streamed = String::new();
        let execution = tools
            .execute_shell_operations(
                &event,
                &StreamingShell,
                None,
                None,
                &pi_rs::CancellationToken::new(),
                &mut |chunk| streamed.push_str(chunk),
            )
            .await
            .unwrap();
        assert_eq!(execution.output, "日本\n");
        assert_eq!(streamed, execution.output);
        assert_eq!(execution.exit_code, Some(7));
        assert!(execution.exclude_from_context);
        event.command = "wait".into();
        let execution = tools
            .execute_shell_operations(
                &event,
                &StreamingShell,
                None,
                Some(Duration::from_millis(30)),
                &pi_rs::CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        assert!(execution.cancelled);
        assert!(execution.output.starts_with("日本\n"));
        assert!(execution.output.contains("Command timed out"));
        event.command = "large".into();
        let execution = tools
            .execute_shell_operations(
                &event,
                &StreamingShell,
                None,
                None,
                &pi_rs::CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        assert!(execution.truncated);
        let path = execution.full_output_path.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), vec![b'x'; 60_000]);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn local_file_queries_report_missing_paths_and_cancellation() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        std::fs::write(root.join("file.txt"), "match\nmatch\n").unwrap();
        let mut tool = CodingTool {
            root: root.clone(),
            unrestricted: true,
            shell: ShellConfig::default(),
            kind: Kind::Grep,
        };
        let cancellation = pi_rs::CancellationToken::new();
        let context = || ToolContext {
            operation_id: uuid::Uuid::nil(),
            cancellation: cancellation.clone(),
        };
        let output = tool
            .invoke(json!({"pattern": "match", "limit": 0}), context())
            .await
            .tool_content();
        assert!(output.starts_with("file.txt:1: match\n"));
        assert!(!output.contains("file.txt:2:"));
        for kind in [Kind::Grep, Kind::List] {
            tool.kind = kind;
            let output = tool
                .invoke(json!({"pattern": "match", "path": "missing"}), context())
                .await;
            assert!(matches!(output, Outcome::Failed(message)
                    if message == format!("Path not found: {}", root.join("missing").display())));
        }
        cancellation.cancel();
        tool.kind = Kind::Read {
            auto_resize: true,
            profile: None,
        };
        assert!(matches!(
            tool.invoke(json!({"path":"file.txt"}), context()).await,
            Outcome::Failed(message) if message == "Operation aborted"
        ));
        for kind in [Kind::Grep, Kind::Find, Kind::List] {
            tool.kind = kind;
            let output = tool.invoke(json!({"pattern": "match"}), context()).await;
            assert!(matches!(output, Outcome::Failed(message) if message == "Operation aborted"));
        }
    }

    #[test]
    fn user_shell_stream_removes_only_the_initial_utf8_bom() {
        let mut stream = ShellStream::default();
        let mut filter = UserShellFilter::default();
        let mut text = String::new();
        for byte in "\u{feff}first\u{feff}second".as_bytes() {
            stream.push(&[*byte], &mut |part| text.push_str(&filter.filter(part)));
        }
        stream.finish(&mut |part| text.push_str(&filter.filter(part)));
        text.push_str(&filter.finish());
        assert_eq!(text, "first\u{feff}second");
    }

    #[test]
    fn user_shell_filter_handles_fragmented_escape_sequences() {
        let mut filter = UserShellFilter::default();
        let output = [
            "A\u{1b}[",
            "31mred\u{1b}[0m\r\n",
            "\u{1b}]0;title",
            "\u{7}ok\u{0}\u{fff9}",
        ]
        .into_iter()
        .map(|part| filter.filter(part))
        .collect::<String>();
        assert_eq!(output, "Ared\nok");
    }

    #[test]
    fn user_shell_filter_flushes_incomplete_escapes_and_preserves_delete() {
        let mut filter = UserShellFilter::default();
        assert_eq!(filter.filter("x\u{1b}[<35;59"), "x");
        assert_eq!(filter.finish(), "");
        assert_eq!(filter.filter("x\u{7f}y"), "x\u{7f}y");
        assert_eq!(filter.filter("x\u{1b}]title"), "x");
        assert_eq!(filter.finish(), "itle");
    }

    #[test]
    fn shell_updates_preserve_multibyte_boundaries() {
        let mut decoder = ShellStream::default();
        let mut text = String::new();
        let mut output = |chunk: &str| text.push_str(chunk);
        for byte in "日本語".as_bytes() {
            decoder.push(&[*byte], &mut output);
        }
        decoder.finish(&mut output);
        assert_eq!(text, "日本語");
    }
    #[tokio::test]
    async fn explicit_shell_preserves_failed_output_and_cancelled_processes() {
        let directory = tempfile::tempdir().unwrap();
        let tools = CodingTools::local(directory.path()).unwrap();
        if !cfg!(windows) {
            let mut streamed = String::new();
            let result = tools
                .execute_shell(
                    "printf '\\033[31mred\\033[0m\\r\\n'",
                    None,
                    &pi_rs::CancellationToken::new(),
                    &mut |chunk| streamed.push_str(chunk),
                )
                .await
                .unwrap();
            assert_eq!(result.output, "red\n");
            assert_eq!(streamed, result.output);
        }
        if !cfg!(windows) {
            let result = tools
                .execute_shell(
                    "head -c 52000 /dev/zero; printf hello",
                    None,
                    &pi_rs::CancellationToken::new(),
                    &mut |_| {},
                )
                .await
                .unwrap();
            assert_eq!(result.output, "hello");
            assert!(!result.truncated);
            let path = result.full_output_path.unwrap();
            assert_eq!(std::fs::read_to_string(&path).unwrap(), "hello");
            std::fs::remove_file(path).unwrap();
        }
        if !cfg!(windows) {
            for (command, expected) in [
                ("printf '\\357\\273\\277first'", "first"),
                ("printf '\\000\\357\\273\\277second'", "\u{feff}second"),
            ] {
                let mut streamed = String::new();
                let result = tools
                    .execute_shell(
                        command,
                        None,
                        &pi_rs::CancellationToken::new(),
                        &mut |part| streamed.push_str(part),
                    )
                    .await
                    .unwrap();
                assert_eq!(streamed, expected);
                assert_eq!(result.output, expected);
            }
        }
        for command in ["", "  "] {
            let result = tools
                .execute_shell(command, None, &pi_rs::CancellationToken::new(), &mut |_| {})
                .await
                .unwrap();
            assert_eq!(result.exit_code, Some(0));
            assert!(result.output.is_empty());
        }
        let command = if cfg!(windows) {
            "Write-Output 'failed'; exit 7"
        } else {
            "printf failed; exit 7"
        };
        let result = tools
            .execute_shell(command, None, &pi_rs::CancellationToken::new(), &mut |_| {})
            .await
            .unwrap();
        assert_eq!(result.exit_code, Some(7));
        assert!(result.output.contains("failed"));
        let cancellation = pi_rs::CancellationToken::new();
        let cancel = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            cancel.cancel();
        });
        let command = if cfg!(windows) {
            "Start-Sleep -Seconds 10"
        } else {
            "sleep 10"
        };
        let result = tools
            .execute_shell(command, None, &cancellation, &mut |_| {})
            .await
            .unwrap();
        assert!(result.cancelled);
        let result = tools
            .execute_shell(
                command,
                Some(Duration::from_millis(10)),
                &pi_rs::CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        assert!(result.cancelled);
        assert!(result.output.contains("Command timed out"));
    }

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
                        shell: ShellConfig::default(),
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
            let Outcome::SucceededWithDetails { output, details } = handle.join().unwrap() else {
                panic!("expected edit details");
            };
            assert_eq!(output, "Successfully replaced 1 block(s) in file.txt.");
            assert!(details["patch"].as_str().unwrap().contains("@@"));
        }
        assert_eq!(
            std::fs::read_to_string(root.join("file.txt")).unwrap(),
            "A B\n"
        );
    }

    #[test]
    fn local_image_reads_preserve_supported_bytes_and_report_resized_coordinates() {
        let directory = tempfile::tempdir().unwrap();
        let small = directory.path().join("small.jpg");
        image::RgbImage::from_pixel(32, 24, image::Rgb([12, 34, 56]))
            .save(&small)
            .unwrap();
        let wide = directory.path().join("wide.png");
        image::RgbImage::from_pixel(2100, 42, image::Rgb([12, 34, 56]))
            .save(&wide)
            .unwrap();
        let tool = CodingTool {
            root: directory.path().canonicalize().unwrap(),
            unrestricted: true,
            shell: ShellConfig::default(),
            kind: Kind::Read {
                auto_resize: true,
                profile: None,
            },
        };
        assert_eq!(tool.output_limit(), 8 * 1024 * 1024);
        let result = tool.read_image(&small).unwrap().unwrap();
        assert_eq!(result["content"][0]["text"], "Read image file [image/jpeg]");
        assert_eq!(result["content"][1]["mimeType"], "image/jpeg");
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(result["content"][1]["data"].as_str().unwrap())
                .unwrap(),
            std::fs::read(&small).unwrap()
        );
        let result = tool.read_image(&wide).unwrap().unwrap();
        let note = result["content"][0]["text"].as_str().unwrap();
        assert!(
            note.contains("original 2100x42, displayed at 2000x40"),
            "{note}"
        );
        assert!(note.contains("Multiply coordinates by 1.05"), "{note}");
        let tall = directory.path().join("tall.png");
        image::RgbImage::from_pixel(1, 4000, image::Rgb([12, 34, 56]))
            .save(&tall)
            .unwrap();
        let result = tool.read_image(&tall).unwrap().unwrap();
        let note = result["content"][0]["text"].as_str().unwrap();
        assert!(
            note.contains("Multiply x coordinates by 1.00 and y coordinates by 2.00"),
            "{note}"
        );
        let result = CodingTool {
            kind: Kind::Read {
                auto_resize: false,
                profile: None,
            },
            ..tool
        }
        .read_image(&small)
        .unwrap()
        .unwrap();
        assert_eq!(result["content"][0]["text"], "Read image file [image/jpeg]");
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
            shell: ShellConfig::default(),
            kind: Kind::Read {
                auto_resize: true,
                profile: None,
            },
        };
        let value = tool.read_image(&path).unwrap().unwrap();
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
            shell: ShellConfig::default(),
            kind: Kind::Read {
                auto_resize: true,
                profile: None,
            },
        };
        assert!(scoped.path(skill.to_str().unwrap()).is_err());
        let local = CodingTool {
            root: workspace,
            unrestricted: true,
            shell: ShellConfig::default(),
            kind: Kind::Read {
                auto_resize: true,
                profile: None,
            },
        };
        assert_eq!(
            local
                .read(&json!({"path":skill}), &pi_rs::CancellationToken::new())
                .unwrap()
                .tool_content(),
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
            shell: ShellConfig::default(),
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
            shell: ShellConfig::default(),
            root: root.path().canonicalize().unwrap(),
            kind: Kind::Write,
        };
        assert_eq!(
            tool.write(&json!({"path":"src/a.txt","content":"first\n日本語\nlast\n"}))
                .unwrap(),
            "Successfully wrote to src/a.txt"
        );
        assert_eq!(
            tool.read(
                &json!({"path":"src/a.txt","offset":2,"limit":1}),
                &pi_rs::CancellationToken::new()
            )
            .unwrap()
            .tool_content(),
            "日本語\n\n[2 more lines in file. Use offset=3 to continue.]"
        );
        assert!(
            tool.write(&json!({"path":"../escape.txt","content":"bad"}))
                .is_err()
        );
    }

    #[test]
    fn file_tools_share_path_normalization_and_reader_fallbacks() {
        let root = tempfile::tempdir().unwrap();
        let tool = CodingTool {
            root: root.path().canonicalize().unwrap(),
            unrestricted: false,
            shell: ShellConfig::default(),
            kind: Kind::Write,
        };
        tool.write(&json!({"path":"@nested/some\u{202f}file.txt", "content":"hello"}))
            .unwrap();
        let url = reqwest::Url::from_file_path(root.path().join("nested/some file.txt")).unwrap();
        let reader = CodingTool {
            kind: Kind::Read {
                auto_resize: true,
                profile: None,
            },
            ..tool
        };
        assert_eq!(
            reader
                .read(
                    &json!({"path":url.as_str()}),
                    &pi_rs::CancellationToken::new()
                )
                .unwrap()
                .tool_content(),
            "hello"
        );
        assert!(
            reader
                .read(
                    &json!({"path":"@../escape"}),
                    &pi_rs::CancellationToken::new()
                )
                .is_err()
        );
        std::fs::write(root.path().join("Capture d’ecran.txt"), "curly").unwrap();
        assert_eq!(
            reader
                .read(
                    &json!({"path":"Capture d'ecran.txt"}),
                    &pi_rs::CancellationToken::new()
                )
                .unwrap()
                .tool_content(),
            "curly"
        );
        let editor = CodingTool {
            kind: Kind::Edit,
            ..reader
        };
        assert!(
            editor
                .edit(&json!({"path":"Capture d'ecran.txt", "oldText":"curly", "newText":"other"}))
                .is_err()
        );
    }

    #[tokio::test]
    async fn cancelled_mutation_stops_while_another_writer_holds_the_lock() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file");
        std::fs::write(&path, "before").unwrap();
        let lock = mutation_lock(
            &path.canonicalize().unwrap(),
            &pi_rs::CancellationToken::new(),
        )
        .unwrap();
        let tool = CodingTool {
            root: root.path().canonicalize().unwrap(),
            unrestricted: true,
            shell: ShellConfig::default(),
            kind: Kind::Write,
        };
        let cancellation = pi_rs::CancellationToken::new();
        let context = ToolContext {
            operation_id: uuid::Uuid::new_v4(),
            cancellation: cancellation.clone(),
        };
        let task = tokio::spawn(async move {
            tool.invoke(json!({"path":"file","content":"after"}), context)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!task.is_finished());
        cancellation.cancel();
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(outcome, Outcome::Failed(message) if message == "Operation aborted"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "before");
        drop(lock);
    }

    #[tokio::test]
    async fn cancelled_mutations_do_not_change_files() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file");
        std::fs::write(&path, "before").unwrap();
        let mut tool = CodingTool {
            root: root.path().canonicalize().unwrap(),
            unrestricted: true,
            shell: ShellConfig::default(),
            kind: Kind::Write,
        };
        let cancellation = pi_rs::CancellationToken::new();
        cancellation.cancel();
        for (kind, args) in [
            (Kind::Write, json!({"path":"file","content":"after"})),
            (
                Kind::Edit,
                json!({"path":"file","oldText":"before","newText":"after"}),
            ),
        ] {
            tool.kind = kind;
            let context = ToolContext {
                operation_id: uuid::Uuid::new_v4(),
                cancellation: cancellation.clone(),
            };
            assert!(
                matches!(tool.invoke(args, context).await, Outcome::Failed(message) if message == "Operation aborted")
            );
            assert_eq!(std::fs::read_to_string(&path).unwrap(), "before");
        }
    }

    #[cfg(unix)]
    #[test]
    fn new_files_follow_umask_and_existing_file_permissions_survive_editing() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let reference = root.path().join("reference");
        std::fs::write(&reference, "reference").unwrap();
        let tool = CodingTool {
            root: root.path().canonicalize().unwrap(),
            unrestricted: true,
            shell: ShellConfig::default(),
            kind: Kind::Write,
        };
        tool.write(&json!({"path":"new","content":"before"}))
            .unwrap();
        let target = root.path().join("new");
        assert_eq!(
            target.metadata().unwrap().permissions().mode() & 0o777,
            reference.metadata().unwrap().permissions().mode() & 0o777
        );
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o751)).unwrap();
        let editor = CodingTool {
            kind: Kind::Edit,
            ..tool
        };
        editor
            .edit(&json!({"path":"new","oldText":"before","newText":"after"}))
            .unwrap();
        assert_eq!(
            target.metadata().unwrap().permissions().mode() & 0o777,
            0o751
        );
    }

    #[test]
    fn local_write_and_edit_support_files_larger_than_four_mebibytes() {
        let root = tempfile::tempdir().unwrap();
        let tool = CodingTool {
            root: root.path().canonicalize().unwrap(),
            unrestricted: true,
            shell: ShellConfig::default(),
            kind: Kind::Write,
        };
        let content = format!("{}\nchange me\n", "a".repeat(MAX_FILE_BYTES as usize));
        tool.write(&json!({"path":"large.txt","content":content}))
            .unwrap();
        let editor = CodingTool {
            kind: Kind::Edit,
            ..tool
        };
        editor
            .edit(&json!({"path":"large.txt","oldText":"change me","newText":"changed"}))
            .unwrap();
        let result = std::fs::read_to_string(root.path().join("large.txt")).unwrap();
        assert_eq!(result.len(), content.len() - 2);
        assert!(result.ends_with("\nchanged\n"));
    }

    #[test]
    fn directory_output_sorts_case_insensitively_and_reports_limits() {
        let root = tempfile::tempdir().unwrap();
        let tool = CodingTool {
            root: root.path().canonicalize().unwrap(),
            unrestricted: false,
            shell: ShellConfig::default(),
            kind: Kind::List,
        };
        assert_eq!(
            tool.list(&json!({}), &pi_rs::CancellationToken::new())
                .unwrap()
                .tool_content(),
            "(empty directory)"
        );
        std::fs::write(root.path().join("Zeta"), "").unwrap();
        std::fs::write(root.path().join("alpha"), "").unwrap();
        std::fs::create_dir(root.path().join("Beta")).unwrap();
        assert_eq!(
            tool.list(&json!({}), &pi_rs::CancellationToken::new())
                .unwrap()
                .tool_content(),
            "alpha\nBeta/\nZeta"
        );
        let output = tool
            .list(&json!({"limit":1}), &pi_rs::CancellationToken::new())
            .unwrap()
            .tool_content();
        assert!(output.starts_with("alpha\n"));
        assert!(output.contains("1 entries limit reached"));
    }

    #[test]
    fn directory_collation_matches_pi_for_punctuation_and_local_accents() {
        let sort = |locale, mut names: Vec<&'static str>| {
            let collator = directory_collator(locale).unwrap();
            names.sort_by(|left, right| {
                collator.compare(&left.to_lowercase(), &right.to_lowercase())
            });
            names
        };
        assert_eq!(
            sort(
                "en_US.UTF-8",
                vec![
                    "Zeta", "_cache", ".git", "alpha", "éclair", "eagle", "Beta", "-build"
                ]
            ),
            [
                "_cache", "-build", ".git", "alpha", "Beta", "eagle", "éclair", "Zeta"
            ]
        );
        assert_eq!(
            sort("sv_SE.UTF-8", vec!["Älmhult", "Zeta", "Åre", "Örebro"]),
            ["Zeta", "Åre", "Älmhult", "Örebro"]
        );
        assert_eq!(
            sort("C.UTF-8", vec!["Zeta", "Älmhult"]),
            ["Älmhult", "Zeta"]
        );
    }

    #[test]
    fn directory_byte_limit_keeps_complete_entries_and_combines_notices() {
        let root = tempfile::tempdir().unwrap();
        let tool = CodingTool {
            root: root.path().canonicalize().unwrap(),
            unrestricted: false,
            shell: ShellConfig::default(),
            kind: Kind::List,
        };
        let names: Vec<_> = (0..301)
            .map(|index| format!("{index:03}-{}", "x".repeat(196)))
            .collect();
        for name in &names {
            std::fs::write(root.path().join(name), "").unwrap();
        }
        let output = tool
            .list(&json!({"limit":300}), &pi_rs::CancellationToken::new())
            .unwrap()
            .tool_content();
        let (body, notice) = output.rsplit_once("\n\n").unwrap();
        assert!(body.len() <= MAX_OUTPUT_BYTES);
        assert_eq!(body.lines().collect::<Vec<_>>(), names[..254]);
        assert_eq!(
            notice,
            "[300 entries limit reached. Use limit=600 for more. 50.0KB limit reached]"
        );
        let output = tool
            .list(&json!({"limit":301}), &pi_rs::CancellationToken::new())
            .unwrap()
            .tool_content();
        assert!(output.ends_with("\n\n[50.0KB limit reached]"));
        let output = tool
            .list(&json!({"limit":1}), &pi_rs::CancellationToken::new())
            .unwrap()
            .tool_content();
        assert_eq!(
            output,
            format!(
                "{}\n\n[1 entries limit reached. Use limit=2 for more]",
                names[0]
            )
        );
        assert!(
            tool.list(&json!({"path":names[0]}), &pi_rs::CancellationToken::new())
                .unwrap_err()
                .to_string()
                .contains("Not a directory:")
        );
        let find = CodingTool {
            kind: Kind::Find,
            ..tool
        };
        let output = find
            .search(
                &json!({"pattern":"*", "limit":300}),
                &pi_rs::CancellationToken::new(),
            )
            .unwrap()
            .tool_content();
        let (body, notice) = output.rsplit_once("\n\n").unwrap();
        assert_eq!(body.lines().collect::<Vec<_>>(), names[..254]);
        assert_eq!(
            notice,
            "[300 results limit reached. Use limit=600 for more, or refine pattern. 50.0KB limit reached]"
        );
    }

    #[test]
    fn directory_byte_limit_includes_an_entry_ending_exactly_at_the_boundary() {
        let root = tempfile::tempdir().unwrap();
        let tool = CodingTool {
            root: root.path().canonicalize().unwrap(),
            unrestricted: false,
            shell: ShellConfig::default(),
            kind: Kind::List,
        };
        let names: Vec<_> = (0..257)
            .map(|index| {
                format!(
                    "{index:03}-{}",
                    "x".repeat(if index < 255 { 195 } else { 196 })
                )
            })
            .collect();
        for name in &names {
            std::fs::write(root.path().join(name), "").unwrap();
        }
        let output = tool
            .list(&json!({}), &pi_rs::CancellationToken::new())
            .unwrap()
            .tool_content();
        let (body, notice) = output.rsplit_once("\n\n").unwrap();
        assert_eq!(body.len(), MAX_OUTPUT_BYTES);
        assert_eq!(body.lines().collect::<Vec<_>>(), names[..256]);
        assert_eq!(notice, "[50.0KB limit reached]");
    }

    #[test]
    fn find_keeps_dependency_paths_but_respects_vcs_and_fd_ignores() {
        let root = tempfile::tempdir().unwrap();
        for directory in ["node_modules/nested", ".git/objects", "src", "fd-ignored"] {
            std::fs::create_dir_all(root.path().join(directory)).unwrap();
            std::fs::write(root.path().join(directory).join("sample.rs"), "").unwrap();
        }
        std::fs::write(root.path().join(".fdignore"), "fd-ignored/\n").unwrap();
        let tool = CodingTool {
            unrestricted: false,
            shell: ShellConfig::default(),
            root: root.path().canonicalize().unwrap(),
            kind: Kind::Find,
        };
        assert_eq!(
            tool.search(
                &json!({"pattern":"**/*.rs"}),
                &pi_rs::CancellationToken::new()
            )
            .unwrap()
            .tool_content(),
            "node_modules/nested/sample.rs\nsrc/sample.rs"
        );
        assert_eq!(
            tool.search(
                &json!({"pattern":"**/*.rs", "path":"node_modules"}),
                &pi_rs::CancellationToken::new()
            )
            .unwrap()
            .tool_content(),
            "nested/sample.rs"
        );
    }

    #[test]
    fn find_matches_directories_smart_case_and_path_globs() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("src/nested")).unwrap();
        std::fs::write(root.path().join("src/sample.rs"), "").unwrap();
        std::fs::write(root.path().join("src/nested/Upper.RS"), "").unwrap();
        std::fs::write(root.path().join(".hidden"), "").unwrap();
        let tool = CodingTool {
            unrestricted: false,
            shell: ShellConfig::default(),
            root: root.path().canonicalize().unwrap(),
            kind: Kind::Find,
        };
        assert_eq!(
            tool.search(&json!({"pattern":"*.rs"}), &pi_rs::CancellationToken::new())
                .unwrap()
                .tool_content(),
            "src/nested/Upper.RS\nsrc/sample.rs"
        );
        assert_eq!(
            tool.search(&json!({"pattern":"*.RS"}), &pi_rs::CancellationToken::new())
                .unwrap()
                .tool_content(),
            "src/nested/Upper.RS"
        );
        assert_eq!(
            tool.search(
                &json!({"pattern":"src/**/*.rs"}),
                &pi_rs::CancellationToken::new()
            )
            .unwrap()
            .tool_content(),
            "src/nested/Upper.RS\nsrc/sample.rs"
        );
        assert_eq!(
            tool.search(&json!({"pattern":"src"}), &pi_rs::CancellationToken::new())
                .unwrap()
                .tool_content(),
            "src/"
        );
        assert_eq!(
            tool.search(&json!({"pattern":"*"}), &pi_rs::CancellationToken::new())
                .unwrap()
                .tool_content(),
            ".hidden\nsrc/\nsrc/nested/\nsrc/nested/Upper.RS\nsrc/sample.rs"
        );
        assert!(
            tool.search(
                &json!({"pattern":"*","path":".hidden"}),
                &pi_rs::CancellationToken::new()
            )
            .is_err()
        );
    }

    #[test]
    fn find_respects_gitignore_outside_a_repository_and_reports_limits() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("node_modules")).unwrap();
        std::fs::write(root.path().join("node_modules/dependency.rs"), "").unwrap();
        std::fs::write(root.path().join(".gitignore"), "node_modules/\n").unwrap();
        std::fs::write(root.path().join("a.rs"), "").unwrap();
        std::fs::write(root.path().join("b.rs"), "").unwrap();
        let tool = CodingTool {
            unrestricted: false,
            shell: ShellConfig::default(),
            root: root.path().canonicalize().unwrap(),
            kind: Kind::Find,
        };
        assert_eq!(
            tool.search(&json!({"pattern":"*.rs"}), &pi_rs::CancellationToken::new())
                .unwrap()
                .tool_content(),
            "a.rs\nb.rs"
        );
        assert_eq!(
            tool.search(
                &json!({"pattern":"*.rs","limit":1}),
                &pi_rs::CancellationToken::new()
            )
            .unwrap()
            .tool_content(),
            "a.rs\n\n[1 results limit reached. Use limit=2 for more, or refine pattern]"
        );
    }

    #[cfg(unix)]
    #[test]
    fn find_lists_symlinks_without_traversing_their_targets() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("src")).unwrap();
        std::fs::write(root.path().join("src/a.rs"), "").unwrap();
        std::os::unix::fs::symlink("src", root.path().join("alias")).unwrap();
        std::os::unix::fs::symlink("missing", root.path().join("broken")).unwrap();
        let tool = CodingTool {
            unrestricted: false,
            shell: ShellConfig::default(),
            root: root.path().canonicalize().unwrap(),
            kind: Kind::Find,
        };
        assert_eq!(
            tool.search(&json!({"pattern":"*"}), &pi_rs::CancellationToken::new())
                .unwrap()
                .tool_content(),
            "alias\nbroken\nsrc/\nsrc/a.rs"
        );
    }

    #[test]
    fn glob_search_and_file_scoped_grep_use_the_correct_matcher() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("main.rs"), "fn main() {}\n").unwrap();
        let tool = CodingTool {
            unrestricted: false,
            shell: ShellConfig::default(),
            root: root.path().canonicalize().unwrap(),
            kind: Kind::Find,
        };
        assert_eq!(
            tool.search(
                &json!({"pattern":"**/*.rs"}),
                &pi_rs::CancellationToken::new()
            )
            .unwrap()
            .tool_content(),
            "main.rs"
        );
        let tool = CodingTool {
            unrestricted: false,
            shell: ShellConfig::default(),
            kind: Kind::Grep,
            ..tool
        };
        assert_eq!(
            tool.search(
                &json!({"pattern":"fn", "path":"main.rs"}),
                &pi_rs::CancellationToken::new()
            )
            .unwrap()
            .tool_content(),
            "main.rs:1: fn main() {}"
        );
    }

    #[test]
    fn grep_globs_accept_exclusions_and_workspace_relative_patterns() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("src")).unwrap();
        for file in ["src/a.rs", "src/b.txt"] {
            std::fs::write(root.path().join(file), "needle\n").unwrap();
        }
        let tool = CodingTool {
            unrestricted: false,
            shell: ShellConfig::default(),
            root: root.path().canonicalize().unwrap(),
            kind: Kind::Grep,
        };
        assert_eq!(
            tool.search(
                &json!({"pattern":"needle","path":"src","glob":"!*.rs"}),
                &pi_rs::CancellationToken::new()
            )
            .unwrap()
            .tool_content(),
            "b.txt:1: needle"
        );
        assert_eq!(
            tool.search(
                &json!({"pattern":"needle","path":"src","glob":"/src/*.rs"}),
                &pi_rs::CancellationToken::new()
            )
            .unwrap()
            .tool_content(),
            "a.rs:1: needle"
        );
        assert_eq!(
            tool.search(
                &json!({"pattern":"needle","path":"src/a.rs","glob":"!*.rs"}),
                &pi_rs::CancellationToken::new()
            )
            .unwrap()
            .tool_content(),
            "a.rs:1: needle"
        );
    }

    #[tokio::test]
    async fn shell_timeout_retains_partial_output_and_rejects_invalid_values() {
        let root = tempfile::tempdir().unwrap();
        let tool = CodingTool {
            root: root.path().to_path_buf(),
            unrestricted: false,
            shell: ShellConfig::default(),
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
        let Outcome::Failed(error) = tool
            .bash(
                &json!({"command":command, "timeout":timeout}),
                &pi_rs::CancellationToken::new(),
            )
            .await
        else {
            panic!("expected a timeout")
        };
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
            shell: ShellConfig::default(),
            kind: Kind::Bash,
        };
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            tool.shell(
                &json!({"command":"sleep 1 & printf done"}),
                &pi_rs::CancellationToken::new(),
            ),
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
            shell: ShellConfig::default(),
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
        let Outcome::Failed(output) = tool
            .bash(
                &json!({"command":command}),
                &pi_rs::CancellationToken::new(),
            )
            .await
        else {
            panic!("expected a nonzero exit")
        };
        assert!(output.contains("out"));
        assert!(output.contains("err"));
        assert!(output.contains('7'));
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn local_shell_uses_pi_output_and_status_text() {
        let root = tempfile::tempdir().unwrap();
        let tool = CodingTool {
            unrestricted: true,
            shell: ShellConfig::default(),
            root: root.path().to_path_buf(),
            kind: Kind::Bash,
        };
        let cancellation = pi_rs::CancellationToken::new();
        assert_eq!(
            tool.bash(&json!({"command":"printf hello"}), &cancellation)
                .await,
            Outcome::Succeeded("hello".into())
        );
        assert_eq!(
            tool.bash(&json!({"command":"true"}), &cancellation).await,
            Outcome::Succeeded("(no output)".into())
        );
        assert_eq!(
            tool.bash(&json!({"command":"printf hello; exit 7"}), &cancellation)
                .await,
            Outcome::Failed("hello\n\nCommand exited with code 7".into())
        );
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
            extension
                .tools()
                .iter()
                .any(|tool| tool.spec().name == "bash")
        );
        let tool = CodingTool {
            root: root.path().to_owned(),
            unrestricted: false,
            shell: ShellConfig::default(),
            kind: Kind::PowerShell,
        };
        let output = tool
            .shell(
                &json!({"command":"Write-Output '日本語'"}),
                &pi_rs::CancellationToken::new(),
            )
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
            tool.shell(
                &json!({"command":command,"timeout":5}),
                &pi_rs::CancellationToken::new()
            )
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
