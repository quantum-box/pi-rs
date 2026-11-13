//! Structured backend grep for hosts composing a remote tool set.
use pi_rs::{
    CancellationToken, Error, Extension, Outcome, Result, Tool, ToolContext, ToolExecution,
    ToolSpec,
};
use serde_json::{Map, Value, json};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

const MAX_BYTES: usize = 50 * 1024;
const UNLIMITED_LINES: u64 = 9_007_199_254_740_991;

/// Backend search request. Paths are absolute in the backend namespace.
#[derive(Clone, Debug)]
pub struct GrepRequest {
    pub path: PathBuf,
    pub pattern: String,
    pub glob: Option<String>,
    pub ignore_case: bool,
    pub literal: bool,
    pub context: u64,
    pub limit: usize,
}
/// A match and adjacent context, ordered from earliest to latest line.
#[derive(Clone, Debug)]
pub struct GrepMatch {
    /// UTF-8 relative path under a directory search, or the searched file's
    /// basename; an absolute path in the backend namespace is also accepted.
    pub path: String,
    pub line_number: u64,
    pub line: String,
    pub before: Vec<String>,
    pub after: Vec<String>,
}
/// Remote backend. Search must honor the request's regex/literal, case, glob,
/// ignore-file and binary-file semantics, emit matches in desired display order,
/// and finish sending before returning. It must observe cancellation or stop
/// when its future is dropped. Match/context data should be bounded by the
/// backend; each channel message is owned and can otherwise be arbitrarily large.
#[async_trait::async_trait]
pub trait GrepOperations: Send + Sync {
    async fn is_directory(&self, path: &Path) -> Result<bool>;
    async fn search(
        &self,
        request: GrepRequest,
        cancellation: CancellationToken,
        matches: tokio::sync::mpsc::Sender<GrepMatch>,
    ) -> Result<()>;
}
/// Configuration for remote path resolution.
#[derive(Clone, Debug, Default)]
pub struct GrepToolOptions {
    pub remote_home: Option<PathBuf>,
}
pub fn create_grep_tool(root: &Path, operations: Arc<dyn GrepOperations>) -> Result<Arc<dyn Tool>> {
    create_grep_tool_with_options(root, operations, GrepToolOptions::default())
}
pub fn create_grep_tool_with_options(
    root: &Path,
    operations: Arc<dyn GrepOperations>,
    options: GrepToolOptions,
) -> Result<Arc<dyn Tool>> {
    if !root.is_absolute() {
        return Err(Error::Config(
            "remote working directory must be absolute".into(),
        ));
    }
    if options
        .remote_home
        .as_ref()
        .is_some_and(|home| !home.is_absolute())
    {
        return Err(Error::Config(
            "remote home directory must be absolute".into(),
        ));
    }
    Ok(Arc::new(RemoteGrep {
        root: resolve(root, Path::new(".")),
        operations,
        options,
    }))
}
pub struct GrepExtension {
    tool: Arc<dyn Tool>,
}
impl GrepExtension {
    pub fn new(root: &Path, operations: Arc<dyn GrepOperations>) -> Result<Self> {
        Self::with_options(root, operations, GrepToolOptions::default())
    }
    pub fn with_options(
        root: &Path,
        operations: Arc<dyn GrepOperations>,
        options: GrepToolOptions,
    ) -> Result<Self> {
        Ok(Self {
            tool: create_grep_tool_with_options(root, operations, options)?,
        })
    }
}
impl Extension for GrepExtension {
    fn id(&self) -> &str {
        "remote-grep"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![self.tool.clone()]
    }
}
struct RemoteGrep {
    root: PathBuf,
    operations: Arc<dyn GrepOperations>,
    options: GrepToolOptions,
}

fn normalize(value: &str, remote_home: Option<&Path>) -> Result<PathBuf> {
    let value: String = value
        .chars()
        .map(|ch| match ch {
            '\u{a0}' | '\u{2000}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
            ch => ch,
        })
        .collect();
    let value = value.strip_prefix('@').unwrap_or(&value);
    #[cfg(windows)]
    let windows_value = windows_shell_path(value);
    #[cfg(windows)]
    let value = windows_value.as_str();
    if value == "~" || value.starts_with("~/") || (cfg!(windows) && value.starts_with("~\\")) {
        let home = remote_home
            .ok_or_else(|| Error::Config("remote home directory is not configured".into()))?;
        return if value == "~" {
            Ok(home.to_owned())
        } else {
            let tail = Path::new(&value[2..]);
            if tail.has_root() || matches!(tail.components().next(), Some(Component::Prefix(_))) {
                return Err(Error::Config("invalid backend home-relative path".into()));
            }
            checked_resolve(home, tail)
        };
    }
    if value.starts_with("file://") {
        return url::Url::parse(value)
            .map_err(|error| Error::Config(error.to_string()))?
            .to_file_path()
            .map_err(|_| Error::Config("invalid file URL".into()));
    }
    Ok(PathBuf::from(value))
}
#[cfg(any(windows, test))]
fn windows_shell_path(value: &str) -> String {
    if !value.starts_with('/') || value.starts_with("//") || value.contains('\\') {
        return value.into();
    }
    let drive = value
        .strip_prefix("/mnt/")
        .or_else(|| value.strip_prefix("/cygdrive/"))
        .unwrap_or(&value[1..]);
    let bytes = drive.as_bytes();
    if bytes.first().is_some_and(u8::is_ascii_alphabetic)
        && (bytes.len() == 1 || bytes.get(1) == Some(&b'/'))
    {
        return format!(
            "{}:\\{}",
            char::from(bytes[0]).to_ascii_uppercase(),
            drive.get(2..).unwrap_or("").replace('/', "\\")
        );
    }
    value.into()
}

fn resolve(root: &Path, input: &Path) -> PathBuf {
    let path = if input.is_absolute() {
        input.to_owned()
    } else {
        root.join(input)
    };
    let mut resolved = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            part => resolved.push(part.as_os_str()),
        }
    }
    resolved
}

fn checked_resolve(root: &Path, input: &Path) -> Result<PathBuf> {
    if !input.is_absolute() && matches!(input.components().next(), Some(Component::Prefix(_))) {
        return Err(Error::Config(
            "drive-relative backend path is not supported".into(),
        ));
    }
    let path = resolve(root, input);
    if !path.is_absolute() {
        return Err(Error::Config("backend path must be absolute".into()));
    }
    Ok(path)
}

struct Listing {
    prefix: Vec<u8>,
    total_bytes: u64,
    newlines: u64,
    ends_in_newline: bool,
    lines: u64,
    matches: usize,
    long_lines: bool,
}
impl Listing {
    fn new() -> Self {
        Self {
            prefix: Vec::with_capacity(MAX_BYTES + 1),
            total_bytes: 0,
            newlines: 0,
            ends_in_newline: false,
            lines: 0,
            matches: 0,
            long_lines: false,
        }
    }
    async fn append(&mut self, bytes: &[u8], path: bool, token: &CancellationToken) -> Result<()> {
        for (index, chunk) in bytes.chunks(8192).enumerate() {
            if token.is_cancelled() {
                return Err(Error::Cancelled);
            }
            self.total_bytes = self.total_bytes.saturating_add(chunk.len() as u64);
            self.newlines += chunk.iter().filter(|byte| **byte == b'\n').count() as u64;
            self.ends_in_newline = chunk.last() == Some(&b'\n');
            let keep = (MAX_BYTES + 1)
                .saturating_sub(self.prefix.len())
                .min(chunk.len());
            if cfg!(windows) && path {
                self.prefix.extend(
                    chunk[..keep]
                        .iter()
                        .map(|byte| if *byte == b'\\' { b'/' } else { *byte }),
                );
            } else {
                self.prefix.extend_from_slice(&chunk[..keep]);
            }
            if index % 8 == 7 {
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    }
    async fn line(
        &mut self,
        path: &str,
        number: u64,
        matched: bool,
        text: &str,
        token: &CancellationToken,
    ) -> Result<()> {
        if self.lines != 0 {
            self.append(b"\n", false, token).await?;
        }
        self.lines += 1;
        self.append(path.as_bytes(), true, token).await?;
        let marker = if matched { ':' } else { '-' };
        self.append(
            format!("{marker}{number}{marker} ").as_bytes(),
            false,
            token,
        )
        .await?;
        let text = text.strip_suffix('\n').unwrap_or(text);
        for (index, chunk) in text.as_bytes().chunks(8192).enumerate() {
            if token.is_cancelled() {
                return Err(Error::Cancelled);
            }
            if chunk.contains(&b'\n') {
                return Err(Error::Config(
                    "grep backend returned multiple lines in one line field".into(),
                ));
            }
            if index % 8 == 7 {
                tokio::task::yield_now().await;
            }
        }
        let mut units = Vec::with_capacity(500);
        let mut truncated = false;
        for unit in text.encode_utf16().filter(|unit| *unit != b'\r' as u16) {
            if token.is_cancelled() {
                return Err(Error::Cancelled);
            }
            if units.len() == 500 {
                truncated = true;
                break;
            }
            units.push(unit);
        }
        let text = String::from_utf16_lossy(&units);
        self.append(text.as_bytes(), false, token).await?;
        if truncated {
            self.long_lines = true;
            self.append(b"... [truncated]", false, token).await?;
        }
        Ok(())
    }
    fn finish(mut self, reached: bool, limit: usize) -> Outcome {
        if self.matches == 0 {
            return Outcome::Succeeded("No matches found".into());
        }
        let mut details = Map::new();
        let mut notices = Vec::new();
        if reached {
            details.insert("matchLimitReached".into(), json!(limit));
            notices.push(format!(
                "{limit} matches limit reached. Use limit={} for more, or refine pattern",
                limit.saturating_mul(2)
            ));
        }
        let output = if self.total_bytes > MAX_BYTES as u64 {
            let end = self.prefix.iter().rposition(|byte| *byte == b'\n');
            self.prefix.truncate(end.unwrap_or(0));
            let output_lines = end.map_or(0, |_| {
                self.prefix.iter().filter(|byte| **byte == b'\n').count() + 1
            });
            let text = String::from_utf8(self.prefix).expect("truncated at newline");
            let total_lines = self.newlines + u64::from(!self.ends_in_newline);
            details.insert("truncation".into(),json!({"content":text,"truncated":true,"truncatedBy":"bytes","totalLines":total_lines,"totalBytes":self.total_bytes,"outputLines":output_lines,"outputBytes":text.len(),"lastLinePartial":false,"firstLineExceedsLimit":end.is_none(),"maxLines":UNLIMITED_LINES,"maxBytes":MAX_BYTES}));
            notices.push("50.0KB limit reached".into());
            text
        } else {
            String::from_utf8(self.prefix).expect("complete UTF-8 output")
        };
        if self.long_lines {
            details.insert("linesTruncated".into(), json!(true));
            notices
                .push("Some lines truncated to 500 chars. Use read tool to see full lines".into());
        }
        let mut output = output;
        if !notices.is_empty() {
            output.push_str(&format!("\n\n[{}]", notices.join(". ")));
        }
        if details.is_empty() {
            Outcome::Succeeded(output)
        } else {
            Outcome::SucceededWithDetails {
                output,
                details: Value::Object(details),
            }
        }
    }
}
fn display_path<'a>(
    value: &'a str,
    base: &Path,
    directory: bool,
    cancellation: &CancellationToken,
) -> Result<&'a str> {
    let path = Path::new(value);
    let relative = if directory {
        if path.is_absolute() {
            path.strip_prefix(base).map_err(|_| {
                Error::Config("grep backend returned a path outside the search directory".into())
            })?
        } else {
            path
        }
    } else {
        let name = base
            .file_name()
            .ok_or_else(|| Error::Config("search file needs a name".into()))?;
        if path.is_absolute() {
            if path != base {
                return Err(Error::Config(
                    "grep backend returned a different file".into(),
                ));
            }
        } else if path != Path::new(name) {
            return Err(Error::Config(
                "grep backend returned a different file".into(),
            ));
        }
        path.file_name()
            .map(Path::new)
            .ok_or_else(|| Error::Config("grep backend returned an empty path".into()))?
    };
    if relative.as_os_str().is_empty() {
        return Err(Error::Config("grep backend returned an empty path".into()));
    }
    for part in relative.components() {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if matches!(
            part,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        ) {
            return Err(Error::Config(
                "grep backend returned a path outside the search directory".into(),
            ));
        }
    }
    relative
        .to_str()
        .ok_or_else(|| Error::Config("grep backend path must be UTF-8".into()))
}
async fn append_match(
    output: &mut Listing,
    item: GrepMatch,
    base: &Path,
    directory: bool,
    context: u64,
    token: &CancellationToken,
) -> Result<()> {
    let path = display_path(&item.path, base, directory, token)?;
    let before = u64::try_from(item.before.len()).unwrap_or(u64::MAX);
    let after = u64::try_from(item.after.len()).unwrap_or(u64::MAX);
    if item.line_number == 0
        || before > context
        || after > context
        || before >= item.line_number
        || item.line_number.checked_add(after).is_none()
    {
        return Err(Error::Config(
            "grep backend returned invalid match or context line numbers".into(),
        ));
    }
    for (index, text) in item.before.iter().enumerate() {
        output
            .line(
                path,
                item.line_number - before + index as u64,
                false,
                text,
                token,
            )
            .await?;
    }
    output
        .line(path, item.line_number, true, &item.line, token)
        .await?;
    for (index, text) in item.after.iter().enumerate() {
        output
            .line(
                path,
                item.line_number + index as u64 + 1,
                false,
                text,
                token,
            )
            .await?;
    }
    output.matches += 1;
    Ok(())
}
fn optional_bool(args: &Value, name: &str) -> Result<bool> {
    match args.get(name) {
        None => Ok(false),
        Some(Value::Bool(value)) => Ok(*value),
        _ => Err(Error::Config(format!("{name} must be a boolean"))),
    }
}
#[async_trait::async_trait]
impl Tool for RemoteGrep {
    fn spec(&self) -> ToolSpec {
        ToolSpec {name:"grep".into(),description:"Search file contents for patterns. Returns path and line numbers, respecting ignore files; output is bounded to the match limit or 50KB.".into(),parameters:json!({"type":"object","properties":{"pattern":{"type":"string"},"path":{"type":"string"},"glob":{"type":"string"},"ignoreCase":{"type":"boolean"},"literal":{"type":"boolean"},"context":{"type":"integer","minimum":0},"limit":{"type":"integer","minimum":1,"maximum":100000}},"required":["pattern"],"additionalProperties":false})}
    }
    fn execution(&self) -> ToolExecution {
        ToolExecution::ParallelRead
    }
    async fn invoke(&self, args: Value, context: ToolContext) -> Outcome {
        let cancellation = context.cancellation.child_token();
        let _guard = cancellation.clone().drop_guard();
        let operation = async {
            let pattern = args
                .get("pattern")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Config("pattern must be a string".into()))?
                .to_owned();
            let requested = match args.get("path") {
                None => ".",
                Some(Value::String(path)) if path.is_empty() => ".",
                Some(Value::String(path)) => path,
                Some(_) => return Err(Error::Config("path must be a string".into())),
            };
            let glob = match args.get("glob") {
                None => None,
                Some(Value::String(value)) => Some(value.clone()),
                Some(_) => return Err(Error::Config("glob must be a string".into())),
            };
            let limit = match args.get("limit") {
                None => 100,
                Some(value) => value
                    .as_u64()
                    .filter(|n| (1..=100_000).contains(n))
                    .ok_or_else(|| {
                        Error::Config("limit must be an integer between 1 and 100000".into())
                    })? as usize,
            };
            let context = match args.get("context") {
                None => 0,
                Some(value) => value
                    .as_u64()
                    .ok_or_else(|| Error::Config("context must be a nonnegative integer".into()))?,
            };
            let path = checked_resolve(
                &self.root,
                &normalize(requested, self.options.remote_home.as_deref())?,
            )?;
            let request = GrepRequest {
                path: path.clone(),
                pattern,
                glob,
                ignore_case: optional_bool(&args, "ignoreCase")?,
                literal: optional_bool(&args, "literal")?,
                context,
                limit,
            };
            let directory = self
                .operations
                .is_directory(&path)
                .await
                .map_err(|_| Error::Config(format!("Path not found: {}", path.display())))?;
            let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
            let backend_cancel = cancellation.child_token();
            let _backend_guard = backend_cancel.clone().drop_guard();
            let search = self
                .operations
                .search(request, backend_cancel.clone(), sender);
            tokio::pin!(search);
            let mut open = true;
            let mut output = Listing::new();
            let mut reached = false;
            loop {
                tokio::select! {
                    biased;
                    _=cancellation.cancelled()=>return Err(Error::Cancelled),
                    value=receiver.recv(), if open => match value {
                        Some(item)=>{append_match(&mut output,item,&path,directory,context,&cancellation).await?;if output.matches>=limit { reached=true;backend_cancel.cancel();break; }},
                        None=>open=false,
                    },
                    result=&mut search=>{
                        result?;
                        while let Ok(item)=receiver.try_recv(){append_match(&mut output,item,&path,directory,context,&cancellation).await?;if output.matches>=limit{reached=true;backend_cancel.cancel();break;}}
                        break;
                    }
                }
            }
            Ok(output.finish(reached, limit))
        };
        let result = tokio::select! {biased;_=cancellation.cancelled()=>Err(Error::Cancelled),result=operation=>result};
        match result {
            Ok(outcome) => outcome,
            Err(Error::Cancelled) => Outcome::Failed("Operation aborted".into()),
            Err(Error::Config(message)) => Outcome::Failed(message),
            Err(error) => Outcome::Failed(error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn windows_shell_paths_support_drive_prefixes() {
        assert_eq!(windows_shell_path("/mnt/c/a"), "C:\\a");
        assert_eq!(windows_shell_path("//host/share"), "//host/share");
    }
    #[tokio::test]
    async fn formatting_is_bounded_and_cancellable() {
        let token = CancellationToken::new();
        let mut output = Listing::new();
        let path = "p".repeat(60 * 1024);
        for _ in 0..20 {
            output.line(&path, 1, true, "x", &token).await.unwrap();
        }
        assert!(output.prefix.len() <= MAX_BYTES + 1);
        token.cancel();
        assert!(matches!(
            output.line("p", 2, true, "x", &token).await,
            Err(Error::Cancelled)
        ));
    }
}
