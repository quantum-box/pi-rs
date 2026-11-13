//! Model-facing remote find for hosts composing their own tool set.
use pi_rs::{Error, Extension, Outcome, Result, Tool, ToolContext, ToolExecution, ToolSpec};
use serde_json::{Map, Value, json};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

const MAX_BYTES: usize = 50 * 1024;
const UNLIMITED_LINES: u64 = 9_007_199_254_740_991;

/// Search options passed to the backend.
#[derive(Clone, Debug)]
pub struct FindOptions {
    pub ignore: Vec<String>,
    pub limit: usize,
}

/// Remote find operations. Paths are absolute and the backend enforces its own
/// access policy and ignore rules. Results must be within the search directory.
/// Implementations must honor the limit and stop work when a future is dropped.
#[async_trait::async_trait]
pub trait FindOperations: Send + Sync {
    async fn exists(&self, path: &Path) -> Result<bool>;
    async fn glob(&self, pattern: &str, cwd: &Path, options: FindOptions) -> Result<Vec<PathBuf>>;
}

/// Construct an unrestricted model-facing find tool with an absolute backend cwd.
pub fn create_find_tool(root: &Path, operations: Arc<dyn FindOperations>) -> Result<Arc<dyn Tool>> {
    if !root.is_absolute() {
        return Err(Error::Config(
            "remote working directory must be absolute".into(),
        ));
    }
    Ok(Arc::new(RemoteFind {
        root: resolve(root, Path::new(".")),
        operations,
    }))
}

/// Register only this remote find tool in an explicitly composed tool set.
pub struct FindExtension {
    tool: Arc<dyn Tool>,
}
impl FindExtension {
    pub fn new(root: &Path, operations: Arc<dyn FindOperations>) -> Result<Self> {
        Ok(Self {
            tool: create_find_tool(root, operations)?,
        })
    }
}
impl Extension for FindExtension {
    fn id(&self) -> &str {
        "remote-find"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![self.tool.clone()]
    }
}
struct RemoteFind {
    root: PathBuf,
    operations: Arc<dyn FindOperations>,
}

fn normalize(value: &str) -> Result<PathBuf> {
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
        let home = if cfg!(windows) {
            std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME"))
        } else {
            std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))
        }
        .ok_or_else(|| Error::Config("home directory is unavailable".into()))?;
        return Ok(if value == "~" {
            PathBuf::from(home)
        } else {
            PathBuf::from(home).join(&value[2..])
        });
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
fn display_path(path: &Path, base: &Path) -> Result<String> {
    let trailing = path.to_string_lossy().ends_with(['/', '\\']);
    let relative = if path.is_absolute() {
        path.strip_prefix(base).map_err(|_| {
            Error::Config("find backend returned a path outside the search directory".into())
        })?
    } else {
        path
    };
    if relative.components().any(|c| {
        matches!(
            c,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(Error::Config(
            "find backend returned a path outside the search directory".into(),
        ));
    }
    let mut display = relative
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/");
    if display.is_empty() {
        return Err(Error::Config("find backend returned an empty path".into()));
    }
    if trailing && !display.ends_with('/') {
        display.push('/');
    }
    Ok(display)
}
fn listing(
    mut output: String,
    mut details: Map<String, Value>,
    mut notices: Vec<String>,
) -> Outcome {
    if output.len() > MAX_BYTES {
        let total_lines = output.split_terminator('\n').count();
        let total_bytes = output.len();
        let end = output.as_bytes()[..=MAX_BYTES]
            .iter()
            .rposition(|byte| *byte == b'\n');
        let first_line_exceeds_limit = end.is_none();
        output.truncate(end.unwrap_or(0));
        let output_lines = end.map_or(0, |_| {
            output.bytes().filter(|byte| *byte == b'\n').count() + 1
        });
        details.insert("truncation".into(), json!({"content":output, "truncated":true,"truncatedBy":"bytes", "totalLines":total_lines,"totalBytes":total_bytes,"outputLines":output_lines,"outputBytes":output.len(),"lastLinePartial":false,"firstLineExceedsLimit":first_line_exceeds_limit,"maxLines":UNLIMITED_LINES,"maxBytes":MAX_BYTES}));
        notices.push("50.0KB limit reached".into());
    }
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
#[async_trait::async_trait]
impl Tool for RemoteFind {
    fn spec(&self) -> ToolSpec {
        ToolSpec { name:"find".into(), description:"Search for files by glob pattern. Returns matching file paths relative to the search directory. Respects .gitignore. Output is truncated to 1000 results or 50KB (whichever is hit first).".into(), parameters:json!({"type":"object","properties":{"pattern":{"type":"string"},"path":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":100000}},"required":["pattern"],"additionalProperties":false}) }
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
                .ok_or_else(|| Error::Config("pattern must be a string".into()))?;
            let requested = match args.get("path") {
                None => ".",
                Some(Value::String(path)) if path.is_empty() => ".",
                Some(Value::String(path)) => path,
                Some(_) => return Err(Error::Config("path must be a string".into())),
            };
            let limit = match args.get("limit") {
                None => 1000,
                Some(value) => value
                    .as_u64()
                    .filter(|n| (1..=100_000).contains(n))
                    .ok_or_else(|| {
                        Error::Config("limit must be an integer between 1 and 100000".into())
                    })? as usize,
            };
            let path = resolve(&self.root, &normalize(requested)?);
            if !self.operations.exists(&path).await? {
                return Err(Error::Config(format!("Path not found: {}", path.display())));
            }
            let results = self
                .operations
                .glob(
                    pattern,
                    &path,
                    FindOptions {
                        ignore: vec!["**/node_modules/**".into(), "**/.git/**".into()],
                        limit,
                    },
                )
                .await?;
            if results.is_empty() {
                return Ok(Outcome::Succeeded("No files found matching pattern".into()));
            }
            let reached = results.len() >= limit;
            let mut output = Vec::with_capacity(results.len().min(limit));
            for result in results.iter().take(limit) {
                if cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                output.push(display_path(result, &path)?);
            }
            let mut details = Map::new();
            let mut notices = Vec::new();
            if reached {
                details.insert("resultLimitReached".into(), json!(limit));
                notices.push(format!("{limit} results limit reached"));
            }
            Ok(listing(output.join("\n"), details, notices))
        };
        let result = tokio::select! { biased; _ = cancellation.cancelled() => Err(Error::Cancelled), result = operation => result };
        match result {
            Ok(output) => output,
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
    fn windows_shell_paths_support_drive_prefixes_and_preserve_unc() {
        assert_eq!(windows_shell_path("/mnt/c/a"), "C:\\a");
        assert_eq!(windows_shell_path("/cygdrive/d/file"), "D:\\file");
        assert_eq!(windows_shell_path("//host/share"), "//host/share");
    }
}
