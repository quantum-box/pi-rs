//! Model-facing remote directory listing for hosts composing their own tool set.
use pi_rs::{Error, Extension, Outcome, Result, Tool, ToolContext, ToolExecution, ToolSpec};
use serde_json::{Map, Value, json};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

const MAX_BYTES: usize = 50 * 1024;
const UNLIMITED_LINES: u64 = 9_007_199_254_740_991;

/// Directory metadata needed by the listing tool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LsStat {
    pub is_directory: bool,
}

/// Remote listing operations. Paths are absolute. The backend enforces its
/// access policy and stops work when an operation future is dropped.
#[async_trait::async_trait]
pub trait LsOperations: Send + Sync {
    async fn exists(&self, path: &Path) -> Result<bool>;
    async fn stat(&self, path: &Path) -> Result<LsStat>;
    /// Return entry names, not absolute or relative paths.
    async fn readdir(&self, path: &Path) -> Result<Vec<String>>;
}

/// Configuration for remote path resolution.
#[derive(Clone, Debug, Default)]
pub struct LsToolOptions {
    /// Absolute home directory on the backend. Required to expand `~`.
    pub remote_home: Option<PathBuf>,
}
/// Construct an unrestricted listing tool. `~` requires a configured remote home.
pub fn create_ls_tool(root: &Path, operations: Arc<dyn LsOperations>) -> Result<Arc<dyn Tool>> {
    create_ls_tool_with_options(root, operations, LsToolOptions::default())
}
/// Construct a listing tool with backend path configuration.
pub fn create_ls_tool_with_options(
    root: &Path,
    operations: Arc<dyn LsOperations>,
    options: LsToolOptions,
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
    Ok(Arc::new(RemoteLs {
        root: resolve(root, Path::new(".")),
        operations,
        options,
    }))
}
/// Register only the remote ls tool in an explicitly composed tool set.
pub struct LsExtension {
    tool: Arc<dyn Tool>,
}
impl LsExtension {
    pub fn new(root: &Path, operations: Arc<dyn LsOperations>) -> Result<Self> {
        Self::with_options(root, operations, LsToolOptions::default())
    }
    pub fn with_options(
        root: &Path,
        operations: Arc<dyn LsOperations>,
        options: LsToolOptions,
    ) -> Result<Self> {
        Ok(Self {
            tool: create_ls_tool_with_options(root, operations, options)?,
        })
    }
}

impl Extension for LsExtension {
    fn id(&self) -> &str {
        "remote-ls"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![self.tool.clone()]
    }
}
struct RemoteLs {
    root: PathBuf,
    operations: Arc<dyn LsOperations>,
    options: LsToolOptions,
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
// Retain only the bounded display prefix while counting complete output for
// truncation metadata. Yield periodically so cancellation can interrupt work.
struct Listing {
    prefix: Vec<u8>,
    total_bytes: usize,
    newlines: usize,
    ends_in_newline: bool,
}
impl Listing {
    fn new() -> Self {
        Self {
            prefix: Vec::with_capacity(MAX_BYTES + 1),
            total_bytes: 0,
            newlines: 0,
            ends_in_newline: false,
        }
    }
    async fn append(
        &mut self,
        bytes: &[u8],
        cancellation: &pi_rs::CancellationToken,
    ) -> Result<()> {
        for (index, chunk) in bytes.chunks(8192).enumerate() {
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            self.total_bytes = self.total_bytes.saturating_add(chunk.len());
            self.newlines += chunk.iter().filter(|byte| **byte == b'\n').count();
            self.ends_in_newline = chunk.last() == Some(&b'\n');
            let keep = (MAX_BYTES + 1)
                .saturating_sub(self.prefix.len())
                .min(chunk.len());
            self.prefix.extend_from_slice(&chunk[..keep]);
            if index % 8 == 7 {
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    }
    fn finish(mut self, reached: bool, limit: usize) -> Outcome {
        let mut details = Map::new();
        let mut notices = Vec::new();
        if reached {
            details.insert("entryLimitReached".into(), json!(limit));
            notices.push(format!(
                "{limit} entries limit reached. Use limit={} for more",
                limit.saturating_mul(2)
            ));
        }
        if self.total_bytes > MAX_BYTES {
            let end = self.prefix.iter().rposition(|byte| *byte == b'\n');
            self.prefix.truncate(end.unwrap_or(0));
            let output_lines = end.map_or(0, |_| {
                self.prefix.iter().filter(|byte| **byte == b'\n').count() + 1
            });
            let content = String::from_utf8(self.prefix).expect("truncated at a newline");
            let total_lines = self.newlines + usize::from(!self.ends_in_newline);
            details.insert("truncation".into(), json!({"content":content,"truncated":true,"truncatedBy":"bytes","totalLines":total_lines,"totalBytes":self.total_bytes,"outputLines":output_lines,"outputBytes":content.len(),"lastLinePartial":false,"firstLineExceedsLimit":end.is_none(),"maxLines":UNLIMITED_LINES,"maxBytes":MAX_BYTES}));
            notices.push("50.0KB limit reached".into());
            return finish(content, details, notices);
        }
        finish(
            String::from_utf8(self.prefix).expect("complete UTF-8 output"),
            details,
            notices,
        )
    }
}
fn finish(mut output: String, details: Map<String, Value>, notices: Vec<String>) -> Outcome {
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

fn directory_collator() -> Result<icu_collator::CollatorBorrowed<'static>> {
    let locale = ["LC_ALL", "LANG"]
        .into_iter()
        .filter_map(|key| std::env::var(key).ok())
        .find(|value| !value.is_empty())
        .unwrap_or_default();
    let locale = locale
        .split(['.', '@'])
        .next()
        .unwrap_or_default()
        .replace('_', "-");
    let preferences = locale
        .parse::<icu_locale_core::Locale>()
        .map(icu_collator::CollatorPreferences::from)
        .unwrap_or_default();
    icu_collator::Collator::try_new(preferences, Default::default())
        .map_err(|error| Error::Config(format!("Cannot initialize directory collation: {error}")))
}
fn valid_name(name: &str) -> bool {
    if name.is_empty() || name.contains('/') || (cfg!(windows) && name.contains('\\')) {
        return false;
    }
    let mut components = Path::new(name).components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}
#[async_trait::async_trait]
impl Tool for RemoteLs {
    fn spec(&self) -> ToolSpec {
        ToolSpec { name:"ls".into(), description:"List directory contents. Returns entries sorted alphabetically, with '/' suffix for directories. Includes dotfiles. Output is truncated to 500 entries or 50KB (whichever is hit first).".into(), parameters:json!({"type":"object","properties":{"path":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":100000}},"required":[],"additionalProperties":false}) }
    }
    fn execution(&self) -> ToolExecution {
        ToolExecution::ParallelRead
    }
    async fn invoke(&self, args: Value, context: ToolContext) -> Outcome {
        let cancellation = context.cancellation.child_token();
        let _guard = cancellation.clone().drop_guard();
        let operation = async {
            let requested = match args.get("path") {
                None => ".",
                Some(Value::String(path)) if path.is_empty() => ".",
                Some(Value::String(path)) => path,
                Some(_) => return Err(Error::Config("path must be a string".into())),
            };
            let limit = match args.get("limit") {
                None => 500,
                Some(value) => value
                    .as_u64()
                    .filter(|n| (1..=100_000).contains(n))
                    .ok_or_else(|| {
                        Error::Config("limit must be an integer between 1 and 100000".into())
                    })? as usize,
            };
            let path = checked_resolve(
                &self.root,
                &normalize(requested, self.options.remote_home.as_deref())?,
            )?;
            if !self.operations.exists(&path).await? {
                return Err(Error::Config(format!("Path not found: {}", path.display())));
            }
            if !self.operations.stat(&path).await?.is_directory {
                return Err(Error::Config(format!(
                    "Not a directory: {}",
                    path.display()
                )));
            }
            let entries = self
                .operations
                .readdir(&path)
                .await
                .map_err(|error| Error::Config(format!("Cannot read directory: {error}")))?;
            let sort_token = cancellation.clone();
            let entries = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
                let collator = directory_collator()?;
                let mut keyed = Vec::with_capacity(entries.len());
                for entry in entries {
                    if sort_token.is_cancelled() {
                        return Err(Error::Cancelled);
                    }
                    // Reject backend-supplied paths before any entry stat calls.
                    if !valid_name(&entry) {
                        return Err(Error::Config(
                            "ls backend returned an invalid entry name".into(),
                        ));
                    }
                    keyed.push((entry.to_lowercase(), entry));
                }
                let mut entries = keyed;
                entries.sort_by(|a, b| collator.compare(&a.0, &b.0));
                if sort_token.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                Ok(entries.into_iter().map(|(_, entry)| entry).collect())
            })
            .await
            .map_err(|error| Error::Config(error.to_string()))??;
            let mut listing = Listing::new();
            let mut count = 0;
            let mut processed = 0usize;
            let mut reached = false;
            for entry in entries {
                if cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                if count >= limit {
                    reached = true;
                    break;
                }
                let stat = self.operations.stat(&path.join(&entry)).await;
                processed += 1;
                // Immediately-ready backend futures must not monopolize a
                // current-thread runtime, even when every stat fails.
                if processed.is_multiple_of(64) {
                    tokio::task::yield_now().await;
                }
                if cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                if let Ok(stat) = stat {
                    if count != 0 {
                        listing.append(b"\n", &cancellation).await?;
                    }
                    listing.append(entry.as_bytes(), &cancellation).await?;
                    if stat.is_directory {
                        listing.append(b"/", &cancellation).await?;
                    }
                    count += 1;
                }
            }
            if count == 0 {
                return Ok(Outcome::Succeeded("(empty directory)".into()));
            }
            Ok(listing.finish(reached, limit))
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
    #[tokio::test]
    async fn listing_keeps_bounded_prefix_and_checks_cancellation() {
        let token = pi_rs::CancellationToken::new();
        let mut listing = Listing::new();
        let chunk = "entry\n".repeat(2000);
        for _ in 0..100 {
            listing.append(chunk.as_bytes(), &token).await.unwrap();
        }
        assert!(listing.prefix.len() <= MAX_BYTES + 1);
        assert_eq!(listing.total_bytes, chunk.len() * 100);
        assert!(
            matches!(listing.finish(false, 500), Outcome::SucceededWithDetails { ref details, .. } if details["truncation"]["totalBytes"] == chunk.len() * 100 && details["truncation"]["totalLines"] == 200000)
        );
        let mut listing = Listing::new();
        token.cancel();
        assert!(matches!(
            listing.append(b"next", &token).await,
            Err(Error::Cancelled)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn backslash_is_a_valid_unix_filename() {
        assert!(valid_name("literal\\"));
    }

    #[test]
    fn windows_shell_paths_preserve_unc() {
        assert_eq!(windows_shell_path("/mnt/c/a"), "C:\\a");
        assert_eq!(windows_shell_path("//host/share"), "//host/share");
    }
}
