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
// Keep only the display prefix while counting the complete output for Pi's
// truncation metadata. Conversion and accounting proceed in bounded chunks.
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
    fn append(&mut self, bytes: &[u8], cancellation: &pi_rs::CancellationToken) -> Result<()> {
        for chunk in bytes.chunks(8192) {
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            self.total_bytes = self.total_bytes.saturating_add(chunk.len());
            self.newlines += chunk.iter().filter(|byte| **byte == b'\n').count();
            self.ends_in_newline = chunk.last() == Some(&b'\n');
            let keep = (MAX_BYTES + 1)
                .saturating_sub(self.prefix.len())
                .min(chunk.len());
            #[cfg(windows)]
            self.prefix.extend(
                chunk[..keep]
                    .iter()
                    .map(|byte| if *byte == b'\\' { b'/' } else { *byte }),
            );
            #[cfg(not(windows))]
            self.prefix.extend_from_slice(&chunk[..keep]);
        }
        Ok(())
    }
    fn finish(mut self, reached: bool, limit: usize) -> Outcome {
        let mut details = Map::new();
        let mut notices = Vec::new();
        if reached {
            details.insert("resultLimitReached".into(), json!(limit));
            notices.push(format!("{limit} results limit reached"));
        }
        if self.total_bytes > MAX_BYTES {
            let end = self.prefix.iter().rposition(|byte| *byte == b'\n');
            self.prefix.truncate(end.unwrap_or(0));
            let output_lines = end.map_or(0, |_| {
                self.prefix.iter().filter(|byte| **byte == b'\n').count() + 1
            });
            let content = String::from_utf8(self.prefix).expect("truncated at a newline");
            let total_lines = self.newlines + usize::from(!self.ends_in_newline);
            details.insert("truncation".into(), json!({"content":content, "truncated":true,"truncatedBy":"bytes", "totalLines":total_lines,"totalBytes":self.total_bytes,"outputLines":output_lines,"outputBytes":content.len(),"lastLinePartial":false,"firstLineExceedsLimit":end.is_none(),"maxLines":UNLIMITED_LINES,"maxBytes":MAX_BYTES}));
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

#[cfg(unix)]
fn append_os_path(
    listing: &mut Listing,
    value: &std::ffi::OsStr,
    token: &pi_rs::CancellationToken,
) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let mut bytes = value.as_bytes();
    while !bytes.is_empty() {
        if token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let mut end = bytes.len().min(8192);
        // Extend a chunk by at most three bytes to avoid splitting UTF-8.
        while end < bytes.len()
            && matches!(std::str::from_utf8(&bytes[..end]), Err(error) if error.error_len().is_none())
        {
            end += 1;
        }
        match std::str::from_utf8(&bytes[..end]) {
            Ok(_) => {
                listing.append(&bytes[..end], token)?;
                bytes = &bytes[end..];
            }
            Err(error) => {
                let valid = error.valid_up_to();
                listing.append(&bytes[..valid], token)?;
                listing.append("�".as_bytes(), token)?;
                let invalid = error.error_len().unwrap_or(bytes.len() - valid);
                bytes = &bytes[valid + invalid..];
            }
        }
    }
    Ok(())
}
#[cfg(windows)]
fn append_os_path(
    listing: &mut Listing,
    value: &std::ffi::OsStr,
    token: &pi_rs::CancellationToken,
) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    let mut chunk = String::with_capacity(8192);
    for ch in std::char::decode_utf16(value.encode_wide()) {
        if token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        chunk.push(ch.unwrap_or('�'));
        if chunk.len() >= 8192 {
            listing.append(chunk.as_bytes(), token)?;
            chunk.clear();
        }
    }
    listing.append(chunk.as_bytes(), token)
}
#[cfg(not(any(unix, windows)))]
fn append_os_path(
    listing: &mut Listing,
    value: &std::ffi::OsStr,
    token: &pi_rs::CancellationToken,
) -> Result<()> {
    listing.append(value.to_string_lossy().as_bytes(), token)
}
fn format_results(
    results: Vec<PathBuf>,
    base: &Path,
    limit: usize,
    cancellation: &pi_rs::CancellationToken,
) -> Result<Outcome> {
    let reached = results.len() >= limit;
    let mut listing = Listing::new();
    for (index, path) in results.iter().take(limit).enumerate() {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let raw = path.as_os_str().as_encoded_bytes();
        let trailing = raw.last() == Some(&b'/') || (cfg!(windows) && raw.last() == Some(&b'\\'));
        let relative = if path.is_absolute() {
            path.strip_prefix(base).map_err(|_| {
                Error::Config("find backend returned a path outside the search directory".into())
            })?
        } else {
            path
        };
        if relative.as_os_str().is_empty() {
            return Err(Error::Config("find backend returned an empty path".into()));
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
                    "find backend returned a path outside the search directory".into(),
                ));
            }
        }
        if index != 0 {
            listing.append(b"\n", cancellation)?;
        }
        append_os_path(&mut listing, relative.as_os_str(), cancellation)?;
        // Separators are already present in the relative path; Path can drop a
        // trailing slash during stripping, so restore it if needed.
        if trailing
            && !relative
                .as_os_str()
                .as_encoded_bytes()
                .last()
                .is_some_and(|byte| *byte == b'/' || (cfg!(windows) && *byte == b'\\'))
        {
            listing.append(b"/", cancellation)?;
        }
    }
    Ok(listing.finish(reached, limit))
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
            let worker_token = cancellation.clone();
            tokio::task::spawn_blocking(move || {
                format_results(results, &path, limit, &worker_token)
            })
            .await
            .map_err(|error| Error::Config(error.to_string()))?
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
    fn bounded_listing_counts_full_output_and_observes_cancellation() {
        let token = pi_rs::CancellationToken::new();
        let mut listing = Listing::new();
        let chunk = "entry\n".repeat(2000);
        for _ in 0..100 {
            listing.append(chunk.as_bytes(), &token).unwrap();
        }
        assert!(listing.prefix.len() <= MAX_BYTES + 1);
        assert_eq!(listing.total_bytes, chunk.len() * 100);
        let outcome = listing.finish(false, 1000);
        assert!(
            matches!(outcome, Outcome::SucceededWithDetails { ref output, ref details } if output.len() <= MAX_BYTES + 100 && details["truncation"]["totalBytes"] == chunk.len() * 100 && details["truncation"]["totalLines"] == 200000)
        );
        let mut cancelled = Listing::new();
        cancelled.append(b"first", &token).unwrap();
        token.cancel();
        assert!(matches!(
            cancelled.append(b"second", &token),
            Err(Error::Cancelled)
        ));
        assert!(matches!(
            format_results(vec![PathBuf::from("entry")], Path::new("/"), 1000, &token),
            Err(Error::Cancelled)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn unix_backslash_is_filename_and_invalid_utf8_is_lossy() {
        use std::os::unix::ffi::OsStringExt;
        let paths = vec![
            PathBuf::from("literal\\"),
            PathBuf::from(std::ffi::OsString::from_vec(vec![b'x', 0xff, b'y'])),
        ];
        let result = format_results(
            paths,
            Path::new("/"),
            1000,
            &pi_rs::CancellationToken::new(),
        )
        .unwrap();
        assert!(matches!(result, Outcome::Succeeded(text) if text == "literal\\\nx�y"));
    }

    #[cfg(windows)]
    #[test]
    fn windows_backslash_is_directory_separator() {
        let result = format_results(
            vec![PathBuf::from("folder\\")],
            Path::new("C:\\"),
            1000,
            &pi_rs::CancellationToken::new(),
        )
        .unwrap();
        assert!(matches!(result, Outcome::Succeeded(text) if text == "folder/"));
    }

    #[test]
    fn windows_shell_paths_support_drive_prefixes_and_preserve_unc() {
        assert_eq!(windows_shell_path("/mnt/c/a"), "C:\\a");
        assert_eq!(windows_shell_path("/cygdrive/d/file"), "D:\\file");
        assert_eq!(windows_shell_path("//host/share"), "//host/share");
    }
}
