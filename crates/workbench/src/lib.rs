//! Representative Rust extensions, composed by the CLI or embedded by a host.
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use pi_rs::{
    Compaction, Decision, Error, Extension, ExtensionResult, ExtensionState, Message, ModelRequest,
    Outcome, Result, Tool, ToolContext, ToolExecution, ToolSpec,
};
use reqwest::{Client, Url};
use serde_json::{Value, json};
use tokio::io::AsyncReadExt;

const MAX_FILE: u64 = 64 * 1024;
const MAX_COMMAND_INPUT: u64 = 1024 * 1024;

pub mod coding;
mod coding_paths;
mod file_search;
pub mod mcp;
pub mod mcp_auth;
mod mcp_provider_auth;
#[doc(hidden)]
pub mod oauth_callback;
mod shell_output;
mod text_edit;
mod text_page;

pub fn agent_dir() -> PathBuf {
    std::env::var_os("PI_RS_HOME")
        .or_else(|| std::env::var_os("PI_CODING_AGENT_DIR"))
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .map(|home| PathBuf::from(home).join(".pi-rs"))
        })
        .unwrap_or_else(|| std::env::temp_dir().join("pi-rs"))
}

pub struct Tools {
    root: PathBuf,
    orders: Option<Arc<OrderLookup>>,
}

impl Tools {
    pub fn new(root: &Path, orders: Option<(&str, Option<String>)>) -> Result<Self> {
        let root = root.canonicalize()?;
        if !root.is_dir() {
            return Err(Error::Config("workspace root must be a directory".into()));
        }
        let orders = orders
            .map(|(endpoint, token)| OrderLookup::new(endpoint, token).map(Arc::new))
            .transpose()?;
        Ok(Self { root, orders })
    }
}

impl Extension for Tools {
    fn id(&self) -> &str {
        "workbench-tools"
    }

    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        let mut tools: Vec<Arc<dyn Tool>> = vec![
            Arc::new(FileTool {
                root: self.root.clone(),
                kind: FileKind::Read,
            }),
            Arc::new(FileTool {
                root: self.root.clone(),
                kind: FileKind::Count,
            }),
        ];
        if let Some(orders) = &self.orders {
            tools.push(orders.clone());
        }
        tools
    }
}

pub struct CommandPolicy {
    pub allow_commands: bool,
}

#[async_trait::async_trait]
impl Extension for CommandPolicy {
    fn id(&self) -> &str {
        "workbench-command-policy"
    }

    async fn before_tool(
        &self,
        name: &str,
        _: &Value,
        _: &ToolContext,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<Decision> {
        Ok(
            if matches!(name, "count_lines" | "bash" | "powershell") && !self.allow_commands {
                Decision::Deny("host command policy requires --allow-commands".into())
            } else {
                Decision::Allow
            },
        )
    }
}

pub struct ProjectInstructions {
    pub resources: pi_rs::ProjectResources,
    pub skills: Vec<String>,
}

impl Extension for ProjectInstructions {
    fn id(&self) -> &str {
        "workbench-project"
    }

    fn context(
        &self,
        request: &mut ModelRequest,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        let Some(Message::System { content }) = request.messages.first_mut() else {
            return Err("system message missing".into());
        };
        content.push_str(
            &self
                .resources
                .context(&self.skills)
                .map_err(|error| error.to_string())?,
        );
        Ok(())
    }
}

/// Add a host-provided instruction and persist branch-local diagnostics across rebuilds.
pub struct WorkbenchContext {
    pub instruction: Option<String>,
}

impl Extension for WorkbenchContext {
    fn id(&self) -> &str {
        "workbench-context"
    }

    fn migrate(
        &self,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        match previous {
            None => Ok(Some(state(0, 0))),
            Some(previous) if previous.version == 1 => {
                let tools = previous.data.as_u64().ok_or("invalid version 1 state")?;
                Ok(Some(state(tools, 0)))
            }
            Some(previous) if previous.version == 2 => {
                counts(Some(previous))?;
                Ok(None)
            }
            Some(_) => Err("unsupported extension state version".into()),
        }
    }

    fn context(
        &self,
        request: &mut ModelRequest,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        if let Some(instruction) = &self.instruction {
            let Some(Message::System { content }) = request.messages.first_mut() else {
                return Err("system message missing".into());
            };
            content.push_str("\n\n");
            content.push_str(instruction);
        }
        Ok(())
    }

    fn after_tool(
        &self,
        _: &str,
        _: &Outcome,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        let (tools, compactions) = counts(previous)?;
        Ok(Some(state(tools.saturating_add(1), compactions)))
    }

    fn after_compaction(
        &self,
        _: &Compaction,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        let (tools, compactions) = counts(previous)?;
        Ok(Some(state(tools, compactions.saturating_add(1))))
    }
}

fn counts(previous: Option<&ExtensionState>) -> ExtensionResult<(u64, u64)> {
    let previous = previous.ok_or("extension state was not initialized")?;
    let tools = previous.data["tool_results"]
        .as_u64()
        .ok_or("invalid tool result count")?;
    let compactions = previous.data["compactions"]
        .as_u64()
        .ok_or("invalid compaction count")?;
    Ok((tools, compactions))
}

fn state(tools: u64, compactions: u64) -> ExtensionState {
    ExtensionState {
        version: 2,
        data: json!({"tool_results": tools, "compactions": compactions}),
    }
}

#[derive(Clone, Copy)]
enum FileKind {
    Read,
    Count,
}

struct FileTool {
    root: PathBuf,
    kind: FileKind,
}

#[async_trait::async_trait]
impl Tool for FileTool {
    fn spec(&self) -> ToolSpec {
        let (name, description) = match self.kind {
            FileKind::Read => (
                "read_text",
                "Read the complete UTF-8 contents of a workspace text file.",
            ),
            FileKind::Count => (
                "count_lines",
                "Count newline characters in a workspace file using wc -l.",
            ),
        };
        ToolSpec {
            name: name.into(),
            description: description.into(),
            parameters: string_argument("path"),
        }
    }

    fn execution(&self) -> ToolExecution {
        match self.kind {
            FileKind::Read => ToolExecution::ParallelRead,
            FileKind::Count => ToolExecution::Sequential,
        }
    }

    async fn invoke(&self, arguments: Value, _: ToolContext) -> Outcome {
        let Some(path) = arguments["path"].as_str() else {
            return Outcome::Failed("missing path".into());
        };
        let result = match self.kind {
            FileKind::Read => self.read(path).await,
            FileKind::Count => self.count(path).await,
        };
        match result {
            Ok(output) => Outcome::Succeeded(output),
            Err(reason) => Outcome::Failed(reason),
        }
    }
}

impl FileTool {
    async fn file(&self, path: &str, limit: u64) -> ExtensionResult<tokio::fs::File> {
        let path = Path::new(path);
        if path.as_os_str().is_empty()
            || path
                .components()
                .any(|part| !matches!(part, Component::Normal(_)))
        {
            return Err("expected a relative workspace file path".into());
        }
        let path = tokio::fs::canonicalize(self.root.join(path))
            .await
            .map_err(|_| "file not found")?;
        if !path.starts_with(&self.root) {
            return Err("file is outside the workspace".into());
        }
        let metadata = tokio::fs::metadata(&path)
            .await
            .map_err(|_| "file metadata unavailable")?;
        if !metadata.is_file() || metadata.len() > limit {
            return Err("expected a regular file within the size limit".into());
        }
        tokio::fs::File::open(&path)
            .await
            .map_err(|_| "cannot open file".into())
    }

    async fn read(&self, path: &str) -> ExtensionResult<String> {
        let mut output = String::new();
        self.file(path, MAX_FILE)
            .await?
            .take(MAX_FILE + 1)
            .read_to_string(&mut output)
            .await
            .map_err(|_| "cannot read UTF-8 file")?;
        if output.len() as u64 > MAX_FILE {
            return Err("file exceeds size limit".into());
        }
        Ok(output)
    }

    async fn count(&self, path: &str) -> ExtensionResult<String> {
        let file = self.file(path, MAX_COMMAND_INPUT).await?;
        let output = tokio::process::Command::new("/usr/bin/wc")
            .arg("-l")
            .env_clear()
            .stdin(Stdio::from(file.into_std().await))
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|_| "could not run wc")?;
        if !output.status.success() {
            return Err("wc failed".into());
        }
        let count = std::str::from_utf8(&output.stdout)
            .map_err(|_| "invalid wc output")?
            .trim()
            .parse::<u64>()
            .map_err(|_| "invalid wc count")?;
        Ok(count.to_string())
    }
}

struct OrderLookup {
    client: Client,
    base: Url,
    token: Option<String>,
}

impl OrderLookup {
    fn new(endpoint: &str, token: Option<String>) -> Result<Self> {
        let base =
            Url::parse(endpoint).map_err(|_| Error::Config("invalid orders endpoint".into()))?;
        let local = matches!(base.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
        if !(base.scheme() == "https" || (base.scheme() == "http" && local))
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(Error::Config("orders endpoint must use HTTPS or loopback HTTP and contain no credentials, query, or fragment".into()));
        }
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|_| Error::Config("could not build orders HTTP client".into()))?;
        Ok(Self {
            client,
            base,
            token,
        })
    }
}

#[async_trait::async_trait]
impl Tool for OrderLookup {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "lookup_order".into(),
            description: "Retrieve an order by its exact order ID.".into(),
            parameters: string_argument("id"),
        }
    }

    fn execution(&self) -> ToolExecution {
        ToolExecution::ParallelRead
    }

    async fn invoke(&self, arguments: Value, _: ToolContext) -> Outcome {
        let id = arguments["id"].as_str().unwrap_or_default();
        if id.is_empty()
            || id.len() > 64
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Outcome::Failed("invalid order ID".into());
        }
        match self.lookup(id).await {
            Ok(body) => Outcome::Succeeded(body),
            Err(error) => Outcome::Failed(error),
        }
    }
}

impl OrderLookup {
    async fn lookup(&self, id: &str) -> ExtensionResult<String> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|_| "invalid orders endpoint")?
            .pop_if_empty()
            .push(id);
        let mut request = self.client.get(url);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let mut response = request.send().await.map_err(|_| "orders request failed")?;
        if !response.status().is_success() {
            return Err(format!("orders returned HTTP {}", response.status()));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| "orders response interrupted")?
        {
            if body.len() + chunk.len() > MAX_FILE as usize {
                return Err("orders response exceeds size limit".into());
            }
            body.extend_from_slice(&chunk);
        }
        let value: Value =
            serde_json::from_slice(&body).map_err(|_| "orders response is not JSON")?;
        Ok(value.to_string())
    }
}

fn string_argument(name: &str) -> Value {
    json!({
        "type": "object",
        "properties": {(name): {"type": "string", "minLength": 1}},
        "required": [name],
        "additionalProperties": false
    })
}
