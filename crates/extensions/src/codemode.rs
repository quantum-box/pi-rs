//! A bounded QuickJS runner that reaches external effects only through ToolHost.
use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine;
use pi_rs_core::{
    Extension, ExtensionResult, ExtensionState, ModelRequest, Outcome, RequestPurpose, Tool,
    ToolContext, ToolHost, ToolReply, ToolSpec,
};
use rquickjs::{
    AsyncContext, AsyncRuntime, Promise,
    function::{Async, Func},
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::tool_search::{SearchDocument, rank};

const ID: &str = "codemode";
const MAX_OUTPUT: usize = 16 * 1024 * 1024;
const MAX_ITEMS: usize = 100_000;

pub struct Codemode {
    pub only: bool,
    pub inline_budget: usize,
    pub timeout: Duration,
}
impl Default for Codemode {
    fn default() -> Self {
        Self {
            only: false,
            inline_budget: 3000,
            timeout: Duration::from_secs(30),
        }
    }
}
#[async_trait::async_trait]
impl Extension for Codemode {
    fn id(&self) -> &str {
        ID
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(ScriptTool {
            timeout: self.timeout,
        })]
    }
    fn migrate(
        &self,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        if previous.is_some_and(|state| state.version != 1) {
            return Err("unsupported codemode store version".into());
        }
        Ok(previous.is_none().then(|| ExtensionState {
            version: 1,
            data: json!({}),
        }))
    }
    async fn before_request(
        &self,
        request: &mut ModelRequest,
        _: RequestPurpose,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        let mut declarations = String::new();
        for tool in request
            .tools
            .iter()
            .filter(|tool| !matches!(tool.name.as_str(), "codemode" | "tool_search"))
        {
            let entry = describe(tool);
            if declarations.len() + entry.len() > self.inline_budget.saturating_mul(4) {
                break;
            }
            declarations.push_str(&entry);
            declarations.push('\n');
        }
        if let Some(tool) = request
            .tools
            .iter_mut()
            .find(|tool| tool.name == "codemode")
        {
            tool.description
                .push_str("\n\nAvailable tool declarations\n");
            tool.description.push_str(&declarations);
            if self.only {
                request
                    .tools
                    .retain(|tool| matches!(tool.name.as_str(), "codemode" | "tool_search"));
            }
        }
        Ok(())
    }
}
struct ScriptTool {
    timeout: Duration,
}
#[async_trait::async_trait]
impl Tool for ScriptTool {
    fn output_limit(&self) -> usize {
        8 * 1024 * 1024
    }
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: ID.into(),
            description: "Run JavaScript in a QuickJS sandbox. Use await tools.<name>(args), text(value), image(value), console.log, return, exit(), store(key,value), load(key), ALL_TOOLS, searchTools(query), describeTool(name), and describeNamespace(name). The sandbox has no filesystem, network, Node APIs or timers. Only emitted output reaches the model. A first-line // @options: JSON object may set max_output_tokens and timeout_ms. Store writes commit only after success.".into(),
            parameters: json!({"type":"object","properties":{"code":{"type":"string","minLength":1}},"required":["code"],"additionalProperties":false}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Failed("codemode requires the native tool host".into())
    }
    async fn invoke_transaction(
        &self,
        arguments: Value,
        context: ToolContext,
        previous: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ToolReply {
        let started = Instant::now();
        let Some(code) = arguments["code"].as_str() else {
            return Outcome::Failed("code must be a string".into()).into();
        };
        let (options, source) = match options(code, self.timeout) {
            Ok(value) => value,
            Err(error) => return Outcome::Failed(error).into(),
        };
        if previous.is_some_and(|state| state.version != 1 || !state.data.is_object()) {
            return Outcome::Failed("invalid codemode store state".into()).into();
        }
        static WORKERS: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
        let workers = WORKERS.get_or_init(|| {
            let maximum = std::env::var("PI_CODEMODE_MAX_WORKERS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value > 0 && *value <= 1024)
                .unwrap_or(2);
            tokio::sync::Semaphore::new(maximum)
        });
        let _permit = tokio::select! {
            _ = context.cancellation.cancelled() => return Outcome::Failed("Script cancelled while waiting for a worker".into()).into(),
            permit = tokio::time::timeout(options.timeout.min(Duration::from_secs(60)), workers.acquire()) => match permit {
                Ok(Ok(permit)) => permit,
                _ => return Outcome::Failed("Timed out waiting for a codemode worker".into()).into(),
            }
        };
        let Some(remaining) = options
            .timeout
            .checked_sub(started.elapsed())
            .filter(|time| !time.is_zero())
        else {
            return Outcome::Failed("Script deadline elapsed before execution".into()).into();
        };
        let store = previous
            .map(|state| state.data.clone())
            .unwrap_or_else(|| json!({}));
        let sink = Arc::new(Mutex::new(Output::default()));
        let result = evaluate(source, store, host, context, remaining, sink.clone()).await;
        let (success, changed, store, error) = match result {
            Ok(result) => (result.ok, result.changed, result.store, result.error),
            Err(error) => (false, false, json!({}), Some(error)),
        };
        let output = sink.lock().expect("codemode output").finish(
            success,
            started.elapsed(),
            error.as_deref(),
            options.max_output_tokens,
        );
        let outcome = if success {
            Outcome::Structured {
                data: output,
                attachments: vec![],
            }
        } else {
            Outcome::Failed(
                output["content"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|block| block["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n"),
            )
        };
        ToolReply {
            outcome,
            state: (success && changed).then_some(ExtensionState {
                version: 1,
                data: store,
            }),
        }
    }
}
struct Options {
    max_output_tokens: usize,
    timeout: Duration,
}
#[derive(Deserialize)]
struct WireOptions {
    max_output_tokens: Option<usize>,
    timeout_ms: Option<u64>,
}
fn options(code: &str, maximum: Duration) -> Result<(Options, &str), String> {
    let first = code.lines().next().unwrap_or_default();
    let parsed = first
        .strip_prefix("// @options:")
        .map(|value| serde_json::from_str::<WireOptions>(value).map_err(|error| error.to_string()))
        .transpose()?;
    if code
        .lines()
        .skip(1)
        .any(|line| line.trim_start().starts_with("// @options:"))
    {
        return Err("options must be on the first line".into());
    }
    let tokens = parsed
        .as_ref()
        .and_then(|options| options.max_output_tokens)
        .unwrap_or(10_000);
    let timeout = parsed
        .and_then(|options| options.timeout_ms)
        .map(Duration::from_millis)
        .unwrap_or(maximum)
        .min(maximum);
    if tokens == 0 || timeout.is_zero() {
        return Err("output and timeout limits must be positive".into());
    }
    let source = if first.starts_with("// @options:") {
        code.split_once('\n').map(|(_, rest)| rest).unwrap_or("")
    } else {
        code
    };
    Ok((
        Options {
            max_output_tokens: tokens,
            timeout,
        },
        source,
    ))
}
#[derive(Deserialize)]
struct Completion {
    ok: bool,
    changed: bool,
    store: Value,
    error: Option<String>,
}

async fn evaluate(
    source: &str,
    store: Value,
    host: Arc<dyn ToolHost>,
    context: ToolContext,
    timeout: Duration,
    sink: Arc<Mutex<Output>>,
) -> Result<Completion, String> {
    let runtime = AsyncRuntime::new().map_err(|error| error.to_string())?;
    runtime.set_memory_limit(256 * 1024 * 1024).await;
    runtime.set_max_stack_size(512 * 1024).await;
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or("Invalid script deadline")?;
    let cancellation = context.cancellation.clone();
    runtime
        .set_interrupt_handler(Some(Box::new(move || {
            cancellation.is_cancelled() || Instant::now() >= deadline
        })))
        .await;
    let javascript = AsyncContext::full(&runtime)
        .await
        .map_err(|error| error.to_string())?;
    let documents: Arc<Vec<_>> = Arc::new(
        host.tools()
            .into_iter()
            .filter(|tool| !matches!(tool.name.as_str(), "codemode" | "tool_search"))
            .map(|tool| SearchDocument {
                namespace: tool_namespace(&tool.name),
                tool,
            })
            .collect(),
    );
    let code = format!("__run(async () => {{\n{source}\n}})");
    let operation = javascript.async_with(async move |ctx| {
        let result: rquickjs::Result<String> = async {
            install_globals(&ctx, documents, host, store, sink)?;
            match ctx.eval::<Promise, _>(code) {
                Ok(promise) => promise.into_future::<String>().await,
                Err(error) => Err(error),
            }
        }
        .await;
        result.map_err(|error| {
            let value = ctx.catch();
            let detail = value
                .as_exception()
                .map(|exception| {
                    format!(
                        "{}\n{}",
                        exception.message().unwrap_or_default(),
                        exception.stack().unwrap_or_default()
                    )
                })
                .unwrap_or_default();
            format!("{error} {detail}")
        })
    });
    let result = tokio::select! {
        _ = context.cancellation.cancelled() => return Err("Script cancelled".into()),
        result = tokio::time::timeout(timeout, operation) => result.map_err(|_| "Script timed out".to_owned())?,
    }?;
    serde_json::from_str(&result).map_err(|error| error.to_string())
}
fn install_globals(
    ctx: &rquickjs::Ctx<'_>,
    documents: Arc<Vec<SearchDocument>>,
    host: Arc<dyn ToolHost>,
    store: Value,
    sink: Arc<Mutex<Output>>,
) -> rquickjs::Result<()> {
    let globals = ctx.globals();
    let catalogue: Vec<_> = documents
        .iter()
        .map(|document| json!({"name":document.tool.name,"description":document.tool.description}))
        .collect();
    globals.set(
        "__catalog",
        ctx.json_parse(serde_json::to_string(&catalogue).unwrap())?,
    )?;
    globals.set("__initial_store", ctx.json_parse(store.to_string())?)?;
    globals.set(
        "__emit",
        Func::from(move |kind: String, value: String| {
            sink.lock()
                .expect("codemode output")
                .emit(&kind, value)
                .err()
                .unwrap_or_default()
        }),
    )?;
    globals.set(
        "__call_tool",
        Func::from(Async(move |name: String, arguments: String| {
            let host = host.clone();
            async move {
                let result = match serde_json::from_str(&arguments) {
                    Ok(_) if matches!(name.as_str(), "codemode" | "tool_search") => {
                        json!({"ok":false,"error":"This tool is not callable from a script"})
                    }
                    Ok(arguments) => tool_value(host.call(&name, arguments).await),
                    Err(error) => json!({"ok":false,"error":error.to_string()}),
                };
                Ok::<_, rquickjs::Error>(result.to_string())
            }
        })),
    )?;
    let searchable = documents.clone();
    globals.set("__search_tools", Func::from(move |query: String, options: String| {
        let options: Value = serde_json::from_str(&options).unwrap_or_default();
        let limit = options["limit"].as_u64().unwrap_or(8).min(1000) as usize;
        let wanted = options["namespace"].as_str().map(normalize_namespace);
        let documents: Vec<_> = searchable.iter().filter(|document| wanted.as_ref().is_none_or(|wanted| tool_namespace(&document.tool.name) == *wanted)).cloned().collect();
        let matches: Vec<_> = rank(&query, &documents, limit).into_iter().map(|(index, _)| json!({"name":documents[index].tool.name,"description":documents[index].tool.description})).collect();
        serde_json::to_string(&matches).unwrap()
    }))?;
    let described = documents.clone();
    globals.set("__describe_tool", Func::from(move |name: String| {
        described.iter().find(|document| document.tool.name == name || document.tool.name.replace('-', "_") == name).map(|document| json!({"name":document.tool.name,"description":document.tool.description,"declaration":describe(&document.tool)})).unwrap_or(Value::Null).to_string()
    }))?;
    globals.set("__describe_namespace", Func::from(move |name: String| {
        let name = normalize_namespace(&name);
        let tools: Vec<_> = documents.iter().filter(|document| tool_namespace(&document.tool.name) == name).map(|document| json!({"name":document.tool.name,"description":document.tool.description,"declaration":describe(&document.tool)})).collect();
        if tools.is_empty() { "null".into() } else { json!({"name":name,"tools":tools}).to_string() }
    }))?;
    ctx.eval::<(), _>(include_str!("codemode-bootstrap.js"))?;
    Ok(())
}
fn tool_value(outcome: Outcome) -> Value {
    match outcome {
        Outcome::Succeeded(value) => json!({"ok":true,"value":value}),
        Outcome::Structured { data, .. } => json!({"ok":true,"value":data}),
        outcome => json!({"ok":false,"error":outcome.tool_content()}),
    }
}
fn tool_namespace(name: &str) -> String {
    if name.starts_with("mcp__") {
        normalize_namespace(name)
    } else {
        "builtin".into()
    }
}
fn normalize_namespace(name: &str) -> String {
    let name = name.trim().replace('-', "_");
    if matches!(name.as_str(), "builtin" | "pi") {
        return "builtin".into();
    }
    let name = name.strip_prefix("mcp__").unwrap_or(&name);
    format!("mcp__{}", name.split("__").next().unwrap_or(name))
}
fn describe(tool: &ToolSpec) -> String {
    format!(
        "{}\n{}(args: {}): Promise<unknown>;",
        tool.description,
        tool.name.replace('-', "_"),
        typescript(&tool.parameters)
    )
}
fn typescript(schema: &Value) -> String {
    match schema["type"].as_str() {
        Some("string") => "string".into(),
        Some("number" | "integer") => "number".into(),
        Some("boolean") => "boolean".into(),
        Some("array") => format!("{}[]", typescript(&schema["items"])),
        Some("object") => {
            let fields = schema["properties"]
                .as_object()
                .map(|properties| {
                    properties
                        .iter()
                        .map(|(name, value)| {
                            let required = schema["required"]
                                .as_array()
                                .is_some_and(|names| names.iter().any(|value| value == name));
                            format!(
                                "{}{}: {}",
                                serde_json::to_string(name).unwrap(),
                                if required { "" } else { "?" },
                                typescript(value)
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("; ")
                })
                .unwrap_or_default();
            format!("{{ {fields} }}")
        }
        _ => "unknown".into(),
    }
}

#[derive(Default)]
struct Output {
    items: Vec<Item>,
    console: Vec<String>,
    bytes: usize,
    calls: usize,
    saved_images: std::collections::BTreeMap<String, std::result::Result<String, String>>,
}
enum Item {
    Text(String),
    Image {
        mime: String,
        data: String,
        path: std::result::Result<String, String>,
    },
}
impl Output {
    fn emit(&mut self, kind: &str, value: String) -> Result<(), String> {
        self.calls += 1;
        self.bytes = self.bytes.saturating_add(value.len());
        if self.calls > MAX_ITEMS || self.bytes > MAX_OUTPUT {
            return Err("Script output limit exceeded".into());
        }
        match kind {
            "text" => self.items.push(Item::Text(value)),
            "console" => self.console.push(value),
            "image" => {
                let block: Value =
                    serde_json::from_str(&value).map_err(|error| error.to_string())?;
                let mime = block["mimeType"].as_str().unwrap_or_default();
                let suffix = match mime {
                    "image/png" => ".png",
                    "image/jpeg" => ".jpg",
                    "image/gif" => ".gif",
                    "image/webp" => ".webp",
                    _ => return Err("Unsupported image type".into()),
                };
                let data = block["data"].as_str().ok_or("Missing image data")?;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .map_err(|_| "Invalid image data".to_owned())?;
                let valid = match mime {
                    "image/png" => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
                    "image/jpeg" => bytes.starts_with(&[0xff, 0xd8, 0xff]),
                    "image/gif" => bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a"),
                    "image/webp" => bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP"),
                    _ => false,
                };
                if !valid {
                    return Err("Image data does not match its MIME type".into());
                }
                let path = self
                    .saved_images
                    .entry(data.to_owned())
                    .or_insert_with(|| {
                        save_output("pi-image-", suffix, &bytes).map_err(|error| error.to_string())
                    })
                    .clone();
                self.items.push(Item::Image {
                    mime: mime.into(),
                    data: data.into(),
                    path,
                });
            }
            _ => return Err("Unknown output kind".into()),
        }
        Ok(())
    }
    fn finish(
        &self,
        success: bool,
        elapsed: Duration,
        error: Option<&str>,
        tokens: usize,
    ) -> Value {
        let mut content = vec![json!({"type":"text","text":format!(
            "Script {}\nWall time: {:.3}s\n",
            if success { "completed" } else { "failed" }, elapsed.as_secs_f64()
        )})];
        let text_count = self
            .items
            .iter()
            .filter(|item| matches!(item, Item::Text(_)))
            .count();
        let mut index = 0;
        for item in &self.items {
            match item {
                Item::Text(text) => {
                    index += 1;
                    let text = if text_count > 1 {
                        format!("==> text {index}/{text_count} <==\n{text}")
                    } else {
                        text.clone()
                    };
                    content.push(json!({"type":"text","text":text}));
                }
                Item::Image { mime, data, path } => {
                    let label = match path {
                        Ok(path) => format!("Image saved to {path}"),
                        Err(error) => format!("Image could not be saved: {error}"),
                    };
                    content.push(json!({"type":"text","text":label}));
                    content.push(json!({"type":"image","mimeType":mime,"data":data}));
                }
            }
        }
        if !self.console.is_empty() {
            content.push(json!({"type":"text","text":format!("<console_output>\n{}\n</console_output>", self.console.join("\n"))}));
        }
        if let Some(error) = error {
            content.push(json!({"type":"text","text":format!("Script error: {error}")}));
        }
        let mut merged: Vec<Value> = Vec::new();
        for block in content {
            if block["type"] == "text"
                && let Some(last) = merged.last_mut().filter(|last| last["type"] == "text")
                && let Some(Value::String(text)) = last.get_mut("text")
            {
                let next = block["text"].as_str().unwrap_or_default();
                if !text.ends_with('\n') && !next.starts_with('\n') {
                    text.push('\n');
                }
                text.push_str(next);
            } else {
                merged.push(block);
            }
        }
        let mut content = merged;
        let report = content
            .iter()
            .filter_map(|block| block["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let maximum = tokens.saturating_mul(4).min(100 * 1024);
        let characters: Vec<_> = report.chars().collect();
        let mut full_output_path = None;
        if characters.len() > maximum {
            let (location, path) = match save_output("pi-output-", ".txt", report.as_bytes()) {
                Ok(path) => (format!("Full output: {path}"), Some(path)),
                Err(error) => (format!("Could not save the full output: {error}"), None),
            };
            full_output_path = path;
            let head = maximum / 2;
            let tail = maximum - head;
            let report = format!(
                "Warning: truncated output (original token count: {})\nTotal output lines: {}\n\n{}…{} tokens truncated…{}\n\n[{location}]",
                characters.len().div_ceil(4),
                report.lines().count(),
                characters[..head].iter().collect::<String>(),
                (characters.len() - maximum).div_ceil(4),
                characters[characters.len() - tail..]
                    .iter()
                    .collect::<String>()
            );
            content.retain(|block| block["type"] == "image");
            content.insert(0, json!({"type":"text","text":report}));
        }
        let text_size = content
            .iter()
            .filter(|block| block["type"] == "text")
            .map(|block| block.to_string().len())
            .sum::<usize>();
        let mut image_budget = (7 * 1024 * 1024_usize).saturating_sub(text_size);
        let mut omitted = 0;
        content.retain(|block| {
            if block["type"] != "image" {
                return true;
            }
            let size = block.to_string().len();
            if size > image_budget {
                omitted += 1;
                return false;
            }
            image_budget -= size;
            true
        });
        if omitted != 0 {
            content.push(json!({"type":"text","text":format!("{omitted} image(s) omitted from inline output because of the result size limit. Image locations and save errors are included in the text output.")}));
        }
        json!({"content":content,"isError":!success,"full_output_path":full_output_path,"wall_time_seconds":elapsed.as_secs_f64()})
    }
}

fn save_output(prefix: &str, suffix: &str, bytes: &[u8]) -> std::io::Result<String> {
    let mut file = tempfile::Builder::new()
        .prefix(prefix)
        .suffix(suffix)
        .tempfile()?;
    file.write_all(bytes)?;
    let (_, path) = file.keep().map_err(|error| error.error)?;
    Ok(path.to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Host;
    #[async_trait::async_trait]
    impl ToolHost for Host {
        fn tools(&self) -> Vec<ToolSpec> {
            vec![ToolSpec {
                name: "echo".into(),
                description: "Echo a value".into(),
                parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
            }]
        }
        async fn call(&self, name: &str, arguments: Value) -> Outcome {
            if name == "echo" {
                Outcome::Succeeded(arguments["text"].as_str().unwrap_or_default().into())
            } else {
                Outcome::Failed("unknown tool".into())
            }
        }
    }
    fn run(code: &str, previous: Option<&ExtensionState>) -> ToolReply {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(
                ScriptTool {
                    timeout: Duration::from_secs(2),
                }
                .invoke_transaction(
                    json!({"code":code}),
                    ToolContext {
                        operation_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                        cancellation: Default::default(),
                    },
                    previous,
                    Arc::new(Host),
                ),
            )
    }
    #[test]
    fn scripts_call_tools_and_commit_private_store_without_io_globals() {
        let reply = run(
            "text([typeof process, typeof require, typeof fetch, typeof setTimeout]); text(await tools.echo({text:'hello'})); console.log('log', 1); store('value', {n:7}); return load('value').n;",
            None,
        );
        let Outcome::Structured { data, .. } = reply.outcome else {
            panic!("{:?}", reply.outcome);
        };
        let text = data["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("undefined"));
        assert!(text.contains("hello"));
        assert!(text.contains("log 1"));
        assert_eq!(reply.state.as_ref().unwrap().data["value"]["n"], 7);
        assert!(!data.to_string().contains("\"value\""));
        let next = run("return load('value').n", reply.state.as_ref());
        assert!(next.outcome.tool_content().contains('7'));
    }
    #[test]
    fn failed_scripts_keep_output_and_rollback_store() {
        let state = ExtensionState {
            version: 1,
            data: json!({"value":1}),
        };
        let reply = run(
            "store('value',2); text('partial output'); throw new Error('fixture failure');",
            Some(&state),
        );
        assert!(reply.state.is_none());
        assert!(
            matches!(&reply.outcome, Outcome::Failed(message) if message.contains("partial output") && message.contains("fixture failure"))
        );
        assert_eq!(state.data["value"], 1);
    }
    #[test]
    fn deadline_interrupts_busy_loops_and_unresolved_promises() {
        for source in ["while (true) {}", "await new Promise(() => {})"] {
            let started = Instant::now();
            let reply = run(
                &format!("// @options: {{\"timeout_ms\":20}}\n{source}"),
                None,
            );
            assert!(matches!(reply.outcome, Outcome::Failed(_)));
            assert!(started.elapsed() < Duration::from_secs(1));
        }
    }
    #[test]
    fn metadata_helpers_and_output_truncation_are_available() {
        let reply = run(
            "const found = await searchTools('echo'); text(found[0].name); text((await describeTool('echo')).declaration); return (await describeNamespace('builtin')).tools.length;",
            None,
        );
        assert!(reply.outcome.tool_content().contains("Echo a value"));
        let reply = run(
            "// @options: {\"max_output_tokens\":10}\ntext('x'.repeat(1000));",
            None,
        );
        let Outcome::Structured { data, .. } = reply.outcome else {
            panic!();
        };
        let path = data["full_output_path"].as_str().unwrap();
        assert!(
            std::fs::read_to_string(path)
                .unwrap()
                .contains(&"x".repeat(1000))
        );
        assert!(
            data["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("Warning: truncated output")
        );
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn memory_and_stack_limits_reject_excessive_allocations() {
        for code in [
            "new ArrayBuffer(512 * 1024 * 1024)",
            "function recurse() { recurse(); } recurse();",
        ] {
            assert!(matches!(run(code, None).outcome, Outcome::Failed(_)));
        }
        assert!(matches!(
            run("store('huge', 'x'.repeat(262145))", None).outcome,
            Outcome::Failed(_)
        ));
    }
    #[test]
    fn images_preserve_output_order_and_reuse_saved_files() {
        let data = base64::engine::general_purpose::STANDARD.encode(b"\x89PNG\r\n\x1a\n\0");
        let image = json!({"type":"image","mimeType":"image/png","data":data}).to_string();
        let mut output = Output::default();
        output.emit("text", "before".into()).unwrap();
        output.emit("image", image.clone()).unwrap();
        output.emit("text", "between".into()).unwrap();
        output.emit("image", image).unwrap();
        output.emit("text", "after".into()).unwrap();
        let value = output.finish(true, Duration::ZERO, None, 10000);
        let blocks = value["content"].as_array().unwrap();
        assert_eq!(
            blocks
                .iter()
                .map(|block| block["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["text", "image", "text", "image", "text"]
        );
        assert!(blocks[0]["text"].as_str().unwrap().contains("before"));
        assert!(blocks[2]["text"].as_str().unwrap().contains("between"));
        assert!(blocks[4]["text"].as_str().unwrap().contains("after"));
        assert_eq!(output.saved_images.len(), 1);
        std::fs::remove_file(
            output
                .saved_images
                .values()
                .next()
                .unwrap()
                .as_ref()
                .unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn image_save_failures_keep_successful_output_and_inline_content() {
        let output = Output {
            items: vec![Item::Image {
                mime: "image/png".into(),
                data: "image".into(),
                path: Err("disk unavailable".into()),
            }],
            ..Output::default()
        };
        let value = output.finish(true, Duration::ZERO, None, 10000);
        assert_eq!(value["isError"], false);
        assert!(
            value["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("could not be saved")
        );
        assert_eq!(value["content"][1]["type"], "image");
    }

    struct LargeImageModel;
    #[async_trait::async_trait]
    impl pi_rs_core::Model for LargeImageModel {
        async fn stream(
            &self,
            request: ModelRequest,
        ) -> pi_rs_core::Result<pi_rs_core::ModelStream> {
            use pi_rs_core::{Message, ModelEvent, ToolCall};
            let events = if let Some(content) = request.messages.iter().find_map(|message| {
                if let Message::Tool { content, .. } = message {
                    Some(content)
                } else {
                    None
                }
            }) {
                let value: Value = serde_json::from_str(content).unwrap();
                let blocks = value["data"]["content"].as_array().unwrap();
                let image = blocks
                    .iter()
                    .find(|block| block["type"] == "image")
                    .unwrap();
                assert!(image["data"].as_str().unwrap().len() > 1024 * 1024);
                let label = blocks[0]["text"].as_str().unwrap();
                let path = label
                    .lines()
                    .find_map(|line| line.strip_prefix("Image saved to "))
                    .unwrap();
                std::fs::remove_file(path).unwrap();
                vec![
                    Ok(ModelEvent::Text("done".into())),
                    Ok(ModelEvent::Complete {
                        calls: vec![],
                        usage: None,
                    }),
                ]
            } else {
                let prefix =
                    base64::engine::general_purpose::STANDARD.encode(b"\x89PNG\r\n\x1a\n\0");
                let code = format!(
                    "image({{type:'image',mimeType:'image/png',data:'{prefix}'+'AAAA'.repeat(300000)}}); store('saved',true);"
                );
                vec![Ok(ModelEvent::Complete {
                    calls: vec![ToolCall {
                        id: "image-script".into(),
                        name: "codemode".into(),
                        arguments: json!({"code":code}).to_string(),
                    }],
                    usage: None,
                })]
            };
            Ok(Box::pin(futures_util::stream::iter(events)))
        }
    }
    #[tokio::test]
    async fn large_inline_images_survive_the_journal_result_limit() {
        let mut session = pi_rs_core::SessionBuilder::new(Arc::new(LargeImageModel), "image")
            .extension(Arc::new(Codemode::default()))
            .unwrap()
            .in_memory()
            .unwrap();
        assert_eq!(session.prompt("image").await.unwrap(), "done");
        assert_eq!(session.journal().extension_states()[ID].data["saved"], true);
        session.close().await.unwrap();
    }

    struct ParallelTool(Arc<tokio::sync::Barrier>);
    #[async_trait::async_trait]
    impl Tool for ParallelTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "barrier".into(),
                description: "Concurrent read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> pi_rs_core::ToolExecution {
            pi_rs_core::ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.wait().await;
            Outcome::Succeeded("read".into())
        }
    }
    struct ParallelExtension;
    impl Extension for ParallelExtension {
        fn id(&self) -> &str {
            "parallel-test"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(ParallelTool(Arc::new(tokio::sync::Barrier::new(
                2,
            ))))]
        }
    }
    struct ParallelModel;
    #[async_trait::async_trait]
    impl pi_rs_core::Model for ParallelModel {
        async fn stream(
            &self,
            request: ModelRequest,
        ) -> pi_rs_core::Result<pi_rs_core::ModelStream> {
            use pi_rs_core::{Message, ModelEvent, ToolCall};
            let events = if let Some(content) = request.messages.iter().find_map(|message| {
                if let Message::Tool { content, .. } = message {
                    Some(content)
                } else {
                    None
                }
            }) {
                assert!(content.contains("Script completed"), "{content}");
                assert!(content.contains("read,read"), "{content}");
                vec![
                    Ok(ModelEvent::Text("done".into())),
                    Ok(ModelEvent::Complete {
                        calls: vec![],
                        usage: None,
                    }),
                ]
            } else {
                vec![Ok(ModelEvent::Complete {
                    calls: vec![ToolCall {
                        id: "script".into(), name: "codemode".into(),
                        arguments: json!({"code":"text((await Promise.all([tools.barrier({}), tools.barrier({})])).join(','));"}).to_string(),
                    }], usage: None,
                })]
            };
            Ok(Box::pin(futures_util::stream::iter(events)))
        }
    }
    #[tokio::test]
    async fn javascript_parallel_calls_use_the_journaled_host() {
        let mut session = pi_rs_core::SessionBuilder::new(Arc::new(ParallelModel), "fixture")
            .extension(Arc::new(Codemode::default()))
            .unwrap()
            .extension(Arc::new(ParallelExtension))
            .unwrap()
            .in_memory()
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), session.prompt("parallel"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result, "done");
        assert_eq!(session.journal().operations().len(), 3);
        session.close().await.unwrap();
    }
    #[test]
    fn options_are_restricted_to_the_first_line() {
        let reply = run("text('first');\n// @options: {}", None);
        assert!(matches!(reply.outcome, Outcome::Failed(_)));
    }
}
