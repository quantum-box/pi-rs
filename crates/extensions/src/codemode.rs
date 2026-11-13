//! A bounded QuickJS runner that reaches external effects only through ToolHost.
#[cfg(test)]
use pi_rs_core::ModelRequest;
use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine;
use pi_rs_core::{
    Extension, ExtensionResult, ExtensionState, Outcome, RequestPurpose, Tool, ToolContext,
    ToolHost, ToolReply, ToolSpec,
};
use rquickjs::{
    AsyncContext, AsyncRuntime, Promise,
    function::{Async, Func},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::tool_search::{SearchDocument, rank};

const ID: &str = "codemode";
const MODEL_OPERATION: &str = "pi_model_operation";
const MAX_OUTPUT: usize = 16 * 1024 * 1024;
const MAX_ITEMS: usize = 100_000;

pub struct Codemode {
    pub only: bool,
    pub inline_budget: usize,
    /// Maximum script duration; `Duration::MAX` means no deadline.
    pub timeout: Duration,
}
impl Default for Codemode {
    fn default() -> Self {
        Self {
            only: false,
            inline_budget: 3000,
            timeout: Duration::MAX,
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
            timeout: (self.timeout != Duration::MAX).then_some(self.timeout),
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
        request: &mut pi_rs_core::ContextRequest<'_>,
        _: RequestPurpose,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<pi_rs_core::ContextChange> {
        let original_tools = request.tools.clone();
        let mut declarations = String::new();
        for tool in request.tools.iter().filter(|tool| {
            !matches!(
                tool.name.as_str(),
                "codemode" | "tool_search" | MODEL_OPERATION
            )
        }) {
            let entry = describe(tool);
            if declarations.len() + entry.len() > self.inline_budget.saturating_mul(4) {
                break;
            }
            declarations.push_str(&entry);
            declarations.push('\n');
        }
        if let Some(tool) = request
            .tools_mut()
            .iter_mut()
            .find(|tool| tool.name == "codemode")
        {
            tool.description
                .push_str("\n\nAvailable tool declarations\n");
            tool.description.push_str(&declarations);
            if self.only {
                request
                    .tools_mut()
                    .retain(|tool| matches!(tool.name.as_str(), "codemode" | "tool_search"));
            }
        }
        Ok(if request.tools == original_tools {
            pi_rs_core::ContextChange::Unchanged
        } else {
            pi_rs_core::ContextChange::Changed
        })
    }
}
struct ScriptTool {
    timeout: Option<Duration>,
}
#[async_trait::async_trait]
impl Tool for ScriptTool {
    fn use_default_timeout(&self) -> bool {
        false
    }
    fn output_limit(&self) -> usize {
        8 * 1024 * 1024
    }
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: ID.into(),
            description: "Run JavaScript in a QuickJS sandbox. Use await tools.<name>(args), models.getAvailableOfType(type), models.classify(model, context), models.generateImages(model, context), text(value), image(value), console.log, return, exit(), store(key,value), load(key), ALL_TOOLS, searchTools(query), describeTool(name), and describeNamespace(name). The sandbox has no filesystem, network, Node APIs or timers. Only emitted output reaches the model. A first-line // @options: JSON object may set max_output_tokens and timeout_ms. Store writes commit only after success.".into(),
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
        self.invoke_transaction_with_timeout(arguments, context, previous, host, None)
            .await
    }
    async fn invoke_transaction_with_timeout(
        &self,
        arguments: Value,
        context: ToolContext,
        previous: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
        host_timeout: Option<Duration>,
    ) -> ToolReply {
        let started = Instant::now();
        let Some(code) = arguments["code"].as_str() else {
            return Outcome::Failed("code must be a string".into()).into();
        };
        let maximum = match (self.timeout, host_timeout) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let (options, source) = match options(code, maximum) {
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
        let acquire = async {
            match options.timeout {
                Some(timeout) => tokio::time::timeout(timeout, workers.acquire())
                    .await
                    .map_err(|_| "Timed out waiting for a codemode worker")?,
                None => workers.acquire().await,
            }
            .map_err(|_| "Codemode worker pool closed")
        };
        let permit = tokio::select! {
            _ = context.cancellation.cancelled() => return Outcome::Failed("Script cancelled while waiting for a worker".into()).into(),
            permit = acquire => match permit {
                Ok(permit) => permit,
                Err(error) => return Outcome::Failed(error.into()).into(),
            }
        };
        let store = previous
            .map(|state| state.data.clone())
            .unwrap_or_else(|| json!({}));
        let sink = Arc::new(Mutex::new(Output::default()));
        let worker_sink = sink.clone();
        let worker_cancel = context.cancellation.child_token();
        // The caller may drop this future on timeout or cancellation. Keep the
        // permit in the worker until QuickJS has actually stopped, and wake its
        // interrupt handler even when nobody remains to await the join handle.
        let _cancel_on_drop = worker_cancel.clone().drop_guard();
        let worker_context = ToolContext {
            operation_id: context.operation_id,
            cancellation: worker_cancel,
        };
        let source = source.to_owned();
        let handle = tokio::runtime::Handle::current();
        let timeout = options.timeout;
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let remaining = match timeout {
                Some(timeout) => {
                    match timeout
                        .checked_sub(started.elapsed())
                        .filter(|time| !time.is_zero())
                    {
                        Some(remaining) => Some(remaining),
                        None => return Err("Script deadline elapsed before execution".into()),
                    }
                }
                None => None,
            };
            handle.block_on(evaluate(
                &source,
                store,
                host,
                worker_context,
                remaining,
                worker_sink,
            ))
        })
        .await
        .unwrap_or_else(|error| Err(format!("Codemode worker failed: {error}")));
        let limit_exceeded = sink.lock().expect("codemode output").limit_exceeded;
        let (success, changed, store, error) = match result {
            _ if limit_exceeded => (
                false,
                false,
                json!({}),
                Some("Script output limit exceeded".into()),
            ),
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
    timeout: Option<Duration>,
}
fn options(code: &str, maximum: Option<Duration>) -> Result<(Options, &str), String> {
    const PREFIX: &str = "// @options:";
    const FIELDS: &str = "`max_output_tokens` and `timeout_ms`";
    if code.trim().is_empty() {
        return Err("Expected JavaScript source text (non-empty)".into());
    }
    for (index, line) in code.split('\n').enumerate().skip(1) {
        if line.trim_start().starts_with(PREFIX) {
            return Err(format!(
                "The {PREFIX} line must be the first line of the input; found one on line {}",
                index + 1
            ));
        }
    }
    let first = code.split('\n').next().unwrap_or_default().trim_start();
    let Some(directive) = first.strip_prefix(PREFIX) else {
        return Ok((
            Options {
                max_output_tokens: 10_000,
                timeout: maximum,
            },
            code,
        ));
    };
    // Keep the newline so errors point at the original source line.
    let source = code.find('\n').map(|index| &code[index..]).unwrap_or("");
    if source.trim().is_empty() {
        return Err(
            "The @options line must be followed by JavaScript source on subsequent lines".into(),
        );
    }
    let directive = directive.trim();
    if directive.is_empty() {
        return Err(format!(
            "@options must be a JSON object with supported fields {FIELDS}"
        ));
    }
    let value: Value = serde_json::from_str(directive).map_err(|error| {
        format!("@options must be valid JSON with supported fields {FIELDS}: {error}")
    })?;
    let fields = value
        .as_object()
        .ok_or_else(|| format!("@options must be a JSON object with supported fields {FIELDS}"))?;
    for key in fields.keys() {
        if key != "max_output_tokens" && key != "timeout_ms" {
            return Err(format!("@options only supports {FIELDS}; got `{key}`"));
        }
    }
    let integer = |name: &str, limit: u64| -> Result<Option<u64>, String> {
        let Some(value) = fields.get(name) else {
            return Ok(None);
        };
        let number = value
            .as_f64()
            .filter(|n| *n >= 0.0 && *n <= limit as f64 && n.fract() == 0.0);
        number.map(|n| Some(n as u64)).ok_or_else(|| match name {
            "timeout_ms" => {
                "@options field `timeout_ms` must be a positive integer up to 2147483647".into()
            }
            _ => "@options field `max_output_tokens` must be a non-negative safe integer".into(),
        })
    };
    let tokens = integer("max_output_tokens", 9_007_199_254_740_991)?.unwrap_or(10_000);
    let timeout = integer("timeout_ms", 2_147_483_647)?;
    if timeout == Some(0) {
        return Err(
            "@options field `timeout_ms` must be a positive integer up to 2147483647".into(),
        );
    }
    Ok((
        Options {
            max_output_tokens: usize::try_from(tokens).unwrap_or(usize::MAX),
            timeout: match (timeout.map(Duration::from_millis), maximum) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            },
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
    timeout: Option<Duration>,
    sink: Arc<Mutex<Output>>,
) -> Result<Completion, String> {
    let runtime = AsyncRuntime::new().map_err(|error| error.to_string())?;
    runtime.set_memory_limit(256 * 1024 * 1024).await;
    runtime.set_max_stack_size(512 * 1024).await;
    let deadline = timeout
        .map(|timeout| {
            Instant::now()
                .checked_add(timeout)
                .ok_or("Invalid script deadline")
        })
        .transpose()?;
    let cancellation = context.cancellation.clone();
    runtime
        .set_interrupt_handler(Some(Box::new(move || {
            cancellation.is_cancelled()
                || deadline.is_some_and(|deadline| Instant::now() >= deadline)
        })))
        .await;
    let javascript = AsyncContext::full(&runtime)
        .await
        .map_err(|error| error.to_string())?;
    let documents: Arc<Vec<_>> = Arc::new(
        host.tools()
            .into_iter()
            .filter(|tool| {
                !matches!(
                    tool.name.as_str(),
                    "codemode" | "tool_search" | MODEL_OPERATION
                )
            })
            .map(|tool| SearchDocument {
                namespace: tool_namespace(&tool.name),
                tool,
            })
            .collect(),
    );
    let code = format!("__run(async () => {{\n{source}\n}})");
    let output_cancellation = context.cancellation.clone();
    let operation = javascript.async_with(async move |ctx| {
        let result: rquickjs::Result<String> = async {
            install_globals(&ctx, documents, host, store, sink, output_cancellation)?;
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
        result = async {
            match timeout {
                Some(timeout) => tokio::time::timeout(timeout, operation).await.map_err(|_| "Script timed out".to_owned())?,
                None => operation.await,
            }
        } => result,
    };
    if context.cancellation.is_cancelled() {
        return Err("Script cancelled".into());
    }
    let result = result?;
    serde_json::from_str(&result).map_err(|error| error.to_string())
}
fn install_globals(
    ctx: &rquickjs::Ctx<'_>,
    documents: Arc<Vec<SearchDocument>>,
    host: Arc<dyn ToolHost>,
    store: Value,
    sink: Arc<Mutex<Output>>,
    cancellation: pi_rs_core::CancellationToken,
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
    let model_usage = sink.clone();
    let tool_output = sink.clone();
    let output_cancel = cancellation.clone();
    let model_cancel = cancellation.clone();
    globals.set(
        "__emit",
        Func::from(move |kind: String, value: String| {
            let mut output = sink.lock().expect("codemode output");
            let error = output.emit(&kind, value).err().unwrap_or_default();
            if output.limit_exceeded {
                output_cancel.cancel();
            }
            error
        }),
    )?;
    let model_host = host.clone();
    let has_operations = model_host
        .tools()
        .iter()
        .any(|tool| tool.name == MODEL_OPERATION);
    let model_slots = Arc::new(tokio::sync::Semaphore::new(4));
    globals.set(
        "__call_model_operation",
        Func::from(Async(move |arguments: String| {
            let model_host = model_host.clone();
            let slots = model_slots.clone();
            let model_usage = model_usage.clone();
            let cancellation = model_cancel.clone();
            async move {
                let result = match serde_json::from_str::<Value>(&arguments) {
                    Ok(args) if has_operations => {
                        let _permit = slots.acquire().await.expect("model operation slots");
                        if model_usage.lock().expect("codemode output").limit_exceeded {
                            return Ok::<_, rquickjs::Error>(
                                json!({"ok":false,"error":"Script output limit exceeded"})
                                    .to_string(),
                            );
                        }
                        let outcome = model_host
                            .call_with_cancellation(MODEL_OPERATION, args, cancellation)
                            .await;
                        if let Some(usage) = ModelUsage::from_outcome(&outcome) {
                            model_usage
                                .lock()
                                .expect("codemode output")
                                .model_usage
                                .add(&usage);
                        }
                        tool_value(outcome)
                    }
                    Ok(args) if args["action"] == "list" => json!({"ok":true,"value":[]}),
                    Ok(_) => json!({"ok":false,"error":"native model operations are unavailable"}),
                    Err(error) => json!({"ok":false,"error":error.to_string()}),
                };
                Ok::<_, rquickjs::Error>(result.to_string())
            }
        })),
    )?;
    globals.set(
        "__call_tool",
        Func::from(Async(move |name: String, arguments: String| {
            let host = host.clone();
            let output = tool_output.clone();
            let cancellation = cancellation.clone();
            async move {
                if output.lock().expect("codemode output").limit_exceeded {
                    return Ok::<_, rquickjs::Error>(
                        json!({"ok":false,"error":"Script output limit exceeded"}).to_string(),
                    );
                }
                let result = match serde_json::from_str(&arguments) {
                    Ok(_)
                        if matches!(
                            name.as_str(),
                            "codemode" | "tool_search" | MODEL_OPERATION
                        ) =>
                    {
                        json!({"ok":false,"error":"This tool is not callable from a script"})
                    }
                    Ok(arguments) => tool_value(
                        host.call_with_cancellation(&name, arguments, cancellation)
                            .await,
                    ),
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
    if outcome.is_error() {
        return json!({"ok":false,"error":outcome.tool_content()});
    }
    match outcome {
        Outcome::Succeeded(value)
        | Outcome::SucceededWithDetails { output: value, .. }
        | Outcome::TextResult {
            output: value,
            is_error: false,
            ..
        } => {
            json!({"ok":true,"value":value})
        }
        Outcome::Structured { data, .. } | Outcome::StructuredWithDetails { data, .. } => {
            json!({"ok":true,"value":data})
        }
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

/// Usage reported by provider operations invoked from a native script.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelUsage {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub total_tokens: u64,
    pub cost: ModelCost,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCost {
    pub input: f64,
    pub output: f64,
    pub cache_read: f64,
    pub cache_write: f64,
    pub total: f64,
}

impl ModelUsage {
    pub fn from_outcome(outcome: &Outcome) -> Option<Self> {
        let Outcome::Structured { data, .. } = outcome else {
            return None;
        };
        serde_json::from_value(data.get("usage")?.clone()).ok()
    }

    pub fn add(&mut self, usage: &Self) {
        self.input = self.input.saturating_add(usage.input);
        self.output = self.output.saturating_add(usage.output);
        self.cache_read = self.cache_read.saturating_add(usage.cache_read);
        self.cache_write = self.cache_write.saturating_add(usage.cache_write);
        self.total_tokens = self.total_tokens.saturating_add(usage.total_tokens);
        self.cost.input += usage.cost.input;
        self.cost.output += usage.cost.output;
        self.cost.cache_read += usage.cost.cache_read;
        self.cost.cache_write += usage.cost.cache_write;
        self.cost.total += usage.cost.total;
    }

    pub fn measured(&self) -> bool {
        self.total_tokens != 0 || self.cost.total != 0.0
    }
}

#[derive(Default)]
struct Output {
    items: Vec<Item>,
    console: Vec<String>,
    bytes: usize,
    calls: usize,
    limit_exceeded: bool,
    model_usage: ModelUsage,
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
            self.limit_exceeded = true;
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
        let mut result = json!({"content":content,"isError":!success,"full_output_path":full_output_path,"wall_time_seconds":elapsed.as_secs_f64()});
        if self.model_usage.measured() {
            result["usage"] = json!(self.model_usage);
        }
        result
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

    #[test]
    fn structured_tool_error_flags_propagate_without_exposing_details() {
        for with_details in [false, true] {
            for flag in [Some(true), Some(false), None] {
                let mut data = json!({"content":[{"type":"text","text":"tool output"}]});
                if let Some(flag) = flag {
                    data["isError"] = json!(flag);
                }
                let outcome = if with_details {
                    Outcome::StructuredWithDetails {
                        data: data.clone(),
                        attachments: vec![],
                        details: json!({"secret":"private metadata"}),
                    }
                } else {
                    Outcome::Structured {
                        data: data.clone(),
                        attachments: vec![],
                    }
                };
                let result = tool_value(outcome);
                if flag == Some(true) {
                    assert_eq!(result["ok"], false);
                    assert!(result["error"].as_str().unwrap().contains("tool output"));
                    assert!(!result.to_string().contains("private metadata"));
                } else {
                    assert_eq!(result, json!({"ok":true,"value":data}));
                }
            }
        }
    }

    #[tokio::test]
    async fn structured_tool_errors_throw_in_scripts() {
        struct ErrorHost(bool);
        #[async_trait::async_trait]
        impl ToolHost for ErrorHost {
            fn cancellation_safe_calls(&self) -> bool {
                true
            }
            fn tools(&self) -> Vec<ToolSpec> {
                vec![ToolSpec {
                    name: "broken".into(),
                    description: "Fail".into(),
                    parameters: json!({"type":"object"}),
                }]
            }
            async fn call(&self, _: &str, _: Value) -> Outcome {
                let data = json!({"isError":true,"content":[{"type":"text","text":"tool failed"}]});
                if self.0 {
                    Outcome::StructuredWithDetails {
                        data,
                        attachments: vec![],
                        details: json!({"secret":"private metadata"}),
                    }
                } else {
                    Outcome::Structured {
                        data,
                        attachments: vec![],
                    }
                }
            }
        }
        for with_details in [false, true] {
            let reply = ScriptTool {
                timeout: Some(Duration::from_secs(2)),
            }
            .invoke_transaction(
                json!({"code":"await tools.broken({}); text('unexpected success')"}),
                ToolContext {
                    operation_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                    cancellation: Default::default(),
                },
                None,
                Arc::new(ErrorHost(with_details)),
            )
            .await;
            let Outcome::Failed(error) = reply.outcome else {
                panic!("expected script failure")
            };
            assert!(error.contains("tool failed"));
            assert!(!error.contains("unexpected success"));
            assert!(!error.contains("private metadata"));
        }
    }
    #[tokio::test]
    async fn patched_text_results_reach_scripts_with_independent_status() {
        struct PatchedHost;
        #[async_trait::async_trait]
        impl ToolHost for PatchedHost {
            fn cancellation_safe_calls(&self) -> bool {
                true
            }
            fn tools(&self) -> Vec<ToolSpec> {
                ["recovered", "failed"]
                    .into_iter()
                    .map(|name| ToolSpec {
                        name: name.into(),
                        description: "patched".into(),
                        parameters: json!({"type":"object"}),
                    })
                    .collect()
            }
            async fn call(&self, name: &str, _: Value) -> Outcome {
                Outcome::TextResult {
                    output: if name == "recovered" {
                        "exact success"
                    } else {
                        "exact failure"
                    }
                    .into(),
                    details: Some(json!({"secret":"private"})),
                    is_error: name == "failed",
                }
            }
        }
        let reply = ScriptTool { timeout: Some(Duration::from_secs(2)) }.invoke_transaction(
            json!({"code":"text(await tools.recovered({})); try { await tools.failed({}); } catch (e) { text(e.message); }"}),
            ToolContext { operation_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(), cancellation: Default::default() }, None, Arc::new(PatchedHost)).await;
        let Outcome::Structured { data, .. } = reply.outcome else {
            panic!("script should succeed")
        };
        assert_eq!(data["isError"], false);
        let output = data["content"].to_string();
        assert!(output.contains("exact success"));
        assert!(output.contains("exact failure"));
        assert!(!output.contains("Tool failed:"));
        assert!(!output.contains("private"));
    }

    struct Host;
    #[async_trait::async_trait]
    impl ToolHost for Host {
        fn cancellation_safe_calls(&self) -> bool {
            true
        }
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
                    timeout: Some(Duration::from_secs(2)),
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
    fn options_match_reference_source_validation() {
        let maximum = Some(Duration::from_secs(2));
        let (parsed, source) = options(
            "  // @options: {\"max_output_tokens\":0,\"timeout_ms\":25}\r\ntext(1)",
            maximum,
        )
        .unwrap();
        assert_eq!(parsed.max_output_tokens, 0);
        assert_eq!(parsed.timeout, Some(Duration::from_millis(25)));
        assert_eq!(source, "\ntext(1)");
        for invalid in [
            "",
            "// @options: {}",
            "// @options: {}\n  ",
            "// @options: {\"yield\":1}\ntext(1)",
            "// @options: {\"timeout_ms\":null}\ntext(1)",
            "// @options: {\"timeout_ms\":2147483648}\ntext(1)",
            "// @options: {\"max_output_tokens\":1.5}\ntext(1)",
            "text(1)\n // @options: {}",
        ] {
            assert!(options(invalid, maximum).is_err(), "accepted {invalid:?}");
        }
        let reply = run("  // @options: {\"timeout_ms\":20}\nwhile (true) {}", None);
        assert!(matches!(reply.outcome, Outcome::Failed(_)));
        assert!(matches!(
            run("// @options: {\"yield\":1}\nstore('x', 1)", None).outcome,
            Outcome::Failed(_)
        ));
    }
    #[test]
    fn deadlines_are_optional_and_explicit_limits_are_combined() {
        assert_eq!(options("text(1)", None).unwrap().0.timeout, None);
        assert_eq!(
            options("// @options: {\"timeout_ms\":40}\ntext(1)", None)
                .unwrap()
                .0
                .timeout,
            Some(Duration::from_millis(40))
        );
        assert_eq!(
            options(
                "// @options: {\"timeout_ms\":40}\ntext(1)",
                Some(Duration::from_millis(20))
            )
            .unwrap()
            .0
            .timeout,
            Some(Duration::from_millis(20))
        );
    }
    #[tokio::test]
    async fn unbounded_script_honors_cancellation() {
        let cancellation = pi_rs_core::CancellationToken::new();
        let trigger = cancellation.clone();
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            trigger.cancel();
        });
        let reply = tokio::time::timeout(
            Duration::from_secs(1),
            ScriptTool { timeout: None }.invoke_transaction(
                json!({"code":"await new Promise(() => {})"}),
                ToolContext {
                    operation_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                    cancellation,
                },
                None,
                Arc::new(Host),
            ),
        )
        .await
        .unwrap();
        task.await.unwrap();
        assert!(matches!(reply.outcome, Outcome::Failed(message) if message.contains("cancelled")));
    }
    struct StartedHost(Arc<tokio::sync::Notify>);
    #[async_trait::async_trait]
    impl ToolHost for StartedHost {
        fn cancellation_safe_calls(&self) -> bool {
            true
        }
        fn tools(&self) -> Vec<ToolSpec> {
            vec![ToolSpec {
                name: "started".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }]
        }
        async fn call(&self, _: &str, _: Value) -> Outcome {
            self.0.notify_one();
            Outcome::Succeeded("ready".into())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn current_thread_cancels_unbounded_cpu_loop() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let cancellation = pi_rs_core::CancellationToken::new();
        let started = Arc::new(tokio::sync::Notify::new());
        let (stop_watchdog, watchdog_stopped) = std::sync::mpsc::channel::<()>();
        let watchdog_fired = Arc::new(AtomicBool::new(false));
        let watchdog = {
            let cancellation = cancellation.clone();
            let watchdog_fired = watchdog_fired.clone();
            std::thread::spawn(move || {
                if watchdog_stopped
                    .recv_timeout(Duration::from_secs(3))
                    .is_err()
                {
                    watchdog_fired.store(true, Ordering::SeqCst);
                    cancellation.cancel();
                }
            })
        };
        let script_cancel = cancellation.clone();
        let host = Arc::new(StartedHost(started.clone()));
        let script = tokio::spawn(async move {
            ScriptTool { timeout: None }.invoke_transaction(
                json!({"code":"await tools.started({}); store('uncommitted', true); while (true) {}"}),
                ToolContext { operation_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(), cancellation: script_cancel },
                None, host,
            ).await
        });
        started.notified().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancellation.cancel();
        let reply = tokio::time::timeout(Duration::from_secs(1), script)
            .await
            .unwrap()
            .unwrap();
        let _ = stop_watchdog.send(());
        watchdog.join().unwrap();
        assert!(
            !watchdog_fired.load(Ordering::SeqCst),
            "current-thread executor was blocked"
        );
        assert!(matches!(reply.outcome, Outcome::Failed(message) if message.contains("cancelled")));
        assert!(reply.state.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_invocation_stops_busy_worker() {
        let started = Arc::new(tokio::sync::Notify::new());
        let host = Arc::new(StartedHost(started.clone()));
        let weak_host = Arc::downgrade(&host);
        let cancellation = pi_rs_core::CancellationToken::new();
        let watchdog_cancel = cancellation.clone();
        let (stop_watchdog, watchdog_stopped) = std::sync::mpsc::channel::<()>();
        let watchdog = std::thread::spawn(move || {
            if watchdog_stopped
                .recv_timeout(Duration::from_secs(3))
                .is_err()
            {
                watchdog_cancel.cancel();
            }
        });
        let script = tokio::spawn(async move {
            ScriptTool { timeout: None }
                .invoke_transaction(
                    json!({"code":"await tools.started({}); while (true) {}"}),
                    ToolContext {
                        operation_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                        cancellation,
                    },
                    None,
                    host,
                )
                .await
        });
        started.notified().await;
        script.abort();
        let _ = script.await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while weak_host.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("worker kept running after invocation was dropped");
        let _ = stop_watchdog.send(());
        watchdog.join().unwrap();
    }
    #[tokio::test]
    async fn explicit_host_deadline_interrupts_busy_script() {
        let reply = ScriptTool { timeout: None }
            .invoke_transaction_with_timeout(
                json!({"code":"while (true) {}"}),
                ToolContext {
                    operation_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                    cancellation: Default::default(),
                },
                None,
                Arc::new(Host),
                Some(Duration::from_millis(20)),
            )
            .await;
        assert!(matches!(reply.outcome, Outcome::Failed(_)));
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
    #[tokio::test]
    async fn output_limits_terminate_even_when_caught() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct CountingHost(Arc<AtomicUsize>);
        #[async_trait::async_trait]
        impl ToolHost for CountingHost {
            fn cancellation_safe_calls(&self) -> bool {
                true
            }
            fn tools(&self) -> Vec<ToolSpec> {
                ["side_effect", MODEL_OPERATION]
                    .into_iter()
                    .map(|name| ToolSpec {
                        name: name.into(),
                        description: String::new(),
                        parameters: json!({"type":"object"}),
                    })
                    .collect()
            }
            async fn call(&self, _: &str, _: Value) -> Outcome {
                self.0.fetch_add(1, Ordering::SeqCst);
                Outcome::Succeeded(String::new())
            }
        }
        for (overflow, after, calls_before) in [
            (
                "text('x'.repeat(16 * 1024 * 1024 + 1))",
                "await tools.side_effect({})",
                0,
            ),
            (
                "text('')",
                "await models.getModelsOfType('image')",
                MAX_ITEMS,
            ),
            (
                "text('x'.repeat(16 * 1024 * 1024 + 1))",
                "while (true) {}",
                0,
            ),
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let code = format!(
                "store('uncommitted', true); try {{ {overflow}; }} catch (_) {{}} {after}; store('later', true);"
            );
            let sink = Arc::new(Mutex::new(Output {
                calls: calls_before,
                ..Output::default()
            }));
            let result = tokio::time::timeout(
                Duration::from_secs(15),
                evaluate(
                    &code,
                    json!({}),
                    Arc::new(CountingHost(calls.clone())),
                    ToolContext {
                        operation_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                        cancellation: Default::default(),
                    },
                    None,
                    sink.clone(),
                ),
            )
            .await
            .expect("output limit must stop the worker");
            assert!(result.is_err());
            assert!(sink.lock().unwrap().limit_exceeded);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
        let reply = ScriptTool { timeout: None }
            .invoke_transaction(
                json!({"code":"store('uncommitted', true); try { text('x'.repeat(16 * 1024 * 1024 + 1)); } catch (_) { exit(); }"}),
                ToolContext { operation_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(), cancellation: Default::default() },
                None,
                Arc::new(Host),
            )
            .await;
        assert!(
            matches!(reply.outcome, Outcome::Failed(message) if message.contains("Script output limit exceeded"))
        );
        assert!(reply.state.is_none());
    }
    #[tokio::test]
    async fn output_breach_cancels_tokens_of_already_queued_calls() {
        struct QueuedHost {
            tokens: Mutex<Vec<pi_rs_core::CancellationToken>>,
            changed: tokio::sync::Notify,
        }
        #[async_trait::async_trait]
        impl ToolHost for QueuedHost {
            fn tools(&self) -> Vec<ToolSpec> {
                ["hold", "write", "ready"]
                    .into_iter()
                    .map(|name| ToolSpec {
                        name: name.into(),
                        description: String::new(),
                        parameters: json!({"type":"object"}),
                    })
                    .collect()
            }
            async fn call(&self, _: &str, _: Value) -> Outcome {
                panic!("codemode must pass its invocation cancellation token")
            }
            async fn call_with_cancellation(
                &self,
                name: &str,
                _: Value,
                cancellation: pi_rs_core::CancellationToken,
            ) -> Outcome {
                if name == "ready" {
                    loop {
                        let notified = self.changed.notified();
                        if self.tokens.lock().unwrap().len() == 2 {
                            break;
                        }
                        notified.await;
                    }
                    return Outcome::Succeeded("queued".into());
                }
                self.tokens.lock().unwrap().push(cancellation.clone());
                self.changed.notify_one();
                cancellation.cancelled().await;
                Outcome::Skipped("cancelled".into())
            }
        }
        let host = Arc::new(QueuedHost {
            tokens: Mutex::new(vec![]),
            changed: tokio::sync::Notify::new(),
        });
        let parent = pi_rs_core::CancellationToken::new();
        let reply = tokio::time::timeout(Duration::from_secs(10), ScriptTool { timeout: None }.invoke_transaction(
            json!({"code":"tools.hold({}); tools.write({}); await tools.ready({}); try { text('x'.repeat(16 * 1024 * 1024 + 1)); } catch (_) {}"}),
            ToolContext { operation_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(), cancellation: parent.clone() }, None, host.clone(),
        )).await.expect("script stalled before output breach");
        assert!(
            matches!(reply.outcome, Outcome::Failed(message) if message.contains("Script output limit exceeded"))
        );
        assert!(!parent.is_cancelled());
        let tokens = host.tokens.lock().unwrap();
        assert_eq!(tokens.len(), 2);
        assert!(tokens.iter().all(|token| token.is_cancelled()));
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

    struct DeadlineModel;
    #[async_trait::async_trait]
    impl pi_rs_core::Model for DeadlineModel {
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
                assert!(
                    content.contains("error")
                        || content.contains("timed out")
                        || content.contains("interrupt"),
                    "{content}"
                );
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
                        id: "deadline".into(),
                        name: "codemode".into(),
                        arguments: json!({"code":"while (true) {}"}).to_string(),
                    }],
                    usage: None,
                })]
            };
            Ok(Box::pin(futures_util::stream::iter(events)))
        }
    }

    #[tokio::test]
    async fn public_sdk_explicit_timeouts_interrupt_busy_scripts() {
        for explicit_limits in [false, true] {
            // Keep the original public struct literal and Duration field usable.
            let extension = Codemode {
                only: false,
                inline_budget: 3000,
                timeout: if explicit_limits {
                    Duration::MAX
                } else {
                    Duration::from_millis(20)
                },
            };
            let mut builder = pi_rs_core::SessionBuilder::new(Arc::new(DeadlineModel), "fixture")
                .extension(Arc::new(extension))
                .unwrap();
            if explicit_limits {
                builder = builder.limits(pi_rs_core::Limits {
                    tool_timeout: Duration::from_millis(20),
                    ..Default::default()
                });
            }
            let mut session = builder.in_memory().unwrap();
            match session.prompt("test").await {
                Ok(answer) => assert_eq!(answer, "done"),
                // The host deadline can win the race with the worker's interrupt.
                // Until the worker reports back, its outcome is safely unknown.
                Err(pi_rs_core::Error::Reconciliation(operation_id)) if explicit_limits => {
                    assert!(
                        session
                            .journal().unwrap()
                            .operations()
                            .iter()
                            .any(|(invocation, state)| {
                                invocation.operation_id == operation_id
                                    && matches!(
                                        state,
                                        pi_rs_core::OperationState::Finished(Outcome::Unknown(reason)) if reason == "tool timed out"
                                    )
                            })
                    );
                    assert!(matches!(
                        session.resume().await,
                        Err(pi_rs_core::Error::Reconciliation(_))
                    ));
                }
                result => panic!("unexpected timeout result: {result:?}"),
            }
            session.close().await.unwrap();
        }
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
        assert_eq!(
            session.journal().unwrap().extension_states()[ID].data["saved"],
            true
        );
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
        assert_eq!(session.journal().unwrap().operations().len(), 3);
        session.close().await.unwrap();
    }
    #[tokio::test]
    async fn output_breach_skips_queued_write_and_returns_tool_failure_to_model() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct QueueTool {
            name: &'static str,
            writes: Arc<AtomicUsize>,
            prepared: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl Tool for QueueTool {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: self.name.into(),
                    description: String::new(),
                    parameters: json!({"type":"object"}),
                }
            }
            async fn prepare_arguments(&self, arguments: Value) -> ExtensionResult<Value> {
                if self.name == "hold" {
                    self.prepared.fetch_add(1, Ordering::SeqCst);
                    std::future::pending::<()>().await;
                }
                Ok(arguments)
            }
            async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
                self.writes.fetch_add(1, Ordering::SeqCst);
                Outcome::Succeeded("done".into())
            }
        }
        struct QueueExtension(Arc<AtomicUsize>, Arc<AtomicUsize>);
        impl Extension for QueueExtension {
            fn id(&self) -> &str {
                "queue-test"
            }
            fn tools(&self) -> Vec<Arc<dyn Tool>> {
                ["hold", "write"]
                    .into_iter()
                    .map(|name| {
                        Arc::new(QueueTool {
                            name,
                            writes: self.0.clone(),
                            prepared: self.1.clone(),
                        }) as Arc<dyn Tool>
                    })
                    .collect()
            }
        }
        struct QueueModel;
        #[async_trait::async_trait]
        impl pi_rs_core::Model for QueueModel {
            async fn stream(
                &self,
                request: ModelRequest,
            ) -> pi_rs_core::Result<pi_rs_core::ModelStream> {
                use pi_rs_core::{Message, ModelEvent, ToolCall};
                let events = if let Some(content) =
                    request.messages.iter().find_map(|message| match message {
                        Message::Tool { content, .. } => Some(content),
                        _ => None,
                    }) {
                    assert!(
                        content.contains("Script output limit exceeded"),
                        "{content}"
                    );
                    vec![
                        Ok(ModelEvent::Text("done".into())),
                        Ok(ModelEvent::Complete {
                            calls: vec![],
                            usage: None,
                        }),
                    ]
                } else {
                    vec![Ok(ModelEvent::Complete { calls: vec![ToolCall {
                        id:"script".into(), name:"codemode".into(),
                        arguments: json!({"code":"tools.hold({}); tools.write({}); await Promise.resolve(); text('x'.repeat(16 * 1024 * 1024 + 1));"}).to_string(),
                    }], usage: None })]
                };
                Ok(Box::pin(futures_util::stream::iter(events)))
            }
        }
        let writes = Arc::new(AtomicUsize::new(0));
        let prepared = Arc::new(AtomicUsize::new(0));
        let mut session = pi_rs_core::SessionBuilder::new(Arc::new(QueueModel), "fixture")
            .extension(Arc::new(Codemode::default()))
            .unwrap()
            .extension(Arc::new(QueueExtension(writes.clone(), prepared.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        let answer = tokio::time::timeout(Duration::from_secs(10), session.prompt("test"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(answer, "done");
        assert_eq!(writes.load(Ordering::SeqCst), 0);
        assert_eq!(prepared.load(Ordering::SeqCst), 1);
        let operations = session.journal().unwrap().operations();
        assert!(
            operations
                .iter()
                .any(|(invocation, state)| invocation.call.name == "write"
                    && matches!(
                        state,
                        pi_rs_core::OperationState::Finished(Outcome::Skipped(_))
                    ))
        );
        session.close().await.unwrap();
    }
    #[test]
    fn options_are_restricted_to_the_first_line() {
        let reply = run("text('first');\n// @options: {}", None);
        assert!(matches!(reply.outcome, Outcome::Failed(_)));
    }
}
