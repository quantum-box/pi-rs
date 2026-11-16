//! Opt-in adapter for tools that need durable nested invocation keys.
use std::sync::{
    Arc, Mutex, OnceLock, Weak,
    atomic::{AtomicBool, Ordering},
};

use pi_rs_core::{
    ExtensionResult, ExtensionState, Outcome, Tool, ToolContext, ToolHost, ToolReply, ToolSpec,
};
use serde_json::Value;

use crate::nested::NestedHost;

struct Dispatch {
    nested: Arc<NestedHost>,
    identity: Option<Arc<()>>,
    operation_id: uuid::Uuid,
    ordinary: Arc<dyn ToolHost>,
    claimed: AtomicBool,
    control: Arc<Mutex<crate::reset::ToolControl>>,
    diagnostics: Option<Arc<dyn DiagnosticSink>>,
    stream: Option<Arc<dyn StreamSink>>,
}
tokio::task_local! { static CURRENT_HOST: Dispatch; }

/// Execute within the runtime's invocation-scoped nested host.
#[cfg(test)]
pub(crate) async fn scope<F: std::future::Future>(
    nested: Arc<NestedHost>,
    tool: Arc<dyn Tool>,
    operation_id: uuid::Uuid,
    ordinary: Arc<dyn ToolHost>,
    future: F,
) -> F::Output {
    scope_with_control(
        nested,
        tool,
        operation_id,
        ordinary,
        Arc::new(Mutex::new(Default::default())),
        (None, None),
        future,
    )
    .await
}

type OutputChannels = (Option<Arc<dyn DiagnosticSink>>, Option<Arc<dyn StreamSink>>);

pub(crate) async fn scope_with_control<F: std::future::Future>(
    nested: Arc<NestedHost>,
    tool: Arc<dyn Tool>,
    operation_id: uuid::Uuid,
    ordinary: Arc<dyn ToolHost>,
    control: Arc<Mutex<crate::reset::ToolControl>>,
    (diagnostics, stream): OutputChannels,
    future: F,
) -> F::Output {
    let _invocation_scope = crate::nested::InvocationScope(nested.clone());
    let dispatch = Dispatch {
        nested,
        identity: registered_identity(&tool),
        operation_id,
        ordinary,
        claimed: AtomicBool::new(false),
        control,
        diagnostics,
        stream,
    };
    CURRENT_HOST.scope(dispatch, future).await
}

/// A durable-only tool. Register its adapter using `KeyedToolAdapter::register`;
/// the adapter fails closed outside its registered durable dispatch.
#[async_trait::async_trait]
pub trait KeyedTool: Send + Sync {
    fn spec(&self) -> ToolSpec;
    /// Opt in to resuming a committed checkpointed park when registered unsafe.
    /// The implementation must branch on `pending_wake()` before repeating any
    /// pre-park effect and must not leave detached effects running after return.
    /// Return a positive, stable version and change it when the implementation
    /// or checkpoint interpretation changes. Interrupted invocations still do
    /// not replay. The host must restore the same tool definition on reopen.
    fn unsafe_park_version(&self) -> Option<u32> {
        None
    }
    /// Non-secret host identity binding implementation, configuration and tenant.
    /// Change it whenever a parked invocation must not resume against this tool.
    fn unsafe_park_identity(&self) -> Option<String> {
        None
    }
    async fn prepare_arguments(&self, arguments: Value) -> ExtensionResult<Value> {
        Ok(arguments)
    }
    async fn invoke(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply;

    /// Return a control with the reply to opt into durable tool additions, round termination or handoff.
    /// Nested calls do not affect their caller’s model round.
    async fn invoke_controlled(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> (ToolReply, crate::reset::ToolControl) {
        (
            self.invoke(arguments, context, state, host).await,
            Default::default(),
        )
    }
}

/// The invocation's ordinary host plus its durable keyed-call capability.
/// Calls remain subject to fencing and sealing checks. Ordinary keyed calls
/// remain serialized; the bounded keyed batch polls accepted jobs concurrently.
#[derive(Clone)]
pub struct KeyedHost {
    ordinary: Arc<dyn ToolHost>,
    nested: Arc<NestedHost>,
    stream: Option<Arc<dyn StreamSink>>,
    diagnostics: Option<Arc<dyn DiagnosticSink>>,
}
pub(crate) trait DiagnosticSink: Send + Sync {
    fn record(
        &self,
        key: Option<&str>,
        diagnostic: crate::runtime::ToolDiagnostic,
    ) -> Result<(), String>;
}
impl KeyedHost {
    /// Record a new bounded diagnostic. Replayed calls without an identity are
    /// recorded again because they cannot safely be distinguished from new events.
    pub fn diagnostic(&self, diagnostic: crate::runtime::ToolDiagnostic) -> Result<(), String> {
        self.record_diagnostic(None, diagnostic)
    }
    /// Record an event under a caller-supplied identity stable across retries of
    /// this tool operation. Reusing a key with different content is an error.
    pub fn diagnostic_keyed(
        &self,
        key: &str,
        diagnostic: crate::runtime::ToolDiagnostic,
    ) -> Result<(), String> {
        if key.is_empty() || key.len() > 128 {
            return Err("tool diagnostic key must be 1..=128 bytes".into());
        }
        self.record_diagnostic(Some(key), diagnostic)
    }
    fn record_diagnostic(
        &self,
        key: Option<&str>,
        diagnostic: crate::runtime::ToolDiagnostic,
    ) -> Result<(), String> {
        if diagnostic.message.len() > 4096
            || diagnostic
                .code
                .as_ref()
                .is_some_and(|code| code.len() > 256)
        {
            return Err("tool diagnostic exceeds size limit".into());
        }
        self.diagnostics
            .as_ref()
            .ok_or("tool diagnostic checkpoint is unavailable")?
            .record(key, diagnostic)
    }
    pub fn ordinary(&self) -> &Arc<dyn ToolHost> {
        &self.ordinary
    }
    pub async fn call_keyed(&self, key: &str, name: &str, arguments: Value) -> Outcome {
        self.nested.call_keyed(key, name, arguments).await
    }
    /// Dispatch a keyed child without polling its pending result.
    pub async fn call_keyed_unpolled(&self, key: &str, name: &str, arguments: Value) -> Outcome {
        self.nested.call_keyed_unpolled(key, name, arguments).await
    }

    /// Dispatch or reattach without polling and return the persisted operation ID.
    /// The initial child invocation is awaited; this is not upstream `wait: false`.
    pub async fn call_keyed_unpolled_with_id(
        &self,
        key: &str,
        name: &str,
        arguments: Value,
    ) -> Result<crate::nested::UnpolledCall, String> {
        self.nested
            .call_keyed_unpolled_with_id(key, name, arguments)
            .await
    }

    /// End this invocation at a pending-child boundary. The parent must be
    /// registered replay-safe and return normally without state or controls.
    /// The host must resolve every pending child before the parent can replay.
    pub fn suspend_pending(&self) -> Result<(), String> {
        self.nested.suspend_pending()
    }
    /// Park the root until all accepted pending nested jobs settle, recording a
    /// checkpoint in the same conversation transition. Unsafe registrations
    /// require `KeyedTool::unsafe_park_version` and its replay contract. The
    /// invocation must return normally without state or controls.
    pub fn suspend_pending_with_checkpoint(&self, checkpoint: Value) -> Result<(), String> {
        self.nested
            .suspend_pending_with_checkpoint(Some(checkpoint))
    }

    /// Checkpoint and ended nested operation IDs from the preceding park.
    /// This is an all-pending wake, not an upstream task `any` wake.
    pub fn pending_wake(&self) -> Option<crate::nested::PendingWake> {
        self.nested.resumed_checkpoint()
    }

    /// Invoke a keyed child and read its durable diagnostics with the outcome.
    /// Fails closed when ordered output would be lost; use `call_keyed_with_blocks`
    /// to consume changed block output.
    pub async fn call_keyed_with_diagnostics(
        &self,
        key: &str,
        name: &str,
        arguments: Value,
    ) -> crate::nested::KeyedResult {
        self.nested
            .call_keyed_with_diagnostics(key, name, arguments)
            .await
    }
    /// Invoke a keyed child and retain ordered output blocks, if any. When
    /// `output_blocks` is absent, `outcome` contains the ordinary result.
    pub async fn call_keyed_with_blocks(
        &self,
        key: &str,
        name: &str,
        arguments: Value,
    ) -> crate::nested::KeyedResult {
        self.nested
            .call_keyed_result(key, name, arguments, true)
            .await
    }

    /// Dispatch without polling and retain ordered output blocks, if any.
    pub async fn call_keyed_unpolled_with_blocks(
        &self,
        key: &str,
        name: &str,
        arguments: Value,
    ) -> crate::nested::KeyedResult {
        self.nested
            .call_keyed_result(key, name, arguments, false)
            .await
    }
    /// Admit distinct keyed calls in order and poll accepted jobs concurrently.
    pub async fn call_keyed_batch(&self, calls: Vec<(String, String, Value)>) -> Vec<Outcome> {
        self.nested.call_keyed_batch(calls).await
    }
}

/// Maximum UTF-8 bytes in an opt-in streamed result per attempt.
pub const MAX_STREAMED_OUTPUT_BYTES: usize = 64 * 1024;

#[async_trait::async_trait]
pub(crate) trait StreamSink: Send + Sync {
    async fn record(&self, text: &str) -> ExtensionResult<()>;
}

/// An explicit durable-only result. `None` selects this attempt's streamed text;
/// `Some("")` is an explicit empty result. Other outcomes remain ordinary replies.
pub enum StreamedReply {
    Success {
        output: Option<String>,
        state: Option<ExtensionState>,
    },
    Explicit(ToolReply),
}

/// A durable tool opting in to streamed text. Nested dispatch is not supported.
#[async_trait::async_trait]
pub trait StreamedTool: Send + Sync {
    fn spec(&self) -> ToolSpec;
    async fn prepare_arguments(&self, arguments: Value) -> ExtensionResult<Value> {
        Ok(arguments)
    }
    async fn invoke(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
        output: StreamOutput,
    ) -> StreamedReply;
}

struct StreamState {
    text: String,
    failed: bool,
}
struct StreamInner {
    open: AtomicBool,
    state: tokio::sync::Mutex<StreamState>,
    sink: Arc<dyn StreamSink>,
}
/// Invocation-scoped append-only text. Each successful append is committed before returning.
#[derive(Clone)]
pub struct StreamOutput(Arc<StreamInner>);
impl StreamOutput {
    pub async fn append(&self, chunk: &str) -> ExtensionResult<()> {
        let mut state = self.0.state.lock().await;
        if !self.0.open.load(Ordering::Acquire) {
            return Err("stream is closed".into());
        }
        if state.failed {
            return Err("stream recording failed".into());
        }
        if chunk.len() > MAX_STREAMED_OUTPUT_BYTES.saturating_sub(state.text.len()) {
            state.failed = true;
            return Err("streamed output exceeds byte limit".into());
        }
        let mut next = state.text.clone();
        next.push_str(chunk);
        if let Err(error) = self.0.sink.record(&next).await {
            state.failed = true;
            return Err(error);
        }
        state.text = next;
        Ok(())
    }
}
struct StreamGuard(Arc<StreamInner>);
impl StreamGuard {
    async fn finish(&self) -> tokio::sync::MutexGuard<'_, StreamState> {
        self.0.open.store(false, Ordering::Release);
        self.0.state.lock().await
    }
}
impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.0.open.store(false, Ordering::Release);
    }
}

/// Wrap this in `KeyedToolAdapter` and register it for durable execution.
pub struct StreamedToolAdapter<T>(pub T);
#[async_trait::async_trait]
impl<T: StreamedTool> KeyedTool for StreamedToolAdapter<T> {
    fn spec(&self) -> ToolSpec {
        self.0.spec()
    }
    async fn prepare_arguments(&self, arguments: Value) -> ExtensionResult<Value> {
        self.0.prepare_arguments(arguments).await
    }
    async fn invoke(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        let Some(sink) = host.stream.clone() else {
            return Outcome::Skipped("streamed tool requires model-issued durable attempt".into())
                .into();
        };
        let inner = Arc::new(StreamInner {
            open: AtomicBool::new(true),
            state: tokio::sync::Mutex::new(StreamState {
                text: String::new(),
                failed: false,
            }),
            sink,
        });
        let guard = StreamGuard(inner.clone());
        let reply = self
            .0
            .invoke(arguments, context, state, host, StreamOutput(inner.clone()))
            .await;
        // Close before waiting for an in-flight append, so queued or detached
        // writers cannot start recording after the tool has returned.
        let state = guard.finish().await;
        if state.failed {
            return Outcome::Unknown(
                "streamed output recording failed; reconciliation required".into(),
            )
            .into();
        }
        match reply {
            StreamedReply::Success {
                output,
                state: tool_state,
            } => ToolReply {
                outcome: Outcome::Succeeded(output.unwrap_or_else(|| state.text.clone())),
                state: tool_state,
            },
            StreamedReply::Explicit(reply) => reply,
        }
    }
}

/// Opt-in streamed tool using bounded head or tail retention. Its retained text
/// is checkpointed per attempt through the same fence as `StreamedToolAdapter`.
#[async_trait::async_trait]
pub trait BoundedStreamedTool: Send + Sync {
    fn spec(&self) -> ToolSpec;
    async fn prepare_arguments(&self, arguments: Value) -> ExtensionResult<Value> {
        Ok(arguments)
    }
    async fn invoke(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
        output: BoundedStreamOutput,
    ) -> StreamedReply;
}

struct BoundedStreamState {
    buffer: crate::output::OutputBuffer,
    failed: bool,
}
struct BoundedStreamInner {
    open: AtomicBool,
    state: tokio::sync::Mutex<BoundedStreamState>,
    sink: Arc<dyn StreamSink>,
}

/// Invocation-scoped retained output. Each successful append checkpoints the
/// current window; snapshots report attempt-local dropped counts.
#[derive(Clone)]
pub struct BoundedStreamOutput(Arc<BoundedStreamInner>);
impl BoundedStreamOutput {
    async fn update(
        &self,
        apply: impl FnOnce(&mut crate::output::OutputBuffer),
    ) -> ExtensionResult<()> {
        let mut state = self.0.state.lock().await;
        if !self.0.open.load(Ordering::Acquire) {
            return Err("stream is closed".into());
        }
        if state.failed {
            return Err("stream recording failed".into());
        }
        let mut candidate = state.buffer.clone();
        apply(&mut candidate);
        let snapshot = candidate.snapshot();
        if let Err(error) = self.0.sink.record(&snapshot.text).await {
            state.failed = true;
            return Err(error);
        }
        state.buffer = candidate;
        Ok(())
    }
    pub async fn append(&self, text: &str) -> ExtensionResult<()> {
        self.update(|buffer| buffer.push_str(text)).await
    }
    pub async fn append_bytes(&self, bytes: &[u8]) -> ExtensionResult<()> {
        self.update(|buffer| buffer.push_bytes(bytes)).await
    }
    pub async fn snapshot(&self) -> ExtensionResult<crate::output::OutputSnapshot> {
        let mut state = self.0.state.lock().await;
        if !self.0.open.load(Ordering::Acquire) {
            return Err("stream is closed".into());
        }
        if state.failed {
            return Err("stream recording failed".into());
        }
        Ok(state.buffer.snapshot())
    }
}
struct BoundedStreamGuard(Arc<BoundedStreamInner>);
impl Drop for BoundedStreamGuard {
    fn drop(&mut self) {
        self.0.open.store(false, Ordering::Release);
    }
}

/// Wrap with `KeyedToolAdapter` and register normally. `limits.max_bytes` may
/// not exceed the durable stream record's 64 KiB cap.
pub struct BoundedStreamedToolAdapter<T> {
    pub tool: T,
    pub limits: crate::output::OutputLimits,
}
#[async_trait::async_trait]
impl<T: BoundedStreamedTool> KeyedTool for BoundedStreamedToolAdapter<T> {
    fn spec(&self) -> ToolSpec {
        self.tool.spec()
    }
    async fn prepare_arguments(&self, arguments: Value) -> ExtensionResult<Value> {
        self.tool.prepare_arguments(arguments).await
    }
    async fn invoke(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        if self.limits.max_bytes > MAX_STREAMED_OUTPUT_BYTES {
            return Outcome::Skipped(
                "bounded stream byte limit exceeds durable record limit".into(),
            )
            .into();
        }
        let Some(sink) = host.stream.clone() else {
            return Outcome::Skipped("streamed tool requires model-issued durable attempt".into())
                .into();
        };
        let inner = Arc::new(BoundedStreamInner {
            open: AtomicBool::new(true),
            state: tokio::sync::Mutex::new(BoundedStreamState {
                buffer: crate::output::OutputBuffer::new(self.limits),
                failed: false,
            }),
            sink,
        });
        let guard = BoundedStreamGuard(inner.clone());
        let reply = self
            .tool
            .invoke(
                arguments,
                context,
                state,
                host,
                BoundedStreamOutput(inner.clone()),
            )
            .await;
        inner.open.store(false, Ordering::Release);
        let mut state = inner.state.lock().await;
        if !state.failed {
            let mut candidate = state.buffer.clone();
            candidate.end();
            let text = candidate.snapshot().text;
            if inner.sink.record(&text).await.is_err() {
                state.failed = true;
            } else {
                state.buffer = candidate;
            }
        }
        drop(guard);
        if state.failed {
            return Outcome::Unknown(
                "streamed output recording failed; reconciliation required".into(),
            )
            .into();
        }
        match reply {
            StreamedReply::Success {
                output,
                state: tool_state,
            } => ToolReply {
                outcome: Outcome::Succeeded(output.unwrap_or_else(|| state.buffer.snapshot().text)),
                state: tool_state,
            },
            StreamedReply::Explicit(reply) => reply,
        }
    }
}

// Weak entries keep allocations from being reused while they are registered. The
// token belongs to the adapter, not its address or concrete Rust type.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct UnsafeParkIdentity {
    spec: ToolSpec,
    version: u32,
    #[serde(default)]
    pub(crate) identity: String,
}
trait UnsafeParkIdentitySource: Send + Sync {
    fn current_unsafe_park_identity(&self) -> Option<UnsafeParkIdentity>;
    fn definition_version(&self) -> Option<u32>;
}
impl<T: KeyedTool> UnsafeParkIdentitySource for KeyedToolAdapter<T> {
    fn definition_version(&self) -> Option<u32> {
        self.0.unsafe_park_version()
    }
    fn current_unsafe_park_identity(&self) -> Option<UnsafeParkIdentity> {
        let version = self.0.unsafe_park_version().filter(|v| *v > 0)?;
        let identity = self
            .0
            .unsafe_park_identity()
            .filter(|s| !s.is_empty() && s.len() <= 1024)?;
        Some(UnsafeParkIdentity {
            spec: self.0.spec(),
            version,
            identity,
        })
    }
}
type RegistryEntry = (Weak<dyn Tool>, Weak<()>, Weak<dyn UnsafeParkIdentitySource>);
static REGISTRY: OnceLock<Mutex<Vec<RegistryEntry>>> = OnceLock::new();

pub(crate) fn definition_version(tool: &Arc<dyn Tool>) -> Option<Option<u32>> {
    let source = {
        let mut entries = REGISTRY.get_or_init(Default::default).lock().ok()?;
        entries
            .retain(|(tool, identity, _)| tool.strong_count() > 0 && identity.strong_count() > 0);
        entries.iter().find_map(|(registered, _, source)| {
            let registered = registered.upgrade()?;
            Arc::ptr_eq(&registered, tool)
                .then(|| source.upgrade())
                .flatten()
        })?
    };
    Some(source.definition_version())
}

fn registered_identity(tool: &Arc<dyn Tool>) -> Option<Arc<()>> {
    let mut entries = REGISTRY.get_or_init(Default::default).lock().ok()?;
    entries.retain(|(tool, identity, _)| tool.strong_count() > 0 && identity.strong_count() > 0);
    entries.iter().find_map(|(registered, identity, _)| {
        let registered = registered.upgrade()?;
        Arc::ptr_eq(&registered, tool)
            .then(|| identity.upgrade())
            .flatten()
    })
}

pub(crate) fn unsafe_park_identity(tool: &Arc<dyn Tool>) -> Option<UnsafeParkIdentity> {
    let source = {
        let mut entries = REGISTRY.get_or_init(Default::default).lock().ok()?;
        entries
            .retain(|(tool, identity, _)| tool.strong_count() > 0 && identity.strong_count() > 0);
        entries.iter().find_map(|(registered, _, source)| {
            let registered = registered.upgrade()?;
            Arc::ptr_eq(&registered, tool)
                .then(|| source.upgrade())
                .flatten()
        })?
    };
    // A failed host update clears the binding before the tool's partially
    // updated configuration can be consulted.
    let bound = crate::nested_identity::current(tool)?;
    let identity = source.current_unsafe_park_identity()?;
    (identity.spec == tool.spec() && identity.identity == bound).then_some(identity)
}

pub struct KeyedToolAdapter<T>(pub T, Arc<()>);
impl<T> KeyedToolAdapter<T> {
    pub fn new(tool: T) -> Self {
        Self(tool, Arc::new(()))
    }

    /// Opt this exact Arc allocation into durable keyed dispatch.
    pub fn register(
        self: &Arc<Self>,
        replay: crate::recovery::ReplayPolicy,
    ) -> crate::runtime::RegisteredTool
    where
        T: KeyedTool + 'static,
    {
        let tool: Arc<dyn Tool> = self.clone();
        let mut entries = REGISTRY
            .get_or_init(Default::default)
            .lock()
            .expect("keyed registry poisoned");
        entries
            .retain(|(tool, identity, _)| tool.strong_count() > 0 && identity.strong_count() > 0);
        if !entries
            .iter()
            .any(|(registered, _, _)| registered.ptr_eq(&Arc::downgrade(&tool)))
        {
            let source: Arc<dyn UnsafeParkIdentitySource> = self.clone();
            entries.push((
                Arc::downgrade(&tool),
                Arc::downgrade(&self.1),
                Arc::downgrade(&source),
            ));
        }
        crate::runtime::RegisteredTool { tool, replay }
    }
}
#[async_trait::async_trait]
impl<T: KeyedTool> Tool for KeyedToolAdapter<T> {
    fn spec(&self) -> ToolSpec {
        self.0.spec()
    }
    async fn prepare_arguments(&self, arguments: Value) -> ExtensionResult<Value> {
        self.0.prepare_arguments(arguments).await
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Skipped("keyed tool requires durable runtime".into())
    }
    async fn invoke_transaction(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        ordinary: Arc<dyn ToolHost>,
    ) -> ToolReply {
        let nested = CURRENT_HOST
            .try_with(|dispatch| {
                if !dispatch
                    .identity
                    .as_ref()
                    .is_some_and(|identity| Arc::ptr_eq(identity, &self.1))
                    || dispatch.operation_id != context.operation_id
                    || !Arc::ptr_eq(&dispatch.ordinary, &ordinary)
                    || dispatch.claimed.swap(true, Ordering::AcqRel)
                {
                    None
                } else {
                    Some((
                        dispatch.nested.clone(),
                        dispatch.stream.clone(),
                        dispatch.diagnostics.clone(),
                    ))
                }
            })
            .ok()
            .flatten();
        let Some((nested, stream, diagnostics)) = nested else {
            return Outcome::Skipped("keyed tool requires durable runtime".into()).into();
        };
        let (reply, control) = self
            .0
            .invoke_controlled(
                arguments,
                context,
                state,
                KeyedHost {
                    ordinary,
                    nested,
                    stream,
                    diagnostics,
                },
            )
            .await;
        let _ = CURRENT_HOST.try_with(|dispatch| {
            *dispatch.control.lock().expect("control mutex poisoned") = control
        });
        reply
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recovery::ReplayPolicy;
    use serde_json::json;

    struct ZeroSized;
    #[async_trait::async_trait]
    impl KeyedTool for ZeroSized {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "zero".into(),
                description: String::new(),
                parameters: json!({}),
            }
        }
        async fn invoke(
            &self,
            _: Value,
            _: ToolContext,
            _: Option<&ExtensionState>,
            _: KeyedHost,
        ) -> ToolReply {
            Outcome::Succeeded("ok".into()).into()
        }
    }

    #[repr(C)]
    struct Wrapper(KeyedToolAdapter<ZeroSized>);
    #[async_trait::async_trait]
    impl Tool for Wrapper {
        fn spec(&self) -> ToolSpec {
            self.0.spec()
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }

    struct RecordingSink {
        fail: bool,
        values: Mutex<Vec<String>>,
    }
    #[async_trait::async_trait]
    impl StreamSink for RecordingSink {
        async fn record(&self, text: &str) -> ExtensionResult<()> {
            if self.fail {
                return Err("storage failure".into());
            }
            self.values.lock().unwrap().push(text.into());
            Ok(())
        }
    }
    fn test_output(sink: Arc<dyn StreamSink>) -> (StreamOutput, StreamGuard) {
        let inner = Arc::new(StreamInner {
            open: AtomicBool::new(true),
            state: tokio::sync::Mutex::new(StreamState {
                text: String::new(),
                failed: false,
            }),
            sink,
        });
        (StreamOutput(inner.clone()), StreamGuard(inner))
    }
    #[tokio::test]
    async fn bounded_stream_latches_sink_failure_and_fences_stale_handles() {
        use crate::output::{OutputBuffer, OutputLimits, Retain};
        for fail in [false, true] {
            let sink = Arc::new(RecordingSink {
                fail,
                values: Mutex::new(Vec::new()),
            });
            let inner = Arc::new(BoundedStreamInner {
                open: AtomicBool::new(true),
                state: tokio::sync::Mutex::new(BoundedStreamState {
                    buffer: OutputBuffer::new(OutputLimits {
                        max_bytes: 3,
                        max_lines: 1,
                        retain: Retain::Tail,
                    }),
                    failed: false,
                }),
                sink: sink.clone(),
            });
            let output = BoundedStreamOutput(inner.clone());
            let guard = BoundedStreamGuard(inner);
            if fail {
                assert!(output.append("abcdef").await.is_err());
                assert!(output.append("again").await.is_err());
                assert!(output.snapshot().await.is_err());
            } else {
                output.append("abcdef").await.unwrap();
                assert_eq!(output.snapshot().await.unwrap().text, "def");
                assert_eq!(*sink.values.lock().unwrap(), vec!["def"]);
            }
            drop(guard);
            assert!(output.append("late").await.is_err());
            assert!(output.snapshot().await.is_err());
        }
    }

    #[tokio::test]
    async fn streamed_output_bounds_utf8_and_fences_handles() {
        let sink = Arc::new(RecordingSink {
            fail: false,
            values: Mutex::new(Vec::new()),
        });
        let (output, guard) = test_output(sink.clone());
        output.append("é").await.unwrap();
        output
            .append(&"x".repeat(MAX_STREAMED_OUTPUT_BYTES - 2))
            .await
            .unwrap();
        assert!(output.append("x").await.is_err());
        assert_eq!(
            sink.values.lock().unwrap().last().unwrap().len(),
            MAX_STREAMED_OUTPUT_BYTES
        );
        assert!(output.append("").await.is_err());
        drop(guard);
        assert!(output.append("late").await.is_err());
        let failed = Arc::new(RecordingSink {
            fail: true,
            values: Mutex::new(Vec::new()),
        });
        let (output, _guard) = test_output(failed);
        assert!(output.append("never committed").await.is_err());
        assert!(output.0.state.lock().await.failed);
        assert!(output.0.state.lock().await.text.is_empty());
    }

    #[tokio::test]
    async fn finishing_revokes_queued_writers_before_waiting_for_active_append() {
        let sink = Arc::new(RecordingSink {
            fail: false,
            values: Mutex::new(Vec::new()),
        });
        let (output, guard) = test_output(sink.clone());
        let active = output.0.state.lock().await;
        let queued = output.append("late");
        tokio::pin!(queued);
        assert!(futures_util::poll!(queued.as_mut()).is_pending());
        let finishing = guard.finish();
        tokio::pin!(finishing);
        assert!(futures_util::poll!(finishing.as_mut()).is_pending());
        drop(active);
        assert!(queued.await.is_err());
        assert!(finishing.await.text.is_empty());
        assert!(sink.values.lock().unwrap().is_empty());
    }

    #[test]
    fn registration_isolates_zero_sized_and_embedded_adapters() {
        let first = Arc::new(KeyedToolAdapter::new(ZeroSized));
        let second = Arc::new(KeyedToolAdapter::new(ZeroSized));
        let registered = first.register(ReplayPolicy::Safe);
        let other = second.register(ReplayPolicy::Safe);
        let first_id = registered_identity(&registered.tool).unwrap();
        let second_id = registered_identity(&other.tool).unwrap();
        assert!(!Arc::ptr_eq(&first_id, &second_id));
        assert!(Arc::ptr_eq(&first_id, &first.1));
        assert!(!Arc::ptr_eq(&first_id, &second.1));

        // The wrapper and its first field have the same address, but only an
        // explicitly registered adapter allocation may receive a token.
        let wrapper = Arc::new(Wrapper(KeyedToolAdapter::new(ZeroSized)));
        assert_eq!(
            Arc::as_ptr(&wrapper).cast::<()>(),
            (&wrapper.0 as *const KeyedToolAdapter<ZeroSized>).cast::<()>()
        );
        let ordinary: Arc<dyn Tool> = wrapper.clone();
        assert!(registered_identity(&ordinary).is_none());
        assert!(!Arc::ptr_eq(&first_id, &wrapper.0.1));
    }
}
