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
    diagnostics: Option<Arc<Mutex<Vec<crate::runtime::ToolDiagnostic>>>>,
    stream: Option<Arc<dyn StreamSink>>,
}
tokio::task_local! { static CURRENT_HOST: Dispatch; }

/// Execute within the runtime's invocation-scoped nested host.
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
        None,
        None,
        future,
    )
    .await
}

pub(crate) async fn scope_with_control<F: std::future::Future>(
    nested: Arc<NestedHost>,
    tool: Arc<dyn Tool>,
    operation_id: uuid::Uuid,
    ordinary: Arc<dyn ToolHost>,
    control: Arc<Mutex<crate::reset::ToolControl>>,
    diagnostics: Option<Arc<Mutex<Vec<crate::runtime::ToolDiagnostic>>>>,
    stream: Option<Arc<dyn StreamSink>>,
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
    diagnostics: Option<Arc<Mutex<Vec<crate::runtime::ToolDiagnostic>>>>,
}
impl KeyedHost {
    /// Record a bounded diagnostic for the completed top-level tool result.
    pub fn diagnostic(&self, diagnostic: crate::runtime::ToolDiagnostic) -> Result<(), String> {
        if diagnostic.message.len() > 4096
            || diagnostic
                .code
                .as_ref()
                .is_some_and(|code| code.len() > 256)
        {
            return Err("tool diagnostic exceeds size limit".into());
        }
        let mut diagnostics = self
            .diagnostics
            .as_ref()
            .ok_or("nested tool diagnostics are not supported")?
            .lock()
            .map_err(|_| "tool diagnostic lock poisoned")?;
        if diagnostics.len() >= 32 {
            return Err("too many tool diagnostics".into());
        }
        diagnostics.push(diagnostic);
        Ok(())
    }
    pub fn ordinary(&self) -> &Arc<dyn ToolHost> {
        &self.ordinary
    }
    pub async fn call_keyed(&self, key: &str, name: &str, arguments: Value) -> Outcome {
        self.nested.call_keyed(key, name, arguments).await
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

// Weak entries keep allocations from being reused while they are registered. The
// token belongs to the adapter, not its address or concrete Rust type.
type RegistryEntry = (Weak<dyn Tool>, Weak<()>);
static REGISTRY: OnceLock<Mutex<Vec<RegistryEntry>>> = OnceLock::new();

fn registered_identity(tool: &Arc<dyn Tool>) -> Option<Arc<()>> {
    let mut entries = REGISTRY.get_or_init(Default::default).lock().ok()?;
    entries.retain(|(tool, identity)| tool.strong_count() > 0 && identity.strong_count() > 0);
    entries.iter().find_map(|(registered, identity)| {
        let registered = registered.upgrade()?;
        Arc::ptr_eq(&registered, tool)
            .then(|| identity.upgrade())
            .flatten()
    })
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
        entries.retain(|(tool, identity)| tool.strong_count() > 0 && identity.strong_count() > 0);
        if !entries
            .iter()
            .any(|(registered, _)| registered.ptr_eq(&Arc::downgrade(&tool)))
        {
            entries.push((Arc::downgrade(&tool), Arc::downgrade(&self.1)));
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
