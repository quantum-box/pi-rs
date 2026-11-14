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

    /// Return a control with the reply to opt into durable round termination or handoff.
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
/// Calls remain subject to the same serialization, fencing and sealing checks.
#[derive(Clone)]
pub struct KeyedHost {
    ordinary: Arc<dyn ToolHost>,
    nested: Arc<NestedHost>,
}
impl KeyedHost {
    pub fn ordinary(&self) -> &Arc<dyn ToolHost> {
        &self.ordinary
    }
    pub async fn call_keyed(&self, key: &str, name: &str, arguments: Value) -> Outcome {
        self.nested.call_keyed(key, name, arguments).await
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
                    Some(dispatch.nested.clone())
                }
            })
            .ok()
            .flatten();
        let Some(nested) = nested else {
            return Outcome::Skipped("keyed tool requires durable runtime".into()).into();
        };
        let (reply, control) = self
            .0
            .invoke_controlled(arguments, context, state, KeyedHost { ordinary, nested })
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
