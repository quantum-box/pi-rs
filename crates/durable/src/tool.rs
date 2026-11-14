//! Tool execution without persistence or retry. The host must validate policy and
//! commit the stable operation ID, prepared arguments and replay policy before
//! dispatch. Never infer replay safety from `ToolExecution` or from cancellation.

use std::{sync::Arc, time::Duration};

use pi_rs_core::{
    CancellationToken, ExtensionState, Invocation, Outcome, Tool, ToolContext, ToolHost, ToolReply,
    parse_tool_arguments,
};
use serde_json::Value;

/// Arguments normalized and validated against the current tool schema.
/// Persist `arguments()` along with the invocation before calling `execute`.
#[derive(Debug)]
pub struct PreparedTool {
    name: String,
    call_id: String,
    operation_id: uuid::Uuid,
    arguments: Value,
}

impl PreparedTool {
    pub fn arguments(&self) -> &Value {
        &self.arguments
    }
}

/// Parse and normalize the completed call, then validate against its schema.
/// `validate` must implement JSON Schema for the supplied current tool spec;
/// it is required because core's compiled validator is private. Preparation
/// must not perform external effects. Authorization remains the host's job.
/// Errors are known pre-dispatch skips; cancellation cannot prove that an
/// effectful implementation of `prepare_arguments` had no effects.
pub async fn prepare<F>(
    tool: &dyn Tool,
    invocation: &Invocation,
    cancel: &CancellationToken,
    timeout: Duration,
    validate: F,
) -> Result<PreparedTool, Outcome>
where
    F: FnOnce(&Value, &Value) -> bool,
{
    if cancel.is_cancelled() {
        return Err(Outcome::Skipped("cancelled before dispatch".into()));
    }
    let spec = tool.spec();
    if invocation.call.name != spec.name || invocation.call.id.is_empty() {
        return Err(Outcome::Skipped("invalid tool identity".into()));
    }
    let args = parse_tool_arguments(&invocation.call.arguments)
        .map_err(|e| Outcome::Skipped(format!("invalid tool arguments: {e}")))?;
    let args = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(Outcome::Skipped("cancelled before dispatch".into())),
        result = tokio::time::timeout(timeout, tool.prepare_arguments(args)) => match result {
            Ok(Ok(args)) => args,
            Ok(Err(e)) => return Err(Outcome::Skipped(format!("argument preparation failed: {e}"))),
            Err(_) => return Err(Outcome::Skipped("argument preparation timed out".into())),
        }
    };
    if args.to_string().len() > 16 * 1024 * 1024 {
        return Err(Outcome::Skipped(
            "prepared arguments exceed size limit".into(),
        ));
    }
    if !validate(&spec.parameters, &args) {
        return Err(Outcome::Skipped(
            "arguments do not match the tool's JSON Schema".into(),
        ));
    }
    Ok(PreparedTool {
        name: spec.name,
        call_id: invocation.call.id.clone(),
        operation_id: invocation.operation_id,
        arguments: args,
    })
}

/// Reconstruct a prepared call from the exact arguments stored with its intent.
/// Call only after the host's recovery policy permits replay, and validate
/// against the current registration. This function does not grant replay safety.
pub fn restore<F>(
    tool: &dyn Tool,
    invocation: &Invocation,
    arguments: Value,
    validate: F,
) -> Result<PreparedTool, Outcome>
where
    F: FnOnce(&Value, &Value) -> bool,
{
    let spec = tool.spec();
    if invocation.call.name != spec.name || invocation.call.id.is_empty() {
        return Err(Outcome::Skipped("invalid tool identity".into()));
    }
    if arguments.to_string().len() > 16 * 1024 * 1024 || !validate(&spec.parameters, &arguments) {
        return Err(Outcome::Skipped(
            "stored arguments do not match the current tool schema or size limit".into(),
        ));
    }
    Ok(PreparedTool {
        name: spec.name,
        call_id: invocation.call.id.clone(),
        operation_id: invocation.operation_id,
        arguments,
    })
}

/// Execute once after the caller has durably committed the exact prepared intent.
/// The caller must also commit the returned reply and any state atomically.
/// A dropped future or crash after dispatch leaves an uncertain outcome. An
/// unknown or pending result, including cancellation and timeout, is not proof
/// of non-execution. Recovery and replay decisions belong to the host.
pub async fn execute(
    tool: &dyn Tool,
    invocation: &Invocation,
    prepared: PreparedTool,
    state: Option<&ExtensionState>,
    host: Arc<dyn ToolHost>,
    cancel: &CancellationToken,
    timeout: Duration,
) -> ToolReply {
    if invocation.call.name != prepared.name
        || invocation.call.id != prepared.call_id
        || invocation.operation_id != prepared.operation_id
        || tool.spec().name != prepared.name
    {
        return Outcome::Skipped("tool identity changed before dispatch".into()).into();
    }
    if cancel.is_cancelled() {
        return Outcome::Skipped("cancelled before dispatch".into()).into();
    }
    let child = cancel.child_token();
    let _guard = child.clone().drop_guard();
    let context = ToolContext {
        operation_id: invocation.operation_id,
        cancellation: child,
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Outcome::Unknown("cancelled after dispatch began".into()).into(),
        result = tokio::time::timeout(timeout, tool.invoke_transaction(prepared.arguments, context, state, host)) =>
            result.unwrap_or_else(|_| Outcome::Unknown("tool timed out after dispatch began".into()).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use pi_rs_core::{ExtensionResult, ToolSpec};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Fake {
        calls: AtomicUsize,
        wait: bool,
    }
    #[async_trait]
    impl Tool for Fake {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "test".into(),
                description: "".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn prepare_arguments(&self, mut args: Value) -> ExtensionResult<Value> {
            args["normalized"] = json!(true);
            Ok(args)
        }
        async fn invoke(&self, args: Value, ctx: ToolContext) -> Outcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.wait {
                ctx.cancellation.cancelled().await;
            }
            Outcome::Succeeded(args.to_string())
        }
    }
    struct Host;
    #[async_trait]
    impl ToolHost for Host {
        fn tools(&self) -> Vec<ToolSpec> {
            vec![]
        }
        async fn call(&self, _: &str, _: Value) -> Outcome {
            Outcome::Skipped("unused".into())
        }
    }
    fn invocation(raw: &str) -> Invocation {
        Invocation {
            operation_id: uuid::Uuid::new_v4(),
            call: pi_rs_core::ToolCall {
                id: "c".into(),
                name: "test".into(),
                arguments: raw.into(),
            },
        }
    }
    fn fake(wait: bool) -> Fake {
        Fake {
            calls: AtomicUsize::new(0),
            wait,
        }
    }
    const LIMIT: Duration = Duration::from_secs(1);

    #[tokio::test]
    async fn validates_normalized_input_before_dispatch() {
        let tool = fake(false);
        let token = CancellationToken::new();
        let call = invocation("{}");
        let rejected = prepare(&tool, &call, &token, LIMIT, |_, _| false)
            .await
            .unwrap_err();
        assert!(matches!(rejected, Outcome::Skipped(_)));
        assert!(
            prepare(&tool, &invocation("[1]"), &token, LIMIT, |_, _| true)
                .await
                .is_err()
        );
        let ready = prepare(&tool, &call, &token, LIMIT, |schema, args| {
            schema["type"] == "object" && args["normalized"] == true
        })
        .await
        .unwrap();
        assert_eq!(ready.arguments()["normalized"], true);
        let reply = execute(&tool, &call, ready, None, Arc::new(Host), &token, LIMIT).await;
        assert!(matches!(reply.outcome, Outcome::Succeeded(_)));
        assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn cancellation_before_and_timeout_after_dispatch_are_distinct() {
        let tool = fake(true);
        let token = CancellationToken::new();
        let call = invocation("{}");
        let ready = prepare(&tool, &call, &token, LIMIT, |_, _| true)
            .await
            .unwrap();
        let reply = execute(
            &tool,
            &call,
            ready,
            None,
            Arc::new(Host),
            &token,
            Duration::from_millis(1),
        )
        .await;
        assert!(matches!(reply.outcome, Outcome::Unknown(_)));
        let ready = prepare(&tool, &call, &token, LIMIT, |_, _| true)
            .await
            .unwrap();
        token.cancel();
        let reply = execute(&tool, &call, ready, None, Arc::new(Host), &token, LIMIT).await;
        assert!(matches!(reply.outcome, Outcome::Skipped(_)));
        assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn refuses_substituted_invocation() {
        let tool = fake(false);
        let token = CancellationToken::new();
        let call = invocation("{}");
        let ready = prepare(&tool, &call, &token, LIMIT, |_, _| true)
            .await
            .unwrap();
        let reply = execute(
            &tool,
            &invocation("{}"),
            ready,
            None,
            Arc::new(Host),
            &token,
            LIMIT,
        )
        .await;
        assert!(matches!(reply.outcome, Outcome::Skipped(_)));
        assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn cancellation_after_start_is_uncertain_and_restore_revalidates() {
        let tool = fake(true);
        let token = CancellationToken::new();
        let call = invocation("{}");
        let ready = prepare(&tool, &call, &token, LIMIT, |_, _| true)
            .await
            .unwrap();
        let stored = ready.arguments().clone();
        assert!(restore(&tool, &call, stored.clone(), |_, _| false).is_err());
        let ready = restore(&tool, &call, stored, |_, _| true).unwrap();
        let cancel = token.clone();
        let trigger = async {
            tokio::task::yield_now().await;
            cancel.cancel();
        };
        let (reply, ()) = tokio::join!(
            execute(&tool, &call, ready, None, Arc::new(Host), &token, LIMIT),
            trigger
        );
        assert!(matches!(reply.outcome, Outcome::Unknown(_)));
        assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    }
}
