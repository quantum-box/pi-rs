# Execution and extension contract

## Compose a host

Create a `Registry` and register `Arc<dyn Extension>` in policy order.
An extension can register `Arc<dyn Tool>` values and hook context building,
dispatch, tool results, and compaction. The workbench uses separate extensions
for policy, tools, and context. Add another crate as a dependency of the host
and call `registry.register(Arc::new(MyExtension))?` in the host's composition
code. [Working implementation](../crates/workbench/src/lib.rs).

For example, a context extension can be built and registered in the host crate.

```rust
use pi_rs::{Extension, ExtensionResult, ExtensionState, Message, ModelRequest};

pub struct StatusNote;

impl Extension for StatusNote {
    fn id(&self) -> &str {
        "status-note"
    }

    fn context(
        &self,
        request: &mut ModelRequest,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        let Some(Message::System { content }) = request.messages.first_mut() else {
            return Err("expected a system message".into());
        };
        content.push_str("\nReport the current work status in one sentence.");
        Ok(())
    }
}
```

Give each tool a stable name, description, and JSON object schema. Duplicate
extension IDs or tool names and invalid schemas reject registration as a unit.
The CLI opts into `AGENTS.md` and named skills. Treat project files, tool
outputs, model text, and summaries as data from their respective trust levels.

## Hook and state lifecycle

1. On opening a turn or compacting, `migrate` receives the extension's last
   state on this branch. Return a replacement only when initialization or
   migration is needed. Store a version alongside the JSON value and
   explicitly reject unsupported future versions.
2. `context` sees the assembled request before each model call. Hooks run in
   registration order and receive the latest committed branch state.
   `Notification::Request` exposes the final context and tool schema.
3. The journal records the model's complete assistant message and proposed
   operations. Each tool is dispatched in model order, after the harness
   checks the registered schema and the pre-tool hooks. The first denial
   becomes a skipped operation. A hook error stops the run. Cancellation or a
   hook timeout before dispatch also leaves that operation unstarted.
4. `after_tool` receives the outcome and can return a new state. The tool
   result and successful state changes are saved in one journal record. On
   a hook error the known result is still saved and the run stops.
5. Compaction preserves complete turns. `before_compaction` can change the
   candidate summary or retention count, or deny the change.
   `after_compaction` prepares state changes for the same commit as the
   summary. Keep these hooks free of external effects.

Register only trusted Rust extensions. A hook is an application policy.
The host and the destination service enforce authorization, workspace isolation,
network access, and business approvals. The workbench's path checks and fixed
command suit a trusted development directory. A tenant-controlled workspace
needs an isolated runtime and a host-provided tool implementation.

To iterate, inspect the current head, use `checkout` to try a branch, add or
modify the extension, build the host, and open the same journal. The active
branch determines both conversation and extension state. The workbench
migrates its v1 numeric tool counter into a v2 object that also tracks
compactions. The repository exposes this workflow to the agent through
[the harness-extension skill](../.pi/skills/harness-extension/SKILL.md).

## Persistence and recovery

| State | Evidence and action |
| --- | --- |
| Planned | Model call is committed, and dispatch has yet to be recorded. On interruption it is marked skipped. |
| Started | Dispatch was committed before calling the tool. The operation ID is passed through `ToolContext`. |
| Succeeded / failed | The tool supplied a known outcome and the harness committed it. A failure means the adapter has enough evidence to classify it as such. |
| Skipped | The call was invalid, denied, or abandoned before the external operation began. |
| Unknown | The run lost a result after dispatch. Resume requires authoritative reconciliation. |

For effects, map `operation_id` to the destination's supported idempotency
or lookup key. Keep tenant, actor, and approval binding at the destination.
Use `Outcome::Unknown` when an effect may have committed, including ambiguous
HTTP errors. Obtain evidence from the destination or an authorized human
before `Session::reconcile`. The harness never invokes an uncertain
operation again as part of recovery. Reconciliation and branch checkout
restore conversation state. External effects remain in the destination.

The session is a local JSONL file with one exclusive writer. Newline-terminated
records are committed in order. A valid journal can discard an unterminated
tail on reopen. A malformed committed record fails closed. Use independent
journals for concurrent sessions. Tools within one turn run sequentially.
This store holds the history in memory and assumes a local filesystem with
file locking and durable writes. It is suited to an isolated worker or a
developer session.

The adapter buffers tool calls until it receives a complete SSE response.
Transient text can stream to observers. After a disconnected response,
`resume` can issue the model request again without replaying a completed
tool call. The request count, per-request output cap, run output budget,
and model/tool timeouts are host settings. When usage is absent, the
harness deducts the full output allowance. Production spend control belongs
at the model gateway.

Transport credentials are separate from model context and structured events.
The journal stores actual conversation payloads, and the opt-in trace exposes
the context sent to the adapter. Use fixture data for full traces. A managed
deployment needs a tenant-scoped store, content redaction at the observation
boundary, and retention policies.
