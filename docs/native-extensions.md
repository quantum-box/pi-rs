# Native Rust extensions

The CLI and terminal compose Rust implementations of the public `Extension`
trait. A host can use the same implementations through Cargo dependencies, or
load an explicitly selected Rust executable through the native process protocol.

## Bundled extensions

| Extension | Entry point | Behavior |
| --- | --- | --- |
| Plan mode | `/plan <plan>`, `/plan accept`, `/plan discard` | Restricts planning to declared read-only tools and persists the accepted plan |
| Branch review | `/review <task>` | Runs a read-only child session and records its result in the parent |
| Notes | `/notes <text>` | Stores bounded session notes and includes them in subsequent model context |
| TODO list | `/todo <JSON array>`, `update_todos` tool | Maintains a checklist with `pending`, `in_progress`, and `completed` states |
| Project instructions | `--project . --skill <name>` | Loads the project's AGENTS.md hierarchy and selected skills |
| Workbench context | `--instruction <text>` | Adds a host instruction and preserves versioned tool/compaction counters |

Notes and TODOs are enabled by the terminal. In one-shot CLI mode, add
`--extension notes` or `--extension todo`. `/notes` and `/todo` without an argument
show their saved state. `/extensions` lists the bundled terminal extensions.
`--disable-extension <id>` removes an optional extension from composition.
The TODO extension's ID is `todo-list`, while its CLI selector is `todo`.

All state changes use the existing journal. Reopening a session restores its
state, and checking out an earlier record restores the state at that branch.
A failed TODO update preserves the prior list. A successful update and its
extension state are committed with the same tool-result record.

Extension commands can persist branch-local custom entries without sending
them to the model. Opt in with `supports_command_entries` and return an
`ExtensionCommandOutput` from `command_output`; its optional `state` updates
the extension state and its `entries` contain typed JSON data. Entries appear
as Pi-style `custom` entries in RPC and follow the active branch across checkout
and restart. An SDK host can also call `AgentSession::append_custom_entry`.
[`bookmark_entry.rs`](../crates/extensions/examples/bookmark_entry.rs) shows an
executable extension. This currently covers command results and the SDK; it
does not yet expose immediate `appendEntry` calls from every hook or tool.

Commands can also add messages to the next model context. Implement
`supports_command_effects` and return `ExtensionCommandEffects` from
`command_effects`, containing ordered `Entry` and `Message` actions. A
`CustomMessage` carries a custom type, text or image content, a `display` flag,
and optional details. Content reaches the model regardless of `display`;
details remain extension metadata. The terminal displays messages whose flag is
true. SDK hosts can use `AgentSession::append_custom_message`. Executable
extensions opt into this protocol with `command_effects` in their descriptor;
older extensions continue to use `command_output`. Use a `TriggerMessage`
action to request a model turn after the command's effects have been applied;
multiple trigger actions in one command produce one turn. SDK hosts can call
`AgentSession::extension_command_and_run` for commands that may request a turn;
the synchronous command API rejects those requests. An idle SDK host can also
call `AgentSession::send_custom_message(message, true)` to start a model turn,
or pass `false` to save the message without a turn. Hooks and tools do not yet
expose Pi's immediate `sendMessage` API, streaming delivery modes, or custom
message renderers.

## Write another extension

Add the extension crate to the host's Cargo dependencies, implement
`pi_rs::Extension`, and register it before constructing the agent:

```rust
use std::sync::Arc;
use pi_rs::{Registry, Result};
use pi_rs_extensions::{Notes, TodoList};

fn registry() -> Result<Registry> {
    let mut registry = Registry::default();
    registry.register(Arc::new(Notes))?;
    registry.register(Arc::new(TodoList))?;
    Ok(registry)
}
```

The public API supports tool registration, commands, request-context hooks,
pre-tool decisions, post-tool state, state migrations, and compaction hooks.
Use stable extension IDs and explicit state versions. Let the host select the
allowed tools and register policy extensions before tools.

For successful or failed text results, opt in with `handles_tool_result` and
implement `tool_result_patch` to change output or details. Implement
`tool_result_patch_v2` to also change `is_error` independently. Omitted fields
retain their previous values, and handlers see earlier handlers' changes.
Patched output is passed verbatim to the model, while details stay outside model
context. This works for executable extensions. Existing patch hooks continue to
work; older executables receive a lossy legacy projection of patched failures,
which cannot represent failure details. The same projection applies to nested
host-call responses sent to older executables; current executables receive the
full result. Tool messages carry `is_error` independently of their text; older
context-hook executables retain the original status by tool-call ID when they
rewrite messages. Patches to non-text outcomes are reported as hook errors; use
the full-outcome hook for those results.

Extensions can intercept user-entered `!` and `!!` shell commands by opting in
with `handles_user_bash` and implementing `user_bash`. The event includes the
command, working directory, and whether the output is excluded from model
context. Return `None` to let the next handler or local shell run, or return
`Some(UserBashResult)` to supply the output and exit status. The first result
wins; errors stop execution. Cancellation also interrupts a running handler.
The same API works for executable extensions. For streaming operations, opt in
with `handles_user_bash_operations`, select commands in `user_bash_operations`,
and implement `execute_user_bash`. Send combined output bytes over its bounded
channel and return the exit code. The host streams sanitized UTF-8, applies the
normal shell output limits, retains truncated output in a file, and propagates
timeout and cancellation. Handlers are checked in registration order, whether
they supply a result or operations. See
[`streaming_shell.rs`](../crates/extensions/examples/streaming_shell.rs) for
a separately built executable example.

When embedding the coding tools, `ShellConfig::with_spawn_hook` customizes the
model-invoked shell tool's command, working directory, and environment after
the command prefix and session variables are applied. The hook is an in-process
Rust callback and is not applied to user `!` commands. Use
`with_session_environment(false)` to omit the tool's Pi session metadata.
Custom user-shell operations receive the configured command prefix at execution
time while the event and recorded transcript retain the original user command.

Native extensions can call `pi_rs_extensions::exec_command` to execute a
program directly with literal arguments, separate stdout and stderr, an optional
working directory and timeout, cancellation, and a combined output cap. It
returns the exit code, termination and truncation status. The helper is available
to in-process and separately built executable extensions. It does not invoke a
shell; request one explicitly when shell syntax is needed.

```rust,ignore
let result = pi_rs_extensions::exec_command(
    "git",
    &["status".into(), "--short".into()],
    &workspace,
    &pi_rs_extensions::ExecOptions {
        timeout: Some(std::time::Duration::from_secs(10)),
        cancellation: Some(cancel.clone()),
        ..Default::default()
    },
).await?;
```

To observe model or thinking-level changes, implement `handles_lifecycle` and
`handles_model_selection` and handle `LifecycleEvent::ModelSelect` and
`LifecycleEvent::ThinkingLevelSelect`. Terminal and RPC selection commands
deliver the new and previous values after the journal is updated, including
changes to the active model's per-model thinking setting. Model events identify
whether a model was explicitly set or selected by cycling; reselecting the same
value does not emit an event. Owned SDK hosts can await
`AgentSession::set_model_with_hooks` and provide model descriptors for the
event; the existing synchronous `set_model` does not invoke asynchronous hooks.
Pi v1.5.0 declares a `restore` source but its session restoration path does not
emit a model selection event.

Extensions that implement `handles_lifecycle` also receive `agent_settled`
after `agent_end` when a run has finished its retries and automatic compaction.
Its `aborted` field distinguishes cancellation from completion or failure.

Before a model request, an extension may opt into `context_with_system` to
transform the complete message list, including system messages. All ordinary
`before_request` hooks run first; these hooks then run in registration order.
Changes apply only to the current request and do not rewrite the session.
Executable extensions opt in through the same Rust trait and receive the
`context_with_system` protocol request with `messages` and branch-local `state`.
If a handler removes the leading system message, the host reports a non-fatal `context_with_system` extension error
attributed to that handler. Later handlers and the model still receive the
transformed messages, as in Pi v1.5.0.

An extension can also add temporary skills, prompt templates, and themes:

```rust,ignore
fn handles_resource_discovery(&self) -> bool { true }

async fn discover_resources(
    &self,
    event: &pi_rs::ResourceDiscoveryEvent,
    _state: Option<&pi_rs::ExtensionState>,
) -> pi_rs::ExtensionResult<pi_rs::DiscoveredResources> {
    Ok(pi_rs::DiscoveredResources {
        skill_paths: vec!["resources/skills".into()],
        prompt_paths: vec!["resources/prompts".into()],
        theme_paths: vec!["resources/themes".into()],
    })
}
```

Relative paths are resolved from the event's working directory, matching Pi;
absolute paths, home-relative paths, and local file URLs are also supported.
The callback runs after
session start and on `/reload`, with the active branch's extension state. Paths
are temporary, remain available when automatic discovery is disabled, and do
not override an existing resource with the same name. SDK hosts can call
`Registry::discover_resources` and integrate the returned paths into their own
resource loader.

[The execution contract](extensions.md) describes hook ordering, cancellation,
reconciliation, and durable state. [The standard implementations](../crates/extensions/src/lib.rs)
and [the plan-mode extension](../crates/plan-mode/src/lib.rs) provide working
examples. A newly written in-process extension is included when its Rust host is rebuilt.

## Load a separately built Rust extension

The example exports a custom `project-notes` extension through the versioned
native protocol. Build it once, then select its executable:

```sh
cargo build --locked -p pi-rs-extensions --example project_notes
cargo run --locked -p pi-rs-workbench --bin pi -- \
  --extension ./target/debug/examples/project_notes
```

Its commands can also run through the one-shot interface:

```sh
target/debug/pi --session /tmp/project-notes.jsonl \
  --extension ./target/debug/examples/project_notes \
  extension project-notes set 'Keep the public API compatible'
```

The entry point calls `serve_extension(Arc::new(MyExtension))`. The host obtains
the tool descriptions, command names and dependency IDs, then forwards context,
request, tool, compaction, command and migration hooks. Tool calls retain the
host's operation ID and execution classification. The session supplies the
previous extension state on each request. Each hook runs in a fresh process;
put durable state in `ExtensionState`.

Protocol version 1 uses a bounded JSON request and response on stdin/stdout.
Write diagnostic logs to stderr. Requests and responses are limited to 4 MiB,
and the host stops a request after 30 seconds. Unknown outcomes retain the
operation's reconciliation requirement. Load only executables you trust with
the permissions and environment of the local CLI.

Current executables also advertise bidirectional host calls through protocol v2.
A tool can override `invoke_with_host` or `invoke_transaction`, inspect
`host.tools()` and call `host.call(name, arguments)` or `host.call_batch(calls)`.
Batches preserve result order and allow host-approved parallel reads. Every
nested call keeps the host's validation, policies, journal and cancellation.
Protocol-v2 frames are bounded to 4 MiB, and invocation follows the host's
configured tool deadline. Metadata and other hooks retain protocol v1.

A working host-call example is available with the standard extensions.

```sh
cargo build --locked -p pi-rs-extensions --example parallel_read
pi -e ./target/debug/examples/parallel_read
```

The `parallel_read` tool accepts a `paths` array and reads those files through
the host's registered `read` tool.

## Create and install a native package

```sh
pi init-extension ./my-extension
pi install ./my-extension
pi --session /tmp/native.jsonl extension my-extension set 'Project guidance'
pi list
pi update ./my-extension
pi remove ./my-extension
```

The generated Cargo binary implements the native extension protocol. For work
against a local pi-rs checkout, pass `--dev-path /path/to/pi-rs` to
`init-extension`. Installation builds a release binary into the agent package
cache and validates its descriptor before adding it to settings. Installed
extensions load in the CLI, terminal and RPC runtime. Reload the terminal after
changing installed packages.

Sources can be local binaries, local Cargo packages, `cargo:crate@version`, or
`git:https://host/repository.git@ref` (`#ref` also works). `install -l` and `remove -l` select
project settings. Project packages load only after the project is trusted.
Removal updates settings and retains the build cache.

Packages may declare `extensions`, `skills`, `prompts`, and `themes` in
`[package.metadata.pi]` or the `pi` object of `pi-package.json`. Extension names
select compiled binaries. Resource paths select files or directories under the
package root, with glob patterns, `!`/`-` exclusions and exact `+` re-inclusions. Without declarations, the conventional `skills`, `prompts`,
`themes` and `extensions` directories are discovered. An empty list disables
that resource kind. A settings package entry may additionally restrict these
resources with `{ "source": "...", "skills": ["skills/example"] }`.

Native executable tool descriptors also advertise `can_poll`. The process
protocol forwards polling by tool name, stable job reference and original
operation ID. Existing executable descriptors default this capability to false.

## Deferred tool discovery

`pi_rs_extensions::tool_search::ToolSearch` accepts `SearchDocument` values for
registered tools that should be discovered on demand. It hides those schemas
until `tool_search` finds a match. Ranking uses names, descriptions, schema
property descriptions and namespace text. Loaded names are stored in extension
state, so they follow checkout, fork and resume.

The local MCP integration creates this extension for `deferred` tools. Exact
`toolExposure` entries take precedence over wildcard entries. A CLI tool
allowlist without an MCP namespace keeps those MCP tools discoverable while
omitting their schemas from the initial model request.

Stateful native tools may implement `invoke_with_state` to receive their owning
extension's current branch state. The default delegates to `invoke`, preserving
existing tools. The executable protocol forwards the same optional state.

## Native interaction

Tools can inspect `host.has_ui()` and call `host.ui(request).await` inside
`invoke_with_host`. `UiRequest` supports selection, confirmation, text input,
multiline editing, notifications, status entries, positioned widgets, window
titles and editor text. Dialog requests can carry an optional millisecond
`timeout`. Cancelled dialogs return `UiResponse::Cancelled`.

```sh
cargo build --locked -p pi-rs-extensions --example interactive_review
pi -e ./target/debug/examples/interactive_review
```

Ask the agent to call `interactive_review` to exercise the dialogs. The native
executable exchanges correlated UI frames with the host. RPC clients receive
`extension_ui_request` and answer with `extension_ui_response`. Embedded Rust
applications install their own `ExtensionUi` on `SessionBuilder` or `Registry`.

Set `autoload` to `false` in a package declaration to load only explicitly
selected resources. When a trusted project uses this declaration for a package
already selected by the user, its patterns apply as ordered changes to that
user selection. For example, a project can remove one prompt with
`"prompts": ["-prompts/example.md"]` and retain the other user-selected resources.

Model-generated manual and automatic threshold compactions emit
`session_compact` after the compacted record is committed, or
`session_compact_failed` if summary generation or commit fails. These events
are delivered to extensions that opt into session lifecycle notifications.
Direct SDK compaction with a supplied summary remains synchronous and does not
emit these asynchronous lifecycle notifications.

`agent_settled` follows a terminal run result; a run waiting for an external
operation or reconciliation emits `agent_end` but does not claim settlement.
After the operation is resolved and the run continues to a terminal result,
the extension receives `agent_settled`.

Extensions may also opt into `tool_execution_start`, `tool_execution_update`, and `tool_execution_end`
with both `handles_lifecycle` and `handles_tool_execution_lifecycle`. Events
include the tool call ID, name and arguments or the final native outcome;
end events indicate errors and include execution duration when the tool ran.
Nested tool calls include their parent tool-call ID. Executable extensions
advertise this capability with `tool_execution_lifecycle_hook`. A tool using
`invoke_with_host` may call `host.report_progress(partial_result).await` to emit
ephemeral update events during execution. Progress is limited to 1 MiB per
update and is not journaled. The bounded progress queue returns an error when
full so a slow observer cannot indefinitely block a tool. The v2 executable protocol negotiates support;
new executables work with old hosts, where progress is ignored. The built-in
shell tool publishes bounded cumulative output snapshots while it runs; slow
observers can receive coalesced updates. Parallel calls currently emit end
notifications in journal order after the batch settles; completion-order
notifications and progress from other built-in tools remain open.
Observers opt into updates separately with `handles_tool_execution_updates`
(`tool_execution_update_hook` for executable extensions), so older observers
continue to receive only event types they understand.

Executable extensions built with the current SDK advertise
`pi_lifecycle_fields` and receive Pi's camelCase lifecycle and tree fields,
such as `previousSessionFile`, `entryId`, and `turnIndex`. Older executables
that do not advertise this capability continue receiving the previous
snake_case fields. New executables accept either format, and return tree
decisions in the format requested by the host, so older hosts remain usable.
