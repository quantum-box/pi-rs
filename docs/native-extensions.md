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
