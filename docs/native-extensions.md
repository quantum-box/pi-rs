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
