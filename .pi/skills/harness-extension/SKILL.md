---
name: harness-extension
description: Add or update a compiled Rust extension and resume the current pi-rs session.
---

Read [the extension contract](../../../docs/extensions.md) and
[the working examples](../../../crates/workbench/src/lib.rs). Keep the
extension in a host crate and register it with `Registry::register`. Give
tools a JSON object schema and return a known result only when the execution
boundary confirms the outcome. Keep credentials in the host or adapter.

Before rebuilding, inspect the journal and save its current head. To experiment
on a separate conversation branch, use `checkout` with a settled record ID.
Keep the extension ID stable and implement `migrate` for any stored state
versions. Use `cargo test --locked --workspace`, rebuild the CLI, and point
the rebuilt CLI at the same journal. Use `resume` for a pending turn and
`run` to submit a new user turn. Resolve any unknown external outcome against
the authoritative service before continuing.
