# pi-rs-durable (experimental foundation)

This crate is a separate, unfinished Rust foundation for a Pi Durable-style
runtime. It does not yet provide `Harness`, a scheduler, model or tool
dispatch, task recovery, conversation forks, watch APIs, or automatic resume.
Do not use the state transitions as a crash-safe agent runner.

It currently contains:

- SQLite-backed atomic conversation transitions, plus ephemeral memory storage.
- Request-ID deduplication within a conversation, persistent queued inputs,
  explicit placement and settlement, and a small model stream adapter.
- Pure recovery-policy decisions; nothing executes or reconciles tools yet.
- A standalone serializable task state machine; it is not yet wired into storage.

The conversation state is currently stored as one JSON value. It grows with
history and does not provide the paginated storage or task-level atomic commits
needed for long-lived workloads. SQLite synchronizes commits, but there is no
exclusive lifetime scheduler lock in `Conversations`. An `OwnedStore` guard is
available separately on Unix, but is not yet wired into this API. Multiple storage connections can use CAS;
this alone does not make concurrent model or tool dispatch safe. Transient model
progress is not persisted. The model adapter has a 1 MiB text and 64 tool-call
limit; these are initial local limits, not claims of upstream parity.

`docs/pi-durable-design.md` describes the proposed end state. Existing pi-rs
sessions and CLI continue to use their existing engine and journal format.
