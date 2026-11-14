# pi-rs-durable (experimental)

An independent, local durable agent runtime for pi-rs. The existing CLI and
session journal do not use this runtime and their format is unchanged.

`runtime::Harness::open` obtains an exclusive local scheduler owner lock,
recovers unfinished work, and drives it to completion or a blocked state.
`create_conversation` stores model settings; `submit_and_run` durably admits a
request and executes it. A repeated request ID returns the existing submission.
`submit` only admits work; call `resume` to execute it. `open_paused` acquires
ownership and recovers the stored phases without executing them, for inspection.

Completed model responses, request IDs and tool intents are committed before
further work. On restart interrupted model requests are charged as unconfirmed
and retried with a fresh ID. A tool is replayed only when its recorded and
current policies are both `Safe`; unsafe interrupted tools return an interrupted
result by default. `RequireReconciliation` instead blocks until a host supplies
a known outcome via `reconcile`. An explicit `Unknown` or `Pending` tool outcome
also blocks. Tool result and its optional state update commit together. A
cancelled or dropped execution future poisons its harness; close and reopen it
before running more work. Remote effect idempotency remains the tool's and the
destination's responsibility.

This initial runtime is deliberately bounded and not full Pi Durable parity:

- Conversations are scheduled sequentially; no background worker, steering
  while a run is in progress, concurrent conversation execution, or task graph.
- A failed model request returns an error and leaves recovery for the next open;
  one failure can stop the current scheduling pass. `open_paused` permits
  inspection if automatic open encounters an error; `resume_conversation`
  drives one selected conversation. `continue_queued` releases work retained
  behind a failed run. No retry backoff yet.
- There is no partial-output checkpoint, watch, fork, compaction, signals,
  timers, subagent or nested-tool execution. Polling pending remote jobs is
  currently host-owned through `reconcile`.
- Conversation history is one JSON value and grows with the transcript; it is
  not intended for arbitrarily long-lived production workloads yet. `task_store`
  exposes separate durable task primitives but is not wired to the harness.
- Scheduler ownership uses a cooperating lock protocol and currently supports
  Unix local filesystems. Other processes must not bypass it or replace files.
  The model adapter applies 1 MiB text and 64 tool-call limits.

The design and planned parity work are in `docs/pi-durable-design.md`. Upstream
reference: `earendil-works/pi` commit `827383a15262905f82e418483825a49dbb1adbf5`.
