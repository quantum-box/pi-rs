# pi-rs-durable (experimental)

An independent, local durable agent runtime for pi-rs. The existing CLI and
session journal do not use this runtime and their format is unchanged. See
[`examples/resume.rs`](examples/resume.rs) for a runnable two-process demo.

`runtime::Scheduler::open` acquires exclusive local ownership and schedules
persisted work in the background. Its `submit` durably accepts input before
scheduling it; `wait` waits for a scheduled conversation and `shutdown` stops
and joins workers. Different conversations can run concurrently. Each
conversation executes one operation at a time. Follow-ups wait for the run;
`submit_with_mode` accepts durable steering at the next safe tool-round boundary. `runtime::Harness` also provides an explicitly driven
API with `submit_and_run`, `resume`, and `open_paused`. Request IDs deduplicate
within a conversation.

Model requests, completed responses, tool intents and results are committed
before the next effect. Interrupted model requests settle as unconfirmed before
retry. Tools rerun only if recorded and current replay policies are both safe;
unsafe tools normally produce an interruption result. `RequireReconciliation`
instead blocks pending authoritative resolution. Explicit `Unknown` blocks too.
A `Pending` remote operation is persisted and checked with its tool's read-only
poll method; the background scheduler retries pending polls once per second.
The explicitly driven Harness polls once per resume. Polling never redispatches
the original operation. External effect
idempotency remains the tool's and destination's responsibility.

The runtime can fork a settled conversation at its current state or a supported
historical snapshot cursor, and performs explicit idle compaction. It also
attempts compaction when a provider reports a context window and the estimated
request approaches it. Older committed snapshots remain in the observation
log. `history` and `watch_history` read or poll from an exclusive cursor; slow
consumers can reconnect. Snapshot records are stored atomically with state
transitions. History produced before the observation log was introduced is not
backfilled. `Scheduler::start_tasks` attaches a background generic task driver
to the same owner; registered synchronous handlers, timers, signals, and
direct-child waits use persistent task state. Hosts must register definitions
again after reopening. `reset` preserves older observation records and can
carry handoff text; it currently requires an idle conversation with no queue.

Nested tool calls are journaled by parent operation and call order. A replayed
parent reuses completed child results, while unsafe interrupted children block
the parent for reconciliation even when it ignores the child's result. Nested
calls are serialized, bounded in depth, and cannot update extension state.

Current limits and differences from upstream Pi Durable:

- Historical fork currently accepts settled snapshots, not every upstream entry.
  Automatic compaction requires a known context window; provider overflow retry
  and concurrent compaction remain unsupported. Streaming checkpoints retain
  text for aborted display but not all upstream live view/metadata semantics.
  There is no built-in subagent or automatic conversation/task ownership tree.
- Generic task waits support one deadline, one signal, or direct-child
  all/any/fail-fast joins. Combined waits and abort handlers are not yet
  implemented. Local cancellation propagates through descendants, but cannot
  cancel an already running external effect. Handler steps run synchronously
  and shutdown waits for an in-flight step.
- Polling uses an in-process interval rather than a durable timer; reopening
  resumes it. Model errors use a fixed retry delay. There is no flow control.
- The scheduler retains scheduled join handles until `wait` or `shutdown`;
  callers should bound submissions and drain them. Owner locking currently
  supports cooperating Unix processes on local filesystems.
- Each conversation is one growing JSON value. Observation reads use bounded
  pages, but committed snapshots copy messages; the runtime is not yet suitable
  for arbitrarily long-lived workloads. Model responses have a 1 MiB text and
  64 tool-call bound.
- The host must supply valid replay-safe tools, models and storage. Unknown
  effects still require destination-level idempotency or reconciliation.

The design and planned parity work are in `docs/pi-durable-design.md`. Upstream
reference: `earendil-works/pi` commit `827383a15262905f82e418483825a49dbb1adbf5`.
