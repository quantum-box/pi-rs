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
within a conversation. `Harness`, `Scheduler`, and the lower-level
`Conversations` helper also expose `submit_with_options`: set
`AdmissionOptions::reject_when_busy` to refuse input while a run is active, or
`reject_when_idle` to refuse it while no run is active. Both may be set; an
existing request ID returns its original submission before either policy is
checked. Rejections persist nothing, and concurrent admissions recheck their
policies after a version conflict. The runtime still validates input before
checking a request ID and does not expose upstream's submit-in-commit API.

`Scheduler::abort` durably withdraws queued inputs and fences further local
dispatch for that conversation; `abort_and_wait` also waits for its workers.
Queued resets remain. Uncertain dispatched effects require reconciliation, even
for replay-safe tools; cancellation cannot undo an external effect. Reporter
workers use the same conversation fence as ordinary scheduled work.

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
carry handoff text; busy resets are queued until a safe boundary.

Nested tool calls are journaled by parent operation and call order. A replayed
parent reuses completed child results, while unsafe interrupted children block
the parent for reconciliation even when it ignores the child's result. Nested
calls are serialized, bounded in depth, and cannot update extension state.

Selected extensions may provide tools and before/after-tool hooks. The runtime
commits terminal tool output and state before invoking after-tool hooks; a crash
replays the hook with the same operation memo, not the completed tool. Hook
callbacks can run more than once and must memoize or make external effects
idempotent. Extension tools are conservatively unsafe to replay, and recovery
with selected extensions also avoids replaying potentially overridden host tools.
The runtime supports before-request message replacement and observes completed
responses; hook errors and non-string after-tool replacements have limited
reporting. Provider-body hooks, on-yield, after-tools and after-run hooks and
their durable child tasks are not integrated. Tool sleep gates remain unsupported.
The conversation signal inbox provides deduplicated receipts and transaction
fragments for a future wake, but is not connected to runtime admission, phase
persistence, scheduler rescanning, hook replay, abort, or recovery. In
particular, it does not yet persist sleep deadlines or enable sleeping hooks.

Current limits and differences from upstream Pi Durable:

- Historical fork currently accepts settled snapshots, not every upstream entry.
  Automatic compaction uses a known context window or provider overflow; manual
  compaction still requires an idle conversation. Streaming checkpoints retain
  text for aborted display but not all upstream live view/metadata semantics.
  The subagent link primitive does not provide an automatic conversation/task ownership tree.
- Generic task waits support deadlines, signals, deadline-or-signal sleeps, and direct-child
  all/any/fail-fast joins. `register_with_abort` and `request_abort` provide
  persisted, child-first cleanup for generic task-owned subtrees. Cleanup must
  tolerate replay after a crash; a missing handler produces a failed record.
  Handler steps run synchronously, so an abort request waits for the active step
  and cannot interrupt it. Legacy `cancel` and fail-fast cancellation bypass
  cleanup. Conversation abort does not propagate to these generic tasks.
  Shutdown waits for an in-flight step.
- Polling uses an in-process interval rather than a durable timer; reopening
  resumes it. Model retry deadlines use persisted bounded exponential backoff.
  There is no flow control.
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

`subagent` provides a persistent parent/key link to a separate conversation.
`ensure_subagent_with_history` can seed a child with a settled parent's messages
and tool state, using the requested child configuration. The seed is saved with
the reservation before creation, so retry after interruption uses the same
snapshot even if the parent has changed. Queued or running parents cannot be
forked. Existing links retain their original configuration and history mode.
Children do not inherit submissions, active work, or ownership.

`Scheduler::submit_subagent_reported` persists a reporter intent before child
admission. The background scheduler resumes pending intents after reopening,
submits the child input idempotently, and admits an answer or failure as a parent
follow-up. Multiple steering inputs sharing one answer use a persisted answer ID
and a single reserved parent request ID. Retrying after parent admission but
before the reporter receipt cannot insert a duplicate report. The prefix
`__pi_rs_subagent_report/` is reserved for the runtime in all conversations.
These guarantees assume the database is retained and all writers use this API;
they do not make model or tool external effects exactly once. Reporter work is
retried while the scheduler is open; `reporter_error` exposes the most recent
background error. Storage errors require shutdown and reopening. The explicitly
driven `Harness` persists intents but does not run the reporter; open a
`Scheduler` to deliver them. Legacy answers created before answer IDs cannot be
reported and require reconciliation.

This is not the complete upstream background-subagent extension. There is no
child-tool restriction, status command, or parent ownership tree.
A parent cancellation does not cancel child work. Scheduler shutdown requests
cooperative cancellation of all local workers and waits for them; it is not a
durable abort and cannot guarantee termination or rollback of external effects.
Dropping a scheduler stops its reporter poller; reopening resumes unfinished
intents. Report delivery can be delayed by other work in the same child
conversation. Reporter receipts and answer IDs are retained and scanned without
a retention policy.
Full upstream semantics require ownership/background boundaries and corresponding
task, tool and reporting integration.
