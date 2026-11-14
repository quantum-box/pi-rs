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
policies after a version conflict. A duplicate input request ID is resolved before content validation. The runtime
does not expose upstream's submit-in-commit API.

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

`Harness::view` and `Scheduler::view` capture conversation metadata, the
committed running model/compaction progress checkpoint, and a newest-first
message page. Pass 50 for a window comparable to upstream's initial window.
`view_page` follows `older` through the same immutable snapshot even after
later commits or resets. Each call visits at most the requested number of
transcript records and returns at most that many messages; an empty page can
still have an older cursor. Submission, abort and other persisted conversation
fields remain visible. Legacy conversations without a transcript must first be
saved by the runtime. Cursors are in-process handles, not a wire format.

This pull API does not provide upstream's watch operations, `from`, automatic
min/max window maintenance, or transcript entries for reset and compaction
markers. Progress updates alone do not appear in observation history; poll the
view for them. Message count and record reads are bounded, but a stored batch
(especially a replacement), message, and conversation metadata can each be
arbitrarily large and are decoded in full. Bytes, metadata copying and peak
memory are therefore not independently bounded. Existing history and execution
paths still hydrate all messages. Store reads are synchronous and may block.

Nested tool calls are journaled by parent operation and call order. The standalone
`NestedHost::call_keyed` API also accepts explicit caller-scoped keys and reuses
completed results across reordered replay, rejecting a changed tool or arguments.
The core `ToolHost` interface exposes only positional calls, so tools invoked by
the runtime cannot request keys through it without a core integration change.
Unsafe interrupted children block the parent for reconciliation even if ignored;
so do omitted recorded calls in a replayed scope, unlike upstream. Calls are
serialized, bounded in depth, and cannot update extension state. A safe replay
still requires destination-level idempotency for external effects.

Selected extensions may provide tools and before/after-tool hooks. The runtime
commits terminal tool output and state before invoking after-tool hooks; a crash
replays the hook with the same operation memo, not the completed tool. Hook
callbacks can run more than once and must memoize or make external effects
idempotent. Extension tools are conservatively unsafe to replay, and recovery
with selected extensions also avoids replaying potentially overridden host tools.
The runtime supports before-request message replacement and observes completed
responses. Non-string after-tool replacements are ignored without reporting. After-tools receives the completed round's calls; after-run receives
`{"answer": ...}` after the answer and submission settlement are committed.
Both run in persistent phases with a stable operation memo and may execute again
after a crash. Nonempty requests are committed as conversation-owned tasks in
the same transaction as a persistent wait phase and its child IDs. The runtime
waits for all children to become terminal before proceeding. The host must start
a task driver and register the exact selected extension handler with
`Registry::register_shared` and its replay policy; a missing driver leaves the
hook phase pending, and a mismatched registration fails closed. Reopen with the
extensions and driver registered to resume. Harness users explicitly resume;
Scheduler polls waiting phases. Ordinary hook and section errors are reported and skipped; before-tool errors block dispatch. An after-tools child that succeeds with exactly `{ "terminate": true }` settles the current inputs with the tool-calling answer and runs after-run hooks before queued inputs. onYield may create tasks and chooses the first successful nonempty string continuation in creation order; queued input or reset takes precedence. Failed hook children are reported when observed; invalid child outcomes are ignored without reporting. Unregistered or foreign task requests are reported and skipped. Provider-body hooks and tool-result terminate controls remain unsupported. Before-tool hooks may sleep on
an absolute deadline or named conversation signals; `send_signal` deduplicates
receipts, and wake consumption commits atomically with the phase transition.
The hook sees the wake in `Context.snapshot["phase"]["ToolPreparing"]["wake"]`.
The scheduler rescans once per second and reopening resumes the wait. Signal
names are conversation-scoped single-value keys: a wake consumes its key, and a deadline closes empty keys. A repeated receipt ID is idempotent; a different ID for an occupied key is rejected. The `set_signal_clock` hook supports deterministic deadline tests. Unlike upstream typed signal/key pairs, this API uses a single string name.

`TaskStore::commit_owned_tasks` and `prepare_owned_tasks` also expose the
lower-level atomic checkpoint and child creation primitive to other hosts.
Conversation abort atomically marks owned task descendants and fences new
admissions. Cleanup runs child-first after restart when a task driver or
scheduler is attached. Synchronous handlers cannot be interrupted and cleanup
can replay after a crash; external effects require idempotency and reconciliation.
Conversation abort skips tasks explicitly marked as background and their
descendants; subagent conversations are not yet part of that ownership graph.
Current limits and differences from upstream Pi Durable:

- Historical forks accept safe committed observation snapshots, including during runs,
  but not every upstream entry or incomplete tool batch.
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
  cleanup. Conversation abort marks foreground conversation-owned task descendants;
  separate subagent conversations are not included. Shutdown waits for an in-flight step.
- Polling uses an in-process interval rather than a durable timer; reopening
  resumes it. Model retry deadlines use persisted bounded exponential backoff.
  There is no flow control.
- The scheduler retains scheduled join handles until `wait` or `shutdown`;
  callers should bound submissions and drain them. Owner locking currently
  supports cooperating Unix processes on local filesystems.
- New commits store message deltas in an immutable transcript, atomically with
  state, observation and fork metadata; legacy conversations migrate on their
  next successful save. Old records are retained. Context replacement during
  reset or compaction can still copy messages; subagent reservations retain a
  seed copy. Fork heads share immutable transcript entries.
  History reads reconstruct complete snapshots, and submissions and other metadata
  are still copied in each commit, so storage and memory remain unbounded. No
  garbage collection is provided. Older binaries cannot read migrated state.
  Model responses have a 1 MiB text and 64 tool-call bound.
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

### Durable queue delivery modes

`Harness::set_queue_modes` and `Scheduler::set_queue_modes` persist a
conversation's `QueueModes { steering_mode, follow_up_mode }`. Both default to
`QueueMode::OneAtATime`, including when reading older conversation records.
Set either independently to `QueueMode::All` to place all queued inputs of
that kind at the next eligible boundary. Post-tool boundaries select steers;
final boundaries select both kinds in admission order, after queued resets
and writes. Changes made during a run apply at the next boundary. Forks retain
the selections.

### Opt-in local coding tools

Enable the `coding-tools` feature and pass
`pi_rs_durable::coding_tools::local(workspace)?` as `Options::tools` (or extend
that list) to register the workbench's `read`, `write`, `edit`, and `bash`.
The directory must already exist. This is the local workbench profile: paths
and shell commands can access resources outside the directory; it is not a
sandbox. Supply authorization and isolation appropriate for the host.

All four registrations default to `ReplayPolicy::Unsafe`, as in upstream's
coding tools. A committed result is reused, but an interrupted dispatch may
have partially run. The configured uncertain policy returns an interruption or
requires host reconciliation. Cancellation or timeout cannot prove that a
mutation or command did not happen. A host should change a replay policy only
when it can establish that replay is safe for every call to that registration.

The adapter uses pi-rs workbench schemas, output, image handling, shell and
mutation behavior, which are not identical to upstream Durable's environment,
structured outputs, diagnostics, streaming and file mutation queue. It does
not install tools automatically, provide an environment abstraction, or add
exactly-once execution. The optional dependency pulls in the workbench and its
dependencies.

The native `before_compact` extension hook runs on manual, threshold and
provider-overflow compaction after message-range selection. The first decline
or summary wins; a hook error is skipped. The hook's task-scoped memo survives
interruption and reopening, and the selected range and operation identity are
persisted before invocation. Hook-supplied summaries bypass model usage.

This is a partial upstream durable hook: the host selects message ranges rather
than upstream entry records, so it exposes messages and a message index rather
than entry IDs or head markers. Manual compaction currently has no instructions
argument. Hook errors are retained in the extension report log; the harness
does not yet expose upstream's `onReport` callback. Empty hook
summaries are rejected by the existing compaction validation. Compaction is
still subject to the host's existing idle/manual and context-selection limits.

### Experimental custom documents and checked composition

`document::Documents` reads application-addressed JSON objects with store
versions. `document::Transaction` stages version-checked puts and deletes for
atomic commit, and can append `TaskStore::prepare_save` or
`prepare_owned_tasks` batches. Each participant must provide checks for its
writes; duplicate checks or writes are rejected. All checks see pre-commit
state. A conflict rolls back all participants. Reload snapshots after conflict;
commit errors may have an unknown outcome and require inspection on reopen.
Versions are store-wide CAS tokens, not upstream document versions or commit
sequence numbers. Addresses are caller-defined and do not enforce ownership.
Raw prepared batches are trusted and must respect reserved namespaces and JSON
invariants. Do not perform external effects while preparing a batch.

`Harness::submit_with_documents` and `Scheduler::submit_with_documents` admit one new input and document changes in
the same durable commit as the conversation, transcript and observation updates.
Its callback reads documents before staging any writes; every read, including
missing or read-only values, is checked at commit. Write only addresses read in
the attempt, once each. A CAS conflict reruns the whole callback, so it must be
deterministic and have no external effects. Duplicate request IDs return the
original submission without running the callback, and rejections do not write.
A failed storage commit can have an unknown outcome: reopen and reconcile using
a request ID. This API does not schedule the input; call `resume` as usual.

This is a synchronous, bounded API, not upstream `Session.commit` or
Chord: it has no upstream-style draft transactions, general transaction snapshot reads, definitions,
operation history, observation, fork/rewind, deferred persistence, or automatic
lifecycle/ownership validation for arbitrary host batches. Admission callbacks cannot compose arbitrary task batches; task batches
are low-level records and do not by themselves reserve executors or schedule work.

### Extension error observation

`Harness::extension_reports()` and `Scheduler::extension_reports()` return persisted
batches with conversation and operation IDs, phase, extension, operation and error.
The first batch for an operation and phase wins across retries and reopening. This
is a polling log, not upstream's `onReport` callback: it has no cursor, deletion,
retention policy, or notification and grows without bound. Hook task failures are
reported when their parent observes them; general task failures and handler `Err` (which poisons the task driver), storage, host and
listener errors are not covered. Invalid child decisions, malformed hook values,
wrapped tools, and some tool execution errors are not yet reported here. A batch is
committed separately from the phase decision; a crash can rerun hooks and their
external effects, which require memoization or destination-level idempotency. The
first recorded batch may differ from a later retry's errors; callback delivery and
exactly-once external effects are not guaranteed.
