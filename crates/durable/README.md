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
workers use the same conversation fence as ordinary scheduled work. The store owner
shares process-local conversation gates, cancellation epochs, and worker locks
with attached components. Controls are recreated on reopen; they do not fence
noncooperating writers or processes. A task driver started through the harness
atomically marks its foreground owned graph, cancels active conversation workers,
and drives cleanup using the harness registrations. Pending cleanup is discovered
again when the driver is restarted. Shutdown can interrupt cleanup; blocked effects
still require reconciliation. A driver started directly from a standalone task
scheduler has no conversation runtime and continues to reject foreground aborts.

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

New transcript batches expose immutable per-occurrence message IDs and their
originating atomic observation commit through positioned views. New append batches
also record when fork-relevant state was unchanged across the commit. For such a
batch, `fork_at_position` and `fork_by_entry_id` can fork an interior message
using the captured state and its exact message prefix, provided the prefix has a
complete tool-call/result context. A newly recorded tool-free append may also change only model configuration in
the same commit; an interior fork uses that commit’s final configuration. Other
fork-state changes, configuration-changing batches containing any tool call or
result, legacy batches without the required proof, and interior replacement
batches fail closed. The document revision must also be unchanged across the
commit, even if an unrelated scope caused the change.
Proven compaction replacements retain each kept message's occurrence ID and
original commit, origin and boundary; their new summary has a new ID. Other
replacement batches assign fresh IDs, and an interior fork currently materializes
a new transcript with fresh IDs. Legacy messages without provenance stay
unidentified. Loading a compacted context and authorizing a fork verify the
predecessor projection, original observation links, matching runtime-owned fork
snapshots and ID uniqueness; these checks scan the retained transcript history
and are unbounded. Positioned page
reads remain bounded and expose structurally checked metadata; the fork APIs
perform the full authorization. Annotated occurrences in a shared immutable
chain can be selected by ID using their origin’s historical cut. See the parity
document for remaining limitations. The new replacement record is not readable
by older pi-rs versions; use a compatible version when reopening a store after
such a compaction.
Observer streams can be appended through the public observer API, so an
observation alone is not proof of a runtime commit. Compacted contexts whose
annotated origins lack a matching runtime fork snapshot fail closed. Runtime
fork snapshot keys must remain reserved to the runtime.
Collision checks cover the predecessor chain; the store has no global index
of occurrence IDs across unrelated conversation branches.

The runtime can also fork a settled conversation at its current state or a
supported historical snapshot cursor, and performs explicit idle compaction. It also
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

`RuntimeOwner::open` installs extension and task registries before recovering
conversations, and owns both the scheduler and driver. Await `shutdown` to join
conversation and task workers and release the store. Handlers must eventually
return. The `open_with_provider_adapter`, `open_with_subagent_management`,
`open_with_restricted_subagent_management`, and
`open_with_provider_and_subagent_management` variants install those services
before recovery as well. They use the same parent validation and child tool
policies as the corresponding `Scheduler` constructors. Provider selection
remains opt-in per conversation, and hosts must supply the adapter and task
registries again on reopen.

For a finite catch-up on the standalone `observer::Observer`, capture `head()`
and page with `read_through(processed, head, limit)`, persisting both cursors
after processing. The boundary remains fixed across later appends and reopening;
when `has_more` is false, the existing `watch` can resume at `next` without a
gap. Cursors are scoped to the same stream and store, and the observer namespace
must remain append-only. Each record is returned whole, but record bytes and log
retention are unbounded. This is a fixed range of explicitly logged records, not
an atomic view attachment or a replacement for upstream's watch frames; progress
and commits omitted from the observer log remain absent. The higher-level
`history` and `watch_history` APIs are unchanged.

`Observer::watch_for` bounds a polling wait and returns either an ordered page
or `TimedOut` with the unchanged cursor. A zero wait still reads once; errors
are returned even at the deadline. After processing a page, persist its `next`
cursor before resuming, including after reopening. A timeout means only that the
final poll found no record: it is not a stream or harness end reason. A record
found by that poll wins even if scheduling delays it past the deadline.
Dropping the future cancels the wait. Synchronous store reads can block past the
deadline; polling works across processes but does not wake on a close or failure.

`Harness::view` and `Scheduler::view` capture conversation metadata, the
committed running model/compaction progress checkpoint, the current tool attempt’s
committed replacement partial, and a newest-first
message page. Pass 50 for a window comparable to upstream's initial window.
`view_page` follows `older` through the same immutable snapshot even after
later commits or resets. Each call visits at most the requested number of
transcript records and returns at most that many messages; an empty page can
still have an older cursor. Submission, abort and other persisted conversation
fields remain visible. Legacy conversations without a transcript must first be
saved by the runtime. Cursors are in-process handles, not a wire format.

Tool progress is display-only: it is not a tool result or evidence that an effect
is safe to replay. Each invocation uses a fresh attempt identity. Completion, abort and interruption retain the last committed partial in the
sealed attempt snapshot and event. Older completed snapshots without a partial
remain readable. Read completed partials with `Snapshot::retained_partial()` or
the terminal event’s `retained_partial` field; aborted partials are in the state.
Events set `partial_is_null` when the last partial is JSON null, distinguishing
it from the absence of a partial. Retention does not turn progress into
authoritative output.
Callbacks are fenced by the conversation phase and attempt identity, and progress
is sealed atomically with the corresponding transition. Legacy running attempts
without an identity do not report durable progress until replay creates one.

For a complete, already available UTF-8 tool result, `output::bound_output` is
an opt-in helper matching upstream's `boundOutput` slice rules. Set independent
byte and line limits and choose `Retain::Head` or `Retain::Tail`; the result
borrows the retained slice and reports retained bytes plus dropped bytes and
lines. It prefers complete lines, preserves trailing newlines, and cuts a
single oversized line at a UTF-8 character boundary. For example:

```rust
use pi_rs_core::Outcome;
use pi_rs_durable::output::{bound_output, OutputLimits, Retain};

let result = bound_output("old\nnew\n", OutputLimits {
    max_bytes: 1024, max_lines: 1, retain: Retain::Tail,
});
assert_eq!((result.text, result.dropped_bytes, result.dropped_lines), ("new\n", 4, 1));
let reply = Outcome::Succeeded(result.text.to_owned());
```

Tools must call the helper explicitly and choose how to expose dropped counts;
the runtime does not append a truncation diagnostic automatically. It processes
materialized valid UTF-8 text and does not sanitize terminal escapes. It does
not change the streaming adapter's strict cap below.

`output::OutputBuffer` separately supports incremental UTF-8 text and byte
chunks with head/tail retention, split-character decoding, display sanitization,
and counts of decoded bytes and lines dropped before sanitization. `end()`
flushes an incomplete final byte sequence as U+FFFD. Memory is bounded by the
retained window plus the largest chunk; take snapshots to compact tail storage.
It does not support upstream's producer-supplied skipped-output counts.

For opt-in durable streaming, wrap a `BoundedStreamedTool` in
`KeyedToolAdapter::new(BoundedStreamedToolAdapter { tool, limits })`. A bounded
stream's byte limit must be at most 64 KiB. `append` and `append_bytes` commit
the sanitized retained window before returning; `snapshot` exposes dropped
counts while the invocation runs. Returning `StreamedReply::Success` with
`output: None` uses the retained window. Explicit replies keep their ordinary
meaning. The persisted stream contains only retained text: dropped counts and
incremental decoder state are not recovered after interruption, and no automatic
truncation diagnostic is emitted. An incomplete byte sequence is not persisted
until completed or flushed at normal settlement. The separate strict adapter
below retains its existing overflow behavior.

For model-issued calls, `KeyedToolAdapter::new(StreamedToolAdapter(tool))`
provides a separate, opt-in `StreamedTool` contract. Register the adapter as
usual with an explicit replay policy. `StreamOutput::append` appends UTF-8 text
and commits the accumulated replacement to a separate attempt record before it
returns. `StreamedReply::Success { output: None, state }` uses the accumulated
text; `Some(text)` is explicit, including `Some("")`. `StreamedReply::Explicit`
returns an ordinary `ToolReply` and does not use the stream. Existing tool replies
and `report_progress` retain their behavior.

Each attempt starts with an empty stream and is limited to 64 KiB of UTF-8 text.
An append that would exceed this limit, or a recording failure, makes the
attempt outcome unknown even if the tool ignores the error; reconciliation may
be required. Append calls are serialized. Retained handles reject calls after
settlement or cancellation; database writes are additionally fenced by the
conversation version and attempt identity. A safe replay gets a fresh record,
not the previous attempt's text. `streamed_output::load` reads committed text
by attempt UUID after reopen, but the record is not a terminal result and records
are not automatically deleted. The final result and extension state use the
existing atomic conversation transition. When recovery produces an interruption
or restart-limit error result, it includes the last committed text from the
same attempt before the error message. The explicit reconciliation policy still
blocks for host resolution and does not publish an error result. Nested calls cannot use this
adapter because they do not have this runtime's model-issued attempt fence.
Legacy running calls without an attempt identity also fail closed. There is no
line limit, tail retention, truncation diagnostic, streamed details fallback, or
upstream park/wake integration; this is not the upstream output API.

`Harness::tool_progress_events` and `Scheduler::tool_progress_events` poll a
separate, per-attempt stream of committed replacement states and its terminal
seal. Discover attempt UUIDs with `Harness::tool_attempts` or `Scheduler::tool_attempts`:
poll the conversation-scoped stream from `Cursor(0)` in bounded pages, persist
`next` after processing, and filter records by `attempt.operation_id` when tracking
one operation. Each claim is recorded atomically before dispatch, including retries
that never emit progress. Then start each attempt stream at `Cursor(0)`,
and persist `next` after processing each bounded page. Every record includes
`conversation_id`, `attempt` (operation and attempt IDs), and `state`. Reopen and
resume with the same attempt UUID and cursor; replay creates a different stream.
The cursor is exclusive and delivery is at least once if processing precedes
cursor persistence. Progress and its event, including abort/completion, commit
atomically under the existing attempt fence. Attempts claimed before discovery was introduced are not backfilled, even if their
progress events exist. Legacy running attempts without IDs remain undiscoverable;
a subsequent replay is recorded. Discovery and progress cursors are independent;
process every discovered attempt and retain its progress cursor. Discovery orders
claims, while progress ordering is only per attempt. Both logs grow without retention;
progress records store whole replacement partials (running and aborted snapshots up to 1 MiB). Completed snapshots that would exceed 1 MiB store the partial in an atomically written sidecar; versions before this change can read the completed state but cannot see sidecar partials. Versions before null markers also cannot distinguish JSON null from an absent partial. Pages bound record
count, not total bytes; polling is synchronous and has no notification or automatic
operation filter. This is not upstream's Chord operation stream.

This pull API does not provide upstream's watch operations, `from`, automatic
min/max window maintenance, or transcript entries for reset and compaction
markers. Progress updates alone do not appear in conversation observation history;
poll the view or the per-attempt stream for them. Message count and record reads are bounded, but a stored batch
(especially a replacement), message, and conversation metadata can each be
arbitrarily large and are decoded in full. Bytes, metadata copying and peak
memory are therefore not independently bounded. Existing history and execution
paths still hydrate all messages. Store reads are synchronous and may block.

`Harness::watch_view` and `Scheduler::watch_view` attach a pull watch with an
initial `ConversationView`; `next().await` polls for the next changed bounded
snapshot, including committed generation and tool progress. The interval must be nonzero.
Cancel a wait by dropping its future without ending the watch. `next_with_cancel`
ends it when its cancellation token fires; `abort` and `close` also end it and
release store ownership. `end_reason` records the first stopped, aborted, or
source-failure reason. A watch retains the store owner even after the originating handle is
dropped or the scheduler shuts down, so close it before reopening the database.

This is a latest-state watch, not an audit stream: it has no background queue,
and any number of commits between reads (including progress changes that later
revert) can be coalesced or missed. Each returned value replaces the previous
one. Unlike upstream, it has no Chord operations, callbacks, automatic window
adjustment, 100-frame queue, or automatic termination on harness close. It observes persisted state only, and synchronous reads can block the
executor. The view's count and record-visit bounds still apply, but metadata,
individual batches, and the retained initial snapshot are not byte-bounded.
For an append-only cursor stream use `watch_history` instead; it covers only
records explicitly included in that observation log.

`merged_progress_from_now` captures a consistent store boundary and returns a
versioned cursor for conversation observations, forward-only progress, and
conversation-scoped application documents.
`merged_progress` pages complete transactions in store commit order, and
`watch_merged_progress` polls until a page is available; dropping its future
cancels that wait without consuming the cursor. Boundary capture retries a
bounded number of times if commits race with attachment. A frame contains the
validated observation, progress record, document changes, or any combination
written in the same transaction; paging never splits a frame. Persist `next` only after processing the page.
Attachment explicitly starts after the captured boundary: older overwritten
progress and document-to-commit links are never reconstructed. Version 2 cursors
include documents starting at attachment; existing version 1 cursors retain their
observation-and-progress-only behavior. Document scope is the UUID string by
host convention, not an access-control boundary. Each document change is its
final state or `None` for deletion, with no original mutation order. Missing
revision-to-commit evidence, including writes from an older store binary after
the tracking boundary, fails closed. Documents in one frame are capped at 256
and 8 MiB of encoded keys and values; oversized commits fail without advancing
the cursor. Cursors are scoped to the same database and conversation; malformed,
untracked and skipped source records fail closed. This is a pull stream of
root-replacement observations, progress and document states, not upstream's
windowed Chord diffs. Standalone progress writers are not included. Polling is
synchronous, retained history and observation bytes are not globally bounded,
and pages contain at most 256 frames.

`watch_merged_progress_handle` adds a lifecycle-aware pull handle over the same
v1/v2 cursors and bounded pages. Attach from now or resume a consumed cursor;
`next` advances only when returning a complete page, and dropping a pending wait
leaves the cursor unchanged. `close`, `abort` and token cancellation preserve the
first stopped or aborted reason; a read error ends the handle with a failure
reason. A terminal progress record does not itself close the watch. When the last
source-capable harness or worker releases its lease, the watch captures a store
commit boundary, drains pages through it and ends with `Ended`, releasing its
reader. `source_closed().await` reports the captured boundary without consuming
pages; the drained watch end remains distinct. Watch-only clones do not keep the source alive. Dropping a scheduler
does not stop its spawned workers, so closure waits for them; awaited shutdown
joins them. Active progress callbacks retain the source only while they write,
and stale callbacks cannot write after it closes. A task driver holding a
source-capable harness also keeps it live. Independent raw store writers are not
source leases: commits after the close boundary require reattachment. The handle
retains the store owner until it ends, is closed or is dropped. It does not change
the forward-only coverage boundary or implement an upstream Chord queue.

`MergedProgressWatch::into_listener` transfers a pull handle into an explicit
serialized callback driver. `run` calls the listener with one bounded page at a
time, advancing its cursor only when the callback returns successfully. A
listener error or panic leaves that page unacknowledged for replay. Independent
stop, abort and host-attested source-close tokens produce stopped, aborted and
ended results. Automatic source closure drains through its captured boundary
before returning ended; the host-attested close signal ends without that drain.
A signal is checked before each callback, while an already running callback is awaited
before the result is returned; listeners must therefore eventually return. If
signals are simultaneously ready, stop takes precedence over abort, then source
close. Dropping `run` can interrupt a callback and replays its unacknowledged page after reattachment,
so effects need idempotency. This pull-driven API has no background callback
queue, upstream listener context or recoverable error frames; total page bytes remain subject to the merged stream's existing limits.

`merged_progress_operations` and `MergedProgressWatch::next_operations` are
opt-in operation variants over the same cursor and atomic frames. They
compare validated observation predecessors and tracked document predecessors,
emitting top-level Chord `s`/`d` operations and append-only array `p` operations
when smaller than a root replacement. Missing or unproven predecessors, unsafe
Chord path segments, excessive changes and non-beneficial patches fall back to
`["r", value]`. Document deletions are retirement frames with no operations;
creation and recreation use root replacements. The incremental frame supplies
its verified predecessor where available so a consumer can apply the operations
only to that state. Progress operations apply to the serialized entire progress
change, including its request or attempt identity. A running predecessor must
be uniquely identified in the immediately preceding tracked progress record;
otherwise the change is a replacement. Nested paths and operation counts are
bounded, and old overwritten progress is never reconstructed. Observation and
progress operations are derived state transitions, not original upstream batches.

For existing documents, `Transaction::apply_operations` provides an opt-in
forward-only boundary **per write**: it applies and atomically persists the exact
caller batch alongside the version-checked update. Incremental merged watch
returns it with `MergedDocumentOperations::actual == true` only after verifying
its commit and result against the predecessor. Existing writes, creations,
deletions, missing or invalid evidence use the prior derived/replacement or
retirement behavior and report `actual == false`; no old batches are backfilled.
This bounded subset accepts decoded `r`, `s`, `d`, `a`, `t`, `p`, and `m`
operations over nested object and array paths, with at most 128 operations,
eight path segments, 1 MiB of batch JSON, and a 2 MiB intermediate value.
String truncation uses upstream's UTF-16 code-unit offsets; cuts that would
produce an unpaired surrogate are rejected. Sparse array writes, unsafe path
segments, numbers outside JavaScript's safe integer range, and wire-compressed
operations are rejected. Operation evidence is retained
as immutable store data; it is not garbage collected. Observation and record
sizes remain subject to the existing merged-watch limits. Conversion errors do
not advance the watch cursor.

`progress_history_from_now` captures a versioned, conversation-scoped exclusive
cursor. `progress_history` and `watch_progress_history` page committed generation
and tool progress records after that boundary. Runtime checkpoints and terminal
seals append their record atomically with the existing progress write, including
when a conversation transition shares the transaction. Each record is capped at
2 MiB plus 8 KiB and pages contain at most 256 records; backpressure is pull-based
and callers persist `next` only after processing the entire page. Cursors are
scoped to the same database and conversation. The boundary explicitly excludes
previously overwritten progress: history is not backfilled on upgrade. Only
runtime-owned progress writes participate; standalone progress-store writers,
streamed output and uncommitted provider deltas are not covered. Records are
whole replacement states, not upstream Chord diffs. Each frame includes its
tracked database-local commit sequence; missing commit metadata fails closed. Retention and total page
bytes are unbounded apart from per-record and page-count limits, and the reserved
`durable/progress-history/` namespace must remain append-only.

`Harness::watch_committed_view` and `Scheduler::watch_committed_view` add a
separate pull stream of runtime conversation observations. Attachment captures
the latest observation and returns it as `initial`; pass a saved
`CommittedViewCursor` to resume from that exact observation after reopen. Each
`next` returns the next observation without coalescing, with a single Chord root
replacement `["r", value]`. Stop, abort, cancellation and source failure have the
same first-wins end reasons as the latest-state watch. Each observation must have
a matching runtime fork snapshot in the same tracked store commit; untracked
legacy history, foreign observer appends and inconsistent snapshots fail closed.
Cursors are scoped to the same store and conversation, and the observer and
fork namespaces must remain immutable. The value is the committed observation
(messages, block bindings, submissions, reset and write markers), not the full
`ConversationView`: separate progress-only updates, application document commits
and other unlogged state are absent. The frames synthesize full replacements,
not upstream's fine-grained operations or windowed view. There is no background
queue or automatic termination on harness close. Individual frames and retained
history are not byte-bounded.

Nested tool calls are journaled by parent operation and call order. The standalone
`NestedHost::call_keyed` API also accepts explicit caller-scoped keys and reuses
completed results across reordered replay, rejecting a changed tool or arguments.
Tools can opt into keyed calls by implementing `keyed_tool::KeyedTool` and
calling `Arc::new(KeyedToolAdapter::new(tool)).register(replay_policy)`. Its `KeyedHost`
provides `call_keyed` and `ordinary()` for positional calls and progress. The
adapter fails closed outside durable runtime dispatch; existing core tools remain
unchanged. Explicit keys are scoped to the parent invocation. The host must be
used sequentially; calls through a retained child host after its invocation returns
are rejected and poison the root for reconciliation. A child that leaves nested
work running also makes the root uncertain.
Unsafe interrupted children block the parent for reconciliation even if ignored;
so do omitted recorded calls in a replayed scope, unlike upstream. Calls are
serialized, bounded in depth, and cannot update extension state. A safe replay
still requires destination-level idempotency for external effects.

Within a task-backed tool handler, `TaskTool::stage_child` stages another
registered TaskTool under a stable key. Return a task wait step to commit the
child and checkpoint together; on wake, `ended_children` and `get_owned_child`
expose the frozen wake IDs and checked results. The driver verifies the child
handler, version and replay policy before admission. This handler-only API does
not add asynchronous child dispatch to `KeyedHost`. Repeating a key within the
same handler invocation returns the same child ID when its definition, input and
handler match; conflicting reuse fails without replacing the staged child.

`KeyedTool::invoke_controlled` can return termination and handoff controls with its reply.
The runtime persists it with the outcome and ends the run without another model
request only when every result in the nonempty tool round requests termination.
The last defined handoff in model call order (including empty text) appends a reset after after-tools tasks and before queued boundary items. It starts a new model context while preserving history. Controls are persisted with tool outcomes and survive reopen; nested calls do not control their caller’s round.

**Nested pending identity.** An unsafe park requires a durable host-supplied
identity for every child. For identity-bound jobs under any parent, polling
requires the same identity on the exact registered child `Arc<dyn Tool>`.
Before starting or reopening the scheduler, call
`nested_identity::register(&registered.tool, Some(identity))` on each backend
to bind new jobs. The non-secret identity is persisted
in plaintext and must distinguish the implementation, backend configuration and
tenant; restore the same identity for the same backend. A duplicate registration
for a live allocation is rejected. Keep the returned `IdentityHandle`, and make
all configuration, tenant and unsafe-parent version changes inside its
`update(new_identity, || { /* change configuration */ })` callback. Updates
return a busy error rather than wait while an invocation or poll holds a lease;
retry after it finishes. The identity is cleared before the callback, so a
panic leaves the tool unbound. Never mutate backend configuration outside this
handle or change it during a call.

For compatibility, a replay-safe parent's unbound legacy job continues to poll:
the host must restore the original backend and tenant because such a record
cannot prove either. If that tool has an identity handle, polling holds its
read lease even for an unbound legacy record; an unset registered identity
fails closed, including after an update panic. New nested dispatch also refuses
an unset registered identity. A bound job with missing or changed registration never
polls. An unsafe park cannot poll or resume with any unbound child. Operators can
inspect `nested_pending_jobs`, verify the remote reference and tenant through an
independent authoritative channel, then explicitly call
`bind_nested_pending_identity(conversation, root, operation, reference,
identity)` with the current registered identity. The checked bind only fills a
missing identity; it refuses to replace a different existing binding, does not
contact the backend, and schedules a recheck. Alternatively, use
`reconcile_nested_pending` or `cancel_nested_pending` after independently
confirming a terminal outcome. The runtime cannot verify the operator's claim.

A root `KeyedTool` can use
`call_keyed_unpolled_with_id` to retrieve a nested journal operation ID and
`suspend_pending_with_checkpoint` to persist up to 64 KiB of JSON alongside
an explicit suspension. On replay `pending_wake()` returns that checkpoint
and the IDs of the pending operations captured at suspension. It is present
only on the resumed root, including a replay after process death; a completed
parent clears it. The call still waits for the child's initial invocation, and
the parent resumes only after **all** captured pending jobs settle. IDs are
journal operation IDs, not task handles. Checkpointed suspension rejects
before-tool gates; use the existing gate suspension API for those. A crash
before the suspension commit cannot recover the proposed checkpoint. The
parent must return normally without state or controls. A replay-safe parent must
make its own effects replay-safe. A parent registered unsafe must explicitly opt
in with a positive `unsafe_park_version`, branch on `pending_wake()` before
repeating pre-park effects, and leave no detached effects running after return.
The host must supply `unsafe_park_identity()` matching the identity registered
for the parent, bump the version when implementation or checkpoint meaning
changes, and restore the same parent and child identities, spec, version and
unsafe policy after reopen. Unsafe parks require identities for every recorded
nested child; legacy unbound children cannot automatically resume them.
Experimental older unsafe park proofs without a parent identity cannot be
recovered by binding children: authoritative parent reconciliation is required.
Before claiming a resumed unsafe attempt, the runtime leases the parent and all
child identities through handler execution and the checked result commit.
An identity mismatch at claim blocks for reconciliation. A temporarily busy
identity releases the conversation worker lock and retries on a bounded timer;
abort and shutdown can proceed while it waits. For a registered unsafe tool,
the lease is acquired and the host identity, specification, version and policy
are persisted before argument preparation and before-tool hooks. The same tool
allocation remains leased through dispatch and the checked result commit. A
previous experimental busy-attempt marker without a preparation identity cannot
prove which definition prepared its arguments and requires reconciliation.
On resume, the parent and all child allocations are pinned from claim through
the checked result commit, even if the extension registry is replaced.
Older executors do not enforce these new preparation proofs and must not resume
records written by this implementation.
The final resume decision after the last child settles also distinguishes a
temporarily busy identity from a mismatch, and retries without redispatching the
settled child.
The standalone `Harness` does not run a background timer; its caller must
drive it again after an identity becomes available.
The committed checkpoint and original attempt identity fence the park; a new
attempt is claimed before resumption. Process death after that claim is an
interrupted unsafe invocation and requires reconciliation (or follows the
configured interruption policy), even if an earlier wake exists. A crash
before the claim does not start an invocation or consume the restart limit.
The runtime cannot make arbitrary external effects idempotent; the declared
wake branch and any destination-side idempotency remain the host's responsibility.
Conversation abort fences polling and replay
but does not cancel an accepted external job, which remains available for
explicit reconciliation. This is not upstream's `wait: false`, `ToolPark`,
`any` wake, or independently executing child task. Unlike upstream's distinct
`ToolPark` return type, this API requests suspension before returning a normal
reply. For an unsafe parent, if that reply is `Failed` or `Skipped`, the suspension is rejected and
the failure is included in the blocked reason; the still-pending child must be
reconciled before parent settlement. A replay-safe parent retains its earlier
behavior and may suspend after such a reply.

Pending nested jobs are resolved by tool name and replay policy; their stored
references are not bound to an implementation or tenant identity. On reopen,
the host must restore the same nested tool configuration and tenant before
polling, or reconcile the job explicitly. Reusing a name and opaque reference
with a different backend can otherwise associate the wrong result with a job.

Selected extensions may provide tools and before/after-tool hooks. Nested calls with a resolved, valid tool run before-tool hooks before dispatch; pass, rewrites and blocks are durably recorded, rewritten arguments are revalidated, and rejected calls pass through after-tool hooks. A nested before-tool sleep returns a pending gate; a replay-safe keyed parent must explicitly call `suspend_pending` and return normally, and a Scheduler resumes it when the deadline or named conversation signal fires. On wake the child is resolved and its original arguments are prepared and validated again before the before-tool chain runs, with `Context.snapshot["nested_before_tool_wake"]` containing the wake reason and, for a signal, its name, ID and payload. Invalid gates settle through after-tool with an `invalid_park` diagnostic. Gate and signal consumption survive reopening, and abort fences the wake. The parent may run again and must make its own effects replay-safe; the existing restart limit also bounds these resumes. A parent that does not explicitly suspend fails closed during normal execution. On reopen, a committed pre-dispatch gate can recover an interrupted replay-safe parent if every other child is terminal, has an accepted pending reference, has a staged result, or has a committed gate, and the parent registration still satisfies the existing identity and replay-policy checks; the host must restore the same tool and extension implementations. A crash before gate persistence, an unsafe parent, or another uncertain child still requires reconciliation. Harness alone does not drive parked gates. Gate waits are not listed by the external-job `nested_pending_jobs` API. The explicit parent suspension waits for all admitted children: gate wake receipts can commit while another child poll is running, but gate hooks and child execution resume when the parent replays after all pending work settles, in the parent’s call order. This differs from upstream independently running nested tasks; abort fences local polling and replay but does not guarantee cancellation of an already accepted external job. Preparation failures and unavailable nested tools still require reconciliation rather than entering the result-hook path. Interrupted hooks may replay and must make effects idempotent.
The runtime
commits terminal tool output and state before invoking after-tool hooks; a crash
replays the hook with the same operation memo, not the completed tool. Hook
callbacks can run more than once and must memoize or make external effects
idempotent. For an explicitly suspended replay-safe keyed parent, pollable
children commit their result and finish selected nested result hooks before the
parent wakes, including after reopening. The child hook uses its stored snapshot
and memo; interrupted hooks may repeat. Extension tools are conservatively unsafe
to replay, and a selected extension overriding the parent prevents this replay.
The host must retain the same extension and tool semantics across recovery.
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
Scheduler polls waiting phases. Ordinary hook and section errors are reported and skipped; before-tool errors block dispatch. An after-tools child that succeeds with exactly `{ "terminate": true }` settles the current inputs with the tool-calling answer and runs after-run hooks before queued inputs. onYield may create tasks and chooses the first successful supported continuation in creation order, including empty strings and text/image arrays supported by core or a selected durable-aware provider; extensions must bound continuation loops; queued input or reset takes precedence. Failed hook children are reported when observed; invalid after-tools decisions are reported, while unsupported on-yield outcomes are ignored. Unregistered or foreign task requests are reported and skipped. Provider-body hooks require an explicitly configured adapter; tool-result termination and handoff are supported by opt-in `KeyedTool` implementations. Before-tool hooks may sleep on
an absolute deadline or named conversation signals; `send_signal` deduplicates
receipts, and wake consumption commits atomically with the phase transition.
Invalid sleep requests (no deadline or signal, a negative deadline, an empty name, or a consumed or expired signal key) finish the call without dispatch, with an `invalid_park` error diagnostic. The result passes through after-tool hooks before publication; a failed store read remains a runtime error. The hook sees the wake in `Context.snapshot["phase"]["ToolPreparing"]["wake"]`.
The scheduler rescans once per second and reopening resumes the wait. Signal
names are conversation-scoped single-value keys: a wake consumes its key, and a deadline closes empty keys. A repeated receipt ID is idempotent; a different ID for an occupied key is rejected. The `set_signal_clock` hook supports deterministic deadline tests. Unlike upstream typed signal/key pairs, this API uses a single string name.

`TaskStore::commit_owned_tasks` and `prepare_owned_tasks` also expose the
lower-level atomic checkpoint and child creation primitive to other hosts.
Conversation abort atomically marks owned task descendants and fences new
admissions. Cleanup runs child-first after restart when a task driver or
scheduler is attached. Synchronous handlers cannot be interrupted and cleanup
can replay after a crash; external effects require idempotency and reconciliation.
Conversation abort skips tasks explicitly marked as background and their
descendants. Task-owned foreground subagent conversations are marked and
scheduled for cleanup by the conversation scheduler; background edges are boundaries.
Task cleanup waits until each foreground child conversation has settled its abort,
including that conversation's foreground tasks. `Scheduler::abort_and_wait` joins
workers for the entire affected foreground scope. Standalone task-driver aborts
still reject foreground conversation children because they cannot drive or cancel
their workers. A blocked uncertain operation requires reconciliation, and a task
driver must be running to drain tasks.

As a pi-rs extension, `abort_and_wait_withdrawn` returns the queued inputs withdrawn by its abort
commit, including each conversation, original mode and submission. The
`abort_and_wait_with_background_withdrawn` variant crosses background boundaries.
Queued writes are not withdrawn and may be placed during cleanup; repeated aborts
do not return inputs already withdrawn.

Task-to-conversation edge preparation and owned-subagent admission reject owner
cycles by checking the persisted owner chain, including background links. Versions
and absent root edges are checked in the transaction, so concurrent changes require
fresh preparation. Task creation also checks staged tasks and persisted ownership,
including missing task owners, to prevent cycles that would block child-first
cleanup. Missing owners remain supported but are fenced against concurrent creation.
Prepared task batches and edges must not be combined with separately prepared
ownership changes in one transaction. Prepared edges must be committed one at a time without other
ownership changes; raw store writes must preserve immutable ownership. Existing
corrupt cycles are rejected on traversal, not repaired. In particular, `Store::commit`
evaluates every check against the state before any mutation: concatenating two
independently prepared batches can therefore commit a cycle even when both missing-owner
checks pass. The raw store cannot identify preparation boundaries from flattened
checks and mutations. `document::Transaction::append` rejects cross-component
missing-owner check/write overlaps, but raw commits do not apply that protection;
this conservative document check can also reject an acyclic composition. Prepare
related task creations together with `prepare_save`, which validates their staged
graph, or commit separately and re-prepare after conflicts. Background remains an
abort/idle boundary, not a break in ownership identity.
Current limits and differences from upstream Pi Durable:

- Historical forks accept safe committed observation snapshots, including during runs,
  but not every upstream entry or incomplete tool batch.
  Automatic compaction uses a known context window or provider overflow; manual
  compaction still requires an idle conversation. Streaming checkpoints retain
  text for aborted display but not all upstream live view/metadata semantics.
  The subagent link primitive does not provide an automatic conversation/task ownership tree.
- Generic task waits support deadlines, signals, deadline-or-signal sleeps,
  all-settled and any joins over existing tasks, and direct-child fail-fast joins.
  Empty any waits wake on the next scheduling pass. Wait admission checks
  durable wait and foreground ownership cycles and fences concurrent graph puts.
  `register_with_abort` and `request_abort` provide
  persisted, child-first cleanup for generic task-owned subtrees. Cleanup must
  tolerate replay after a crash; a missing handler produces a failed record.
  Handler steps run synchronously, so an abort request waits for the active step
  and cannot interrupt it. Fail-fast persists abort intent for each live sibling
  and drains its owned work and abort handler before waking the waiting parent.
  Unlike upstream, siblings are marked in separate scheduling passes. On reopen,
  interrupted invocations under those graphs wait until the marks are persisted
  before recovery. If an owned child is admitted after a successful step was
  prepared, a graph-fenced retry holds that result as `Completing` without
  rerunning the handler. Same-value reservation rewrites are rebased while
  document, signal and deadline checks remain in force. Legacy `cancel` bypasses cleanup. Conversation abort marks foreground conversation-owned task descendants;
  foreground task-owned subagent conversations are included. Shutdown waits for an in-flight step.
- Standalone task signals also support `SignalToken::new(name, version)`,
  `Signal::new_typed`, and `WaitFor::TypedSignal(token.key(key)?)`. The stored
  signal carries its positive version; waits match kind and key regardless of
  version. Typed and legacy named waits do not consume each other's signals.
  A task/key holds one version and JSON value: identical retries, including with
  another receipt ID, succeed without creating another receipt; conflicting
  values or versions are rejected. Consumed keys reject later waits. Legacy
  records and task-scoped UUID idempotency remain supported.
  `SignalToken::with_schema` accepts a JSON Schema, validates newly constructed
  typed signals, and checks values returned by `TaskStore::read_typed_signal`.
  An optional `with_migration` callback converts any supported older version;
  missing, failed, panicking, invalid, and newer-version reads return errors.
  Reads do not rewrite receipts, so migrations must be deterministic and may run
  again after reopening. Legacy `SignalToken::new` accepts any JSON, and raw
  receipt reads and wake results bypass validation; hosts must use the typed
  read to enforce it. Token definitions and migration callbacks are supplied by
  the host after reopening. Duplicate historical typed keys fail typed reads.
  `TaskStore::deliver_signal` and `Scheduler::deliver_signal` return upstream-shaped
  delivered, duplicate, invalid, closed, or not-found outcomes. The older
  `WaitFor::TypedSleep` persists ordered typed keys and an optional deadline.
  A wake consumes the first available key; a deadline expires empty keys atomically
  with the wake. `read_typed_signal_state` distinguishes expired keys from values,
  and `Task::typed_wake` persists signal, expired, or time wake reasons through
  recovery until checkpoint progress, another wait, or termination. A delivery
  to an expired key returns closed. `deliver_signal` uses wall time; deterministic
  hosts should use `deliver_signal_at` with the same clock as scheduler ticks.
- Opt-in task handlers using `TaskDocuments::signal_at` can stage multiple distinct typed
  signals to existing or missing tasks, including the running task and distinct
  keys on one task, before or after staging document and application-entry writes.
  Upstream `Tx` rejects a sequential signal read after an entry table write;
  allowing this order is a pi-rs extension.
  Its status is provisional: the target task version and signal revision are
  checked with the handler's checkpoint and documents, and a conflict rolls
  back all of them. Replay-safe handlers are retried; other handlers require
  reconciliation. The supplied timestamp must use the scheduler's clock. For
  an empty key with a future deadline, monotonic elapsed time since staging is
  checked under the storage write lock; crossing the deadline conflicts and
  replay must sample a fresh timestamp. Clock jumps after staging are not observed.
  Repeating a key after a delivered or duplicate result validates the new
  value and returns duplicate without changing the staged receipt. An invalid
  value may be retried. Repeated missing or closed targets retain their fenced
  outcome; for an expired key, each new value is validated before closed is
  returned. A self-delivery commits atomically with the handler's transition and
  can satisfy a typed wait returned by that same invocation, including after
  reopen. A committed value takes precedence when the wait is polled after its
  deadline. Abort or failure discards uncommitted effects. Signal checks can
  still conflict with other batch participants' checked keys (for example a
  newly created child). It is not a general host transaction or upstream `tx.signal`.
  A running task driver continues scheduling after a successful step; external
  effects inside handlers remain the caller's idempotency responsibility.
  The older `send_signal` retains receipt idempotency semantics and reports
  an expired typed key as an error. General upstream transactional delivery remains missing.
- `document::HostTransaction::new(&store)` offers a store-bound host transaction
  for checked document writes, typed signals and checked host batches. A signal
  result is provisional until commit; conflicts roll back all writes. It allows
  multiple keys and repeated calls per target; repeated keys validate the new
  value and preserve the first accepted receipt. Calls remain provisional until
  commit, and the earliest deadline and all target/revision fences are enforced.
  `wake_with_signal` (also available as `wake_with_staged_signal`) can consume
  a committed or newly staged delivery for a `TypedSignal` or `TypedSleep`
  wait. It selects the first available value in wait-key order, and atomically
  commits the receipt, wake reason, task and document writes. Consumption of an
  existing receipt also advances its signal-revision fence. An abort, competing
  delivery or deadline conflict rolls all back. Ambiguous duplicate records and
  consumed wait keys fail closed. `wake_at(task_id, now_millis)` also supports
  deadline wakes for typed sleeps: an available value wins, otherwise at or
  after the deadline it atomically expires missing keys (or records a time-only
  wake for an empty key list). A not-yet-due wait returns an error without
  staging changes. The supplied time must use the scheduler's clock; this API
  does not create a timer. Arbitrary task writes and raw signal-namespace writes
  remain excluded. After staging the
  wake, further signals to that target are rejected. Callers retry the entire transaction with a fresh
  clock after conflicts. Unlike upstream `Tx`, sequential signal calls remain
  allowed after a staged delivery; the explicit atomic wake is a pi-rs API, not
  a general upstream task-record replacement.
  Storage errors can have uncertain outcomes. Same-ID retries remain successful
  after task termination,
  while a new ID is rejected for a terminal task. Existing stores with
  multiple typed records for one key are not consolidated; conflicting records
  reject new deliveries, and any consumed record closes the key.
- Normal scheduler and driver typed waits apply the same signal-first deadline
  ordering and atomically fence task and signal revisions. Consuming a receipt
  advances the signal revision; mismatched record identities and ambiguous
  duplicate typed receipts fail closed instead of selecting one arbitrarily.
- `worker_activity()` observes live task handler attempts, independent summary
  workers, and scheduled conversation drivers (including lock queues and poll sleeps) without
  locking the task scheduler. Summary entries use their operation ID. It is
  process-local and excludes direct harness drives and persisted waiting work.
  `WorkerActivity::subscribe` supplies coalesced lifecycle snapshots; slow
  readers can skip transitions. It is not an `onWake` or durable-idleness API.
- `Scheduler::subscribe_changes` combines local store commits, worker
  transitions and task registry replacement into coalesced hints. A registry
  hint can carry unchanged values. Attach before inspecting state; the
  returned commit sequence and worker list are not an atomic wake snapshot.
  Writes through another SQLite connection do not generate these hints.
- `Driver::wake_snapshot(now)` reads task definitions and durable waits under
  the task gate without waiting for the handler-held scheduler mutex. It does
  not include conversation deadlines or provide `onWake` notifications.
- `RuntimeOwner::subscribe_wake(clock, callback, on_error)` attaches a
  coalescing combined conversation/task wake subscription. The clock returns
  Unix milliseconds; keep the returned handle alive and call `kick()` to
  recompute after a host timer fires or to retry a failed callback. Unknown
  conversations are reported as active. Snapshot failures are reported and
  yield a conservative active wake. Dropping the handle or shutting down the
  owner stops delivery, although an already claimed callback may still run.
  The host supplies its own timer; external SQLite writers do not notify the
  subscription. Callbacks must not block and the Tokio runtime must stay alive.
- Polling uses an in-process interval rather than a durable timer; reopening
  resumes it. Model retry deadlines use persisted bounded exponential backoff;
  scheduled conversations release their worker lock until the deadline.
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

Background-inclusive aborts advance a persisted generation for each affected
parent atomically with graph marks. Reporter intents are bound to that generation;
stale intents are retired on replay, and child and report admissions check it in
their storage commit. Normal aborts do not advance it. Reporter operation IDs are
unique per parent, including across named children and after an abort; use a new
ID for a new request. New reporter records use a separate key namespace from
older child-keyed and parent-keyed records, which remain readable on reopen.

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

`Harness::subagent_status` and `Scheduler::subagent_status` inspect a parent's
named children, optionally by name, without creating or scheduling them.
`format_status` produces the upstream example's `working`/`idle` text, with
`reserved` for a persisted reservation whose child was not created before an
interruption. Queued inputs count as working, even if a failed run has left them
waiting for another input. This is a committed snapshot, not a watch.

A host can call `Harness::select_tools` or `Scheduler::select_tools` on a
settled conversation to persist an exact allowlist of tool names. `None` restores
the host default; `Some(vec![])` disables every tool. Missing names remain in
the selection and take effect if later registered. The filter applies to model
specs, model-issued and nested dispatch, and interrupted-tool recovery. It
also applies to global and selected extension tools. Forks retain the captured
selection; independently created named children start unrestricted, while
history-seeded children inherit the parent snapshot unless explicitly restricted.

`Harness::select_model_tools` and `Scheduler::select_model_tools` optionally persist
an exact ordered model-facing selection on a settled conversation. `None` offers
enabled non-deferred tools by default; an empty list offers none. The enabled allowlist remains the
ceiling, and nested calls can still access enabled tools hidden from the model.
Model-issued calls to hidden tools are rejected. Unknown names remain stored;
forks retain the selection and old conversations default to `None`. Tool control
additions append to both lists when each is set. `select_tools_except` replaces
the enabled selection with a name-based remove filter; `select_model_tools_patch`
replaces the model selection with remove and add lists. `set_deferred_tools`
declares conversation-scoped deferred names: they remain available to nested
tools but are omitted from model requests unless explicitly selected or loaded
by the model add list. Non-deferred names in the add list do not change model
ordering. With a patch, normal tools retain enabled order and loaded deferred
tools follow in add order. Unknown names remain stored for later registration.
`add_tools` removes added names from both remove filters and appends them to
exact selections or the model add list at the tool-round boundary. A tool can
therefore widen execution and model selection; only trusted tools should return
this control. The filter and deferred declarations survive reopen and forks;
updates require a settled conversation. Unlike upstream's registration-level
deferred metadata, these declarations are set per conversation, and unset host
tools are ordered by name. Selection APIs reject empty or duplicate names.
Install `tool_search::extension()` and select `tool-search` on a conversation
to offer the model-only `tool_search` tool. It searches currently enabled,
model-callable deferred tools that are not already offered, including names
removed by the model filter, and loads matches at the tool-round boundary.
Ranking uses BM25 over names, descriptions and parameter metadata, including
local schema references. The default limit is eight; this implementation caps
it at 128, query size at 4 KiB, candidates at 256, and candidate metadata at
1 MiB. Exceeding a bound fails the search without loading tools. Registered
tools are searched in name order for ties. Install the same extension before
resuming an interrupted conversation. A committed search control survives
reopen; interruption before its outcome is committed follows the existing
extension-tool recovery and reconciliation policy.
The model-visible result lists loaded names; its `loaded` details are also
available in the conversation's durable tool-result metadata.
Each conversation exposes `tool_results`, ordered terminal-operation metadata
with operation ID, call ID, tool name, error flag, optional details or structured
output, attachments, and diagnostics. The outcome is checkpointed privately
before after-tool hooks; metadata is published with the finalized result,
included in history snapshots and inherited at a proven fork cut. A hook that
replaces output also withholds its associated metadata and records a redaction
diagnostic. Aborting before this phase finishes while extensions are selected
withholds the pending output and metadata. Resets and compaction retain finalized records. `SucceededWithDetails`
keeps details out of model messages. Failed or skipped results produce an error diagnostic, and truncated
output produces a warning. Durable keyed tools can call `KeyedHost::diagnostic`
while executing a top-level attempt; at most 32 diagnostics of at most 4 KiB
each are accepted. Result metadata above 64 KiB is replaced by a truncation
diagnostic, while the tool's model output is retained and the tool is not
re-executed. Truncated metadata cannot be schema-validated. Keyed diagnostics
are privately checkpointed while the attempt is active and carried through
pending, unknown, interruption and authoritative reconciliation. A keyed parent
can use `call_keyed_with_diagnostics` to retrieve a child's committed diagnostics
alongside its outcome; a reattached child keeps the same diagnostics. Diagnostics
remain private until incorporated into a finalized result, so legacy output
redaction can withhold them. Checkpoints and result records have no retention
policy. Older records and synthetic skips without operation identities are not
backfilled. Use `KeyedHost::diagnostic_keyed(event_id, diagnostic)` when the tool can provide a stable event identity across retries. An event ID is scoped to the tool operation, must contain 1–128 UTF-8 bytes, and must identify the same diagnostic content on every retry. Repeating that ID and content is idempotent; conflicting content is rejected. Different IDs preserve distinct events even with identical content. Calls to `diagnostic` have no identity and always append, including on replay. Existing checkpoints retain their diagnostics but cannot retroactively gain event identities. This differs from upstream, which clears interrupted non-wake progress before replay and retains prior diagnostics across wake. Reconciliation accepts the existing terminal outcome types.

`Harness::set_tool_output_schema` and `Scheduler::set_tool_output_schema` set
an optional JSON schema for a registered tool on a settled conversation. The
schema survives reopen and forks and permits local references only. On a
model-issued call, an invalid or missing structured value is discarded with an
`invalid_structured_output` diagnostic while the call's error status remains
unchanged. For a nested call it becomes a failed outcome. Errors need not
include a structured value. With a schema, structured data is not placed in
model messages; schema-less tools keep their existing core serialization.
These are conversation-scoped name-based schemas rather than upstream
registration metadata. Legacy after-tool text replacements conservatively
withhold metadata. An opt-in `Hook::after_tool_result` receives all result
channels and can explicitly replace them; its chain runs after the legacy text
hook chain, and the replacement is bounded and schema-validated before publication.
Nested calls and legacy checkpoints without result metadata do not run this hook chain.
Earlier history already published before this feature cannot be retroactively
redacted, and previously streamed output cannot be retracted.

`Harness::set_tool_callers` and `Scheduler::set_tool_callers` persist a
registered tool's caller override for a settled conversation: `Both`,
`ModelOnly`, `ToolsOnly`, or `Neither`; `None` restores the default of both.
Model-only tools are omitted from nested tool discovery and execution, while
tools-only tools are omitted from model requests and model-issued calls. Enabled
and model filters remain additional restrictions, and loading a tool cannot
override its caller permission. Names must refer to a currently registered host
or selected extension tool. Overrides survive reopen and forks, and are visible
in hook conversation snapshots. A denied nested call follows the existing
conservative nested failure path and can require parent reconciliation.
Unlike upstream registration-level caller metadata, these overrides are
conversation-scoped and apply by name, including to a replacement registered
under that name; hosts must maintain the policy when registrations change.

Use `ensure_subagent_restricted` or `submit_subagent_reported_restricted` to bind an
initial child allowlist to the durable reservation before submission. For task-owned
children, `ensure_task_owned_subagent_restricted` and
`submit_task_owned_subagent_restricted` commit the selection with the child and,
for submit, its initial input. The selection is part of the reservation identity:
a retry with a different selection fails, even after reopening. Existing child
selections may subsequently be changed using the settled-conversation API. This limits tool dispatch, not other effects of trusted tools,
hooks, or host APIs.

An embedding host can opt in to a parent-bound `subagent` extension with
`Scheduler::open_with_subagent_management(path, options, registry, parent, child_config)`.
Create the parent first with `Harness::open_paused`, then open the scheduler and
select `subagent_management::extension_name(parent)` on that settled parent,
preserving any other selected extensions. On reopen use the same constructor,
parent and child configuration: the extension must be installed before recovery.
By default, new managed children receive an allowlist of the global tool names
registered in `options` at scheduler open, excluding `subagent`. This mirrors the
upstream examples' removal of their subagent extension to prevent recursive
delegation. Named children also start without selected extensions. The allowlist
is captured, so tools added later, including extension tools selected later, are
not automatically enabled. Reopen with the same tool names: a changed default
selection is rejected when sending to an existing named child. Existing children
created without a restriction retain their selection; to keep sending to them,
use `open_with_restricted_subagent_management` with `None` (the legacy policy).
That constructor and `open_with_provider_and_subagent_management` use the explicit
selection supplied by the host: `None` is unrestricted, while `Some(vec![])`
disables all child tools. The bound selection is recorded per named child, and
changing it for an existing name is rejected. The restriction is by tool name,
not by extension identity, and trusted tools may have effects outside dispatch.
The tool exposes `status` (or its alias `list`), `stop`, and `send` (optional `follow_up` or `steer` mode).
A send creates the named child if needed and queues its answer as a parent
follow-up. Calls with the same persisted operation ID reuse the first message,
child and reporter; a new operation ID is a new send. `stop` addresses only a
named child of the bound parent, atomically persists the abort and a receipt for
the tool operation, then cancels and schedules cleanup of its foreground owned
graph. Replaying the operation, including after reopening, does not stop later
work. The child and transcript remain available for new sends. After a child-only
stop, existing reporters can report aborted submissions as failures. A parent
background-inclusive abort instead retires pending reporters from earlier generations.
Errors after admission
starts are uncertain and require reconciliation. The reporter runs while the
scheduler is open. Extension tools are currently marked unsafe for automatic
replay, so an interrupted invocation may require manual reconciliation even
though its admission uses an idempotency key.

The extension is bound to one parent and rejects selection on other conversations,
including forks; named children start without selected extensions. Do not register
this tool globally or replace the bound extension with another implementation.
`ToolContext` does not identify the caller or its owning task. In particular,
this API cannot verify a caller through independently implemented `ToolHost`s
or implement task-owner-scoped stop or wait. The upstream Durable background
example has no `wait` action; its separate foreground waiting example parks on
a task-owned child's submission signal, which this extension does not support.
Stop is scoped by the bound parent
and its named reservation, not by a calling task. The host must trust the configured tools and registry. It must also ensure
the child configuration is suitable for every child; no per-child model,
agent definition, working directory, history inheritance, or recursion policy is
provided. Lower-level named links are independent by default. The managed `subagent` send atomically creates a named reservation, child and completed background anchor on first use; existing explicit links are anchored before managed delivery. `Harness::anchor_subagent` can explicitly attach an immediately succeeded, conversation-owned background task and a foreground ownership edge to an existing named child. Attachment is atomic and idempotent across reopen; an ordinary parent abort does not reach the child, while background-inclusive parent abort marks the child for abort. The caller must drive cleanup. The management stop action still aborts only the child's current work and leaves the anchor in place. Reporter intent and input admission remain separate recoverable commits after creation, unlike upstream’s single spawn transaction. `request_abort_with_background`, `abort_with_background`, and `abort_and_wait_with_background` explicitly cross background boundaries, including terminal anchors; the waiting form requires a running task driver for live tasks.

This is not the complete upstream background-subagent extension. Lower-level
named links do not automatically form an ownership tree, and named children do
not inherit parent management extensions. Ordinary parent cancellation does not
cancel child work. Scheduler shutdown requests
cooperative cancellation of all local workers and waits for them; it is not a
durable abort and cannot guarantee termination or rollback of external effects.
Dropping a scheduler stops its reporter poller; reopening resumes unfinished
intents. Report delivery can be delayed by other work in the same child
conversation. Reporter receipts, stop receipts and answer IDs are retained without
a retention policy; reporter receipts are scanned.
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
than entry IDs or head markers. `request_compaction_with_instructions` persists
optional manual instructions for the summary request and hook, including across
recovery; `request_compaction` remains the instruction-free convenience API.
`request_manual_compaction` returns an operation ID, or `None` when there is
nothing to compact. Its `manual_compactions` record survives reopen and records
whether the operation is running, applied, declined, stale, aborted, or stopped at the
restart limit. The same ID is used for the hook across recovery; each model
attempt has its own request ID. Records are not inherited by forks and currently
grow without retention. Failures requiring reopen leave the operation running
until recovery or abort resolves it. This remains an idle-only conversation
phase, not a separately scheduled upstream task. Admission also rejects an
idle conversation with an unsettled abort, so that an earlier abort cannot
cancel a newly accepted operation.
After hook-supplied or accounted model output, the completed summary is
persisted before placement. A new tracked manual preparation pins its selected
occurrence and immutable source when provenance is available. Placement proves
the source is an ancestor, validates its selection and original metadata, and
checks that the same occurrence remains active. A retained target can be applied
after another compaction; a removed or unprovable target settles stale. The
placement, provenance and status commit atomically. An abort committed first
settles it aborted. Legacy or unannotated preparations retain the conservative
generation and source-prefix check. Proof validation scans transcript history
and is unbounded. Conversations created before reset identity boundaries were
recorded use that conservative check until a newly committed reset establishes
a proven boundary. An identical-text reset still creates new occurrence IDs.
For tracked manual operations, queued resets committed before the summary's
placement admission are placed first, making the summary stale; resets admitted
after that checkpoint are placed afterward. Resets queued during an aborted
operation stay ahead of later resets and their intervening writes, including
after reopening. Concurrent reset admissions merged into the checkpoint count
as earlier. Older checkpoints without ordering data
retain their previous placement behavior. This orders only manual summaries and
resets, not a general inbox or independently scheduled compaction.
Runtime-owned preparations also pin the context generation. Automatic and
legacy manual preparations without a proven target use it with the message
prefix to reject changed contexts, including resets with identical later text.
Legacy in-flight preparations without a generation can finish while the
conversation also has no generation; a reset, compaction, or fork fences them.
These checks do not enable busy manual compaction or provide upstream's full
stale-submission API.
`Scheduler::request_busy_independent_compaction` accepts a conversation while
a single model request is running or a model-issued tool call is running,
parked on a known pending reference, or waiting on an identified owned task,
with no queued inputs or resets at selection. Tool admission requires a
persisted attempt, a matching assistant round and the exact ordered prefix of
already completed results; a running call also requires a dispatch identity.
It persists a bounded selection snapshot. When summarization completes during
an active model request or eligible tool call, on-yield, after-run or after-tools hook, a parked
lifecycle hook task, or at idle, it checks the source prefix, context generation, original
submission and write identities, and configuration.
This permits append-only model progress and fenced model restarts, while queued
inputs, resets and application writes are ordered against the summary. An
after-tools task's result settles the tool round before placement; a tool
handoff starts its new context first and makes the older summary stale. A
queued reset preceding the summary is placed first and makes the summary stale;
queued inputs follow the selected writes. The placement ticket records the version at summary admission and
commits with the conversation's pending marker. Later writes follow that marker.
The marker and its preceding reset IDs survive reopen. Conversation abort
withdraws queued inputs but preserves a summary already admitted as a write;
an explicit operation abort withdraws it. Recovery requires the matching
provider identity. Use `Scheduler::open_with_summary_recovery_and_tasks` to
install the extension and task registries, adapter and summary provider before
recovered workers are scheduled. Keep its returned task driver alive and shut
it down after the scheduler. Task replay follows the registered handler policy;
external task effects still require stable idempotency and reconciliation.
The unfinished tool round is retained in the selected context, and the summary
waits for its full result sequence and tool boundary. An interrupted unsafe tool remains
blocked until its outcome is reconciled; summary recovery does not redispatch
it. Task-backed waits require the persisted task adapter identity and the
matching registered adapter and driver on recovery. Blocked or replay-ready
tools and unproven attempts are not eligible. A transient
unsupported phase can conservatively make a completed summary stale.

This is not a general ordered inbox: a changed context, changed selected
identities or configuration, another unsupported phase, or an old operation
without a selection checkpoint settles stale. The selection and hook
snapshots share a 512 KiB limit, and at most 128 preceding resets can be ordered. `wait_independent_compaction` may
return `Running` while boundary placement is pending; inspect its status after
the conversation reaches the boundary. On-yield decisions are checkpointed
before inbox selection, so an admission race does not repeat a completed hook.
An after-tools hook's completed decision and any child tasks commit with its
phase transition; concurrent summary admission is merged into that commit. A
crash before the decision commits can replay a hook; use its durable memo and
idempotent external effects to handle replay.

`Scheduler::request_idle_independent_compaction` starts an independent summary
for an idle conversation. Supply and retain a
unique operation ID; the call returns false if nothing can be compacted. On a
storage error, reopen and inspect that ID before retrying because admission may
have committed. Use
`wait_independent_compaction`, `independent_compaction_status`, and
`abort_independent_compaction` to observe or abort that operation. A failed
worker can be retried with `resume_independent_compaction` or recovered on
reopen. Selected `beforeCompact` hooks run from a persisted admission snapshot
with operation-scoped durable memo; the first decision wins. Use
`request_idle_independent_compaction_with_instructions` for optional
instructions. Admission checks the conversation's own phase and queue, not
activity in its owned tasks or descendants. Conversation abort marks running owned
independent operations atomically and waits for their cancellation and usage
settlement; an already queued summary write is preserved. Foreground idle waits
include them. Hook replay after interruption
may execute the hook again, as on the existing manual path. Recovery requires
the selected extensions and matching summary provider identity.

```rust,ignore
let operation = uuid::Uuid::new_v4();
if scheduler.request_idle_independent_compaction(conversation, operation, 0)? {
    let status = scheduler.wait_independent_compaction(operation).await?;
    // Successful completion is Applied, Stale, Aborted, or Declined.
}
```

Idle independent placement accepts only the exact context version captured
at preparation; a change settles the operation stale. The context edit
and terminal status commit together. Before an eligible placement decision,
the internal path atomically stores a version-fenced pending state and an
immutable placement-admission ticket. Its store commit sequence survives
reopen and can be compared with tracked conversation commits. This is an
ordering boundary for the narrow busy path, not a general shared inbox.
Placement limits transcript records and
processed payload sizes, but the storage API still materializes individual
entries before their sizes can be checked. `Scheduler::open_with_summary_recovery`
opts into startup discovery with host-attested model and usage-settlement
identities. The identities must cover the provider, account or tenant and
request-ID settlement namespace, and change when replay semantics change.
Legacy or mismatched operations are blocked and listed by
`blocked_summary_recovery`. Public operations automatically resume completed or
pending placement after reopen. Discovery fails before dispatch if there are
more than 128 total summary records or any record exceeds 4 MiB, including
terminal records. Public admission rejects a full store; there is currently no
retention policy. The store materializes each record
before checking its size. Busy compaction admission is not exposed. Usage
settlement requires request-ID idempotence; interrupted attempts are reported
unconfirmed.
Hook errors are retained in the extension report log and available through the
pull subscription and opt-in dispatcher described below; upstream's `onReport` API is not exposed. Empty hook
summaries are rejected by the existing compaction validation. Compaction is
still subject to the host's existing idle/manual and context-selection limits.

### Experimental custom documents and checked composition

`Documents::watch` returns an initial document (including absence) at an
immutable database-local revision and a pull-based `DocumentWatch`. `poll`
returns up to 256 subsequent committed states in revision order, including
deletes; an empty page means caught up at the time of the query. Save the
cursor after processing the whole page and use `resume_watch` with the same
database and address after reopening. Replaying from an older cursor can
repeat states. A cursor is not an acknowledgement or a portable database ID;
callers must retain its database and address and schedule their own polling.

`Documents::watch_commits` captures all live documents at one immutable
revision and returns a database-wide `DocumentCommitWatch`. Its `poll` returns
up to 256 document commits in revision order, each containing all document
states changed by that store transaction, ordered by encoded address; deletes
are `None`. A page never splits a commit. Persist the cursor only after the
entire page is processed; `resume_commit_watch` resumes exclusively from that
database-local revision after reopen. The initial snapshot and each individual
commit can contain arbitrarily many documents and bytes: the limit bounds only
the number of commits. A commit without document mutations is not emitted.
The stream records final states, not mutation order, intermediate writes, or
non-document parts of a store transaction. It includes every document scope;
callers must apply their own access control and filtering.

`DocumentWatch::poll_incremental_operations` derives the same bounded Chord
operations from successive immutable states, resuming from the existing document
cursor. The caller must retain the value at that cursor; creation/recreation
falls back to replacement and deletion is a retirement frame.

`DocumentWatch::poll_recorded_operations` uses the same cursor but prefers the
original decoded batch from `Transaction::apply_operations` when the document
version, operation record and store transaction can be verified together. Its
`actual` flag distinguishes verified original batches from derived or replacement
fallbacks. Tracking begins with each opt-in write; legacy writes, missing or
invalid evidence and deletions retain the existing fallback semantics. There is
no reconstruction of old operations. Pages contain at most 256 document
changes, but the page limit does not bound total bytes from legacy documents.
On errors the cursor remains unchanged.

`DocumentWatch::into_listener(interval)` adds serial, pull-driven async callback
delivery using the same `MergedListenerSignals` and `MergedListenerEnd` types
as the merged listener. Each successful callback acknowledges one complete
frame; an error, panic, or dropped run leaves its frame unacknowledged for
reattachment with `resume_watch`. Stop takes precedence over abort and then the
host source-close signal. A callback already in progress is awaited before a
signal ends the listener, and its successful result is acknowledged first. A
host source-close ends without draining subsequent frames, which remain
available after reattachment. The borrowed standalone store has no source lease,
so the host must signal closure explicitly; dropping other store handles does
not end the listener. The listener reads one frame at a time, with a 2 MiB
limit on each current and previous document and a 1.1 MB evidence limit; an
oversized document ends it with a failure and leaves the cursor unchanged.
Callbacks must eventually return and should make external effects idempotent.

`DocumentWatch::poll_operations` also returns one frame per committed state:
a put produces a Chord root replacement `["r", value]`, and a delete produces
an empty-operation retirement frame with no live document. It shares the same
exclusive cursor as `poll`, including after reopen; callers should not mix the
methods unless they intend to consume the same stream. These replacements are
synthesized from final states, not the original mutation operations. Unlike the
upstream retired frame, deletion exposes no previous value. This is a subset of
upstream `watchDoc`: the recorded-operation variant provides fine-grained
operations for opted-in writes and an opt-in callback driver; automatic source
lifetime notifications and push-based live notifications are not provided.
Upstream attaches only to an existing address and delivers operation batches,
folding pending operations and falling back to a root replacement above 10,000
operations. Its retirement/incarnation and task-end semantics, listener
lifecycle, delivery context, and operation-level ordering are not implemented.
The database-wide stream is a pi-rs extension, not an upstream Chord frame or
an implementation of its convergent callback queue. See upstream
`packages/durable/docs/spec.md` section 9.2 (compared at c1d4b0ac).
Multiple mutations of one key in a single raw batch collapse to the final
state, and raw deletes of missing keys still appear. Pages bound the number of
states, not their bytes, and history has no garbage collection. Legacy databases
start with their live documents at revision zero; earlier changes cannot be
replayed. Only writes through this storage implementation are recorded.

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

`Documents::snapshot` captures a database-local document revision; `get_at`
reads an address at that boundary, including across updates, deletes, recreation,
and reopen. All document mutations in one store commit share a revision, and
failed commits leave no history. CAS versions remain distinct from revisions.
A first open with this version seeds revision 0 from existing live documents;
earlier changes and deleted values cannot be recovered. Snapshots are valid only
for the same database, are not authorization tokens, and must not be fabricated.
History is retained indefinitely. Older binaries writing the database after
migration bypass history and are incompatible. Raw writes to the reserved
namespace are recorded but still must obey document invariants.

Each new conversation observation records the post-commit document revision in
the same SQLite transaction, including document writes staged during admission.
`Harness::document_at_cut` reads a known address at that boundary. `fork_at`,
entry/position forks, and `fork` copy documents whose scope is exactly the
parent conversation UUID string into the corresponding child UUID scope. Copies
are independent documents with fresh CAS versions; all other scopes remain
shared and are not copied. Historical forks use the selected cut; current forks
use a revision captured during the call while the parent conversation version
remains unchanged. Copies and child creation commit atomically. A fork of a fork follows the same rule.

An optional per-address `ForkPolicy` can be set with
`Transaction::set_fork_policy`, using the policy version returned by
`Documents::fork_policy` (or `Expected::Missing`). `Copy` selects the value at
the fork cut; `Current` selects the latest committed value and policy when the
fork commits, even for a historical cut; `Initial { value }` stores an object to use as the initial child
value. The policy and initial object are persisted, independently versioned,
and inherited by the child. Policy changes can be committed atomically with
document writes. Historical forks select by the policy at the cut: `Current`
then copies the latest value and policy, including deletion; addresses first
introduced after the cut participate when their latest policy is `Current`. An
absent policy, including for legacy documents, means copy. Policies belong to
addresses and survive document deletion and recreation. A fork inherits policies
even when the corresponding document is missing, without creating that document. Policies affect only conversation-scoped fork copies.
Policy changes advance the document revision but do not appear in document
watch streams.

This scope convention is not enforced ownership or access control. Existing
cuts from before this feature have no recorded boundary and historical document
reads and forks fail closed. Interior message cuts additionally require equal
document revisions across the surrounding commit; unrelated document writes
can therefore conservatively prevent such a fork. Revision enumeration scans
retained history and copying is proportional to the documents found. History is
unbounded. A database-wide revision fence retries forks after any concurrent document,
policy, or application-entry mutation, including unrelated scopes. After eight failed
attempts the fork returns a conflict and may be retried by the caller. Historical
`Copy` and `Initial` retain their cut semantics even if
the current policy changes. Unlike upstream, `Initial` eagerly copies a persisted
object when the source exists, and mutable per-address policies do not model upstream
document incarnations or duplicate-source rejection. This is a subset of upstream:
there are no typed definitions, task ownership, deferred visibility,
or general Chord transactions. Subagent history seeding is separate and does not
copy application documents.

`Harness::submit_with_documents` and `Scheduler::submit_with_documents` admit input, document changes, and optional tasks staged with `AdmissionDocuments::create_task` in
the same durable commit as the conversation, transcript and observation updates.
Its callback reads documents before staging any writes; every read, including
missing or read-only values, is checked at commit. Write only addresses read in
the attempt, once each. A CAS conflict reruns the whole callback, so it must be
deterministic and have no external effects. Duplicate request IDs return the
original submission without running the callback, and rejections do not write.
A failed storage commit can have an unknown outcome: reopen and reconcile using
a request ID. This API does not schedule the input; call `resume` as usual.
Tasks must be ready, owned by the admitted conversation, and registered with
the running task driver with the same replay policy. The driver wakes only
after the commit. An abort racing admission is serialized with task creation;
an already aborting conversation cannot admit tasks. Task IDs must be fresh.

This is a synchronous, bounded API, not upstream `Session.commit` or
Chord: it has no upstream-style draft transactions, general transaction snapshot reads, definitions,
operation history, observation, fork/rewind, deferred persistence, or automatic
lifecycle/ownership validation for arbitrary host batches. Admission supports
conversation-owned generic tasks, not arbitrary task batches or task-owned
children; those still require their respective scheduler ownership paths.

### Extension error observation

Unwinding section and hook panics are contained and reported whether they occur while
constructing the callback future or while polling it. A failing before-tool
hook still blocks the call; other hook and section failures retain their
existing continuation behavior. Reports use the same operation-and-phase
identity and first-batch-wins rule as other extension failures. Panic payloads
are not exposed; the report error is `extension panicked`. This does not cover
panics when querying section keys or tool metadata, nor tool execution errors.
The process panic hook still runs, and aborting panics cannot be caught. Catching
an unwind does not roll back extension state or external effects; extensions
must preserve their own invariants if reused after a panic. Cancelling a pending
callback drops it without producing a panic report.

`Harness::extension_reports()` and `Scheduler::extension_reports()` return all
persisted batches. `subscribe_extension_reports()` provides a pull subscription
on either host: `read(cursor, limit)` returns up to 256 batches (batch byte sizes are not capped), and
`next(cursor, limit, poll_interval).await` waits for a nonempty page. Begin at
cursor 0 and save each delivery's cursor **after** successfully processing its
batch. A cursor belongs to this database, not another database or an observer
stream. Invalid future cursors, zero intervals and page limits outside 1..=256
are rejected. Reopening and resubscribing from the saved cursor replays any
unacknowledged batch; external side effects require idempotency or an atomic
host-side checkpoint. Separate consumers maintain their own cursors.

A caught model-issued tool execution panic is reported with a fixed, redacted
message as soon as the panic is caught, before waiting for nested calls. Its
operation ID and phase deduplicate the report across reopen and reconciliation.
Recovery treats the committed report as a replay barrier even if the blocked
phase was not saved yet. A legacy blocked phase missing its report is repaired
on reopen or reconciliation.
The report does not include the panic payload, tool name, arguments, or output.
It is attributed to `runtime`, not an extension; nested tool panics and other
tool failures are not covered. Rust’s process-wide panic hook may independently
log the panic payload; this report does not control that hook. A report storage
failure requires reopening. If the report could not be committed before a crash,
recovery cannot distinguish that panic from an ordinary interrupted dispatch.

A new batch and its ordered index commit atomically; concurrent commits are
serialized by the store. Legacy batches are indexed when a subscription opens,
in bounded pages, before it returns. Legacy ordering is indexing order, not
original commit order. A batch is visible only after its report commit, which is
separate from the phase decision. The first batch for an operation and phase
wins across retries. An unknown storage commit outcome requires reopening and
reconciliation. No callback or delivery task runs on the execution path: a
consumer error or panic does not advance its cursor or block execution, and a
consumer may reenter the host. Dropping the pending future or subscription stops
waiting; subscriptions borrow the host and must be dropped before host shutdown.
Polling uses synchronous store reads on the calling async thread and adds latency
up to the selected interval. Subscription setup scans the existing log and may
perform storage writes; it can contend with execution. A storage error during
subscription setup poisons the host, requiring reopening and reconciliation.
The report index and reserved observer stream are internal namespaces; modifying
them externally invalidates delivery guarantees. Malformed records are reported
as errors, and an orphaned next observer record is rejected instead of retried.
There is no deletion or retention policy, and the persistent log and index grow without bound.

An opt-in `Harness::dispatch_extension_reports(cursor, poll, listener)` (also
on `Scheduler`) starts one host-owned Tokio task. The asynchronous listener gets
one `Delivery` at a time in cursor order, outside storage and runtime locks, and
may reenter the host. Return `Ok(())` only after processing and durably saving the
cursor in the host's own checkpoint. The dispatcher's cursor advances in memory
only after success; it does not persist checkpoints. A crash between side effects
and checkpointing can replay a batch, so make effects idempotent or atomically
checkpoint them. Each dispatcher has an independent cursor; it does not delete
reports. Listener `Err(String)`, panic, or a read error stops that dispatcher;
`close().await` returns the error and last successful cursor, and the host can
restart from its durable checkpoint. Failures are not emitted as new reports.

`close().await` requests stop, waits for a listener already selected for delivery
to finish, and returns its last successful cursor. A callback selected just before
the stop request may start after the request. It does not drain the log: later or
unstarted reports remain for replay. A listener that never completes can prevent
close from completing; it must not await its own dispatcher's close. Cancelling
the close future and awaiting it again is supported; subsequent calls return the
same result. Dropping the dispatcher
aborts the worker without waiting and cannot establish the awaited-close
guarantee; close explicitly before releasing the host. A callback must not spawn
untracked work if that work must finish before close. The dispatcher retains the
store owner until its worker ends, and synchronous polling can block the Tokio
thread during storage access. Starting it outside a Tokio runtime returns an
invalid-operation error before accessing the report store. Listener
errors do not poison the host; setup storage errors follow subscription setup's
poisoning rules and later read errors stop only the dispatcher.

This is an increment toward upstream's [`onReport(report)`](https://github.com/earendil-works/pi/blob/main/packages/durable/README.md#errors), not callback parity:
it exposes batches of extension errors rather than individual reports and does
not cover general task failures and handler `Err` (which poisons the task driver),
storage, host and listener errors. Hook task failures are reported when their
parent observes them. Invalid child decisions, malformed hook values, wrapped
tools, and some tool execution errors are not yet reported here. A crash can
rerun hooks and their external effects, which require memoization or
destination-level idempotency. The first recorded batch may differ from a later
retry's errors; exactly-once external effects are not guaranteed.

### Generic task definition versions

Generic task `Handler` implementations can override `run_with_documents` to
read and stage JSON documents through `TaskDocuments`. For `Continue`, `Wait`,
and `Succeed`, all document reads (including missing and read-only addresses),
document writes, the task transition and any children in `Wait` commit in one
checked batch. Each invocation captures one immutable document revision before the handler runs.
Reads use that revision and see staged writes and deletes; writes require a prior
read and can be replaced. Staged new documents report version zero; existing
staged documents retain their original version until commit. Every read address
is checked for any intervening mutation, including create/delete cycles. The default method calls the existing `run`.
`Fail` and abort handlers do not commit staged documents. Children can only be
created through `Wait` and remain subject to its ownership and registration
checks. A checked document conflict rolls back the entire batch. A replay-safe
handler is returned to ready and retried on a later tick without reopening;
contention retries do not consume the crash restart limit and can continue
indefinitely. A non-replay-safe handler instead fails with a reconciliation
reason because its external effects may already have happened. An abort that
arrives during the invocation fences its output. Other task conflicts and
storage errors still poison the scheduler and require reopening; storage errors
can have an unknown outcome. If owned-subagent admission only rewrites the
running reservation unchanged, the scheduler commits the existing handler
result against the refreshed version without rerunning it. Admission and the
post-handler graph decision share a gate; independently observed document and
child checks still apply. The reservation is a separate durable commit,
so a crash can leave the old checkpoint running. External effects require their
own idempotency or reconciliation. Unlike upstream's transaction callback,
this synchronous task callback supports conversation-owned application entries
(described below), but has no general session transaction or arbitrary task creation. It uses optimistic checks rather than the upstream
Session mutation line and does not expose mutable Chord drafts. Upstream commits writes
through `runtime.commit()` and documents replay-safe tool recovery on restart;
this scheduler additionally gates whole-handler conflict retries on its recorded
and current replay policies.

Generic `Task` records carry a positive definition version, JSON input, and a
checkpoint. Older records without these fields read as version 1 with null input.
Use `registered_versioned` and `Registry::register_versioned` to create tasks
and register definitions; legacy registration remains version 1. A definition's
migration receives the stored input, checkpoint, and version and returns both
new values. Migration must be deterministic and free of external effects: it can
be retried after a crash. The scheduler commits the migrated record and running
reservation together before invoking the handler. Failed or missing migrations,
missing definitions, and newer stored versions block ordinary execution without
changing the record. `blocked_reason` inspects this state without running code;
`replace_registry` reconsiders blocked work at a scheduling boundary, and reopen
also retries failures. New submissions must match the registered version.

`Driver::wait_for_task(id, &cancel)` asynchronously returns the persisted task
when it succeeds, fails, or is cancelled. Unknown IDs return an error. Cancelling
the token or dropping the future stops only the wait; use `request_abort` to
abort work. A running driver is required, including after reopening. The wait
polls storage every 100 ms, so it may lag a terminal commit; unlike upstream's
registered notifications, it also polls for driver shutdown and failure.

`Scheduler::wait_for_idle(conversation_id, &cancel)` waits for the conversation's
queued and running submissions, pending resets, and foreground owned tasks and child conversations.
It traverses terminal task anchors, but does not cross background boundaries.
Cancellation or dropping the future stops only the wait. Unknown conversations
return an error. Live generic tasks require a running task driver, including after
reopen; a missing or stopped driver returns an error. The wait polls every 100 ms,
does not start a driver or repair blocked work, and can remain pending for work
that needs host intervention. This is an observation, not a barrier against new
admissions: concurrent changes can immediately make an idle result stale. Each
check reads conversations, tasks, and ownership edges in one database snapshot.
Unlike upstream, this polls storage rather than using scheduler notifications;
there is no background-inclusive idle option. Shutdown consumes the scheduler,
so a borrowed idle wait must be cancelled or dropped before shutdown; upstream
instead rejects waiters when closing begins. Each poll reads all conversation,
task, and ownership records, so its cost grows with the store's graph size.

Both schedulers expose `blocked_tasks` as a polling snapshot of nonterminal tasks,
including their IDs, kinds, owners and current reasons; `blocked_reason` also
inspects an individual record. The background driver's `replace_registry` waits
for the current synchronous handler to finish, updates admission policies at the
same boundary, and wakes its worker. Lifecycle hook children are committed only
when their selected handler identity, replay policy, and version still match the
registry; replacement cannot interleave with that validation and commit. Calling
it or `blocked_tasks` from a handler
on that driver would deadlock. Replacement clears all cached migration failures,
even for unchanged definitions, and reopening retries them. Snapshots are not a
durable change log: short-lived changes can be missed and missing-migration and
migration callback failures share `MigrationFailed` (with diagnostic text).

Parity limitations: the standalone scheduler uses host-driven ticks and both
schedulers replace the whole registry. Unlike upstream, report coverage and
task-graph integration are incomplete. The driver exposes coalesced wake hints
and a readiness snapshot (described below), rather than upstream wake-state
notifications; extension-error subscriptions cover only a subset of reports.
The synchronous callback uses Rust `Result` and a structured
`Checkpoint` rather than upstream's JavaScript task types and JSON checkpoint;
panics are not converted into migration failure reports. Explicit aborts of
blocked definitions wait for owned children, then persist an orphan diagnostic
in the existing `Failed(String)` state, including the missing definition,
version incompatibility, or migration failure. This is not upstream's typed
orphaned outcome. A migration callback is attempted during abort only when an
abort handler is registered; otherwise the existing missing-handler diagnostic
is retained. Restart abandonment is not represented. Existing replay-safety and interrupted-execution recovery
policies remain pi-rs extensions and can differ from upstream, including leaving
some interrupted tasks running until a missing definition returns. Generic tasks
are not yet fully integrated into upstream's registry, task graph, or lifecycle.

Inherited entry-ID forks are supported for annotated occurrences visible in the captured shared transcript chain, including proven compaction copies. They use the original conversation’s recorded observation and document cut; unannotated or ambiguous chains and materialized interior forks are rejected.

### Inputs queued after a failed run

`Harness::set_after_failed_run` and `Scheduler::set_after_failed_run` persist a
per-conversation `AfterFailedRun` policy. `Keep` is the default: inputs remain
queued until a later submission, reset, or explicit `continue_queued` releases
them. `Continue` schedules the next queued inputs; repeated failures can drain
the queue. `Withdraw` settles queued inputs as `Unanswered { reason:
"run_failed" }`. Request IDs stay reserved, including after withdrawal and
reopening. Queued application writes are not withdrawn. The policy does not
change abort or reset behavior.

The failed settlement and policy decision are committed together. Unlike
upstream, `Continue` records an idle runnable boundary first and places inputs
in the next commit; reopening resumes that boundary. Configuration is
per-conversation here, rather than upstream's harness setting. Historical
snapshots predating this setting default to `Keep`; inheritance by independently
created or task-owned children is not implemented. Existing failure reasons and
supported failure paths remain those of this runtime.

### Generic task wake hints

`Driver::subscribe_wake()` takes the driver's single capacity-one channel. It is
initially signaled and later hints coalesce; notifications may be spurious and
close on shutdown or failure. After a hint, `Driver::wake_snapshot(now_millis)`
reads the task state and registry at a scheduling boundary without invoking
handlers or migrations. `active` reports ready executable tasks, running work,
abort cleanup candidates and executable waits that can be polled now. `at` is the earliest
future durable task deadline; `blocked` lists live definition failures. The
explicit clock is Unix milliseconds. A satisfied wait with a blocked definition
does not count as active until that definition becomes executable.
The standalone task scheduler exposes the same inspection method.

Inspection does not wait for an in-flight synchronous handler, but can wait for
a task commit and become stale as soon as it returns. Signal and child wait evaluation reads additional records;
driver and scheduler signal delivery is fenced by the task gate; direct unfenced
TaskStore writers are outside the consistency protocol. Abort
cleanup may still depend on foreground conversations. A standalone scheduler
cannot abort a task with foreground owned conversations; use the attached driver
for that ownership graph. Unlike upstream `onWake`, hints are
not a change-only `{active, at}` callback, and the snapshot covers generic tasks
only. The driver continues polling; this is not a host-managed alarm or a
conversation scheduling snapshot.

### Task application entries

A conversation-owned generic task or any of its task-owned descendants can call `TaskDocuments::append_entry(kind, data)` in `run_with_documents`. The root runtime conversation must exist and not be aborting; its version and the active task ancestry are checked at commit. Its ordered application records, successful checkpoint transition, checked document writes and Wait children commit atomically. Read committed records with `application_entry::list(&store, conversation_id)`; records include conversation and author task IDs. The returned entry ID is provisional until commit and may change on retry. Empty kinds and the `pi.` namespace are reserved.

`TaskDocuments::append_entry_if_head(expected, kind, data)` additionally requires the last application-record ID to match `expected`, or an empty log when `expected` is `None`. Earlier entries staged by the same invocation count as the head. A mismatch returns `ApplicationEntryHeadMismatch` without staging that entry; the caller decides how to handle it. The observed log is retained as a read dependency, so concurrent changes conflict even if the caller handles the mismatch and writes other state. Subsequent checks in the invocation use the same observation. The existing log-version check fences concurrent appends at commit, and a conflict follows the task's replay policy. IDs observed from `application_entry::list` can become stale, and IDs returned by staging are provisional. This check concerns the application log only, not the conversation transcript or upstream's entry `head` marker.

This is an independent, unbounded per-conversation JSON log, rewritten on each append. Current and historical forks inherit the records visible at the selected committed cut, retaining their original IDs, conversation IDs and authors. Later appends are isolated to their own conversation, and resets do not erase this log. Historical cuts from before entry history was recorded fail closed because their contents cannot be reconstructed. It is not the conversation runtime transcript or `submit_write` stream and is not included in observations, model context or message-entry ID lookup. Every task ancestor must exist, be nonterminal and not have an abort requested; their versions are checked atomically at commit. Invalid ancestry (including cycles) is rejected. The root conversation ID comes from the first conversation owner and is checked against the persisted conversation. Ancestor changes can force retries, and ancestry traversal is linear in depth with no configured depth limit. Staged-entry reads are unsupported. Failed steps and aborts discard staged records; conflicts retry only replay-safe handlers under the existing task transaction policy.

### Forking at a task application entry

`Harness::fork_at_application_entry(conversation, entry_id)` (also on `Scheduler`) forks at a committed task application entry, including a proven inherited entry. The child receives the application log through that entry inclusive, including when later entries were appended in the same task commit. Historical conversation documents and runtime state come from the entry-owning conversation at that commit; current-policy documents come from the immediate parent at fork creation. The runtime uses the latest verified snapshot before the task commit. The child, truncated log and document copies are committed atomically. Tasks and task-scoped documents are not copied. Later parent writes do not alter the fork, and current-policy documents still use their value at fork creation.

A task handler can call `TaskDocuments::checkpoint_runtime()` after staging an entry and before staging further entries. It writes a fresh observation and fork snapshot of the unchanged runtime state, plus an ordered proof identifying entries on either side of that checkpoint, in the task settlement transaction. The task, ancestry, conversation version, observer head, entries and document writes remain fenced together. Forks at those entries use the proven snapshot; document state is the final state of that commit. A concurrent runtime change causes the normal task replay or reconciliation policy to apply. This checkpoint does not itself change model context or configuration.

The verifier checks that the proof starts at the preceding application-log length and names the immediately preceding runtime snapshot. The proof is a trusted runtime record, not a cryptographic attestation: a caller that writes directly into reserved storage namespaces can forge an internally consistent checkpoint and ordering claim.

`TaskDocuments::configure_runtime_at_checkpoint(config)` instead changes an idle conversation's model configuration at the ordered checkpoint. At least one application entry must precede it, and later entries may follow. The task settlement atomically records the runtime write, ordered proof and documents. Forks at either side retain the commit's final configuration, matching upstream's end-of-commit agent-document semantics; their application log still stops at the selected entry. This operation does not change message context. It rejects active, aborting, queued, or compacting conversations and follows the existing task conflict/replay policy. Legacy or unproven same-commit changes remain rejected.

This bounded increment searches at most 256 application-log versions. An inherited prefix must be installed with the conversation’s first tracked runtime snapshot and an atomic lineage record. Each recorded immediate parent must contain the copied prefix at the fenced revision, recursively back to the entry owner; later entries must be local. Older inherited forks without lineage evidence fail closed, while local entries in conversations without an inherited prefix remain eligible. It rejects legacy untracked history, malformed or non-append-only logs, missing runtime snapshots, and same-commit runtime writes without a matching ordered checkpoint proof. Other state-changing runtime writes remain unsupported. The application log remains distinct from the message transcript and does not enter model context. As with other historical forks, incomplete tool contexts fail closed; document-fence contention may exhaust the bounded retries. Storage namespaces must be reserved for the runtime.

### Unified committed history

`Harness::unified_history_start(id, false)` captures a finite store-sequence boundary; `unified_history(&cursor, limit)` returns up to 255 complete commit frames and a serializable cursor for reopening. Each frame contains expanded runtime observation snapshots and, when changed, the complete task application-entry log. Snapshots repeat previously seen data; a fork exposes its inherited state in the child's fork commit, not the parent's original commit stream. Entries within one commit have no independent order. This is not upstream's entry transcript and does not add entries to model context.

Stores with any pre-sequence versions fail closed unless `unified_history_start(id, true)` explicitly accepts the migration boundary. That mode shows only commits recorded after tracking began, and cannot reconstruct earlier ordering. Cursors are trusted and database-local. Page limits bound commit frames, not snapshot size, transcript expansion or database work. The reader assumes reserved namespaces are not changed or deleted by raw or older writers.

### Host submission settlement waits

`Scheduler::wait_for_submission(conversation_id, submission_id, &cancel)` waits
for the specific committed submission, including queued work and work recovered
after reopening. Input and continue results include the stored answer text and
optional answer ID; legacy answers can lack an ID. Unanswered results retain the
submission kind and stored reason. A display-only write settles when placed,
including after a preceding reset; this API reports placement, not an upstream
transcript entry reference. Unknown conversations and submission IDs return
`NotFound` and `SubmissionNotFound` respectively. Cancelling or dropping the
wait leaves the submission running; an already-cancelled token takes precedence
when observed. Multiple waits are independent.

This is an incremental counterpart to upstream's [`Submission.wait`](https://github.com/earendil-works/pi/blob/main/packages/durable/README.md):
it polls committed conversation state at 100 ms intervals rather than subscribing
to session commits. A pending wait returns `Interrupted` on scheduler stop and
reports a scheduled worker error or panic; a committed settlement takes precedence
when observed. A retry starts a fresh worker-error observation when it acquires the conversation worker lock, while
`Scheduler::wait` and `shutdown` retain their own join/error behavior. A wait
borrows the scheduler, so cancel or drop it before consuming the scheduler in
`shutdown`. The wait does not start a task driver or resolve blocked work; a
missing/stopped driver or an externally blocked operation can leave it pending
until cancellation or host intervention. It does not offer upstream's global
submission handle or durable task notification.

### Ordered transcript block storage

The transcript storage API can append version 2 records with `prepare_append_with_blocks` and read them with `read_with_blocks`. Each block array must reference a unique user-message occurrence ID in the same versioned runtime transcript change; the binding is checked on write and read. Earlier experimental version 2 records without that binding are rejected as corrupt rather than interpreted as complete messages. Ordering, metadata and empty arrays are preserved across append and store reopen. Existing version 1 records remain readable.

`Harness::append_user_blocks` atomically appends a block-bearing user occurrence to an idle conversation. When the selected provider adapter opts into durable blocks, direct and child-task `onYield` continuations can also persist ordered blocks and continue the run. `Conversation::content_blocks` reconstructs each binding by message index on reopen; its occurrence ID is the same ID stored in the transcript change. The core message at that index is an empty placeholder and must not be used as content. The block map is reconstructed from the transcript and is not included in serialized conversation snapshots. Expanded history snapshots expose the map as `content_blocks` when nonempty, indexed into their `messages` array.

The durable provider interface passes bindings indexed into the prepared request, including the leading system message. Before-request hooks receive the map in `content_blocks`; while it is present they must preserve both `messages` and `content_blocks` exactly. Before-provider-request hooks receive the encoded body, subject to adapter validation. The opt-in OpenAI Responses adapter supports ordered text and base64 image blocks and the image `detail` field. Empty arrays and other metadata, including `textSignature`, fail before dispatch rather than being discarded. Adapters without durable support and the core model path reject block-bearing generation. Forks at complete transcript boundaries share the validated immutable block-bearing history, retaining block order, occurrence IDs, and their original provenance. Historical and entry-ID cuts also support safe interior positions in proven stable append batches when all retained occurrence provenance is known. The child stores a validated prefix projection and exact retained block arrays linked to the immutable source batch; reopening rechecks its source, occurrence histories, document cut, and block content. Unproven batches and incomplete tool contexts remain unsupported. Reset and tool handoff start a fresh context while retaining earlier ordered blocks and their occurrence provenance in immutable history. `Harness::reset_with_blocks` and `Scheduler::reset_with_blocks` admit a reset with one or more ordered user block occurrences. Each reset atomically places its handoff first, then its blocks in input order; eligible queued inputs follow all resets at the boundary. The returned reset and occurrence IDs are durable across retries, process loss, and abort cleanup. Multiple queued resets are placed in admission order in separate durable transitions as needed, so each block-bearing reset retains its own immutable history and occurrence IDs. Queued user inputs follow the resets at the boundary, including inputs admitted between them. This is a pi-rs extension; upstream reset exposes a text handoff. Manual, automatic threshold, and provider-overflow compaction support block-bearing contexts with proven occurrence history when every selected hook explicitly opts in to ordered blocks. Automatic generation also requires a block-aware provider adapter. Retention and threshold estimates count ordered text in UTF-16 units and charge a fixed 4,800 units per image, using the durable runtime’s approximate four units per token; image base64 and block metadata are not tokenized. Its persisted summary input contains the upstream text projection; images in the summarized prefix, including tool images, are omitted. Retained block arrays and occurrence provenance are reconstructed from the validated predecessor after compaction and reopen. A changed context or block snapshot makes the result stale. Hooks opt in by returning true from `Hook::supports_compaction_blocks`; `CompactionInput::content_blocks` supplies ordered source bindings indexed into the hook’s `messages`, including the original occurrence IDs. Bound messages are placeholders, so hooks must use the block map. The persisted preparation retains the complete source block snapshot and operation-scoped hook memo across recovery. The runtime rechecks hook capability before invocation; if any selected hook does not opt in, automatic threshold compaction is skipped and manual, overflow, and independent compaction fail closed. Idle and busy independent compaction also support block-bearing contexts under the same hook opt-in and occurrence-proof requirements. The independent preparation persists the source block snapshot; hook recovery verifies it against the immutable captured transcript. Idle placement requires the exact captured context and block map. Busy placement permits an append-only message tail only while the original block map and context generation remain unchanged; new or changed block bindings make the result stale. Existing independent-summary payload and history limits still apply. Summary requests for supported plain-message compaction serialize visible text, assistant tool calls and truncated tool results using upstream-style role labels; images in summarized user messages are omitted. Transcript-level forks validate the captured chain; the legacy transcript reader still rejects block-bearing records.

### Explicit nested pending-job settlement

When a conversation is blocked, `nested_pending_jobs` exposes the operation ID
and the last persisted opaque reference for each nested pending job.
`reconcile_nested_pending` records an authoritative terminal outcome, and
`cancel_nested_pending` records a host-confirmed remote cancellation as a
failed outcome. Neither API polls, dispatches, or cancels a remote job. The
caller must verify the outcome externally. The root ID, nested ID, reference,
record version, and blocked conversation version fence the update. Identical
retries are accepted while the same root remains blocked; conflicting outcomes
are rejected. References are retained in the journal, including after
settlement. After a storage error, reopen and inspect before retrying.

A confirmed job result can automatically reattach a direct nested call when
the parent returned a known, stateless result and the only nested failure was
a failed poll. The persisted and current parent replay policies must both be
safe, the dispatch identity and tool selection must still match, the restart
limit must permit replay, and every nested record must be terminal. The parent
is replayed with its original operation ID and receives the recorded child result;
the child is not dispatched or polled again. The scheduler schedules this after
confirmation; a reopened harness also resumes it. A persisted conversation abort
always prevents replay and remains marked until cleanup and parent settlement.

Other cases require an authoritative parent result with `reconcile`. Parent
settlement is rejected while any nested operation remains unresolved, including
intermediate calls and unsafe effects. This is narrower than upstream's
scheduler-owned nested task settlement: it does not replay intermediate callers,
legacy blocked records, extension-selected or stateful parent calls, or arbitrary
interrupted nested calls without a persisted pending reference.

For task-backed tools, `TaskTool::start_child` stages a keyed child and returns a
serializable `NestedTaskCall`. The handler can checkpoint the handle and return
`Step::Wait` with `WaitFor::TasksAny` and the handle's task ID. After a wake,
`Task::ended_children` identifies the ended children and `NestedTaskCall::result`
reads a fenced, direct-child snapshot: it returns `None` while unfinished and
preserves success, failure, and cancellation separately. Admission, the parent
checkpoint and wait commit together, so a handle cannot read an uncommitted
staged child. Reopening requires the same task registrations. This is a
synchronous task-handler API for `TaskTool`, not general `executeTool` support;
its child runs the registered task handler rather than the ordinary nested tool
hook pipeline, and starting it does not dispatch until the handler commits.

New TaskTool tasks persist their adapter type. When a TaskTool succeeds, fails,
returns a handler error, or is interrupted, it holds its outcome and atomically
marks all live direct nested TaskTool children and their foreground descendants
for abort. The parent waits for owned foreground work to finish before its
outcome becomes terminal. Abort handlers must be registered and perform their
own external cleanup; an abort handler's `Step::Fail`, `Err`, unwinding panic,
or nonterminal step settles that child as failed and lets sibling cleanup
continue. A process death or a failure to commit that outcome can still cause
the abort handler to run again on reopen, so its effects must be replay-safe.
Panics configured to abort the process cannot be caught. Generic
children are not marked by normal TaskTool settlement, even if they share a
handler kind, and generic task success still waits for its children naturally.
An explicit parent abort retains its broader ownership semantics. Handler errors
and `Step::Fail` discard children staged in that same uncommitted invocation.
Records written before the adapter type was added lack it and retain generic
semantics; existing keyed children cannot be silently upgraded. Drain or
reconcile such work before upgrading. A commit recorded by the new runtime must
not be resumed with an older runtime that does not enforce this type.

`KeyedHost::call_keyed_batch` is a bounded pi-rs extension for at most eight
distinct keyed calls. It admits calls in input order, durably records each
accepted pending reference, and then polls those jobs concurrently. Results
retain input order. Invalid, duplicate, or oversized batches fail before any
dispatch; an uncertain admission stops later admissions. Reopen reuses each
operation ID and reference, and abort stops local polling while retaining
pending jobs for host reconciliation. The parent remains live while waiting;
this is not upstream's independently scheduled child tasks or a parked parent.
Ordinary nested calls and `call_batch` retain their existing serial behavior.

The private `nested_suspension` storage prototype records a versioned parent
identity, checkpoint, bounded snapshot of pending job IDs and opaque references,
abort marker, and active or retired executor ID in one record. Its prepared
retire, claim, abort, and dependent-write checks use the record version to fence
stale transitions. The pending set is the immutable snapshot at suspension; this
prototype does not yet record individual job completion or wake the parent.
The caller must independently stop and join an executor before recording its
retirement, and compose these checks with authoritative conversation, task and
abort fences in the same transaction. The record is not an executor lease and
is not connected to live execution: it does not park or release a worker, start
pollers, transfer ownership, or cancel external jobs.

The generic task scheduler retains the last committed handler attempt as an
internal retired ID in the task record. A version-checked snapshot can fence a
future handoff against reservation, replay, and abort; its check must be
composed with conversation and ownership fences in the dependent transaction.
New reservations and abort marks clear the marker. Legacy records without it,
interrupted handlers, and discarded transitions do not acquire one. Older
executors do not enforce this fence and must not participate in a handoff. It
records that the handler returned and its transition committed, not that detached work
or another executor has joined, and it is not wired to live parking.

### Preparing host-supplied images

The opt-in `images` module implements the host-input portion of upstream image fitting. Provide an `ImageProcessor` or enable the optional `builtin-images` feature and use `BuiltinImageProcessor`; call `fit_image` for bytes or `fit_images` for ordered text/image JSON blocks **before** passing them to `append_user_blocks`, `reset_with_blocks`, or a continuation. The built-in processor decodes PNG, JPEG, GIF, WebP and BMP; it preserves upright inline images that fit, applies JPEG/WebP EXIF orientation (including WebP EXIF chunks with or without the `Exif\0\0` prefix), scales with Lanczos3, and re-encodes as PNG or JPEG until the base64 fits. As with Photon, images requiring transformation are normalized to 8-bit RGBA; this reduces 16-bit source precision, and transformed animations become a still image of their first frame. JPEG output does not retain alpha. The processor uses image-rs rather than upstream Photon, so resampling and compressed bytes can differ, including which candidate fits a boundary. Direct `ImageProcessor::prepare` rejects a declared MIME that disagrees with the decoded format; `fit_image` and `fit_images` sniff supported input first. It rejects source files above 20 MiB or decoded RGBA dimensions above 128 MiB; processing runs on a Tokio blocking worker when available, and cancellation does not stop a worker already running. `ImageLimits::default()` is 2000 by 2000 pixels and 4.5 MiB of base64. `ImageLimitsCatalog` accepts host-supplied upstream-shaped model descriptors and resolves `inputLimits.images.resize` field by field, defaulting absent fields. It can supply a snapshot-based resolver to `create_fit_images_extension`; model names must match the conversation's configured model exactly. pi-rs does not automatically import upstream's model catalog. A custom processor remains responsible for decoding and enforcing its limits, and should return `None` for images it cannot prepare. Every image is passed to it, with the MIME type sniffed from bytes when recognized. Conversion and resizing notes include the coordinate scale. Errors, invalid base64 and unpreparable images are omitted with notes; hosts should include those notes in model-visible text if desired. Text and unchanged image blocks retain their metadata and order; a changed image is replaced by the processor's image block, as upstream does. Invalid block shapes return an error instead of partially transforming the input.

For model-specific tool-result fitting with the optional feature enabled:

```rust,ignore
use std::sync::Arc;
use pi_rs_durable::images::{BuiltinImageProcessor, ImageLimitsCatalog, create_fit_images_extension};

let mut catalog = ImageLimitsCatalog::default();
catalog.insert("provider/model", &serde_json::json!({
    "inputLimits": {"images": {"resize": {"maxWidth": 1024}}}
}))?;
harness.install_extension(create_fit_images_extension(
    Arc::new(BuiltinImageProcessor), catalog.resolver(),
))?;
// Select "fit-images" for the conversation, after hooks that produce image blocks.
```

For example, given an existing `processor`, `harness` and conversation `id`:

```rust,ignore
use pi_rs_durable::images::{fit_images, FittedImages, ImageLimits};

let blocks = match fit_images(&blocks, ImageLimits::default(), &processor).await? {
    FittedImages::Unchanged => blocks,
    FittedImages::Fitted { mut content, notes } => {
        if !notes.is_empty() {
            content.push(serde_json::json!({"type": "text", "text": notes.join(" ")}));
        }
        content
    }
};
harness.append_user_blocks(id, blocks)?;
```

`FittedImages::Unchanged` means the caller keeps the original array; `FittedImages::Fitted` supplies replacement content and notes. For tool results, `create_fit_images_extension` installs a block-aware hook. The runtime runs hooks that opt in with `supports_runtime_tool_blocks` after its existing string and `after_tool_result` hooks, before committing the result. Such hooks receive the text result as an initial block and can replace it with ordered text/image blocks; the existing result channels are exposed read-only in `extra`, with structured output validated against any configured schema first. Nested calls run the same opt-in chain with `nested: true`; `call_keyed_with_blocks` and `call_keyed_unpolled_with_blocks` return persisted ordered blocks to their parent. Legacy outcome-only calls fail closed if blocks would be lost. The built-in image fitter still skips nested results, fitting only model-facing results. The fitter uses host-supplied limits and appends an `info` diagnostic with code `image`. Hooks may replay after interruption; their effects must be idempotent. Legacy hooks do not run twice and block-hook failures stop the transition. Existing core tools still return text, so producing image blocks requires a block-aware hook; there is no built-in Photon processor.

Ordered tool blocks are stored with their tool occurrence and reconstructed after restart, with the tool call ID intact. Full validated forks preserve them. Manual, threshold, overflow and independent compaction also support tool-block contexts when every selected hook opts into ordered blocks and separately returns true from `supports_compaction_tool_blocks`. This separate opt-in prevents existing user-block-only hooks from accidentally interpreting a tool placeholder as empty output. Summaries include tool text in block order and omit images; retained tool blocks keep their occurrence IDs and call IDs, including across forks and reopen. Reset starts a fresh context and retains the blocks in history. Concurrent independent placement rejects changed block snapshots. Provider delivery requires an adapter implementing and opting into `DurableToolRequest`; OpenAI Responses joins text and sends images in its function output; Ollama joins text and sends images in a following user message. Anthropic retains successful result blocks in order and moves error-result images after the complete tool-result batch. The Google adapter defaults to upstream pi's projection: text blocks are joined with newlines and images are separated; older Gemini models receive images in a following user message. Original blocks remain in durable storage, but their interleaving is not preserved in this provider request. An explicit `GoogleAdapter::new_ordered_tool_blocks` uses a distinct adapter ID and Gemini image references to preserve ordering; its Google wire representation intentionally differs from upstream. The core model path rejects tool blocks. Before-request hooks receive `tool_content_blocks` and cannot replace it or the message list; provider-body hooks remain subject to adapter validation. Existing transcripts are not refitted after a model change or restart.

The bundled adapters reject unsupported metadata and ambiguous call IDs before
sending; Google upstream-compatible mode also accepts empty tool output arrays.
They validate the complete provider message sequence after body hooks, so a hook
cannot alter, drop or reorder a bound result.
Adapters use the bundled image-profile snapshot where available and host-supplied
model descriptors can override it. Ollama has no bundled model catalog; unknown
models retain their previous image-capable behavior. These adapters do not probe
model vision support. A provider can reject an otherwise valid image or
model combination; no silent fallback is performed. Hosts may configure model
`input` descriptors; text-only models receive an omission placeholder in place
of tool-result images, while unknown profiles retain the previous image-capable
default. Google and Anthropic receive persisted `is_error` state for both text and block
results and emit their upstream `error` or `is_error` fields. OpenAI Responses and
Ollama consume the metadata but have no corresponding upstream wire flag; the
error text is sent as the tool output. A legacy result with no status metadata
uses the provider's default treatment of an unknown status. If a reused call ID cannot be associated with a unique occurrence,
request preparation fails closed. A metadata-aware adapter persists even a
single-text tool result as a block, binding its occurrence ID to operation
metadata. Consequently that context uses the existing tool-block compaction and
hook opt-in requirements. Legacy metadata without an occurrence binding fails
closed rather than being matched by call ID. When status metadata is forwarded, a
before-request hook cannot replace the message list. Descriptors are host-owned
and must be restored on reopen. The ordered opt-in requires
recognized Gemini 3+ model IDs and PNG, JPEG or WebP images, and uses
`$ref` entries in an ordered JSON output array; live model acceptance and
interpretation of this representation have not been tested. Non-text/image
output, provider-specific block metadata, and model-specific image conversion
are not implemented.

### Model image profiles

Durable adapters resolve model image profiles for each generation. The default
`ProviderAdapter::image_profile` uses exact model IDs in a snapshot of pi-rs
workbench's bundled `builtin-chat-models.json` for the OpenAI Responses,
Anthropic Messages and Google adapter identities (Google also accepts the
`models/` prefix). This snapshot can age; a custom or OpenAI-compatible endpoint
must override the method when its capabilities differ. Unknown adapters and
models return `None`, preserving the previous behavior; Ollama discovery is
not wired to this catalog. Core `Model` does not expose modality or resize limits,
so the core-only model path cannot resolve them automatically.

Google's `set_model_descriptor` overrides catalog input capability for the
same model (with or without the `models/` prefix), while retaining catalog
limits. The descriptor is host configuration and must be reinstalled on reopen.

For a known text-only model, durable replaces user and tool images with upstream
placeholder text in the outgoing request copy, including images in legacy tool
JSON. Ordered image runs collapse to one placeholder. The stored transcript and
its original blocks remain intact. Existing request and provider hooks still run;
the adapter validates the projected request after provider-body hooks.

Install `create_model_fit_images_extension` with an image processor to use the
selected adapter's current model limits for model-facing tool results before
they are stored. A custom adapter can provide its own profile; unknown models
use the upstream default limits. `Harness::model_image_profile` exposes the
selected profile to hosts preparing user images with `fit_image` or `fit_images`
before admission. User image fitting remains explicit and existing history is
not refitted on model changes. Model input limits are based on catalog metadata,
not runtime probing, and do not imply provider acceptance.

### Opt-in user image admission

`UserImageAdmission::new(Arc<dyn ImageProcessor>)` selects the processor for
`Harness::submit_fitted_user_blocks` or the corresponding `Scheduler` method.
The method fits an ordered nonempty text/image block array using the selected
adapter's current model limits, then commits the fitted payload and submission
in one transition. Immediately placed inputs also enter the transcript in that
transition; queued inputs enter it when selected at a boundary. The scheduler
schedules only after admission; a harness
caller drives the run with `resume_conversation`. Unselected ordinary string
submissions and `append_user_blocks` do not fit images. A selected block-aware
adapter is required; an unknown image profile uses the default limits.

The returned `ImageAdmissionResult` contains the submission ID and preparation
notes. Notes are stored in `Submission::image_notes` and appended as a text
block visible to the model. An image that cannot be prepared is omitted with a
note, including processor and malformed-base64 errors. Existing block metadata
is retained on unchanged images; transformed images use the fitter's output
shape. Reusing an input request ID returns the original ID and notes without
running the processor again, even after reopen. A conflict, cancellation or
invalid input commits no submission; after an indeterminate storage error, reopen
and use the same request ID to check the outcome. Processors can run again after
a conflict or interruption and must tolerate retries and cancellation. Model
configuration is version-fenced
across the asynchronous fit, and the adapter profile is rechecked before commit.
Hosts must keep adapter profile configuration stable during admission.

`submit_fitted_user_blocks_with_options` also admits steering or follow-up
blocks while busy. The fitted payload and occurrence ID are durable while
queued, use the existing queue ordering and rejection policies, and are not
refitted at placement or after a model change. Abort withdraws queued inputs
under the existing rules. Reusing a request ID returns the original result.
`reset_with_fitted_blocks` fits each reset addition using the admission-time
model and preserves the existing reset ordering. Its optional reset request ID
has a separate conversation-scoped deduplication namespace; retries return the
same reset ID, occurrence IDs and notes, including after reopen. Forks start a
new reset-deduplication namespace. An admission or reset racing a conversation
change can return a conflict; retry the operation with the same request ID.

For an explicitly selected `onYield` hook, call
`UserImageAdmission::prepare_for_context` and return its blocks in
`GenerationDecision::Continue`. The lifecycle context supplies the current
adapter model profile. Notes are appended to the continuation; the existing
continuation precedence, recovery, and cancellation rules still apply. The
host must opt in on each continuation; reset, string inputs, other hooks and
document-composed admission are not fitted automatically. Fitted history is
never refitted after a model change. These are pi-rs extensions: upstream
exposes `fitImages` for the host to call before submit, not a general
user-admission hook. Installing a tool-result fit extension alone does not
select these APIs.

### Explicit task document ownership

`TaskDocuments::acquire_owned` acquires a currently absent address for the
executing task; `Transaction::acquire_task_document` stages the same explicit
association in a checked transaction. Existing unowned documents cannot be
implicitly acquired, and metadata is permanent: choose an unused address.
Unregistered addresses and their arbitrary host scopes keep their former
behavior. Registered addresses are checked against the candidate task inside
the store's transaction, including writes made through raw `Store::commit`.
Completing retains the document. Terminal settlement retires the live document
in the same commit; a new document staged in that terminal commit never becomes
live. Owner records cannot be deleted or resurrected once a document is owned.

`Documents::watch_task_document` and `resume_task_document_watch` expose a
pull watch with `ended()`. Retirement carries the last delivered value and no
operations; the watch ends after its terminal retirement is consumed. If the
document was already absent, it ends after the available changes are drained.
This does not implement upstream document definitions, incarnation semantics,
observer lifecycle, or automatic wakeups. Raw writes to ownership metadata are
stored in a dedicated SQLite table; ordinary store byte keys are not reserved.
Direct SQLite writes bypass this protocol. The store validates owned task
records with the same persisted schema used by `TaskStore`. It validates only
owners affected by a commit, so a malformed owner does not block unrelated
writes; mutations involving that owner fail closed, including attempts to
replace its malformed task record. Persisted task declarations live in
`crates/durable-task-schema`, so schema changes must be made there for both
runtime and store. Previously corrupted records or already
retired documents are not automatically repaired. Each store commit currently scans ownership metadata,
so large deployments should account for its cost.

Databases used with the earlier experimental byte-key ownership implementation
must be migrated explicitly before writes resume. Stop all old writers, identify
the genuine owned document keys and task IDs, then call
`Store::migrate_legacy_task_documents` with the complete set. It validates the
old records and tasks and migrates the supplied set atomically. Old byte keys
remain ordinary untouched keys: they are ambiguous with arbitrary user data and
are never auto-migrated. Without this explicit migration those old records do
not enforce ownership. Do not use old writers after migration.
