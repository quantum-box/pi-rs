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
Replacement batches assign fresh occurrence IDs, and an interior fork currently
materializes a new transcript with fresh IDs. Annotated occurrences in a shared
immutable chain, including complete replacement boundaries, may be selected by ID
using their origin’s historical cut. See the parity document for remaining limitations.

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
is safe to replay. Each invocation uses a fresh attempt identity. Completion clears
the partial; abort or an interrupted attempt retains the last committed partial.
Callbacks are fenced by the conversation phase and attempt identity, and progress
is sealed atomically with the corresponding transition. Legacy running attempts
without an identity do not report durable progress until replay creates one.

This pull API does not provide upstream's watch operations, `from`, automatic
min/max window maintenance, or transcript entries for reset and compaction
markers. Progress updates alone do not appear in observation history; poll the
view for them. Message count and record reads are bounded, but a stored batch
(especially a replacement), message, and conversation metadata can each be
arbitrarily large and are decoded in full. Bytes, metadata copying and peak
memory are therefore not independently bounded. Existing history and execution
paths still hydrate all messages. Store reads are synchronous and may block.

`Harness::watch_view` and `Scheduler::watch_view` attach a pull watch with an
initial `ConversationView`; `next().await` polls for the next changed bounded
snapshot, including committed generation and tool progress. The interval must be nonzero.
Cancel a wait by dropping its future; close or drop the watch to release its store
ownership. A watch retains the store owner even after the originating handle is
dropped or the scheduler shuts down, so close it before reopening the database.

This is a latest-state watch, not an audit stream: it has no background queue,
and any number of commits between reads (including progress changes that later
revert) can be coalesced or missed. Each returned value replaces the previous
one. Unlike upstream, it has no Chord operations, callbacks, automatic window
adjustment, 100-frame queue, end reason, or automatic termination on harness
close. It observes persisted state only, and synchronous reads can block the
executor. The view's count and record-visit bounds still apply, but metadata,
individual batches, and the retained initial snapshot are not byte-bounded.
For an append-only cursor stream use `watch_history` instead; it covers only
records explicitly included in that observation log.

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
descendants. Task-owned foreground subagent conversations are marked and
scheduled for cleanup by the conversation scheduler; background edges are boundaries.
Task cleanup waits until each foreground child conversation has settled its abort,
including that conversation's foreground tasks. `Scheduler::abort_and_wait` joins
workers for the entire affected foreground scope. Standalone task-driver aborts
still reject foreground conversation children because they cannot drive or cancel
their workers. A blocked uncertain operation requires reconciliation, and a task
driver must be running to drain tasks.

Task-to-conversation edge preparation and owned-subagent admission reject owner
cycles by checking the persisted owner chain, including background links. Versions
and absent root edges are checked in the transaction, so concurrent changes require
fresh preparation. Prepared edges must be committed one at a time without other
ownership changes; raw store writes must preserve immutable ownership. Existing
corrupt cycles are rejected on traversal, not repaired. Background remains an
abort/idle boundary, not a break in ownership identity.
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
  The older `send_signal` retains receipt idempotency semantics and reports
  an expired typed key as an error. General transactional delivery remains missing.
  Storage errors can have uncertain outcomes. Same-ID retries remain successful
  after task termination,
  while a new ID is rejected for a terminal task. Existing stores with
  multiple typed records for one key are not consolidated; conflicting records
  reject new deliveries, and any consumed record closes the key.
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
than entry IDs or head markers. Manual compaction currently has no instructions
argument. Hook errors are retained in the extension report log and available through the
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

This is a subset of upstream `watchDoc`: it exposes whole committed states,
not Chord operations, callbacks, abort/lifecycle handling or live notifications.
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
the fork cut; `Initial { value }` stores an object to use as the initial child
value. The policy and initial object are persisted, independently versioned,
and inherited by the child. Policy changes can be committed atomically with
document writes, and historical forks use the policy at the selected cut. An
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
unbounded. This is a subset of upstream: there are no upstream-style current-at-fork policies for historical cuts,
typed definitions, task ownership, deferred visibility,
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
can have an unknown outcome. The reservation is a separate durable commit,
so a crash can leave the old checkpoint running. External effects require their
own idempotency or reconciliation. Unlike upstream's transaction callback,
this synchronous task callback has no entries, general session transaction,
or arbitrary task creation. It uses optimistic checks rather than the upstream
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
schedulers replace the whole registry. Unlike upstream, there is no report callback,
task-graph integration, or host wake notification for blocked changes. The synchronous callback uses Rust `Result` and a structured
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

Inherited entry-ID forks are supported for annotated append batches visible in the captured shared transcript chain. They use the original conversation’s recorded observation and document cut; replacement boundaries, unannotated or ambiguous chains, and materialized interior forks are rejected.

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
abort cleanup candidates and waits that can be polled now. `at` is the earliest
future durable task deadline; `blocked` lists live definition failures. The
explicit clock is Unix milliseconds. A satisfied wait counts as active even if
its definition is blocked, because the scheduler can still persist its wake.
The standalone task scheduler exposes the same inspection method.

Inspection waits for an in-flight synchronous handler and can become stale as
soon as it returns. Signal and child wait evaluation reads additional records;
driver and scheduler signal delivery is fenced by the task gate; direct unfenced
TaskStore writers are outside the consistency protocol. Abort
cleanup may still depend on foreground conversations, and fail-fast sibling
cleanup is not independently represented. Unlike upstream `onWake`, hints are
not a change-only `{active, at}` callback, and the snapshot covers generic tasks
only. The driver continues polling; this is not a host-managed alarm or a
conversation scheduling snapshot.

### Task application entries

A conversation-owned generic task or any of its task-owned descendants can call `TaskDocuments::append_entry(kind, data)` in `run_with_documents`. The root runtime conversation must exist and not be aborting; its version and the active task ancestry are checked at commit. Its ordered application records, successful checkpoint transition, checked document writes and Wait children commit atomically. Read committed records with `application_entry::list(&store, conversation_id)`; records include conversation and author task IDs. The returned entry ID is provisional until commit and may change on retry. Empty kinds and the `pi.` namespace are reserved.

This is an independent, unbounded per-conversation JSON log, rewritten on each append. Current and historical forks inherit the records visible at the selected committed cut, retaining their original IDs, conversation IDs and authors. Later appends are isolated to their own conversation, and resets do not erase this log. Historical cuts from before entry history was recorded fail closed because their contents cannot be reconstructed. It is not the conversation runtime transcript or `submit_write` stream and is not included in observations, model context or entry-ID lookup. Every task ancestor must exist, be nonterminal and not have an abort requested; their versions are checked atomically at commit. Invalid ancestry (including cycles) is rejected. The root conversation ID comes from the first conversation owner; no conversation record is required. Ancestor changes can force retries, and ancestry traversal is linear in depth with no configured depth limit. Staged-entry reads are unsupported. Failed steps and aborts discard staged records; conflicts retry only replay-safe handlers under the existing task transaction policy.
