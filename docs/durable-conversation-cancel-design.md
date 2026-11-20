# Conversation-owned external cancellation: integration notes

## Nested cancellation groundwork

Nested callback dispatch remains disabled. The task store can atomically record
an exact child-to-conversation-root ancestry together with matching callback
markers and immutable ancestry evidence. Recovery can check that evidence and
retire the same markers after a reopen. While markers exist, task writes and
wakes on the marked chain are fenced and unrelated graphs can continue. Scoped
callback reentry checks match the durable marker on an ancestor, including in a
joined task given the callback scope.

These primitives do not authorize a callback. A nested implementation must bind
its claim, the complete ancestry evidence, active attempt and lease, registry
and identity to one transaction, fence the conversation and owner graph through
first poll, and settle callback evidence without redispatch. The driver tests
exercise the marker foundation, not nested external cancellation.

## Opt-in first-poll dispatch

`register_conversation_unsafe_durable_tool_with_at_most_once_park_cancel`
explicitly enables cancellation only for an unhooked, conversation-owned root
with a confirmed park and no live owned children. The host identity must cover
the Tool and cancel handler, and host updates must use its `IdentityHandle`.
The original registration still does not dispatch callbacks and keeps its
existing definition identity. Opting in changes the registered definition identity;
previously parked tasks must retain their original registration and cannot be
silently upgraded. Older claims lacking the claim-time conversation version fail
closed and require reconciliation.

The driver obtains a nonblocking target start lease before committing the
identity-bound claim's conversation and task markers. It holds that lease
through the callback's first poll, and releases it before subsequent polls.
A successful callback receipt and exact marker retirement share one checked
transaction. A callback error, panic, timeout or competing late Tool outcome
is recorded as unknown and requires reconciliation; no subsequent attempt may
redispatch the same claim. A process interruption recovers the original claim
and matching markers into a reconciliation-required terminal task. The callback
is bounded to 30 seconds and must cooperate with cancellation and avoid detached
work. Direct Store writes outside the public mutation APIs are a trusted-host
boundary. Nested cancel callbacks and woken cancellation remain unsupported by
this opt-in.

`register_conversation_unsafe_durable_tool_with_selected_hooks_and_park_cancel`
records a selected-hook identity and a cancel identity in one definition. It
does not enable callback dispatch. Aborting a parked call still requires
reconciliation; in particular, this registration does not yet authorize the
callback or run the conversation after hook on abort.

`register_conversation_unsafe_durable_tool_with_selected_hooks_and_at_most_once_park_cancel`
is a separate explicit opt-in for a conversation root with selected replay-safe
hooks. Its definition identity differs from the registration-only form; hosts
must retain the original registration for existing parked tasks. Abort cleanup
waits for owned children before invoking the callback and fences unresolved
unsafe descendant evidence at claim, receipt, settlement and publication. The selected after hook
runs with the committed conversation snapshot and prepared arguments. Its memo
operations are durable and fenced by the active cleanup attempt, identity,
claim, snapshot, and start markers. A failed memo operation, hook error or
timeout, or a conflict saving the after result retains a confirmed callback
receipt when it can be safely committed and requires reconciliation for the
incomplete after result. A process interruption before the after result is saved
still requires reconciliation; memo persistence does not authorize another callback. Hook output schemas on this
abort path are not supported. Nested child callbacks and woken cancellation are
not enabled by this registration. Result publication currently requires the
root adapter in the Harness tool list; an extension-only tool is rejected before
callback dispatch.

The claim, start-marker and receipt transactions hold the selection gate
from validation through commit. A per-name, nonblocking selection start lease
is acquired before the claim and is wired to release after the callback's first
poll. Selected registry updates return busy during that lease; updates to other
names remain independent. The version 2 receipt contains both the confirmed
callback result and bounded after output in one checked write; the terminal
marker is committed with the task transition. The after hook's conversation
snapshot version is checked in the receipt transaction. Conversation publication checks
the claim, marker, receipt, park evidence and current identity. A process killed
before or during the first poll remains unknown without another dispatch. A
committed after receipt can be recovered after a process stop only when those
checks still hold; a late pending outcome remains available for reconciliation.
Version 1 receipts continue to represent callback-only evidence. Older runtimes
that do not understand version 2 cannot safely publish its after result; keep
the upgraded runtime or reconcile the task before downgrading.

`DurableParkCancel::cancel_scoped` supplies an explicit callback scope. When a
callback uses a joined Tokio task, it must pass the scope into that task and
run its work through `scope.scope(...)`; task-local state is not inherited by
`tokio::spawn`. This lets same-target retrying signal and abort operations reject
self-wait. Without the scope, use a cancellation token or an explicit timeout
for retrying operations, and join every spawned task before returning. The
30-second callback timeout cannot preempt blocking work or join detached tasks.

Conversation saves acquire a shared mutation lease and check the durable start
marker in the same checked commit. A task-input admission moves that lease to
its prepared settlement, retaining it through retries until commit or disposal.
While a callback marker exists, the internal abort-cleanup reservation checks
the original claim, attempt, owner, definition identity and both markers in one
transaction. External cleanup work is deferred while the unsafe checkpointable
root remains live. Direct writes to the store remain outside these API fences.

If the store reports writer contention before a settlement transaction starts,
driver shutdown returns the pending outcome and its lease for retry or
reconciliation instead of waiting for the unrelated writer. This does not
classify an indeterminate commit as uncommitted.

The sections below describe the earlier stages; references to dispatch being
disabled apply to those stages and to the original registration.

Status: cancellation is not enabled. The scheduler defers conversation-owned
interrupted wake or parked-abort recovery and its owned task graph until a
Harness is bound; unrelated tasks continue. Binding triggers re-evaluation,
which still uses the existing refusal for unsupported cancellation. Standalone cancel registrations must
not be attached to a conversation as a substitute.

Upstream `packages/durable/src/harness/tool.ts` invokes Tool cancellation for a
committed park and an interrupted woken execution, and can run the abort result
through after hooks. pi-rs currently retains external outcomes as uncertain and
requires reconciliation; it does not provide that conversation cancellation API.

The existing standalone claim, dispatch and abort-receipt paths explicitly
require a standalone admission. Removing that restriction alone would omit the
conversation's root binding, active attempt, selected extension identity and
abort state from the checked transactions. Normal conversation intent checks
also reject an abort-marked conversation and cannot be reused unchanged.

A root-owned unsafe Tool registered for cancellation can write a claim for a
committed park after its owned work is drained. Version 2 claims contain an
immutable claim ID, original cleanup attempt and lease, and an at-most-once
callback policy. A dependent transaction must carry the version check returned
by claim reading. Version 1 claims lack the ID and policy; they cannot be
safely upgraded automatically and require explicit reconciliation. The claim does not dispatch the
callback and is followed by a reconciliation-required task failure. Its commit
checks the conversation, task and park evidence; the selected Tool identity
lease is held through the commit. Nested roots, interrupted wake cancellation,
callback dispatch and receipt settlement remain unsupported. Registrations made
without an IdentityHandle use an older definition identity and cannot resume
under the new claim registration; reconcile them before upgrading.

Unhooked unsafe conversation Tools may register a park-cancel handler using
`register_conversation_unsafe_durable_tool_with_park_cancel`. The host supplies
an identity that must change with handler implementation or configuration;
that identity is included in the persisted task definition. Reopening a pending
call with a different identity is rejected. This registration retains the
handler but does not dispatch it or enable external cancellation. Hooked registrations
and callback retry policy are not included in this initial registration API.

A preliminary receipt write transaction checks the live claim, lease,
conversation binding, owner graph and park evidence atomically. It is exercised
only by a test-only synthetic receipt; no callback is invoked and this record
is not proof that external cleanup occurred. Stopped-root test receipt recovery checks a terminal marker written in the
same commit as the task failure, the claim and receipt, conversation binding,
park provenance and late-result fence. The marker carries the original claim,
attempt and running task version; the task and marker commit identities must
match. Recovery remains limited to a failed, abort-marked unhooked root while
the conversation retains its pending binding. It does not authorize callback
dispatch or establish that a callback ran.

The scheduled worker's Idle-to-ModelReady input placement now takes the
callback-start gate without blocking. If it is busy, the uncommitted placement
is discarded and the worker retries from durable state. Other save and direct
Harness paths do not yet take this gate, so it does not authorize dispatch.

The direct `Harness::reset` and `Harness::withdraw_submission` paths also try the gate before constructing or
saving a reset, returning `TaskPublicationBusy` for the caller to retry. This
is not an automatic retry. Other direct mutation APIs (input and write
admission, session transactions, reset-with-blocks, abort and
extension selection) and the general Harness save/commit paths have not been
connected. Some of those compose task and document writes or use the blocking
conversation fence; their lock ordering and retry owners still need separate
verification before dispatch can be enabled.

Mutation leases may overlap one another; the callback first-poll lease is
exclusive against all of them. Durable transaction checks remain responsible
for conflicts between concurrent mutations. This removes gate-induced Busy
between ordinary mutations without authorizing callback dispatch.

A nonblocking target-specific start lease can bracket exactly the first poll
of a future without holding registry, task or poll mutexes. It is a primitive
only: callback dispatch and most mutation paths do not yet use it. Existing
`try_fence` still acquires all registered start gates and can defer unrelated
graphs; blocking abort and signal delivery are not protected by this primitive.
The task wake and signal revision must be checked before dispatch is enabled.

Deferred task cleanup now discovers its foreground conversations, takes only
those poll and start gates, and repeats graph discovery under the task gate
before committing. A newly reached conversation causes a non-mutating retry;
a removed edge permits a conservative extra fence. This applies only to the
nonblocking task cleanup and settlement paths. Direct abort, signal delivery,
and other blocking graph fences remain outside this change. The controls
registry is still held through commit. These remaining boundaries must be
addressed before the callback can be dispatched.

Harness-mediated conversation signal delivery now takes the target mutation
lease and checks its signal revision in the same transaction as the receipt.
This excludes a callback first poll and allows independent conversations to
progress. A durable signal-start marker protocol additionally makes public
`Inbox` receipt writes and prepared wake commits check the absence of the
marker in the same Store transaction, including writes from a separately opened
Store. A marker-start candidate checks the conversation signal revision, and
retirement checks the exact marker version and value. A marker surviving a
crash blocks the target until reconciliation; it does not block another
conversation. These helpers are not connected to callback dispatch and are
not by themselves authorization to invoke it: the start transaction must also
fence the claim, identity, task and task-signal revision, owner, abort, late
outcome and selection. Public `Inbox` APIs must retain their returned checks;
discarding them or writing directly through raw `Store` is outside the
cooperating host trust boundary.
A separate per-task durable marker protocol now fences the task record and
signal revision at start, and makes public task signal delivery and task wake
transactions check for marker absence. The scheduler leaves an affected waiting
task parked and periodically retries after marker retirement. The start and
retirement helpers still require the owning claim and graph checks; they are
not connected to callback dispatch, and a root marker alone does not cover a
nested task graph.

Direct abort transactions now check marker absence for their discovered tasks
and conversations; an existing marker makes abort return busy without writes,
and a concurrent marker insertion forces transaction conflict and rescan.
Deferred task cleanup retries this busy condition. The synchronous abort and
signal APIs still expose busy to the caller. The async `abort_and_wait` paths,
`Inbox::send_with_retry`, `Scheduler::send_signal_with_retry`, and the task
`Driver` signal retry methods yield and retry marker busy; signal retries take
cancellation tokens. Existing synchronous calls do not automatically retry.
Generic public task save and update batches now also check marker absence;
known pre-transaction conflicts defer reservation instead of poisoning the
driver. The async `abort_and_wait` paths discover and fence only the reached
conversation graph, repeat discovery under the task gate at commit, and yield
and retry on busy. Explicit cancellation-token variants stop the wait; if
cancellation occurs after the abort commit, it does not roll back that commit. The synchronous direct abort API still takes the legacy
global poll fence; full graph marker coverage and the composed callback start
transaction remain unconnected. Callback dispatch remains disabled until
every boundary and retry path is complete.

The pre-dispatch conversation cancel claim transaction checks the root task
signal revision and conversation signal revision and stores a versioned sidecar
with those observations. The separate start-marker transaction rechecks that
sidecar, the live claim, attempt and identity lease, conversation and selection,
park proof, late outcome, and child graph; it commits the conversation and task
markers together. A public Inbox or task signal write after the claim makes the
start transaction fail closed. Claims created by an older version without the
sidecar require reconciliation and cannot create start markers.

A test-only driver route exercises marker creation without polling a callback.
An actual process-kill test reopens the Store and verifies that both markers
remain equal and continue to block the corresponding Inbox and task signal.
It does not establish callback execution or recovery after dispatch.

The start markers are evidence and gate cooperating writes; they are not an
execution claim. Their creation and retirement must not be used to dispatch or
re-dispatch a callback. Callback dispatch and the at-most-once first-poll
protocol remain disabled, and direct raw Store modifications remain outside
the cooperating host trust boundary.

The `Scheduler::schedule` worker also fences its ModelReady-to-ModelRunning
save before model dispatch. Busy leaves the durable state unchanged and retries
through the scheduled worker. This does not yet fence the general Harness save
path, post-effect result saves, signal delivery, or blocking abort.

A safe implementation needs all of the following before dispatch is enabled:

* Bind the callback identity, retry policy, Tool identity and selected hook
  registration into an explicitly opted-in conversation task definition. A
  host identity lease must cover mutable Tool and callback configuration through
  the callback. Busy must defer; changed identity must fail closed.
* Validate the exact TaskPending root binding and tool attempt, each task owner
  and abort generation, live cleanup attempt and lease, and current selected
  hook registration. Include the relevant version checks in claim, immediately
  before dispatch, callback receipt and final conversation settlement. Define
  separate checks for explicit conversation abort and interrupted-wake
  recovery; an abort-marked graph cannot use ordinary admission checks.
* `Scheduler::attach` calls reconciliation before `bind_harness`. The unbound
  scheduler now defers eligible cancellation roots and their owned tasks from
  recovery, wait polling and reservation. Binding requests re-evaluation; this
  does not yet authorize conversation cancellation or supply conversation proof
  to the descendant-abort transaction. Busy proof acquisition still needs a
  bounded automatic retry without starving unrelated tasks.
* Preserve the original park proof and prepared arguments, woken intent, joined
  result and late receipt independently. A callback claim is not proof of its
  completion; non-idempotent claims cannot be replayed after interruption.
  An uncertain Tool outcome must remain visible even if cleanup returns.
* Do not hold a blocking extension-selection or scheduler lock while calling
  user code. Define how selected-hook changes are fenced across callback start
  and return, including reentrant host updates, before enabling hooked cleanup.

Required real-driver coverage includes parked abort and woken interruption,
restart during claim and callback, child-drain ordering across multiple levels,
late receipt races, replaced task/conversation/selection/attempt evidence,
identity busy and reentrant update, callback failure and retry authorization,
and process kill/reopen. Until these paths are implemented and verified, the
existing standalone-only guards and conversation fail-closed behavior remain.

The provisional nested cleanup conversation check walks and version-fences the
exact task ancestry, then binds it to the pending root, selected registration,
conversation snapshot and root capability restrictions. It is evidence for a
future child claim transaction, not authorization to run a callback. The
cleanup attempt and identity lease, park, signal revisions, claim, first-poll
markers, callback receipt and parent settlement still require their own
composed checks. Child callback dispatch remains disabled.

A separate provisional nested-claim ancestry sidecar captures the full
child-to-root chain and each task's version and signal revision. It can be
composed with a claim and later checked while creating the authenticated start
markers, so a signal or owner change between the claim and first poll causes a
conflict. Its reopen and ancestor-signal race are tested, but neither the claim
nor dispatch calls it yet; the sidecar alone does not authorize a callback.

An experimental, test-only nested path now composes the child claim with the
ancestry sidecar, creates the full marker chain, polls an unhooked cancellation
callback once under the conversation start lease, and saves its receipt while
retiring the markers in one checked commit. Real-driver tests reject an
ancestor signal or late Tool receipt arriving between claim and marker creation;
a process killed during callback leaves its claim and ancestor markers durable.
On real-driver reopen, the original interrupted cleanup is failed for
reconciliation and its authenticated markers are retired atomically; the
callback is not rerun. This path is not enabled in production. In particular,
parent settlement does not yet consume a successful child. A test-only path
commits the cancelled child and its terminal evidence together; a late Tool
receipt arriving at settlement forces reconciliation. A selected-child test
also pins selection through first poll and runs the replay-safe after hook with
fenced memo before its version-2 receipt. These paths are not enabled in
production.

The provisional nested-success reader checks the claim and ancestry sidecar
were co-committed, marker creation preceded the callback receipt, and the
receipt preceded the atomic task and terminal-evidence commit. It checks the
park and late-outcome fence, registration identity, marker retirement and
current ancestry, and returns a host identity lease plus store checks. A
separate Harness check binds an already-cancelled child to the current root and
selection. Neither check is wired into parent settlement; a caller must hold
the selection gate and the returned identity lease through the parent commit.
