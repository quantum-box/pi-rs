# Conversation-owned external cancellation: integration notes

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

A nonblocking target-specific start lease can bracket exactly the first poll
of a future without holding registry, task or poll mutexes. It is a primitive
only: callback dispatch and most mutation paths do not yet use it. Existing
`try_fence` still acquires all registered start gates and can defer unrelated
graphs; blocking abort and signal delivery are not protected by this primitive.
The task wake and signal revision must be checked before dispatch is enabled.

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
