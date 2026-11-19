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

Unhooked unsafe conversation Tools may register a park-cancel handler using
`register_conversation_unsafe_durable_tool_with_park_cancel`. The host supplies
an identity that must change with handler implementation or configuration;
that identity is included in the persisted task definition. Reopening a pending
call with a different identity is rejected. This registration retains the
handler but does not dispatch it or enable cancellation. Hooked registrations
and callback retry policy are not included in this initial registration API.

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
