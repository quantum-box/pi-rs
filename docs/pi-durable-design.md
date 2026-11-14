# Pi Durable: independent foundation for pi-rs

Status: proposed design, not a description of implemented Durable support. The
initial implementation must coexist with the existing pi-rs session engine. The
comparison below was checked against upstream Pi commit
[`827383a15262905f82e418483825a49dbb1adbf5`](https://github.com/earendil-works/pi/tree/827383a15262905f82e418483825a49dbb1adbf5/packages/durable)
and pi-rs commit `a642cef`. Recheck upstream before implementing each phase.

## Implementation checkpoint

The experimental `pi-rs-durable::runtime::Harness` now provides a
local scheduler, durable input admission, model and tool calls, and recovery on
open. Interrupted model requests are settled unconfirmed before retry. Tool
replay requires both recorded and current safe policies; unsafe tools use the
upstream-compatible interruption result by default, with opt-in blocking
reconciliation. Model responses, tool intents, and tool results are saved
before the next effect. Cancellation or a dropped scheduling future poisons
the open harness until it is reopened.

`Scheduler` drives conversations concurrently in the background. Current-state
fork, explicit compaction, durable snapshot observation, and one-pass polling
of accepted external jobs are supported. Background workers repoll pending jobs,
and nested calls are journaled with parent-owned identities. This is a subset of the design below,
not complete upstream parity: tool-output progress is not automatically wired into the runtime,
and entry-ID forks, full observer semantics, and distributed storage remain incomplete.
The current capabilities and limits are documented in `crates/durable/README.md`. Restricted task-owned children can atomically commit an initial tool
allowlist with their first input. Named background children can opt into a
reservation-bound initial allowlist before submission, including through the
parent-bound management extension. These controls remain pi-rs host APIs, not
the complete upstream task and subagent extension semantics.

The experimental `tool_progress` API provides opt-in, bounded replacement
partials and a scoped `ToolHost` adapter. Hosts must explicitly connect a sink,
commit snapshots with their own lease checks, and fence completion or abort in
their terminal transaction. A reopened store can inspect the last committed
partial; it is display-only and is not a result or proof of replay safety.
The scheduler, TUI, and observer log do not automatically publish these updates.
There is no upstream-style publication cadence or automatic checkpoint timer;
uncommitted updates may be lost. Callbacks already in flight when a scope closes
must be fenced by the host transaction.

## Scope and compatibility

Upstream `packages/durable` is a harness for durable conversations, submissions,
tasks and documents, not simply the coding agent's JSONL transcript. Its
[specification](https://github.com/earendil-works/pi/blob/827383a15262905f82e418483825a49dbb1adbf5/packages/durable/docs/spec.md)
defines a session mutation line and atomic commits across records and documents;
the [harness](https://github.com/earendil-works/pi/blob/827383a15262905f82e418483825a49dbb1adbf5/packages/durable/README.md)
schedules recoverable work. SQLite, JSONL, IndexedDB and other storage adapters
exist upstream. This proposal targets observable semantics, not binary, API or
on-disk compatibility with any of those adapters.

| Concern | Upstream Durable at the reference revision | pi-rs today / proposed difference |
| --- | --- | --- |
| Commit | A session commit atomically updates entries, task records and documents. Stored commits publish in line order after durability; explicitly deferred document-only commits can be visible before storage and lost on crash. | `SessionStore::append` stores one journal record; `FileStore` writes newline-delimited JSON and syncs it. It is not a multi-record transaction. New Durable storage will use atomic batches and initially **no deferred visibility**. |
| Recovery | `resume()` schedules persisted work. Tool intent precedes execution; a cut-off call reruns only when both stored and current tool registrations say `replay: "safe"`; otherwise upstream settles it as interrupted. Restart limits bound crash loops. | Existing journal distinguishes planned, started, pending, unknown and finished operations. Uncertain dispatched effects block progress until reconciliation. Keep that conservative behavior in the existing engine. Durable defaults to upstream interruption semantics, with an explicit host policy requiring reconciliation for uncertain effects. |
| Nested calls | Durable tasks are owned; a replay-safe parent can recover nested calls by position or explicit key. An interrupted child follows its own replay policy; external idempotency is still the adapter's responsibility. | Existing operation and nested-call journal behavior is not a general durable task graph. The standalone nested host provides caller-scoped keyed identities and tests reordered replay; core tool APIs do not expose them, and replay and ownership remain more restrictive than upstream. |
| Admission | Durable has submissions, request IDs, settlement, and durable waiting/sleeping tasks. | Existing conversation input is journal based, not a Durable submission API. Implement these as new APIs; translate only where semantics are established. |
| Concurrency | One Session mutation line serializes callbacks, with ordered publication and storage batching. | Existing local journal takes an exclusive file lock. Proposed Durable store has one exclusive writer per store lifetime, including across processes; no distributed writer or multi-host failover in the initial backend. |
| User-facing coding agent | Durable is a separate upstream package; coding-agent behavior and session formats remain their own compatibility target. | Existing CLI, RPC, TUI, JSONL import/export and branch semantics remain supported by their existing engine. Durable is opt-in; neither existing journals nor external effects are silently migrated. |

Existing behavior is documented in [extensions.md](extensions.md) and implemented
in [journal.rs](../crates/core/src/journal.rs) and
[agent.rs](../crates/core/src/agent.rs). In particular, a complete JSONL record
is the existing recovery unit; an incomplete tail after a valid header is
truncated, while malformed committed records fail closed. This does not make a
sequence of records atomic. Existing `operation_id` is useful as an adapter
correlation key, but passing it does not prove that a destination deduplicates.

## Architecture and ownership

Introduce a separate Durable module/crate, with versioned types independent of
`SessionStore`: `Conversation`, immutable `Entry`, `Submission`, `Task`,
versioned `Document`, and `CommitSeq`. A session owns their identity space and
one serialized mutation queue. A transaction reads a consistent committed
revision, stages a write set, validates ownership, unique IDs, references and
state transitions, then writes the entire set in one storage transaction. Each
successful batch receives a monotonically increasing sequence. In-memory state,
notifications and acknowledgements follow durable commit in sequence order.
Transaction closures may use transaction operations only: no network, tool,
model or arbitrary callback while holding the mutation line. No nested commit.

Start with a local SQLite backend and an exclusive, OS-enforced lifetime lock
on a stable lock file adjacent to the database, acquired **before** opening or
recovering the store and retained through close. SQLite's own write transaction
serializes database changes; it is not a substitute for the lifetime owner
lock. All writers must follow this protocol. The supported deployment is a
single machine and local filesystem with working locking and durable SQLite
sync; network filesystems, copied/live databases, and noncooperating writers
are unsupported. Open through a canonical store identity, reject aliases that
could bypass the lock (including hard links), and never replace the database or
lock file while open. No automatic stale-lock deletion: OS releases a lock on
process death. Any future distributed backend needs a transactional lease with
monotonic fencing checked on every write, including after pauses; a lease alone
is insufficient. This is a new contract, not an assertion about upstream's
cross-process locking.

Use SQLite transactions to include records, task checkpoint, document updates,
submission settlement and sequence allocation in one unit. Configure and
verify durable sync settings; define backup and restore together with WAL and
locking, rather than copying an active database file. Schema/version migration
is transactional and fails closed on an unknown newer version. No implicit
conversion of existing journals: a later explicit importer must retain original
IDs/branch ancestry in a mapping, validate before commit, and document any
unrepresentable records. The existing journal is never dual-written as part of
an alleged atomic commit. A UI projection or export is derived after commit and
can be rebuilt.

## State machines and invariants

1. A submission is admitted atomically with its request identity and initial
   task. `(session, conversation, request_id)` identifies one logical request;
   the same ID and submission type returns the same receipt even when the
   resent content differs, matching upstream. A different type is a conflict. IDs are not reused, including after restart. Admission and
   settlement are distinct; cancelling a client wait does not retract work.
2. A task's persisted state is `ready`, `running` with phase and attempt,
   `waiting` (deadline, signals or child IDs), `succeeded`, `failed`, or
   `cancelled`. State transitions, ownership and child creation are committed
   atomically. A scheduler only executes committed runnable states. Durable
   deadlines use wall time; on restart due deadlines are scheduled, and signal
   delivery/consumption is atomic and deduplicated. Missing task definitions or
   incompatible versions remain blocked with an actionable error, never lost.
3. Every externally visible durable change has one commit sequence; readers see
   either all or none of a batch, in order. No observer is told success before
   durable storage. Transient model/tool streaming is explicitly provisional,
   may disappear on restart, and cannot trigger further durable work. This
   deliberately excludes upstream's deferred document visibility initially.
4. A write, sync or ambiguous commit error poisons the open session: stop
   scheduling, publish no unconfirmed state, release resources and reopen.
   On reopen inspect durable state before deciding whether the batch committed;
   do not assume an error means rollback. A durable but unacknowledged commit
   is recovered by its stable request/operation identity. Never retry a batch
   using new identities merely because its acknowledgement was lost.
5. Forks retain immutable ancestry and snapshot document versions in one commit;
   they do not roll back effects in the outside world. Branch-specific state
   cannot change the identity or evidence of a previously dispatched effect.
   Retention/checkpointing must preserve every live task dependency and the
   evidence needed for deduplication and reconciliation.
6. There is at most one active writer per store. Failed lock acquisition means
   no migration, recovery, scheduling or write. Closing releases the lock only
   after scheduler and writes have stopped. Readers must observe committed
   snapshots; stale handles cannot keep writing after close or ownership loss.

## External effects and crash recovery

Use a durable intent / effect / outcome protocol. Commit the invocation's
stable operation ID, canonical arguments, destination scope and policy before
dispatch. Execute outside the transaction; commit the outcome and all related
task/submission changes atomically afterwards. The operation ID is stable over
retries and reopen, scoped by tenant/destination where appropriate. Store no
credentials in task state. Cancellation is itself an external request, not
proof that a remote job stopped.

| Crash point | Recovery rule |
| --- | --- |
| Before intent commit | No dispatch is permitted. Retry admission with its original request ID. |
| Intent committed, dispatch not known to have begun | Unless non-dispatch is durably proven, treat the operation as possibly executed; do not infer safety from absence of a result. |
| During dispatch, or remote success before outcome commit | Unsafe effects are never automatically repeated. Upstream-compatible policy records an interruption; the optional reconciliation policy blocks until an authoritative resolution. |
| Outcome commit durable, acknowledgement lost | Reopen, find the committed outcome by operation ID, return it; do not dispatch again. |
| Remote job accepted, completion outstanding | Persist its stable reference and pending status; resume read-only polling with bounded backoff. A timeout or ambiguous response remains pending/unknown, not a definitive failure. |

A known successful or failed outcome may be committed only with adequate
adapter evidence. Reconciliation records evidence, actor/source, operation ID
and resolution in an auditable atomic transition; conflicting or repeated
resolutions must not create another effect. Under the reconciliation policy, resume model work only after all
blocking operations are resolved. An authoritative not-found result permits a
new attempt only when the destination contract proves non-execution and the
host explicitly allows it.

Future opt-in replay of a safe tool must persist the policy at dispatch and
recheck the current registration, as upstream does, with bounded restarts.
A read may be replay-safe only if its changing result is acceptable. For a
write, the adapter must demonstrate destination-side atomic deduplication of a
stable key, identical payload binding, appropriate scope and retention covering
the full retry/recovery window. A lookup key or a local journal alone is not
idempotency; expiration, lost responses and non-idempotent hooks must be
considered. Without that contract, remain unknown and reconcile. Even with it,
this provides at-least-once attempts with destination deduplication, not a
general exactly-once guarantee across the database and external service.

For nested calls, `(parent task ID, explicit child key)` is the durable identity;
store the tool name and canonical argument digest and reject reuse with different
arguments. Reopening returns an existing child or result, not another dispatch.
Position-derived keys are only allowed for deterministic call sequences; an
explicit key is required for dynamic ordering. Parent cancellation and child
settlement must not erase uncertain child effects. Model requests are external
and may be repeated after a lost response, with potential duplicate cost or
nondeterministic text; only committed tool intents may cause dispatch.

## Phases and acceptance gates

Each gate includes automated tests against the real storage backend and a
fresh-process reopen, not only mocks. Fault injection must cover process death
and storage errors at the listed boundaries. Compatibility claims and parity
documentation are updated only when the corresponding gate passes.

**Phase 0 — contract and isolation.** Define versioned interfaces and an
opt-in entry point. Existing CLI/session tests pass unchanged; opening a legacy
JSONL as a Durable database fails without changing it. Test unknown versions,
invalid transitions and same request ID with identical versus different input.

**Phase 1 — store and single writer.** In one transaction create a conversation,
entry, task, submission and document; inject failure before commit and verify
none exist, then kill after commit before acknowledgement and verify all exist
once on reopen. Inject commit/sync errors and verify poisoning and recovery.
Check ordered sequence/publication, immutable IDs and callback rollback. Start
two independent processes on the same path and supported aliases: exactly one
acquires ownership, the loser performs no writes; kill the owner and verify a
new process acquires it and sees a consistent store. Test close, hard-link and
unsupported-path handling, migration rollback and WAL-aware backup/restore.

**Phase 2 — scheduler and submissions.** Crash after admission and in every
task transition; reopen and verify one logical submission, no lost tasks and
one terminal settlement. Verify concurrent duplicate admissions, same-ID different-content reuse,
different-type conflicts, idempotent `resume`, timer already due after downtime, duplicate
signals, child wait/ownership, cancellation, missing definitions and bounded
crash loops. Confirm provisional output is never presented as committed after
restart.

**Phase 3 — effects and nested calls.** With a fake destination that records
keys and can lose replies, kill before intent, after intent, during dispatch,
after remote success and after outcome commit; assert the table above, including
no blind retry, no duplicate external effect, and blocked model continuation
until reconciliation. Test pending polling, ambiguous cancellation, conflicting
reconciliation, destination key expiry and changed payload. Test duplicate
nested keys, argument mismatch, parent restart and interrupted child. Explicitly
verify a replay-safe opt-in only after both policies and destination guarantees
are checked; a changed registration must not permit replay.

**Phase 4 — compatibility and integration.** Run common upstream behavioral
fixtures for submissions, forks, task ownership, waits and tool interruption;
record the optional reconciliation policy and initial no-deferred-visibility
differences in parity docs. Exercise CLI/RPC/TUI only after their mapping to
Durable is specified, including restart while streaming, branch selection and
errors. Run existing journal and extension suites and verify no implicit import,
dual write or changed legacy recovery. Load-test ordered commits and restart
with many pending jobs; performance does not relax atomicity or ownership.
