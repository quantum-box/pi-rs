# Durable standalone share context: investigation

Pi v1.5.0 exports `session.state.systemPrompt` and `session.state.tools` from
an already running session. A standalone pi-rs invocation has no such runtime.
A durable snapshot can represent the last cleanly closed live session; it
cannot by itself prove what a newly configured runtime would expose now.

At pinned Pi v1.5.0 commit `f9a26365568127461c47a07a37dcb42c140a3ac5`,
the `/share` handler passes its own interactive `this.session` to
`shareSession`, and `createShareTrailingEntries` reads that session's state.
Pi's CLI argument parser treats a positional `share` as a message; the pinned
source has no standalone `share` subcommand or cross-process worker lookup.
Standalone `pi share`, context reconstruction and any cross-process discovery
are pi-rs extensions, not requirements established by this Pi behavior. They
must still honor pi-rs's explicit safety and cancellation requirements; the
absence of an upstream feature does not make stale metadata safe.

## Evidence

- `SessionSnapshot::track_share_event` changes the effective prompt and tool
  schemas when the selected runtime changes without requiring a new journal
  record. Request-only context transforms are excluded from sharing, as Pi's
  `session.state` exports the session state rather than the transformed request. The
  `equal_session_head_does_not_identify_live_share_context` test demonstrates
  different prompts and schemas at the same session ID and head.
- `snapshot_for_share` already rejects a changed worker revision and compares
  all retained journal records before accepting a newer disk snapshot. The
  capture path now returns the exact validated in-memory journal with its
  context. Focused tests exercise capture-to-store and a reopened-cache-to-Radius
  upload with a fixture provenance. The live worker and standalone CLI do not
  yet publish or read it. A persisted
  version needs equivalent validation; `(session ID, head)` alone
  can identify neither a unique journal nor a unique runtime context.
- The live interactive worker response now includes an HMAC over its exact
  captured journal, prompt and tool schemas under a one-request key held by the
  frontend. The Radius upload boundary verifies the whole authenticated payload,
  rejecting same-ID/same-head content replacement and context tampering. The worker rechecks its context revision after computing
  the tag, rejecting changes during preparation. The key and tag do not establish
  that dynamic MCP or extension context remained unchanged after the response,
  and do not connect the
  standalone CLI to a live worker or durable cache.
- The existing private stdin/stdout worker control protocol can optionally
  return the exact captured journal alongside its context and keyed journal
  tag. The response requires a caller-supplied key and is size-limited; legacy
  requests retain their original shape. This is available only on an already
  connected worker control channel. A caller-supplied journal key verifies
  pairing but does not authenticate an independently discovered endpoint or
  prove registration coverage.
- Pi does not write the trailing share entry to its source session. Persisting
  a prompt and schemas therefore adds a new at-rest copy that may contain
  secrets and must be treated like private session data.
- The existing `process::configuration` fingerprint contains prompt paths,
  not file contents. `worker_configuration_does_not_capture_prompt_file_contents`
  confirms it cannot establish cache freshness. The core protected-model
  fingerprint supplies keyed integrity, but its key helper may create and sync
  files; it is not automatically safe inside a deadline-bound read-only lookup.
- The standalone CLI can select different tools, explicit extensions,
  settings, workspace, model, or trust from the previous live run. MCP servers
  and extension hooks can change tools or prompt at the same journal head, with
  no offline way to ask what they currently expose without executing them.
- Existing refresh barriers keep rotating credentials safe by waiting for the
  refresh to finish. They do not make worker termination bounded; package and
  MCP subprocesses additionally have their own process groups.

## Candidate protocol

1. A live worker creates a per-session dirty generation *before* starting
   extensions or connecting servers. Today `run_worker` calls `configure`
   before creating its session snapshot, so invalidating only at the current
   snapshot initialization point would be too late. A crash, concurrent writer, unavailable
   private storage, or interrupted write leaves it dirty. Only a clean,
   quiescent shutdown can publish a clean snapshot for that generation. If a
   runtime can proceed after failing to invalidate a previously published cache
   (for example a read-only or failing filesystem), a later reader can mistake
   the old generation for current. Integration must resolve this explicitly;
   ignoring invalidation errors is unsafe.
2. Immediately before session shutdown, capture the idle share context under
   the existing revision/idle validation. Bind it to a format version, the
   canonical session identity, session ID, head, and a cryptographic digest of
   the complete validated journal. Do not publish an active request, summary,
   pending context, managed session, or untrusted project snapshot.
3. Persist outside the workspace, in an account-private directory using
   atomic replacement and platform-appropriate protection against symlinks,
   reparse points, concurrent writers, and disclosure. A keyed integrity check
   backed by a private installation key is needed if an untrusted workspace can
   supply or alter journal data. Do not write prompts, schemas, credentials, or
   fingerprints containing secrets to logs. Bound cache size and retention and
   clean it when a session is removed. Do not assume a Windows inherited ACL is
   private; fail closed unless the owner-only ACL can be established.
4. On standalone share, validate the clean generation, session identity,
   journal digest, trust, and provenance before using it. Bind any settings and
   explicit command-line choices that affect prompt or tool selection. A
   head-bound snapshot must be described as the *last clean live context*, not
   evidence that an MCP server or extension would return the same state today.
   Explicit share options that alter context must invalidate or bypass it.
5. For legacy sessions or mismatches, retain a clearly identified fallback.
   Full parity needs a separately supervised reconstruction process with an
   independently owned credential-refresh broker: cancellation must terminate
   all reconstruction descendants without killing a refresh after it has sent a
   rotating-token request, and no new refresh may start after cancellation.
   A metadata-free upload is permissible only when no live context could have
   been established; it is not safe after an extension context may have been
   applied. The current unbounded reconstruction is not a bounded fallback and
   must not be described as one.

For an interactive frontend, a worker announces context initialization after
configuration and before lifecycle hooks. The frontend retains this observation for its own
lifetime and refuses metadata-free fallback after initialization, a failed
response, or a timeout. It may preserve the legacy fallback when a worker exits
normally with an explicit pre-initialization failure marker, such as when model
credentials are missing; a crash or lost output is not such proof. This
process-wide signal is intentionally conservative across session switches; a
replaced worker with an unobserved outcome also disables fallback. It does not
authenticate a worker across frontend restarts and
does not establish provenance for standalone sharing.

A standalone CLI fixture places a clean, deliberately stale cache beside a
session and checks that `pi share` reconstructs the current prompt, uploads
that metadata to a loopback Radius endpoint, and leaves the source journal
unchanged. This exercises the current cache-independent path; it does not make
reconstruction deadline-bounded or establish durable cache provenance.

## Failure-fence constraint

Suppose a clean snapshot is present, then a worker starts while the cache is
read-only. An extension or MCP server can change the live context without
changing the journal. If the worker is allowed to proceed and cannot record an
invalidation elsewhere, restoring cache access leaves exactly the same durable
bytes as the case where that worker never ran. An offline reader cannot
distinguish those cases. Ignoring the error and later accepting the old entry
is therefore unsafe even if its journal and static configuration hashes match.

A usable cache needs an independently durable invalidation signal whose
failure behavior still preserves ordinary worker availability, or a conservative
policy that declines the cache whenever such a signal cannot be established.
A writable journal, OS credential service, or supervisor is not universally
available and must not be assumed to solve this without a verified protocol.
The `failed_optional_invalidation_can_leave_previous_snapshot_readable` test
records the counterexample. Until this constraint is resolved, normal worker
startup does not touch the experimental store and standalone cache reads remain
disabled.

## Live-attested lookup candidate

An alternative to an authoritative durable snapshot is an authenticated,
per-session live-worker endpoint. A standalone caller would accept metadata
only after the currently running worker proves possession of a per-generation
secret and returns its current context with the one-request journal tag. A
stale registry file by itself must never authorize a cache hit: connection
failure, expired ownership, a different session, failed authentication, changed
worker revision, or journal mismatch must cause reconstruction. After the
worker exits, its durable snapshot alone remains insufficient evidence.

A viable implementation still needs an owner-private registry, authenticated
local IPC on Unix and Windows, bounded connection and request handling,
crash-safe endpoint replacement, concurrent-worker arbitration, and protection
against path aliases and reparse/symlink substitution. The registry must not
become a new startup dependency when its storage is unavailable. Capturing a
live response and releasing the worker before uploading also leaves a window
for dynamic context changes; a bounded lease or explicit capture-time semantics
must resolve that window. This candidate is not implemented. The existing
frontend-to-child protocol and per-request journal tag provide part of the
verification, but do not make that worker discoverable by another process.

For sessions with no attested live worker, the CLI must reconstruct metadata.
The current reconstruction has no deadline. An authenticated live endpoint does
not solve its cancellation, subprocess containment, or in-flight OAuth
persistence requirements.

## Proposed identity and live-discovery protocol

This section is a design, not a claim that the current store implements it.
Pi's interactive exporter reads the running session at export time; it does
not authorize an older persistent record merely because the journal head is
unchanged. A standalone reader therefore needs evidence about both the opened
journal and the worker whose effective context it uses.

### Journal identity and invalidation

Open the journal first and retain the handle through identity checks and the
capture. Obtain the filesystem identity from that handle: device and inode on
supported Unix filesystems, and volume serial plus file ID on Windows. Obtain
the Windows identity from a handle opened with appropriate reparse-point and
sharing checks, not from a normalized pathname. Reject non-regular files and
platforms or filesystems where these guarantees cannot be established. The
identity is only an incarnation identifier while the handle remains open;
inode/file-ID reuse after close is possible and must not authenticate an old
record by itself. Bind the session UUID and a digest of every complete journal
record as well as the filesystem identity. Compare the identity and full
journal again on the handle used for export; if a pathname is part of the
request, reopen it and verify it still denotes that same incarnation.

Path replacement and hard-link aliases require two independent invalidation
domains: a path-incarnation domain for replacement at the requested name and a
filesystem-identity domain shared by aliases. Acquire their locks in a stable
total order, validate the handle after acquiring them, and publish or
invalidate both before a participating worker can expose a context. Readers
must validate both domains under the same coordination, including after a
concurrent rename, and reject any missing, dirty, inconsistent or unverified
generation. Do not infer that a journal had no alias from its current link
count: the hard-link regression removes the alias before the stale read.
Define platform-specific canonical-path rules separately; text path
normalization alone does not establish file identity.

These domains help only while every relevant writer participates and can
durably invalidate. An old version, another process, a failed invalidation or
a rollback of all durable anchors is not made safe by the identity key. A
process that cannot establish the required invalidation must remain able to
start, but its existence makes an offline cache hit unprovable. Therefore a
durable cache remains an optimization and is never authoritative without an
independent live attestation or a verifiable failure fence. Expiry, a clean
marker, file identity, the journal digest, and a successful lock probe are not
substitutes for that attestation.

### Worker discovery when the agent directory is read-only

Use a separately available, owner-private local rendezvous service when one can
be established; a worker must not depend on it for normal startup. The service
must bind registrations to a live worker instance and an opened journal
identity, arbitrate multiple workers for the same identity, authenticate both
registration and lookup, and revoke registrations on process death or session
switch. A stale file or an endpoint chosen solely by a predictable pathname or
PID cannot establish ownership. Unix peer credentials or a Windows named-pipe
DACL can restrict the account, but by themselves do not prove that an endpoint
belongs to the worker holding the relevant session; a supervisor-issued,
unforgeable instance capability and lifecycle/ownership protocol are also
needed. Any registry or IPC capability must be private, bounded, replay-safe,
and protected against symlink, reparse-point, endpoint-replacement and
cross-user attacks. Specify and test the Unix and Windows mechanisms before
enabling this path; do not assume a writable runtime directory exists.
The supervisor must also prove registration coverage for the scope in which it
claims a unique current worker. A responsive registered worker alone does not
exclude another worker that could not register, or an older version that does
not participate. If coverage cannot be established, an explicit, authenticated
worker selection may identify that worker's capture but cannot claim to be the
unique current context for the journal. An automatic lookup must report
ambiguity or use safe reconstruction instead of accepting an arbitrary match.

A lookup is eligible only for an authenticated, currently registered worker
whose held journal identity, session UUID and complete journal match the
standalone request, and whose effective configuration is compatible with
explicit share options and trust. The caller supplies a fresh unpredictable
challenge. Bind the worker instance, challenge, journal identity, session UUID,
head, complete journal digest, context and worker revision in the authenticated
response; reject reuse, mismatch, malformed data and late responses. Recheck
the worker revision and journal at capture as in the existing live frontend
path. The context represents the state at that successful capture, matching
Pi's export-time semantics; it is not a guarantee that dynamic MCP or
extension state remains unchanged during the subsequent upload. If stronger
semantics are required, specify a bounded lease and its failure behavior rather
than treating an earlier response as current indefinitely.

The standalone CLI now opens the journal for share without recovery writes;
it still holds an exclusive writer lock and conflicts with a live worker holding
the journal.
Changing it to an unlocked snapshot alone is not sufficient: the authenticated
worker capture and the final exported journal must still be paired and
revalidated. The existing core snapshot-with-writer-lock test is Unix-only;
the workbench's Windows integration notes that a running agent can prevent
reading the locked journal. A Windows IPC path must therefore test real active
writer behavior and may need the worker to return the authenticated journal
alongside its context instead of relying on an independently opened file.

One possible bootstrap is a long-lived per-user supervisor started independently
of the agent directory, with a private inherited channel for each worker and
another authenticated local endpoint for standalone clients. Capabilities must
travel over that channel rather than command arguments or logs. On Windows the
client endpoint could be an owner-restricted named pipe; on Linux an abstract
Unix socket avoids a filesystem pathname, but still needs authentication and
protection against name squatting. macOS has no Linux abstract-socket namespace;
an available, verified per-user service or private writable runtime location
would be needed there. These are candidates, not interchangeable guarantees.
An inherited anonymous pipe alone cannot let a later independent CLI discover
the supervisor. A supervisor also cannot guarantee coverage over processes it
did not launch or otherwise enroll. The protocol must expose that lack of
coverage as unknown, including when registration fails, instead of interpreting
an empty registry as proof that no worker exists.

When no trusted rendezvous is available, or when any worker or identity cannot
be authenticated, the reader cannot conclude that no worker exists and must
not use an old cache as live evidence. Use a separately supervised, safe
reconstruction where its containment and credential-rotation guarantees have
been established; otherwise return a clear failure rather than silently upload
stale context. The interactive legacy case in which a worker explicitly
proves it failed before context initialization remains a separate case. No
generic deadline may be asserted until the broker and descendant-containment
protocol described below is implemented.

### Verification matrix

- Same inode through two hard links, including alias removal before lookup;
  same pathname replaced with a different inode; rename and concurrent
  replacement during capture; inode/file-ID reuse after the original handle
  closes. Check both identity and path domains without relying on link count.
- Two workers registered through aliases, simultaneous registrations, worker
  crash, exit and session switch, stale registry and endpoint replacement;
  include one registered worker and one unregistered or old-version worker;
  a reader must not silently choose an unverified worker or old generation.
- Read-only or unavailable agent and runtime directories, failed durable
  invalidation, an older non-participating worker and restored old cache/anchor:
  ordinary worker startup survives, but an unattested offline hit is refused.
- Wrong account, forged or replayed challenge, wrong instance, journal, head,
  context, configuration or trust; stalled or oversized IPC; changed journal or
  context during capture; connection loss and deadline races.
- Real Unix and Windows processes with their respective ownership and endpoint
  protections, dynamic MCP/extension changes, standalone Radius export, and
  cancellation concurrent with every provider's rotating-token persistence.

## Required tests before adoption

- Restart after a clean close preserves the exact effective prompt, tool schemas,
  order, session/head and journal bytes; active and summary contexts never win.
- Crash, aborted publication, concurrent workers and stale generation cannot
  publish or revive old data, including changes with an unchanged head.
- Alter journal content while preserving session ID/head, rename/copy/fork,
  change workspace, trust, settings, explicit options, extensions, prompt files,
  or tool selection: reject mismatched provenance rather than upload stale data.
- Exercise dynamic MCP tool changes, extension-provided resources, and model
  hooks. Decide and test last-live semantics rather than pretending to requery.
- Test file permissions, symlink/reparse substitution, atomic writes, size
  limits, deletion, private-data leakage, and behavior on Windows and Unix.
- Test legacy reconstruction with blocked package I/O, startup cancellation,
  process descendants and a token rotation whose response arrives after the
  metadata deadline. Prove no new effects start after cancellation and a
  started rotation is persisted exactly once.

An isolated storage prototype now exists in `private_share_store`: on Linux and
macOS it uses an owner-only directory and files, no-follow directory-relative
operations, generation locks, durable invalidation, atomic publication, exact
journal/provenance binding checks, keyed filenames and fingerprints,
authenticated envelopes, typed session/context round trips, and read-only
legacy misses. An authenticated per-session anchor outside the cache directory
rejects replay of an older snapshot and marker together. Restoring the anchor
as well still replays a valid old generation; the explicit regression test
records this unresolved boundary. macOS extended ACLs are rejected; unverified
Unix ACL semantics fail closed. Tests include a killed process,
concurrent generations, old format, symlink substitution, and access modes.

The prototype provides `begin_and_initialize`, which refuses to poll the
initializer if invalidation fails and keeps an unsuccessful initialization
dirty. A nonblocking generation acquisition rejects concurrent ownership. The
worker invalidation bridge is currently compiled only for tests. Normal worker
startup performs no added cache I/O, so an unavailable or stalled store cannot
change existing worker startup. The test bridge attempts to open an existing
store and invalidate before configuration and session switching, ignoring
errors to model the required availability fallback. Its failed-invalidation
regression shows that an old snapshot can remain readable after such a failure;
therefore this bridge cannot safely be enabled with cache reads or publication
until an independently durable failure fence is implemented. The bridge's
filesystem operations are synchronous and need isolation before activation.
A missing journal is left untouched. No live snapshot is published, and the
standalone CLI does not read one. Before
adding publication, the missing-store and missing-journal cases, journal pathname aliases and concurrent replacement still need a complete identity
protocol; cross-version workers that do not participate in invalidation are
also outside the guarantee. Generations and locks are now keyed by pathname rather than
session ID: the `replacing_journal_after_invalidation_cannot_revive_another_identity`
regression covers replacement by another identity at the same pathname. The
`hard_link_alias_can_bypass_path_keyed_invalidation` regression shows that a
worker using a hard-link alias leaves the original path's snapshot readable.
Symlinks and Windows case variants are also not yet proven to share this
boundary. On Unix, the opened parent descriptor is pinned, but the
full ancestor path is not held against replacement across later invocations;
replacing an ancestor can therefore direct a later lookup at different storage.
The caller must still provide and revalidate complete provenance. Windows DPAPI, owner-only DACLs, pinned ancestor paths, reparse rejection,
private files and hard-link rejection have passed focused Windows CI tests.
A Windows generation-store prototype now uses DPAPI-protected keys and records,
private anchor and cache directories, exclusive generation locks, and atomic
replacement. Focused Windows CI covers clean publication, invalidation and
replay of the snapshot and marker, and cross-process lock exclusion. An
initialization failure regression checks that the storage primitive refuses to
poll its initializer on invalidation failure; the worker deliberately does not
use that strict helper. Neither platform is yet connected end to end. DPAPI alone cannot prevent a stale
ciphertext or lock-file replay. The protection needs independent review, and a
safe legacy reconstruction path remains unresolved. In particular, a
session/head keyed sidecar would produce misleading or unsafe results.

## Legacy supervisor and credential broker candidate

A deadline-capable legacy path cannot simply kill the current worker. A
candidate splits responsibilities as follows:

1. A supervisor owns the deadline and a gate for new side effects. It gives a
   disposable reconstruction worker its validated configuration through a
   private, length-limited IPC channel. Credentials are never passed in command
   arguments or logs. The worker cannot directly start a token refresh: every
   refresh request goes to a separate broker.
2. The broker serializes cancellation and refresh admission. If the deadline
   closes the gate first, it rejects the request. If admission commits first,
   it owns the exchange and the existing credential-store lease through durable
   persistence, even if the reconstruction worker is stopped. Its acknowledgment
   distinguishes “not admitted” from “admitted and broker-owned”; a lost
   acknowledgment must not cause a second exchange with the same refresh token.
3. At the deadline the supervisor closes admission and stops/reaps the
   reconstruction worker and its descendants, returning metadata-free failure
   for that legacy request. A separately owned admitted rotation may continue
   solely to preserve its result. A late worker result is discarded. The broker
   must not share the worker's kill boundary or rely on a thread that dies with
   it. Conversely, it must not admit a queued refresh after the gate closes.
4. Unix `ProcessGroup::leader()` in both package and MCP paths currently makes
   nested groups; killing only the reconstruction worker's group therefore does
   not establish descendant cleanup. Windows job objects require equivalent
   nested-job verification. A process group cannot by itself constrain an
   arbitrary extension that deliberately detaches; either an enforceable
   platform containment mechanism or an explicit trusted-extension limitation is
   needed. The current check-then-spawn cancellation is also racy: a deterministic
   gate reproduces a child starting after its cancellation token is set.

This is a proposed protocol, not an implementation. It needs actual IPC,
private credential transfer, crash/acknowledgment recovery, platform-specific
containment, and provider/MCP integration before it can satisfy the deadline.
An unintegrated refresh-admission change races cancellation against token
exchange admission for ChatGPT, Kimi, xAI, Meta and Radius, while retaining the
existing lock through persistence after admission. Admission can precede the
actual network request: if cancellation loses that race, the request may start
after cancellation. MCP OAuth has a separate
transaction and worker barrier. Focused CI also exercises Meta mint persistence after caller cancellation;
admission alone does not move an in-flight exchange to another process, provide a credential broker, or
establish a standalone-share deadline.

The existing `worker_teardown_waits_for_refresh_barrier_after_grace`,
`rotating_refresh_holds_shared_store_lock_until_persisted`, and
`cancelled_refresh_survives_runtime_shutdown_and_persists_rotation` tests passed;
they establish that safe existing teardown waits for refresh rather than that
it meets a hard deadline.
