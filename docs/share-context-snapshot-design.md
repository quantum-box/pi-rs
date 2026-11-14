# Durable standalone share context: investigation

Pi v1.5.0 exports `session.state.systemPrompt` and `session.state.tools` from
an already running session. A standalone pi-rs invocation has no such runtime.
A durable snapshot can represent the last cleanly closed live session; it
cannot by itself prove what a newly configured runtime would expose now.

## Evidence

- `SessionSnapshot::track_share_event` changes the effective prompt and tool
  schemas when model context changes without requiring a new journal record.
  `settle_share_context` also switches between active and idle context. The
  `equal_session_head_does_not_identify_live_share_context` test demonstrates
  different prompts and schemas at the same session ID and head.
- `snapshot_for_share` already rejects a changed worker revision and compares
  all retained journal records before accepting a newer disk snapshot. A
  persisted version needs equivalent validation; `(session ID, head)` alone
  can identify neither a unique journal nor a unique runtime context.
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
   A metadata-free upload is a safe failure fallback, not a replacement for
   standalone metadata. The current unbounded reconstruction is not a bounded
   fallback and must not be described as one.

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
authenticated envelopes,
and read-only legacy misses. macOS extended ACLs are rejected;
unverified Unix ACL semantics fail closed. Tests include a killed process,
concurrent generations, old format, symlink substitution, and access modes.

The prototype is not connected to live sessions or standalone sharing. The
caller must still provide and revalidate complete provenance, Windows storage
is not implemented, the protection must receive independent review, and
a safe legacy reconstruction path remains unresolved. In particular, a
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
The existing `worker_teardown_waits_for_refresh_barrier_after_grace`,
`rotating_refresh_holds_shared_store_lock_until_persisted`, and
`cancelled_refresh_survives_runtime_shutdown_and_persists_rotation` tests passed;
they establish that safe existing teardown waits for refresh rather than that
it meets a hard deadline.
