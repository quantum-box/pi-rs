# durable-store

A standalone synchronous byte-key/byte-value store for a durable runtime. Use
`Store::memory()` for ephemeral storage or `Store::open(path)` for SQLite.
Construct `Check { key, expected: Expected::Missing | Expected::Version(v) }`
and `Mutation::Put { key, value }` or `Mutation::Delete { key }`, then call
`commit(&checks, &mutations)`. `get` returns the value and version; `list`
returns a snapshot of all entries with a byte prefix, sorted by byte key.

Every check observes the state before any mutation. If a check fails,
`Error::Conflict { key }` identifies the first failed check and no mutation is
applied. Mutations are applied in order. Every put, including an overwrite,
allocates a new database-wide monotonically increasing version. Deletion does
not reset the counter. Versions are positive and limited to `i64::MAX`.
`get` after a successful commit can observe a later concurrent commit; it is
not a receipt for the committed version.

SQLite uses an immediate write transaction, WAL, and FULL synchronization. A
successful return means SQLite has committed, subject to the filesystem and
hardware honoring synchronization. A storage error during commit may leave its
outcome unknown; reopen and inspect before retrying a non-idempotent operation.
A store uses one internally locked SQLite connection and coordinates with other
connections to the same database. Both backends use the same transaction logic;
the memory backend has no persistence. This is a generic building block, not
the upstream durable record schema, deferred commits, or session API. The
upstream specification's atomic visibility rule informed the design:
https://github.com/earendil-works/pi/blob/main/packages/durable/docs/spec.md

The SQLite tables are `durable_store_meta(id, version)` and
`durable_store_entries(key, value, version)`; they are implementation details.
