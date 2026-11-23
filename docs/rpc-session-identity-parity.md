# RPC session identity

RPC `get_state` and `get_session_stats` project the externally visible Pi
session ID, including imported legacy IDs, rather than the native journal's
internal UUID. This matches Pi's RPC state and session statistics. Native SDK
statistics and journal identity remain UUID-based. After a fork, RPC reports the
fork's newly generated ID. Share, terminal UI and other projections are separate.
