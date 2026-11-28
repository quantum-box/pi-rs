# RPC session identity

RPC `get_state` and `get_session_stats` project the externally visible Pi
session ID, including imported legacy IDs. The Rust SDK exposes the same
external ID through `AgentSession::session_id()` and `AgentSession::session_stats()`.
`SessionStats` totals and the native journal identity use the native UUID.
After a fork, RPC and the SDK snapshot report the fork's newly generated ID.
