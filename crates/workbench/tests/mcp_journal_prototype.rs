//! Exercise the unconnected journal/key prototypes with real dependencies.
//! This does not enable them in the application or establish crash recovery.
#![cfg(any(target_os = "linux", target_os = "macos"))]
#![allow(dead_code)]

#[path = "../src/mcp_journal_identity.rs"]
mod mcp_journal_identity;
#[path = "../src/mcp_transaction_journal.rs"]
mod mcp_transaction_journal;
