//! Experimental primitives for a durable agent runtime.
//!
//! State transitions must be committed atomically by the host. This crate is
//! under development; it does not yet provide a complete agent harness.

#[cfg(feature = "coding-tools")]
pub mod coding_tools;
pub mod compact;
pub mod conversation;
pub mod conversation_signal;
pub mod document;
pub mod extension;
pub mod fork;
pub mod model;
pub mod nested;
pub mod observer;
pub mod owned_subagent;
pub mod ownership_edge;
pub mod poll;
pub mod progress;
pub mod recovery;
pub mod reset;
pub mod retry_policy;
pub mod runtime;
pub mod state;
pub mod subagent;
pub mod task;
pub mod task_driver;
pub mod task_scheduler;
pub mod task_store;
pub mod tool;
pub mod transcript;
