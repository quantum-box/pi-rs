//! Experimental primitives for a durable agent runtime.
//!
//! State transitions must be committed atomically by the host. This crate is
//! under development; it does not yet provide a complete agent harness.

pub mod application_entry;
#[cfg(feature = "coding-tools")]
pub mod coding_tools;
pub mod compact;
pub mod conversation;
pub mod conversation_signal;
pub mod document;
pub mod extension;
pub mod fork;
#[allow(dead_code)] // Storage foundation; not wired into tool execution yet.
mod keyed_child;
pub mod keyed_tool;
pub mod model;
pub mod nested;
pub mod observer;
pub mod owned_subagent;
pub mod ownership_edge;
pub mod poll;
pub mod progress;
pub mod recovery;
pub mod report;
pub mod reset;
pub mod retry_policy;
pub mod runtime;
pub mod state;
pub mod streamed_output;
pub mod subagent;
pub mod subagent_management;
pub mod task;
pub mod task_driver;
pub mod task_scheduler;
pub mod task_store;
pub mod tool;
pub mod tool_progress;
pub mod transcript;
