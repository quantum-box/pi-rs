//! Experimental primitives for a durable agent runtime.
//!
//! State transitions must be committed atomically by the host. This crate is
//! under development; it does not yet provide a complete agent harness.

pub mod application_entry;
mod chord_ops;
#[cfg(feature = "coding-tools")]
pub mod coding_tools;
pub mod compact;
pub mod content;
pub mod conversation;
pub mod conversation_signal;
pub mod document;
#[allow(dead_code)] // Internal identity binding, not a dispatch API.
mod executor_handoff;
pub mod extension;
pub mod fork;
#[allow(dead_code)] // Storage foundation; not wired into tool execution yet.
mod keyed_child;
pub mod keyed_tool;
pub mod model;
pub mod nested;
#[allow(dead_code)] // Storage protocol only; no scheduler or executor handoff.
mod nested_suspension;
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
#[allow(dead_code)] // Private operation storage, not a public admission API.
mod summary_store;
#[allow(dead_code)] // Private worker until placement and admission are integrated.
mod summary_worker;
pub mod task;
pub mod task_driver;
pub mod task_scheduler;
pub mod task_store;
pub mod task_tool;
pub mod tool;
pub mod tool_progress;
pub mod tool_search;
pub mod transcript;
