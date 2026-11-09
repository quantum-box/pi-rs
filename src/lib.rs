//! Public entry point for the agent core and its optional Ollama adapter.
pub use pi_rs_core::*;

#[cfg(feature = "ollama")]
pub use pi_rs_ollama as ollama;
