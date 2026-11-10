//! Public entry point for the agent core and its optional model adapters.
pub use pi_rs_core::*;

#[cfg(feature = "ollama")]
pub use pi_rs_ollama as ollama;

#[cfg(feature = "anthropic")]
pub use pi_rs_anthropic as anthropic;
