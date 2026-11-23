//! Minimal owned SDK session using a local Ollama server.
use std::sync::Arc;

use pi_rs_core::{Result, SessionBuilder};
use pi_rs_ollama::Ollama;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let model_name = std::env::var("PI_RS_EXAMPLE_MODEL").unwrap_or_else(|_| "qwen2.5:7b".into());
    let model = Arc::new(Ollama::new(
        "http://127.0.0.1:11434/v1/chat/completions",
        None,
    )?);
    let mut session = SessionBuilder::new(model, model_name)
        .system_prompt("You are a helpful assistant.")
        .in_memory()?;

    let answer = session.prompt("Say hello in one sentence.").await?;
    println!("{answer}");
    session.close().await
}
