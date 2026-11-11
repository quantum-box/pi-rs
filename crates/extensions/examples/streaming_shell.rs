//! Example native shell operations. Build with `cargo build -p pi-rs-extensions --example streaming_shell`.
use std::sync::Arc;
use std::time::Duration;

use pi_rs_core::{
    CancellationToken, Extension, ExtensionResult, ExtensionState, Result, UserBashEvent,
};
use pi_rs_extensions::serve_extension;

struct StreamingShell;

#[async_trait::async_trait]
impl Extension for StreamingShell {
    fn id(&self) -> &str {
        "streaming-shell-example"
    }
    fn handles_user_bash_operations(&self) -> bool {
        true
    }
    async fn user_bash_operations(
        &self,
        event: &UserBashEvent,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<bool> {
        Ok(event.command == "remote-demo")
    }
    async fn execute_user_bash(
        &self,
        _: &UserBashEvent,
        _: Option<&ExtensionState>,
        _: Option<Duration>,
        cancel: CancellationToken,
        output: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> ExtensionResult<Option<i32>> {
        output
            .send(b"Running remote operation...\n".to_vec())
            .await
            .map_err(|error| error.to_string())?;
        tokio::select! {
            _ = cancel.cancelled() => return Err("cancelled".into()),
            _ = tokio::time::sleep(Duration::from_millis(150)) => {}
        }
        output
            .send("完了\n".as_bytes().to_vec())
            .await
            .map_err(|error| error.to_string())?;
        Ok(Some(0))
    }
}

fn main() -> Result<()> {
    serve_extension(Arc::new(StreamingShell))
}
