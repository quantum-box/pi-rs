//! Build with `cargo build -p pi-rs-extensions --example project_notes`.
//! Load with `pi --extension target/debug/examples/project_notes`.
use pi_rs_core::{Extension, ExtensionResult, ExtensionState, ModelRequest, Result};
use pi_rs_extensions::{Notes, serve_extension};
use std::sync::Arc;

struct ProjectNotes;
impl Extension for ProjectNotes {
    fn id(&self) -> &str {
        "project-notes"
    }
    fn commands(&self) -> &[&str] {
        &["set", "append", "clear"]
    }
    fn command(
        &self,
        name: &str,
        arguments: &str,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<ExtensionState> {
        Notes.command(name, arguments, state)
    }
    fn context(
        &self,
        request: &mut ModelRequest,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<pi_rs_core::ContextChange> {
        Notes.context(request, state)
    }
}
fn main() -> Result<()> {
    serve_extension(Arc::new(ProjectNotes))
}
