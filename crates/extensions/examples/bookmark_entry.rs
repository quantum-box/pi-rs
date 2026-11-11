//! Build with `cargo build -p pi-rs-extensions --example bookmark_entry`.
use std::sync::Arc;

use pi_rs_core::{
    CustomEntry, Extension, ExtensionCommandOutput, ExtensionResult, ExtensionState, Result,
};
use pi_rs_extensions::serve_extension;

struct Bookmarks;
impl Extension for Bookmarks {
    fn id(&self) -> &str {
        "bookmark-example"
    }
    fn commands(&self) -> &[&str] {
        &["bookmark"]
    }
    fn supports_command_entries(&self) -> bool {
        true
    }
    fn command_output(
        &self,
        _: &str,
        arguments: &str,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<ExtensionCommandOutput> {
        Ok(ExtensionCommandOutput {
            state: None,
            entries: vec![CustomEntry {
                custom_type: "bookmark".into(),
                data: Some(serde_json::json!({"note":arguments})),
            }],
        })
    }
}

fn main() -> Result<()> {
    serve_extension(Arc::new(Bookmarks))
}
