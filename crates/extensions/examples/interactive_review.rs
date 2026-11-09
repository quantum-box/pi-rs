//! Build with `cargo build -p pi-rs-extensions --example interactive_review`.
//! Load with `pi -e target/debug/examples/interactive_review`.
use pi_rs_core::{
    Extension, ExtensionState, Outcome, Result, Tool, ToolContext, ToolHost, ToolSpec, UiRequest,
    UiResponse,
};
use pi_rs_extensions::serve_extension;
use serde_json::{Value, json};
use std::sync::Arc;

struct ReviewTool;
#[async_trait::async_trait]
impl Tool for ReviewTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "interactive_review".into(),
            description: "Ask the user for review settings through native dialogs.".into(),
            parameters: json!({"type":"object","additionalProperties":false}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Failed("interactive review requires a UI host".into())
    }
    async fn invoke_with_host(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> Outcome {
        if !host.has_ui() {
            return Outcome::Failed("interactive UI is unavailable".into());
        }
        let result: pi_rs_core::ExtensionResult<Value> = async {
            host.ui(UiRequest::SetStatus { key: "review".into(), text: Some("Review setup".into()) }).await?;
            host.ui(UiRequest::SetWidget { key: "review".into(), lines: Some(vec!["Native review widget".into()]), placement: pi_rs_core::WidgetPlacement::AboveEditor }).await?;
            let UiResponse::Text(depth) = host.ui(UiRequest::Select { title: "Review depth".into(), options: vec!["Quick".into(), "Thorough".into()], timeout: None }).await? else { return Ok(json!({"cancelled":true})); };
            let UiResponse::Text(note) = host.ui(UiRequest::Input { title: "Review note".into(), placeholder: Some("Enter a note".into()), initial_value: None, timeout: None }).await? else { return Ok(json!({"cancelled":true})); };
            let UiResponse::Text(details) = host.ui(UiRequest::Editor { title: "Review details".into(), initial_value: Some("Initial detail".into()), timeout: None }).await? else { return Ok(json!({"cancelled":true})); };
            let confirmed = host.ui(UiRequest::Confirm { title: "Confirm review".into(), message: format!("Use {depth} review?"), timeout: None }).await?;
            Ok(json!({"depth":depth,"note":note,"details":details,"confirmed":confirmed == UiResponse::Confirmed(true)}))
        }.await;
        let _ = host
            .ui(UiRequest::SetStatus {
                key: "review".into(),
                text: None,
            })
            .await;
        let _ = host
            .ui(UiRequest::SetWidget {
                key: "review".into(),
                lines: None,
                placement: pi_rs_core::WidgetPlacement::AboveEditor,
            })
            .await;
        match result {
            Ok(result) => Outcome::Succeeded(result.to_string()),
            Err(error) => Outcome::Failed(error),
        }
    }
}
struct ReviewExtension;
impl Extension for ReviewExtension {
    fn id(&self) -> &str {
        "interactive-review"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(ReviewTool)]
    }
}
fn main() -> Result<()> {
    serve_extension(Arc::new(ReviewExtension))
}
