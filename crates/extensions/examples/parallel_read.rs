//! Build with `cargo build -p pi-rs-extensions --example parallel_read`.
//! Load with `pi -e target/debug/examples/parallel_read`.
use pi_rs_core::{
    Extension, ExtensionState, Outcome, Result, Tool, ToolContext, ToolHost, ToolSpec,
};
use pi_rs_extensions::serve_extension;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;

struct ParallelRead;
#[derive(Deserialize)]
struct Arguments {
    paths: Vec<String>,
}
#[async_trait::async_trait]
impl Tool for ParallelRead {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "parallel_read".into(),
            description: "Read up to eight files through the host's journaled read tool.".into(),
            parameters: json!({"type":"object","required":["paths"],"additionalProperties":false,
                "properties":{"paths":{"type":"array","minItems":1,"maxItems":8,
                    "items":{"type":"string"}}}}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Failed("parallel_read requires the host tool interface".into())
    }
    async fn invoke_with_host(
        &self,
        arguments: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> Outcome {
        let arguments: Arguments = match serde_json::from_value(arguments) {
            Ok(arguments) => arguments,
            Err(error) => return Outcome::Failed(error.to_string()),
        };
        let calls = arguments
            .paths
            .iter()
            .map(|path| ("read".into(), json!({"path":path})))
            .collect();
        let results = host.call_batch(calls).await;
        Outcome::Structured {
            data: json!({"paths":arguments.paths,"results":results}),
            attachments: vec![],
        }
    }
}
struct ParallelReadExtension;
impl Extension for ParallelReadExtension {
    fn id(&self) -> &str {
        "parallel-read"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(ParallelRead)]
    }
}
fn main() -> Result<()> {
    serve_extension(Arc::new(ParallelReadExtension))
}
