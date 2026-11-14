//! A local, two-process durable request demo (no credentials or network).
//!
//! cargo run -p pi-rs-durable --example resume -- start /tmp/pi-durable-demo.db
//! cargo run -p pi-rs-durable --example resume -- resume /tmp/pi-durable-demo.db CONVERSATION_ID
//!
//! Repeat the second command to retrieve the same completed submission without
//! invoking the model or tool again. Use a fresh path for a separate demo.

use std::{error::Error as StdError, sync::Arc};

use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    CancellationToken, Error, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool,
    ToolCall, ToolContext, ToolSpec,
};
use pi_rs_durable::{
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, Harness, Options, RegisteredTool},
};
use serde_json::json;
use uuid::Uuid;

const REQUEST_ID: &str = "example-request-1";
const PROMPT: &str = "What is 6 times 7?";

struct LocalModel;

#[async_trait]
impl Model for LocalModel {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let result = request.messages.iter().find_map(|message| match message {
            Message::Tool { content, .. } => Some(content),
            _ => None,
        });
        let events = if let Some(result) = result {
            println!("local model: answering from tool result");
            vec![
                Ok(ModelEvent::Text(format!("The answer is {result}."))),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ]
        } else {
            println!("local model: requesting multiply tool");
            vec![Ok(ModelEvent::Complete {
                calls: vec![ToolCall {
                    id: "multiply-1".into(),
                    name: "multiply".into(),
                    arguments: r#"{"a":6,"b":7}"#.into(),
                }],
                usage: None,
            })]
        };
        Ok(stream::iter(events).boxed())
    }
}

struct Multiply;

#[async_trait]
impl Tool for Multiply {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "multiply".into(),
            description: "Multiply two small integers locally".into(),
            parameters: json!({
                "type": "object",
                "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
                "required": ["a", "b"],
                "additionalProperties": false
            }),
        }
    }

    async fn invoke(&self, args: serde_json::Value, _ctx: ToolContext) -> Outcome {
        println!("local tool: multiplying");
        match (args["a"].as_i64(), args["b"].as_i64()) {
            (Some(a), Some(b)) => match a.checked_mul(b) {
                Some(product) => Outcome::Succeeded(product.to_string()),
                None => Outcome::Failed("integer overflow".into()),
            },
            _ => Outcome::Failed("expected two integers".into()),
        }
    }
}

fn options() -> Options {
    Options {
        model: Arc::new(LocalModel),
        tools: vec![RegisteredTool {
            tool: Arc::new(Multiply),
            replay: ReplayPolicy::Safe,
        }],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn StdError>> {
    let args: Vec<_> = std::env::args().collect();
    if !(args.len() == 3 && args[1] == "start" || args.len() == 4 && args[1] == "resume") {
        return Err("usage: resume start DB_PATH | resume resume DB_PATH CONVERSATION_ID".into());
    }
    let mut harness = Harness::open_paused(&args[2], options())?;
    if args[1] == "start" {
        let id = harness.create_conversation(Config {
            model: "local-fake".into(),
            system_prompt: "Use the multiply tool.".into(),
            max_output_tokens: 100,
        })?;
        let submission = harness.submit(id, Some(REQUEST_ID.into()), PROMPT.into())?;
        println!("queued submission: {submission}\nconversation: {id}");
        println!("Run again with: resume {} {id}", args[2]);
    } else {
        let id: Uuid = args[3].parse()?;
        let before = harness.conversation(id)?;
        let original = before
            .submissions
            .iter()
            .find(|s| s.request_id.as_deref() == Some(REQUEST_ID))
            .ok_or("request not found in this conversation")?;
        let original_id = original.id;
        println!("persisted status: {:?}", original.status);
        let submission = harness
            .submit_and_run(
                id,
                Some(REQUEST_ID.into()),
                PROMPT.into(),
                &CancellationToken::new(),
            )
            .await?;
        assert_eq!(
            submission, original_id,
            "request must resolve to the original submission"
        );
        let conversation = harness.conversation(id)?;
        let saved = conversation
            .submissions
            .iter()
            .find(|s| s.id == submission)
            .ok_or("submission missing")?;
        println!("submission: {submission}\nstatus: {:?}", saved.status);
    }
    Ok(())
}
