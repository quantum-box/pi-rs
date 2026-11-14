//! Opt-in parent-bound management of independently running named conversations.
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pi_rs_core::{Outcome, Tool, ToolContext, ToolSpec};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    extension::Extension,
    runtime::{Config, Harness},
    state::InputMode,
    subagent::{ReportedInput, format_status},
};

pub(crate) const EXTENSION_PREFIX: &str = "pi-rs.subagent-management/";
/// Extension name to select on the bound, settled parent conversation.
pub fn extension_name(parent: Uuid) -> String {
    format!("{EXTENSION_PREFIX}{parent}")
}

pub(crate) fn extension(harness: Harness, parent: Uuid, config: Config) -> Extension {
    Extension {
        name: extension_name(parent),
        tools: vec![Arc::new(Management {
            harness: Mutex::new(harness),
            parent,
            config,
        })],
        ..Extension::default()
    }
}

struct Management {
    harness: Mutex<Harness>,
    parent: Uuid,
    config: Config,
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum Args {
    List {
        name: Option<String>,
    },
    Send {
        name: String,
        message: String,
        mode: Option<Mode>,
    },
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    FollowUp,
    Steer,
}

#[async_trait]
impl Tool for Management {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "subagent".into(),
            description: "List named background subagents, or send a message to one (creating it if needed). Answers arrive as follow-up messages. Each send is a separate request.".into(),
            parameters: json!({
                "oneOf": [
                    {"type":"object","properties":{"action":{"const":"list"},"name":{"type":"string","minLength":1,"maxLength":128}},"required":["action"],"additionalProperties":false},
                    {"type":"object","properties":{"action":{"const":"send"},"name":{"type":"string","minLength":1,"maxLength":128},"message":{"type":"string","minLength":1,"maxLength":65536},"mode":{"enum":["follow_up","steer"]}},"required":["action","name","message"],"additionalProperties":false}
                ]
            }),
        }
    }
    async fn invoke(&self, args: Value, context: ToolContext) -> Outcome {
        if context.cancellation.is_cancelled() {
            return Outcome::Skipped("cancelled before dispatch".into());
        }
        let args: Args = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(e) => return Outcome::Failed(format!("invalid subagent arguments: {e}")),
        };
        let valid_name = |name: &str| {
            !name.trim().is_empty() && name.len() <= 128 && !name.chars().any(char::is_control)
        };
        let mut harness = match self.harness.lock() {
            Ok(harness) => harness,
            Err(_) => {
                return Outcome::Unknown(
                    "subagent store lock poisoned; reopen the scheduler".into(),
                );
            }
        };
        if context.cancellation.is_cancelled() {
            return Outcome::Skipped("cancelled before dispatch".into());
        }
        match args {
            Args::List { name } => {
                if name.as_deref().is_some_and(|n| !valid_name(n)) {
                    return Outcome::Failed("invalid subagent name".into());
                }
                match harness.subagent_status(self.parent, name.as_deref()) {
                    Ok(status) => Outcome::Succeeded(format_status(&status)),
                    Err(e) => Outcome::Failed(e.to_string()),
                }
            }
            Args::Send {
                name,
                message,
                mode,
            } => {
                if !valid_name(&name) || message.trim().is_empty() || message.len() > 65536 {
                    return Outcome::Failed("valid name and nonempty message required".into());
                }
                let request = format!("tool/{}", context.operation_id);
                let input = ReportedInput {
                    history: false,
                    request_id: request,
                    content: message,
                    mode: match mode {
                        Some(Mode::Steer) => InputMode::Steer,
                        _ => InputMode::FollowUp,
                    },
                };
                match harness.submit_subagent_reported(
                    self.parent,
                    &name,
                    self.config.clone(),
                    input,
                ) {
                    Ok((link, submission)) => Outcome::Succeeded(format!(
                        "Sent to {name} (child {}, request {}). The answer will be reported asynchronously.",
                        link.child, submission
                    )),
                    Err(e) => Outcome::Unknown(format!(
                        "subagent admission may have committed: {e}; reconcile before retrying"
                    )),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        recovery::UncertainPolicy,
        runtime::{Options, Scheduler},
    };
    use futures_util::{StreamExt, stream};
    use pi_rs_core::{CancellationToken, Error, Model, ModelEvent, ModelRequest, ModelStream};

    struct ModelStub;
    #[async_trait]
    impl Model for ModelStub {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
            Ok(stream::iter(vec![
                Ok(ModelEvent::Text("answer".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])
            .boxed())
        }
    }
    fn options() -> Options {
        Options {
            model: Arc::new(ModelStub),
            tools: vec![],
            restart_limit: 4,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        }
    }
    fn config() -> Config {
        Config {
            model: "fake".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        }
    }
    fn context(id: Uuid) -> ToolContext {
        ToolContext {
            operation_id: id,
            cancellation: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn repeated_operation_reuses_child_and_submission_and_report_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let parent = h.create_conversation(config()).unwrap();
        let tool = Management {
            harness: Mutex::new(h),
            parent,
            config: config(),
        };
        let op = Uuid::new_v4();
        let args = json!({"action":"send","name":"worker","message":"hello"});
        let first = tool.invoke(args.clone(), context(op)).await;
        let second = tool
            .invoke(
                json!({"action":"send","name":"worker","message":"changed","mode":"steer"}),
                context(op),
            )
            .await;
        assert_eq!(format!("{first:?}"), format!("{second:?}"));
        let link = {
            let h = tool.harness.lock().unwrap();
            let link = h.subagent(parent, "worker").unwrap().unwrap();
            let child = h.conversation(link.child).unwrap();
            assert_eq!(child.submissions.len(), 1);
            link
        };
        drop(tool);
        let scheduler = Scheduler::open_with_subagent_management(
            &path,
            options(),
            Default::default(),
            parent,
            config(),
        )
        .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if scheduler.conversation(parent).unwrap().submissions.len() == 1 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        scheduler.shutdown().await.unwrap();
        let h = Harness::open_paused(&path, options()).unwrap();
        assert_eq!(h.conversation(parent).unwrap().submissions.len(), 1);
        assert_eq!(h.conversation(link.child).unwrap().submissions.len(), 1);
        let tool = Management {
            harness: Mutex::new(h),
            parent,
            config: config(),
        };
        let after_restart = tool.invoke(args, context(op)).await;
        assert_eq!(format!("{first:?}"), format!("{after_restart:?}"));
        let h = tool.harness.lock().unwrap();
        assert_eq!(h.conversation(parent).unwrap().submissions.len(), 1);
        assert_eq!(h.conversation(link.child).unwrap().submissions.len(), 1);
    }

    #[tokio::test]
    async fn extension_is_parent_scoped_and_children_do_not_inherit_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let parent = h.create_conversation(config()).unwrap();
        let other = h.create_conversation(config()).unwrap();
        drop(h);
        let mut scheduler = Scheduler::open_with_subagent_management(
            &path,
            options(),
            Default::default(),
            parent,
            config(),
        )
        .unwrap();
        let name = extension_name(parent);
        assert!(
            scheduler
                .select_extensions(other, vec![name.clone()])
                .is_err()
        );
        scheduler.select_extensions(parent, vec![name]).unwrap();
        let link = scheduler
            .ensure_subagent(parent, "child", config())
            .unwrap();
        assert!(
            scheduler
                .conversation(link.child)
                .unwrap()
                .extensions
                .is_empty()
        );
        scheduler.shutdown().await.unwrap();
    }
    struct CallingModel;
    #[async_trait]
    impl Model for CallingModel {
        async fn stream(&self, req: ModelRequest) -> Result<ModelStream, Error> {
            use pi_rs_core::{Message, ToolCall};
            let initial = req
                .messages
                .iter()
                .any(|m| matches!(m, Message::User { content } if content == "start"));
            let tool_result = req
                .messages
                .iter()
                .any(|m| matches!(m, Message::Tool { .. }));
            let report = req.messages.iter().any(|m| matches!(m, Message::User { content } if content.contains("[subagent worker answered")));
            let events = if initial && !tool_result && !report {
                assert!(req.tools.iter().any(|t| t.name == "subagent"));
                vec![Ok(ModelEvent::Complete {
                    calls: vec![ToolCall {
                        id: "call".into(),
                        name: "subagent".into(),
                        arguments: json!({"action":"send","name":"worker","message":"child task"})
                            .to_string(),
                    }],
                    usage: None,
                })]
            } else {
                if !initial {
                    assert!(!req.tools.iter().any(|t| t.name == "subagent"));
                }
                vec![
                    Ok(ModelEvent::Text("answer".into())),
                    Ok(ModelEvent::Complete {
                        calls: vec![],
                        usage: None,
                    }),
                ]
            };
            Ok(stream::iter(events).boxed())
        }
    }
    #[tokio::test]
    async fn model_can_dispatch_and_receive_background_answer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let opts = || {
            let mut o = options();
            o.model = Arc::new(CallingModel);
            o
        };
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        let parent = h.create_conversation(config()).unwrap();
        drop(h);
        let mut scheduler = Scheduler::open_with_subagent_management(
            &path,
            opts(),
            Default::default(),
            parent,
            config(),
        )
        .unwrap();
        scheduler
            .select_extensions(parent, vec![extension_name(parent)])
            .unwrap();
        scheduler.submit(parent, None, "start".into()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let c = scheduler.conversation(parent).unwrap();
                if c.submissions.len() == 2
                    && c.submissions
                        .iter()
                        .all(|s| matches!(s.status, crate::runtime::Status::Done { .. }))
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(scheduler.subagent_status(parent, None).unwrap().len(), 1);
        scheduler.shutdown().await.unwrap();
    }
}
