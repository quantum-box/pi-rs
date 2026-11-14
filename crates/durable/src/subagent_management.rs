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

pub(crate) fn extension(
    harness: Harness,
    parent: Uuid,
    config: Config,
    allowed_tools: Option<Vec<String>>,
) -> Extension {
    Extension {
        name: extension_name(parent),
        tools: vec![Arc::new(Management {
            harness: Mutex::new(harness),
            parent,
            config,
            allowed_tools,
        })],
        ..Extension::default()
    }
}

struct Management {
    harness: Mutex<Harness>,
    parent: Uuid,
    config: Config,
    allowed_tools: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum Args {
    List {
        name: Option<String>,
    },
    Stop {
        name: String,
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
            description: "List named background subagents, send a message to one (creating it if needed), or stop its current work. Answers arrive as follow-up messages. Each send is a separate request.".into(),
            parameters: json!({
                "oneOf": [
                    {"type":"object","properties":{"action":{"const":"list"},"name":{"type":"string","minLength":1,"maxLength":128}},"required":["action"],"additionalProperties":false},
                    {"type":"object","properties":{"action":{"const":"stop"},"name":{"type":"string","minLength":1,"maxLength":128}},"required":["action","name"],"additionalProperties":false},
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
        if let Args::Stop { name } = &args {
            if !valid_name(name) {
                return Outcome::Failed("invalid subagent name".into());
            }
            let mut worker = match self.harness.lock() {
                Ok(harness) => harness.worker(),
                Err(_) => {
                    return Outcome::Unknown(
                        "subagent store lock poisoned; reopen the scheduler".into(),
                    );
                }
            };
            let name = name.clone();
            let parent = self.parent;
            let operation_id = context.operation_id;
            let runtime = tokio::runtime::Handle::current();
            return match tokio::task::spawn_blocking(move || {
                let harness = &mut worker;
                let key = format!("runtime/subagent-stop/{parent}/{operation_id}").into_bytes();
                let owner = harness.owner.clone();
                let mut stopped = name.clone();
                let result = owner.conversation_controls().fence(
                    || -> Result<Vec<Uuid>, crate::runtime::Error> {
                        if let Some(entry) = harness.subagent_store()?.get(&key)? {
                            stopped = serde_json::from_slice(&entry.value)?;
                            return Ok(Vec::new());
                        }
                        let link = harness.subagent(parent, &name)?.ok_or_else(|| {
                            crate::runtime::Error::Invalid(format!("unknown subagent: {name}"))
                        })?;
                        harness.conversation(link.child)?;
                        harness.abort_owned_graph_once(
                            Some(link.child),
                            None,
                            Some((key, serde_json::to_vec(&name)?)),
                        )
                    },
                );
                if let Ok(affected) = &result {
                    for id in affected {
                        let mut cleanup = harness.worker();
                        let shutdown = owner.conversation_controls().root();
                        let id = *id;
                        runtime.spawn(async move {
                            let _ = cleanup.drive_owned_cleanup(id, &shutdown).await;
                        });
                    }
                }
                (result, stopped)
            })
            .await
            {
                Ok((Ok(_), stopped)) => Outcome::Succeeded(format!("Stopped {stopped}.")),
                Ok((Err(durable_store::controls::FenceError::Poisoned), _)) => {
                    Outcome::Unknown("subagent control lock poisoned; reopen the scheduler".into())
                }
                Ok((
                    Err(durable_store::controls::FenceError::Commit(
                        crate::runtime::Error::Invalid(e),
                    )),
                    _,
                )) => Outcome::Failed(e),
                Ok((
                    Err(durable_store::controls::FenceError::Commit(
                        crate::runtime::Error::NotFound,
                    )),
                    _,
                )) => Outcome::Failed("subagent conversation not found".into()),
                Ok((Err(durable_store::controls::FenceError::Commit(e)), _)) => {
                    Outcome::Unknown(format!(
                        "subagent stop failed or may have committed: {e}; reconcile before retrying"
                    ))
                }
                Err(e) => Outcome::Unknown(format!(
                    "subagent stop may have committed: {e}; reconcile before retrying"
                )),
            };
        }
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
            Args::Stop { .. } => unreachable!(),
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
                match harness.submit_subagent_reported_restricted(
                    self.parent,
                    &name,
                    self.config.clone(),
                    input,
                    self.allowed_tools.clone(),
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
            allowed_tools: None,
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
            allowed_tools: None,
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
    #[tokio::test]
    async fn restricted_child_is_configured_before_first_dispatch_and_reopen() {
        use crate::{recovery::ReplayPolicy, runtime::RegisteredTool};
        struct Forbidden;
        #[async_trait]
        impl Tool for Forbidden {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "forbidden".into(),
                    description: "".into(),
                    parameters: json!({"type":"object"}),
                }
            }
            async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
                panic!("forbidden tool executed")
            }
        }
        struct Inspect;
        #[async_trait]
        impl Model for Inspect {
            async fn stream(&self, req: ModelRequest) -> Result<ModelStream, Error> {
                if req.messages.iter().any(
                    |m| matches!(m, pi_rs_core::Message::User { content } if content == "hello"),
                ) {
                    assert!(
                        req.tools.is_empty(),
                        "child must be restricted on its first dispatch"
                    );
                }
                Ok(stream::iter(vec![Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                })])
                .boxed())
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let opts = || {
            let mut o = options();
            o.model = Arc::new(Inspect);
            o.tools.push(RegisteredTool {
                tool: Arc::new(Forbidden),
                replay: ReplayPolicy::Safe,
            });
            o
        };
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        let parent = h.create_conversation(config()).unwrap();
        h.select_tools(parent, Some(vec!["forbidden".into()]))
            .unwrap();
        let tool = Management {
            harness: Mutex::new(h),
            parent,
            config: config(),
            allowed_tools: Some(vec![]),
        };
        let op = Uuid::new_v4();
        let args = json!({"action":"send","name":"worker","message":"hello"});
        assert!(matches!(
            tool.invoke(args.clone(), context(op)).await,
            Outcome::Succeeded(_)
        ));
        let child = tool
            .harness
            .lock()
            .unwrap()
            .subagent(parent, "worker")
            .unwrap()
            .unwrap()
            .child;
        assert_eq!(
            tool.harness
                .lock()
                .unwrap()
                .conversation(parent)
                .unwrap()
                .allowed_tools,
            Some(vec!["forbidden".into()])
        );
        drop(tool);
        let scheduler = Scheduler::open_with_restricted_subagent_management(
            &path,
            opts(),
            Default::default(),
            parent,
            config(),
            Some(vec![]),
        )
        .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if matches!(
                    scheduler.conversation(child).unwrap().submissions[0].status,
                    crate::runtime::Status::Done { .. }
                ) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            scheduler.conversation(child).unwrap().allowed_tools,
            Some(vec![])
        );
        scheduler.shutdown().await.unwrap();
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        assert!(
            h.ensure_subagent_restricted(
                parent,
                "worker",
                config(),
                false,
                Some(vec!["forbidden".into()])
            )
            .is_err()
        );
        assert_eq!(
            h.ensure_subagent_restricted(parent, "worker", config(), false, Some(vec![]))
                .unwrap()
                .child,
            child
        );
    }

    #[tokio::test]
    async fn stop_is_parent_scoped_and_replay_does_not_stop_later_work_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let parent = h.create_conversation(config()).unwrap();
        let other = h.create_conversation(config()).unwrap();
        let child = h.ensure_subagent(parent, "worker", config()).unwrap().child;
        let other_child = h.ensure_subagent(other, "worker", config()).unwrap().child;
        h.submit_with_mode(
            child,
            Some("first".into()),
            "first".into(),
            InputMode::FollowUp,
        )
        .unwrap();
        h.submit_with_mode(
            other_child,
            Some("other".into()),
            "other".into(),
            InputMode::FollowUp,
        )
        .unwrap();
        let tool = Management {
            harness: Mutex::new(h),
            parent,
            config: config(),
            allowed_tools: None,
        };
        let op = Uuid::new_v4();
        let first = tool
            .invoke(json!({"action":"stop","name":"worker"}), context(op))
            .await;
        assert!(matches!(first, Outcome::Succeeded(_)));
        // Wait until cleanup has released its worker before reopening the store.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if !tool
                    .harness
                    .lock()
                    .unwrap()
                    .conversation(child)
                    .unwrap()
                    .abort_requested
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let worker_lock = tool
            .harness
            .lock()
            .unwrap()
            .owner
            .conversation_controls()
            .get(child)
            .worker;
        let guard = worker_lock.lock().await;
        {
            let h = tool.harness.lock().unwrap();
            assert!(matches!(
                h.conversation(child).unwrap().submissions[0].status,
                crate::runtime::Status::Unanswered { .. }
            ));
            assert!(matches!(
                h.conversation(other_child).unwrap().submissions[0].status,
                crate::runtime::Status::Running
            ));
        }
        drop(guard);
        drop(worker_lock);
        drop(tool);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.submit_with_mode(
            child,
            Some("later".into()),
            "later".into(),
            InputMode::FollowUp,
        )
        .unwrap();
        let tool = Management {
            harness: Mutex::new(h),
            parent,
            config: config(),
            allowed_tools: None,
        };
        let repeated = tool
            .invoke(json!({"action":"stop","name":"different"}), context(op))
            .await;
        assert_eq!(format!("{first:?}"), format!("{repeated:?}"));
        assert!(matches!(
            tool.harness
                .lock()
                .unwrap()
                .conversation(child)
                .unwrap()
                .submissions[1]
                .status,
            crate::runtime::Status::Running
        ));
        assert!(!matches!(
            tool.invoke(
                json!({"action":"stop","name":"missing"}),
                context(Uuid::new_v4())
            )
            .await,
            Outcome::Succeeded(_)
        ));
    }

    struct StopModel;
    #[async_trait]
    impl Model for StopModel {
        async fn stream(&self, req: ModelRequest) -> Result<ModelStream, Error> {
            use pi_rs_core::{Message, ToolCall};
            if req
                .messages
                .iter()
                .any(|m| matches!(m, Message::User { content } if content == "child task"))
            {
                return Ok(stream::pending().boxed());
            }
            let start = req
                .messages
                .iter()
                .any(|m| matches!(m, Message::User { content } if content == "start"));
            let result = req
                .messages
                .iter()
                .any(|m| matches!(m, Message::Tool { .. }));
            let events = if start && !result {
                vec![Ok(ModelEvent::Complete {
                    calls: vec![ToolCall {
                        id: "stop".into(),
                        name: "subagent".into(),
                        arguments: json!({"action":"stop","name":"worker"}).to_string(),
                    }],
                    usage: None,
                })]
            } else {
                vec![
                    Ok(ModelEvent::Text("done".into())),
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
    async fn model_stop_drains_child_and_reports_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let opts = || {
            let mut o = options();
            o.model = Arc::new(StopModel);
            o
        };
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        let parent = h.create_conversation(config()).unwrap();
        let (link, _) = h
            .submit_subagent_reported(
                parent,
                "worker",
                config(),
                ReportedInput {
                    history: false,
                    request_id: "request".into(),
                    content: "child task".into(),
                    mode: InputMode::FollowUp,
                },
            )
            .unwrap();
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
                let parent_state = scheduler.conversation(parent).unwrap();
                let child = scheduler.conversation(link.child).unwrap();
                if parent_state
                    .submissions
                    .iter()
                    .any(|s| s.content.contains("[subagent worker failed:"))
                    && !child.abort_requested
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(scheduler.reporter_error().is_none());
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
