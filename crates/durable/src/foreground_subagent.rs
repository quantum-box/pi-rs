//! Parent-bound foreground delegation backed by a durable tool task.
use std::sync::{Arc, Mutex};

use pi_rs_core::ToolSpec;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    extension::Extension,
    runtime::{Config, Harness},
    task::{Checkpoint, Task, TaskOwner, WaitFor},
    task_scheduler::{Handler, Step},
    task_store::TaskStore,
    task_tool::TaskTool,
};

pub(crate) const PREFIX: &str = "pi-rs.foreground-subagent/";
pub(crate) const KIND: &str = "pi-rs.foreground-subagent";
const SIGNAL: &str = "answered";

/// Name of the foreground extension selected on its bound parent.
pub fn extension_name(parent: Uuid) -> String {
    format!("{PREFIX}{parent}")
}

pub(crate) fn extension(
    harness: Harness,
    parent: Uuid,
    config: Config,
    allowed_tools: Option<Vec<String>>,
    schedule: tokio::sync::mpsc::UnboundedSender<Uuid>,
) -> (Extension, Arc<dyn Handler>) {
    let binding = json!({"parent": parent, "config": config, "allowed_tools": allowed_tools});
    let handler: Arc<dyn Handler> = Arc::new(Foreground {
        harness: Mutex::new(harness),
        parent,
        config,
        allowed_tools,
        schedule,
    });
    let adapter = Arc::new(
        TaskTool::new(
            ToolSpec {
                name: "subagent".into(),
                description: "Delegate a task to a foreground subagent and wait for its answer."
                    .into(),
                parameters: json!({"type":"object", "properties":{
                "task":{"type":"string","minLength":1,"maxLength":65536}
            },"required":["task"],"additionalProperties":false}),
            },
            KIND,
            true,
            1,
            handler.clone(),
        )
        .with_direct_binding(binding),
    );
    (
        Extension {
            name: extension_name(parent),
            tools: vec![adapter.register().tool],
            ..Extension::default()
        },
        handler,
    )
}

struct Foreground {
    harness: Mutex<Harness>,
    parent: Uuid,
    config: Config,
    allowed_tools: Option<Vec<String>>,
    schedule: tokio::sync::mpsc::UnboundedSender<Uuid>,
}

impl Handler for Foreground {
    fn run(&self, task: &Task) -> Result<Step, String> {
        if task.owner() != &TaskOwner::Conversation(self.parent) {
            return Err("foreground subagent task has a different owner".into());
        }
        let content = task
            .input()
            .get("task")
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty() && text.len() <= 65536)
            .ok_or("invalid foreground subagent task")?;
        let mut harness = self
            .harness
            .lock()
            .map_err(|_| "foreground harness lock poisoned")?;
        let admission = harness
            .submit_task_owned_subagent_notified(
                task.id(),
                task.owner().clone(),
                "child",
                self.config.clone(),
                content.into(),
                self.allowed_tools.clone(),
                SIGNAL.into(),
            )
            .map_err(|e| format!("foreground admission: {e}"))?;
        let token = crate::runtime::submission_settled_signal();
        let receipt = TaskStore::new(harness.subagent_store().map_err(|e| e.to_string())?)
            .read_typed_signal(task.id(), &token, SIGNAL)
            .map_err(|e| e.to_string())?;
        if let Some(receipt) = receipt {
            let submission = admission
                .submission
                .as_ref()
                .ok_or("missing initial submission")?;
            if receipt["id"] != json!(submission.id)
                || receipt["conversationId"] != json!(admission.child)
            {
                return Err("foreground settlement identity mismatch".into());
            }
            if receipt["status"] == "done" {
                return Ok(Step::Succeed(Value::String(
                    receipt["answer"].as_str().ok_or("missing answer")?.into(),
                )));
            }
            return Ok(Step::Succeed(
                json!({"foreground_error": format!("Subagent ended: {}", receipt["status"].as_str().unwrap_or("unknown"))}),
            ));
        }
        self.schedule
            .send(admission.child)
            .map_err(|_| "foreground scheduler stopped")?;
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "waiting".into(),
                data: Value::Null,
            },
            condition: WaitFor::TypedSignal(token.key(SIGNAL).map_err(|e| e.to_string())?),
            children: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        recovery::UncertainPolicy,
        runtime::{Options, RuntimeOwner, Status},
        task_scheduler::Registry,
        task_store::Signal,
    };
    use async_trait::async_trait;
    use futures_util::{StreamExt, stream};
    use pi_rs_core::{Error, Message, Model, ModelEvent, ModelRequest, ModelStream, ToolCall};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct ModelStub {
        parent_dispatches: Arc<AtomicUsize>,
        child_dispatches: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Model for ModelStub {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
            let child = request
                .messages
                .iter()
                .any(|m| matches!(m, Message::User { content } if content == "child job"));
            let result = request.messages.iter().find_map(|m| match m {
                Message::Tool { content, .. } => Some(content.clone()),
                _ => None,
            });
            let (text, calls) = if let Some(result) = result {
                if child {
                    ("child answer".to_string(), vec![])
                } else {
                    (format!("parent saw: {result}"), vec![])
                }
            } else if child {
                self.child_dispatches.fetch_add(1, Ordering::SeqCst);
                (
                    String::new(),
                    vec![ToolCall {
                        id: "ask".into(),
                        name: "ask_person".into(),
                        arguments: "{}".into(),
                    }],
                )
            } else {
                self.parent_dispatches.fetch_add(1, Ordering::SeqCst);
                (
                    String::new(),
                    vec![ToolCall {
                        id: "delegate".into(),
                        name: "subagent".into(),
                        arguments: "{\"task\":\"child job\"}".into(),
                    }],
                )
            };
            Ok(stream::iter(vec![
                Ok(ModelEvent::Text(text)),
                Ok(ModelEvent::Complete { calls, usage: None }),
            ])
            .boxed())
        }
    }
    struct Ask {
        id: Arc<Mutex<Option<Uuid>>>,
        starts: Arc<AtomicUsize>,
    }
    impl Handler for Ask {
        fn run(&self, task: &Task) -> Result<Step, String> {
            if task.checkpoint().phase == "initial" {
                self.starts.fetch_add(1, Ordering::SeqCst);
                *self.id.lock().unwrap() = Some(task.id());
                Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "waiting".into(),
                        data: Value::Null,
                    },
                    condition: WaitFor::Signal("go".into()),
                    children: vec![],
                })
            } else {
                Ok(Step::Succeed(json!("yes")))
            }
        }
    }
    fn config() -> Config {
        Config {
            model: "fake".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        }
    }
    async fn until(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(15), async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn foreground_extension_parks_reopens_and_returns_child_answer_without_redispatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let parent_dispatches = Arc::new(AtomicUsize::new(0));
        let child_dispatches = Arc::new(AtomicUsize::new(0));
        let starts = Arc::new(AtomicUsize::new(0));
        let ask_id = Arc::new(Mutex::new(None));
        let handler: Arc<dyn Handler> = Arc::new(Ask {
            id: ask_id.clone(),
            starts: starts.clone(),
        });
        let adapter = Arc::new(TaskTool::new(
            ToolSpec {
                name: "ask_person".into(),
                description: "ask".into(),
                parameters: json!({"type":"object"}),
            },
            "test.ask",
            true,
            1,
            handler.clone(),
        ));
        let options = || Options {
            model: Arc::new(ModelStub {
                parent_dispatches: parent_dispatches.clone(),
                child_dispatches: child_dispatches.clone(),
            }),
            tools: vec![adapter.register()],
            restart_limit: 5,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        };
        let tasks = || {
            let mut registry = Registry::default();
            assert!(registry.register_shared("test.ask", true, handler.clone()));
            registry
        };
        let mut initial = Harness::open_paused(&path, options()).unwrap();
        let parent = initial.create_conversation(config()).unwrap();
        drop(initial);
        let mut owner = RuntimeOwner::open_with_foreground_subagent(
            &path,
            options(),
            crate::extension::Registry::default(),
            tasks(),
            parent,
            config(),
            Some(vec!["ask_person".into()]),
        )
        .unwrap();
        owner
            .scheduler_mut()
            .select_extensions(parent, vec![extension_name(parent)])
            .unwrap();
        let other = owner.scheduler_mut().create_conversation(config()).unwrap();
        assert!(
            owner
                .scheduler_mut()
                .select_extensions(other, vec![extension_name(parent)])
                .is_err()
        );
        owner
            .scheduler_mut()
            .submit(parent, None, "root".into())
            .unwrap();
        until(|| ask_id.lock().unwrap().is_some()).await;
        let ask = ask_id.lock().unwrap().unwrap();
        until(|| {
            matches!(
                owner.tasks().get(ask).unwrap().unwrap().state(),
                crate::task::TaskState::Waiting(_)
            )
        })
        .await;
        let parent_state = owner.scheduler().conversation(parent).unwrap();
        let caller = parent_state.tool_attempt().unwrap().operation_id;
        let caller_task = owner.tasks().get(caller).unwrap().unwrap();
        assert_eq!(caller_task.owner(), &TaskOwner::Conversation(parent));
        assert!(matches!(
            caller_task.state(),
            crate::task::TaskState::Waiting(_)
        ));
        let child = match owner.tasks().get(ask).unwrap().unwrap().owner() {
            TaskOwner::Conversation(child) => *child,
            _ => panic!("ask task must belong to the child conversation"),
        };
        assert_ne!(child, parent);
        let child_state = owner.scheduler().conversation(child).unwrap();
        assert!(child_state.extensions.is_empty());
        assert_eq!(
            child_state
                .submission_notification(child_state.submissions[0].id)
                .unwrap()
                .task_id,
            caller
        );
        owner.shutdown().await.unwrap();
        assert!(
            RuntimeOwner::open_with_foreground_subagent(
                &path,
                options(),
                crate::extension::Registry::default(),
                tasks(),
                parent,
                config(),
                Some(vec![])
            )
            .is_err()
        );
        let owner = RuntimeOwner::open_with_foreground_subagent(
            &path,
            options(),
            crate::extension::Registry::default(),
            tasks(),
            parent,
            config(),
            Some(vec!["ask_person".into()]),
        )
        .unwrap();
        owner
            .tasks()
            .signal(&Signal::new(Uuid::new_v4(), ask, "go".into(), json!(null)))
            .unwrap();
        until(|| {
            matches!(
                owner.scheduler().conversation(parent).unwrap().submissions[0].status,
                Status::Done { .. }
            )
        })
        .await;
        let conversation = owner.scheduler().conversation(parent).unwrap();
        assert!(
            matches!(&conversation.submissions[0].status, Status::Done { answer } if answer == "parent saw: child answer")
        );
        assert_eq!(parent_dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(child_dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        owner.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn child_abort_returns_known_tool_failure() {
        for abort_parent in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let ask_id = Arc::new(Mutex::new(None));
            let handler: Arc<dyn Handler> = Arc::new(Ask {
                id: ask_id.clone(),
                starts: Arc::new(AtomicUsize::new(0)),
            });
            let adapter = Arc::new(TaskTool::new(
                ToolSpec {
                    name: "ask_person".into(),
                    description: "ask".into(),
                    parameters: json!({"type":"object"}),
                },
                "test.ask",
                true,
                1,
                handler.clone(),
            ));
            let options = || Options {
                model: Arc::new(ModelStub {
                    parent_dispatches: Arc::new(AtomicUsize::new(0)),
                    child_dispatches: Arc::new(AtomicUsize::new(0)),
                }),
                tools: vec![adapter.register()],
                restart_limit: 5,
                max_turns: 4,
                uncertain_policy: UncertainPolicy::RequireReconciliation,
            };
            let mut tasks = Registry::default();
            assert!(tasks.register_shared("test.ask", true, handler));
            assert!(tasks.set_abort("test.ask", |_: &Task| Ok(Step::Succeed(Value::Null))));
            let mut h = Harness::open_paused(&path, options()).unwrap();
            let parent = h.create_conversation(config()).unwrap();
            drop(h);
            let mut owner = RuntimeOwner::open_with_foreground_subagent(
                &path,
                options(),
                crate::extension::Registry::default(),
                tasks,
                parent,
                config(),
                Some(vec!["ask_person".into()]),
            )
            .unwrap();
            owner
                .scheduler_mut()
                .select_extensions(parent, vec![extension_name(parent)])
                .unwrap();
            owner
                .scheduler_mut()
                .submit(parent, None, "root".into())
                .unwrap();
            until(|| ask_id.lock().unwrap().is_some()).await;
            let ask = ask_id.lock().unwrap().unwrap();
            until(|| {
                matches!(
                    owner.tasks().get(ask).unwrap().unwrap().state(),
                    crate::task::TaskState::Waiting(_)
                )
            })
            .await;
            let child = match owner.tasks().get(ask).unwrap().unwrap().owner() {
                TaskOwner::Conversation(child) => *child,
                _ => panic!("ask task must belong to child"),
            };
            if abort_parent {
                owner.scheduler_mut().abort_and_wait(parent).await.unwrap();
                let parent_state = owner.scheduler().conversation(parent).unwrap();
                let child_state = owner.scheduler().conversation(child).unwrap();
                assert!(matches!(
                    parent_state.submissions[0].status,
                    Status::Unanswered { .. }
                ));
                assert!(matches!(
                    child_state.submissions[0].status,
                    Status::Unanswered { .. }
                ));
                owner.shutdown().await.unwrap();
                continue;
            }
            owner.scheduler_mut().abort(child).unwrap();
            let finished = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if matches!(
                        owner.scheduler().conversation(parent).unwrap().submissions[0].status,
                        Status::Done { .. }
                    ) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await;
            if finished.is_err() {
                let parent_state = owner.scheduler().conversation(parent).unwrap();
                let caller = parent_state
                    .tool_attempt()
                    .map(|attempt| attempt.operation_id);
                panic!(
                    "parent={parent_state:?} child={:?} caller={:?} ask={:?} reporter={:?}",
                    owner.scheduler().conversation(child),
                    caller.map(|id| owner.tasks().get(id)),
                    owner.tasks().get(ask),
                    owner.scheduler().reporter_error()
                );
            }
            let conversation = owner.scheduler().conversation(parent).unwrap();
            assert!(
                conversation
                    .tool_results
                    .iter()
                    .any(|result| result.tool == "subagent" && result.is_error)
            );
            assert!(
                matches!(&conversation.submissions[0].status, Status::Done { answer } if answer.contains("Subagent ended: unanswered"))
            );
            owner.shutdown().await.unwrap();
        }
    }
}
