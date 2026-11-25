//! Persist the preflight results so a resumed run does not repeat completed callbacks.
use std::time::Duration;

use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    AgentStartTrigger, BeforeAgentStartEvent, CancellationToken, CustomMessage, Error, Event,
    Notification, Observer, Record, Registry, Result, Session,
};

const PLAN_FORMAT: &str = "pi-rs:before-agent-start-v1";

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Plan {
    system_prompt: Option<String>,
    pub(crate) messages: Vec<(Uuid, CustomMessage)>,
}

fn recorded_plan<'a>(branch: &[&'a Record], run_id: Uuid) -> Option<&'a Value> {
    branch.iter().rev().find_map(|record| match &record.event {
        Event::SourceEntry { format, entry }
            if record.run_id == Some(run_id) && format == PLAN_FORMAT =>
        {
            Some(entry)
        }
        _ => None,
    })
}

pub(crate) fn prompt_override(branch: &[&Record], run_id: Uuid) -> Option<String> {
    recorded_plan(branch, run_id)?
        .get("systemPrompt")?
        .as_str()
        .map(str::to_owned)
}

pub(crate) fn plan(session: &Session, run_id: Uuid) -> Result<Option<Plan>> {
    recorded_plan(&session.branch(), run_id)
        .map(|entry| serde_json::from_value(entry.clone()).map_err(Error::from))
        .transpose()
}

fn starting_input(
    branch: &[&Record],
    run_id: Uuid,
    effective_input: Option<&str>,
) -> Option<AgentStartTrigger> {
    if let Some(prompt) = effective_input {
        return Some(AgentStartTrigger::User {
            prompt: prompt.into(),
            images: Vec::new(),
        });
    }
    branch.iter().find_map(|record| match &record.event {
        Event::User {
            content,
            input_id: Some(id),
        } if *id == run_id => Some(AgentStartTrigger::User {
            prompt: content.clone(),
            images: Vec::new(),
        }),
        Event::UserWithImages {
            content,
            input_id: Some(id),
            images,
        } if *id == run_id => Some(AgentStartTrigger::User {
            prompt: content.clone(),
            images: images.clone(),
        }),
        Event::CustomMessageQueued { input } | Event::TreeMessageQueued { input }
            if input.id == run_id
                && input.delivery == crate::CustomMessageDelivery::TriggerTurn =>
        {
            Some(AgentStartTrigger::Custom {
                message: input.message.clone(),
            })
        }
        _ => None,
    })
}

pub(crate) async fn prepare(
    registry: &Registry,
    session: &mut Session,
    run_id: Uuid,
    effective_input: Option<&str>,
    fallback_prompt: &str,
    timeout: Duration,
    cancel: &CancellationToken,
    observer: &mut Observer<'_>,
) -> Result<()> {
    if !registry
        .extensions
        .iter()
        .any(|extension| extension.handles_before_agent_start())
    {
        return Ok(());
    }
    let branch = session.branch();
    if recorded_plan(&branch, run_id).is_some()
        || branch.iter().any(|record| {
            record.run_id == Some(run_id) && matches!(record.event, Event::ModelRequested { .. })
        })
    {
        return Ok(());
    }
    let Some(trigger) = starting_input(&branch, run_id, effective_input) else {
        return Ok(());
    };
    let mut event = BeforeAgentStartEvent {
        trigger,
        system_prompt: crate::journal::system_context(&branch, fallback_prompt, Vec::new()).0,
    };
    let states = session.extension_states();
    let mut plan = Plan::default();
    let deadline = crate::runtime::time::Instant::now() + timeout.min(Duration::from_secs(10));
    for extension in &registry.extensions {
        if !extension.handles_before_agent_start() {
            continue;
        }
        let callback = std::panic::AssertUnwindSafe(async {
            extension
                .before_agent_start(&event, states.get(extension.id()))
                .await
        })
        .catch_unwind();
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            result = crate::runtime::time::timeout_at(deadline, callback) => match result {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err("handler panicked".into()),
                Err(_) => {
                    report(extension.id(), "handler deadline expired", observer);
                    break;
                }
            }
        };
        match result {
            Ok(result) => {
                if let Some(message) = result.message {
                    match crate::journal::validate_custom_message(&message) {
                        Ok(()) => plan.messages.push((Uuid::new_v4(), message)),
                        Err(error) => report(extension.id(), &error.to_string(), observer),
                    }
                }
                if let Some(prompt) = result.system_prompt {
                    event.system_prompt.clone_from(&prompt);
                    plan.system_prompt = Some(prompt);
                }
            }
            Err(error) => report(extension.id(), &error, observer),
        }
    }
    if cancel.is_cancelled() {
        return Err(Error::Cancelled);
    }
    let record = session.append(Event::SourceEntry {
        format: PLAN_FORMAT.into(),
        entry: serde_json::to_value(plan)?,
    })?;
    observer(Notification::Committed(record));
    Ok(())
}

fn report(extension: &str, message: &str, observer: &mut Observer<'_>) {
    observer(Notification::ExtensionLifecycleError {
        extension,
        event: "before_agent_start",
        message,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ContextContent, Extension, ExtensionResult, ExtensionState, Message, Model, ModelEvent,
        ModelRequest, ModelStream, SessionBuilder,
    };
    use std::sync::{Arc, Mutex};

    struct Capture(Arc<Mutex<Vec<Vec<Message>>>>);

    #[async_trait::async_trait]
    impl Model for Capture {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
            self.0.lock().unwrap().push(request.messages);
            Ok(Box::pin(futures_util::stream::iter([Ok(
                ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                },
            )])))
        }
    }

    fn message(text: &str) -> CustomMessage {
        CustomMessage {
            custom_type: "preflight".into(),
            content: ContextContent::Text(text.into()),
            display: false,
            details: None,
        }
    }

    struct Hook {
        id: &'static str,
        observations: Arc<Mutex<Vec<(&'static str, BeforeAgentStartEvent)>>>,
    }

    #[async_trait::async_trait]
    impl Extension for Hook {
        fn id(&self) -> &str {
            self.id
        }

        fn handles_before_agent_start(&self) -> bool {
            true
        }

        async fn before_agent_start(
            &self,
            event: &BeforeAgentStartEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<crate::BeforeAgentStartResult> {
            self.observations
                .lock()
                .unwrap()
                .push((self.id, event.clone()));
            if self.id == "broken" {
                return Err("expected test error".into());
            }
            if matches!(event.trigger, AgentStartTrigger::Custom { .. }) {
                return Ok(Default::default());
            }
            Ok(crate::BeforeAgentStartResult {
                message: Some(message(self.id)),
                system_prompt: Some(self.id.into()),
            })
        }
    }

    #[tokio::test]
    async fn before_agent_start_orders_results_and_keeps_overrides_run_local() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observations = Arc::new(Mutex::new(Vec::new()));
        let mut builder = SessionBuilder::new(Arc::new(Capture(requests.clone())), "fixture")
            .system_prompt("host prompt");
        for id in ["first", "broken", "last"] {
            builder = builder
                .extension(Arc::new(Hook {
                    id,
                    observations: observations.clone(),
                }))
                .unwrap();
        }
        let mut session = builder.in_memory().unwrap();
        session
            .queue_custom_message_next_turn(message("queued"))
            .unwrap();
        session.prompt("starting input").await.unwrap();
        session
            .send_custom_message(message("custom trigger"), true)
            .await
            .unwrap();

        let observations = observations.lock().unwrap();
        assert_eq!(
            observations
                .iter()
                .map(|(id, event)| (*id, event.system_prompt.as_str()))
                .collect::<Vec<_>>(),
            [
                ("first", "host prompt"),
                ("broken", "first"),
                ("last", "first"),
                ("first", "host prompt"),
                ("broken", "host prompt"),
                ("last", "host prompt"),
            ]
        );
        assert!(matches!(&observations[0].1.trigger,
            AgentStartTrigger::User { prompt, images } if prompt == "starting input" && images.is_empty()));
        assert!(matches!(&observations[3].1.trigger,
            AgentStartTrigger::Custom { message } if matches!(&message.content, ContextContent::Text(text) if text == "custom trigger")));
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(matches!(&requests[0][0], Message::System { content } if content == "last"));
        assert!(matches!(&requests[1][0], Message::System { content } if content == "host prompt"));
        let first_request: Vec<_> = requests[0]
            .iter()
            .filter_map(|message| match message {
                Message::User { content } => Some(content.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(first_request, ["starting input", "queued", "first", "last"]);
    }

    #[tokio::test]
    async fn before_agent_start_reuses_recorded_effects_after_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let mut session = Session::open(&path).unwrap();
        let run_id = Uuid::new_v4();
        let observations = Arc::new(Mutex::new(Vec::new()));
        let mut registry = Registry::default();
        registry
            .register(Arc::new(Hook {
                id: "first",
                observations: observations.clone(),
            }))
            .unwrap();
        registry
            .register(Arc::new(Hook {
                id: "last",
                observations: observations.clone(),
            }))
            .unwrap();
        let cancel = CancellationToken::new();
        session.set_run_id(Some(run_id));
        session
            .append(Event::Run {
                state: crate::RunState::Running,
                initial: Some("question".into()),
            })
            .unwrap();
        prepare(
            &registry,
            &mut session,
            run_id,
            Some("question"),
            "host prompt",
            Duration::from_secs(1),
            &cancel,
            &mut |_| {},
        )
        .await
        .unwrap();
        let (id, message) = plan(&session, run_id).unwrap().unwrap().messages.remove(0);
        session
            .append(Event::User {
                content: "question".into(),
                input_id: Some(run_id),
            })
            .unwrap();
        session
            .append(Event::CustomMessageDelivered { id, message })
            .unwrap();
        drop(session);

        let mut session = Session::open(&path).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let model = Capture(requests.clone());
        let agent = crate::Agent {
            registry: &registry,
            model: &model,
            model_name: "fixture",
            system_prompt: "host prompt",
            limits: crate::Limits::default(),
        };
        let (_send, mut inputs) = tokio::sync::mpsc::channel::<crate::AgentInput>(1);
        agent
            .run_correlated(
                &mut session,
                run_id,
                None,
                &mut inputs,
                &cancel,
                &mut |_| {},
            )
            .await
            .unwrap();
        assert_eq!(observations.lock().unwrap().len(), 2);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(matches!(&requests[0][0], Message::System { content } if content == "last"));
        let texts: Vec<_> = requests[0]
            .iter()
            .filter_map(|message| match message {
                Message::User { content } => Some(content.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(texts, ["question", "first", "last"]);
    }

    #[tokio::test]
    async fn before_agent_start_cancellation_prevents_prompt_dispatch() {
        struct Waiting(Arc<tokio::sync::Notify>);

        #[async_trait::async_trait]
        impl Extension for Waiting {
            fn id(&self) -> &str {
                "waiting"
            }

            fn handles_before_agent_start(&self) -> bool {
                true
            }

            async fn before_agent_start(
                &self,
                _: &BeforeAgentStartEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<crate::BeforeAgentStartResult> {
                self.0.notify_one();
                std::future::pending().await
            }
        }

        let waiting = Arc::new(tokio::sync::Notify::new());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut registry = Registry::default();
        registry
            .register(Arc::new(Waiting(waiting.clone())))
            .unwrap();
        let model = Capture(requests.clone());
        let agent = crate::Agent {
            registry: &registry,
            model: &model,
            model_name: "fixture",
            system_prompt: "host prompt",
            limits: crate::Limits::default(),
        };
        let directory = tempfile::tempdir().unwrap();
        let mut session = Session::open(directory.path().join("session.jsonl")).unwrap();
        let cancel = CancellationToken::new();
        let run_id = Uuid::new_v4();
        let (_send, mut inputs) = tokio::sync::mpsc::channel::<crate::AgentInput>(1);
        let mut observe = |_: Notification<'_>| {};
        let (result, ()) = tokio::join!(
            agent.run_correlated(
                &mut session,
                run_id,
                Some("question".into()),
                &mut inputs,
                &cancel,
                &mut observe,
            ),
            async {
                tokio::select! {
                    _ = waiting.notified() => cancel.cancel(),
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        panic!("run did not enter before_agent_start");
                    }
                }
            },
        );

        assert!(matches!(result, Err(Error::Cancelled)));
        assert!(requests.lock().unwrap().is_empty());
        assert!(plan(&session, run_id).unwrap().is_none());
    }
}
