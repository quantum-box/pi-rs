//! Persist the preflight results so a resumed run does not repeat completed callbacks.
use std::time::Duration;

use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    Agent, AgentInput, AgentStartTrigger, BeforeAgentStartEvent, CancellationToken, CustomMessage,
    CustomMessageDelivery, Error, Event, Notification, Observer, Record, Result, RunState, Session,
};

const PLAN_FORMAT: &str = "pi-rs:before-agent-start-v1";

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Plan {
    system_prompt: Option<String>,
    pub(crate) messages: Vec<(Uuid, CustomMessage)>,
    // New plans use their own journal position as the next-turn queue cutoff.
    #[serde(default)]
    pub(crate) captures_next_turn: bool,
}

fn recorded_plan<'a>(branch: &[&'a Record], run_id: Uuid) -> Option<(u64, &'a Value)> {
    branch.iter().rev().find_map(|record| match &record.event {
        Event::SourceEntry { format, entry }
            if record.run_id == Some(run_id) && format == PLAN_FORMAT =>
        {
            Some((record.id, entry))
        }
        _ => None,
    })
}

pub(crate) fn prompt_override(branch: &[&Record], run_id: Uuid) -> Option<String> {
    recorded_plan(branch, run_id)?
        .1
        .get("systemPrompt")?
        .as_str()
        .map(str::to_owned)
}

pub(crate) fn plan(session: &Session, run_id: Uuid) -> Result<Option<(u64, Plan)>> {
    recorded_plan(&session.branch(), run_id)
        .map(|(id, entry)| Ok((id, serde_json::from_value(entry.clone())?)))
        .transpose()
}

pub(crate) fn pending_run(session: &Session) -> Option<Uuid> {
    let branch = session.branch();
    let run = branch
        .iter()
        .rev()
        .find(|record| matches!(record.event, Event::Run { .. }))?;
    let run_id = run.run_id?;
    if !matches!(
        run.event,
        Event::Run {
            state: RunState::Running
                | RunState::WaitingForOperation
                | RunState::WaitingForReconciliation,
            ..
        }
    ) {
        return None;
    }
    let (plan_id, _) = recorded_plan(&branch, run_id)?;
    // A custom trigger already has its own recovery protocol, including branch
    // handoff. Provider dispatch and tool recovery likewise have their own rules.
    let user_run = branch.iter().any(|record| match &record.event {
        Event::Run {
            initial: Some(_), ..
        } => record.run_id == Some(run_id),
        Event::User { input_id, .. } | Event::UserWithImages { input_id, .. } => {
            *input_id == Some(run_id)
        }
        _ => false,
    });
    let advanced = session.records()[plan_id as usize..].iter().any(|record| {
        record.run_id == Some(run_id)
            && matches!(
                record.event,
                Event::ModelRequested { .. }
                    | Event::Run {
                        state: RunState::Completed | RunState::Failed | RunState::Cancelled,
                        ..
                    }
            )
    });
    (user_run && !advanced).then_some(run_id)
}

fn starting_input(
    session: &Session,
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
    branch
        .iter()
        .find_map(|record| match &record.event {
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
                if input.id == run_id && input.delivery == CustomMessageDelivery::TriggerTurn =>
            {
                Some(AgentStartTrigger::Custom {
                    message: input.message.clone(),
                })
            }
            Event::CustomMessageDelivered { id, message } if *id == run_id => {
                Some(AgentStartTrigger::Custom {
                    message: message.clone(),
                })
            }
            _ => None,
        })
        .or_else(|| {
            session
                .queued_custom_messages()
                .into_iter()
                .find_map(|(_, input)| {
                    (input.id == run_id && input.delivery == CustomMessageDelivery::TriggerTurn)
                        .then_some(AgentStartTrigger::Custom {
                            message: input.message,
                        })
                })
        })
}

pub(crate) async fn prepare<T: Into<AgentInput> + Send>(
    agent: &Agent<'_>,
    session: &mut Session,
    run_id: Uuid,
    effective_input: Option<&str>,
    inputs: &mut mpsc::Receiver<T>,
    cancel: &CancellationToken,
    observer: &mut Observer<'_>,
) -> Result<()> {
    if !agent
        .registry
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
    let Some(trigger) = starting_input(session, &branch, run_id, effective_input) else {
        return Ok(());
    };
    let mut event = BeforeAgentStartEvent {
        trigger,
        system_prompt: crate::journal::system_context(&branch, agent.system_prompt, Vec::new()).0,
    };
    let states = session.extension_states();
    let mut plan = Plan {
        captures_next_turn: true,
        ..Plan::default()
    };
    let deadline = crate::runtime::time::Instant::now()
        + agent.limits.model_timeout.min(Duration::from_secs(10));
    'handlers: for extension in &agent.registry.extensions {
        if !extension.handles_before_agent_start() {
            continue;
        }
        let result = {
            let callback = std::panic::AssertUnwindSafe(async {
                extension
                    .before_agent_start(&event, states.get(extension.id()))
                    .await
            })
            .catch_unwind();
            let callback = crate::runtime::time::timeout_at(deadline, callback);
            tokio::pin!(callback);
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(Error::Cancelled),
                    result = &mut callback => match result {
                        Ok(Ok(result)) => break result,
                        Ok(Err(_)) => break Err("handler panicked".into()),
                        Err(_) => {
                            report(extension.id(), "handler deadline expired", observer);
                            break 'handlers;
                        }
                    },
                    Some(input) = inputs.recv() => {
                        crate::agent::queue_input(session, input.into(), observer)?;
                    }
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
    crate::agent::queue_inputs(session, inputs, observer)?;
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
        ModelRequest, ModelStream, Registry, SessionBuilder,
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
                system_prompt: Some(if self.id == "clear" { "" } else { self.id }.into()),
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
    async fn before_agent_start_sdk_resume_reuses_saved_plan_and_run_id() {
        enum Resume {
            BySession,
            WithObserver,
            WithEventObserver,
        }
        for (partially_delivered, resume) in [
            (false, Resume::BySession),
            (true, Resume::BySession),
            (false, Resume::WithObserver),
            (true, Resume::WithEventObserver),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("session.jsonl");
            let mut journal = Session::open(&path).unwrap();
            let run_id = Uuid::new_v4();
            let first_id = Uuid::new_v4();
            let last_id = Uuid::new_v4();
            journal.set_run_id(Some(run_id));
            journal
                .append(Event::Run {
                    state: RunState::Running,
                    initial: Some("question".into()),
                })
                .unwrap();
            crate::agent::queue_custom_message(
                &mut journal,
                crate::QueuedCustomMessage {
                    id: Uuid::new_v4(),
                    message: message("during preflight"),
                    delivery: CustomMessageDelivery::NextTurn,
                },
                &mut |_| {},
            )
            .unwrap();
            let queue = journal.queued_custom_messages().pop().unwrap().1;
            let plan_id = journal
                .append(Event::SourceEntry {
                    format: PLAN_FORMAT.into(),
                    entry: serde_json::to_value(Plan {
                        captures_next_turn: true,
                        system_prompt: Some("saved prompt".into()),
                        messages: vec![(first_id, message("first")), (last_id, message("last"))],
                    })
                    .unwrap(),
                })
                .unwrap()
                .id;
            if partially_delivered {
                journal
                    .append(Event::User {
                        content: "question".into(),
                        input_id: Some(run_id),
                    })
                    .unwrap();
                for (id, message) in [(queue.id, queue.message), (first_id, message("first"))] {
                    journal
                        .append(Event::CustomMessageDelivered { id, message })
                        .unwrap();
                }
            }
            journal.set_run_id(None);
            crate::agent::queue_custom_message(
                &mut journal,
                crate::QueuedCustomMessage {
                    id: Uuid::new_v4(),
                    message: message("after the cutoff"),
                    delivery: CustomMessageDelivery::NextTurn,
                },
                &mut |_| {},
            )
            .unwrap();
            drop(journal);

            let observations = Arc::new(Mutex::new(Vec::new()));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let mut sdk = SessionBuilder::new(Arc::new(Capture(requests.clone())), "fixture")
                .extension(Arc::new(Hook {
                    id: "unexpected",
                    observations: observations.clone(),
                }))
                .unwrap()
                .open(&path)
                .unwrap();
            assert!(matches!(
                sdk.prompt("new prompt").await,
                Err(Error::Session(_))
            ));
            assert!(matches!(
                sdk.run(None, Uuid::new_v4(), &mut |_| {}).await,
                Err(Error::Session(_))
            ));
            assert!(matches!(
                sdk.run(Some("new prompt".into()), run_id, &mut |_| {})
                    .await,
                Err(Error::Session(_))
            ));
            let mut observed_run = None;
            let mut observe = |notification: Notification<'_>| {
                if let Notification::RunStarted { run_id } = notification {
                    observed_run = Some(run_id);
                }
            };
            match resume {
                Resume::BySession => {
                    sdk.resume().await.unwrap();
                }
                Resume::WithObserver => {
                    sdk.run(None, run_id, &mut observe).await.unwrap();
                    assert_eq!(observed_run, Some(run_id));
                }
                Resume::WithEventObserver => {
                    sdk.run_with_events(None, run_id, &mut |event| {
                        if let crate::AgentEvent::Notification(notification) = event {
                            observe(notification);
                        }
                    })
                    .await
                    .unwrap();
                    assert_eq!(observed_run, Some(run_id));
                }
            }
            assert!(observations.lock().unwrap().is_empty());
            let journal = sdk.journal().unwrap();
            assert!(
                journal
                    .records()
                    .iter()
                    .any(|record| record.run_id == Some(run_id)
                        && matches!(
                            record.event,
                            Event::Run {
                                state: RunState::Completed,
                                ..
                            }
                        ))
            );
            assert_eq!(journal.pending_before_agent_start_run(), None);
            assert_eq!(journal.queued_custom_messages().len(), 1);
            let mut historical = Session::from_records(journal.records().to_vec()).unwrap();
            historical.checkout(plan_id).unwrap();
            assert_eq!(historical.pending_before_agent_start_run(), None);
            drop(journal);

            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert!(
                matches!(&requests[0][0], Message::System { content } if content == "saved prompt")
            );
            assert_eq!(
                user_texts(&requests[0]),
                ["question", "during preflight", "first", "last"]
            );
        }
    }

    fn user_texts(messages: &[Message]) -> Vec<&str> {
        messages
            .iter()
            .filter_map(|message| match message {
                Message::User { content } => Some(content.as_str()),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn before_agent_start_observes_tree_trigger_after_checkout() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let mut journal = Session::open(&path).unwrap();
        let trigger_id = Uuid::new_v4();
        journal
            .append(Event::TreeMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: trigger_id,
                    message: message("tree trigger"),
                    delivery: CustomMessageDelivery::TriggerTurn,
                },
            })
            .unwrap();
        journal.checkout(1).unwrap();
        drop(journal);

        let observations = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut sdk = SessionBuilder::new(Arc::new(Capture(requests.clone())), "fixture")
            .extension(Arc::new(Hook {
                id: "observe",
                observations: observations.clone(),
            }))
            .unwrap()
            .open(&path)
            .unwrap();
        sdk.start().await.unwrap();
        assert!(matches!(observations.lock().unwrap().as_slice(),
            [(_, BeforeAgentStartEvent { trigger: AgentStartTrigger::Custom { message }, .. })]
                if message.content == ContextContent::Text("tree trigger".into())));
        assert_eq!(user_texts(&requests.lock().unwrap()[0]), ["tree trigger"]);
        assert_eq!(sdk.journal().unwrap().pending_custom_trigger(), None);
    }

    #[tokio::test]
    async fn before_agent_start_forces_final_prompt_after_context_transforms() {
        struct Rewrite;
        #[async_trait::async_trait]
        impl Extension for Rewrite {
            fn id(&self) -> &str {
                "rewrite"
            }
            fn handles_context_with_system(&self) -> bool {
                true
            }
            async fn context_with_system(
                &self,
                messages: &mut crate::ContextMessages<'_>,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<crate::ContextChange> {
                messages.remove(0);
                messages.push(Message::System {
                    content: "ctx replacement".into(),
                });
                messages.push(Message::User {
                    content: "ctx user edit".into(),
                });
                Ok(crate::ContextChange::Changed)
            }
        }
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observations = Arc::new(Mutex::new(Vec::new()));
        let mut sdk = SessionBuilder::new(Arc::new(Capture(requests.clone())), "fixture")
            .extension(Arc::new(Hook {
                id: "first",
                observations: observations.clone(),
            }))
            .unwrap()
            .extension(Arc::new(Hook {
                id: "clear",
                observations: observations.clone(),
            }))
            .unwrap()
            .extension(Arc::new(Rewrite))
            .unwrap()
            .in_memory()
            .unwrap();
        sdk.prompt("question").await.unwrap();
        let requests = requests.lock().unwrap();
        assert!(matches!(&requests[0][0], Message::System { content } if content.is_empty()));
        assert!(
            !requests[0][1..]
                .iter()
                .any(|message| matches!(message, Message::System { .. }))
        );
        assert!(user_texts(&requests[0]).contains(&"ctx user edit"));
    }

    #[tokio::test]
    async fn before_agent_start_consumes_next_turn_arrivals_before_hook_messages() {
        struct Paused {
            entered: tokio::sync::Notify,
            release: tokio::sync::Notify,
        }
        #[async_trait::async_trait]
        impl Extension for Paused {
            fn id(&self) -> &str {
                "paused"
            }
            fn handles_before_agent_start(&self) -> bool {
                true
            }
            async fn before_agent_start(
                &self,
                _: &BeforeAgentStartEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<crate::BeforeAgentStartResult> {
                self.entered.notify_one();
                self.release.notified().await;
                Ok(crate::BeforeAgentStartResult {
                    message: Some(message("hook message")),
                    system_prompt: None,
                })
            }
        }
        let paused = Arc::new(Paused {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut sdk = SessionBuilder::new(Arc::new(Capture(requests.clone())), "fixture")
            .extension(paused.clone())
            .unwrap()
            .in_memory()
            .unwrap();
        sdk.queue_custom_message_next_turn(message("already queued"))
            .unwrap();
        let control = sdk.controls();
        tokio::time::timeout(Duration::from_secs(2), async {
            let (result, ()) = tokio::join!(sdk.prompt("question"), async {
                paused.entered.notified().await;
                control
                    .send_custom_message(
                        message("arrived during hook"),
                        CustomMessageDelivery::NextTurn,
                    )
                    .await
                    .unwrap();
                paused.release.notify_one();
            });
            result.unwrap();
        })
        .await
        .unwrap();
        assert_eq!(
            user_texts(&requests.lock().unwrap()[0]),
            [
                "question",
                "already queued",
                "arrived during hook",
                "hook message"
            ]
        );
    }

    #[tokio::test]
    async fn before_agent_start_abort_interrupts_returned_message_lifecycle() {
        struct PausedMessage(tokio::sync::Notify);
        #[async_trait::async_trait]
        impl Extension for PausedMessage {
            fn id(&self) -> &str {
                "paused-message"
            }
            fn handles_custom_message_lifecycle(&self) -> bool {
                true
            }
            async fn custom_message_lifecycle(
                &self,
                _: &crate::CustomMessageLifecycleEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<Option<CustomMessage>> {
                self.0.notify_one();
                std::future::pending().await
            }
        }
        let paused = Arc::new(PausedMessage(tokio::sync::Notify::new()));
        let observations = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut sdk = SessionBuilder::new(Arc::new(Capture(requests.clone())), "fixture")
            .extension(Arc::new(Hook {
                id: "first",
                observations,
            }))
            .unwrap()
            .extension(paused.clone())
            .unwrap()
            .in_memory()
            .unwrap();
        let control = sdk.controls();
        tokio::time::timeout(Duration::from_secs(2), async {
            let (result, ()) = tokio::join!(sdk.prompt("question"), async {
                paused.0.notified().await;
                control.abort();
            });
            assert!(matches!(result, Err(Error::Cancelled)));
        })
        .await
        .unwrap();
        assert!(requests.lock().unwrap().is_empty());
        let journal = sdk.journal().unwrap();
        assert!(
            !journal
                .records()
                .iter()
                .any(|record| matches!(record.event, Event::CustomMessageDelivered { .. }))
        );
        assert_eq!(journal.pending_before_agent_start_run(), None);
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
