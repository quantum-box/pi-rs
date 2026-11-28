//! Persist the preflight results so a resumed run does not repeat completed callbacks.
use std::time::Duration;

use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    Agent, AgentInput, AgentStartTrigger, BeforeAgentStartEvent, BeforeAgentStartPromptOptions,
    BeforeAgentStartPromptResult, CancellationToken, CustomMessage, CustomMessageDelivery, Error,
    Event, Notification, Observer, Record, Result, RunState, Session,
};

const PLAN_FORMAT: &str = "pi-rs:before-agent-start-v1";
type PromptSectionEdits = Vec<(String, Option<String>)>;

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Plan {
    system_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    system_prompt_sections: PromptSectionEdits,
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

pub(crate) fn prompt_section_edits(
    branch: &[&Record],
    run_id: Uuid,
) -> Result<Option<(u64, PromptSectionEdits)>> {
    let Some((source, edits)) = recorded_plan(branch, run_id).and_then(|(source, plan)| {
        plan.get("systemPromptSections")
            .map(|edits| (source, edits))
    }) else {
        return Ok(None);
    };
    let edits: PromptSectionEdits = serde_json::from_value(edits.clone())?;
    // An image-bearing or already accepted prompt can precede its preflight plan.
    let source = branch
        .iter()
        .take_while(|record| record.id < source)
        .find(|record| match &record.event {
            Event::User { input_id, .. } | Event::UserWithImages { input_id, .. } => {
                *input_id == Some(run_id)
            }
            Event::CustomMessageDelivered { id, .. } => *id == run_id,
            _ => false,
        })
        .map_or(source, |record| record.id);
    Ok((!edits.is_empty()).then_some((source, edits)))
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
    let (base_prompt, _) = crate::journal::resolve_system_context(
        &branch,
        agent.system_prompt,
        Vec::new(),
        agent.registry.host_system_prompt.as_deref(),
    );
    let mut event = BeforeAgentStartEvent {
        trigger,
        system_prompt: base_prompt.render(),
    };
    let mut options = prompt_options(&base_prompt);
    let states = session.extension_states();
    let mut edits = indexmap::IndexMap::new();
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
                    .before_agent_start_with_prompt_options(
                        &event,
                        &options,
                        states.get(extension.id()),
                    )
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
            Ok(BeforeAgentStartPromptResult {
                result,
                system_prompt_options,
            }) => {
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
                let changes = system_prompt_options.sections;
                if !changes.is_empty() {
                    if let Some(name) = changes.keys().find(|name| !valid_section_name(name)) {
                        report(
                            extension.id(),
                            &format!("invalid system prompt section name: {name:?}"),
                            observer,
                        );
                        continue;
                    }
                    for (name, value) in changes {
                        if value.as_ref().is_some_and(String::is_empty) {
                            edits.shift_remove(&name);
                            continue;
                        }
                        let original = base_prompt
                            .sections
                            .iter()
                            .find_map(|(key, text)| (key == &name).then_some(text));
                        let value = value
                            .map(|body| crate::journal::wrap_system_prompt_section(&name, &body));
                        if value.as_ref() == original {
                            edits.shift_remove(&name);
                        } else {
                            edits.insert(name, value);
                        }
                    }
                    let mut prompt = base_prompt.clone();
                    let applied = edits
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect::<Vec<_>>();
                    prompt.apply_sections(&applied);
                    options = prompt_options(&prompt);
                    if plan.system_prompt.is_none() {
                        event.system_prompt = prompt.render();
                    }
                }
            }
            Err(error) => report(extension.id(), &error, observer),
        }
    }
    crate::agent::queue_inputs(session, inputs, observer)?;
    if cancel.is_cancelled() {
        return Err(Error::Cancelled);
    }
    plan.system_prompt_sections = edits.into_iter().collect();
    let record = session.append(Event::SourceEntry {
        format: PLAN_FORMAT.into(),
        entry: serde_json::to_value(plan)?,
    })?;
    observer(Notification::Committed(record));
    Ok(())
}

fn valid_section_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    name != "preamble"
        && bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn prompt_options(state: &crate::journal::SystemPromptState) -> BeforeAgentStartPromptOptions {
    BeforeAgentStartPromptOptions {
        sections: state
            .sections
            .iter()
            .filter(|(name, _)| valid_section_name(name))
            .map(|(name, text)| {
                let body = text
                    .strip_prefix(&format!("<{name}>\n"))
                    .and_then(|text| text.strip_suffix(&format!("\n</{name}>")))
                    .unwrap_or(text);
                (name.clone(), body.to_owned())
            })
            .collect(),
    }
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
    async fn before_agent_start_edits_named_sections_in_order_and_only_for_the_run() {
        struct EditSections {
            id: &'static str,
            observations: Arc<Mutex<Vec<(String, BeforeAgentStartPromptOptions)>>>,
        }

        #[async_trait::async_trait]
        impl Extension for EditSections {
            fn id(&self) -> &str {
                self.id
            }

            fn handles_before_agent_start(&self) -> bool {
                true
            }

            async fn before_agent_start_with_prompt_options(
                &self,
                event: &BeforeAgentStartEvent,
                options: &BeforeAgentStartPromptOptions,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<BeforeAgentStartPromptResult> {
                if !matches!(&event.trigger, AgentStartTrigger::User { prompt, .. } if prompt == "edit")
                {
                    return Ok(Default::default());
                }
                self.observations
                    .lock()
                    .unwrap()
                    .push((event.system_prompt.clone(), options.clone()));
                let sections = match self.id {
                    "first" => {
                        serde_json::json!({"rules":"first rules","retired":null,"cwd":"first cwd","z-added":"Z"})
                    }
                    "invalid" => serde_json::json!({"never_added":"ignored","preamble":"invalid"}),
                    "last" => serde_json::json!({"rules":"final rules","cwd":"","a_added":"A"}),
                    _ => unreachable!(),
                };
                Ok(BeforeAgentStartPromptResult {
                    system_prompt_options: crate::BeforeAgentStartPromptChanges {
                        sections: serde_json::from_value(sections).unwrap(),
                    },
                    ..Default::default()
                })
            }
        }

        let requests = Arc::new(Mutex::new(Vec::new()));
        let observations = Arc::new(Mutex::new(Vec::new()));
        let mut registry = Registry::default();
        let prompt = registry.compose_system_prompt(
            "host prompt".into(),
            vec![
                ("rules".into(), "host rules".into()),
                ("retired".into(), "remove this".into()),
                ("cwd".into(), "work".into()),
            ],
        );
        for id in ["first", "invalid", "last"] {
            registry
                .register(Arc::new(EditSections {
                    id,
                    observations: observations.clone(),
                }))
                .unwrap();
        }
        let model = Capture(requests.clone());
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: &prompt,
            limits: crate::Limits::default(),
        };
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        let mut errors = Vec::new();
        for input in ["edit", "next"] {
            let (_sender, mut followups) = mpsc::channel::<String>(1);
            agent
                .run(
                    &mut session,
                    Some(input.into()),
                    &mut followups,
                    &CancellationToken::new(),
                    &mut |notification| {
                        if let Notification::ExtensionLifecycleError {
                            extension, message, ..
                        } = notification
                        {
                            errors.push((extension.to_owned(), message.to_owned()));
                        }
                    },
                )
                .await
                .unwrap();
        }
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, "invalid");
        assert!(errors[0].1.contains("preamble"));
        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 3);
        assert_eq!(observations[0].0, prompt);
        let (last_prompt, options) = &observations[2];
        let sections = &options.sections;
        assert_eq!(
            sections.keys().map(String::as_str).collect::<Vec<_>>(),
            ["rules", "cwd", "z-added"]
        );
        assert_eq!(sections["rules"], "first rules");
        assert_eq!(sections["cwd"], "first cwd");
        assert!(last_prompt.contains("<z-added>\nZ\n</z-added>"));
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests[0][0],
            Message::System {
                content: "host prompt\n\n<rules>\nfinal rules\n</rules>\n\n<cwd>\nwork\n</cwd>\n\n<z-added>\nZ\n</z-added>\n\n<a_added>\nA\n</a_added>".into(),
            }
        );
        assert_eq!(requests[1][0], Message::System { content: prompt });
    }

    #[tokio::test]
    async fn before_agent_start_resumes_structured_edits_with_the_imported_transcript() {
        struct TranscriptCapture(Capture);

        #[async_trait::async_trait]
        impl Model for TranscriptCapture {
            fn supports_mid_conversation_system_messages(&self, _: &str) -> bool {
                true
            }

            async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
                self.0.stream(request).await
            }
        }

        for mid_conversation in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("session.jsonl");
            let run_id = Uuid::new_v4();
            let mut journal = Session::open(&path).unwrap();
            journal
                .append(Event::SystemCheckpoint {
                    content: "imported base".into(),
                    sections: vec![("rules".into(), Some("<rules>\nold\n</rules>".into()))],
                    tools_added: vec![],
                    tools_removed: vec![],
                })
                .unwrap();
            journal
                .append(Event::User {
                    content: "earlier".into(),
                    input_id: None,
                })
                .unwrap();
            journal.set_run_id(Some(run_id));
            journal
                .append(Event::Run {
                    state: RunState::Running,
                    initial: (!mid_conversation).then(|| "question".into()),
                })
                .unwrap();
            if mid_conversation {
                journal
                    .append(Event::User {
                        content: "question".into(),
                        input_id: Some(run_id),
                    })
                    .unwrap();
            }
            journal
                .append(Event::SourceEntry {
                    format: PLAN_FORMAT.into(),
                    entry: serde_json::to_value(Plan {
                        system_prompt_sections: vec![(
                            "rules".into(),
                            Some("<rules>\nnew\n</rules>".into()),
                        )],
                        captures_next_turn: true,
                        ..Default::default()
                    })
                    .unwrap(),
                })
                .unwrap();
            drop(journal);
            let requests = Arc::new(Mutex::new(Vec::new()));
            let model: Arc<dyn Model> = if mid_conversation {
                Arc::new(TranscriptCapture(Capture(requests.clone())))
            } else {
                Arc::new(Capture(requests.clone()))
            };
            let observations = Arc::new(Mutex::new(Vec::new()));
            let mut sdk = SessionBuilder::new(model, "fixture")
                .extension(Arc::new(Hook {
                    id: "unexpected",
                    observations: observations.clone(),
                }))
                .unwrap()
                .open(&path)
                .unwrap();
            sdk.resume().await.unwrap();
            assert!(observations.lock().unwrap().is_empty());
            let requests = requests.lock().unwrap();
            let sections: Vec<_> = requests[0]
                .iter()
                .filter_map(|message| match message {
                    Message::System { content } => Some(content.as_str()),
                    _ => None,
                })
                .collect();
            if mid_conversation {
                assert_eq!(
                    sections,
                    [
                        "imported base\n\n<rules>\nold\n</rules>",
                        "Updated system prompt section \"rules\":\n\n<rules>\nnew\n</rules>",
                    ]
                );
                assert!(
                    matches!(&requests[0][1], Message::User { content } if content == "earlier")
                );
                assert!(matches!(&requests[0][2], Message::System { .. }));
            } else {
                assert_eq!(sections, ["imported base\n\n<rules>\nnew\n</rules>"]);
            }
            assert_eq!(user_texts(&requests[0]), ["earlier", "question"]);
        }
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
                        ..Plan::default()
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
        struct EditAfterForce;
        #[async_trait::async_trait]
        impl Extension for EditAfterForce {
            fn id(&self) -> &str {
                "section-after-force"
            }

            fn handles_before_agent_start(&self) -> bool {
                true
            }

            async fn before_agent_start_with_prompt_options(
                &self,
                event: &BeforeAgentStartEvent,
                _: &BeforeAgentStartPromptOptions,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<BeforeAgentStartPromptResult> {
                assert!(event.system_prompt.is_empty());
                Ok(BeforeAgentStartPromptResult {
                    system_prompt_options: crate::BeforeAgentStartPromptChanges {
                        sections: [("rules".into(), Some("section after force".into()))]
                            .into_iter()
                            .collect(),
                    },
                    ..Default::default()
                })
            }
        }

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
            .extension(Arc::new(EditAfterForce))
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
                    ..Default::default()
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
