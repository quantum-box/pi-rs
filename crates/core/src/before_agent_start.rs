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

pub(crate) const COMPACTION_FORMAT: &str = "pi-rs:prestart-compaction-v1";
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

fn is_compaction_start(record: &Record, run_id: Uuid) -> bool {
    record.run_id == Some(run_id)
        && matches!(&record.event, Event::SourceEntry { format, .. } if format == COMPACTION_FORMAT)
}

pub(crate) fn compaction_start(branch: &[&Record], run_id: Uuid) -> Option<u64> {
    branch
        .iter()
        .find(|record| is_compaction_start(record, run_id))
        .map(|record| record.id)
}

pub(crate) fn compaction_on_another_branch(
    session: &Session,
    branch: &[&Record],
    run_id: Uuid,
) -> bool {
    let mut records = session.records().iter();
    let Some(start) = records.find(|record| is_compaction_start(record, run_id)) else {
        return false;
    };
    if compaction_start(branch, run_id) != Some(start.id) {
        return true;
    }
    // A checkout may keep the marker but leave its request or result on a
    // different branch. Check the preflight records through the saved plan.
    for record in records.filter(|record| record.run_id == Some(run_id)) {
        let plan =
            matches!(&record.event, Event::SourceEntry { format, .. } if format == PLAN_FORMAT);
        if (plan
            || matches!(
                record.event,
                Event::ModelRequested { .. }
                    | Event::ModelFinished { .. }
                    | Event::Compacted { .. }
            ))
            && branch
                .binary_search_by_key(&record.id, |entry| entry.id)
                .is_err()
        {
            return true;
        }
        if plan {
            break;
        }
    }
    false
}

pub(crate) fn interrupted_compaction(branch: &[&Record], run_id: Uuid) -> Option<u64> {
    if recorded_plan(branch, run_id).is_some() {
        return None;
    }
    compaction_start(branch, run_id)
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
    let plan_id = recorded_plan(&branch, run_id).map(|(id, _)| id);
    let preflight_id = plan_id.or_else(|| compaction_start(&branch, run_id))?;
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
    let advanced = session.records()[preflight_id as usize..]
        .iter()
        .any(|record| {
            record.run_id == Some(run_id)
                && (plan_id.is_some() && matches!(record.event, Event::ModelRequested { .. })
                    || matches!(
                        record.event,
                        Event::Run {
                            state: RunState::Completed | RunState::Failed | RunState::Cancelled,
                            ..
                        }
                    ))
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
    let has_handlers = agent
        .registry
        .extensions
        .iter()
        .any(|extension| extension.handles_before_agent_start());
    let branch = session.branch();
    let compaction_start = compaction_start(&branch, run_id);
    if recorded_plan(&branch, run_id).is_some()
        || (compaction_start.is_none()
            && branch.iter().any(|record| {
                record.run_id == Some(run_id)
                    && matches!(record.event, Event::ModelRequested { .. })
            }))
    {
        return Ok(());
    }
    let Some(trigger) = starting_input(session, &branch, run_id, effective_input) else {
        return Ok(());
    };
    let attempted_compaction = compaction_start.is_some()
        || (cfg!(not(target_family = "wasm"))
            && Box::pin(agent.compact_before_start(session, run_id, cancel, observer, inputs))
                .await?);
    if !has_handlers && !attempted_compaction {
        return Ok(());
    }
    let (base_prompt, _) = crate::journal::resolve_system_context(
        &session.branch(),
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
        captures_next_turn: has_handlers,
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

    #[derive(Clone, Copy, Default)]
    enum SummaryOutcome {
        #[default]
        Success,
        PreparationBudget,
        PreparationUnavailable,
        ProviderFailure,
        ProviderIo,
        ProviderJson,
        SettlementIo,
        AllowanceOverrun,
        BudgetUnavailable,
        RecoveredSummary {
            id: Uuid,
            output_tokens: u64,
        },
        Veto,
    }

    #[derive(Default)]
    struct CompactionTrace(Mutex<Vec<&'static str>>, SummaryOutcome);

    fn is_summary_request(request: &ModelRequest) -> bool {
        matches!(request.messages.last(), Some(Message::User { content })
            if content.starts_with("Summarize only this older conversation."))
    }

    #[async_trait::async_trait]
    impl Model for CompactionTrace {
        fn minimum_output_tokens(&self) -> u32 {
            if matches!(self.1, SummaryOutcome::BudgetUnavailable) {
                17
            } else {
                1
            }
        }

        async fn prepare_context<'a>(
            &'a self,
            request: &ModelRequest,
        ) -> Result<Option<Arc<dyn Model + 'a>>> {
            if is_summary_request(request) {
                let error = match self.1 {
                    SummaryOutcome::PreparationBudget => Error::Budget,
                    SummaryOutcome::PreparationUnavailable => {
                        std::io::Error::other("summary adapter unavailable").into()
                    }
                    _ => return Ok(None),
                };
                self.0.lock().unwrap().push("prepare summary");
                return Err(error);
            }
            Ok(None)
        }

        async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
            let summary = is_summary_request(&request);
            let text = if summary {
                assert_eq!(user_texts(&request.messages)[0], "oldest question");
                assert!(!user_texts(&request.messages).contains(&"new question"));
                assert!(
                    !request
                        .messages
                        .iter()
                        .any(|message| matches!(message, Message::UserWithImages { .. }))
                );
                assert!(
                    matches!(&request.messages[0], Message::System { content } if content == "base prompt")
                );
                self.0.lock().unwrap().push("summary request");
                match self.1 {
                    SummaryOutcome::ProviderFailure | SummaryOutcome::SettlementIo => {
                        return Err(Error::RetryableModel("summary provider unavailable".into()));
                    }
                    SummaryOutcome::ProviderIo => {
                        return Err(
                            std::io::Error::from(std::io::ErrorKind::ConnectionReset).into()
                        );
                    }
                    SummaryOutcome::ProviderJson => {
                        return Err(serde_json::from_str::<Value>("").unwrap_err().into());
                    }
                    _ => {}
                }
                "old conversation summary"
            } else {
                let mut trace = self.0.lock().unwrap();
                let prompt = if trace.contains(&"before_agent_start") {
                    "run prompt"
                } else {
                    "base prompt"
                };
                assert!(
                    matches!(&request.messages[0], Message::System { content } if content == prompt)
                );
                trace.push("turn request");
                "answer"
            };
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Text(text.into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: Some(crate::Usage {
                        input_tokens: 24,
                        output_tokens: if summary
                            && matches!(self.1, SummaryOutcome::AllowanceOverrun)
                        {
                            u64::from(request.max_output_tokens) + 1
                        } else {
                            3
                        },
                    }),
                }),
            ])))
        }

        async fn record_usage_with_metadata(
            &self,
            request_id: Uuid,
            usage: &crate::UsageReport,
            _: &crate::ModelMetadata,
        ) -> Result<()> {
            if let SummaryOutcome::RecoveredSummary { id, output_tokens } = self.1
                && request_id == id
            {
                assert_eq!(
                    *usage,
                    crate::UsageReport::Measured {
                        usage: crate::Usage {
                            input_tokens: 24,
                            output_tokens
                        },
                    }
                );
                self.0.lock().unwrap().push("settle measured summary");
            }
            if *usage == crate::UsageReport::Unconfirmed {
                self.0.lock().unwrap().push("settle unconfirmed summary");
                if matches!(self.1, SummaryOutcome::SettlementIo) {
                    return Err(std::io::Error::other("summary settlement unavailable").into());
                }
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl Extension for CompactionTrace {
        fn id(&self) -> &str {
            "compaction trace"
        }

        fn handles_before_agent_start(&self) -> bool {
            true
        }

        async fn before_agent_start(
            &self,
            _: &BeforeAgentStartEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<crate::BeforeAgentStartResult> {
            self.0.lock().unwrap().push("before_agent_start");
            Ok(crate::BeforeAgentStartResult {
                system_prompt: Some("run prompt".into()),
                message: Some(message("hook context")),
            })
        }

        fn handles_before_summary_compaction(&self) -> bool {
            true
        }

        async fn before_summary_compaction(
            &self,
            preparation: &crate::CompactionPreparation,
            _: Option<&ExtensionState>,
            _: Arc<dyn crate::ToolHost>,
        ) -> ExtensionResult<Option<crate::BeforeSummaryCompactionResult>> {
            assert!(matches!(
                preparation.reason,
                crate::CompactionTrigger::Threshold
            ));
            assert!(!preparation.will_retry);
            assert_eq!(preparation.tokens_before, 3955);
            assert!(
                !preparation
                    .branch_entries
                    .iter()
                    .any(|record| matches!(&record.event,
                        Event::User { content, .. } | Event::UserWithImages { content, .. }
                            if content == "new question"
                    ))
            );
            self.0.lock().unwrap().push("before compaction");
            Ok(matches!(self.1, SummaryOutcome::Veto).then_some(
                crate::BeforeSummaryCompactionResult {
                    cancel: true,
                    compaction: None,
                },
            ))
        }
    }

    fn seed_compaction_history(session: &mut Session, stale: bool) {
        for content in ["oldest question", "recent question"] {
            session
                .append(Event::User {
                    content: content.into(),
                    input_id: None,
                })
                .unwrap();
            session
                .append(Event::Assistant {
                    content: "previous answer".into(),
                    invocations: vec![],
                    request_id: None,
                    usage: Some(crate::Usage {
                        input_tokens: 3950,
                        output_tokens: 5,
                    }),
                })
                .unwrap();
        }
        if stale {
            session
                .append(Event::Compacted {
                    summary: "already compacted".into(),
                    system_prompt: None,
                    retain_from: Some(3),
                    tokens_before: Some(3955),
                    states: Default::default(),
                    usage: None,
                    usage_metadata: None,
                })
                .unwrap();
        }
    }

    fn compaction_builder(trace: &Arc<CompactionTrace>) -> SessionBuilder {
        SessionBuilder::new(trace.clone(), "fixture")
            .system_prompt("base prompt")
            .limits(compaction_limits())
    }

    fn compaction_limits() -> crate::Limits {
        crate::Limits {
            context_window: Some(4096),
            output_tokens_per_request: 32,
            compaction_tokens: Some(crate::CompactionTokens {
                reserve_tokens: Some(256),
                keep_recent_tokens: Some(1),
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn before_agent_start_threshold_compacts_prior_context_for_new_triggers() {
        for (trigger, enabled, stale) in [
            ("text", true, false),
            ("image", true, false),
            ("custom", true, false),
            ("text", false, false),
            ("text", true, true),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("session.jsonl");
            seed_compaction_history(&mut Session::open(&path).unwrap(), stale);
            let trace = Arc::new(CompactionTrace::default());
            let mut sdk = compaction_builder(&trace)
                .extension(trace.clone())
                .unwrap()
                .open(path)
                .unwrap();
            sdk.controls().set_auto_compaction_enabled(enabled).unwrap();
            let answer = match trigger {
                "custom" => sdk
                    .send_custom_message(message("new question"), true)
                    .await
                    .unwrap()
                    .unwrap(),
                "image" => sdk
                    .prompt_with_images(
                        "new question",
                        vec![crate::ImageContent {
                            data: "AA==".into(),
                            mime_type: "image/png".into(),
                        }],
                    )
                    .await
                    .unwrap(),
                _ => sdk.prompt("new question").await.unwrap(),
            };
            assert_eq!(answer, "answer");
            let expected: &[_] = if enabled && !stale {
                &[
                    "before compaction",
                    "summary request",
                    "before_agent_start",
                    "turn request",
                ]
            } else {
                &["before_agent_start", "turn request"]
            };
            assert_eq!(*trace.0.lock().unwrap(), expected, "{trigger}");
            let journal = sdk.journal().unwrap();
            let branch = journal.branch();
            let plan = branch
                .iter()
                .position(|record| {
                    matches!(&record.event, Event::SourceEntry {
                format, ..
            } if format == PLAN_FORMAT)
                })
                .unwrap();
            if enabled && !stale {
                let compacted = branch
                    .iter()
                    .position(|record| {
                        matches!(&record.event, Event::Compacted {
                    summary, tokens_before: Some(3955), ..
                } if summary.starts_with("old conversation summary"))
                    })
                    .unwrap();
                assert!(compacted < plan);
            }
            assert!(
                journal
                    .current_messages()
                    .iter()
                    .any(|message| match message {
                        Message::User { content } | Message::UserWithImages { content, .. } =>
                            content == "new question",
                        _ => false,
                    })
            );
        }
    }

    #[tokio::test]
    async fn prestart_compaction_preserves_structured_prompt_edits() {
        struct SectionHook;
        #[async_trait::async_trait]
        impl Extension for SectionHook {
            fn id(&self) -> &str {
                "section-hook"
            }
            fn handles_before_agent_start(&self) -> bool {
                true
            }
            async fn before_agent_start_with_prompt_options(
                &self,
                _: &BeforeAgentStartEvent,
                options: &BeforeAgentStartPromptOptions,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<BeforeAgentStartPromptResult> {
                assert_eq!(options.sections["rules"], "original rules");
                Ok(BeforeAgentStartPromptResult {
                    system_prompt_options: crate::BeforeAgentStartPromptChanges {
                        sections: [("rules".into(), Some("edited rules".into()))]
                            .into_iter()
                            .collect(),
                    },
                    ..Default::default()
                })
            }
        }
        struct SummaryCapture(Arc<Mutex<Vec<Vec<Message>>>>);
        #[async_trait::async_trait]
        impl Model for SummaryCapture {
            async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
                let summary = is_summary_request(&request);
                self.0.lock().unwrap().push(request.messages);
                let text = if summary { "summary" } else { "answer" };
                Ok(Box::pin(futures_util::stream::iter([
                    Ok(ModelEvent::Text(text.into())),
                    Ok(ModelEvent::Complete {
                        calls: vec![],
                        usage: None,
                    }),
                ])))
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        seed_compaction_history(&mut Session::open(&path).unwrap(), false);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let model = SummaryCapture(requests.clone());
        let mut registry = Registry::default();
        let prompt = registry.compose_system_prompt(
            "host prompt".into(),
            vec![("rules".into(), "original rules".into())],
        );
        registry.register(Arc::new(SectionHook)).unwrap();
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: &prompt,
            limits: compaction_limits(),
        };
        let mut session = Session::open(&path).unwrap();
        let (_sender, mut followups) = mpsc::channel::<String>(1);
        agent
            .run(
                &mut session,
                Some("new question".into()),
                &mut followups,
                &CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        assert!(
            session
                .branch()
                .iter()
                .any(|record| matches!(record.event, Event::Compacted { .. }))
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(
            matches!(&requests[0][0], Message::System { content } if content.contains("original rules") && !content.contains("edited rules"))
        );
        assert!(
            matches!(&requests[1][0], Message::System { content } if content.contains("edited rules"))
        );
    }

    #[tokio::test]
    async fn before_agent_start_continues_after_optional_summary_failure_or_veto() {
        for (summary, with_extension, expected) in [
            (
                SummaryOutcome::ProviderFailure,
                true,
                &[
                    "before compaction",
                    "summary request",
                    "settle unconfirmed summary",
                    "before_agent_start",
                    "turn request",
                ][..],
            ),
            (
                SummaryOutcome::ProviderFailure,
                false,
                &[
                    "summary request",
                    "settle unconfirmed summary",
                    "turn request",
                ][..],
            ),
            (
                SummaryOutcome::Veto,
                true,
                &["before compaction", "before_agent_start", "turn request"][..],
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("session.jsonl");
            seed_compaction_history(&mut Session::open(&path).unwrap(), false);
            let trace = Arc::new(CompactionTrace(Mutex::default(), summary));
            let mut builder = compaction_builder(&trace);
            if with_extension {
                builder = builder.extension(trace.clone()).unwrap();
            }
            let mut sdk = builder.open(path).unwrap();

            assert_eq!(sdk.prompt("new question").await.unwrap(), "answer");
            assert_eq!(*trace.0.lock().unwrap(), expected);
            let journal = sdk.journal().unwrap();
            assert_eq!(journal.pending_before_agent_start_run(), None);
            assert!(
                !journal
                    .records()
                    .iter()
                    .any(|record| matches!(record.event, Event::Compacted { .. }))
            );
        }
    }

    #[tokio::test]
    async fn before_agent_start_preserves_model_budget_errors_during_summary_preparation() {
        for summary in [
            SummaryOutcome::PreparationBudget,
            SummaryOutcome::PreparationUnavailable,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("session.jsonl");
            seed_compaction_history(&mut Session::open(&path).unwrap(), false);
            let trace = Arc::new(CompactionTrace(Mutex::default(), summary));
            let mut sdk = compaction_builder(&trace)
                .extension(trace.clone())
                .unwrap()
                .open(path)
                .unwrap();

            let result = sdk.prompt("new question").await;
            let seen = trace.0.lock().unwrap();
            if matches!(summary, SummaryOutcome::PreparationBudget) {
                assert!(matches!(result, Err(Error::Budget)), "{result:?}");
                assert_eq!(*seen, ["before compaction", "prepare summary"]);
                let journal = sdk.journal().unwrap();
                assert!(!journal.records().iter().any(|record| matches!(
                    record.event,
                    Event::ModelRequested { .. } | Event::Compacted { .. }
                )));
                assert!(matches!(
                    journal.records().last().unwrap().event,
                    Event::Run {
                        state: RunState::Failed,
                        ..
                    }
                ));
            } else {
                assert_eq!(result.unwrap(), "answer");
                assert_eq!(
                    *seen,
                    [
                        "before compaction",
                        "prepare summary",
                        "before_agent_start",
                        "turn request"
                    ]
                );
            }
        }
    }

    #[tokio::test]
    async fn before_agent_start_distinguishes_provider_errors_from_settlement_errors() {
        for summary in [
            SummaryOutcome::ProviderIo,
            SummaryOutcome::ProviderJson,
            SummaryOutcome::SettlementIo,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("session.jsonl");
            seed_compaction_history(&mut Session::open(&path).unwrap(), false);
            let trace = Arc::new(CompactionTrace(Mutex::default(), summary));
            let mut sdk = compaction_builder(&trace)
                .extension(trace.clone())
                .unwrap()
                .open(path)
                .unwrap();
            let result = sdk.prompt("new question").await;
            let trace = trace.0.lock().unwrap();
            if matches!(summary, SummaryOutcome::SettlementIo) {
                assert!(
                    matches!(result, Err(Error::Io(error)) if error.to_string() == "summary settlement unavailable")
                );
                assert_eq!(
                    *trace,
                    [
                        "before compaction",
                        "summary request",
                        "settle unconfirmed summary"
                    ]
                );
            } else {
                assert_eq!(result.unwrap(), "answer");
                assert_eq!(
                    *trace,
                    [
                        "before compaction",
                        "summary request",
                        "settle unconfirmed summary",
                        "before_agent_start",
                        "turn request",
                    ]
                );
            }
        }
    }

    #[tokio::test]
    async fn before_agent_start_distinguishes_unavailable_budget_from_dispatched_overrun() {
        for (summary, cancel_on_finish) in [
            (SummaryOutcome::BudgetUnavailable, false),
            (SummaryOutcome::AllowanceOverrun, false),
            (SummaryOutcome::AllowanceOverrun, true),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("session.jsonl");
            seed_compaction_history(&mut Session::open(&path).unwrap(), false);
            let trace = Arc::new(CompactionTrace(Mutex::default(), summary));
            let mut sdk = compaction_builder(&trace)
                .limits(crate::Limits {
                    output_tokens_per_run: 32,
                    ..compaction_limits()
                })
                .extension(trace.clone())
                .unwrap()
                .open(path)
                .unwrap();
            let controls = sdk.controls();
            let mut cancelled_after_summary = false;
            let result = sdk.prompt_with_observer("new question", &mut |event| {
                if cancel_on_finish
                    && matches!(event, Notification::Committed(record) if matches!(
                        record.event, Event::ModelFinished { usage: crate::UsageReport::Measured { usage }, .. }
                            if usage.output_tokens == 17
                    ))
                {
                    cancelled_after_summary = true;
                    controls.abort();
                }
            }).await;
            assert_eq!(cancelled_after_summary, cancel_on_finish);
            let seen = trace.0.lock().unwrap();
            if matches!(summary, SummaryOutcome::AllowanceOverrun) {
                assert!(matches!(result, Err(Error::Budget)), "{result:?}");
                assert_eq!(*seen, ["before compaction", "summary request"]);
                let journal = sdk.journal().unwrap();
                assert!(journal.records().iter().any(|record| matches!(
                    record.event, Event::ModelFinished { usage: crate::UsageReport::Measured { usage }, .. }
                        if usage.output_tokens == 17
                )));
                assert!(matches!(
                    journal.records().last().unwrap().event,
                    Event::Run {
                        state: RunState::Failed,
                        ..
                    }
                ));
            } else {
                assert_eq!(result.unwrap(), "answer");
                assert_eq!(
                    *seen,
                    ["before compaction", "before_agent_start", "turn request"]
                );
            }
        }
    }

    #[tokio::test]
    async fn before_agent_start_surfaces_journal_failure_from_failed_compaction_hook() {
        struct FailingStore {
            inner: crate::MemoryStore,
            cancel_on_failure: Option<CancellationToken>,
        }
        impl crate::SessionStore for FailingStore {
            fn load(&mut self) -> Result<Vec<Record>> {
                self.inner.load()
            }
            fn append(&mut self, record: &Record) -> Result<()> {
                if matches!(&record.event, Event::CustomMessageQueued { input }
                    if input.delivery == CustomMessageDelivery::NextTurn)
                {
                    if let Some(cancel) = &self.cancel_on_failure {
                        cancel.cancel();
                    }
                    return Err(std::io::Error::other("compaction callback journal failure").into());
                }
                self.inner.append(record)
            }
        }

        struct FailureHook;
        #[async_trait::async_trait]
        impl Extension for FailureHook {
            fn id(&self) -> &str {
                "compaction failure fixture"
            }
            fn handles_lifecycle(&self) -> bool {
                true
            }
            fn handles_session_lifecycle(&self) -> bool {
                true
            }
            async fn lifecycle_with_host(
                &self,
                event: &crate::LifecycleEvent,
                _: Option<&ExtensionState>,
                host: Arc<dyn crate::ToolHost>,
            ) -> ExtensionResult<()> {
                if matches!(event, crate::LifecycleEvent::SessionCompactFailed { .. }) {
                    host.send_message(message("failed summary"), CustomMessageDelivery::NextTurn)
                        .await?;
                }
                Ok(())
            }
        }

        for (cancel_during_write, queued_after_turn) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let trace = Arc::new(CompactionTrace(
                Mutex::default(),
                SummaryOutcome::ProviderIo,
            ));
            let mut registry = Registry::default();
            registry.register(trace.clone()).unwrap();
            registry.register(Arc::new(FailureHook)).unwrap();
            let cancel = CancellationToken::new();
            let mut session = Session::with_store(FailingStore {
                inner: crate::MemoryStore::default(),
                cancel_on_failure: cancel_during_write.then(|| cancel.clone()),
            })
            .unwrap();
            seed_compaction_history(&mut session, false);
            let queued_id = Uuid::new_v4();
            if queued_after_turn {
                crate::agent::queue_custom_message(
                    &mut session,
                    crate::QueuedCustomMessage {
                        id: queued_id,
                        delivery: CustomMessageDelivery::AfterTurn,
                        message: message("pending after-turn message"),
                    },
                    &mut |_| {},
                )
                .unwrap();
            }
            let agent = Agent {
                model: trace.as_ref(),
                model_name: "fixture",
                registry: &registry,
                system_prompt: "base prompt",
                limits: compaction_limits(),
            };
            let (_sender, mut inputs) = mpsc::channel::<String>(1);
            let result = agent
                .run(
                    &mut session,
                    Some("new question".into()),
                    &mut inputs,
                    &cancel,
                    &mut |_| {},
                )
                .await;
            assert_eq!(cancel.is_cancelled(), cancel_during_write);
            assert!(
                matches!(&result, Err(Error::Io(error)) if error.to_string() == "compaction callback journal failure"),
                "cancel={cancel_during_write}, after-turn queue={queued_after_turn}: {result:?}"
            );
            assert_eq!(
                *trace.0.lock().unwrap(),
                [
                    "before compaction",
                    "summary request",
                    "settle unconfirmed summary"
                ]
            );
            assert_eq!(
                session
                    .queued_custom_messages()
                    .iter()
                    .any(|(_, input)| input.id == queued_id),
                queued_after_turn
            );
        }
    }

    #[tokio::test]
    async fn before_agent_start_full_cancellation_stops_preflight() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        seed_compaction_history(&mut Session::open(&path).unwrap(), false);
        let trace = Arc::new(CompactionTrace::default());
        let mut sdk = compaction_builder(&trace)
            .extension(trace.clone())
            .unwrap()
            .open(path)
            .unwrap();
        let control = sdk.controls();
        let result = sdk
            .run_with_events(Some("new question".into()), Uuid::new_v4(), &mut |event| {
                if matches!(
                    event,
                    crate::AgentEvent::Notification(Notification::SummaryRequest)
                ) {
                    control.abort();
                }
            })
            .await;

        assert!(matches!(result, Err(Error::Cancelled)));
        assert_eq!(
            *trace.0.lock().unwrap(),
            ["before compaction", "settle unconfirmed summary"]
        );
        let journal = sdk.journal().unwrap();
        assert_eq!(journal.pending_before_agent_start_run(), None);
        assert!(matches!(
            journal.records().last().unwrap().event,
            Event::Run {
                state: RunState::Cancelled,
                ..
            }
        ));
        assert!(
            !journal
                .records()
                .iter()
                .any(|record| matches!(record.event, Event::Compacted { .. }))
        );
    }

    fn seed_interrupted_summary(
        journal: &mut Session,
        run_id: Uuid,
        summary_id: Uuid,
        max_output_tokens: u32,
    ) -> (u64, u64, u64) {
        seed_compaction_history(journal, false);
        journal.set_run_id(Some(run_id));
        let running = journal
            .append(Event::Run {
                state: RunState::Running,
                initial: Some("new question".into()),
            })
            .unwrap()
            .id;
        let marker = journal
            .append(Event::SourceEntry {
                format: COMPACTION_FORMAT.into(),
                entry: Value::Null,
            })
            .unwrap()
            .id;
        let requested = journal
            .append(Event::ModelRequested {
                model: "fixture".into(),
                messages: 2,
                tools: 0,
                max_output_tokens,
                request_id: Some(summary_id),
                context_head: None,
                context_sha256: None,
                context_transformed: None,
                context_configuration_sha256: None,
                tool_decision: None,
            })
            .unwrap()
            .id;
        (running, marker, requested)
    }

    #[tokio::test]
    async fn before_agent_start_resume_skips_interrupted_summary_and_starts_original_turn() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let run_id = Uuid::new_v4();
        let summary_id = Uuid::new_v4();
        let (running, marker, requested) = {
            let mut journal = Session::open(&path).unwrap();
            seed_interrupted_summary(&mut journal, run_id, summary_id, 32)
        };
        let trace = Arc::new(CompactionTrace::default());
        let mut sdk = compaction_builder(&trace)
            .extension(trace.clone())
            .unwrap()
            .open(path)
            .unwrap();
        assert_eq!(
            sdk.journal().unwrap().pending_before_agent_start_run(),
            Some(run_id)
        );
        assert!(matches!(
            sdk.prompt("different question").await,
            Err(Error::Session(_))
        ));
        for target in [running, marker] {
            sdk.checkout(target).unwrap();
            assert!(matches!(
                sdk.run(None, run_id, &mut |_| {}).await,
                Err(Error::Session(message)) if message.contains("branch that recorded its attempt")
            ));
            assert!(trace.0.lock().unwrap().is_empty());
        }
        sdk.checkout(requested).unwrap();
        assert_eq!(sdk.resume().await.unwrap(), "answer");
        assert_eq!(
            *trace.0.lock().unwrap(),
            [
                "settle unconfirmed summary",
                "before_agent_start",
                "turn request",
            ]
        );
        let journal = sdk.journal().unwrap();
        assert_eq!(journal.pending_before_agent_start_run(), None);
        assert_eq!(
            journal
                .records()
                .iter()
                .filter(|record| {
                    record.run_id == Some(run_id)
                        && matches!(record.event, Event::ModelRequested { .. })
                })
                .count(),
            2
        );
        assert!(
            journal
                .records()
                .iter()
                .any(|record| matches!(&record.event, Event::User {
            content, input_id: Some(id),
        } if *id == run_id && content == "new question"))
        );
    }

    #[tokio::test]
    async fn before_agent_start_resume_settles_then_enforces_the_recorded_summary_allowance() {
        for output_tokens in [8, 9] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("session.jsonl");
            let run_id = Uuid::new_v4();
            let summary_id = Uuid::new_v4();
            {
                let mut journal = Session::open(&path).unwrap();
                seed_interrupted_summary(&mut journal, run_id, summary_id, 8);
                journal
                    .append(Event::ModelFinished {
                        request_id: summary_id,
                        metadata: Default::default(),
                        usage: crate::UsageReport::Measured {
                            usage: crate::Usage {
                                input_tokens: 24,
                                output_tokens,
                            },
                        },
                        failure: None,
                        partial: String::new(),
                        reply: None,
                    })
                    .unwrap();
            }
            let trace = Arc::new(CompactionTrace(
                Mutex::default(),
                SummaryOutcome::RecoveredSummary {
                    id: summary_id,
                    output_tokens,
                },
            ));
            let mut sdk = compaction_builder(&trace)
                .extension(trace.clone())
                .unwrap()
                .open(path)
                .unwrap();
            assert_eq!(
                sdk.journal().unwrap().pending_before_agent_start_run(),
                Some(run_id)
            );
            let result = sdk.resume().await;
            let journal = sdk.journal().unwrap();
            assert_eq!(journal.pending_before_agent_start_run(), None);
            let seen = trace.0.lock().unwrap();
            if output_tokens > 8 {
                assert!(matches!(result, Err(Error::Budget)), "{result:?}");
                assert_eq!(*seen, ["settle measured summary"]);
                assert!(matches!(
                    journal.records().last().unwrap().event,
                    Event::Run {
                        state: RunState::Failed,
                        ..
                    }
                ));
            } else {
                assert_eq!(result.unwrap(), "answer");
                assert_eq!(
                    *seen,
                    [
                        "settle measured summary",
                        "before_agent_start",
                        "turn request"
                    ]
                );
            }
        }
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
