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

pub(crate) const COMPACTION_FORMAT: &str = "pi-rs:prestart-compaction-v1";
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
    let mut event = BeforeAgentStartEvent {
        trigger,
        system_prompt: crate::journal::system_context(
            &session.branch(),
            agent.system_prompt,
            Vec::new(),
        )
        .0,
    };
    let states = session.extension_states();
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

    #[derive(Clone, Copy, Default)]
    enum SummaryOutcome {
        #[default]
        Success,
        ProviderFailure,
        ProviderIo,
        ProviderJson,
        SettlementIo,
        AllowanceOverrun,
        BudgetUnavailable,
        Veto,
    }

    #[derive(Default)]
    struct CompactionTrace(Mutex<Vec<&'static str>>, SummaryOutcome);

    #[async_trait::async_trait]
    impl Model for CompactionTrace {
        fn minimum_output_tokens(&self) -> u32 {
            if matches!(self.1, SummaryOutcome::BudgetUnavailable) {
                17
            } else {
                1
            }
        }

        async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
            let summary = matches!(request.messages.last(), Some(Message::User { content })
                if content.starts_with("Summarize only this older conversation."));
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
            _: Uuid,
            usage: &crate::UsageReport,
            _: &crate::ModelMetadata,
        ) -> Result<()> {
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
        for summary in [
            SummaryOutcome::BudgetUnavailable,
            SummaryOutcome::AllowanceOverrun,
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
            let result = sdk.prompt("new question").await;
            let seen = trace.0.lock().unwrap();
            if matches!(summary, SummaryOutcome::AllowanceOverrun) {
                assert!(matches!(result, Err(Error::Budget)), "{result:?}");
                assert_eq!(*seen, ["before compaction", "summary request"]);
                let journal = sdk.journal().unwrap();
                assert!(journal.records().iter().any(|record| matches!(
                    record.event, Event::ModelFinished { usage: crate::UsageReport::Measured { usage }, .. }
                        if usage.output_tokens == 17
                )));
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
        struct FailingStore(crate::MemoryStore);
        impl crate::SessionStore for FailingStore {
            fn load(&mut self) -> Result<Vec<Record>> {
                self.0.load()
            }
            fn append(&mut self, record: &Record) -> Result<()> {
                if matches!(record.event, Event::CustomMessageQueued { .. }) {
                    return Err(std::io::Error::other("compaction callback journal failure").into());
                }
                self.0.append(record)
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

        let trace = Arc::new(CompactionTrace(
            Mutex::default(),
            SummaryOutcome::ProviderIo,
        ));
        let mut registry = Registry::default();
        registry.register(trace.clone()).unwrap();
        registry.register(Arc::new(FailureHook)).unwrap();
        let mut session = Session::with_store(FailingStore(crate::MemoryStore::default())).unwrap();
        seed_compaction_history(&mut session, false);
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
                &CancellationToken::new(),
                &mut |_| {},
            )
            .await;
        assert!(
            matches!(result, Err(Error::Io(error)) if error.to_string() == "compaction callback journal failure")
        );
        assert_eq!(
            *trace.0.lock().unwrap(),
            [
                "before compaction",
                "summary request",
                "settle unconfirmed summary"
            ]
        );
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

    #[tokio::test]
    async fn before_agent_start_resume_skips_interrupted_summary_and_starts_original_turn() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let run_id = Uuid::new_v4();
        let summary_id = Uuid::new_v4();
        let (running, marker, requested) = {
            let mut journal = Session::open(&path).unwrap();
            seed_compaction_history(&mut journal, false);
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
                    max_output_tokens: 32,
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
