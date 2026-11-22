//! Actionable extension boundaries over the session's journal projection.
use std::time::Duration;

use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    ContextContent, CustomEntry, CustomMessage, CustomMessageDelivery, Error, Event,
    ExtensionResult, InputKind, LifecycleMessage, Message, Notification, Observer, QueueMode,
    Registry, Result, Session, Usage,
};

const PLAN_FORMAT: &str = "pi-rs:before-settle-plan-v1";
const TURN_PLAN_FORMAT: &str = "pi-rs:turn-end-plan-v1";
const ACTIVITY_FORMAT: &str = "pi-rs:before-settle-activity-v1";

pub(crate) fn is_marker(format: &str) -> bool {
    matches!(format, PLAN_FORMAT | TURN_PLAN_FORMAT | ACTIVITY_FORMAT)
}

pub(crate) fn validate_navigation(session: &Session, target: u64) -> Result<()> {
    let boundary_end =
        session
            .path(target)
            .into_iter()
            .rev()
            .find_map(|record| match &record.event {
                Event::SourceEntry { format, entry }
                    if matches!(format.as_str(), PLAN_FORMAT | TURN_PLAN_FORMAT) =>
                {
                    entry["entries"]
                        .as_array()
                        .map(|entries| record.id.saturating_add(entries.len() as u64))
                }
                _ => None,
            });
    if boundary_end.is_some_and(|end| target < end) {
        return Err(Error::Session(
            "navigation cannot split an extension boundary plan".into(),
        ));
    }
    Ok(())
}

pub(crate) fn activity_marker() -> Event {
    Event::SourceEntry {
        format: ACTIVITY_FORMAT.into(),
        entry: Value::Null,
    }
}

pub(crate) fn activity_marker_id(session: &Session, run_id: uuid::Uuid) -> Option<u64> {
    session.branch().into_iter().rev().find_map(|record| {
        (record.run_id == Some(run_id)
            && matches!(&record.event, Event::SourceEntry { format, .. } if format == ACTIVITY_FORMAT))
        .then_some(record.id)
    })
}

pub(crate) fn has_activity_queue(session: &Session) -> bool {
    !session.queued_inputs().is_empty()
        || session.queued_custom_messages().iter().any(|(_, input)| {
            matches!(
                input.delivery,
                CustomMessageDelivery::Steer | CustomMessageDelivery::FollowUp
            )
        })
}

#[derive(Serialize, Deserialize)]
struct BoundaryPlan {
    entries: Vec<BoundaryDraft>,
    should_continue: bool,
    settled: BoundaryResult,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    turn_record: Option<u64>,
}

/// The result returned to the caller if the planned boundary stops the run.
/// Successful runs refer to the original assistant text; drafts can edit
/// projected context but do not change that answer.
#[derive(Serialize, Deserialize)]
enum BoundaryResult {
    AnswerAt(u64),
    Answer(String),
    Error,
    Cancelled,
}

impl BoundaryResult {
    fn capture(session: &Session, result: &Result<String>, cancelled: bool) -> Self {
        if cancelled {
            return Self::Cancelled;
        }
        match result {
            Ok(answer) => session
                .branch()
                .into_iter()
                .rev()
                .find_map(|record| {
                    matches!(&record.event, Event::Assistant { content, .. } if content == answer)
                        .then_some(Self::AnswerAt(record.id))
                })
                .unwrap_or_else(|| Self::Answer(answer.clone())),
            Err(_) => Self::Error,
        }
    }

    fn restore(self, session: &Session) -> Result<String> {
        match self {
            Self::AnswerAt(id) => {
                match session
                    .records()
                    .get(id.saturating_sub(1) as usize)
                    .map(|record| &record.event)
                {
                    Some(Event::Assistant { content, .. }) => Ok(content.clone()),
                    _ => Err(Error::Session("boundary answer record is missing".into())),
                }
            }
            Self::Answer(answer) => Ok(answer),
            Self::Error => Err(Error::Session(
                "previous activity failed before settlement".into(),
            )),
            Self::Cancelled => Err(Error::Cancelled),
        }
    }
}

pub(crate) enum RecoveredBoundary {
    Continue { activity_started: bool },
    TurnContinue,
    ProviderFailure(Error),
    Stop(Result<String>),
}

pub(crate) fn recover_agent_before_settle(
    session: &mut Session,
    run_id: uuid::Uuid,
    observer: &mut Observer<'_>,
) -> Result<Option<RecoveredBoundary>> {
    let mut activity_started = false;
    let mut terminal_failure = None;
    let mut observed_completion = false;
    let recovering_compaction =
        crate::agent::resumable_compaction_run(session).is_some_and(|(id, _, _)| id == run_id);
    for record in session.branch().into_iter().rev() {
        if record.run_id != Some(run_id) {
            continue;
        }
        match &record.event {
            Event::Run {
                state:
                    crate::RunState::Completed | crate::RunState::Cancelled | crate::RunState::Failed,
                ..
            }
            | Event::Assistant { .. } => break,
            Event::ModelFinished { failure, .. }
                if !observed_completion && !recovering_compaction =>
            {
                observed_completion = true;
                terminal_failure = match failure {
                    Some(crate::ModelFailure::Failed) => {
                        Some(Error::Model("saved continuation request failed".into()))
                    }
                    Some(crate::ModelFailure::Cancelled) => Some(Error::Cancelled),
                    Some(crate::ModelFailure::Retryable) | None => None,
                };
            }
            Event::SourceEntry { format, .. } if format == ACTIVITY_FORMAT => {
                activity_started = true;
            }
            Event::SourceEntry { format, entry }
                if matches!(format.as_str(), PLAN_FORMAT | TURN_PLAN_FORMAT) =>
            {
                let marker = record.id;
                let turn = format == TURN_PLAN_FORMAT;
                let plan: BoundaryPlan = serde_json::from_value(entry.clone())?;
                persist_plan_suffix(session, marker, &plan, observer)?;
                if (turn || activity_started)
                    && let Some(error) = terminal_failure
                {
                    return Ok(Some(RecoveredBoundary::ProviderFailure(error)));
                }
                if turn {
                    return Ok(plan
                        .should_continue
                        .then_some(RecoveredBoundary::TurnContinue));
                }
                if session.ensure_settled().is_ok() {
                    crate::agent::apply_settled_after_turn(session, observer)?;
                }
                return Ok(Some(
                    if continuation_is_runnable(session, plan.should_continue, observer)
                        || (!matches!(plan.settled, BoundaryResult::Cancelled)
                            && has_activity_queue(session))
                    {
                        RecoveredBoundary::Continue { activity_started }
                    } else {
                        RecoveredBoundary::Stop(plan.settled.restore(session))
                    },
                ));
            }
            _ => {}
        }
    }
    Ok(activity_started.then_some(RecoveredBoundary::Continue { activity_started }))
}

pub(crate) fn resumable_run(session: &Session) -> Option<uuid::Uuid> {
    let branch = session.branch();
    let (last_state, state, run_id) =
        branch.iter().rev().find_map(|record| match record.event {
            Event::Run { state, .. } => Some((record.id, state, record.run_id?)),
            _ => None,
        })?;
    if !matches!(
        state,
        crate::RunState::Running
            | crate::RunState::WaitingForOperation
            | crate::RunState::WaitingForReconciliation
    ) || session.records()[last_state as usize..]
        .iter()
        .any(|record| {
            record.run_id == Some(run_id)
                && matches!(
                    record.event,
                    Event::Run {
                        state: crate::RunState::Completed
                            | crate::RunState::Cancelled
                            | crate::RunState::Failed,
                        ..
                    }
                )
        })
    {
        return None;
    }
    branch
        .iter()
        .any(|record| {
            record.run_id == Some(run_id)
                && matches!(&record.event, Event::SourceEntry {format, ..} if is_marker(format))
        })
        .then_some(run_id)
}

pub(crate) fn repair_plan_on_open(session: &mut Session) -> Result<()> {
    if let Some(run_id) = resumable_run(session) {
        session.set_run_id(Some(run_id));
        let recovery = recover_agent_before_settle(session, run_id, &mut |_| {});
        session.set_run_id(None);
        recovery?;
    }
    Ok(())
}

fn plan_marker(entry: Value) -> Event {
    Event::SourceEntry {
        format: PLAN_FORMAT.into(),
        entry,
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentActivityOutcome {
    Completed,
    Aborted,
    Error,
}

/// A proposed branch entry. Drafts are validated together before applying any
/// of them to the live journal.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum BoundaryDraft {
    Custom {
        #[serde(flatten)]
        entry: CustomEntry,
    },
    CustomMessage {
        #[serde(flatten)]
        message: CustomMessage,
    },
    ContextEdit {
        target_id: u64,
        replacement: Option<ContextContent>,
    },
    Compaction {
        summary: String,
        first_kept_entry_id: Option<u64>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::extension::deserialize_present_json"
        )]
        details: Option<Value>,
        #[serde(default)]
        usage: Option<Usage>,
    },
}

/// Current model context after applying the proposed drafts in order.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BoundaryContext {
    pub context_messages: Vec<LifecycleMessage>,
    pub pending_messages: Vec<Message>,
    pub can_continue: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentBeforeSettle {
    pub outcome: AgentActivityOutcome,
    pub entries: Vec<BoundaryDraft>,
    #[serde(rename = "continue")]
    pub should_continue: bool,
    pub context: BoundaryContext,
}

/// Actionable turn boundary over the just-persisted assistant and tool results.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnEndBoundary {
    pub run_id: uuid::Uuid,
    pub turn_index: u32,
    pub message_entry_id: u64,
    pub message: Message,
    pub tool_result_entry_ids: Vec<u64>,
    pub tool_results: Vec<LifecycleMessage>,
    pub outcome: AgentActivityOutcome,
    pub entries: Vec<BoundaryDraft>,
    #[serde(rename = "continue")]
    pub should_continue: bool,
    pub context: BoundaryContext,
}

enum StructuralBoundary {
    Settle(AgentBeforeSettle),
    Turn(TurnEndBoundary),
}

impl StructuralBoundary {
    fn kind(&self) -> &'static str {
        match self {
            Self::Settle(_) => "agent_before_settle",
            Self::Turn(_) => "turn_end",
        }
    }

    fn format(&self) -> &'static str {
        match self {
            Self::Settle(_) => PLAN_FORMAT,
            Self::Turn(_) => TURN_PLAN_FORMAT,
        }
    }

    fn proposal(&mut self) -> (&mut Vec<BoundaryDraft>, &mut bool, &mut BoundaryContext) {
        match self {
            Self::Settle(event) => (
                &mut event.entries,
                &mut event.should_continue,
                &mut event.context,
            ),
            Self::Turn(event) => (
                &mut event.entries,
                &mut event.should_continue,
                &mut event.context,
            ),
        }
    }

    fn supported_by(&self, extension: &dyn crate::Extension) -> bool {
        match self {
            Self::Settle(_) => extension.handles_agent_before_settle(),
            Self::Turn(_) => extension.handles_turn_end_boundary(),
        }
    }

    async fn call(
        &self,
        extension: &dyn crate::Extension,
        state: Option<&crate::ExtensionState>,
    ) -> ExtensionResult<BoundaryDecision> {
        match self {
            Self::Settle(event) => extension.agent_before_settle(event, state).await,
            Self::Turn(event) => extension.turn_end_boundary(event, state).await,
        }
    }
}

/// Each supplied field replaces the previous handler's proposal. Omitted
/// fields keep their current values.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BoundaryDecision {
    #[serde(default)]
    pub entries: Option<Vec<BoundaryDraft>>,
    #[serde(default, rename = "continue")]
    pub should_continue: Option<bool>,
}

fn pending_messages(session: &Session) -> Vec<Message> {
    let user = session.queued_input_records().filter_map(|record| {
        let (kind, message) = match &record.event {
            Event::InputQueued { input } => (
                input.kind,
                Message::User {
                    content: input.content.clone(),
                },
            ),
            Event::InputQueuedWithImages { input } => (
                input.input.kind,
                Message::UserWithImages {
                    content: input.input.content.clone(),
                    images: input.images.clone(),
                },
            ),
            _ => return None,
        };
        Some((record.id, Some(kind), message))
    });
    let custom = session
        .queued_custom_messages()
        .into_iter()
        .filter_map(|(id, queued)| {
            let kind = match queued.delivery {
                CustomMessageDelivery::Steer => Some(InputKind::Steer),
                CustomMessageDelivery::FollowUp => Some(InputKind::FollowUp),
                CustomMessageDelivery::AfterTurn => None,
                CustomMessageDelivery::NextTurn | CustomMessageDelivery::TriggerTurn => {
                    return None;
                }
            };
            Some((
                id,
                kind,
                crate::journal::project_content(
                    Message::User {
                        content: String::new(),
                    },
                    &queued.message.content,
                ),
            ))
        });
    let mut pending: Vec<_> = user.chain(custom).collect();
    pending.sort_unstable_by_key(|(id, _, _)| *id);
    let next_kind = if pending
        .iter()
        .any(|(_, kind, _)| *kind == Some(InputKind::Steer))
    {
        InputKind::Steer
    } else {
        InputKind::FollowUp
    };
    let batch_size = if session.queue_mode(next_kind) == QueueMode::OneAtATime {
        1
    } else {
        usize::MAX
    };
    let (queued, context): (Vec<_>, Vec<_>) = pending
        .into_iter()
        .filter_map(|(_, kind, message)| {
            (kind.is_none() || kind == Some(next_kind)).then_some((kind, message))
        })
        .partition(|(kind, _)| kind.is_some());
    queued
        .into_iter()
        .take(batch_size)
        .chain(context)
        .map(|(_, message)| message)
        .collect()
}

fn context(session: &Session) -> BoundaryContext {
    let context_messages = crate::lifecycle::messages(session, |_| true);
    let pending_messages = pending_messages(session);
    let last = context_messages
        .iter()
        .rev()
        .find(|entry| !matches!(entry.message, Message::System { .. }));
    let can_continue = last.is_some_and(|entry| {
        !matches!(entry.message, Message::Assistant { .. }) || !pending_messages.is_empty()
    });
    BoundaryContext {
        context_messages,
        pending_messages,
        can_continue,
    }
}

pub(crate) fn continuation_is_runnable(
    session: &Session,
    requested: bool,
    observer: &mut Observer<'_>,
) -> bool {
    if !requested {
        return false;
    }
    if context(session).can_continue {
        return true;
    }
    report(
        "<boundary>",
        "continuation requires runnable model context",
        observer,
    );
    false
}

fn append_draft(session: &mut Session, draft: &BoundaryDraft) -> Result<()> {
    match draft {
        BoundaryDraft::Custom { entry } => session.append_custom_entry(entry.clone()),
        BoundaryDraft::CustomMessage { message } => session.append_custom_message(message.clone()),
        BoundaryDraft::ContextEdit {
            target_id,
            replacement,
        } => session.edit_context(*target_id, replacement.clone()),
        BoundaryDraft::Compaction {
            summary,
            first_kept_entry_id,
            details,
            usage,
        } => {
            session.ensure_settled()?;
            if let Some(id) = first_kept_entry_id
                && !session.branch().iter().any(|entry| entry.id == *id)
            {
                return Err(Error::Session(
                    "compaction boundary is outside the active branch".into(),
                ));
            }
            if details.is_some() && !session.supports_record_metadata() {
                return Err(Error::Session(
                    "session store cannot retain compaction details".into(),
                ));
            }
            let event = Event::Compacted {
                summary: summary.clone(),
                system_prompt: None,
                retain_from: *first_kept_entry_id,
                tokens_before: Some(session.estimated_context_tokens()),
                states: Default::default(),
                usage: *usage,
                usage_metadata: None,
            };
            let metadata = session.supports_record_metadata().then(|| match details {
                Some(details) => json!({"details": details, "fromExtension": true}),
                None => json!({"fromExtension": true}),
            });
            if let Some(metadata) = metadata {
                session.append_with_record_metadata(event, metadata)?;
            } else {
                session.append(event)?;
            }
            Ok(())
        }
    }
}

fn preview(session: &Session, drafts: &[BoundaryDraft]) -> Result<BoundaryContext> {
    if drafts.is_empty() {
        return Ok(context(session));
    }
    let mut preview = session.extension_boundary_snapshot();
    // Reserve the durable plan's position during previews so an entry ID
    // observed by the next handler remains the ID used for persistence.
    preview.append(plan_marker(Value::Null))?;
    for draft in drafts {
        append_draft(&mut preview, draft)?;
    }
    Ok(context(&preview))
}

/// Dispatch structural proposals against the live branch. The run owner
/// supplies the phase outcome and is responsible for starting a continuation.
pub(crate) async fn dispatch_agent_before_settle(
    registry: &Registry,
    session: &mut Session,
    result: &Result<String>,
    cancel: &crate::CancellationToken,
    timeout: Duration,
    observer: &mut Observer<'_>,
) -> Result<bool> {
    let event = AgentBeforeSettle {
        outcome: if result.is_ok() {
            AgentActivityOutcome::Completed
        } else {
            AgentActivityOutcome::Error
        },
        entries: Vec::new(),
        should_continue: false,
        context: context(session),
    };
    persist_boundary(
        registry,
        session,
        StructuralBoundary::Settle(event),
        result,
        cancel,
        timeout,
        observer,
    )
    .await
}

fn turn_plan_record(
    session: &Session,
    run_id: uuid::Uuid,
    assistant_record: u64,
) -> Option<&crate::Record> {
    session.branch().into_iter().rev().find(|record| {
        record.run_id == Some(run_id)
            && matches!(&record.event, Event::SourceEntry { format, entry }
                if format == TURN_PLAN_FORMAT && entry["turn_record"].as_u64() == Some(assistant_record))
    })
}

pub(crate) fn recovered_turn_observation(
    session: &Session,
    run_id: uuid::Uuid,
    assistant: u64,
    include: impl Fn(u64) -> bool,
) -> Vec<LifecycleMessage> {
    let Some(parent) =
        turn_plan_record(session, run_id, assistant).and_then(|record| record.parent)
    else {
        return crate::lifecycle::messages(session, include);
    };
    session
        .current_message_entries_at(parent)
        .into_iter()
        .filter(|(id, _)| include(*id))
        .map(|(record_id, mut message)| {
            if let Message::Assistant { replay, .. } = &mut message {
                *replay = None;
            }
            LifecycleMessage { record_id, message }
        })
        .collect()
}

/// Dispatch a completed native turn before notifying observational handlers.
pub(crate) async fn dispatch_turn_end(
    registry: &Registry,
    session: &mut Session,
    turn: (uuid::Uuid, u32, u64),
    cancel: &crate::CancellationToken,
    timeout: Duration,
    observer: &mut Observer<'_>,
) -> Result<bool> {
    let (run_id, turn_index, message_entry_id) = turn;
    if let Some(record) = turn_plan_record(session, run_id, message_entry_id) {
        let Event::SourceEntry { entry, .. } = &record.event else {
            unreachable!()
        };
        let plan: BoundaryPlan = serde_json::from_value(entry.clone())?;
        return persist_plan_suffix(session, record.id, &plan, observer);
    }
    if !registry
        .extensions
        .iter()
        .any(|extension| extension.handles_turn_end_boundary())
    {
        return Ok(false);
    }

    let Event::Assistant {
        content,
        invocations,
        ..
    } = &session.records()[(message_entry_id - 1) as usize].event
    else {
        return Err(Error::Session(
            "turn boundary assistant record is missing".into(),
        ));
    };
    let message = Message::Assistant {
        content: content.clone(),
        calls: invocations
            .iter()
            .map(|invocation| invocation.call.clone())
            .collect(),
        replay: None,
    };
    let branch = session.branch();
    let next_assistant = branch.iter().find_map(|record| {
        (record.id > message_entry_id
            && record.run_id == Some(run_id)
            && matches!(record.event, Event::Assistant { .. }))
        .then_some(record.id)
    });
    let tool_results: Vec<_> = crate::lifecycle::messages(session, |id| {
        id > message_entry_id
            && next_assistant.is_none_or(|end| id < end)
            && session.records()[(id - 1) as usize].run_id == Some(run_id)
    })
    .into_iter()
    .filter(|entry| matches!(entry.message, Message::Tool { .. }))
    .collect();
    let outcome = if cancel.is_cancelled() {
        AgentActivityOutcome::Aborted
    } else if crate::journal::failed_assistant_records(&branch).contains(&message_entry_id) {
        AgentActivityOutcome::Error
    } else {
        AgentActivityOutcome::Completed
    };
    let mut preview = context(session);
    preview.can_continue |= !preview.pending_messages.is_empty();
    let event = TurnEndBoundary {
        run_id,
        turn_index,
        message_entry_id,
        message,
        tool_result_entry_ids: tool_results.iter().map(|result| result.record_id).collect(),
        tool_results,
        outcome,
        entries: Vec::new(),
        should_continue: false,
        context: preview,
    };
    let result = Ok(String::new());
    persist_boundary(
        registry,
        session,
        StructuralBoundary::Turn(event),
        &result,
        cancel,
        timeout,
        observer,
    )
    .await
}

pub(crate) fn turn_continuation_is_runnable(
    session: &Session,
    requested: bool,
    observer: &mut Observer<'_>,
) -> bool {
    if !requested {
        return false;
    }
    let preview = context(session);
    if preview.can_continue || !preview.pending_messages.is_empty() {
        return true;
    }
    report_event(
        "turn_end",
        "<boundary>",
        "continuation requires runnable model context",
        observer,
    );
    false
}

async fn persist_boundary(
    registry: &Registry,
    session: &mut Session,
    mut event: StructuralBoundary,
    result: &Result<String>,
    cancel: &crate::CancellationToken,
    timeout: Duration,
    observer: &mut Observer<'_>,
) -> Result<bool> {
    let started = crate::runtime::time::Instant::now();
    let hook_budget = timeout.min(Duration::from_secs(10));
    let deadline = started + hook_budget;
    let states = session.extension_states();
    let kind = event.kind();
    let format = event.format();
    let mut invalid_entries = None;
    for extension in &registry.extensions {
        if !event.supported_by(extension.as_ref()) {
            continue;
        }
        if cancel.is_cancelled() {
            break;
        }
        if started.elapsed() >= hook_budget {
            report_event(
                kind,
                extension.id(),
                "boundary handler deadline expired",
                observer,
            );
            break;
        }
        let callback = std::panic::AssertUnwindSafe(async {
            event
                .call(extension.as_ref(), states.get(extension.id()))
                .await
        })
        .catch_unwind();
        let result: ExtensionResult<BoundaryDecision> =
            match crate::runtime::time::timeout_at(deadline, callback).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err("boundary handler panicked".into()),
                Err(_) => Err("boundary handler timed out".into()),
            };
        let (drafts, continuation, projection) = event.proposal();
        match result {
            Ok(decision) => {
                if let Some(should_continue) = decision.should_continue {
                    *continuation = should_continue;
                }
                if let Some(entries) = decision.entries {
                    *drafts = entries;
                    invalid_entries = match preview(session, drafts) {
                        Ok(mut context) => {
                            if format == TURN_PLAN_FORMAT && !context.pending_messages.is_empty() {
                                context.can_continue = true;
                            }
                            *projection = context;
                            None
                        }
                        Err(error) => Some(format!("invalid boundary entries: {error}")),
                    };
                }
            }
            Err(error) => report_event(kind, extension.id(), &error, observer),
        }
        if let Some(error) = &invalid_entries {
            report_event(kind, extension.id(), error, observer);
        }
    }
    if invalid_entries.is_some() {
        return Ok(false);
    }
    let cancelled = cancel.is_cancelled();
    let turn_record = match &event {
        StructuralBoundary::Turn(turn) => Some(turn.message_entry_id),
        StructuralBoundary::Settle(_) => None,
    };
    let hard_exit = matches!(&event, StructuralBoundary::Turn(turn)
        if !matches!(turn.outcome, AgentActivityOutcome::Completed));
    let (entries, continuation, _) = event.proposal();
    let plan = BoundaryPlan {
        entries: std::mem::take(entries),
        should_continue: !cancelled && !hard_exit && *continuation,
        settled: turn_record.map_or_else(
            || BoundaryResult::capture(session, result, cancelled),
            BoundaryResult::AnswerAt,
        ),
        turn_record,
    };
    let marker = Event::SourceEntry {
        format: format.into(),
        entry: serde_json::to_value(&plan)?,
    };
    // Catch deterministic store limits before writing the plan itself.
    if let Err(error) = session.extension_boundary_snapshot().append(marker.clone()) {
        report_event(
            kind,
            "<boundary>",
            &format!("cannot persist boundary plan: {error}"),
            observer,
        );
        return Ok(false);
    }
    let marker = session.append(marker)?;
    let marker_id = marker.id;
    observer(Notification::Committed(marker));
    persist_plan_suffix(session, marker_id, &plan, observer)
}

fn persist_plan_suffix(
    session: &mut Session,
    marker_id: u64,
    plan: &BoundaryPlan,
    observer: &mut Observer<'_>,
) -> Result<bool> {
    let completed = session
        .head()
        .checked_sub(marker_id)
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| Error::Session("boundary plan is outside its journal window".into()))?
        .min(plan.entries.len());
    if completed < plan.entries.len() && session.head() != session.records().len() as u64 {
        return Err(Error::Session(
            "boundary suffix no longer has its reserved journal IDs".into(),
        ));
    }
    let start = usize::try_from(marker_id)
        .map_err(|_| Error::Session("invalid boundary plan entry ID".into()))?;
    let prefix = session
        .records()
        .get(start..start.saturating_add(completed))
        .ok_or_else(|| Error::Session("boundary plan prefix is missing".into()))?;
    let mut parent = marker_id;
    for (record, draft) in prefix.iter().zip(&plan.entries) {
        if record.parent != Some(parent)
            || !matches_committed_draft(draft, &record.event, session.record_metadata(record.id))
        {
            return Err(Error::Session(
                "unexpected entry inside the boundary plan".into(),
            ));
        }
        parent = record.id;
    }
    if completed != 0 && !session.branch().iter().any(|record| record.id == parent) {
        return Err(Error::Session(
            "boundary plan prefix is outside the active branch".into(),
        ));
    }
    for draft in &plan.entries[completed..] {
        let start = session.records().len();
        append_draft(session, draft)?;
        let [record] = &session.records()[start..] else {
            return Err(Error::Session(
                "boundary draft must append exactly one entry".into(),
            ));
        };
        observer(Notification::Committed(record));
    }
    Ok(plan.should_continue)
}

fn matches_committed_draft(draft: &BoundaryDraft, event: &Event, metadata: Option<&Value>) -> bool {
    match (draft, event) {
        (BoundaryDraft::Custom { entry: expected }, Event::CustomEntry { entry }) => {
            expected.custom_type == entry.custom_type && expected.data == entry.data
        }
        (BoundaryDraft::CustomMessage { message: expected }, Event::CustomMessage { message }) => {
            expected.custom_type == message.custom_type
                && expected.content == message.content
                && expected.display == message.display
                && expected.details == message.details
        }
        (
            BoundaryDraft::ContextEdit {
                target_id,
                replacement: expected,
            },
            Event::ContextEdited {
                target,
                replacement,
            },
        ) => target_id == target && expected == replacement,
        (
            BoundaryDraft::Compaction {
                summary: expected,
                first_kept_entry_id,
                details,
                usage,
            },
            Event::Compacted {
                summary,
                retain_from,
                usage: recorded_usage,
                ..
            },
        ) => {
            expected == summary
                && first_kept_entry_id == retain_from
                && usage == recorded_usage
                && details.as_ref() == metadata.and_then(|value| value.get("details"))
        }
        _ => false,
    }
}

fn report_event(kind: &'static str, extension: &str, message: &str, observer: &mut Observer<'_>) {
    observer(Notification::ExtensionLifecycleError {
        extension,
        event: kind,
        message,
    });
}

fn report(extension: &str, message: &str, observer: &mut Observer<'_>) {
    observer(Notification::ExtensionLifecycleError {
        extension,
        event: "agent_before_settle",
        message,
    });
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::{Extension, MemoryStore};

    #[tokio::test]
    async fn actionable_turn_end_continues_without_new_input_and_replays_the_durable_plan() {
        struct Model {
            requests: Mutex<Vec<crate::ModelRequest>>,
            responses: std::sync::atomic::AtomicUsize,
        }
        #[async_trait::async_trait]
        impl crate::Model for Model {
            async fn stream(&self, request: crate::ModelRequest) -> Result<crate::ModelStream> {
                let turn = self
                    .responses
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.requests.lock().unwrap().push(request);
                let answer = if turn == 0 {
                    "replace this"
                } else {
                    "final answer"
                };
                Ok(Box::pin(futures_util::stream::iter([
                    Ok(crate::ModelEvent::Text(answer.into())),
                    Ok(crate::ModelEvent::Complete {
                        calls: Vec::new(),
                        usage: None,
                    }),
                ])))
            }
        }

        struct Replace(Arc<Mutex<Vec<Vec<u64>>>>);
        #[async_trait::async_trait]
        impl Extension for Replace {
            fn id(&self) -> &str {
                "replace-turn"
            }
            fn handles_lifecycle(&self) -> bool {
                true
            }

            async fn lifecycle(
                &self,
                event: &crate::LifecycleEvent,
                _: Option<&crate::ExtensionState>,
            ) -> ExtensionResult<()> {
                if let crate::LifecycleEvent::TurnEnd { messages, .. } = event {
                    self.0
                        .lock()
                        .unwrap()
                        .push(messages.iter().map(|message| message.record_id).collect());
                }
                Ok(())
            }

            fn handles_turn_end_boundary(&self) -> bool {
                true
            }
            async fn turn_end_boundary(
                &self,
                event: &TurnEndBoundary,
                _: Option<&crate::ExtensionState>,
            ) -> ExtensionResult<BoundaryDecision> {
                if event.turn_index != 0 {
                    return Ok(BoundaryDecision::default());
                }
                assert!(
                    matches!(&event.message, Message::Assistant { content, .. } if content == "replace this")
                );
                assert!(event.tool_result_entry_ids.is_empty());
                Ok(BoundaryDecision {
                    entries: Some(vec![BoundaryDraft::ContextEdit {
                        target_id: event.message_entry_id,
                        replacement: None,
                    }]),
                    should_continue: Some(true),
                })
            }
        }

        let model = Model {
            requests: Mutex::default(),
            responses: 0.into(),
        };
        let mut registry = Registry::default();
        let observations = Arc::new(Mutex::new(Vec::new()));
        registry
            .register(Arc::new(Replace(observations.clone())))
            .unwrap();
        let agent = crate::Agent {
            model: &model,
            registry: &registry,
            model_name: "fixture",
            system_prompt: "help",
            limits: Default::default(),
        };
        let mut journal = Session::with_store(MemoryStore::default()).unwrap();
        let (_, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
        let run = uuid::Uuid::new_v4();
        let cancel = crate::CancellationToken::new();
        assert_eq!(
            agent
                .run_correlated(
                    &mut journal,
                    run,
                    Some("review".into()),
                    &mut inputs,
                    &cancel,
                    &mut |_| {}
                )
                .await
                .unwrap(),
            "final answer"
        );
        {
            let requests = model.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert!(
                !requests[1]
                    .messages
                    .iter()
                    .any(|message| matches!(message, Message::Assistant { .. }))
            );
        }
        let marker = journal.records().iter().find(|record| matches!(&record.event, Event::SourceEntry { format, .. } if format == TURN_PLAN_FORMAT)).unwrap().id;
        let before_drafts = observations.lock().unwrap()[0].clone();
        assert_eq!(before_drafts.len(), 1);
        let finished = journal
            .records()
            .iter()
            .find(|record| record.id > marker && matches!(record.event, Event::TurnFinished { .. }))
            .unwrap()
            .id;
        for cutoff in [marker, finished] {
            let records = journal.records()[..cutoff as usize].to_vec();
            let metadata = records
                .iter()
                .filter_map(|record| {
                    journal
                        .record_metadata(record.id)
                        .cloned()
                        .map(|value| (record.id, value))
                })
                .collect();
            let mut restored = Session::from_records_with_metadata(records, metadata).unwrap();
            model.requests.lock().unwrap().clear();
            observations.lock().unwrap().clear();
            assert_eq!(
                agent
                    .run_correlated(&mut restored, run, None, &mut inputs, &cancel, &mut |_| {})
                    .await
                    .unwrap(),
                "final answer"
            );
            assert_eq!(model.requests.lock().unwrap().len(), 1);
            if cutoff == marker {
                assert_eq!(observations.lock().unwrap()[0], before_drafts);
            }
            assert_eq!(
                restored
                    .records()
                    .iter()
                    .filter(|record| matches!(record.event, Event::ContextEdited { .. }))
                    .count(),
                1
            );
        }
        let response = journal
            .records()
            .iter()
            .find(|record| {
                record.id > finished && matches!(record.event, Event::ModelFinished { .. })
            })
            .unwrap();
        let records = journal.records()[..response.id as usize - 1].to_vec();
        let metadata = records
            .iter()
            .filter_map(|record| {
                journal
                    .record_metadata(record.id)
                    .cloned()
                    .map(|value| (record.id, value))
            })
            .collect();
        let mut failed = Session::from_records_with_metadata(records, metadata).unwrap();
        let mut event = response.event.clone();
        let Event::ModelFinished { failure, reply, .. } = &mut event else {
            unreachable!()
        };
        *failure = Some(crate::ModelFailure::Failed);
        *reply = None;
        failed.set_run_id(Some(run));
        failed.append(event).unwrap();
        failed.set_run_id(None);
        model.requests.lock().unwrap().clear();
        assert!(matches!(
            agent
                .run_correlated(&mut failed, run, None, &mut inputs, &cancel, &mut |_| {})
                .await,
            Err(Error::Model(_))
        ));
        assert!(model.requests.lock().unwrap().is_empty());

        let records = failed.records()[..response.id as usize - 1].to_vec();
        let metadata = records
            .iter()
            .filter_map(|record| {
                failed
                    .record_metadata(record.id)
                    .cloned()
                    .map(|value| (record.id, value))
            })
            .collect();
        let mut overflow = Session::from_records_with_metadata(records, metadata).unwrap();
        overflow.set_run_id(Some(run));
        let mut event = response.event.clone();
        let Event::ModelFinished {
            metadata,
            failure,
            reply,
            ..
        } = &mut event
        else {
            unreachable!()
        };
        metadata.finish_reason = Some("pi-rs:context-overflow".into());
        *failure = Some(crate::ModelFailure::Failed);
        *reply = None;
        overflow.append(event).unwrap();
        assert!(crate::agent::resumable_compaction_run(&overflow).is_some());
        assert!(matches!(
            recover_agent_before_settle(&mut overflow, run, &mut |_| {}).unwrap(),
            Some(RecoveredBoundary::TurnContinue)
        ));

        journal.checkout(marker + 1).unwrap();
        assert_eq!(journal.pending_extension_boundary_run(), None);
        assert_eq!(
            agent
                .run_correlated(&mut journal, run, None, &mut inputs, &cancel, &mut |_| {})
                .await
                .unwrap(),
            "replace this"
        );
        assert!(model.requests.lock().unwrap().is_empty());
    }

    #[derive(Default)]
    struct CaptureModel(Mutex<Vec<crate::ModelRequest>>);

    #[async_trait::async_trait]
    impl crate::Model for CaptureModel {
        async fn stream(&self, request: crate::ModelRequest) -> Result<crate::ModelStream> {
            self.0.lock().unwrap().push(request);
            let answer = if self
                .0
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .messages
                .iter()
                .any(|message| {
                    matches!(
                        message, Message::User { content } if content == "continue here"
                    )
                }) {
                "second response"
            } else {
                "first response"
            };
            Ok(Box::pin(futures_util::stream::iter([
                Ok(crate::ModelEvent::Text(answer.into())),
                Ok(crate::ModelEvent::Complete {
                    calls: Vec::new(),
                    usage: None,
                }),
            ])))
        }
    }

    struct ReviewHook {
        phases: Arc<Mutex<Vec<&'static str>>>,
        abort: Option<crate::CancellationToken>,
        after_turn: bool,
    }

    #[async_trait::async_trait]
    impl Extension for ReviewHook {
        fn id(&self) -> &str {
            "review-hook"
        }

        fn handles_agent_before_settle(&self) -> bool {
            true
        }

        fn handles_lifecycle(&self) -> bool {
            true
        }

        async fn lifecycle(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
        ) -> ExtensionResult<()> {
            self.phases.lock().unwrap().push(event.kind());
            Ok(())
        }

        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            state: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> ExtensionResult<()> {
            self.lifecycle(event, state).await?;
            if self.after_turn
                && matches!(event, crate::LifecycleEvent::AgentEnd { messages, .. }
                if messages.iter().any(|entry| matches!(&entry.message,
                    Message::Assistant { content, .. } if content == "first response"
                )))
            {
                host.send_message(
                    CustomMessage {
                        custom_type: "agent-end".into(),
                        content: ContextContent::Text("agent end context".into()),
                        display: false,
                        details: None,
                    },
                    CustomMessageDelivery::AfterTurn,
                )
                .await?;
            }
            Ok(())
        }

        async fn agent_before_settle(
            &self,
            event: &AgentBeforeSettle,
            _: Option<&crate::ExtensionState>,
        ) -> ExtensionResult<BoundaryDecision> {
            self.phases.lock().unwrap().push("agent_before_settle");
            let Some(assistant) = event.context.context_messages.iter().find(|entry| matches!(
                &entry.message, Message::Assistant { content, .. } if content == "first response"
            )) else {
                return Ok(BoundaryDecision::default());
            };
            if self.after_turn {
                assert!(event.context.context_messages.iter().any(
                    |entry| matches!(&entry.message,
                        Message::User { content } if content == "agent end context"
                    )
                ));
            }
            if let Some(abort) = &self.abort {
                abort.cancel();
            }
            Ok(BoundaryDecision {
                entries: Some(vec![
                    BoundaryDraft::ContextEdit {
                        target_id: assistant.record_id,
                        replacement: None,
                    },
                    BoundaryDraft::CustomMessage {
                        message: CustomMessage {
                            custom_type: "review".into(),
                            content: ContextContent::Text("continue here".into()),
                            display: false,
                            details: None,
                        },
                    },
                ]),
                should_continue: Some(true),
            })
        }
    }

    #[tokio::test]
    async fn agent_before_settle_continues_once_and_recovers_without_replaying_a_decision() {
        let model = CaptureModel::default();
        let phases = Arc::new(Mutex::new(Vec::new()));
        let mut registry = Registry::default();
        registry
            .register(Arc::new(ReviewHook {
                phases: phases.clone(),
                abort: None,
                after_turn: true,
            }))
            .unwrap();
        let agent = crate::Agent {
            model: &model,
            registry: &registry,
            model_name: "fixture",
            system_prompt: "help",
            limits: Default::default(),
        };
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let run = uuid::Uuid::new_v4();
        let (_, mut queue) = tokio::sync::mpsc::channel::<String>(1);
        let cancel = crate::CancellationToken::new();
        assert_eq!(
            agent
                .run_correlated(
                    &mut session,
                    run,
                    Some("review this".into()),
                    &mut queue,
                    &cancel,
                    &mut |_| {}
                )
                .await
                .unwrap(),
            "second response"
        );
        {
            let calls = model.0.lock().unwrap();
            assert_eq!(calls.len(), 2);
            assert!(calls[1].messages.iter().any(|message| matches!(message,
                Message::User { content } if content == "agent end context"
            )));
            assert!(!calls[1].messages.iter().any(|message| matches!(message, Message::Assistant { content, .. } if content == "first response")));
        }
        assert_eq!(
            phases
                .lock()
                .unwrap()
                .iter()
                .copied()
                .filter(|phase| phase.starts_with("agent_"))
                .collect::<Vec<_>>(),
            [
                "agent_start",
                "agent_end",
                "agent_before_settle",
                "agent_start",
                "agent_end",
                "agent_before_settle",
                "agent_settled"
            ],
        );

        let marker = session
            .records()
            .iter()
            .find(|record| {
                matches!(&record.event,
                    Event::SourceEntry { format, .. } if format == PLAN_FORMAT
                )
            })
            .unwrap()
            .id;
        let activity = activity_marker_id(&session, run).unwrap();
        let second_answer = session
            .records()
            .iter()
            .find(|record| {
                matches!(
                    &record.event, Event::Assistant { content, .. } if content == "second response"
                )
            })
            .unwrap()
            .id;
        let second_reply = session
            .records()
            .iter()
            .rev()
            .find(|record| {
                record.id < second_answer
                    && matches!(&record.event, Event::ModelFinished { reply: Some(_), .. })
            })
            .unwrap()
            .id;
        for (cutoff, expected_requests) in [
            (marker, 1),
            (marker + 1, 1),
            (activity, 1),
            (second_reply, 0),
            (second_answer, 0),
        ] {
            let records = session.records()[..cutoff as usize].to_vec();
            let metadata = records
                .iter()
                .filter_map(|record| {
                    session
                        .record_metadata(record.id)
                        .cloned()
                        .map(|metadata| (record.id, metadata))
                })
                .collect();
            let mut restored = Session::from_records_with_metadata(records, metadata).unwrap();
            assert_eq!(restored.pending_extension_boundary_run(), Some(run));
            let before = restored.records().len();
            assert!(
                agent
                    .run(
                        &mut restored,
                        Some("new prompt".into()),
                        &mut queue,
                        &cancel,
                        &mut |_| {}
                    )
                    .await
                    .is_err()
            );
            assert_eq!(restored.records().len(), before);
            phases.lock().unwrap().clear();
            model.0.lock().unwrap().clear();
            assert_eq!(
                agent
                    .run_correlated(&mut restored, run, None, &mut queue, &cancel, &mut |_| {})
                    .await
                    .unwrap(),
                "second response"
            );
            assert_eq!(model.0.lock().unwrap().len(), expected_requests);
            assert_eq!(
                phases
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|&&phase| phase == "agent_before_settle")
                    .count(),
                1
            );
            assert_eq!(
                restored
                    .records()
                    .iter()
                    .filter(|record| matches!(record.event, Event::ContextEdited { .. }))
                    .count(),
                1
            );
        }

        let records = session.records()[..marker as usize].to_vec();
        let metadata = records
            .iter()
            .filter_map(|record| {
                session
                    .record_metadata(record.id)
                    .cloned()
                    .map(|metadata| (record.id, metadata))
            })
            .collect();
        let restored = Session::from_records_with_metadata(records, metadata).unwrap();
        let sdk_model = Arc::new(CaptureModel::default());
        let mut sdk = crate::SessionBuilder::new(sdk_model.clone(), "fixture")
            .extension(Arc::new(ReviewHook {
                phases: Arc::default(),
                abort: None,
                after_turn: false,
            }))
            .unwrap()
            .build(restored)
            .unwrap();
        assert_eq!(sdk.resume().await.unwrap(), "second response");
        assert_eq!(sdk_model.0.lock().unwrap().len(), 1);
        assert_eq!(
            sdk.journal_async().await.pending_extension_boundary_run(),
            None
        );
        sdk.close().await.unwrap();

        let completed_plan = marker + 2;
        assert!(
            session
                .fork_records(completed_plan)
                .unwrap()
                .iter()
                .all(|record| {
                    !matches!(&record.event, Event::SourceEntry { format, .. } if is_marker(format))
                })
        );
        session.checkout(completed_plan).unwrap();
        assert_eq!(session.pending_extension_boundary_run(), None);
    }

    #[tokio::test]
    async fn deferred_message_arriving_during_settlement_makes_continuation_runnable() {
        struct LateMessage(tokio::sync::mpsc::Sender<crate::AgentInput>);

        #[async_trait::async_trait]
        impl Extension for LateMessage {
            fn id(&self) -> &str {
                "late-message"
            }

            fn handles_agent_before_settle(&self) -> bool {
                true
            }

            async fn agent_before_settle(
                &self,
                event: &AgentBeforeSettle,
                _: Option<&crate::ExtensionState>,
            ) -> ExtensionResult<BoundaryDecision> {
                if !matches!(event.context.context_messages.last(),
                    Some(entry) if matches!(&entry.message,
                        Message::Assistant { content, .. } if content == "first response"))
                {
                    return Ok(BoundaryDecision::default());
                };
                assert!(!event.context.can_continue);
                let (reply, _) = tokio::sync::oneshot::channel();
                self.0
                    .send(crate::AgentInput::EnqueueCustom {
                        message: crate::QueuedCustomMessage {
                            id: uuid::Uuid::new_v4(),
                            delivery: CustomMessageDelivery::AfterTurn,
                            message: CustomMessage {
                                custom_type: "late".into(),
                                content: ContextContent::Text("continue here".into()),
                                display: false,
                                details: None,
                            },
                        },
                        reply,
                    })
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(BoundaryDecision {
                    entries: None,
                    should_continue: Some(true),
                })
            }
        }

        let (sender, mut queue) = tokio::sync::mpsc::channel(1);
        let mut registry = Registry::default();
        registry.register(Arc::new(LateMessage(sender))).unwrap();
        let model = CaptureModel::default();
        let agent = crate::Agent {
            model: &model,
            registry: &registry,
            model_name: "fixture",
            system_prompt: "help",
            limits: Default::default(),
        };
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let run = uuid::Uuid::new_v4();
        let cancel = crate::CancellationToken::new();
        assert_eq!(
            agent
                .run_correlated(
                    &mut session,
                    run,
                    Some("review".into()),
                    &mut queue,
                    &cancel,
                    &mut |_| {}
                )
                .await
                .unwrap(),
            "second response"
        );
        let queued = session.records().iter().find(|record| matches!(
            &record.event, Event::CustomMessageQueued { input } if input.message.custom_type == "late"
        )).unwrap().id;
        let records = session.records()[..queued as usize].to_vec();
        let metadata = records
            .iter()
            .filter_map(|record| {
                session
                    .record_metadata(record.id)
                    .cloned()
                    .map(|value| (record.id, value))
            })
            .collect();
        let mut restored = Session::from_records_with_metadata(records, metadata).unwrap();
        model.0.lock().unwrap().clear();
        assert_eq!(
            agent
                .run_correlated(&mut restored, run, None, &mut queue, &cancel, &mut |_| {})
                .await
                .unwrap(),
            "second response"
        );
        assert_eq!(model.0.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn abort_during_agent_before_settle_keeps_drafts_and_stops_continuation() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let model = CaptureModel::default();
        let cancel = crate::CancellationToken::new();
        let phases = Arc::new(Mutex::new(Vec::new()));
        let mut registry = Registry::default();
        registry
            .register(Arc::new(ReviewHook {
                phases: phases.clone(),
                abort: Some(cancel.clone()),
                after_turn: false,
            }))
            .unwrap();
        let agent = crate::Agent {
            model: &model,
            registry: &registry,
            model_name: "fixture",
            system_prompt: "help",
            limits: Default::default(),
        };
        let (_, mut queue) = tokio::sync::mpsc::channel::<String>(1);
        assert!(matches!(
            agent
                .run(
                    &mut session,
                    Some("review this".into()),
                    &mut queue,
                    &cancel,
                    &mut |_| {}
                )
                .await,
            Err(Error::Cancelled)
        ));
        assert_eq!(model.0.lock().unwrap().len(), 1);
        assert!(session.records().iter().any(|record| matches!(&record.event, Event::CustomMessage { message } if message.custom_type == "review")));
        assert_eq!(
            phases.lock().unwrap().last().copied(),
            Some("agent_settled")
        );
    }

    struct Hook {
        id: &'static str,
        recovered: bool,
    }

    #[async_trait::async_trait]
    impl Extension for Hook {
        fn id(&self) -> &str {
            self.id
        }

        fn handles_agent_before_settle(&self) -> bool {
            true
        }

        async fn agent_before_settle(
            &self,
            event: &AgentBeforeSettle,
            _: Option<&crate::ExtensionState>,
        ) -> ExtensionResult<BoundaryDecision> {
            let reminder = BoundaryDraft::CustomMessage {
                message: CustomMessage {
                    custom_type: "reminder".into(),
                    content: ContextContent::Text("one more request".into()),
                    display: false,
                    details: None,
                },
            };
            Ok(match self.id {
                "draft" => BoundaryDecision {
                    entries: Some(vec![reminder]),
                    should_continue: Some(true),
                },
                "invalidate" => {
                    assert!(event.context.can_continue);
                    assert!(matches!(
                        &event.context.context_messages.last().unwrap().message,
                        Message::User { content } if content == "one more request"
                    ));
                    let mut entries = event.entries.clone();
                    entries.push(BoundaryDraft::ContextEdit {
                        target_id: u64::MAX,
                        replacement: None,
                    });
                    BoundaryDecision {
                        entries: Some(entries),
                        ..Default::default()
                    }
                }
                _ => {
                    assert_eq!(event.entries.len(), 2);
                    assert!(event.should_continue);
                    assert!(event.context.can_continue);
                    BoundaryDecision {
                        entries: self.recovered.then_some(vec![reminder]),
                        ..Default::default()
                    }
                }
            })
        }
    }

    #[tokio::test]
    async fn invalid_boundary_drafts_are_atomic_and_can_be_replaced_by_a_later_handler() {
        for recovered in [false, true] {
            let mut session = Session::with_store(MemoryStore::default()).unwrap();
            let head = session.head();
            let mut registry = Registry::default();
            for id in ["draft", "invalidate", "recover"] {
                registry.register(Arc::new(Hook { id, recovered })).unwrap();
            }
            let mut errors = Vec::new();
            let mut committed = Vec::new();
            let requested = dispatch_agent_before_settle(
                &registry,
                &mut session,
                &Ok("first answer".into()),
                &crate::CancellationToken::new(),
                Duration::from_secs(1),
                &mut |notification| match notification {
                    Notification::ExtensionLifecycleError { extension, .. } => {
                        errors.push(extension.to_owned())
                    }
                    Notification::Committed(record) => committed.push(record.id),
                    _ => {}
                },
            )
            .await
            .unwrap();
            assert_eq!(requested, recovered);
            if recovered {
                assert_eq!(errors, ["invalidate"]);
                assert_eq!(committed, [head + 1, head + 2]);
                assert_eq!(session.head(), head + 2);
            } else {
                assert_eq!(errors, ["invalidate", "recover"]);
                assert!(committed.is_empty());
                assert_eq!(session.head(), head);
            }
        }
    }

    #[test]
    fn boundary_preview_uses_next_queue_batch_and_retains_compaction_context() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .set_queue_mode(InputKind::Steer, QueueMode::OneAtATime)
            .unwrap();
        for (kind, text) in [
            (InputKind::FollowUp, "future follow-up"),
            (InputKind::Steer, "first steering"),
            (InputKind::Steer, "future steering"),
        ] {
            session
                .append(Event::InputQueued {
                    input: crate::QueuedInput {
                        id: uuid::Uuid::new_v4(),
                        kind,
                        content: text.into(),
                    },
                })
                .unwrap();
        }
        for (delivery, text) in [
            (CustomMessageDelivery::TriggerTurn, "separate run"),
            (CustomMessageDelivery::NextTurn, "next prompt"),
            (CustomMessageDelivery::AfterTurn, "deferred context"),
        ] {
            session
                .append(Event::CustomMessageQueued {
                    input: crate::QueuedCustomMessage {
                        id: uuid::Uuid::new_v4(),
                        delivery,
                        message: CustomMessage {
                            custom_type: "fixture".into(),
                            content: ContextContent::Text(text.into()),
                            display: false,
                            details: None,
                        },
                    },
                })
                .unwrap();
        }
        let head = session.head();
        let preview = preview(
            &session,
            &[BoundaryDraft::Compaction {
                summary: "preserved summary".into(),
                first_kept_entry_id: None,
                details: Some(json!({"tag":"test"})),
                usage: None,
            }],
        )
        .unwrap();
        assert!(preview.can_continue);
        assert_eq!(preview.context_messages.len(), 1);
        assert_eq!(preview.context_messages[0].record_id, head + 2);
        assert!(
            matches!(&preview.context_messages[0].message, Message::User { content } if content.ends_with("preserved summary"))
        );
        let queued: Vec<_> = preview
            .pending_messages
            .iter()
            .filter_map(|message| match message {
                Message::User { content } => Some(content.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(queued, ["first steering", "deferred context"]);
        assert_eq!(session.head(), head);
    }

    #[test]
    fn durable_boundary_plan_applies_only_its_uncommitted_suffix_after_reopen() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let plan = BoundaryPlan {
            entries: vec![
                BoundaryDraft::Custom {
                    entry: CustomEntry {
                        custom_type: "checkpoint".into(),
                        data: None,
                    },
                },
                BoundaryDraft::CustomMessage {
                    message: CustomMessage {
                        custom_type: "prompt".into(),
                        content: ContextContent::Text("continue here".into()),
                        display: false,
                        details: None,
                    },
                },
            ],
            should_continue: true,
            settled: BoundaryResult::Answer("prior answer".into()),
            turn_record: None,
        };
        let marker_id = session
            .append(plan_marker(serde_json::to_value(&plan).unwrap()))
            .unwrap()
            .id;
        append_draft(&mut session, &plan.entries[0]).unwrap();
        let mut reopened = Session::from_records(session.records().to_vec()).unwrap();
        let Event::SourceEntry { entry, .. } = &reopened.records()[(marker_id - 1) as usize].event
        else {
            panic!("expected durable boundary plan");
        };
        let recovered: BoundaryPlan = serde_json::from_value(entry.clone()).unwrap();
        let mut committed = Vec::new();
        assert!(
            persist_plan_suffix(&mut reopened, marker_id, &recovered, &mut |event| {
                if let Notification::Committed(record) = event {
                    committed.push(record.id);
                }
            })
            .unwrap()
        );
        assert_eq!(committed, [marker_id + 2]);
        assert_eq!(reopened.head(), marker_id + 2);
        assert!(
            matches!(reopened.current_messages().as_slice(), [Message::User { content }] if content == "continue here")
        );

        let mut unexpected =
            Session::from_records(session.records()[..marker_id as usize].to_vec()).unwrap();
        unexpected
            .append(Event::User {
                content: "unrelated".into(),
                input_id: None,
            })
            .unwrap();
        assert!(persist_plan_suffix(&mut unexpected, marker_id, &recovered, &mut |_| {}).is_err());
        assert_eq!(unexpected.head(), marker_id + 1);

        let mut diverged = Session::from_records(session.records().to_vec()).unwrap();
        diverged
            .append(Event::User {
                content: "off-branch input".into(),
                input_id: None,
            })
            .unwrap();
        assert!(diverged.checkout(marker_id + 1).is_err());
        assert!(diverged.fork_records(marker_id + 1).is_err());
        diverged
            .append(Event::Checkout {
                target: marker_id + 1,
            })
            .unwrap();
        assert_eq!(diverged.head(), marker_id + 1);
        let global_record_count = diverged.records().len();
        assert!(persist_plan_suffix(&mut diverged, marker_id, &recovered, &mut |_| {}).is_err());
        assert_eq!(diverged.records().len(), global_record_count);
    }
}
