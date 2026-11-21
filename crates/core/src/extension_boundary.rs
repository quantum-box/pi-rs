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
const ACTIVITY_FORMAT: &str = "pi-rs:before-settle-activity-v1";

pub(crate) fn is_marker(format: &str) -> bool {
    matches!(format, PLAN_FORMAT | ACTIVITY_FORMAT)
}

pub(crate) fn validate_navigation(session: &Session, target: u64) -> Result<()> {
    let boundary_end =
        session
            .path(target)
            .into_iter()
            .rev()
            .find_map(|record| match &record.event {
                Event::SourceEntry { format, entry } if format == PLAN_FORMAT => entry["entries"]
                    .as_array()
                    .map(|entries| record.id.saturating_add(entries.len() as u64)),
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
    Stop(Result<String>),
}

pub(crate) fn recover_agent_before_settle(
    session: &mut Session,
    run_id: uuid::Uuid,
    observer: &mut Observer<'_>,
) -> Result<Option<RecoveredBoundary>> {
    let mut activity_started = false;
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
            | Event::TurnFinished { .. }
            | Event::Assistant { .. } => break,
            Event::SourceEntry { format, .. } if format == ACTIVITY_FORMAT => {
                activity_started = true;
            }
            Event::SourceEntry { format, entry } if format == PLAN_FORMAT => {
                let marker = record.id;
                let plan: BoundaryPlan = serde_json::from_value(entry.clone())?;
                persist_plan_suffix(session, marker, &plan, observer)?;
                return Ok(Some(
                    if plan.should_continue
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
    let deadline = crate::runtime::time::Instant::now() + timeout.min(Duration::from_secs(10));
    let states = session.extension_states();
    let mut event = AgentBeforeSettle {
        outcome: if result.is_ok() {
            AgentActivityOutcome::Completed
        } else {
            AgentActivityOutcome::Error
        },
        entries: Vec::new(),
        should_continue: false,
        context: context(session),
    };
    let mut invalid_entries = None;
    for extension in registry
        .extensions
        .iter()
        .filter(|ext| ext.handles_agent_before_settle())
    {
        if cancel.is_cancelled() {
            break;
        }
        let callback = std::panic::AssertUnwindSafe(async {
            extension
                .agent_before_settle(&event, states.get(extension.id()))
                .await
        })
        .catch_unwind();
        let result: ExtensionResult<BoundaryDecision> =
            match crate::runtime::time::timeout_at(deadline, callback).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err("boundary handler panicked".into()),
                Err(_) => Err("boundary handler timed out".into()),
            };
        match result {
            Ok(decision) => {
                if let Some(should_continue) = decision.should_continue {
                    event.should_continue = should_continue;
                }
                if let Some(entries) = decision.entries {
                    event.entries = entries;
                    invalid_entries = match preview(session, &event.entries) {
                        Ok(context) => {
                            event.context = context;
                            None
                        }
                        Err(error) => Some(format!("invalid boundary entries: {error}")),
                    };
                }
            }
            Err(error) => report(extension.id(), &error, observer),
        }
        if let Some(error) = &invalid_entries {
            report(extension.id(), error, observer);
        }
    }
    if invalid_entries.is_some() {
        return Ok(false);
    }
    let cancelled = cancel.is_cancelled();
    let should_continue = !cancelled && event.should_continue && event.context.can_continue;
    if event.should_continue && !cancelled && !event.context.can_continue {
        report(
            "<boundary>",
            "continuation requires runnable model context",
            observer,
        );
    }
    let plan = BoundaryPlan {
        entries: event.entries,
        should_continue,
        settled: BoundaryResult::capture(session, result, cancelled),
    };
    let marker = plan_marker(serde_json::to_value(&plan)?);
    // Catch deterministic store limits before writing the plan itself.
    if let Err(error) = session.extension_boundary_snapshot().append(marker.clone()) {
        report(
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
