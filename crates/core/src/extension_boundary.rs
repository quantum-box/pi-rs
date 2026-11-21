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

#[derive(Serialize, Deserialize)]
struct BoundaryPlan {
    entries: Vec<BoundaryDraft>,
    should_continue: bool,
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
    outcome: AgentActivityOutcome,
    timeout: Duration,
    observer: &mut Observer<'_>,
) -> Result<bool> {
    let states = session.extension_states();
    let mut event = AgentBeforeSettle {
        outcome,
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
        let callback = std::panic::AssertUnwindSafe(async {
            extension
                .agent_before_settle(&event, states.get(extension.id()))
                .await
        })
        .catch_unwind();
        let result: ExtensionResult<BoundaryDecision> =
            match crate::runtime::time::timeout(timeout, callback).await {
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
    let should_continue = event.should_continue && event.context.can_continue;
    if event.should_continue && !should_continue {
        report(
            "<boundary>",
            "continuation requires runnable model context",
            observer,
        );
    }
    if event.entries.is_empty() && !should_continue {
        return Ok(false);
    }
    let plan = BoundaryPlan {
        entries: event.entries,
        should_continue,
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
        .filter(|count| *count <= plan.entries.len())
        .ok_or_else(|| Error::Session("boundary plan is outside its journal window".into()))?;
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
    use std::sync::Arc;

    use super::*;
    use crate::{Extension, MemoryStore};

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
                AgentActivityOutcome::Completed,
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
        diverged.checkout(marker_id + 1).unwrap();
        assert_eq!(diverged.head(), marker_id + 1);
        let global_record_count = diverged.records().len();
        assert!(persist_plan_suffix(&mut diverged, marker_id, &recovered, &mut |_| {}).is_err());
        assert_eq!(diverged.records().len(), global_record_count);
    }
}
