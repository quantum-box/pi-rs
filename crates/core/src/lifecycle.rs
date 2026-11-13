//! Observation hooks for sessions, agent runs, and completed model turns.
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    ExtensionStates, Message, Notification, Observer, Record, Registry, RunState, Session, Usage,
};

/// Branch navigation as selected by the user, before any session changes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TreePreparation {
    #[serde(alias = "target_id")]
    pub target_id: u64,
    #[serde(alias = "old_leaf_id")]
    pub old_leaf_id: u64,
    #[serde(alias = "common_ancestor_id")]
    pub common_ancestor_id: Option<u64>,
    #[serde(alias = "entries_to_summarize")]
    pub entries_to_summarize: Vec<Record>,
    #[serde(alias = "user_wants_summary")]
    pub user_wants_summary: bool,
    #[serde(alias = "custom_instructions")]
    pub custom_instructions: Option<String>,
    #[serde(default, alias = "replace_instructions")]
    pub replace_instructions: bool,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TreeDecision {
    pub cancel: bool,
    pub summary: Option<TreeSummary>,
    #[serde(alias = "custom_instructions")]
    pub custom_instructions: Option<String>,
    #[serde(alias = "replace_instructions")]
    pub replace_instructions: Option<bool>,
    pub label: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TreeSummary {
    pub summary: String,
    pub details: Option<serde_json::Value>,
    pub usage: Option<Usage>,
}

/// Presence marks a branch summary supplied by an extension.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExtensionTreeSummary {
    pub details: Option<serde_json::Value>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStartReason {
    Startup,
    Reload,
    New,
    Resume,
    Fork,
}

/// How a model was selected by the host.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelSelectSource {
    Set,
    Cycle,
    Restore,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionShutdownReason {
    Quit,
    Reload,
    New,
    Resume,
    Fork,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionSwitchReason {
    New,
    Resume,
}

/// What initiated a model-generated context compaction.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionReason {
    Manual,
    Threshold,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForkPosition {
    Before,
    At,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BeforeSessionTransition {
    #[serde(rename = "session_before_switch")]
    Switch {
        reason: SessionSwitchReason,
        #[serde(rename = "targetSessionFile", alias = "target_session_file")]
        target_session_file: Option<PathBuf>,
    },
    #[serde(rename = "session_before_fork")]
    Fork {
        #[serde(rename = "entryId", alias = "entry_id")]
        entry_id: u64,
        position: ForkPosition,
    },
}

impl BeforeSessionTransition {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Switch { .. } => "session_before_switch",
            Self::Fork { .. } => "session_before_fork",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LifecycleEvent {
    ModelSelect {
        model: serde_json::Value,
        #[serde(rename = "previousModel")]
        previous_model: Option<serde_json::Value>,
        source: ModelSelectSource,
    },
    ThinkingLevelSelect {
        level: String,
        #[serde(rename = "previousLevel")]
        previous_level: String,
    },
    SessionInfoChanged {
        name: Option<String>,
    },
    SessionCompact {
        #[serde(rename = "compactionEntry")]
        compaction_entry: Box<Record>,
        #[serde(rename = "fromExtension")]
        from_extension: bool,
        reason: CompactionReason,
        #[serde(rename = "willRetry")]
        will_retry: bool,
    },
    SessionCompactFailed {
        reason: CompactionReason,
        #[serde(rename = "errorMessage", skip_serializing_if = "Option::is_none")]
        error_message: Option<String>,
        aborted: bool,
        #[serde(rename = "willRetry")]
        will_retry: bool,
        #[serde(rename = "fromExtension")]
        from_extension: bool,
    },
    SessionStart {
        reason: SessionStartReason,
        #[serde(rename = "previousSessionFile", alias = "previous_session_file")]
        previous_session_file: Option<PathBuf>,
    },
    SessionShutdown {
        reason: SessionShutdownReason,
        #[serde(rename = "targetSessionFile", alias = "target_session_file")]
        target_session_file: Option<PathBuf>,
    },
    AgentStart {
        #[serde(rename = "sessionId", alias = "session_id")]
        session_id: Uuid,
        #[serde(rename = "runId", alias = "run_id")]
        run_id: Uuid,
        model: String,
    },
    AgentEnd {
        #[serde(rename = "sessionId", alias = "session_id")]
        session_id: Uuid,
        #[serde(rename = "runId", alias = "run_id")]
        run_id: Uuid,
        state: RunState,
        messages: Vec<LifecycleMessage>,
    },
    /// The run is idle after retries, compaction and queued continuations.
    AgentSettled {
        aborted: bool,
    },
    TurnStart {
        #[serde(rename = "runId", alias = "run_id")]
        run_id: Uuid,
        #[serde(rename = "turnIndex", alias = "turn_index")]
        turn_index: u32,
        timestamp: u64,
        recovered: bool,
    },
    TurnEnd {
        #[serde(rename = "runId", alias = "run_id")]
        run_id: Uuid,
        #[serde(rename = "turnIndex", alias = "turn_index")]
        turn_index: u32,
        messages: Vec<LifecycleMessage>,
    },
    ToolExecutionStart {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
        args: serde_json::Value,
        #[serde(rename = "parentToolCallId", skip_serializing_if = "Option::is_none")]
        parent_tool_call_id: Option<String>,
    },
    ToolExecutionUpdate {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
        args: serde_json::Value,
        #[serde(rename = "partialResult")]
        partial_result: serde_json::Value,
        #[serde(rename = "parentToolCallId", skip_serializing_if = "Option::is_none")]
        parent_tool_call_id: Option<String>,
    },
    ToolExecutionEnd {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
        result: crate::Outcome,
        #[serde(rename = "isError")]
        is_error: bool,
        #[serde(rename = "durationMs", skip_serializing_if = "Option::is_none")]
        duration_ms: Option<u64>,
        #[serde(rename = "parentToolCallId", skip_serializing_if = "Option::is_none")]
        parent_tool_call_id: Option<String>,
    },
    SessionTree {
        #[serde(rename = "newLeafId", alias = "new_leaf_id")]
        new_leaf_id: u64,
        #[serde(rename = "oldLeafId", alias = "old_leaf_id")]
        old_leaf_id: u64,
        #[serde(rename = "summaryEntry", alias = "summary_entry")]
        summary_entry: Option<Box<Record>>,
        #[serde(rename = "fromExtension", alias = "from_extension")]
        from_extension: Option<bool>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleMessage {
    #[serde(alias = "record_id")]
    pub record_id: u64,
    pub message: Message,
}

impl LifecycleEvent {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ModelSelect { .. } => "model_select",
            Self::ThinkingLevelSelect { .. } => "thinking_level_select",
            Self::SessionInfoChanged { .. } => "session_info_changed",
            Self::SessionCompact { .. } => "session_compact",
            Self::SessionCompactFailed { .. } => "session_compact_failed",
            Self::SessionStart { .. } => "session_start",
            Self::SessionShutdown { .. } => "session_shutdown",
            Self::AgentStart { .. } => "agent_start",
            Self::AgentEnd { .. } => "agent_end",
            Self::AgentSettled { .. } => "agent_settled",
            Self::TurnStart { .. } => "turn_start",
            Self::TurnEnd { .. } => "turn_end",
            Self::ToolExecutionStart { .. } => "tool_execution_start",
            Self::ToolExecutionUpdate { .. } => "tool_execution_update",
            Self::ToolExecutionEnd { .. } => "tool_execution_end",
            Self::SessionTree { .. } => "session_tree",
        }
    }
}

pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn messages(session: &Session, include: impl Fn(u64) -> bool) -> Vec<LifecycleMessage> {
    session
        .current_message_entries()
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

pub(crate) fn enabled(registry: &Registry) -> bool {
    registry
        .extensions
        .iter()
        .any(|extension| extension.handles_lifecycle())
}

pub async fn notify(
    registry: &Registry,
    event: LifecycleEvent,
    states: &ExtensionStates,
    timeout: Duration,
    observer: &mut Observer<'_>,
) {
    notify_inner(registry, event, states, timeout, observer, None).await;
}

pub(crate) async fn notify_with_host(
    registry: &Registry,
    event: LifecycleEvent,
    states: &ExtensionStates,
    timeout: Duration,
    observer: &mut Observer<'_>,
    host: std::sync::Arc<dyn crate::ToolHost>,
) {
    notify_inner(registry, event, states, timeout, observer, Some(host)).await;
}

pub(crate) async fn notify_idle_with_host(
    registry: &Registry,
    session: &mut Session,
    event: LifecycleEvent,
    timeout: Duration,
    observer: &mut Observer<'_>,
) -> crate::Result<()> {
    notify_session_with_host(registry, session, event, timeout, observer, true).await
}

pub(crate) async fn notify_session_with_host(
    registry: &Registry,
    session: &mut Session,
    event: LifecycleEvent,
    timeout: Duration,
    observer: &mut Observer<'_>,
    idle: bool,
) -> crate::Result<()> {
    let states = session.extension_states();
    let (sender, mut incoming) = tokio::sync::mpsc::channel(32);
    let host = std::sync::Arc::new(crate::agent::LifecycleMessageHost { sender, idle });
    let mut errors = Vec::new();
    let mut collect = |event: Notification<'_>| {
        if let Notification::ExtensionLifecycleError {
            extension,
            event,
            message,
        } = event
        {
            errors.push((extension.to_owned(), event, message.to_owned()));
        }
    };
    {
        let future = notify_with_host(registry, event, &states, timeout, &mut collect, host);
        tokio::pin!(future);
        loop {
            tokio::select! {
                biased;
                Some((input, reply)) = incoming.recv() => {
                    let result = if !idle || matches!(input.delivery, crate::CustomMessageDelivery::NextTurn | crate::CustomMessageDelivery::TriggerTurn) {
                        crate::agent::queue_custom_message(session, input, observer)
                    } else {
                        session.append_custom_message(input.message).map(|()| observer(Notification::Committed(session.records().last().expect("appended custom message"))))
                    };
                    match result { Ok(()) => { let _ = reply.send(Ok(())); }, Err(error) => { let _ = reply.send(Err(error.to_string())); return Err(error); } }
                }
                _ = &mut future => break,
            }
        }
    }
    for (extension, event, message) in &errors {
        observer(Notification::ExtensionLifecycleError {
            extension,
            event,
            message,
        });
    }
    Ok(())
}

async fn notify_inner(
    registry: &Registry,
    event: LifecycleEvent,
    states: &ExtensionStates,
    timeout: Duration,
    observer: &mut Observer<'_>,
    host: Option<std::sync::Arc<dyn crate::ToolHost>>,
) {
    let deadline = tokio::time::Instant::now() + timeout.min(Duration::from_secs(10));
    for extension in registry.extensions.iter().filter(|extension| {
        extension.handles_lifecycle()
            && match event {
                LifecycleEvent::ModelSelect { .. } | LifecycleEvent::ThinkingLevelSelect { .. } => {
                    extension.handles_model_selection()
                }
                LifecycleEvent::SessionStart { .. }
                | LifecycleEvent::SessionShutdown { .. }
                | LifecycleEvent::SessionInfoChanged { .. }
                | LifecycleEvent::SessionCompact { .. }
                | LifecycleEvent::SessionCompactFailed { .. } => {
                    extension.handles_session_lifecycle()
                }
                LifecycleEvent::SessionTree { .. } => extension.handles_tree_navigation(),
                LifecycleEvent::ToolExecutionStart { .. }
                | LifecycleEvent::ToolExecutionEnd { .. } => {
                    extension.handles_tool_execution_lifecycle()
                }
                LifecycleEvent::ToolExecutionUpdate { .. } => {
                    extension.handles_tool_execution_lifecycle()
                        && extension.handles_tool_execution_updates()
                }
                _ => true,
            }
    }) {
        let error = match tokio::time::timeout_at(deadline, async {
            match &host {
                Some(host) => {
                    extension
                        .lifecycle_with_host(
                            &event,
                            states.get(extension.id()),
                            host.for_extension(extension.id())
                                .unwrap_or_else(|| host.clone()),
                        )
                        .await
                }
                None => {
                    extension
                        .lifecycle(&event, states.get(extension.id()))
                        .await
                }
            }
        })
        .await
        {
            Ok(Ok(())) => continue,
            Ok(Err(error)) => error,
            Err(_) => "lifecycle hook timed out".into(),
        };
        observer(Notification::ExtensionLifecycleError {
            extension: extension.id(),
            event: event.kind(),
            message: &error,
        });
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn transition_and_lifecycle_fields_use_pi_names() {
        let fork = BeforeSessionTransition::Fork {
            entry_id: 42,
            position: ForkPosition::Before,
        };
        assert_eq!(
            serde_json::to_value(&fork).unwrap(),
            json!({"type":"session_before_fork","entryId":42,"position":"before"})
        );
        assert!(
            serde_json::from_value::<BeforeSessionTransition>(
                json!({"type":"session_before_fork","entry_id":42,"position":"before"})
            )
            .is_ok()
        );
        let start = LifecycleEvent::SessionStart {
            reason: SessionStartReason::Resume,
            previous_session_file: Some("previous.jsonl".into()),
        };
        let value = serde_json::to_value(&start).unwrap();
        assert_eq!(value["previousSessionFile"], "previous.jsonl");
        assert!(value.get("previous_session_file").is_none());
        let tree = LifecycleEvent::SessionTree {
            new_leaf_id: 4,
            old_leaf_id: 2,
            summary_entry: None,
            from_extension: Some(true),
        };
        let value = serde_json::to_value(&tree).unwrap();
        assert_eq!(value["newLeafId"], 4);
        assert_eq!(value["oldLeafId"], 2);
        assert_eq!(value["fromExtension"], true);
        assert!(value.get("summary_entry").is_none());
    }

    #[test]
    fn tree_decisions_accept_pi_and_previous_native_field_names() {
        for value in [
            json!({"customInstructions":"custom","replaceInstructions":true}),
            json!({"custom_instructions":"custom","replace_instructions":true}),
        ] {
            let decision: TreeDecision = serde_json::from_value(value).unwrap();
            assert_eq!(decision.custom_instructions.as_deref(), Some("custom"));
            assert_eq!(decision.replace_instructions, Some(true));
            let encoded = serde_json::to_value(decision).unwrap();
            assert_eq!(encoded["customInstructions"], "custom");
            assert_eq!(encoded["replaceInstructions"], true);
        }
        let preparation = TreePreparation {
            target_id: 3,
            old_leaf_id: 5,
            common_ancestor_id: Some(2),
            entries_to_summarize: Vec::new(),
            user_wants_summary: true,
            custom_instructions: Some("custom".into()),
            replace_instructions: false,
            label: None,
        };
        let encoded = serde_json::to_value(preparation).unwrap();
        assert_eq!(encoded["targetId"], 3);
        assert_eq!(encoded["oldLeafId"], 5);
        assert_eq!(encoded["commonAncestorId"], 2);
        assert_eq!(encoded["userWantsSummary"], true);
        assert_eq!(encoded["customInstructions"], "custom");
        assert!(encoded.get("target_id").is_none());
    }
}
