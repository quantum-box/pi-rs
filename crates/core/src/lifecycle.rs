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
pub struct TreePreparation {
    pub target_id: u64,
    pub old_leaf_id: u64,
    pub common_ancestor_id: Option<u64>,
    pub entries_to_summarize: Vec<Record>,
    pub user_wants_summary: bool,
    pub custom_instructions: Option<String>,
    #[serde(default)]
    pub replace_instructions: bool,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TreeDecision {
    pub cancel: bool,
    pub summary: Option<TreeSummary>,
    pub custom_instructions: Option<String>,
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionShutdownReason {
    Quit,
    Reload,
    New,
    Resume,
    Fork,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LifecycleEvent {
    SessionStart {
        reason: SessionStartReason,
        previous_session_file: Option<PathBuf>,
    },
    SessionShutdown {
        reason: SessionShutdownReason,
        target_session_file: Option<PathBuf>,
    },
    AgentStart {
        session_id: Uuid,
        run_id: Uuid,
        model: String,
    },
    AgentEnd {
        session_id: Uuid,
        run_id: Uuid,
        state: RunState,
        messages: Vec<LifecycleMessage>,
    },
    TurnStart {
        run_id: Uuid,
        turn_index: u32,
        timestamp: u64,
        recovered: bool,
    },
    TurnEnd {
        run_id: Uuid,
        turn_index: u32,
        messages: Vec<LifecycleMessage>,
    },
    SessionTree {
        new_leaf_id: u64,
        old_leaf_id: u64,
        summary_entry: Option<Box<Record>>,
        from_extension: Option<bool>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LifecycleMessage {
    pub record_id: u64,
    pub message: Message,
}

impl LifecycleEvent {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::SessionStart { .. } => "session_start",
            Self::SessionShutdown { .. } => "session_shutdown",
            Self::AgentStart { .. } => "agent_start",
            Self::AgentEnd { .. } => "agent_end",
            Self::TurnStart { .. } => "turn_start",
            Self::TurnEnd { .. } => "turn_end",
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
    let deadline = tokio::time::Instant::now() + timeout.min(Duration::from_secs(10));
    for extension in registry.extensions.iter().filter(|extension| {
        extension.handles_lifecycle()
            && match event {
                LifecycleEvent::SessionStart { .. } | LifecycleEvent::SessionShutdown { .. } => {
                    extension.handles_session_lifecycle()
                }
                LifecycleEvent::SessionTree { .. } => extension.handles_tree_navigation(),
                _ => true,
            }
    }) {
        let error = match tokio::time::timeout_at(
            deadline,
            extension.lifecycle(&event, states.get(extension.id())),
        )
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
