//! Observation hooks for agent runs and completed model turns.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ExtensionStates, Message, Notification, Observer, Registry, RunState, Session};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LifecycleEvent {
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LifecycleMessage {
    pub record_id: u64,
    pub message: Message,
}

impl LifecycleEvent {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::AgentStart { .. } => "agent_start",
            Self::AgentEnd { .. } => "agent_end",
            Self::TurnStart { .. } => "turn_start",
            Self::TurnEnd { .. } => "turn_end",
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

pub(crate) async fn notify(
    registry: &Registry,
    event: LifecycleEvent,
    states: &ExtensionStates,
    timeout: Duration,
    observer: &mut Observer<'_>,
) {
    let deadline = tokio::time::Instant::now() + timeout.min(Duration::from_secs(10));
    for extension in registry
        .extensions
        .iter()
        .filter(|extension| extension.handles_lifecycle())
    {
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
