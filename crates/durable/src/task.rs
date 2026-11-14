//! Standalone, deterministic task transitions. The host must atomically persist
//! each transition and ensure only one executor acts on a committed task.
//! `task_store` provides durable wait evaluation; the host schedules polling
//! and executes tasks after acquiring a reservation.

pub use crate::task_scheduler as scheduler;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// Ownership is an immutable identity edge, not an executor lease.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskOwner {
    Conversation(Uuid),
    Task(Uuid),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub phase: String,
    pub data: Value,
}

/// Conditions persisted with the task. Deadlines are absolute Unix milliseconds;
/// Tasks waits for all listed direct children to reach any terminal state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WaitFor {
    DeadlineMillis(i64),
    Signal(String),
    /// Wake on the first matching signal or when the absolute deadline passes.
    Sleep {
        until_millis: Option<i64>,
        signals: Vec<String>,
    },
    Tasks(Vec<Uuid>),
    /// Wake when any direct child ends; an empty set remains pending.
    TasksAny(Vec<Uuid>),
    /// Wait for all; a failed child cancels the other listed subtrees.
    TasksFailFast(Vec<Uuid>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    Ready,
    Running,
    Waiting(WaitFor),
    Succeeded(Value),
    Failed(String),
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    id: Uuid,
    owner: TaskOwner,
    checkpoint: Checkpoint,
    state: TaskState,
    /// Consecutive recoveries since the last changed checkpoint.
    restarts: u32,
    restart_limit: u32,
    #[serde(default)]
    delivered_signal: Option<Uuid>,
    /// Registration identity; absent on records created by the standalone API.
    #[serde(default)]
    kind: Option<String>,
    /// Only explicitly replay-safe tasks may run again after an interrupted invocation.
    #[serde(default)]
    replay_safe: bool,
    /// Durable cancellation intent; retained on terminal records.
    #[serde(default)]
    abort_requested: bool,
    /// Conversation-owned background tasks form an ordinary abort boundary.
    #[serde(default)]
    background: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransitionError {
    InvalidState,
    RestartLimit,
    UnchangedCheckpoint,
    SelfOwned,
    InvalidWait,
    BackgroundChild,
}

impl Task {
    pub fn new(
        id: Uuid,
        owner: TaskOwner,
        checkpoint: Checkpoint,
        restart_limit: u32,
    ) -> Result<Self, TransitionError> {
        if owner == TaskOwner::Task(id) {
            return Err(TransitionError::SelfOwned);
        }
        Ok(Self {
            id,
            owner,
            checkpoint,
            state: TaskState::Ready,
            restarts: 0,
            restart_limit,
            delivered_signal: None,
            kind: None,
            replay_safe: false,
            abort_requested: false,
            background: false,
        })
    }

    /// Mark a conversation-owned task as background before registration.
    pub fn background(mut self) -> Result<Self, TransitionError> {
        if !matches!(self.owner, TaskOwner::Conversation(_)) {
            return Err(TransitionError::BackgroundChild);
        }
        if self.state != TaskState::Ready {
            return Err(TransitionError::InvalidState);
        }
        self.background = true;
        Ok(self)
    }

    pub fn is_background(&self) -> bool {
        self.background
    }

    /// Bind a new task to a registered implementation and its persisted replay policy.
    pub fn registered(mut self, kind: String, replay_safe: bool) -> Self {
        self.kind = Some(kind);
        self.replay_safe = replay_safe;
        self
    }

    pub fn kind(&self) -> Option<&str> {
        self.kind.as_deref()
    }
    pub fn replay_safe(&self) -> bool {
        self.replay_safe
    }

    pub fn abort_requested(&self) -> bool {
        self.abort_requested
    }

    pub(crate) fn request_abort(&mut self) -> Result<(), TransitionError> {
        if self.is_terminal() {
            return Err(TransitionError::InvalidState);
        }
        if !self.abort_requested {
            self.abort_requested = true;
            self.restarts = 0;
        }
        if matches!(self.state, TaskState::Waiting(_)) {
            self.state = TaskState::Ready;
        }
        Ok(())
    }

    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn owner(&self) -> &TaskOwner {
        &self.owner
    }
    pub fn state(&self) -> &TaskState {
        &self.state
    }
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
    pub fn restarts(&self) -> u32 {
        self.restarts
    }
    pub fn restart_limit(&self) -> u32 {
        self.restart_limit
    }

    /// Signal receipt associated with the most recently completed wait. It is
    /// retained across recovery and cleared when a new wait is registered.
    pub fn delivered_signal(&self) -> Option<Uuid> {
        self.delivered_signal
    }

    /// Reserve only ready work. Recovery at the limit stays ready but cannot run.
    pub fn start(&mut self) -> Result<(), TransitionError> {
        if self.state != TaskState::Ready {
            return Err(TransitionError::InvalidState);
        }
        if self.restarts > 0 && self.restarts >= self.restart_limit {
            return Err(TransitionError::RestartLimit);
        }
        self.state = TaskState::Running;
        Ok(())
    }

    /// Explicitly recover a persisted running task after the host establishes
    /// that its prior executor is gone. No effects are replayed by this method.
    pub fn recover(&mut self) -> Result<(), TransitionError> {
        self.require_running()?;
        self.restarts = self.restarts.saturating_add(1);
        self.state = TaskState::Ready;
        Ok(())
    }

    pub(crate) fn yield_ready(&mut self) -> Result<(), TransitionError> {
        self.require_running()?;
        self.state = TaskState::Ready;
        Ok(())
    }

    /// A changed checkpoint is durable progress and resets the restart count.
    pub fn advance(&mut self, checkpoint: Checkpoint) -> Result<(), TransitionError> {
        self.require_running()?;
        if self.checkpoint == checkpoint {
            return Err(TransitionError::UnchangedCheckpoint);
        }
        self.checkpoint = checkpoint;
        self.restarts = 0;
        Ok(())
    }

    pub fn wait(&mut self, condition: WaitFor) -> Result<(), TransitionError> {
        self.require_running()?;
        if let WaitFor::Sleep {
            until_millis,
            signals,
        } = &condition
            && ((until_millis.is_none() && signals.is_empty())
                || until_millis.is_some_and(|time| time < 0)
                || signals.iter().any(String::is_empty))
        {
            return Err(TransitionError::InvalidWait);
        }
        self.delivered_signal = None;
        self.state = TaskState::Waiting(condition);
        Ok(())
    }

    /// The host is responsible for establishing that the wait was satisfied.
    pub fn wake(&mut self) -> Result<(), TransitionError> {
        if !matches!(self.state, TaskState::Waiting(_)) {
            return Err(TransitionError::InvalidState);
        }
        self.state = TaskState::Ready;
        Ok(())
    }

    pub(crate) fn wake_with_signal(&mut self, id: Uuid) -> Result<(), TransitionError> {
        self.wake()?;
        self.delivered_signal = Some(id);
        Ok(())
    }

    pub fn succeed(&mut self, result: Value) -> Result<(), TransitionError> {
        self.require_running()?;
        self.state = TaskState::Succeeded(result);
        Ok(())
    }

    pub fn fail(&mut self, reason: String) -> Result<(), TransitionError> {
        self.require_running()?;
        self.state = TaskState::Failed(reason);
        Ok(())
    }

    pub(crate) fn fail_abort_restart(&mut self) -> Result<(), TransitionError> {
        if self.state != TaskState::Ready || !self.abort_requested {
            return Err(TransitionError::InvalidState);
        }
        self.state = TaskState::Failed("abort restart limit".into());
        Ok(())
    }

    /// Cancellation is permitted for any nonterminal state, including blocked work.
    pub fn cancel(&mut self) -> Result<(), TransitionError> {
        if matches!(
            self.state,
            TaskState::Succeeded(_) | TaskState::Failed(_) | TaskState::Cancelled
        ) {
            return Err(TransitionError::InvalidState);
        }
        self.state = TaskState::Cancelled;
        Ok(())
    }

    /// Terminal children satisfy a task wait regardless of their outcome.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            TaskState::Succeeded(_) | TaskState::Failed(_) | TaskState::Cancelled
        )
    }

    fn require_running(&self) -> Result<(), TransitionError> {
        if self.state == TaskState::Running {
            Ok(())
        } else {
            Err(TransitionError::InvalidState)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn task(limit: u32) -> Task {
        Task::new(
            Uuid::from_u128(1),
            TaskOwner::Conversation(Uuid::from_u128(2)),
            checkpoint("first", 0),
            limit,
        )
        .unwrap()
    }
    fn checkpoint(phase: &str, data: u32) -> Checkpoint {
        Checkpoint {
            phase: phase.into(),
            data: json!(data),
        }
    }

    #[test]
    fn roundtrip_recovery_preserves_checkpoint_owner_and_enforces_limit() {
        let mut t = task(2);
        t.start().unwrap();
        let mut t: Task = serde_json::from_str(&serde_json::to_string(&t).unwrap()).unwrap();
        assert_eq!(t.start(), Err(TransitionError::InvalidState));
        t.recover().unwrap();
        assert_eq!(t.checkpoint(), &checkpoint("first", 0));
        assert_eq!(t.owner(), &TaskOwner::Conversation(Uuid::from_u128(2)));
        t.start().unwrap();
        t.recover().unwrap();
        let before = t.clone();
        assert_eq!(t.start(), Err(TransitionError::RestartLimit));
        assert_eq!(t, before);
        t.cancel().unwrap();
    }

    #[test]
    fn progress_resets_restarts_but_unchanged_checkpoint_does_not() {
        let mut t = task(2);
        t.start().unwrap();
        t.recover().unwrap();
        t.start().unwrap();
        assert_eq!(
            t.advance(checkpoint("first", 0)),
            Err(TransitionError::UnchangedCheckpoint)
        );
        assert_eq!(t.restarts(), 1);
        t.advance(checkpoint("second", 1)).unwrap();
        assert_eq!(t.restarts(), 0);
        t.recover().unwrap();
        t.start().unwrap();
    }

    #[test]
    fn waiting_and_terminal_states_cannot_execute() {
        let mut t = task(3);
        t.start().unwrap();
        t.wait(WaitFor::Signal("approval".into())).unwrap();
        let before = t.clone();
        assert_eq!(t.start(), Err(TransitionError::InvalidState));
        assert_eq!(t.recover(), Err(TransitionError::InvalidState));
        assert_eq!(t.succeed(json!(1)), Err(TransitionError::InvalidState));
        assert_eq!(t, before);
        t.wake().unwrap();
        t.start().unwrap();
        t.succeed(json!({"ok": true})).unwrap();
        let before = t.clone();
        assert_eq!(t.start(), Err(TransitionError::InvalidState));
        assert_eq!(t.wake(), Err(TransitionError::InvalidState));
        assert_eq!(t.cancel(), Err(TransitionError::InvalidState));
        assert_eq!(t, before);
    }

    #[test]
    fn ownership_and_failure_are_explicit() {
        assert_eq!(
            Task::new(
                Uuid::from_u128(1),
                TaskOwner::Task(Uuid::from_u128(1)),
                checkpoint("x", 0),
                1
            ),
            Err(TransitionError::SelfOwned)
        );
        let mut t = Task::new(
            Uuid::from_u128(1),
            TaskOwner::Task(Uuid::from_u128(2)),
            checkpoint("x", 0),
            0,
        )
        .unwrap();
        t.start().unwrap(); // Zero permits the initial invocation, but no restart.
        t.fail("failed".into()).unwrap();
        assert_eq!(t.start(), Err(TransitionError::InvalidState));
        assert_eq!(t.recover(), Err(TransitionError::InvalidState));
    }
}
