//! Standalone, deterministic task transitions. The host must atomically persist
//! each transition and ensure only one executor acts on a committed task.
//! `task_store` provides durable wait evaluation; the host schedules polling
//! and executes tasks after acquiring a reservation.

pub use crate::task_scheduler as scheduler;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

durable_task_schema::declare_task_schema!(crate::task_store::SignalKey);

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
            definition_version: 1,
            input: Value::Null,
            state: TaskState::Ready,
            attempt_id: None,
            retired_attempt_id: None,
            restarts: 0,
            restart_limit,
            delivered_signal: None,
            typed_wake: None,
            ended_children: None,
            wake_children: None,
            kind: None,
            replay_safe: false,
            abort_requested: false,
            background: false,
            task_tool: false,
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

    pub(crate) fn into_task_tool(mut self) -> Self {
        self.task_tool = true;
        self
    }

    pub(crate) fn is_task_tool(&self) -> bool {
        self.task_tool
    }

    /// Bind a new task to a registered implementation and its persisted replay policy.
    pub fn registered(mut self, kind: String, replay_safe: bool) -> Self {
        self.kind = Some(kind);
        self.replay_safe = replay_safe;
        self
    }

    /// Bind a newly created task to a positive definition version and input.
    pub fn registered_versioned(
        mut self,
        kind: String,
        replay_safe: bool,
        version: u32,
        input: Value,
    ) -> Result<Self, TransitionError> {
        if version == 0 || self.state != TaskState::Ready {
            return Err(TransitionError::InvalidState);
        }
        self.kind = Some(kind);
        self.replay_safe = replay_safe;
        self.definition_version = version;
        self.input = input;
        Ok(self)
    }
    pub fn definition_version(&self) -> u32 {
        self.definition_version
    }
    pub fn input(&self) -> &Value {
        &self.input
    }
    pub(crate) fn migrate(&mut self, version: u32, input: Value, checkpoint: Checkpoint) {
        self.definition_version = version;
        self.input = input;
        self.typed_wake = None;
        // Migration does not consume a child wake or unfreeze a replayed one.
        self.checkpoint = checkpoint;
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
            self.retired_attempt_id = None;
            self.restarts = 0;
        }
        self.typed_wake = None;
        self.ended_children = None;
        self.wake_children = None;
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

    /// Wake reason retained through recovery until progress, parking, or termination.
    pub fn typed_wake(&self) -> Option<&TypedWake> {
        self.typed_wake.as_ref()
    }

    pub(crate) fn set_typed_wake(&mut self, wake: TypedWake) {
        self.typed_wake = Some(wake);
    }

    /// Terminal direct-child IDs frozen at reservation after a child wait.
    /// The same snapshot is retained if this invocation is replayed after a crash.
    pub fn ended_children(&self) -> Option<&[Uuid]> {
        self.ended_children.as_deref()
    }

    pub(crate) fn set_child_wake(&mut self, on: Vec<Uuid>, ended: Vec<Uuid>) {
        self.wake_children = Some(on);
        self.ended_children = Some(ended);
    }

    pub(crate) fn wake_children(&self) -> Option<&[Uuid]> {
        self.wake_children.as_deref()
    }

    pub(crate) fn refresh_ended_children(&mut self, ended: Vec<Uuid>) {
        self.ended_children = Some(ended);
        // The first reservation freezes the wake for deterministic replay.
        self.wake_children = None;
    }

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
        self.attempt_id = None;
        self.retired_attempt_id = None;
        Ok(())
    }

    pub(crate) fn start_attempt(&mut self, attempt_id: Uuid) -> Result<(), TransitionError> {
        self.start()?;
        self.attempt_id = Some(attempt_id);
        Ok(())
    }

    pub(crate) fn attempt_id(&self) -> Option<Uuid> {
        self.attempt_id
    }

    /// A completed reservation is valid only together with the task's storage
    /// version and external executor/abort fences; it does not prove a join.
    pub(crate) fn retired_attempt_id(&self) -> Option<Uuid> {
        self.retired_attempt_id
    }

    /// Called only after the handler returned and its proposed transition has
    /// been applied to a clone. Persist it in the same CAS as that transition.
    pub(crate) fn record_completed_attempt(
        &mut self,
        running: &Task,
    ) -> Result<(), TransitionError> {
        if self.id != running.id
            || running.state != TaskState::Running
            || self.state == TaskState::Running
            || self.attempt_id.is_some()
            || self.abort_requested
            || running.abort_requested
        {
            return Err(TransitionError::InvalidState);
        }
        self.retired_attempt_id = Some(running.attempt_id.ok_or(TransitionError::InvalidState)?);
        Ok(())
    }

    /// Explicitly recover a persisted running task after the host establishes
    /// that its prior executor is gone. No effects are replayed by this method.
    pub fn recover(&mut self) -> Result<(), TransitionError> {
        self.require_running()?;
        self.restarts = self.restarts.saturating_add(1);
        self.state = TaskState::Ready;
        self.attempt_id = None;
        self.retired_attempt_id = None;
        Ok(())
    }

    pub(crate) fn yield_ready(&mut self) -> Result<(), TransitionError> {
        self.require_running()?;
        self.state = TaskState::Ready;
        self.attempt_id = None;
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
        self.typed_wake = None;
        self.ended_children = None;
        self.wake_children = None;
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
        if let WaitFor::TypedSleep {
            until_millis,
            signals,
        } = &condition
            && ((until_millis.is_none() && signals.is_empty())
                || until_millis.is_some_and(|time| time < 0)
                || signals
                    .iter()
                    .enumerate()
                    .any(|(i, key)| signals[..i].contains(key)))
        {
            return Err(TransitionError::InvalidWait);
        }
        self.delivered_signal = None;
        self.typed_wake = None;
        self.ended_children = None;
        self.wake_children = None;
        self.state = TaskState::Waiting(condition);
        self.attempt_id = None;
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
        self.attempt_id = None;
        self.typed_wake = None;
        self.ended_children = None;
        self.wake_children = None;
        Ok(())
    }

    pub(crate) fn hold_success(&mut self, result: Value) -> Result<(), TransitionError> {
        self.succeed(result)?;
        self.state = match std::mem::replace(&mut self.state, TaskState::Ready) {
            TaskState::Succeeded(value) => TaskState::Completing(value),
            _ => unreachable!(),
        };
        Ok(())
    }

    /// Turn a prepared successful candidate into a held outcome after a
    /// fenced rescan discovers newly admitted owned work.
    pub(crate) fn hold_prepared_success(&mut self) -> Result<(), TransitionError> {
        let TaskState::Succeeded(value) = &self.state else {
            return Err(TransitionError::InvalidState);
        };
        self.state = TaskState::Completing(value.clone());
        Ok(())
    }

    pub fn is_completing(&self) -> bool {
        matches!(
            self.state,
            TaskState::Completing(_) | TaskState::CompletingFailed(_)
        )
    }

    pub(crate) fn hold_failure(&mut self, reason: String) -> Result<(), TransitionError> {
        self.fail(reason)?;
        self.state = match std::mem::replace(&mut self.state, TaskState::Ready) {
            TaskState::Failed(reason) => TaskState::CompletingFailed(reason),
            _ => unreachable!(),
        };
        Ok(())
    }

    pub(crate) fn finalize(&mut self) -> Result<(), TransitionError> {
        self.state = match &self.state {
            TaskState::Completing(value) => TaskState::Succeeded(value.clone()),
            TaskState::CompletingFailed(reason) => TaskState::Failed(reason.clone()),
            _ => return Err(TransitionError::InvalidState),
        };
        Ok(())
    }

    pub fn fail(&mut self, reason: String) -> Result<(), TransitionError> {
        self.require_running()?;
        self.state = TaskState::Failed(reason);
        self.attempt_id = None;
        self.typed_wake = None;
        self.ended_children = None;
        self.wake_children = None;
        Ok(())
    }

    pub(crate) fn fail_abort_restart(&mut self) -> Result<(), TransitionError> {
        if self.state != TaskState::Ready || !self.abort_requested {
            return Err(TransitionError::InvalidState);
        }
        self.state = TaskState::Failed("abort restart limit".into());
        self.typed_wake = None;
        self.ended_children = None;
        self.wake_children = None;
        Ok(())
    }

    /// Cancellation is permitted for any nonterminal state, including blocked work.
    pub fn cancel(&mut self) -> Result<(), TransitionError> {
        if matches!(
            self.state,
            TaskState::Completing(_)
                | TaskState::CompletingFailed(_)
                | TaskState::Succeeded(_)
                | TaskState::Failed(_)
                | TaskState::Cancelled
        ) {
            return Err(TransitionError::InvalidState);
        }
        self.state = TaskState::Cancelled;
        self.attempt_id = None;
        self.retired_attempt_id = None;
        self.typed_wake = None;
        self.ended_children = None;
        self.wake_children = None;
        Ok(())
    }

    /// Terminal children satisfy a task wait regardless of their outcome.
    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal_state()
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
