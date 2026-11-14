//! Serializable conversation state and submission transitions.
//!
//! A host must persist a complete transition atomically before publishing it.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InputMode {
    Steer,
    FollowUp,
}

/// Optional input admission policies. Defaults preserve ordinary follow-up admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionOptions {
    pub mode: InputMode,
    pub reject_when_busy: bool,
    pub reject_when_idle: bool,
}

impl Default for AdmissionOptions {
    fn default() -> Self {
        Self {
            mode: InputMode::FollowUp,
            reject_when_busy: false,
            reject_when_idle: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionError {
    #[error("conversation is busy")]
    Busy,
    #[error("conversation is idle")]
    Idle,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionStatus {
    Queued,
    Placed,
    Done { answer_id: Uuid },
    Unanswered { reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Submission {
    pub id: Uuid,
    pub request_id: Option<String>,
    pub content: String,
    pub mode: InputMode,
    pub status: SubmissionStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub id: Uuid,
    pub role: EntryRole,
    pub content: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryRole {
    User,
    Assistant,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationState {
    pub id: Uuid,
    pub entries: Vec<Entry>,
    pub submissions: Vec<Submission>,
    /// Inputs placed in the current run, including steering inputs.
    pub active: Vec<Uuid>,
}

impl ConversationState {
    pub fn new(id: Uuid) -> Self {
        Self {
            id,
            entries: Vec::new(),
            submissions: Vec::new(),
            active: Vec::new(),
        }
    }

    /// Match upstream request-ID admission: reuse an existing input even if the
    /// resent content differs. IDs are scoped to this conversation.
    pub fn submit(&mut self, request_id: Option<String>, content: String, mode: InputMode) -> Uuid {
        self.submit_with_options(
            request_id,
            content,
            AdmissionOptions {
                mode,
                ..Default::default()
            },
        )
        .expect("default admission cannot reject")
    }

    pub fn submit_with_options(
        &mut self,
        request_id: Option<String>,
        content: String,
        options: AdmissionOptions,
    ) -> Result<Uuid, AdmissionError> {
        if let Some(key) = request_id.as_ref()
            && let Some(existing) = self
                .submissions
                .iter()
                .find(|s| s.request_id.as_ref() == Some(key))
        {
            return Ok(existing.id);
        }
        let busy = !self.active.is_empty();
        if busy && options.reject_when_busy {
            return Err(AdmissionError::Busy);
        }
        if !busy && options.reject_when_idle {
            return Err(AdmissionError::Idle);
        }
        let id = Uuid::new_v4();
        self.submissions.push(Submission {
            id,
            request_id,
            content,
            mode: options.mode,
            status: SubmissionStatus::Queued,
        });
        // A queued follow-up prevents a later submission jumping the queue.
        if !busy
            && !self
                .submissions
                .iter()
                .any(|s| s.id != id && s.status == SubmissionStatus::Queued)
        {
            self.place(id);
        }
        Ok(id)
    }

    fn place(&mut self, id: Uuid) {
        let submission = self
            .submissions
            .iter_mut()
            .find(|s| s.id == id)
            .expect("known submission");
        submission.status = SubmissionStatus::Placed;
        self.entries.push(Entry {
            id: Uuid::new_v4(),
            role: EntryRole::User,
            content: submission.content.clone(),
        });
        self.active.push(id);
    }

    /// Place one queued steer at a safe boundary. Follow-ups remain queued.
    pub fn place_steer(&mut self) -> Option<Uuid> {
        if self.active.is_empty() {
            return None;
        }
        let id = self
            .submissions
            .iter()
            .find(|s| s.status == SubmissionStatus::Queued && s.mode == InputMode::Steer)?
            .id;
        self.place(id);
        Some(id)
    }

    /// Settle every input in this run with the same committed answer.
    pub fn answer(&mut self, content: String) -> Option<Uuid> {
        if self.active.is_empty() {
            return None;
        }
        let answer_id = Uuid::new_v4();
        self.entries.push(Entry {
            id: answer_id,
            role: EntryRole::Assistant,
            content,
        });
        for submission in &mut self.submissions {
            if self.active.contains(&submission.id) {
                submission.status = SubmissionStatus::Done { answer_id };
            }
        }
        self.active.clear();
        Some(answer_id)
    }

    /// A failed run leaves later queued inputs in place, matching the upstream
    /// default `afterFailedRun: keep`.
    pub fn fail(&mut self, reason: String) {
        for submission in &mut self.submissions {
            if self.active.contains(&submission.id) {
                submission.status = SubmissionStatus::Unanswered {
                    reason: reason.clone(),
                };
            }
        }
        self.active.clear();
    }

    /// Start the oldest queued input after a successful run or an explicit host
    /// decision to continue following failure.
    pub fn place_next(&mut self) -> Option<Uuid> {
        if !self.active.is_empty() {
            return None;
        }
        let id = self
            .submissions
            .iter()
            .find(|s| s.status == SubmissionStatus::Queued)?
            .id;
        self.place(id);
        Some(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_request_keeps_original_and_queued_input_survives_roundtrip() {
        let mut state = ConversationState::new(Uuid::new_v4());
        let first = state.submit(Some("same".into()), "original".into(), InputMode::FollowUp);
        assert_eq!(
            state.submit(Some("same".into()), "changed".into(), InputMode::Steer),
            first
        );
        let queued = state.submit(Some("next".into()), "next".into(), InputMode::FollowUp);
        let bytes = serde_json::to_vec(&state).unwrap();
        let mut state: ConversationState = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(state.submissions.len(), 2);
        let answer = state.answer("done".into()).unwrap();
        assert_eq!(
            state.submissions[0].status,
            SubmissionStatus::Done { answer_id: answer }
        );
        assert_eq!(state.place_next(), Some(queued));
        assert_eq!(state.entries.last().unwrap().content, "next");
    }

    #[test]
    fn steer_joins_active_run_followup_waits_and_failure_keeps_queue() {
        let mut s = ConversationState::new(Uuid::new_v4());
        let initial = s.submit(None, "one".into(), InputMode::FollowUp);
        let followup = s.submit(None, "later".into(), InputMode::FollowUp);
        let steer = s.submit(None, "change".into(), InputMode::Steer);
        assert_eq!(s.place_steer(), Some(steer));
        s.fail("interrupted".into());
        assert!(
            s.submissions
                .iter()
                .filter(|x| x.id == initial || x.id == steer)
                .all(|x| matches!(x.status, SubmissionStatus::Unanswered { .. }))
        );
        assert_eq!(s.place_next(), Some(followup));
    }
}
