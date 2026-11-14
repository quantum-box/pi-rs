//! Atomic persistence for the initial conversation state model.
//!
//! This is not yet a scheduler: it does not invoke a model or run tools.

use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use durable_store::{Check, Expected, Mutation, Store};
use uuid::Uuid;

use crate::state::{ConversationState, InputMode};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("storage: {0}")]
    Storage(#[from] durable_store::Error),
    #[error("invalid conversation state: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("conversation does not exist")]
    NotFound,
    #[error("storage operation failed; reopen before further use")]
    Failed,
    #[error("conversation mutation lock poisoned")]
    LockPoisoned,
}

/// A storage-backed set of conversation state transitions. Storage failures
/// poison this handle: a commit's outcome might be unknown and must be
/// recovered by reopening the store. CAS conflicts are safe to retry.
pub struct Conversations {
    store: Store,
    failed: AtomicBool,
    gate: Mutex<()>,
}

impl Conversations {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            failed: AtomicBool::new(false),
            gate: Mutex::new(()),
        }
    }

    fn check_open(&self) -> Result<(), Error> {
        if self.failed.load(Ordering::Acquire) {
            Err(Error::Failed)
        } else {
            Ok(())
        }
    }

    fn key(id: Uuid) -> Vec<u8> {
        format!("conversation/{id}").into_bytes()
    }

    pub fn create(&self, id: Uuid) -> Result<(), Error> {
        let _guard = self.gate.lock().map_err(|_| Error::LockPoisoned)?;
        self.check_open()?;
        let key = Self::key(id);
        let value = serde_json::to_vec(&ConversationState::new(id))?;
        match self.store.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Missing,
            }],
            &[Mutation::Put { key, value }],
        ) {
            Ok(()) => Ok(()),
            Err(error @ durable_store::Error::Conflict { .. }) => Err(error.into()),
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                Err(error.into())
            }
        }
    }

    pub fn get(&self, id: Uuid) -> Result<ConversationState, Error> {
        let _guard = self.gate.lock().map_err(|_| Error::LockPoisoned)?;
        self.check_open()?;
        let entry = self
            .store
            .get(&Self::key(id))
            .map_err(|error| {
                self.failed.store(true, Ordering::Release);
                Error::Storage(error)
            })?
            .ok_or(Error::NotFound)?;
        Ok(serde_json::from_slice(&entry.value)?)
    }

    /// Applies a deterministic in-memory change, then commits the complete
    /// state with a version check. No external effect may occur in `change`.
    fn update<T>(
        &self,
        id: Uuid,
        change: impl Fn(&mut ConversationState) -> T,
    ) -> Result<T, Error> {
        let _guard = self.gate.lock().map_err(|_| Error::LockPoisoned)?;
        self.check_open()?;
        let key = Self::key(id);
        loop {
            let entry = self
                .store
                .get(&key)
                .map_err(|error| {
                    self.failed.store(true, Ordering::Release);
                    Error::Storage(error)
                })?
                .ok_or(Error::NotFound)?;
            let mut state: ConversationState = serde_json::from_slice(&entry.value)?;
            let previous = state.clone();
            let result = change(&mut state);
            if state == previous {
                return Ok(result);
            }
            let value = serde_json::to_vec(&state)?;
            match self.store.commit(
                &[Check {
                    key: key.clone(),
                    expected: Expected::Version(entry.version),
                }],
                &[Mutation::Put {
                    key: key.clone(),
                    value,
                }],
            ) {
                Ok(()) => return Ok(result),
                Err(durable_store::Error::Conflict { .. }) => continue,
                Err(error) => {
                    self.failed.store(true, Ordering::Release);
                    return Err(error.into());
                }
            }
        }
    }

    pub fn submit(
        &self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
        mode: InputMode,
    ) -> Result<Uuid, Error> {
        self.update(id, |state| {
            state.submit(request_id.clone(), content.clone(), mode)
        })
    }

    pub fn place_steer(&self, id: Uuid) -> Result<Option<Uuid>, Error> {
        self.update(id, ConversationState::place_steer)
    }

    pub fn answer(&self, id: Uuid, content: String) -> Result<Option<Uuid>, Error> {
        self.update(id, |state| state.answer(content.clone()))
    }

    pub fn fail(&self, id: Uuid, reason: String) -> Result<(), Error> {
        self.update(id, |state| state.fail(reason.clone()))
    }

    pub fn place_next(&self, id: Uuid) -> Result<Option<Uuid>, Error> {
        self.update(id, ConversationState::place_next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::SubmissionStatus;

    #[test]
    fn admission_and_queued_work_survive_reopen_and_retry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let conversation = Uuid::new_v4();
        let first;
        let queued;
        {
            let db = Conversations::new(Store::open(&path).unwrap());
            db.create(conversation).unwrap();
            first = db
                .submit(
                    conversation,
                    Some("job".into()),
                    "original".into(),
                    InputMode::FollowUp,
                )
                .unwrap();
            queued = db
                .submit(
                    conversation,
                    Some("next".into()),
                    "next".into(),
                    InputMode::FollowUp,
                )
                .unwrap();
        }
        let db = Conversations::new(Store::open(&path).unwrap());
        assert_eq!(
            db.submit(
                conversation,
                Some("job".into()),
                "different".into(),
                InputMode::Steer
            )
            .unwrap(),
            first
        );
        let answer = db.answer(conversation, "done".into()).unwrap().unwrap();
        assert_eq!(
            db.get(conversation).unwrap().submissions[0].status,
            SubmissionStatus::Done { answer_id: answer }
        );
        assert_eq!(db.place_next(conversation).unwrap(), Some(queued));
    }

    #[test]
    fn concurrent_retries_admit_one_submission() {
        let db = std::sync::Arc::new(Conversations::new(Store::memory().unwrap()));
        let id = Uuid::new_v4();
        db.create(id).unwrap();
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let db = db.clone();
                std::thread::spawn(move || {
                    db.submit(
                        id,
                        Some("same".into()),
                        "content".into(),
                        InputMode::FollowUp,
                    )
                    .unwrap()
                })
            })
            .collect();
        let ids: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert!(ids.iter().all(|i| *i == ids[0]));
        assert_eq!(db.get(id).unwrap().submissions.len(), 1);
    }
}
