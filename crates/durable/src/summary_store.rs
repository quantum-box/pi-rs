//! Version-fenced persistence for independent summary operations.
//!
//! This is a storage primitive, not a scheduler or a placement API. Before
//! recovery the caller must fence and join the old executor, including any
//! usage settlement. A matching owner ID is an identity check, not a lease.
//! Commit errors can have an unknown outcome; reopen and inspect before retrying.

use crate::compact::{CompactionResult, Preparation};
use crate::model::CompletedGeneration;
use durable_store::{Check, Expected, Mutation, Store};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const PREFIX: &[u8] = b"durable/summary-operation/";
const REQUEST_PREFIX: &[u8] = b"durable/summary-request/";
/// Bounds the persisted request and result separately, in serialized bytes.
pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
/// Bounds both retries and the retained attempt/usage history per operation.
pub const MAX_ATTEMPTS: usize = 16;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("invalid or reused identity")]
    Identity,
    #[error("operation not found")]
    NotFound,
    #[error("operation version changed")]
    Conflict,
    #[error("invalid operation state")]
    State,
    #[error("attempt owner does not match")]
    Owner,
    #[error("operation payload or attempt limit exceeded")]
    Limit,
    #[error("invalid completed summary: {0}")]
    InvalidResult(String),
    #[error("invalid persisted operation")]
    InvalidRecord,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum State {
    Ready,
    Reserved {
        request_id: Uuid,
        owner: Uuid,
    },
    Running {
        request_id: Uuid,
        owner: Uuid,
    },
    Completed {
        request_id: Uuid,
        result: Box<CompactionResult>,
    },
    Aborting {
        owner: Option<Uuid>,
    },
    Aborted,
}

/// Unconfirmed records are retained until a future accounting layer resolves
/// them; they do not imply that the provider did or did not perform work.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Attempt {
    pub request_id: Uuid,
    pub unconfirmed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Operation {
    id: Uuid,
    session_id: Uuid,
    preparation: Preparation,
    state: State,
    attempts: Vec<Attempt>,
}
impl Operation {
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn session_id(&self) -> Uuid {
        self.session_id
    }
    pub fn preparation(&self) -> &Preparation {
        &self.preparation
    }
    pub fn state(&self) -> &State {
        &self.state
    }
    pub fn attempts(&self) -> &[Attempt] {
        &self.attempts
    }
}
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub operation: Operation,
    pub version: u64,
}

pub struct SummaryStore<'a> {
    store: &'a Store,
}
impl<'a> SummaryStore<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }
    fn key(id: Uuid) -> Vec<u8> {
        [PREFIX, id.as_bytes()].concat()
    }

    pub fn create(
        &self,
        id: Uuid,
        session_id: Uuid,
        preparation: Preparation,
    ) -> Result<(), Error> {
        if id.is_nil() || session_id.is_nil() {
            return Err(Error::Identity);
        }
        if serde_json::to_vec(&preparation)?.len() > MAX_PAYLOAD_BYTES {
            return Err(Error::Limit);
        }
        let op = Operation {
            id,
            session_id,
            preparation,
            state: State::Ready,
            attempts: vec![],
        };
        self.write(&op, Expected::Missing)
    }
    pub fn get(&self, id: Uuid) -> Result<Option<Snapshot>, Error> {
        let Some(entry) = self.store.get(&Self::key(id))? else {
            return Ok(None);
        };
        let op: Operation = serde_json::from_slice(&entry.value)?;
        if op.id != id
            || op.id.is_nil()
            || op.session_id.is_nil()
            || op.attempts.len() > MAX_ATTEMPTS
        {
            return Err(Error::InvalidRecord);
        }
        Ok(Some(Snapshot {
            operation: op,
            version: entry.version,
        }))
    }
    fn write(&self, op: &Operation, expected: Expected) -> Result<(), Error> {
        let key = Self::key(op.id);
        self.store
            .commit(
                &[Check {
                    key: key.clone(),
                    expected,
                }],
                &[Mutation::Put {
                    key,
                    value: serde_json::to_vec(op)?,
                }],
            )
            .map_err(|e| match e {
                durable_store::Error::Conflict { .. } => Error::Conflict,
                e => Error::Store(e),
            })
    }
    fn load(&self, id: Uuid, version: u64) -> Result<Operation, Error> {
        let s = self.get(id)?.ok_or(Error::NotFound)?;
        if s.version != version {
            return Err(Error::Conflict);
        }
        Ok(s.operation)
    }
    /// Reserve an attempt before any provider call. Request IDs cannot be reused
    /// within an operation, including reservations interrupted before dispatch.
    pub fn reserve(
        &self,
        id: Uuid,
        version: u64,
        owner: Uuid,
        request_id: Uuid,
    ) -> Result<(), Error> {
        let mut op = self.load(id, version)?;
        if !matches!(op.state, State::Ready) {
            return Err(Error::State);
        }
        if owner.is_nil()
            || request_id.is_nil()
            || op.attempts.iter().any(|a| a.request_id == request_id)
        {
            return Err(Error::Identity);
        }
        if op.attempts.len() == MAX_ATTEMPTS {
            return Err(Error::Limit);
        }
        op.attempts.push(Attempt {
            request_id,
            unconfirmed: false,
        });
        op.state = State::Reserved { request_id, owner };
        let key = Self::key(id);
        let request_key = [REQUEST_PREFIX, request_id.as_bytes()].concat();
        self.store
            .commit(
                &[
                    Check {
                        key: key.clone(),
                        expected: Expected::Version(version),
                    },
                    Check {
                        key: request_key.clone(),
                        expected: Expected::Missing,
                    },
                ],
                &[
                    Mutation::Put {
                        key,
                        value: serde_json::to_vec(&op)?,
                    },
                    Mutation::Put {
                        key: request_key,
                        value: id.as_bytes().to_vec(),
                    },
                ],
            )
            .map_err(|e| match e {
                durable_store::Error::Conflict { .. } => Error::Conflict,
                e => Error::Store(e),
            })
    }
    /// Commit this transition before dispatching the model request.
    pub fn start(&self, id: Uuid, version: u64, owner: Uuid) -> Result<(), Error> {
        let mut op = self.load(id, version)?;
        let State::Reserved {
            request_id,
            owner: actual,
        } = op.state
        else {
            return Err(Error::State);
        };
        if owner != actual {
            return Err(Error::Owner);
        }
        op.state = State::Running { request_id, owner };
        self.write(&op, Expected::Version(version))
    }
    pub fn complete(
        &self,
        id: Uuid,
        version: u64,
        owner: Uuid,
        generation: CompletedGeneration,
    ) -> Result<(), Error> {
        let mut op = self.load(id, version)?;
        let State::Running {
            request_id,
            owner: actual,
        } = op.state
        else {
            return Err(Error::State);
        };
        if owner != actual {
            return Err(Error::Owner);
        }
        let result = op
            .preparation
            .clone()
            .finish(generation)
            .map_err(|e| Error::InvalidResult(e.to_string()))?;
        if serde_json::to_vec(&result)?.len() > MAX_PAYLOAD_BYTES {
            return Err(Error::Limit);
        }
        op.state = State::Completed {
            request_id,
            result: Box::new(result),
        };
        self.write(&op, Expected::Version(version))
    }
    fn mark_unconfirmed(op: &mut Operation, request_id: Uuid) -> Result<(), Error> {
        let attempt = op
            .attempts
            .iter_mut()
            .find(|a| a.request_id == request_id)
            .ok_or(Error::InvalidRecord)?;
        attempt.unconfirmed = true;
        Ok(())
    }
    /// Fence completion immediately. An active request is marked unconfirmed;
    /// the caller must stop and join its executor before finishing the abort.
    pub fn begin_abort(&self, id: Uuid, version: u64) -> Result<(), Error> {
        let mut op = self.load(id, version)?;
        let owner = match op.state {
            State::Ready => None,
            State::Reserved { owner, .. } => Some(owner),
            State::Running { owner, request_id } => {
                Self::mark_unconfirmed(&mut op, request_id)?;
                Some(owner)
            }
            _ => return Err(Error::State),
        };
        op.state = State::Aborting { owner };
        self.write(&op, Expected::Version(version))
    }
    /// Caller must have joined the matching executor before this transition.
    pub fn finish_abort(&self, id: Uuid, version: u64, owner: Option<Uuid>) -> Result<(), Error> {
        let mut op = self.load(id, version)?;
        let State::Aborting { owner: actual } = op.state else {
            return Err(Error::State);
        };
        if owner != actual {
            return Err(Error::Owner);
        }
        op.state = State::Aborted;
        self.write(&op, Expected::Version(version))
    }
    /// Recover only after the prior owner is fenced. A reserved request has not
    /// started; a running request may have incurred usage and is marked unknown.
    /// Aborting operations finish abort rather than being retried.
    pub fn recover(&self, id: Uuid, version: u64, owner: Uuid) -> Result<(), Error> {
        let mut op = self.load(id, version)?;
        match op.state {
            State::Reserved { owner: actual, .. } if actual == owner => op.state = State::Ready,
            State::Running {
                owner: actual,
                request_id,
            } if actual == owner => {
                Self::mark_unconfirmed(&mut op, request_id)?;
                op.state = State::Ready;
            }
            State::Aborting {
                owner: Some(actual),
            } if actual == owner => op.state = State::Aborted,
            State::Reserved { .. } | State::Running { .. } | State::Aborting { owner: Some(_) } => {
                return Err(Error::Owner);
            }
            _ => return Err(Error::State),
        }
        self.write(&op, Expected::Version(version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_rs_core::{Message, ModelMetadata};
    use std::sync::{Arc, Barrier};

    fn preparation() -> Preparation {
        crate::compact::prepare(
            &[
                Message::User {
                    content: "old".into(),
                },
                Message::User {
                    content: "recent".into(),
                },
            ],
            "test",
            64,
            0,
            None,
        )
        .unwrap()
        .unwrap()
    }
    fn generation() -> CompletedGeneration {
        CompletedGeneration {
            content: "summary".into(),
            calls: vec![],
            usage: None,
            metadata: ModelMetadata::default(),
        }
    }
    fn snapshot(s: &SummaryStore<'_>, id: Uuid) -> Snapshot {
        s.get(id).unwrap().unwrap()
    }

    #[test]
    fn transitions_fence_owner_version_and_abort() {
        let store = Store::memory().unwrap();
        let s = SummaryStore::new(&store);
        let (id, session, owner, other, request) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        s.create(id, session, preparation()).unwrap();
        let v = snapshot(&s, id).version;
        s.reserve(id, v, owner, request).unwrap();
        assert!(matches!(
            s.reserve(id, v, other, Uuid::new_v4()),
            Err(Error::Conflict)
        ));
        let v = snapshot(&s, id).version;
        assert!(matches!(s.start(id, v, other), Err(Error::Owner)));
        s.start(id, v, owner).unwrap();
        let v = snapshot(&s, id).version;
        assert!(matches!(
            s.complete(id, v, other, generation()),
            Err(Error::Owner)
        ));
        s.begin_abort(id, v).unwrap();
        assert!(matches!(
            s.complete(id, v, owner, generation()),
            Err(Error::Conflict)
        ));
        let snap = snapshot(&s, id);
        assert!(snap.operation.attempts()[0].unconfirmed);
        assert!(matches!(
            s.finish_abort(id, snap.version, Some(other)),
            Err(Error::Owner)
        ));
        s.finish_abort(id, snap.version, Some(owner)).unwrap();
        assert!(matches!(snapshot(&s, id).operation.state(), State::Aborted));
    }

    #[test]
    fn completion_and_global_request_deduplication() {
        let store = Store::memory().unwrap();
        let s = SummaryStore::new(&store);
        let (id, other_id, session, owner, request) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        s.create(id, session, preparation()).unwrap();
        s.create(other_id, session, preparation()).unwrap();
        s.reserve(id, snapshot(&s, id).version, owner, request)
            .unwrap();
        assert!(matches!(
            s.reserve(other_id, snapshot(&s, other_id).version, owner, request),
            Err(Error::Conflict)
        ));
        s.start(id, snapshot(&s, id).version, owner).unwrap();
        let v = snapshot(&s, id).version;
        let mut invalid = generation();
        invalid.content.clear();
        assert!(matches!(
            s.complete(id, v, owner, invalid),
            Err(Error::InvalidResult(_))
        ));
        s.complete(id, v, owner, generation()).unwrap();
        let snap = snapshot(&s, id);
        assert!(
            matches!(snap.operation.state(), State::Completed { request_id, result } if *request_id == request && result.generation.content == "summary")
        );
        assert!(!snap.operation.attempts()[0].unconfirmed);
    }

    #[test]
    fn reopen_recovers_reserved_running_and_aborting() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let (id, owner, request) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        {
            let store = Store::open(&path).unwrap();
            let s = SummaryStore::new(&store);
            s.create(id, Uuid::new_v4(), preparation()).unwrap();
            s.reserve(id, snapshot(&s, id).version, owner, request)
                .unwrap();
        }
        {
            let store = Store::open(&path).unwrap();
            let s = SummaryStore::new(&store);
            let v = snapshot(&s, id).version;
            assert!(matches!(
                s.recover(id, v, Uuid::new_v4()),
                Err(Error::Owner)
            ));
            s.recover(id, v, owner).unwrap();
            let snap = snapshot(&s, id);
            assert!(!snap.operation.attempts()[0].unconfirmed);
            assert!(matches!(
                s.reserve(id, snap.version, owner, request),
                Err(Error::Identity)
            ));
            s.reserve(id, snap.version, owner, Uuid::new_v4()).unwrap();
            s.start(id, snapshot(&s, id).version, owner).unwrap();
        }
        {
            let store = Store::open(&path).unwrap();
            let s = SummaryStore::new(&store);
            s.recover(id, snapshot(&s, id).version, owner).unwrap();
            let snap = snapshot(&s, id);
            assert_eq!(snap.operation.id(), id);
            assert!(matches!(snap.operation.state(), State::Ready));
            assert!(snap.operation.attempts()[1].unconfirmed);
            s.reserve(id, snap.version, owner, Uuid::new_v4()).unwrap();
            s.start(id, snapshot(&s, id).version, owner).unwrap();
            s.begin_abort(id, snapshot(&s, id).version).unwrap();
        }
        {
            let store = Store::open(&path).unwrap();
            let s = SummaryStore::new(&store);
            s.recover(id, snapshot(&s, id).version, owner).unwrap();
            let snap = snapshot(&s, id);
            assert!(matches!(snap.operation.state(), State::Aborted));
            assert!(snap.operation.attempts()[2].unconfirmed);
        }
    }

    #[test]
    fn attempts_and_payloads_are_bounded() {
        let store = Store::memory().unwrap();
        let s = SummaryStore::new(&store);
        let id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        s.create(id, Uuid::new_v4(), preparation()).unwrap();
        for _ in 0..MAX_ATTEMPTS {
            s.reserve(id, snapshot(&s, id).version, owner, Uuid::new_v4())
                .unwrap();
            s.recover(id, snapshot(&s, id).version, owner).unwrap();
        }
        assert!(matches!(
            s.reserve(id, snapshot(&s, id).version, owner, Uuid::new_v4()),
            Err(Error::Limit)
        ));
        let large = crate::compact::prepare(
            &[
                Message::User {
                    content: "x".repeat(MAX_PAYLOAD_BYTES),
                },
                Message::User {
                    content: "recent".into(),
                },
            ],
            "test",
            64,
            0,
            None,
        )
        .unwrap()
        .unwrap();
        assert!(matches!(
            s.create(Uuid::new_v4(), Uuid::new_v4(), large),
            Err(Error::Limit)
        ));
    }

    #[test]
    fn concurrent_claims_have_one_winner_across_connections() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let id = Uuid::new_v4();
        let store = Store::open(&path).unwrap();
        let s = SummaryStore::new(&store);
        s.create(id, Uuid::new_v4(), preparation()).unwrap();
        let v = snapshot(&s, id).version;
        let barrier = Arc::new(Barrier::new(3));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let store = Store::open(path).unwrap();
                    let s = SummaryStore::new(&store);
                    barrier.wait();
                    s.reserve(id, v, Uuid::new_v4(), Uuid::new_v4())
                })
            })
            .collect();
        barrier.wait();
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Err(Error::Conflict)))
                .count(),
            1
        );
        assert_eq!(snapshot(&s, id).operation.attempts().len(), 1);
    }
}
