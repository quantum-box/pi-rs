//! Version-fenced persistence for independent summary operations.
//!
//! This is a storage primitive, not a scheduler or a placement API. Before
//! recovery the caller must fence and join the old executor, including any
//! usage settlement. A matching owner ID is an identity check, not a lease.
//! Commit errors can have an unknown outcome; reopen and inspect before retrying.

use crate::compact::{CompactionResult, Preparation};
use crate::model::CompletedGeneration;
use durable_store::{Check, Expected, Mutation, Store};
use pi_rs_core::UsageReport;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const PREFIX: &[u8] = b"durable/summary-operation/";
const REQUEST_PREFIX: &[u8] = b"durable/summary-request/";
const PLACEMENT_PREFIX: &[u8] = b"durable/summary-placement/";
type Discovery = (Vec<(Uuid, Uuid)>, Vec<Uuid>);
const OWNER_PREFIX: &[u8] = b"durable/summary-owner/";
/// Bounds the persisted request and result separately, in serialized bytes.
pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
/// Bounds both retries and the retained attempt/usage history per operation.
pub const MAX_ATTEMPTS: usize = 16;
/// Upper bound on records inspected during one startup discovery.
pub const MAX_DISCOVERY_OPERATIONS: usize = 128;
// A completed operation retains the preparation twice and one bounded
// generation. This leaves room for their enclosing metadata and attempts.
const MAX_DISCOVERY_RECORD_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_HOOK_SNAPSHOT_BYTES: usize = 512 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct HookCheckpoint {
    pub extensions: Vec<String>,
    pub snapshot: serde_json::Value,
}

/// Host-attested, versioned identities for the model and usage-settlement
/// service. Change either identity whenever its replay semantics change.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderIdentity {
    pub model: String,
    pub settlement: String,
}
impl ProviderIdentity {
    pub fn valid(&self) -> bool {
        [&self.model, &self.settlement]
            .into_iter()
            .all(|s| !s.is_empty() && s.len() <= 128 && s.bytes().all(|b| b.is_ascii_graphic()))
    }
}

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
    HookPending,
    Ready,
    Reserved {
        request_id: Uuid,
        owner: Uuid,
    },
    Running {
        request_id: Uuid,
        owner: Uuid,
    },
    Accounting {
        request_id: Uuid,
        owner: Uuid,
        generation: Box<CompletedGeneration>,
    },
    Completed {
        request_id: Uuid,
        result: Box<CompactionResult>,
    },
    /// A result admitted for placement, with an immutable ticket committed in
    /// the same transaction. Placement or staleness has not yet been decided.
    PlacementPending {
        request_id: Uuid,
        result: Box<CompactionResult>,
    },
    Placed {
        request_id: Uuid,
    },
    Stale {
        request_id: Uuid,
    },
    Declined,
    Aborting {
        owner: Option<Uuid>,
        #[serde(default)]
        accounting: Option<PendingAccounting>,
    },
    Aborted,
}

/// A completed generation retained while abort waits for usage settlement.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PendingAccounting {
    pub request_id: Uuid,
    pub generation: Box<CompletedGeneration>,
}

/// Attempt identity, uncertainty and durable settlement receipt. Unconfirmed
/// does not imply that the provider did or did not perform work.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Attempt {
    pub request_id: Uuid,
    pub unconfirmed: bool,
    /// Durable receipt after the idempotent external settlement succeeds.
    #[serde(default)]
    pub settled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Operation {
    id: Uuid,
    session_id: Uuid,
    preparation: Preparation,
    state: State,
    attempts: Vec<Attempt>,
    #[serde(default)]
    placement_version: Option<u64>,
    #[serde(default)]
    admission_version: Option<u64>,
    #[serde(default)]
    busy_checkpoint: Option<serde_json::Value>,
    #[serde(default)]
    provider: Option<ProviderIdentity>,
    #[serde(default)]
    auto_place: bool,
    #[serde(default)]
    hook: Option<HookCheckpoint>,
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
    pub(crate) fn placement_version(&self) -> Option<u64> {
        self.placement_version
    }
    pub(crate) fn busy_checkpoint(&self) -> Option<&serde_json::Value> {
        self.busy_checkpoint.as_ref()
    }
    pub(crate) fn provider(&self) -> Option<&ProviderIdentity> {
        self.provider.as_ref()
    }
    pub(crate) fn auto_place(&self) -> bool {
        self.auto_place
    }
    pub(crate) fn hook(&self) -> Option<&HookCheckpoint> {
        self.hook.as_ref()
    }
    pub(crate) fn is_live_owned(&self) -> bool {
        self.auto_place
            && !matches!(
                self.state,
                State::Placed { .. } | State::Stale { .. } | State::Aborted | State::Declined
            )
    }

    /// Derive the exact report to replay from the durable checkpoint. Once an
    /// attempt is unconfirmed, later observations must not change its report:
    /// the external settlement may already have accepted this value.
    pub fn settlement_report(&self, request_id: Uuid) -> Result<UsageReport, Error> {
        let attempt = self
            .attempts
            .iter()
            .find(|a| a.request_id == request_id)
            .ok_or(Error::Identity)?;
        if attempt.unconfirmed {
            return Ok(UsageReport::Unconfirmed);
        }
        let generation = match &self.state {
            State::Accounting {
                request_id: current,
                generation,
                ..
            } if *current == request_id => generation,
            State::Aborting {
                accounting: Some(pending),
                ..
            } if pending.request_id == request_id => &pending.generation,
            _ => return Err(Error::State),
        };
        Ok(generation
            .usage
            .map_or(UsageReport::Unconfirmed, |usage| UsageReport::Measured {
                usage,
            }))
    }
}
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub operation: Operation,
    pub version: u64,
}

pub(crate) fn decode_entry(key: &[u8], entry: durable_store::Entry) -> Result<Snapshot, Error> {
    if entry.value.len() > MAX_DISCOVERY_RECORD_BYTES {
        return Err(Error::Limit);
    }
    let id = Uuid::from_slice(key.strip_prefix(PREFIX).ok_or(Error::InvalidRecord)?)
        .map_err(|_| Error::InvalidRecord)?;
    let operation: Operation = serde_json::from_slice(&entry.value)?;
    if operation.id != id
        || id.is_nil()
        || operation.session_id.is_nil()
        || operation.attempts.len() > MAX_ATTEMPTS
    {
        return Err(Error::InvalidRecord);
    }
    Ok(Snapshot {
        operation,
        version: entry.version,
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PlacementTicket {
    pub operation: Uuid,
    pub conversation: Uuid,
    pub conversation_version: u64,
    pub request_id: Uuid,
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
    fn placement_key(id: Uuid) -> Vec<u8> {
        [PLACEMENT_PREFIX, id.as_bytes()].concat()
    }
    fn owner_key(id: Uuid) -> Vec<u8> {
        [OWNER_PREFIX, id.as_bytes()].concat()
    }

    #[cfg(test)]
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
            placement_version: None,
            admission_version: None,
            busy_checkpoint: None,
            provider: None,
            auto_place: false,
            hook: None,
        };
        self.write(&op, Expected::Missing)
    }

    #[cfg(test)]
    pub(crate) fn create_identified(
        &self,
        id: Uuid,
        session_id: Uuid,
        preparation: Preparation,
        provider: ProviderIdentity,
    ) -> Result<(), Error> {
        if id.is_nil() || session_id.is_nil() || !provider.valid() {
            return Err(Error::Identity);
        }
        self.write(
            &Operation {
                id,
                session_id,
                preparation,
                state: State::Ready,
                attempts: vec![],
                placement_version: None,
                admission_version: None,
                busy_checkpoint: None,
                provider: Some(provider),
                auto_place: false,
                hook: None,
            },
            Expected::Missing,
        )
    }

    /// Atomically bind a preparation to the exact conversation version whose
    /// context was inspected by the caller. The conversation key is reserved by
    /// the runtime and is checked again at placement.
    pub(crate) fn create_bound(
        &self,
        id: Uuid,
        session_id: Uuid,
        preparation: Preparation,
        (conversation_key, version): (Vec<u8>, u64),
        provider: ProviderIdentity,
        (auto_place, hook, busy_checkpoint): (
            bool,
            Option<HookCheckpoint>,
            Option<serde_json::Value>,
        ),
    ) -> Result<(), Error> {
        if id.is_nil() || session_id.is_nil() || version == 0 || !provider.valid() {
            return Err(Error::Identity);
        }
        if serde_json::to_vec(&preparation)?.len() > MAX_PAYLOAD_BYTES {
            return Err(Error::Limit);
        }
        let hook_bytes = hook
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()?
            .map_or(0, |value| value.len());
        let busy_bytes = busy_checkpoint
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()?
            .map_or(0, |value| value.len());
        if hook_bytes.saturating_add(busy_bytes) > MAX_HOOK_SNAPSHOT_BYTES {
            return Err(Error::Limit);
        }
        let op = Operation {
            id,
            session_id,
            preparation,
            state: if hook.is_some() {
                State::HookPending
            } else {
                State::Ready
            },
            attempts: vec![],
            placement_version: Some(version),
            admission_version: None,
            busy_checkpoint,
            provider: Some(provider),
            auto_place,
            hook,
        };
        let key = Self::key(id);
        let owner_key = Self::owner_key(session_id);
        let owner_entry = if auto_place {
            self.store.get(&owner_key)?
        } else {
            None
        };
        let mut checks = vec![
            Check {
                key: key.clone(),
                expected: Expected::Missing,
            },
            Check {
                key: conversation_key,
                expected: Expected::Version(version),
            },
        ];
        let mut mutations = vec![Mutation::Put {
            key,
            value: serde_json::to_vec(&op)?,
        }];
        if auto_place {
            checks.push(Check {
                key: owner_key.clone(),
                expected: owner_entry
                    .map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
            });
            mutations.push(Mutation::Put {
                key: owner_key,
                value: id.as_bytes().to_vec(),
            });
        }
        self.store
            .commit(&checks, &mutations)
            .map_err(|error| match error {
                durable_store::Error::Conflict { .. } => Error::Conflict,
                other => Error::Store(other),
            })
    }

    /// Read all summary records with the same bounded scan used at startup.
    pub(crate) fn list(&self) -> Result<Vec<Snapshot>, Error> {
        let mut after = PREFIX.to_vec();
        let mut result = Vec::new();
        for count in 0..=MAX_DISCOVERY_OPERATIONS {
            let (_, mut page) = self.store.page_with_metadata(
                b"durable/summary-discovery/unused",
                PREFIX,
                &after,
                1,
            )?;
            let Some((key, entry)) = page.pop() else {
                return Ok(result);
            };
            if count == MAX_DISCOVERY_OPERATIONS || entry.value.len() > MAX_DISCOVERY_RECORD_BYTES {
                return Err(Error::Limit);
            }
            after = key.clone();
            result.push(decode_entry(&key, entry)?);
        }
        unreachable!()
    }

    /// Check admission epochs before scanning, so a concurrently created
    /// summary forces the enclosing graph abort transaction to retry.
    pub(crate) fn prepare_owned_abort(
        &self,
        conversations: &std::collections::HashSet<Uuid>,
        pending_writes: &std::collections::HashMap<Uuid, Uuid>,
    ) -> Result<(Vec<Check>, Vec<Mutation>), Error> {
        let mut checks = Vec::new();
        let mut mutations = Vec::new();
        for id in conversations {
            let key = Self::owner_key(*id);
            let entry = self.store.get(&key)?;
            checks.push(Check {
                key,
                expected: entry.map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
            });
        }
        for snap in self.list()? {
            if !conversations.contains(&snap.operation.session_id)
                || !snap.operation.is_live_owned()
            {
                continue;
            }
            if pending_writes.get(&snap.operation.id) == Some(&snap.operation.session_id)
                && matches!(snap.operation.state, State::PlacementPending { .. })
            {
                checks.push(Check {
                    key: Self::key(snap.operation.id),
                    expected: Expected::Version(snap.version),
                });
                continue;
            }
            if matches!(snap.operation.state, State::Aborting { .. }) {
                continue;
            }
            let mut op = snap.operation;
            Self::mark_aborting(&mut op)?;
            let key = Self::key(op.id);
            checks.push(Check {
                key: key.clone(),
                expected: Expected::Version(snap.version),
            });
            mutations.push(Mutation::Put {
                key,
                value: serde_json::to_vec(&op)?,
            });
        }
        Ok((checks, mutations))
    }

    /// Bounded discovery is all-or-nothing: if the store exceeds its work or
    /// payload budget no executor is started by this scan. Terminal records
    /// count toward the limit, so callers can never silently skip older work.
    pub(crate) fn discover(&self, provider: &ProviderIdentity) -> Result<Discovery, Error> {
        if !provider.valid() {
            return Err(Error::Identity);
        }
        let mut after = PREFIX.to_vec();
        let mut runnable = Vec::new();
        let mut blocked = Vec::new();
        for count in 0..=MAX_DISCOVERY_OPERATIONS {
            let (_, mut page) = self.store.page_with_metadata(
                b"durable/summary-discovery/unused",
                PREFIX,
                &after,
                1,
            )?;
            let Some((key, entry)) = page.pop() else {
                return Ok((runnable, blocked));
            };
            if count == MAX_DISCOVERY_OPERATIONS {
                return Err(Error::Limit);
            }
            if entry.value.len() > MAX_DISCOVERY_RECORD_BYTES {
                return Err(Error::Limit);
            }
            let id = Uuid::from_slice(key.strip_prefix(PREFIX).ok_or(Error::InvalidRecord)?)
                .map_err(|_| Error::InvalidRecord)?;
            let op: Operation = serde_json::from_slice(&entry.value)?;
            if op.id != id
                || id.is_nil()
                || op.session_id.is_nil()
                || op.attempts.len() > MAX_ATTEMPTS
            {
                return Err(Error::InvalidRecord);
            }
            let needs_worker = matches!(
                op.state,
                State::HookPending
                    | State::Ready
                    | State::Reserved { .. }
                    | State::Running { .. }
                    | State::Accounting { .. }
                    | State::Aborting { .. }
            );
            if needs_worker
                || matches!(
                    op.state,
                    State::Completed { .. } | State::PlacementPending { .. }
                )
            {
                if op.provider.as_ref() != Some(provider) {
                    blocked.push(id);
                } else if needs_worker
                    || (op.auto_place
                        && matches!(
                            op.state,
                            State::Completed { .. } | State::PlacementPending { .. }
                        ))
                {
                    runnable.push((id, op.session_id));
                }
            }
            after = key;
        }
        unreachable!()
    }

    /// Public admissions are serialized by the owning Scheduler. Reserve
    /// capacity before creating another operation so a successful admission
    /// cannot make the next bounded startup scan exceed its record limit.
    pub(crate) fn ensure_creation_capacity(&self) -> Result<(), Error> {
        let mut after = PREFIX.to_vec();
        for _ in 0..MAX_DISCOVERY_OPERATIONS {
            let (_, mut page) = self.store.page_with_metadata(
                b"durable/summary-discovery/unused",
                PREFIX,
                &after,
                1,
            )?;
            let Some((key, entry)) = page.pop() else {
                return Ok(());
            };
            if entry.value.len() > MAX_DISCOVERY_RECORD_BYTES {
                return Err(Error::Limit);
            }
            after = key;
        }
        Err(Error::Limit)
    }

    /// Checkpoint the first hook decision before starting any model request.
    pub(crate) fn finish_hook(
        &self,
        id: Uuid,
        version: u64,
        decision: crate::extension::BeforeCompact,
    ) -> Result<(), Error> {
        let mut op = self.load(id, version)?;
        if !matches!(op.state, State::HookPending) || op.hook.is_none() {
            return Err(Error::State);
        }
        op.state = match decision {
            crate::extension::BeforeCompact::Pass => State::Ready,
            crate::extension::BeforeCompact::Decline => State::Declined,
            crate::extension::BeforeCompact::Summary(content) => {
                let generation = CompletedGeneration {
                    content,
                    calls: vec![],
                    usage: None,
                    metadata: Default::default(),
                };
                if serde_json::to_vec(&generation)?.len() > MAX_PAYLOAD_BYTES {
                    return Err(Error::Limit);
                }
                let result = op
                    .preparation
                    .clone()
                    .finish(generation)
                    .map_err(|e| Error::InvalidResult(e.to_string()))?;
                State::Completed {
                    request_id: op.id,
                    result: Box::new(result),
                }
            }
        };
        self.write(&op, Expected::Version(version))
    }

    /// Commit a placement ticket and pending state while fencing the observed
    /// conversation version. A changed version requires a busy checkpoint and
    /// a conversation marker; the runtime validates the checkpoint before calling.
    /// The ticket's store commit sequence is the durable ordering boundary.
    #[cfg(test)]
    pub(crate) fn admit_placement(
        &self,
        snap: &Snapshot,
        conversation_key: Vec<u8>,
        conversation_version: u64,
    ) -> Result<(), Error> {
        self.admit_placement_with_marker(snap, conversation_key, conversation_version, None)
    }

    pub(crate) fn admit_placement_with_marker(
        &self,
        snap: &Snapshot,
        conversation_key: Vec<u8>,
        conversation_version: u64,
        marker: Option<Vec<u8>>,
    ) -> Result<(), Error> {
        let mut op = snap.operation.clone();
        if (op.placement_version != Some(conversation_version)
            && (marker.is_none() || op.busy_checkpoint.is_none()))
            || conversation_key != crate::runtime::Harness::key(op.session_id)
        {
            return Err(Error::State);
        }
        let State::Completed { request_id, result } = op.state else {
            return Err(Error::State);
        };
        let ticket = PlacementTicket {
            operation: op.id,
            conversation: op.session_id,
            conversation_version,
            request_id,
        };
        op.admission_version = Some(conversation_version);
        op.state = State::PlacementPending { request_id, result };
        let key = Self::key(op.id);
        let ticket_key = Self::placement_key(op.id);
        let mut mutations = vec![
            Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(&op)?,
            },
            Mutation::Put {
                key: ticket_key.clone(),
                value: serde_json::to_vec(&ticket)?,
            },
        ];
        if let Some(value) = marker {
            mutations.push(Mutation::Put {
                key: conversation_key.clone(),
                value,
            });
        }
        self.store
            .commit(
                &[
                    Check {
                        key: key.clone(),
                        expected: Expected::Version(snap.version),
                    },
                    Check {
                        key: conversation_key,
                        expected: Expected::Version(conversation_version),
                    },
                    Check {
                        key: ticket_key.clone(),
                        expected: Expected::Missing,
                    },
                ],
                &mutations,
            )
            .map_err(|e| match e {
                durable_store::Error::Conflict { .. } => Error::Conflict,
                other => Error::Store(other),
            })
    }

    /// Read and validate the immutable ticket and its original store commit.
    pub(crate) fn placement_ticket(
        &self,
        snap: &Snapshot,
    ) -> Result<(PlacementTicket, u64), Error> {
        let (request_id, pending) = match snap.operation.state() {
            State::PlacementPending { request_id, .. } => (request_id, true),
            State::Placed { request_id } | State::Stale { request_id } => (request_id, false),
            _ => return Err(Error::State),
        };
        let key = Self::placement_key(snap.operation.id);
        let entry = self.store.get(&key)?.ok_or(Error::InvalidRecord)?;
        let ticket: PlacementTicket = serde_json::from_slice(&entry.value)?;
        if ticket.operation != snap.operation.id
            || ticket.conversation != snap.operation.session_id
            || Some(ticket.conversation_version)
                != snap
                    .operation
                    .admission_version
                    .or(snap.operation.placement_version)
            || ticket.request_id != *request_id
        {
            return Err(Error::InvalidRecord);
        }
        let sequence = self
            .store
            .commit_for_version(&key, entry.version)?
            .filter(|sequence| *sequence > 0)
            .ok_or(Error::InvalidRecord)?;
        let operation_sequence = self
            .store
            .commit_for_version(&Self::key(snap.operation.id), snap.version)?
            .ok_or(Error::InvalidRecord)?;
        if self.store.first_commit_for_key(&key)? != Some(sequence)
            || (pending && operation_sequence != sequence)
            || (!pending && operation_sequence <= sequence)
        {
            return Err(Error::InvalidRecord);
        }
        Ok((ticket, sequence))
    }

    /// Prepare a terminal placement decision for the host's atomic transaction.
    pub(crate) fn placement_batch(
        &self,
        snap: &Snapshot,
        placed: bool,
    ) -> Result<(Vec<Check>, Vec<Mutation>), Error> {
        let mut op = snap.operation.clone();
        let request_id = match op.state {
            State::Completed { request_id, .. } if !placed => request_id,
            State::PlacementPending { request_id, .. } => request_id,
            _ => return Err(Error::State),
        };
        op.state = if placed {
            State::Placed { request_id }
        } else {
            State::Stale { request_id }
        };
        let key = Self::key(op.id);
        Ok((
            vec![Check {
                key: key.clone(),
                expected: Expected::Version(snap.version),
            }],
            vec![Mutation::Put {
                key,
                value: serde_json::to_vec(&op)?,
            }],
        ))
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
            settled: false,
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
    /// Persist a completed generation before settling usage; does not publish it.
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
        if serde_json::to_vec(&generation)?.len() > MAX_PAYLOAD_BYTES {
            return Err(Error::Limit);
        }
        op.state = State::Accounting {
            request_id,
            owner,
            generation: Box::new(generation),
        };
        self.write(&op, Expected::Version(version))
    }
    /// Record the successful provider settlement for a persisted accounting
    /// checkpoint or an unconfirmed attempt. The provider must make settlement
    /// idempotent by request ID: a crash after its acknowledgement but before
    /// this commit requires replay with the same ID and the report derived by
    /// `settlement_report`. The caller must
    /// fence the settlement executor before another executor retries it.
    pub fn record_settlement(&self, id: Uuid, version: u64, request_id: Uuid) -> Result<(), Error> {
        let mut op = self.load(id, version)?;
        let accounting = matches!(&op.state, State::Accounting { request_id: current, .. } if *current == request_id)
            || matches!(&op.state, State::Aborting { accounting: Some(pending), .. } if pending.request_id == request_id);
        let attempt = op
            .attempts
            .iter_mut()
            .find(|a| a.request_id == request_id)
            .ok_or(Error::Identity)?;
        if !attempt.unconfirmed && !accounting && !attempt.settled {
            return Err(Error::State);
        }
        if attempt.settled {
            return Ok(());
        }
        attempt.settled = true;
        self.write(&op, Expected::Version(version))
    }
    /// Publish only after all potentially billable attempts have durable receipts.
    pub fn publish(&self, id: Uuid, version: u64, owner: Uuid) -> Result<(), Error> {
        let mut op = self.load(id, version)?;
        let State::Accounting {
            request_id,
            owner: actual,
            ref generation,
        } = op.state
        else {
            return Err(Error::State);
        };
        if actual != owner {
            return Err(Error::Owner);
        }
        if op
            .attempts
            .iter()
            .any(|a| (a.unconfirmed || a.request_id == request_id) && !a.settled)
        {
            return Err(Error::State);
        }
        let result = op
            .preparation
            .clone()
            .finish((**generation).clone())
            .map_err(|e| Error::InvalidResult(e.to_string()))?;
        op.state = State::Completed {
            request_id,
            result: Box::new(result),
        };
        self.write(&op, Expected::Version(version))
    }
    /// Transfer an accounting checkpoint only after fencing the old owner.
    /// The result and receipt survive unchanged, so settlement can be replayed.
    pub fn recover_accounting(
        &self,
        id: Uuid,
        version: u64,
        old_owner: Uuid,
        new_owner: Uuid,
    ) -> Result<(), Error> {
        if new_owner.is_nil() || new_owner == old_owner {
            return Err(Error::Identity);
        }
        let mut op = self.load(id, version)?;
        let State::Accounting { owner, .. } = &mut op.state else {
            return Err(Error::State);
        };
        if *owner != old_owner {
            return Err(Error::Owner);
        }
        *owner = new_owner;
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
        Self::mark_aborting(&mut op)?;
        self.write(&op, Expected::Version(version))
    }
    fn mark_aborting(op: &mut Operation) -> Result<(), Error> {
        let (owner, accounting) = match op.state {
            State::HookPending | State::Ready => (None, None),
            State::Reserved { owner, .. } => (Some(owner), None),
            State::Running { owner, request_id } => {
                Self::mark_unconfirmed(op, request_id)?;
                (Some(owner), None)
            }
            State::Accounting {
                owner,
                request_id,
                ref generation,
            } => (
                Some(owner),
                Some(PendingAccounting {
                    request_id,
                    generation: generation.clone(),
                }),
            ),
            State::Completed { .. } | State::PlacementPending { .. } => (None, None),
            _ => return Err(Error::State),
        };
        op.state = State::Aborting { owner, accounting };
        Ok(())
    }
    /// Caller must have joined the matching executor before this transition.
    pub fn finish_abort(&self, id: Uuid, version: u64, owner: Option<Uuid>) -> Result<(), Error> {
        let mut op = self.load(id, version)?;
        let State::Aborting {
            owner: actual,
            ref accounting,
        } = op.state
        else {
            return Err(Error::State);
        };
        if owner != actual {
            return Err(Error::Owner);
        }
        if op.attempts.iter().any(|a| {
            (a.unconfirmed
                || accounting
                    .as_ref()
                    .is_some_and(|p| p.request_id == a.request_id))
                && !a.settled
        }) {
            return Err(Error::State);
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
                ref accounting,
            } if actual == owner => {
                if op.attempts.iter().any(|a| {
                    (a.unconfirmed
                        || accounting
                            .as_ref()
                            .is_some_and(|p| p.request_id == a.request_id))
                        && !a.settled
                }) {
                    return Err(Error::State);
                }
                op.state = State::Aborted;
            }
            State::Reserved { .. }
            | State::Running { .. }
            | State::Aborting { owner: Some(_), .. } => {
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
    fn lost_settlement_ack_replays_identical_reports_after_reopen_and_abort() {
        use std::collections::HashMap;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let (id, owner, first, second) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        // Simulate an external service which rejects a changed report for an ID.
        let mut accepted = HashMap::new();
        let mut settle = |request, report: UsageReport| {
            if let Some(previous) = accepted.insert(request, report.clone()) {
                assert_eq!(previous, report);
            }
        };
        {
            let store = Store::open(&path).unwrap();
            let s = SummaryStore::new(&store);
            s.create(id, Uuid::new_v4(), preparation()).unwrap();
            s.reserve(id, snapshot(&s, id).version, owner, first)
                .unwrap();
            s.start(id, snapshot(&s, id).version, owner).unwrap();
            s.recover(id, snapshot(&s, id).version, owner).unwrap();
            settle(
                first,
                snapshot(&s, id).operation.settlement_report(first).unwrap(),
            );
            // Service accepted it; receipt acknowledgement is lost.
        }
        {
            let store = Store::open(&path).unwrap();
            let s = SummaryStore::new(&store);
            settle(
                first,
                snapshot(&s, id).operation.settlement_report(first).unwrap(),
            );
            s.record_settlement(id, snapshot(&s, id).version, first)
                .unwrap();
            s.reserve(id, snapshot(&s, id).version, owner, second)
                .unwrap();
            s.start(id, snapshot(&s, id).version, owner).unwrap();
            let mut completed = generation();
            completed.usage = Some(pi_rs_core::Usage {
                input_tokens: 42,
                output_tokens: 7,
            });
            s.complete(id, snapshot(&s, id).version, owner, completed)
                .unwrap();
            let report = snapshot(&s, id)
                .operation
                .settlement_report(second)
                .unwrap();
            assert!(matches!(report, UsageReport::Measured { .. }));
            settle(second, report);
            s.begin_abort(id, snapshot(&s, id).version).unwrap();
        }
        {
            let store = Store::open(&path).unwrap();
            let s = SummaryStore::new(&store);
            settle(
                second,
                snapshot(&s, id)
                    .operation
                    .settlement_report(second)
                    .unwrap(),
            );
            assert!(matches!(
                s.finish_abort(id, snapshot(&s, id).version, Some(owner)),
                Err(Error::State)
            ));
            s.record_settlement(id, snapshot(&s, id).version, second)
                .unwrap();
            s.finish_abort(id, snapshot(&s, id).version, Some(owner))
                .unwrap();
            assert!(matches!(snapshot(&s, id).operation.state(), State::Aborted));
        }
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
        assert!(matches!(
            s.finish_abort(id, snap.version, Some(owner)),
            Err(Error::State)
        ));
        s.record_settlement(id, snap.version, request).unwrap();
        s.finish_abort(id, snapshot(&s, id).version, Some(owner))
            .unwrap();
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
        s.complete(id, v, owner, generation()).unwrap();
        let snap = snapshot(&s, id);
        assert!(matches!(snap.operation.state(), State::Accounting { .. }));
        assert!(matches!(
            s.publish(id, snap.version, owner),
            Err(Error::State)
        ));
        s.record_settlement(id, snap.version, request).unwrap();
        s.record_settlement(id, snapshot(&s, id).version, request)
            .unwrap();
        s.publish(id, snapshot(&s, id).version, owner).unwrap();
        let snap = snapshot(&s, id);
        assert!(
            matches!(snap.operation.state(), State::Completed { request_id, result } if *request_id == request && result.generation.content == "summary")
        );
        assert!(!snap.operation.attempts()[0].unconfirmed);
    }

    #[test]
    fn abort_during_accounting_retains_generation_until_settled() {
        let store = Store::memory().unwrap();
        let s = SummaryStore::new(&store);
        let (id, owner, request) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        s.create(id, Uuid::new_v4(), preparation()).unwrap();
        s.reserve(id, snapshot(&s, id).version, owner, request)
            .unwrap();
        s.start(id, snapshot(&s, id).version, owner).unwrap();
        s.complete(id, snapshot(&s, id).version, owner, generation())
            .unwrap();
        s.begin_abort(id, snapshot(&s, id).version).unwrap();
        let snap = snapshot(&s, id);
        assert!(
            matches!(snap.operation.state(), State::Aborting { accounting: Some(p), .. } if p.generation.content == "summary")
        );
        assert!(matches!(
            s.finish_abort(id, snap.version, Some(owner)),
            Err(Error::State)
        ));
        s.record_settlement(id, snap.version, request).unwrap();
        s.finish_abort(id, snapshot(&s, id).version, Some(owner))
            .unwrap();
        assert!(matches!(snapshot(&s, id).operation.state(), State::Aborted));
    }

    #[test]
    fn publish_waits_for_old_unconfirmed_receipts() {
        let store = Store::memory().unwrap();
        let s = SummaryStore::new(&store);
        let (id, owner, first, second) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        s.create(id, Uuid::new_v4(), preparation()).unwrap();
        s.reserve(id, snapshot(&s, id).version, owner, first)
            .unwrap();
        assert!(matches!(
            s.record_settlement(id, snapshot(&s, id).version, first),
            Err(Error::State)
        ));
        s.start(id, snapshot(&s, id).version, owner).unwrap();
        s.recover(id, snapshot(&s, id).version, owner).unwrap();
        s.reserve(id, snapshot(&s, id).version, owner, second)
            .unwrap();
        s.start(id, snapshot(&s, id).version, owner).unwrap();
        s.complete(id, snapshot(&s, id).version, owner, generation())
            .unwrap();
        s.record_settlement(id, snapshot(&s, id).version, second)
            .unwrap();
        assert!(matches!(
            s.publish(id, snapshot(&s, id).version, owner),
            Err(Error::State)
        ));
        s.record_settlement(id, snapshot(&s, id).version, first)
            .unwrap();
        s.publish(id, snapshot(&s, id).version, owner).unwrap();
    }

    #[test]
    fn invalid_generation_is_checkpointed_and_accounted_before_rejection() {
        let store = Store::memory().unwrap();
        let s = SummaryStore::new(&store);
        let (id, owner, request) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        s.create(id, Uuid::new_v4(), preparation()).unwrap();
        s.reserve(id, snapshot(&s, id).version, owner, request)
            .unwrap();
        s.start(id, snapshot(&s, id).version, owner).unwrap();
        let mut invalid = generation();
        invalid.content.clear();
        s.complete(id, snapshot(&s, id).version, owner, invalid)
            .unwrap();
        s.record_settlement(id, snapshot(&s, id).version, request)
            .unwrap();
        assert!(matches!(
            s.publish(id, snapshot(&s, id).version, owner),
            Err(Error::InvalidResult(_))
        ));
        assert!(snapshot(&s, id).operation.attempts()[0].settled);
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
            let pending = snapshot(&s, id);
            let request = pending.operation.attempts()[2].request_id;
            assert!(matches!(
                s.recover(id, pending.version, owner),
                Err(Error::State)
            ));
            s.record_settlement(id, pending.version, request).unwrap();
            let earlier = snapshot(&s, id).operation.attempts()[1].request_id;
            s.record_settlement(id, snapshot(&s, id).version, earlier)
                .unwrap();
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
    fn crash_boundary_child() {
        let Ok(path) = std::env::var("PI_SUMMARY_STORE_CRASH_DB") else {
            return;
        };
        let phase = std::env::var("PI_SUMMARY_STORE_CRASH_PHASE").unwrap();
        let ids = std::fs::read_to_string(format!("{path}.ids")).unwrap();
        let mut ids = ids.split_whitespace().map(|s| Uuid::parse_str(s).unwrap());
        let id = ids.next().unwrap();
        let owner = ids.next().unwrap();
        let first = ids.next().unwrap();
        let second = ids.next().unwrap();
        let store = Store::open(&path).unwrap();
        let s = SummaryStore::new(&store);
        s.create(id, Uuid::new_v4(), preparation()).unwrap();
        s.reserve(id, snapshot(&s, id).version, owner, first)
            .unwrap();
        s.start(id, snapshot(&s, id).version, owner).unwrap();
        if phase != "running" {
            // This test simulates a fenced, interrupted first attempt.
            s.recover(id, snapshot(&s, id).version, owner).unwrap();
            if phase == "unconfirmed_provider_ack" {
                std::fs::write(format!("{path}.provider_ack"), first.to_string()).unwrap();
            }
            if phase != "unconfirmed" && phase != "unconfirmed_provider_ack" {
                s.record_settlement(id, snapshot(&s, id).version, first)
                    .unwrap();
                if phase != "unconfirmed_receipt" {
                    s.reserve(id, snapshot(&s, id).version, owner, second)
                        .unwrap();
                    s.start(id, snapshot(&s, id).version, owner).unwrap();
                    s.complete(id, snapshot(&s, id).version, owner, generation())
                        .unwrap();
                    if matches!(phase.as_str(), "aborting" | "abort_receipt" | "aborted") {
                        s.begin_abort(id, snapshot(&s, id).version).unwrap();
                        if phase != "aborting" {
                            s.record_settlement(id, snapshot(&s, id).version, second)
                                .unwrap();
                            if phase == "aborted" {
                                s.finish_abort(id, snapshot(&s, id).version, Some(owner))
                                    .unwrap();
                            }
                        }
                    } else {
                        if phase == "provider_ack" {
                            std::fs::write(format!("{path}.provider_ack"), second.to_string())
                                .unwrap();
                        }
                        if phase != "accounting" && phase != "provider_ack" {
                            s.record_settlement(id, snapshot(&s, id).version, second)
                                .unwrap();
                            if phase == "published" {
                                s.publish(id, snapshot(&s, id).version, owner).unwrap();
                            }
                        }
                    }
                }
            }
        }
        std::fs::write(format!("{path}.ready"), b"ready").unwrap();
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn killed_process_recovers_each_accounting_boundary() {
        use std::time::{Duration, Instant};
        for phase in [
            "running",
            "unconfirmed",
            "unconfirmed_provider_ack",
            "unconfirmed_receipt",
            "accounting",
            "provider_ack",
            "accounting_receipt",
            "published",
            "aborting",
            "abort_receipt",
            "aborted",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let path_text = path.to_str().unwrap();
            let (id, owner, first, second) = (
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
            );
            std::fs::write(
                format!("{path_text}.ids"),
                format!("{id} {owner} {first} {second}"),
            )
            .unwrap();
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "summary_store::tests::crash_boundary_child"])
                .env("PI_SUMMARY_STORE_CRASH_DB", path_text)
                .env("PI_SUMMARY_STORE_CRASH_PHASE", phase)
                .spawn()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(20);
            while !std::path::Path::new(&format!("{path_text}.ready")).exists() {
                if Instant::now() >= deadline || child.try_wait().unwrap().is_some() {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("child failed at {phase}");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            child.kill().unwrap();
            child.wait().unwrap();
            if phase == "provider_ack" || phase == "unconfirmed_provider_ack" {
                let expected = if phase == "provider_ack" {
                    second
                } else {
                    first
                };
                assert_eq!(
                    std::fs::read_to_string(format!("{path_text}.provider_ack")).unwrap(),
                    expected.to_string()
                );
            }
            let store = Store::open(&path).unwrap();
            let s = SummaryStore::new(&store);
            if phase == "running" {
                s.recover(id, snapshot(&s, id).version, owner).unwrap();
            }
            let snap = snapshot(&s, id);
            assert!(snap.operation.attempts()[0].unconfirmed, "{phase}");
            if !snap.operation.attempts()[0].settled {
                s.record_settlement(id, snap.version, first).unwrap();
            }
            let snap = snapshot(&s, id);
            assert!(snap.operation.attempts()[0].settled);
            if matches!(snap.operation.state(), State::Ready) {
                s.reserve(id, snap.version, owner, second).unwrap();
                s.start(id, snapshot(&s, id).version, owner).unwrap();
                s.complete(id, snapshot(&s, id).version, owner, generation())
                    .unwrap();
            }
            let snap = snapshot(&s, id);
            if matches!(
                snap.operation.state(),
                State::Aborting { .. } | State::Aborted
            ) {
                if !snap.operation.attempts()[1].settled {
                    s.record_settlement(id, snap.version, second).unwrap();
                }
                if matches!(snapshot(&s, id).operation.state(), State::Aborting { .. }) {
                    s.recover(id, snapshot(&s, id).version, owner).unwrap();
                }
                assert!(matches!(snapshot(&s, id).operation.state(), State::Aborted));
                continue;
            }
            if matches!(snap.operation.state(), State::Accounting { .. }) {
                let new_owner = Uuid::new_v4();
                s.recover_accounting(id, snap.version, owner, new_owner)
                    .unwrap();
                let snap = snapshot(&s, id);
                if !snap.operation.attempts()[1].settled {
                    assert!(matches!(
                        s.publish(id, snap.version, new_owner),
                        Err(Error::State)
                    ));
                    s.record_settlement(id, snap.version, second).unwrap();
                }
                s.publish(id, snapshot(&s, id).version, new_owner).unwrap();
            }
            let snap = snapshot(&s, id);
            assert!(
                matches!(snap.operation.state(), State::Completed { request_id, .. } if *request_id == second)
            );
            assert!(snap.operation.attempts().iter().all(|a| a.settled));
            let version = snap.version;
            s.record_settlement(id, version, second).unwrap();
            assert_eq!(snapshot(&s, id).version, version);
        }
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
