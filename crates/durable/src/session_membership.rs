//! Host-managed, opt-in conversation membership and checked authorization.
//!
//! Access to this API and its underlying store is a host privilege. Session IDs
//! are identifiers, not credentials; never expose this API directly to tools.
use durable_store::{Check, Expected, Mutation, Store};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("session or membership is missing, invalid, or does not match")]
    Unauthorized,
    #[error("conversation is missing, invalid, or aborting")]
    InvalidConversation,
    #[error("authorized host batch touches a protected key")]
    ProtectedKey,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionRecord {
    version: u8,
    id: Uuid,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Membership {
    version: u8,
    session: Uuid,
    conversation: Uuid,
}

// Conversation deserialization intentionally defaults fields for old runtime
// snapshots. Authorization cannot infer the absence of an abort mark from an
// omitted field, so require the relevant evidence independently.
#[derive(Deserialize)]
struct AuthorizationEvidence {
    id: Uuid,
    abort_requested: bool,
    config: crate::runtime::Config,
}

pub(crate) fn validate_conversation(id: Uuid, bytes: &[u8]) -> Result<(), Error> {
    let evidence: AuthorizationEvidence =
        serde_json::from_slice(bytes).map_err(|_| Error::InvalidConversation)?;
    let state: crate::runtime::Conversation =
        serde_json::from_slice(bytes).map_err(|_| Error::InvalidConversation)?;
    if id.is_nil()
        || evidence.id != id
        || evidence.abort_requested
        || state.id != id
        || state.abort_requested
        || crate::runtime::validate_config(&evidence.config).is_err()
    {
        return Err(Error::InvalidConversation);
    }
    Ok(())
}

fn session_key(id: Uuid) -> Vec<u8> {
    format!("durable/session-membership/v1/session/{id}").into_bytes()
}
fn member_key(id: Uuid) -> Vec<u8> {
    format!("durable/session-membership/v1/conversation/{id}").into_bytes()
}
fn checked(key: Vec<u8>, version: u64) -> Check {
    Check {
        key,
        expected: Expected::Version(version),
    }
}

/// Resolve authority from the owner's membership, never from a caller-supplied
/// session identifier. Every record used here is retained as a commit check.
pub(crate) fn task_pair(
    store: &Store,
    owner: Uuid,
    target: Uuid,
) -> Result<Vec<Check>, crate::document::Error> {
    let key = member_key(owner);
    let entry = store.get(&key)?.ok_or_else(|| {
        crate::document::Error::InvalidSessionAdmission("owner is not session scoped".into())
    })?;
    let member: Membership = serde_json::from_slice(&entry.value)?;
    if member.version != 1 || member.conversation != owner || member.session.is_nil() {
        return Err(crate::document::Error::InvalidSessionAdmission(
            "invalid owner session membership".into(),
        ));
    }
    let directory = SessionMembership::new(store);
    let proof = match directory.authorize_pair(member.session, owner, target) {
        Ok(proof) => proof,
        Err(error @ Error::Store(_)) | Err(error @ Error::Json(_)) => {
            return Err(match error {
                Error::Store(error) => crate::document::Error::Store(error),
                Error::Json(error) => crate::document::Error::Json(error),
                _ => unreachable!(),
            });
        }
        Err(_) => {
            if store.get(&key)?.map(|current| current.version) != Some(entry.version) {
                return Err(durable_store::Error::Conflict { key }.into());
            }
            return Err(crate::document::Error::InvalidSessionAdmission(
                "conversations are not active members of the same session".into(),
            ));
        }
    };
    let observed_owner = proof.checks.iter().find(|check| check.key == key);
    if observed_owner != Some(&checked(key.clone(), entry.version)) {
        return Err(durable_store::Error::Conflict { key }.into());
    }
    // The admission captures and checks its own target snapshot. Retaining a
    // second independently read version would turn a race into InvalidBatch
    // rather than the scheduler's normal replay-safe conflict.
    let target_key = crate::runtime::Harness::key(target);
    Ok(proof
        .checks
        .into_iter()
        .filter(|check| check.key != target_key)
        .collect())
}

/// Host authority over membership in a single store. Creating this object is
/// not authorization for an untrusted caller: access to the store is privileged.
pub struct SessionMembership<'a> {
    store: &'a Store,
}
impl<'a> SessionMembership<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Create a new durable, initially empty session identity.
    pub fn create(&self) -> Result<Uuid, Error> {
        let id = Uuid::new_v4();
        let key = session_key(id);
        let value = serde_json::to_vec(&SessionRecord { version: 1, id })?;
        self.store.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Missing,
            }],
            &[Mutation::Put { key, value }],
        )?;
        Ok(id)
    }

    fn session(&self, id: Uuid) -> Result<Check, Error> {
        let key = session_key(id);
        let entry = self.store.get(&key)?.ok_or(Error::Unauthorized)?;
        let record: SessionRecord = serde_json::from_slice(&entry.value)?;
        if id.is_nil() || record.version != 1 || record.id != id {
            return Err(Error::Unauthorized);
        }
        Ok(checked(key, entry.version))
    }
    fn conversation(&self, id: Uuid) -> Result<Check, Error> {
        let key = crate::runtime::Harness::key(id);
        let entry = self.store.get(&key)?.ok_or(Error::InvalidConversation)?;
        validate_conversation(id, &entry.value)?;
        Ok(checked(key, entry.version))
    }
    fn member(&self, session: Uuid, conversation: Uuid) -> Result<Check, Error> {
        let key = member_key(conversation);
        let entry = self.store.get(&key)?.ok_or(Error::Unauthorized)?;
        let record: Membership = serde_json::from_slice(&entry.value)?;
        if record.version != 1 || record.session != session || record.conversation != conversation {
            return Err(Error::Unauthorized);
        }
        Ok(checked(key, entry.version))
    }

    /// Explicitly enroll an unscoped, non-aborting conversation. Existing
    /// membership cannot be overwritten; remove it first to move sessions.
    pub fn enroll(&self, session: Uuid, conversation: Uuid) -> Result<(), Error> {
        let boundary = self.store.put_version()?;
        let key = member_key(conversation);
        let checks = [
            self.session(session)?,
            self.conversation(conversation)?,
            Check {
                key: key.clone(),
                expected: Expected::MissingUnchangedSince(boundary),
            },
        ];
        let value = serde_json::to_vec(&Membership {
            version: 1,
            session,
            conversation,
        })?;
        self.store
            .commit(&checks, &[Mutation::Put { key, value }])?;
        Ok(())
    }

    /// Revoke membership even if the conversation is aborting or missing.
    pub fn revoke(&self, session: Uuid, conversation: Uuid) -> Result<(), Error> {
        let checks = [self.session(session)?, self.member(session, conversation)?];
        self.store.commit(
            &checks,
            &[Mutation::Delete {
                key: member_key(conversation),
            }],
        )?;
        Ok(())
    }

    /// Prepare a host-only authorization for two active conversations. The
    /// returned proof must be committed with the host's effects; it is not a
    /// durable capability and does not authorize task-side submission.
    pub fn authorize_pair(
        &self,
        session: Uuid,
        first: Uuid,
        second: Uuid,
    ) -> Result<AuthorizedPair<'a>, Error> {
        let mut checks = vec![
            self.session(session)?,
            self.member(session, first)?,
            self.conversation(first)?,
        ];
        if first != second {
            checks.push(self.member(session, second)?);
            checks.push(self.conversation(second)?);
        }
        Ok(AuthorizedPair {
            store: self.store,
            checks,
        })
    }
}

/// One-shot optimistic proof, bound to this store and the observed versions.
pub struct AuthorizedPair<'a> {
    store: &'a Store,
    checks: Vec<Check>,
}
impl AuthorizedPair<'_> {
    /// Atomically commit host-owned effects if both memberships and both
    /// conversation snapshots still match. Effects are restricted to the
    /// host-owned `host/` namespace and cannot change authority records.
    pub fn commit(self, checks: &[Check], mutations: &[Mutation]) -> Result<(), Error> {
        for mutation in mutations {
            let key = match mutation {
                Mutation::Put { key, .. } | Mutation::Delete { key } => key,
            };
            if !key.starts_with(b"host/") {
                return Err(Error::ProtectedKey);
            }
        }
        let mut all = self.checks;
        all.extend_from_slice(checks);
        self.store.commit(&all, mutations)?;
        Ok(())
    }
}
