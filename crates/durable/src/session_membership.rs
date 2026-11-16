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
        let state: crate::runtime::Conversation = serde_json::from_slice(&entry.value)?;
        if id.is_nil() || state.id != id || state.abort_requested {
            return Err(Error::InvalidConversation);
        }
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
