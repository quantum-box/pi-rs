//! Whole current values for documents shared by members of one session.

use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::Duration,
};

use durable_store::{Check, Expected, Mutation, Store, owner::OwnedStore};
use pi_rs_core::CancellationToken;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use uuid::Uuid;

use super::{DefinedDocumentValue, Error, StoredDocumentDefinition, defined_document_binding};
use crate::task_store::OwnedBatch;

/// Definition of a current-only document shared by one Durable session.
/// The family key is supplied when accessing it in a session transaction.
pub struct SessionDocument<T> {
    pub kind: String,
    pub version: std::num::NonZeroU32,
    pub initial: T,
}

/// Fork behavior for a current-only conversation document.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LatestDocumentFork {
    /// Start the child without an instance at this address.
    Initial,
    /// Copy the parent's committed current value into a new incarnation.
    Current,
}

/// Definition of a conversation document with no rewindable payload history.
pub struct LatestConversationDocument<T> {
    pub kind: String,
    pub version: std::num::NonZeroU32,
    pub initial: T,
    pub fork: LatestDocumentFork,
}

pub(crate) struct CurrentDefinition<'a, T> {
    kind: &'a str,
    version: std::num::NonZeroU32,
    initial: &'a T,
    fork: Option<LatestDocumentFork>,
}

impl<'a, T> From<&'a SessionDocument<T>> for CurrentDefinition<'a, T> {
    fn from(definition: &'a SessionDocument<T>) -> Self {
        Self {
            kind: &definition.kind,
            version: definition.version,
            initial: &definition.initial,
            fork: None,
        }
    }
}
impl<'a, T> From<&'a LatestConversationDocument<T>> for CurrentDefinition<'a, T> {
    fn from(definition: &'a LatestConversationDocument<T>) -> Self {
        Self {
            kind: &definition.kind,
            version: definition.version,
            initial: &definition.initial,
            fork: Some(definition.fork),
        }
    }
}

const CONVERSATION_PREFIX: &[u8] = b"durable/conversation-current-document/v1/";

fn conversation_prefix(id: Uuid) -> Vec<u8> {
    [CONVERSATION_PREFIX, format!("{id}/").as_bytes()].concat()
}

fn conversation_key(id: Uuid, kind: &str, key: &str) -> Result<Vec<u8>, Error> {
    let mut encoded = conversation_prefix(id);
    encoded.extend(serde_json::to_vec(&(kind, key))?);
    Ok(encoded)
}

fn conversation_marker(id: Uuid) -> Vec<u8> {
    format!("durable/conversation-current-document-marker/v1/{id}").into_bytes()
}

pub(crate) fn append_latest_fork(
    store: &Store,
    parent: Uuid,
    child: Uuid,
    batch: &mut OwnedBatch,
) -> Result<(), Error> {
    let marker = conversation_marker(parent);
    let boundary = store.put_version()?;
    let version = store.get_bounded(&marker, 1)?.map(|entry| entry.version);
    batch.checks.push(Check {
        key: marker,
        expected: version.map_or(Expected::MissingUnchangedSince(boundary), Expected::Version),
    });
    let prefix = conversation_prefix(parent);
    for (key, entry) in fork_rows(store, &prefix)? {
        let (kind, family): (String, String) = serde_json::from_slice(&key[prefix.len()..])?;
        if conversation_key(parent, &kind, &family)? != key {
            return Err(Error::InvalidDocumentDefinition);
        }
        let mut record: StoredDocument = serde_json::from_slice(&entry.value)?;
        if record.fork.is_none()
            || record.value.is_some() != record.definition.incarnation.is_some()
            || record
                .definition
                .incarnation
                .as_ref()
                .is_some_and(Uuid::is_nil)
        {
            return Err(Error::InvalidDocumentDefinition);
        }
        if record.fork != Some(LatestDocumentFork::Current) || record.value.is_none() {
            continue;
        }
        record.definition.incarnation = Some(Uuid::new_v4());
        let key = conversation_key(child, &kind, &family)?;
        batch.checks.push(Check {
            key: key.clone(),
            expected: Expected::Missing,
        });
        batch.mutations.push(Mutation::Put {
            key,
            value: serde_json::to_vec(&record)?,
        });
    }
    Ok(())
}

fn fork_rows(store: &Store, prefix: &[u8]) -> Result<Vec<durable_store::KeyedEntry>, Error> {
    if !crate::transcript::bounded_fork_reads() {
        return Ok(store.list(prefix)?);
    }
    let mut result = Vec::new();
    let mut remaining = crate::transcript::DISPLAY_FORK_RECORD_LIMIT;
    let mut after = prefix.to_vec();
    loop {
        let mut page = store.prefix_page_bounded(
            prefix,
            &after,
            1,
            crate::transcript::DISPLAY_FORK_RECORD_LIMIT,
            remaining.max(1),
        )?;
        let Some((key, entry)) = page.pop() else {
            return Ok(result);
        };
        remaining = remaining
            .checked_sub(key.len().saturating_add(entry.value.len()))
            .ok_or(durable_store::Error::DocumentFrameTooLarge)?;
        after = key.clone();
        result.push((key, entry));
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredDocument {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fork: Option<LatestDocumentFork>,
    definition: StoredDocumentDefinition,
    value: Option<Map<String, Value>>,
}

impl StoredDocument {
    fn validate(&self, binding: &StoredDocumentDefinition) -> Result<(), Error> {
        if self.definition.schema != binding.schema
            || self.definition.initial_digest != binding.initial_digest
            || self.value.is_some() != self.definition.incarnation.is_some()
            || self
                .definition
                .incarnation
                .as_ref()
                .is_some_and(Uuid::is_nil)
        {
            return Err(Error::InvalidDocumentDefinition);
        }
        Ok(())
    }

    fn typed<T: DeserializeOwned + Serialize>(
        &self,
    ) -> Result<Option<DefinedDocumentValue<T>>, Error> {
        let Some(value) = &self.value else {
            return Ok(None);
        };
        let value = Value::Object(value.clone());
        let typed = serde::Deserialize::deserialize(&value)?;
        if serde_json::to_value(&typed)? != value {
            return Err(Error::InvalidDocumentDefinition);
        }
        Ok(Some(DefinedDocumentValue {
            incarnation: self
                .definition
                .incarnation
                .ok_or(Error::InvalidDocumentDefinition)?,
            value: typed,
        }))
    }
}

fn storage_key(session: Uuid, kind: &str, key: &str) -> Result<Vec<u8>, Error> {
    let mut storage_key = b"durable/session-current-document/v1/".to_vec();
    storage_key.extend(serde_json::to_vec(&(session, kind, key))?);
    Ok(storage_key)
}

struct Staged {
    expected: Expected,
    original: Option<StoredDocument>,
    current: Option<StoredDocument>,
}

impl Staged {
    fn read<T: DeserializeOwned + Serialize>(
        &self,
    ) -> Result<Option<DefinedDocumentValue<T>>, Error> {
        self.current
            .as_ref()
            .map(StoredDocument::typed)
            .transpose()
            .map(Option::flatten)
    }
}

fn check_history_mode(
    store: &Store,
    conversation: Uuid,
    kind: &str,
    key: &str,
) -> Result<[Check; 2], Error> {
    let address = super::Address {
        scope: conversation.to_string(),
        name: kind.into(),
        key: key.into(),
    };
    let revision = store.document_snapshot()?;
    let document_key = address.storage_key()?;
    let policy_key = address.policy_key()?;
    if store
        .document_last_revision(&document_key, revision)?
        .is_some()
        || store.document_at(&policy_key, revision)?.is_some()
    {
        return Err(Error::InvalidDocumentDefinition);
    }
    Ok([document_key, policy_key].map(|key| Check {
        key,
        expected: Expected::DocumentUnchangedSince(revision),
    }))
}

pub(crate) struct CurrentDocuments<'a> {
    store: &'a Store,
    owner: Uuid,
    scope: Option<(Uuid, [Check; 2])>,
    staged: HashMap<Vec<u8>, Staged>,
    other_history: HashMap<Vec<u8>, Expected>,
}

impl<'a> CurrentDocuments<'a> {
    pub(crate) fn new(store: &'a Store, owner: Uuid) -> Self {
        Self {
            store,
            owner,
            scope: None,
            staged: HashMap::new(),
            other_history: HashMap::new(),
        }
    }

    pub(crate) fn guard_rewindable(&mut self, address: &super::Address) -> Result<(), Error> {
        let Ok(conversation) = address.scope.parse::<Uuid>() else {
            return Ok(());
        };
        if conversation.to_string() != address.scope {
            return Ok(());
        }
        let key = conversation_key(conversation, &address.name, &address.key)?;
        let boundary = self.store.put_version()?;
        if self
            .staged
            .get(&key)
            .is_some_and(|row| row.current.is_some())
        {
            return Err(Error::InvalidDocumentDefinition);
        }
        if self.store.get(&key)?.is_some() {
            return Err(Error::InvalidDocumentDefinition);
        }
        self.other_history
            .entry(key)
            .or_insert(Expected::MissingUnchangedSince(boundary));
        Ok(())
    }

    fn document<T: Serialize>(
        &mut self,
        definition: CurrentDefinition<'_, T>,
        key: &str,
    ) -> Result<(&mut Staged, StoredDocumentDefinition), Error> {
        let (binding, _) =
            defined_document_binding(definition.kind, definition.version, definition.initial)?;
        let storage_key = if definition.fork.is_some() {
            for Check { key, expected } in
                check_history_mode(self.store, self.owner, definition.kind, key)?
            {
                self.other_history.entry(key).or_insert(expected);
            }
            conversation_key(self.owner, definition.kind, key)?
        } else {
            if self.scope.is_none() {
                self.scope = Some(crate::session_membership::document_scope(
                    self.store, self.owner,
                )?);
            }
            let (session, _) = self.scope.as_ref().expect("resolved scope");
            storage_key(*session, definition.kind, key)?
        };
        let staged = match self.staged.entry(storage_key) {
            std::collections::hash_map::Entry::Occupied(staged) => staged.into_mut(),
            std::collections::hash_map::Entry::Vacant(slot) => {
                let boundary = self.store.put_version()?;
                let entry = self.store.get(slot.key())?;
                let original: Option<StoredDocument> = entry
                    .as_ref()
                    .map(|entry| serde_json::from_slice(&entry.value))
                    .transpose()?;
                slot.insert(Staged {
                    expected: entry.map_or(Expected::MissingUnchangedSince(boundary), |entry| {
                        Expected::Version(entry.version)
                    }),
                    current: original.clone(),
                    original,
                })
            }
        };
        if let Some(record) = &staged.current {
            record.validate(&binding)?;
            if record.fork != definition.fork {
                return Err(Error::InvalidDocumentDefinition);
            }
        }
        Ok((staged, binding))
    }

    pub(crate) fn read<T: DeserializeOwned + Serialize>(
        &mut self,
        definition: CurrentDefinition<'_, T>,
        key: &str,
    ) -> Result<Option<DefinedDocumentValue<T>>, Error> {
        self.document(definition, key)?.0.read()
    }

    pub(crate) fn edit<T: Clone + DeserializeOwned + Serialize, R>(
        &mut self,
        definition: CurrentDefinition<'_, T>,
        key: &str,
        edit: impl FnOnce(&mut T) -> Result<R, Error>,
    ) -> Result<R, Error> {
        let fork = definition.fork;
        let initial = definition.initial;
        let (staged, mut binding) = self.document(definition, key)?;
        let current: Option<DefinedDocumentValue<T>> = staged.read()?;
        let (id, mut value) = current
            .map(|current| (current.incarnation, current.value))
            .unwrap_or_else(|| (Uuid::new_v4(), initial.clone()));
        let result = edit(&mut value)?;
        let Value::Object(value) = serde_json::to_value(value)? else {
            return Err(Error::InvalidDocumentDefinition);
        };
        binding.incarnation = Some(id);
        staged.current = Some(StoredDocument {
            fork,
            definition: binding,
            value: Some(value),
        });
        Ok(result)
    }

    pub(crate) fn retire<T: Serialize>(
        &mut self,
        definition: CurrentDefinition<'_, T>,
        key: &str,
    ) -> Result<bool, Error> {
        let (staged, _) = self.document(definition, key)?;
        let Some(record) = staged.current.as_mut() else {
            return Ok(false);
        };
        record.definition.incarnation = None;
        Ok(record.value.take().is_some())
    }

    pub(crate) fn migrate<From: DeserializeOwned + Serialize, To: Serialize>(
        &mut self,
        old: CurrentDefinition<'_, From>,
        new: CurrentDefinition<'_, To>,
        key: &str,
        migrate: impl FnOnce(From) -> Result<To, Error>,
    ) -> Result<(), Error> {
        if old.kind != new.kind || old.version >= new.version || old.fork != new.fork {
            return Err(Error::InvalidDocumentDefinition);
        }
        let (mut binding, _) = defined_document_binding(new.kind, new.version, new.initial)?;
        let (staged, _) = self.document(old, key)?;
        if staged.current.is_none() {
            return Err(Error::InvalidDocumentDefinition);
        }
        let value = if let Some(current) = staged.read()? {
            let Value::Object(value) = serde_json::to_value(migrate(current.value)?)? else {
                return Err(Error::InvalidDocumentDefinition);
            };
            binding.incarnation = Some(current.incarnation);
            Some(value)
        } else {
            None
        };
        staged.current = Some(StoredDocument {
            fork: new.fork,
            definition: binding,
            value,
        });
        Ok(())
    }

    pub(crate) fn append_to(self, batch: &mut OwnedBatch) -> Result<bool, Error> {
        if let Some((_, checks)) = self.scope {
            batch.checks.extend(checks);
        }
        let mut changed = false;
        let mut latest_changed = false;
        for (key, staged) in self.staged {
            batch.checks.push(Check {
                key: key.clone(),
                expected: staged.expected,
            });
            if staged.current != staged.original {
                let record = staged.current.ok_or(Error::InvalidDocumentDefinition)?;
                latest_changed |= record.fork.is_some();
                batch.mutations.push(Mutation::Put {
                    key,
                    value: serde_json::to_vec(&record)?,
                });
                changed = true;
            }
        }
        for (key, expected) in self.other_history {
            if !batch.checks.iter().any(|check| check.key == key) {
                batch.checks.push(Check { key, expected });
            }
        }
        if latest_changed {
            let key = conversation_marker(self.owner);
            let boundary = self.store.put_version()?;
            let version = self.store.get_bounded(&key, 1)?.map(|entry| entry.version);
            batch.checks.push(Check {
                key: key.clone(),
                expected: version
                    .map_or(Expected::MissingUnchangedSince(boundary), Expected::Version),
            });
            batch.mutations.push(Mutation::Put {
                key,
                value: vec![1],
            });
        }
        Ok(changed)
    }
}

/// The latest committed document and whether the previously observed
/// incarnation retired. A retirement without replacement carries the last value.
pub struct CurrentDocumentUpdate<T> {
    pub value: Arc<DefinedDocumentValue<T>>,
    pub retired: bool,
    /// Root replacement for a present value, empty for retirement alone.
    pub operations: Vec<Value>,
}

/// Pull watch over a current-only document. It coalesces intermediate
/// commits and holds no reference that can keep the store open.
pub struct CurrentDocumentWatch<T> {
    owner: Weak<OwnedStore>,
    source_closed: CancellationToken,
    cancelled: CancellationToken,
    conversation: Uuid,
    membership: Option<[Check; 2]>,
    fork: Option<LatestDocumentFork>,
    key: Vec<u8>,
    binding: StoredDocumentDefinition,
    interval: Duration,
    wake: durable_store::CommitUpdates,
    version: u64,
    value: Arc<DefinedDocumentValue<T>>,
    present: bool,
    end: Option<crate::runtime::ViewWatchEnd>,
}

impl<T: DeserializeOwned + Serialize> CurrentDocumentWatch<T> {
    pub(crate) fn attach(
        owner: &Arc<OwnedStore>,
        source_closed: CancellationToken,
        conversation: Uuid,
        definition: CurrentDefinition<'_, T>,
        key: &str,
        interval: Duration,
        cancelled: CancellationToken,
    ) -> Result<Option<Self>, Error> {
        if interval.is_zero() {
            return Err(Error::InvalidSessionAdmission(
                "document watch interval must be nonzero".into(),
            ));
        }
        let store = owner.store();
        let wake = store.subscribe_commits()?;
        let (binding, _) =
            defined_document_binding(definition.kind, definition.version, definition.initial)?;
        for _ in 0..128 {
            if cancelled.is_cancelled() || source_closed.is_cancelled() {
                return Ok(None);
            }
            let before = store.commit_sequence()?;
            let (key, membership) = if definition.fork.is_some() {
                check_history_mode(store, conversation, definition.kind, key)?;
                (conversation_key(conversation, definition.kind, key)?, None)
            } else {
                let (session, scope) =
                    crate::session_membership::document_scope(store, conversation)?;
                (storage_key(session, definition.kind, key)?, Some(scope))
            };
            let entry = store.get(&key)?;
            if store.commit_sequence()? != before {
                continue;
            }
            let Some(entry) = entry else { return Ok(None) };
            let record: StoredDocument = serde_json::from_slice(&entry.value)?;
            record.validate(&binding)?;
            if record.fork != definition.fork {
                return Err(Error::InvalidDocumentDefinition);
            }
            let Some(value) = record.typed()? else {
                return Ok(None);
            };
            return Ok(Some(Self {
                owner: Arc::downgrade(owner),
                source_closed,
                cancelled,
                conversation,
                membership,
                fork: definition.fork,
                key,
                binding,
                interval,
                wake,
                version: entry.version,
                value: Arc::new(value),
                present: true,
                end: None,
            }));
        }
        Err(Error::InvalidSessionAdmission(
            "could not capture a stable current document snapshot".into(),
        ))
    }

    /// The acquisition value, or the last value delivered by `next`.
    pub fn value(&self) -> &Arc<DefinedDocumentValue<T>> {
        &self.value
    }

    pub fn end_reason(&self) -> Option<&crate::runtime::ViewWatchEnd> {
        self.end.as_ref()
    }

    pub fn stop(&mut self) {
        self.end
            .get_or_insert(crate::runtime::ViewWatchEnd::Stopped);
    }

    fn latest(&self, store: &Store) -> Result<Option<durable_store::Entry>, Error> {
        let before = store.commit_sequence()?;
        let entry = store.get(&self.key)?;
        if let Some(membership) = &self.membership {
            let (_, checks) = crate::session_membership::document_scope(store, self.conversation)?;
            if &checks != membership {
                return Err(Error::InvalidSessionAdmission(
                    "session document watch membership changed".into(),
                ));
            }
        }
        if store.commit_sequence()? != before {
            return Ok(None);
        }
        Ok(Some(entry.ok_or(Error::InvalidDocumentDefinition)?))
    }

    fn update(
        &mut self,
        entry: durable_store::Entry,
    ) -> Result<Option<CurrentDocumentUpdate<T>>, Error> {
        if entry.version == self.version {
            return Ok(None);
        }
        let record: StoredDocument = serde_json::from_slice(&entry.value)?;
        record.validate(&self.binding)?;
        if record.fork != self.fork {
            return Err(Error::InvalidDocumentDefinition);
        }
        let value: Option<DefinedDocumentValue<T>> = record.typed()?;
        let retired = self.present
            && value
                .as_ref()
                .is_none_or(|value| value.incarnation != self.value.incarnation);
        self.present = value.is_some();
        self.version = entry.version;
        if let Some(value) = value {
            self.value = Arc::new(value);
        } else if !retired {
            return Ok(None);
        }
        let operations = record.value.map_or_else(Vec::new, |value| {
            crate::chord_ops::replacement(&Value::Object(value))
        });
        Ok(Some(CurrentDocumentUpdate {
            value: self.value.clone(),
            retired,
            operations,
        }))
    }

    /// Wait for a new value or retirement. Dropping the pending read leaves
    /// the watch open. Revoking the acquisition membership ends the watch.
    pub async fn next(&mut self) -> Result<Option<CurrentDocumentUpdate<T>>, Error> {
        use crate::runtime::ViewWatchEnd;

        while self.end.is_none() {
            if self.cancelled.is_cancelled() {
                self.end = Some(ViewWatchEnd::Aborted);
                break;
            }
            if self.source_closed.is_cancelled() {
                self.end = Some(ViewWatchEnd::Ended);
                break;
            }
            let Some(owner) = self.owner.upgrade() else {
                self.end = Some(ViewWatchEnd::Ended);
                break;
            };
            let entry = self.latest(owner.store());
            drop(owner);
            let result = match entry {
                Ok(Some(entry))
                    if !self.source_closed.is_cancelled() && !self.cancelled.is_cancelled() =>
                {
                    self.update(entry)
                }
                Ok(Some(_)) => continue,
                Ok(None) => Ok(None),
                Err(Error::InvalidSessionAdmission(_)) => {
                    self.end = Some(ViewWatchEnd::Ended);
                    break;
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(Some(update)) => return Ok(Some(update)),
                Ok(None) => (),
                Err(error) => {
                    self.end = Some(ViewWatchEnd::Failed(error.to_string()));
                    return Err(error);
                }
            }
            tokio::select! {
                _ = self.cancelled.cancelled() => {}
                _ = self.source_closed.cancelled() => {}
                _ = self.wake.changed() => {}
                _ = tokio::time::sleep(self.interval) => {}
            }
        }
        Ok(None)
    }
}

/// A current document watch opened through session membership.
pub type SessionDocumentWatch<T> = CurrentDocumentWatch<T>;

/// A value or retirement delivered by a session document watch.
pub type SessionDocumentUpdate<T> = CurrentDocumentUpdate<T>;
