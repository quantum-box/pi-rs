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

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredDocument {
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

pub(crate) struct SessionDocuments<'a> {
    store: &'a Store,
    owner: Uuid,
    scope: Option<(Uuid, [Check; 2])>,
    staged: HashMap<Vec<u8>, Staged>,
}

impl<'a> SessionDocuments<'a> {
    pub(crate) fn new(store: &'a Store, owner: Uuid) -> Self {
        Self {
            store,
            owner,
            scope: None,
            staged: HashMap::new(),
        }
    }

    fn document<T: Serialize>(
        &mut self,
        definition: &SessionDocument<T>,
        key: &str,
    ) -> Result<(&mut Staged, StoredDocumentDefinition), Error> {
        let (binding, _) =
            defined_document_binding(&definition.kind, definition.version, &definition.initial)?;
        if self.scope.is_none() {
            self.scope = Some(crate::session_membership::document_scope(
                self.store, self.owner,
            )?);
        }
        let (session, _) = self.scope.as_ref().expect("resolved scope");
        let storage_key = storage_key(*session, &definition.kind, key)?;
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
        }
        Ok((staged, binding))
    }

    pub(crate) fn read<T: DeserializeOwned + Serialize>(
        &mut self,
        definition: &SessionDocument<T>,
        key: &str,
    ) -> Result<Option<DefinedDocumentValue<T>>, Error> {
        self.document(definition, key)?.0.read()
    }

    pub(crate) fn edit<T: Clone + DeserializeOwned + Serialize, R>(
        &mut self,
        definition: &SessionDocument<T>,
        key: &str,
        edit: impl FnOnce(&mut T) -> Result<R, Error>,
    ) -> Result<R, Error> {
        let (staged, mut binding) = self.document(definition, key)?;
        let current: Option<DefinedDocumentValue<T>> = staged.read()?;
        let (id, mut value) = current
            .map(|current| (current.incarnation, current.value))
            .unwrap_or_else(|| (Uuid::new_v4(), definition.initial.clone()));
        let result = edit(&mut value)?;
        let Value::Object(value) = serde_json::to_value(value)? else {
            return Err(Error::InvalidDocumentDefinition);
        };
        binding.incarnation = Some(id);
        staged.current = Some(StoredDocument {
            definition: binding,
            value: Some(value),
        });
        Ok(result)
    }

    pub(crate) fn retire<T: Serialize>(
        &mut self,
        definition: &SessionDocument<T>,
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
        old: &SessionDocument<From>,
        new: &SessionDocument<To>,
        key: &str,
        migrate: impl FnOnce(From) -> Result<To, Error>,
    ) -> Result<(), Error> {
        if old.kind != new.kind || old.version >= new.version {
            return Err(Error::InvalidDocumentDefinition);
        }
        let (mut binding, _) = defined_document_binding(&new.kind, new.version, &new.initial)?;
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
        for (key, staged) in self.staged {
            batch.checks.push(Check {
                key: key.clone(),
                expected: staged.expected,
            });
            if staged.current != staged.original {
                let record = staged.current.ok_or(Error::InvalidDocumentDefinition)?;
                batch.mutations.push(Mutation::Put {
                    key,
                    value: serde_json::to_vec(&record)?,
                });
                changed = true;
            }
        }
        Ok(changed)
    }
}

/// The latest committed session document and whether the previously observed
/// incarnation retired. A retirement without replacement carries the last value.
pub struct SessionDocumentUpdate<T> {
    pub value: Arc<DefinedDocumentValue<T>>,
    pub retired: bool,
}

/// Pull watch over the current document at a session address. It coalesces
/// commits and holds no reference that can keep the session's store open.
pub struct SessionDocumentWatch<T> {
    owner: Weak<OwnedStore>,
    source_closed: CancellationToken,
    cancelled: CancellationToken,
    conversation: Uuid,
    scope: [Check; 2],
    key: Vec<u8>,
    binding: StoredDocumentDefinition,
    interval: Duration,
    version: u64,
    value: Arc<DefinedDocumentValue<T>>,
    present: bool,
    end: Option<crate::runtime::ViewWatchEnd>,
}

impl<T: DeserializeOwned + Serialize> SessionDocumentWatch<T> {
    pub(crate) fn attach(
        owner: &Arc<OwnedStore>,
        source_closed: CancellationToken,
        conversation: Uuid,
        definition: &SessionDocument<T>,
        key: &str,
        interval: Duration,
        cancelled: CancellationToken,
    ) -> Result<Option<Self>, Error> {
        if interval.is_zero() {
            return Err(Error::InvalidSessionAdmission(
                "session document watch interval must be nonzero".into(),
            ));
        }
        let store = owner.store();
        let (binding, _) =
            defined_document_binding(&definition.kind, definition.version, &definition.initial)?;
        for _ in 0..128 {
            if cancelled.is_cancelled() || source_closed.is_cancelled() {
                return Ok(None);
            }
            let before = store.commit_sequence()?;
            let (session, scope) = crate::session_membership::document_scope(store, conversation)?;
            let key = storage_key(session, &definition.kind, key)?;
            let entry = store.get(&key)?;
            if store.commit_sequence()? != before {
                continue;
            }
            let Some(entry) = entry else { return Ok(None) };
            let record: StoredDocument = serde_json::from_slice(&entry.value)?;
            record.validate(&binding)?;
            let Some(value) = record.typed()? else {
                return Ok(None);
            };
            return Ok(Some(Self {
                owner: Arc::downgrade(owner),
                source_closed,
                cancelled,
                conversation,
                scope,
                key,
                binding,
                interval,
                version: entry.version,
                value: Arc::new(value),
                present: true,
                end: None,
            }));
        }
        Err(Error::InvalidSessionAdmission(
            "could not capture a stable session document snapshot".into(),
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
        let (_, checks) = crate::session_membership::document_scope(store, self.conversation)?;
        if checks != self.scope {
            return Err(Error::InvalidSessionAdmission(
                "session document watch membership changed".into(),
            ));
        }
        if store.commit_sequence()? != before {
            return Ok(None);
        }
        Ok(Some(entry.ok_or(Error::InvalidDocumentDefinition)?))
    }

    fn update(
        &mut self,
        entry: durable_store::Entry,
    ) -> Result<Option<SessionDocumentUpdate<T>>, Error> {
        if entry.version == self.version {
            return Ok(None);
        }
        let record: StoredDocument = serde_json::from_slice(&entry.value)?;
        record.validate(&self.binding)?;
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
        Ok(Some(SessionDocumentUpdate {
            value: self.value.clone(),
            retired,
        }))
    }

    /// Wait for a new value or retirement. Dropping the pending read leaves
    /// the watch open. Revoking the acquisition membership ends the watch.
    pub async fn next(&mut self) -> Result<Option<SessionDocumentUpdate<T>>, Error> {
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
                _ = tokio::time::sleep(self.interval) => {}
            }
        }
        Ok(None)
    }
}
