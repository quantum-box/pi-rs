//! Whole current values for documents shared by members of one session.

use std::collections::HashMap;

use durable_store::{Check, Expected, Mutation, Store};
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

struct Staged {
    expected: Expected,
    original: Option<StoredDocument>,
    current: Option<StoredDocument>,
}

impl Staged {
    fn read<T: DeserializeOwned + Serialize>(
        &self,
    ) -> Result<Option<DefinedDocumentValue<T>>, Error> {
        let Some(record) = &self.current else {
            return Ok(None);
        };
        let Some(value) = &record.value else {
            return Ok(None);
        };
        let value = Value::Object(value.clone());
        let typed = serde::Deserialize::deserialize(&value)?;
        if serde_json::to_value(&typed)? != value {
            return Err(Error::InvalidDocumentDefinition);
        }
        Ok(Some(DefinedDocumentValue {
            incarnation: record
                .definition
                .incarnation
                .ok_or(Error::InvalidDocumentDefinition)?,
            value: typed,
        }))
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
        let mut storage_key = b"durable/session-current-document/v1/".to_vec();
        storage_key.extend(serde_json::to_vec(&(*session, &definition.kind, key))?);
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
        if let Some(record) = &staged.current
            && (record.definition.schema != binding.schema
                || record.definition.initial_digest != binding.initial_digest
                || record.value.is_some() != record.definition.incarnation.is_some()
                || record
                    .definition
                    .incarnation
                    .as_ref()
                    .is_some_and(Uuid::is_nil))
        {
            return Err(Error::InvalidDocumentDefinition);
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
