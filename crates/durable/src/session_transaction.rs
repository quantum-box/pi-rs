//! A checked host transaction over one conversation and JSON documents.
//!
//! Changes are provisional until the enclosing `Harness::session_transaction`
//! commits. The callback is never replayed after a conflict.
use crate::{
    document::{Address, Documents, Error},
    runtime::ApplicationWrite,
    task_store::OwnedBatch,
};
use durable_store::{Check, Expected, Mutation, Store};
use serde_json::{Map, Value};
use std::collections::HashMap;
use uuid::Uuid;

/// The candidate state of one host-initiated session transaction.
pub struct SessionTransaction<'a> {
    store: &'a Store,
    revision: u64,
    entries: Vec<ApplicationWrite>,
    original_entries: usize,
    after_reset: Option<Uuid>,
    entries_allowed: bool,
    reads: HashMap<Address, Option<Map<String, Value>>>,
    writes: HashMap<Address, Option<Map<String, Value>>>,
}

impl<'a> SessionTransaction<'a> {
    pub(crate) fn new(
        store: &'a Store,
        entries: Vec<ApplicationWrite>,
        after_reset: Option<Uuid>,
        entries_allowed: bool,
    ) -> Result<Self, Error> {
        Ok(Self {
            store,
            revision: store.document_snapshot()?,
            original_entries: entries.len(),
            entries,
            after_reset,
            entries_allowed,
            reads: HashMap::new(),
            writes: HashMap::new(),
        })
    }

    /// Read a document at the transaction's snapshot, including local writes.
    pub fn document(&mut self, address: &Address) -> Result<Option<Map<String, Value>>, Error> {
        if let Some(value) = self.writes.get(address) {
            return Ok(value.clone());
        }
        if let Some(value) = self.reads.get(address) {
            return Ok(value.clone());
        }
        let value = Documents::new(self.store)
            .get_at(address, self.revision)?
            .map(|doc| doc.value);
        self.reads.insert(address.clone(), value.clone());
        Ok(value)
    }

    /// Replace or create a JSON-object document. The original snapshot is fenced.
    pub fn put_document(
        &mut self,
        address: &Address,
        value: Map<String, Value>,
    ) -> Result<(), Error> {
        self.document(address)?;
        self.writes.insert(address.clone(), Some(value));
        Ok(())
    }

    /// Delete a document, retaining the original snapshot as a dependency.
    pub fn delete_document(&mut self, address: &Address) -> Result<(), Error> {
        self.document(address)?;
        self.writes.insert(address.clone(), None);
        Ok(())
    }

    /// Return placed application writes, including entries staged by this transaction.
    pub fn entries(&self) -> Vec<ApplicationWrite> {
        self.entries
            .iter()
            .filter(|entry| entry.placed && !entry.withdrawn)
            .cloned()
            .collect()
    }

    /// Append a display-only application entry. The ID is provisional until commit.
    pub fn append_entry(&mut self, kind: &str, data: Option<Value>) -> Result<Uuid, Error> {
        if kind.is_empty() || kind.starts_with("pi.") {
            return Err(Error::InvalidSessionEntry);
        }
        if !self.entries_allowed {
            return Err(Error::SessionEntryBlocked);
        }
        let mut id = Uuid::new_v4();
        while self.entries.iter().any(|entry| entry.id == id) {
            id = Uuid::new_v4();
        }
        self.entries.push(ApplicationWrite {
            id,
            request_id: None,
            kind: kind.into(),
            data,
            placed: true,
            withdrawn: false,
            after_reset: self.after_reset,
        });
        Ok(id)
    }

    pub(crate) fn into_parts(self) -> Result<(Vec<ApplicationWrite>, OwnedBatch, bool), Error> {
        let changed = self.entries.len() != self.original_entries || !self.writes.is_empty();
        let mut batch = OwnedBatch {
            checks: Vec::new(),
            mutations: Vec::new(),
        };
        for address in self.reads.keys() {
            let key = address.storage_key()?;
            batch.checks.push(Check {
                key: key.clone(),
                expected: Expected::DocumentUnchangedSince(self.revision),
            });
            if let Some(value) = self.writes.get(address) {
                batch.mutations.push(match value {
                    Some(value) => Mutation::Put {
                        key,
                        value: serde_json::to_vec(value)?,
                    },
                    None => Mutation::Delete { key },
                });
            }
        }
        Ok((self.entries, batch, changed))
    }
}
