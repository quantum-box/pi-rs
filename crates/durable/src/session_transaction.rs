//! A checked host transaction over one conversation and JSON documents.
//!
//! Changes are provisional until the enclosing `Harness::session_transaction`
//! commits. The callback is never replayed after a conflict.
use crate::{
    document::{Address, Documents, Error},
    runtime::{ApplicationWrite, Conversation},
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
    pub(crate) state: Conversation,
    pub(crate) table_written: bool,
    pub(crate) supports_context: bool,
    pub(crate) adapter: Option<std::sync::Arc<dyn crate::model::ProviderAdapter>>,
    pub(crate) image_profile_guard: Option<Option<crate::images::ModelImageProfile>>,
    pub(crate) after_reset: Option<Uuid>,
    pub(crate) entries_allowed: bool,
    reads: HashMap<Address, Option<Map<String, Value>>>,
    writes: HashMap<Address, Option<Map<String, Value>>>,
    signals: crate::document::HostTransaction<'a>,
    clock: std::sync::Arc<dyn Fn() -> i64 + Send + Sync>,
    tasks: Vec<crate::task::Task>,
}

impl<'a> SessionTransaction<'a> {
    pub(crate) fn new(
        store: &'a Store,
        state: Conversation,
        after_reset: Option<Uuid>,
        entries_allowed: bool,
        supports_context: bool,
        adapter: Option<std::sync::Arc<dyn crate::model::ProviderAdapter>>,
        clock: std::sync::Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<Self, Error> {
        Ok(Self {
            store,
            revision: store.document_snapshot()?,
            state,
            table_written: false,
            supports_context,
            adapter,
            image_profile_guard: None,
            after_reset,
            entries_allowed,
            reads: HashMap::new(),
            writes: HashMap::new(),
            signals: crate::document::HostTransaction::new(store),
            clock,
            tasks: Vec::new(),
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

    /// Stage a typed task signal before the first table write. The result is
    /// provisional until the enclosing session transaction commits.
    pub fn signal(
        &mut self,
        task_id: Uuid,
        token: &crate::task_store::SignalToken,
        key: &str,
        value: Value,
    ) -> Result<crate::task_store::SignalResult, Error> {
        if self.table_written {
            return Err(Error::ReadAfterWrite);
        }
        let result = self
            .signals
            .signal_at(task_id, token, key, value, (self.clock)())?;
        if self.signals.has_signal_mutations() {
            self.table_written = true;
        }
        Ok(result)
    }

    /// Stage a registered task owned by this conversation. Its ID is provisional
    /// until commit; the active driver validates its definition and policy.
    pub fn create_task(&mut self, task: crate::task::Task) -> Result<Uuid, Error> {
        if self.tasks.iter().any(|existing| existing.id() == task.id()) {
            return Err(Error::DuplicateTask);
        }
        if task.owner() != &crate::task::TaskOwner::Conversation(self.state.id)
            || task.state() != &crate::task::TaskState::Ready
            || task.abort_requested()
            || task.kind().is_none()
        {
            return Err(Error::InvalidSessionAdmission(
                "invalid conversation-owned task".into(),
            ));
        }
        let id = task.id();
        self.tasks.push(task);
        self.table_written = true;
        Ok(id)
    }

    /// Return placed application writes, including entries staged by this transaction.
    pub fn entries(&self) -> Vec<ApplicationWrite> {
        self.state
            .application_writes
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
        while self
            .state
            .application_writes
            .iter()
            .any(|entry| entry.id == id)
        {
            id = Uuid::new_v4();
        }
        self.state.application_writes.push(ApplicationWrite {
            id,
            request_id: None,
            kind: kind.into(),
            data,
            placed: true,
            withdrawn: false,
            after_reset: self.after_reset,
        });
        self.table_written = true;
        Ok(id)
    }

    pub(crate) fn into_parts(
        mut self,
    ) -> Result<(Conversation, OwnedBatch, bool, Vec<crate::task::Task>), Error> {
        if let Some(profile) = self.image_profile_guard {
            let adapter = self
                .adapter
                .as_ref()
                .filter(|adapter| {
                    self.state.provider_adapter.as_deref() == Some(adapter.id())
                        && adapter.supports_durable_blocks()
                })
                .ok_or_else(|| {
                    Error::InvalidSessionAdmission(
                        "block adapter changed during transaction".into(),
                    )
                })?;
            if adapter.image_profile(&self.state.config.model) != profile {
                return Err(Error::InvalidSessionAdmission(
                    "model image profile changed during transaction".into(),
                ));
            }
        }
        let changed = self.table_written || !self.writes.is_empty();
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
        self.signals.append(batch)?;
        Ok((self.state, self.signals.into_batch()?, changed, self.tasks))
    }
}
