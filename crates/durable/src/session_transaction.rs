//! A checked host transaction over one conversation and JSON documents.
//!
//! Changes are provisional until the enclosing `Harness::session_transaction`
//! commits. The callback is never replayed after a conflict.
use crate::{
    document::{Address, Documents, Error},
    runtime::{ApplicationWrite, Conversation, Submission},
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
    task_reads: HashMap<Uuid, (Expected, Option<crate::task_store::StoredTask>)>,
    tasks: Vec<crate::task::Task>,
    signals: crate::document::HostTransaction<'a>,
    clock: std::sync::Arc<dyn Fn() -> i64 + Send + Sync>,
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
            task_reads: HashMap::new(),
            tasks: Vec::new(),
            signals: crate::document::HostTransaction::new(store),
            clock,
        })
    }

    fn check_table_read(&self) -> Result<(), Error> {
        if self.table_written {
            Err(Error::ReadAfterWrite)
        } else {
            Ok(())
        }
    }

    /// Read a committed submission in this transaction's conversation.
    pub fn submission(&self, id: Uuid) -> Result<Option<Submission>, Error> {
        self.check_table_read()?;
        Ok(self
            .state
            .submissions
            .iter()
            .find(|item| item.id == id)
            .cloned())
    }

    /// Read a committed input or continuation by its conversation-scoped request ID.
    pub fn submission_by_request(&self, request_id: &str) -> Result<Option<Submission>, Error> {
        self.check_table_read()?;
        Ok(self
            .state
            .submissions
            .iter()
            .find(|item| item.request_id.as_deref() == Some(request_id))
            .cloned())
    }

    /// Read a committed application write in this conversation.
    pub fn entry(&self, id: Uuid) -> Result<Option<ApplicationWrite>, Error> {
        self.check_table_read()?;
        Ok(self
            .state
            .application_writes
            .iter()
            .find(|item| item.id == id && item.placed && !item.withdrawn)
            .cloned())
    }

    /// Read a directly conversation-owned task and fence its observed version.
    /// Tasks owned by another scope are not exposed.
    pub fn task(&mut self, id: Uuid) -> Result<Option<crate::task_store::StoredTask>, Error> {
        self.check_table_read()?;
        if let Some((_, value)) = self.task_reads.get(&id) {
            return Ok(value.clone());
        }
        let boundary = self.store.put_version()?;
        let value = crate::task_store::TaskStore::new(self.store).load(id)?;
        let expected = value
            .as_ref()
            .map_or(Expected::MissingUnchangedSince(boundary), |task| {
                Expected::Version(task.version)
            });
        let visible = value.filter(|task| {
            task.task.owner() == &crate::task::TaskOwner::Conversation(self.state.id)
        });
        self.task_reads.insert(id, (expected, visible.clone()));
        Ok(visible)
    }

    /// Create a ready task directly owned by this conversation. The returned ID
    /// is provisional until the enclosing transaction commits.
    pub fn create_task(&mut self, task: crate::task::Task) -> Result<Uuid, Error> {
        if task.id().is_nil()
            || task.owner() != &crate::task::TaskOwner::Conversation(self.state.id)
            || task.state() != &crate::task::TaskState::Ready
            || task.abort_requested()
            || self.tasks.iter().any(|existing| existing.id() == task.id())
        {
            return Err(Error::InvalidSessionAdmission(
                "invalid scoped task creation".into(),
            ));
        }
        // Task is deserializable by hosts. Reject forged attempt, wake or
        // internal adapter fields by comparing against a fresh task built
        // through its public constructors.
        let mut fresh = crate::task::Task::new(
            task.id(),
            task.owner().clone(),
            task.checkpoint().clone(),
            task.restart_limit(),
        )
        .map_err(|_| Error::InvalidSessionAdmission("invalid scoped task creation".into()))?;
        if let Some(kind) = task.kind() {
            fresh = fresh
                .registered_versioned(
                    kind.to_owned(),
                    task.replay_safe(),
                    task.definition_version(),
                    task.input().clone(),
                )
                .map_err(|_| {
                    Error::InvalidSessionAdmission("invalid scoped task creation".into())
                })?;
        }
        if task.is_background() {
            fresh = fresh.background().map_err(|_| {
                Error::InvalidSessionAdmission("invalid scoped task creation".into())
            })?;
        }
        if fresh != task {
            return Err(Error::InvalidSessionAdmission(
                "invalid scoped task creation".into(),
            ));
        }
        let id = task.id();
        self.tasks.push(task);
        self.table_written = true;
        Ok(id)
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
        let boundary = self.store.put_version()?;
        let result = self
            .signals
            .signal_at(task_id, token, key, value, (self.clock)())?;
        if result == crate::task_store::SignalResult::NotFound {
            self.task_reads
                .entry(task_id)
                .or_insert((Expected::MissingUnchangedSince(boundary), None));
        }
        if self.signals.has_signal_mutations() {
            self.table_written = true;
        }
        Ok(result)
    }

    /// Preview placed application writes, including entries staged in this transaction.
    pub fn entries(&self) -> Vec<ApplicationWrite> {
        self.state
            .application_writes
            .iter()
            .filter(|entry| entry.placed && !entry.withdrawn)
            .cloned()
            .collect()
    }

    /// Read placed application writes under the table read-before-write rule.
    pub fn checked_entries(&self) -> Result<Vec<ApplicationWrite>, Error> {
        self.check_table_read()?;
        Ok(self.entries())
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
        for (id, (expected, _)) in &self.task_reads {
            batch.checks.push(Check {
                key: crate::task_store::key(*id),
                expected: expected.clone(),
            });
        }
        if !self.tasks.is_empty() {
            let saves: Vec<_> = self
                .tasks
                .iter()
                .map(crate::task_store::Save::Create)
                .collect();
            let mut tasks = crate::task_store::TaskStore::new(self.store).prepare_save(&saves)?;
            for check in &mut tasks.checks {
                if let Some(existing) = batch.checks.iter().find(|old| old.key == check.key)
                    && check.expected == Expected::Missing
                    && matches!(existing.expected, Expected::MissingUnchangedSince(_))
                {
                    check.expected = existing.expected.clone();
                }
            }
            batch.checks.extend(tasks.checks);
            batch.mutations.extend(tasks.mutations);
        }
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
        self.signals.append_session_candidate(batch, &self.tasks)?;
        Ok((self.state, self.signals.into_batch()?, changed, self.tasks))
    }
}
