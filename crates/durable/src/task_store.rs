//! Atomic persistence and optimistic concurrency for standalone tasks.
//!
//! Recovery is explicit: the host must first establish that the previous
//! executor has stopped. A successful reservation is durable before execution;
//! a commit error can have an unknown outcome and requires inspection on reopen.

use crate::task::{Task, TaskOwner, TaskState, TransitionError, WaitFor};
use durable_store::{Check, Entry, Expected, Mutation, Store};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

pub(crate) const PREFIX: &[u8] = b"durable/task/";
pub(crate) const CALLBACK_START_PREFIX: &[u8] = b"durable/task-callback-start/";
const SIGNAL_PREFIX: &[u8] = b"durable/signal/";
const SIGNAL_REVISION_PREFIX: &[u8] = b"durable/signal-revision/";
const SUBMISSION_SETTLED_NAME: &str = "pi.submission-settled";
const SUBMISSION_SETTLED_VERSION: u32 = 1;

fn is_submission_settled(token: &SignalToken) -> bool {
    token.name() == SUBMISSION_SETTLED_NAME && token.version() == SUBMISSION_SETTLED_VERSION
}

fn validate_builtin_receipt(signal: &Signal) -> Result<(), Error> {
    let typed = signal.typed.as_ref().ok_or(Error::InvalidSignalToken)?;
    if signal.name != SUBMISSION_SETTLED_NAME {
        return Err(Error::InvalidSignalToken);
    }
    if typed.version != SUBMISSION_SETTLED_VERSION {
        return Err(Error::NewerSignalVersion {
            stored: typed.version,
            current: SUBMISSION_SETTLED_VERSION,
        });
    }
    if !signal.expired {
        crate::runtime::submission_settled_signal()
            .read_value(signal.payload.clone(), typed.version)?;
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("invalid task transition: {0:?}")]
    Transition(TransitionError),
    #[error("task record key does not match its ID")]
    InvalidKey,
    #[error("duplicate task in batch")]
    DuplicateTask,
    #[error("signal ID was already used with different content")]
    SignalMismatch,
    #[error("typed signal key already holds different content")]
    SignalKeyMismatch,
    #[error("typed signal key has already been consumed")]
    SignalKeyConsumed,
    #[error("wait contains a missing, duplicate, or non-child task")]
    InvalidChild,
    #[error("task is not waiting")]
    NotWaiting,
    #[error("signal target was not found")]
    SignalTargetNotFound,
    #[error("signal target is closed")]
    SignalTargetClosed,
    #[error("callback start is temporarily fenced")]
    CallbackStartBusy,
    #[error("invalid owner checkpoint key or child task")]
    InvalidOwnedBatch,
    #[error("task ownership cannot be changed")]
    OwnerChanged,
    #[error("retired task attempt is stale or aborted")]
    StaleRetiredAttempt,
    #[error("task ownership contains a cycle or invalid edge")]
    InvalidOwnership,
    #[error("signal name, version, or key is invalid")]
    InvalidSignalToken,
    #[error("invalid signal schema: {0}")]
    InvalidSignalSchema(String),
    #[error("signal payload does not match its schema")]
    InvalidSignalPayload,
    #[error("signal version {stored} is newer than reader version {current}")]
    NewerSignalVersion { stored: u32, current: u32 },
    #[error("signal migration failed: {0}")]
    SignalMigration(String),
}

/// Identity and value version of a signal kind. `new` retains the legacy
/// unconstrained JSON behavior; `with_schema` enables checked typed reads.
#[derive(Clone)]
pub struct SignalToken {
    name: String,
    version: u32,
    schema: Option<Value>,
    migrate: Option<Arc<SignalMigration>>,
}

type SignalMigration = dyn Fn(Value, u32) -> Result<Value, String> + Send + Sync;

impl std::fmt::Debug for SignalToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignalToken")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("schema", &self.schema)
            .field("has_migration", &self.migrate.is_some())
            .finish()
    }
}

// A token's identity has always been its name and version. Definitions sharing
// that identity are the caller's responsibility, as in upstream.
impl PartialEq for SignalToken {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.version == other.version
    }
}
impl Eq for SignalToken {}

durable_task_schema::declare_signal_key!();

impl SignalToken {
    pub fn new(name: impl Into<String>, version: u32) -> Result<Self, Error> {
        let name = name.into();
        if name.is_empty() || name.contains('/') || version == 0 {
            return Err(Error::InvalidSignalToken);
        }
        Ok(Self {
            name,
            version,
            schema: None,
            migrate: None,
        })
    }

    /// Define a JSON Schema for values read with this token.
    pub fn with_schema(
        name: impl Into<String>,
        version: u32,
        schema: Value,
    ) -> Result<Self, Error> {
        let mut token = Self::new(name, version)?;
        jsonschema::validator_for(&schema)
            .map_err(|e| Error::InvalidSignalSchema(e.to_string()))?;
        token.schema = Some(schema);
        Ok(token)
    }

    /// Register one migration from any supported older version to this version.
    pub fn with_migration<F>(mut self, migration: F) -> Self
    where
        F: Fn(Value, u32) -> Result<Value, String> + Send + Sync + 'static,
    {
        self.migrate = Some(Arc::new(migration));
        self
    }

    fn read_value(&self, value: Value, stored: u32) -> Result<Value, Error> {
        if stored == 0 || stored > self.version {
            return Err(Error::NewerSignalVersion {
                stored,
                current: self.version,
            });
        }
        let value = if stored < self.version {
            let migrate = self
                .migrate
                .as_ref()
                .ok_or_else(|| Error::SignalMigration("missing migration".into()))?;
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| migrate(value, stored)))
                .map_err(|_| Error::SignalMigration("migration panicked".into()))?
                .map_err(Error::SignalMigration)?
        } else {
            value
        };
        if let Some(schema) = &self.schema {
            let validator = jsonschema::validator_for(schema)
                .map_err(|e| Error::InvalidSignalSchema(e.to_string()))?;
            if !validator.is_valid(&value) {
                return Err(Error::InvalidSignalPayload);
            }
        }
        Ok(value)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn key(&self, key: impl Into<String>) -> Result<SignalKey, Error> {
        let key = key.into();
        if key.is_empty() {
            return Err(Error::InvalidSignalToken);
        }
        Ok(SignalKey {
            signal: self.name.clone(),
            key,
        })
    }
}

impl SignalKey {
    pub fn signal(&self) -> &str {
        &self.signal
    }
    pub fn key(&self) -> &str {
        &self.key
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TypedSignal {
    key: SignalKey,
    version: u32,
}

/// Outcome of delivering a typed signal to a task.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalState {
    Value(Value),
    Expired,
}

/// Outcome of delivering a typed signal to a task.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalResult {
    Delivered,
    Duplicate,
    Invalid,
    Closed,
    NotFound,
}

/// A task and the storage version that must be supplied for its next update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredTask {
    pub task: Task,
    pub version: u64,
}

/// Prepare a version fence for a committed handler transition. The returned
/// check must be committed with dependent writes, after independently proving
/// the executor has stopped and validating conversation and ownership fences.
#[allow(dead_code)] // Storage prerequisite, not yet used for executor handoff.
pub(crate) fn retired_attempt_check(snapshot: &StoredTask, attempt: Uuid) -> Result<Check, Error> {
    if snapshot.task.abort_requested()
        || snapshot.task.state() == &TaskState::Running
        || snapshot.task.attempt_id().is_some()
        || snapshot.task.retired_attempt_id() != Some(attempt)
    {
        return Err(Error::StaleRetiredAttempt);
    }
    Ok(Check {
        key: key(snapshot.task.id()),
        expected: Expected::Version(snapshot.version),
    })
}

/// One member of an atomic batch. Updates must use a previously read version.
pub enum Save<'a> {
    Create(&'a Task),
    Update(&'a StoredTask),
}

/// Checks and writes for one owned checkpoint, to be included intact in a
/// single host transaction alongside any transcript and observation writes.
#[derive(Clone)]
pub struct OwnedBatch {
    pub checks: Vec<Check>,
    pub mutations: Vec<Mutation>,
}

/// A durable, uniquely identified signal. IDs are scoped to a target task.
/// Reusing an ID with identical content is idempotent, even after consumption.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signal {
    pub id: Uuid,
    pub task_id: Uuid,
    pub name: String,
    pub payload: Value,
    consumed: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    expired: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    typed: Option<TypedSignal>,
}

impl Signal {
    pub fn new(id: Uuid, task_id: Uuid, name: String, payload: Value) -> Self {
        Self {
            id,
            task_id,
            name,
            payload,
            consumed: false,
            expired: false,
            typed: None,
        }
    }
    /// Construct a versioned signal. IDs retain their existing task-scoped
    /// deduplication semantics; a typed key holds at most one value.
    pub fn new_typed(
        id: Uuid,
        task_id: Uuid,
        token: &SignalToken,
        key: impl Into<String>,
        payload: Value,
    ) -> Result<Self, Error> {
        let key = token.key(key)?;
        let payload = token.read_value(payload, token.version)?;
        // A caller can construct an unconstrained token with the built-in
        // identity. Never let it bypass the built-in payload schema.
        if is_submission_settled(token) {
            crate::runtime::submission_settled_signal()
                .read_value(payload.clone(), SUBMISSION_SETTLED_VERSION)?;
        }
        Ok(Self {
            id,
            task_id,
            name: token.name.clone(),
            payload,
            consumed: false,
            expired: false,
            typed: Some(TypedSignal {
                key,
                version: token.version,
            }),
        })
    }
    pub fn typed_key(&self) -> Option<&SignalKey> {
        self.typed.as_ref().map(|typed| &typed.key)
    }
    pub fn typed_version(&self) -> Option<u32> {
        self.typed.as_ref().map(|typed| typed.version)
    }
    pub fn is_consumed(&self) -> bool {
        self.consumed
    }
}

/// Result of reevaluating a waiting task. Signal payloads remain readable after
/// delivery via `signal`, including after a crash following the commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeResult {
    Pending,
    Woken { signal: Option<Signal> },
}

#[cfg(test)]
type CallbackWakeHooks =
    std::sync::Mutex<std::collections::HashMap<Uuid, Box<dyn FnOnce() + Send>>>;
#[cfg(test)]
fn callback_wake_hooks() -> &'static CallbackWakeHooks {
    static HOOKS: std::sync::OnceLock<CallbackWakeHooks> = std::sync::OnceLock::new();
    HOOKS.get_or_init(Default::default)
}
#[cfg(test)]
pub(crate) fn on_callback_wake_blocked(id: Uuid, hook: impl FnOnce() + Send + 'static) {
    callback_wake_hooks()
        .lock()
        .unwrap()
        .insert(id, Box::new(hook));
}
#[cfg(test)]
pub(crate) fn report_callback_wake_blocked(id: Uuid) {
    let hook = callback_wake_hooks().lock().unwrap().remove(&id);
    if let Some(hook) = hook {
        hook();
    }
}

pub struct TaskStore<'a> {
    store: &'a Store,
}

impl<'a> TaskStore<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    pub(crate) fn store(&self) -> &Store {
        self.store
    }

    pub fn load(&self, id: Uuid) -> Result<Option<StoredTask>, Error> {
        let key = key(id);
        self.store
            .get(&key)?
            .map(|entry| decode(&key, entry))
            .transpose()
    }

    /// Returns a snapshot; its versions can become stale immediately.
    pub fn list(&self) -> Result<Vec<StoredTask>, Error> {
        self.store
            .list(PREFIX)?
            .into_iter()
            .map(|(key, entry)| decode(&key, entry))
            .collect()
    }

    /// Atomically create and/or update tasks. A conflict leaves the whole batch
    /// untouched. Versions are not returned: a subsequent read may observe a
    /// later writer, so it must not be used as proof of an exclusive reservation.
    pub fn save(&self, tasks: &[Save<'_>]) -> Result<(), Error> {
        let batch = self.prepare_save(tasks)?;
        match self.store.commit(&batch.checks, &batch.mutations) {
            Ok(()) => Ok(()),
            Err(error @ durable_store::Error::Conflict { .. }) => {
                for save in tasks {
                    let task = match save {
                        Save::Create(task) => *task,
                        Save::Update(stored) => &stored.task,
                    };
                    if self.store.get(&callback_start_key(task.id()))?.is_some() {
                        return Err(Error::CallbackStartBusy);
                    }
                }
                Err(error.into())
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Prepare task writes for an atomic commit with other checked records.
    /// Do not compose separately prepared ownership changes in the same commit:
    /// their checks observe the pre-commit state, not each other's writes.
    pub fn prepare_save(&self, tasks: &[Save<'_>]) -> Result<OwnedBatch, Error> {
        let mut checks = Vec::with_capacity(tasks.len());
        let mut mutations = Vec::with_capacity(tasks.len());
        let mut seen = std::collections::HashSet::new();
        let mut created = std::collections::HashMap::new();
        for save in tasks {
            if let Save::Create(task) = save
                && created.insert(task.id(), *task).is_some()
            {
                return Err(Error::DuplicateTask);
            }
        }
        for task in created.values() {
            checks.extend(self.check_create_ownership(task, &created)?);
        }
        for save in tasks {
            let (task, expected) = match save {
                Save::Create(task) => (*task, Expected::Missing),
                Save::Update(stored) => {
                    if let Some(current) = self.load(stored.task.id())?
                        && current.version == stored.version
                        && current.task.owner() != stored.task.owner()
                    {
                        return Err(Error::OwnerChanged);
                    }
                    (&stored.task, Expected::Version(stored.version))
                }
            };
            let wait_keys: &[SignalKey] = match task.state() {
                TaskState::Waiting(WaitFor::TypedSignal(k)) => std::slice::from_ref(k),
                TaskState::Waiting(WaitFor::TypedSleep { signals, .. }) => signals,
                _ => &[],
            };
            if !wait_keys.is_empty() {
                let revision_key = signal_revision_key(task.id());
                let revision = self.store.get(&revision_key)?;
                checks.push(Check {
                    key: revision_key,
                    expected: revision.map_or(Expected::Missing, |e| Expected::Version(e.version)),
                });
                self.validate_typed_wait_keys(task.id(), wait_keys)?;
            }
            let key = key(task.id());
            if !seen.insert(key.clone()) {
                return Err(Error::DuplicateTask);
            }
            // An unconditional absence CAS keeps prepared task writes safe
            // without introducing a pre-transaction error after a scheduler
            // has marked an operation as indeterminate.
            checks.push(Check {
                key: callback_start_key(task.id()),
                expected: Expected::Missing,
            });
            checks.push(Check {
                key: key.clone(),
                expected,
            });
            mutations.push(Mutation::Put {
                key,
                value: serde_json::to_vec(task)?,
            });
        }
        Ok(OwnedBatch { checks, mutations })
    }

    pub(crate) fn validate_typed_wait_keys(
        &self,
        task_id: Uuid,
        keys: &[SignalKey],
    ) -> Result<(), Error> {
        for (_, entry) in self.store.list(&signal_task_prefix(task_id))? {
            let signal: Signal = serde_json::from_slice(&entry.value)?;
            if signal.typed_key().is_some_and(|key| keys.contains(key))
                && (signal.consumed || signal.expired)
            {
                return Err(Error::SignalKeyConsumed);
            }
        }
        Ok(())
    }

    fn check_create_ownership(
        &self,
        task: &Task,
        created: &std::collections::HashMap<Uuid, &Task>,
    ) -> Result<Vec<Check>, Error> {
        let mut checks = Vec::new();
        let mut seen_tasks = std::collections::HashSet::from([task.id()]);
        let mut seen_conversations = std::collections::HashSet::new();
        let mut next = task.owner().clone();
        let mut fence_scope = true;
        loop {
            if fence_scope {
                checks.push(crate::ownership_edge::abort_fence_check(self.store, &next)?);
            }
            match next {
                TaskOwner::Task(id) => {
                    if !seen_tasks.insert(id) {
                        return Err(Error::InvalidOwnership);
                    }
                    if let Some(parent) = created.get(&id) {
                        if matches!(task.owner(), TaskOwner::Task(owner) if *owner == id)
                            && parent.is_completing()
                        {
                            return Err(Error::InvalidOwnership);
                        }
                        if parent.is_background() {
                            fence_scope = false;
                        }
                        next = parent.owner().clone();
                    } else {
                        let parent = self.load(id)?;
                        checks.push(Check {
                            key: key(id),
                            expected: parent
                                .as_ref()
                                .map_or(Expected::Missing, |p| Expected::Version(p.version)),
                        });
                        match parent {
                            Some(parent) => {
                                if matches!(task.owner(), TaskOwner::Task(owner) if *owner == id)
                                    && parent.task.is_completing()
                                {
                                    return Err(Error::InvalidOwnership);
                                }
                                if parent.task.is_background() {
                                    fence_scope = false;
                                }
                                next = parent.task.owner().clone();
                            }
                            None => return Ok(checks),
                        }
                    }
                }
                TaskOwner::Conversation(id) => {
                    if !seen_conversations.insert(id) {
                        return Err(Error::InvalidOwnership);
                    }
                    let key = crate::ownership_edge::edge_key(id);
                    let entry = self.store.get(&key)?;
                    checks.push(Check {
                        key: key.clone(),
                        expected: entry
                            .as_ref()
                            .map_or(Expected::Missing, |e| Expected::Version(e.version)),
                    });
                    match entry {
                        None => return Ok(checks),
                        Some(entry) => {
                            let edge = crate::ownership_edge::decode(&key, entry)
                                .map_err(|_| Error::InvalidOwnership)?;
                            if edge.edge.mode == crate::ownership_edge::Mode::Background {
                                fence_scope = false;
                            }
                            let owner = if let Some(owner) = created.get(&edge.edge.task) {
                                (*owner).clone()
                            } else {
                                let owner =
                                    self.load(edge.edge.task)?.ok_or(Error::InvalidOwnership)?;
                                checks.push(Check {
                                    key: self::key(edge.edge.task),
                                    expected: Expected::Version(owner.version),
                                });
                                owner.task
                            };
                            if owner.owner() != &edge.edge.task_owner {
                                return Err(Error::InvalidOwnership);
                            }
                            next = TaskOwner::Task(edge.edge.task);
                        }
                    }
                }
            }
        }
    }

    /// Commit an owner's new checkpoint and its newly registered tasks in one
    /// transaction. `expected` must be the version read before preparing the
    /// tasks; on a conflict or uncertain commit, reload the owner checkpoint
    /// before deciding whether to prepare the batch again. The checkpoint must
    /// itself record the child IDs, so recovery can find the committed tasks.
    ///
    /// This is a storage primitive, not an executor lease: callers must fence
    /// the previous owner and validate task kinds against their registry. Owner
    /// records must live outside the task store's reserved namespaces.
    pub fn commit_owned_tasks(
        &self,
        owner: &TaskOwner,
        owner_key: &[u8],
        expected: Expected,
        checkpoint: &[u8],
        children: &[Task],
    ) -> Result<(), Error> {
        let batch = self.prepare_owned_tasks(owner, owner_key, expected, checkpoint, children)?;
        self.store.commit(&batch.checks, &batch.mutations)?;
        Ok(())
    }

    /// Prepare the same atomic checkpoint operation for composition with other
    /// records. The host must commit every check and mutation together, and
    /// must not add conflicting writes to the same keys. Preparing this batch
    /// does not reserve children or make them visible.
    pub fn prepare_owned_tasks(
        &self,
        owner: &TaskOwner,
        owner_key: &[u8],
        expected: Expected,
        checkpoint: &[u8],
        children: &[Task],
    ) -> Result<OwnedBatch, Error> {
        if owner_key.is_empty() || owner_key.starts_with(b"durable/") {
            return Err(Error::InvalidOwnedBatch);
        }
        let mut checks = vec![Check {
            key: owner_key.to_vec(),
            expected,
        }];
        let mut mutations = vec![Mutation::Put {
            key: owner_key.to_vec(),
            value: checkpoint.to_vec(),
        }];
        let mut seen = std::collections::HashSet::new();
        for child in children {
            if child.owner() != owner
                || child.state() != &TaskState::Ready
                || child.kind().is_none_or(str::is_empty)
                || !seen.insert(child.id())
                || matches!(owner, TaskOwner::Task(id) if *id == child.id())
            {
                return Err(Error::InvalidOwnedBatch);
            }
        }
        let saves: Vec<_> = children.iter().map(Save::Create).collect();
        let batch = self.prepare_save(&saves)?;
        checks.extend(batch.checks);
        mutations.extend(batch.mutations);
        Ok(OwnedBatch { checks, mutations })
    }

    /// Atomically commit any deterministic task transition against its snapshot.
    /// The closure may change checkpoint and state together; on error nothing is saved.
    /// Reload after success. The caller must fence any previous executor.
    pub fn update<F>(&self, snapshot: &StoredTask, f: F) -> Result<(), Error>
    where
        F: FnOnce(&mut Task) -> Result<(), TransitionError>,
    {
        let mut next = snapshot.clone();
        f(&mut next.task).map_err(Error::Transition)?;
        self.save(&[Save::Update(&next)])
    }

    // Fence a foreground submission's exclusive destination against any
    // typed-signal writer, including an already prepared concurrent delivery.
    fn foreground_binding_check(
        &self,
        signal: &Signal,
        settlement: Option<(Uuid, Uuid)>,
    ) -> Result<Option<Check>, Error> {
        let Some(typed) = signal
            .typed
            .as_ref()
            .filter(|typed| typed.key.signal == SUBMISSION_SETTLED_NAME)
        else {
            return Ok(None);
        };
        let notify = crate::runtime::SubmissionNotify {
            task_id: signal.task_id,
            key: typed.key.key.clone(),
        };
        let key = crate::owned_subagent::notification_binding_key(&notify);
        let entry = self.store.get(&key)?;
        if let Some(entry) = &entry {
            let (conversation, submission): (Uuid, Uuid) = serde_json::from_slice(&entry.value)?;
            if settlement != Some((conversation, submission))
                || signal.payload.get("id") != Some(&serde_json::json!(submission))
                || signal.payload.get("conversationId") != Some(&serde_json::json!(conversation))
                || typed.version != SUBMISSION_SETTLED_VERSION
            {
                return Err(Error::SignalKeyMismatch);
            }
        }
        Ok(Some(Check {
            key,
            expected: entry.map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
        }))
    }

    /// Durably enqueue a signal. The same task/ID/content can be retried safely.
    /// A conflicting ID returns SignalMismatch. Signals are retained as receipts.
    pub fn send_signal(&self, signal: &Signal) -> Result<(), Error> {
        let builtin = signal.typed.as_ref().is_some_and(|typed| {
            typed.key.signal == SUBMISSION_SETTLED_NAME
                && typed.version == SUBMISSION_SETTLED_VERSION
        });
        if builtin {
            // Incoming expiry is not authoritative: delivery recalculates it.
            let mut candidate = signal.clone();
            candidate.expired = false;
            validate_builtin_receipt(&candidate)?;
        }
        let k = signal_key(signal.task_id, signal.id);
        let mut fresh = signal.clone();
        fresh.consumed = false;
        loop {
            let callback_start = clear_callback_start_check(self.store, signal.task_id)?;
            let binding = self.foreground_binding_check(&fresh, None)?;
            if let Some(entry) = self.store.get(&k)? {
                let old: Signal = serde_json::from_slice(&entry.value)?;
                if builtin {
                    validate_builtin_receipt(&old)?;
                }
                if old.expired {
                    return Err(Error::SignalKeyConsumed);
                }
                if old.id == fresh.id
                    && old.task_id == fresh.task_id
                    && old.name == fresh.name
                    && old.payload == fresh.payload
                    && old.typed == fresh.typed
                {
                    return Ok(());
                }
                return Err(Error::SignalMismatch);
            }
            let target = self
                .load(signal.task_id)?
                .ok_or(Error::SignalTargetNotFound)?;
            if target.task.is_terminal() || target.task.is_completing() {
                return Err(Error::SignalTargetClosed);
            }
            // The revision fences the scan against all concurrent insertions,
            // including records written by versions without a typed-key index.
            let revision_key = signal_revision_key(signal.task_id);
            let revision = self.store.get(&revision_key)?;
            if let Some(typed_key) = fresh.typed_key() {
                let mut found = false;
                for (_, entry) in self.store.list(&signal_task_prefix(signal.task_id))? {
                    let old: Signal = serde_json::from_slice(&entry.value)?;
                    if old.typed_key() == Some(typed_key) {
                        if builtin {
                            validate_builtin_receipt(&old)?;
                        }
                        if old.expired {
                            return Err(Error::SignalKeyConsumed);
                        }
                        if old.payload != fresh.payload || old.typed != fresh.typed {
                            return Err(Error::SignalKeyMismatch);
                        }
                        found = true;
                    }
                }
                if found {
                    // Validate the snapshot even for a no-op: the caller's ID
                    // may have been claimed while the key was being scanned.
                    let mut checks = vec![
                        callback_start.clone(),
                        Check {
                            key: k.clone(),
                            expected: Expected::Missing,
                        },
                        Check {
                            key: revision_key.clone(),
                            expected: revision.as_ref().map_or(Expected::Missing, |entry| {
                                Expected::Version(entry.version)
                            }),
                        },
                        Check {
                            key: key(signal.task_id),
                            expected: Expected::Version(target.version),
                        },
                    ];
                    checks.extend(binding.clone());
                    match self.store.commit(&checks, &[]) {
                        Ok(()) => return Ok(()),
                        Err(durable_store::Error::Conflict { .. }) => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
            }
            fresh.expired = fresh.typed_key().is_some_and(|key| matches!(target.task.state(), TaskState::Waiting(WaitFor::TypedSleep { until_millis: Some(deadline), signals }) if crate::runtime::wall_time_millis() >= *deadline && signals.contains(key)));
            let mut checks = vec![
                callback_start,
                Check {
                    key: revision_key,
                    expected: revision
                        .map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
                },
                Check {
                    key: k.clone(),
                    expected: Expected::Missing,
                },
                Check {
                    key: key(signal.task_id),
                    expected: Expected::Version(target.version),
                },
            ];
            checks.extend(binding);
            match self.store.commit(
                &checks,
                &[
                    Mutation::Put {
                        key: k.clone(),
                        value: serde_json::to_vec(&fresh)?,
                    },
                    Mutation::Put {
                        key: signal_revision_key(signal.task_id),
                        value: vec![],
                    },
                ],
            ) {
                Ok(()) if fresh.expired => return Err(Error::SignalKeyConsumed),
                Ok(()) => return Ok(()),
                Err(durable_store::Error::Conflict { .. }) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Deliver a typed value, returning the upstream-style status. The generated
    /// receipt ID is an internal detail; retries deduplicate by task, kind and key.
    /// Schema-bearing tokens validate the payload. Uses the wall clock.
    pub fn deliver_signal(
        &self,
        task_id: Uuid,
        token: &SignalToken,
        key_text: &str,
        value: Value,
    ) -> Result<SignalResult, Error> {
        self.deliver_signal_with_clock(
            task_id,
            token,
            key_text,
            value,
            crate::runtime::wall_time_millis,
        )
    }

    /// Deliver with an explicit clock value, for deterministic hosts and tests.
    pub fn deliver_signal_at(
        &self,
        task_id: Uuid,
        token: &SignalToken,
        key_text: &str,
        value: Value,
        now_millis: i64,
    ) -> Result<SignalResult, Error> {
        let start = std::time::Instant::now();
        self.deliver_signal_with_clock(task_id, token, key_text, value, || {
            now_millis
                .saturating_add(i64::try_from(start.elapsed().as_millis()).unwrap_or(i64::MAX))
        })
    }

    fn deliver_signal_with_clock(
        &self,
        task_id: Uuid,
        token: &SignalToken,
        key_text: &str,
        value: Value,
        mut now: impl FnMut() -> i64,
    ) -> Result<SignalResult, Error> {
        loop {
            let (result, batch) =
                self.prepare_signal_at(task_id, token, key_text, value.clone(), &mut now)?;
            match self.store.commit(&batch.checks, &batch.mutations) {
                Ok(()) => return Ok(result),
                Err(durable_store::Error::Conflict { .. }) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    // The returned outcome is provisional until the batch commits. Callers must
    // retry the entire enclosing operation on conflict.
    pub(crate) fn prepare_signal_at(
        &self,
        task_id: Uuid,
        token: &SignalToken,
        key_text: &str,
        value: Value,
        now: impl FnMut() -> i64,
    ) -> Result<(SignalResult, OwnedBatch), Error> {
        self.prepare_signal_at_overlay(task_id, token, key_text, value, now, None, &[], None)
    }

    /// Prepare against the transaction's candidate target and signal writes,
    /// while retaining checks against the original committed records.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_signal_at_overlay(
        &self,
        task_id: Uuid,
        token: &SignalToken,
        key_text: &str,
        value: Value,
        mut now: impl FnMut() -> i64,
        candidate: Option<Option<Task>>,
        mutations: &[Mutation],
        settlement: Option<(Uuid, Uuid)>,
    ) -> Result<(SignalResult, OwnedBatch), Error> {
        let clock_start = std::time::Instant::now();
        let stored = self.load(task_id)?;
        let target = candidate.unwrap_or_else(|| stored.as_ref().map(|item| item.task.clone()));
        if target.as_ref().is_some_and(|task| task.id() != task_id) {
            return Err(Error::InvalidKey);
        }
        let mut batch = OwnedBatch {
            checks: vec![Check {
                key: key(task_id),
                expected: stored
                    .as_ref()
                    .map_or(Expected::Missing, |t| Expected::Version(t.version)),
            }],
            mutations: vec![],
        };
        let Some(target) = target else {
            return Ok((SignalResult::NotFound, batch));
        };
        if target.is_terminal() || target.abort_requested() || target.is_completing() {
            return Ok((SignalResult::Closed, batch));
        }
        if key_text.is_empty() {
            return Ok((SignalResult::Invalid, batch));
        }
        batch
            .checks
            .push(clear_callback_start_check(self.store, task_id)?);
        let receipt_id = Uuid::new_v4();
        let mut signal = match Signal::new_typed(receipt_id, task_id, token, key_text, value) {
            Ok(signal) => signal,
            Err(Error::InvalidSignalPayload) => return Ok((SignalResult::Invalid, batch)),
            Err(error) => return Err(error),
        };
        if let Some(check) = self.foreground_binding_check(&signal, settlement)? {
            batch.checks.push(check);
        }
        let revision_key = signal_revision_key(task_id);
        let revision = self.store.get(&revision_key)?;
        batch.checks.push(Check {
            key: revision_key.clone(),
            expected: revision.map_or(Expected::Missing, |e| Expected::Version(e.version)),
        });
        let prefix = signal_task_prefix(task_id);
        let mut receipts: std::collections::BTreeMap<Vec<u8>, Signal> = self
            .store
            .list(&prefix)?
            .into_iter()
            .map(|(key, entry)| Ok((key, serde_json::from_slice(&entry.value)?)))
            .collect::<Result<_, Error>>()?;
        for mutation in mutations {
            match mutation {
                Mutation::Put { key, value } if key.starts_with(&prefix) => {
                    let candidate: Signal = serde_json::from_slice(value)?;
                    if candidate.task_id != task_id || *key != signal_key(task_id, candidate.id) {
                        return Err(Error::InvalidKey);
                    }
                    receipts.insert(key.clone(), candidate);
                }
                Mutation::Delete { key } if key.starts_with(&prefix) => {
                    receipts.remove(key);
                }
                _ => (),
            }
        }
        let builtin = token.name() == SUBMISSION_SETTLED_NAME
            && token.version() == SUBMISSION_SETTLED_VERSION;
        if builtin {
            for (receipt_key, receipt) in &receipts {
                if receipt.task_id != task_id || *receipt_key != signal_key(task_id, receipt.id) {
                    return Err(Error::InvalidKey);
                }
            }
        }
        if builtin
            && receipts
                .values()
                .filter(|old| old.typed_key() == signal.typed_key())
                .count()
                > 1
        {
            return Err(Error::SignalKeyMismatch);
        }
        for old in receipts.values() {
            if old.typed_key() == signal.typed_key() {
                // A legacy unconstrained token can have written this reserved
                // built-in key. Do not let an incompatible value silently
                // impersonate a settled submission during notification.
                if builtin {
                    validate_builtin_receipt(old)?;
                }
                return Ok((
                    if old.expired {
                        SignalResult::Closed
                    } else {
                        SignalResult::Duplicate
                    },
                    batch,
                ));
            }
        }
        let deadline = match target.state() {
            TaskState::Waiting(WaitFor::TypedSleep {
                until_millis: Some(deadline),
                signals,
            }) if signals.contains(signal.typed_key().unwrap()) => Some(*deadline),
            _ => None,
        };
        let sampled_now = deadline.map(|_| now());
        let expired = deadline
            .zip(sampled_now)
            .is_some_and(|(deadline, now)| now >= deadline);
        if let Some((deadline, now)) = deadline.zip(sampled_now).filter(|_| !expired) {
            // Anchor the caller's clock to elapsed monotonic time so a staged
            // delivery cannot cross its deadline while waiting for the writer lock.
            let boundary = crate::runtime::conservative_deadline(clock_start, now, deadline);
            batch.checks[0].expected = stored
                .as_ref()
                .map_or(Expected::MissingBefore(boundary), |item| {
                    Expected::VersionBefore(item.version, boundary)
                });
        }
        signal.expired = expired;
        let receipt_key = signal_key(task_id, receipt_id);
        batch.checks.push(Check {
            key: receipt_key.clone(),
            expected: Expected::Missing,
        });
        batch.mutations.push(Mutation::Put {
            key: receipt_key,
            value: serde_json::to_vec(&signal)?,
        });
        batch.mutations.push(Mutation::Put {
            key: revision_key,
            value: vec![],
        });
        Ok((
            if expired {
                SignalResult::Closed
            } else {
                SignalResult::Delivered
            },
            batch,
        ))
    }

    /// Prepare a typed wake against both committed and staged receipts. The
    /// caller must atomically commit the resulting task and signal batches.
    pub(crate) fn prepare_staged_signal_wake(
        &self,
        task_id: Uuid,
        signals: &mut OwnedBatch,
        now_millis: Option<i64>,
    ) -> Result<OwnedBatch, Error> {
        let snapshot = self.load(task_id)?.ok_or(Error::SignalTargetNotFound)?;
        clear_callback_start_check(self.store, task_id)?;
        if snapshot.task.abort_requested() || snapshot.task.is_terminal() {
            return Err(Error::SignalTargetClosed);
        }
        let (keys, typed_sleep, deadline) = match snapshot.task.state() {
            TaskState::Waiting(WaitFor::TypedSignal(key)) => {
                (std::slice::from_ref(key), false, None)
            }
            TaskState::Waiting(WaitFor::TypedSleep {
                signals,
                until_millis,
            }) => (signals.as_slice(), true, *until_millis),
            _ => return Err(Error::InvalidOwnedBatch),
        };
        let revision_key = signal_revision_key(task_id);
        let revision = self.store.get(&revision_key)?;
        let prefix = signal_task_prefix(task_id);
        // A candidate has a staged mutation index, or a committed key/version.
        type Candidate = (Option<usize>, Vec<u8>, Option<u64>, Signal);
        let mut candidates: Vec<Candidate> = Vec::new();
        for (stored_key, entry) in self.store.list(&prefix)? {
            let signal: Signal = serde_json::from_slice(&entry.value)?;
            if signal.task_id != task_id || stored_key != signal_key(task_id, signal.id) {
                return Err(Error::InvalidOwnedBatch);
            }
            if signal.typed_key().is_some_and(|key| keys.contains(key)) {
                candidates.push((None, stored_key, Some(entry.version), signal));
            }
        }
        for (index, mutation) in signals.mutations.iter().enumerate() {
            if let Mutation::Put {
                key: stored_key,
                value,
            } = mutation
                && stored_key.starts_with(&prefix)
            {
                let signal: Signal = serde_json::from_slice(value)?;
                if signal.task_id != task_id || *stored_key != signal_key(task_id, signal.id) {
                    return Err(Error::InvalidOwnedBatch);
                }
                if signal.typed_key().is_some_and(|key| keys.contains(key)) {
                    candidates.push((Some(index), stored_key.clone(), None, signal));
                }
            }
        }
        for (index, (_, _, _, signal)) in candidates.iter().enumerate() {
            if signal.consumed || (!typed_sleep && signal.expired) {
                return Err(Error::SignalKeyConsumed);
            }
            if candidates[..index]
                .iter()
                .any(|(_, _, _, previous)| previous.typed_key() == signal.typed_key())
            {
                return Err(Error::SignalKeyMismatch);
            }
        }
        let selected = keys.iter().find_map(|key| {
            candidates
                .iter()
                .position(|(_, _, _, signal)| !signal.expired && signal.typed_key() == Some(key))
        });
        let deadline_due = typed_sleep
            && deadline
                .zip(now_millis)
                .is_some_and(|(deadline, now)| now >= deadline);
        if selected.is_none() && !deadline_due {
            return Err(Error::InvalidOwnedBatch);
        }
        let mut next = snapshot.clone();
        if let Some(index) = selected {
            let signal = &candidates[index].3;
            next.task
                .wake_with_signal(signal.id)
                .map_err(Error::Transition)?;
            if typed_sleep {
                next.task.set_typed_wake(crate::task::TypedWake::Signal(
                    signal.typed_key().ok_or(Error::InvalidOwnedBatch)?.clone(),
                ));
            }
        } else {
            next.task.wake().map_err(Error::Transition)?;
            next.task.set_typed_wake(if keys.is_empty() {
                crate::task::TypedWake::Time
            } else {
                crate::task::TypedWake::Expired
            });
        }
        let mut batch = self.prepare_save(&[Save::Update(&next)])?;
        batch.checks.push(Check {
            key: revision_key.clone(),
            expected: revision.map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
        });
        let mut signal_changed = false;
        if let Some(selected) = selected {
            let (index, stored_key, version, mut signal) = candidates.swap_remove(selected);
            signal.consumed = true;
            let value = serde_json::to_vec(&signal)?;
            if let Some(index) = index {
                if let Mutation::Put { value: staged, .. } = &mut signals.mutations[index] {
                    *staged = value;
                }
            } else {
                batch.checks.push(Check {
                    key: stored_key.clone(),
                    expected: Expected::Version(version.ok_or(Error::InvalidOwnedBatch)?),
                });
                batch.mutations.push(Mutation::Put {
                    key: stored_key,
                    value,
                });
                signal_changed = true;
            }
        } else {
            for key in keys {
                if candidates
                    .iter()
                    .any(|(_, _, _, signal)| signal.typed_key() == Some(key))
                {
                    continue;
                }
                let id = Uuid::new_v4();
                let signal = Signal {
                    id,
                    task_id,
                    name: key.signal.clone(),
                    payload: Value::Null,
                    consumed: false,
                    expired: true,
                    typed: Some(TypedSignal {
                        key: key.clone(),
                        version: 1,
                    }),
                };
                let stored_key = signal_key(task_id, id);
                batch.checks.push(Check {
                    key: stored_key.clone(),
                    expected: Expected::Missing,
                });
                batch.mutations.push(Mutation::Put {
                    key: stored_key,
                    value: serde_json::to_vec(&signal)?,
                });
                signal_changed = true;
            }
        }
        if signal_changed
            && !signals.mutations.iter().any(
                |mutation| matches!(mutation, Mutation::Put { key, .. } if *key == revision_key),
            )
        {
            batch.mutations.push(Mutation::Put {
                key: revision_key,
                value: vec![],
            });
        }
        Ok(batch)
    }

    /// Read a signal receipt, including whether it has been consumed.
    pub fn signal(&self, task_id: Uuid, id: Uuid) -> Result<Option<Signal>, Error> {
        self.store
            .get(&signal_key(task_id, id))?
            .map(|entry| serde_json::from_slice(&entry.value).map_err(Error::from))
            .transpose()
    }

    /// Read the committed value for a typed key, including consumed values.
    /// Migration is read-only: the original receipt remains unchanged.
    pub fn read_typed_signal(
        &self,
        task_id: Uuid,
        token: &SignalToken,
        key: impl Into<String>,
    ) -> Result<Option<Value>, Error> {
        let key = token.key(key)?;
        let mut found = None;
        for (receipt_key, entry) in self.store.list(&signal_task_prefix(task_id))? {
            let signal: Signal = serde_json::from_slice(&entry.value)?;
            if signal.typed_key() == Some(&key) {
                if is_submission_settled(token) {
                    if signal.task_id != task_id || receipt_key != signal_key(task_id, signal.id) {
                        return Err(Error::InvalidKey);
                    }
                    validate_builtin_receipt(&signal)?;
                }
                // Historical duplicate keys are ambiguous and must not silently
                // select an arbitrary value.
                if found.is_some() {
                    return Err(Error::SignalKeyMismatch);
                }
                found = Some(signal);
            }
        }
        found
            .map(|signal| {
                if signal.expired {
                    return Err(Error::SignalKeyConsumed);
                }
                token.read_value(
                    signal.payload,
                    signal.typed.expect("typed key matched").version,
                )
            })
            .transpose()
    }

    /// Read a typed key including its durable expiry state.
    pub fn read_typed_signal_state(
        &self,
        task_id: Uuid,
        token: &SignalToken,
        key: impl Into<String>,
    ) -> Result<Option<SignalState>, Error> {
        let key = token.key(key)?;
        let mut found = None;
        for (receipt_key, entry) in self.store.list(&signal_task_prefix(task_id))? {
            let signal: Signal = serde_json::from_slice(&entry.value)?;
            if signal.typed_key() == Some(&key) {
                if is_submission_settled(token) {
                    if signal.task_id != task_id || receipt_key != signal_key(task_id, signal.id) {
                        return Err(Error::InvalidKey);
                    }
                    validate_builtin_receipt(&signal)?;
                }
                if found.is_some() {
                    return Err(Error::SignalKeyMismatch);
                }
                found = Some(if signal.expired {
                    SignalState::Expired
                } else {
                    SignalState::Value(
                        token.read_value(signal.payload, signal.typed.expect("matched").version)?,
                    )
                });
            }
        }
        Ok(found)
    }

    /// Reevaluate a persisted wait using the supplied wall-clock Unix milliseconds.
    /// Call after startup and whenever a deadline, signal, or child may have changed.
    /// Waking and consuming a signal are one CAS transaction. Child versions are
    /// checked in that same transaction. Conflicts require a fresh snapshot.
    /// Pending is only a snapshot: the scheduler must arrange another reevaluation
    /// without losing concurrent notifications (for example periodic rescanning).
    pub fn poll_wait(&self, snapshot: &StoredTask, now_millis: i64) -> Result<WakeResult, Error> {
        self.evaluate_wait(snapshot, now_millis, true, None)
    }

    pub(crate) fn poll_wait_with_graph_fence(
        &self,
        snapshot: &StoredTask,
        now_millis: i64,
        graph_version: u64,
    ) -> Result<WakeResult, Error> {
        self.evaluate_wait(snapshot, now_millis, true, Some(graph_version))
    }

    pub(crate) fn wait_ready(&self, snapshot: &StoredTask, now_millis: i64) -> Result<bool, Error> {
        Ok(matches!(
            self.evaluate_wait(snapshot, now_millis, false, None)?,
            WakeResult::Woken { .. }
        ))
    }

    fn evaluate_wait(
        &self,
        snapshot: &StoredTask,
        now_millis: i64,
        commit: bool,
        graph_version: Option<u64>,
    ) -> Result<WakeResult, Error> {
        let condition = match snapshot.task.state() {
            TaskState::Waiting(condition) => condition,
            _ => return Err(Error::NotWaiting),
        };
        let callback_start = match clear_callback_start_check(self.store, snapshot.task.id()) {
            Ok(check) => check,
            Err(Error::CallbackStartBusy) => {
                #[cfg(test)]
                report_callback_wake_blocked(snapshot.task.id());
                return Ok(WakeResult::Pending);
            }
            Err(error) => return Err(error),
        };
        let mut checks = vec![
            callback_start,
            Check {
                key: key(snapshot.task.id()),
                expected: Expected::Version(snapshot.version),
            },
        ];
        let mut mutations = Vec::new();
        let mut delivered = None;
        let mut ended_children = None;
        match condition {
            WaitFor::DeadlineMillis(deadline) => {
                if now_millis < *deadline {
                    return Ok(WakeResult::Pending);
                }
            }
            WaitFor::Signal(name) => {
                let prefix = signal_task_prefix(snapshot.task.id());
                let mut found = None;
                for (k, entry) in self.store.list(&prefix)? {
                    let signal: Signal = serde_json::from_slice(&entry.value)?;
                    if !signal.consumed && signal.typed.is_none() && signal.name == *name {
                        found = Some((k, entry.version, signal));
                        break;
                    }
                }
                let Some((k, version, mut signal)) = found else {
                    return Ok(WakeResult::Pending);
                };
                signal.consumed = true;
                checks.push(Check {
                    key: k.clone(),
                    expected: Expected::Version(version),
                });
                mutations.push(Mutation::Put {
                    key: k,
                    value: serde_json::to_vec(&signal)?,
                });
                delivered = Some(signal);
            }
            WaitFor::TypedSignal(key) => {
                let revision_key = signal_revision_key(snapshot.task.id());
                let revision = self.store.get(&revision_key)?;
                checks.push(Check {
                    key: revision_key.clone(),
                    expected: revision
                        .map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
                });
                let mut found = None;
                for (k, entry) in self.store.list(&signal_task_prefix(snapshot.task.id()))? {
                    let signal: Signal = serde_json::from_slice(&entry.value)?;
                    if signal.task_id != snapshot.task.id()
                        || k != signal_key(snapshot.task.id(), signal.id)
                    {
                        return Err(Error::InvalidKey);
                    }
                    if signal.typed_key() == Some(key) {
                        if found.is_some() {
                            return Err(Error::SignalKeyMismatch);
                        }
                        if signal.consumed || signal.expired {
                            return Err(Error::SignalKeyConsumed);
                        }
                        found = Some((k, entry.version, signal));
                    }
                }
                let Some((k, version, mut signal)) = found else {
                    return Ok(WakeResult::Pending);
                };
                signal.consumed = true;
                checks.push(Check {
                    key: k.clone(),
                    expected: Expected::Version(version),
                });
                mutations.push(Mutation::Put {
                    key: k,
                    value: serde_json::to_vec(&signal)?,
                });
                mutations.push(Mutation::Put {
                    key: revision_key,
                    value: vec![],
                });
                delivered = Some(signal);
            }
            WaitFor::TypedSleep {
                until_millis,
                signals,
            } => {
                let revision_key = signal_revision_key(snapshot.task.id());
                let revision = self.store.get(&revision_key)?;
                checks.push(Check {
                    key: revision_key.clone(),
                    expected: revision.map_or(Expected::Missing, |e| Expected::Version(e.version)),
                });
                let mut records = Vec::new();
                for (k, entry) in self.store.list(&signal_task_prefix(snapshot.task.id()))? {
                    let signal: Signal = serde_json::from_slice(&entry.value)?;
                    if signal.task_id != snapshot.task.id()
                        || k != signal_key(snapshot.task.id(), signal.id)
                    {
                        return Err(Error::InvalidKey);
                    }
                    if signal.typed_key().is_some_and(|key| signals.contains(key)) {
                        if records
                            .iter()
                            .any(|(_, _, previous): &(Vec<u8>, u64, Signal)| {
                                previous.typed_key() == signal.typed_key()
                            })
                        {
                            return Err(Error::SignalKeyMismatch);
                        }
                        records.push((k, entry.version, signal));
                    }
                }
                if records.iter().any(|(_, _, s)| s.consumed) {
                    return Err(Error::SignalKeyConsumed);
                }
                let selected = signals.iter().find_map(|key| {
                    records
                        .iter()
                        .position(|(_, _, s)| !s.expired && s.typed_key() == Some(key))
                });
                if let Some(index) = selected {
                    let (k, version, mut signal) = records.swap_remove(index);
                    signal.consumed = true;
                    checks.push(Check {
                        key: k.clone(),
                        expected: Expected::Version(version),
                    });
                    mutations.push(Mutation::Put {
                        key: k,
                        value: serde_json::to_vec(&signal)?,
                    });
                    mutations.push(Mutation::Put {
                        key: revision_key.clone(),
                        value: vec![],
                    });
                    delivered = Some(signal);
                } else if until_millis.is_some_and(|deadline| now_millis >= deadline) {
                    for key in signals {
                        if records.iter().any(|(_, _, s)| s.typed_key() == Some(key)) {
                            continue;
                        }
                        let id = Uuid::new_v4();
                        let signal = Signal {
                            id,
                            task_id: snapshot.task.id(),
                            name: key.signal.clone(),
                            payload: Value::Null,
                            consumed: false,
                            expired: true,
                            typed: Some(TypedSignal {
                                key: key.clone(),
                                version: 1,
                            }),
                        };
                        let k = signal_key(snapshot.task.id(), id);
                        checks.push(Check {
                            key: k.clone(),
                            expected: Expected::Missing,
                        });
                        mutations.push(Mutation::Put {
                            key: k,
                            value: serde_json::to_vec(&signal)?,
                        });
                    }
                    if !signals.is_empty() {
                        mutations.push(Mutation::Put {
                            key: revision_key,
                            value: vec![],
                        });
                    }
                } else {
                    return Ok(WakeResult::Pending);
                }
            }
            WaitFor::Sleep {
                until_millis,
                signals,
            } => {
                // Fence a signal insertion between the scan and the wake commit.
                let revision_key = signal_revision_key(snapshot.task.id());
                let revision = self.store.get(&revision_key)?;
                checks.push(Check {
                    key: revision_key,
                    expected: revision
                        .map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
                });
                let mut found = None;
                for (k, entry) in self.store.list(&signal_task_prefix(snapshot.task.id()))? {
                    let signal: Signal = serde_json::from_slice(&entry.value)?;
                    if !signal.consumed && signal.typed.is_none() && signals.contains(&signal.name)
                    {
                        found = Some((k, entry.version, signal));
                        break;
                    }
                }
                if let Some((k, version, mut signal)) = found {
                    signal.consumed = true;
                    checks.push(Check {
                        key: k.clone(),
                        expected: Expected::Version(version),
                    });
                    mutations.push(Mutation::Put {
                        key: k,
                        value: serde_json::to_vec(&signal)?,
                    });
                    delivered = Some(signal);
                } else if !until_millis.is_some_and(|deadline| now_millis >= deadline) {
                    return Ok(WakeResult::Pending);
                }
            }
            WaitFor::Tasks(ids) | WaitFor::TasksAny(ids) | WaitFor::TasksFailFast(ids) => {
                let mut seen = std::collections::HashSet::new();
                let mut pending = false;
                let mut ended = Vec::new();
                for id in ids {
                    if *id == snapshot.task.id() || !seen.insert(*id) {
                        return Err(Error::InvalidChild);
                    }
                    let child = self.load(*id)?.ok_or(Error::InvalidChild)?;
                    if matches!(condition, WaitFor::TasksFailFast(_))
                        && child.task.owner() != &TaskOwner::Task(snapshot.task.id())
                    {
                        return Err(Error::InvalidChild);
                    }
                    if !child.task.is_terminal() {
                        pending = true;
                    } else {
                        ended.push(*id);
                    }
                    checks.push(Check {
                        key: key(*id),
                        expected: Expected::Version(child.version),
                    });
                }
                if if matches!(condition, WaitFor::TasksAny(_)) {
                    !ids.is_empty() && ended.is_empty()
                } else {
                    pending
                } {
                    return Ok(WakeResult::Pending);
                }
                ended_children = Some((ids.clone(), ended));
            }
        }
        if !commit {
            return Ok(WakeResult::Woken { signal: delivered });
        }
        let mut next = snapshot.task.clone();
        match &delivered {
            Some(signal) => next.wake_with_signal(signal.id),
            None => next.wake(),
        }
        .map_err(Error::Transition)?;
        if let Some((on, ended)) = ended_children {
            next.set_child_wake(on, ended);
        }
        if let WaitFor::TypedSleep { signals, .. } = condition {
            let wake = match &delivered {
                Some(signal) => {
                    crate::task::TypedWake::Signal(signal.typed_key().expect("typed wake").clone())
                }
                None if signals.is_empty() => crate::task::TypedWake::Time,
                None => crate::task::TypedWake::Expired,
            };
            next.set_typed_wake(wake);
        }
        mutations.push(Mutation::Put {
            key: key(next.id()),
            value: serde_json::to_vec(&next)?,
        });
        if let Some(version) = graph_version {
            self.store.commit_with_prefix_put_fence(
                &checks,
                &mutations,
                &[
                    PREFIX,
                    crate::ownership_edge::PREFIX,
                    b"runtime/conversation/",
                ],
                version,
            )?;
        } else {
            self.store.commit(&checks, &mutations)?;
        }
        Ok(WakeResult::Woken { signal: delivered })
    }

    /// Commit Ready -> Running using the snapshot version. Only the winner of
    /// this CAS may execute. Reload before any subsequent update.
    pub fn reserve(&self, snapshot: &StoredTask) -> Result<(), Error> {
        let mut next = snapshot.clone();
        let checks = self.refresh_child_wake(&mut next.task)?;
        next.task.start().map_err(Error::Transition)?;
        let mut batch = self.prepare_save(&[Save::Update(&next)])?;
        batch.checks.extend(checks);
        self.store.commit(&batch.checks, &batch.mutations)?;
        Ok(())
    }

    /// Refresh child wake information immediately before reservation; the
    /// returned checks must commit with the task's Running transition.
    pub(crate) fn refresh_child_wake(&self, task: &mut Task) -> Result<Vec<Check>, Error> {
        let Some(ids) = task.wake_children().map(<[Uuid]>::to_vec) else {
            return Ok(Vec::new());
        };
        let mut seen = std::collections::HashSet::new();
        let mut checks = Vec::with_capacity(ids.len());
        let mut ended = Vec::new();
        for id in ids {
            if id == task.id() || !seen.insert(id) {
                return Err(Error::InvalidChild);
            }
            let child = self.load(id)?.ok_or(Error::InvalidChild)?;
            if child.task.is_terminal() {
                ended.push(id);
            }
            checks.push(Check {
                key: key(id),
                expected: Expected::Version(child.version),
            });
        }
        task.refresh_ended_children(ended);
        Ok(checks)
    }

    /// Commit Running -> Ready after the host has fenced the former executor.
    /// Conflicts are surfaced, never retried against a potentially new execution.
    pub fn recover(&self, snapshot: &StoredTask) -> Result<(), Error> {
        self.transition(snapshot, Task::recover)
    }

    fn transition(
        &self,
        snapshot: &StoredTask,
        f: fn(&mut Task) -> Result<(), TransitionError>,
    ) -> Result<(), Error> {
        let mut next = snapshot.clone();
        f(&mut next.task).map_err(Error::Transition)?;
        self.save(&[Save::Update(&next)])
    }
}

fn signal_task_prefix(task_id: Uuid) -> Vec<u8> {
    let mut key = SIGNAL_PREFIX.to_vec();
    key.extend_from_slice(task_id.as_bytes());
    key
}

pub(crate) fn callback_start_key(task_id: Uuid) -> Vec<u8> {
    format!("durable/task-callback-start/{task_id}").into_bytes()
}

pub(crate) fn reentrant_callback(store: &Store, task: Uuid) -> Result<bool, Error> {
    let Some(marker) = durable_store::controls::current_callback_marker() else {
        return Ok(false);
    };
    Ok(store
        .get(&callback_start_key(task))?
        .is_some_and(|entry| entry.value == marker))
}

pub(crate) fn clear_callback_start_check(store: &Store, task_id: Uuid) -> Result<Check, Error> {
    let key = callback_start_key(task_id);
    if store.get(&key)?.is_some() {
        return Err(Error::CallbackStartBusy);
    }
    Ok(Check {
        key,
        expected: Expected::Missing,
    })
}

/// Only the task-signal portion of a callback-start transaction. The caller
/// must atomically compose conversation, claim, owner, identity, abort, late
/// outcome and graph checks; this batch alone never authorizes a callback.
#[allow(dead_code)]
pub(crate) fn prepare_callback_start_task_fence(
    store: &Store,
    task_id: Uuid,
    marker: &[u8],
) -> Result<OwnedBatch, Error> {
    if marker.is_empty() {
        return Err(Error::InvalidKey);
    }
    let task = TaskStore::new(store)
        .load(task_id)?
        .ok_or(Error::SignalTargetNotFound)?;
    let rev_key = signal_revision_key(task_id);
    let revision = store.get(&rev_key)?;
    Ok(OwnedBatch {
        checks: vec![
            Check {
                key: key(task_id),
                expected: Expected::Version(task.version),
            },
            Check {
                key: callback_start_key(task_id),
                expected: Expected::Missing,
            },
            Check {
                key: rev_key,
                expected: revision
                    .map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
            },
        ],
        mutations: vec![Mutation::Put {
            key: callback_start_key(task_id),
            value: marker.to_vec(),
        }],
    })
}

#[allow(dead_code)]
pub(crate) fn prepare_callback_finish_task_fence(
    store: &Store,
    task_id: Uuid,
    marker: &[u8],
) -> Result<OwnedBatch, Error> {
    let key = callback_start_key(task_id);
    let entry = store.get(&key)?.ok_or(Error::InvalidKey)?;
    if marker.is_empty() || entry.value != marker {
        return Err(Error::InvalidKey);
    }
    Ok(OwnedBatch {
        checks: vec![Check {
            key: key.clone(),
            expected: Expected::Version(entry.version),
        }],
        mutations: vec![Mutation::Delete { key }],
    })
}

/// Prepare task markers for the exact child-to-root ownership chain. The
/// caller must compose conversation, claim, attempt, identity, park and graph
/// evidence in the same checked commit. This batch alone does not authorize a
/// callback. Persist the returned chain with the claim before dispatch so that
/// recovery can retire exactly these markers.
#[allow(dead_code)]
pub(crate) fn prepare_callback_start_ancestry_fence(
    store: &Store,
    task_id: Uuid,
    conversation: Uuid,
    marker: &[u8],
) -> Result<(Vec<Uuid>, OwnedBatch), Error> {
    let tasks = TaskStore::new(store);
    let mut current = task_id;
    let mut seen = std::collections::HashSet::new();
    let mut ancestry = Vec::new();
    let mut batch = OwnedBatch {
        checks: Vec::new(),
        mutations: Vec::new(),
    };
    loop {
        if !seen.insert(current) || seen.len() > 1024 {
            return Err(Error::InvalidKey);
        }
        let task = tasks.load(current)?.ok_or(Error::SignalTargetNotFound)?;
        batch.checks.push(Check {
            key: key(current),
            expected: Expected::Version(task.version),
        });
        let part = prepare_callback_start_task_fence(store, current, marker)?;
        batch.checks.extend(part.checks);
        batch.mutations.extend(part.mutations);
        ancestry.push(current);
        match task.task.owner() {
            crate::task::TaskOwner::Task(parent) => current = *parent,
            crate::task::TaskOwner::Conversation(id) if *id == conversation => break,
            crate::task::TaskOwner::Conversation(_) => return Err(Error::InvalidKey),
        }
    }
    Ok((ancestry, batch))
}

/// Prepare marker retirement for an authenticated persisted chain. The caller
/// must fence that chain against its immutable claim in the same commit; this
/// helper cannot detect a substituted subset. Duplicates and a missing or
/// changed marker fail closed.
#[allow(dead_code)]
pub(crate) fn prepare_callback_finish_ancestry_fence(
    store: &Store,
    ancestry: &[Uuid],
    marker: &[u8],
) -> Result<OwnedBatch, Error> {
    if ancestry.is_empty() || ancestry.len() > 1024 {
        return Err(Error::InvalidKey);
    }
    let mut seen = std::collections::HashSet::new();
    let mut batch = OwnedBatch {
        checks: Vec::new(),
        mutations: Vec::new(),
    };
    for &id in ancestry {
        if !seen.insert(id) {
            return Err(Error::InvalidKey);
        }
        let part = prepare_callback_finish_task_fence(store, id, marker)?;
        batch.checks.extend(part.checks);
        batch.mutations.extend(part.mutations);
    }
    Ok(batch)
}

/// Ancestor versions and signal revisions read before a nested callback claim.
/// It must be committed atomically with the identity-bound claim; neither this
/// sidecar nor its presence authorizes execution.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NestedCancelClaimAncestry {
    version: u32,
    conversation: Uuid,
    task: Uuid,
    claim: Uuid,
    ancestry: Vec<NestedCancelClaimAncestor>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NestedCancelClaimAncestor {
    task: Uuid,
    version: u64,
    signal_revision: Option<u64>,
}
fn nested_cancel_claim_ancestry_key(task: Uuid) -> Vec<u8> {
    format!("durable/task-callback-claim-ancestry/{task}").into_bytes()
}

/// Compose with the nested claim and its conversation, park, registration and
/// lease checks. The signal revision and owner versions are captured together.
#[allow(dead_code)]
pub(crate) fn prepare_nested_cancel_claim_ancestry(
    store: &Store,
    task: Uuid,
    conversation: Uuid,
    claim: Uuid,
) -> Result<OwnedBatch, Error> {
    if claim.is_nil() || task.is_nil() || conversation.is_nil() {
        return Err(Error::InvalidKey);
    }
    let (ancestry, _) =
        prepare_callback_start_ancestry_fence(store, task, conversation, b"claim-probe")?;
    if ancestry.len() < 2 {
        return Err(Error::InvalidKey);
    }
    let tasks = TaskStore::new(store);
    let mut checks = Vec::new();
    let mut records = Vec::with_capacity(ancestry.len());
    for (i, id) in ancestry.iter().enumerate() {
        let record = tasks.load(*id)?.ok_or(Error::SignalTargetNotFound)?;
        let expected_owner = ancestry
            .get(i + 1)
            .map_or(TaskOwner::Conversation(conversation), |parent| {
                TaskOwner::Task(*parent)
            });
        if record.task.owner() != &expected_owner {
            return Err(Error::InvalidKey);
        }
        let revision = store.get(&signal_revision_key(*id))?.map(|e| e.version);
        checks.push(Check {
            key: key(*id),
            expected: Expected::Version(record.version),
        });
        checks.push(Check {
            key: signal_revision_key(*id),
            expected: revision.map_or(Expected::Missing, Expected::Version),
        });
        records.push(NestedCancelClaimAncestor {
            task: *id,
            version: record.version,
            signal_revision: revision,
        });
    }
    let evidence = NestedCancelClaimAncestry {
        version: 1,
        conversation,
        task,
        claim,
        ancestry: records,
    };
    let key = nested_cancel_claim_ancestry_key(task);
    checks.push(Check {
        key: key.clone(),
        expected: Expected::Missing,
    });
    Ok(OwnedBatch {
        checks,
        mutations: vec![Mutation::Put {
            key,
            value: serde_json::to_vec(&evidence)?,
        }],
    })
}

/// Read and authenticate the stored nested claim ancestry for recovery. The
/// caller must check the claim/sidecar commit identity and compose these checks
/// with the terminal transition and any exact marker retirement.
#[allow(dead_code)]
pub(crate) fn read_nested_cancel_claim_ancestry(
    store: &Store,
    task: Uuid,
    claim: Uuid,
) -> Result<(Uuid, Vec<Uuid>, Vec<Check>, u64), Error> {
    let key = nested_cancel_claim_ancestry_key(task);
    let entry = store.get(&key)?.ok_or(Error::InvalidKey)?;
    let evidence: NestedCancelClaimAncestry = serde_json::from_slice(&entry.value)?;
    if evidence.version != 1
        || evidence.task != task
        || evidence.claim != claim
        || claim.is_nil()
        || evidence.conversation.is_nil()
        || evidence.ancestry.len() < 2
        || evidence.ancestry.len() > 1024
    {
        return Err(Error::InvalidKey);
    }
    let chain: Vec<_> = evidence.ancestry.iter().map(|item| item.task).collect();
    let mut seen = std::collections::HashSet::new();
    if chain.first() != Some(&task) || chain.iter().any(|id| id.is_nil() || !seen.insert(*id)) {
        return Err(Error::InvalidKey);
    }
    let tasks = TaskStore::new(store);
    let mut checks = vec![Check {
        key,
        expected: Expected::Version(entry.version),
    }];
    for (i, id) in chain.iter().enumerate() {
        let record = tasks.load(*id)?.ok_or(Error::SignalTargetNotFound)?;
        let expected_owner = chain
            .get(i + 1)
            .map_or(TaskOwner::Conversation(evidence.conversation), |parent| {
                TaskOwner::Task(*parent)
            });
        if record.task.owner() != &expected_owner {
            return Err(Error::InvalidKey);
        }
        checks.push(Check {
            key: self::key(*id),
            expected: Expected::Version(record.version),
        });
    }
    Ok((evidence.conversation, chain, checks, entry.version))
}

/// Start the exact chain and recheck the immutable sidecar and every version
/// observed at claim time. Compose with the live claim, park, identity and
/// conversation marker checks in the same transaction before first poll.
#[allow(dead_code)]
pub(crate) fn prepare_nested_cancel_claimed_ancestry_start(
    store: &Store,
    task: Uuid,
    conversation: Uuid,
    claim: Uuid,
    marker: &[u8],
) -> Result<OwnedBatch, Error> {
    let key = nested_cancel_claim_ancestry_key(task);
    let entry = store.get(&key)?.ok_or(Error::InvalidKey)?;
    let evidence: NestedCancelClaimAncestry = serde_json::from_slice(&entry.value)?;
    if evidence.version != 1
        || evidence.task != task
        || evidence.conversation != conversation
        || evidence.claim != claim
        || claim.is_nil()
        || evidence.ancestry.len() < 2
        || evidence.ancestry.len() > 1024
    {
        return Err(Error::InvalidKey);
    }
    let (chain, _) = prepare_callback_start_ancestry_fence(store, task, conversation, marker)?;
    if chain
        .iter()
        .copied()
        .ne(evidence.ancestry.iter().map(|e| e.task))
    {
        return Err(Error::InvalidKey);
    }
    let mut batch =
        prepare_authenticated_callback_ancestry_start(store, task, conversation, marker)?;
    batch.checks.push(Check {
        key,
        expected: Expected::Version(entry.version),
    });
    for ancestor in evidence.ancestry {
        batch.checks.push(Check {
            key: self::key(ancestor.task),
            expected: Expected::Version(ancestor.version),
        });
        batch.checks.push(Check {
            key: signal_revision_key(ancestor.task),
            expected: ancestor
                .signal_revision
                .map_or(Expected::Missing, Expected::Version),
        });
    }
    Ok(batch)
}

/// Immutable ancestry evidence. This authenticates which markers a future
/// cleanup must retire; it does not by itself authorize external dispatch.
#[derive(Serialize, Deserialize)]
struct CallbackAncestryEvidence {
    version: u32,
    conversation: Uuid,
    task: Uuid,
    marker: Vec<u8>,
    ancestry: Vec<Uuid>,
}
fn callback_ancestry_key(task: Uuid) -> Vec<u8> {
    format!("durable/task-callback-ancestry/{task}").into_bytes()
}

#[allow(dead_code)]
pub(crate) fn prepare_authenticated_callback_ancestry_start(
    store: &Store,
    task: Uuid,
    conversation: Uuid,
    marker: &[u8],
) -> Result<OwnedBatch, Error> {
    let (ancestry, mut batch) =
        prepare_callback_start_ancestry_fence(store, task, conversation, marker)?;
    let evidence = CallbackAncestryEvidence {
        version: 1,
        conversation,
        task,
        marker: marker.to_vec(),
        ancestry,
    };
    let key = callback_ancestry_key(task);
    batch.checks.push(Check {
        key: key.clone(),
        expected: Expected::Missing,
    });
    batch.mutations.push(Mutation::Put {
        key,
        value: serde_json::to_vec(&evidence)?,
    });
    Ok(batch)
}

/// Finish exactly the persisted chain. The caller must also authenticate its
/// immutable claim, callback outcome, identity, and active attempt atomically.
#[allow(dead_code)]
pub(crate) fn prepare_authenticated_callback_ancestry_finish(
    store: &Store,
    task: Uuid,
    conversation: Uuid,
    marker: &[u8],
) -> Result<OwnedBatch, Error> {
    let key = callback_ancestry_key(task);
    let entry = store.get(&key)?.ok_or(Error::InvalidKey)?;
    let evidence: CallbackAncestryEvidence = serde_json::from_slice(&entry.value)?;
    if evidence.version != 1
        || evidence.task != task
        || evidence.conversation != conversation
        || evidence.marker != marker
        || evidence.ancestry.first() != Some(&task)
    {
        return Err(Error::InvalidKey);
    }
    let mut batch = prepare_callback_finish_ancestry_fence(store, &evidence.ancestry, marker)?;
    batch.checks.push(Check {
        key,
        expected: Expected::Version(entry.version),
    });
    Ok(batch)
}

pub(crate) fn signal_revision_key(task_id: Uuid) -> Vec<u8> {
    let mut key = SIGNAL_REVISION_PREFIX.to_vec();
    key.extend_from_slice(task_id.as_bytes());
    key
}

fn signal_key(task_id: Uuid, id: Uuid) -> Vec<u8> {
    let mut key = signal_task_prefix(task_id);
    key.extend_from_slice(id.as_bytes());
    key
}

pub(crate) fn key(id: Uuid) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.extend_from_slice(id.as_bytes());
    key
}

pub(crate) fn decode(key_bytes: &[u8], entry: Entry) -> Result<StoredTask, Error> {
    let task: Task = serde_json::from_slice(&entry.value)?;
    if key(task.id()) != key_bytes {
        return Err(Error::InvalidKey);
    }
    Ok(StoredTask {
        task,
        version: entry.version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{Checkpoint, TaskOwner, TaskState, WaitFor};
    use serde_json::json;

    #[test]
    fn wall_clock_is_refreshed_after_delivery_conflict() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let t = task(1, 3);
        let token = SignalToken::new("answer", 1).unwrap();
        ts.save(&[Save::Create(&t)]).unwrap();
        ts.reserve(&ts.load(t.id()).unwrap().unwrap()).unwrap();
        ts.update(&ts.load(t.id()).unwrap().unwrap(), |t| {
            t.wait(WaitFor::TypedSleep {
                until_millis: Some(100),
                signals: vec![token.key("a").unwrap()],
            })
        })
        .unwrap();
        let mut calls = 0;
        let result = ts
            .deliver_signal_with_clock(t.id(), &token, "a", json!(1), || {
                calls += 1;
                if calls == 1 {
                    // Change the revision after it was read, forcing a retry.
                    assert_eq!(
                        ts.deliver_signal_at(t.id(), &token, "other", json!(2), 99)
                            .unwrap(),
                        SignalResult::Delivered
                    );
                    99
                } else {
                    100
                }
            })
            .unwrap();
        assert_eq!(calls, 2);
        assert_eq!(result, SignalResult::Closed);
        assert_eq!(
            ts.read_typed_signal_state(t.id(), &token, "a").unwrap(),
            Some(SignalState::Expired)
        );
    }
    use std::sync::{Arc, Barrier};

    fn task(id: u128, limit: u32) -> Task {
        Task::new(
            Uuid::from_u128(id),
            TaskOwner::Conversation(Uuid::from_u128(100)),
            Checkpoint {
                phase: "first".into(),
                data: json!({"n": 1}),
            },
            limit,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn ancestry_callback_fences_each_owner_and_detects_scoped_reentry() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let root = task(1, 3);
        let child = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Task(root.id()),
            Checkpoint {
                phase: "first".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap();
        let unrelated = task(3, 3);
        ts.save(&[
            Save::Create(&root),
            Save::Create(&child),
            Save::Create(&unrelated),
        ])
        .unwrap();
        let marker = b"authenticated claim";
        let (ancestry, prepared) =
            prepare_callback_start_ancestry_fence(&db, child.id(), Uuid::from_u128(100), marker)
                .unwrap();
        assert_eq!(ancestry, vec![child.id(), root.id()]);
        db.commit(&prepared.checks, &prepared.mutations).unwrap();
        for id in &ancestry {
            assert!(matches!(
                ts.update(&ts.load(*id).unwrap().unwrap(), |t| t.request_abort()),
                Err(Error::CallbackStartBusy)
            ));
        }
        ts.update(&ts.load(unrelated.id()).unwrap().unwrap(), |t| {
            t.request_abort()
        })
        .unwrap();
        durable_store::controls::with_callback_context(
            Uuid::from_u128(100),
            child.id(),
            marker.to_vec(),
            async {
                assert!(reentrant_callback(&db, child.id()).unwrap());
                assert!(reentrant_callback(&db, root.id()).unwrap());
                assert!(!reentrant_callback(&db, unrelated.id()).unwrap());
                let scope = durable_store::controls::CallbackScope::current().unwrap();
                std::thread::scope(|threads| {
                    threads
                        .spawn(|| {
                            let rt = tokio::runtime::Builder::new_current_thread()
                                .build()
                                .unwrap();
                            rt.block_on(scope.scope(async {
                                assert!(reentrant_callback(&db, root.id()).unwrap());
                            }));
                        })
                        .join()
                        .unwrap();
                });
            },
        )
        .await;
        assert!(prepare_callback_finish_ancestry_fence(&db, &ancestry, b"wrong").is_err());
        let finish = prepare_callback_finish_ancestry_fence(&db, &ancestry, marker).unwrap();
        db.commit(&finish.checks, &finish.mutations).unwrap();
        assert!(!reentrant_callback(&db, root.id()).unwrap());
    }

    #[test]
    fn ancestry_callback_start_rejects_changed_owner_before_commit() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let root = task(1, 3);
        let child = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Task(root.id()),
            Checkpoint {
                phase: "first".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap();
        ts.save(&[Save::Create(&root), Save::Create(&child)])
            .unwrap();
        let (_, prepared) =
            prepare_callback_start_ancestry_fence(&db, child.id(), Uuid::from_u128(100), b"claim")
                .unwrap();
        ts.update(&ts.load(root.id()).unwrap().unwrap(), |t| t.request_abort())
            .unwrap();
        assert!(matches!(
            db.commit(&prepared.checks, &prepared.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(db.get(&callback_start_key(child.id())).unwrap().is_none());
        assert!(
            prepare_callback_start_ancestry_fence(&db, child.id(), Uuid::new_v4(), b"claim")
                .is_err()
        );
    }

    #[test]
    fn nested_claim_fences_ancestor_signal_and_attempt_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Store::open(&path).unwrap();
        let ts = TaskStore::new(&db);
        let root = task(1, 3);
        let child = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Task(root.id()),
            Checkpoint {
                phase: "first".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap();
        ts.save(&[Save::Create(&root), Save::Create(&child)])
            .unwrap();
        let conversation = Uuid::from_u128(100);
        let claim = Uuid::new_v4();
        let batch =
            prepare_nested_cancel_claim_ancestry(&db, child.id(), conversation, claim).unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        drop(db);
        let db = Store::open(&path).unwrap();
        assert!(
            prepare_nested_cancel_claimed_ancestry_start(
                &db,
                child.id(),
                conversation,
                Uuid::new_v4(),
                b"marker"
            )
            .is_err()
        );
        let start = prepare_nested_cancel_claimed_ancestry_start(
            &db,
            child.id(),
            conversation,
            claim,
            b"marker",
        )
        .unwrap();
        db.commit(&start.checks, &[]).unwrap();
        let rev = signal_revision_key(root.id());
        db.commit(
            &[Check {
                key: rev.clone(),
                expected: Expected::Missing,
            }],
            &[Mutation::Put {
                key: rev,
                value: vec![1],
            }],
        )
        .unwrap();
        assert!(matches!(
            db.commit(&start.checks, &start.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(db.get(&callback_start_key(child.id())).unwrap().is_none());
        let stale = prepare_nested_cancel_claimed_ancestry_start(
            &db,
            child.id(),
            conversation,
            claim,
            b"marker",
        )
        .unwrap();
        assert!(matches!(
            db.commit(&stale.checks, &stale.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
    }

    #[test]
    fn authenticated_callback_ancestry_survives_reopen_and_rejects_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Store::open(&path).unwrap();
        let ts = TaskStore::new(&db);
        let root = task(1, 3);
        let child = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Task(root.id()),
            Checkpoint {
                phase: "first".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap();
        ts.save(&[Save::Create(&root), Save::Create(&child)])
            .unwrap();
        let conversation = Uuid::from_u128(100);
        let start =
            prepare_authenticated_callback_ancestry_start(&db, child.id(), conversation, b"claim")
                .unwrap();
        db.commit(&start.checks, &start.mutations).unwrap();
        drop(db);
        let db = Store::open(&path).unwrap();
        assert!(
            prepare_authenticated_callback_ancestry_finish(&db, child.id(), conversation, b"wrong")
                .is_err()
        );
        let finish =
            prepare_authenticated_callback_ancestry_finish(&db, child.id(), conversation, b"claim")
                .unwrap();
        let key = callback_ancestry_key(child.id());
        let entry = db.get(&key).unwrap().unwrap();
        db.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[Mutation::Put {
                key,
                value: entry.value,
            }],
        )
        .unwrap();
        assert!(matches!(
            db.commit(&finish.checks, &finish.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        let finish =
            prepare_authenticated_callback_ancestry_finish(&db, child.id(), conversation, b"claim")
                .unwrap();
        db.commit(&finish.checks, &finish.mutations).unwrap();
        assert!(db.get(&callback_start_key(root.id())).unwrap().is_none());
        assert!(db.get(&callback_start_key(child.id())).unwrap().is_none());
        assert!(
            db.get(&callback_ancestry_key(child.id()))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn callback_start_fences_generic_task_save_and_update() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let target = task(1, 3);
        let unrelated = task(2, 3);
        ts.save(&[Save::Create(&target), Save::Create(&unrelated)])
            .unwrap();
        let mut candidate = ts.load(target.id()).unwrap().unwrap();
        candidate.task.request_abort().unwrap();
        let prepared = ts.prepare_save(&[Save::Update(&candidate)]).unwrap();
        let start = prepare_callback_start_task_fence(&db, target.id(), b"claim").unwrap();
        db.commit(&start.checks, &start.mutations).unwrap();
        assert!(matches!(
            db.commit(&prepared.checks, &prepared.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(matches!(
            ts.update(&ts.load(target.id()).unwrap().unwrap(), |task| task
                .request_abort()),
            Err(Error::CallbackStartBusy)
        ));
        ts.update(&ts.load(unrelated.id()).unwrap().unwrap(), |task| {
            task.request_abort()
        })
        .unwrap();
        assert!(
            !ts.load(target.id())
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
        let finish = prepare_callback_finish_task_fence(&db, target.id(), b"claim").unwrap();
        db.commit(&finish.checks, &finish.mutations).unwrap();
        ts.update(&ts.load(target.id()).unwrap().unwrap(), |task| {
            task.request_abort()
        })
        .unwrap();
        assert!(
            ts.load(target.id())
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
    }

    #[test]
    fn callback_start_fences_public_signal_and_wake_transactions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Store::open(&path).unwrap();
        let other = Store::open(&path).unwrap();
        let ts = TaskStore::new(&db);
        let t = task(1, 3);
        let unrelated = task(2, 3);
        let token = SignalToken::new("answer", 1).unwrap();
        ts.save(&[Save::Create(&t), Save::Create(&unrelated)])
            .unwrap();
        ts.reserve(&ts.load(t.id()).unwrap().unwrap()).unwrap();
        ts.update(&ts.load(t.id()).unwrap().unwrap(), |task| {
            task.wait(WaitFor::TypedSignal(token.key("k").unwrap()))
        })
        .unwrap();
        let stale_start = prepare_callback_start_task_fence(&other, t.id(), b"claim-old").unwrap();
        let legacy = Signal::new(Uuid::new_v4(), t.id(), "other".into(), json!(1));
        ts.send_signal(&legacy).unwrap();
        assert!(matches!(
            other.commit(&stale_start.checks, &stale_start.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        let (_, prepared) = ts
            .prepare_signal_at(t.id(), &token, "k", json!(2), || 0)
            .unwrap();
        let start = prepare_callback_start_task_fence(&other, t.id(), b"claim").unwrap();
        other.commit(&start.checks, &start.mutations).unwrap();
        assert!(matches!(
            db.commit(&prepared.checks, &prepared.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(matches!(
            ts.deliver_signal_at(t.id(), &token, "k", json!(2), 0),
            Err(Error::CallbackStartBusy)
        ));
        assert!(matches!(
            ts.send_signal(&Signal::new(Uuid::new_v4(), t.id(), "x".into(), json!(3))),
            Err(Error::CallbackStartBusy)
        ));
        assert!(matches!(
            ts.poll_wait(&ts.load(t.id()).unwrap().unwrap(), 0).unwrap(),
            WakeResult::Pending
        ));
        ts.send_signal(&Signal::new(
            Uuid::new_v4(),
            unrelated.id(),
            "other".into(),
            json!(4),
        ))
        .unwrap();
        assert!(matches!(
            prepare_callback_finish_task_fence(&other, t.id(), b"other"),
            Err(Error::InvalidKey)
        ));
        let finish = prepare_callback_finish_task_fence(&other, t.id(), b"claim").unwrap();
        other.commit(&finish.checks, &finish.mutations).unwrap();
        assert_eq!(
            ts.deliver_signal_at(t.id(), &token, "k", json!(2), 0)
                .unwrap(),
            SignalResult::Delivered
        );
        assert!(matches!(
            ts.poll_wait(&ts.load(t.id()).unwrap().unwrap(), 0).unwrap(),
            WakeResult::Woken { .. }
        ));
    }

    #[test]
    fn submission_builtin_uses_canonical_schema_without_changing_generic_versions() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let t = task(1, 3);
        ts.save(&[Save::Create(&t)]).unwrap();
        let v1 = SignalToken::new(SUBMISSION_SETTLED_NAME, 1).unwrap();
        let v2 = SignalToken::new(SUBMISSION_SETTLED_NAME, 2).unwrap();
        let valid = json!({"id":Uuid::new_v4(), "conversationId":Uuid::new_v4(), "type":"input", "status":"unanswered", "reason":"failed"});
        assert_eq!(
            ts.deliver_signal_at(t.id(), &v1, "forged", json!({"id":"invalid"}), 0)
                .unwrap(),
            SignalResult::Invalid
        );
        assert_eq!(
            ts.deliver_signal_at(t.id(), &v1, "valid", valid.clone(), 0)
                .unwrap(),
            SignalResult::Delivered
        );
        assert_eq!(
            ts.deliver_signal_at(t.id(), &v1, "valid", valid.clone(), 0)
                .unwrap(),
            SignalResult::Duplicate
        );
        // A generic v2 caller still gets Duplicate for a v1 key, without a migration.
        assert_eq!(
            ts.deliver_signal_at(t.id(), &v2, "valid", json!({"arbitrary":true}), 0)
                .unwrap(),
            SignalResult::Duplicate
        );
        assert_eq!(
            ts.deliver_signal_at(t.id(), &v2, "generic", json!({"arbitrary":true}), 0)
                .unwrap(),
            SignalResult::Delivered
        );
        assert_eq!(
            ts.deliver_signal_at(t.id(), &v2, "generic", json!(null), 0)
                .unwrap(),
            SignalResult::Duplicate
        );
    }

    #[test]
    fn builtin_send_and_reads_reject_tampered_payloads() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let t = task(1, 3);
        ts.save(&[Save::Create(&t)]).unwrap();
        let builtin = crate::runtime::submission_settled_signal();
        let unconstrained = SignalToken::new(SUBMISSION_SETTLED_NAME, 1).unwrap();
        let generic = SignalToken::new(SUBMISSION_SETTLED_NAME, 2).unwrap();
        let valid = json!({"id":Uuid::new_v4(), "conversationId":Uuid::new_v4(), "type":"input", "status":"unanswered", "reason":"failed"});
        let mut receipt =
            Signal::new_typed(Uuid::new_v4(), t.id(), &builtin, "tampered", valid).unwrap();
        receipt.payload = json!({"id":"not-a-uuid"});
        assert!(matches!(
            ts.send_signal(&receipt),
            Err(Error::InvalidSignalPayload)
        ));
        assert!(ts.signal(t.id(), receipt.id).unwrap().is_none());
        // A pre-hardening record remains readable by unrelated generic v2,
        // but neither form of the builtin v1 token may read its payload.
        db.commit(
            &[],
            &[Mutation::Put {
                key: signal_key(t.id(), receipt.id),
                value: serde_json::to_vec(&receipt).unwrap(),
            }],
        )
        .unwrap();
        for token in [&builtin, &unconstrained] {
            assert!(matches!(
                ts.read_typed_signal(t.id(), token, "tampered"),
                Err(Error::InvalidSignalPayload)
            ));
            assert!(matches!(
                ts.read_typed_signal_state(t.id(), token, "tampered"),
                Err(Error::InvalidSignalPayload)
            ));
        }
        // The legacy v2 reader needs its usual migration to read a v1 value.
        let generic = generic.with_migration(|value, _| Ok(value));
        assert_eq!(
            ts.read_typed_signal(t.id(), &generic, "tampered").unwrap(),
            Some(receipt.payload)
        );
    }

    #[test]
    fn builtin_expired_receipts_require_version_one() {
        for version in [0, 1, 2, u32::MAX] {
            let db = Store::memory().unwrap();
            let ts = TaskStore::new(&db);
            let t = task(1, 3);
            ts.save(&[Save::Create(&t)]).unwrap();
            let token = crate::runtime::submission_settled_signal();
            let receipt = Signal {
                id: Uuid::new_v4(),
                task_id: t.id(),
                name: token.name().into(),
                payload: Value::Null,
                consumed: false,
                expired: true,
                typed: Some(TypedSignal {
                    key: token.key("expired").unwrap(),
                    version,
                }),
            };
            db.commit(
                &[],
                &[Mutation::Put {
                    key: signal_key(t.id(), receipt.id),
                    value: serde_json::to_vec(&receipt).unwrap(),
                }],
            )
            .unwrap();
            let value = json!({"id":Uuid::new_v4(), "conversationId":Uuid::new_v4(), "type":"input", "status":"unanswered", "reason":"failed"});
            let result = ts.deliver_signal_at(t.id(), &token, "expired", value, 0);
            if version == 1 {
                assert_eq!(result.unwrap(), SignalResult::Closed);
                assert_eq!(
                    ts.read_typed_signal_state(t.id(), &token, "expired")
                        .unwrap(),
                    Some(SignalState::Expired)
                );
                assert!(matches!(
                    ts.read_typed_signal(t.id(), &token, "expired"),
                    Err(Error::SignalKeyConsumed)
                ));
            } else {
                assert!(
                    matches!(result, Err(Error::NewerSignalVersion { stored, current: 1 }) if stored == version)
                );
                assert!(
                    matches!(ts.read_typed_signal_state(t.id(), &token, "expired"), Err(Error::NewerSignalVersion { stored, current: 1 }) if stored == version)
                );
                assert!(
                    matches!(ts.read_typed_signal(t.id(), &token, "expired"), Err(Error::NewerSignalVersion { stored, current: 1 }) if stored == version)
                );
            }
            let generic = SignalToken::new(SUBMISSION_SETTLED_NAME, 2).unwrap();
            assert_eq!(
                ts.deliver_signal_at(t.id(), &generic, "expired", json!(null), 0)
                    .unwrap(),
                SignalResult::Closed
            );
            assert_eq!(
                ts.read_typed_signal_state(t.id(), &generic, "expired")
                    .unwrap(),
                Some(SignalState::Expired)
            );
        }
    }

    fn owned(id: u128, parent: u128) -> Task {
        Task::new(
            Uuid::from_u128(id),
            TaskOwner::Task(Uuid::from_u128(parent)),
            Checkpoint {
                phase: "first".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
    }

    #[test]
    fn task_creation_rejects_cycles_and_fences_missing_owners() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let a = owned(1, 2);
        let b = owned(2, 1);
        assert!(matches!(
            ts.save(&[Save::Create(&a), Save::Create(&b)]),
            Err(Error::InvalidOwnership)
        ));
        assert!(ts.list().unwrap().is_empty());

        let first = ts.prepare_save(&[Save::Create(&a)]).unwrap();
        let second = ts.prepare_save(&[Save::Create(&b)]).unwrap();
        db.commit(&first.checks, &first.mutations).unwrap();
        assert!(matches!(
            db.commit(&second.checks, &second.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(matches!(
            ts.save(&[Save::Create(&b)]),
            Err(Error::InvalidOwnership)
        ));
        assert!(ts.load(b.id()).unwrap().is_none());
        let registered_b = b.clone().registered("job".into(), true);
        assert!(matches!(
            ts.commit_owned_tasks(
                &TaskOwner::Task(a.id()),
                b"checkpoint",
                Expected::Missing,
                b"value",
                &[registered_b]
            ),
            Err(Error::InvalidOwnership)
        ));
        assert!(db.get(b"checkpoint").unwrap().is_none());

        let child = owned(3, 1).registered("job".into(), true);
        assert!(
            ts.commit_owned_tasks(
                &TaskOwner::Task(a.id()),
                b"checkpoint",
                Expected::Missing,
                b"value",
                &[child]
            )
            .is_ok()
        );
    }

    #[test]
    fn raw_commit_of_independently_prepared_batches_can_create_a_cycle() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let a = owned(1, 2);
        let b = owned(2, 1);
        let first = ts.prepare_save(&[Save::Create(&a)]).unwrap();
        let second = ts.prepare_save(&[Save::Create(&b)]).unwrap();
        let checks = [first.checks, second.checks].concat();
        let mutations = [first.mutations, second.mutations].concat();
        // Raw commits evaluate both missing-owner checks before either write.
        db.commit(&checks, &mutations).unwrap();
        assert_eq!(
            ts.load(a.id()).unwrap().unwrap().task.owner(),
            &TaskOwner::Task(b.id())
        );
        assert_eq!(
            ts.load(b.id()).unwrap().unwrap().task.owner(),
            &TaskOwner::Task(a.id())
        );
        assert!(matches!(
            ts.save(&[Save::Create(&owned(3, 1))]),
            Err(Error::InvalidOwnership)
        ));
    }

    #[test]
    fn jointly_prepared_parent_and_child_can_be_committed_together() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let parent = owned(1, 9);
        let child = owned(2, 1);
        let batch = ts
            .prepare_save(&[Save::Create(&parent), Save::Create(&child)])
            .unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        assert_eq!(ts.list().unwrap().len(), 2);
    }

    #[test]
    fn task_creation_rejects_cycle_through_background_conversation_edge() {
        use crate::ownership_edge::{Edge, Mode, edge_key};
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let parent = owned(2, 1);
        ts.save(&[Save::Create(&parent)]).unwrap();
        let conversation = Uuid::from_u128(100);
        let edge = Edge {
            task: parent.id(),
            task_owner: parent.owner().clone(),
            child: conversation,
            mode: Mode::Background,
        };
        db.commit(
            &[],
            &[Mutation::Put {
                key: edge_key(conversation),
                value: serde_json::to_vec(&edge).unwrap(),
            }],
        )
        .unwrap();
        let child = task(1, 3);
        assert!(matches!(
            ts.save(&[Save::Create(&child)]),
            Err(Error::InvalidOwnership)
        ));
        assert!(ts.load(child.id()).unwrap().is_none());
    }

    #[test]
    fn reopen_recover_and_enforce_restart_limit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let original = task(1, 1);
        {
            let db = Store::open(&path).unwrap();
            let ts = TaskStore::new(&db);
            ts.save(&[Save::Create(&original)]).unwrap();
            ts.reserve(&ts.load(original.id()).unwrap().unwrap())
                .unwrap();
        }
        {
            let db = Store::open(&path).unwrap();
            let ts = TaskStore::new(&db);
            let running = ts.load(original.id()).unwrap().unwrap();
            assert_eq!(running.task.state(), &TaskState::Running);
            ts.recover(&running).unwrap();
            assert!(matches!(
                ts.recover(&running),
                Err(Error::Store(durable_store::Error::Conflict { .. }))
            ));
        }
        let db = Store::open(&path).unwrap();
        let ts = TaskStore::new(&db);
        let recovered = ts.load(original.id()).unwrap().unwrap();
        assert_eq!(recovered.task.state(), &TaskState::Ready);
        assert_eq!(recovered.task.restarts(), 1);
        assert_eq!(recovered.task.checkpoint(), original.checkpoint());
        assert_eq!(recovered.task.owner(), original.owner());
        assert!(matches!(
            ts.reserve(&recovered),
            Err(Error::Transition(TransitionError::RestartLimit))
        ));
        assert_eq!(ts.load(original.id()).unwrap().unwrap(), recovered);
    }

    #[test]
    fn batch_is_atomic_and_rejects_duplicates() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let a = task(1, 3);
        let b = task(2, 3);
        ts.save(&[Save::Create(&a)]).unwrap();
        assert!(matches!(
            ts.save(&[Save::Create(&b), Save::Create(&a)]),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert!(ts.load(b.id()).unwrap().is_none());
        assert!(matches!(
            ts.save(&[Save::Create(&b), Save::Create(&b)]),
            Err(Error::DuplicateTask)
        ));
        assert_eq!(ts.list().unwrap().len(), 1);
    }

    #[test]
    fn owned_batch_composes_with_other_transaction_records() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let child = task(1, 3).registered("job".into(), true);
        let owner = TaskOwner::Conversation(Uuid::from_u128(100));
        let prepare = || {
            ts.prepare_owned_tasks(
                &owner,
                b"runtime/conversation/100",
                Expected::Missing,
                b"checkpoint",
                std::slice::from_ref(&child),
            )
            .unwrap()
        };
        let mut batch = prepare();
        batch.checks.push(Check {
            key: b"observation".to_vec(),
            expected: Expected::Version(1),
        });
        batch.mutations.push(Mutation::Put {
            key: b"observation".to_vec(),
            value: b"event".to_vec(),
        });
        assert!(matches!(
            db.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(ts.load(child.id()).unwrap().is_none());
        assert!(db.get(b"runtime/conversation/100").unwrap().is_none());
        let mut batch = prepare();
        batch.checks.push(Check {
            key: b"observation".to_vec(),
            expected: Expected::Missing,
        });
        batch.mutations.push(Mutation::Put {
            key: b"observation".to_vec(),
            value: b"event".to_vec(),
        });
        db.commit(&batch.checks, &batch.mutations).unwrap();
        assert_eq!(ts.load(child.id()).unwrap().unwrap().task, child);
        assert_eq!(
            db.get(b"runtime/conversation/100").unwrap().unwrap().value,
            b"checkpoint"
        );
        assert_eq!(db.get(b"observation").unwrap().unwrap().value, b"event");
    }

    #[test]
    fn owned_batch_checkpoint_and_children_survive_reopen_and_fence_retries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owned.db");
        let owner = TaskOwner::Conversation(Uuid::from_u128(100));
        let owner_key = b"runtime/conversation/100";
        let child = task(1, 3).registered("extension.task".into(), true);
        let checkpoint =
            serde_json::to_vec(&json!({"phase": "after_run", "tasks": [child.id()]})).unwrap();
        let version;
        {
            let db = Store::open(&path).unwrap();
            db.commit(
                &[Check {
                    key: owner_key.to_vec(),
                    expected: Expected::Missing,
                }],
                &[Mutation::Put {
                    key: owner_key.to_vec(),
                    value: b"finish".to_vec(),
                }],
            )
            .unwrap();
            version = db.get(owner_key).unwrap().unwrap().version;
            TaskStore::new(&db)
                .commit_owned_tasks(
                    &owner,
                    owner_key,
                    Expected::Version(version),
                    &checkpoint,
                    std::slice::from_ref(&child),
                )
                .unwrap();
        }
        let db = Store::open(&path).unwrap();
        let ts = TaskStore::new(&db);
        assert_eq!(db.get(owner_key).unwrap().unwrap().value, checkpoint);
        assert_eq!(ts.load(child.id()).unwrap().unwrap().task, child);
        let another = task(2, 3).registered("extension.task".into(), true);
        assert!(matches!(
            ts.commit_owned_tasks(
                &owner,
                owner_key,
                Expected::Version(version),
                b"incorrect",
                &[another]
            ),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert_eq!(ts.list().unwrap().len(), 1);
        assert_eq!(db.get(owner_key).unwrap().unwrap().value, checkpoint);
    }

    #[test]
    fn owned_batch_rejects_invalid_children_and_rolls_back_collisions() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let owner = TaskOwner::Conversation(Uuid::from_u128(100));
        let existing = task(1, 3).registered("task".into(), true);
        let fresh = task(2, 3).registered("task".into(), true);
        ts.save(&[Save::Create(&existing)]).unwrap();
        let key = b"runtime/owner";
        assert!(matches!(
            ts.commit_owned_tasks(
                &owner,
                key,
                Expected::Missing,
                b"next",
                &[fresh.clone(), existing]
            ),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert!(db.get(key).unwrap().is_none());
        assert!(ts.load(fresh.id()).unwrap().is_none());
        assert!(matches!(
            ts.commit_owned_tasks(
                &owner,
                key,
                Expected::Missing,
                b"next",
                &[fresh.clone(), fresh]
            ),
            Err(Error::InvalidOwnedBatch)
        ));
        assert!(matches!(
            ts.commit_owned_tasks(
                &TaskOwner::Conversation(Uuid::new_v4()),
                key,
                Expected::Missing,
                b"next",
                &[task(3, 3).registered("task".into(), true)]
            ),
            Err(Error::InvalidOwnedBatch)
        ));
        assert!(matches!(
            ts.commit_owned_tasks(&owner, b"durable/task/x", Expected::Missing, b"next", &[]),
            Err(Error::InvalidOwnedBatch)
        ));
        assert!(db.get(key).unwrap().is_none());
    }

    #[test]
    fn concurrent_reservations_have_one_winner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let a = Store::open(&path).unwrap();
        let b = Store::open(&path).unwrap();
        let t = task(1, 3);
        TaskStore::new(&a).save(&[Save::Create(&t)]).unwrap();
        let snapshot = TaskStore::new(&a).load(t.id()).unwrap().unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let threads: Vec<_> = [a, b]
            .into_iter()
            .map(|db| {
                let barrier = barrier.clone();
                let snapshot = snapshot.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    TaskStore::new(&db).reserve(&snapshot)
                })
            })
            .collect();
        let results: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Err(Error::Store(durable_store::Error::Conflict { .. }))))
                .count(),
            1
        );
        let db = Store::open(&path).unwrap();
        assert_eq!(
            TaskStore::new(&db)
                .load(t.id())
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Running
        );
    }

    #[test]
    fn updates_and_invalid_transitions_preserve_state() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let t = task(1, 3);
        ts.save(&[Save::Create(&t)]).unwrap();
        let before = ts.load(t.id()).unwrap().unwrap();
        assert!(matches!(
            ts.recover(&before),
            Err(Error::Transition(TransitionError::InvalidState))
        ));
        ts.reserve(&before).unwrap();
        let mut current = ts.load(t.id()).unwrap().unwrap();
        current.task.wait(WaitFor::Signal("go".into())).unwrap();
        ts.save(&[Save::Update(&current)]).unwrap();
        assert!(matches!(
            ts.save(&[Save::Update(&before)]),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        let waiting = ts.load(t.id()).unwrap().unwrap();
        assert!(matches!(
            ts.reserve(&waiting),
            Err(Error::Transition(TransitionError::InvalidState))
        ));
        assert_eq!(ts.load(t.id()).unwrap().unwrap(), waiting);
    }
    #[test]
    fn deadline_survives_reopen_and_stale_wake_conflicts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("timer.db");
        let t = task(1, 3);
        {
            let db = Store::open(&path).unwrap();
            let ts = TaskStore::new(&db);
            ts.save(&[Save::Create(&t)]).unwrap();
            ts.reserve(&ts.load(t.id()).unwrap().unwrap()).unwrap();
            let running = ts.load(t.id()).unwrap().unwrap();
            ts.update(&running, |t| t.wait(WaitFor::DeadlineMillis(100)))
                .unwrap();
        }
        let db = Store::open(&path).unwrap();
        let ts = TaskStore::new(&db);
        let waiting = ts.load(t.id()).unwrap().unwrap();
        assert_eq!(ts.poll_wait(&waiting, 99).unwrap(), WakeResult::Pending);
        assert_eq!(
            ts.poll_wait(&waiting, 100).unwrap(),
            WakeResult::Woken { signal: None }
        );
        assert!(matches!(
            ts.poll_wait(&waiting, 101),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert_eq!(
            ts.load(t.id()).unwrap().unwrap().task.state(),
            &TaskState::Ready
        );
    }

    #[test]
    fn independent_wake_rejects_changed_ownership_graph() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let t = task(1, 3);
        ts.save(&[Save::Create(&t)]).unwrap();
        ts.reserve(&ts.load(t.id()).unwrap().unwrap()).unwrap();
        ts.update(&ts.load(t.id()).unwrap().unwrap(), |task| {
            task.wait(WaitFor::DeadlineMillis(100))
        })
        .unwrap();
        let version = db.put_version().unwrap();
        let waiting = ts.load(t.id()).unwrap().unwrap();
        let edge = crate::ownership_edge::edge_key(Uuid::new_v4());
        db.commit(
            &[Check {
                key: edge.clone(),
                expected: Expected::Missing,
            }],
            &[Mutation::Put {
                key: edge,
                value: b"changed".to_vec(),
            }],
        )
        .unwrap();
        assert!(matches!(
            ts.poll_wait_with_graph_fence(&waiting, 100, version),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert_eq!(
            ts.load(t.id()).unwrap().unwrap().task.state(),
            &TaskState::Waiting(WaitFor::DeadlineMillis(100))
        );
        let version = db.put_version().unwrap();
        assert!(matches!(
            ts.poll_wait_with_graph_fence(&waiting, 100, version),
            Ok(WakeResult::Woken { .. })
        ));
    }

    #[test]
    fn sleep_wakes_for_signal_or_expired_deadline_after_reopen() {
        for with_signal in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("sleep.db");
            let t = task(1, 3);
            let signal = Signal::new(
                Uuid::from_u128(42),
                t.id(),
                "go".into(),
                serde_json::json!(1),
            );
            {
                let db = Store::open(&path).unwrap();
                let ts = TaskStore::new(&db);
                ts.save(&[Save::Create(&t)]).unwrap();
                ts.reserve(&ts.load(t.id()).unwrap().unwrap()).unwrap();
                ts.update(&ts.load(t.id()).unwrap().unwrap(), |t| {
                    t.wait(WaitFor::Sleep {
                        until_millis: Some(100),
                        signals: vec!["go".into(), "other".into()],
                    })
                })
                .unwrap();
                assert_eq!(
                    ts.poll_wait(&ts.load(t.id()).unwrap().unwrap(), 99)
                        .unwrap(),
                    WakeResult::Pending
                );
                if with_signal {
                    ts.send_signal(&signal).unwrap();
                }
            }
            let db = Store::open(&path).unwrap();
            let ts = TaskStore::new(&db);
            let result = ts
                .poll_wait(&ts.load(t.id()).unwrap().unwrap(), 100)
                .unwrap();
            assert!(matches!(result, WakeResult::Woken { signal: Some(_) }) == with_signal);
            assert_eq!(ts.signal(t.id(), signal.id).unwrap().is_some(), with_signal);
            if with_signal {
                assert!(ts.signal(t.id(), signal.id).unwrap().unwrap().is_consumed());
            }
        }
    }

    #[test]
    fn signal_is_idempotent_and_delivery_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("signals.db");
        let t = task(1, 3);
        let sig = Signal::new(
            Uuid::from_u128(42),
            t.id(),
            "go".into(),
            json!({"value": 3}),
        );
        {
            let db = Store::open(&path).unwrap();
            let ts = TaskStore::new(&db);
            ts.save(&[Save::Create(&t)]).unwrap();
            ts.send_signal(&sig).unwrap();
            ts.send_signal(&sig).unwrap();
            let mut changed = sig.clone();
            changed.payload = json!(4);
            assert!(matches!(
                ts.send_signal(&changed),
                Err(Error::SignalMismatch)
            ));
            ts.reserve(&ts.load(t.id()).unwrap().unwrap()).unwrap();
            let running = ts.load(t.id()).unwrap().unwrap();
            ts.update(&running, |t| t.wait(WaitFor::Signal("go".into())))
                .unwrap();
            let waiting = ts.load(t.id()).unwrap().unwrap();
            assert!(matches!(
                ts.poll_wait(&waiting, 0),
                Ok(WakeResult::Woken { signal: Some(_) })
            ));
            assert!(matches!(
                ts.poll_wait(&waiting, 0),
                Ok(WakeResult::Pending) | Err(Error::Store(durable_store::Error::Conflict { .. }))
            ));
        }
        let db = Store::open(&path).unwrap();
        let ts = TaskStore::new(&db);
        let restored = ts.load(t.id()).unwrap().unwrap();
        let id = restored.task.delivered_signal().unwrap();
        let receipt = ts.signal(t.id(), id).unwrap().unwrap();
        assert!(receipt.is_consumed());
        assert_eq!(receipt.payload, sig.payload);
        ts.send_signal(&sig).unwrap();
        assert!(ts.signal(t.id(), id).unwrap().unwrap().is_consumed());
    }
    #[test]
    fn signal_rejects_missing_and_terminal_targets() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let mut t = task(1, 3);
        let signal = Signal::new(Uuid::new_v4(), t.id(), "go".into(), json!(null));
        assert!(matches!(
            ts.send_signal(&signal),
            Err(Error::SignalTargetNotFound)
        ));
        t.start().unwrap();
        t.succeed(json!("done")).unwrap();
        ts.save(&[Save::Create(&t)]).unwrap();
        assert!(matches!(
            ts.send_signal(&signal),
            Err(Error::SignalTargetClosed)
        ));
    }

    #[test]
    fn child_wake_serialization_round_trips_and_accepts_legacy_records() {
        let mut parent = task(1, 3);
        let child = Uuid::from_u128(2);
        let legacy = serde_json::to_value(&parent).unwrap();
        assert!(legacy.get("ended_children").is_none());
        assert!(legacy.get("wake_children").is_none());
        assert_eq!(serde_json::from_value::<Task>(legacy).unwrap(), parent);

        parent.set_child_wake(vec![child], vec![child]);
        let encoded = serde_json::to_value(&parent).unwrap();
        assert_eq!(
            serde_json::from_value::<Task>(encoded.clone()).unwrap(),
            parent
        );
        let mut old_wake = encoded;
        old_wake.as_object_mut().unwrap().remove("wake_children");
        let restored: Task = serde_json::from_value(old_wake).unwrap();
        assert_eq!(restored.ended_children(), Some(&[child][..]));
        assert_eq!(restored.wake_children(), None);
    }

    #[test]
    fn child_completion_racing_reservation_conflicts_and_retry_refreshes_wake() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let mut parent = task(1, 3);
        let mut first = owned(2, 1);
        let second = owned(3, 1);
        first.start().unwrap();
        first.succeed(json!(1)).unwrap();
        parent.start().unwrap();
        parent
            .wait(WaitFor::TasksAny(vec![first.id(), second.id()]))
            .unwrap();
        ts.save(&[
            Save::Create(&parent),
            Save::Create(&first),
            Save::Create(&second),
        ])
        .unwrap();
        let waiting = ts.load(parent.id()).unwrap().unwrap();
        ts.poll_wait(&waiting, 0).unwrap();
        let ready = ts.load(parent.id()).unwrap().unwrap();
        let mut next = ready.clone();
        let checks = ts.refresh_child_wake(&mut next.task).unwrap();
        next.task.start().unwrap();
        let mut batch = ts.prepare_save(&[Save::Update(&next)]).unwrap();
        batch.checks.extend(checks);
        let child = ts.load(second.id()).unwrap().unwrap();
        ts.update(&child, |t| t.cancel()).unwrap();
        assert!(matches!(
            db.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert_eq!(ts.load(parent.id()).unwrap().unwrap(), ready);
        ts.reserve(&ready).unwrap();
        assert_eq!(
            ts.load(parent.id()).unwrap().unwrap().task.ended_children(),
            Some(&[first.id(), second.id()][..])
        );
    }

    #[test]
    fn child_wake_records_only_ended_children_and_abort_clears_it() {
        for all in [false, true] {
            let db = Store::memory().unwrap();
            let ts = TaskStore::new(&db);
            let mut parent = task(1, 3);
            let mut first = owned(2, 1);
            let mut second = owned(3, 1);
            first.start().unwrap();
            first.succeed(json!(1)).unwrap();
            if all {
                second.cancel().unwrap();
            }
            parent.start().unwrap();
            let ids = vec![second.id(), first.id()];
            parent
                .wait(if all {
                    WaitFor::Tasks(ids)
                } else {
                    WaitFor::TasksAny(ids)
                })
                .unwrap();
            ts.save(&[
                Save::Create(&parent),
                Save::Create(&first),
                Save::Create(&second),
            ])
            .unwrap();
            let waiting = ts.load(parent.id()).unwrap().unwrap();
            assert!(matches!(
                ts.poll_wait(&waiting, 0).unwrap(),
                WakeResult::Woken { .. }
            ));
            let ready = ts.load(parent.id()).unwrap().unwrap();
            let expected = if all {
                vec![second.id(), first.id()]
            } else {
                vec![first.id()]
            };
            assert_eq!(ready.task.ended_children(), Some(expected.as_slice()));
            ts.update(&ready, |t| t.request_abort()).unwrap();
            assert_eq!(
                ts.load(parent.id()).unwrap().unwrap().task.ended_children(),
                None
            );
        }
    }

    #[test]
    fn children_must_be_owned_terminal_and_unique() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let parent = task(1, 3);
        let child = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Task(parent.id()),
            parent.checkpoint().clone(),
            3,
        )
        .unwrap();
        ts.save(&[Save::Create(&parent), Save::Create(&child)])
            .unwrap();
        ts.reserve(&ts.load(parent.id()).unwrap().unwrap()).unwrap();
        ts.update(&ts.load(parent.id()).unwrap().unwrap(), |t| {
            t.wait(WaitFor::Tasks(vec![child.id()]))
        })
        .unwrap();
        let waiting = ts.load(parent.id()).unwrap().unwrap();
        assert_eq!(ts.poll_wait(&waiting, 0).unwrap(), WakeResult::Pending);
        let child_snapshot = ts.load(child.id()).unwrap().unwrap();
        ts.update(&child_snapshot, |t| t.cancel()).unwrap();
        assert_eq!(
            ts.poll_wait(&waiting, 0).unwrap(),
            WakeResult::Woken { signal: None }
        );
        ts.reserve(&ts.load(parent.id()).unwrap().unwrap()).unwrap();
        ts.update(&ts.load(parent.id()).unwrap().unwrap(), |t| {
            t.wait(WaitFor::Tasks(vec![child.id(), child.id()]))
        })
        .unwrap();
        assert!(matches!(
            ts.poll_wait(&ts.load(parent.id()).unwrap().unwrap(), 0),
            Err(Error::InvalidChild)
        ));
    }

    #[test]
    fn failed_atomic_update_does_not_commit_checkpoint() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let t = task(1, 3);
        ts.save(&[Save::Create(&t)]).unwrap();
        ts.reserve(&ts.load(t.id()).unwrap().unwrap()).unwrap();
        let running = ts.load(t.id()).unwrap().unwrap();
        let result = ts.update(&running, |t| {
            t.advance(Checkpoint {
                phase: "next".into(),
                data: json!(2),
            })?;
            Err(TransitionError::InvalidState)
        });
        assert!(matches!(
            result,
            Err(Error::Transition(TransitionError::InvalidState))
        ));
        assert_eq!(ts.load(t.id()).unwrap().unwrap(), running);
    }
}
