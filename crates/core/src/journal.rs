use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    Error, ExtensionStates, Invocation, Message, ModelMetadata, Outcome, QueuedInput, Result,
    Usage, UsageReport,
};

const FORMAT_VERSION: u32 = 2;
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Running,
    Completed,
    Cancelled,
    Failed,
    WaitingForReconciliation,
    WaitingForOperation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    Created {
        session_id: Uuid,
    },
    HostBound {
        scope_digest: String,
    },
    HostRunBound {
        run_id: Uuid,
        identity_digest: String,
    },
    Checkout {
        target: u64,
    },
    Run {
        state: RunState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        initial: Option<String>,
    },
    User {
        content: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input_id: Option<Uuid>,
    },
    InputQueued {
        input: QueuedInput,
    },
    ModelRequested {
        model: String,
        messages: usize,
        tools: usize,
        max_output_tokens: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<Uuid>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_head: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_sha256: Option<String>,
    },
    ModelFinished {
        request_id: Uuid,
        metadata: ModelMetadata,
        usage: UsageReport,
        failure: Option<ModelFailure>,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        partial: String,
    },
    TurnFinished {
        assistant_record: u64,
    },
    Assistant {
        content: String,
        invocations: Vec<Invocation>,
        usage: Option<Usage>,
    },
    ToolStarted {
        operation_id: Uuid,
    },
    ToolFinished {
        operation_id: Uuid,
        outcome: Outcome,
        #[serde(default, skip_serializing_if = "ExtensionStates::is_empty")]
        states: ExtensionStates,
    },
    ToolReconciled {
        operation_id: Uuid,
        outcome: Outcome,
    },
    Compacted {
        summary: String,
        retain_from: Option<u64>,
        states: ExtensionStates,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<Usage>,
    },
    ExtensionStates {
        states: ExtensionStates,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<Uuid>,
    pub version: u32,
    pub id: u64,
    pub parent: Option<u64>,
    pub event: Event,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelFailure {
    Cancelled,
    Retryable,
    Failed,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OperationState {
    Planned,
    Started,
    Finished(Outcome),
}

/// A store represents one exclusively held session. `append` must commit the
/// whole record before returning success. Failed writes require reopening.
pub trait SessionStore: Send {
    fn load(&mut self) -> Result<Vec<Record>>;
    fn append(&mut self, record: &Record) -> Result<()>;
}

#[derive(Default)]
pub struct MemoryStore {
    records: Vec<Record>,
}

impl SessionStore for MemoryStore {
    fn load(&mut self) -> Result<Vec<Record>> {
        Ok(self.records.clone())
    }

    fn append(&mut self, record: &Record) -> Result<()> {
        self.records.push(record.clone());
        Ok(())
    }
}

pub struct Session {
    store: Box<dyn SessionStore>,
    records: Vec<Record>,
    head: u64,
    run_id: Option<Uuid>,
    poisoned: bool,
}

impl Session {
    /// Hold an exclusive OS lock for the lifetime of the session.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::with_store(FileStore::open(path.as_ref())?)
    }

    pub fn with_store(mut store: impl SessionStore + 'static) -> Result<Self> {
        let records = store.load()?;
        let head = validated_head(&records)?;
        let mut session = Self {
            store: Box::new(store),
            records,
            head,
            run_id: None,
            poisoned: false,
        };
        if session.records.is_empty() {
            session.append(Event::Created {
                session_id: Uuid::new_v4(),
            })?;
        }
        Ok(session)
    }

    pub(crate) fn set_run_id(&mut self, run_id: Option<Uuid>) {
        self.run_id = run_id;
    }

    /// Bind an otherwise empty session to a host security scope. Reopening
    /// requires the same scope. A local CLI cannot resume a bound session.
    pub fn bind_host(&mut self, scope_digest: &str) -> Result<()> {
        match self.host_binding() {
            Some(existing) if existing == scope_digest => Ok(()),
            None if self.records.len() == 1 && !scope_digest.is_empty() => {
                self.append(Event::HostBound {
                    scope_digest: scope_digest.into(),
                })?;
                Ok(())
            }
            _ => Err(Error::Session(
                "session belongs to a different execution scope".into(),
            )),
        }
    }

    pub fn bind_host_run(&mut self, run_id: Uuid, identity_digest: &str) -> Result<()> {
        if self.host_binding().is_none() || identity_digest.is_empty() || run_id.is_nil() {
            return Err(Error::Session(
                "host run binding requires a bound session and a valid identity".into(),
            ));
        }
        for record in &self.records {
            if let Event::HostRunBound {
                run_id: id,
                identity_digest: existing,
            } = &record.event
                && *id == run_id
            {
                return if existing == identity_digest {
                    Ok(())
                } else {
                    Err(Error::Session(
                        "run belongs to a different host identity".into(),
                    ))
                };
            }
        }
        self.append(Event::HostRunBound {
            run_id,
            identity_digest: identity_digest.into(),
        })?;
        Ok(())
    }

    pub fn host_binding(&self) -> Option<&str> {
        match self.records.get(1).map(|record| &record.event) {
            Some(Event::HostBound { scope_digest }) => Some(scope_digest),
            _ => None,
        }
    }

    pub fn session_id(&self) -> Uuid {
        match self.records[0].event {
            Event::Created { session_id } => session_id,
            _ => unreachable!("sessions always start with a creation record"),
        }
    }

    pub fn head(&self) -> u64 {
        self.head
    }

    pub fn records(&self) -> &[Record] {
        &self.records
    }

    pub fn branch(&self) -> Vec<&Record> {
        self.path(self.head)
    }

    /// Conversation-only snapshot of a settled branch. External effects and
    /// extension state remain with the original session.
    pub fn conversation_at(&self, record_id: u64) -> Result<Vec<Message>> {
        if record_id == 0
            || record_id > self.records.len() as u64
            || matches!(
                self.records[record_id as usize - 1].event,
                Event::Checkout { .. }
            )
        {
            return Err(Error::Session(
                "invalid conversation snapshot record".into(),
            ));
        }
        let branch = self.path(record_id);
        let ops = operations(&branch);
        ensure_settled(ops.clone())?;
        let mut messages = Vec::new();
        if let Some(summary) = branch.iter().rev().find_map(|record| match &record.event {
            Event::Compacted { summary, .. } => Some(summary),
            _ => None,
        }) {
            messages.push(Message::User {
                content: format!("Prior conversation summary:\n{summary}"),
            });
        }
        messages.extend(conversation(&branch, ops));
        Ok(messages)
    }

    pub fn operations(&self) -> Vec<(Invocation, OperationState)> {
        operations(&self.branch())
    }

    pub fn extension_states(&self) -> ExtensionStates {
        let mut result = BTreeMap::new();
        for record in self.branch() {
            match &record.event {
                Event::ExtensionStates { states }
                | Event::Compacted { states, .. }
                | Event::ToolFinished { states, .. } => result.extend(states.clone()),
                _ => {}
            }
        }
        result
    }

    /// Fork from an existing record. The abandoned branch stays available by ID.
    pub fn checkout(&mut self, target: u64) -> Result<()> {
        if target == 0
            || (target == 1 && self.host_binding().is_some())
            || target > self.records.len() as u64
            || matches!(
                self.records[target as usize - 1].event,
                Event::Checkout { .. }
            )
        {
            return Err(Error::Session("invalid checkout target".into()));
        }
        for path in [self.branch(), self.path(target)] {
            ensure_settled(operations(&path))?;
        }
        self.append(Event::Checkout { target })?;
        Ok(())
    }

    /// Confirm the outcome via the system that owns the effect, using the operation ID.
    pub fn reconcile(&mut self, operation_id: Uuid, outcome: Outcome) -> Result<()> {
        if !matches!(outcome, Outcome::Succeeded(_) | Outcome::Failed(_))
            || !self.operations().iter().any(|(inv, state)| {
                inv.operation_id == operation_id
                    && matches!(
                        state,
                        OperationState::Started
                            | OperationState::Finished(
                                Outcome::Unknown(_) | Outcome::Pending { .. }
                            )
                    )
            })
        {
            return Err(Error::Session(
                "reconciliation needs an uncertain operation and a known outcome".into(),
            ));
        }
        self.append(Event::ToolReconciled {
            operation_id,
            outcome,
        })?;
        Ok(())
    }

    pub(crate) fn path(&self, mut id: u64) -> Vec<&Record> {
        let mut branch = Vec::new();
        while id != 0 {
            let record = &self.records[id as usize - 1];
            branch.push(record);
            id = record.parent.unwrap_or(0);
        }
        branch.reverse();
        branch
    }

    pub(crate) fn append(&mut self, event: Event) -> Result<&Record> {
        if self.poisoned {
            return Err(Error::Session(
                "journal write failed; reopen before continuing".into(),
            ));
        }
        let id = self.records.len() as u64 + 1;
        let next_head = match event {
            Event::Checkout { target } => target,
            _ => id,
        };
        let record = Record {
            run_id: self.run_id,
            version: FORMAT_VERSION,
            id,
            parent: (self.head != 0).then_some(self.head),
            event,
        };
        if let Err(err) = self.store.append(&record) {
            self.poisoned = true;
            return Err(err);
        }
        self.head = next_head;
        self.records.push(record);
        Ok(self.records.last().unwrap())
    }
}

fn validated_head(records: &[Record]) -> Result<u64> {
    let mut head = 0;
    let mut version = 1;
    for (index, record) in records.iter().enumerate() {
        let id = index as u64 + 1;
        if !(version..=FORMAT_VERSION).contains(&record.version)
            || record.id != id
            || record.parent != (head != 0).then_some(head)
            || matches!(record.event, Event::Created { .. }) != (id == 1)
        {
            return Err(Error::Session(format!("invalid journal record {id}")));
        }
        version = record.version;
        head = match record.event {
            Event::Checkout { target }
                if target > 0
                    && target < id
                    && !matches!(records[target as usize - 1].event, Event::Checkout { .. }) =>
            {
                target
            }
            Event::Checkout { .. } => return Err(Error::Session("invalid checkout target".into())),
            _ => id,
        };
    }
    Ok(head)
}

struct FileStore {
    file: File,
    #[cfg(unix)]
    parent: std::path::PathBuf,
}

impl FileStore {
    fn open(path: &Path) -> Result<Self> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path)?;
        file.try_lock()
            .map_err(|err| Error::Session(format!("cannot lock journal: {err}")))?;
        Ok(Self {
            file,
            #[cfg(unix)]
            parent: path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."))
                .into(),
        })
    }
}

impl SessionStore for FileStore {
    fn load(&mut self) -> Result<Vec<Record>> {
        let mut records = Vec::new();
        let mut offset = 0;
        let mut reader = BufReader::new(&mut self.file);
        let mut line = Vec::new();
        while reader
            .by_ref()
            .take(MAX_RECORD_BYTES as u64 + 1)
            .read_until(b'\n', &mut line)?
            > 0
        {
            if line.len() > MAX_RECORD_BYTES {
                return Err(Error::Session("journal record exceeds size limit".into()));
            }
            if !line.ends_with(b"\n") {
                if records.is_empty() {
                    return Err(Error::Session("journal has no committed header".into()));
                }
                validated_head(&records)?;
                self.file.set_len(offset)?;
                self.file.sync_all()?;
                break;
            }
            records.push(serde_json::from_slice(&line)?);
            offset += line.len() as u64;
            line.clear();
        }
        self.file.seek(SeekFrom::End(0))?;
        Ok(records)
    }

    fn append(&mut self, record: &Record) -> Result<()> {
        let mut line = serde_json::to_vec(record)?;
        line.push(b'\n');
        if line.len() > MAX_RECORD_BYTES {
            return Err(Error::Session("journal record exceeds size limit".into()));
        }
        self.file.write_all(&line)?;
        self.file.sync_data()?;
        #[cfg(unix)]
        if record.id == 1 {
            File::open(&self.parent)?.sync_all()?;
        }
        Ok(())
    }
}

fn operations(path: &[&Record]) -> Vec<(Invocation, OperationState)> {
    let mut indices = BTreeMap::new();
    let mut ops = Vec::<(Invocation, OperationState)>::new();
    for record in path {
        match &record.event {
            Event::Assistant { invocations, .. } => {
                for inv in invocations {
                    indices.insert(inv.operation_id, ops.len());
                    ops.push((inv.clone(), OperationState::Planned));
                }
            }
            Event::ToolStarted { operation_id } => {
                if let Some(&index) = indices.get(operation_id) {
                    ops[index].1 = OperationState::Started;
                }
            }
            Event::ToolFinished {
                operation_id,
                outcome,
                ..
            }
            | Event::ToolReconciled {
                operation_id,
                outcome,
            } => {
                if let Some(&index) = indices.get(operation_id) {
                    ops[index].1 = OperationState::Finished(outcome.clone());
                }
            }
            _ => {}
        }
    }
    ops
}

pub(crate) fn ensure_settled(ops: Vec<(Invocation, OperationState)>) -> Result<()> {
    for (inv, state) in ops {
        match state {
            OperationState::Started | OperationState::Finished(Outcome::Unknown(_)) => {
                return Err(Error::Reconciliation(inv.operation_id));
            }
            OperationState::Finished(Outcome::Pending { .. }) => {
                return Err(Error::PendingOperation(inv.operation_id));
            }
            OperationState::Planned => {
                return Err(Error::Session(
                    "unfinished tool batch; resume the current branch first".into(),
                ));
            }
            OperationState::Finished(_) => {}
        }
    }
    Ok(())
}

pub(crate) fn conversation(
    path: &[&Record],
    operations: Vec<(Invocation, OperationState)>,
) -> Vec<Message> {
    let retain_from = path
        .iter()
        .rev()
        .find_map(|record| match record.event {
            Event::Compacted { retain_from, .. } => Some(retain_from.unwrap_or(record.id)),
            _ => None,
        })
        .unwrap_or(0);
    let results: BTreeMap<_, _> = operations
        .into_iter()
        .filter_map(|(inv, state)| match state {
            OperationState::Finished(outcome) => Some((inv.operation_id, outcome)),
            _ => None,
        })
        .collect();
    let mut messages = Vec::new();
    for record in path.iter().filter(|record| record.id >= retain_from) {
        match &record.event {
            Event::User { content, .. } => messages.push(Message::User {
                content: content.clone(),
            }),
            Event::Assistant {
                content,
                invocations,
                ..
            } => {
                messages.push(Message::Assistant {
                    content: content.clone(),
                    calls: invocations
                        .iter()
                        .map(|inv| {
                            let mut call = inv.call.clone();
                            if !serde_json::from_str::<serde_json::Value>(&call.arguments)
                                .is_ok_and(|value| value.is_object())
                            {
                                call.arguments = "{}".into();
                            }
                            call
                        })
                        .collect(),
                });
                for inv in invocations {
                    if let Some(outcome) = results.get(&inv.operation_id) {
                        messages.push(Message::Tool {
                            call_id: inv.call.id.clone(),
                            content: outcome.tool_content(),
                        });
                    }
                }
            }
            _ => {}
        }
    }
    messages
}
