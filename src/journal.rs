use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, ExtensionStates, Invocation, Message, Outcome, Result, Usage};

const FORMAT_VERSION: u32 = 1;
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Running,
    Completed,
    Cancelled,
    Failed,
    WaitingForReconciliation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    Created {
        session_id: Uuid,
    },
    Checkout {
        target: u64,
    },
    Run {
        state: RunState,
    },
    User {
        content: String,
    },
    ModelRequested {
        model: String,
        messages: usize,
        tools: usize,
        max_output_tokens: u32,
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
    pub version: u32,
    pub id: u64,
    pub parent: Option<u64>,
    pub event: Event,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OperationState {
    Planned,
    Started,
    Finished(Outcome),
}

pub struct Session {
    file: File,
    records: Vec<Record>,
    head: u64,
    poisoned: bool,
}

impl Session {
    /// Hold an exclusive OS lock for the lifetime of the session. Recover only an
    /// unterminated tail, since terminated records must already be valid.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path.as_ref())?;
        file.try_lock()
            .map_err(|err| Error::Session(format!("cannot lock journal: {err}")))?;

        let mut records = Vec::new();
        let mut head = 0;
        let mut offset = 0;
        let mut reader = BufReader::new(&mut file);
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
                file.set_len(offset)?;
                file.sync_all()?;
                break;
            }
            let record: Record = serde_json::from_slice(&line)?;
            let expected_id = records.len() as u64 + 1;
            if record.version != FORMAT_VERSION
                || record.id != expected_id
                || record.parent != (head != 0).then_some(head)
                || matches!(record.event, Event::Created { .. }) != (expected_id == 1)
            {
                return Err(Error::Session(format!(
                    "invalid journal record {expected_id}"
                )));
            }
            head = match record.event {
                Event::Checkout { target }
                    if target != 0
                        && target < expected_id
                        && !matches!(
                            records.get(target as usize - 1).map(|r: &Record| &r.event),
                            Some(Event::Checkout { .. })
                        ) =>
                {
                    target
                }
                Event::Checkout { .. } => {
                    return Err(Error::Session("invalid checkout target".into()));
                }
                _ => expected_id,
            };
            records.push(record);
            offset += line.len() as u64;
            line.clear();
        }
        file.seek(SeekFrom::End(0))?;
        let mut session = Self {
            file,
            records,
            head,
            poisoned: false,
        };
        if session.records.is_empty() {
            session.append(Event::Created {
                session_id: Uuid::new_v4(),
            })?;
            #[cfg(unix)]
            {
                let directory = path
                    .as_ref()
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new("."));
                File::open(directory)?.sync_all()?;
            }
        }
        Ok(session)
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
                        OperationState::Started | OperationState::Finished(Outcome::Unknown(_))
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
            version: FORMAT_VERSION,
            id,
            parent: (self.head != 0).then_some(self.head),
            event,
        };
        let mut line = serde_json::to_vec(&record)?;
        line.push(b'\n');
        if line.len() > MAX_RECORD_BYTES {
            return Err(Error::Session("journal record exceeds size limit".into()));
        }
        if let Err(err) = self
            .file
            .write_all(&line)
            .and_then(|()| self.file.sync_data())
        {
            self.poisoned = true;
            return Err(err.into());
        }
        self.head = next_head;
        self.records.push(record);
        Ok(self.records.last().unwrap())
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
            Event::User { content } => messages.push(Message::User {
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
