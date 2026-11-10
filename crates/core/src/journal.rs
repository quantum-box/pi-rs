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

/// A validated turn response stored before settling usage or dispatching tools.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordedReply {
    pub content: String,
    pub invocations: Vec<Invocation>,
    pub advertised_tools: Vec<String>,
    pub max_output_tokens: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
// Keep the public journal event shape stable as optional provider metadata grows.
#[allow(clippy::large_enum_variant)]
pub enum Event {
    /// Original external entry retained during an explicit session import.
    SourceEntry {
        format: String,
        entry: serde_json::Value,
    },
    /// Replace or omit one message in future model context, preserving raw history.
    ContextEdited {
        target: u64,
        replacement: Option<crate::ContextContent>,
    },
    /// Imported transcript instructions and ordered section/tool updates.
    SystemCheckpoint {
        content: String,
        #[serde(default)]
        sections: Vec<(String, Option<String>)>,
        #[serde(default)]
        tools_added: Vec<crate::ToolSpec>,
        #[serde(default)]
        tools_removed: Vec<String>,
    },
    Created {
        session_id: Uuid,
    },
    /// Originating session path, retained independently of branch context.
    ParentSession {
        path: String,
    },
    /// Presentation metadata does not enter model context.
    SessionInfo {
        name: Option<String>,
        cwd: Option<String>,
    },
    Label {
        target: u64,
        label: Option<String>,
    },
    ModelSelected {
        model: String,
        thinking: Option<String>,
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
    /// Atomic image-bearing user input. Legacy UserImages entries remain readable.
    UserWithImages {
        content: String,
        images: Vec<crate::ImageContent>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input_id: Option<Uuid>,
    },
    /// Images associated with the preceding user message on this branch.
    UserImages {
        images: Vec<crate::ImageContent>,
    },
    BashExecution {
        #[serde(flatten)]
        execution: crate::BashExecution,
    },
    InputQueuedWithImages {
        input: crate::QueuedMessage,
    },
    InputQueued {
        input: QueuedInput,
    },
    InputQueueCleared {
        ids: Vec<Uuid>,
    },
    QueueModeSelected {
        kind: crate::InputKind,
        mode: crate::QueueMode,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reply: Option<RecordedReply>,
    },
    TurnFinished {
        assistant_record: u64,
    },
    Assistant {
        content: String,
        invocations: Vec<Invocation>,
        usage: Option<Usage>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<Uuid>,
    },
    /// A tool requested by another tool. Only the outer result enters model context.
    ToolPlanned {
        invocation: Invocation,
        parent_operation: Uuid,
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
    pub timestamp: Option<String>,
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
    auto_compaction: crate::AutoCompactionControl,
    retry: crate::RetryControl,
    store: Box<dyn SessionStore>,
    records: Vec<Record>,
    head: u64,
    run_id: Option<Uuid>,
    poisoned: bool,
}

impl Session {
    pub fn retry_control(&self) -> crate::RetryControl {
        self.retry.clone()
    }
    pub fn set_retry_control(&mut self, control: crate::RetryControl) {
        self.retry = control;
    }

    /// Clone this control to change automatic compaction during an active run.
    pub fn auto_compaction_control(&self) -> crate::AutoCompactionControl {
        self.auto_compaction.clone()
    }

    /// Attach a host-owned control across successive openings of this session.
    pub fn set_auto_compaction_control(&mut self, control: crate::AutoCompactionControl) {
        self.auto_compaction = control;
    }

    /// Hold an exclusive OS lock for the lifetime of the session.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::with_store(FileStore::open(path.as_ref())?)
    }

    /// Read a stable, complete-record prefix without locking or repairing the source.
    pub fn snapshot(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        let length = file.metadata()?.len();
        let (records, _, _) = read_complete_records(file.take(length))?;
        if records.is_empty() {
            return Err(Error::Session("journal has no committed header".into()));
        }
        Self::from_records(records)
    }

    /// Restore a validated, in-memory snapshot without mutating its source.
    pub fn from_records(records: Vec<Record>) -> Result<Self> {
        Self::with_store(MemoryStore { records })
    }

    pub fn with_store(mut store: impl SessionStore + 'static) -> Result<Self> {
        let records = store.load()?;
        let head = validated_head(&records)?;
        let mut session = Self {
            auto_compaction: crate::AutoCompactionControl::default(),
            retry: crate::RetryControl::default(),
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
        Ok(project_messages(&branch, ops))
    }

    /// Current projected messages, including calls whose tools are still running.
    pub fn current_messages(&self) -> Vec<Message> {
        let branch = self.branch();
        project_messages(&branch, operations(&branch))
    }

    /// Measured context when usable, otherwise a visible-content estimate.
    pub fn estimated_context_tokens(&self) -> u64 {
        self.context_usage_estimate().tokens
    }

    pub fn context_usage_estimate(&self) -> crate::ContextUsageEstimate {
        let branch = self.branch();
        let entries = conversation_entries(&branch, self.operations());
        let invalidated_at = branch
            .iter()
            .rev()
            .find_map(|record| {
                matches!(
                    record.event,
                    Event::ContextEdited { .. }
                        | Event::Compacted { .. }
                        | Event::SystemCheckpoint { .. }
                        | Event::ModelSelected { .. }
                )
                .then_some(record.id)
            })
            .unwrap_or(0);
        let completions: BTreeMap<_, _> = branch
            .iter()
            .filter_map(|record| match &record.event {
                Event::ModelFinished {
                    request_id,
                    metadata,
                    failure,
                    ..
                } => Some((*request_id, (metadata, failure))),
                _ => None,
            })
            .collect();
        for (index, (source, message)) in entries.iter().enumerate().rev() {
            if !matches!(message, Message::Assistant { .. }) || *source <= invalidated_at {
                continue;
            }
            let Some(record) = self.records.get(source.saturating_sub(1) as usize) else {
                continue;
            };
            let Event::Assistant {
                usage: Some(usage),
                request_id,
                ..
            } = &record.event
            else {
                continue;
            };
            let completion =
                request_id.and_then(|request_id| completions.get(&request_id).copied());
            if completion.is_some_and(|(metadata, failure)| {
                failure.is_some()
                    || matches!(
                        metadata.finish_reason.as_deref(),
                        Some("error" | "aborted" | "cancelled")
                    )
            }) {
                continue;
            }
            let input = completion
                .and_then(|(metadata, _)| metadata.provider_prompt_tokens)
                .filter(|tokens| *tokens > 0)
                .unwrap_or(usage.input_tokens);
            let usage_tokens = input.saturating_add(usage.output_tokens);
            if usage_tokens == 0 {
                continue;
            }
            let trailing_tokens =
                entries[index + 1..]
                    .iter()
                    .fold(0_u64, |tokens, (source, message)| {
                        tokens.saturating_add(crate::tokens::estimate_source_tokens(
                            self, *source, message,
                        ))
                    });
            return crate::ContextUsageEstimate {
                tokens: usage_tokens.saturating_add(trailing_tokens),
                usage_tokens,
                trailing_tokens,
                last_usage_record: Some(*source),
            };
        }
        let summary = branch.iter().rev().find_map(|record| match &record.event {
            Event::Compacted { summary, .. } => Some(summary),
            _ => None,
        });
        let initial = summary.map_or(0, |summary| {
            crate::estimate_message_tokens(&Message::User {
                content: summary.clone(),
            })
        });
        let tokens = entries.iter().fold(initial, |tokens, (source, message)| {
            tokens.saturating_add(crate::tokens::estimate_source_tokens(
                self, *source, message,
            ))
        });
        crate::ContextUsageEstimate {
            tokens,
            usage_tokens: 0,
            trailing_tokens: tokens,
            last_usage_record: None,
        }
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

    pub fn model_selection(&self) -> Option<(String, Option<String>)> {
        let branch = self.branch();
        branch
            .iter()
            .rev()
            .find_map(|record| match &record.event {
                Event::ModelSelected { model, thinking } => Some((model.clone(), thinking.clone())),
                _ => None,
            })
            .or_else(|| {
                branch.iter().rev().find_map(|record| match &record.event {
                    Event::ModelRequested { model, .. } => Some((model.clone(), None)),
                    _ => None,
                })
            })
    }

    pub fn select_model(&mut self, model: &str, thinking: Option<&str>) -> Result<()> {
        if model.trim().is_empty() {
            return Err(Error::Config("model name is empty".into()));
        }
        if self.model_selection() != Some((model.into(), thinking.map(str::to_owned))) {
            self.append(Event::ModelSelected {
                model: model.into(),
                thinking: thinking.map(str::to_owned),
            })?;
        }
        Ok(())
    }

    pub fn name(&self) -> Option<&str> {
        self.records
            .iter()
            .rev()
            .find_map(|record| match &record.event {
                Event::SessionInfo {
                    name: Some(name), ..
                } => Some(name.as_str()),
                _ => None,
            })
            .filter(|name| !name.is_empty())
    }

    pub fn working_directory(&self) -> Option<&str> {
        self.records
            .iter()
            .rev()
            .find_map(|record| match &record.event {
                Event::SessionInfo { cwd: Some(cwd), .. } => Some(cwd.as_str()),
                _ => None,
            })
    }

    pub fn parent_session(&self) -> Option<&str> {
        self.records
            .iter()
            .rev()
            .find_map(|record| match &record.event {
                Event::ParentSession { path } => Some(path.as_str()),
                _ => None,
            })
    }

    /// Record provenance without reading or modifying the parent journal.
    pub fn set_parent_session(&mut self, path: &str) -> Result<()> {
        if path.is_empty() {
            return Err(Error::Config("parent session path cannot be empty".into()));
        }
        self.append(Event::ParentSession { path: path.into() })?;
        Ok(())
    }

    pub fn set_name(&mut self, name: &str) -> Result<()> {
        self.append(Event::SessionInfo {
            name: Some(sanitize_session_name(name)),
            cwd: None,
        })?;
        Ok(())
    }

    pub fn set_working_directory(&mut self, path: &Path) -> Result<()> {
        let cwd = path.canonicalize()?.to_string_lossy().into_owned();
        self.append(Event::SessionInfo {
            name: None,
            cwd: Some(cwd),
        })?;
        Ok(())
    }

    pub fn append_user_with_images(
        &mut self,
        content: String,
        images: Vec<crate::ImageContent>,
        input_id: Option<Uuid>,
    ) -> Result<()> {
        ensure_settled(self.operations())?;
        if content.trim().is_empty() && images.is_empty() {
            return Err(Error::Config("empty user input".into()));
        }
        for image in &images {
            image.validate()?;
        }
        let event = if images.is_empty() {
            Event::User { content, input_id }
        } else {
            Event::UserWithImages {
                content,
                images,
                input_id,
            }
        };
        if serde_json::to_vec(&event)?.len() > 8 * 1024 * 1024 {
            return Err(Error::Config("user input exceeds size limit".into()));
        }
        self.append(event)?;
        Ok(())
    }

    pub fn queued_inputs(&self) -> Vec<crate::QueuedMessage> {
        let branch = self.branch();
        let mut consumed = std::collections::HashSet::new();
        for record in &branch {
            match &record.event {
                Event::User {
                    input_id: Some(id), ..
                }
                | Event::UserWithImages {
                    input_id: Some(id), ..
                } => {
                    consumed.insert(*id);
                }
                Event::InputQueueCleared { ids } => consumed.extend(ids),
                _ => {}
            }
        }
        branch
            .iter()
            .filter_map(|record| match &record.event {
                Event::InputQueued { input } if !consumed.contains(&input.id) => {
                    Some(input.clone().into())
                }
                Event::InputQueuedWithImages { input } if !consumed.contains(&input.input.id) => {
                    Some(input.clone())
                }
                _ => None,
            })
            .collect()
    }
    pub fn clear_queued_inputs(&mut self) -> Result<Vec<crate::QueuedMessage>> {
        let pending = self.queued_inputs();
        if !pending.is_empty() {
            self.append(Event::InputQueueCleared {
                ids: pending.iter().map(|input| input.input.id).collect(),
            })?;
        }
        Ok(pending)
    }
    pub fn queue_mode(&self, kind: crate::InputKind) -> crate::QueueMode {
        self.selected_queue_mode(kind).unwrap_or_default()
    }
    fn selected_queue_mode(&self, kind: crate::InputKind) -> Option<crate::QueueMode> {
        self.branch()
            .iter()
            .rev()
            .find_map(|record| match record.event {
                Event::QueueModeSelected {
                    kind: selected,
                    mode,
                } if kind == selected => Some(mode),
                _ => None,
            })
    }
    pub fn set_queue_mode(&mut self, kind: crate::InputKind, mode: crate::QueueMode) -> Result<()> {
        if self.selected_queue_mode(kind) != Some(mode) {
            self.append(Event::QueueModeSelected { kind, mode })?;
        }
        Ok(())
    }

    /// Check that no tool invocation needs completion or reconciliation.
    pub fn ensure_settled(&self) -> Result<()> {
        ensure_settled(self.operations())
    }

    pub fn append_bash_execution(&mut self, execution: crate::BashExecution) -> Result<()> {
        self.ensure_settled()?;
        if execution.command.trim().is_empty() {
            return Err(Error::Config("empty shell command".into()));
        }
        let event = Event::BashExecution { execution };
        if serde_json::to_vec(&event)?.len() > 8 * 1024 * 1024 {
            return Err(Error::Config("shell execution exceeds size limit".into()));
        }
        self.append(event)?;
        Ok(())
    }

    pub fn edit_context(
        &mut self,
        target: u64,
        replacement: Option<crate::ContextContent>,
    ) -> Result<()> {
        ensure_settled(self.operations())?;
        if !self.branch().iter().any(|record| record.id == target) {
            return Err(Error::Session(
                "context edit target is outside the active branch".into(),
            ));
        }
        validate_context_edit(&self.records, target, replacement.as_ref())?;
        self.append(Event::ContextEdited {
            target,
            replacement,
        })?;
        Ok(())
    }

    pub fn set_label(&mut self, target: u64, label: Option<String>) -> Result<()> {
        if target == 0 || target > self.records.len() as u64 {
            return Err(Error::Session("invalid label target".into()));
        }
        let label = label.filter(|label| !label.is_empty());
        self.append(Event::Label { target, label })?;
        Ok(())
    }

    pub fn labels(&self) -> BTreeMap<u64, String> {
        let mut labels = BTreeMap::new();
        for record in &self.records {
            if let Event::Label { target, label } = &record.event {
                if let Some(label) = label.as_ref().filter(|label| !label.is_empty()) {
                    labels.insert(*target, label.clone());
                } else {
                    labels.remove(target);
                }
            }
        }
        labels
    }

    /// Latest user or assistant activity, falling back to the session header time.
    pub fn activity_time(&self) -> Option<std::time::SystemTime> {
        let parse = |value: &str| {
            chrono::DateTime::parse_from_rfc3339(value)
                .ok()
                .map(|time| time.to_utc())
        };
        let mut latest = None;
        let mut imported = None;
        for record in &self.records {
            match &record.event {
                Event::SourceEntry { format, entry }
                    if format == "pi"
                        && entry["type"] == "message"
                        && matches!(
                            entry["message"]["role"].as_str(),
                            Some("user" | "assistant")
                        ) =>
                {
                    imported = Some(
                        entry["message"]["timestamp"]
                            .as_i64()
                            .and_then(chrono::DateTime::from_timestamp_millis)
                            .or_else(|| entry["timestamp"].as_str().and_then(parse)),
                    );
                }
                Event::User { .. } | Event::UserWithImages { .. } | Event::Assistant { .. } => {
                    let time = imported
                        .take()
                        .unwrap_or_else(|| record.timestamp.as_deref().and_then(parse));
                    latest = latest.max(time);
                }
                _ => {}
            }
        }
        latest
            .filter(|time| time.timestamp_millis() > 0)
            .or_else(|| self.records.first()?.timestamp.as_deref().and_then(parse))
            .map(std::time::SystemTime::from)
    }

    /// Last known label modification time, absent for legacy records without timestamps.
    pub fn label_timestamps(&self) -> BTreeMap<u64, String> {
        let mut timestamps = BTreeMap::new();
        for record in &self.records {
            if let Event::Label { target, label } = &record.event {
                if label.as_ref().is_some_and(|label| !label.is_empty()) {
                    if let Some(timestamp) = &record.timestamp {
                        timestamps.insert(*target, timestamp.clone());
                    } else {
                        timestamps.remove(target);
                    }
                } else {
                    timestamps.remove(target);
                }
            }
        }
        timestamps
    }

    /// Export one settled branch with fresh session identity and remapped record references.
    pub fn fork_records(&self, target: u64) -> Result<Vec<Record>> {
        if self.host_binding().is_some() {
            return Err(Error::Session(
                "managed sessions must be forked by their host".into(),
            ));
        }
        self.conversation_at(target)?;
        let branch = self.path(target);
        let ids: BTreeMap<_, _> = branch
            .iter()
            .enumerate()
            .map(|(index, record)| (record.id, index as u64 + 1))
            .collect();
        let mut records = Vec::new();
        for record in branch {
            let mut record = record.clone();
            record.id = ids[&record.id];
            record.parent = record.parent.and_then(|parent| ids.get(&parent).copied());
            match &mut record.event {
                Event::Created { session_id } => {
                    *session_id = Uuid::new_v4();
                    record.timestamp = Some(record_timestamp());
                }
                Event::ModelRequested { context_head, .. } => {
                    *context_head = context_head.and_then(|id| ids.get(&id).copied());
                }
                Event::TurnFinished { assistant_record } => {
                    *assistant_record = *ids
                        .get(assistant_record)
                        .ok_or_else(|| Error::Session("missing fork response".into()))?;
                }
                Event::Compacted { retain_from, .. } => {
                    *retain_from = retain_from.and_then(|id| ids.get(&id).copied());
                }
                Event::Label { target, .. } | Event::ContextEdited { target, .. } => {
                    if let Some(id) = ids.get(target) {
                        *target = *id;
                    } else {
                        record.event = Event::SessionInfo {
                            name: None,
                            cwd: None,
                        };
                    }
                }
                _ => {}
            }
            records.push(record);
        }
        validated_head(&records)?;
        Ok(records)
    }

    /// Publish a complete local journal without replacing an existing file.
    pub fn export_jsonl(&self, destination: impl AsRef<Path>) -> Result<()> {
        if self.host_binding().is_some() {
            return Err(Error::Session(
                "managed sessions must be exported by their host".into(),
            ));
        }
        publish_snapshot(&self.records, destination.as_ref())
    }

    /// Create and open an independent, persistent branch with fresh session identity.
    pub fn fork_to(&self, target: u64, destination: impl AsRef<Path>) -> Result<Self> {
        let records = self.fork_records(target)?;
        publish_snapshot(&records, destination.as_ref())?;
        Self::open(destination)
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
        if !matches!(
            outcome,
            Outcome::Succeeded(_) | Outcome::SucceededWithDetails { .. } | Outcome::Failed(_)
        ) || !self.operations().iter().any(|(inv, state)| {
            inv.operation_id == operation_id
                && matches!(
                    state,
                    OperationState::Started
                        | OperationState::Finished(Outcome::Unknown(_) | Outcome::Pending { .. })
                )
        }) {
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
            timestamp: Some(record_timestamp()),
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

fn publish_snapshot(records: &[Record], destination: &Path) -> Result<()> {
    validated_head(records)?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    for record in records {
        serde_json::to_writer(&mut temporary, record)?;
        writeln!(temporary)?;
    }
    temporary.as_file().sync_all()?;
    temporary
        .persist_noclobber(destination)
        .map_err(|error| Error::Io(error.error))?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn validate_context_edit(
    records: &[Record],
    target: u64,
    replacement: Option<&crate::ContextContent>,
) -> Result<()> {
    let record = target
        .checked_sub(1)
        .and_then(|index| records.get(index as usize))
        .ok_or_else(|| Error::Session("invalid context edit target".into()))?;
    let assistant = matches!(record.event, Event::Assistant { .. });
    if !assistant
        && !matches!(
            record.event,
            Event::User { .. }
                | Event::UserWithImages { .. }
                | Event::ToolFinished { .. }
                | Event::ToolReconciled { .. }
        )
    {
        return Err(Error::Session(
            "context edits require a user, assistant or tool-result target".into(),
        ));
    }
    if let Some(replacement) = replacement {
        if serde_json::to_vec(replacement)?.len() > 8 * 1024 * 1024 {
            return Err(Error::Session(
                "context replacement exceeds size limit".into(),
            ));
        }
        if let crate::ContextContent::Blocks(blocks) = replacement {
            for block in blocks {
                match block {
                    crate::ContentBlock::Thinking { .. } | crate::ContentBlock::ToolCall { .. }
                        if !assistant =>
                    {
                        return Err(Error::Session(
                            "thinking and tool-call blocks require an assistant target".into(),
                        ));
                    }
                    crate::ContentBlock::ToolCall {
                        id,
                        name,
                        arguments,
                    } if id.is_empty() || name.is_empty() || !arguments.is_object() => {
                        return Err(Error::Session("invalid replacement tool call".into()));
                    }
                    crate::ContentBlock::Image { mime_type, .. }
                        if assistant
                            || !matches!(
                                mime_type.as_str(),
                                "image/png" | "image/jpeg" | "image/gif" | "image/webp"
                            ) =>
                    {
                        return Err(Error::Session(
                            "invalid image block for context target".into(),
                        ));
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
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
        if let Event::ContextEdited {
            target,
            replacement,
        } = &record.event
        {
            if *target >= id {
                return Err(Error::Session(
                    "context edit target must precede the edit".into(),
                ));
            }
            validate_context_edit(records, *target, replacement.as_ref())?;
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

impl Drop for FileStore {
    fn drop(&mut self) {
        // Release the lock even when a concurrently spawned child still holds a
        // duplicate file descriptor until its exec completes.
        let _ = self.file.unlock();
    }
}

fn read_complete_records(reader: impl Read) -> Result<(Vec<Record>, u64, bool)> {
    let mut records = Vec::new();
    let mut offset = 0;
    let mut reader = BufReader::new(reader);
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
            return Ok((records, offset, true));
        }
        records.push(serde_json::from_slice(&line)?);
        offset += line.len() as u64;
        line.clear();
    }
    Ok((records, offset, false))
}

impl SessionStore for FileStore {
    fn load(&mut self) -> Result<Vec<Record>> {
        let (records, offset, incomplete) = read_complete_records(&mut self.file)?;
        if incomplete {
            validated_head(&records)?;
            self.file.set_len(offset)?;
            self.file.sync_all()?;
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
            Event::ToolPlanned { invocation, .. } => {
                indices.insert(invocation.operation_id, ops.len());
                ops.push((invocation.clone(), OperationState::Planned));
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

pub(crate) fn system_context(
    path: &[&Record],
    fallback: &str,
    available: Vec<crate::ToolSpec>,
) -> (String, Vec<crate::ToolSpec>) {
    let mut content = Vec::new();
    let mut sections: Vec<(String, String)> = Vec::new();
    let mut tools = Vec::new();
    let mut found = false;
    for record in path {
        let Event::SystemCheckpoint {
            content: text,
            sections: changes,
            tools_added,
            tools_removed,
        } = &record.event
        else {
            continue;
        };
        found = true;
        if !text.is_empty() {
            content.push(text.clone());
        }
        for (name, value) in changes {
            match (sections.iter().position(|(key, _)| key == name), value) {
                (Some(index), Some(value)) => sections[index].1 = value.clone(),
                (Some(index), None) => {
                    sections.remove(index);
                }
                (None, Some(value)) => sections.push((name.clone(), value.clone())),
                (None, None) => {}
            }
        }
        tools.retain(|tool: &crate::ToolSpec| !tools_removed.contains(&tool.name));
        for declared in tools_added {
            // Imported declarations select current native implementations and schemas.
            if let Some(registered) = available.iter().find(|tool| tool.name == declared.name) {
                if let Some(index) = tools.iter().position(|tool| tool.name == declared.name) {
                    tools[index] = registered.clone();
                } else {
                    tools.push(registered.clone());
                }
            }
        }
    }
    if !found {
        return (fallback.into(), available);
    }
    content.extend(
        sections
            .into_iter()
            .map(|(_, text)| text)
            .filter(|text| !text.is_empty()),
    );
    (content.join("\n\n"), tools)
}

fn project_messages(
    branch: &[&Record],
    operations: Vec<(Invocation, OperationState)>,
) -> Vec<Message> {
    let mut messages = Vec::new();
    if let Some(summary) = branch.iter().rev().find_map(|record| match &record.event {
        Event::Compacted { summary, .. } => Some(summary),
        _ => None,
    }) {
        messages.push(Message::User {
            content: format!("Prior conversation summary:\n{summary}"),
        });
    }
    messages.extend(conversation(branch, operations));
    messages
}

pub(crate) fn conversation(
    path: &[&Record],
    operations: Vec<(Invocation, OperationState)>,
) -> Vec<Message> {
    conversation_entries(path, operations)
        .into_iter()
        .map(|(_, message)| message)
        .collect()
}

pub(crate) fn conversation_entries(
    path: &[&Record],
    operations: Vec<(Invocation, OperationState)>,
) -> Vec<(u64, Message)> {
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
    let result_sources: BTreeMap<_, _> = path
        .iter()
        .filter_map(|record| match record.event {
            Event::ToolFinished { operation_id, .. }
            | Event::ToolReconciled { operation_id, .. } => Some((operation_id, record.id)),
            _ => None,
        })
        .collect();
    let edits: BTreeMap<_, _> = path
        .iter()
        .filter_map(|record| match &record.event {
            Event::ContextEdited {
                target,
                replacement,
            } => Some((*target, replacement)),
            _ => None,
        })
        .collect();
    let mut messages = Vec::new();
    for record in path.iter().filter(|record| record.id >= retain_from) {
        match &record.event {
            Event::User { content, .. } => messages.push((
                record.id,
                Message::User {
                    content: content.clone(),
                },
            )),
            Event::BashExecution { execution } if !execution.exclude_from_context => {
                messages.push((
                    record.id,
                    Message::User {
                        content: execution.context_text(),
                    },
                ))
            }
            Event::UserWithImages {
                content, images, ..
            } => messages.push((
                record.id,
                Message::UserWithImages {
                    content: content.clone(),
                    images: images.clone(),
                },
            )),
            Event::UserImages { images } => {
                if let Some((_, message @ Message::User { .. })) = messages.last_mut() {
                    let Message::User { content } = message else {
                        unreachable!()
                    };
                    *message = Message::UserWithImages {
                        content: content.clone(),
                        images: images.clone(),
                    };
                }
            }
            Event::Assistant {
                content,
                invocations,
                ..
            } => {
                messages.push((
                    record.id,
                    Message::Assistant {
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
                    },
                ));
                for inv in invocations {
                    if let Some(outcome) = results.get(&inv.operation_id) {
                        messages.push((
                            *result_sources.get(&inv.operation_id).unwrap_or(&record.id),
                            Message::Tool {
                                call_id: inv.call.id.clone(),
                                content: outcome.tool_content(),
                            },
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    let mut messages: Vec<_> = messages
        .into_iter()
        .filter_map(|(source, message)| match edits.get(&source) {
            Some(Some(replacement)) => Some((source, project_content(message, replacement))),
            Some(None) => None,
            None => Some((source, message)),
        })
        .collect();
    if !edits.is_empty() {
        messages = repair_tool_pairs(messages);
    }
    messages
}

fn repair_tool_pairs(messages: Vec<(u64, Message)>) -> Vec<(u64, Message)> {
    let mut projected = Vec::new();
    let mut messages = messages.into_iter().peekable();
    while let Some((source, message)) = messages.next() {
        match message {
            Message::Assistant { content, mut calls } => {
                let mut results = Vec::new();
                while matches!(messages.peek(), Some((_, Message::Tool { .. }))) {
                    results.push(messages.next().expect("peeked tool result"));
                }
                let result_ids: std::collections::BTreeSet<_> = results
                    .iter()
                    .filter_map(|(_, message)| match message {
                        Message::Tool { call_id, .. } => Some(call_id.as_str()),
                        _ => None,
                    })
                    .collect();
                calls.retain(|call| result_ids.contains(call.id.as_str()));
                let call_ids: std::collections::BTreeSet<_> =
                    calls.iter().map(|call| call.id.clone()).collect();
                if !content.is_empty() || !calls.is_empty() {
                    projected.push((source, Message::Assistant { content, calls }));
                }
                projected.extend(results.into_iter().filter(|(_, message)| matches!(message, Message::Tool { call_id, .. } if call_ids.contains(call_id))));
            }
            Message::Tool { .. } => {}
            message => projected.push((source, message)),
        }
    }
    projected
}

fn project_content(message: Message, replacement: &crate::ContextContent) -> Message {
    use crate::{ContentBlock, ContextContent};
    let blocks = match replacement {
        ContextContent::Text(text) => vec![ContentBlock::Text { text: text.clone() }],
        ContextContent::Blocks(blocks) => blocks.clone(),
    };
    let text = blocks
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("");
    let images: Vec<_> = blocks
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Image { data, mime_type } => Some(crate::ImageContent {
                data: data.clone(),
                mime_type: mime_type.clone(),
            }),
            _ => None,
        })
        .collect();
    match message {
        Message::User { .. } | Message::UserWithImages { .. } if images.is_empty() => {
            Message::User { content: text }
        }
        Message::User { .. } | Message::UserWithImages { .. } => Message::UserWithImages {
            content: text,
            images,
        },
        Message::Assistant { .. } => Message::Assistant {
            content: text,
            calls: blocks
                .into_iter()
                .filter_map(|block| match block {
                    ContentBlock::ToolCall {
                        id,
                        name,
                        arguments,
                    } => Some(crate::ToolCall {
                        id,
                        name,
                        arguments: arguments.to_string(),
                    }),
                    _ => None,
                })
                .collect(),
        },
        Message::Tool { call_id, .. } => Message::Tool {
            call_id,
            content: if images.is_empty() {
                text
            } else {
                Outcome::Structured {
                    data: serde_json::json!({"content":blocks}),
                    attachments: Vec::new(),
                }
                .tool_content()
            },
        },
        other => other,
    }
}

#[cfg(test)]
mod session_metadata_tests {
    use super::*;

    #[test]
    fn dropping_the_store_unlocks_duplicated_file_descriptors() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let store = FileStore::open(&path).unwrap();
        let duplicate = store.file.try_clone().unwrap();
        drop(store);
        let session = Session::open(&path).unwrap();
        drop(session);
        drop(duplicate);
    }

    #[test]
    fn model_and_thinking_selection_follow_the_active_branch() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session.select_model("first", Some("high")).unwrap();
        let first = session.head();
        session.select_model("second", Some("off")).unwrap();
        session.checkout(first).unwrap();
        assert_eq!(
            session.model_selection(),
            Some(("first".into(), Some("high".into())))
        );
        let count = session.records().len();
        session.select_model("first", Some("high")).unwrap();
        assert_eq!(session.records().len(), count);
    }

    #[test]
    fn fork_remaps_branch_references_and_keeps_the_original_tree() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append(Event::User {
                content: "question".into(),
                input_id: None,
            })
            .unwrap();
        session
            .append(Event::Assistant {
                content: "old answer".into(),
                invocations: vec![],
                usage: None,
                request_id: None,
            })
            .unwrap();
        session
            .append(Event::TurnFinished {
                assistant_record: 3,
            })
            .unwrap();
        session.checkout(2).unwrap();
        session
            .append(Event::Assistant {
                content: "new answer".into(),
                invocations: vec![],
                usage: None,
                request_id: None,
            })
            .unwrap();
        session
            .append(Event::TurnFinished {
                assistant_record: 6,
            })
            .unwrap();
        session.set_label(6, Some("chosen".into())).unwrap();
        session.set_name("experiment").unwrap();
        let source_records = session.records().len();
        let records = session.fork_records(session.head()).unwrap();
        let fork = Session::with_store(MemoryStore { records }).unwrap();
        assert_ne!(fork.session_id(), session.session_id());
        assert_eq!(fork.name(), Some("experiment"));
        assert_eq!(fork.labels().get(&3).map(String::as_str), Some("chosen"));
        assert!(matches!(
            fork.records()[3].event,
            Event::TurnFinished {
                assistant_record: 3
            }
        ));
        assert_eq!(
            fork.conversation_at(fork.head()).unwrap(),
            session.conversation_at(session.head()).unwrap()
        );
        assert_eq!(session.records().len(), source_records);
        assert!(session.records().iter().any(|record| matches!(&record.event, Event::Assistant { content, .. } if content == "old answer")));
    }

    #[test]
    fn host_bound_sessions_cannot_be_forked_as_local_sessions() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session.bind_host("tenant-scope").unwrap();
        assert!(session.fork_records(session.head()).is_err());
    }
}

#[cfg(test)]
mod context_edit_tests {
    use super::*;
    use crate::{ContextContent, ToolCall};

    #[test]
    fn edits_leave_history_intact_and_follow_forks() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let target = session
            .append(Event::User {
                content: "original".into(),
                input_id: None,
            })
            .unwrap()
            .id;
        session
            .edit_context(target, Some(ContextContent::Text("replacement".into())))
            .unwrap();
        let edited = session.head();
        assert_eq!(
            session.conversation_at(edited).unwrap(),
            vec![Message::User {
                content: "replacement".into()
            }]
        );
        assert!(
            matches!(&session.records()[target as usize - 1].event, Event::User { content, .. } if content == "original")
        );
        let fork = Session::from_records(session.fork_records(edited).unwrap()).unwrap();
        assert_eq!(
            fork.conversation_at(fork.head()).unwrap(),
            session.conversation_at(edited).unwrap()
        );
        session.checkout(target).unwrap();
        assert_eq!(
            session.conversation_at(target).unwrap(),
            vec![Message::User {
                content: "original".into()
            }]
        );
        let count = session.records().len();
        assert!(session.edit_context(1, None).is_err());
        assert_eq!(session.records().len(), count);
    }

    #[test]
    fn fork_remaps_context_targets_after_skipping_an_abandoned_branch() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let root = session
            .append(Event::User {
                content: "root".into(),
                input_id: None,
            })
            .unwrap()
            .id;
        session
            .append(Event::User {
                content: "abandoned".into(),
                input_id: None,
            })
            .unwrap();
        session.checkout(root).unwrap();
        let target = session
            .append(Event::User {
                content: "kept".into(),
                input_id: None,
            })
            .unwrap()
            .id;
        session
            .edit_context(target, Some(ContextContent::Text("edited".into())))
            .unwrap();
        let fork = Session::from_records(session.fork_records(session.head()).unwrap()).unwrap();
        assert!(
            matches!(&fork.conversation_at(fork.head()).unwrap()[1], Message::User { content } if content == "edited")
        );
        assert!(
            fork.records()
                .iter()
                .any(|record| matches!(record.event, Event::ContextEdited { target: 3, .. }))
        );
    }

    #[test]
    fn deleting_results_preserves_pairs_even_when_call_ids_repeat() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append(Event::User {
                content: "start".into(),
                input_id: None,
            })
            .unwrap();
        let mut targets = Vec::new();
        for output in ["first", "second"] {
            let operation_id = Uuid::new_v4();
            session
                .append(Event::Assistant {
                    content: String::new(),
                    invocations: vec![Invocation {
                        operation_id,
                        call: ToolCall {
                            id: "reused".into(),
                            name: "read".into(),
                            arguments: "{}".into(),
                        },
                    }],
                    usage: None,
                    request_id: None,
                })
                .unwrap();
            targets.push(
                session
                    .append(Event::ToolFinished {
                        operation_id,
                        outcome: Outcome::Succeeded(output.into()),
                        states: Default::default(),
                    })
                    .unwrap()
                    .id,
            );
        }
        session.edit_context(targets[0], None).unwrap();
        let messages = session.conversation_at(session.head()).unwrap();
        assert_eq!(messages.len(), 3);
        assert!(matches!(&messages[2], Message::Tool { content, .. } if content == "second"));
        assert_eq!(session.operations().len(), 2);
        assert!(
            matches!(&session.operations()[0].1, OperationState::Finished(Outcome::Succeeded(content)) if content == "first")
        );
    }
}

#[cfg(test)]
mod shell_tests {
    use super::*;
    #[test]
    fn user_shell_context_exclusions_survive_journal_reload() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let mut session = Session::open(&path).unwrap();
        let execution = crate::BashExecution {
            command: "printf visible".into(),
            output: "visible".into(),
            exit_code: Some(0),
            cancelled: false,
            truncated: false,
            full_output_path: None,
            exclude_from_context: false,
        };
        session.append_bash_execution(execution.clone()).unwrap();
        session
            .append_bash_execution(crate::BashExecution {
                output: "secret".into(),
                exclude_from_context: true,
                ..execution
            })
            .unwrap();
        drop(session);
        let session = Session::open(&path).unwrap();
        let messages = session.conversation_at(session.head()).unwrap();
        assert_eq!(messages.len(), 1);
        assert!(
            matches!(&messages[0], Message::User { content } if content.contains("visible") && !content.contains("secret"))
        );
        assert_eq!(
            session
                .branch()
                .iter()
                .filter(|record| matches!(record.event, Event::BashExecution { .. }))
                .count(),
            2
        );
    }
}

#[cfg(test)]
mod system_checkpoint_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn system_updates_follow_branch_history_and_current_tool_permissions() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let tool = crate::ToolSpec {
            name: "read".into(),
            description: "Current native tool".into(),
            parameters: json!({"type":"object"}),
        };
        assert_eq!(
            system_context(&session.branch(), "fallback", vec![tool.clone()]),
            ("fallback".into(), vec![tool.clone()])
        );
        session
            .append(Event::SystemCheckpoint {
                content: "base".into(),
                sections: vec![
                    ("z".into(), Some("first".into())),
                    ("a".into(), Some("second".into())),
                ],
                tools_added: vec![crate::ToolSpec {
                    description: "Outdated declaration".into(),
                    ..tool.clone()
                }],
                tools_removed: vec![],
            })
            .unwrap();
        let initial = session.head();
        session
            .append(Event::SystemCheckpoint {
                content: "addition".into(),
                sections: vec![
                    ("z".into(), Some("replacement".into())),
                    ("a".into(), None),
                    ("last".into(), Some("last section".into())),
                ],
                tools_added: vec![],
                tools_removed: vec!["read".into()],
            })
            .unwrap();
        assert_eq!(
            system_context(&session.branch(), "fallback", vec![tool.clone()]),
            (
                "base\n\naddition\n\nreplacement\n\nlast section".into(),
                vec![]
            )
        );
        session.checkout(initial).unwrap();
        assert_eq!(
            system_context(&session.branch(), "fallback", vec![tool.clone()]),
            ("base\n\nfirst\n\nsecond".into(), vec![tool])
        );
        assert!(
            system_context(&session.branch(), "fallback", vec![])
                .1
                .is_empty()
        );
    }
}

fn record_timestamp() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Normalize a display name using Pi's line-break and ECMAScript whitespace rules.
pub fn sanitize_session_name(name: &str) -> String {
    name.split(['\r', '\n'])
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
        .trim_matches(|character| {
            matches!(character,
                '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' |
                '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' |
                '\u{205f}' | '\u{3000}' | '\u{feff}'
            )
        })
        .to_owned()
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    #[test]
    fn activity_time_tracks_conversation_and_ignores_metadata_changes() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append_user_with_images("hello".into(), vec![], None)
            .unwrap();
        session.set_name("later rename").unwrap();
        let mut records = session.records().to_vec();
        records[0].timestamp = Some("2019-01-01T00:00:00.000Z".into());
        records[1].timestamp = Some("2020-01-01T00:00:00.000Z".into());
        records[2].timestamp = Some("2030-01-01T00:00:00.000Z".into());
        let restored = Session::from_records(records.clone()).unwrap();
        let expected = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap();
        assert_eq!(restored.activity_time(), Some(expected.into()));
        records[1].timestamp = None;
        let restored = Session::from_records(records).unwrap();
        let header = chrono::DateTime::parse_from_rfc3339("2019-01-01T00:00:00Z").unwrap();
        assert_eq!(restored.activity_time(), Some(header.into()));
    }

    #[test]
    fn timestamps_survive_reopening_and_legacy_records_remain_undated() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session.set_name("dated").unwrap();
        for record in session.records() {
            let timestamp = record.timestamp.as_deref().unwrap();
            assert!(chrono::DateTime::parse_from_rfc3339(timestamp).is_ok());
            assert!(timestamp.ends_with('Z'));
        }
        let original = session.records().to_vec();
        let restored = Session::from_records(original.clone()).unwrap();
        assert_eq!(restored.records()[1].timestamp, original[1].timestamp);
        let mut legacy = serde_json::to_value(&original).unwrap();
        for record in legacy.as_array_mut().unwrap() {
            record.as_object_mut().unwrap().remove("timestamp");
        }
        let legacy: Vec<Record> = serde_json::from_value(legacy).unwrap();
        let restored = Session::from_records(legacy).unwrap();
        assert!(
            restored
                .records()
                .iter()
                .all(|record| record.timestamp.is_none())
        );
        let fork = session.fork_records(session.head()).unwrap();
        assert_eq!(fork[1].timestamp, original[1].timestamp);
        assert!(fork[0].timestamp.is_some());
    }

    #[test]
    fn label_timestamps_track_the_latest_change_and_clear_with_labels() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session.set_label(1, Some("first".into())).unwrap();
        assert_eq!(
            session.label_timestamps().get(&1),
            session.records().last().unwrap().timestamp.as_ref()
        );
        let mut records = session.records().to_vec();
        records.last_mut().unwrap().timestamp = None;
        let restored = Session::from_records(records).unwrap();
        assert!(restored.label_timestamps().is_empty());
        session.set_label(1, None).unwrap();
        assert!(session.label_timestamps().is_empty());
    }

    #[test]
    fn empty_labels_clear_bookmarks_in_new_and_legacy_records() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append_user_with_images("question".into(), vec![], None)
            .unwrap();
        let target = session.head();
        session.set_label(target, Some("bookmark".into())).unwrap();
        assert_eq!(
            session.labels().get(&target).map(String::as_str),
            Some("bookmark")
        );
        session.set_label(target, Some(String::new())).unwrap();
        assert!(session.labels().is_empty());
        assert!(matches!(
            &session.records().last().unwrap().event,
            Event::Label { label: None, .. }
        ));
        session.set_label(target, Some(" ".into())).unwrap();
        assert_eq!(session.labels().get(&target).map(String::as_str), Some(" "));
        session
            .append(Event::Label {
                target,
                label: Some(String::new()),
            })
            .unwrap();
        let restored = Session::from_records(session.records().to_vec()).unwrap();
        assert!(restored.labels().is_empty());
        let before = session.records().len();
        assert!(session.set_label(0, None).is_err());
        assert!(session.set_label(u64::MAX, None).is_err());
        assert_eq!(session.records().len(), before);
    }

    #[test]
    fn display_names_sanitize_line_breaks_and_allow_explicit_clearing() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .set_name("\u{feff}  first\r\n\nsecond  \u{3000}")
            .unwrap();
        assert_eq!(session.name(), Some("first second"));
        session.set_name("\u{0085}title\u{0085}").unwrap();
        assert_eq!(session.name(), Some("\u{0085}title\u{0085}"));
        session.set_name("\u{feff}\t ").unwrap();
        assert!(session.name().is_none());
        let restored = Session::from_records(session.records().to_vec()).unwrap();
        assert!(restored.name().is_none());
    }

    #[test]
    fn snapshots_ignore_incomplete_tails_without_repairing_files() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let mut session = Session::open(&path).unwrap();
        session
            .append_user_with_images("hello".into(), vec![], None)
            .unwrap();
        let head = session.head();
        drop(session);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{\"incomplete\":\"").unwrap();
        drop(file);
        let before = std::fs::read(&path).unwrap();
        let snapshot = Session::snapshot(&path).unwrap();
        assert_eq!(snapshot.head(), head);
        assert_eq!(snapshot.current_messages().len(), 1);
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let repaired = Session::open(&path).unwrap();
        assert_eq!(repaired.head(), head);
        drop(repaired);
        assert!(std::fs::read(&path).unwrap().len() < before.len());
    }

    #[test]
    fn snapshot_errors_do_not_create_or_modify_sources() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("missing.jsonl");
        assert!(Session::snapshot(&path).is_err());
        assert!(!path.exists());
        std::fs::write(&path, b"").unwrap();
        assert!(Session::snapshot(&path).is_err());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        let session = Session::open(&path).unwrap();
        drop(session);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"invalid\n")
            .unwrap();
        let before = std::fs::read(&path).unwrap();
        assert!(Session::snapshot(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn current_messages_include_pending_tool_calls_without_weakening_forks() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append_user_with_images("hello".into(), vec![], None)
            .unwrap();
        let operation_id = Uuid::new_v4();
        session
            .append(Event::Assistant {
                content: "working".into(),
                invocations: vec![Invocation {
                    operation_id,
                    call: crate::ToolCall {
                        id: "call".into(),
                        name: "read".into(),
                        arguments: "{}".into(),
                    },
                }],
                usage: None,
                request_id: None,
            })
            .unwrap();
        session.append(Event::ToolStarted { operation_id }).unwrap();
        assert!(session.conversation_at(session.head()).is_err());
        assert!(
            matches!(&session.current_messages()[1], Message::Assistant { calls, .. } if calls.len()==1)
        );
        session
            .append(Event::ToolFinished {
                operation_id,
                outcome: Outcome::Succeeded("done".into()),
                states: Default::default(),
            })
            .unwrap();
        assert_eq!(
            session.current_messages(),
            session.conversation_at(session.head()).unwrap()
        );
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_does_not_take_or_release_the_writer_lock() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let session = Session::open(&path).unwrap();
        assert_eq!(
            Session::snapshot(&path).unwrap().session_id(),
            session.session_id()
        );
        assert!(Session::open(&path).is_err());
        drop(session);
        assert!(Session::open(&path).is_ok());
    }
}
