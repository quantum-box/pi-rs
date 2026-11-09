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
    Created {
        session_id: Uuid,
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
    InputQueuedWithImages {
        input: crate::QueuedMessage,
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

    /// Restore a validated, in-memory snapshot without mutating its source.
    pub fn from_records(records: Vec<Record>) -> Result<Self> {
        Self::with_store(MemoryStore { records })
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

    pub fn set_name(&mut self, name: &str) -> Result<()> {
        self.append(Event::SessionInfo {
            name: Some(name.trim().into()),
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
        self.append(Event::Label { target, label })?;
        Ok(())
    }

    pub fn labels(&self) -> BTreeMap<u64, String> {
        let mut labels = BTreeMap::new();
        for record in &self.records {
            if let Event::Label { target, label } = &record.event {
                if let Some(label) = label {
                    labels.insert(*target, label.clone());
                } else {
                    labels.remove(target);
                }
            }
        }
        labels
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
                Event::Created { session_id } => *session_id = Uuid::new_v4(),
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
            Some(Some(replacement)) => Some(project_content(message, replacement)),
            Some(None) => None,
            None => Some(message),
        })
        .collect();
    if !edits.is_empty() {
        messages = repair_tool_pairs(messages);
    }
    messages
}

fn repair_tool_pairs(messages: Vec<Message>) -> Vec<Message> {
    let mut projected = Vec::new();
    let mut messages = messages.into_iter().peekable();
    while let Some(message) = messages.next() {
        match message {
            Message::Assistant { content, mut calls } => {
                let mut results = Vec::new();
                while matches!(messages.peek(), Some(Message::Tool { .. })) {
                    results.push(messages.next().expect("peeked tool result"));
                }
                let result_ids: std::collections::BTreeSet<_> = results
                    .iter()
                    .filter_map(|message| match message {
                        Message::Tool { call_id, .. } => Some(call_id.as_str()),
                        _ => None,
                    })
                    .collect();
                calls.retain(|call| result_ids.contains(call.id.as_str()));
                let call_ids: std::collections::BTreeSet<_> =
                    calls.iter().map(|call| call.id.clone()).collect();
                if !content.is_empty() || !calls.is_empty() {
                    projected.push(Message::Assistant { content, calls });
                }
                projected.extend(results.into_iter().filter(|message| matches!(message, Message::Tool { call_id, .. } if call_ids.contains(call_id))));
            }
            Message::Tool { .. } => {}
            message => projected.push(message),
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
