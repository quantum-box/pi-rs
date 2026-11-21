use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    Error, ExtensionStates, Invocation, Message, ModelMetadata, Outcome, QueuedInput, Result,
    Usage, UsageReport,
};

pub(crate) const TURN_END_JOURNAL_VERSION: u32 = 3;
const BRANCH_FALLBACK_JOURNAL_VERSION: u32 = 4;
pub(crate) const TOOL_DECISION_JOURNAL_VERSION: u32 = 5;
const TOOL_EXECUTION_JOURNAL_VERSION: u32 = 6;
const FORMAT_VERSION: u32 = TOOL_EXECUTION_JOURNAL_VERSION;
const MAX_TOOL_DECISION_METADATA_BYTES: usize = 4 * 1024 * 1024;
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

/// Billing retained with a summary, including when its request is on a discarded branch.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SummaryUsageMetadata {
    pub request_id: Uuid,
    pub metadata: ModelMetadata,
}

impl SummaryUsageMetadata {
    pub fn from_completion(event: &Event) -> Option<(Usage, Self)> {
        let Event::ModelFinished {
            request_id,
            metadata,
            usage: UsageReport::Measured { usage },
            failure: None,
            reply: None,
            ..
        } = event
        else {
            return None;
        };
        let mut metadata = metadata.clone();
        metadata.replay = None;
        metadata.reasoning = None;
        metadata.diagnostics.clear();
        Some((
            *usage,
            Self {
                request_id: *request_id,
                metadata,
            },
        ))
    }
}

/// Resolved system instructions and declared tools saved with an imported compaction.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CompactedSystemPrompt {
    pub content: String,
    pub sections: Vec<(String, Option<String>)>,
    pub tools: Vec<crate::ToolSpec>,
}

impl CompactedSystemPrompt {
    /// Decode the system prompt and tool declarations in a Pi JSONL entry.
    pub fn from_pi(message: &serde_json::Value) -> Result<Self> {
        if !message.is_object() {
            return Err(Error::Session("invalid imported system message".into()));
        }
        let sections = message
            .get("sections")
            .map(|value| {
                value
                    .as_object()
                    .ok_or_else(|| Error::Session("invalid system sections".into()))?
                    .iter()
                    .map(|(name, value)| {
                        let value = if value.is_null() {
                            None
                        } else {
                            Some(
                                value
                                    .as_str()
                                    .ok_or_else(|| Error::Session("invalid system section".into()))?
                                    .to_owned(),
                            )
                        };
                        Ok((name.clone(), value))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        let tools = message
            .get("toolsAdded")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?
            .unwrap_or_default();
        let content = match &message["content"] {
            serde_json::Value::Null => String::new(),
            serde_json::Value::String(text) => text.clone(),
            serde_json::Value::Array(blocks) => blocks
                .iter()
                .filter(|block| block["type"] == "text")
                .map(|block| {
                    block["text"]
                        .as_str()
                        .ok_or_else(|| Error::Session("invalid system text block".into()))
                })
                .collect::<Result<Vec<_>>>()?
                .join("\n"),
            _ => return Err(Error::Session("invalid imported system content".into())),
        };
        Ok(Self {
            content,
            sections,
            tools,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
// Keep the public journal event shape stable as optional provider metadata grows.
#[allow(clippy::large_enum_variant)]
pub enum Event {
    /// Identity of one request-hook invocation, retained across interrupted runs.
    RequestCallbackStarted {
        id: Uuid,
        purpose: String,
    },
    /// A prepared request was discarded before model execution.
    RequestCallbackAbandoned {
        id: Uuid,
    },
    /// The turn-end extension callbacks completed for this assistant entry.
    TurnEndCallbacksCompleted {
        assistant_record: u64,
    },
    /// Deferred context for the completed turn was applied.
    TurnEndBoundaryApplied {
        assistant_record: u64,
    },
    /// Original external entry retained during an explicit session import.
    SourceEntry {
        format: String,
        entry: serde_json::Value,
    },
    /// An imported result whose reused call ID cannot identify one operation.
    ImportedUnattributedToolResult {
        call_id: String,
        tool_name: String,
        content: crate::ContextContent,
        is_error: bool,
        details: Option<serde_json::Value>,
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
    /// Custom extension data on this branch, excluded from model context.
    CustomEntry {
        #[serde(flatten)]
        entry: crate::CustomEntry,
    },
    /// Extension message included in model context; display is a UI preference.
    CustomMessage {
        #[serde(flatten)]
        message: crate::CustomMessage,
    },
    /// Durable pending extension message, excluded from context until delivered.
    CustomMessageQueued {
        input: crate::QueuedCustomMessage,
    },
    /// Session-wide queue entry produced before switching branches.
    TreeMessageQueued {
        input: crate::QueuedCustomMessage,
    },
    /// Persist the selected branch while an interrupted tree trigger resumes elsewhere.
    TreeTriggerHandoff {
        run_id: Uuid,
        return_to: u64,
    },
    /// Atomically consumes a queued message and adds it to model context.
    CustomMessageDelivered {
        id: Uuid,
        message: crate::CustomMessage,
    },
    ModelSelected {
        model: String,
        thinking: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider: Option<String>,
    },
    /// Atomically switch branches and select a host-resolved fallback model.
    BranchModelSelected {
        target: u64,
        provider: String,
        model: String,
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
        /// Whether request hooks changed model-visible input (unknown in older journals).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_transformed: Option<bool>,
        /// Fingerprint of the effective model, system messages and tool schemas.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_configuration_sha256: Option<String>,
        /// Marks a request whose tool decisions are committed in record metadata.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "deserialize_decision_marker"
        )]
        tool_decision: Option<ToolDecisionMarker>,
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
    /// Billed cache refresh usage. The response never enters the conversation.
    CacheWarm {
        metadata: ModelMetadata,
        usage: Usage,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
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
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "deserialize_execution_marker"
        )]
        tool_execution: Option<ToolExecutionMarker>,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        system_prompt: Option<Box<CompactedSystemPrompt>>,
        retain_from: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tokens_before: Option<u64>,
        states: ExtensionStates,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<Usage>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage_metadata: Option<Box<SummaryUsageMetadata>>,
    },
    /// A summary message imported from Pi, without applying a new compaction.
    CompactionSummaryMessage {
        summary: String,
        tokens_before: u64,
    },
    /// Summary of a branch left behind, optionally committed at a new position.
    BranchSummary {
        summary: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fallback_model: Option<BranchFallbackModel>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        extension_summary: Option<crate::ExtensionTreeSummary>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        from: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<Usage>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage_metadata: Option<Box<SummaryUsageMetadata>>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        read_files: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        modified_files: Vec<String>,
    },
    ExtensionStates {
        states: ExtensionStates,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BranchFallbackModel {
    pub provider: String,
    pub model: String,
}

#[derive(Default)]
pub(crate) struct AbandonedBranch {
    pub messages: Vec<Message>,
    pub read_files: BTreeSet<String>,
    pub modified_files: BTreeSet<String>,
}

impl AbandonedBranch {
    fn collect_file(&mut self, call: &crate::ToolCall) {
        let files = match call.name.as_str() {
            "read" => &mut self.read_files,
            "edit" | "write" => &mut self.modified_files,
            _ => return,
        };
        if let Ok(arguments) = crate::parse_tool_arguments(&call.arguments)
            && let Some(path) = arguments["path"].as_str().filter(|path| !path.is_empty())
        {
            files.insert(path.to_owned());
        }
    }
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

impl Record {
    /// Read current summary billing, or recover it from an older journal's adjacent completion.
    pub fn summary_usage(&self, records: &[Self]) -> Option<(Usage, Option<SummaryUsageMetadata>)> {
        let (usage, metadata) = match &self.event {
            Event::Compacted {
                usage,
                usage_metadata,
                ..
            }
            | Event::BranchSummary {
                usage,
                usage_metadata,
                ..
            } => ((*usage)?, usage_metadata),
            _ => return None,
        };
        let metadata = metadata.as_deref().cloned().or_else(|| {
            let previous = records.get(self.id.checked_sub(2)? as usize)?;
            let (measured, metadata) = SummaryUsageMetadata::from_completion(&previous.event)?;
            (usage == measured).then_some(metadata)
        });
        Some((usage, metadata))
    }

    pub fn timestamp_millis(&self) -> Option<i64> {
        chrono::DateTime::parse_from_rfc3339(self.timestamp.as_deref()?)
            .ok()
            .map(|timestamp| timestamp.timestamp_millis())
    }
}

/// Independent request binding retained even when auxiliary metadata is lost.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDecisionMarker {
    version: u32,
    origin_session_id: Uuid,
    origin_context_head: u64,
    context_sha256: String,
    event_sha256: String,
    request_sha256: String,
    dispatch_sha256: String,
    binding_sha256: String,
}

fn deserialize_decision_marker<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<ToolDecisionMarker>, D::Error> {
    ToolDecisionMarker::deserialize(deserializer).map(Some)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolDecisionMetadata {
    version: u32,
    snapshot: crate::ToolDecisionSnapshot,
}

/// Independent binding for one tool start. The snapshot is atomic metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolExecutionMarker {
    version: u32,
    origin_session_id: Uuid,
    request_id: Uuid,
    decision_sha256: String,
    binding_sha256: String,
}

fn deserialize_execution_marker<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<ToolExecutionMarker>, D::Error> {
    ToolExecutionMarker::deserialize(deserializer).map(Some)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolExecutionMetadata {
    version: u32,
    snapshot: crate::ToolExecutionSnapshot,
}

/// A decision from the active branch or historical session/branch context.
/// Historical decisions must not authorize execution on the active branch.
#[derive(Debug)]
pub enum RecordedToolDecision {
    Legacy,
    Current(crate::ToolDecisionSnapshot),
    Historical(crate::ToolDecisionSnapshot),
}

// The fork remaps context_head, so bind all other request event fields.
fn decision_event_sha256(event: &Event) -> Result<String> {
    let mut value = serde_json::to_value(event)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| Error::Session("invalid model request".into()))?;
    object.remove("context_head");
    object.remove("tool_decision");
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(&value)?)))
}

fn decision_context_sha256(context: &crate::ToolDispatchContext) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(context)?)
    ))
}

fn decision_valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
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

    /// Whether this store supports atomic auxiliary record metadata.
    fn supports_record_metadata(&self) -> bool {
        false
    }

    /// Maximum encoded record size, including its terminating newline, when
    /// this store uses the native JSONL record format.
    fn encoded_record_limit(&self) -> Option<usize> {
        None
    }

    /// Load auxiliary data committed atomically with records. Legacy stores
    /// need not implement this until they support metadata-bearing records.
    fn load_record_metadata(&mut self) -> Result<BTreeMap<u64, serde_json::Value>> {
        Ok(BTreeMap::new())
    }

    /// Atomically commit a record and its optional auxiliary data. Legacy
    /// stores fail closed instead of silently discarding metadata.
    fn append_with_metadata(
        &mut self,
        record: &Record,
        metadata: Option<&serde_json::Value>,
    ) -> Result<()> {
        if metadata.is_some() {
            return Err(Error::Session(
                "session store does not support record metadata".into(),
            ));
        }
        self.append(record)
    }
}

#[derive(Default)]
pub struct MemoryStore {
    records: Vec<Record>,
    metadata: BTreeMap<u64, serde_json::Value>,
}

impl SessionStore for MemoryStore {
    fn supports_record_metadata(&self) -> bool {
        true
    }
    fn load(&mut self) -> Result<Vec<Record>> {
        Ok(self.records.clone())
    }

    fn append(&mut self, record: &Record) -> Result<()> {
        self.records.push(record.clone());
        Ok(())
    }

    fn load_record_metadata(&mut self) -> Result<BTreeMap<u64, serde_json::Value>> {
        Ok(self.metadata.clone())
    }

    fn append_with_metadata(
        &mut self,
        record: &Record,
        metadata: Option<&serde_json::Value>,
    ) -> Result<()> {
        self.records.push(record.clone());
        if let Some(metadata) = metadata {
            self.metadata.insert(record.id, metadata.clone());
        }
        Ok(())
    }
}

pub struct Session {
    auto_compaction: crate::AutoCompactionControl,
    retry: crate::RetryControl,
    store: Box<dyn SessionStore>,
    records: Vec<Record>,
    record_metadata: BTreeMap<u64, serde_json::Value>,
    head: u64,
    run_id: Option<Uuid>,
    poisoned: bool,
}

impl Session {
    pub fn supports_record_metadata(&self) -> bool {
        self.store.supports_record_metadata()
    }
    pub(crate) fn compaction_details_budget(
        &self,
        event: &Event,
        from_extension: bool,
        truncated: bool,
    ) -> Result<usize> {
        let Some(limit) = self.store.encoded_record_limit() else {
            return Ok(4 * 1024 * 1024);
        };
        let mut metadata = serde_json::json!({"details":null});
        if truncated {
            metadata["nativeFileDetailsTruncated"] = true.into();
        }
        if from_extension {
            metadata["fromExtension"] = true.into();
            metadata["nativeGeneratedFileDetails"] = true.into();
        }
        let record = Record {
            timestamp: Some(record_timestamp()),
            run_id: self.run_id,
            version: self
                .records
                .last()
                .map_or(TURN_END_JOURNAL_VERSION, |record| {
                    record.version.max(TURN_END_JOURNAL_VERSION)
                }),
            id: self.records.len() as u64 + 1,
            parent: (self.head != 0).then_some(self.head),
            event: event.clone(),
        };
        let envelope = encode_record(&record, Some(&metadata))?
            .len()
            .saturating_add(1);
        // `null` in the template occupies four bytes; replace it with details.
        Ok(limit
            .saturating_sub(envelope)
            .saturating_add(4)
            .min(4 * 1024 * 1024))
    }
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
        let mut session = Self::with_store(FileStore::open(path.as_ref())?)?;
        if session.host_binding().is_none() {
            session.recover_tree_trigger_handoff()?;
        }
        Ok(session)
    }

    /// Hold the journal lock while reading a complete session without repairing it.
    /// This is intended for exports that must not modify the source journal.
    pub fn open_for_share(path: impl AsRef<Path>) -> Result<Self> {
        let session = Self::with_store(ShareFileStore(FileStore::open_existing(path.as_ref())?))?;
        if session.host_binding().is_none() && session.pending_tree_trigger_handoff().is_some() {
            return Err(Error::Session(
                "journal requires handoff recovery before sharing".into(),
            ));
        }
        Ok(session)
    }

    /// Read a stable, complete-record prefix without locking or repairing the source.
    pub fn snapshot(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        let length = file.metadata()?.len();
        let (records, metadata, _, _) = read_complete_records(file.take(length))?;
        if records.is_empty() {
            return Err(Error::Session("journal has no committed header".into()));
        }
        Self::with_store(MemoryStore { records, metadata })
    }

    /// Restore a validated, in-memory snapshot without mutating its source.
    /// `Record` does not contain auxiliary metadata; use
    /// `from_records_with_metadata` when the source includes it.
    pub fn from_records(records: Vec<Record>) -> Result<Self> {
        Self::with_store(MemoryStore {
            records,
            metadata: BTreeMap::new(),
        })
    }

    /// Restore records together with their atomic auxiliary metadata.
    pub fn from_records_with_metadata(
        records: Vec<Record>,
        metadata: BTreeMap<u64, serde_json::Value>,
    ) -> Result<Self> {
        Self::with_store(MemoryStore { records, metadata })
    }

    /// Restore serialized native records, preserving auxiliary metadata.
    pub fn from_serialized_records(values: Vec<serde_json::Value>) -> Result<Self> {
        let mut records = Vec::with_capacity(values.len());
        let mut metadata = BTreeMap::new();
        for value in values {
            let legacy = legacy_compaction_metadata(&value);
            let stored: PersistedRecord = serde_json::from_value(value)?;
            if let Some(extra) = stored.metadata.or(legacy) {
                metadata.insert(stored.record.id, extra);
            }
            records.push(stored.record);
        }
        Self::from_records_with_metadata(records, metadata)
    }

    pub fn with_store(mut store: impl SessionStore + 'static) -> Result<Self> {
        let mut records = store.load()?;
        let record_metadata = store.load_record_metadata()?;
        if record_metadata
            .keys()
            .any(|id| *id == 0 || *id > records.len() as u64)
        {
            return Err(Error::Session(
                "record metadata references a missing record".into(),
            ));
        }
        if record_metadata.values().any(serde_json::Value::is_null) {
            return Err(Error::Session("record metadata must not be null".into()));
        }
        let head = validated_head(&records)?;
        restore_imported_compaction_prompts(&mut records);
        let mut session = Self {
            auto_compaction: crate::AutoCompactionControl::default(),
            retry: crate::RetryControl::default(),
            store: Box::new(store),
            records,
            record_metadata,
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

    /// Return auxiliary metadata attached atomically to a journal record.
    pub fn record_metadata(&self, id: u64) -> Option<&serde_json::Value> {
        self.record_metadata.get(&id)
    }

    /// Commit a marked request and its snapshot together. This does not dispatch
    /// the model or make a recovered decision safe to execute on its own.
    pub fn append_tool_decision_request(
        &mut self,
        mut event: Event,
        snapshot: &crate::ToolDecisionSnapshot,
        request: &crate::ModelRequest,
        dispatch_request: &crate::ModelRequest,
        declarations: &[crate::ToolDeclaration],
    ) -> Result<&Record> {
        let context = snapshot.context();
        let Event::ModelRequested {
            model,
            messages,
            tools,
            max_output_tokens,
            request_id: Some(id),
            context_head: Some(head),
            context_sha256: Some(request_hash),
            tool_decision,
            ..
        } = &mut event
        else {
            return Err(Error::Session(
                "tool decision requires an identified model request".into(),
            ));
        };
        if tool_decision.is_some()
            || id.is_nil()
            || *id != context.request_id
            || context.session_id != self.session_id()
            || *head != self.head
            || request_hash.as_str() != snapshot.request_sha256()
            || *model != request.model
            || *messages != request.messages.len()
            || *tools != request.tools.len()
            || *max_output_tokens != request.max_output_tokens
        {
            return Err(Error::Session(
                "tool decision request binding mismatch".into(),
            ));
        }
        snapshot
            .validate(context, request, dispatch_request, declarations)
            .map_err(|_| Error::Session("invalid tool decision snapshot".into()))?;
        // Finish mutating the event before hashing its stable fields.
        let _ = tool_decision;
        let event_sha256 = decision_event_sha256(&event)?;
        let Event::ModelRequested {
            tool_decision,
            context_head: Some(head),
            ..
        } = &mut event
        else {
            unreachable!()
        };
        *tool_decision = Some(ToolDecisionMarker {
            version: 1,
            origin_session_id: context.session_id,
            origin_context_head: *head,
            context_sha256: decision_context_sha256(context)?,
            event_sha256,
            request_sha256: snapshot.request_sha256().into(),
            dispatch_sha256: snapshot.dispatch_sha256().into(),
            binding_sha256: snapshot.binding_sha256().into(),
        });
        let metadata = serde_json::json!({"toolDecision": ToolDecisionMetadata { version: 1, snapshot: snapshot.clone() }});
        if serde_json::to_vec(&metadata["toolDecision"])?.len() > MAX_TOOL_DECISION_METADATA_BYTES {
            return Err(Error::Session(
                "tool decision metadata exceeds size limit".into(),
            ));
        }
        self.append_inner_with_metadata(event, true, Some(metadata), false)
    }

    /// Validate the snapshot using the marker retained independently in the event.
    /// A fork retains the original context and is reported as historical. A
    /// decision on an inactive branch is also historical, even in its source
    /// session. Neither classification authorizes execution by itself.
    pub fn recorded_tool_decision(&self, id: u64) -> Result<RecordedToolDecision> {
        let record = id
            .checked_sub(1)
            .and_then(|i| usize::try_from(i).ok())
            .and_then(|i| self.records.get(i))
            .ok_or_else(|| Error::Session("missing model request record".into()))?;
        let Event::ModelRequested {
            request_id,
            context_head,
            context_sha256,
            tool_decision,
            ..
        } = &record.event
        else {
            return Err(Error::Session("record is not a model request".into()));
        };
        let Some(marker) = tool_decision else {
            if self
                .record_metadata(id)
                .is_some_and(|metadata| metadata.get("toolDecision").is_some())
            {
                return Err(Error::Session(
                    "tool decision metadata without a request marker".into(),
                ));
            }
            return Ok(RecordedToolDecision::Legacy);
        };
        if record.version < TOOL_DECISION_JOURNAL_VERSION
            || marker.version != 1
            || marker.origin_session_id.is_nil()
            || marker.origin_context_head == 0
            || request_id.is_none_or(|id| id.is_nil())
            || *context_head != record.parent
            || context_head.is_none()
            || context_sha256.as_deref() != Some(marker.request_sha256.as_str())
            || decision_event_sha256(&record.event)? != marker.event_sha256
            || ![
                &marker.context_sha256,
                &marker.event_sha256,
                &marker.request_sha256,
                &marker.dispatch_sha256,
                &marker.binding_sha256,
            ]
            .iter()
            .all(|v| decision_valid_hash(v))
        {
            return Err(Error::Session("invalid tool decision marker".into()));
        }
        let value = self
            .record_metadata(id)
            .and_then(|metadata| metadata.get("toolDecision"))
            .ok_or_else(|| Error::Session("missing tool decision metadata".into()))?;
        if serde_json::to_vec(value)?.len() > MAX_TOOL_DECISION_METADATA_BYTES {
            return Err(Error::Session(
                "tool decision metadata exceeds size limit".into(),
            ));
        }
        let metadata: ToolDecisionMetadata = serde_json::from_value(value.clone())
            .map_err(|_| Error::Session("invalid tool decision metadata".into()))?;
        let context = metadata.snapshot.context();
        if metadata.version != 1
            || Some(context.request_id) != *request_id
            || context.session_id != marker.origin_session_id
            || decision_context_sha256(context)? != marker.context_sha256
            || (self.session_id() == marker.origin_session_id
                && *context_head != Some(marker.origin_context_head))
        {
            return Err(Error::Session("tool decision context mismatch".into()));
        }
        metadata
            .snapshot
            .validate_recorded(
                context,
                &marker.request_sha256,
                &marker.dispatch_sha256,
                &marker.binding_sha256,
            )
            .map_err(|_| Error::Session("invalid tool decision snapshot".into()))?;
        Ok(
            if self.session_id() == marker.origin_session_id
                && self
                    .path(self.head)
                    .iter()
                    .any(|ancestor| ancestor.id == id)
            {
                RecordedToolDecision::Current(metadata.snapshot)
            } else {
                RecordedToolDecision::Historical(metadata.snapshot)
            },
        )
    }

    /// Locate a unique request on the active branch and validate its decision.
    /// The returned decision does not itself prove an invocation belongs to it.
    pub fn active_tool_decision(&self, request_id: Uuid) -> Result<RecordedToolDecision> {
        if request_id.is_nil() {
            return Err(Error::Session("invalid tool decision request ID".into()));
        }
        let mut matches = self.branch().into_iter().filter(|record| {
            matches!(
                &record.event,
                Event::ModelRequested { request_id: Some(id), .. } if *id == request_id
            )
        });
        let record = matches
            .next()
            .ok_or_else(|| Error::Session("missing active tool decision request".into()))?;
        if matches.next().is_some() {
            return Err(Error::Session("duplicate tool decision request ID".into()));
        }
        self.recorded_tool_decision(record.id)
    }

    // Resolve an unambiguous direct model invocation. Nested tool-planned calls
    // do not carry a request ID and are deliberately rejected here.
    pub(crate) fn execution_provenance(
        &self,
        request_id: Uuid,
        operation_id: Uuid,
        before: u64,
        run_id: Option<Uuid>,
        owner: &str,
        spec: &crate::ToolSpec,
    ) -> Result<(crate::Invocation, String)> {
        let RecordedToolDecision::Current(decision) = self.active_tool_decision(request_id)? else {
            return Err(Error::Session(
                "tool execution requires a current decision".into(),
            ));
        };
        if decision.context().summary {
            return Err(Error::Session(
                "summary request cannot authorize tool execution".into(),
            ));
        }
        let branch = self.branch();
        let request_record = branch.iter().find(|record| matches!(&record.event, Event::ModelRequested { request_id: Some(id), .. } if *id == request_id))
            .ok_or_else(|| Error::Session("missing tool execution request".into()))?;
        let Event::ModelRequested {
            tool_decision: Some(marker),
            max_output_tokens,
            ..
        } = &request_record.event
        else {
            return Err(Error::Session("missing tool decision marker".into()));
        };
        if request_record.id >= before || request_record.run_id != run_id {
            return Err(Error::Session(
                "tool execution request position or run mismatch".into(),
            ));
        }
        let validated = decision
            .validate_recorded(
                decision.context(),
                &marker.request_sha256,
                &marker.dispatch_sha256,
                &marker.binding_sha256,
            )
            .map_err(|_| Error::Session("invalid tool decision".into()))?;
        if !decision
            .matches_registered_tool(owner, spec)
            .map_err(|_| Error::Session("invalid tool registration binding".into()))?
        {
            return Err(Error::Session(
                "registered tool is not in the request decision".into(),
            ));
        }
        // A request, even with a valid decision, does not authorize a fabricated
        // assistant entry. Require its unique durable successful completion and
        // the exact assistant projection before accepting any invocation.
        let mut completions = branch.iter().filter(|record| {
            matches!(&record.event,
            Event::ModelFinished { request_id: id, .. } if *id == request_id)
        });
        let completion = completions
            .next()
            .ok_or_else(|| Error::Session("missing model completion".into()))?;
        if completions.next().is_some()
            || completion.id <= request_record.id
            || completion.id >= before
            || completion.run_id != run_id
        {
            return Err(Error::Session("ambiguous model completion".into()));
        }
        let Event::ModelFinished {
            metadata,
            failure: None,
            partial,
            reply: Some(reply),
            ..
        } = &completion.event
        else {
            return Err(Error::Session(
                "model completion has no successful reply".into(),
            ));
        };
        let advertised: Vec<_> = validated
            .effective_tools()
            .iter()
            .map(|tool| tool.spec.name.as_str())
            .collect();
        if metadata.finish_reason.as_deref() == Some("length")
            || !partial.is_empty()
            || reply.max_output_tokens != *max_output_tokens
            || reply
                .advertised_tools
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                != advertised
        {
            return Err(Error::Session(
                "invalid or truncated model completion".into(),
            ));
        }
        let mut assistants = branch.iter().filter(|record| {
            matches!(&record.event,
            Event::Assistant { request_id: Some(id), .. } if *id == request_id)
        });
        let assistant = assistants
            .next()
            .ok_or_else(|| Error::Session("missing assistant reply".into()))?;
        if assistants.next().is_some()
            || assistant.id <= completion.id
            || assistant.id >= before
            || assistant.run_id != run_id
        {
            return Err(Error::Session("ambiguous assistant reply".into()));
        }
        // Another request or assistant between this request and the tool start
        // supersedes the turn, even if the old records remain on this branch.
        // Only inspect up to the start so later turns do not invalidate history.
        if branch.iter().any(|record| {
            record.id > request_record.id
                && record.id < before
                && (matches!(&record.event, Event::ModelRequested { .. })
                    || (record.id != assistant.id
                        && matches!(&record.event, Event::Assistant { .. })))
        }) {
            return Err(Error::Session(
                "tool execution belongs to a superseded model turn".into(),
            ));
        }
        let Event::Assistant {
            content,
            invocations,
            ..
        } = &assistant.event
        else {
            unreachable!()
        };
        if *content != reply.content || *invocations != reply.invocations {
            return Err(Error::Session(
                "assistant does not match model completion".into(),
            ));
        }
        let mut found = None;
        for record in &branch {
            match &record.event {
                Event::Assistant {
                    invocations,
                    request_id: assistant_request,
                    ..
                } => {
                    for invocation in invocations
                        .iter()
                        .filter(|inv| inv.operation_id == operation_id)
                    {
                        if found.is_some()
                            || record.id <= request_record.id
                            || record.id >= before
                            || record.run_id != run_id
                            || *assistant_request != Some(request_id)
                            || invocation.call.name != spec.name
                        {
                            return Err(Error::Session(
                                "ambiguous tool invocation provenance".into(),
                            ));
                        }
                        found = Some(invocation.clone());
                    }
                }
                Event::ToolPlanned { invocation, .. }
                    if invocation.operation_id == operation_id =>
                {
                    return Err(Error::Session("ambiguous or nested tool invocation".into()));
                }
                Event::ToolStarted {
                    operation_id: id, ..
                }
                | Event::ToolFinished {
                    operation_id: id, ..
                }
                | Event::ToolReconciled {
                    operation_id: id, ..
                } if *id == operation_id && record.id < before => {
                    return Err(Error::Session(
                        "tool operation already started or finished".into(),
                    ));
                }
                _ => {}
            }
        }
        Ok((
            found.ok_or_else(|| Error::Session("missing tool invocation".into()))?,
            marker.binding_sha256.clone(),
        ))
    }

    /// Atomically record an authorized execution snapshot before starting a tool.
    /// `owner`, `spec`, and `recovery_identity` must come from the current registry.
    pub fn append_tool_execution_started(
        &mut self,
        request_id: Uuid,
        operation_id: Uuid,
        snapshot: &crate::ToolExecutionSnapshot,
        owner: &str,
        spec: &crate::ToolSpec,
        recovery_identity: &str,
    ) -> Result<&Record> {
        let (invocation, decision) = self.execution_provenance(
            request_id,
            operation_id,
            self.records.len() as u64 + 1,
            self.run_id,
            owner,
            spec,
        )?;
        snapshot.validate(
            request_id,
            &invocation,
            owner,
            spec,
            recovery_identity,
            &decision,
            snapshot.binding_sha256(),
        )?;
        let event = Event::ToolStarted {
            operation_id,
            tool_execution: Some(ToolExecutionMarker {
                version: 1,
                origin_session_id: self.session_id(),
                request_id,
                decision_sha256: decision,
                binding_sha256: snapshot.binding_sha256().to_owned(),
            }),
        };
        let metadata = serde_json::json!({"toolExecution": ToolExecutionMetadata { version: 1, snapshot: snapshot.clone() }});
        self.append_inner_with_metadata(event, true, Some(metadata), false)
    }

    /// Return validated arguments for an active-branch start, or None for a
    /// legacy start. Historical/forked records cannot authorize execution.
    pub fn recorded_tool_execution(
        &self,
        id: u64,
        owner: &str,
        spec: &crate::ToolSpec,
        recovery_identity: &str,
    ) -> Result<Option<serde_json::Value>> {
        let record = id
            .checked_sub(1)
            .and_then(|i| usize::try_from(i).ok())
            .and_then(|i| self.records.get(i))
            .ok_or_else(|| Error::Session("missing tool start record".into()))?;
        let Event::ToolStarted {
            operation_id,
            tool_execution,
        } = &record.event
        else {
            return Err(Error::Session("record is not a tool start".into()));
        };
        let Some(marker) = tool_execution else {
            if self
                .record_metadata(id)
                .is_some_and(|value| value.get("toolExecution").is_some())
            {
                return Err(Error::Session(
                    "tool execution metadata without marker".into(),
                ));
            }
            return Ok(None);
        };
        if record.version < TOOL_EXECUTION_JOURNAL_VERSION
            || marker.version != 1
            || marker.origin_session_id != self.session_id()
            || marker.request_id.is_nil()
            || operation_id.is_nil()
            || !decision_valid_hash(&marker.decision_sha256)
            || !decision_valid_hash(&marker.binding_sha256)
            || !self.branch().iter().any(|entry| entry.id == id)
        {
            return Err(Error::Session(
                "invalid or historical tool execution marker".into(),
            ));
        }
        if self.branch().iter().filter(|entry| matches!(&entry.event, Event::ToolStarted { operation_id: other, .. } if other == operation_id)).count() != 1 {
            return Err(Error::Session("ambiguous tool start".into()));
        }
        let (invocation, decision) = self.execution_provenance(
            marker.request_id,
            *operation_id,
            id,
            record.run_id,
            owner,
            spec,
        )?;
        if decision != marker.decision_sha256 {
            return Err(Error::Session("tool execution decision mismatch".into()));
        }
        let value = self
            .record_metadata(id)
            .and_then(|metadata| metadata.get("toolExecution"))
            .ok_or_else(|| Error::Session("missing tool execution metadata".into()))?;
        let metadata: ToolExecutionMetadata = serde_json::from_value(value.clone())
            .map_err(|_| Error::Session("invalid tool execution metadata".into()))?;
        if metadata.version != 1 {
            return Err(Error::Session(
                "unsupported tool execution metadata version".into(),
            ));
        }
        let validated = metadata.snapshot.validate(
            marker.request_id,
            &invocation,
            owner,
            spec,
            recovery_identity,
            &decision,
            &marker.binding_sha256,
        )?;
        Ok(Some(validated.arguments().clone()))
    }

    /// Verify the provenance of all Pi source entries in this snapshot.
    pub fn validated_pi_conversion_counts(&self) -> Result<BTreeMap<u64, u64>> {
        let relevant = self
            .records
            .iter()
            .filter_map(|record| {
                matches!(&record.event,
            Event::SourceEntry { format, .. } if format == "pi")
                .then_some(record.id)
            })
            .collect();
        crate::pi_import::validate_conversions(self, &relevant)
    }

    pub(crate) fn set_run_id(&mut self, run_id: Option<Uuid>) {
        self.run_id = run_id;
    }

    pub(crate) fn current_run_id(&self) -> Option<Uuid> {
        self.run_id
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

    /// Pi-compatible externally visible identity; the native UUID remains the
    /// stable key for existing SDKs, stores, and host isolation.
    pub fn external_session_id(&self) -> String {
        let native = self.session_id().to_string();
        self.records
            .iter()
            .find_map(|record| match &record.event {
                Event::SourceEntry { format, entry }
                    if format == "pi-rs:external-session-id"
                        && entry["native_id"].as_str() == Some(native.as_str()) =>
                {
                    entry["id"].as_str().map(str::to_owned)
                }
                _ => None,
            })
            .unwrap_or(native)
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

    /// Resolve the current branch's system context against the active runtime tools.
    pub fn effective_system_context(
        &self,
        fallback: &str,
        tools: Vec<crate::ToolSpec>,
    ) -> (String, Vec<crate::ToolSpec>) {
        system_context(&self.branch(), fallback, tools)
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

    /// Snapshot a settled branch for use as model input. Failed or cancelled
    /// assistant records remain available through `conversation_at` for display,
    /// but are excluded here even when their content was edited.
    pub fn model_conversation_at(&self, record_id: u64) -> Result<Vec<Message>> {
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
        let excluded = failed_assistant_records(&branch);
        Ok(project_message_entries(&branch, ops)
            .into_iter()
            .filter_map(|(source, message)| (!excluded.contains(&source)).then_some(message))
            .collect())
    }

    pub(crate) fn abandoned_branch(&self, target: u64) -> AbandonedBranch {
        let (branch, common_ancestor) = self.branch_divergence(target);
        let first_abandoned = common_ancestor.map_or(0, |index| index + 1);
        let mut abandoned = AbandonedBranch::default();
        let Some(first) = branch.get(first_abandoned) else {
            return abandoned;
        };
        let excluded = failed_assistant_records(&branch);
        let mut messages = conversation_entries_since(&branch, self.operations(), first.id);
        messages.retain(|(source, message)| {
            !excluded.contains(source) && !matches!(message, Message::Tool { .. })
        });
        for (_, message) in &messages {
            if let Message::Assistant { calls, .. } = message {
                for call in calls {
                    abandoned.collect_file(call);
                }
            }
        }
        for record in &branch[first_abandoned..] {
            match &record.event {
                Event::Compacted { summary, .. } => messages.push((
                    record.id,
                    Message::User {
                        content: format!("Prior conversation summary:\n{summary}"),
                    },
                )),
                Event::BranchSummary {
                    read_files,
                    modified_files,
                    ..
                } => {
                    abandoned.read_files.extend(read_files.iter().cloned());
                    abandoned
                        .modified_files
                        .extend(modified_files.iter().cloned());
                }
                Event::ToolPlanned { invocation, .. } => abandoned.collect_file(&invocation.call),
                _ => {}
            }
        }
        messages.sort_by_key(|(id, _)| *id);
        abandoned.messages = messages.into_iter().map(|(_, message)| message).collect();
        abandoned
            .read_files
            .retain(|path| !abandoned.modified_files.contains(path));
        abandoned
    }

    pub(crate) fn branch_divergence(&self, target: u64) -> (Vec<&Record>, Option<usize>) {
        let retained: BTreeSet<_> = self.path(target).iter().map(|record| record.id).collect();
        let branch = self.branch();
        let common_ancestor = branch
            .iter()
            .rposition(|record| retained.contains(&record.id));
        (branch, common_ancestor)
    }

    /// Current projected messages, including calls whose tools are still running.
    pub fn current_messages(&self) -> Vec<Message> {
        self.current_message_entries()
            .into_iter()
            .map(|(_, message)| message)
            .collect()
    }

    /// Current projected messages and the journal record that supplied each one.
    pub fn current_message_entries(&self) -> Vec<(u64, Message)> {
        let branch = self.branch();
        project_message_entries(&branch, operations(&branch))
    }

    /// Measured context when usable, otherwise a visible-content estimate.
    pub fn estimated_context_tokens(&self) -> u64 {
        self.context_usage_estimate().tokens
    }

    pub fn context_usage_estimate(&self) -> crate::ContextUsageEstimate {
        let branch = self.branch();
        let token_index = crate::tokens::SourceTokenIndex::new(self, &branch);
        let entries = conversation_entries_unrepaired(&branch, self.operations());
        let system_invalidations = crate::tokens::non_additive_native_system_checkpoints(&branch);
        let invalidated_at = branch
            .iter()
            .rev()
            .find_map(|record| {
                (matches!(
                    record.event,
                    Event::ContextEdited { .. } | Event::Compacted { .. }
                ) || system_invalidations.contains(&record.id))
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
                    || metadata.assistant_text_edited
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
            let usage_tokens = self
                .imported_context_total(*source)
                .unwrap_or_else(|| input.saturating_add(usage.output_tokens));
            if usage_tokens == 0 {
                continue;
            }
            let trailing_tokens =
                entries[index + 1..]
                    .iter()
                    .fold(0_u64, |tokens, (source, message)| {
                        tokens.saturating_add(token_index.estimate(*source, message))
                    });
            // Pi estimates each system delta that follows the measured reply.
            let trailing_tokens = branch
                .iter()
                .filter(|record| {
                    record.id > *source && matches!(record.event, Event::SystemCheckpoint { .. })
                })
                .fold(trailing_tokens, |tokens, record| {
                    tokens.saturating_add(crate::tokens::estimate_system_delta(self, record))
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
        let initial = summary
            .map_or(0, |summary| {
                crate::estimate_message_tokens(&Message::User {
                    content: summary.clone(),
                })
            })
            .saturating_add(crate::tokens::estimate_system_tokens(&branch));
        let tokens = entries.iter().fold(initial, |tokens, (source, message)| {
            tokens.saturating_add(token_index.estimate(*source, message))
        });
        crate::ContextUsageEstimate {
            tokens,
            usage_tokens: 0,
            trailing_tokens: tokens,
            last_usage_record: None,
        }
    }

    pub(crate) fn context_budget_estimate(&self) -> crate::ContextUsageEstimate {
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
                        | Event::BranchModelSelected { .. }
                        | Event::BranchSummary {
                            fallback_model: Some(_),
                            ..
                        }
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
                    || metadata.assistant_text_edited
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
                        tokens.saturating_add(crate::tokens::estimate_budget_source_tokens(
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
            tokens.saturating_add(crate::tokens::estimate_budget_source_tokens(
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

    pub(crate) fn imported_context_total(&self, source: u64) -> Option<u64> {
        self.imported_assistant_message(source)?["usage"]["totalTokens"]
            .as_u64()
            .filter(|total| *total > 0)
    }

    pub(crate) fn imported_assistant_message(&self, source: u64) -> Option<&serde_json::Value> {
        let assistant = self.records.get(source.checked_sub(1)? as usize)?;
        let Event::Assistant {
            request_id: Some(request_id),
            ..
        } = &assistant.event
        else {
            return None;
        };
        let completion = self
            .records
            .get(assistant.parent?.checked_sub(1)? as usize)?;
        if !matches!(&completion.event, Event::ModelFinished { request_id: id, .. } if id == request_id)
        {
            return None;
        }
        let original = self
            .records
            .get(completion.parent?.checked_sub(1)? as usize)?;
        let Event::SourceEntry { format, entry } = &original.event else {
            return None;
        };
        (format == "pi" && entry["type"] == "message" && entry["message"]["role"] == "assistant")
            .then_some(&entry["message"])
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
        self.model_selection_at(self.head())
    }

    pub(crate) fn model_selection_at(&self, target: u64) -> Option<(String, Option<String>)> {
        self.model_identity_at(target)
            .map(|(_, model, thinking)| (model, thinking))
    }

    /// The provider belongs to the same selection record as the model. Older
    /// records and request-only histories have no reliable provider identity.
    pub(crate) fn model_identity_at(
        &self,
        target: u64,
    ) -> Option<(Option<String>, String, Option<String>)> {
        let branch = self.path(target);
        branch
            .iter()
            .rev()
            .find_map(|record| match &record.event {
                Event::ModelSelected {
                    model,
                    thinking,
                    provider,
                } => Some((provider.clone(), model.clone(), thinking.clone())),
                Event::BranchModelSelected {
                    provider, model, ..
                } => Some((Some(provider.clone()), model.clone(), None)),
                Event::BranchSummary {
                    fallback_model: Some(selection),
                    ..
                } => Some((
                    Some(selection.provider.clone()),
                    selection.model.clone(),
                    None,
                )),
                _ => None,
            })
            .or_else(|| {
                branch.iter().rev().find_map(|record| match &record.event {
                    Event::ModelRequested { model, .. } => Some((None, model.clone(), None)),
                    _ => None,
                })
            })
    }

    pub fn model_provider(&self) -> Option<&str> {
        self.branch()
            .into_iter()
            .rev()
            .find_map(|record| match &record.event {
                Event::ModelSelected { provider, .. } => Some(provider.as_deref()),
                Event::BranchModelSelected { provider, .. } => Some(Some(provider.as_str())),
                Event::BranchSummary {
                    fallback_model: Some(selection),
                    ..
                } => Some(Some(selection.provider.as_str())),
                _ => None,
            })
            .flatten()
    }

    pub fn select_model(&mut self, model: &str, thinking: Option<&str>) -> Result<()> {
        self.select_provider_model(None, model, thinking)
    }

    pub(crate) fn needs_native_model_selection_marker(&self) -> bool {
        self.records.get(self.head.saturating_sub(1) as usize).is_some_and(|record| {
            matches!(&record.event, Event::SourceEntry { format, entry }
                if format == "pi" && matches!(entry["type"].as_str(), Some("model_change" | "thinking_level_change" | "message")))
        })
    }

    pub fn select_provider_model(
        &mut self,
        provider: Option<&str>,
        model: &str,
        thinking: Option<&str>,
    ) -> Result<()> {
        if model.trim().is_empty() {
            return Err(Error::Config("model name is empty".into()));
        }
        if self.model_selection() != Some((model.into(), thinking.map(str::to_owned)))
            || self.model_provider() != provider
            || self.has_imported_response_since_selection()
            || self.has_only_thinking_selection()
        {
            self.append(Event::ModelSelected {
                model: model.into(),
                thinking: thinking.map(str::to_owned),
                provider: provider.map(str::to_owned),
            })?;
        }
        Ok(())
    }

    /// Commit a native fallback in one record, even on an imported Pi head.
    /// A distinct timestamp preserves the existing imported-conversion marker
    /// understood by older journal and RPC readers.
    pub(crate) fn select_provider_model_atomic(
        &mut self,
        provider: &str,
        model: &str,
    ) -> Result<()> {
        if provider.trim().is_empty() || model.trim().is_empty() {
            return Err(Error::Config("provider or model name is empty".into()));
        }
        self.append_inner(
            Event::ModelSelected {
                model: model.into(),
                thinking: None,
                provider: Some(provider.into()),
            },
            false,
        )?;
        Ok(())
    }

    pub(crate) fn checkout_provider_model_atomic(
        &mut self,
        target: u64,
        provider: &str,
        model: &str,
    ) -> Result<()> {
        self.validate_checkout(target)?;
        if provider.trim().is_empty() || model.trim().is_empty() {
            return Err(Error::Config(
                "fallback provider or model name is empty".into(),
            ));
        }
        self.append_inner(
            Event::BranchModelSelected {
                target,
                provider: provider.into(),
                model: model.into(),
            },
            false,
        )?;
        Ok(())
    }

    // Older journals may contain only a thinking marker and its synthetic
    // ModelSelected record. An explicit model choice must establish a model.
    fn has_only_thinking_selection(&self) -> bool {
        let branch = self.branch();
        let mut saw_thinking = false;
        for (index, record) in branch.iter().enumerate().rev() {
            if matches!(
                record.event,
                Event::BranchModelSelected { .. }
                    | Event::BranchSummary {
                        fallback_model: Some(_),
                        ..
                    }
            ) {
                return false;
            }
            if matches!(record.event, Event::ModelSelected { .. }) {
                let thinking_only = index.checked_sub(1).and_then(|i| branch.get(i)).is_some_and(|parent| {
                    matches!(&parent.event, Event::SourceEntry { format, .. } if format == "pi-rs:thinking-selection")
                });
                if !thinking_only {
                    return false;
                }
                saw_thinking = true;
            }
        }
        saw_thinking
    }

    /// Persist a thinking-only change without implying a model selection.
    pub fn select_thinking_level(
        &mut self,
        provider: Option<&str>,
        model: &str,
        thinking: &str,
    ) -> Result<()> {
        if model.trim().is_empty() {
            return Err(Error::Config("model name is empty".into()));
        }
        if !self.has_model_context() {
            return self.select_provider_model(provider, model, Some(thinking));
        }
        if self
            .model_selection()
            .is_some_and(|(_, level)| level.as_deref() == Some(thinking))
        {
            return Ok(());
        }
        self.append(Event::SourceEntry {
            format: "pi-rs:thinking-selection".into(),
            entry: serde_json::Value::Null,
        })?;
        self.append(Event::ModelSelected {
            model: model.into(),
            provider: provider.map(str::to_owned),
            thinking: Some(thinking.into()),
        })?;
        Ok(())
    }

    fn has_model_context(&self) -> bool {
        let branch = self.branch();
        branch.iter().enumerate().any(|(index, record)| match &record.event {
            Event::SourceEntry { format, entry } if format == "pi" => entry["type"] == "model_change"
                || (entry["type"] == "message" && entry["message"]["role"] == "assistant"),
            Event::ModelSelected { .. } => !index.checked_sub(1).and_then(|i| branch.get(i))
                .is_some_and(|parent| matches!(&parent.event, Event::SourceEntry { format, .. } if format == "pi-rs:thinking-selection")),
            Event::BranchModelSelected { .. } | Event::BranchSummary { fallback_model: Some(_), .. } => true,
            Event::Assistant { request_id: Some(_), .. } => true,
            _ => false,
        })
    }

    /// Record the host connection used for subsequent model requests.
    pub fn record_request_identity(&mut self, provider: &str, model: &str) -> Result<()> {
        let identity = serde_json::json!({"provider": provider, "model": model});
        if self
            .branch()
            .iter()
            .rev()
            .find_map(|record| match &record.event {
                Event::SourceEntry { format, entry } if format == "pi-rs:request-identity" => {
                    Some(entry)
                }
                _ => None,
            })
            .is_some_and(|previous| previous == &identity)
        {
            return Ok(());
        }
        self.append(Event::SourceEntry {
            format: "pi-rs:request-identity".into(),
            entry: identity,
        })?;
        Ok(())
    }

    // A Pi assistant may have been routed to another model after the last
    // explicit selection. Without the host catalog we cannot resolve virtual
    // models here, so persist an explicit user selection instead of deduping it.
    fn has_imported_response_since_selection(&self) -> bool {
        let branch = self.branch();
        for (index, record) in branch.iter().enumerate().rev() {
            match &record.event {
                Event::BranchModelSelected { .. }
                | Event::BranchSummary {
                    fallback_model: Some(_),
                    ..
                } => return false,
                Event::SourceEntry { format, entry } if format == "pi" => {
                    if entry["type"] == "model_change" {
                        return false;
                    }
                    if entry["type"] == "message"
                        && entry["message"]["role"] == "assistant"
                        && entry["message"]["api"] != "pi-virtual"
                    {
                        return true;
                    }
                }
                Event::ModelSelected { .. } => {
                    let imported = index.checked_sub(1).and_then(|i| branch.get(i)).is_some_and(|parent| {
                        matches!(&parent.event, Event::SourceEntry { format, .. } if format == "pi-rs:thinking-selection") ||
                        (parent.timestamp == record.timestamp && record.run_id.is_none() && matches!(&parent.event, Event::SourceEntry { format, entry } if format == "pi" && matches!(entry["type"].as_str(), Some("model_change" | "thinking_level_change" | "message"))))
                    });
                    if !imported {
                        return false;
                    }
                }
                _ => {}
            }
        }
        false
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
            .map(|name| name.trim_matches(is_ecmascript_whitespace))
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

    pub fn queued_custom_messages(&self) -> Vec<(u64, crate::QueuedCustomMessage)> {
        let branch = self.branch();
        let mut consumed = std::collections::HashSet::new();
        let mut globally_consumed = std::collections::HashSet::new();
        for record in &self.records {
            match &record.event {
                Event::CustomMessageDelivered { id, .. } => {
                    globally_consumed.insert(*id);
                }
                Event::InputQueueCleared { ids } => globally_consumed.extend(ids.iter().copied()),
                _ => {}
            }
        }
        for record in &branch {
            match &record.event {
                Event::CustomMessageDelivered { id, .. } => {
                    consumed.insert(*id);
                }
                Event::InputQueueCleared { ids } => consumed.extend(ids.iter().copied()),
                _ => {}
            }
        }
        let mut queued: Vec<_> = branch
            .into_iter()
            .filter_map(|record| match &record.event {
                Event::CustomMessageQueued { input } if !consumed.contains(&input.id) => {
                    Some((record.id, input.clone()))
                }
                _ => None,
            })
            .collect();
        queued.extend(
            self.records
                .iter()
                .filter_map(|record| match &record.event {
                    Event::TreeMessageQueued { input }
                        if !globally_consumed.contains(&input.id) =>
                    {
                        Some((record.id, input.clone()))
                    }
                    _ => None,
                }),
        );
        queued.sort_by_key(|(id, _)| *id);
        queued
    }

    /// Find a callback-triggered run that has not started or was interrupted.
    pub fn pending_custom_trigger(&self) -> Option<Uuid> {
        let branch = self.branch();
        let branch_ids: std::collections::HashSet<_> =
            branch.iter().map(|record| record.id).collect();
        self.records.iter().find_map(|record| {
            let (input, global) = match &record.event {
                Event::CustomMessageQueued { input } if branch_ids.contains(&record.id) => (input, false),
                Event::TreeMessageQueued { input } => (input, true),
                _ => return None,
            };
            if input.delivery != crate::CustomMessageDelivery::TriggerTurn {
                return None;
            }
            let entries: Vec<_> = if global { self.records.iter().collect() } else { branch.clone() };
            let last_state = entries.iter().rev().find_map(|entry| match entry.event {
                Event::Run { state, .. } if entry.run_id == Some(input.id) => Some(state),
                _ => None,
            });
            let delivered = entries.iter().any(|entry| matches!(entry.event, Event::CustomMessageDelivered { id, .. } if id == input.id));
            match last_state {
                None | Some(RunState::Running | RunState::WaitingForOperation | RunState::WaitingForReconciliation) => Some(input.id),
                Some(RunState::Cancelled) if !delivered => Some(input.id),
                _ => None,
            }
        })
    }

    /// Restore the branch that owns an interrupted session-wide tree trigger.
    /// The current branch must be settled; the destination may contain the
    /// pending operation that recovery is about to reconcile.
    pub(crate) fn begin_tree_trigger_handoff(
        &mut self,
        run_id: Uuid,
    ) -> Result<Option<(u64, Vec<Record>)>> {
        let is_tree_trigger = self.records.iter().any(|record| matches!(&record.event,
            Event::TreeMessageQueued { input } if input.id == run_id && input.delivery == crate::CustomMessageDelivery::TriggerTurn));
        if !is_tree_trigger || self.pending_custom_trigger() != Some(run_id) {
            return Ok(None);
        }
        let Some(target) = self
            .records
            .iter()
            .rev()
            .find(|record| {
                record.run_id == Some(run_id) && !matches!(record.event, Event::Checkout { .. })
            })
            .map(|record| record.id)
        else {
            return Ok(None);
        };
        if self.head == target {
            return Ok(None);
        }
        ensure_settled(self.operations())?;
        let return_to = self.head;
        let marker = self
            .append(Event::TreeTriggerHandoff { run_id, return_to })?
            .clone();
        let checkout = self.append(Event::Checkout { target })?.clone();
        Ok(Some((return_to, vec![marker, checkout])))
    }

    /// Restore the selected branch without abandoning the recoverable operation
    /// on the temporary branch. The saved target was settled when handoff began.
    pub(crate) fn finish_tree_trigger_handoff(&mut self, return_to: u64) -> Result<&Record> {
        if return_to == 0
            || return_to > self.records.len() as u64
            || matches!(
                self.records[(return_to - 1) as usize].event,
                Event::Checkout { .. }
            )
        {
            return Err(Error::Session("invalid tree trigger handoff target".into()));
        }
        self.append(Event::Checkout { target: return_to })
    }

    pub(crate) fn recover_tree_trigger_handoff(&mut self) -> Result<Option<&Record>> {
        let pending = self
            .pending_tree_trigger_handoff()
            .map(|(_, return_to)| return_to);
        match pending {
            Some(return_to) => self.finish_tree_trigger_handoff(return_to).map(Some),
            None => Ok(None),
        }
    }

    fn pending_tree_trigger_handoff(&self) -> Option<(u64, u64)> {
        self.records.iter().rev().find_map(|record| {
            let Event::TreeTriggerHandoff { return_to, .. } = record.event else {
                return None;
            };
            let returned = self.records[record.id as usize..].iter().any(|later| {
                matches!(later.event,
                Event::Checkout { target } if target == return_to)
            });
            (!returned).then_some((record.id, return_to))
        })
    }

    /// Resolve a transient handoff target before extracting a fork ancestry.
    /// The marker can live on the other branch and must be inspected in the
    /// complete source journal, not in the already-filtered fork records.
    pub(crate) fn fork_target_after_handoff(&self, requested: u64) -> Result<u64> {
        if requested == 0 || requested > self.records.len() as u64 {
            return Err(Error::Session("invalid fork entry".into()));
        }
        let mut target = requested;
        if let Some((marker, return_to)) = self.pending_tree_trigger_handoff() {
            let checkout =
                self.records[marker as usize..]
                    .iter()
                    .find_map(|record| match record.event {
                        Event::Checkout { target } => Some((record.id, target)),
                        _ => None,
                    });
            let temporary_descendant = checkout.is_some_and(|(checkout_id, temporary)| {
                requested == checkout_id
                    || (requested > checkout_id
                        && self
                            .path(requested)
                            .iter()
                            .any(|record| record.id == temporary))
            });
            if requested == marker || requested == self.head || temporary_descendant {
                target = return_to;
            }
        }
        Ok(target)
    }

    pub(crate) fn fork_records_after_handoff(&self, requested: u64) -> Result<Vec<Record>> {
        let mut records = self.fork_records(self.fork_target_after_handoff(requested)?)?;
        // A handoff belongs to the source session. Once a fork's final target
        // is chosen it must not replay a source handoff on reopen.
        for record in &mut records {
            if matches!(record.event, Event::TreeTriggerHandoff { .. }) {
                record.event = Event::SessionInfo {
                    name: None,
                    cwd: None,
                };
            }
        }
        Ok(records)
    }

    pub(crate) fn fork_session_after_handoff(&self, requested: u64) -> Result<Self> {
        let target = self.validated_fork_target(self.fork_target_after_handoff(requested)?)?;
        let records = self.fork_records_after_handoff(requested)?;
        let metadata = self.fork_metadata(target, &records);
        Self::from_records_with_metadata(records, metadata)
    }

    pub fn queued_inputs(&self) -> Vec<crate::QueuedMessage> {
        self.queued_input_records()
            .filter_map(|record| match &record.event {
                Event::InputQueued { input } => Some(input.clone().into()),
                Event::InputQueuedWithImages { input } => Some(input.clone()),
                _ => None,
            })
            .collect()
    }

    /// Pending text in steering and follow-up order, without copying attachments.
    pub fn queued_text(&self) -> (Vec<&str>, Vec<&str>) {
        let mut steering = Vec::new();
        let mut follow_up = Vec::new();
        for record in self.queued_input_records() {
            let input = match &record.event {
                Event::InputQueued { input } => input,
                Event::InputQueuedWithImages { input } => &input.input,
                _ => continue,
            };
            match input.kind {
                crate::InputKind::Steer => steering.push(input.content.as_str()),
                crate::InputKind::FollowUp => follow_up.push(input.content.as_str()),
            }
        }
        (steering, follow_up)
    }

    fn queued_input_records(&self) -> impl Iterator<Item = &Record> {
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
            .into_iter()
            .filter(move |record| match &record.event {
                Event::InputQueued { input } => !consumed.contains(&input.id),
                Event::InputQueuedWithImages { input } => !consumed.contains(&input.input.id),
                _ => false,
            })
    }
    pub fn clear_queued_inputs(&mut self) -> Result<Vec<crate::QueuedMessage>> {
        let pending = self.queued_inputs();
        let mut ids: Vec<_> = pending.iter().map(|input| input.input.id).collect();
        ids.extend(
            self.queued_custom_messages()
                .into_iter()
                .filter_map(|(_, input)| {
                    matches!(
                        input.delivery,
                        crate::CustomMessageDelivery::Steer
                            | crate::CustomMessageDelivery::FollowUp
                    )
                    .then_some(input.id)
                }),
        );
        if !ids.is_empty() {
            self.append(Event::InputQueueCleared { ids })?;
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
        self.record_bash_result(execution)
    }

    /// Append extension data without adding a message to the model context.
    pub fn append_custom_entry(&mut self, entry: crate::CustomEntry) -> Result<()> {
        self.ensure_settled()?;
        validate_custom_entry(&entry)?;
        self.append(Event::CustomEntry { entry })?;
        Ok(())
    }

    /// Append a custom message without starting a model turn.
    pub fn append_custom_message(&mut self, message: crate::CustomMessage) -> Result<()> {
        self.ensure_settled()?;
        validate_custom_message(&message)?;
        self.append(Event::CustomMessage { message })?;
        Ok(())
    }

    /// Record a separately running user shell after the local agent yields.
    /// An interrupted tool can still await reconciliation. Its eventual result
    /// is projected beside its tool call, before this shell output.
    pub fn append_local_bash_result(&mut self, execution: crate::BashExecution) -> Result<()> {
        if self.host_binding().is_some() {
            return Err(Error::Session(
                "a managed session must record shell output through its host".into(),
            ));
        }
        self.record_bash_result(execution)
    }

    /// Record background cache usage only on the session and branch that sent it.
    pub fn record_cache_warm(
        &mut self,
        session_id: Uuid,
        head: u64,
        metadata: ModelMetadata,
        usage: Usage,
    ) -> Result<Option<&Record>> {
        self.record_cache_warm_with_note(session_id, head, metadata, usage, None)
    }

    pub fn record_cache_warm_with_note(
        &mut self,
        session_id: Uuid,
        head: u64,
        metadata: ModelMetadata,
        usage: Usage,
        note: Option<String>,
    ) -> Result<Option<&Record>> {
        if self.host_binding().is_some() || self.session_id() != session_id || self.head != head {
            return Ok(None);
        }
        self.append_cache_warm(metadata, usage, note).map(Some)
    }

    /// A refresh during a run belongs to the latest unchanged model request.
    pub fn cache_request_current(&self, session_id: Uuid, request_id: Uuid) -> bool {
        self.session_id() == session_id
            && self.host_binding().is_none()
            && self
                .branch()
                .into_iter()
                .rev()
                .find_map(|record| match record.event {
                    Event::ModelRequested {
                        request_id: current,
                        ..
                    } => Some(current == Some(request_id)),
                    Event::Compacted { .. }
                    | Event::BranchSummary { .. }
                    | Event::ContextEdited { .. }
                    | Event::BashExecution { .. }
                    | Event::CustomMessage { .. }
                    | Event::CustomMessageDelivered { .. }
                    | Event::User { .. }
                    | Event::UserWithImages { .. }
                    | Event::UserImages { .. } => Some(false),
                    _ => None,
                })
                == Some(true)
    }

    pub(crate) fn append_cache_warm(
        &mut self,
        mut metadata: ModelMetadata,
        usage: Usage,
        note: Option<String>,
    ) -> Result<&Record> {
        metadata.replay = None;
        metadata.reasoning = None;
        metadata.diagnostics.clear();
        self.append(Event::CacheWarm {
            metadata,
            usage,
            note,
        })
    }

    fn record_bash_result(&mut self, execution: crate::BashExecution) -> Result<()> {
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

    /// Clone every branch only when the journal contains no executable or queued work.
    /// Unsupported histories return `None` so callers may retain branch-only behavior.
    pub fn fork_inert_full_history(&self) -> Result<Option<Self>> {
        if self.host_binding().is_some()
            || self.records.iter().any(|record| {
                matches!(
                    record.event,
                    Event::HostBound { .. } | Event::HostRunBound { .. }
                )
            })
        {
            return Err(Error::Session(
                "managed sessions must be forked by their host".into(),
            ));
        }
        let inert = self.records.iter().all(|record| {
            record.run_id.is_none()
                && match &record.event {
                    Event::Created { .. }
                    | Event::SessionInfo { .. }
                    | Event::ParentSession { .. }
                    | Event::Checkout { .. }
                    | Event::User { .. }
                    | Event::UserWithImages { .. }
                    | Event::UserImages { .. }
                    | Event::ModelSelected { .. }
                    | Event::Label { .. }
                    | Event::ContextEdited { .. }
                    | Event::Compacted { .. }
                    | Event::BranchSummary { .. }
                    | Event::BashExecution { .. } => true,
                    Event::SourceEntry { format, .. } => {
                        matches!(format.as_str(), "pi" | "pi-rs:external-session-id")
                    }
                    Event::ModelFinished { reply, .. } => reply.is_none(),
                    Event::Assistant { invocations, .. } => invocations.is_empty(),
                    _ => false,
                }
        });
        if !inert {
            return Ok(None);
        }
        let mut records = self.records.clone();
        if let Event::Created { session_id } = &mut records[0].event {
            *session_id = Uuid::new_v4();
            records[0].timestamp = Some(record_timestamp());
        }
        Ok(Some(Self::from_records_with_metadata(
            records,
            self.record_metadata.clone(),
        )?))
    }

    /// Export one settled branch with fresh session identity and remapped record references.
    /// Auxiliary record metadata is not representable in this legacy return type;
    /// use `fork_session` or `fork_to` when it must be preserved.
    pub fn fork_records(&self, target: u64) -> Result<Vec<Record>> {
        if self.host_binding().is_some() {
            return Err(Error::Session(
                "managed sessions must be forked by their host".into(),
            ));
        }
        let target = self.validated_fork_target(target)?;
        self.conversation_at(target)?;
        let branch = self.path(target);
        let pi_sources: Vec<_> = self.records.iter().filter(|record|
            matches!(&record.event, Event::SourceEntry { format, .. } if format == "pi")).collect();
        let legacy_pi = pi_sources.first().is_some_and(|record| {
            matches!(&record.event, Event::SourceEntry { entry, .. }
                if entry["type"] == "session" && entry["version"].as_u64().unwrap_or(1) == 1)
        });
        let source_ids: BTreeMap<u64, String> = pi_sources
            .iter()
            .enumerate()
            .filter_map(|(index, record)| {
                let Event::SourceEntry { entry, .. } = &record.event else {
                    return None;
                };
                if entry["type"] == "session" {
                    return None;
                }
                entry["id"]
                    .as_str()
                    .map(str::to_owned)
                    .or_else(|| (legacy_pi && index > 0).then(|| format!("legacy-{index}")))
                    .map(|id| (record.id, id))
            })
            .collect();
        let sources_by_id: BTreeMap<_, _> = source_ids
            .iter()
            .map(|(record, id)| (id.as_str(), *record))
            .collect();
        let source_indices: BTreeMap<u64, usize> = pi_sources
            .iter()
            .enumerate()
            .map(|(index, record)| (record.id, index))
            .collect();
        // Only durable import provenance proves which native records belong
        // to a source entry. In particular, an entry may produce zero or
        // several records, and timestamps alone cannot distinguish a native
        // child from a conversion.
        let mut canonical = BTreeMap::new();
        let mut unproven = BTreeMap::<u64, u64>::new();
        let selected: std::collections::HashSet<_> =
            branch.iter().map(|record| record.id).collect();
        let selected_source_ids: std::collections::HashSet<_> = source_ids
            .iter()
            .filter_map(|(id, source)| selected.contains(id).then_some(source.as_str()))
            .collect();
        let mut relevant_sources = selected.clone();
        for source in &self.records {
            if let Event::SourceEntry { format, entry } = &source.event
                && format == "pi" && entry["type"] == "label"
                && (entry["targetId"].as_str().is_some_and(|id| selected_source_ids.contains(id))
                    || self.records.get(source.id as usize).is_some_and(|record|
                        matches!(&record.event, Event::Label { target, .. } if selected.contains(target))))
            {
                relevant_sources.insert(source.id);
            }
        }
        let verified_conversions = crate::pi_import::validate_conversions(self, &relevant_sources)?;
        for source in &self.records {
            let Event::SourceEntry { format, entry } = &source.event else {
                continue;
            };
            if format != "pi" || entry["type"] == "session" {
                continue;
            }
            if !selected.contains(&source.id)
                && !(entry["type"] == "label"
                    && (entry["targetId"].as_str().is_some_and(|id| selected_source_ids.contains(id))
                        || self.records.get(source.id as usize).is_some_and(|record|
                            matches!(&record.event, Event::Label { target, .. } if selected.contains(target)))))
            {
                continue;
            }
            let count = verified_conversions.get(&source.id).copied();
            if let Some(count) = count {
                if count > self.records.len() as u64 - source.id {
                    return Err(Error::Session(
                        "invalid imported Pi conversion provenance".into(),
                    ));
                }
                let mut parent = source.id;
                for offset in 0..count {
                    let derived = self
                        .records
                        .get((source.id + offset) as usize)
                        .filter(|record| {
                            record.parent == Some(parent)
                                && record.timestamp == source.timestamp
                                && record.run_id.is_none()
                                && !matches!(
                                    record.event,
                                    Event::SourceEntry { .. } | Event::Checkout { .. }
                                )
                        })
                        .ok_or_else(|| {
                            Error::Session("invalid imported Pi conversion provenance".into())
                        })?;
                    if canonical.insert(derived.id, source.id).is_some() {
                        return Err(Error::Session(
                            "overlapping imported Pi conversion provenance".into(),
                        ));
                    }
                    parent = derived.id;
                }
                if selected.contains(&source.id)
                    && (1..=count).any(|offset| !selected.contains(&(source.id + offset)))
                {
                    return Err(Error::Session(
                        "cannot fork through a partial imported Pi conversion".into(),
                    ));
                }
            } else {
                let mut parent = source.id;
                for derived in self.records.iter().skip(source.id as usize) {
                    if derived.parent != Some(parent)
                        || derived.timestamp != source.timestamp
                        || derived.run_id.is_some()
                        || matches!(
                            derived.event,
                            Event::SourceEntry { .. } | Event::Checkout { .. }
                        )
                    {
                        break;
                    }
                    unproven.insert(source.id, source.id);
                    unproven.insert(derived.id, source.id);
                    parent = derived.id;
                }
            }
        }
        let mut uncertain_targets = BTreeMap::<u64, std::collections::HashSet<u64>>::new();
        for record in &self.records {
            if let Event::Label { target, .. } = record.event
                && let Some(source) = unproven.get(&target)
                && selected.contains(source)
            {
                uncertain_targets.entry(*source).or_default().insert(target);
            }
        }
        if uncertain_targets.values().any(|targets| {
            targets.len() > 1 || targets.iter().any(|target| !selected.contains(target))
        }) {
            return Err(Error::Session(
                "cannot resolve ambiguous imported label targets without conversion provenance"
                    .into(),
            ));
        }
        // Labels are session-wide, but a selected fork contains fresh labels
        // only for targets retained on its path. Keep stable record slots for
        // removed labels so all other native references remain remappable.
        let removed_labels: std::collections::HashSet<u64> = branch
            .iter()
            .filter_map(|record| match &record.event {
                Event::Label { .. } => Some(record.id),
                Event::SourceEntry { format, entry }
                    if format == "pi" && entry["type"] == "label" =>
                {
                    Some(record.id)
                }
                _ => None,
            })
            .collect();
        let mut label_replacements = BTreeMap::new();
        let mut next = None;
        for record in branch.iter().rev() {
            if removed_labels.contains(&record.id) {
                label_replacements.insert(record.id, next);
            } else {
                next = Some(record.id);
            }
        }
        let mut source_labels = BTreeMap::new();
        for record in &branch {
            if let Event::SourceEntry { format, entry } = &record.event
                && format == "pi"
                && entry["type"] == "label"
                && let Some(id) = source_ids.get(&record.id)
            {
                source_labels.insert(id.clone(), record.id);
            }
        }
        // A Pi label is stored as a source record followed by its native
        // projection. A fork through only one half cannot preserve both the
        // Pi tree and the native session's global label state.
        for (index, record) in branch.iter().enumerate() {
            let Event::SourceEntry { format, entry } = &record.event else {
                continue;
            };
            if format != "pi" {
                continue;
            }
            if entry["type"] == "label" {
                let paired = branch.get(index + 1).is_some_and(|next| {
                    next.parent == Some(record.id) && next.timestamp == record.timestamp
                        && next.run_id.is_none()
                        && (canonical.get(&next.id) == Some(&record.id)
                            || (unproven.get(&next.id) == Some(&record.id)
                                && entry["targetId"].as_str().is_some_and(|target_id| {
                                    sources_by_id.get(target_id)
                                        .is_some_and(|target_source| matches!(&next.event,
                                            Event::Label { target, .. }
                                                if target == target_source || unproven.get(target) == Some(target_source)
                                                    || canonical.get(target) == Some(target_source)))
                                })))
                        && matches!(&next.event, Event::Label { label, .. }
                            if label.as_deref() == entry["label"].as_str())
                });
                if !paired {
                    return Err(Error::Session(
                        "cannot fork through an incomplete or ambiguous imported Pi label".into(),
                    ));
                }
            } else {
                for key in ["targetId", "fromId"] {
                    if entry[key]
                        .as_str()
                        .is_some_and(|id| source_labels.contains_key(id))
                    {
                        return Err(Error::Session(format!(
                            "cannot fork: Pi {key} references a removed label"
                        )));
                    }
                }
                if entry["type"] == "message"
                    && entry["message"]["role"] == "branchSummary"
                    && entry["message"]["fromId"]
                        .as_str()
                        .is_some_and(|id| source_labels.contains_key(id))
                {
                    return Err(Error::Session(
                        "cannot fork: Pi branch summary references a removed label".into(),
                    ));
                }
            }
        }
        for record in &branch {
            let referenced = match &record.event {
                Event::ContextEdited { target, .. } => Some(*target),
                Event::BranchSummary { from, .. } => *from,
                _ => None,
            };
            if referenced.is_some_and(|id| removed_labels.contains(&id)) {
                return Err(Error::Session(
                    "cannot fork: retained entry references a removed label".into(),
                ));
            }
        }
        let ids: BTreeMap<_, _> = branch
            .iter()
            .enumerate()
            .map(|(index, record)| (record.id, index as u64 + 1))
            .collect();
        let mut labels = BTreeMap::<u64, (String, Option<String>, u64)>::new();
        let mut label_order = 0_u64;
        for record in &self.records {
            if let Event::Label { target, label } = &record.event {
                let target = canonical.get(target).copied().unwrap_or(*target);
                if let Some(label) = label.as_ref().filter(|label| !label.is_empty()) {
                    let order = labels.get(&target).map_or_else(
                        || {
                            let order = label_order;
                            label_order += 1;
                            order
                        },
                        |(_, _, order)| *order,
                    );
                    labels.insert(target, (label.clone(), record.timestamp.clone(), order));
                } else {
                    labels.remove(&target);
                }
            }
        }
        // Tree messages are consumed session-wide, even when their delivery is
        // on a branch that is absent from the fork's ancestry.
        let globally_consumed: std::collections::HashSet<Uuid> = self
            .records
            .iter()
            .flat_map(|record| match &record.event {
                Event::CustomMessageDelivered { id, .. } => vec![*id],
                Event::InputQueueCleared { ids } => ids.clone(),
                _ => Vec::new(),
            })
            .collect();
        let mut records = Vec::new();
        for record in branch {
            let summary_metadata = record
                .summary_usage(&self.records)
                .and_then(|(_, metadata)| metadata.map(Box::new));
            let mut record = record.clone();
            let original_id = record.id;
            record.id = ids[&record.id];
            record.parent = record.parent.and_then(|parent| ids.get(&parent).copied());
            if removed_labels.contains(&original_id) {
                record.event = Event::SessionInfo {
                    name: None,
                    cwd: None,
                };
            }
            if matches!(&record.event, Event::TreeMessageQueued { input } if globally_consumed.contains(&input.id))
            {
                record.event = Event::SessionInfo {
                    name: None,
                    cwd: None,
                };
            }
            match &mut record.event {
                Event::Created { session_id } => {
                    *session_id = Uuid::new_v4();
                    record.timestamp = Some(record_timestamp());
                }
                Event::ModelRequested { context_head, .. } => {
                    *context_head = context_head.and_then(|id| ids.get(&id).copied());
                }
                Event::SourceEntry { format, entry } if format == "pi-rs:overflow-retention-v1" => {
                    if let Some(old) = entry.get("boundary").and_then(serde_json::Value::as_u64) {
                        if let Some(new) = ids.get(&old) {
                            entry["boundary"] = serde_json::json!(new);
                        } else if let Some(object) = entry.as_object_mut() {
                            // A missing retained record must not become a valid
                            // all-history boundary on recovery.
                            object.remove("boundary");
                        }
                    }
                }
                Event::SourceEntry { format, entry } if format == "pi" => {
                    if legacy_pi && entry["type"] != "session" {
                        if let Some(id) = source_ids.get(&original_id) {
                            entry["id"] = serde_json::json!(id);
                        }
                        if entry["type"] == "compaction"
                            && let Some(index) = entry["firstKeptEntryIndex"]
                                .as_u64()
                                .and_then(|n| usize::try_from(n).ok())
                        {
                            let current = source_indices[&original_id];
                            let boundary = if index >= current && index < pi_sources.len() {
                                current
                            } else {
                                index
                            };
                            let source = pi_sources
                                .get(boundary)
                                .and_then(|record| source_ids.get(&record.id))
                                .ok_or_else(|| {
                                    Error::Session(
                                        "cannot map legacy Pi compaction boundary".into(),
                                    )
                                })?;
                            entry["firstKeptEntryId"] = serde_json::json!(source);
                            entry
                                .as_object_mut()
                                .expect("Pi entry object")
                                .remove("firstKeptEntryIndex");
                        }
                    }
                    if entry["type"] == "compaction"
                        && let Some(old) = entry["firstKeptEntryId"].as_str()
                        && let Some(label_id) = source_labels.get(old)
                    {
                        let replacement = label_replacements
                            .get(label_id)
                            .copied()
                            .flatten()
                            .ok_or_else(|| {
                                Error::Session(
                                    "fork compaction boundary has no retained entry".into(),
                                )
                            })?;
                        let replacement_entry = &self.records[(replacement - 1) as usize];
                        let Event::SourceEntry { format, .. } = &replacement_entry.event else {
                            return Err(Error::Session(
                                "fork compaction boundary cannot be represented as a Pi entry"
                                    .into(),
                            ));
                        };
                        let replacement_id = source_ids
                            .get(&replacement)
                            .filter(|_| format == "pi")
                            .ok_or_else(|| {
                                Error::Session(
                                    "fork compaction boundary lacks a Pi entry ID".into(),
                                )
                            })?;
                        entry["firstKeptEntryId"] = serde_json::json!(replacement_id);
                    }
                }
                Event::TreeTriggerHandoff { return_to, .. } => {
                    *return_to = *ids
                        .get(return_to)
                        .ok_or_else(|| Error::Session("missing tree handoff branch".into()))?;
                }
                Event::BranchModelSelected { target, .. } => {
                    *target = *ids
                        .get(target)
                        .ok_or_else(|| Error::Session("missing branch model target".into()))?;
                }
                Event::TurnFinished { assistant_record }
                | Event::TurnEndCallbacksCompleted { assistant_record }
                | Event::TurnEndBoundaryApplied { assistant_record } => {
                    *assistant_record = *ids
                        .get(assistant_record)
                        .ok_or_else(|| Error::Session("missing fork response".into()))?;
                }
                Event::Compacted {
                    retain_from,
                    usage_metadata,
                    ..
                } => {
                    *retain_from = retain_from.and_then(|id| {
                        let replacement =
                            label_replacements.get(&id).copied().unwrap_or(Some(id))?;
                        let is_self = label_replacements.contains_key(&id)
                            && matches!(&self.records[(replacement - 1) as usize].event,
                                Event::SourceEntry { format, entry }
                                if format == "pi" && entry["type"] == "compaction")
                            && canonical.get(&original_id) == Some(&replacement);
                        if is_self || replacement == original_id {
                            return None;
                        }
                        let replacement = if label_replacements.contains_key(&id)
                            && canonical.get(&(replacement + 1)) == Some(&replacement)
                        {
                            replacement + 1
                        } else {
                            replacement
                        };
                        ids.get(&replacement).copied()
                    });
                    *usage_metadata = summary_metadata;
                }
                Event::BranchSummary {
                    from,
                    target,
                    usage_metadata,
                    ..
                } => {
                    *from = from.and_then(|id| ids.get(&id).copied());
                    *target = None;
                    *usage_metadata = summary_metadata;
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
        // Tree callback queues are session-wide and may live on the branch left
        // behind. Preserve still-pending entries when exporting another branch.
        for (source, input) in self.queued_custom_messages() {
            if !ids.contains_key(&source)
                && matches!(
                    self.records[(source - 1) as usize].event,
                    Event::TreeMessageQueued { .. }
                )
            {
                let id = records.len() as u64 + 1;
                records.push(Record {
                    timestamp: Some(record_timestamp()),
                    run_id: None,
                    version: records.last().map_or(TURN_END_JOURNAL_VERSION, |record| {
                        record.version.max(TURN_END_JOURNAL_VERSION)
                    }),
                    id,
                    parent: Some(id - 1),
                    event: Event::TreeMessageQueued { input },
                });
            }
        }
        let mut retained_labels: Vec<_> = labels
            .into_iter()
            .filter_map(|(target, (label, timestamp, order))| {
                (!removed_labels.contains(&target))
                    .then(|| ids.get(&target).copied())
                    .flatten()
                    .map(|target| (target, label, timestamp, order))
            })
            .collect();
        retained_labels.sort_by_key(|(_, _, _, order)| *order);
        for (target, label, timestamp, _) in retained_labels {
            let id = records.len() as u64 + 1;
            records.push(Record {
                timestamp,
                run_id: None,
                version: records
                    .last()
                    .map_or(TURN_END_JOURNAL_VERSION, |record| record.version),
                id,
                parent: Some(id - 1),
                event: Event::Label {
                    target,
                    label: Some(label),
                },
            });
        }
        validated_head(&records)?;
        Ok(records)
    }

    /// Fork a settled branch in memory, preserving auxiliary record metadata.
    pub fn fork_session(&self, target: u64) -> Result<Self> {
        let target = self.validated_fork_target(target)?;
        let records = self.fork_records(target)?;
        let metadata = self.fork_metadata(target, &records);
        Self::from_records_with_metadata(records, metadata)
    }

    fn validated_fork_target(&self, target: u64) -> Result<u64> {
        if target == 0 || target > self.records.len() as u64 {
            return Err(Error::Session("invalid fork entry".into()));
        }
        let path = self.path(target);
        let mut relevant: std::collections::HashSet<_> =
            path.iter().map(|record| record.id).collect();
        let sources: Vec<_> = self.records.iter().filter(|record| matches!(&record.event, Event::SourceEntry { format, .. } if format == "pi")).collect();
        let legacy = sources.first().is_some_and(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["type"] == "session" && entry["version"].as_u64().unwrap_or(1) == 1));
        let names: std::collections::HashSet<String> = sources
            .iter()
            .enumerate()
            .filter_map(|(index, record)| {
                if !relevant.contains(&record.id) {
                    return None;
                }
                let Event::SourceEntry { entry, .. } = &record.event else {
                    return None;
                };
                entry["id"]
                    .as_str()
                    .map(str::to_owned)
                    .or_else(|| (legacy && index > 0).then(|| format!("legacy-{index}")))
            })
            .collect();
        for source in &sources {
            if let Event::SourceEntry { entry, .. } = &source.event && entry["type"] == "label"
                && (entry["targetId"].as_str().is_some_and(|id| names.contains(id))
                    || self.records.get(source.id as usize).is_some_and(|record| matches!(&record.event, Event::Label { target, .. } if relevant.contains(target))))
            { relevant.insert(source.id); }
        }
        let verified = crate::pi_import::validate_conversions(self, &relevant)?;
        if let Some(source) = path
            .iter()
            .rev()
            .find(|record| matches!(&record.event, Event::SourceEntry { .. }))
            && let Some(count) = verified.get(&source.id)
            && target >= source.id
            && target < source.id + count
        {
            return Ok(source.id + count);
        }
        Ok(target)
    }

    fn fork_metadata(&self, target: u64, records: &[Record]) -> BTreeMap<u64, serde_json::Value> {
        let path = self.path(target);
        path
            .iter()
            .enumerate()
            .filter_map(|(index, original)| {
                let forked = records.get(index)?;
                (std::mem::discriminant(&original.event) == std::mem::discriminant(&forked.event)
                    || matches!(&original.event, Event::Label { .. })
                    || matches!(&original.event, Event::SourceEntry { format, entry } if format == "pi" && entry["type"] == "label"))
                    .then(|| {
                        self.record_metadata.get(&original.id).map(|value| (forked.id, value.clone()))
                    })
                    .flatten()
            })
            .collect()
    }

    /// Publish a complete local journal without replacing an existing file.
    pub fn export_jsonl(&self, destination: impl AsRef<Path>) -> Result<()> {
        if self.host_binding().is_some() {
            return Err(Error::Session(
                "managed sessions must be exported by their host".into(),
            ));
        }
        publish_snapshot(&self.records, &self.record_metadata, destination.as_ref())
    }

    /// Publish an already validated in-memory fork and keep the same locked
    /// file handle, avoiding a reopen/recovery window after publication.
    pub(crate) fn publish_and_bind_snapshot(&mut self, destination: &Path) -> Result<()> {
        if self.host_binding().is_some() {
            return Err(Error::Session(
                "managed sessions must be exported by their host".into(),
            ));
        }
        validated_head(&self.records)?;
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        let parent_dir = File::open(parent)?;
        #[cfg(windows)]
        let lock_file = {
            let mut lock_path = destination.as_os_str().to_os_string();
            lock_path.push(".lock");
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(lock_path)?;
            lock.try_lock()
                .map_err(|error| Error::Session(format!("cannot lock journal: {error}")))?;
            lock
        };
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        for record in &self.records {
            let mut line = encode_record(record, self.record_metadata.get(&record.id))?;
            line.push(b'\n');
            if line.len() > MAX_RECORD_BYTES {
                return Err(Error::Session("journal record exceeds size limit".into()));
            }
            temporary.write_all(&line)?;
        }
        temporary.as_file().sync_all()?;
        #[cfg(not(windows))]
        temporary
            .as_file()
            .try_lock()
            .map_err(|error| Error::Session(format!("cannot lock journal: {error}")))?;
        let file = temporary
            .persist_noclobber(destination)
            .map_err(|error| Error::Io(error.error))?;
        #[cfg(unix)]
        parent_dir.sync_all()?;
        self.store = Box::new(FileStore {
            file,
            metadata: self.record_metadata.clone(),
            #[cfg(windows)]
            lock_file,
            #[cfg(unix)]
            parent: parent.into(),
        });
        Ok(())
    }

    /// Create and open an independent, persistent branch with fresh session identity.
    pub fn fork_to(&self, target: u64, destination: impl AsRef<Path>) -> Result<Self> {
        let fork = self.fork_session(target)?;
        publish_snapshot(&fork.records, &fork.record_metadata, destination.as_ref())?;
        Self::open(destination)
    }

    /// Fork from an existing record. The abandoned branch stays available by ID.
    pub fn checkout(&mut self, target: u64) -> Result<()> {
        self.validate_checkout(target)?;
        self.append(Event::Checkout { target })?;
        Ok(())
    }

    pub(crate) fn validate_checkout(&self, target: u64) -> Result<()> {
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
        Ok(())
    }

    /// Resolve the actual destination of a tree selection without changing the journal.
    pub(crate) fn tree_navigation_target(&self, selected: u64) -> Result<Option<u64>> {
        self.validate_checkout(selected)?;
        if selected == self.head() {
            return Ok(None);
        }
        let record = &self.records[selected as usize - 1];
        let target = if matches!(
            record.event,
            Event::User { .. } | Event::UserWithImages { .. }
        ) {
            record.parent.unwrap_or(1)
        } else {
            selected
        };
        self.validate_checkout(target)?;
        Ok(Some(target))
    }

    /// Confirm the outcome via the system that owns the effect, using the operation ID.
    pub fn reconcile(&mut self, operation_id: Uuid, outcome: Outcome) -> Result<()> {
        if !matches!(
            outcome,
            Outcome::Succeeded(_)
                | Outcome::SucceededWithDetails { .. }
                | Outcome::Failed(_)
                | Outcome::TextResult { .. }
                | Outcome::TextResultWithStructuredContent { .. }
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
        self.append_inner(event, true)
    }

    fn append_inner(&mut self, event: Event, model_marker: bool) -> Result<&Record> {
        self.append_inner_with_metadata(event, model_marker, None, false)
    }

    pub(crate) fn append_with_record_metadata(
        &mut self,
        event: Event,
        metadata: serde_json::Value,
    ) -> Result<&Record> {
        if metadata.is_null() {
            return Err(Error::Session("record metadata must not be null".into()));
        }
        self.append_inner_with_metadata(event, true, Some(metadata), false)
    }

    /// Append optional presentation metadata only when the store can retain it.
    pub(crate) fn append_with_advisory_metadata(
        &mut self,
        event: Event,
        metadata: Option<serde_json::Value>,
    ) -> Result<&Record> {
        self.append_inner_with_metadata(event, true, metadata, true)
    }

    fn append_inner_with_metadata(
        &mut self,
        event: Event,
        model_marker: bool,
        mut metadata: Option<serde_json::Value>,
        advisory: bool,
    ) -> Result<&Record> {
        if advisory && !self.store.supports_record_metadata() {
            metadata = None;
        }
        if self.poisoned {
            return Err(Error::Session(
                "journal write failed; reopen before continuing".into(),
            ));
        }
        // Reject unsupported metadata before appending even implicit marker
        // records. No write was attempted, so the journal remains usable.
        if metadata.is_some() && !self.store.supports_record_metadata() {
            return Err(Error::Session(
                "session store does not support record metadata".into(),
            ));
        }
        if matches!(
            &event,
            Event::ModelRequested {
                tool_decision: Some(_),
                ..
            }
        ) && metadata.is_none()
        {
            return Err(Error::Session(
                "marked request requires tool decision metadata".into(),
            ));
        }
        if matches!(
            &event,
            Event::ToolStarted {
                tool_execution: Some(_),
                ..
            }
        ) && metadata.is_none()
        {
            return Err(Error::Session("marked tool start requires metadata".into()));
        }
        // A native message branching directly from a legacy Pi source needs a
        // distinct parent, even if its content is identical to the old message.
        if matches!(&event, Event::CustomMessage { .. } | Event::CustomMessageDelivered { .. })
            && self.records.get(self.head.saturating_sub(1) as usize).is_some_and(|record| {
                matches!(&record.event, Event::SourceEntry { format, entry }
                    if format == "pi" && entry["type"] == "message"
                        && matches!(entry["message"]["role"].as_str(), Some("custom" | "hookMessage")))
            })
        {
            self.append(Event::SourceEntry {
                format: "pi-rs:native-custom-message".into(),
                entry: serde_json::Value::Null,
            })?;
        }
        // Distinguish a native selection from a conversion appended directly
        // after a Pi source entry that did not itself produce a selection.
        if model_marker
            && matches!(&event, Event::ModelSelected { .. })
            && self.needs_native_model_selection_marker()
        {
            self.append(Event::SourceEntry {
                format: "pi-rs:native-model-selection".into(),
                entry: serde_json::Value::Null,
            })?;
        }
        let id = self.records.len() as u64 + 1;
        let next_head = match event {
            Event::Checkout { target } => target,
            _ => id,
        };
        let parent = match &event {
            Event::BranchModelSelected { target, .. } => Some(*target),
            Event::BranchSummary {
                target: Some(target),
                ..
            } => Some(*target),
            _ => (self.head != 0).then_some(self.head),
        };
        let mut timestamp = record_timestamp();
        if !model_marker
            && self.needs_native_model_selection_marker()
            && self
                .records
                .get(self.head.saturating_sub(1) as usize)
                .and_then(|record| record.timestamp.as_deref())
                == Some(timestamp.as_str())
        {
            timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
        }
        let version = if matches!(
            &event,
            Event::ToolStarted {
                tool_execution: Some(_),
                ..
            }
        ) {
            TOOL_EXECUTION_JOURNAL_VERSION
        } else if matches!(
            &event,
            Event::ModelRequested {
                tool_decision: Some(_),
                ..
            }
        ) {
            self.records
                .last()
                .map_or(TOOL_DECISION_JOURNAL_VERSION, |record| {
                    record.version.max(TOOL_DECISION_JOURNAL_VERSION)
                })
        } else if matches!(
            &event,
            Event::BranchModelSelected { .. }
                | Event::BranchSummary {
                    fallback_model: Some(_),
                    ..
                }
        ) {
            self.records
                .last()
                .map_or(BRANCH_FALLBACK_JOURNAL_VERSION, |record| {
                    record.version.max(BRANCH_FALLBACK_JOURNAL_VERSION)
                })
        } else {
            self.records
                .last()
                .map_or(TURN_END_JOURNAL_VERSION, |record| {
                    record.version.max(TURN_END_JOURNAL_VERSION)
                })
        };
        let record = Record {
            timestamp: Some(timestamp),
            run_id: self.run_id,
            version,
            id,
            parent,
            event,
        };
        if matches!(
            &record.event,
            Event::ToolStarted {
                tool_execution: Some(_),
                ..
            }
        ) {
            let limit = self
                .store
                .encoded_record_limit()
                .unwrap_or(MAX_RECORD_BYTES)
                .min(MAX_RECORD_BYTES);
            if encode_record(&record, metadata.as_ref())?
                .len()
                .saturating_add(1)
                > limit
            {
                return Err(Error::Session("journal record exceeds size limit".into()));
            }
        }
        // Reject an oversized metadata record before a store write is attempted;
        // this is a deterministic validation failure, not an ambiguous I/O error.
        if metadata.is_some()
            && let Some(limit) = self.store.encoded_record_limit()
            && encode_record(&record, metadata.as_ref())?
                .len()
                .saturating_add(1)
                > limit
        {
            if advisory {
                metadata = None;
            } else {
                return Err(Error::Session("journal record exceeds size limit".into()));
            }
        }
        if let Err(err) = self.store.append_with_metadata(&record, metadata.as_ref()) {
            self.poisoned = true;
            return Err(err);
        }
        self.head = next_head;
        if let Some(metadata) = metadata {
            self.record_metadata.insert(id, metadata);
        }
        self.records.push(record);
        Ok(self.records.last().unwrap())
    }
}

pub(crate) fn validate_custom_entry(entry: &crate::CustomEntry) -> Result<()> {
    if serde_json::to_vec(entry)?.len() > 8 * 1024 * 1024 {
        return Err(Error::Config("custom entry exceeds size limit".into()));
    }
    Ok(())
}

pub(crate) fn validate_custom_message(message: &crate::CustomMessage) -> Result<()> {
    if serde_json::to_vec(message)?.len() > 8 * 1024 * 1024 {
        return Err(Error::Config("custom message exceeds size limit".into()));
    }
    if let crate::ContextContent::Blocks(blocks) = &message.content {
        for block in blocks {
            match block {
                crate::ContentBlock::Text { .. } => {}
                crate::ContentBlock::Image { data, mime_type } => crate::ImageContent {
                    data: data.clone(),
                    mime_type: mime_type.clone(),
                }
                .validate()?,
                _ => {
                    return Err(Error::Config(
                        "custom messages accept only text and images".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn publish_snapshot(
    records: &[Record],
    metadata: &BTreeMap<u64, serde_json::Value>,
    destination: &Path,
) -> Result<()> {
    validated_head(records)?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    for record in records {
        let mut line = encode_record(record, metadata.get(&record.id))?;
        line.push(b'\n');
        if line.len() > MAX_RECORD_BYTES {
            return Err(Error::Session("journal record exceeds size limit".into()));
        }
        temporary.write_all(&line)?;
    }
    temporary.as_file().sync_all()?;
    temporary
        .persist_noclobber(destination)
        .map_err(|error| Error::Io(error.error))?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    Ok(())
}

/// Whether a completion is a marked, display-only cancelled turn partial.
#[doc(hidden)]
pub fn is_aborted_turn_partial(records: &[Record], record: &Record) -> bool {
    let Event::ModelFinished {
        request_id,
        failure: Some(ModelFailure::Cancelled),
        partial,
        ..
    } = &record.event
    else {
        return false;
    };
    let Some(parent) = record
        .parent
        .and_then(|id| id.checked_sub(1))
        .and_then(|index| records.get(index as usize))
    else {
        return false;
    };
    matches!(&parent.event, Event::SourceEntry { format, entry }
        if format == "pi-rs:aborted-turn-partial"
            && entry["requestId"].as_str().and_then(|id| Uuid::parse_str(id).ok()) == Some(*request_id)
            && !partial.is_empty())
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
    let assistant =
        matches!(record.event, Event::Assistant { .. }) || is_aborted_turn_partial(records, record);
    if !assistant
        && !matches!(
            record.event,
            Event::User { .. }
                | Event::CustomMessage { .. }
                | Event::CustomMessageDelivered { .. }
                | Event::UserWithImages { .. }
                | Event::ToolFinished { .. }
                | Event::ToolReconciled { .. }
                | Event::ImportedUnattributedToolResult { .. }
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
                        ..
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
        let is_prior_position = |target: u64| {
            target > 0
                && target < id
                && !matches!(records[target as usize - 1].event, Event::Checkout { .. })
        };
        let expected_parent = match &record.event {
            Event::BranchModelSelected {
                target,
                provider,
                model,
            } => {
                if !is_prior_position(*target)
                    || provider.trim().is_empty()
                    || model.trim().is_empty()
                {
                    return Err(Error::Session("invalid branch model selection".into()));
                }
                Some(*target)
            }
            Event::BranchSummary {
                target,
                from,
                fallback_model,
                ..
            } => {
                if target.is_some_and(|target| !is_prior_position(target))
                    || from.is_some_and(|from| !is_prior_position(from))
                    || fallback_model.as_ref().is_some_and(|model| {
                        model.provider.trim().is_empty() || model.model.trim().is_empty()
                    })
                {
                    return Err(Error::Session("invalid branch summary position".into()));
                }
                target.or((head != 0).then_some(head))
            }
            _ => (head != 0).then_some(head),
        };
        if let Event::TreeTriggerHandoff { return_to, .. } = record.event
            && !is_prior_position(return_to)
        {
            return Err(Error::Session("invalid tree trigger handoff target".into()));
        }
        if !(version..=FORMAT_VERSION).contains(&record.version)
            || record.id != id
            || record.parent != expected_parent
            || matches!(record.event, Event::Created { .. }) != (id == 1)
        {
            return Err(Error::Session(format!("invalid journal record {id}")));
        }
        if record.version < TOOL_EXECUTION_JOURNAL_VERSION
            && matches!(
                &record.event,
                Event::ToolStarted {
                    tool_execution: Some(_),
                    ..
                }
            )
        {
            return Err(Error::Session(format!(
                "invalid tool execution version in record {id}"
            )));
        }
        if record.version < TOOL_DECISION_JOURNAL_VERSION
            && matches!(
                &record.event,
                Event::ModelRequested {
                    tool_decision: Some(_),
                    ..
                }
            )
        {
            return Err(Error::Session(format!(
                "invalid tool decision version in record {id}"
            )));
        }
        if record.version < BRANCH_FALLBACK_JOURNAL_VERSION
            && matches!(
                &record.event,
                Event::BranchModelSelected { .. }
                    | Event::BranchSummary {
                        fallback_model: Some(_),
                        ..
                    }
            )
        {
            return Err(Error::Session(format!(
                "invalid branch fallback version in record {id}"
            )));
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
            Event::Checkout { target } if is_prior_position(target) => target,
            Event::Checkout { .. } => return Err(Error::Session("invalid checkout target".into())),
            _ => id,
        };
    }
    Ok(head)
}

struct FileStore {
    file: File,
    metadata: BTreeMap<u64, serde_json::Value>,
    #[cfg(windows)]
    lock_file: File,
    #[cfg(unix)]
    parent: std::path::PathBuf,
}

impl FileStore {
    fn open(path: &Path) -> Result<Self> {
        Self::open_with_create(path, true)
    }

    fn open_existing(path: &Path) -> Result<Self> {
        Self::open_with_create(path, false)
    }

    fn open_with_create(path: &Path, create: bool) -> Result<Self> {
        let mut options = OpenOptions::new();
        options
            .read(true)
            .write(true)
            .create(create)
            .truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path)?;
        #[cfg(windows)]
        let lock_file = {
            let mut lock_path = path.as_os_str().to_os_string();
            lock_path.push(".lock");
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(lock_path)?
        };
        #[cfg(windows)]
        lock_file
            .try_lock()
            .map_err(|err| Error::Session(format!("cannot lock journal: {err}")))?;
        #[cfg(not(windows))]
        file.try_lock()
            .map_err(|err| Error::Session(format!("cannot lock journal: {err}")))?;
        Ok(Self {
            file,
            metadata: BTreeMap::new(),
            #[cfg(windows)]
            lock_file,
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
        #[cfg(windows)]
        let _ = self.lock_file.unlock();
        #[cfg(not(windows))]
        let _ = self.file.unlock();
    }
}

#[derive(Deserialize)]
struct PersistedRecord {
    #[serde(flatten)]
    record: Record,
    #[serde(
        default,
        rename = "piRsRecordMetadata",
        deserialize_with = "deserialize_present_metadata"
    )]
    metadata: Option<serde_json::Value>,
}

fn deserialize_present_metadata<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<serde_json::Value>, D::Error> {
    <serde_json::Value as Deserialize>::deserialize(deserializer).map(Some)
}

// Draft versions stored these fields inside Event::Compacted. Preserve them
// while restoring the original public enum shape.
fn legacy_compaction_metadata(value: &serde_json::Value) -> Option<serde_json::Value> {
    let event = value.get("event")?.as_object()?;
    if event.get("type")?.as_str()? != "compacted" {
        return None;
    }
    let from_extension = event
        .get("from_extension")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let details = event.get("details");
    if !from_extension && details.is_none() {
        return None;
    }
    let mut result = serde_json::json!({"fromExtension":from_extension});
    if let Some(details) = details {
        result["details"] = details.clone();
    }
    Some(result)
}

fn encode_record(record: &Record, metadata: Option<&serde_json::Value>) -> Result<Vec<u8>> {
    if let Some(metadata) = metadata {
        if metadata.is_null() {
            return Err(Error::Session("record metadata must not be null".into()));
        }
        let mut value = serde_json::to_value(record)?;
        value
            .as_object_mut()
            .expect("record serializes as an object")
            .insert("piRsRecordMetadata".into(), metadata.clone());
        Ok(serde_json::to_vec(&value)?)
    } else {
        Ok(serde_json::to_vec(record)?)
    }
}

type CompleteRecords = (Vec<Record>, BTreeMap<u64, serde_json::Value>, u64, bool);

fn read_complete_records(reader: impl Read) -> Result<CompleteRecords> {
    let mut records = Vec::new();
    let mut metadata = BTreeMap::new();
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
            return Ok((records, metadata, offset, true));
        }
        let stored: PersistedRecord = serde_json::from_slice(&line)?;
        let legacy = if stored.metadata.is_none()
            && matches!(stored.record.event, Event::Compacted { .. })
        {
            legacy_compaction_metadata(&serde_json::from_slice(&line)?)
        } else {
            None
        };
        if let Some(extra) = stored.metadata.or(legacy) {
            metadata.insert(stored.record.id, extra);
        }
        records.push(stored.record);
        offset += line.len() as u64;
        line.clear();
    }
    Ok((records, metadata, offset, false))
}

// Keep the same exclusive lock as a normal session without recovery writes.
struct ShareFileStore(FileStore);

impl SessionStore for ShareFileStore {
    fn encoded_record_limit(&self) -> Option<usize> {
        Some(MAX_RECORD_BYTES)
    }
    fn supports_record_metadata(&self) -> bool {
        true
    }
    fn load(&mut self) -> Result<Vec<Record>> {
        let (records, metadata, _, incomplete) = read_complete_records(&mut self.0.file)?;
        if incomplete || records.is_empty() {
            return Err(Error::Session(
                "journal is incomplete; reopen it before sharing".into(),
            ));
        }
        self.0.metadata = metadata;
        Ok(records)
    }

    fn append(&mut self, record: &Record) -> Result<()> {
        self.0.append(record)
    }

    fn load_record_metadata(&mut self) -> Result<BTreeMap<u64, serde_json::Value>> {
        self.0.load_record_metadata()
    }

    fn append_with_metadata(
        &mut self,
        record: &Record,
        metadata: Option<&serde_json::Value>,
    ) -> Result<()> {
        self.0.append_with_metadata(record, metadata)
    }
}

impl SessionStore for FileStore {
    fn encoded_record_limit(&self) -> Option<usize> {
        Some(MAX_RECORD_BYTES)
    }
    fn supports_record_metadata(&self) -> bool {
        true
    }
    fn load(&mut self) -> Result<Vec<Record>> {
        let (records, metadata, offset, incomplete) = read_complete_records(&mut self.file)?;
        if incomplete {
            validated_head(&records)?;
            self.file.set_len(offset)?;
            self.file.sync_all()?;
        }
        self.file.seek(SeekFrom::End(0))?;
        self.metadata = metadata;
        Ok(records)
    }

    fn append(&mut self, record: &Record) -> Result<()> {
        self.append_with_metadata(record, None)
    }

    fn load_record_metadata(&mut self) -> Result<BTreeMap<u64, serde_json::Value>> {
        Ok(self.metadata.clone())
    }

    fn append_with_metadata(
        &mut self,
        record: &Record,
        metadata: Option<&serde_json::Value>,
    ) -> Result<()> {
        let mut line = encode_record(record, metadata)?;
        line.push(b'\n');
        if line.len() > MAX_RECORD_BYTES {
            return Err(Error::Session("journal record exceeds size limit".into()));
        }
        self.file.write_all(&line)?;
        self.file.sync_data()?;
        if let Some(metadata) = metadata {
            self.metadata.insert(record.id, metadata.clone());
        }
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
            Event::ToolStarted { operation_id, .. } => {
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

fn restore_imported_compaction_prompts(records: &mut [Record]) {
    for index in 0..records.len() {
        let (previous, remaining) = records.split_at_mut(index);
        let record = &mut remaining[0];
        let Event::Compacted { system_prompt, .. } = &mut record.event else {
            continue;
        };
        if system_prompt.is_some() || record.run_id.is_some() {
            continue;
        }
        let parent = |id: u64| previous.get(id.checked_sub(1)? as usize);
        let source = record.parent.and_then(parent).and_then(|entry| {
            if matches!(entry.event, Event::ModelFinished { .. }) {
                entry.parent.and_then(parent)
            } else {
                Some(entry)
            }
        });
        let Some(Record {
            event: Event::SourceEntry { format, entry },
            timestamp,
            ..
        }) = source
        else {
            continue;
        };
        if format == "pi"
            && entry["type"] == "compaction"
            && timestamp == &record.timestamp
            && let Some(message) = entry
                .get("systemMessage")
                .filter(|message| !message.is_null())
        {
            // A legacy journal can contain source metadata that was never validated.
            *system_prompt = CompactedSystemPrompt::from_pi(message).ok().map(Box::new);
        }
    }
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
        let (text, changes, tools_added, tools_removed) = match &record.event {
            Event::SystemCheckpoint {
                content,
                sections,
                tools_added,
                tools_removed,
            } => (content, sections, tools_added, tools_removed.as_slice()),
            Event::Compacted {
                system_prompt: Some(snapshot),
                ..
            } => {
                content.clear();
                sections.clear();
                tools.clear();
                (
                    &snapshot.content,
                    &snapshot.sections,
                    &snapshot.tools,
                    &[][..],
                )
            }
            _ => continue,
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
    project_message_entries(branch, operations)
        .into_iter()
        .map(|(_, message)| message)
        .collect()
}

fn project_message_entries(
    branch: &[&Record],
    operations: Vec<(Invocation, OperationState)>,
) -> Vec<(u64, Message)> {
    let mut messages = Vec::new();
    if let Some((id, summary)) = branch.iter().rev().find_map(|record| match &record.event {
        Event::Compacted { summary, .. } => Some((record.id, summary)),
        _ => None,
    }) {
        messages.push((
            id,
            Message::User {
                content: format!("Prior conversation summary:\n{summary}"),
            },
        ));
    }
    messages.extend(conversation_entries(branch, operations));
    messages
}

/// Failed assistant records remain visible in history, but are never model input.
pub(crate) fn failed_assistant_records(path: &[&Record]) -> std::collections::HashSet<u64> {
    let failed: std::collections::HashSet<_> = path
        .iter()
        .filter_map(|record| match record.event {
            Event::ModelFinished {
                request_id,
                failure: Some(_),
                ..
            } => Some(request_id),
            _ => None,
        })
        .collect();
    path.iter()
        .filter_map(|record| match &record.event {
            Event::Assistant {
                request_id: Some(id),
                ..
            } if failed.contains(id) => Some(record.id),
            _ => None,
        })
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
    conversation_entries_since(path, operations, retain_from)
}

pub(crate) fn conversation_entries_unrepaired(
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
    conversation_entries_since_inner(path, operations, retain_from, false)
}

fn conversation_entries_since(
    path: &[&Record],
    operations: Vec<(Invocation, OperationState)>,
    retain_from: u64,
) -> Vec<(u64, Message)> {
    conversation_entries_since_inner(path, operations, retain_from, true)
}

fn conversation_entries_since_inner(
    path: &[&Record],
    operations: Vec<(Invocation, OperationState)>,
    retain_from: u64,
    repair: bool,
) -> Vec<(u64, Message)> {
    let repair_operations = if !repair {
        let assistants: BTreeSet<_> = path
            .iter()
            .filter_map(|record| {
                matches!(record.event, Event::Assistant { .. }).then_some(record.id)
            })
            .collect();
        path.iter().any(|record| matches!(&record.event, Event::ContextEdited { target, replacement: Some(_), .. } if assistants.contains(target))).then(|| operations.clone())
    } else {
        None
    };
    let invocations: BTreeMap<_, _> = path
        .iter()
        .flat_map(|record| match &record.event {
            Event::Assistant { invocations, .. } => invocations.as_slice(),
            _ => &[],
        })
        .map(|invocation| (invocation.operation_id, &invocation.call.id))
        .collect();
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
    let metadata_by_request: BTreeMap<_, _> = path
        .iter()
        .filter_map(|record| match &record.event {
            Event::ModelFinished {
                request_id,
                metadata,
                failure: None,
                ..
            } => Some((*request_id, metadata)),
            _ => None,
        })
        .collect();
    let replay_by_request: BTreeMap<_, _> = path
        .iter()
        .filter_map(|record| match &record.event {
            Event::ModelFinished {
                request_id,
                metadata,
                failure: None,
                ..
            } => metadata.replay.as_ref().map(|replay| (*request_id, replay)),
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
    // Replacement blocks may introduce calls with no corresponding result.
    // Compare those calls with the actual repaired model projection, while
    // keeping an otherwise empty assistant for its thinking and usage.
    let replacement_calls: BTreeMap<u64, BTreeSet<String>> = if !repair {
        let mut allowed: BTreeMap<_, BTreeSet<String>> = path
            .iter()
            .filter_map(|record| {
                (matches!(record.event, Event::Assistant { .. })
                    && matches!(edits.get(&record.id), Some(Some(_))))
                .then_some((record.id, BTreeSet::new()))
            })
            .collect();
        if !allowed.is_empty() {
            for (source, message) in conversation_entries_since_inner(
                path,
                repair_operations.expect("unrepaired projection"),
                retain_from,
                true,
            ) {
                if let (
                    Some(calls),
                    Message::Assistant {
                        calls: repaired, ..
                    },
                ) = (allowed.get_mut(&source), message)
                {
                    calls.extend(repaired.into_iter().map(|call| call.id));
                }
            }
        }
        allowed
    } else {
        BTreeMap::new()
    };
    // An explicit edit to an assistant can remove its tool calls. Keep
    // independently retained results, but exclude results orphaned by that edit.
    let removed_tool_sources: BTreeSet<_> = if repair {
        BTreeSet::new()
    } else {
        path.iter().flat_map(|record| {
            let Event::Assistant { invocations, .. } = &record.event else { return Vec::new(); };
            let Some(replacement) = edits.get(&record.id) else { return Vec::new(); };
            invocations.iter().filter_map(|invocation| {
                let retained = matches!(replacement, Some(crate::ContextContent::Blocks(blocks)) if blocks.iter().any(|block| matches!(block, crate::ContentBlock::ToolCall { id, .. } if id == &invocation.call.id)));
                (!retained).then(|| result_sources.get(&invocation.operation_id).copied()).flatten()
            }).collect::<Vec<_>>()
        }).collect()
    };
    // Preserve assistant messages for estimation (including thinking-only
    // messages), but mirror the repair that drops calls whose results were
    // explicitly omitted from context.
    let omitted_calls: BTreeMap<_, BTreeSet<_>> = if repair {
        BTreeMap::new()
    } else {
        path.iter()
            .filter_map(|record| {
                let Event::Assistant { invocations, .. } = &record.event else {
                    return None;
                };
                let calls: BTreeSet<_> = invocations
                    .iter()
                    .filter_map(|invocation| {
                        let result = result_sources.get(&invocation.operation_id)?;
                        matches!(edits.get(result), Some(None)).then(|| invocation.call.id.clone())
                    })
                    .collect();
                (!calls.is_empty()).then_some((record.id, calls))
            })
            .collect()
    };
    let unattributed: BTreeMap<_, _> = path
        .iter()
        .filter_map(|record| match &record.event {
            Event::ImportedUnattributedToolResult {
                call_id,
                tool_name,
                is_error,
                ..
            } => Some((record.id, (call_id, tool_name, is_error))),
            _ => None,
        })
        .collect();
    let mut messages = Vec::new();
    for record in path.iter().filter(|record| record.id >= retain_from) {
        match &record.event {
            Event::CustomMessage { message } | Event::CustomMessageDelivered { message, .. } => messages.push((record.id, project_content(Message::User { content: String::new() }, &message.content))),
            Event::User { content, .. } => messages.push((
                record.id,
                Message::User {
                    content: content.clone(),
                },
            )),
            Event::ImportedUnattributedToolResult { content, .. } => messages.push((
                record.id,
                project_content(Message::User { content: String::new() }, content),
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
            Event::BranchSummary { summary, .. } => messages.push((
                record.id,
                Message::User {
                    content: format!("The following is a summary of a branch that this conversation came back from:\n\n<summary>\n{summary}</summary>"),
                },
            )),
            Event::CompactionSummaryMessage { summary, .. } => messages.push((
                record.id,
                Message::User {
                    content: format!("The conversation history before this point was compacted into the following summary:\n\n<summary>\n{summary}\n</summary>"),
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
                request_id,
                ..
            } => {
                messages.push((
                    record.id,
                    Message::Assistant {
                        content: content.clone(),
                        replay: request_id.and_then(|id| replay_by_request.get(&id).map(|replay| (*replay).clone())),
                        calls: invocations
                            .iter()
                            .map(|inv| {
                                let mut call = inv.call.clone();
                                if !serde_json::from_str::<serde_json::Value>(&call.arguments)
                                    .is_ok_and(|value| value.is_object()) {
                                    call.arguments = crate::parse_tool_arguments(&call.arguments)
                                        .map_or_else(|_| "{}".into(), |value| value.to_string());
                                }
                                call
                            })
                            .collect(),
                    },
                ));
                for inv in invocations.iter().filter(|_| repair) {
                    if let Some(outcome) = results.get(&inv.operation_id) {
                        messages.push((
                            *result_sources.get(&inv.operation_id).unwrap_or(&record.id),
                            Message::Tool {
                                call_id: inv.call.id.clone(),
                                content: outcome.tool_content(),
                                is_error: outcome.is_error(),
                                is_structured: Some(outcome.is_structured()),
                            },
                        ));
                    }
                }
            }
            Event::ToolFinished { operation_id, .. } | Event::ToolReconciled { operation_id, .. } if !repair => {
                if result_sources.get(operation_id) == Some(&record.id)
                    && let (Some(call_id), Some(outcome)) = (invocations.get(operation_id), results.get(operation_id)) {
                    messages.push((record.id, Message::Tool {
                        call_id: (*call_id).clone(), content: outcome.tool_content(),
                        is_error: outcome.is_error(), is_structured: Some(outcome.is_structured()),
                    }));
                }
            }
            _ => {}
        }
    }
    let mut messages: Vec<_> = messages
        .into_iter()
        .filter(|(source, _)| !removed_tool_sources.contains(source))
        .filter_map(|(source, message)| match edits.get(&source) {
            Some(Some(replacement)) => Some((source, project_content(message, replacement))),
            Some(None) => None,
            None => Some((source, message)),
        })
        .map(|(source, mut message)| {
            if let Some((call_id, tool_name, is_error)) = unattributed.get(&source) {
                let status = if **is_error { "error" } else { "success" };
                let label = format!(
                    "Historical unattributed tool result ({tool_name}, call {call_id}, {status}):\n"
                );
                match &mut message {
                    Message::User { content } | Message::UserWithImages { content, .. } => {
                        content.insert_str(0, &label)
                    }
                    _ => {}
                }
            }
            if let (Some(omitted), Message::Assistant { calls, replay, .. }) =
                (omitted_calls.get(&source), &mut message)
            {
                let count = calls.len();
                calls.retain(|call| !omitted.contains(&call.id));
                if calls.len() != count {
                    *replay = None;
                }
            }
            if let (Some(allowed), Message::Assistant { calls, replay, .. }) =
                (replacement_calls.get(&source), &mut message)
            {
                let count = calls.len();
                calls.retain(|call| allowed.contains(&call.id));
                if calls.len() != count {
                    *replay = None;
                }
            }
            (source, message)
        })
        .collect();
    if repair && !edits.is_empty() {
        let thinking_sources: BTreeSet<_> = messages
            .iter()
            .filter_map(|(source, message)| {
                let (
                    Some(Some(replacement @ crate::ContextContent::Blocks(blocks))),
                    Message::Assistant { calls, .. },
                ) = (edits.get(source), message)
                else {
                    return None;
                };
                if !blocks
                    .iter()
                    .any(|b| matches!(b, crate::ContentBlock::Thinking { .. }))
                {
                    return None;
                }
                let Event::Assistant {
                    request_id: Some(request_id),
                    ..
                } = &path.iter().find(|r| r.id == *source)?.event
                else {
                    return None;
                };
                crate::context_replay::replacement_replay(
                    replacement,
                    metadata_by_request.get(request_id)?,
                    calls,
                )
                .map(|_| *source)
            })
            .collect();
        messages = repair_tool_pairs(messages, &thinking_sources);
    }
    for (source, message) in &mut messages {
        if let (Some(Some(replacement)), Message::Assistant { calls, replay, .. }) =
            (edits.get(source), message)
            && let Some(Event::Assistant {
                request_id: Some(request_id),
                ..
            }) = path.iter().find(|r| r.id == *source).map(|r| &r.event)
            && let Some(metadata) = metadata_by_request.get(request_id)
        {
            *replay = crate::context_replay::replacement_replay(replacement, metadata, calls);
        }
    }
    messages
}

fn repair_tool_pairs(
    messages: Vec<(u64, Message)>,
    thinking_sources: &BTreeSet<u64>,
) -> Vec<(u64, Message)> {
    let mut projected = Vec::new();
    let mut messages = messages.into_iter().peekable();
    while let Some((source, message)) = messages.next() {
        match message {
            Message::Assistant {
                content,
                mut calls,
                mut replay,
            } => {
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
                let original_calls = calls.len();
                calls.retain(|call| result_ids.contains(call.id.as_str()));
                if calls.len() != original_calls {
                    replay = None;
                }
                let call_ids: std::collections::BTreeSet<_> =
                    calls.iter().map(|call| call.id.clone()).collect();
                if !content.is_empty() || !calls.is_empty() || thinking_sources.contains(&source) {
                    projected.push((
                        source,
                        Message::Assistant {
                            content,
                            calls,
                            replay,
                        },
                    ));
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
        ContextContent::Text(text) => vec![ContentBlock::Text {
            text: text.clone(),
            text_signature: None,
        }],
        ContextContent::Blocks(blocks) => blocks.clone(),
    };
    let text = blocks
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text, .. } => Some(text.as_str()),
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
            replay: None,
            calls: blocks
                .into_iter()
                .filter_map(|block| match block {
                    ContentBlock::ToolCall {
                        id,
                        name,
                        arguments,
                        ..
                    } => Some(crate::ToolCall {
                        id,
                        name,
                        arguments: arguments.to_string(),
                    }),
                    _ => None,
                })
                .collect(),
        },
        Message::Tool {
            call_id, is_error, ..
        } => Message::Tool {
            call_id,
            is_error,
            is_structured: Some(!images.is_empty()),
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
mod record_metadata_tests {
    use super::*;

    #[test]
    fn legacy_compaction_fields_survive_journal_and_serialized_import() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append(Event::Compacted {
                summary: "summary".into(),
                system_prompt: None,
                retain_from: None,
                tokens_before: None,
                states: Default::default(),
                usage: None,
                usage_metadata: None,
            })
            .unwrap();
        let mut values: Vec<_> = session
            .records()
            .iter()
            .map(|record| serde_json::to_value(record).unwrap())
            .collect();
        values[1]["event"]["from_extension"] = true.into();
        values[1]["event"]["details"] = serde_json::Value::Null;
        let expected = serde_json::json!({"fromExtension":true,"details":null});
        let imported = Session::from_serialized_records(values.clone()).unwrap();
        assert_eq!(imported.record_metadata(2), Some(&expected));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.jsonl");
        let bytes = values
            .iter()
            .map(|value| serde_json::to_string(value).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        std::fs::write(&path, bytes).unwrap();
        let reopened = Session::open(&path).unwrap();
        assert_eq!(reopened.record_metadata(2), Some(&expected));
    }

    #[test]
    fn fork_remaps_overflow_retention_boundary() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append(Event::User {
                content: "base".into(),
                input_id: None,
            })
            .unwrap();
        let base = session.head();
        session
            .append(Event::User {
                content: "abandoned".into(),
                input_id: None,
            })
            .unwrap();
        session.checkout(base).unwrap();
        session
            .append(Event::User {
                content: "retained".into(),
                input_id: None,
            })
            .unwrap();
        let retained = session.head();
        session
            .append(Event::SourceEntry {
                format: "pi-rs:overflow-retention-v1".into(),
                entry: serde_json::json!({"boundary":retained,"keepTurns":1,"splitTurn":false}),
            })
            .unwrap();
        let fork = session.fork_session(session.head()).unwrap();
        let (marker, boundary) = fork
            .records()
            .iter()
            .find_map(|record| match &record.event {
                Event::SourceEntry { format, entry } if format == "pi-rs:overflow-retention-v1" => {
                    Some((record, entry["boundary"].as_u64().unwrap()))
                }
                _ => None,
            })
            .unwrap();
        assert_ne!(boundary, retained);
        assert!(boundary < marker.id);
        assert!(
            matches!(&fork.records()[(boundary - 1) as usize].event, Event::User { content, .. } if content == "retained")
        );
    }

    #[test]
    fn fork_invalidates_overflow_boundary_on_another_branch() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append(Event::User {
                content: "base".into(),
                input_id: None,
            })
            .unwrap();
        let base = session.head();
        session
            .append(Event::User {
                content: "abandoned".into(),
                input_id: None,
            })
            .unwrap();
        let abandoned = session.head();
        session.checkout(base).unwrap();
        session
            .append(Event::SourceEntry {
                format: "pi-rs:overflow-retention-v1".into(),
                entry: serde_json::json!({"boundary":abandoned,"keepTurns":1,"splitTurn":false}),
            })
            .unwrap();
        let fork = session.fork_session(session.head()).unwrap();
        let entry = fork
            .records()
            .iter()
            .find_map(|record| match &record.event {
                Event::SourceEntry { format, entry } if format == "pi-rs:overflow-retention-v1" => {
                    Some(entry)
                }
                _ => None,
            })
            .unwrap();
        assert!(entry.get("boundary").is_none());
    }

    #[test]
    fn metadata_envelope_rejects_duplicate_record_fields() {
        let record = Session::with_store(MemoryStore::default())
            .unwrap()
            .records()[0]
            .clone();
        let valid = serde_json::to_string(&record).unwrap();
        assert!(serde_json::from_str::<PersistedRecord>(&valid).is_ok());
        let duplicate = valid.replacen("\"id\":1", "\"id\":1,\"id\":2", 1);
        assert_ne!(duplicate, valid);
        assert!(serde_json::from_str::<PersistedRecord>(&duplicate).is_err());
    }

    #[test]
    fn null_record_metadata_is_rejected_on_restore() {
        let session = Session::with_store(MemoryStore::default()).unwrap();
        let mut value = serde_json::to_value(&session.records()[0]).unwrap();
        value["piRsRecordMetadata"] = serde_json::Value::Null;
        assert!(Session::from_serialized_records(vec![value]).is_err());
        assert!(
            Session::from_records_with_metadata(
                session.records().to_vec(),
                [(1, serde_json::Value::Null)].into_iter().collect()
            )
            .is_err()
        );
    }

    #[test]
    fn file_metadata_is_atomic_and_survives_reopen_and_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("metadata.jsonl");
        let metadata = serde_json::json!({"details": {"origin": "extension"}});
        let mut session = Session::open(&path).unwrap();
        let id = session
            .append_with_record_metadata(
                Event::SessionInfo {
                    name: None,
                    cwd: None,
                },
                metadata.clone(),
            )
            .unwrap()
            .id;
        assert_eq!(session.record_metadata(id), Some(&metadata));
        let snapshot = Session::snapshot(&path).unwrap();
        assert_eq!(snapshot.record_metadata(id), Some(&metadata));
        let export = directory.path().join("export.jsonl");
        session.export_jsonl(&export).unwrap();
        let exported = Session::snapshot(&export).unwrap();
        assert_eq!(exported.record_metadata(id), Some(&metadata));
        let values = std::fs::read_to_string(&export)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let imported = Session::from_serialized_records(values).unwrap();
        assert_eq!(imported.record_metadata(id), Some(&metadata));
        let fork_path = directory.path().join("fork.jsonl");
        let fork = session.fork_to(id, &fork_path).unwrap();
        assert_eq!(fork.record_metadata(id), Some(&metadata));
        let in_memory_fork = session.fork_session_after_handoff(id).unwrap();
        assert_eq!(in_memory_fork.record_metadata(id), Some(&metadata));
        let public_fork = session.fork_session(id).unwrap();
        assert_eq!(public_fork.record_metadata(id), Some(&metadata));
        drop(session);
        let share = Session::open_for_share(&path).unwrap();
        assert_eq!(share.record_metadata(id), Some(&metadata));
        let share_export = directory.path().join("share-export.jsonl");
        share.export_jsonl(&share_export).unwrap();
        assert_eq!(
            Session::snapshot(&share_export)
                .unwrap()
                .record_metadata(id),
            Some(&metadata)
        );
        drop(share);
        let reopened = Session::open(&path).unwrap();
        assert_eq!(reopened.record_metadata(id), Some(&metadata));
    }

    #[test]
    fn legacy_store_rejects_metadata_without_appending() {
        struct Legacy(MemoryStore);
        impl SessionStore for Legacy {
            fn load(&mut self) -> Result<Vec<Record>> {
                self.0.load()
            }
            fn append(&mut self, record: &Record) -> Result<()> {
                self.0.append(record)
            }
        }
        let mut session = Session::with_store(Legacy(MemoryStore::default())).unwrap();
        let length = session.records().len();
        let result = session.append_with_record_metadata(
            Event::SessionInfo {
                name: None,
                cwd: None,
            },
            serde_json::json!({"x": 1}),
        );
        assert!(result.is_err());
        assert_eq!(session.records().len(), length);
        let record = session
            .append(Event::SessionInfo {
                name: Some("still writable".into()),
                cwd: None,
            })
            .unwrap();
        assert_eq!(record.id, length as u64 + 1);
        assert_eq!(session.records().len(), length + 1);
    }

    #[test]
    fn oversized_metadata_rejection_does_not_poison_file_session() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oversized-metadata.jsonl");
        let mut session = Session::open(&path).unwrap();
        let length = session.records().len();
        let result = session.append_with_record_metadata(
            Event::SessionInfo {
                name: None,
                cwd: None,
            },
            serde_json::json!({"value": "x".repeat(MAX_RECORD_BYTES)}),
        );
        assert!(result.is_err());
        assert_eq!(session.records().len(), length);
        session
            .append(Event::User {
                content: "still writable".into(),
                input_id: None,
            })
            .unwrap();
        drop(session);
        let reopened = Session::open(&path).unwrap();
        assert!(reopened.records().iter().any(|record| matches!(&record.event, Event::User { content, .. } if content == "still writable")));
    }
}

#[cfg(test)]
mod session_metadata_tests {
    use super::*;

    #[test]
    fn inert_full_history_preserves_branches_and_record_metadata() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append(Event::User {
                content: "root".into(),
                input_id: None,
            })
            .unwrap();
        let root = session.head();
        session
            .append_with_record_metadata(
                Event::User {
                    content: "sibling".into(),
                    input_id: None,
                },
                serde_json::json!({"fixture":"retained"}),
            )
            .unwrap();
        let sibling = session.head();
        session.checkout(root).unwrap();
        session
            .append(Event::User {
                content: "active".into(),
                input_id: None,
            })
            .unwrap();
        let active = session.head();
        session.checkout(sibling).unwrap();
        let original_id = session.session_id();
        let mut fork = session.fork_inert_full_history().unwrap().unwrap();
        assert_ne!(fork.session_id(), original_id);
        assert_eq!(fork.head(), sibling);
        assert_eq!(
            fork.record_metadata(sibling),
            Some(&serde_json::json!({"fixture":"retained"}))
        );
        fork.checkout(active).unwrap();
        assert!(
            fork.current_messages()
                .iter()
                .any(|m| matches!(m, Message::User { content } if content == "active"))
        );
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fork.jsonl");
        fork.export_jsonl(&path).unwrap();
        assert_eq!(
            Session::open(&path).unwrap().record_metadata(sibling),
            Some(&serde_json::json!({"fixture":"retained"}))
        );
    }

    #[test]
    fn inert_full_history_excludes_recoverable_and_queued_work() {
        let message = crate::CustomMessage {
            custom_type: "queue".into(),
            content: crate::ContextContent::Text("text".into()),
            display: false,
            details: None,
        };
        let events = [
            Event::Run {
                state: RunState::Running,
                initial: None,
            },
            Event::TreeMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: Uuid::new_v4(),
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                    message,
                },
            },
            Event::Assistant {
                content: String::new(),
                invocations: vec![crate::Invocation {
                    operation_id: Uuid::new_v4(),
                    call: crate::ToolCall {
                        id: "x".into(),
                        name: "read".into(),
                        arguments: "{}".into(),
                    },
                }],
                usage: None,
                request_id: None,
            },
            Event::ModelFinished {
                request_id: Uuid::new_v4(),
                metadata: Default::default(),
                usage: crate::UsageReport::Unconfirmed,
                failure: None,
                partial: String::new(),
                reply: Some(RecordedReply {
                    content: "reply".into(),
                    invocations: Vec::new(),
                    advertised_tools: Vec::new(),
                    max_output_tokens: 16,
                }),
            },
        ];
        for event in events {
            let mut session = Session::with_store(MemoryStore::default()).unwrap();
            session.append(event).unwrap();
            assert!(session.fork_inert_full_history().unwrap().is_none());
        }
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session.set_run_id(Some(Uuid::new_v4()));
        session
            .append(Event::Assistant {
                content: "partial".into(),
                invocations: Vec::new(),
                usage: None,
                request_id: None,
            })
            .unwrap();
        assert!(session.fork_inert_full_history().unwrap().is_none());
    }

    #[test]
    fn fork_does_not_resurrect_tree_message_consumed_on_another_branch() {
        for delivery in [
            crate::CustomMessageDelivery::TriggerTurn,
            crate::CustomMessageDelivery::NextTurn,
        ] {
            let mut session = Session::with_store(MemoryStore::default()).unwrap();
            let id = Uuid::new_v4();
            let message = crate::CustomMessage {
                custom_type: "tree".into(),
                content: crate::ContextContent::Text("once".into()),
                display: false,
                details: None,
            };
            session
                .append(Event::TreeMessageQueued {
                    input: crate::QueuedCustomMessage {
                        id,
                        message: message.clone(),
                        delivery,
                    },
                })
                .unwrap();
            let source = session.head();
            session.checkout(1).unwrap();
            session
                .append(Event::CustomMessageDelivered { id, message })
                .unwrap();
            session.checkout(source).unwrap();
            assert!(session.queued_custom_messages().is_empty());
            let fork = Session::from_records(session.fork_records(source).unwrap()).unwrap();
            assert!(fork.queued_custom_messages().is_empty());
            assert_eq!(fork.pending_custom_trigger(), None);
        }
    }

    #[test]
    fn tree_trigger_restore_rejects_an_unsettled_current_branch() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let trigger = Uuid::new_v4();
        session
            .append(Event::TreeMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: trigger,
                    message: crate::CustomMessage {
                        custom_type: "tree".into(),
                        content: crate::ContextContent::Text("trigger".into()),
                        display: false,
                        details: None,
                    },
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                },
            })
            .unwrap();
        session.set_run_id(Some(trigger));
        session
            .append(Event::Run {
                state: RunState::Running,
                initial: None,
            })
            .unwrap();
        session.set_run_id(None);
        session.checkout(1).unwrap();
        let operation_id = Uuid::new_v4();
        session
            .append(Event::Assistant {
                content: String::new(),
                invocations: vec![crate::Invocation {
                    operation_id,
                    call: crate::ToolCall {
                        id: "tool".into(),
                        name: "tool".into(),
                        arguments: "{}".into(),
                    },
                }],
                usage: None,
                request_id: None,
            })
            .unwrap();
        session
            .append(Event::ToolStarted {
                operation_id,
                tool_execution: None,
            })
            .unwrap();
        let head = session.head();
        assert!(
            matches!(session.begin_tree_trigger_handoff(trigger), Err(Error::Reconciliation(id)) if id == operation_id)
        );
        assert_eq!(session.head(), head);
        assert_eq!(session.pending_custom_trigger(), Some(trigger));
    }

    #[test]
    fn tree_trigger_handoff_can_checkout_a_different_named_provider() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .select_provider_model(Some("provider-a"), "model-a", None)
            .unwrap();
        let trigger = Uuid::new_v4();
        session
            .append(Event::TreeMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: trigger,
                    message: crate::CustomMessage {
                        custom_type: "tree".into(),
                        content: crate::ContextContent::Text("trigger".into()),
                        display: false,
                        details: None,
                    },
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                },
            })
            .unwrap();
        session.set_run_id(Some(trigger));
        session
            .append(Event::Run {
                state: RunState::Running,
                initial: None,
            })
            .unwrap();
        session.set_run_id(None);
        session.checkout(1).unwrap();
        session
            .select_provider_model(Some("provider-b"), "model-b", None)
            .unwrap();
        let before = session.head();
        let (return_to, _) = session
            .begin_tree_trigger_handoff(trigger)
            .unwrap()
            .unwrap();
        assert_eq!(return_to, before);
        assert_eq!(session.model_provider(), Some("provider-a"));
        session.finish_tree_trigger_handoff(return_to).unwrap();
        assert_eq!(session.model_provider(), Some("provider-b"));
        assert_eq!(session.pending_custom_trigger(), Some(trigger));
    }

    #[test]
    fn opening_a_host_bound_journal_does_not_repair_its_handoff_without_the_host() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("managed-handoff.jsonl");
        let mut session = Session::open(&path).unwrap();
        session.bind_host("tenant").unwrap();
        session
            .append_user_with_images("A".into(), vec![], None)
            .unwrap();
        let run_id = Uuid::new_v4();
        let message = crate::CustomMessage {
            custom_type: "tree".into(),
            content: crate::ContextContent::Text("trigger".into()),
            display: false,
            details: None,
        };
        session
            .append(Event::TreeMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: run_id,
                    message,
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                },
            })
            .unwrap();
        session.set_run_id(Some(run_id));
        session
            .append(Event::Run {
                state: RunState::Running,
                initial: None,
            })
            .unwrap();
        session.set_run_id(None);
        session.checkout(2).unwrap();
        session
            .append_user_with_images("B".into(), vec![], None)
            .unwrap();
        let branch_b = session.head();
        session.begin_tree_trigger_handoff(run_id).unwrap().unwrap();
        let temporary_head = session.head();
        let count = session.records().len();
        drop(session);
        let mut reopened = Session::open(&path).unwrap();
        assert_eq!(reopened.head(), temporary_head);
        assert_eq!(reopened.records().len(), count);
        reopened.recover_tree_trigger_handoff().unwrap().unwrap();
        assert_eq!(reopened.head(), branch_b);
    }

    #[test]
    fn share_open_rejects_pending_handoff_without_repair() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("handoff.jsonl");
        let mut session = Session::open(&path).unwrap();
        session
            .append_user_with_images("A".into(), vec![], None)
            .unwrap();
        let run_id = Uuid::new_v4();
        session
            .append(Event::TreeMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: run_id,
                    message: crate::CustomMessage {
                        custom_type: "tree".into(),
                        content: crate::ContextContent::Text("trigger".into()),
                        display: false,
                        details: None,
                    },
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                },
            })
            .unwrap();
        session.set_run_id(Some(run_id));
        session
            .append(Event::Run {
                state: RunState::Running,
                initial: None,
            })
            .unwrap();
        session.set_run_id(None);
        session.checkout(2).unwrap();
        session
            .append_user_with_images("B".into(), vec![], None)
            .unwrap();
        let original_head = session.head();
        session.begin_tree_trigger_handoff(run_id).unwrap().unwrap();
        drop(session);
        let original = std::fs::read(&path).unwrap();
        assert!(Session::open_for_share(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert_eq!(Session::open(&path).unwrap().head(), original_head);
    }

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
        let fork = Session::with_store(MemoryStore {
            records,
            metadata: BTreeMap::new(),
        })
        .unwrap();
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
    fn abandoned_branch_excludes_failed_assistant_when_failure_precedes_boundary() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let request_id = Uuid::new_v4();
        let target = session
            .append(Event::ModelFinished {
                request_id,
                metadata: ModelMetadata::default(),
                usage: UsageReport::Unconfirmed,
                failure: Some(ModelFailure::Cancelled),
                partial: String::new(),
                reply: None,
            })
            .unwrap()
            .id;
        session
            .append(Event::Assistant {
                content: "failed partial".into(),
                invocations: vec![],
                usage: None,
                request_id: Some(request_id),
            })
            .unwrap();
        session
            .append(Event::User {
                content: "keep this".into(),
                input_id: None,
            })
            .unwrap();
        let abandoned = session.abandoned_branch(target);
        assert_eq!(
            abandoned.messages,
            vec![Message::User {
                content: "keep this".into()
            }]
        );
    }

    #[test]
    fn branch_summary_replays_and_forks_at_the_new_position() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        for content in ["shared context", "abandoned context"] {
            session
                .append(Event::User {
                    content: content.into(),
                    input_id: None,
                })
                .unwrap();
        }
        assert_eq!(
            session.abandoned_branch(2).messages,
            [Message::User {
                content: "abandoned context".into()
            }]
        );
        let usage = Usage {
            input_tokens: 40,
            output_tokens: 5,
        };
        session
            .append(Event::ModelFinished {
                request_id: Uuid::new_v4(),
                metadata: ModelMetadata {
                    cached_input_tokens: Some(10),
                    cost_usd: serde_json::Number::from_f64(0.4),
                    reasoning: Some("model reasoning stays on the old branch".into()),
                    ..Default::default()
                },
                usage: UsageReport::Measured { usage },
                failure: None,
                partial: String::new(),
                reply: None,
            })
            .unwrap();
        session
            .append(Event::BranchSummary {
                fallback_model: None,
                summary: "decision from the old branch".into(),
                extension_summary: None,
                from: Some(3),
                target: Some(2),
                usage: Some(usage),
                usage_metadata: None,
                read_files: vec!["source.txt".into()],
                modified_files: vec!["result.txt".into()],
            })
            .unwrap();
        let restored = Session::from_records(session.records().to_vec()).unwrap();
        let fork = Session::from_records(restored.fork_records(restored.head()).unwrap()).unwrap();
        assert_eq!(
            restored
                .branch()
                .iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            [1, 2, 5]
        );
        assert_eq!(restored.current_messages(), fork.current_messages());
        assert_eq!(restored.statistics().cost, Some(0.4));
        assert_eq!(fork.statistics().cost, Some(0.4));
        assert_eq!(fork.statistics().tokens.cache_read, 10);
        let (_, billing) = fork
            .records()
            .last()
            .unwrap()
            .summary_usage(fork.records())
            .unwrap();
        assert!(billing.unwrap().metadata.reasoning.is_none());
        assert!(
            matches!(&restored.current_messages()[1], Message::User { content } if content.contains("decision from the old branch"))
        );
        assert_eq!(restored.records()[2].id, 3);
        let abandoned = restored.abandoned_branch(2);
        assert!(abandoned.read_files.contains("source.txt"));
        assert!(abandoned.modified_files.contains("result.txt"));
        assert!(matches!(
            &fork.records()[2].event,
            Event::BranchSummary {
                from: None,
                target: None,
                ..
            }
        ));
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
    fn compacted_system_prompt_replaces_earlier_deltas_on_its_branch() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let read = crate::ToolSpec {
            name: "read".into(),
            description: "Current read tool".into(),
            parameters: json!({}),
        };
        let old = crate::ToolSpec {
            name: "old".into(),
            ..read.clone()
        };
        session
            .append(Event::SystemCheckpoint {
                content: "old base".into(),
                sections: vec![("old".into(), Some("old section".into()))],
                tools_added: vec![old.clone()],
                tools_removed: vec![],
            })
            .unwrap();
        let previous = session.head();
        session
            .append(Event::Compacted {
                summary: "summary".into(),
                system_prompt: Some(Box::new(CompactedSystemPrompt {
                    content: "snapshot".into(),
                    sections: vec![("rules".into(), Some("saved".into()))],
                    tools: vec![crate::ToolSpec {
                        description: "Historical read tool".into(),
                        ..read.clone()
                    }],
                })),
                retain_from: Some(previous),
                tokens_before: None,
                states: Default::default(),
                usage: None,
                usage_metadata: None,
            })
            .unwrap();
        session
            .append(Event::SystemCheckpoint {
                content: "new instructions".into(),
                sections: vec![("rules".into(), Some("updated".into()))],
                tools_added: vec![],
                tools_removed: vec![],
            })
            .unwrap();
        let reopened = Session::from_records(session.records().to_vec()).unwrap();
        assert_eq!(
            system_context(
                &reopened.branch(),
                "fallback",
                vec![read.clone(), old.clone()]
            ),
            (
                "snapshot\n\nnew instructions\n\nupdated".into(),
                vec![read.clone()]
            )
        );
        session.checkout(previous).unwrap();
        assert_eq!(
            system_context(&session.branch(), "fallback", vec![read, old.clone()]),
            ("old base\n\nold section".into(), vec![old])
        );
    }

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

fn is_ecmascript_whitespace(character: char) -> bool {
    matches!(character,
        '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' |
        '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' |
        '\u{205f}' | '\u{3000}' | '\u{feff}'
    )
}

/// Normalize a display name using Pi's line-break and ECMAScript whitespace rules.
pub fn sanitize_session_name(name: &str) -> String {
    name.split(['\r', '\n'])
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
        .trim_matches(is_ecmascript_whitespace)
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
    fn fork_resolves_off_branch_label_changes_and_timestamps() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append_user_with_images("first".into(), vec![], None)
            .unwrap();
        let first = session.head();
        session
            .append_user_with_images("second".into(), vec![], None)
            .unwrap();
        let second = session.head();
        session.set_label(first, Some("stale".into())).unwrap();
        let target = session.head();
        session.set_label(first, None).unwrap();
        session.set_label(second, Some("current".into())).unwrap();
        let timestamp = session.records().last().unwrap().timestamp.clone();
        let fork = session.fork_session(target).unwrap();
        assert_eq!(fork.labels().len(), 1);
        assert_eq!(
            fork.labels().get(&second).map(String::as_str),
            Some("current")
        );
        assert_eq!(fork.label_timestamps().get(&second), timestamp.as_ref());
    }

    #[test]
    fn fork_keeps_compaction_boundary_at_immediate_parent_user() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append_user_with_images("retained".into(), vec![], None)
            .unwrap();
        let user = session.head();
        session
            .append(Event::Compacted {
                summary: "summary".into(),
                system_prompt: None,
                retain_from: Some(user),
                tokens_before: None,
                states: Default::default(),
                usage: None,
                usage_metadata: None,
            })
            .unwrap();
        let fork = session.fork_session(session.head()).unwrap();
        assert!(
            fork.current_messages().iter().any(
                |message| matches!(message, Message::User { content } if content == "retained")
            )
        );
        assert!(fork.records().iter().any(|record|
            matches!(record.event, Event::Compacted { retain_from: Some(id), .. } if id == user)));
    }

    #[test]
    fn fork_rejects_malformed_import_conversion_ranges() {
        let base = Session::with_store(MemoryStore::default()).unwrap();
        let mut records = base.records().to_vec();
        let mut source = records[0].clone();
        source.id = 2;
        source.parent = Some(1);
        source.timestamp = None;
        source.event = Event::SourceEntry {
            format: "pi".into(),
            entry: serde_json::json!({"type":"custom","id":"a","customType":"x"}),
        };
        records.push(source.clone());
        for count in [serde_json::json!(u64::MAX), serde_json::json!("invalid")] {
            let session = Session::from_records_with_metadata(
                records.clone(),
                [(
                    2,
                    serde_json::json!({"pi_rs_import_conversion_count_v1":count}),
                )]
                .into_iter()
                .collect(),
            )
            .unwrap();
            assert!(
                session
                    .fork_records(2)
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("provenance")
            );
        }
        for nested in [
            Event::SourceEntry {
                format: "pi".into(),
                entry: serde_json::json!({"type":"custom","id":"b","customType":"y"}),
            },
            Event::Checkout { target: 1 },
        ] {
            let mut child = source.clone();
            child.id = 3;
            child.parent = Some(2);
            child.event = nested;
            let mut records = records.clone();
            records.push(child);
            let session = Session::from_records_with_metadata(
                records,
                [
                    (2, serde_json::json!({"pi_rs_import_conversion_count_v1":1})),
                    (3, serde_json::json!({"pi_rs_import_conversion_count_v1":0})),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();
            assert!(
                session
                    .fork_records(2)
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("provenance")
            );
        }
    }

    #[test]
    fn fork_rechains_labels_in_reinsert_order_and_keeps_slot_metadata() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append_user_with_images("one".into(), vec![], None)
            .unwrap();
        let first = session.head();
        session
            .append_user_with_images("two".into(), vec![], None)
            .unwrap();
        let second = session.head();
        session
            .append_with_record_metadata(
                Event::Label {
                    target: first,
                    label: Some("old".into()),
                },
                serde_json::json!({"note":"preserve"}),
            )
            .unwrap();
        let old_label = session.head();
        session.set_label(second, Some("second".into())).unwrap();
        session.set_label(first, None).unwrap();
        session.set_label(first, Some("first".into())).unwrap();
        let target = session.head();
        for fork in [
            session.fork_session(target).unwrap(),
            session.fork_session_after_handoff(target).unwrap(),
        ] {
            let labels: Vec<_> = fork
                .records()
                .iter()
                .filter_map(|record| match &record.event {
                    Event::Label {
                        target,
                        label: Some(label),
                    } => Some((*target, label.as_str())),
                    _ => None,
                })
                .collect();
            assert_eq!(labels, vec![(second, "second"), (first, "first")]);
            assert_eq!(
                fork.record_metadata(old_label),
                Some(&serde_json::json!({"note":"preserve"}))
            );
        }
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
    fn imported_display_names_are_trimmed_and_can_clear_earlier_names() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session.set_name("earlier").unwrap();
        session
            .append(Event::SessionInfo {
                name: Some("\u{feff}  imported\nname \u{3000}".into()),
                cwd: None,
            })
            .unwrap();
        assert_eq!(session.name(), Some("imported\nname"));
        session
            .append(Event::SessionInfo {
                name: None,
                cwd: Some("/tmp".into()),
            })
            .unwrap();
        assert_eq!(session.name(), Some("imported\nname"));
        session
            .append(Event::SessionInfo {
                name: Some("\u{feff} \t".into()),
                cwd: None,
            })
            .unwrap();
        let restored = Session::from_records(session.records().to_vec()).unwrap();
        assert_eq!(restored.name(), None);
        session
            .append(Event::SessionInfo {
                name: Some("\u{0085}".into()),
                cwd: None,
            })
            .unwrap();
        assert_eq!(session.name(), Some("\u{0085}"));
    }

    #[test]
    fn share_open_retains_lock_and_never_repairs_or_creates_journal() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        assert!(Session::open_for_share(&path).is_err());
        assert!(!path.exists());
        drop(Session::open(&path).unwrap());
        let original = std::fs::read(&path).unwrap();
        let share = Session::open_for_share(&path).unwrap();
        assert!(
            Session::open(&path).is_err(),
            "share must retain the journal lock"
        );
        drop(share);
        assert_eq!(std::fs::read(&path).unwrap(), original);
        let mut named = Session::open_for_share(&path).unwrap();
        named.set_name("explicit name").unwrap();
        drop(named);
        assert_eq!(
            Session::snapshot(&path).unwrap().name(),
            Some("explicit name")
        );
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{\"incomplete\":")
            .unwrap();
        let incomplete = std::fs::read(&path).unwrap();
        assert!(Session::open_for_share(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), incomplete);
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
    fn reconcile_accepts_patched_known_text_results() {
        for is_error in [false, true] {
            let mut session = Session::with_store(MemoryStore::default()).unwrap();
            session
                .append_user_with_images("hello".into(), vec![], None)
                .unwrap();
            let operation_id = Uuid::new_v4();
            session
                .append(Event::Assistant {
                    content: String::new(),
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
            session
                .append(Event::ToolStarted {
                    operation_id,
                    tool_execution: None,
                })
                .unwrap();
            session
                .append(Event::ToolFinished {
                    operation_id,
                    outcome: Outcome::Unknown("uncertain".into()),
                    states: Default::default(),
                })
                .unwrap();
            let result = Outcome::TextResult {
                output: "confirmed".into(),
                details: Some(serde_json::json!({"evidence":true})),
                is_error,
            };
            session.reconcile(operation_id, result.clone()).unwrap();
            assert!(
                matches!(&session.operations()[0].1, OperationState::Finished(outcome) if outcome == &result)
            );
            assert!(session.current_messages().iter().any(
                |message| matches!(message, Message::Tool { content, is_error: status, .. } if content == "confirmed" && *status == is_error)
            ));
            let target = session.head();
            session
                .edit_context(target, Some(crate::ContextContent::Text("edited".into())))
                .unwrap();
            assert!(session.current_messages().iter().any(|message| matches!(message, Message::Tool { content, is_error: status, .. } if content == "edited" && *status == is_error)));
        }
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
        session
            .append(Event::ToolStarted {
                operation_id,
                tool_execution: None,
            })
            .unwrap();
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

#[cfg(test)]
mod durable_tool_decision_tests {
    use super::*;

    fn fixture(
        session: &Session,
    ) -> (
        Event,
        crate::ToolDecisionSnapshot,
        crate::ModelRequest,
        Vec<crate::ToolDeclaration>,
    ) {
        fixture_with_summary(session, false)
    }

    fn fixture_with_summary(
        session: &Session,
        summary: bool,
    ) -> (
        Event,
        crate::ToolDecisionSnapshot,
        crate::ModelRequest,
        Vec<crate::ToolDeclaration>,
    ) {
        let spec = crate::ToolSpec {
            name: "read".into(),
            description: "read a file".into(),
            parameters: serde_json::json!({"type":"object", "properties":{"path":{"type":"string"}}}),
        };
        let request = crate::ModelRequest {
            model: "fixture".into(),
            messages: vec![],
            tools: vec![spec.clone()],
            max_output_tokens: 10,
        };
        let context = crate::ToolDispatchContext {
            request_id: Uuid::new_v4(),
            session_id: session.session_id(),
            external_id: session.external_session_id(),
            summary,
        };
        let declaration = crate::ToolDeclaration {
            spec: spec.clone(),
            constrained_sampling: Some(crate::ToolConstraint::JsonSchema {
                strict: crate::StrictToolPolicy::Require,
            }),
        };
        let effective = crate::EffectiveToolDeclaration {
            spec: spec.clone(),
            sampling: crate::EffectiveToolSampling::StrictJsonSchema {
                original_schema: spec.parameters,
                normalization_version: crate::STRICT_TOOL_NORMALIZATION_VERSION,
            },
        };
        let snapshot = crate::ToolDecisionSnapshot::capture(
            &context,
            &request,
            std::slice::from_ref(&declaration),
            &[effective],
        )
        .unwrap();
        let event = Event::ModelRequested {
            model: request.model.clone(),
            messages: 0,
            tools: 1,
            max_output_tokens: 10,
            request_id: Some(context.request_id),
            context_head: Some(session.head()),
            context_sha256: Some(snapshot.request_sha256().into()),
            context_transformed: Some(false),
            context_configuration_sha256: None,
            tool_decision: None,
        };
        (event, snapshot, request, vec![declaration])
    }

    #[test]
    fn durable_tool_decision_atomic_reopen_legacy_and_later_request() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.jsonl");
        let mut session = Session::open(&path).unwrap();
        let (legacy, _, _, _) = fixture(&session);
        let legacy_id = session.append(legacy).unwrap().id;
        let (marked, snapshot, request, declarations) = fixture(&session);
        let id = session
            .append_tool_decision_request(marked, &snapshot, &request, &request, &declarations)
            .unwrap()
            .id;
        let (ordinary, _, _, _) = fixture(&session);
        let ordinary_id = session.append(ordinary).unwrap().id;
        session
            .append(Event::BranchModelSelected {
                target: ordinary_id,
                provider: "provider".into(),
                model: "other".into(),
            })
            .unwrap();
        assert!(matches!(
            session.recorded_tool_decision(legacy_id).unwrap(),
            RecordedToolDecision::Legacy
        ));
        assert!(matches!(
            session.recorded_tool_decision(ordinary_id).unwrap(),
            RecordedToolDecision::Legacy
        ));
        drop(session);
        let reopened = Session::open(&path).unwrap();
        assert!(matches!(
            reopened.recorded_tool_decision(id).unwrap(),
            RecordedToolDecision::Current(_)
        ));
        assert_eq!(
            reopened.records()[id as usize - 1].version,
            TOOL_DECISION_JOURNAL_VERSION
        );
        assert!(matches!(
            reopened.recorded_tool_decision(ordinary_id).unwrap(),
            RecordedToolDecision::Legacy
        ));
        let stripped = Session::from_records(reopened.records().to_vec()).unwrap();
        assert!(stripped.recorded_tool_decision(id).is_err());
        let read_only = Session::snapshot(&path).unwrap();
        assert!(matches!(
            read_only.recorded_tool_decision(id).unwrap(),
            RecordedToolDecision::Current(_)
        ));
        let values = reopened
            .records()
            .iter()
            .map(|record| {
                serde_json::from_slice::<serde_json::Value>(
                    &encode_record(record, reopened.record_metadata(record.id)).unwrap(),
                )
                .unwrap()
            })
            .collect();
        let imported = Session::from_serialized_records(values).unwrap();
        assert!(matches!(
            imported.recorded_tool_decision(id).unwrap(),
            RecordedToolDecision::Current(_)
        ));
        let bytes = std::fs::read_to_string(path).unwrap();
        assert!(bytes.contains("toolDecision"));
        assert!(bytes.contains("read a file"));
    }

    #[test]
    fn durable_tool_decision_rejects_corruption_and_unknown_versions() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let (event, snapshot, request, declarations) = fixture(&session);
        let id = session
            .append_tool_decision_request(event, &snapshot, &request, &request, &declarations)
            .unwrap()
            .id;
        let records = session.records().to_vec();
        let mut metadata = session.record_metadata.clone();
        metadata.get_mut(&id).unwrap()["toolDecision"]["snapshot"]["request_sha256"] =
            "0".repeat(64).into();
        let corrupt = Session::from_records_with_metadata(records.clone(), metadata).unwrap();
        assert!(corrupt.recorded_tool_decision(id).is_err());
        let mut metadata = session.record_metadata.clone();
        metadata.get_mut(&id).unwrap()["toolDecision"]["version"] = 2.into();
        let unknown = Session::from_records_with_metadata(records.clone(), metadata).unwrap();
        assert!(unknown.recorded_tool_decision(id).is_err());
        let mut changed = records.clone();
        if let Event::ModelRequested { model, .. } = &mut changed[id as usize - 1].event {
            *model = "changed".into();
        }
        let changed =
            Session::from_records_with_metadata(changed, session.record_metadata.clone()).unwrap();
        assert!(changed.recorded_tool_decision(id).is_err());
        let mut records = records;
        if let Event::ModelRequested {
            tool_decision: Some(marker),
            ..
        } = &mut records[id as usize - 1].event
        {
            marker.version = 2;
        }
        let unknown =
            Session::from_records_with_metadata(records.clone(), session.record_metadata.clone())
                .unwrap();
        assert!(unknown.recorded_tool_decision(id).is_err());
        if let Event::ModelRequested {
            tool_decision: Some(marker),
            ..
        } = &mut records[id as usize - 1].event
        {
            marker.version = 1;
            marker.binding_sha256 = "f".repeat(64);
        }
        let mismatch =
            Session::from_records_with_metadata(records, session.record_metadata.clone()).unwrap();
        assert!(mismatch.recorded_tool_decision(id).is_err());
        let mut value = serde_json::to_value(&session.records()[id as usize - 1]).unwrap();
        value["event"]["tool_decision"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<Record>(value).is_err());
    }

    #[test]
    fn durable_tool_decision_fork_retains_historical_context_and_remaps_head() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let base = session.head();
        session
            .append(Event::User {
                content: "discarded".into(),
                input_id: None,
            })
            .unwrap();
        session.checkout(base).unwrap();
        session
            .append(Event::User {
                content: "retained".into(),
                input_id: None,
            })
            .unwrap();
        let (event, snapshot, request, declarations) = fixture(&session);
        let id = session
            .append_tool_decision_request(event, &snapshot, &request, &request, &declarations)
            .unwrap()
            .id;
        let fork = session.fork_session(id).unwrap();
        let forked = fork
            .records()
            .iter()
            .find(|r| {
                matches!(
                    r.event,
                    Event::ModelRequested {
                        tool_decision: Some(_),
                        ..
                    }
                )
            })
            .unwrap();
        assert_ne!(id, forked.id);
        assert_ne!(session.session_id(), fork.session_id());
        assert!(matches!(
            fork.recorded_tool_decision(forked.id).unwrap(),
            RecordedToolDecision::Historical(_)
        ));
        let handoff_fork = session.fork_session_after_handoff(id).unwrap();
        let handoff_id = handoff_fork
            .records()
            .iter()
            .find(|r| {
                matches!(
                    r.event,
                    Event::ModelRequested {
                        tool_decision: Some(_),
                        ..
                    }
                )
            })
            .unwrap()
            .id;
        assert!(matches!(
            handoff_fork.recorded_tool_decision(handoff_id).unwrap(),
            RecordedToolDecision::Historical(_)
        ));
        let stripped = Session::from_records(session.fork_records(id).unwrap()).unwrap();
        assert!(stripped.recorded_tool_decision(forked.id).is_err());
        let (ordinary, _, _, _) = fixture(&fork);
        let mut fork = fork;
        let next = fork.append(ordinary).unwrap().id;
        assert!(matches!(
            fork.recorded_tool_decision(next).unwrap(),
            RecordedToolDecision::Legacy
        ));
    }

    #[test]
    fn durable_tool_decision_rejects_orphan_metadata_but_allows_unrelated_metadata() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let (event, snapshot, request, declarations) = fixture(&session);
        let id = session
            .append_tool_decision_request(event, &snapshot, &request, &request, &declarations)
            .unwrap()
            .id;
        let mut records = session.records().to_vec();
        if let Event::ModelRequested { tool_decision, .. } = &mut records[id as usize - 1].event {
            *tool_decision = None;
        }
        let orphan =
            Session::from_records_with_metadata(records.clone(), session.record_metadata.clone())
                .unwrap();
        assert!(orphan.recorded_tool_decision(id).is_err());
        let mut old_version = records.clone();
        old_version[id as usize - 1].version = BRANCH_FALLBACK_JOURNAL_VERSION;
        let old_orphan =
            Session::from_records_with_metadata(old_version, session.record_metadata.clone())
                .unwrap();
        assert!(old_orphan.recorded_tool_decision(id).is_err());
        let mut metadata = BTreeMap::new();
        metadata.insert(id, serde_json::json!({"toolDecision": null}));
        let orphan_null = Session::from_records_with_metadata(records.clone(), metadata).unwrap();
        assert!(orphan_null.recorded_tool_decision(id).is_err());
        let mut metadata = BTreeMap::new();
        metadata.insert(id, serde_json::json!({"other": "value"}));
        let unrelated = Session::from_records_with_metadata(records.clone(), metadata).unwrap();
        assert!(matches!(
            unrelated.recorded_tool_decision(id).unwrap(),
            RecordedToolDecision::Legacy
        ));
        // Without either field, this is indistinguishable from an ordinary
        // request written after the journal's version had already advanced.
        let both_missing = Session::from_records(records).unwrap();
        assert!(matches!(
            both_missing.recorded_tool_decision(id).unwrap(),
            RecordedToolDecision::Legacy
        ));
    }

    #[test]
    fn durable_tool_decision_on_inactive_branch_is_historical() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let base = session.head();
        let (event, snapshot, request, declarations) = fixture(&session);
        let id = session
            .append_tool_decision_request(event, &snapshot, &request, &request, &declarations)
            .unwrap()
            .id;
        assert!(matches!(
            session.recorded_tool_decision(id).unwrap(),
            RecordedToolDecision::Current(_)
        ));
        let request_id = snapshot.context().request_id;
        assert!(matches!(
            session.active_tool_decision(request_id).unwrap(),
            RecordedToolDecision::Current(_)
        ));
        session.checkout(base).unwrap();
        assert!(matches!(
            session.recorded_tool_decision(id).unwrap(),
            RecordedToolDecision::Historical(_)
        ));
        assert!(session.active_tool_decision(request_id).is_err());
        session.checkout(id).unwrap();
        assert!(matches!(
            session.recorded_tool_decision(id).unwrap(),
            RecordedToolDecision::Current(_)
        ));
        let (mut duplicate, _, _, _) = fixture(&session);
        if let Event::ModelRequested { request_id: id, .. } = &mut duplicate {
            *id = Some(request_id);
        } else {
            unreachable!();
        }
        session.append(duplicate).unwrap();
        assert!(session.active_tool_decision(request_id).is_err());
    }

    #[test]
    fn durable_tool_decision_version_is_monotonic_but_not_a_per_request_marker() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let base = session.head();
        let (event, snapshot, request, declarations) = fixture(&session);
        let id = session
            .append_tool_decision_request(event, &snapshot, &request, &request, &declarations)
            .unwrap()
            .id;
        session.checkout(base).unwrap();
        let (ordinary, _, _, _) = fixture(&session);
        let ordinary_id = session.append(ordinary).unwrap().id;
        let mut downgraded = session.records().to_vec();
        downgraded[ordinary_id as usize - 1].version = TOOL_DECISION_JOURNAL_VERSION - 1;
        assert!(Session::from_records(downgraded).is_err());
        // A fork can omit the marked request while retaining later v5 records.
        let fork = session.fork_session(ordinary_id).unwrap();
        assert!(fork.records().iter().all(|record| !matches!(
            record.event,
            Event::ModelRequested {
                tool_decision: Some(_),
                ..
            }
        )));
        assert!(matches!(
            fork.recorded_tool_decision(fork.head()).unwrap(),
            RecordedToolDecision::Legacy
        ));
        assert_eq!(
            fork.records().last().unwrap().version,
            TOOL_DECISION_JOURNAL_VERSION
        );
        assert_eq!(
            session.records()[id as usize - 1].version,
            TOOL_DECISION_JOURNAL_VERSION
        );
    }

    #[test]
    fn durable_tool_decision_unsupported_store_fails_before_append() {
        struct LegacyStore(MemoryStore);
        impl SessionStore for LegacyStore {
            fn load(&mut self) -> Result<Vec<Record>> {
                self.0.load()
            }
            fn append(&mut self, record: &Record) -> Result<()> {
                self.0.append(record)
            }
        }
        let mut session = Session::with_store(LegacyStore(MemoryStore::default())).unwrap();
        let (event, snapshot, request, declarations) = fixture(&session);
        assert!(
            session
                .append_tool_decision_request(event, &snapshot, &request, &request, &declarations)
                .is_err()
        );
        assert_eq!(session.records().len(), 1);
    }

    #[test]
    fn durable_tool_decision_rejects_wrong_request_before_append() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let (mut event, snapshot, request, declarations) = fixture(&session);
        if let Event::ModelRequested { request_id, .. } = &mut event {
            *request_id = Some(Uuid::new_v4());
        }
        assert!(
            session
                .append_tool_decision_request(event, &snapshot, &request, &request, &declarations)
                .is_err()
        );
        assert_eq!(session.records().len(), 1);
    }

    fn execution_fixture(
        session: &mut Session,
    ) -> (
        Uuid,
        crate::Invocation,
        crate::ToolSpec,
        crate::ToolExecutionSnapshot,
    ) {
        execution_fixture_with_summary(session, false)
    }

    fn execution_fixture_with_summary(
        session: &mut Session,
        summary: bool,
    ) -> (
        Uuid,
        crate::Invocation,
        crate::ToolSpec,
        crate::ToolExecutionSnapshot,
    ) {
        let (event, decision, request, declarations) = fixture_with_summary(session, summary);
        let request_id = decision.context().request_id;
        let spec = request.tools[0].clone();
        session
            .append_tool_decision_request(event, &decision, &request, &request, &declarations)
            .unwrap();
        let invocation = crate::Invocation {
            operation_id: Uuid::new_v4(),
            call: crate::ToolCall {
                id: "call".into(),
                name: spec.name.clone(),
                arguments: "{}".into(),
            },
        };
        session
            .append(Event::ModelFinished {
                request_id,
                metadata: ModelMetadata::default(),
                usage: UsageReport::Unconfirmed,
                failure: None,
                partial: String::new(),
                reply: Some(RecordedReply {
                    content: String::new(),
                    invocations: vec![invocation.clone()],
                    advertised_tools: vec![spec.name.clone()],
                    max_output_tokens: request.max_output_tokens,
                }),
            })
            .unwrap();
        session
            .append(Event::Assistant {
                content: "".into(),
                invocations: vec![invocation.clone()],
                usage: None,
                request_id: Some(request_id),
            })
            .unwrap();
        let execution = crate::ToolExecutionSnapshot::capture(
            request_id,
            &invocation,
            "owner",
            &spec,
            "backend-v1",
            decision.binding_sha256(),
            serde_json::json!({"path":"authorized"}),
        )
        .unwrap();
        (request_id, invocation, spec, execution)
    }

    #[test]
    fn tool_execution_roundtrip_and_legacy_after_marker() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.jsonl");
        let mut session = Session::open(&path).unwrap();
        let (request, inv, spec, execution) = execution_fixture(&mut session);
        let id = session
            .append_tool_execution_started(
                request,
                inv.operation_id,
                &execution,
                "owner",
                &spec,
                "backend-v1",
            )
            .unwrap()
            .id;
        assert_eq!(
            session
                .recorded_tool_execution(id, "owner", &spec, "backend-v1")
                .unwrap(),
            Some(serde_json::json!({"path":"authorized"}))
        );
        assert!(
            session
                .recorded_tool_execution(id, "owner", &spec, "changed-backend")
                .is_err()
        );
        session
            .append(Event::ToolStarted {
                operation_id: Uuid::new_v4(),
                tool_execution: None,
            })
            .unwrap();
        assert_eq!(
            session
                .recorded_tool_execution(session.head(), "owner", &spec, "backend-v1")
                .unwrap(),
            None
        );
        drop(session);
        let session = Session::open(&path).unwrap();
        assert_eq!(
            session
                .recorded_tool_execution(id, "owner", &spec, "backend-v1")
                .unwrap(),
            Some(serde_json::json!({"path":"authorized"}))
        );
        let stripped = Session::from_records(session.records().to_vec()).unwrap();
        assert!(
            stripped
                .recorded_tool_execution(id, "owner", &spec, "backend-v1")
                .is_err()
        );
    }

    #[test]
    fn tool_execution_rejects_orphan_corruption_and_identity_mismatch() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let (request, inv, spec, execution) = execution_fixture(&mut session);
        let id = session
            .append_tool_execution_started(
                request,
                inv.operation_id,
                &execution,
                "owner",
                &spec,
                "backend-v1",
            )
            .unwrap()
            .id;
        assert!(
            session
                .recorded_tool_execution(id, "wrong", &spec, "backend-v1")
                .is_err()
        );
        let mut records = session.records().to_vec();
        if let Event::ToolStarted { tool_execution, .. } = &mut records[id as usize - 1].event {
            *tool_execution = None;
        }
        let orphan =
            Session::from_records_with_metadata(records.clone(), session.record_metadata.clone())
                .unwrap();
        assert!(
            orphan
                .recorded_tool_execution(id, "owner", &spec, "backend-v1")
                .is_err()
        );
        let mut metadata = session.record_metadata.clone();
        metadata.get_mut(&id).unwrap()["toolExecution"]["snapshot"]["authorized_arguments"] =
            serde_json::json!({"path":"tampered"});
        let corrupt =
            Session::from_records_with_metadata(session.records().to_vec(), metadata).unwrap();
        assert!(
            corrupt
                .recorded_tool_execution(id, "owner", &spec, "backend-v1")
                .is_err()
        );
        let mut records = session.records().to_vec();
        if let Event::ToolStarted {
            tool_execution: Some(marker),
            ..
        } = &mut records[id as usize - 1].event
        {
            marker.version = 99;
        }
        let unknown =
            Session::from_records_with_metadata(records, session.record_metadata.clone()).unwrap();
        assert!(
            unknown
                .recorded_tool_execution(id, "owner", &spec, "backend-v1")
                .is_err()
        );
        let mut changed = session.records().to_vec();
        let assistant = changed
            .iter_mut()
            .find(|r| matches!(r.event, Event::Assistant { .. }))
            .unwrap();
        if let Event::Assistant { invocations, .. } = &mut assistant.event {
            invocations[0].call.arguments = "{\"changed\":true}".into();
        }
        let changed =
            Session::from_records_with_metadata(changed, session.record_metadata.clone()).unwrap();
        assert!(
            changed
                .recorded_tool_execution(id, "owner", &spec, "backend-v1")
                .is_err()
        );
        let mut changed = session.records().to_vec();
        let assistant = changed
            .iter_mut()
            .find(|r| matches!(r.event, Event::Assistant { .. }))
            .unwrap();
        if let Event::Assistant { request_id, .. } = &mut assistant.event {
            *request_id = Some(Uuid::new_v4());
        }
        let changed =
            Session::from_records_with_metadata(changed, session.record_metadata.clone()).unwrap();
        assert!(
            changed
                .recorded_tool_execution(id, "owner", &spec, "backend-v1")
                .is_err()
        );
        let mut changed = session.records().to_vec();
        if let Event::ToolStarted {
            tool_execution: Some(marker),
            ..
        } = &mut changed[id as usize - 1].event
        {
            marker.decision_sha256 = "f".repeat(64);
        }
        let changed =
            Session::from_records_with_metadata(changed, session.record_metadata.clone()).unwrap();
        assert!(
            changed
                .recorded_tool_execution(id, "owner", &spec, "backend-v1")
                .is_err()
        );
        let mut value = serde_json::to_value(&session.records()[id as usize - 1]).unwrap();
        value["event"]["tool_execution"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<Record>(value).is_err());
    }

    #[test]
    fn tool_execution_rejects_wrong_provenance_and_historical_start() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let base = session.head();
        let (request, inv, spec, execution) = execution_fixture(&mut session);
        assert!(
            session
                .append_tool_execution_started(
                    Uuid::new_v4(),
                    inv.operation_id,
                    &execution,
                    "owner",
                    &spec,
                    "backend-v1"
                )
                .is_err()
        );
        assert!(
            session
                .append_tool_execution_started(
                    request,
                    Uuid::new_v4(),
                    &execution,
                    "owner",
                    &spec,
                    "backend-v1"
                )
                .is_err()
        );
        let id = session
            .append_tool_execution_started(
                request,
                inv.operation_id,
                &execution,
                "owner",
                &spec,
                "backend-v1",
            )
            .unwrap()
            .id;
        session
            .append(Event::ToolFinished {
                operation_id: inv.operation_id,
                outcome: Outcome::Succeeded("ok".into()),
                states: Default::default(),
            })
            .unwrap();
        let fork = session.fork_session(session.head()).unwrap();
        let forked = fork
            .records()
            .iter()
            .find(|record| {
                matches!(
                    record.event,
                    Event::ToolStarted {
                        tool_execution: Some(_),
                        ..
                    }
                )
            })
            .unwrap()
            .id;
        assert!(
            fork.recorded_tool_execution(forked, "owner", &spec, "backend-v1")
                .is_err()
        );
        session.checkout(base).unwrap();
        assert!(
            session
                .recorded_tool_execution(id, "owner", &spec, "backend-v1")
                .is_err()
        );
    }

    #[test]
    fn tool_execution_checks_complete_record_size_before_write() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct Limited {
            inner: MemoryStore,
            limit: Arc<AtomicUsize>,
        }
        impl SessionStore for Limited {
            fn load(&mut self) -> Result<Vec<Record>> {
                self.inner.load()
            }
            fn append(&mut self, record: &Record) -> Result<()> {
                self.inner.append(record)
            }
            fn supports_record_metadata(&self) -> bool {
                true
            }
            fn encoded_record_limit(&self) -> Option<usize> {
                Some(self.limit.load(Ordering::Relaxed))
            }
            fn load_record_metadata(&mut self) -> Result<BTreeMap<u64, serde_json::Value>> {
                self.inner.load_record_metadata()
            }
            fn append_with_metadata(
                &mut self,
                record: &Record,
                metadata: Option<&serde_json::Value>,
            ) -> Result<()> {
                self.inner.append_with_metadata(record, metadata)
            }
        }
        let limit = Arc::new(AtomicUsize::new(MAX_RECORD_BYTES));
        let mut session = Session::with_store(Limited {
            inner: MemoryStore::default(),
            limit: limit.clone(),
        })
        .unwrap();
        let (request, inv, spec, execution) = execution_fixture(&mut session);
        let before = session.records().len();
        limit.store(100, Ordering::Relaxed);
        assert!(
            session
                .append_tool_execution_started(
                    request,
                    inv.operation_id,
                    &execution,
                    "owner",
                    &spec,
                    "backend-v1"
                )
                .is_err()
        );
        assert_eq!(session.records().len(), before);
        limit.store(MAX_RECORD_BYTES, Ordering::Relaxed);
        assert!(
            session
                .append_tool_execution_started(
                    request,
                    inv.operation_id,
                    &execution,
                    "owner",
                    &spec,
                    "backend-v1"
                )
                .is_ok()
        );
    }

    #[test]
    fn tool_execution_requires_matching_unique_successful_completion_at_append_and_lookup() {
        for case in [
            "missing",
            "failure",
            "reply_missing",
            "length",
            "partial",
            "content",
            "invocations",
            "advertised",
            "budget",
            "wrong_run",
            "wrong_request",
            "wrong_order",
            "duplicate",
        ] {
            let mut session = Session::with_store(MemoryStore::default()).unwrap();
            let (request, invocation, spec, execution) = execution_fixture(&mut session);
            let start = session
                .append_tool_execution_started(
                    request,
                    invocation.operation_id,
                    &execution,
                    "owner",
                    &spec,
                    "backend-v1",
                )
                .unwrap()
                .id;
            let mut records = session.records().to_vec();
            let completion = records
                .iter_mut()
                .find(|record| matches!(record.event, Event::ModelFinished { .. }))
                .unwrap();
            if case == "wrong_run" {
                completion.run_id = Some(Uuid::new_v4());
            }
            if case == "missing" {
                completion.event = Event::SessionInfo {
                    name: None,
                    cwd: None,
                };
            } else if let Event::ModelFinished {
                request_id,
                metadata,
                failure,
                partial,
                reply,
                ..
            } = &mut completion.event
            {
                match case {
                    "wrong_request" => *request_id = Uuid::new_v4(),
                    "failure" => *failure = Some(ModelFailure::Failed),
                    "reply_missing" => *reply = None,
                    "length" => metadata.finish_reason = Some("length".into()),
                    "partial" => *partial = "incomplete".into(),
                    "content" => reply.as_mut().unwrap().content = "different".into(),
                    "invocations" => {
                        reply.as_mut().unwrap().invocations[0].call.arguments = "{\"x\":1}".into()
                    }
                    "advertised" => reply.as_mut().unwrap().advertised_tools.clear(),
                    "budget" => reply.as_mut().unwrap().max_output_tokens += 1,
                    _ => {}
                }
            }
            if case == "wrong_order" {
                let completion_index = records
                    .iter()
                    .position(|r| matches!(r.event, Event::ModelFinished { .. }))
                    .unwrap();
                let assistant_index = records
                    .iter()
                    .position(|r| matches!(r.event, Event::Assistant { .. }))
                    .unwrap();
                let completion_event = records[completion_index].event.clone();
                records[completion_index].event = records[assistant_index].event.clone();
                records[assistant_index].event = completion_event;
            }
            let mut metadata = session.record_metadata.clone();
            let mut lookup =
                Session::from_records_with_metadata(records.clone(), metadata.clone()).unwrap();
            if case == "duplicate" {
                let event = records
                    .iter()
                    .find(|r| matches!(r.event, Event::ModelFinished { .. }))
                    .unwrap()
                    .event
                    .clone();
                lookup.append(event).unwrap();
            }
            assert!(
                lookup
                    .recorded_tool_execution(start, "owner", &spec, "backend-v1")
                    .is_err(),
                "lookup accepted {case}"
            );
            records.pop();
            metadata.remove(&start);
            let mut append = Session::from_records_with_metadata(records, metadata).unwrap();
            if case == "duplicate" {
                let event = append
                    .records()
                    .iter()
                    .find(|r| matches!(r.event, Event::ModelFinished { .. }))
                    .unwrap()
                    .event
                    .clone();
                append.append(event).unwrap();
            }
            assert!(
                append
                    .append_tool_execution_started(
                        request,
                        invocation.operation_id,
                        &execution,
                        "owner",
                        &spec,
                        "backend-v1"
                    )
                    .is_err(),
                "append accepted {case}"
            );
        }
    }

    #[test]
    fn tool_execution_rejects_summary_at_append_and_lookup() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let (request, invocation, spec, execution) =
            execution_fixture_with_summary(&mut session, true);
        let error = session
            .append_tool_execution_started(
                request,
                invocation.operation_id,
                &execution,
                "owner",
                &spec,
                "backend-v1",
            )
            .unwrap_err();
        assert!(error.to_string().contains("summary request"));
        let RecordedToolDecision::Current(decision) =
            session.active_tool_decision(request).unwrap()
        else {
            panic!("missing decision")
        };
        let marker = ToolExecutionMarker {
            version: 1,
            origin_session_id: session.session_id(),
            request_id: request,
            decision_sha256: decision.binding_sha256().into(),
            binding_sha256: execution.binding_sha256().into(),
        };
        let metadata = serde_json::json!({"toolExecution": ToolExecutionMetadata { version: 1, snapshot: execution }});
        let id = session
            .append_inner_with_metadata(
                Event::ToolStarted {
                    operation_id: invocation.operation_id,
                    tool_execution: Some(marker),
                },
                true,
                Some(metadata),
                false,
            )
            .unwrap()
            .id;
        let error = session
            .recorded_tool_execution(id, "owner", &spec, "backend-v1")
            .unwrap_err();
        assert!(error.to_string().contains("summary request"));
    }

    #[test]
    fn tool_execution_rejects_superseding_turns_at_append_and_lookup() {
        for (kind, before_assistant) in [
            ("request", false),
            ("assistant", false),
            ("request", true),
            ("assistant", true),
        ] {
            let mut original = Session::with_store(MemoryStore::default()).unwrap();
            let (request, invocation, spec, execution) = execution_fixture(&mut original);
            let start = original
                .append_tool_execution_started(
                    request,
                    invocation.operation_id,
                    &execution,
                    "owner",
                    &spec,
                    "backend-v1",
                )
                .unwrap()
                .id;
            let start_event = original.records()[start as usize - 1].event.clone();
            let start_metadata = original.record_metadata(start).unwrap().clone();
            let mut records = original.records().to_vec();
            records.pop();
            let delayed_assistant = before_assistant.then(|| records.pop().unwrap().event);
            let mut metadata = original.record_metadata.clone();
            metadata.remove(&start);
            let mut session = Session::from_records_with_metadata(records, metadata).unwrap();
            let superseding = if kind == "request" {
                fixture(&session).0
            } else {
                Event::Assistant {
                    content: "later turn".into(),
                    invocations: vec![],
                    usage: None,
                    request_id: Some(Uuid::new_v4()),
                }
            };
            session.append(superseding).unwrap();
            if let Some(assistant) = delayed_assistant {
                session.append(assistant).unwrap();
            }
            assert!(
                session
                    .append_tool_execution_started(
                        request,
                        invocation.operation_id,
                        &execution,
                        "owner",
                        &spec,
                        "backend-v1"
                    )
                    .is_err(),
                "append accepted later {kind}"
            );
            let id = session
                .append_inner_with_metadata(start_event, true, Some(start_metadata), false)
                .unwrap()
                .id;
            assert!(
                session
                    .recorded_tool_execution(id, "owner", &spec, "backend-v1")
                    .is_err(),
                "lookup accepted later {kind}"
            );
        }
    }

    #[test]
    fn tool_execution_allows_parallel_starts_and_intervening_lifecycle() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let (request, first, spec, first_snapshot) = execution_fixture(&mut session);
        let mut second = first.clone();
        second.operation_id = Uuid::new_v4();
        second.call.id = "second".into();
        let mut records = session.records().to_vec();
        for record in &mut records {
            match &mut record.event {
                Event::ModelFinished {
                    reply: Some(reply), ..
                } => reply.invocations.push(second.clone()),
                Event::Assistant { invocations, .. } => invocations.push(second.clone()),
                _ => {}
            }
        }
        let mut session =
            Session::from_records_with_metadata(records, session.record_metadata.clone()).unwrap();
        let RecordedToolDecision::Current(decision) =
            session.active_tool_decision(request).unwrap()
        else {
            panic!("missing decision")
        };
        let second_snapshot = crate::ToolExecutionSnapshot::capture(
            request,
            &second,
            "owner",
            &spec,
            "backend-v1",
            decision.binding_sha256(),
            serde_json::json!({"path":"second"}),
        )
        .unwrap();
        let first_id = session
            .append_tool_execution_started(
                request,
                first.operation_id,
                &first_snapshot,
                "owner",
                &spec,
                "backend-v1",
            )
            .unwrap()
            .id;
        session
            .append(Event::RequestCallbackStarted {
                id: Uuid::new_v4(),
                purpose: "lifecycle".into(),
            })
            .unwrap();
        session
            .append(Event::InputQueueCleared { ids: vec![] })
            .unwrap();
        let second_id = session
            .append_tool_execution_started(
                request,
                second.operation_id,
                &second_snapshot,
                "owner",
                &spec,
                "backend-v1",
            )
            .unwrap()
            .id;
        assert!(
            session
                .recorded_tool_execution(first_id, "owner", &spec, "backend-v1")
                .unwrap()
                .is_some()
        );
        assert!(
            session
                .recorded_tool_execution(second_id, "owner", &spec, "backend-v1")
                .unwrap()
                .is_some()
        );
        let later_request = fixture(&session).0;
        session.append(later_request).unwrap();
        assert!(
            session
                .recorded_tool_execution(first_id, "owner", &spec, "backend-v1")
                .unwrap()
                .is_some()
        );
    }
}
