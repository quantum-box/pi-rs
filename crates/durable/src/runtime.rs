//! Durable conversation execution with a single local scheduler owner.
//!
//! A committed phase is the authority for recovery. External calls never run
//! inside storage transactions. Independent conversations can run concurrently.
use crate::{
    model::{CompletedGeneration, collect_generation, model_request},
    recovery::{ReplayPolicy, ToolRecovery, UncertainPolicy, recover_tool},
};
use durable_store::{
    Check, Expected, Mutation,
    owner::{OwnedStore, OwnerError},
};
use futures_util::FutureExt;
use pi_rs_core::{
    CancellationToken, ExtensionState, Invocation, Message, Model, Outcome, Tool, ToolCall,
    ToolSpec, UsageReport,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("owner: {0}")]
    Owner(#[from] OwnerError),
    #[error("storage: {0}")]
    Storage(#[from] durable_store::Error),
    #[error("serialization: {0}")]
    Json(#[from] serde_json::Error),
    #[error("observation: {0}")]
    Observer(#[from] crate::observer::Error),
    #[error("model: {0}")]
    Model(#[from] pi_rs_core::Error),
    #[error("conversation not found")]
    NotFound,
    #[error("session failed; reopen before further operations")]
    Failed,
    #[error("execution interrupted; reopen to recover")]
    Interrupted,
    #[error("invalid operation: {0}")]
    Invalid(String),
}

#[derive(Clone)]
pub struct RegisteredTool {
    pub tool: Arc<dyn Tool>,
    pub replay: ReplayPolicy,
}

#[derive(Clone)]
pub struct Options {
    pub model: Arc<dyn Model>,
    pub tools: Vec<RegisteredTool>,
    pub restart_limit: u32,
    pub max_turns: u32,
    pub uncertain_policy: UncertainPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub model: String,
    pub system_prompt: String,
    pub max_output_tokens: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Status {
    Queued,
    Running,
    Done { answer: String },
    Unanswered { reason: String },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Submission {
    pub id: Uuid,
    pub request_id: Option<String>,
    pub content: String,
    pub status: Status,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Phase {
    Idle,
    /// A failed run retains its queued inputs until a new input arrives.
    FailedIdle,
    ModelReady {
        turns: u32,
    },
    ModelRunning {
        turns: u32,
        request_id: Uuid,
        restarts: u32,
    },
    AbandonedModel {
        turns: u32,
        request_id: Uuid,
        restarts: u32,
    },
    CompactionReady {
        preparation: Box<crate::compact::Preparation>,
        restarts: u32,
    },
    CompactionRunning {
        preparation: Box<crate::compact::Preparation>,
        request_id: Uuid,
        restarts: u32,
    },
    CompactionAbandoned {
        preparation: Box<crate::compact::Preparation>,
        request_id: Uuid,
        restarts: u32,
    },
    CompactionAccounting {
        result: Box<crate::compact::CompactionResult>,
        request_id: Uuid,
    },
    Accounting {
        turns: u32,
        request_id: Uuid,
        response: Box<CompletedGeneration>,
    },
    Tools {
        turns: u32,
        calls: Vec<ToolCall>,
        index: usize,
    },
    ToolRunning {
        turns: u32,
        calls: Vec<ToolCall>,
        index: usize,
        operation_id: Uuid,
        replay: ReplayPolicy,
        arguments: serde_json::Value,
        restarts: u32,
        #[serde(default)]
        replay_ready: bool,
    },
    Blocked {
        turns: u32,
        calls: Vec<ToolCall>,
        index: usize,
        operation_id: Uuid,
        reason: String,
    },
    Pending {
        turns: u32,
        calls: Vec<ToolCall>,
        index: usize,
        operation_id: Uuid,
        reference: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Conversation {
    pub id: Uuid,
    pub config: Config,
    pub messages: Vec<Message>,
    pub submissions: Vec<Submission>,
    #[serde(default)]
    tool_states: HashMap<String, ExtensionState>,
    active: Option<Uuid>,
    phase: Phase,
}

pub struct Harness {
    owner: Arc<OwnedStore>,
    options: Options,
    tools: HashMap<String, RegisteredTool>,
    failed: bool,
    dirty: Arc<AtomicBool>,
    merge_admissions: bool,
    storage_failed: Arc<AtomicBool>,
    /// Snapshots loaded by this worker, used to distinguish admission-only
    /// conflicts from changes to execution state.
    loaded: std::sync::Mutex<HashMap<Uuid, (u64, Conversation)>>,
}
struct ResumeGuard {
    dirty: Arc<AtomicBool>,
    complete: bool,
}
impl Drop for ResumeGuard {
    fn drop(&mut self) {
        if !self.complete {
            self.dirty.store(true, Ordering::Release);
        }
    }
}
impl Harness {
    /// Opens under exclusive scheduler ownership and resumes all unfinished work.
    /// A cancelled or failed open drops the owner; the next open can recover it.
    pub async fn open(
        path: impl AsRef<Path>,
        options: Options,
        cancel: &CancellationToken,
    ) -> Result<Self, Error> {
        let mut harness = Self::open_paused(path, options)?;
        harness.resume(cancel).await?;
        Ok(harness)
    }
    /// Acquire ownership and recover persisted phases without executing them.
    /// This lets an operator inspect blocked or failing conversations.
    pub fn open_paused(path: impl AsRef<Path>, options: Options) -> Result<Self, Error> {
        if options.restart_limit == 0 || options.max_turns == 0 {
            return Err(Error::Invalid("limits must be positive".into()));
        }
        let mut tools = HashMap::new();
        for registered in &options.tools {
            let spec = registered.tool.spec();
            if spec.name.is_empty()
                || tools
                    .insert(spec.name.clone(), registered.clone())
                    .is_some()
            {
                return Err(Error::Invalid(format!(
                    "duplicate or empty tool name: {}",
                    spec.name
                )));
            }
        }
        let mut harness = Self {
            owner: Arc::new(OwnedStore::open(path)?),
            options,
            tools,
            failed: false,
            dirty: Arc::new(AtomicBool::new(false)),
            merge_admissions: false,
            storage_failed: Arc::new(AtomicBool::new(false)),
            loaded: std::sync::Mutex::new(HashMap::new()),
        };
        harness.recover()?;
        Ok(harness)
    }
    fn worker(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            options: self.options.clone(),
            tools: self.tools.clone(),
            failed: false,
            dirty: Arc::new(AtomicBool::new(false)),
            merge_admissions: true,
            storage_failed: self.storage_failed.clone(),
            loaded: std::sync::Mutex::new(HashMap::new()),
        }
    }
    fn key(id: Uuid) -> Vec<u8> {
        format!("runtime/conversation/{id}").into_bytes()
    }
    fn load(&self, id: Uuid) -> Result<(Conversation, u64), Error> {
        if self.failed
            || self.dirty.load(Ordering::Acquire)
            || self.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        let entry = self
            .owner
            .store()
            .get(&Self::key(id))?
            .ok_or(Error::NotFound)?;
        let state: Conversation = serde_json::from_slice(&entry.value)?;
        self.loaded
            .lock()
            .map_err(|_| Error::Failed)?
            .insert(id, (entry.version, state.clone()));
        Ok((state, entry.version))
    }
    fn save(&mut self, state: &Conversation, expected: Expected) -> Result<(), Error> {
        if self.failed
            || self.dirty.load(Ordering::Acquire)
            || self.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        let key = Self::key(state.id);
        let mut state = state.clone();
        let mut expected = expected;
        loop {
            let value = serde_json::to_vec(&state)?;
            let event =
                serde_json::json!({"messages": state.messages, "submissions": state.submissions});
            let append = crate::observer::Observer::new(self.owner.store(), state.id)
                .prepare_append(&event)?;
            let mut checks = vec![Check {
                key: key.clone(),
                expected: expected.clone(),
            }];
            checks.extend(append.checks);
            let mut mutations = vec![Mutation::Put {
                key: key.clone(),
                value,
            }];
            mutations.extend(append.mutations);
            match self.owner.store().commit(&checks, &mutations) {
                Ok(()) => break,
                Err(durable_store::Error::Conflict { .. }) if self.merge_admissions => {
                    let entry = self.owner.store().get(&key)?.ok_or(Error::NotFound)?;
                    let latest: Conversation = serde_json::from_slice(&entry.value)?;
                    let base = self
                        .loaded
                        .lock()
                        .map_err(|_| Error::Failed)?
                        .get(&state.id)
                        .cloned();
                    let Some((base_version, base)) = base else {
                        return Err(Error::Invalid("missing conflict baseline".into()));
                    };
                    if expected != Expected::Version(base_version)
                        || !admission_only(&base, &latest)?
                    {
                        return Err(Error::Invalid(
                            "conversation changed during execution; reopen before retry".into(),
                        ));
                    }
                    for submission in latest.submissions {
                        if !state.submissions.iter().any(|s| s.id == submission.id) {
                            state.submissions.push(submission);
                        }
                    }
                    expected = Expected::Version(entry.version);
                    self.loaded.lock().map_err(|_| Error::Failed)?.insert(
                        state.id,
                        (entry.version, serde_json::from_slice(&entry.value)?),
                    );
                }
                Err(e @ durable_store::Error::Conflict { .. }) => return Err(e.into()),
                Err(e) => {
                    self.failed = true;
                    self.storage_failed.store(true, Ordering::Release);
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }
    pub fn create_conversation(&mut self, config: Config) -> Result<Uuid, Error> {
        if config.model.is_empty() || config.max_output_tokens == 0 {
            return Err(Error::Invalid("model and output limit required".into()));
        }
        let id = Uuid::new_v4();
        let state = Conversation {
            id,
            config,
            messages: vec![],
            submissions: vec![],
            tool_states: HashMap::new(),
            active: None,
            phase: Phase::Idle,
        };
        self.save(&state, Expected::Missing)?;
        Ok(id)
    }
    /// Fork the current settled conversation. Queued inputs and request IDs
    /// are not inherited. Historical entry-point forking is not yet supported.
    pub fn fork(&mut self, id: Uuid) -> Result<Uuid, Error> {
        let (mut state, _) = self.load(id)?;
        if !matches!(state.phase, Phase::Idle | Phase::FailedIdle) || state.active.is_some() {
            return Err(Error::Invalid(
                "fork requires a settled conversation".into(),
            ));
        }
        state.id = Uuid::new_v4();
        state.active = None;
        state.submissions.clear();
        state.phase = Phase::Idle;
        self.save(&state, Expected::Missing)?;
        Ok(state.id)
    }
    pub fn conversation(&self, id: Uuid) -> Result<Conversation, Error> {
        self.load(id).map(|x| x.0)
    }
    /// Read committed snapshots after a cursor. Earlier cursors may be
    /// resumed; snapshots are retained independently of model context.
    pub fn history(
        &self,
        id: Uuid,
        after: crate::observer::Cursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<crate::observer::Page, Error> {
        self.load(id)?;
        Ok(crate::observer::Observer::new(self.owner.store(), id).read(after, limit)?)
    }
    pub async fn watch_history(
        &self,
        id: Uuid,
        after: crate::observer::Cursor,
        limit: std::num::NonZeroUsize,
        interval: Duration,
    ) -> Result<crate::observer::Page, Error> {
        self.load(id)?;
        Ok(crate::observer::Observer::new(self.owner.store(), id)
            .watch(after, limit, interval)
            .await?)
    }
    /// Admission is durable before returning. Call `resume` to drive work now;
    /// opening the database also drives all work left by a previous process.
    /// Commit a summary intent while idle. Previous snapshots remain in history.
    pub fn request_compaction(
        &mut self,
        id: Uuid,
        keep_recent_tokens: usize,
    ) -> Result<bool, Error> {
        let (mut c, version) = self.load(id)?;
        if !matches!(c.phase, Phase::Idle) || c.active.is_some() {
            return Err(Error::Invalid(
                "compaction requires an idle conversation".into(),
            ));
        }
        let Some(preparation) = crate::compact::prepare(
            &c.messages,
            c.config.model.clone(),
            c.config.max_output_tokens,
            keep_recent_tokens,
            None,
        )?
        else {
            return Ok(false);
        };
        c.phase = Phase::CompactionReady {
            preparation: Box::new(preparation),
            restarts: 0,
        };
        self.save(&c, Expected::Version(version))?;
        Ok(true)
    }
    pub fn submit(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
    ) -> Result<Uuid, Error> {
        if content.trim().is_empty() {
            return Err(Error::Invalid("empty input".into()));
        }
        loop {
            let (mut c, version) = self.load(id)?;
            if let Some(key) = request_id.as_ref()
                && let Some(s) = c
                    .submissions
                    .iter()
                    .find(|s| s.request_id.as_ref() == Some(key))
            {
                return Ok(s.id);
            }
            let sid = Uuid::new_v4();
            c.submissions.push(Submission {
                id: sid,
                request_id: request_id.clone(),
                content: content.clone(),
                status: Status::Queued,
            });
            if matches!(c.phase, Phase::FailedIdle) {
                c.phase = Phase::Idle;
            }
            match self.save(&c, Expected::Version(version)) {
                Ok(()) => return Ok(sid),
                Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                Err(e) => return Err(e),
            }
        }
    }
    /// Admit and run pending work immediately. Repeating a request ID returns
    /// the original submission, even when its answer was committed earlier.
    pub async fn submit_and_run(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
        cancel: &CancellationToken,
    ) -> Result<Uuid, Error> {
        let submission = self.submit(id, request_id, content)?;
        self.resume(cancel).await?;
        Ok(submission)
    }
    fn conversations(&self) -> Result<Vec<Uuid>, Error> {
        if self.failed
            || self.dirty.load(Ordering::Acquire)
            || self.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        let mut ids = Vec::new();
        for (_, entry) in self.owner.store().list(b"runtime/conversation/")? {
            let state: Conversation = serde_json::from_slice(&entry.value)?;
            ids.push(state.id);
        }
        Ok(ids)
    }
    fn recover(&mut self) -> Result<(), Error> {
        for id in self.conversations()? {
            self.recover_conversation(id)?;
        }
        Ok(())
    }
    fn recover_conversation(&mut self, id: Uuid) -> Result<(), Error> {
        {
            let (mut c, version) = self.load(id)?;
            match c.phase.clone() {
                Phase::ModelRunning {
                    turns,
                    request_id,
                    restarts,
                } => {
                    let n = restarts.saturating_add(1);
                    c.phase = Phase::AbandonedModel {
                        turns,
                        request_id,
                        restarts: n,
                    };
                }
                Phase::CompactionRunning {
                    preparation,
                    request_id,
                    restarts,
                } => {
                    c.phase = Phase::CompactionAbandoned {
                        preparation,
                        request_id,
                        restarts: restarts.saturating_add(1),
                    };
                }
                Phase::ToolRunning {
                    turns,
                    calls,
                    index,
                    operation_id,
                    replay,
                    arguments,
                    restarts,
                    replay_ready,
                } => {
                    let n = if replay_ready {
                        restarts
                    } else {
                        restarts.saturating_add(1)
                    };
                    let current = calls
                        .get(index)
                        .and_then(|x| self.tools.get(&x.name))
                        .map(|x| x.replay);
                    match recover_tool(
                        replay,
                        current,
                        n,
                        self.options.restart_limit,
                        self.options.uncertain_policy,
                    ) {
                        ToolRecovery::Replay => {
                            c.phase = Phase::ToolRunning {
                                turns,
                                calls,
                                index,
                                operation_id,
                                replay,
                                arguments,
                                restarts: n,
                                replay_ready: true,
                            }
                        }
                        ToolRecovery::Reconcile => {
                            c.phase = Phase::Blocked {
                                turns,
                                calls,
                                index,
                                operation_id,
                                reason: "interrupted; external outcome requires reconciliation"
                                    .into(),
                            }
                        }
                        ToolRecovery::Interrupted | ToolRecovery::RestartLimit => {
                            let reason = if n >= self.options.restart_limit {
                                "tool restart limit"
                            } else {
                                "tool interrupted and may have partially run"
                            };
                            append_tool(&mut c, &calls, index, reason.to_owned())?;
                            c.phase = Phase::Tools {
                                turns,
                                calls,
                                index: index + 1,
                            };
                        }
                    }
                }
                _ => return Ok(()),
            }
            self.save(&c, Expected::Version(version))?;
        }
        Ok(())
    }
    /// Drive committed pending work. Calling again is safe after success; after
    /// interruption the owner must be dropped and reopened before retrying.
    pub async fn resume(&mut self, cancel: &CancellationToken) -> Result<(), Error> {
        let mut guard = ResumeGuard {
            dirty: self.dirty.clone(),
            complete: false,
        };
        let ids = self.conversations()?;
        let results = futures_util::future::join_all(ids.into_iter().map(|id| {
            let mut worker = self.worker();
            async move { worker.drive(id, cancel).await }
        }))
        .await;
        let mut first_error = None;
        for result in results {
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        guard.complete = true;
        Ok(())
    }
    /// Drive one conversation, allowing hosts to work around another
    /// conversation that repeatedly fails. A failed attempt still requires
    /// reopening before further calls.
    pub async fn resume_conversation(
        &mut self,
        id: Uuid,
        cancel: &CancellationToken,
    ) -> Result<(), Error> {
        let mut guard = ResumeGuard {
            dirty: self.dirty.clone(),
            complete: false,
        };
        self.drive(id, cancel).await?;
        guard.complete = true;
        Ok(())
    }
    /// Release queued submissions held by the default failed-run policy.
    pub fn continue_queued(&mut self, id: Uuid) -> Result<(), Error> {
        let (mut c, version) = self.load(id)?;
        if matches!(c.phase, Phase::FailedIdle) {
            c.phase = Phase::Idle;
            self.save(&c, Expected::Version(version))?;
        }
        Ok(())
    }
    async fn drive(&mut self, id: Uuid, cancel: &CancellationToken) -> Result<(), Error> {
        loop {
            if cancel.is_cancelled() {
                self.failed = true;
                return Err(Error::Interrupted);
            }
            let (mut c, version) = self.load(id)?;
            match c.phase.clone() {
                Phase::Idle => {
                    let Some(i) = c
                        .submissions
                        .iter()
                        .position(|s| s.status == Status::Queued)
                    else {
                        return Ok(());
                    };
                    let s = &mut c.submissions[i];
                    s.status = Status::Running;
                    c.active = Some(s.id);
                    c.messages.push(Message::User {
                        content: s.content.clone(),
                    });
                    c.phase = Phase::ModelReady { turns: 0 };
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::CompactionReady {
                    preparation,
                    restarts,
                } => {
                    c.phase = Phase::CompactionRunning {
                        preparation,
                        request_id: Uuid::new_v4(),
                        restarts,
                    };
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::CompactionRunning {
                    preparation,
                    request_id,
                    ..
                } => {
                    let result = match (*preparation)
                        .summarize(self.options.model.as_ref(), request_id, id, cancel)
                        .await
                    {
                        Ok(value) => value,
                        Err(error) => {
                            self.failed = true;
                            return Err(error.into());
                        }
                    };
                    c.phase = Phase::CompactionAccounting {
                        result: Box::new(result),
                        request_id,
                    };
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::CompactionAbandoned {
                    preparation,
                    request_id,
                    restarts,
                } => {
                    let settlement = self
                        .options
                        .model
                        .record_usage(request_id, &UsageReport::Unconfirmed);
                    tokio::select! { biased;
                        _ = cancel.cancelled() => { self.failed = true; return Err(Error::Interrupted); },
                        result = tokio::time::timeout(Duration::from_secs(120), settlement) => match result {
                            Ok(Ok(())) => (), Ok(Err(error)) => { self.failed = true; return Err(error.into()); },
                            Err(_) => { self.failed = true; return Err(Error::Interrupted); }
                        }
                    }
                    if restarts >= self.options.restart_limit {
                        c.phase = Phase::Idle;
                    } else {
                        c.phase = Phase::CompactionReady {
                            preparation,
                            restarts,
                        };
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::CompactionAccounting { result, request_id } => {
                    let usage = result
                        .generation
                        .usage
                        .map_or(UsageReport::Unconfirmed, |usage| UsageReport::Measured {
                            usage,
                        });
                    let settlement = self.options.model.record_usage_with_metadata(
                        request_id,
                        &usage,
                        &result.generation.metadata,
                    );
                    tokio::select! { biased;
                        _ = cancel.cancelled() => { self.failed = true; return Err(Error::Interrupted); },
                        value = tokio::time::timeout(Duration::from_secs(120), settlement) => match value {
                            Ok(Ok(())) => (), Ok(Err(error)) => { self.failed = true; return Err(error.into()); },
                            Err(_) => { self.failed = true; return Err(Error::Interrupted); }
                        }
                    }
                    c.messages = result.apply(&c.messages)?;
                    c.phase = Phase::Idle;
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::ModelReady { turns } => {
                    if turns >= self.options.max_turns {
                        finish_failure(&mut c, "turn limit".into());
                    } else {
                        c.phase = Phase::ModelRunning {
                            turns,
                            request_id: Uuid::new_v4(),
                            restarts: 0,
                        };
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::ModelRunning {
                    turns, request_id, ..
                } => {
                    let mut messages = vec![Message::System {
                        content: c.config.system_prompt.clone(),
                    }];
                    messages.extend(c.messages.clone());
                    let mut specs: Vec<ToolSpec> =
                        self.tools.values().map(|t| t.tool.spec()).collect();
                    specs.sort_by(|a, b| a.name.cmp(&b.name));
                    let request =
                        model_request(&c.config.model, messages, specs, c.config.max_output_tokens);
                    let response = match collect_generation(
                        self.options
                            .model
                            .stream_in_session(request_id, id, request),
                        cancel,
                        |_| {},
                    )
                    .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            self.failed = true;
                            return Err(e.into());
                        }
                    };
                    c.phase = Phase::Accounting {
                        turns,
                        request_id,
                        response: Box::new(response),
                    };
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::AbandonedModel {
                    turns,
                    request_id,
                    restarts,
                } => {
                    let settlement = self
                        .options
                        .model
                        .record_usage(request_id, &UsageReport::Unconfirmed);
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => { self.failed = true; return Err(Error::Interrupted); },
                        result = tokio::time::timeout(Duration::from_secs(120), settlement) => match result {
                            Ok(Ok(())) => (),
                            Ok(Err(e)) => { self.failed = true; return Err(e.into()); },
                            Err(_) => { self.failed = true; return Err(Error::Interrupted); },
                        }
                    }
                    if restarts >= self.options.restart_limit {
                        finish_failure(&mut c, "model restart limit".into());
                    } else {
                        c.phase = Phase::ModelRunning {
                            turns,
                            request_id: Uuid::new_v4(),
                            restarts,
                        };
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::Accounting {
                    turns,
                    request_id,
                    response,
                } => {
                    let usage = response.usage.map_or(UsageReport::Unconfirmed, |usage| {
                        UsageReport::Measured { usage }
                    });
                    let settlement = self.options.model.record_usage_with_metadata(
                        request_id,
                        &usage,
                        &response.metadata,
                    );
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => { self.failed = true; return Err(Error::Interrupted); },
                        result = tokio::time::timeout(Duration::from_secs(120), settlement) => match result {
                            Ok(Ok(())) => (),
                            Ok(Err(e)) => { self.failed = true; return Err(e.into()); },
                            Err(_) => { self.failed = true; return Err(Error::Interrupted); },
                        }
                    }
                    if response.is_truncated() && !response.calls.is_empty() {
                        finish_failure(&mut c, "truncated tool calls".into());
                    } else {
                        c.messages.push(response.assistant_message());
                        if response.calls.is_empty() {
                            finish_answer(&mut c, response.content);
                        } else {
                            c.phase = Phase::Tools {
                                turns: turns + 1,
                                calls: response.calls,
                                index: 0,
                            };
                        }
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::Tools {
                    turns,
                    calls,
                    index,
                } => {
                    if index >= calls.len() {
                        c.phase = Phase::ModelReady { turns };
                    } else if let Some(reg) = self.tools.get(&calls[index].name) {
                        let operation_id = Uuid::new_v4();
                        let invocation = Invocation {
                            operation_id,
                            call: calls[index].clone(),
                        };
                        let prepared = crate::tool::prepare(
                            reg.tool.as_ref(),
                            &invocation,
                            cancel,
                            Duration::from_secs(120),
                            valid_schema,
                        )
                        .await;
                        if cancel.is_cancelled() {
                            self.failed = true;
                            return Err(Error::Interrupted);
                        }
                        match prepared {
                            Ok(prepared) => {
                                c.phase = Phase::ToolRunning {
                                    turns,
                                    calls,
                                    index,
                                    operation_id,
                                    replay: reg.replay,
                                    arguments: prepared.arguments().clone(),
                                    restarts: 0,
                                    replay_ready: false,
                                }
                            }
                            Err(outcome) => {
                                append_tool(&mut c, &calls, index, outcome.tool_content())?;
                                c.phase = Phase::Tools {
                                    turns,
                                    calls,
                                    index: index + 1,
                                };
                            }
                        }
                    } else {
                        append_tool(&mut c, &calls, index, "tool unavailable".into())?;
                        c.phase = Phase::Tools {
                            turns,
                            calls,
                            index: index + 1,
                        };
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::ToolRunning {
                    turns,
                    calls,
                    index,
                    operation_id,
                    arguments,
                    replay_ready,
                    ..
                } => {
                    if replay_ready {
                        if let Phase::ToolRunning { replay_ready, .. } = &mut c.phase {
                            *replay_ready = false;
                        }
                        self.save(&c, Expected::Version(version))?;
                        continue;
                    }
                    let call = calls
                        .get(index)
                        .ok_or_else(|| Error::Invalid("invalid tool index".into()))?;
                    let reply = if let Some(reg) = self.tools.get(&call.name) {
                        let invocation = Invocation {
                            operation_id,
                            call: call.clone(),
                        };
                        match crate::tool::restore(
                            reg.tool.as_ref(),
                            &invocation,
                            arguments,
                            valid_schema,
                        ) {
                            Ok(prepared) => {
                                match crate::nested::NestedHost::new(
                                    self.owner.clone(),
                                    operation_id,
                                    self.tools.values().cloned().collect(),
                                    cancel.child_token(),
                                    Duration::from_secs(120),
                                    8,
                                ) {
                                    Ok(host) => {
                                        let reply =
                                            std::panic::AssertUnwindSafe(crate::tool::execute(
                                                reg.tool.as_ref(),
                                                &invocation,
                                                prepared,
                                                c.tool_states.get(&call.name),
                                                host.clone(),
                                                cancel,
                                                Duration::from_secs(120),
                                            ))
                                            .catch_unwind()
                                            .await;
                                        match host.seal_and_wait().await {
                                            Ok(()) => match reply {
                                                Ok(reply) => reply,
                                                Err(_) => Outcome::Unknown(
                                                    "parent tool panicked after dispatch".into(),
                                                )
                                                .into(),
                                            },
                                            Err(error) => Outcome::Unknown(format!(
                                                "nested tool requires reconciliation: {error}"
                                            ))
                                            .into(),
                                        }
                                    }
                                    Err(error) => {
                                        Outcome::Unknown(format!("nested tool host: {error}"))
                                            .into()
                                    }
                                }
                            }
                            Err(outcome) => outcome.into(),
                        }
                    } else {
                        Outcome::Failed(format!("tool {} unavailable", call.name)).into()
                    };
                    if cancel.is_cancelled() {
                        self.failed = true;
                        return Err(Error::Interrupted);
                    }
                    if let Some(state) = reply.state {
                        c.tool_states.insert(call.name.clone(), state);
                    }
                    match reply.outcome {
                        Outcome::Unknown(reason) => {
                            c.phase = Phase::Blocked {
                                turns,
                                calls,
                                index,
                                operation_id,
                                reason,
                            }
                        }
                        Outcome::Pending { reference } => {
                            c.phase = Phase::Pending {
                                turns,
                                calls,
                                index,
                                operation_id,
                                reference,
                            }
                        }
                        other => {
                            append_tool(&mut c, &calls, index, other.tool_content())?;
                            c.phase = Phase::Tools {
                                turns,
                                calls,
                                index: index + 1,
                            };
                        }
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::Pending {
                    turns,
                    calls,
                    index,
                    operation_id,
                    reference,
                } => {
                    let call = calls
                        .get(index)
                        .ok_or_else(|| Error::Invalid("invalid tool index".into()))?;
                    let Some(tool) = self.tools.get(&call.name) else {
                        return Ok(());
                    };
                    let result = crate::poll::poll_once(
                        tool.tool.as_ref(),
                        operation_id,
                        &reference,
                        cancel,
                        Duration::from_secs(120),
                    )
                    .await;
                    match result {
                        crate::poll::PollResult::Outcome(Outcome::Pending { reference: next }) => {
                            if next != reference {
                                c.phase = Phase::Pending {
                                    turns,
                                    calls,
                                    index,
                                    operation_id,
                                    reference: next,
                                };
                                self.save(&c, Expected::Version(version))?;
                            }
                            return Ok(());
                        }
                        crate::poll::PollResult::Outcome(Outcome::Unknown(reason)) => {
                            c.phase = Phase::Blocked {
                                turns,
                                calls,
                                index,
                                operation_id,
                                reason,
                            };
                        }
                        crate::poll::PollResult::Outcome(outcome) => {
                            append_tool(&mut c, &calls, index, outcome.tool_content())?;
                            c.phase = Phase::Tools {
                                turns,
                                calls,
                                index: index + 1,
                            };
                        }
                        crate::poll::PollResult::Cancelled => return Err(Error::Interrupted),
                        crate::poll::PollResult::Unavailable
                        | crate::poll::PollResult::TimedOut => return Ok(()),
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::Blocked { .. } | Phase::FailedIdle => return Ok(()),
            }
        }
    }
    /// Supply an authoritative outcome for an interrupted or pending operation.
    pub fn reconcile(
        &mut self,
        id: Uuid,
        operation_id: Uuid,
        outcome: Outcome,
    ) -> Result<(), Error> {
        if !matches!(
            outcome,
            Outcome::Succeeded(_) | Outcome::SucceededWithDetails { .. } | Outcome::Failed(_)
        ) {
            return Err(Error::Invalid(
                "reconciliation requires a known outcome".into(),
            ));
        }
        let (mut c, version) = self.load(id)?;
        let (turns, calls, index, expected) = match c.phase.clone() {
            Phase::Blocked {
                turns,
                calls,
                index,
                operation_id,
                ..
            }
            | Phase::Pending {
                turns,
                calls,
                index,
                operation_id,
                ..
            } => (turns, calls, index, operation_id),
            _ => return Err(Error::Invalid("conversation is not blocked".into())),
        };
        if operation_id != expected {
            return Err(Error::Invalid("operation ID mismatch".into()));
        }
        append_tool(&mut c, &calls, index, outcome.tool_content())?;
        c.phase = Phase::Tools {
            turns,
            calls,
            index: index + 1,
        };
        self.save(&c, Expected::Version(version))
    }
    pub fn blocked(&self, id: Uuid) -> Result<Option<(Uuid, String)>, Error> {
        let (c, _) = self.load(id)?;
        Ok(match c.phase {
            Phase::Blocked {
                operation_id,
                reason,
                ..
            } => Some((operation_id, reason)),
            Phase::Pending {
                operation_id,
                reference,
                ..
            } => Some((operation_id, format!("pending: {reference}"))),
            _ => None,
        })
    }
}
/// A background scheduler. Dropping it does not cancel already spawned work;
/// those tasks retain scheduler ownership until they stop. Call `shutdown` to
/// wait for all admitted work and release ownership deterministically.
pub struct Scheduler {
    harness: Harness,
    tasks: std::collections::HashMap<Uuid, Vec<tokio::task::JoinHandle<Result<(), Error>>>>,
    locks: std::collections::HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>,
    cancel: CancellationToken,
}

impl Scheduler {
    /// Must be called inside a Tokio runtime. Recovers and schedules all
    /// persisted conversations, including work admitted by an earlier owner.
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Self, Error> {
        tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Invalid("scheduler requires a Tokio runtime".into()))?;
        let harness = Harness::open_paused(path, options)?;
        let ids = harness.conversations()?;
        let mut scheduler = Self {
            harness,
            tasks: HashMap::new(),
            locks: HashMap::new(),
            cancel: CancellationToken::new(),
        };
        for id in ids {
            scheduler.schedule(id);
        }
        Ok(scheduler)
    }
    pub fn create_conversation(&mut self, config: Config) -> Result<Uuid, Error> {
        self.harness.create_conversation(config)
    }
    pub fn fork(&mut self, id: Uuid) -> Result<Uuid, Error> {
        self.harness.fork(id)
    }
    pub fn conversation(&self, id: Uuid) -> Result<Conversation, Error> {
        self.harness.conversation(id)
    }
    pub fn history(
        &self,
        id: Uuid,
        after: crate::observer::Cursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<crate::observer::Page, Error> {
        self.harness.history(id, after, limit)
    }
    pub async fn watch_history(
        &self,
        id: Uuid,
        after: crate::observer::Cursor,
        limit: std::num::NonZeroUsize,
        interval: Duration,
    ) -> Result<crate::observer::Page, Error> {
        self.harness.watch_history(id, after, limit, interval).await
    }
    pub fn request_compaction(
        &mut self,
        id: Uuid,
        keep_recent_tokens: usize,
    ) -> Result<bool, Error> {
        let queued = self.harness.request_compaction(id, keep_recent_tokens)?;
        if queued {
            self.schedule(id);
        }
        Ok(queued)
    }
    /// Commits the admission before scheduling, including duplicate request IDs.
    pub fn submit(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
    ) -> Result<Uuid, Error> {
        let submission = self.harness.submit(id, request_id, content)?;
        self.schedule(id);
        Ok(submission)
    }
    pub fn blocked(&self, id: Uuid) -> Result<Option<(Uuid, String)>, Error> {
        self.harness.blocked(id)
    }
    /// Record a known tool outcome and resume the conversation.
    pub fn reconcile(
        &mut self,
        id: Uuid,
        operation_id: Uuid,
        outcome: Outcome,
    ) -> Result<(), Error> {
        self.harness.reconcile(id, operation_id, outcome)?;
        self.schedule(id);
        Ok(())
    }
    /// Release inputs held by the failed-run policy and schedule them.
    pub fn continue_queued(&mut self, id: Uuid) -> Result<(), Error> {
        self.harness.continue_queued(id)?;
        self.schedule(id);
        Ok(())
    }
    /// Schedule a retry after an execution error or a dropped worker.
    pub fn schedule(&mut self, id: Uuid) {
        let lock = self.locks.entry(id).or_default().clone();
        let mut worker = self.harness.worker();
        let cancel = self.cancel.clone();
        let task = tokio::spawn(async move {
            let _lock = lock.lock().await;
            worker.recover_conversation(id)?;
            loop {
                if let Err(error) = worker.drive(id, &cancel).await {
                    let mut fresh = worker.worker();
                    let retryable = matches!(error, Error::Model(_))
                        && fresh
                            .conversation(id)
                            .is_ok_and(|c| matches!(c.phase, Phase::ModelRunning { .. }));
                    if !retryable {
                        return Err(error);
                    }
                    tokio::select! {
                        _ = cancel.cancelled() => return Err(Error::Interrupted),
                        _ = tokio::time::sleep(Duration::from_secs(2)) => (),
                    }
                    fresh.recover_conversation(id)?;
                    worker = fresh;
                    continue;
                }
                let state = worker.conversation(id)?;
                let should_poll = match &state.phase {
                    Phase::Pending { calls, index, .. } => calls
                        .get(*index)
                        .and_then(|call| worker.tools.get(&call.name))
                        .is_some_and(|tool| tool.tool.can_poll()),
                    _ => false,
                };
                if !should_poll {
                    return Ok(());
                }
                tokio::select! {
                    _ = cancel.cancelled() => return Err(Error::Interrupted),
                    _ = tokio::time::sleep(Duration::from_secs(1)) => (),
                }
            }
        });
        self.tasks.entry(id).or_default().push(task);
    }
    /// Wait for the most recently scheduled execution of a conversation.
    pub async fn wait(&mut self, id: Uuid) -> Result<(), Error> {
        if let Some(tasks) = self.tasks.get_mut(&id) {
            while !tasks.is_empty() {
                let result = (&mut tasks[0])
                    .await
                    .map_err(|e| Error::Invalid(format!("scheduler task failed: {e}")))?;
                tasks.remove(0);
                result?;
            }
        }
        self.tasks.remove(&id);
        Ok(())
    }
    /// Stop workers at cancellation points and wait for them before returning.
    pub async fn shutdown(mut self) -> Result<(), Error> {
        self.cancel.cancel();
        let tasks = std::mem::take(&mut self.tasks);
        let mut error = None;
        for task in tasks.into_values().flatten() {
            match task.await {
                Ok(Ok(())) | Ok(Err(Error::Interrupted)) => (),
                Ok(Err(e)) => {
                    error.get_or_insert(e);
                }
                Err(e) => {
                    error.get_or_insert(Error::Invalid(format!("scheduler task failed: {e}")));
                }
            }
        }
        error.map_or(Ok(()), Err)
    }
}

fn admission_only(base: &Conversation, latest: &Conversation) -> Result<bool, Error> {
    let mut old = serde_json::to_value(base)?;
    let mut new = serde_json::to_value(latest)?;
    let old_submissions = old
        .as_object_mut()
        .and_then(|v| v.remove("submissions"))
        .ok_or_else(|| Error::Invalid("invalid baseline".into()))?;
    let new_submissions = new
        .as_object_mut()
        .and_then(|v| v.remove("submissions"))
        .ok_or_else(|| Error::Invalid("invalid snapshot".into()))?;
    let (Some(old_submissions), Some(new_submissions)) =
        (old_submissions.as_array(), new_submissions.as_array())
    else {
        return Err(Error::Invalid("invalid submissions".into()));
    };
    Ok(old == new && new_submissions.starts_with(old_submissions))
}

fn append_tool(
    c: &mut Conversation,
    calls: &[ToolCall],
    index: usize,
    content: String,
) -> Result<(), Error> {
    let call = calls
        .get(index)
        .ok_or_else(|| Error::Invalid("invalid tool index".into()))?;
    c.messages.push(Message::Tool {
        call_id: call.id.clone(),
        content,
    });
    Ok(())
}
fn finish_answer(c: &mut Conversation, answer: String) {
    if let Some(id) = c.active.take()
        && let Some(s) = c.submissions.iter_mut().find(|s| s.id == id)
    {
        s.status = Status::Done { answer };
    }
    c.phase = Phase::Idle;
}
fn finish_failure(c: &mut Conversation, reason: String) {
    if matches!(
        c.messages.last(),
        Some(Message::User { .. } | Message::UserWithImages { .. } | Message::Tool { .. })
    ) {
        c.messages.push(Message::Assistant {
            content: format!("[Run ended: {reason}]"),
            calls: Vec::new(),
            replay: None,
        });
    }
    if let Some(id) = c.active.take()
        && let Some(s) = c.submissions.iter_mut().find(|s| s.id == id)
    {
        s.status = Status::Unanswered { reason };
    }
    c.phase = Phase::FailedIdle;
}

fn valid_schema(schema: &serde_json::Value, arguments: &serde_json::Value) -> bool {
    jsonschema::validator_for(schema).is_ok_and(|v| v.is_valid(arguments))
}

#[cfg(test)]
mod scheduler_tests {
    use super::*;
    use futures_util::{StreamExt, stream};
    use pi_rs_core::{ModelEvent, ModelRequest, ModelStream};
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Semaphore;

    struct GatedModel {
        started: Arc<Semaphore>,
        release: Arc<Semaphore>,
        active: AtomicUsize,
        maximum: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl Model for GatedModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream, pi_rs_core::Error> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(stream::iter(vec![
                Ok(ModelEvent::Text("ok".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])
            .boxed())
        }
    }
    fn options(model: Arc<GatedModel>) -> Options {
        Options {
            model,
            tools: vec![],
            restart_limit: 3,
            max_turns: 3,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        }
    }
    fn config() -> Config {
        Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        }
    }
    #[test]
    fn conflict_merge_rejects_non_admission_state_changes() {
        let base = Conversation {
            id: Uuid::new_v4(),
            config: config(),
            messages: vec![],
            submissions: vec![],
            tool_states: HashMap::new(),
            active: None,
            phase: Phase::Idle,
        };
        let mut latest = base.clone();
        latest.submissions.push(Submission {
            id: Uuid::new_v4(),
            request_id: None,
            content: "queued".into(),
            status: Status::Queued,
        });
        assert!(admission_only(&base, &latest).unwrap());
        latest.phase = Phase::ModelReady { turns: 0 };
        assert!(!admission_only(&base, &latest).unwrap());
    }
    #[tokio::test]
    async fn concurrent_conversations_and_admission_during_execution() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut s = Scheduler::open(&path, options(model.clone())).unwrap();
        let a = s.create_conversation(config()).unwrap();
        let b = s.create_conversation(config()).unwrap();
        s.submit(a, None, "one".into()).unwrap();
        s.submit(b, None, "two".into()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), model.started.acquire_many(2))
            .await
            .unwrap()
            .unwrap()
            .forget();
        s.submit(a, Some("second".into()), "three".into()).unwrap();
        assert_eq!(model.maximum.load(Ordering::SeqCst), 2);
        model.release.add_permits(3);
        s.wait(a).await.unwrap();
        s.wait(b).await.unwrap();
        assert_eq!(s.conversation(a).unwrap().submissions.len(), 2);
        assert!(
            s.conversation(a)
                .unwrap()
                .submissions
                .iter()
                .all(|x| matches!(x.status, Status::Done { .. }))
        );
        assert_eq!(model.maximum.load(Ordering::SeqCst), 2);
        s.shutdown().await.unwrap();
    }
    struct FailingModel;
    #[async_trait::async_trait]
    impl Model for FailingModel {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream, pi_rs_core::Error> {
            if request
                .messages
                .iter()
                .any(|message| matches!(message, Message::User { content } if content == "fail"))
            {
                return Err(pi_rs_core::Error::Model("deliberate failure".into()));
            }
            Ok(stream::iter(vec![
                Ok(ModelEvent::Text("ok".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])
            .boxed())
        }
    }
    #[tokio::test]
    async fn errors_do_not_stop_other_conversations() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            model: Arc::new(FailingModel),
            tools: vec![],
            restart_limit: 3,
            max_turns: 3,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        };
        let mut s = Scheduler::open(dir.path().join("db"), opts).unwrap();
        let a = s.create_conversation(config()).unwrap();
        let b = s.create_conversation(config()).unwrap();
        s.submit(a, None, "fail".into()).unwrap();
        s.submit(b, None, "ok".into()).unwrap();
        s.wait(b).await.unwrap();
        assert!(matches!(
            s.conversation(b).unwrap().submissions[0].status,
            Status::Done { .. }
        ));
        s.wait(a).await.unwrap();
        assert!(matches!(
            s.conversation(a).unwrap().submissions[0].status,
            Status::Unanswered { .. }
        ));
        s.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn dropped_wait_keeps_worker_and_owner_alive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut s = Scheduler::open(&path, options(model.clone())).unwrap();
        let id = s.create_conversation(config()).unwrap();
        s.submit(id, None, "one".into()).unwrap();
        model.started.acquire().await.unwrap().forget();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), s.wait(id))
                .await
                .is_err()
        );
        assert!(matches!(
            Harness::open_paused(&path, options(model.clone())),
            Err(Error::Owner(OwnerError::Busy))
        ));
        model.release.add_permits(1);
        s.wait(id).await.unwrap();
        assert!(matches!(
            s.conversation(id).unwrap().submissions[0].status,
            Status::Done { .. }
        ));
        s.shutdown().await.unwrap();
        assert!(Harness::open_paused(&path, options(model)).is_ok());
    }
}
