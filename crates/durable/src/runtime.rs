//! Durable conversation execution with a single local scheduler owner.
//!
//! A committed phase is the authority for recovery. External calls never run
//! inside storage transactions. This first runtime executes one conversation at
//! a time; it does not implement upstream's parallel task graph or watch API.
use crate::{
    model::{CompletedGeneration, collect_generation, model_request},
    recovery::{ReplayPolicy, ToolRecovery, UncertainPolicy, recover_tool},
};
use durable_store::{
    Check, Expected, Mutation,
    owner::{OwnedStore, OwnerError},
};
use pi_rs_core::{
    CancellationToken, ExtensionState, Invocation, Message, Model, Outcome, Tool, ToolCall,
    ToolHost, ToolSpec, UsageReport,
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
    owner: OwnedStore,
    options: Options,
    tools: HashMap<String, RegisteredTool>,
    failed: bool,
    dirty: Arc<AtomicBool>,
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
            owner: OwnedStore::open(path)?,
            options,
            tools,
            failed: false,
            dirty: Arc::new(AtomicBool::new(false)),
        };
        harness.recover()?;
        Ok(harness)
    }
    fn key(id: Uuid) -> Vec<u8> {
        format!("runtime/conversation/{id}").into_bytes()
    }
    fn load(&self, id: Uuid) -> Result<(Conversation, u64), Error> {
        if self.failed || self.dirty.load(Ordering::Acquire) {
            return Err(Error::Failed);
        }
        let entry = self
            .owner
            .store()
            .get(&Self::key(id))?
            .ok_or(Error::NotFound)?;
        Ok((serde_json::from_slice(&entry.value)?, entry.version))
    }
    fn save(&mut self, state: &Conversation, expected: Expected) -> Result<(), Error> {
        if self.failed || self.dirty.load(Ordering::Acquire) {
            return Err(Error::Failed);
        }
        let key = Self::key(state.id);
        let value = serde_json::to_vec(state)?;
        if let Err(e) = self.owner.store().commit(
            &[Check {
                key: key.clone(),
                expected,
            }],
            &[Mutation::Put { key, value }],
        ) {
            self.failed = true;
            return Err(e.into());
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
    pub fn conversation(&self, id: Uuid) -> Result<Conversation, Error> {
        self.load(id).map(|x| x.0)
    }
    /// Admission is durable before returning. Call `resume` to drive work now;
    /// opening the database also drives all work left by a previous process.
    pub fn submit(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
    ) -> Result<Uuid, Error> {
        if content.trim().is_empty() {
            return Err(Error::Invalid("empty input".into()));
        }
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
            request_id,
            content,
            status: Status::Queued,
        });
        if matches!(c.phase, Phase::FailedIdle) {
            c.phase = Phase::Idle;
        }
        self.save(&c, Expected::Version(version))?;
        Ok(sid)
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
        if self.failed || self.dirty.load(Ordering::Acquire) {
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
                _ => continue,
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
        for id in self.conversations()? {
            self.drive(id, cancel).await?;
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
                                crate::tool::execute(
                                    reg.tool.as_ref(),
                                    &invocation,
                                    prepared,
                                    c.tool_states.get(&call.name),
                                    Arc::new(NoNestedTools),
                                    cancel,
                                    Duration::from_secs(120),
                                )
                                .await
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
                            c.phase = Phase::Blocked {
                                turns,
                                calls,
                                index,
                                operation_id,
                                reason: format!("pending: {reference}"),
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
        let Phase::Blocked {
            turns,
            calls,
            index,
            operation_id: expected,
            ..
        } = c.phase.clone()
        else {
            return Err(Error::Invalid("conversation is not blocked".into()));
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
            _ => None,
        })
    }
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

struct NoNestedTools;
#[async_trait::async_trait]
impl ToolHost for NoNestedTools {
    fn tools(&self) -> Vec<ToolSpec> {
        Vec::new()
    }
    async fn call(&self, _name: &str, _arguments: serde_json::Value) -> Outcome {
        Outcome::Failed("nested tool calls are not supported by this runtime".into())
    }
}
fn valid_schema(schema: &serde_json::Value, arguments: &serde_json::Value) -> bool {
    jsonschema::validator_for(schema).is_ok_and(|v| v.is_valid(arguments))
}
