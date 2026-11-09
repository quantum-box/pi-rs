use std::collections::{HashSet, VecDeque};
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::extension::{RegisteredTool, hook};
use crate::journal::{conversation, conversation_entries, ensure_settled};
use crate::{
    AgentInput, CancellationToken, Compaction, Decision, Error, Event, ExtensionStates, InputKind,
    Invocation, Message, Model, ModelEvent, ModelFailure, ModelMetadata, ModelRequest,
    OperationState, Outcome, QueuedMessage, Record, RecordedReply, Registry, RequestPurpose,
    Result, RunState, Session, ToolContext, ToolExecution, ToolHost, ToolReply, ToolSpec, Usage,
    UsageReport,
};

/// Shared switch checked before each automatic context compaction.
#[derive(Clone, Debug)]
pub struct AutoCompactionControl {
    enabled: Arc<AtomicBool>,
    tokens: Arc<RwLock<CompactionTokens>>,
}

/// Explicit token settings. None preserves the host's existing bounded policy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CompactionTokens {
    pub reserve_tokens: Option<u64>,
    pub keep_recent_tokens: Option<u64>,
}

impl CompactionTokens {
    /// Standard Pi compaction policy. Low-level hosts may retain None for custom policy.
    pub const fn pi_defaults() -> Self {
        Self {
            reserve_tokens: Some(16_384),
            keep_recent_tokens: Some(20_000),
        }
    }
}

impl AutoCompactionControl {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled: Arc::new(AtomicBool::new(enabled)),
            tokens: Arc::new(RwLock::new(CompactionTokens::default())),
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Release);
    }

    pub fn token_settings(&self) -> CompactionTokens {
        *self.tokens.read().expect("compaction settings")
    }

    /// Select complete recent turns using the configured token target.
    pub fn retained_turns(&self, session: &Session, fallback: usize) -> Result<usize> {
        match self.token_settings().keep_recent_tokens {
            Some(target) => retained_turn_count(session, Some(target)),
            None => Ok(fallback),
        }
    }

    pub fn set_token_settings(&self, settings: CompactionTokens) {
        *self.tokens.write().expect("compaction settings") = settings;
    }
}

impl Default for AutoCompactionControl {
    fn default() -> Self {
        Self::new(true)
    }
}

#[derive(Clone, Debug)]
pub struct Limits {
    pub model_requests: usize,
    pub output_tokens_per_request: u32,
    pub output_tokens_per_run: u32,
    pub model_timeout: Duration,
    pub tool_timeout: Duration,
    /// Explicit input/output context capacity. Otherwise use provider discovery when available.
    pub context_window: Option<u32>,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            model_requests: 12,
            output_tokens_per_request: 2048,
            output_tokens_per_run: 8192,
            model_timeout: Duration::from_secs(120),
            tool_timeout: Duration::from_secs(30),
            context_window: None,
        }
    }
}

pub struct Agent<'a> {
    pub model: &'a dyn Model,
    pub model_name: &'a str,
    pub registry: &'a Registry,
    pub system_prompt: &'a str,
    pub limits: Limits,
}

pub enum Notification<'a> {
    Retry(crate::RetryEvent),
    Committed(&'a Record),
    /// Final logical context after extensions, before provider encoding. May contain sensitive data.
    Request(&'a ModelRequest),
    /// Transient text. Only a fully completed response is committed.
    Text(&'a str),
}

pub type Observer<'a> = dyn for<'event> FnMut(Notification<'event>) + Send + 'a;

impl Agent<'_> {
    /// A fresh prompt appends a user message. Resume starts from the existing branch.
    /// Follow-ups are consumed at safe boundaries, in FIFO order. The sender owns
    /// any input left in the receiver when this call returns.
    pub async fn run(
        &self,
        session: &mut Session,
        input: Option<String>,
        followups: &mut mpsc::Receiver<String>,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<String> {
        self.run_correlated(session, Uuid::new_v4(), input, followups, cancel, observer)
            .await
    }

    /// Move queued input into a settled session before starting a queued continuation.
    pub fn apply_queued_inputs(
        &self,
        session: &mut Session,
        observer: &mut Observer<'_>,
    ) -> Result<bool> {
        ensure_settled(session.operations())?;
        apply_inputs(session, true, observer)
    }

    /// A repeated initial submission never starts a second run. Passing no
    /// input is an explicit resume request authorized by the host.
    pub async fn run_correlated<T: Into<AgentInput> + Send>(
        &self,
        session: &mut Session,
        run_id: Uuid,
        input: Option<String>,
        followups: &mut mpsc::Receiver<T>,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<String> {
        if input.as_ref().is_some_and(|text| text.trim().is_empty()) {
            return Err(Error::Config("empty user input".into()));
        }
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let recorded = session
            .records()
            .iter()
            .any(|record| record.run_id == Some(run_id));
        if recorded {
            let branch = session.branch();
            if input.is_some() || !branch.iter().any(|record| record.run_id == Some(run_id)) {
                return Err(Error::DuplicateRun(run_id));
            }
            if branch.iter().rev().find_map(|record| match record.event {
                Event::Run { .. } => record.run_id,
                _ => None,
            }) != Some(run_id)
            {
                return Err(Error::DuplicateRun(run_id));
            }
        }
        session.set_run_id(Some(run_id));
        let result = self
            .run_turns(session, run_id, input, followups, cancel, observer)
            .await;
        session.set_run_id(None);
        result
    }

    async fn run_turns<T: Into<AgentInput> + Send>(
        &self,
        session: &mut Session,
        run_id: Uuid,
        input: Option<String>,
        followups: &mut mpsc::Receiver<T>,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<String> {
        let branch = session.branch();
        let accepted = branch.iter().any(
            |record| matches!(record.event, Event::User { input_id: Some(id), .. } | Event::UserWithImages { input_id: Some(id), .. } if id == run_id),
        );
        let pending_initial = if accepted {
            None
        } else {
            branch.iter().find_map(|record| match &record.event {
                Event::Run { initial, .. } if record.run_id == Some(run_id) => initial.clone(),
                _ => None,
            })
        };
        let effective_input = input
            .clone()
            .or(pending_initial)
            .map(|content| (run_id, content));
        commit(
            session,
            Event::Run {
                state: RunState::Running,
                initial: input,
            },
            observer,
        )?;
        let mut retry_attempt = 0;
        let mut retry_observer = |event: Notification<'_>| {
            match &event {
                Notification::Retry(crate::RetryEvent::Start { attempt, .. }) => {
                    retry_attempt = *attempt
                }
                Notification::Retry(crate::RetryEvent::End { .. }) => retry_attempt = 0,
                _ => {}
            }
            observer(event);
        };
        let mut result = self
            .drive(
                session,
                run_id,
                effective_input,
                followups,
                cancel,
                &mut retry_observer,
            )
            .await;
        if retry_attempt > 0 {
            observer(Notification::Retry(crate::RetryEvent::End {
                success: result.is_ok(),
                attempt: retry_attempt,
                final_error: result.as_ref().err().map(|error| {
                    if matches!(error, Error::Cancelled) {
                        "Retry cancelled".into()
                    } else {
                        error.to_string()
                    }
                }),
            }));
        }
        if let Err(error) = queue_inputs(session, followups, observer)
            && result.is_ok()
        {
            result = Err(error);
        }
        let state = match &result {
            Ok(_) => RunState::Completed,
            Err(Error::Cancelled) => RunState::Cancelled,
            Err(Error::Reconciliation(_)) => RunState::WaitingForReconciliation,
            Err(Error::PendingOperation(_)) => RunState::WaitingForOperation,
            Err(_) => RunState::Failed,
        };
        commit(
            session,
            Event::Run {
                state,
                initial: None,
            },
            observer,
        )?;
        result
    }

    async fn drive<T: Into<AgentInput> + Send>(
        &self,
        session: &mut Session,
        run_id: Uuid,
        input: Option<(Uuid, String)>,
        followups: &mut mpsc::Receiver<T>,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<String> {
        let recovery = recover(session, observer);
        let mut states = match recovery {
            Ok(()) => self.migrate(session, observer)?,
            Err(Error::PendingOperation(_)) => {
                if let Some((invocation, _)) =
                    session
                        .operations()
                        .into_iter()
                        .find(|(invocation, state)| {
                            matches!(state, OperationState::Finished(Outcome::Pending { .. }))
                                && !self
                                    .registry
                                    .tools
                                    .get(&invocation.call.name)
                                    .is_some_and(|tool| tool.tool.can_poll())
                        })
                {
                    return Err(Error::PendingOperation(invocation.operation_id));
                }
                let mut states = self.migrate(session, observer)?;
                self.poll_pending(session, &mut states, cancel, observer)
                    .await?;
                recover(session, observer)?;
                states
            }
            Err(error) => return Err(error),
        };
        if let Some((id, content)) = input {
            commit(
                session,
                Event::User {
                    content,
                    input_id: Some(id),
                },
                observer,
            )?;
        }

        let mut pending = pending_reply(session, run_id);
        queue_inputs(session, followups, observer)?;
        if pending.is_none() {
            apply_inputs(session, false, observer)?;
        }
        if let Some(answer) = session
            .branch()
            .iter()
            .rev()
            .find_map(|record| match &record.event {
                Event::User { .. } | Event::UserWithImages { .. } => Some(None),
                Event::Assistant {
                    content,
                    invocations,
                    ..
                } => Some(invocations.is_empty().then(|| content.clone())),
                _ => None,
            })
            .flatten()
        {
            if !apply_inputs(session, true, observer)? {
                return Ok(answer);
            }
        } else if !session.branch().iter().any(|record| {
            matches!(
                record.event,
                Event::User { .. } | Event::UserWithImages { .. }
            )
        }) {
            apply_inputs(session, true, observer)?;
        }

        let mut budget = self.limits.output_tokens_per_run;
        let mut compacted = false;
        let mut retry_attempt = 0;
        for request_index in 0..self.limits.model_requests {
            queue_inputs(session, followups, observer)?;
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let (reply, advertised, allowance) = if let Some((request, metadata, usage, recorded)) =
                pending.take()
            {
                self.settle_usage(request, &usage, &metadata).await?;
                let allowance = recorded
                    .max_output_tokens
                    .min(self.limits.output_tokens_per_request)
                    .min(budget);
                let measured = match usage {
                    UsageReport::Measured { usage } => Some(usage),
                    UsageReport::Estimated { output_tokens }
                        if output_tokens <= u64::from(allowance) =>
                    {
                        None
                    }
                    _ => return Err(Error::Budget),
                };
                (
                    Reply {
                        request_id: request,
                        content: recorded.content,
                        invocations: recorded.invocations,
                        usage: measured,
                    },
                    recorded.advertised_tools.into_iter().collect(),
                    allowance,
                )
            } else {
                apply_inputs(session, false, observer)?;
                let max_output_tokens = self.limits.output_tokens_per_request.min(budget);
                if max_output_tokens == 0 {
                    return Err(Error::Budget);
                }
                let mut request = self
                    .request(
                        session,
                        &states,
                        max_output_tokens,
                        None,
                        RequestPurpose::Turn,
                        cancel,
                    )
                    .await?;
                // A context extension can lower the budget but cannot increase the host's limit.
                request.max_output_tokens = request.max_output_tokens.min(max_output_tokens);
                if request.max_output_tokens == 0 {
                    return Err(Error::Budget);
                }
                let capacity = self.context_capacity(&request, cancel).await?;
                let estimate = session.context_usage_estimate();
                let input_tokens = if estimate.last_usage_record.is_some() {
                    estimate.tokens
                } else {
                    crate::estimate_request_tokens(&request)
                };
                let exceeds = capacity.is_some_and(|window| {
                    input_tokens.saturating_add(u64::from(request.max_output_tokens))
                        > u64::from(window)
                });
                let control = session.auto_compaction_control();
                let tokens = control.token_settings();
                let retention = configured_retention(session, tokens.keep_recent_tokens, 1)?;
                let reserve_reached =
                    capacity
                        .zip(tokens.reserve_tokens)
                        .is_some_and(|(window, reserve)| {
                            input_tokens > u64::from(window).saturating_sub(reserve)
                        });
                let has_prefix = reserve_reached && {
                    // Metadata alone does not provide anything to summarize.
                    has_summary_prefix(session, retention.boundary)
                };
                if exceeds
                    || (control.enabled()
                        && !compacted
                        && budget > 1
                        && request_index + 1 < self.limits.model_requests
                        && reserve_reached
                        && has_prefix)
                {
                    if !control.enabled() {
                        return Err(Error::Model(
                            "context does not fit and automatic compaction is disabled".into(),
                        ));
                    }
                    if compacted || request_index + 1 >= self.limits.model_requests {
                        return Err(Error::Model(
                            "context does not fit after the bounded compaction attempt".into(),
                        ));
                    }
                    let summary_limit = max_output_tokens.min(512).min(budget / 2);
                    let spent = self
                        .summarize_prefix(
                            session,
                            &states,
                            SummaryScope {
                                retention,
                                instructions: None,
                                output_limit: summary_limit,
                                cancellation: cancel,
                            },
                            observer,
                            Some(followups),
                        )
                        .await?;
                    budget -= spent;
                    states = session.extension_states();
                    compacted = true;
                    continue;
                }
                let advertised: HashSet<_> =
                    request.tools.iter().map(|spec| spec.name.clone()).collect();
                let allowance = request.max_output_tokens;
                let result = self
                    .reply(session, request, true, cancel, observer, Some(followups))
                    .await?
                    .outcome;
                let control = session.retry_control();
                let settings = control.settings();
                if let Err(Error::RetryableModel(message)) = &result
                    && settings.enabled
                    && retry_attempt < settings.max_retries
                    && request_index + 1 < self.limits.model_requests
                {
                    retry_attempt += 1;
                    let delay_ms = settings.delay_ms(retry_attempt);
                    let retry_wait = control.begin_wait();
                    observer(Notification::Retry(crate::RetryEvent::Start {
                        attempt: retry_attempt,
                        max_attempts: settings.max_retries,
                        delay_ms,
                        error_message: message.clone(),
                    }));
                    let sleep = tokio::time::sleep(Duration::from_millis(delay_ms));
                    tokio::pin!(sleep);
                    let waited = loop {
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => break Err(Error::Cancelled),
                            _ = retry_wait.cancel.cancelled() => break Err(Error::Cancelled),
                            _ = &mut sleep => break Ok(()),
                            Some(input) = followups.recv() => {
                                if let Err(error) = queue_input(session, input.into(), observer) { break Err(error); }
                            }
                        }
                    };
                    drop(retry_wait);
                    waited?;
                    continue;
                }
                let reply = result?;
                if retry_attempt > 0 {
                    observer(Notification::Retry(crate::RetryEvent::End {
                        success: true,
                        attempt: retry_attempt,
                        final_error: None,
                    }));
                    retry_attempt = 0;
                }
                (reply, advertised, allowance)
            };
            let finished = reply.invocations.is_empty();
            let answer = finished.then(|| reply.content.clone());
            let invocations = reply.invocations.clone();
            let output_tokens = reply
                .usage
                .map_or(u64::from(allowance), |usage| usage.output_tokens);
            if allowance == 0 || output_tokens > u64::from(allowance) {
                return Err(Error::Budget);
            }
            budget -= output_tokens as u32;
            commit(
                session,
                Event::Assistant {
                    content: reply.content,
                    invocations: reply.invocations,
                    usage: reply.usage,
                    request_id: Some(reply.request_id),
                },
                observer,
            )?;

            if let Err(err) = self
                .tool_batch(
                    session,
                    &invocations,
                    &DispatchScope {
                        cancellation: cancel,
                        advertised: &advertised,
                        depth: 0,
                        call_count: Arc::new(AtomicUsize::new(0)),
                    },
                    &mut states,
                    observer,
                    Some(followups),
                )
                .await
            {
                recover_unstarted(session, observer)?;
                return Err(err);
            }

            if let Some(answer) = answer {
                let assistant_record = session.head();
                commit(session, Event::TurnFinished { assistant_record }, observer)?;
                queue_inputs(session, followups, observer)?;
                if cancel.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                if apply_inputs(session, false, observer)? {
                    continue;
                }
                if !apply_inputs(session, true, observer)? {
                    return Ok(answer);
                }
            }
        }
        Err(Error::Budget)
    }

    fn migrate(
        &self,
        session: &mut Session,
        observer: &mut Observer<'_>,
    ) -> Result<ExtensionStates> {
        let mut states = session.extension_states();
        let mut changes = ExtensionStates::new();
        for extension in &self.registry.extensions {
            let previous = states.get(extension.id());
            if let Some(state) = hook(extension.as_ref(), extension.migrate(previous))? {
                changes.insert(extension.id().into(), state);
            }
        }
        if !changes.is_empty() {
            states.extend(changes.clone());
            commit(
                session,
                Event::ExtensionStates { states: changes },
                observer,
            )?;
        }
        Ok(states)
    }

    async fn request(
        &self,
        session: &mut Session,
        states: &ExtensionStates,
        max_output_tokens: u32,
        before: Option<u64>,
        purpose: RequestPurpose,
        cancel: &CancellationToken,
    ) -> Result<ModelRequest> {
        let branch = session.branch();
        let summary = branch.iter().rev().find_map(|record| match &record.event {
            Event::Compacted { summary, .. } => Some(summary.as_str()),
            _ => None,
        });
        let history = match before {
            Some(before) => &branch[..branch.partition_point(|record| record.id < before)],
            None => &branch,
        };
        let (system_prompt, tools) = crate::journal::system_context(
            history,
            self.system_prompt,
            self.registry.active_tool_specs(),
        );
        let mut messages = vec![Message::System {
            content: system_prompt,
        }];
        if let Some(summary) = summary {
            // Compaction content has the authority of conversation history.
            messages.push(Message::User {
                content: format!("Prior conversation summary (context only):\n{summary}"),
            });
        }
        if let Some(boundary) = before {
            let mut keep_group = false;
            messages.extend(
                conversation_entries(&branch, session.operations())
                    .into_iter()
                    .filter_map(|(source, message)| {
                        if !matches!(message, Message::Tool { .. }) {
                            keep_group = source < boundary;
                        }
                        keep_group.then_some(message)
                    }),
            );
        } else {
            messages.extend(conversation(history, session.operations()));
        }
        if messages.len() == 1 {
            return Err(Error::Session("there is no conversation to resume".into()));
        }
        let mut request = ModelRequest {
            model: self.model_name.into(),
            messages,
            tools,
            max_output_tokens,
        };
        for extension in &self.registry.extensions {
            let result = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Error::Cancelled),
                result = tokio::time::timeout(self.limits.model_timeout, extension.before_request(&mut request, purpose, states.get(extension.id()))) =>
                    result.map_err(|_| Error::Config("request context hook timed out".into()))?,
            };
            hook(extension.as_ref(), result)?;
        }
        request
            .tools
            .retain(|spec| self.registry.has_tool(&spec.name));
        Ok(request)
    }

    async fn reply<T: Into<AgentInput> + Send>(
        &self,
        session: &mut Session,
        request: ModelRequest,
        retain_reply: bool,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
        mut inputs: Option<&mut mpsc::Receiver<T>>,
    ) -> Result<ReplyAttempt> {
        let request_id = Uuid::new_v4();
        let allowance = request.max_output_tokens;
        let advertised_tools =
            retain_reply.then(|| request.tools.iter().map(|tool| tool.name.clone()).collect());
        let context_head = session.head();
        let context_sha256 = format!("{:x}", Sha256::digest(serde_json::to_vec(&request)?));
        commit(
            session,
            Event::ModelRequested {
                model: request.model.clone(),
                messages: request.messages.len(),
                tools: request.tools.len(),
                max_output_tokens: allowance,
                request_id: Some(request_id),
                context_head: Some(context_head),
                context_sha256: Some(context_sha256),
            },
            observer,
        )?;
        observer(Notification::Request(&request));
        const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
        let mut content = String::new();
        let mut metadata = ModelMetadata::default();
        let collect = async {
            let opening = self.model.stream_with_id(request_id, request);
            tokio::pin!(opening);
            let mut stream = loop {
                tokio::select! {
                    opened = &mut opening => break opened?,
                    Some(input) = receive_input(&mut inputs) => queue_input(session, input.into(), observer)?,
                }
            };
            loop {
                let event = tokio::select! {
                    event = stream.next() => event,
                    Some(input) = receive_input(&mut inputs) => {
                        queue_input(session, input.into(), observer)?;
                        continue;
                    }
                };
                let Some(event) = event else {
                    break;
                };
                match event? {
                    ModelEvent::Text(delta) => {
                        if content.len() + delta.len() > MAX_RESPONSE_BYTES {
                            return Err(Error::Model("response exceeds size limit".into()));
                        }
                        observer(Notification::Text(&delta));
                        content.push_str(&delta);
                    }
                    ModelEvent::Metadata(update) => metadata = update,
                    ModelEvent::Complete { calls, usage } => {
                        let mut seen = HashSet::new();
                        let mut invocations = Vec::with_capacity(calls.len());
                        if calls.len() > 64 {
                            return Err(Error::Model("too many tool calls".into()));
                        }
                        for call in calls {
                            if call.id.is_empty()
                                || call.name.is_empty()
                                || !seen.insert(call.id.clone())
                            {
                                return Err(Error::Model(
                                    "invalid or duplicate tool call identifier".into(),
                                ));
                            }
                            invocations.push(Invocation {
                                operation_id: Uuid::new_v4(),
                                call,
                            });
                        }
                        return Ok(Reply {
                            request_id,
                            content: std::mem::take(&mut content),
                            invocations,
                            usage,
                        });
                    }
                }
            }
            Err(Error::Model("stream ended before completion".into()))
        };
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Error::Cancelled),
            result = tokio::time::timeout(self.limits.model_timeout, collect) =>
                result.unwrap_or_else(|_| Err(Error::RetryableModel("request timed out".into()))),
        };
        let (usage, failure) = match &result {
            Ok(reply) => (
                reply.usage.map_or(
                    UsageReport::Estimated {
                        output_tokens: allowance.into(),
                    },
                    |usage| UsageReport::Measured { usage },
                ),
                None,
            ),
            Err(error) => (
                UsageReport::Unconfirmed,
                Some(match error {
                    Error::Cancelled => ModelFailure::Cancelled,
                    Error::RetryableModel(_) => ModelFailure::Retryable,
                    _ => ModelFailure::Failed,
                }),
            ),
        };
        commit(
            session,
            Event::ModelFinished {
                request_id,
                metadata: metadata.clone(),
                usage: usage.clone(),
                failure,
                partial: content,
                reply: advertised_tools.and_then(|advertised_tools| {
                    result.as_ref().ok().map(|reply| RecordedReply {
                        content: reply.content.clone(),
                        invocations: reply.invocations.clone(),
                        advertised_tools,
                        max_output_tokens: allowance,
                    })
                }),
            },
            observer,
        )?;
        self.settle_usage(request_id, &usage, &metadata).await?;
        Ok(ReplyAttempt { outcome: result })
    }

    async fn settle_usage(
        &self,
        request_id: Uuid,
        usage: &UsageReport,
        metadata: &ModelMetadata,
    ) -> Result<()> {
        tokio::time::timeout(
            self.limits.model_timeout,
            self.model
                .record_usage_with_metadata(request_id, usage, metadata),
        )
        .await
        .map_err(|_| Error::RetryableModel("model usage settlement timed out".into()))?
    }

    async fn tool_batch<T: Into<AgentInput> + Send>(
        &self,
        session: &mut Session,
        invocations: &[Invocation],
        scope: &DispatchScope<'_>,
        states: &mut ExtensionStates,
        observer: &mut Observer<'_>,
        mut inputs: Option<&mut mpsc::Receiver<T>>,
    ) -> Result<()> {
        let cancel = scope.cancellation;
        let parallel = |inv: &Invocation| {
            self.registry
                .tools
                .get(&inv.call.name)
                .is_some_and(|tool| tool.tool.execution() == ToolExecution::ParallelRead)
        };
        let mut remaining = invocations;
        while let Some((first, rest)) = remaining.split_first() {
            // Only consecutive read-only tools overlap. Writes and job dispatches are barriers.
            let count = if parallel(first) {
                1 + rest.iter().take(7).take_while(|inv| parallel(inv)).count()
            } else {
                1
            };
            let (group, next) = remaining.split_at(count);
            let mut prepared = Vec::with_capacity(count);
            for invocation in group {
                let preparation = self.prepare_tool(invocation, scope.advertised, states, cancel);
                tokio::pin!(preparation);
                let preparation = loop {
                    tokio::select! {
                        result = &mut preparation => break result?,
                        Some(input) = receive_input(&mut inputs) => queue_input(session, input.into(), observer)?,
                    }
                };
                prepared.push(preparation);
            }
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let mut dispatches = Vec::new();
            for (invocation, preparation) in group.iter().zip(&prepared) {
                if matches!(preparation, Preparation::Ready { .. }) {
                    if cancel.is_cancelled() {
                        for operation_id in dispatches {
                            commit(
                                session,
                                Event::ToolFinished {
                                    operation_id,
                                    outcome: Outcome::Skipped(
                                        "cancelled before the parallel group dispatched".into(),
                                    ),
                                    states: ExtensionStates::new(),
                                },
                                observer,
                            )?;
                        }
                        return Err(Error::Cancelled);
                    }
                    commit(
                        session,
                        Event::ToolStarted {
                            operation_id: invocation.operation_id,
                        },
                        observer,
                    )?;
                    dispatches.push(invocation.operation_id);
                }
            }
            let (requests, mut incoming) = mpsc::channel(32);
            let specs = Arc::new(self.registry.tool_specs());
            let invocations = group.iter().zip(prepared).map(|(invocation, prepared)| {
                let cancellation = match &prepared {
                    Preparation::Ready { context, .. } => context.cancellation.clone(),
                    Preparation::Skipped(_) => cancel.child_token(),
                };
                let host = Arc::new(NestedHost {
                    requests: requests.clone(),
                    specs: specs.clone(),
                    parent_operation: invocation.operation_id,
                    cancellation,
                    depth: scope.depth,
                    call_count: scope.call_count.clone(),
                    ui: self.registry.ui.clone(),
                });
                self.invoke(prepared, cancel, host, scope.depth > 0)
            });
            let outcomes = futures_util::future::join_all(invocations);
            tokio::pin!(outcomes);
            let mut nested_error = None;
            let mut pending_requests = VecDeque::<NestedRequest>::new();
            let outcomes = loop {
                tokio::select! {
                    completed = &mut outcomes => break completed,
                    Some(input) = receive_input(&mut inputs) => queue_input(session, input.into(), observer)?,
                    Some(request) = async {
                        if pending_requests.is_empty() {
                            pending_requests.extend(incoming.recv().await?);
                        }
                        pending_requests.pop_front()
                    } => {
                        let parent = request.parent_operation;
                        let mut batch = vec![request];
                        while batch.len() < 8 {
                            if pending_requests.is_empty() {
                                match incoming.try_recv() {
                                    Ok(requests) => pending_requests.extend(requests),
                                    Err(_) => break,
                                }
                            }
                            let request = pending_requests.pop_front().expect("queued nested request");
                            if request.parent_operation != parent {
                                pending_requests.push_front(request);
                                break;
                            }
                            batch.push(request);
                        }
                        let nested = self.nested_tools(session, batch, states, observer, inputs.as_deref_mut());
                        tokio::pin!(nested);
                        tokio::select! {
                            result = &mut nested => {
                                if let Err(error) = result { nested_error.get_or_insert(error); }
                            },
                            completed = &mut outcomes => {
                                if let Err(error) = nested.await { nested_error.get_or_insert(error); }
                                break completed;
                            }
                        }
                    }
                }
            };
            let mut first_error = None;
            // All started outcomes are recorded even if one parallel member failed.
            for (invocation, outcome) in group.iter().zip(outcomes) {
                if let Err(error) = self.finish_tool(session, invocation, outcome, states, observer)
                    && (first_error.is_none()
                        || (matches!(first_error, Some(Error::PendingOperation(_)))
                            && !matches!(error, Error::PendingOperation(_))))
                {
                    first_error = Some(error);
                }
            }
            match first_error {
                Some(Error::PendingOperation(_)) => {
                    self.poll_pending(session, states, cancel, observer).await?
                }
                Some(error) => return Err(error),
                None => {}
            }
            if let Some(error) = nested_error {
                return Err(error);
            }
            remaining = next;
        }
        Ok(())
    }

    async fn nested_tools<T: Into<AgentInput> + Send>(
        &self,
        session: &mut Session,
        requests: Vec<NestedRequest>,
        states: &mut ExtensionStates,
        observer: &mut Observer<'_>,
        inputs: Option<&mut mpsc::Receiver<T>>,
    ) -> Result<()> {
        let invocations: Vec<_> = requests
            .iter()
            .map(|request| {
                let operation_id = Uuid::new_v4();
                Invocation {
                    operation_id,
                    call: crate::ToolCall {
                        id: operation_id.to_string(),
                        name: request.name.clone(),
                        arguments: request.arguments.to_string(),
                    },
                }
            })
            .collect();
        let execute = async {
            for (request, invocation) in requests.iter().zip(&invocations) {
                commit(
                    session,
                    Event::ToolPlanned {
                        invocation: invocation.clone(),
                        parent_operation: request.parent_operation,
                    },
                    observer,
                )?;
            }
            let advertised = self.registry.tools.keys().cloned().collect();
            Box::pin(self.tool_batch(
                session,
                &invocations,
                &DispatchScope {
                    cancellation: &requests[0].cancellation,
                    advertised: &advertised,
                    depth: requests[0].depth,
                    call_count: requests[0].call_count.clone(),
                },
                states,
                observer,
                inputs,
            ))
            .await
        }
        .await;
        if execute.is_err() {
            requests[0].cancellation.cancel();
        }
        for (request, invocation) in requests.into_iter().zip(invocations) {
            let operation_id = invocation.operation_id;
            let operation = session
                .operations()
                .into_iter()
                .find(|(current, _)| current.operation_id == operation_id);
            if matches!(
                operation.as_ref().map(|(_, state)| state),
                Some(OperationState::Planned)
            ) {
                let reason = execute
                    .as_ref()
                    .err()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "nested dispatch did not start".into());
                commit(
                    session,
                    Event::ToolFinished {
                        operation_id,
                        outcome: Outcome::Skipped(reason),
                        states: ExtensionStates::new(),
                    },
                    observer,
                )?;
            }
            let outcome = session
                .operations()
                .into_iter()
                .find_map(|(current, state)| {
                    if current.operation_id != operation_id {
                        return None;
                    }
                    match state {
                        OperationState::Finished(outcome) => Some(outcome),
                        _ => None,
                    }
                })
                .unwrap_or_else(|| {
                    Outcome::Unknown("nested tool produced no durable result".into())
                });
            let _ = request.reply.send(outcome);
        }
        execute
    }

    async fn poll_pending(
        &self,
        session: &mut Session,
        states: &mut ExtensionStates,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<()> {
        for (invocation, state) in session.operations() {
            let OperationState::Finished(Outcome::Pending { reference }) = state else {
                continue;
            };
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let operation_id = invocation.operation_id;
            let tool = self
                .registry
                .tools
                .get(&invocation.call.name)
                .ok_or(Error::PendingOperation(operation_id))?;
            if !tool.tool.can_poll() {
                return Err(Error::PendingOperation(operation_id));
            }
            let arguments = serde_json::from_str(&invocation.call.arguments)?;
            let arguments = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Error::Cancelled),
                result = tokio::time::timeout(self.limits.tool_timeout, tool.tool.prepare_arguments(arguments)) =>
                    result.ok().and_then(std::result::Result::ok).ok_or(Error::PendingOperation(operation_id))?,
            };
            if arguments.to_string().len() > 16 * 1024 * 1024
                || !tool.validator.is_valid(&arguments)
            {
                return Err(Error::PendingOperation(operation_id));
            }
            let context = ToolContext {
                operation_id,
                cancellation: cancel.child_token(),
            };
            if self
                .authorize(&invocation.call.name, &arguments, &context, states)
                .await?
                != Decision::Allow
            {
                return Err(Error::PendingOperation(operation_id));
            }
            let outcome = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Error::Cancelled),
                result = tokio::time::timeout(self.limits.tool_timeout, tool.tool.poll(&reference, context)) => result.ok().flatten().ok_or(Error::PendingOperation(operation_id))?,
            };
            self.finish_tool(session, &invocation, outcome.into(), states, observer)?;
        }
        Ok(())
    }

    async fn prepare_tool(
        &self,
        invocation: &Invocation,
        advertised: &HashSet<String>,
        states: &ExtensionStates,
        cancel: &CancellationToken,
    ) -> Result<Preparation<'_>> {
        let call = &invocation.call;
        let Some(tool) = self
            .registry
            .tools
            .get(&call.name)
            .filter(|_| advertised.contains(&call.name))
        else {
            return Ok(Preparation::Skipped(
                "tool is not registered or advertised".into(),
            ));
        };
        let Ok(args) = serde_json::from_str(&call.arguments) else {
            return Ok(Preparation::Skipped("arguments are not valid JSON".into()));
        };
        let args = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            result = tokio::time::timeout(self.limits.tool_timeout, tool.tool.prepare_arguments(args)) => match result {
                Ok(Ok(arguments)) if arguments.to_string().len() <= 16 * 1024 * 1024 => arguments,
                Ok(Ok(_)) => return Ok(Preparation::Skipped("prepared arguments exceed size limit".into())),
                Ok(Err(error)) => return Ok(Preparation::Skipped(format!("argument preparation failed: {error}"))),
                Err(_) => return Ok(Preparation::Skipped("argument preparation timed out".into())),
            },
        };
        if !tool.validator.is_valid(&args) {
            return Ok(Preparation::Skipped(
                "arguments do not match the tool's JSON Schema".into(),
            ));
        }
        let cancellation = cancel.child_token();
        let guard = cancellation.clone().drop_guard();
        let context = ToolContext {
            operation_id: invocation.operation_id,
            cancellation,
        };
        let decision = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            decision = tokio::time::timeout(self.limits.tool_timeout, self.authorize(&call.name, &args, &context, states)) =>
                decision.map_err(|_| Error::Config("pre-tool hooks timed out".into()))??,
        };
        Ok(match decision {
            Decision::Allow => Preparation::Ready {
                tool,
                args,
                context,
                state: states.get(&tool.owner).cloned(),
                guard,
            },
            Decision::Deny(reason) => Preparation::Skipped(reason),
        })
    }

    fn finish_tool(
        &self,
        session: &mut Session,
        invocation: &Invocation,
        reply: ToolReply,
        states: &mut ExtensionStates,
        observer: &mut Observer<'_>,
    ) -> Result<()> {
        let ToolReply {
            outcome,
            state: update,
        } = reply;
        // Programmatic results remain in the journal but are not injected into
        // model context. Leave room for a 1 MiB string and its JSON escaping.
        let max_output_bytes = if session.records().iter().any(|record| {
            matches!(&record.event, Event::ToolPlanned { invocation: nested, .. } if nested.operation_id == invocation.operation_id)
        }) {
            8 * 1024 * 1024
        } else {
            self.registry.tools.get(&invocation.call.name)
                .map_or(1024 * 1024, |tool| tool.tool.output_limit().min(8 * 1024 * 1024))
        };
        let outcome = match outcome {
            Outcome::Succeeded(output) if output.len() > max_output_bytes => Outcome::Truncated {
                original_bytes: output.len(),
            },
            outcome @ Outcome::Structured { .. } => {
                let size = outcome.tool_content().len();
                if size > max_output_bytes {
                    Outcome::Truncated {
                        original_bytes: size,
                    }
                } else {
                    outcome
                }
            }
            Outcome::Failed(reason) if reason.len() > max_output_bytes => {
                Outcome::Failed("failure detail exceeds size limit".into())
            }
            Outcome::Skipped(reason) if reason.len() > max_output_bytes => {
                Outcome::Skipped("skip detail exceeds size limit".into())
            }
            Outcome::Unknown(reason) if reason.len() > max_output_bytes => {
                Outcome::Unknown("uncertain result detail exceeds size limit".into())
            }
            Outcome::Pending { reference }
                if reference.trim().is_empty() || reference.len() > 1024 =>
            {
                Outcome::Unknown("tool returned an invalid operation reference".into())
            }
            outcome => outcome,
        };
        let operation_id = invocation.operation_id;
        let waiting = match &outcome {
            Outcome::Unknown(_) => Some(Error::Reconciliation(operation_id)),
            Outcome::Pending { .. } => Some(Error::PendingOperation(operation_id)),
            _ => None,
        };
        let changes = self.tool_state_changes(&invocation.call.name, &outcome, states, update);
        let persisted = changes.as_ref().cloned().unwrap_or_default();
        commit(
            session,
            Event::ToolFinished {
                operation_id,
                outcome,
                states: persisted,
            },
            observer,
        )?;
        if let Some(error) = waiting {
            return Err(error);
        }
        states.extend(changes?);
        Ok(())
    }

    fn tool_state_changes(
        &self,
        name: &str,
        outcome: &Outcome,
        states: &ExtensionStates,
        update: Option<crate::ExtensionState>,
    ) -> Result<ExtensionStates> {
        let mut changes = ExtensionStates::new();
        if let Some(update) = update {
            if serde_json::to_vec(&update)?.len() > 4 * 1024 * 1024 {
                return Err(Error::Config("tool state exceeds size limit".into()));
            }
            let owner = &self
                .registry
                .tools
                .get(name)
                .ok_or_else(|| Error::Config("tool state has no owning extension".into()))?
                .owner;
            changes.insert(owner.clone(), update);
        }
        for extension in &self.registry.extensions {
            if let Some(state) = hook(
                extension.as_ref(),
                extension.after_tool(
                    name,
                    outcome,
                    changes
                        .get(extension.id())
                        .or_else(|| states.get(extension.id())),
                ),
            )? {
                changes.insert(extension.id().into(), state);
            }
        }
        Ok(changes)
    }

    async fn authorize(
        &self,
        name: &str,
        args: &serde_json::Value,
        context: &ToolContext,
        states: &ExtensionStates,
    ) -> Result<Decision> {
        for extension in &self.registry.extensions {
            let decision = hook(
                extension.as_ref(),
                extension
                    .before_tool(name, args, context, states.get(extension.id()))
                    .await,
            )?;
            if decision != Decision::Allow {
                return Ok(decision);
            }
        }
        Ok(Decision::Allow)
    }

    async fn invoke(
        &self,
        preparation: Preparation<'_>,
        cancel: &CancellationToken,
        host: Arc<dyn ToolHost>,
        programmatic: bool,
    ) -> ToolReply {
        let (tool, args, context, state, _guard) = match preparation {
            Preparation::Ready {
                tool,
                args,
                context,
                state,
                guard,
            } => (tool, args, context, state, guard),
            Preparation::Skipped(reason) => return Outcome::Skipped(reason).into(),
        };
        let invocation = async {
            if programmatic {
                tool.tool
                    .invoke_programmatic(args, context, state.as_ref(), host)
                    .await
            } else {
                tool.tool
                    .invoke_transaction(args, context, state.as_ref(), host)
                    .await
            }
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Outcome::Unknown("cancelled after dispatch was committed".into()).into(),
            result = tokio::time::timeout(self.limits.tool_timeout, invocation) =>
                result.unwrap_or_else(|_| Outcome::Unknown("tool timed out".into()).into()),
        }
    }

    /// Run a host-only extension command against this branch's durable state.
    pub fn extension_command(
        &self,
        session: &mut Session,
        extension_id: &str,
        command: &str,
        arguments: &str,
        observer: &mut Observer<'_>,
    ) -> Result<()> {
        ensure_settled(session.operations())?;
        let extension = self
            .registry
            .extensions
            .iter()
            .find(|extension| {
                extension.id() == extension_id && extension.command_names().contains(&command)
            })
            .ok_or_else(|| Error::Config("unknown or disabled extension command".into()))?;
        let states = self.migrate(session, observer)?;
        let state = hook(
            extension.as_ref(),
            extension.command(command, arguments, states.get(extension_id)),
        )?;
        commit(
            session,
            Event::ExtensionStates {
                states: [(extension_id.to_string(), state)].into(),
            },
            observer,
        )
    }

    /// Replace older context with a supplied summary, preserving the original journal.
    pub fn compact(
        &self,
        session: &mut Session,
        compaction: Compaction,
        observer: &mut Observer<'_>,
    ) -> Result<()> {
        ensure_settled(session.operations())?;
        let states = self.migrate(session, observer)?;
        self.commit_compaction(session, compaction, &states, None, None, observer)
    }

    /// Commit a caller-supplied summary using configured token retention.
    pub fn compact_configured(
        &self,
        session: &mut Session,
        summary: String,
        observer: &mut Observer<'_>,
    ) -> Result<()> {
        ensure_settled(session.operations())?;
        let states = self.migrate(session, observer)?;
        let retention = configured_retention(
            session,
            session
                .auto_compaction_control()
                .token_settings()
                .keep_recent_tokens,
            1,
        )?;
        self.commit_compaction(
            session,
            Compaction {
                summary,
                keep_turns: retention.keep_turns,
            },
            &states,
            None,
            Some(retention),
            observer,
        )
    }

    /// Request a model-generated summary in a tool-free turn, then compact atomically.
    pub async fn summarize(
        &self,
        session: &mut Session,
        keep_turns: usize,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<()> {
        self.summarize_with_inputs::<AgentInput>(session, keep_turns, cancel, observer, None)
            .await
    }

    pub async fn summarize_with_inputs<T: Into<AgentInput> + Send>(
        &self,
        session: &mut Session,
        keep_turns: usize,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
        inputs: Option<&mut mpsc::Receiver<T>>,
    ) -> Result<()> {
        self.summarize_with_instructions(session, keep_turns, cancel, observer, inputs, None)
            .await
    }

    /// Summarize older turns with optional user-supplied focus instructions.
    pub async fn summarize_with_instructions<T: Into<AgentInput> + Send>(
        &self,
        session: &mut Session,
        keep_turns: usize,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
        inputs: Option<&mut mpsc::Receiver<T>>,
        instructions: Option<&str>,
    ) -> Result<()> {
        self.summarize_selected(
            session,
            Some(keep_turns),
            cancel,
            observer,
            inputs,
            instructions,
        )
        .await
    }

    /// Summarize using the session's configured token-retention boundary.
    pub async fn summarize_configured<T: Into<AgentInput> + Send>(
        &self,
        session: &mut Session,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
        inputs: Option<&mut mpsc::Receiver<T>>,
        instructions: Option<&str>,
    ) -> Result<()> {
        self.summarize_selected(session, None, cancel, observer, inputs, instructions)
            .await
    }

    async fn summarize_selected<T: Into<AgentInput> + Send>(
        &self,
        session: &mut Session,
        keep_turns: Option<usize>,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
        inputs: Option<&mut mpsc::Receiver<T>>,
        instructions: Option<&str>,
    ) -> Result<()> {
        ensure_settled(session.operations())?;
        let states = self.migrate(session, observer)?;
        let retention = match keep_turns {
            Some(turns) => RetentionPlan::turns(session, turns),
            None => configured_retention(
                session,
                session
                    .auto_compaction_control()
                    .token_settings()
                    .keep_recent_tokens,
                1,
            )?,
        };
        let limit = self
            .limits
            .output_tokens_per_request
            .min(self.limits.output_tokens_per_run);
        self.summarize_prefix(
            session,
            &states,
            SummaryScope {
                retention,
                instructions,
                output_limit: limit,
                cancellation: cancel,
            },
            observer,
            inputs,
        )
        .await?;
        Ok(())
    }

    async fn summarize_prefix<T: Into<AgentInput> + Send>(
        &self,
        session: &mut Session,
        states: &ExtensionStates,
        scope: SummaryScope<'_>,
        observer: &mut Observer<'_>,
        inputs: Option<&mut mpsc::Receiver<T>>,
    ) -> Result<u32> {
        let SummaryScope {
            retention,
            instructions,
            output_limit: limit,
            cancellation: cancel,
        } = scope;
        if limit == 0 || self.limits.model_requests == 0 {
            return Err(Error::Budget);
        }
        let before = retention.boundary;
        if !has_summary_prefix(session, before) {
            return Err(Error::Config(
                "Nothing to compact (session too small)".into(),
            ));
        }
        let mut request = self
            .request(
                session,
                states,
                limit,
                before,
                RequestPurpose::Summary,
                cancel,
            )
            .await?;
        request.max_output_tokens = request.max_output_tokens.min(limit);
        if request.max_output_tokens == 0 {
            return Err(Error::Budget);
        }
        request.tools.clear();
        request.messages.push(Message::User { content: "Summarize only this older conversation. Record confirmed facts, decisions, and explicitly unresolved items. Preserve useful tool outcomes. Do not infer the current work status or invent open tasks. Omit credentials. Return only the summary.".into() });
        if retention.split_turn
            && let Some(Message::User { content }) = request.messages.last_mut()
        {
            content.push_str("\nThe retained context begins within a turn. Preserve the user's request and completed steps needed to interpret the retained messages.");
        }
        if let Some(instructions) = instructions.filter(|value| !value.is_empty())
            && let Some(Message::User { content }) = request.messages.last_mut()
        {
            content.push_str("\n\nAdditional focus: ");
            content.push_str(instructions);
        }
        if self.exceeds_context(&request, cancel).await? {
            return Err(Error::Model(
                "older context does not fit the summary request".into(),
            ));
        }
        let allowance = request.max_output_tokens;
        let reply = self
            .reply(session, request, false, cancel, observer, inputs)
            .await?
            .outcome?;
        if !reply.invocations.is_empty() {
            return Err(Error::Model(
                "summary response attempted a tool call".into(),
            ));
        }
        let output = reply
            .usage
            .map_or(u64::from(allowance), |usage| usage.output_tokens);
        if output > u64::from(allowance) {
            return Err(Error::Budget);
        }
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        self.commit_compaction(
            session,
            Compaction {
                summary: reply.content,
                keep_turns: retention.keep_turns,
            },
            states,
            reply.usage,
            Some(retention),
            observer,
        )?;
        Ok(output as u32)
    }

    async fn context_capacity(
        &self,
        request: &ModelRequest,
        cancel: &CancellationToken,
    ) -> Result<Option<u32>> {
        let window = match self.limits.context_window {
            Some(window) => Some(window),
            None => tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Error::Cancelled),
                result = tokio::time::timeout(self.limits.model_timeout, self.model.context_window(&request.model)) => result.ok().flatten().filter(|window| *window > 0),
            },
        };
        Ok(window)
    }

    async fn exceeds_context(
        &self,
        request: &ModelRequest,
        cancel: &CancellationToken,
    ) -> Result<bool> {
        let Some(window) = self.context_capacity(request, cancel).await? else {
            return Ok(false);
        };
        Ok(crate::estimate_request_tokens(request)
            .saturating_add(u64::from(request.max_output_tokens))
            > u64::from(window))
    }

    fn commit_compaction(
        &self,
        session: &mut Session,
        mut compaction: Compaction,
        states: &ExtensionStates,
        usage: Option<Usage>,
        minimum_retention: Option<RetentionPlan>,
        observer: &mut Observer<'_>,
    ) -> Result<()> {
        for extension in &self.registry.extensions {
            if let Decision::Deny(reason) = hook(
                extension.as_ref(),
                extension.before_compaction(&mut compaction, states.get(extension.id())),
            )? {
                return Err(Error::Extension {
                    id: extension.id().into(),
                    message: reason,
                });
            }
        }
        if compaction.summary.trim().is_empty() || compaction.summary.len() > 64 * 1024 {
            return Err(Error::Config(
                "summary must contain between 1 and 65536 bytes".into(),
            ));
        }
        if minimum_retention.is_some_and(|minimum| compaction.keep_turns < minimum.keep_turns) {
            return Err(Error::Config(
                "compaction would drop turns absent from the summary".into(),
            ));
        }
        let retain_from = match minimum_retention {
            Some(retention) if retention.keep_turns == compaction.keep_turns => retention.boundary,
            _ => retain_from(&session.branch(), compaction.keep_turns),
        };
        let mut changes = ExtensionStates::new();
        for extension in &self.registry.extensions {
            if let Some(state) = hook(
                extension.as_ref(),
                extension.after_compaction(&compaction, states.get(extension.id())),
            )? {
                changes.insert(extension.id().into(), state);
            }
        }
        commit(
            session,
            Event::Compacted {
                summary: compaction.summary,
                retain_from,
                states: changes,
                usage,
            },
            observer,
        )
    }
}

fn has_summary_prefix(session: &Session, boundary: Option<u64>) -> bool {
    conversation_entries(&session.branch(), session.operations())
        .iter()
        .any(|(source, _)| boundary.is_none_or(|boundary| *source < boundary))
}

#[derive(Clone, Copy)]
struct RetentionPlan {
    keep_turns: usize,
    boundary: Option<u64>,
    split_turn: bool,
}
impl RetentionPlan {
    fn turns(session: &Session, keep_turns: usize) -> Self {
        Self {
            keep_turns,
            boundary: retain_from(&session.branch(), keep_turns),
            split_turn: false,
        }
    }
}

fn configured_retention(
    session: &Session,
    target: Option<u64>,
    fallback: usize,
) -> Result<RetentionPlan> {
    let Some(target) = target else {
        return Ok(RetentionPlan::turns(session, fallback));
    };
    let branch = session.branch();
    let entries = conversation_entries(&branch, session.operations());
    let candidates: Vec<_> = entries
        .iter()
        .enumerate()
        .filter_map(|(index, (_, message))| {
            matches!(
                message,
                Message::User { .. } | Message::UserWithImages { .. } | Message::Assistant { .. }
            )
            .then_some(index)
        })
        .collect();
    let Some(&first) = candidates.first() else {
        return Ok(RetentionPlan::turns(session, fallback));
    };
    let mut cut = first;
    let mut accumulated = 0_u64;
    for (index, (source, message)) in entries.iter().enumerate().rev() {
        accumulated = accumulated.saturating_add(crate::tokens::estimate_source_tokens(
            session, *source, message,
        ));
        if accumulated >= target {
            cut = candidates
                .iter()
                .copied()
                .find(|candidate| *candidate >= index)
                .unwrap_or_else(|| *candidates.last().expect("nonempty candidates"));
            break;
        }
    }
    let source = entries[cut].0;
    let mut boundary_index = branch
        .iter()
        .position(|record| record.id == source)
        .expect("projected source belongs to branch");
    let visible: HashSet<_> = entries.iter().map(|(source, _)| *source).collect();
    while boundary_index > 0 {
        let previous = branch[boundary_index - 1];
        if visible.contains(&previous.id) || matches!(previous.event, Event::Compacted { .. }) {
            break;
        }
        boundary_index -= 1;
    }
    let turn_start = entries[..=cut]
        .iter()
        .rev()
        .find_map(|(source, message)| {
            matches!(
                message,
                Message::User { .. } | Message::UserWithImages { .. }
            )
            .then_some(*source)
        })
        .unwrap_or(source);
    let keep_turns = branch
        .iter()
        .filter(|record| {
            record.id >= turn_start
                && matches!(
                    record.event,
                    Event::User { .. } | Event::UserWithImages { .. }
                )
        })
        .count()
        .max(1);
    Ok(RetentionPlan {
        keep_turns,
        boundary: Some(branch[boundary_index].id),
        split_turn: matches!(entries[cut].1, Message::Assistant { .. }),
    })
}

fn retained_turn_count(session: &Session, target: Option<u64>) -> Result<usize> {
    let Some(target) = target else {
        return Ok(1);
    };
    let branch = session.branch();
    let floor = retain_from(&branch, usize::MAX).unwrap_or(u64::MAX);
    let turns: Vec<_> = branch
        .iter()
        .enumerate()
        .filter_map(|(index, record)| {
            (record.id >= floor
                && matches!(
                    record.event,
                    Event::User { .. } | Event::UserWithImages { .. }
                ))
            .then_some(index)
        })
        .collect();
    let operations = session.operations();
    let mut low = 0;
    let mut high = turns.len();
    // Find the newest complete turn meeting the target without scanning every
    // increasingly large suffix. Shell results count as context, not new turns.
    while low < high {
        let middle = low + (high - low) / 2;
        let messages = conversation_entries(&branch[turns[middle]..], operations.clone());
        let tokens = messages.iter().fold(0_u64, |tokens, (source, message)| {
            tokens.saturating_add(crate::tokens::estimate_source_tokens(
                session, *source, message,
            ))
        });
        if tokens >= target {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    Ok(turns
        .len()
        .saturating_sub(low)
        .saturating_add(1)
        .min(turns.len())
        .max(1))
}

fn retain_from(branch: &[&Record], keep_turns: usize) -> Option<u64> {
    let floor = branch
        .iter()
        .rev()
        .find_map(|record| match record.event {
            Event::Compacted { retain_from, .. } => Some(retain_from.unwrap_or(record.id)),
            _ => None,
        })
        .unwrap_or(0);
    let retain = branch
        .iter()
        .rev()
        .filter(|record| {
            record.id >= floor
                && matches!(
                    record.event,
                    Event::User { .. } | Event::UserWithImages { .. }
                )
        })
        .take(keep_turns)
        .last()?
        .id;
    let mut start = retain;
    for record in branch
        .iter()
        .rev()
        .filter(|record| record.id >= floor && record.id < retain)
    {
        match record.event {
            Event::User { .. } | Event::UserWithImages { .. } => start = record.id,
            Event::Assistant { .. } => break,
            _ => {}
        }
    }
    Some(start)
}

struct SummaryScope<'a> {
    retention: RetentionPlan,
    instructions: Option<&'a str>,
    output_limit: u32,
    cancellation: &'a CancellationToken,
}
struct DispatchScope<'a> {
    cancellation: &'a CancellationToken,
    advertised: &'a HashSet<String>,
    depth: usize,
    call_count: Arc<AtomicUsize>,
}
struct NestedRequest {
    name: String,
    arguments: serde_json::Value,
    parent_operation: Uuid,
    cancellation: CancellationToken,
    depth: usize,
    call_count: Arc<AtomicUsize>,
    reply: oneshot::Sender<Outcome>,
}
struct NestedHost {
    ui: Option<Arc<dyn crate::ExtensionUi>>,
    requests: mpsc::Sender<Vec<NestedRequest>>,
    specs: Arc<Vec<ToolSpec>>,
    parent_operation: Uuid,
    cancellation: CancellationToken,
    depth: usize,
    call_count: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl ToolHost for NestedHost {
    fn has_ui(&self) -> bool {
        self.ui.is_some()
    }
    async fn ui(&self, request: crate::UiRequest) -> crate::ExtensionResult<crate::UiResponse> {
        request.validate()?;
        let ui = self
            .ui
            .as_ref()
            .ok_or_else(|| "interactive extension UI is unavailable".to_owned())?;
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err("extension UI cancelled".into()),
            result = ui.request(request.clone(), self.cancellation.clone()) => {
                let response = result?;
                if request.accepts_response(&response) { Ok(response) }
                else { Err("invalid extension UI response".into()) }
            },
        }
    }
    fn tools(&self) -> Vec<ToolSpec> {
        self.specs.as_ref().clone()
    }
    async fn call(&self, name: &str, arguments: serde_json::Value) -> Outcome {
        self.call_batch(vec![(name.into(), arguments)])
            .await
            .remove(0)
    }
    async fn call_batch(&self, calls: Vec<(String, serde_json::Value)>) -> Vec<Outcome> {
        let mut requests = Vec::new();
        let mut results = Vec::new();
        for (name, arguments) in calls {
            let error =
                if self.depth >= 8 || self.call_count.fetch_add(1, Ordering::Relaxed) >= 4096 {
                    Some("nested tool execution limit reached")
                } else if arguments.to_string().len() > 1024 * 1024 {
                    Some("nested tool arguments exceed size limit")
                } else {
                    None
                };
            let (reply, result) = oneshot::channel();
            if let Some(error) = error {
                let _ = reply.send(Outcome::Skipped(error.into()));
            } else {
                requests.push(NestedRequest {
                    name,
                    arguments,
                    parent_operation: self.parent_operation,
                    cancellation: self.cancellation.clone(),
                    depth: self.depth + 1,
                    call_count: self.call_count.clone(),
                    reply,
                });
            }
            results.push(result);
        }
        if !requests.is_empty() {
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return results.into_iter().map(|_| Outcome::Skipped("cancelled before nested dispatch".into())).collect(),
                sent = self.requests.send(requests) => if sent.is_err() {
                    return results.into_iter().map(|_| Outcome::Skipped("parent tool has finished".into())).collect();
                }
            }
        }
        futures_util::future::join_all(results.into_iter().map(|result| async move {
            result
                .await
                .unwrap_or_else(|_| Outcome::Unknown("nested tool response was lost".into()))
        }))
        .await
    }
}

enum Preparation<'a> {
    Ready {
        tool: &'a RegisteredTool,
        args: serde_json::Value,
        context: ToolContext,
        state: Option<crate::ExtensionState>,
        guard: tokio_util::sync::DropGuard,
    },
    Skipped(String),
}

// Journal and accounting failures stay outside the retryable provider outcome.
struct ReplyAttempt {
    outcome: Result<Reply>,
}

struct Reply {
    request_id: Uuid,
    content: String,
    invocations: Vec<Invocation>,
    usage: Option<Usage>,
}

fn pending_reply(
    session: &Session,
    run_id: Uuid,
) -> Option<(Uuid, ModelMetadata, UsageReport, RecordedReply)> {
    for record in session
        .branch()
        .into_iter()
        .rev()
        .filter(|record| record.run_id == Some(run_id))
    {
        match &record.event {
            Event::ModelFinished {
                request_id,
                metadata,
                usage,
                reply,
                ..
            } => {
                return reply
                    .as_ref()
                    .map(|reply| (*request_id, metadata.clone(), usage.clone(), reply.clone()));
            }
            Event::Assistant { .. }
            | Event::User { .. }
            | Event::UserWithImages { .. }
            | Event::ModelRequested { .. } => {
                return None;
            }
            _ => {}
        }
    }
    None
}

fn commit(session: &mut Session, event: Event, observer: &mut Observer<'_>) -> Result<()> {
    observer(Notification::Committed(session.append(event)?));
    Ok(())
}

async fn receive_input<T>(inputs: &mut Option<&mut mpsc::Receiver<T>>) -> Option<T> {
    match inputs {
        Some(inputs) => inputs.recv().await,
        None => std::future::pending().await,
    }
}

pub(crate) fn queue_inputs<T: Into<AgentInput>>(
    session: &mut Session,
    followups: &mut mpsc::Receiver<T>,
    observer: &mut Observer<'_>,
) -> Result<()> {
    for _ in 0..followups.max_capacity() {
        let Ok(input) = followups.try_recv() else {
            break;
        };
        queue_input(session, input.into(), observer)?;
    }
    Ok(())
}

fn reply_to_control<T>(result: Result<T>, reply: oneshot::Sender<Result<T>>) -> Result<()> {
    match result {
        Ok(value) => {
            let _ = reply.send(Ok(value));
            Ok(())
        }
        Err(error) => {
            let _ = reply.send(Err(Error::Session(error.to_string())));
            Err(error)
        }
    }
}
impl AgentInput {
    /// Apply one message or queue control from an embedding application's event loop.
    pub fn apply(self, session: &mut Session, observer: &mut Observer<'_>) -> Result<()> {
        queue_input(session, self, observer)
    }
}

fn queue_input(
    session: &mut Session,
    input: AgentInput,
    observer: &mut Observer<'_>,
) -> Result<()> {
    let input = match input {
        AgentInput::Message(message) => message,
        AgentInput::ClearQueue { reply } => {
            let count = session.records().len();
            let result = session.clear_queued_inputs();
            for record in &session.records()[count..] {
                observer(Notification::Committed(record));
            }
            return reply_to_control(result, reply);
        }
        AgentInput::SetQueueMode { kind, mode, reply } => {
            let count = session.records().len();
            let result = session.set_queue_mode(kind, mode);
            for record in &session.records()[count..] {
                observer(Notification::Committed(record));
            }
            return reply_to_control(result, reply);
        }
    };
    if input.input.content.trim().is_empty() && input.images.is_empty() {
        return Ok(());
    }
    input.validate()?;
    if session
        .records()
        .iter()
        .any(|record| record.run_id == Some(input.input.id))
    {
        return Err(Error::Config("queued input ID is reserved by a run".into()));
    }
    let existing = session
        .records()
        .iter()
        .find_map(|record| match &record.event {
            Event::InputQueued { input: existing } if existing.id == input.input.id => {
                Some(QueuedMessage::from(existing.clone()))
            }
            Event::InputQueuedWithImages { input: existing }
                if existing.input.id == input.input.id =>
            {
                Some(existing.clone())
            }
            _ => None,
        });
    match existing {
        Some(existing) if existing == input => return Ok(()),
        Some(_) => {
            return Err(Error::Config(
                "input ID already exists with different content or kind".into(),
            ));
        }
        None => {}
    }
    let event = if input.images.is_empty() {
        Event::InputQueued { input: input.input }
    } else {
        Event::InputQueuedWithImages { input }
    };
    commit(session, event, observer)
}

fn apply_inputs(
    session: &mut Session,
    include_followups: bool,
    observer: &mut Observer<'_>,
) -> Result<bool> {
    let pending: Vec<_> = session
        .queued_inputs()
        .into_iter()
        .filter(|message| include_followups || message.input.kind == InputKind::Steer)
        .collect();
    let mut accepted = false;
    let steering_mode = session.queue_mode(InputKind::Steer);
    let follow_up_mode = session.queue_mode(InputKind::FollowUp);
    let mut seen = HashSet::new();
    for message in pending {
        let kind = message.input.kind;
        let seen_kind = if kind == InputKind::Steer { 0 } else { 1 };
        let mode = if kind == InputKind::Steer {
            steering_mode
        } else {
            follow_up_mode
        };
        if mode == crate::QueueMode::OneAtATime && !seen.insert(seen_kind) {
            continue;
        }
        accepted = true;
        message.validate()?;
        let event = if message.images.is_empty() {
            Event::User {
                content: message.input.content,
                input_id: Some(message.input.id),
            }
        } else {
            Event::UserWithImages {
                content: message.input.content,
                input_id: Some(message.input.id),
                images: message.images,
            }
        };
        commit(session, event, observer)?;
    }
    Ok(accepted)
}

fn recover_unstarted(session: &mut Session, observer: &mut Observer<'_>) -> Result<()> {
    for (invocation, state) in session.operations() {
        if state == OperationState::Planned {
            commit(
                session,
                Event::ToolFinished {
                    operation_id: invocation.operation_id,
                    outcome: Outcome::Skipped("run ended before dispatch".into()),
                    states: ExtensionStates::new(),
                },
                observer,
            )?;
        }
    }
    Ok(())
}

fn recover(session: &mut Session, observer: &mut Observer<'_>) -> Result<()> {
    recover_unstarted(session, observer)?;
    for (invocation, state) in session.operations() {
        if state == OperationState::Started {
            commit(
                session,
                Event::ToolFinished {
                    operation_id: invocation.operation_id,
                    outcome: Outcome::Unknown(
                        "previous process stopped after dispatch was committed".into(),
                    ),
                    states: ExtensionStates::new(),
                },
                observer,
            )?;
        }
    }
    ensure_settled(session.operations())
}

#[cfg(test)]
mod nested_host_tests {
    use super::*;

    #[tokio::test]
    async fn batch_retains_invalid_argument_positions_without_dispatching_them() {
        let (requests, mut incoming) = mpsc::channel(1);
        let host = NestedHost {
            ui: None,
            requests,
            specs: Arc::new(vec![]),
            parent_operation: Uuid::new_v4(),
            cancellation: CancellationToken::new(),
            depth: 0,
            call_count: Arc::new(AtomicUsize::new(0)),
        };
        let calls = vec![
            (
                "oversized".into(),
                serde_json::json!("x".repeat(1024 * 1024)),
            ),
            ("valid".into(), serde_json::json!({})),
        ];
        let (outcomes, ()) = tokio::join!(host.call_batch(calls), async {
            let mut requests = incoming.recv().await.unwrap();
            assert_eq!(requests.len(), 1);
            let request = requests.remove(0);
            assert_eq!(request.name, "valid");
            request
                .reply
                .send(Outcome::Succeeded("done".into()))
                .unwrap();
        });
        assert!(matches!(outcomes[0], Outcome::Skipped(_)));
        assert_eq!(outcomes[1], Outcome::Succeeded("done".into()));
    }
}

#[cfg(test)]
mod queue_tests {
    use super::*;
    struct WaitingModel(Arc<tokio::sync::Notify>);
    #[async_trait::async_trait]
    impl Model for WaitingModel {
        async fn stream(&self, _: ModelRequest) -> Result<crate::ModelStream> {
            self.0.notify_one();
            std::future::pending().await
        }
    }
    #[tokio::test]
    async fn queue_controls_are_acknowledged_while_a_model_request_is_waiting() {
        let ready = Arc::new(tokio::sync::Notify::new());
        let model = WaitingModel(ready.clone());
        let registry = Registry::default();
        let agent = Agent {
            model: &model,
            model_name: "waiting",
            registry: &registry,
            system_prompt: "test",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        let (sender, mut inputs) = mpsc::channel::<AgentInput>(4);
        sender
            .send(
                QueuedMessage::from(crate::QueuedInput {
                    id: Uuid::new_v4(),
                    kind: InputKind::FollowUp,
                    content: "later".into(),
                })
                .into(),
            )
            .await
            .unwrap();
        let cancellation = CancellationToken::new();
        let driver = async {
            ready.notified().await;
            let (reply, result) = oneshot::channel();
            sender
                .send(AgentInput::SetQueueMode {
                    kind: InputKind::FollowUp,
                    mode: crate::QueueMode::OneAtATime,
                    reply,
                })
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(1), result)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let (reply, result) = oneshot::channel();
            sender.send(AgentInput::ClearQueue { reply }).await.unwrap();
            let removed = tokio::time::timeout(Duration::from_secs(1), result)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(removed[0].input.content, "later");
            cancellation.cancel();
        };
        let mut observer = |_: Notification<'_>| {};
        let (result, ()) = tokio::join!(
            agent.run_correlated(
                &mut session,
                Uuid::new_v4(),
                Some("start".into()),
                &mut inputs,
                &cancellation,
                &mut observer
            ),
            driver
        );
        assert!(matches!(result, Err(Error::Cancelled)));
        assert!(session.queued_inputs().is_empty());
        assert_eq!(
            session.queue_mode(InputKind::FollowUp),
            crate::QueueMode::OneAtATime
        );
    }

    #[test]
    fn text_only_queued_messages_obey_the_same_size_limit() {
        let message = QueuedMessage::from("x".repeat(8 * 1024 * 1024));
        assert!(message.validate().is_err());
    }
    #[test]
    fn queue_modes_and_cleared_messages_follow_the_session_branch() {
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        session
            .set_queue_mode(InputKind::FollowUp, crate::QueueMode::OneAtATime)
            .unwrap();
        let (sender, mut inputs) = mpsc::channel(4);
        for content in ["first", "second", "third"] {
            sender
                .try_send(QueuedMessage::from(crate::QueuedInput {
                    id: Uuid::new_v4(),
                    kind: InputKind::FollowUp,
                    content: content.into(),
                }))
                .unwrap();
        }
        queue_inputs(&mut session, &mut inputs, &mut |_| {}).unwrap();
        assert_eq!(session.queued_inputs().len(), 3);
        assert!(apply_inputs(&mut session, true, &mut |_| {}).unwrap());
        assert_eq!(session.queued_inputs().len(), 2);
        let before_clear = session.head();
        let removed = session.clear_queued_inputs().unwrap();
        assert_eq!(
            removed
                .iter()
                .map(|input| input.input.content.as_str())
                .collect::<Vec<_>>(),
            ["second", "third"]
        );
        assert!(!apply_inputs(&mut session, true, &mut |_| {}).unwrap());
        let earlier = Session::from_records(session.fork_records(before_clear).unwrap()).unwrap();
        assert_eq!(earlier.queued_inputs().len(), 2);
        let fork = Session::from_records(session.fork_records(session.head()).unwrap()).unwrap();
        assert!(fork.queued_inputs().is_empty());
        assert_eq!(
            fork.queue_mode(InputKind::FollowUp),
            crate::QueueMode::OneAtATime
        );
        assert_eq!(fork.queue_mode(InputKind::Steer), crate::QueueMode::All);
    }
}
