use std::collections::HashSet;
use std::time::Duration;

use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::extension::{RegisteredTool, hook};
use crate::journal::{conversation, ensure_settled};
use crate::{
    CancellationToken, Compaction, Decision, Error, Event, ExtensionStates, InputKind, Invocation,
    Message, Model, ModelEvent, ModelFailure, ModelMetadata, ModelRequest, OperationState, Outcome,
    QueuedInput, Record, RecordedReply, Registry, RequestPurpose, Result, RunState, Session,
    ToolContext, ToolExecution, Usage, UsageReport,
};

#[derive(Clone, Debug)]
pub struct Limits {
    pub model_requests: usize,
    pub output_tokens_per_request: u32,
    pub output_tokens_per_run: u32,
    pub model_timeout: Duration,
    pub tool_timeout: Duration,
    /// Estimated total input/output context. When set, try one automatic compaction per run.
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

    /// A repeated initial submission never starts a second run. Passing no
    /// input is an explicit resume request authorized by the host.
    pub async fn run_correlated<T: Into<QueuedInput> + Send>(
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

    async fn run_turns<T: Into<QueuedInput> + Send>(
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
            |record| matches!(record.event, Event::User { input_id: Some(id), .. } if id == run_id),
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
        let mut result = self
            .drive(
                session,
                run_id,
                effective_input,
                followups,
                cancel,
                observer,
            )
            .await;
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

    async fn drive<T: Into<QueuedInput> + Send>(
        &self,
        session: &mut Session,
        run_id: Uuid,
        input: Option<(Uuid, String)>,
        followups: &mut mpsc::Receiver<T>,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<String> {
        recover(session, observer)?;
        let mut states = self.migrate(session, observer)?;
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
                Event::User { .. } => Some(None),
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
        } else if !session
            .branch()
            .iter()
            .any(|record| matches!(record.event, Event::User { .. }))
        {
            apply_inputs(session, true, observer)?;
        }

        let mut budget = self.limits.output_tokens_per_run;
        let mut compacted = false;
        for request_index in 0..self.limits.model_requests {
            queue_inputs(session, followups, observer)?;
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let (reply, advertised, allowance) =
                if let Some((request, metadata, usage, recorded)) = pending.take() {
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
                    if self.exceeds_context(&request)? {
                        if compacted || request_index + 1 >= self.limits.model_requests {
                            return Err(Error::Model(
                                "context does not fit after the bounded compaction attempt".into(),
                            ));
                        }
                        let summary_limit = max_output_tokens.min(512).min(budget / 2);
                        let spent = self
                            .summarize_prefix(session, &states, 1, summary_limit, cancel, observer)
                            .await?;
                        budget -= spent;
                        states = session.extension_states();
                        compacted = true;
                        continue;
                    }
                    let advertised: HashSet<_> =
                        request.tools.iter().map(|spec| spec.name.clone()).collect();
                    let allowance = request.max_output_tokens;
                    let reply = self.reply(session, request, true, cancel, observer).await?;
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
                    &advertised,
                    &mut states,
                    cancel,
                    observer,
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
        let mut messages = vec![Message::System {
            content: self.system_prompt.into(),
        }];
        if let Some(summary) = summary {
            // Compaction content has the authority of conversation history.
            messages.push(Message::User {
                content: format!("Prior conversation summary (context only):\n{summary}"),
            });
        }
        let history = match before {
            Some(before) => &branch[..branch.partition_point(|record| record.id < before)],
            None => &branch,
        };
        messages.extend(conversation(history, session.operations()));
        if messages.len() == 1 {
            return Err(Error::Session("there is no conversation to resume".into()));
        }
        let mut request = ModelRequest {
            model: self.model_name.into(),
            messages,
            tools: self
                .registry
                .tools
                .values()
                .map(|tool| tool.spec.clone())
                .collect(),
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

    async fn reply(
        &self,
        session: &mut Session,
        request: ModelRequest,
        retain_reply: bool,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<Reply> {
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
            let mut stream = self.model.stream_with_id(request_id, request).await?;
            while let Some(event) = stream.next().await {
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
        result
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

    async fn tool_batch(
        &self,
        session: &mut Session,
        invocations: &[Invocation],
        advertised: &HashSet<String>,
        states: &mut ExtensionStates,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<()> {
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
                prepared.push(
                    self.prepare_tool(invocation, advertised, states, cancel)
                        .await?,
                );
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
            let outcomes = futures_util::future::join_all(
                prepared
                    .into_iter()
                    .map(|prepared| self.invoke(prepared, cancel)),
            )
            .await;
            let mut first_error = None;
            // All started outcomes are recorded even if one parallel member failed.
            for (invocation, outcome) in group.iter().zip(outcomes) {
                if let Err(error) = self.finish_tool(session, invocation, outcome, states, observer)
                {
                    first_error.get_or_insert(error);
                }
            }
            if let Some(error) = first_error {
                return Err(error);
            }
            remaining = next;
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
                guard,
            },
            Decision::Deny(reason) => Preparation::Skipped(reason),
        })
    }

    fn finish_tool(
        &self,
        session: &mut Session,
        invocation: &Invocation,
        outcome: Outcome,
        states: &mut ExtensionStates,
        observer: &mut Observer<'_>,
    ) -> Result<()> {
        const MAX_OUTPUT_BYTES: usize = 1024 * 1024;
        let outcome = match outcome {
            Outcome::Succeeded(output) if output.len() > MAX_OUTPUT_BYTES => Outcome::Truncated {
                original_bytes: output.len(),
            },
            outcome @ Outcome::Structured { .. } => {
                let size = outcome.tool_content().len();
                if size > MAX_OUTPUT_BYTES {
                    Outcome::Truncated {
                        original_bytes: size,
                    }
                } else {
                    outcome
                }
            }
            Outcome::Failed(reason) if reason.len() > MAX_OUTPUT_BYTES => {
                Outcome::Failed("failure detail exceeds size limit".into())
            }
            Outcome::Skipped(reason) if reason.len() > MAX_OUTPUT_BYTES => {
                Outcome::Skipped("skip detail exceeds size limit".into())
            }
            Outcome::Unknown(reason) if reason.len() > MAX_OUTPUT_BYTES => {
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
        let changes = self.tool_state_changes(&invocation.call.name, &outcome, states);
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
    ) -> Result<ExtensionStates> {
        let mut changes = ExtensionStates::new();
        for extension in &self.registry.extensions {
            if let Some(state) = hook(
                extension.as_ref(),
                extension.after_tool(name, outcome, states.get(extension.id())),
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

    async fn invoke(&self, preparation: Preparation<'_>, cancel: &CancellationToken) -> Outcome {
        let (tool, args, context, _guard) = match preparation {
            Preparation::Ready {
                tool,
                args,
                context,
                guard,
            } => (tool, args, context, guard),
            Preparation::Skipped(reason) => return Outcome::Skipped(reason),
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Outcome::Unknown("cancelled after dispatch was committed".into()),
            result = tokio::time::timeout(self.limits.tool_timeout, tool.tool.invoke(args, context)) =>
                result.unwrap_or_else(|_| Outcome::Unknown("tool timed out".into())),
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
                extension.id() == extension_id && extension.commands().contains(&command)
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

    /// Request a model-generated summary in a tool-free turn, then compact atomically.
    pub async fn summarize(
        &self,
        session: &mut Session,
        keep_turns: usize,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<()> {
        ensure_settled(session.operations())?;
        let states = self.migrate(session, observer)?;
        let limit = self
            .limits
            .output_tokens_per_request
            .min(self.limits.output_tokens_per_run);
        self.summarize_prefix(session, &states, keep_turns, limit, cancel, observer)
            .await?;
        Ok(())
    }

    async fn summarize_prefix(
        &self,
        session: &mut Session,
        states: &ExtensionStates,
        keep_turns: usize,
        limit: u32,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<u32> {
        if limit == 0 || self.limits.model_requests == 0 {
            return Err(Error::Budget);
        }
        let before = retain_from(&session.branch(), keep_turns);
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
        if self.exceeds_context(&request)? {
            return Err(Error::Model(
                "older context does not fit the summary request".into(),
            ));
        }
        let allowance = request.max_output_tokens;
        let reply = self
            .reply(session, request, false, cancel, observer)
            .await?;
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
                keep_turns,
            },
            states,
            reply.usage,
            Some(keep_turns),
            observer,
        )?;
        Ok(output as u32)
    }

    fn exceeds_context(&self, request: &ModelRequest) -> Result<bool> {
        let Some(window) = self.limits.context_window else {
            return Ok(false);
        };
        // Conservative approximation of the JSON input and role/tool overhead.
        let bytes = serde_json::to_vec(request)?.len();
        Ok(bytes.div_ceil(3) + request.max_output_tokens as usize > window as usize)
    }

    fn commit_compaction(
        &self,
        session: &mut Session,
        mut compaction: Compaction,
        states: &ExtensionStates,
        usage: Option<Usage>,
        minimum_retention: Option<usize>,
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
        if minimum_retention.is_some_and(|minimum| compaction.keep_turns < minimum) {
            return Err(Error::Config(
                "compaction would drop turns absent from the summary".into(),
            ));
        }
        let retain_from = retain_from(&session.branch(), compaction.keep_turns);
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
        .filter(|record| record.id >= floor && matches!(record.event, Event::User { .. }))
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
            Event::User { .. } => start = record.id,
            Event::Assistant { .. } => break,
            _ => {}
        }
    }
    Some(start)
}

enum Preparation<'a> {
    Ready {
        tool: &'a RegisteredTool,
        args: serde_json::Value,
        context: ToolContext,
        guard: tokio_util::sync::DropGuard,
    },
    Skipped(String),
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
            Event::Assistant { .. } | Event::User { .. } | Event::ModelRequested { .. } => {
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

fn queue_inputs<T: Into<QueuedInput>>(
    session: &mut Session,
    followups: &mut mpsc::Receiver<T>,
    observer: &mut Observer<'_>,
) -> Result<()> {
    // Bound the drain so a continuous producer cannot starve the model loop.
    for _ in 0..followups.max_capacity() {
        let Ok(input) = followups.try_recv() else {
            break;
        };
        let input = input.into();
        if input.content.trim().is_empty() {
            continue;
        }
        if session
            .records()
            .iter()
            .any(|record| record.run_id == Some(input.id))
        {
            return Err(Error::Config("queued input ID is reserved by a run".into()));
        }
        let existing = session
            .records()
            .iter()
            .find_map(|record| match &record.event {
                Event::InputQueued { input: existing } if existing.id == input.id => Some(existing),
                _ => None,
            });
        match existing {
            Some(existing) if *existing == input => continue,
            Some(_) => {
                return Err(Error::Config(
                    "input ID already exists with different content or kind".into(),
                ));
            }
            None => commit(session, Event::InputQueued { input }, observer)?,
        }
    }
    Ok(())
}

fn apply_inputs(
    session: &mut Session,
    include_followups: bool,
    observer: &mut Observer<'_>,
) -> Result<bool> {
    let branch = session.branch();
    let consumed: HashSet<_> = branch
        .iter()
        .filter_map(|record| match record.event {
            Event::User { input_id, .. } => input_id,
            _ => None,
        })
        .collect();
    let pending: Vec<_> = branch
        .iter()
        .filter_map(|record| match &record.event {
            Event::InputQueued { input }
                if !consumed.contains(&input.id)
                    && (include_followups || input.kind == InputKind::Steer) =>
            {
                Some(input.clone())
            }
            _ => None,
        })
        .collect();
    let accepted = !pending.is_empty();
    for input in pending {
        commit(
            session,
            Event::User {
                content: input.content,
                input_id: Some(input.id),
            },
            observer,
        )?;
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
