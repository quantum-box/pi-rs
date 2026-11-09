use std::collections::HashSet;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::extension::{RegisteredTool, hook};
use crate::journal::{conversation, ensure_settled};
use crate::{
    CancellationToken, Compaction, Decision, Error, Event, ExtensionStates, Invocation, Message,
    Model, ModelEvent, ModelRequest, OperationState, Outcome, Record, Registry, Result, RunState,
    Session, ToolContext, Usage,
};

#[derive(Clone, Debug)]
pub struct Limits {
    pub model_requests: usize,
    pub output_tokens_per_request: u32,
    pub output_tokens_per_run: u32,
    pub model_timeout: Duration,
    pub tool_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            model_requests: 12,
            output_tokens_per_request: 2048,
            output_tokens_per_run: 8192,
            model_timeout: Duration::from_secs(120),
            tool_timeout: Duration::from_secs(30),
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
        if input.as_ref().is_some_and(|text| text.trim().is_empty()) {
            return Err(Error::Config("empty user input".into()));
        }
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        commit(
            session,
            Event::Run {
                state: RunState::Running,
            },
            observer,
        )?;
        let result = self
            .drive(session, input, followups, cancel, observer)
            .await;
        let state = match &result {
            Ok(_) => RunState::Completed,
            Err(Error::Cancelled) => RunState::Cancelled,
            Err(Error::Reconciliation(_)) => RunState::WaitingForReconciliation,
            Err(_) => RunState::Failed,
        };
        commit(session, Event::Run { state }, observer)?;
        result
    }

    async fn drive(
        &self,
        session: &mut Session,
        input: Option<String>,
        followups: &mut mpsc::Receiver<String>,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<String> {
        recover(session, observer)?;
        let mut states = self.migrate(session, observer)?;
        if let Some(content) = input {
            commit(session, Event::User { content }, observer)?;
        }

        drain_inputs(session, followups, observer)?;
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
            return Ok(answer);
        }

        let mut budget = self.limits.output_tokens_per_run;
        for _ in 0..self.limits.model_requests {
            drain_inputs(session, followups, observer)?;
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let max_output_tokens = self.limits.output_tokens_per_request.min(budget);
            if max_output_tokens == 0 {
                return Err(Error::Budget);
            }
            let mut request = self.request(session, &states, max_output_tokens)?;
            // A context extension can lower the budget but cannot increase the host's limit.
            request.max_output_tokens = request.max_output_tokens.min(max_output_tokens);
            if request.max_output_tokens == 0 {
                return Err(Error::Budget);
            }
            let advertised: HashSet<_> =
                request.tools.iter().map(|spec| spec.name.clone()).collect();
            let allowance = request.max_output_tokens;
            let reply = self.reply(session, request, cancel, observer).await?;
            let finished = reply.invocations.is_empty();
            let answer = finished.then(|| reply.content.clone());
            let invocations = reply.invocations.clone();
            let output_tokens = reply
                .usage
                .map_or(u64::from(allowance), |usage| usage.output_tokens);
            if output_tokens > u64::from(allowance) {
                return Err(Error::Budget);
            }
            budget -= output_tokens as u32;
            commit(
                session,
                Event::Assistant {
                    content: reply.content,
                    invocations: reply.invocations,
                    usage: reply.usage,
                },
                observer,
            )?;

            for invocation in &invocations {
                if let Err(err) = self
                    .tool(
                        session,
                        invocation,
                        &advertised,
                        &mut states,
                        cancel,
                        observer,
                    )
                    .await
                {
                    // A cancellation, uncertain result, or hook failure leaves the rest unstarted.
                    recover_unstarted(session, observer)?;
                    return Err(err);
                }
            }

            if let Some(answer) = answer {
                if cancel.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                if !drain_inputs(session, followups, observer)? {
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

    fn request(
        &self,
        session: &Session,
        states: &ExtensionStates,
        max_output_tokens: u32,
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
        messages.extend(conversation(&branch, session.operations()));
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
            hook(
                extension.as_ref(),
                extension.context(&mut request, states.get(extension.id())),
            )?;
        }
        Ok(request)
    }

    async fn reply(
        &self,
        session: &mut Session,
        request: ModelRequest,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<Reply> {
        commit(
            session,
            Event::ModelRequested {
                model: request.model.clone(),
                messages: request.messages.len(),
                tools: request.tools.len(),
                max_output_tokens: request.max_output_tokens,
            },
            observer,
        )?;
        observer(Notification::Request(&request));
        const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
        let collect = async {
            let mut stream = self.model.stream(request).await?;
            let mut content = String::new();
            while let Some(event) = stream.next().await {
                match event? {
                    ModelEvent::Text(delta) => {
                        if content.len() + delta.len() > MAX_RESPONSE_BYTES {
                            return Err(Error::Model("response exceeds size limit".into()));
                        }
                        observer(Notification::Text(&delta));
                        content.push_str(&delta);
                    }
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
                            content,
                            invocations,
                            usage,
                        });
                    }
                }
            }
            Err(Error::Model("stream ended before completion".into()))
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Error::Cancelled),
            result = tokio::time::timeout(self.limits.model_timeout, collect) =>
                result.map_err(|_| Error::Model("request timed out".into()))?,
        }
    }

    async fn tool(
        &self,
        session: &mut Session,
        invocation: &Invocation,
        advertised: &HashSet<String>,
        states: &mut ExtensionStates,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<()> {
        let operation_id = invocation.operation_id;
        let call = &invocation.call;
        let candidate = self
            .registry
            .tools
            .get(&call.name)
            .filter(|_| advertised.contains(&call.name));
        let outcome = match (candidate, serde_json::from_str(&call.arguments)) {
            (Some(tool), Ok(args)) if tool.validator.is_valid(&args) => {
                let cancellation = cancel.child_token();
                let _cancel_on_drop = cancellation.clone().drop_guard();
                let context = ToolContext {
                    operation_id,
                    cancellation,
                };
                let decision = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(Error::Cancelled),
                    decision = tokio::time::timeout(self.limits.tool_timeout, self.authorize(&call.name, &args, &context, states)) =>
                        decision.map_err(|_| Error::Config("pre-tool hooks timed out".into()))??,
                };
                match decision {
                    Decision::Allow => {
                        self.invoke(session, tool, args, context, cancel, observer)
                            .await?
                    }
                    Decision::Deny(reason) => Outcome::Skipped(reason),
                }
            }
            (None, _) => Outcome::Skipped("tool is not registered or advertised".into()),
            _ => Outcome::Skipped("arguments do not match the tool's JSON Schema".into()),
        };
        let outcome = if outcome.tool_content().len() > 1024 * 1024 {
            match outcome {
                Outcome::Succeeded(_) => {
                    Outcome::Succeeded("[tool output omitted: exceeds size limit]".into())
                }
                Outcome::Failed(_) => Outcome::Failed("failure detail exceeds size limit".into()),
                Outcome::Skipped(_) => Outcome::Skipped("skip detail exceeds size limit".into()),
                Outcome::Unknown(_) => {
                    Outcome::Unknown("uncertain result detail exceeds size limit".into())
                }
            }
        } else {
            outcome
        };
        let changes = self.tool_state_changes(&call.name, &outcome, states);
        let persisted = changes.as_ref().cloned().unwrap_or_default();
        let unknown = matches!(outcome, Outcome::Unknown(_));
        commit(
            session,
            Event::ToolFinished {
                operation_id,
                outcome,
                states: persisted,
            },
            observer,
        )?;
        if unknown {
            return Err(Error::Reconciliation(operation_id));
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

    async fn invoke(
        &self,
        session: &mut Session,
        tool: &RegisteredTool,
        args: serde_json::Value,
        context: ToolContext,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<Outcome> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        commit(
            session,
            Event::ToolStarted {
                operation_id: context.operation_id,
            },
            observer,
        )?;
        Ok(tokio::select! {
            biased;
            _ = cancel.cancelled() => Outcome::Unknown("cancelled after dispatch was committed".into()),
            result = tokio::time::timeout(self.limits.tool_timeout, tool.tool.invoke(args, context)) =>
                result.unwrap_or_else(|_| Outcome::Unknown("tool timed out".into())),
        })
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
        self.commit_compaction(session, compaction, &states, None, observer)
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
        let mut request = self.request(session, &states, limit)?;
        request.max_output_tokens = request.max_output_tokens.min(limit);
        if request.max_output_tokens == 0 || self.limits.model_requests == 0 {
            return Err(Error::Budget);
        }
        request.tools.clear();
        request.messages.push(Message::User { content: "Summarize the prior conversation for a future assistant. Preserve the current goal, decisions, confirmed tool outcomes, and unfinished work. Omit credentials. Return only the summary.".into() });
        let reply = self.reply(session, request, cancel, observer).await?;
        if !reply.invocations.is_empty() {
            return Err(Error::Model(
                "summary response attempted a tool call".into(),
            ));
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
            &states,
            reply.usage,
            observer,
        )
    }

    fn commit_compaction(
        &self,
        session: &mut Session,
        mut compaction: Compaction,
        states: &ExtensionStates,
        usage: Option<Usage>,
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
        let branch = session.branch();
        let floor = branch
            .iter()
            .rev()
            .find_map(|record| match record.event {
                Event::Compacted { retain_from, .. } => Some(retain_from.unwrap_or(record.id)),
                _ => None,
            })
            .unwrap_or(0);
        let retain_from = branch
            .iter()
            .rev()
            .filter(|record| record.id >= floor && matches!(record.event, Event::User { .. }))
            .take(compaction.keep_turns)
            .last()
            .map(|record| record.id);
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

struct Reply {
    content: String,
    invocations: Vec<Invocation>,
    usage: Option<Usage>,
}

fn commit(session: &mut Session, event: Event, observer: &mut Observer<'_>) -> Result<()> {
    observer(Notification::Committed(session.append(event)?));
    Ok(())
}

fn drain_inputs(
    session: &mut Session,
    followups: &mut mpsc::Receiver<String>,
    observer: &mut Observer<'_>,
) -> Result<bool> {
    let mut accepted = false;
    // Bound the drain so a continuous producer cannot starve the model loop.
    for _ in 0..followups.max_capacity() {
        let Ok(content) = followups.try_recv() else {
            break;
        };
        if !content.trim().is_empty() {
            commit(session, Event::User { content }, observer)?;
            accepted = true;
        }
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
