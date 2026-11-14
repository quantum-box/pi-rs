use super::*;
use crate::runtime::time as platform_time;
use futures_util::{FutureExt, stream::FuturesUnordered};

pub(super) enum TurnPreparation {
    Ready {
        tool: Arc<RegisteredTool>,
        args: serde_json::Value,
        context: ToolContext,
        state: Option<crate::ExtensionState>,
        guard: tokio_util::sync::DropGuard,
    },
    Skipped(String),
}

enum TurnNormalized {
    Ready {
        tool: Arc<RegisteredTool>,
        args: serde_json::Value,
    },
    Skipped(String),
}

impl Agent<'_> {
    pub(super) async fn turn_poll_normalize(
        registry: &Registry,
        timeout: Duration,
        invocation: &Invocation,
        cancel: &CancellationToken,
    ) -> Result<(Arc<RegisteredTool>, serde_json::Value)> {
        let pending = || Error::PendingOperation(invocation.operation_id);
        let tool = registry
            .tools
            .get(&invocation.call.name)
            .filter(|tool| tool.tool.can_poll())
            .cloned()
            .ok_or_else(pending)?;
        let args =
            crate::parse_tool_arguments(&invocation.call.arguments).map_err(|_| pending())?;
        let args = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            result = platform_time::timeout(timeout, tool.tool.prepare_arguments(args)) => result.ok().and_then(std::result::Result::ok).ok_or_else(pending)?,
        };
        if args.to_string().len() > 16 * 1024 * 1024 || !tool.validator.is_valid(&args) {
            return Err(pending());
        }
        Ok((tool, args))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn turn_poll_authorize_and_run(
        registry: &Registry,
        timeout: Duration,
        invocation: &Invocation,
        reference: &str,
        prepared: (Arc<RegisteredTool>, serde_json::Value),
        states: &ExtensionStates,
        cancel: &CancellationToken,
        host: Option<Arc<dyn ToolHost>>,
    ) -> Result<Outcome> {
        let (tool, args) = prepared;
        let pending = || Error::PendingOperation(invocation.operation_id);
        let cancellation = cancel.child_token();
        let _guard = cancellation.clone().drop_guard();
        let context = ToolContext {
            operation_id: invocation.operation_id,
            cancellation,
        };
        let authorize = async {
            for extension in &registry.extensions {
                let decision = hook(
                    extension.as_ref(),
                    match &host {
                        Some(host) => {
                            extension
                                .before_tool_with_host(
                                    &invocation.call.name,
                                    &args,
                                    &context,
                                    states.get(extension.id()),
                                    host.clone(),
                                )
                                .await
                        }
                        None => {
                            extension
                                .before_tool(
                                    &invocation.call.name,
                                    &args,
                                    &context,
                                    states.get(extension.id()),
                                )
                                .await
                        }
                    },
                )?;
                if decision != Decision::Allow {
                    return Err(pending());
                }
            }
            Ok(())
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            result = platform_time::timeout(timeout, authorize) => result.map_err(|_| pending())??,
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Error::Cancelled),
            result = platform_time::timeout(timeout, tool.tool.poll(reference, context)) => result.ok().flatten().ok_or_else(pending),
        }
    }

    async fn turn_normalize(
        registry: &Registry,
        tool_timeout: Duration,
        invocation: &Invocation,
        advertised: &HashSet<String>,
        cancel: &CancellationToken,
    ) -> Result<TurnNormalized> {
        let call = &invocation.call;
        let Some(tool) = registry
            .tools
            .get(&call.name)
            .filter(|_| advertised.contains(&call.name))
            .cloned()
        else {
            return Ok(TurnNormalized::Skipped(
                "tool is not registered or advertised".into(),
            ));
        };
        let args = match crate::parse_tool_arguments(&call.arguments) {
            Ok(arguments) => arguments,
            Err(error) => {
                return Ok(TurnNormalized::Skipped(format!(
                    "Invalid arguments for tool \"{}\": {error}",
                    call.name
                )));
            }
        };
        let args = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            result = platform_time::timeout(tool_timeout, tool.tool.prepare_arguments(args)) => match result {
                Ok(Ok(arguments)) if arguments.to_string().len() <= 16 * 1024 * 1024 => arguments,
                Ok(Ok(_)) => return Ok(TurnNormalized::Skipped("prepared arguments exceed size limit".into())),
                Ok(Err(error)) => return Ok(TurnNormalized::Skipped(format!("argument preparation failed: {error}"))),
                Err(_) => return Ok(TurnNormalized::Skipped("argument preparation timed out".into())),
            },
        };
        if !tool.validator.is_valid(&args) {
            return Ok(TurnNormalized::Skipped(
                "arguments do not match the tool's JSON Schema".into(),
            ));
        }
        Ok(TurnNormalized::Ready { tool, args })
    }

    async fn turn_finish_preparation(
        registry: &Registry,
        tool_timeout: Duration,
        invocation: &Invocation,
        normalized: TurnNormalized,
        states: &ExtensionStates,
        cancel: &CancellationToken,
        host: Option<Arc<dyn ToolHost>>,
    ) -> Result<TurnPreparation> {
        let (tool, mut args) = match normalized {
            TurnNormalized::Ready { tool, args } => (tool, args),
            TurnNormalized::Skipped(reason) => return Ok(TurnPreparation::Skipped(reason)),
        };
        let call = &invocation.call;
        let cancellation = cancel.child_token();
        let guard = cancellation.clone().drop_guard();
        let context = ToolContext {
            operation_id: invocation.operation_id,
            cancellation,
        };
        let decision = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            decision = platform_time::timeout(tool_timeout, Self::turn_authorize(registry, &call.name, &mut args, &context, states, host)) =>
                decision.map_err(|_| Error::Config("pre-tool hooks timed out".into()))??,
        };
        Ok(match decision {
            Decision::Allow => TurnPreparation::Ready {
                state: states.get(&tool.owner).cloned(),
                tool,
                args,
                context,
                guard,
            },
            Decision::Deny(reason) => TurnPreparation::Skipped(reason),
        })
    }

    #[cfg(test)]
    pub(super) async fn turn_prepare(
        registry: &Registry,
        tool_timeout: Duration,
        invocation: &Invocation,
        advertised: &HashSet<String>,
        states: &ExtensionStates,
        cancel: &CancellationToken,
    ) -> Result<TurnPreparation> {
        let normalized =
            Self::turn_normalize(registry, tool_timeout, invocation, advertised, cancel).await?;
        Self::turn_finish_preparation(
            registry,
            tool_timeout,
            invocation,
            normalized,
            states,
            cancel,
            None,
        )
        .await
    }

    async fn turn_authorize(
        registry: &Registry,
        name: &str,
        args: &mut serde_json::Value,
        context: &ToolContext,
        states: &ExtensionStates,
        host: Option<Arc<dyn ToolHost>>,
    ) -> Result<Decision> {
        for extension in &registry.extensions {
            let decision = hook(
                extension.as_ref(),
                match &host {
                    Some(host) => {
                        extension
                            .tool_call_with_host(
                                name,
                                args,
                                context,
                                states.get(extension.id()),
                                host.clone(),
                            )
                            .await
                    }
                    None => {
                        extension
                            .tool_call(name, args, context, states.get(extension.id()))
                            .await
                    }
                },
            )?;
            if decision != Decision::Allow {
                return Ok(decision);
            }
            if args.to_string().len() > 16 * 1024 * 1024 {
                return Ok(Decision::Deny("tool arguments exceed size limit".into()));
            }
        }
        Ok(Decision::Allow)
    }

    async fn turn_invoke(
        preparation: TurnPreparation,
        cancel: &CancellationToken,
        host: Arc<dyn ToolHost>,
        programmatic: bool,
        omit_default_tool_timeout: bool,
        tool_timeout: Duration,
    ) -> ToolReply {
        let (tool, args, context, state, _guard) = match preparation {
            TurnPreparation::Ready {
                tool,
                args,
                context,
                state,
                guard,
            } => (tool, args, context, state, guard),
            TurnPreparation::Skipped(reason) => return Outcome::Skipped(reason).into(),
        };
        let timeout = if omit_default_tool_timeout && !tool.tool.use_default_timeout() {
            None
        } else {
            Some(tool_timeout)
        };
        let invocation = async {
            if programmatic {
                tool.tool
                    .invoke_programmatic(args, context, state.as_ref(), host)
                    .await
            } else {
                tool.tool
                    .invoke_transaction_with_timeout(args, context, state.as_ref(), host, timeout)
                    .await
            }
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Outcome::Unknown("cancelled after dispatch was committed".into()).into(),
            result = async {
                match timeout {
                    Some(timeout) => platform_time::timeout(timeout, invocation).await
                        .unwrap_or_else(|_| Outcome::Unknown("tool timed out".into()).into()),
                    None => invocation.await,
                }
            } => result,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn turn_transform(
        registry: &Registry,
        tool_timeout: Duration,
        invocation: &Invocation,
        reply: &mut ToolReply,
        states: &ExtensionStates,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
        host: Option<Arc<dyn ToolHost>>,
    ) {
        if matches!(
            reply.outcome,
            Outcome::Skipped(_) | Outcome::Unknown(_) | Outcome::Pending { .. }
        ) {
            return;
        }
        for extension in registry
            .extensions
            .iter()
            .filter(|item| item.handles_tool_result())
        {
            let context = ToolContext {
                operation_id: invocation.operation_id,
                cancellation: cancel.child_token(),
            };
            let _cancel_on_drop = context.cancellation.clone().drop_guard();
            let result = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                result = platform_time::timeout(tool_timeout, async {
                    match &host {
                        Some(host) => extension.tool_result_update_with_host(&invocation.call, &reply.outcome, &context, states.get(extension.id()), host.for_extension(extension.id()).unwrap_or_else(|| host.clone())).await,
                        None => extension.tool_result_update(&invocation.call, &reply.outcome, &context, states.get(extension.id())).await,
                    }
                }) => result,
            };
            let error = match result {
                Ok(Ok(Some(outcome)))
                    if !matches!(
                        outcome,
                        Outcome::Skipped(_) | Outcome::Unknown(_) | Outcome::Pending { .. }
                    ) =>
                {
                    reply.outcome = outcome;
                    continue;
                }
                Ok(Ok(None)) => continue,
                Ok(Ok(Some(_))) => "invalid replacement outcome".to_owned(),
                Ok(Err(error)) => error,
                Err(_) => "tool result hook timed out".to_owned(),
            };
            observer(Notification::ExtensionLifecycleError {
                extension: extension.id(),
                event: "tool_result",
                message: &error,
            });
        }
    }
}

// The scheduler owns all operation metadata for one model tool turn. Tool futures
// send events back; only this loop is allowed to mutate the journal or state.
struct TurnCall {
    invocation: Invocation,
    parent: Option<String>,
    lane: Option<Uuid>,
    cancellation: CancellationToken,
    depth: usize,
    call_count: Arc<AtomicUsize>,
    reply: Option<oneshot::Sender<Outcome>>,
    parallel: bool,
    polled: bool,
    phase: TurnPhase,
}
enum TurnPhase {
    Queued,
    Preparing,
    Prepared(TurnPreparation),
    Starting,
    Running,
    Polling,
    NotStarted(String),
    Done(Box<ToolReply>, Option<u64>),
    Transforming,
    Notifying,
    Finished,
}
#[derive(Default)]
struct TurnLane {
    calls: VecDeque<usize>,
    parallel: bool,
    active: usize,
}
enum TurnEvent {
    Normalized(
        usize,
        Result<TurnNormalized>,
        Vec<(String, &'static str, String)>,
    ),
    Prepared(
        usize,
        Result<TurnPreparation>,
        Vec<(String, &'static str, String)>,
    ),
    Begin(usize, TurnPreparation),
    Ran(
        usize,
        ToolReply,
        Option<u64>,
        Vec<(String, &'static str, String)>,
    ),
    Transformed(
        usize,
        ToolReply,
        Option<u64>,
        Vec<(String, &'static str, String)>,
    ),
    Notified(usize, Outcome, Vec<(String, &'static str, String)>),
    PollPrepared(
        usize,
        Result<(Arc<RegisteredTool>, serde_json::Value)>,
        String,
        Option<u64>,
        Vec<(String, &'static str, String)>,
    ),
    Polled(
        usize,
        Result<Outcome>,
        String,
        Option<u64>,
        Vec<(String, &'static str, String)>,
    ),
}
type TurnFuture = futures_util::future::BoxFuture<'static, TurnEvent>;

async fn start_turn_future(mut future: TurnFuture) -> TurnFuture {
    match futures_util::poll!(future.as_mut()) {
        std::task::Poll::Ready(event) => futures_util::future::ready(event).boxed(),
        std::task::Poll::Pending => future,
    }
}

fn turn_poll_unsettled(call: &TurnCall) -> bool {
    call.polled
        && matches!(
            call.phase,
            TurnPhase::Polling | TurnPhase::Done(..) | TurnPhase::Transforming
        )
}

fn turn_write_barrier(
    calls: &[TurnCall],
    positions: &HashMap<Uuid, usize>,
    writes: &HashSet<usize>,
    index: usize,
) -> bool {
    writes.iter().any(|&write_index| {
        let write = &calls[write_index];
        let mut parent = calls[index].lane;
        while let Some(operation) = parent {
            if operation == write.invocation.operation_id {
                return false;
            }
            parent = positions
                .get(&operation)
                .and_then(|&index| calls[index].lane);
        }
        // Mutations on unrelated branches must not overlap. Descendants of
        // the active write were handled above and may run on its behalf.
        if !calls[index].parallel {
            return true;
        }
        let Some(candidate_parent) = calls[index].lane else {
            return true;
        };
        let mut parent = write.lane;
        while let Some(operation) = parent {
            if operation == candidate_parent {
                return true;
            }
            parent = positions
                .get(&operation)
                .and_then(|&index| calls[index].lane);
        }
        false
    })
}

fn turn_is_started_peer_descendant(
    calls: &[TurnCall],
    positions: &HashMap<Uuid, usize>,
    origin: usize,
    index: usize,
) -> bool {
    let mut failed_branch = vec![origin];
    let mut parent = calls[origin].lane;
    while let Some(operation) = parent {
        let Some(&ancestor_index) = positions.get(&operation) else {
            break;
        };
        failed_branch.push(ancestor_index);
        parent = calls[ancestor_index].lane;
    }
    let mut parent = calls[index].lane;
    while let Some(operation) = parent {
        let Some(&ancestor_index) = positions.get(&operation) else {
            return false;
        };
        let ancestor = &calls[ancestor_index];
        if !matches!(ancestor.phase, TurnPhase::Queued | TurnPhase::Finished)
            && failed_branch
                .iter()
                .any(|&failed| failed != ancestor_index && calls[failed].lane == ancestor.lane)
        {
            return true;
        }
        parent = ancestor.lane;
    }
    false
}

fn record_turn_error(
    first: &mut Option<Error>,
    origin: &mut Option<usize>,
    index: usize,
    error: Error,
) {
    if first.is_none()
        || (matches!(first, Some(Error::PendingOperation(_)))
            && !matches!(error, Error::PendingOperation(_)))
        || (matches!(error, Error::Reconciliation(_))
            && !matches!(first, Some(Error::Reconciliation(_))))
    {
        *first = Some(error);
        *origin = Some(index);
    }
}

impl Agent<'_> {
    pub(super) async fn dispatch_turn<T: Into<AgentInput> + Send>(
        &self,
        session: &mut Session,
        invocations: &[Invocation],
        scope: &DispatchScope<'_>,
        states: &mut ExtensionStates,
        observer: &mut Observer<'_>,
        mut inputs: Option<&mut mpsc::Receiver<T>>,
    ) -> Result<()> {
        let observe_tools = self.registry.extensions.iter().any(|extension| {
            extension.handles_lifecycle() && extension.handles_tool_execution_lifecycle()
        });
        let (requests, mut incoming) = mpsc::channel::<Vec<NestedRequest>>(32);
        let observe_progress = self.registry.extensions.iter().any(|extension| {
            extension.handles_lifecycle()
                && extension.handles_tool_execution_lifecycle()
                && extension.handles_tool_execution_updates()
        });
        let (progress_output, mut progress) = mpsc::channel::<TurnProgress>(32);
        let (lifecycle_sender, mut lifecycle_messages) = mpsc::channel(32);
        let lifecycle_host = Arc::new(LifecycleMessageHost {
            sender: lifecycle_sender.clone(),
            idle: false,
        });
        let mut futures = FuturesUnordered::<TurnFuture>::new();
        let mut calls = Vec::<TurnCall>::new();
        let mut positions = HashMap::<Uuid, usize>::new();
        let mut writes = HashSet::<usize>::new();
        let mut lanes = HashMap::<Option<Uuid>, TurnLane>::new();
        let mut snapshots = HashMap::<Uuid, ReplySnapshot>::new();
        let mut nested_groups = HashMap::<usize, (Vec<usize>, bool)>::new();
        let all_tools: HashSet<String> = self.registry.tools.keys().cloned().collect();
        let specs = Arc::new(self.registry.tool_specs());
        let mut first_error = None;
        let mut error_origin = None;
        for invocation in invocations {
            let index = calls.len();
            positions.insert(invocation.operation_id, index);
            calls.push(TurnCall {
                invocation: invocation.clone(),
                parent: None,
                lane: None,
                cancellation: scope.cancellation.child_token(),
                depth: scope.depth,
                call_count: scope.call_count.clone(),
                reply: None,
                parallel: self
                    .registry
                    .tools
                    .get(&invocation.call.name)
                    .is_some_and(|tool| tool.tool.execution() == ToolExecution::ParallelRead),
                polled: false,
                phase: TurnPhase::Queued,
            });
            lanes.entry(None).or_default().calls.push_back(index);
        }
        // Root calls are a fixed model-provided list. Match the original
        // dispatch contract by preparing each fixed group before starting any
        // member; dynamically arriving nested calls are never batched here.
        let mut root_groups = VecDeque::<Vec<usize>>::new();
        let mut start = 0;
        while start < invocations.len() {
            let mut end = start + 1;
            if calls[start].parallel {
                while end < invocations.len() && end - start < 8 && calls[end].parallel {
                    end += 1;
                }
            }
            root_groups.push_back((start..end).collect());
            start = end;
        }
        loop {
            while root_groups.front().is_some_and(|group| {
                group
                    .iter()
                    .all(|&index| matches!(calls[index].phase, TurnPhase::Finished))
            }) {
                root_groups.pop_front();
            }
            let fatal = first_error.is_some();
            if fatal {
                for index in 0..calls.len() {
                    let peer_read = calls[index].parallel
                        && error_origin.is_some_and(|origin| {
                            turn_is_started_peer_descendant(&calls, &positions, origin, index)
                        });
                    if matches!(calls[index].phase, TurnPhase::Prepared(_)) && !peer_read {
                        calls[index].phase =
                            TurnPhase::NotStarted("group preparation failed".into());
                    }
                }
            }
            if fatal {
                let failed_lane = error_origin.map(|index: usize| calls[index].lane);
                if failed_lane
                    .is_some_and(|lane| lanes.get(&lane).is_none_or(|lane| lane.active == 0))
                {
                    let mut parent = error_origin.and_then(|index: usize| calls[index].lane);
                    while let Some(operation) = parent {
                        let Some(call) = calls
                            .iter()
                            .find(|call| call.invocation.operation_id == operation)
                        else {
                            break;
                        };
                        call.cancellation.cancel();
                        parent = call.lane;
                    }
                }
                if lanes.values().all(|lane| lane.active == 0) {
                    break;
                }
            }
            let polling = calls.iter().any(turn_poll_unsettled);
            // A lane admits at most eight consecutive reads. A write closes the
            // current lane epoch until all earlier calls have settled.
            for (lane_id, lane) in &mut lanes {
                if lane
                    .calls
                    .iter()
                    .any(|&index| matches!(calls[index].phase, TurnPhase::Polling))
                {
                    continue;
                }
                let mut preparing = lane
                    .calls
                    .iter()
                    .any(|&index| matches!(calls[index].phase, TurnPhase::Preparing));
                for &index in &lane.calls {
                    if !matches!(calls[index].phase, TurnPhase::Queued) {
                        continue;
                    }
                    if preparing {
                        break;
                    }
                    if lane_id.is_none()
                        && !root_groups
                            .front()
                            .is_some_and(|group| group.contains(&index))
                    {
                        break;
                    }
                    if fatal && !calls[index].parallel {
                        lane.active += 1;
                        calls[index].phase =
                            TurnPhase::NotStarted("turn requires reconciliation".into());
                        continue;
                    }
                    if fatal
                        && !error_origin.is_some_and(|origin| {
                            turn_is_started_peer_descendant(&calls, &positions, origin, index)
                        })
                    {
                        break;
                    }
                    let parallel = calls[index].parallel;
                    if polling && !parallel {
                        break;
                    }
                    if turn_write_barrier(&calls, &positions, &writes, index) {
                        break;
                    }
                    if lane.active == 0 {
                        lane.parallel = parallel;
                    }
                    if lane.active != 0 && (!lane.parallel || !parallel) {
                        break;
                    }
                    let preparing_count = lane
                        .calls
                        .iter()
                        .filter(|&&item| {
                            matches!(
                                calls[item].phase,
                                TurnPhase::Preparing | TurnPhase::Prepared(_)
                            )
                        })
                        .count();
                    if (lane_id.is_none() && lane.active >= 8)
                        || (lane_id.is_some() && preparing_count >= 8)
                    {
                        break;
                    }
                    lane.active += 1;
                    calls[index].phase = TurnPhase::Preparing;
                    preparing = true;
                    if !parallel {
                        writes.insert(index);
                    }
                    let registry = self.registry.clone();
                    let state_snapshot = states.clone();
                    let advertised = if calls[index].depth == 0 {
                        scope.advertised.clone()
                    } else {
                        all_tools.clone()
                    };
                    let invocation = calls[index].invocation.clone();
                    let parent = calls[index].parent.clone();
                    let cancel = calls[index].cancellation.clone();
                    let model_timeout = self.limits.model_timeout;
                    let tool_timeout = self.limits.tool_timeout;
                    let hook_host = lifecycle_host.clone();
                    futures.push(async move {
                        let mut errors = Vec::new();
                        if observe_tools {
                            let event = crate::LifecycleEvent::ToolExecutionStart {
                                tool_call_id: invocation.call.id.clone(), tool_name: invocation.call.name.clone(),
                                args: crate::parse_tool_arguments(&invocation.call.arguments).unwrap_or(serde_json::Value::Null),
                                parent_tool_call_id: parent,
                            };
                            let mut collect = |event: Notification<'_>| if let Notification::ExtensionLifecycleError { extension, event, message } = event { errors.push((extension.to_owned(), event, message.to_owned())); };
                            tokio::select! { biased; _ = cancel.cancelled() => {}, _ = crate::lifecycle::notify_with_host(&registry, event, &state_snapshot, model_timeout, &mut collect, hook_host) => {} }
                        }
                        let result = Self::turn_normalize(&registry, tool_timeout, &invocation, &advertised, &cancel).await;
                        TurnEvent::Normalized(index, result, errors)
                    }.boxed());
                    if !parallel {
                        break;
                    }
                }
            }
            if !fatal
                && let Some(group) = root_groups.front()
                && group.iter().all(|&index| {
                    matches!(calls[index].phase, TurnPhase::Prepared(_))
                        && (!polling || calls[index].parallel)
                })
            {
                for &index in group {
                    let TurnPhase::Prepared(prepared) =
                        std::mem::replace(&mut calls[index].phase, TurnPhase::Starting)
                    else {
                        unreachable!()
                    };
                    futures.push(
                        futures_util::future::ready(TurnEvent::Begin(index, prepared)).boxed(),
                    );
                }
            }
            // A submitted batch is authorized together, but available read
            // slots may be filled gradually while other reads are active.
            let group_ids: Vec<usize> = nested_groups.keys().copied().collect();
            for id in group_ids {
                let (group, ready) = nested_groups.get_mut(&id).expect("nested group");
                // A scoped cancellation can arrive after one member prepared
                // but before its peers finish preparation. Retire prepared
                // members and detach skipped members from the batch gate; the
                // lane still journals their skipped outcomes in order.
                for &index in group.iter() {
                    if !scope.cancellation.is_cancelled()
                        && calls[index].cancellation.is_cancelled()
                        && matches!(calls[index].phase, TurnPhase::Prepared(_))
                    {
                        calls[index].phase =
                            TurnPhase::NotStarted("cancelled before dispatch".into());
                    }
                }
                group.retain(|&index| {
                    !matches!(
                        calls[index].phase,
                        TurnPhase::NotStarted(_) | TurnPhase::Finished
                    )
                });
                if group.is_empty() {
                    nested_groups.remove(&id);
                    continue;
                }
                if !*ready
                    && group
                        .iter()
                        .all(|&index| matches!(calls[index].phase, TurnPhase::Prepared(_)))
                {
                    *ready = true;
                }
                if !*ready {
                    continue;
                }
                while let Some(&index) = group.first() {
                    let peer_read = calls[index].parallel
                        && error_origin.is_some_and(|origin| {
                            turn_is_started_peer_descendant(&calls, &positions, origin, index)
                        });
                    if (fatal && !peer_read) || (polling && !calls[index].parallel) {
                        break;
                    }
                    let lane = lanes.get(&calls[index].lane).expect("turn lane");
                    if lane
                        .calls
                        .iter()
                        .take_while(|&&item| item != index)
                        .any(|&item| {
                            matches!(
                                calls[item].phase,
                                TurnPhase::Queued | TurnPhase::Preparing | TurnPhase::Prepared(_)
                            )
                        })
                    {
                        break;
                    }
                    let running = lane
                        .calls
                        .iter()
                        .filter(|&&item| {
                            matches!(
                                calls[item].phase,
                                TurnPhase::Starting
                                    | TurnPhase::Running
                                    | TurnPhase::Done(..)
                                    | TurnPhase::Transforming
                                    | TurnPhase::Notifying
                                    | TurnPhase::Polling
                            )
                        })
                        .count();
                    if running >= 8 {
                        break;
                    }
                    group.remove(0);
                    let TurnPhase::Prepared(prepared) =
                        std::mem::replace(&mut calls[index].phase, TurnPhase::Starting)
                    else {
                        unreachable!()
                    };
                    futures.push(
                        futures_util::future::ready(TurnEvent::Begin(index, prepared)).boxed(),
                    );
                }
                if group.is_empty() {
                    nested_groups.remove(&id);
                }
            }
            let skipped = lanes
                .values()
                .filter_map(|lane| lane.calls.front().copied())
                .filter(|&index| matches!(calls[index].phase, TurnPhase::NotStarted(_)))
                .collect::<Vec<_>>();
            let skipped_any = !skipped.is_empty();
            for index in skipped {
                let TurnPhase::NotStarted(reason) =
                    std::mem::replace(&mut calls[index].phase, TurnPhase::Finished)
                else {
                    unreachable!()
                };
                let outcome = Outcome::Skipped(reason);
                commit_with_reply(
                    session,
                    Event::ToolFinished {
                        operation_id: calls[index].invocation.operation_id,
                        outcome: outcome.clone(),
                        states: ExtensionStates::new(),
                    },
                    observer,
                    Some(&snapshots),
                )?;
                if let Some(reply) = calls[index].reply.take() {
                    let _ = reply.send(outcome);
                }
                snapshots.remove(&calls[index].invocation.operation_id);
                writes.remove(&index);
                let lane = lanes.get_mut(&calls[index].lane).expect("turn lane");
                assert_eq!(lane.calls.pop_front(), Some(index));
                lane.active -= 1;
            }
            if fatal && lanes.values().all(|lane| lane.active == 0) {
                break;
            }
            if skipped_any {
                continue;
            }
            // The lane head alone transforms against the state committed at
            // hook start. Other lanes can commit while this hook is suspended;
            // its snapshot remains unchanged and the hook is never replayed.
            for lane in lanes.values() {
                let Some(&index) = lane.calls.front() else {
                    continue;
                };
                let phase = std::mem::replace(&mut calls[index].phase, TurnPhase::Transforming);
                let TurnPhase::Done(reply, duration) = phase else {
                    calls[index].phase = phase;
                    continue;
                };
                let mut reply = *reply;
                let registry = self.registry.clone();
                let snapshot = states.clone();
                let invocation = calls[index].invocation.clone();
                let cancel = calls[index].cancellation.clone();
                let timeout = self.limits.tool_timeout;
                let hook_host = callback_root_host(
                    session,
                    lifecycle_sender.clone(),
                    invocation.operation_id,
                    "recovered_tool_result",
                );
                let future = async move {
                    let mut errors = Vec::new();
                    let mut collect = |event: Notification<'_>| {
                        if let Notification::ExtensionLifecycleError {
                            extension,
                            event,
                            message,
                        } = event
                        {
                            errors.push((extension.to_owned(), event, message.to_owned()));
                        }
                    };
                    Self::turn_transform(
                        &registry,
                        timeout,
                        &invocation,
                        &mut reply,
                        &snapshot,
                        &cancel,
                        &mut collect,
                        Some(hook_host),
                    )
                    .await;
                    TurnEvent::Transformed(index, reply, duration, errors)
                }
                .boxed();
                let started = start_turn_future(future).await;
                futures.push(started);
            }
            if lanes.values().all(|lane| lane.calls.is_empty()) {
                break;
            }
            tokio::select! {
                Some(batch) = incoming.recv() => {
                    let mut group_indices = Vec::new();
                    for request in batch {
                        let operation_id = Uuid::new_v4();
                        let invocation = Invocation { operation_id, call: crate::ToolCall { id: operation_id.to_string(), name: request.name, arguments: request.arguments.to_string() } };
                        commit(session, Event::ToolPlanned { invocation: invocation.clone(), parent_operation: request.parent_operation }, observer)?;
                        let parent = tool_parent_call_id(session, invocation.operation_id);
                        let lane = Some(request.parent_operation);
                        let index = calls.len();
                        positions.insert(invocation.operation_id, index);
                        snapshots.insert(invocation.operation_id, request.snapshot);
                        calls.push(TurnCall { parallel: self.registry.tools.get(&invocation.call.name).is_some_and(|tool| tool.tool.execution() == ToolExecution::ParallelRead), polled: false, invocation, parent, lane, cancellation: request.cancellation, depth: request.depth, call_count: request.call_count, reply: Some(request.reply), phase: TurnPhase::Queued });
                        lanes.entry(lane).or_default().calls.push_back(index);
                        group_indices.push(index);
                    }
                    let mut start = 0;
                    while start < group_indices.len() {
                        let mut end = start + 1;
                        if calls[group_indices[start]].parallel {
                            while end < group_indices.len() && end - start < 8 && calls[group_indices[end]].parallel { end += 1; }
                        }
                        let group = group_indices[start..end].to_vec();
                        nested_groups.insert(group[0], (group, false));
                        start = end;
                    }
                },
                Some(event) = futures.next() => match event {
                    TurnEvent::Normalized(index, normalized, errors) => {
                        for (extension,event,message) in &errors { observer(Notification::ExtensionLifecycleError { extension, event, message }); }
                        match normalized {
                            Err(error) => { let reason = error.to_string(); if !(matches!(error, Error::Cancelled) && calls[index].lane.is_some() && calls[index].cancellation.is_cancelled() && !scope.cancellation.is_cancelled()) { record_turn_error(&mut first_error, &mut error_origin, index, error); } calls[index].phase = TurnPhase::NotStarted(reason); }
                            Ok(normalized) => {
                                let registry = self.registry.clone(); let snapshot = states.clone();
                                let invocation = calls[index].invocation.clone(); let cancel = calls[index].cancellation.clone(); let timeout = self.limits.tool_timeout;
                                let hook_host = lifecycle_host.clone();
                                let future = async move {
                                    let result = Self::turn_finish_preparation(&registry, timeout, &invocation, normalized, &snapshot, &cancel, Some(hook_host)).await;
                                    TurnEvent::Prepared(index, result, Vec::new())
                                }.boxed();
                                let started = start_turn_future(future).await;
                                futures.push(started);
                            }
                        }
                    }
                    TurnEvent::Prepared(index, prepared, errors) => {
                        for (extension,event,message) in &errors { observer(Notification::ExtensionLifecycleError { extension, event, message }); }
                        match prepared {
                            Err(error) => { let reason = error.to_string(); if !(matches!(error, Error::Cancelled) && calls[index].lane.is_some() && calls[index].cancellation.is_cancelled() && !scope.cancellation.is_cancelled()) { record_turn_error(&mut first_error, &mut error_origin, index, error); } calls[index].phase = TurnPhase::NotStarted(reason); }
                            Ok(prepared) => { calls[index].phase = TurnPhase::Prepared(prepared); }
                        }
                    }
                    TurnEvent::Begin(index, prepared) => {
                                if !calls[index].parallel && calls.iter().any(turn_poll_unsettled) {
                                    calls[index].phase = TurnPhase::Prepared(prepared);
                                    if calls[index].lane.is_some() { nested_groups.insert(index, (vec![index], true)); }
                                    continue;
                                }
                                if calls[index].cancellation.is_cancelled() {
                                    if calls[index].lane.is_none() || scope.cancellation.is_cancelled() {
                                        record_turn_error(&mut first_error, &mut error_origin, index, Error::Cancelled);
                                    }
                                    calls[index].phase = TurnPhase::NotStarted("cancelled before dispatch".into());
                                    continue;
                                }
                                if first_error.is_some() && (!calls[index].parallel || !error_origin.is_some_and(|origin| turn_is_started_peer_descendant(&calls, &positions, origin, index))) {
                                    calls[index].phase = TurnPhase::NotStarted("turn stopped before dispatch".into());
                                    continue;
                                }
                                let ran = matches!(&prepared, TurnPreparation::Ready { .. });
                                if ran { commit(session, Event::ToolStarted { operation_id: calls[index].invocation.operation_id }, observer)?; }
                                let cancellation = match &prepared { TurnPreparation::Ready { context, .. } => context.cancellation.clone(), TurnPreparation::Skipped(_) => calls[index].cancellation.child_token() };
                                let (updates, mut progress_input) = mpsc::channel::<TurnProgress>(32);
                                let (finish_progress, mut finish_signal) = oneshot::channel();
                                let progress_output = progress_output.clone();
                                let progress_registry = self.registry.clone();
                                let progress_cancel = calls[index].cancellation.clone();
                                let progress_states = states.clone();
                                let progress_timeout = self.limits.model_timeout;
                                let hook_host = lifecycle_host.clone();
                                let progress_worker = tokio::spawn(async move {
                                    let mut errors = Vec::new();
                                    let mut closing = false;
                                    loop {
                                        let message = tokio::select! {
                                            _ = &mut finish_signal, if !closing => { closing = true; progress_input.close(); continue; },
                                            message = progress_input.recv() => match message { Some(message) => message, None => break },
                                        };
                                        match message {
                                            TurnProgress::Event(event) => {
                                                let _ = progress_output.send(TurnProgress::Event(event.clone())).await;
                                                if observe_progress {
                                                    let mut collect = |notification: Notification<'_>| {
                                                        if let Notification::ExtensionLifecycleError { extension, event, message } = notification {
                                                            errors.push((extension.to_owned(), event, message.to_owned()));
                                                        }
                                                    };
                                                    tokio::select! { biased; _ = progress_cancel.cancelled() => {}, _ = crate::lifecycle::notify_with_host(&progress_registry, *event, &progress_states, progress_timeout, &mut collect, hook_host.clone()) => {} }
                                                }
                                            }
                                            TurnProgress::Custom(input, reply) => { let _ = progress_output.send(TurnProgress::Custom(input, reply)).await; }
                                            TurnProgress::Flush(reply) => { let _ = progress_output.send(TurnProgress::Flush(reply)).await; }
                                        }
                                    }
                                    errors
                                });
                                let host = Arc::new(NestedHost { ui: self.registry.ui.clone(), requests: requests.clone(), specs: specs.clone(), parent_operation: calls[index].invocation.operation_id, cancellation, depth: calls[index].depth, call_count: calls[index].call_count.clone(), progress: Some(ProgressReporter { sender: ProgressSink::Turn(updates.clone()), tool_call_id: calls[index].invocation.call.id.clone(), tool_name: calls[index].invocation.call.name.clone(), args: crate::parse_tool_arguments(&calls[index].invocation.call.arguments).unwrap_or(serde_json::Value::Null), parent_tool_call_id: calls[index].parent.clone() }) });
                                let cancel = calls[index].cancellation.clone(); let programmatic = calls[index].depth > 0;
                                let omit = self.registry.omit_default_tool_timeout; let timeout = self.limits.tool_timeout;
                                calls[index].phase = TurnPhase::Running;
                                let flush = updates.clone();
                                futures.push(async move { let start = platform_time::Instant::now(); let reply = Self::turn_invoke(prepared, &cancel, host, programmatic, omit, timeout).await; let duration = ran.then(|| start.elapsed().as_millis().min(u64::MAX as u128) as u64); let (send, receive) = oneshot::channel(); if flush.send(TurnProgress::Flush(send)).await.is_ok() { let _ = receive.await; } let _ = finish_progress.send(()); let errors = progress_worker.await.unwrap_or_else(|error| vec![("tool-progress".into(), "tool_execution_update", error.to_string())]); TurnEvent::Ran(index, reply, duration, errors) }.boxed());
                    }
                    TurnEvent::Ran(index, reply, duration, errors) => { for (extension,event,message) in &errors { observer(Notification::ExtensionLifecycleError { extension, event, message }); } calls[index].phase = TurnPhase::Done(Box::new(reply), duration); }
                    TurnEvent::Transformed(index, reply, duration, errors) => {
                        for (extension,event,message) in &errors { observer(Notification::ExtensionLifecycleError { extension, event, message }); }
                        let finished = self.finish_tool(session, &calls[index].invocation, reply, states, observer, Some(&snapshots));
                        let outcome = session.operations().into_iter().find_map(|(invocation,state)| if invocation.operation_id == calls[index].invocation.operation_id { if let OperationState::Finished(outcome) = state { Some(outcome) } else { None } } else { None }).unwrap_or_else(|| Outcome::Unknown("tool produced no durable result".into()));
                        if matches!(&finished, Err(Error::PendingOperation(_)))
                            && let Outcome::Pending { reference } = &outcome
                            && !calls[index].polled
                            && self.registry.tools.get(&calls[index].invocation.call.name).is_some_and(|tool| tool.tool.can_poll())
                        {
                            let registry = self.registry.clone(); let snapshot = states.clone();
                            let invocation = calls[index].invocation.clone(); let cancel = calls[index].cancellation.clone();
                            let timeout = self.limits.tool_timeout; let model_timeout = self.limits.model_timeout; let reference = reference.clone();
                            let parent = calls[index].parent.clone();
                            let pending_outcome = outcome.clone();
                            let hook_host = lifecycle_host.clone();
                            calls[index].phase = TurnPhase::Polling;
                            calls[index].polled = true;
                            let future = async move {
                                let mut errors = Vec::new();
                                if observe_tools {
                                    let event = crate::LifecycleEvent::ToolExecutionEnd { tool_call_id: invocation.call.id.clone(), tool_name: invocation.call.name.clone(), result: pending_outcome, is_error: false, duration_ms: duration, parent_tool_call_id: parent };
                                    let mut collect = |event: Notification<'_>| if let Notification::ExtensionLifecycleError { extension, event, message } = event { errors.push((extension.to_owned(), event, message.to_owned())); };
                                    tokio::select! { biased; _ = cancel.cancelled() => {}, _ = crate::lifecycle::notify_with_host(&registry, event, &snapshot, model_timeout, &mut collect, hook_host) => {} }
                                }
                                let result = Self::turn_poll_normalize(&registry, timeout, &invocation, &cancel).await;
                                TurnEvent::PollPrepared(index, result, reference, duration, errors)
                            }.boxed();
                            let started = start_turn_future(future).await;
                            futures.push(started);
                            continue;
                        }
                        if let Err(error) = finished { record_turn_error(&mut first_error, &mut error_origin, index, error); }
                        calls[index].phase = TurnPhase::Notifying;
                        let registry = self.registry.clone(); let snapshot = states.clone(); let timeout = self.limits.model_timeout;
                        let invocation = calls[index].invocation.clone(); let parent = calls[index].parent.clone();
                        let notify_end = observe_tools && !calls[index].polled;
                        let hook_host = lifecycle_host.clone();
                        futures.push(async move {
                            let mut errors = Vec::new();
                            if notify_end {
                                let is_error = outcome.is_error();
                                let event = crate::LifecycleEvent::ToolExecutionEnd { tool_call_id: invocation.call.id, tool_name: invocation.call.name, result: outcome.clone(), is_error, duration_ms: duration, parent_tool_call_id: parent };
                                let mut collect = |event: Notification<'_>| if let Notification::ExtensionLifecycleError { extension, event, message } = event { errors.push((extension.to_owned(),event,message.to_owned())); };
                                crate::lifecycle::notify_with_host(&registry,event,&snapshot,timeout,&mut collect,hook_host).await;
                            }
                            TurnEvent::Notified(index,outcome,errors)
                        }.boxed());
                    }
                    TurnEvent::Notified(index, outcome, errors) => {
                        for (extension,event,message) in &errors { observer(Notification::ExtensionLifecycleError { extension, event, message }); }
                        if let Some(reply) = calls[index].reply.take() { let _ = reply.send(outcome); }
                        snapshots.remove(&calls[index].invocation.operation_id);
                        calls[index].phase = TurnPhase::Finished;
                        writes.remove(&index);
                        let lane = lanes.get_mut(&calls[index].lane).expect("turn lane");
                        assert_eq!(lane.calls.pop_front(), Some(index)); lane.active -= 1;
                    }
                    TurnEvent::PollPrepared(index, result, reference, duration, errors) => {
                        for (extension,event,message) in &errors { observer(Notification::ExtensionLifecycleError { extension,event,message }); }
                        match result {
                            Ok((tool, args)) => {
                                let registry = self.registry.clone(); let snapshot = states.clone();
                                let invocation = calls[index].invocation.clone(); let cancel = calls[index].cancellation.clone(); let timeout = self.limits.tool_timeout;
                                let hook_host = lifecycle_host.clone();
                                let future = async move {
                                    let result = Self::turn_poll_authorize_and_run(&registry, timeout, &invocation, &reference, (tool, args), &snapshot, &cancel, Some(hook_host)).await;
                                    TurnEvent::Polled(index, result, reference, duration, Vec::new())
                                }.boxed();
                                let started = start_turn_future(future).await;
                                futures.push(started);
                            }
                            Err(error) => { futures.push(futures_util::future::ready(TurnEvent::Polled(index, Err(error), reference, duration, Vec::new())).boxed()); }
                        }
                    }
                    TurnEvent::Polled(index, result, reference, duration, errors) => {
                        for (extension,event,message) in &errors { observer(Notification::ExtensionLifecycleError { extension,event,message }); }
                        match result {
                        Ok(outcome) => calls[index].phase = TurnPhase::Done(Box::new(outcome.into()), duration),
                        Err(error) => {
                            record_turn_error(&mut first_error, &mut error_origin, index, error);
                            calls[index].phase = TurnPhase::Notifying;
                            futures.push(async move { TurnEvent::Notified(index, Outcome::Pending { reference }, Vec::new()) }.boxed());
                        }
                        }
                    },
                },
                Some(message) = progress.recv() => match message { TurnProgress::Event(event) => observer(Notification::ToolProgress(&event)), TurnProgress::Custom(input, reply) => { let result = queue_custom_message(session, input, observer).map_err(|error| error.to_string()); let _ = reply.send(result); }, TurnProgress::Flush(reply) => { let _ = reply.send(()); } },
                Some((message, reply)) = lifecycle_messages.recv() => {
                    let result = queue_custom_message(session, message, observer).map_err(|error| error.to_string());
                    let _ = reply.send(result);
                },
                Some(input) = receive_input(&mut inputs) => queue_input(session,input.into(),observer)?,
                else => return Err(Error::Config("turn scheduler lost its active work".into())),
            }
        }
        drop(progress_output);
        while let Some(message) = progress.recv().await {
            match message {
                TurnProgress::Event(event) => observer(Notification::ToolProgress(&event)),
                TurnProgress::Custom(input, reply) => {
                    let result = queue_custom_message(session, input, observer)
                        .map_err(|error| error.to_string());
                    let _ = reply.send(result);
                }
                TurnProgress::Flush(reply) => {
                    let _ = reply.send(());
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod start_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn first_poll_happens_before_a_foreign_commit() {
        let state = Arc::new(AtomicUsize::new(0));
        let observed = state.clone();
        let (release, wait) = oneshot::channel::<()>();
        let future = async move {
            let at_start = observed.load(Ordering::SeqCst);
            let _ = wait.await;
            TurnEvent::Notified(0, Outcome::Succeeded(at_start.to_string()), Vec::new())
        }
        .boxed();
        let future = start_turn_future(future).await;
        state.store(1, Ordering::SeqCst);
        let _ = release.send(());
        assert!(
            matches!(future.await, TurnEvent::Notified(_, Outcome::Succeeded(value), _) if value == "0")
        );
    }
}
