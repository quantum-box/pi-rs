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

// A validated decision alone is not sufficient authorization: callers must
// first bind the invocation to the successful active-branch model reply.
fn turn_model_arguments(
    invocation: &Invocation,
    spec: &ToolSpec,
    decision: Option<&crate::ValidatedToolDecisionSnapshot<'_>>,
) -> std::result::Result<serde_json::Value, String> {
    match decision {
        None => crate::parse_tool_arguments(&invocation.call.arguments)
            .map_err(|error| error.to_string()),
        Some(decision) => {
            if !decision
                .effective_tools()
                .iter()
                .any(|entry| &entry.spec == spec)
                || invocation.call.name != spec.name
            {
                return Err("registered tool does not match the request decision".into());
            }
            decision
                .normalized_arguments(&invocation.call)
                .map_err(|error| error.to_string())
        }
    }
}

// Reserved identity namespace for tools which cannot be polled. Pollable tool
// identities may never use this prefix, including across a restart.
const NON_POLLABLE_IDENTITY: &str = "pi-rs:non-pollable:v1";

pub(super) fn turn_recovery_identity(tool: &RegisteredTool) -> Result<String> {
    if !tool.tool.can_poll() {
        return Ok(NON_POLLABLE_IDENTITY.into());
    }
    match tool.tool.recovery_identity() {
        Some(identity) if !identity.is_empty() && !identity.starts_with("pi-rs:non-pollable:") => {
            Ok(identity)
        }
        _ => Err(Error::Session(
            "pollable constrained tool has no stable recovery identity".into(),
        )),
    }
}

// Preflight the same durable reply provenance that the journal checks again at
// start. This must run before argument preparation and extension hooks.
fn turn_active_decision(
    session: &Session,
    invocations: &[Invocation],
) -> Result<Option<crate::ToolDecisionSnapshot>> {
    turn_active_decision_inner(session, invocations, false)
}

fn turn_active_decision_inner(
    session: &Session,
    invocations: &[Invocation],
    allow_started: bool,
) -> Result<Option<crate::ToolDecisionSnapshot>> {
    if invocations.is_empty() {
        return Ok(None);
    }
    let branch = session.branch();
    let latest_request = branch
        .iter()
        .rev()
        .find(|record| matches!(record.event, Event::ModelRequested { .. }));
    let assistant = branch
        .iter()
        .rev()
        .find(|record| matches!(record.event, Event::Assistant { .. }));
    let latest_marked = match latest_request {
        Some(record) => !matches!(
            session.recorded_tool_decision(record.id)?,
            crate::RecordedToolDecision::Legacy
        ),
        None => false,
    };
    let Some(assistant) = assistant else {
        return if latest_marked {
            Err(Error::Session(
                "missing assistant reply for tool decision".into(),
            ))
        } else {
            Ok(None)
        };
    };
    let Event::Assistant {
        content,
        invocations: recorded,
        request_id,
        ..
    } = &assistant.event
    else {
        unreachable!()
    };
    if recorded != invocations
        && branch.iter().any(|record| {
            matches!(
                &record.event,
                Event::ModelRequested {
                    tool_decision: Some(_),
                    ..
                }
            )
        })
    {
        return Err(Error::Session(
            "tool invocations do not match the active assistant reply".into(),
        ));
    }
    let Some(request_id) = *request_id else {
        return if latest_marked {
            Err(Error::Session(
                "missing tool decision request identity".into(),
            ))
        } else {
            Ok(None)
        };
    };
    let Some(request) = branch.iter().find(|record| matches!(&record.event, Event::ModelRequested { request_id: Some(id), .. } if *id == request_id)) else {
        return if latest_marked { Err(Error::Session("missing tool decision request".into())) } else { Ok(None) };
    };
    let decision = match session.active_tool_decision(request_id)? {
        crate::RecordedToolDecision::Legacy if !latest_marked => return Ok(None),
        crate::RecordedToolDecision::Current(decision) => decision,
        _ => return Err(Error::Session("tool decision is not current".into())),
    };
    if decision.context().summary
        || decision.context().session_id != session.session_id()
        || recorded != invocations
        || request.run_id != session.current_run_id()
        || assistant.run_id != request.run_id
        || latest_request.map(|r| r.id) != Some(request.id)
    {
        return Err(Error::Session(
            "tool decision does not match active turn".into(),
        ));
    }
    let Event::ModelRequested {
        max_output_tokens, ..
    } = &request.event
    else {
        unreachable!()
    };
    let validated = decision
        .validate_recorded(
            decision.context(),
            decision.request_sha256(),
            decision.dispatch_sha256(),
            decision.binding_sha256(),
        )
        .map_err(|_| Error::Session("invalid active tool decision".into()))?;
    let mut completions = branch.iter().filter(|record| matches!(&record.event, Event::ModelFinished { request_id: id, .. } if *id == request_id));
    let completion = completions
        .next()
        .ok_or_else(|| Error::Session("missing model completion".into()))?;
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
    if completions.next().is_some() || request.id >= completion.id || completion.id >= assistant.id
        || completion.run_id != request.run_id || metadata.finish_reason.as_deref() == Some("length")
        || !partial.is_empty() || reply.max_output_tokens != *max_output_tokens
        || reply.advertised_tools.iter().map(String::as_str).collect::<Vec<_>>() != advertised
        || &reply.content != content || reply.invocations != *recorded
        || branch.iter().filter(|record| matches!(&record.event, Event::Assistant { request_id: Some(id), .. } if *id == request_id)).count() != 1
        || branch.iter().any(|record| record.id > request.id && record.id != assistant.id && matches!(&record.event, Event::Assistant { .. }))
    {
        return Err(Error::Session("tool decision reply provenance mismatch".into()));
    }
    let mut ids = HashSet::new();
    for invocation in invocations {
        if !ids.insert(invocation.operation_id)
            || branch.iter().any(|record| match &record.event {
                Event::Assistant { invocations, .. } if record.id != assistant.id => invocations
                    .iter()
                    .any(|other| other.operation_id == invocation.operation_id),
                Event::ToolPlanned {
                    invocation: other, ..
                } => other.operation_id == invocation.operation_id,
                Event::ToolStarted { operation_id, .. }
                | Event::ToolFinished { operation_id, .. }
                | Event::ToolReconciled { operation_id, .. } => {
                    !allow_started && *operation_id == invocation.operation_id
                }
                _ => false,
            })
        {
            return Err(Error::Session(
                "ambiguous tool invocation provenance".into(),
            ));
        }
    }
    Ok(Some(decision))
}

fn turn_is_strict(decision: &crate::ToolDecisionSnapshot, spec: &ToolSpec) -> Result<bool> {
    let validated = decision
        .validate_recorded(
            decision.context(),
            decision.request_sha256(),
            decision.dispatch_sha256(),
            decision.binding_sha256(),
        )
        .map_err(|_| Error::Session("invalid tool decision".into()))?;
    let effective = validated
        .effective_tools()
        .iter()
        .find(|entry| &entry.spec == spec)
        .ok_or_else(|| Error::Session("tool does not match request decision".into()))?;
    Ok(matches!(
        effective.sampling,
        crate::EffectiveToolSampling::StrictJsonSchema { .. }
    ))
}

fn turn_strict_tool_preflight(tool: &RegisteredTool) -> Result<()> {
    turn_recovery_identity(tool)?;
    // Remove only when both pending recovery paths consume the durable snapshot.
    if tool.tool.can_poll() {
        return Err(Error::Session(
            "strict pollable tools require snapshot-aware recovery".into(),
        ));
    }
    Ok(())
}

// Preflight the complete direct-call batch before any lifecycle callback: a
// later invalid call must not let earlier callbacks perform effects either.
fn turn_preflight_decision_calls(
    registry: &Registry,
    advertised: &HashSet<String>,
    invocations: &[Invocation],
    decision: &crate::ToolDecisionSnapshot,
) -> Result<()> {
    let validated = decision
        .validate_recorded(
            decision.context(),
            decision.request_sha256(),
            decision.dispatch_sha256(),
            decision.binding_sha256(),
        )
        .map_err(|_| Error::Session("invalid tool decision".into()))?;
    for invocation in invocations {
        let Some(effective) = validated
            .effective_tools()
            .iter()
            .find(|entry| entry.spec.name == invocation.call.name)
        else {
            continue;
        };
        let Some(tool) = registry
            .tools
            .get(&invocation.call.name)
            .filter(|_| advertised.contains(&invocation.call.name))
        else {
            continue;
        };
        if tool.spec != effective.spec {
            return Err(Error::Session(
                "tool does not match request decision".into(),
            ));
        }
        if matches!(
            effective.sampling,
            crate::EffectiveToolSampling::StrictJsonSchema { .. }
        ) {
            turn_strict_tool_preflight(tool)?;
        }
    }
    Ok(())
}

// Resolve marked pending calls from their durable authorized arguments. None
// means a genuinely unmarked legacy start; corruption never falls back.
fn turn_poll_snapshot(
    session: &Session,
    registry: &Registry,
    invocation: &Invocation,
) -> Result<Option<(Arc<RegisteredTool>, serde_json::Value)>> {
    let pending = || Error::PendingOperation(invocation.operation_id);
    let tool = registry
        .tools
        .get(&invocation.call.name)
        .filter(|tool| tool.tool.can_poll())
        .cloned()
        .ok_or_else(pending)?;
    if !session
        .operations()
        .iter()
        .any(|(recorded, _)| recorded == invocation)
    {
        return Err(pending());
    }
    let branch = session.branch();
    let mut starts = branch.iter().filter(|record| matches!(&record.event, Event::ToolStarted { operation_id, .. } if *operation_id == invocation.operation_id));
    let start = starts.next().ok_or_else(pending)?;
    if starts.next().is_some() {
        return Err(pending());
    }
    let Event::ToolStarted { tool_execution, .. } = &start.event else {
        unreachable!()
    };
    // Classify direct model calls from their active request, not from the
    // start marker: both the marker and its metadata may have been stripped.
    let mut assistants = branch.iter().filter_map(|record| match &record.event {
        Event::Assistant { invocations, .. }
            if invocations
                .iter()
                .any(|call| call.operation_id == invocation.operation_id) =>
        {
            Some(invocations)
        }
        _ => None,
    });
    let decision = if let Some(invocations) = assistants.next() {
        if assistants.next().is_some() {
            return Err(pending());
        }
        turn_active_decision_inner(session, invocations, true).map_err(|_| pending())?
    } else {
        None
    };
    let strict = decision
        .as_ref()
        .map(|decision| turn_is_strict(decision, &tool.spec))
        .transpose()
        .map_err(|_| pending())?
        .unwrap_or(false);
    if strict && tool_execution.is_none() {
        return Err(pending());
    }
    let identity = if tool_execution.is_some() {
        turn_recovery_identity(&tool).map_err(|_| pending())?
    } else {
        String::new()
    };
    let args = session
        .recorded_tool_execution(start.id, &tool.owner, &tool.spec, &identity)
        .map_err(|_| pending())?;
    if strict && args.is_none() {
        return Err(pending());
    }
    Ok(args.map(|args| (tool, args)))
}

#[cfg(test)]
mod strict_argument_tests {
    use super::*;
    use serde_json::json;

    struct FixtureTool {
        pollable: bool,
        pending: bool,
        identity: Option<&'static str>,
        seen: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
        callbacks: Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>>,
    }

    #[async_trait::async_trait]
    impl crate::Tool for FixtureTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "read".into(),
                description: String::new(),
                parameters: json!({"type":"object", "properties":{"path":{"type":"string"},"limit":{"type":"integer"}}, "required":["path"]}),
            }
        }
        fn can_poll(&self) -> bool {
            self.pollable
        }
        fn recovery_identity(&self) -> Option<String> {
            self.identity.map(str::to_owned)
        }
        async fn prepare_arguments(
            &self,
            args: serde_json::Value,
        ) -> crate::ExtensionResult<serde_json::Value> {
            self.seen.lock().unwrap().push(args.clone());
            Ok(args)
        }
        async fn invoke(&self, args: serde_json::Value, _: ToolContext) -> Outcome {
            self.seen.lock().unwrap().push(args);
            if self.pending {
                Outcome::Pending {
                    reference: "job".into(),
                }
            } else {
                Outcome::Succeeded("ok".into())
            }
        }
        async fn poll(&self, reference: &str, _: ToolContext) -> Option<Outcome> {
            self.seen.lock().unwrap().push(json!({"polled":reference}));
            Some(Outcome::Succeeded("complete".into()))
        }
    }

    struct FixtureExtension(Arc<FixtureTool>);
    #[async_trait::async_trait]
    impl crate::Extension for FixtureExtension {
        fn id(&self) -> &str {
            "fixture"
        }
        fn tools(&self) -> Vec<Arc<dyn crate::Tool>> {
            vec![self.0.clone()]
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_tool_execution_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<()> {
            if let crate::LifecycleEvent::ToolExecutionStart { args, .. } = event {
                self.0
                    .callbacks
                    .lock()
                    .unwrap()
                    .push(("start".into(), args.clone()));
            }
            Ok(())
        }
        async fn tool_call(
            &self,
            _: &str,
            args: &mut serde_json::Value,
            _: &ToolContext,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<Decision> {
            self.0
                .callbacks
                .lock()
                .unwrap()
                .push(("authorize".into(), args.clone()));
            args["path"] = json!("authorized");
            Ok(Decision::Allow)
        }
        async fn before_tool(
            &self,
            _: &str,
            args: &serde_json::Value,
            _: &ToolContext,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<Decision> {
            self.0
                .callbacks
                .lock()
                .unwrap()
                .push(("poll_authorize".into(), args.clone()));
            Ok(Decision::Allow)
        }
    }
    struct FixtureModel;
    #[async_trait::async_trait]
    impl Model for FixtureModel {
        async fn stream(&self, _: ModelRequest) -> Result<crate::ModelStream> {
            unreachable!()
        }
    }

    fn recorded_turn(session: &mut Session, spec: ToolSpec, strict: bool) -> Invocation {
        let request = ModelRequest {
            model: "model".into(),
            messages: vec![],
            tools: vec![spec.clone()],
            max_output_tokens: 10,
        };
        let context = crate::ToolDispatchContext {
            request_id: Uuid::new_v4(),
            session_id: session.session_id(),
            external_id: session.external_session_id(),
            summary: false,
        };
        let declaration = crate::ToolDeclaration {
            spec: spec.clone(),
            constrained_sampling: Some(crate::ToolConstraint::JsonSchema {
                strict: crate::StrictToolPolicy::Prefer,
            }),
        };
        let sampling = if strict {
            crate::EffectiveToolSampling::StrictJsonSchema {
                original_schema: spec.parameters.clone(),
                normalization_version: crate::STRICT_TOOL_NORMALIZATION_VERSION,
            }
        } else {
            crate::EffectiveToolSampling::Unconstrained
        };
        let decision = crate::ToolDecisionSnapshot::capture(
            &context,
            &request,
            std::slice::from_ref(&declaration),
            &[crate::EffectiveToolDeclaration {
                spec: spec.clone(),
                sampling,
            }],
        )
        .unwrap();
        let event = Event::ModelRequested {
            model: request.model.clone(),
            messages: 0,
            tools: 1,
            max_output_tokens: 10,
            request_id: Some(context.request_id),
            context_head: Some(session.head()),
            context_sha256: Some(decision.request_sha256().into()),
            context_transformed: Some(false),
            context_configuration_sha256: None,
            tool_decision: None,
        };
        session
            .append_tool_decision_request(event, &decision, &request, &request, &[declaration])
            .unwrap();
        let invocation = Invocation {
            operation_id: Uuid::new_v4(),
            call: crate::ToolCall {
                id: "call".into(),
                name: spec.name.clone(),
                arguments: r#"{"path":"file","limit":null}"#.into(),
            },
        };
        session
            .append(Event::ModelFinished {
                request_id: context.request_id,
                metadata: ModelMetadata::default(),
                usage: UsageReport::Unconfirmed,
                failure: None,
                partial: String::new(),
                reply: Some(RecordedReply {
                    content: String::new(),
                    invocations: vec![invocation.clone()],
                    advertised_tools: vec![spec.name],
                    max_output_tokens: 10,
                }),
            })
            .unwrap();
        session
            .append(Event::Assistant {
                content: String::new(),
                invocations: vec![invocation.clone()],
                usage: None,
                request_id: Some(context.request_id),
            })
            .unwrap();
        invocation
    }

    async fn run_fixture(
        pollable: bool,
        identity: Option<&'static str>,
        strict: bool,
        pending: bool,
    ) -> (Result<()>, Session, Invocation, Arc<FixtureTool>) {
        run_fixture_with(pollable, identity, strict, pending, false, false).await
    }

    async fn run_fixture_with(
        pollable: bool,
        identity: Option<&'static str>,
        strict: bool,
        pending: bool,
        mismatched_spec: bool,
        forged: bool,
    ) -> (Result<()>, Session, Invocation, Arc<FixtureTool>) {
        let tool = Arc::new(FixtureTool {
            pollable,
            pending,
            identity,
            seen: Arc::new(std::sync::Mutex::new(Vec::new())),
            callbacks: Arc::default(),
        });
        let mut registry = Registry::default();
        registry
            .register(Arc::new(FixtureExtension(tool.clone())))
            .unwrap();
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        let mut spec = crate::Tool::spec(tool.as_ref());
        if mismatched_spec {
            spec.description = "different registered tool".into();
        }
        let mut invocation = recorded_turn(&mut session, spec, strict);
        if forged {
            invocation.call.arguments = "{}".into();
        }
        let agent = Agent {
            model: &FixtureModel,
            model_name: "model",
            registry: &registry,
            system_prompt: "",
            limits: Limits::default(),
        };
        let cancellation = CancellationToken::new();
        let advertised = HashSet::from(["read".to_owned()]);
        let scope = DispatchScope {
            cancellation: &cancellation,
            advertised: &advertised,
            depth: 0,
            call_count: Arc::new(AtomicUsize::new(0)),
        };
        let mut observer = |_: Notification<'_>| {};
        let result = agent
            .dispatch_turn::<String>(
                &mut session,
                std::slice::from_ref(&invocation),
                &scope,
                &mut ExtensionStates::new(),
                &mut observer,
                None,
            )
            .await;
        (result, session, invocation, tool)
    }

    #[tokio::test]
    async fn strict_execution_persists_exact_authorized_arguments() {
        let (result, session, invocation, tool) = run_fixture(false, None, true, false).await;
        result.unwrap();
        assert_eq!(
            *tool.seen.lock().unwrap(),
            vec![json!({"path":"file"}), json!({"path":"authorized"})]
        );
        assert!(invocation.call.arguments.contains("null"));
        assert_eq!(
            *tool.callbacks.lock().unwrap(),
            vec![
                ("start".into(), json!({"path":"file","limit":null})),
                ("authorize".into(), json!({"path":"file"}))
            ]
        );
        let start = session.records().iter().find(|record| matches!(&record.event, Event::ToolStarted { operation_id, .. } if *operation_id == invocation.operation_id)).unwrap();
        assert_eq!(
            session
                .recorded_tool_execution(
                    start.id,
                    "fixture",
                    &crate::Tool::spec(tool.as_ref()),
                    NON_POLLABLE_IDENTITY
                )
                .unwrap(),
            Some(json!({"path":"authorized"}))
        );
    }

    #[tokio::test]
    async fn pollable_without_identity_fails_before_preparation() {
        for identity in [None, Some("pi-rs:non-pollable:v1")] {
            let (result, session, _, tool) = run_fixture(true, identity, true, false).await;
            assert!(result.is_err());
            assert!(tool.seen.lock().unwrap().is_empty());
            assert!(tool.callbacks.lock().unwrap().is_empty());
            assert!(
                !session
                    .records()
                    .iter()
                    .any(|record| matches!(record.event, Event::ToolStarted { .. }))
            );
        }
    }

    #[tokio::test]
    async fn prefer_fallback_preserves_raw_arguments() {
        let (result, session, _, tool) = run_fixture(false, None, false, false).await;
        result.unwrap();
        assert_eq!(
            *tool.seen.lock().unwrap(),
            vec![json!({"path":"file","limit":null})]
        );
        assert!(
            !session
                .records()
                .iter()
                .any(|record| matches!(record.event, Event::ToolStarted { .. }))
        );
    }

    #[tokio::test]
    async fn prefer_fallback_does_not_require_recovery_identity() {
        let (result, _, _, tool) = run_fixture(true, None, false, false).await;
        result.unwrap();
        assert_eq!(
            *tool.seen.lock().unwrap(),
            vec![json!({"path":"file","limit":null})]
        );
        assert_eq!(
            tool.callbacks.lock().unwrap()[0],
            ("start".into(), json!({"path":"file","limit":null}))
        );
    }

    #[tokio::test]
    async fn pollable_tool_waits_for_snapshot_aware_recovery() {
        let (result, session, invocation, tool) =
            run_fixture(true, Some("backend:v1"), true, false).await;
        assert!(result.is_err());
        assert!(tool.seen.lock().unwrap().is_empty());
        assert!(tool.callbacks.lock().unwrap().is_empty());
        assert!(!session.records().iter().any(|record| matches!(&record.event, Event::ToolStarted { operation_id, .. } if *operation_id == invocation.operation_id)));
    }

    #[tokio::test]
    async fn unexpected_pending_requires_reconciliation() {
        let (result, session, invocation, _) = run_fixture(false, None, true, true).await;
        assert!(result.is_err());
        assert!(
            session
                .operations()
                .iter()
                .any(|(call, state)| call.operation_id == invocation.operation_id
                    && matches!(state, OperationState::Finished(Outcome::Unknown(_))))
        );
    }

    #[tokio::test]
    async fn invalid_strict_provenance_or_spec_rejects_before_callbacks() {
        for (mismatched_spec, forged) in [(true, false), (false, true)] {
            let (result, session, _, tool) =
                run_fixture_with(false, None, true, false, mismatched_spec, forged).await;
            assert!(result.is_err());
            assert!(tool.callbacks.lock().unwrap().is_empty());
            assert!(tool.seen.lock().unwrap().is_empty());
            assert!(
                !session
                    .records()
                    .iter()
                    .any(|record| matches!(record.event, Event::ToolStarted { .. }))
            );
        }
    }

    #[tokio::test]
    async fn mixed_batch_rejects_changed_fallback_spec_before_any_callback() {
        struct FallbackTool;
        #[async_trait::async_trait]
        impl crate::Tool for FallbackTool {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "fallback".into(),
                    description: "registered version".into(),
                    parameters: json!({"type":"object"}),
                }
            }
            async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
                panic!("fallback must not run")
            }
        }
        struct FallbackExtension;
        #[async_trait::async_trait]
        impl crate::Extension for FallbackExtension {
            fn id(&self) -> &str {
                "fallback-owner"
            }
            fn tools(&self) -> Vec<Arc<dyn crate::Tool>> {
                vec![Arc::new(FallbackTool)]
            }
        }

        let strict_tool = Arc::new(FixtureTool {
            pollable: false,
            pending: false,
            identity: None,
            seen: Arc::default(),
            callbacks: Arc::default(),
        });
        let strict_spec = crate::Tool::spec(strict_tool.as_ref());
        let mut fallback_spec = crate::Tool::spec(&FallbackTool);
        fallback_spec.description = "request version".into();
        let mut registry = Registry::default();
        registry
            .register(Arc::new(FixtureExtension(strict_tool.clone())))
            .unwrap();
        registry.register(Arc::new(FallbackExtension)).unwrap();
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        let request = ModelRequest {
            model: "model".into(),
            messages: vec![],
            tools: vec![strict_spec.clone(), fallback_spec.clone()],
            max_output_tokens: 10,
        };
        let context = crate::ToolDispatchContext {
            request_id: Uuid::new_v4(),
            session_id: session.session_id(),
            external_id: session.external_session_id(),
            summary: false,
        };
        let declaration = |spec: &ToolSpec| crate::ToolDeclaration {
            spec: spec.clone(),
            constrained_sampling: Some(crate::ToolConstraint::JsonSchema {
                strict: crate::StrictToolPolicy::Prefer,
            }),
        };
        let declarations = vec![declaration(&strict_spec), declaration(&fallback_spec)];
        let effective = vec![
            crate::EffectiveToolDeclaration {
                spec: strict_spec.clone(),
                sampling: crate::EffectiveToolSampling::StrictJsonSchema {
                    original_schema: strict_spec.parameters.clone(),
                    normalization_version: crate::STRICT_TOOL_NORMALIZATION_VERSION,
                },
            },
            crate::EffectiveToolDeclaration {
                spec: fallback_spec,
                sampling: crate::EffectiveToolSampling::Unconstrained,
            },
        ];
        let decision =
            crate::ToolDecisionSnapshot::capture(&context, &request, &declarations, &effective)
                .unwrap();
        let event = Event::ModelRequested {
            model: request.model.clone(),
            messages: 0,
            tools: 2,
            max_output_tokens: 10,
            request_id: Some(context.request_id),
            context_head: Some(session.head()),
            context_sha256: Some(decision.request_sha256().into()),
            context_transformed: Some(false),
            context_configuration_sha256: None,
            tool_decision: None,
        };
        session
            .append_tool_decision_request(event, &decision, &request, &request, &declarations)
            .unwrap();
        let calls: Vec<_> = ["read", "fallback"]
            .into_iter()
            .map(|name| Invocation {
                operation_id: Uuid::new_v4(),
                call: crate::ToolCall {
                    id: name.into(),
                    name: name.into(),
                    arguments: if name == "read" {
                        r#"{"path":"file","limit":null}"#
                    } else {
                        "{}"
                    }
                    .into(),
                },
            })
            .collect();
        session
            .append(Event::ModelFinished {
                request_id: context.request_id,
                metadata: ModelMetadata::default(),
                usage: UsageReport::Unconfirmed,
                failure: None,
                partial: String::new(),
                reply: Some(RecordedReply {
                    content: String::new(),
                    invocations: calls.clone(),
                    advertised_tools: vec!["read".into(), "fallback".into()],
                    max_output_tokens: 10,
                }),
            })
            .unwrap();
        session
            .append(Event::Assistant {
                content: String::new(),
                invocations: calls.clone(),
                usage: None,
                request_id: Some(context.request_id),
            })
            .unwrap();
        let agent = Agent {
            model: &FixtureModel,
            model_name: "model",
            registry: &registry,
            system_prompt: "",
            limits: Limits::default(),
        };
        let cancel = CancellationToken::new();
        let advertised = HashSet::from(["read".into(), "fallback".into()]);
        let scope = DispatchScope {
            cancellation: &cancel,
            advertised: &advertised,
            depth: 0,
            call_count: Arc::new(AtomicUsize::new(0)),
        };
        let mut observer = |_: Notification<'_>| {};
        let result = agent
            .dispatch_turn::<String>(
                &mut session,
                &calls,
                &scope,
                &mut ExtensionStates::new(),
                &mut observer,
                None,
            )
            .await;
        assert!(result.is_err());
        assert!(strict_tool.callbacks.lock().unwrap().is_empty());
        assert!(strict_tool.seen.lock().unwrap().is_empty());
        assert!(
            !session
                .records()
                .iter()
                .any(|record| matches!(record.event, Event::ToolStarted { .. }))
        );
    }

    #[tokio::test]
    async fn mixed_batch_skips_unknown_or_removed_tool_and_runs_valid_call() {
        for removed in [false, true] {
            let tool = Arc::new(FixtureTool {
                pollable: false,
                pending: false,
                identity: None,
                seen: Arc::default(),
                callbacks: Arc::default(),
            });
            let mut registry = Registry::default();
            registry
                .register(Arc::new(FixtureExtension(tool.clone())))
                .unwrap();
            let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
            let strict_spec = crate::Tool::spec(tool.as_ref());
            let missing_spec = ToolSpec {
                name: "missing".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            };
            let mut specs = vec![strict_spec.clone()];
            if removed {
                specs.push(missing_spec);
            }
            let request = ModelRequest {
                model: "model".into(),
                messages: vec![],
                tools: specs.clone(),
                max_output_tokens: 10,
            };
            let context = crate::ToolDispatchContext {
                request_id: Uuid::new_v4(),
                session_id: session.session_id(),
                external_id: session.external_session_id(),
                summary: false,
            };
            let declarations: Vec<_> = specs
                .iter()
                .map(|spec| crate::ToolDeclaration {
                    spec: spec.clone(),
                    constrained_sampling: Some(crate::ToolConstraint::JsonSchema {
                        strict: crate::StrictToolPolicy::Prefer,
                    }),
                })
                .collect();
            let effective: Vec<_> = specs
                .iter()
                .map(|spec| crate::EffectiveToolDeclaration {
                    spec: spec.clone(),
                    sampling: if spec.name == "read" {
                        crate::EffectiveToolSampling::StrictJsonSchema {
                            original_schema: spec.parameters.clone(),
                            normalization_version: crate::STRICT_TOOL_NORMALIZATION_VERSION,
                        }
                    } else {
                        crate::EffectiveToolSampling::Unconstrained
                    },
                })
                .collect();
            let decision =
                crate::ToolDecisionSnapshot::capture(&context, &request, &declarations, &effective)
                    .unwrap();
            session
                .append_tool_decision_request(
                    Event::ModelRequested {
                        model: request.model.clone(),
                        messages: 0,
                        tools: specs.len(),
                        max_output_tokens: 10,
                        request_id: Some(context.request_id),
                        context_head: Some(session.head()),
                        context_sha256: Some(decision.request_sha256().into()),
                        context_transformed: Some(false),
                        context_configuration_sha256: None,
                        tool_decision: None,
                    },
                    &decision,
                    &request,
                    &request,
                    &declarations,
                )
                .unwrap();
            let calls: Vec<_> = ["read", "missing"]
                .into_iter()
                .map(|name| Invocation {
                    operation_id: Uuid::new_v4(),
                    call: crate::ToolCall {
                        id: name.into(),
                        name: name.into(),
                        arguments: if name == "read" {
                            r#"{"path":"file","limit":null}"#
                        } else {
                            "{}"
                        }
                        .into(),
                    },
                })
                .collect();
            let advertised: HashSet<String> = specs.iter().map(|spec| spec.name.clone()).collect();
            session
                .append(Event::ModelFinished {
                    request_id: context.request_id,
                    metadata: ModelMetadata::default(),
                    usage: UsageReport::Unconfirmed,
                    failure: None,
                    partial: String::new(),
                    reply: Some(RecordedReply {
                        content: String::new(),
                        invocations: calls.clone(),
                        advertised_tools: specs.iter().map(|spec| spec.name.clone()).collect(),
                        max_output_tokens: 10,
                    }),
                })
                .unwrap();
            session
                .append(Event::Assistant {
                    content: String::new(),
                    invocations: calls.clone(),
                    usage: None,
                    request_id: Some(context.request_id),
                })
                .unwrap();
            let agent = Agent {
                model: &FixtureModel,
                model_name: "model",
                registry: &registry,
                system_prompt: "",
                limits: Limits::default(),
            };
            let cancel = CancellationToken::new();
            let scope = DispatchScope {
                cancellation: &cancel,
                advertised: &advertised,
                depth: 0,
                call_count: Arc::new(AtomicUsize::new(0)),
            };
            let mut observer = |_: Notification<'_>| {};
            agent
                .dispatch_turn::<String>(
                    &mut session,
                    &calls,
                    &scope,
                    &mut ExtensionStates::new(),
                    &mut observer,
                    None,
                )
                .await
                .unwrap();
            let operations = session.operations();
            assert!(operations.iter().any(|(call, state)| call == &calls[0]
                && matches!(state, OperationState::Finished(Outcome::Succeeded(_)))));
            assert!(operations.iter().any(|(call, state)| call == &calls[1]
                && matches!(state, OperationState::Finished(Outcome::Skipped(_)))));
            assert_eq!(
                *tool.seen.lock().unwrap(),
                vec![json!({"path":"file"}), json!({"path":"authorized"})]
            );
        }
    }

    #[tokio::test]
    async fn marked_poll_uses_authorized_arguments_and_rejects_backend_changes() {
        let tool = Arc::new(FixtureTool {
            pollable: true,
            pending: false,
            identity: Some("backend:v1"),
            seen: Arc::default(),
            callbacks: Arc::default(),
        });
        let mut registry = Registry::default();
        registry
            .register(Arc::new(FixtureExtension(tool.clone())))
            .unwrap();
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        let invocation = recorded_turn(&mut session, crate::Tool::spec(tool.as_ref()), true);
        let request_id = session
            .branch()
            .iter()
            .find_map(|record| match &record.event {
                Event::Assistant {
                    request_id: Some(id),
                    ..
                } => Some(*id),
                _ => None,
            })
            .unwrap();
        let crate::RecordedToolDecision::Current(decision) =
            session.active_tool_decision(request_id).unwrap()
        else {
            panic!("expected decision")
        };
        let spec = crate::Tool::spec(tool.as_ref());
        let snapshot = crate::ToolExecutionSnapshot::capture(
            request_id,
            &invocation,
            "fixture",
            &spec,
            "backend:v1",
            decision.binding_sha256(),
            json!({"path":"authorized"}),
        )
        .unwrap();
        session
            .append_tool_execution_started(
                request_id,
                invocation.operation_id,
                &snapshot,
                "fixture",
                &spec,
                "backend:v1",
            )
            .unwrap();
        session
            .append(Event::ToolFinished {
                operation_id: invocation.operation_id,
                outcome: Outcome::Pending {
                    reference: "job".into(),
                },
                states: ExtensionStates::new(),
            })
            .unwrap();
        let start_id = session
            .branch()
            .iter()
            .find_map(|record| match &record.event {
                Event::ToolStarted { operation_id, .. }
                    if *operation_id == invocation.operation_id =>
                {
                    Some(record.id)
                }
                _ => None,
            })
            .unwrap();
        let records = session.records().to_vec();
        let metadata: std::collections::BTreeMap<_, _> = records
            .iter()
            .filter_map(|record| {
                session
                    .record_metadata(record.id)
                    .cloned()
                    .map(|value| (record.id, value))
            })
            .collect();
        let mut stripped_records = records.clone();
        let Event::ToolStarted { tool_execution, .. } =
            &mut stripped_records[(start_id - 1) as usize].event
        else {
            unreachable!()
        };
        *tool_execution = None;
        let mut stripped_metadata = metadata.clone();
        stripped_metadata.remove(&start_id);
        let stripped =
            Session::from_records_with_metadata(stripped_records, stripped_metadata).unwrap();
        assert!(turn_poll_snapshot(&stripped, &registry, &invocation).is_err());
        let mut duplicate = Session::from_records_with_metadata(records, metadata).unwrap();
        duplicate
            .append(Event::ToolStarted {
                operation_id: invocation.operation_id,
                tool_execution: None,
            })
            .unwrap();
        assert!(turn_poll_snapshot(&duplicate, &registry, &invocation).is_err());
        let prepared = turn_poll_snapshot(&session, &registry, &invocation)
            .unwrap()
            .unwrap();
        assert_eq!(prepared.1, json!({"path":"authorized"}));
        let outcome = Agent::turn_poll_authorize_and_run(
            &registry,
            Duration::from_secs(1),
            &invocation,
            "job",
            prepared,
            &ExtensionStates::new(),
            &CancellationToken::new(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(outcome, Outcome::Succeeded("complete".into()));
        assert_eq!(
            *tool.callbacks.lock().unwrap(),
            vec![("poll_authorize".into(), json!({"path":"authorized"}))]
        );
        assert_eq!(*tool.seen.lock().unwrap(), vec![json!({"polled":"job"})]);
        assert!(invocation.call.arguments.contains("null"));
        for (pollable, identity) in [(true, Some("backend:v2")), (false, None), (true, None)] {
            let changed = Arc::new(FixtureTool {
                pollable,
                pending: false,
                identity,
                seen: Arc::default(),
                callbacks: Arc::default(),
            });
            let mut changed_registry = Registry::default();
            changed_registry
                .register(Arc::new(FixtureExtension(changed.clone())))
                .unwrap();
            assert!(turn_poll_snapshot(&session, &changed_registry, &invocation).is_err());
            assert!(changed.seen.lock().unwrap().is_empty());
            assert!(changed.callbacks.lock().unwrap().is_empty());
        }
        let mut forged = invocation.clone();
        forged.call.arguments = "{}".into();
        assert!(turn_poll_snapshot(&session, &registry, &forged).is_err());
    }

    #[test]
    fn unconstrained_fallback_keeps_legacy_poll_preparation() {
        let tool = Arc::new(FixtureTool {
            pollable: true,
            pending: false,
            identity: None,
            seen: Arc::default(),
            callbacks: Arc::default(),
        });
        let mut registry = Registry::default();
        registry
            .register(Arc::new(FixtureExtension(tool.clone())))
            .unwrap();
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        let invocation = recorded_turn(&mut session, crate::Tool::spec(tool.as_ref()), false);
        session
            .append(Event::ToolStarted {
                operation_id: invocation.operation_id,
                tool_execution: None,
            })
            .unwrap();
        session
            .append(Event::ToolFinished {
                operation_id: invocation.operation_id,
                outcome: Outcome::Pending {
                    reference: "job".into(),
                },
                states: ExtensionStates::new(),
            })
            .unwrap();
        assert!(
            turn_poll_snapshot(&session, &registry, &invocation)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn mixed_parallel_starts_keep_per_call_decisions() {
        struct FallbackTool;
        #[async_trait::async_trait]
        impl crate::Tool for FallbackTool {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "fallback".into(),
                    description: String::new(),
                    parameters: json!({"type":"object"}),
                }
            }
            fn can_poll(&self) -> bool {
                true
            }
            async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
                unreachable!()
            }
        }
        struct FallbackExtension;
        #[async_trait::async_trait]
        impl crate::Extension for FallbackExtension {
            fn id(&self) -> &str {
                "fallback-owner"
            }
            fn tools(&self) -> Vec<Arc<dyn crate::Tool>> {
                vec![Arc::new(FallbackTool)]
            }
        }
        let tool = Arc::new(FixtureTool {
            pollable: true,
            pending: false,
            identity: Some("backend:v1"),
            seen: Arc::default(),
            callbacks: Arc::default(),
        });
        let mut registry = Registry::default();
        registry
            .register(Arc::new(FixtureExtension(tool.clone())))
            .unwrap();
        registry.register(Arc::new(FallbackExtension)).unwrap();
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        let specs = vec![
            crate::Tool::spec(tool.as_ref()),
            crate::Tool::spec(&FallbackTool),
        ];
        let request = ModelRequest {
            model: "model".into(),
            messages: vec![],
            tools: specs.clone(),
            max_output_tokens: 10,
        };
        let context = crate::ToolDispatchContext {
            request_id: Uuid::new_v4(),
            session_id: session.session_id(),
            external_id: session.external_session_id(),
            summary: false,
        };
        let declarations: Vec<_> = specs
            .iter()
            .map(|spec| crate::ToolDeclaration {
                spec: spec.clone(),
                constrained_sampling: Some(crate::ToolConstraint::JsonSchema {
                    strict: crate::StrictToolPolicy::Prefer,
                }),
            })
            .collect();
        let effective: Vec<_> = specs
            .iter()
            .map(|spec| crate::EffectiveToolDeclaration {
                spec: spec.clone(),
                sampling: if spec.name == "read" {
                    crate::EffectiveToolSampling::StrictJsonSchema {
                        original_schema: spec.parameters.clone(),
                        normalization_version: crate::STRICT_TOOL_NORMALIZATION_VERSION,
                    }
                } else {
                    crate::EffectiveToolSampling::Unconstrained
                },
            })
            .collect();
        let decision =
            crate::ToolDecisionSnapshot::capture(&context, &request, &declarations, &effective)
                .unwrap();
        session
            .append_tool_decision_request(
                Event::ModelRequested {
                    model: request.model.clone(),
                    messages: 0,
                    tools: 2,
                    max_output_tokens: 10,
                    request_id: Some(context.request_id),
                    context_head: Some(session.head()),
                    context_sha256: Some(decision.request_sha256().into()),
                    context_transformed: Some(false),
                    context_configuration_sha256: None,
                    tool_decision: None,
                },
                &decision,
                &request,
                &request,
                &declarations,
            )
            .unwrap();
        let calls: Vec<_> = ["read", "fallback"]
            .into_iter()
            .map(|name| Invocation {
                operation_id: Uuid::new_v4(),
                call: crate::ToolCall {
                    id: name.into(),
                    name: name.into(),
                    arguments: if name == "read" {
                        r#"{"path":"file","limit":null}"#
                    } else {
                        "{}"
                    }
                    .into(),
                },
            })
            .collect();
        session
            .append(Event::ModelFinished {
                request_id: context.request_id,
                metadata: ModelMetadata::default(),
                usage: UsageReport::Unconfirmed,
                failure: None,
                partial: String::new(),
                reply: Some(RecordedReply {
                    content: String::new(),
                    invocations: calls.clone(),
                    advertised_tools: vec!["read".into(), "fallback".into()],
                    max_output_tokens: 10,
                }),
            })
            .unwrap();
        session
            .append(Event::Assistant {
                content: String::new(),
                invocations: calls.clone(),
                usage: None,
                request_id: Some(context.request_id),
            })
            .unwrap();
        let snapshot = crate::ToolExecutionSnapshot::capture(
            context.request_id,
            &calls[0],
            "fixture",
            &specs[0],
            "backend:v1",
            decision.binding_sha256(),
            json!({"path":"authorized"}),
        )
        .unwrap();
        session
            .append_tool_execution_started(
                context.request_id,
                calls[0].operation_id,
                &snapshot,
                "fixture",
                &specs[0],
                "backend:v1",
            )
            .unwrap();
        session
            .append(Event::ToolStarted {
                operation_id: calls[1].operation_id,
                tool_execution: None,
            })
            .unwrap();
        for call in &calls {
            session
                .append(Event::ToolFinished {
                    operation_id: call.operation_id,
                    outcome: Outcome::Pending {
                        reference: "job".into(),
                    },
                    states: ExtensionStates::new(),
                })
                .unwrap();
        }
        assert_eq!(
            turn_poll_snapshot(&session, &registry, &calls[0])
                .unwrap()
                .unwrap()
                .1,
            json!({"path":"authorized"})
        );
        assert!(
            turn_poll_snapshot(&session, &registry, &calls[1])
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_forged_invocation_and_superseded_turn_before_hooks() {
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        let tool = FixtureTool {
            pollable: false,
            pending: false,
            identity: None,
            seen: Arc::default(),
            callbacks: Arc::default(),
        };
        let invocation = recorded_turn(&mut session, crate::Tool::spec(&tool), true);
        assert!(
            turn_active_decision(&session, std::slice::from_ref(&invocation))
                .unwrap()
                .is_some()
        );
        let mut forged = invocation.clone();
        forged.call.arguments = "{}".into();
        assert!(turn_active_decision(&session, &[forged]).is_err());
        session
            .append(Event::Assistant {
                content: "later".into(),
                invocations: vec![],
                usage: None,
                request_id: None,
            })
            .unwrap();
        assert!(turn_active_decision(&session, &[invocation]).is_err());
    }

    #[test]
    fn uses_actual_sampling_and_rejects_changed_tool_specs() {
        let spec = ToolSpec {
            name: "read".into(),
            description: String::new(),
            parameters: json!({"type":"object", "properties":{"path":{"type":"string"},"limit":{"type":"integer"}}, "required":["path"]}),
        };
        let request = crate::ModelRequest {
            model: "model".into(),
            messages: vec![],
            tools: vec![spec.clone()],
            max_output_tokens: 10,
        };
        let context = crate::ToolDispatchContext {
            request_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            external_id: "test".into(),
            summary: false,
        };
        let declaration = crate::ToolDeclaration {
            spec: spec.clone(),
            constrained_sampling: Some(crate::ToolConstraint::JsonSchema {
                strict: crate::StrictToolPolicy::Prefer,
            }),
        };
        let invocation = Invocation {
            operation_id: Uuid::new_v4(),
            call: crate::ToolCall {
                id: "call".into(),
                name: "read".into(),
                arguments: r#"{"path":"file","limit":null}"#.into(),
            },
        };
        for (sampling, expected) in [
            (
                crate::EffectiveToolSampling::StrictJsonSchema {
                    original_schema: spec.parameters.clone(),
                    normalization_version: crate::STRICT_TOOL_NORMALIZATION_VERSION,
                },
                json!({"path":"file"}),
            ),
            (
                crate::EffectiveToolSampling::Unconstrained,
                json!({"path":"file","limit":null}),
            ),
        ] {
            let snapshot = crate::ToolDecisionSnapshot::capture(
                &context,
                &request,
                std::slice::from_ref(&declaration),
                &[crate::EffectiveToolDeclaration {
                    spec: spec.clone(),
                    sampling,
                }],
            )
            .unwrap();
            let decision = snapshot
                .validate(
                    &context,
                    &request,
                    &request,
                    std::slice::from_ref(&declaration),
                )
                .unwrap();
            assert_eq!(
                turn_model_arguments(&invocation, &spec, Some(&decision)).unwrap(),
                expected
            );
            let mut changed = spec.clone();
            changed.description = "changed".into();
            assert!(turn_model_arguments(&invocation, &changed, Some(&decision)).is_err());
        }
        assert_eq!(
            turn_model_arguments(&invocation, &spec, None).unwrap(),
            json!({"path":"file","limit":null})
        );
        assert!(invocation.call.arguments.contains("null"));
    }
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
        decision: Option<&crate::ToolDecisionSnapshot>,
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
        let validated = decision
            .map(|decision| {
                decision.validate_recorded(
                    decision.context(),
                    decision.request_sha256(),
                    decision.dispatch_sha256(),
                    decision.binding_sha256(),
                )
            })
            .transpose()
            .map_err(|_| Error::Session("invalid tool decision".into()))?;
        let args = match turn_model_arguments(invocation, &tool.spec, validated.as_ref()) {
            Ok(arguments) => arguments,
            Err(error) => {
                return Ok(TurnNormalized::Skipped(format!(
                    "Invalid arguments for tool \"{}\": {error}",
                    call.name
                )));
            }
        };
        if let Some(decision) = decision
            && turn_is_strict(decision, &tool.spec)?
        {
            turn_strict_tool_preflight(&tool)?;
        }
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
            Self::turn_normalize(registry, tool_timeout, invocation, advertised, cancel, None)
                .await?;
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
    ) -> bool {
        let mut replaced = false;
        if matches!(
            reply.outcome,
            Outcome::Skipped(_) | Outcome::Unknown(_) | Outcome::Pending { .. }
        ) {
            return false;
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
                    let scoped_host = host.as_ref().map(|host| host.for_extension(extension.id()).unwrap_or_else(|| host.clone()));
                    crate::track_tool_result_presence(extension.as_ref(), &invocation.call, &reply.outcome, &context, states.get(extension.id()), scoped_host).await
                }) => result,
            };
            let error = match result {
                Ok(Ok((Some(outcome), preservation)))
                    if !matches!(
                        outcome,
                        Outcome::Skipped(_) | Outcome::Unknown(_) | Outcome::Pending { .. }
                    ) =>
                {
                    if preservation != Some(true) {
                        replaced = true;
                    }
                    reply.outcome = outcome;
                    continue;
                }
                Ok(Ok((None, _))) => continue,
                Ok(Ok((Some(_), _))) => "invalid replacement outcome".to_owned(),
                Ok(Err(error)) => error,
                Err(_) => "tool result hook timed out".to_owned(),
            };
            observer(Notification::ExtensionLifecycleError {
                extension: extension.id(),
                event: "tool_result",
                message: &error,
            });
        }
        replaced
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
        bool,
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
        let turn_decision = if scope.depth == 0 {
            turn_active_decision(session, invocations)?.map(Arc::new)
        } else {
            None
        };
        if let Some(decision) = turn_decision.as_deref() {
            turn_preflight_decision_calls(self.registry, scope.advertised, invocations, decision)?;
        }
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
                    let decision = if calls[index].lane.is_none() {
                        turn_decision.clone()
                    } else {
                        None
                    };
                    futures.push(async move {
                        let mut errors = Vec::new();
                        if observe_tools {
                            // Upstream Pi emits raw model arguments here before
                            // prepareToolCall; strict normalization is for execution.
                            let event = crate::LifecycleEvent::ToolExecutionStart {
                                tool_call_id: invocation.call.id.clone(), tool_name: invocation.call.name.clone(),
                                args: crate::parse_tool_arguments(&invocation.call.arguments).unwrap_or(serde_json::Value::Null),
                                parent_tool_call_id: parent,
                            };
                            let mut collect = |event: Notification<'_>| if let Notification::ExtensionLifecycleError { extension, event, message } = event { errors.push((extension.to_owned(), event, message.to_owned())); };
                            tokio::select! { biased; _ = cancel.cancelled() => {}, _ = crate::lifecycle::notify_with_host(&registry, event, &state_snapshot, model_timeout, &mut collect, hook_host) => {} }
                        }
                        let result = Self::turn_normalize(&registry, tool_timeout, &invocation, &advertised, &cancel, decision.as_deref()).await;
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
                    let replaced = Self::turn_transform(
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
                    TurnEvent::Transformed(index, reply, duration, errors, replaced)
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
                                if let TurnPreparation::Ready { tool, args, .. } = &prepared {
                                    let strict_decision = if calls[index].lane.is_none() {
                                        match turn_decision.as_ref() {
                                            Some(decision) if turn_is_strict(decision, &tool.spec)? => Some(decision),
                                            _ => None,
                                        }
                                    } else { None };
                                    if let Some(decision) = strict_decision {
                                        let identity = turn_recovery_identity(tool)?;
                                        let invocation = &calls[index].invocation;
                                        let request_id = decision.context().request_id;
                                        let snapshot = crate::ToolExecutionSnapshot::capture(request_id, invocation, &tool.owner, &tool.spec, &identity, decision.binding_sha256(), args.clone())?;
                                        let record = session.append_tool_execution_started(request_id, invocation.operation_id, &snapshot, &tool.owner, &tool.spec, &identity)?;
                                        observer(Notification::Committed(record));
                                    } else {
                                        commit(session, Event::ToolStarted { operation_id: calls[index].invocation.operation_id, tool_execution: None }, observer)?;
                                    }
                                }
                                let cancellation = match &prepared { TurnPreparation::Ready { context, .. } => context.cancellation.clone(), TurnPreparation::Skipped(_) => calls[index].cancellation.child_token() };
                                let (updates, mut progress_input) = mpsc::channel::<TurnProgress>(32);
                                let (finish_progress, mut finish_signal) = oneshot::channel();
                                let progress_output = progress_output.clone();
                                let progress_registry = self.registry.clone();
                                let progress_cancel = calls[index].cancellation.clone();
                                let progress_states = states.clone();
                                let progress_timeout = self.limits.model_timeout;
                                let hook_host = lifecycle_host.clone();
                                let progress_worker = async move {
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
                                };
                                #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
                                let progress_worker = tokio::spawn(progress_worker);
                                #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
                                let progress_worker = async move { progress_worker.await.unwrap_or_else(|error| vec![("tool-progress".into(), "tool_execution_update", error.to_string())]) };
                                let host = Arc::new(NestedHost { ui: self.registry.ui.clone(), requests: requests.clone(), specs: specs.clone(), parent_operation: calls[index].invocation.operation_id, cancellation, depth: calls[index].depth, call_count: calls[index].call_count.clone(), progress: Some(ProgressReporter { sender: ProgressSink::Turn(updates.clone()), tool_call_id: calls[index].invocation.call.id.clone(), tool_name: calls[index].invocation.call.name.clone(), args: crate::parse_tool_arguments(&calls[index].invocation.call.arguments).unwrap_or(serde_json::Value::Null), parent_tool_call_id: calls[index].parent.clone() }) });
                                let cancel = calls[index].cancellation.clone(); let programmatic = calls[index].depth > 0;
                                let omit = self.registry.omit_default_tool_timeout; let timeout = self.limits.tool_timeout;
                                calls[index].phase = TurnPhase::Running;
                                let flush = updates.clone();
                                futures.push(async move { let invoke = async move { let start = crate::runtime::Monotonic::now(); let reply = Self::turn_invoke(prepared, &cancel, host, programmatic, omit, timeout).await; let duration = ran.then(|| start.elapsed().as_millis().min(u64::MAX as u128) as u64); let (send, receive) = oneshot::channel(); if flush.send(TurnProgress::Flush(send)).await.is_ok() { let _ = receive.await; } let _ = finish_progress.send(()); (reply, duration) }; let ((reply, duration), errors) = futures_util::future::join(invoke, progress_worker).await; TurnEvent::Ran(index, reply, duration, errors) }.boxed());
                    }
                    TurnEvent::Ran(index, mut reply, duration, errors) => {
                        for (extension,event,message) in &errors { observer(Notification::ExtensionLifecycleError { extension, event, message }); }
                        // A supposedly non-pollable strict tool may still return
                        // Pending. Do not create a pending record that the old
                        // recovery path might later poll using raw arguments.
                        if let Outcome::Pending { reference } = &reply.outcome
                            && calls[index].lane.is_none()
                            && let Some(decision) = turn_decision.as_ref()
                            && let Some(tool) = self.registry.tools.get(&calls[index].invocation.call.name)
                            && !tool.tool.can_poll()
                            && turn_is_strict(decision, &tool.spec)?
                        {
                            reply.outcome = Outcome::Unknown(format!("non-pollable strict tool returned pending ({reference}); reconciliation required"));
                            reply.state = None;
                        }
                        calls[index].phase = TurnPhase::Done(Box::new(reply), duration);
                    }
                    TurnEvent::Transformed(index, reply, duration, errors, replaced) => {
                        for (extension,event,message) in &errors { observer(Notification::ExtensionLifecycleError { extension, event, message }); }
                        let finished = self.finish_tool(session, &calls[index].invocation, reply, states, observer, Some(&snapshots), !replaced);
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
                            let recorded = turn_poll_snapshot(session, self.registry, &invocation);
                            calls[index].phase = TurnPhase::Polling;
                            calls[index].polled = true;
                            let future = async move {
                                let mut errors = Vec::new();
                                if observe_tools {
                                    let event = crate::LifecycleEvent::ToolExecutionEnd { tool_call_id: invocation.call.id.clone(), tool_name: invocation.call.name.clone(), result: pending_outcome, is_error: false, duration_ms: duration, parent_tool_call_id: parent };
                                    let mut collect = |event: Notification<'_>| if let Notification::ExtensionLifecycleError { extension, event, message } = event { errors.push((extension.to_owned(), event, message.to_owned())); };
                                    tokio::select! { biased; _ = cancel.cancelled() => {}, _ = crate::lifecycle::notify_with_host(&registry, event, &snapshot, model_timeout, &mut collect, hook_host) => {} }
                                }
                                let result = match recorded {
                                    Ok(Some(prepared)) => Ok(prepared),
                                    Ok(None) => Self::turn_poll_normalize(&registry, timeout, &invocation, &cancel).await,
                                    Err(error) => Err(error),
                                };
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
