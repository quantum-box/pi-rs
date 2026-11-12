use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, SystemTime};

use pi_rs_aip_host::*;
use pi_rs_core::*;
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Default)]
struct Platform {
    authorization: AtomicUsize,
    exhausted: AtomicBool,
    cancelled: AtomicBool,
    confirmed: AtomicBool,
    effects: Mutex<BTreeSet<Uuid>>,
    usage: Mutex<BTreeMap<Uuid, UsageReport>>,
    reservations: Mutex<BTreeSet<Uuid>>,
}

fn delegation() -> Delegation {
    Delegation {
        spec: ToolSpec {
            name: "fixture_submit".into(),
            description: "Write to an isolated mock".into(),
            parameters: json!({"type":"object","properties":{}}),
        },
        destination: Destination::Business {
            operation: "fixture.submit".into(),
        },
        read_only: false,
        resource: "document-1".into(),
        version: "v1".into(),
    }
}

#[async_trait::async_trait]
impl DelegatedPort for Platform {
    async fn discover(&self, _: &RunScope, names: &BTreeSet<String>) -> Result<Vec<Delegation>> {
        let write = names.contains("fixture_submit").then(delegation);
        let read = names.contains("fixture_read").then(|| {
            let mut read = delegation();
            read.spec.name = "fixture_read".into();
            read.read_only = true;
            read
        });
        Ok(write.into_iter().chain(read).collect())
    }

    async fn authorize(
        &self,
        scope: &RunScope,
        delegation: &Delegation,
        _: &Value,
        operation_id: Uuid,
    ) -> Result<Option<Grant>> {
        let mut grant = Grant {
            scope: scope.clone(),
            operation_id,
            delegation: delegation.clone(),
            expires_at: SystemTime::now() + Duration::from_secs(60),
            capability: "private-fixture-capability".into(),
        };
        match self.authorization.load(Ordering::SeqCst) {
            1 => return Ok(None),
            2 => grant.expires_at = SystemTime::UNIX_EPOCH,
            3 => grant.delegation.version = "v2".into(),
            _ => {}
        }
        Ok(Some(grant))
    }

    async fn execute(&self, grant: Grant, _: Value, _: CancellationToken) -> Outcome {
        assert_eq!(grant.capability, "private-fixture-capability");
        self.effects.lock().unwrap().insert(grant.operation_id);
        Outcome::Pending {
            reference: "remote-1".into(),
        }
    }

    async fn cancel(&self, _: &RunScope, _: Uuid, _: Option<&str>) -> Result<()> {
        self.cancelled.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn inspect(
        &self,
        _: &RunScope,
        operation_id: Uuid,
        reference: Option<&str>,
    ) -> Result<Option<Outcome>> {
        assert_eq!(reference, Some("remote-1"));
        assert!(self.effects.lock().unwrap().contains(&operation_id));
        Ok(self
            .confirmed
            .load(Ordering::SeqCst)
            .then(|| Outcome::Succeeded("confirmed".into())))
    }
}

#[async_trait::async_trait]
impl BudgetPort for Platform {
    async fn reserve(&self, _: &RunScope, request_id: Uuid, _: &ModelRequest) -> Result<()> {
        if self.exhausted.load(Ordering::SeqCst) {
            return Err(Error::Budget);
        }
        self.reservations.lock().unwrap().insert(request_id);
        Ok(())
    }

    async fn settle(&self, _: &RunScope, request_id: Uuid, report: &UsageReport) -> Result<()> {
        if self.reservations.lock().unwrap().contains(&request_id) {
            let mut usage = self.usage.lock().unwrap();
            if let Some(previous) = usage.insert(request_id, report.clone()) {
                assert_eq!(&previous, report);
            }
        }
        Ok(())
    }
}

struct ModelFixture(Mutex<VecDeque<Vec<ToolCall>>>);

#[async_trait::async_trait]
impl Model for ModelFixture {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        let calls = self
            .0
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected request");
        let mut events = Vec::new();
        if calls.is_empty() {
            events.push(Ok(ModelEvent::Text("done".into())));
        }
        events.push(Ok(ModelEvent::Complete {
            calls,
            usage: Some(Usage {
                input_tokens: 31,
                output_tokens: 4,
            }),
        }));
        Ok(Box::pin(futures_util::stream::iter(events)))
    }
}

fn scope() -> RunScope {
    RunScope {
        tenant: "tenant-a".into(),
        actor: "actor-a".into(),
        workspace: "workspace-a".into(),
        journal_run_id: Uuid::new_v4(),
        aip_run: "run-child".into(),
        parent_run: Some("run-parent".into()),
        audit_id: "audit-1".into(),
    }
}

fn model() -> ModelFixture {
    ModelFixture(Mutex::new(
        vec![
            vec![ToolCall {
                id: "provider-call-1".into(),
                name: "fixture_submit".into(),
                arguments: "{}".into(),
            }],
            vec![],
        ]
        .into(),
    ))
}

fn ignore(_: Notification<'_>) {}

#[tokio::test]
async fn pending_operation_survives_restart_and_is_neither_replayed_nor_double_settled() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("session");
    let mut session = Session::open(&path).unwrap();
    let scope = scope();
    let platform = Arc::new(Platform::default());
    let registry = HostRegistry::discover(
        &scope,
        platform.clone(),
        ["fixture_submit".into()].into(),
        vec![],
    )
    .await
    .unwrap();
    let model = model();
    let host = HostTurn {
        scope: &scope,
        budget: platform.as_ref(),
        model: &model,
        model_name: "fixture",
        registry: &registry,
        system_prompt: "fixture",
        limits: Limits::default(),
    };
    let (_, mut inputs) = tokio::sync::mpsc::channel(1);
    let cancel = CancellationToken::new();
    let error = host
        .run(
            &mut session,
            Some("dispatch".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap_err();
    let Error::PendingOperation(operation_id) = error else {
        panic!("{error}")
    };
    let session_id = session.session_id();
    drop(session);

    let mut session = Session::open(&path).unwrap();
    assert_eq!(session.session_id(), session_id);
    assert!(
        !registry
            .reconcile_operation(&mut session, operation_id, true)
            .await
            .unwrap()
    );
    assert!(platform.cancelled.load(Ordering::SeqCst));
    assert!(matches!(
        host.run(&mut session, None, &mut inputs, &cancel, &mut ignore)
            .await,
        Err(Error::PendingOperation(_))
    ));
    platform.confirmed.store(true, Ordering::SeqCst);
    assert!(
        registry
            .reconcile_operation(&mut session, operation_id, false)
            .await
            .unwrap()
    );
    assert_eq!(
        host.run(&mut session, None, &mut inputs, &cancel, &mut ignore)
            .await
            .unwrap(),
        "done"
    );
    assert!(matches!(
        host.run(
            &mut session,
            Some("dispatch".into()),
            &mut inputs,
            &cancel,
            &mut ignore
        )
        .await,
        Err(Error::DuplicateRun(_))
    ));
    assert_eq!(platform.effects.lock().unwrap().len(), 1);
    assert_eq!(platform.usage.lock().unwrap().len(), 2);
    let other_tenant = RunScope {
        tenant: "tenant-b".into(),
        ..scope.clone()
    };
    assert!(other_tenant.bind(&mut session).is_err());
    let another_run_identity = RunScope {
        audit_id: "wrong-audit".into(),
        ..scope
    };
    assert!(another_run_identity.bind(&mut session).is_err());
    drop(session);
    let contents = std::fs::read_to_string(path).unwrap();
    assert!(!contents.contains("private-fixture-capability"));
    assert!(contents.contains("provider-call-1"));
}

#[tokio::test]
async fn revocation_stale_approval_version_change_and_budget_stop_before_external_effect() {
    for condition in 1..=4 {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let scope = scope();
        let platform = Arc::new(Platform::default());
        let registry = HostRegistry::discover(
            &scope,
            platform.clone(),
            ["fixture_submit".into()].into(),
            vec![],
        )
        .await
        .unwrap();
        platform.authorization.store(condition, Ordering::SeqCst);
        platform.exhausted.store(condition == 4, Ordering::SeqCst);
        let model = model();
        let host = HostTurn {
            scope: &scope,
            budget: platform.as_ref(),
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "fixture",
            limits: Limits::default(),
        };
        let (_, mut inputs) = tokio::sync::mpsc::channel(1);
        let result = host
            .run(
                &mut session,
                Some("dispatch".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut ignore,
            )
            .await;
        if condition == 4 {
            assert!(matches!(result, Err(Error::Budget)));
            assert!(session.operations().is_empty());
        } else {
            assert_eq!(result.unwrap(), "done");
            assert!(matches!(
                session.operations()[0].1,
                OperationState::Finished(Outcome::Skipped(_))
            ));
        }
        assert!(platform.effects.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn hosted_review_keeps_parent_child_identity_and_recovers_a_cancelled_child() {
    let platform = Arc::new(Platform::default());
    let parent_scope = RunScope {
        parent_run: None,
        ..scope()
    };
    let child_scope = RunScope {
        journal_run_id: Uuid::new_v4(),
        aip_run: "independent-review".into(),
        parent_run: Some(parent_scope.aip_run.clone()),
        audit_id: "child-audit".into(),
        ..parent_scope.clone()
    };
    let parent_profile = HostRegistry::discover(
        &parent_scope,
        platform.clone(),
        BTreeSet::new(),
        vec![Arc::new(pi_rs_review::ReviewContext)],
    )
    .await
    .unwrap();
    let child_profile =
        HostRegistry::discover(&child_scope, platform.clone(), BTreeSet::new(), vec![])
            .await
            .unwrap();
    let parent_model = ModelFixture(Mutex::new(vec![vec![]].into()));
    let child_model = ModelFixture(Mutex::new(vec![vec![]].into()));
    let parent_turn = HostTurn {
        scope: &parent_scope,
        budget: platform.as_ref(),
        model: &parent_model,
        model_name: "fixture",
        registry: &parent_profile,
        system_prompt: "parent",
        limits: Limits::default(),
    };
    let child_turn = HostTurn {
        scope: &child_scope,
        budget: platform.as_ref(),
        model: &child_model,
        model_name: "fixture",
        registry: &child_profile,
        system_prompt: "review",
        limits: Limits::default(),
    };
    let dir = tempfile::tempdir().unwrap();
    let parent_path = dir.path().join("parent.jsonl");
    let child_path = dir.path().join("child.jsonl");
    let mut parent = Session::open(&parent_path).unwrap();
    let mut child = Session::open(&child_path).unwrap();
    let extra_profile = HostRegistry::discover(
        &child_scope,
        platform.clone(),
        ["fixture_read".into()].into(),
        vec![],
    )
    .await
    .unwrap();
    let extra_child = HostTurn {
        registry: &extra_profile,
        limits: child_turn.limits.clone(),
        ..child_turn
    };
    let review_with_extra_tool = ManagedReview {
        parent: &parent_turn,
        child: &extra_child,
        from_record: parent.head(),
        task: "review with unauthorized tool",
        resume: false,
    };
    assert!(matches!(
        review_with_extra_tool
            .run(
                &mut parent,
                &mut child,
                &CancellationToken::new(),
                &mut ignore
            )
            .await,
        Err(Error::Config(_))
    ));
    let (_, mut queue) = tokio::sync::mpsc::channel(1);
    let cancel = CancellationToken::new();
    parent_turn
        .run(
            &mut parent,
            Some("review this decision".into()),
            &mut queue,
            &cancel,
            &mut ignore,
        )
        .await
        .unwrap();
    let from_record = parent.head();
    let stopped = CancellationToken::new();
    let child_run_id = child_scope.journal_run_id;
    let trigger = stopped.clone();
    let mut cancel_during_request = |event: Notification<'_>| {
        if matches!(event, Notification::Committed(Record { run_id: Some(id), event: Event::ModelRequested { .. }, .. }) if *id == child_run_id)
        {
            trigger.cancel();
        }
    };
    let mut review = ManagedReview {
        parent: &parent_turn,
        child: &child_turn,
        from_record,
        task: "check the original evidence",
        resume: false,
    };
    assert!(matches!(
        review
            .run(
                &mut parent,
                &mut child,
                &stopped,
                &mut cancel_during_request
            )
            .await,
        Err(Error::Cancelled)
    ));
    assert!(matches!(
        pi_rs_review::ReviewContext::find(&parent, child.session_id())
            .unwrap()
            .unwrap()
            .status,
        pi_rs_review::ReviewStatus::Stopped
    ));
    assert!(Session::open(&parent_path).is_err());
    assert!(Session::open(&child_path).is_err());
    let child_id = child.session_id();
    drop((parent, child));
    let mut parent = Session::open(&parent_path).unwrap();
    let mut child = Session::open(&child_path).unwrap();
    assert_eq!(child.session_id(), child_id);
    review.resume = true;
    let changed_run = RunScope {
        journal_run_id: Uuid::new_v4(),
        aip_run: "different-review-run".into(),
        audit_id: "different-audit".into(),
        ..child_scope.clone()
    };
    let changed_registry =
        HostRegistry::discover(&changed_run, platform.clone(), BTreeSet::new(), vec![])
            .await
            .unwrap();
    let changed_child = HostTurn {
        scope: &changed_run,
        registry: &changed_registry,
        limits: child_turn.limits.clone(),
        ..child_turn
    };
    let changed_review = ManagedReview {
        child: &changed_child,
        ..review
    };
    let before = (parent.head(), child.head());
    assert!(matches!(
        changed_review
            .run(&mut parent, &mut child, &cancel, &mut ignore)
            .await,
        Err(Error::Session(_))
    ));
    assert_eq!((parent.head(), child.head()), before);
    assert_eq!(
        review
            .run(&mut parent, &mut child, &cancel, &mut ignore)
            .await
            .unwrap(),
        "done"
    );
    assert!(
        child
            .records()
            .iter()
            .any(|record| record.run_id == Some(child_scope.journal_run_id))
    );
    assert_eq!(platform.usage.lock().unwrap().len(), 2);
    assert!(matches!(
        pi_rs_review::ReviewContext::find(&parent, child.session_id())
            .unwrap()
            .unwrap()
            .status,
        pi_rs_review::ReviewStatus::Done { .. }
    ));
    let completed = (parent.head(), child.head());
    assert_eq!(
        review
            .run(&mut parent, &mut child, &cancel, &mut ignore)
            .await
            .unwrap(),
        "done"
    );
    assert_eq!((parent.head(), child.head()), completed);
    assert_eq!(platform.usage.lock().unwrap().len(), 2);
}

struct References(AtomicBool);
#[async_trait::async_trait]
impl ReferencePort for References {
    async fn load(
        &self,
        scope: &RunScope,
        key: &ReferenceKey,
    ) -> Result<Option<AuthorizedReference>> {
        Ok(self.0.load(Ordering::SeqCst).then(|| AuthorizedReference {
            key: key.clone(),
            tenant: scope.tenant.clone(),
            workspace: scope.workspace.clone(),
            fresh_until: SystemTime::now() + Duration::from_secs(30),
            text: "authorized workspace decision".into(),
        }))
    }
}

#[tokio::test]
async fn revoked_reference_stops_dispatch_after_a_branch_reopens() {
    let platform = Arc::new(Platform::default());
    let scope = scope();
    let source = Arc::new(References(AtomicBool::new(true)));
    let registry = HostRegistry::discover(
        &scope,
        platform.clone(),
        BTreeSet::new(),
        vec![Arc::new(ReferenceMemory {
            scope: scope.clone(),
            source: source.clone(),
            keep_turns: 1,
        })],
    )
    .await
    .unwrap();
    let model = ModelFixture(Mutex::new(vec![vec![]].into()));
    let host = HostTurn {
        scope: &scope,
        budget: platform.as_ref(),
        model: &model,
        model_name: "fixture",
        registry: &registry,
        system_prompt: "fixture",
        limits: Limits::default(),
    };
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    host.extension_command(
        &mut session,
        "aip-reference-memory",
        "select",
        r#"[{"id":"decision:fixture","version":"1"}]"#,
        &mut ignore,
    )
    .unwrap();
    let (_, mut queue) = tokio::sync::mpsc::channel::<QueuedInput>(2);
    let stop = CancellationToken::new();
    host.run(
        &mut session,
        Some("check the decision".into()),
        &mut queue,
        &stop,
        &mut |event: Notification<'_>| {
            if let Notification::Request(request) = event {
                assert!(
                    serde_json::to_string(request)
                        .unwrap()
                        .contains("decision:fixture version 1")
                );
            }
        },
    )
    .await
    .unwrap();
    source.0.store(false, Ordering::SeqCst);
    let anchor = session.head();
    session.checkout(anchor).unwrap();
    let (sender, mut queue) = tokio::sync::mpsc::channel(2);
    sender
        .try_send(QueuedInput {
            id: Uuid::new_v4(),
            kind: InputKind::Steer,
            content: "check it again".into(),
        })
        .unwrap();
    assert!(matches!(
        host.run(&mut session, None, &mut queue, &stop, &mut ignore)
            .await,
        Err(Error::Extension { .. })
    ));
    assert_eq!(platform.reservations.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn hosted_recovery_reuses_reply_and_defers_tools_until_usage_is_recorded() {
    assert_recovered_reply(false).await;
    assert_recovered_reply(true).await;
}

async fn assert_recovered_reply(with_tool: bool) {
    struct MeteredModel {
        sends: AtomicUsize,
        receipts: Mutex<Vec<(Uuid, UsageReport, ModelMetadata)>>,
        with_tool: bool,
    }

    #[async_trait::async_trait]
    impl Model for MeteredModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            assert_eq!(self.sends.fetch_add(1, Ordering::SeqCst), 0);
            let events = [
                Ok(ModelEvent::Text("measured".into())),
                Ok(ModelEvent::Metadata(ModelMetadata {
                    cached_input_tokens: Some(7),
                    cache_creation_input_tokens: Some(4),
                    resolved_model: Some(ResolvedModelIdentity {
                        provider: "fixture".into(),
                        model: "resolved".into(),
                    }),
                    ..Default::default()
                })),
                Ok(ModelEvent::Complete {
                    calls: self
                        .with_tool
                        .then(|| ToolCall {
                            id: "recorded-call".into(),
                            name: "fixture_submit".into(),
                            arguments: "{}".into(),
                        })
                        .into_iter()
                        .collect(),
                    usage: Some(Usage {
                        input_tokens: 21,
                        output_tokens: 8,
                    }),
                }),
            ];
            Ok(Box::pin(futures_util::stream::iter(events)))
        }

        async fn record_usage_with_metadata(
            &self,
            request: Uuid,
            usage: &UsageReport,
            metadata: &ModelMetadata,
        ) -> Result<()> {
            let mut receipts = self.receipts.lock().unwrap();
            receipts.push((request, usage.clone(), metadata.clone()));
            if receipts.len() == 1 {
                return Err(Error::RetryableModel(
                    "receipt temporarily unavailable".into(),
                ));
            }
            Ok(())
        }
    }

    let platform = Arc::new(Platform::default());
    let scope = scope();
    let registry = HostRegistry::discover(
        &scope,
        platform.clone(),
        BTreeSet::from(["fixture_submit".into()]),
        vec![],
    )
    .await
    .unwrap();
    let model = MeteredModel {
        sends: AtomicUsize::new(0),
        receipts: Mutex::new(Vec::new()),
        with_tool,
    };
    let host = HostTurn {
        scope: &scope,
        budget: platform.as_ref(),
        model: &model,
        model_name: "fixture",
        registry: &registry,
        system_prompt: "fixture",
        limits: Limits::default(),
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("usage.jsonl");
    let mut session = Session::open(&path).unwrap();
    let (_, mut inputs) = tokio::sync::mpsc::channel(1);
    assert!(matches!(
        host.run(
            &mut session,
            Some("measure".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore
        )
        .await,
        Err(Error::RetryableModel(_))
    ));
    assert_eq!(model.receipts.lock().unwrap().len(), 1);
    drop(session);

    let mut session = Session::open(&path).unwrap();
    assert!(platform.effects.lock().unwrap().is_empty());
    let result = host
        .run(
            &mut session,
            None,
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await;
    if with_tool {
        let Err(Error::PendingOperation(operation)) = result else {
            panic!("expected exactly one recorded tool to be dispatched");
        };
        assert_eq!(
            *platform.effects.lock().unwrap(),
            BTreeSet::from([operation])
        );
        assert!(session.records().iter().any(|record| matches!(
            &record.event,
            Event::ModelFinished { reply: Some(reply), .. }
                if reply.invocations[0].operation_id == operation
        )));
    } else {
        assert_eq!(result.unwrap(), "measured");
        assert!(platform.effects.lock().unwrap().is_empty());
    }
    assert_eq!(model.sends.load(Ordering::SeqCst), 1);
    let receipts = model.receipts.lock().unwrap();
    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0], receipts[1]);
    assert_eq!(receipts[1].2.cached_input_tokens, Some(7));
    assert_eq!(receipts[1].2.cache_creation_input_tokens, Some(4));
    assert_eq!(platform.usage.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn prepared_model_keeps_aip_budget_and_usage_settlement() {
    struct Resolving {
        sends: Arc<AtomicUsize>,
        receipts: Arc<AtomicUsize>,
        large: bool,
    }
    struct Prepared {
        sends: Arc<AtomicUsize>,
        receipts: Arc<AtomicUsize>,
        large: bool,
    }
    #[async_trait::async_trait]
    impl Model for Resolving {
        async fn prepare_context<'a>(
            &'a self,
            _: &ModelRequest,
        ) -> Result<Option<Arc<dyn Model + 'a>>> {
            Ok(Some(Arc::new(Prepared {
                sends: self.sends.clone(),
                receipts: self.receipts.clone(),
                large: self.large,
            })))
        }
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            panic!("unprepared model was dispatched")
        }
    }
    #[async_trait::async_trait]
    impl Model for Prepared {
        fn context_identity(&self) -> Option<String> {
            Some("prepared-route".into())
        }
        fn estimate_request_tokens(&self, _: &ModelRequest) -> u64 {
            if self.large { 2000 } else { 10 }
        }
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            self.sends.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(futures_util::stream::iter(vec![
                Ok(ModelEvent::Text("done".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: Some(Usage {
                        input_tokens: 20,
                        output_tokens: 8,
                    }),
                }),
            ])))
        }
        async fn record_usage_with_metadata(
            &self,
            _: Uuid,
            _: &UsageReport,
            _: &ModelMetadata,
        ) -> Result<()> {
            self.receipts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    for large in [false, true] {
        let platform = Arc::new(Platform::default());
        let scope = scope();
        let registry = HostRegistry::discover(&scope, platform.clone(), BTreeSet::new(), vec![])
            .await
            .unwrap();
        let sends = Arc::new(AtomicUsize::new(0));
        let receipts = Arc::new(AtomicUsize::new(0));
        let model = Resolving {
            sends: sends.clone(),
            receipts: receipts.clone(),
            large,
        };
        let host = HostTurn {
            scope: &scope,
            budget: platform.as_ref(),
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "fixture",
            limits: Limits {
                context_window: Some(1000),
                output_tokens_per_request: 64,
                ..Limits::default()
            },
        };
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session.auto_compaction_control().set_enabled(false);
        let (_, mut inputs) = tokio::sync::mpsc::channel(1);
        let result = host
            .run(
                &mut session,
                Some("go".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut ignore,
            )
            .await;
        assert_eq!(result.is_ok(), !large);
        assert_eq!(sends.load(Ordering::SeqCst), usize::from(!large));
        assert_eq!(receipts.load(Ordering::SeqCst), usize::from(!large));
        assert_eq!(
            platform.reservations.lock().unwrap().len(),
            usize::from(!large)
        );
        assert_eq!(platform.usage.lock().unwrap().len(), usize::from(!large));
    }
}
