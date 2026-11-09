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
    async fn discover(&self, _: &RunScope, _: &BTreeSet<String>) -> Result<Vec<Delegation>> {
        Ok(vec![delegation()])
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
        !reconcile(&mut session, &scope, platform.as_ref(), operation_id, true)
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
        reconcile(&mut session, &scope, platform.as_ref(), operation_id, false)
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
