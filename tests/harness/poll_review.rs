use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum WaitAt {
    Lifecycle,
    Preparation,
    PolicyCancel,
    PolicyTimeout,
}
struct Parent(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Notify>, WaitAt);
struct Pending(
    Arc<tokio::sync::Notify>,
    Arc<tokio::sync::Notify>,
    Arc<AtomicUsize>,
    WaitAt,
);
struct Update;
#[async_trait::async_trait]
impl Tool for Parent {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "lookup".into(),
            description: "parent".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn execution(&self) -> ToolExecution {
        ToolExecution::ParallelRead
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
    async fn invoke_with_host(
        &self,
        args: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> Outcome {
        self.0.wait().await;
        if args["id"] == 1 {
            host.call("pending", json!({})).await
        } else {
            if matches!(self.2, WaitAt::PolicyCancel | WaitAt::PolicyTimeout) {
                return Outcome::Succeeded("peer".into());
            }
            self.1.notified().await;
            host.call("update", json!({})).await
        }
    }
}
#[async_trait::async_trait]
impl Tool for Pending {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "pending".into(),
            description: "pending".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn execution(&self) -> ToolExecution {
        ToolExecution::ParallelRead
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn prepare_arguments(&self, args: Value) -> ExtensionResult<Value> {
        let count = self.2.fetch_add(1, Ordering::SeqCst);
        if count == 1 && self.3 == WaitAt::Preparation {
            self.0.notify_one();
            self.1.notified().await;
        }
        Ok(args)
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Pending {
            reference: "job".into(),
        }
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        Some(Outcome::Succeeded("polled".into()))
    }
}
#[async_trait::async_trait]
impl Tool for Update {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "update".into(),
            description: "update".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn execution(&self) -> ToolExecution {
        ToolExecution::ParallelRead
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Succeeded("updated".into())
    }
}
struct Fixture {
    barrier: Arc<tokio::sync::Barrier>,
    reached: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    prepares: Arc<AtomicUsize>,
    policies: AtomicUsize,
    wait: WaitAt,
}
#[async_trait::async_trait]
impl Extension for Fixture {
    fn id(&self) -> &str {
        "poll-policy"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![
            Arc::new(Parent(
                self.barrier.clone(),
                self.reached.clone(),
                self.wait,
            )),
            Arc::new(Pending(
                self.reached.clone(),
                self.release.clone(),
                self.prepares.clone(),
                self.wait,
            )),
            Arc::new(Update),
        ]
    }
    fn handles_lifecycle(&self) -> bool {
        true
    }
    fn handles_tool_execution_lifecycle(&self) -> bool {
        true
    }
    async fn lifecycle(
        &self,
        event: &LifecycleEvent,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        if self.wait == WaitAt::Lifecycle
            && matches!(event, LifecycleEvent::ToolExecutionEnd { tool_name, result: Outcome::Pending { .. }, .. } if tool_name == "pending")
        {
            self.reached.notify_one();
            self.release.notified().await;
        }
        Ok(())
    }
    async fn before_tool(
        &self,
        name: &str,
        _: &Value,
        _: &ToolContext,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<Decision> {
        if name != "pending" {
            return Ok(Decision::Allow);
        }
        if self.policies.fetch_add(1, Ordering::SeqCst) == 0 {
            return Ok(Decision::Allow);
        }
        if matches!(self.wait, WaitAt::PolicyCancel | WaitAt::PolicyTimeout) {
            self.reached.notify_one();
            self.release.notified().await;
        }
        if matches!(self.wait, WaitAt::Lifecycle | WaitAt::Preparation) && state.is_none() {
            return Err("poll policy saw stale state".into());
        }
        Ok(Decision::Allow)
    }
    fn after_tool(
        &self,
        name: &str,
        _: &Outcome,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        Ok((name == "update").then(|| ExtensionState {
            version: 1,
            data: json!({"ready":true}),
        }))
    }
}
async fn run_case(wait: WaitAt) {
    let reached = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut registry = Registry::default();
    let fixture = Arc::new(Fixture {
        barrier: Arc::new(tokio::sync::Barrier::new(2)),
        reached: reached.clone(),
        release: release.clone(),
        prepares: Arc::new(AtomicUsize::new(0)),
        policies: AtomicUsize::new(0),
        wait,
    });
    registry.register(fixture.clone()).unwrap();
    let model = Script::new(vec![
        tool_reply(&[r#"{"id":1}"#, r#"{"id":2}"#]),
        answer("done"),
    ]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let mut update_id = None;
    let mut observer = |notification: Notification<'_>| {
        if let Notification::Committed(record) = notification {
            match &record.event {
                Event::ToolPlanned { invocation, .. } if invocation.call.name == "update" => {
                    update_id = Some(invocation.operation_id)
                }
                Event::ToolFinished { operation_id, .. } if Some(*operation_id) == update_id => {
                    release.notify_one()
                }
                _ => {}
            }
        }
    };
    let mut harness = agent(&model, &registry);
    if wait == WaitAt::PolicyTimeout {
        harness.limits.tool_timeout = Duration::from_millis(40);
    }
    let run = harness.run(
        &mut session,
        Some("test".into()),
        &mut inputs,
        &cancel,
        &mut observer,
    );
    let interrupt = async {
        if wait == WaitAt::PolicyCancel {
            reached.notified().await;
            cancel.cancel();
        }
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(run, interrupt)
    })
    .await
    .expect("poll policy stalled");
    match wait {
        WaitAt::Lifecycle | WaitAt::Preparation => assert_eq!(result.unwrap(), "done"),
        WaitAt::PolicyCancel | WaitAt::PolicyTimeout => assert!(result.is_err()),
    }
    assert_eq!(
        fixture.policies.load(Ordering::SeqCst),
        2,
        "poll policy was not reached"
    );
    if matches!(wait, WaitAt::PolicyCancel | WaitAt::PolicyTimeout) {
        assert!(session.operations().iter().any(|(call, state)| call.call.name == "pending" && matches!(state, OperationState::Finished(Outcome::Pending { reference }) if reference == "job")));
    }
}
#[tokio::test]
async fn poll_policy_sees_commit_during_lifecycle() {
    run_case(WaitAt::Lifecycle).await;
}
#[tokio::test]
async fn poll_policy_sees_commit_during_argument_preparation() {
    run_case(WaitAt::Preparation).await;
}
#[tokio::test]
async fn cancellation_interrupts_poll_policy_and_preserves_pending() {
    run_case(WaitAt::PolicyCancel).await;
}
#[tokio::test]
async fn timeout_interrupts_poll_policy_and_preserves_pending() {
    run_case(WaitAt::PolicyTimeout).await;
}
