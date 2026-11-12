use super::*;

struct Pending;
#[async_trait::async_trait]
impl Tool for Pending {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "lookup".into(),
            description: "pending".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Pending {
            reference: "job".into(),
        }
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        None
    }
}
struct Policy {
    calls: AtomicUsize,
    stall_at: usize,
    reached: Arc<tokio::sync::Notify>,
}
#[async_trait::async_trait]
impl Extension for Policy {
    fn id(&self) -> &str {
        "pending-policy"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(Pending)]
    }
    async fn before_tool(
        &self,
        _: &str,
        _: &Value,
        _: &ToolContext,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<Decision> {
        if self.calls.fetch_add(1, Ordering::SeqCst) + 1 == self.stall_at {
            self.reached.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(Decision::Allow)
    }
}
fn assert_pending(session: &Session) {
    assert!(session.operations().iter().any(|(_, state)| matches!(state, OperationState::Finished(Outcome::Pending { reference }) if reference == "job")));
}
fn last_run_state(session: &Session) -> RunState {
    session
        .records()
        .iter()
        .rev()
        .find_map(|record| match record.event {
            Event::Run { state, .. } => Some(state),
            _ => None,
        })
        .unwrap()
}
#[tokio::test]
async fn poll_policy_timeout_leaves_run_waiting_for_operation() {
    let fixture = Arc::new(Policy {
        calls: AtomicUsize::new(0),
        stall_at: 2,
        reached: Arc::new(tokio::sync::Notify::new()),
    });
    let mut registry = Registry::default();
    registry.register(fixture.clone()).unwrap();
    let model = Script::new(vec![tool_reply(&["{}"])]);
    let mut harness = agent(&model, &registry);
    harness.limits.tool_timeout = Duration::from_millis(40);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        harness.run(
            &mut session,
            Some("test".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        ),
    )
    .await
    .expect("poll policy stalled");
    assert!(matches!(result, Err(Error::PendingOperation(_))));
    assert_eq!(last_run_state(&session), RunState::WaitingForOperation);
    assert_pending(&session);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
}
async fn resume_case(cancel_during_hook: bool) {
    let reached = Arc::new(tokio::sync::Notify::new());
    let fixture = Arc::new(Policy {
        calls: AtomicUsize::new(0),
        stall_at: 3,
        reached: reached.clone(),
    });
    let mut registry = Registry::default();
    registry.register(fixture.clone()).unwrap();
    let model = Script::new(vec![tool_reply(&["{}"])]);
    let mut harness = agent(&model, &registry);
    harness.limits.tool_timeout = Duration::from_millis(if cancel_during_hook { 1000 } else { 40 });
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let first = harness
        .run(
            &mut session,
            Some("test".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await;
    assert!(matches!(first, Err(Error::PendingOperation(_))));
    assert_pending(&session);
    let cancel = CancellationToken::new();
    let mut observer = ignore;
    let run = harness.run(&mut session, None, &mut inputs, &cancel, &mut observer);
    let interrupt = async {
        if cancel_during_hook {
            reached.notified().await;
            cancel.cancel();
        }
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(run, interrupt)
    })
    .await
    .expect("resume policy stalled");
    if cancel_during_hook {
        assert!(matches!(result, Err(Error::Cancelled)));
        assert_eq!(last_run_state(&session), RunState::Cancelled);
    } else {
        assert!(matches!(result, Err(Error::PendingOperation(_))));
        assert_eq!(last_run_state(&session), RunState::WaitingForOperation);
    }
    assert_pending(&session);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 3);
}
#[tokio::test]
async fn resumed_poll_policy_timeout_keeps_pending() {
    resume_case(false).await;
}
#[tokio::test]
async fn resumed_poll_policy_can_be_cancelled() {
    resume_case(true).await;
}
