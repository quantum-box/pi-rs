use super::*;
use std::sync::atomic::AtomicBool;

#[tokio::test]
async fn nested_batch_failure_prevents_earlier_read_dispatch() {
    struct Parent;
    struct Read(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Tool for Parent {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "lookup".into(),
                description: "parent".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
        async fn invoke_with_host(
            &self,
            _: Value,
            _: ToolContext,
            _: Option<&ExtensionState>,
            host: Arc<dyn ToolHost>,
        ) -> Outcome {
            let _ = host
                .call_batch(vec![
                    ("read".into(), json!({"id":1})),
                    ("read".into(), json!({"id":2})),
                ])
                .await;
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "read".into(),
                description: "read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.fetch_add(1, Ordering::SeqCst);
            Outcome::Succeeded("read".into())
        }
    }
    struct Fixture(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "batch-policy"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(Parent), Arc::new(Read(self.0.clone()))]
        }
        async fn before_tool(
            &self,
            name: &str,
            args: &Value,
            _: &ToolContext,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<Decision> {
            if name == "read" && args["id"] == 2 {
                Err("policy failed".into())
            } else {
                Ok(Decision::Allow)
            }
        }
    }
    let count = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register(Arc::new(Fixture(count.clone()))).unwrap();
    let model = Script::new(vec![tool_reply(&["{}"])]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        agent(&model, &registry).run(
            &mut session,
            Some("test".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        ),
    )
    .await
    .expect("batch stalled");
    assert!(result.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(
        session
            .operations()
            .iter()
            .filter(|(call, _)| call.call.name == "read")
            .all(|(_, state)| matches!(state, OperationState::Finished(Outcome::Skipped(_))))
    );
}

#[tokio::test]
async fn foreign_mutation_waits_for_poll_to_commit() {
    struct Parent(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Notify>);
    struct Pending(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Write(Arc<AtomicBool>);
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
                self.1.notified().await;
                host.call("write", json!({})).await
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
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            Outcome::Pending {
                reference: "job".into(),
            }
        }
        async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
            self.0.notify_one();
            self.1.notified().await;
            Some(Outcome::Succeeded("polled".into()))
        }
    }
    #[async_trait::async_trait]
    impl Tool for Write {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "write".into(),
                description: "write".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            assert!(
                self.0.load(Ordering::SeqCst),
                "write began before poll committed"
            );
            Outcome::Succeeded("wrote".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Barrier>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<AtomicBool>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "poll-barrier"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone(), self.1.clone())),
                Arc::new(Pending(self.1.clone(), self.2.clone())),
                Arc::new(Write(self.3.clone())),
            ]
        }
    }
    let poll_started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let committed = Arc::new(AtomicBool::new(false));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Barrier::new(2)),
            poll_started,
            release.clone(),
            committed.clone(),
        )))
        .unwrap();
    let model = Script::new(vec![
        tool_reply(&[r#"{"id":1}"#, r#"{"id":2}"#]),
        answer("done"),
    ]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let mut pending_id = None;
    let mut observer = |notification: Notification<'_>| {
        if let Notification::Committed(record) = notification {
            match &record.event {
                Event::ToolPlanned { invocation, .. } if invocation.call.name == "pending" => {
                    pending_id = Some(invocation.operation_id)
                }
                Event::ToolPlanned { invocation, .. } if invocation.call.name == "write" => {
                    release.notify_one()
                }
                Event::ToolFinished {
                    operation_id,
                    outcome: Outcome::Succeeded(_),
                    ..
                } if Some(*operation_id) == pending_id => {
                    committed.store(true, Ordering::SeqCst);
                }
                _ => {}
            }
        }
    };
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        agent(&model, &registry).run(
            &mut session,
            Some("test".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut observer,
        ),
    )
    .await
    .expect("poll barrier stalled");
    assert_eq!(result.unwrap(), "done");
    assert!(committed.load(Ordering::SeqCst));
}

async fn policy_state_after_wait_case(wait_in_preparation: bool) {
    struct Parent(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Notify>);
    struct Child(
        &'static str,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        bool,
    );
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
                host.call("target", json!({})).await
            } else {
                self.1.notified().await;
                host.call("update", json!({})).await
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: self.0.into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn prepare_arguments(&self, args: Value) -> ExtensionResult<Value> {
            if self.0 == "target" && self.3 {
                self.1.notify_one();
                self.2.notified().await;
            }
            Ok(args)
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            Outcome::Succeeded(self.0.into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Barrier>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        bool,
    );
    #[async_trait::async_trait]
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "policy-state"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone(), self.1.clone())),
                Arc::new(Child("target", self.1.clone(), self.2.clone(), self.3)),
                Arc::new(Child("update", self.1.clone(), self.2.clone(), self.3)),
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
            if !self.3
                && matches!(event, LifecycleEvent::ToolExecutionStart { tool_name, .. } if tool_name == "target")
            {
                self.1.notify_one();
                self.2.notified().await;
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
            if name == "target" && state.is_none() {
                return Err("policy saw stale state".into());
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
    let release = Arc::new(tokio::sync::Notify::new());
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Barrier::new(2)),
            Arc::new(tokio::sync::Notify::new()),
            release.clone(),
            wait_in_preparation,
        )))
        .unwrap();
    let model = Script::new(vec![
        tool_reply(&[r#"{"id":1}"#, r#"{"id":2}"#]),
        answer("done"),
    ]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
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
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        agent(&model, &registry).run(
            &mut session,
            Some("test".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut observer,
        ),
    )
    .await
    .expect("policy preparation stalled");
    assert_eq!(result.unwrap(), "done");
}

#[tokio::test]
async fn authorization_sees_commit_during_lifecycle() {
    policy_state_after_wait_case(false).await;
}
#[tokio::test]
async fn authorization_sees_commit_during_argument_preparation() {
    policy_state_after_wait_case(true).await;
}
