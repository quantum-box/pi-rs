use super::*;

// The second request is sent only after the first nested read has started. This
// exercises requests that cannot be coalesced by the dispatcher's initial drain.
#[tokio::test]
async fn late_nested_parallel_reads_overlap_and_are_journaled() {
    struct Parent(Arc<tokio::sync::Notify>);
    struct Read(Arc<tokio::sync::Notify>, tokio::sync::Barrier);
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
            let first = host.call("late_read", json!({"id":1}));
            let second = async {
                self.0.notified().await;
                host.call("late_read", json!({"id":2})).await
            };
            let (first, second) = tokio::join!(first, second);
            assert_eq!(first, Outcome::Succeeded("1".into()));
            assert_eq!(second, Outcome::Succeeded("2".into()));
            Outcome::Succeeded("both".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "late_read".into(),
                description: "read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
            if args["id"] == 1 {
                self.0.notify_one();
            }
            self.1.wait().await;
            if args["id"] == 1 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Outcome::Succeeded(args["id"].to_string())
        }
    }
    struct Fixture(Arc<tokio::sync::Notify>);
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "late-reads"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone())),
                Arc::new(Read(self.0.clone(), tokio::sync::Barrier::new(2))),
            ]
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(Arc::new(tokio::sync::Notify::new()))))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
        Duration::from_secs(2),
        agent(&model, &registry).run(
            &mut session,
            Some("read".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        ),
    )
    .await
    .expect("late parallel read did not start")
    .unwrap();
    let nested: Vec<_> = session
        .records()
        .iter()
        .filter_map(|record| match &record.event {
            Event::ToolPlanned { invocation, .. } if invocation.call.name == "late_read" => {
                Some(invocation.operation_id)
            }
            _ => None,
        })
        .collect();
    assert_eq!(nested.len(), 2);
    let finished: Vec<_> = session
        .records()
        .iter()
        .filter_map(|record| match &record.event {
            Event::ToolFinished {
                operation_id,
                outcome: Outcome::Succeeded(_),
                ..
            } if nested.contains(operation_id) => Some(*operation_id),
            _ => None,
        })
        .collect();
    assert_eq!(
        finished, nested,
        "parallel read results must be committed in call order"
    );
}

#[tokio::test]
async fn foreign_read_routes_nested_call_after_child_returns() {
    struct Parent(Arc<tokio::sync::Notify>);
    struct Read {
        b_started: Arc<tokio::sync::Notify>,
        child_returned: Arc<tokio::sync::Notify>,
    }
    struct Child(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Probe;
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
            let a = host.call("independent_read", json!({"id":"a"}));
            let b = async {
                self.0.notified().await;
                host.call("independent_read", json!({"id":"b"})).await
            };
            let (a, b) = tokio::join!(a, b);
            assert!(matches!(a, Outcome::Succeeded(_)) && matches!(b, Outcome::Succeeded(_)));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_read".into(),
                description: "read".into(),
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
            if args["id"] == "a" {
                let result = host.call("independent_child", json!({})).await;
                self.child_returned.notify_one();
                result
            } else {
                self.b_started.notify_one();
                self.child_returned.notified().await;
                host.call("independent_probe", json!({})).await
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("a".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Probe {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_probe".into(),
                description: "nested read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            Outcome::Succeeded("b".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "independent"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone())),
                Arc::new(Read {
                    b_started: self.1.clone(),
                    child_returned: self.2.clone(),
                }),
                Arc::new(Child(self.0.clone(), self.1.clone())),
                Arc::new(Probe),
            ]
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
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
    .expect("foreign nested route did not survive child completion")
    .unwrap();
    let operations = session.operations();
    let (_, state) = operations
        .iter()
        .find(|(invocation, _)| invocation.call.name == "independent_probe")
        .expect("nested probe was not journaled");
    assert!(matches!(state, OperationState::Finished(Outcome::Succeeded(value)) if value == "b"));
}

#[tokio::test]
async fn foreign_reads_commit_in_plan_order_across_levels() {
    struct Parent(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Read(Arc<tokio::sync::Notify>);
    struct ChildOne(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct ChildTwo(Arc<tokio::sync::Notify>, Arc<tokio::sync::Barrier>);
    struct Third(Arc<tokio::sync::Barrier>);
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
            let one = host.call("ordered_level_read", json!({"id":1}));
            let two = async {
                self.0.notified().await;
                host.call("ordered_level_read", json!({"id":2})).await
            };
            let three = async {
                self.1.notified().await;
                host.call("ordered_level_third", json!({})).await
            };
            let (one, two, three) = tokio::join!(one, two, three);
            assert!(
                matches!(one, Outcome::Succeeded(_))
                    && matches!(two, Outcome::Succeeded(_))
                    && matches!(three, Outcome::Succeeded(_))
            );
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "ordered_level_read".into(),
                description: "read".into(),
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
            if args["id"] == 1 {
                host.call("ordered_level_child_one", json!({})).await
            } else {
                let result = host.call("ordered_level_child_two", json!({})).await;
                self.0.notify_one();
                result
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for ChildOne {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "ordered_level_child_one".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("one".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for ChildTwo {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "ordered_level_child_two".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.wait().await;
            Outcome::Succeeded("two".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Third {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "ordered_level_third".into(),
                description: "read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.wait().await;
            Outcome::Succeeded("three".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Barrier>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "ordered-levels"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone(), self.1.clone())),
                Arc::new(Read(self.2.clone())),
                Arc::new(ChildOne(self.0.clone(), self.2.clone())),
                Arc::new(ChildTwo(self.1.clone(), self.3.clone())),
                Arc::new(Third(self.3.clone())),
            ]
        }
    }
    struct ResultOrder(Arc<Mutex<Vec<String>>>);
    #[async_trait::async_trait]
    impl Extension for ResultOrder {
        fn id(&self) -> &str {
            "result-order"
        }
        fn handles_tool_result(&self) -> bool {
            true
        }
        async fn tool_result(
            &self,
            call: &ToolCall,
            _: &Outcome,
            _: &ToolContext,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<Option<Outcome>> {
            if call.name == "ordered_level_read" || call.name == "ordered_level_third" {
                self.0
                    .lock()
                    .unwrap()
                    .push(format!("{}:{}", call.name, call.arguments));
            }
            Ok(None)
        }
    }
    let result_order = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(ResultOrder(result_order.clone())))
        .unwrap();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Barrier::new(2)),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
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
    .expect("nested reads stalled")
    .unwrap();
    let planned: Vec<_> = session
        .records()
        .iter()
        .filter_map(|record| match &record.event {
            Event::ToolPlanned { invocation, .. }
                if invocation.call.name == "ordered_level_read"
                    || invocation.call.name == "ordered_level_third" =>
            {
                Some(invocation.operation_id)
            }
            _ => None,
        })
        .collect();
    let finished: Vec<_> = session
        .records()
        .iter()
        .filter_map(|record| match &record.event {
            Event::ToolFinished { operation_id, .. } if planned.contains(operation_id) => {
                Some(*operation_id)
            }
            _ => None,
        })
        .collect();
    assert_eq!(planned.len(), 3);
    assert_eq!(
        finished, planned,
        "cross-level results must commit in plan order"
    );
    let hooks = result_order.lock().unwrap();
    assert_eq!(hooks.len(), 3);
    assert!(hooks[0].contains("\"id\":1"));
    assert!(hooks[1].contains("\"id\":2"));
    assert!(hooks[2].starts_with("ordered_level_third:"));
}

#[tokio::test]
async fn late_nested_writes_remain_barriers() {
    struct Parent(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<Mutex<Vec<String>>>,
    );
    struct Read(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<Mutex<Vec<String>>>,
    );
    struct Write(Arc<Mutex<Vec<String>>>);
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
            let first = host.call("ordered_read", json!({"id":"first"}));
            let later = async {
                self.0.notified().await;
                let calls = host.call_batch(vec![
                    ("ordered_write".into(), json!({})),
                    ("ordered_read".into(), json!({"id":"last"})),
                ]);
                let release = async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    assert_eq!(*self.2.lock().unwrap(), vec!["first:start"]);
                    self.1.notify_one();
                };
                let (results, ()) = tokio::join!(calls, release);
                assert!(
                    results
                        .iter()
                        .all(|outcome| matches!(outcome, Outcome::Succeeded(_)))
                );
            };
            let (result, ()) = tokio::join!(first, later);
            assert!(matches!(result, Outcome::Succeeded(_)));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "ordered_read".into(),
                description: "read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
            let id = args["id"].as_str().unwrap();
            self.2.lock().unwrap().push(format!("{id}:start"));
            if id == "first" {
                self.0.notify_one();
                self.1.notified().await;
            }
            self.2.lock().unwrap().push(format!("{id}:end"));
            Outcome::Succeeded(id.into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Write {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "ordered_write".into(),
                description: "write".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.lock().unwrap().push("write".into());
            Outcome::Succeeded("written".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<Mutex<Vec<String>>>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "nested-order"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone(), self.1.clone(), self.2.clone())),
                Arc::new(Read(self.0.clone(), self.1.clone(), self.2.clone())),
                Arc::new(Write(self.2.clone())),
            ]
        }
    }
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            events.clone(),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
        Duration::from_secs(2),
        agent(&model, &registry).run(
            &mut session,
            Some("order".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        ),
    )
    .await
    .expect("barrier stalled")
    .unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        vec![
            "first:start",
            "first:end",
            "write",
            "last:start",
            "last:end"
        ]
    );
}

#[tokio::test]
async fn nested_write_blocks_late_ancestor_read() {
    struct Parent(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<Mutex<Vec<String>>>,
    );
    struct Read(Arc<Mutex<Vec<String>>>);
    struct Write(Arc<Mutex<Vec<String>>>);
    struct Child(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
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
            let first = host.call("ordered_read", json!({"id":"first"}));
            let later = async {
                self.0.notified().await;
                let calls = host.call_batch(vec![("ordered_read".into(), json!({"id":"last"}))]);
                let release = async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    assert_eq!(*self.2.lock().unwrap(), vec!["first:start"]);
                    self.1.notify_one();
                };
                let (results, ()) = tokio::join!(calls, release);
                assert!(
                    results
                        .iter()
                        .all(|outcome| matches!(outcome, Outcome::Succeeded(_)))
                );
            };
            let (result, ()) = tokio::join!(first, later);
            assert!(matches!(result, Outcome::Succeeded(_)));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "ordered_read".into(),
                description: "read".into(),
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
            let id = args["id"].as_str().unwrap();
            self.0.lock().unwrap().push(format!("{id}:start"));
            if id == "first" {
                assert!(matches!(
                    host.call("ordered_child", json!({})).await,
                    Outcome::Succeeded(_)
                ));
            }
            self.0.lock().unwrap().push(format!("{id}:end"));
            Outcome::Succeeded(id.into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "ordered_child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("child".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Write {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "ordered_write".into(),
                description: "write".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.lock().unwrap().push("write".into());
            Outcome::Succeeded("written".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<Mutex<Vec<String>>>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "nested-order"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone(), self.1.clone(), self.2.clone())),
                Arc::new(Read(self.2.clone())),
                Arc::new(Write(self.2.clone())),
                Arc::new(Child(self.0.clone(), self.1.clone())),
            ]
        }
    }
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            events.clone(),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
        Duration::from_secs(2),
        agent(&model, &registry).run(
            &mut session,
            Some("order".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        ),
    )
    .await
    .expect("barrier stalled")
    .unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        vec!["first:start", "first:end", "last:start", "last:end"]
    );
}

#[tokio::test]
async fn nested_parallel_reads_are_limited_to_eight() {
    struct Parent(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Semaphore>,
        Arc<AtomicUsize>,
        Arc<tokio::sync::Notify>,
    );
    struct Read(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Semaphore>,
        Arc<AtomicUsize>,
        Arc<tokio::sync::Notify>,
    );
    struct Child(Read);
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
            let first = host.call("bounded_read", json!({"id":0}));
            let later = async {
                self.3.notified().await;
                let calls = (1..9)
                    .map(|id| ("bounded_read".into(), json!({"id":id})))
                    .collect();
                host.call_batch(calls).await
            };
            let release = async {
                self.0.notified().await;
                assert_eq!(self.2.load(Ordering::SeqCst), 8);
                tokio::task::yield_now().await;
                assert_eq!(self.2.load(Ordering::SeqCst), 8);
                self.1.add_permits(9);
            };
            let (first, results, ()) = tokio::join!(first, later, release);
            assert!(matches!(first, Outcome::Succeeded(_)));
            assert_eq!(results.len(), 8);
            assert!(
                results
                    .iter()
                    .all(|result| matches!(result, Outcome::Succeeded(_)))
            );
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "bounded_read".into(),
                description: "read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke_with_host(
            &self,
            args: Value,
            context: ToolContext,
            _: Option<&ExtensionState>,
            host: Arc<dyn ToolHost>,
        ) -> Outcome {
            if args["id"] == 0 {
                host.call("bounded_child", json!({})).await
            } else {
                self.invoke(args, context).await
            }
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            let count = self.2.fetch_add(1, Ordering::SeqCst) + 1;
            if count == 1 {
                self.3.notify_one();
            }
            if count == 8 {
                self.0.notify_one();
            }
            self.1.acquire().await.unwrap().forget();
            Outcome::Succeeded("read".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "bounded_child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, args: Value, context: ToolContext) -> Outcome {
            self.0.invoke(args, context).await
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Semaphore>,
        Arc<AtomicUsize>,
        Arc<tokio::sync::Notify>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "bounded-nested"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(
                    self.0.clone(),
                    self.1.clone(),
                    self.2.clone(),
                    self.3.clone(),
                )),
                Arc::new(Read(
                    self.0.clone(),
                    self.1.clone(),
                    self.2.clone(),
                    self.3.clone(),
                )),
                Arc::new(Child(Read(
                    self.0.clone(),
                    self.1.clone(),
                    self.2.clone(),
                    self.3.clone(),
                ))),
            ]
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Semaphore::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(tokio::sync::Notify::new()),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
        Duration::from_secs(2),
        agent(&model, &registry).run(
            &mut session,
            Some("bounded".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        ),
    )
    .await
    .expect("bounded reads stalled")
    .unwrap();
}

#[tokio::test]
async fn cancellation_settles_reads_across_nested_dispatch_levels() {
    struct Parent(Arc<tokio::sync::Notify>);
    struct Read(CancellationToken);
    struct Child(Arc<tokio::sync::Notify>);
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
            let first = host.call("level_read", json!({"id":1}));
            let second = async {
                self.0.notified().await;
                host.call("level_read", json!({"id":2})).await
            };
            let _ = tokio::join!(first, second);
            Outcome::Succeeded("unexpected".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "level_read".into(),
                description: "read".into(),
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
            if args["id"] == 1 {
                return host.call("level_child", json!({})).await;
            }
            self.0.cancel();
            std::future::pending().await
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "level_child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            std::future::pending().await
        }
    }
    struct Fixture(Arc<tokio::sync::Notify>, CancellationToken);
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "cancel-levels"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone())),
                Arc::new(Read(self.1.clone())),
                Arc::new(Child(self.0.clone())),
            ]
        }
    }
    let cancel = CancellationToken::new();
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            cancel.clone(),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"])]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        agent(&model, &registry).run(
            &mut session,
            Some("cancel".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        ),
    )
    .await
    .expect("nested cancellation did not settle");
    assert!(result.is_err());
    let nested: Vec<_> = session
        .operations()
        .into_iter()
        .filter(|(invocation, _)| {
            invocation.call.name == "level_read" || invocation.call.name == "level_child"
        })
        .collect();
    assert_eq!(nested.len(), 3);
    assert!(
        nested
            .iter()
            .all(|(_, state)| matches!(state, OperationState::Finished(Outcome::Unknown(_))))
    );
}

#[tokio::test]
async fn foreign_nested_batch_preserves_write_barriers() {
    struct Parent(Arc<tokio::sync::Notify>);
    struct Read(Arc<tokio::sync::Notify>);
    struct Child(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Probe {
        start: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        done: Arc<AtomicUsize>,
    }
    struct Write(Arc<AtomicUsize>, Arc<AtomicUsize>);
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
            let a = host.call("foreign_barrier_read", json!({"id":"a"}));
            let b = async {
                self.0.notified().await;
                host.call("foreign_barrier_read", json!({"id":"b"})).await
            };
            let (a, b) = tokio::join!(a, b);
            assert!(matches!(a, Outcome::Succeeded(_)) && matches!(b, Outcome::Succeeded(_)));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "foreign_barrier_read".into(),
                description: "read".into(),
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
            if args["id"] == "a" {
                host.call("foreign_barrier_child", json!({})).await
            } else {
                let results = host
                    .call_batch(vec![
                        ("foreign_barrier_probe".into(), json!({})),
                        ("foreign_barrier_write".into(), json!({})),
                    ])
                    .await;
                self.0.notify_one();
                assert!(results.iter().all(|r| matches!(r, Outcome::Succeeded(_))));
                Outcome::Succeeded("b".into())
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "foreign_barrier_child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("a".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Probe {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "foreign_barrier_probe".into(),
                description: "probe".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.start.notify_one();
            self.release.notified().await;
            self.done.store(1, Ordering::SeqCst);
            Outcome::Succeeded("read".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Write {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "foreign_barrier_write".into(),
                description: "write".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            if self.0.load(Ordering::SeqCst) == 0 {
                self.1.store(1, Ordering::SeqCst);
            }
            Outcome::Succeeded("write".into())
        }
    }
    struct Fixture {
        start: Arc<tokio::sync::Notify>,
        end: Arc<tokio::sync::Notify>,
        probe_start: Arc<tokio::sync::Notify>,
        probe_release: Arc<tokio::sync::Notify>,
        done: Arc<AtomicUsize>,
        violation: Arc<AtomicUsize>,
    }
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "foreign-barrier"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.start.clone())),
                Arc::new(Read(self.end.clone())),
                Arc::new(Child(self.start.clone(), self.end.clone())),
                Arc::new(Probe {
                    start: self.probe_start.clone(),
                    release: self.probe_release.clone(),
                    done: self.done.clone(),
                }),
                Arc::new(Write(self.done.clone(), self.violation.clone())),
            ]
        }
    }
    let fixture = Arc::new(Fixture {
        start: Arc::new(tokio::sync::Notify::new()),
        end: Arc::new(tokio::sync::Notify::new()),
        probe_start: Arc::new(tokio::sync::Notify::new()),
        probe_release: Arc::new(tokio::sync::Notify::new()),
        done: Arc::new(AtomicUsize::new(0)),
        violation: Arc::new(AtomicUsize::new(0)),
    });
    let mut registry = Registry::default();
    registry.register(fixture.clone()).unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let release = async {
        fixture.probe_start.notified().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        fixture.probe_release.notify_one();
    };
    let dispatcher = agent(&model, &registry);
    let cancellation = CancellationToken::new();
    let mut observer = ignore;
    let run = dispatcher.run(
        &mut session,
        Some("test".into()),
        &mut inputs,
        &cancellation,
        &mut observer,
    );
    let (result, ()) =
        tokio::time::timeout(Duration::from_secs(2), async { tokio::join!(run, release) })
            .await
            .expect("foreign batch stalled");
    result.unwrap();
    assert_eq!(
        fixture.violation.load(Ordering::SeqCst),
        0,
        "write overlapped preceding sibling read"
    );
}

#[tokio::test]
async fn cancellation_during_foreign_preparation_settles_all_operations() {
    struct Parent(Arc<tokio::sync::Notify>);
    struct Read {
        b_started: Arc<tokio::sync::Notify>,
        cancel: CancellationToken,
        child_returned: Arc<tokio::sync::Notify>,
    }
    struct Child(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
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
            let a = host.call("independent_read", json!({"id":"a"}));
            let b = async {
                self.0.notified().await;
                host.call("independent_read", json!({"id":"b"})).await
            };
            let (a, b) = tokio::join!(a, b);
            assert!(matches!(a, Outcome::Succeeded(_)) && matches!(b, Outcome::Succeeded(_)));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_read".into(),
                description: "read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn prepare_arguments(&self, args: Value) -> ExtensionResult<Value> {
            if args["id"] == "b" {
                self.b_started.notify_one();
                self.cancel.cancel();
                std::future::pending::<()>().await;
            }
            Ok(args)
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
            if args["id"] == "a" {
                let result = host.call("independent_child", json!({})).await;
                self.child_returned.notify_one();
                result
            } else {
                Outcome::Succeeded("b".into())
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("a".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        CancellationToken,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "independent"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone())),
                Arc::new(Read {
                    b_started: self.1.clone(),
                    cancel: self.3.clone(),
                    child_returned: self.2.clone(),
                }),
                Arc::new(Child(self.0.clone(), self.1.clone())),
            ]
        }
    }
    let cancel = CancellationToken::new();
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            cancel.clone(),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        agent(&model, &registry).run(
            &mut session,
            Some("test".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        ),
    )
    .await
    .expect("cancellation stalled foreign preparation");
    assert!(result.is_err());
    let nested: Vec<_> = session
        .operations()
        .into_iter()
        .filter(|(invocation, _)| {
            invocation.call.name == "independent_read"
                || invocation.call.name == "independent_child"
        })
        .collect();
    assert_eq!(nested.len(), 3);
    assert!(
        nested
            .iter()
            .all(|(_, state)| matches!(state, OperationState::Finished(_)))
    );
}

#[tokio::test]
async fn foreign_nested_call_does_not_delay_unrelated_child() {
    struct Parent(Arc<tokio::sync::Notify>);
    struct Read {
        child_returned: Arc<tokio::sync::Notify>,
    }
    struct Child(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Probe(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
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
            let a = host.call("independent_read", json!({"id":"a"}));
            let b = async {
                self.0.notified().await;
                host.call("independent_read", json!({"id":"b"})).await
            };
            let (a, b) = tokio::join!(a, b);
            assert!(matches!(a, Outcome::Succeeded(_)) && matches!(b, Outcome::Succeeded(_)));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_read".into(),
                description: "read".into(),
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
            if args["id"] == "a" {
                let result = host.call("independent_child", json!({})).await;
                self.child_returned.notify_one();
                result
            } else {
                host.call("independent_probe", json!({})).await
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("a".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Probe {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_probe".into(),
                description: "nested read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("b".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "independent"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone())),
                Arc::new(Read {
                    child_returned: self.2.clone(),
                }),
                Arc::new(Child(self.0.clone(), self.1.clone())),
                Arc::new(Probe(self.1.clone(), self.2.clone())),
            ]
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
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
    .expect("foreign nested route did not run while child was active")
    .unwrap();
    let operations = session.operations();
    let (_, state) = operations
        .iter()
        .find(|(invocation, _)| invocation.call.name == "independent_probe")
        .expect("nested probe was not journaled");
    assert!(matches!(state, OperationState::Finished(Outcome::Succeeded(value)) if value == "b"));
}

#[tokio::test]
async fn foreign_nested_lifecycle_does_not_delay_ancestor_completion() {
    struct Parent(Arc<tokio::sync::Notify>);
    struct Read {
        child_returned: Arc<tokio::sync::Notify>,
    }
    struct Child(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Probe;
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
            let a = host.call("independent_read", json!({"id":"a"}));
            let b = async {
                self.0.notified().await;
                host.call("independent_read", json!({"id":"b"})).await
            };
            let (a, b) = tokio::join!(a, b);
            assert!(matches!(a, Outcome::Succeeded(_)) && matches!(b, Outcome::Succeeded(_)));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_read".into(),
                description: "read".into(),
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
            if args["id"] == "a" {
                let result = host.call("independent_child", json!({})).await;
                self.child_returned.notify_one();
                result
            } else {
                host.call("independent_probe", json!({})).await
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("a".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Probe {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_probe".into(),
                description: "nested read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            Outcome::Succeeded("b".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    #[async_trait::async_trait]
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "independent"
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
            if matches!(event, LifecycleEvent::ToolExecutionStart { tool_name, .. } if tool_name == "independent_probe")
            {
                self.1.notify_one();
                self.2.notified().await;
            }
            Ok(())
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone())),
                Arc::new(Read {
                    child_returned: self.2.clone(),
                }),
                Arc::new(Child(self.0.clone(), self.1.clone())),
                Arc::new(Probe),
            ]
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
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
    .expect("foreign nested route did not run while child was active")
    .unwrap();
    let operations = session.operations();
    let (_, state) = operations
        .iter()
        .find(|(invocation, _)| invocation.call.name == "independent_probe")
        .expect("nested probe was not journaled");
    assert!(matches!(state, OperationState::Finished(Outcome::Succeeded(value)) if value == "b"));
}

#[tokio::test]
async fn foreign_nested_preparation_does_not_delay_ancestor_completion() {
    struct Parent(Arc<tokio::sync::Notify>);
    struct Read {
        child_returned: Arc<tokio::sync::Notify>,
    }
    struct Child(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Probe(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
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
            let a = host.call("independent_read", json!({"id":"a"}));
            let b = async {
                self.0.notified().await;
                host.call("independent_read", json!({"id":"b"})).await
            };
            let (a, b) = tokio::join!(a, b);
            assert!(matches!(a, Outcome::Succeeded(_)) && matches!(b, Outcome::Succeeded(_)));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_read".into(),
                description: "read".into(),
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
            if args["id"] == "a" {
                let result = host.call("independent_child", json!({})).await;
                self.child_returned.notify_one();
                result
            } else {
                host.call("independent_probe", json!({})).await
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("a".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Probe {
        async fn prepare_arguments(&self, args: Value) -> ExtensionResult<Value> {
            self.0.notify_one();
            self.1.notified().await;
            Ok(args)
        }
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_probe".into(),
                description: "nested read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            Outcome::Succeeded("b".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "independent"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone())),
                Arc::new(Read {
                    child_returned: self.2.clone(),
                }),
                Arc::new(Child(self.0.clone(), self.1.clone())),
                Arc::new(Probe(self.1.clone(), self.2.clone())),
            ]
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
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
    .expect("foreign nested route did not run while child was active")
    .unwrap();
    let operations = session.operations();
    let (_, state) = operations
        .iter()
        .find(|(invocation, _)| invocation.call.name == "independent_probe")
        .expect("nested probe was not journaled");
    assert!(matches!(state, OperationState::Finished(Outcome::Succeeded(value)) if value == "b"));
}

#[tokio::test]
async fn foreign_nested_result_hook_does_not_delay_ancestor_completion() {
    struct Parent(Arc<tokio::sync::Notify>);
    struct Read {
        child_returned: Arc<tokio::sync::Notify>,
    }
    struct Child(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Probe(Arc<tokio::sync::Notify>);
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
            let a = host.call("independent_read", json!({"id":"a"}));
            let b = async {
                self.0.notified().await;
                host.call("independent_read", json!({"id":"b"})).await
            };
            let (a, b) = tokio::join!(a, b);
            assert!(matches!(a, Outcome::Succeeded(_)) && matches!(b, Outcome::Succeeded(_)));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_read".into(),
                description: "read".into(),
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
            if args["id"] == "a" {
                let result = host.call("independent_child", json!({})).await;
                self.child_returned.notify_one();
                result
            } else {
                host.call("independent_probe", json!({})).await
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("a".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Probe {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_probe".into(),
                description: "nested read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            Outcome::Succeeded("b".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    #[async_trait::async_trait]
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "independent"
        }
        fn handles_tool_result(&self) -> bool {
            true
        }
        async fn tool_result(
            &self,
            call: &ToolCall,
            _: &Outcome,
            _: &ToolContext,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<Option<Outcome>> {
            if call.name == "independent_probe" {
                self.2.notified().await;
            }
            Ok(None)
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone())),
                Arc::new(Read {
                    child_returned: self.2.clone(),
                }),
                Arc::new(Child(self.0.clone(), self.1.clone())),
                Arc::new(Probe(self.1.clone())),
            ]
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
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
    .expect("foreign nested route did not run while child was active")
    .unwrap();
    let operations = session.operations();
    let (_, state) = operations
        .iter()
        .find(|(invocation, _)| invocation.call.name == "independent_probe")
        .expect("nested probe was not journaled");
    assert!(matches!(state, OperationState::Finished(Outcome::Succeeded(value)) if value == "b"));
}

#[tokio::test]
async fn foreign_nested_end_hook_does_not_delay_ancestor_completion() {
    struct Parent(Arc<tokio::sync::Notify>);
    struct Read {
        child_returned: Arc<tokio::sync::Notify>,
    }
    struct Child(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Probe(Arc<tokio::sync::Notify>);
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
            let a = host.call("independent_read", json!({"id":"a"}));
            let b = async {
                self.0.notified().await;
                host.call("independent_read", json!({"id":"b"})).await
            };
            let (a, b) = tokio::join!(a, b);
            assert!(matches!(a, Outcome::Succeeded(_)) && matches!(b, Outcome::Succeeded(_)));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_read".into(),
                description: "read".into(),
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
            if args["id"] == "a" {
                let result = host.call("independent_child", json!({})).await;
                self.child_returned.notify_one();
                result
            } else {
                host.call("independent_probe", json!({})).await
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("a".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Probe {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_probe".into(),
                description: "nested read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            Outcome::Succeeded("b".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    #[async_trait::async_trait]
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "independent"
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
            if matches!(event, LifecycleEvent::ToolExecutionEnd { tool_name, .. } if tool_name == "independent_probe")
            {
                self.2.notified().await;
            }
            Ok(())
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone())),
                Arc::new(Read {
                    child_returned: self.2.clone(),
                }),
                Arc::new(Child(self.0.clone(), self.1.clone())),
                Arc::new(Probe(self.1.clone())),
            ]
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
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
    .expect("foreign nested route did not run while child was active")
    .unwrap();
    let operations = session.operations();
    let (_, state) = operations
        .iter()
        .find(|(invocation, _)| invocation.call.name == "independent_probe")
        .expect("nested probe was not journaled");
    assert!(matches!(state, OperationState::Finished(Outcome::Succeeded(value)) if value == "b"));
}

#[tokio::test]
async fn completed_read_reply_is_not_held_by_later_sibling() {
    struct Parent(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Read(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
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
            let first = async {
                let result = host.call("sibling_read", json!({"id":1})).await;
                self.1.notify_one();
                result
            };
            let second = async {
                self.0.notified().await;
                host.call("sibling_read", json!({"id":2})).await
            };
            let (a, b) = tokio::join!(first, second);
            assert!(matches!(a, Outcome::Succeeded(_)) && matches!(b, Outcome::Succeeded(_)));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "sibling_read".into(),
                description: "read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
            if args["id"] == 1 {
                self.0.notify_one();
                self.2.notified().await;
            } else {
                self.2.notify_one();
                self.1.notified().await;
            }
            Outcome::Succeeded(args["id"].to_string())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "ordered-replies"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone(), self.1.clone())),
                Arc::new(Read(self.0.clone(), self.1.clone(), self.2.clone())),
            ]
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
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
    .expect("earlier reply waited for later sibling")
    .unwrap();
}

#[tokio::test]
async fn result_hook_can_finish_while_sibling_nested_call_waits() {
    struct Parent(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Read(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    struct Probe(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
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
            let first = async {
                let result = host.call("sibling_read", json!({"id":1})).await;
                self.1.notify_one();
                result
            };
            let second = async {
                self.0.notified().await;
                host.call("sibling_read", json!({"id":2})).await
            };
            let (a, b) = tokio::join!(first, second);
            assert!(matches!(a, Outcome::Succeeded(_)) && matches!(b, Outcome::Succeeded(_)));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "sibling_read".into(),
                description: "read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn prepare_arguments(&self, args: Value) -> ExtensionResult<Value> {
            if args["id"] == 2 {
                self.1.notify_one();
                self.2.notified().await;
            }
            Ok(args)
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
            if args["id"] == 1 {
                self.0.notify_one();
                self.1.notified().await;
                Outcome::Succeeded("1".into())
            } else {
                host.call("sibling_probe", json!({})).await
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Probe {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "sibling_probe".into(),
                description: "probe".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("2".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    #[async_trait::async_trait]
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "ordered-replies"
        }
        fn handles_tool_result(&self) -> bool {
            true
        }
        async fn tool_result(
            &self,
            call: &ToolCall,
            _: &Outcome,
            _: &ToolContext,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<Option<Outcome>> {
            if call.name == "sibling_read" && call.arguments.contains("1") {
                self.3.notify_one();
                self.4.notified().await;
            }
            Ok(None)
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone(), self.1.clone())),
                Arc::new(Read(self.0.clone(), self.2.clone(), self.3.clone())),
                Arc::new(Probe(self.4.clone(), self.1.clone())),
            ]
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
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
    .expect("earlier reply waited for later sibling")
    .unwrap();
}

#[tokio::test]
async fn ancestor_result_hook_must_not_block_foreign_sibling_dispatch() {
    struct Parent(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Read {
        child_returned: Arc<tokio::sync::Notify>,
        hook_release: Arc<tokio::sync::Notify>,
    }
    struct Child(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Probe(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
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
            let a = host.call("independent_read", json!({"id":"a"}));
            let b = async {
                self.0.notified().await;
                host.call("independent_read", json!({"id":"b"})).await
            };
            let c = async {
                self.1.notified().await;
                host.call("independent_read", json!({"id":"c"})).await
            };
            let (a, b, c) = tokio::join!(a, b, c);
            assert!(
                matches!(a, Outcome::Succeeded(_))
                    && matches!(b, Outcome::Succeeded(_))
                    && matches!(c, Outcome::Succeeded(_))
            );
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_read".into(),
                description: "read".into(),
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
            if args["id"] == "a" {
                let result = host.call("independent_child", json!({})).await;
                self.child_returned.notify_one();
                result
            } else if args["id"] == "b" {
                host.call("independent_probe", json!({})).await
            } else {
                self.hook_release.notify_one();
                Outcome::Succeeded("c".into())
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("a".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Probe {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_probe".into(),
                description: "nested read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("b".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    #[async_trait::async_trait]
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "independent"
        }
        fn handles_tool_result(&self) -> bool {
            true
        }
        async fn tool_result(
            &self,
            call: &ToolCall,
            _: &Outcome,
            _: &ToolContext,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<Option<Outcome>> {
            if call.name == "independent_child" {
                self.3.notify_one();
                self.4.notified().await;
            }
            Ok(None)
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone(), self.3.clone())),
                Arc::new(Read {
                    child_returned: self.2.clone(),
                    hook_release: self.4.clone(),
                }),
                Arc::new(Child(self.0.clone(), self.1.clone())),
                Arc::new(Probe(self.1.clone(), self.2.clone())),
            ]
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
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
    .expect("foreign nested route did not run while child was active")
    .unwrap();
    let operations = session.operations();
    let (_, state) = operations
        .iter()
        .find(|(invocation, _)| invocation.call.name == "independent_probe")
        .expect("nested probe was not journaled");
    assert!(matches!(state, OperationState::Finished(Outcome::Succeeded(value)) if value == "b"));
}

#[tokio::test]
async fn progress_precedes_early_ancestor_finish() {
    struct Parent(Arc<tokio::sync::Notify>);
    struct Read {
        child_returned: Arc<tokio::sync::Notify>,
    }
    struct Child(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Probe(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
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
            let a = host.call("independent_read", json!({"id":"a"}));
            let b = async {
                self.0.notified().await;
                host.call("independent_read", json!({"id":"b"})).await
            };
            let (a, b) = tokio::join!(a, b);
            assert!(matches!(a, Outcome::Succeeded(_)) && matches!(b, Outcome::Succeeded(_)));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_read".into(),
                description: "read".into(),
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
            if args["id"] == "a" {
                let result = host.call("independent_child", json!({})).await;
                self.child_returned.notify_one();
                result
            } else {
                host.call("independent_probe", json!({})).await
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_child".into(),
                description: "child".into(),
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
            _: Value,
            _: ToolContext,
            _: Option<&ExtensionState>,
            host: Arc<dyn ToolHost>,
        ) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            host.report_progress(json!({"step":1})).await.unwrap();
            Outcome::Succeeded("a".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Probe {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_probe".into(),
                description: "nested read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("b".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "independent"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone())),
                Arc::new(Read {
                    child_returned: self.2.clone(),
                }),
                Arc::new(Child(self.0.clone(), self.1.clone())),
                Arc::new(Probe(self.1.clone(), self.2.clone())),
            ]
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let mut child_id = None;
    let mut events = Vec::new();
    let mut observer =
        |event: Notification<'_>| match event {
            Notification::Committed(record) => match &record.event {
                Event::ToolPlanned { invocation, .. }
                    if invocation.call.name == "independent_child" =>
                {
                    child_id = Some(invocation.operation_id)
                }
                Event::ToolFinished { operation_id, .. } if Some(*operation_id) == child_id => {
                    events.push("finished")
                }
                _ => {}
            },
            Notification::ToolProgress(LifecycleEvent::ToolExecutionUpdate {
                tool_name, ..
            }) if tool_name == "independent_child" => events.push("progress"),
            _ => {}
        };
    tokio::time::timeout(
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
    .expect("foreign nested route did not run while child was active")
    .unwrap();
    assert_eq!(
        events,
        vec!["progress", "finished"],
        "progress was delivered after completion"
    );
    let operations = session.operations();
    let (_, state) = operations
        .iter()
        .find(|(invocation, _)| invocation.call.name == "independent_probe")
        .expect("nested probe was not journaled");
    assert!(matches!(state, OperationState::Finished(Outcome::Succeeded(value)) if value == "b"));
}

#[tokio::test]
async fn nested_preparation_must_not_block_foreign_sibling_dispatch() {
    struct Parent(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Read {
        child_returned: Arc<tokio::sync::Notify>,
        hook_release: Arc<tokio::sync::Notify>,
    }
    struct Child(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Probe(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
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
            let a = host.call("independent_read", json!({"id":"a"}));
            let b = async {
                self.0.notified().await;
                host.call("independent_read", json!({"id":"b"})).await
            };
            let c = async {
                self.1.notified().await;
                host.call("independent_read", json!({"id":"c"})).await
            };
            let (a, b, c) = tokio::join!(a, b, c);
            assert!(
                matches!(a, Outcome::Succeeded(_))
                    && matches!(b, Outcome::Succeeded(_))
                    && matches!(c, Outcome::Succeeded(_))
            );
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_read".into(),
                description: "read".into(),
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
            if args["id"] == "a" {
                let result = host.call("independent_child", json!({})).await;
                self.child_returned.notify_one();
                result
            } else if args["id"] == "b" {
                host.call("independent_probe", json!({})).await
            } else {
                self.hook_release.notify_one();
                Outcome::Succeeded("c".into())
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("a".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Probe {
        async fn prepare_arguments(&self, args: Value) -> ExtensionResult<Value> {
            self.0.notify_one();
            self.2.notify_one();
            self.3.notified().await;
            Ok(args)
        }
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_probe".into(),
                description: "nested read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.1.notified().await;
            Outcome::Succeeded("b".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "independent"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone(), self.3.clone())),
                Arc::new(Read {
                    child_returned: self.2.clone(),
                    hook_release: self.4.clone(),
                }),
                Arc::new(Child(self.0.clone(), self.1.clone())),
                Arc::new(Probe(
                    self.1.clone(),
                    self.2.clone(),
                    self.3.clone(),
                    self.4.clone(),
                )),
            ]
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
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
    .expect("foreign nested route did not run while child was active")
    .unwrap();
    let operations = session.operations();
    let (_, state) = operations
        .iter()
        .find(|(invocation, _)| invocation.call.name == "independent_probe")
        .expect("nested probe was not journaled");
    assert!(matches!(state, OperationState::Finished(Outcome::Succeeded(value)) if value == "b"));
}

#[tokio::test]
async fn prepared_tool_starts_while_foreign_grandchild_waits() {
    struct Parent(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Read {
        child_returned: Arc<tokio::sync::Notify>,
    }
    struct Child(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Probe(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    struct Release(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
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
            let a = host.call("independent_read", json!({"id":"a"}));
            let b = async {
                self.0.notified().await;
                host.call("independent_read", json!({"id":"b"})).await
            };
            let c = async {
                self.1.notified().await;
                host.call("independent_read", json!({"id":"c"})).await
            };
            let (a, b, c) = tokio::join!(a, b, c);
            assert!(
                matches!(a, Outcome::Succeeded(_))
                    && matches!(b, Outcome::Succeeded(_))
                    && matches!(c, Outcome::Succeeded(_))
            );
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_read".into(),
                description: "read".into(),
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
            if args["id"] == "a" {
                let result = host.call("independent_child", json!({})).await;
                self.child_returned.notify_one();
                result
            } else if args["id"] == "b" {
                host.call("independent_probe", json!({})).await
            } else {
                host.call("foreign_release", json!({})).await
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("a".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Probe {
        async fn prepare_arguments(&self, args: Value) -> ExtensionResult<Value> {
            self.0.notify_one();
            self.2.notify_one();
            self.3.notified().await;
            Ok(args)
        }
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "independent_probe".into(),
                description: "nested read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.4.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("b".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Release {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "foreign_release".into(),
                description: "release".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("c".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "independent"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone(), self.3.clone())),
                Arc::new(Read {
                    child_returned: self.2.clone(),
                }),
                Arc::new(Child(self.0.clone(), self.1.clone())),
                Arc::new(Probe(
                    self.1.clone(),
                    self.2.clone(),
                    self.3.clone(),
                    self.4.clone(),
                    self.5.clone(),
                )),
                Arc::new(Release(self.4.clone(), self.5.clone())),
            ]
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
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
    .expect("foreign nested route did not run while child was active")
    .unwrap();
    let operations = session.operations();
    let (_, state) = operations
        .iter()
        .find(|(invocation, _)| invocation.call.name == "independent_probe")
        .expect("nested probe was not journaled");
    assert!(matches!(state, OperationState::Finished(Outcome::Succeeded(value)) if value == "b"));
}

#[tokio::test]
async fn nested_sibling_can_start_from_first_call_continuation() {
    struct Parent;
    struct Read;
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
            let first = host.call("continuation_read", json!({"id":1})).await;
            assert_eq!(first, Outcome::Succeeded("1".into()));
            let second = host.call("continuation_read", json!({"id":2})).await;
            assert_eq!(second, Outcome::Succeeded("2".into()));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "continuation_read".into(),
                description: "read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
            Outcome::Succeeded(args["id"].to_string())
        }
    }
    struct Fixture;
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "continuation-read"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(Parent), Arc::new(Read)]
        }
    }
    let mut registry = Registry::default();
    registry.register(Arc::new(Fixture)).unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
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
    .expect("continuation was blocked waiting for a later sibling")
    .unwrap();
    let planned: Vec<_> = session
        .records()
        .iter()
        .filter_map(|record| match &record.event {
            Event::ToolPlanned { invocation, .. }
                if invocation.call.name == "continuation_read" =>
            {
                Some(invocation.operation_id)
            }
            _ => None,
        })
        .collect();
    let finished: Vec<_> = session
        .records()
        .iter()
        .filter_map(|record| match &record.event {
            Event::ToolFinished { operation_id, .. } if planned.contains(operation_id) => {
                Some(*operation_id)
            }
            _ => None,
        })
        .collect();
    assert_eq!(planned.len(), 2);
    assert_eq!(finished, planned);
}

#[tokio::test]
async fn uncertain_tool_stops_later_queued_mutation() {
    struct ToolFixture(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Tool for ToolFixture {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "lookup".into(),
                description: "effect".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
            if args["id"] == 1 {
                Outcome::Unknown("uncertain".into())
            } else {
                self.0.fetch_add(1, Ordering::SeqCst);
                Outcome::Succeeded("executed".into())
            }
        }
    }
    struct Fixture(Arc<AtomicUsize>);
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "uncertain-barrier"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(ToolFixture(self.0.clone()))]
        }
    }
    let count = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register(Arc::new(Fixture(count.clone()))).unwrap();
    let model = Script::new(vec![tool_reply(&[r#"{"id":1}"#, r#"{"id":2}"#])]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let result = agent(&model, &registry)
        .run(
            &mut session,
            Some("test".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await;
    assert!(matches!(result, Err(Error::Reconciliation(_))));
    assert_eq!(
        count.load(Ordering::SeqCst),
        0,
        "queued mutation ran after uncertain result"
    );
    assert!(matches!(
        session.operations()[1].1,
        OperationState::Finished(Outcome::Skipped(_))
    ));
}

#[tokio::test]
async fn pending_tool_stops_later_queued_mutation() {
    struct ToolFixture(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Tool for ToolFixture {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "lookup".into(),
                description: "effect".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
            if args["id"] == 1 {
                Outcome::Pending {
                    reference: "job".into(),
                }
            } else {
                self.0.fetch_add(1, Ordering::SeqCst);
                Outcome::Succeeded("executed".into())
            }
        }
    }
    struct Fixture(Arc<AtomicUsize>);
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "uncertain-barrier"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(ToolFixture(self.0.clone()))]
        }
    }
    let count = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register(Arc::new(Fixture(count.clone()))).unwrap();
    let model = Script::new(vec![tool_reply(&[r#"{"id":1}"#, r#"{"id":2}"#])]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let result = agent(&model, &registry)
        .run(
            &mut session,
            Some("test".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await;
    assert!(matches!(result, Err(Error::PendingOperation(_))));
    assert_eq!(
        count.load(Ordering::SeqCst),
        0,
        "queued mutation ran while previous operation was pending"
    );
    assert!(matches!(
        session.operations()[1].1,
        OperationState::Finished(Outcome::Skipped(_))
    ));
}

#[tokio::test]
async fn pending_tool_is_polled_before_later_mutation() {
    struct ToolFixture(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Tool for ToolFixture {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "lookup".into(),
                description: "effect".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn can_poll(&self) -> bool {
            true
        }
        async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
            self.0.store(1, Ordering::SeqCst);
            Some(Outcome::Succeeded("polled".into()))
        }
        async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
            if args["id"] == 1 {
                Outcome::Pending {
                    reference: "job".into(),
                }
            } else {
                assert_eq!(self.0.load(Ordering::SeqCst), 1);
                self.0.fetch_add(1, Ordering::SeqCst);
                Outcome::Succeeded("executed".into())
            }
        }
    }
    struct Fixture(Arc<AtomicUsize>);
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "uncertain-barrier"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(ToolFixture(self.0.clone()))]
        }
    }
    let count = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register(Arc::new(Fixture(count.clone()))).unwrap();
    let model = Script::new(vec![
        tool_reply(&[r#"{"id":1}"#, r#"{"id":2}"#]),
        answer("done"),
    ]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let result = agent(&model, &registry)
        .run(
            &mut session,
            Some("test".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await;
    assert_eq!(result.unwrap(), "done");
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert!(matches!(
        session.operations()[0].1,
        OperationState::Finished(Outcome::Succeeded(_))
    ));
}

#[tokio::test]
async fn uncertain_parallel_read_does_not_cancel_started_sibling() {
    struct Read(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Notify>);
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "lookup".into(),
                description: "read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
            self.0.wait().await;
            if args["id"] == 1 {
                Outcome::Unknown("uncertain".into())
            } else {
                self.1.notified().await;
                Outcome::Succeeded("settled".into())
            }
        }
    }
    struct Fixture(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Notify>);
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "parallel-uncertain"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(Read(self.0.clone(), self.1.clone()))]
        }
    }
    let release = Arc::new(tokio::sync::Notify::new());
    let signal = release.clone();
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Barrier::new(2)),
            release,
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&[r#"{"id":1}"#, r#"{"id":2}"#])]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let unblock = async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        signal.notify_one();
    };
    let runner = async {
        agent(&model, &registry)
            .run(
                &mut session,
                Some("test".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut ignore,
            )
            .await
    };
    let (result, ()) = tokio::join!(runner, unblock);
    assert!(matches!(result, Err(Error::Reconciliation(_))));
    assert!(
        matches!(&session.operations()[1].1, OperationState::Finished(Outcome::Succeeded(value)) if value == "settled")
    );
}

#[tokio::test]
async fn still_pending_poll_is_not_repeated_in_same_turn() {
    struct Pending(Arc<AtomicUsize>);
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
            self.0.fetch_add(1, Ordering::SeqCst);
            Some(Outcome::Pending {
                reference: "job".into(),
            })
        }
    }
    struct Fixture(Arc<AtomicUsize>);
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "still-pending"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(Pending(self.0.clone()))]
        }
    }
    let polls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register(Arc::new(Fixture(polls.clone()))).unwrap();
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
    .expect("pending poll was repeated indefinitely");
    assert!(matches!(result, Err(Error::PendingOperation(_))));
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn uncertain_parallel_read_does_not_strand_sibling_nested_call() {
    struct Read(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Notify>);
    struct Probe;
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "lookup".into(),
                description: "read".into(),
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
                Outcome::Unknown("uncertain".into())
            } else {
                self.1.notified().await;
                host.call("uncertain_probe", json!({})).await
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Probe {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "uncertain_probe".into(),
                description: "probe".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            Outcome::Succeeded("settled".into())
        }
    }
    struct Fixture(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Notify>);
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "parallel-uncertain-nested"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Read(self.0.clone(), self.1.clone())),
                Arc::new(Probe),
            ]
        }
    }
    let release = Arc::new(tokio::sync::Notify::new());
    let signal = release.clone();
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Barrier::new(2)),
            release,
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&[r#"{"id":1}"#, r#"{"id":2}"#])]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let unblock = async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        signal.notify_one();
    };
    let runner = async {
        agent(&model, &registry)
            .run(
                &mut session,
                Some("test".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut ignore,
            )
            .await
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(runner, unblock)
    })
    .await
    .expect("started sibling was stranded");
    assert!(matches!(result, Err(Error::Reconciliation(_))));
    assert!(
        matches!(&session.operations()[1].1, OperationState::Finished(Outcome::Succeeded(value)) if value == "settled")
    );
}

#[tokio::test]
async fn late_sibling_waits_until_pending_operation_is_polled() {
    struct Parent(Arc<tokio::sync::Notify>);
    struct Read(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<AtomicUsize>,
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
            let first = host.call("poll_lane_read", json!({"id":1}));
            let second = async {
                self.0.notified().await;
                host.call("poll_lane_read", json!({"id":2})).await
            };
            let (a, b) = tokio::join!(first, second);
            assert!(matches!(a, Outcome::Succeeded(_)) && matches!(b, Outcome::Succeeded(_)));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "poll_lane_read".into(),
                description: "read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        fn can_poll(&self) -> bool {
            true
        }
        async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
            self.0.notify_one();
            self.1.notified().await;
            self.2.store(1, Ordering::SeqCst);
            Some(Outcome::Succeeded("polled".into()))
        }
        async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
            if args["id"] == 1 {
                Outcome::Pending {
                    reference: "job".into(),
                }
            } else {
                if self.2.load(Ordering::SeqCst) == 0 {
                    self.2.store(2, Ordering::SeqCst);
                }
                Outcome::Succeeded("second".into())
            }
        }
    }
    struct Fixture(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<AtomicUsize>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "poll-lane"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone())),
                Arc::new(Read(self.0.clone(), self.1.clone(), self.2.clone())),
            ]
        }
    }
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let state = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(started, release.clone(), state.clone())))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let unblock = async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(state.load(Ordering::SeqCst), 0);
        release.notify_one();
    };
    let runner = async {
        agent(&model, &registry)
            .run(
                &mut session,
                Some("test".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut ignore,
            )
            .await
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(runner, unblock)
    })
    .await
    .expect("poll lane stalled");
    result.unwrap();
}

#[tokio::test]
async fn nested_write_does_not_block_an_already_running_sibling_branch() {
    struct Parent;
    struct Read(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Notify>);
    struct Write(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
    struct Probe(Arc<tokio::sync::Notify>);
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
            let (a, b) = tokio::join!(
                host.call("branch_read", json!({"id":1})),
                host.call("branch_read", json!({"id":2}))
            );
            assert!(matches!(a, Outcome::Succeeded(_)) && matches!(b, Outcome::Succeeded(_)));
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "branch_read".into(),
                description: "read".into(),
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
                host.call("branch_write", json!({})).await
            } else {
                self.1.notified().await;
                host.call("branch_probe", json!({})).await
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Write {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "branch_write".into(),
                description: "write".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            self.1.notified().await;
            Outcome::Succeeded("write".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Probe {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "branch_probe".into(),
                description: "probe".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.notify_one();
            Outcome::Succeeded("probe".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Barrier>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "branch-barrier"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent),
                Arc::new(Read(self.0.clone(), self.1.clone())),
                Arc::new(Write(self.1.clone(), self.2.clone())),
                Arc::new(Probe(self.2.clone())),
            ]
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Barrier::new(2)),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
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
    .expect("nested write stranded an existing sibling branch")
    .unwrap();
}

#[tokio::test]
async fn nested_uncertain_result_does_not_strand_other_root_branch() {
    struct Read(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Notify>);
    struct Probe;
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "lookup".into(),
                description: "read".into(),
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
                host.call("uncertain_probe", json!({"fail":true})).await
            } else {
                self.1.notified().await;
                host.call("uncertain_probe", json!({})).await
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Probe {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "uncertain_probe".into(),
                description: "probe".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
            if args["fail"] == true {
                Outcome::Unknown("uncertain".into())
            } else {
                Outcome::Succeeded("settled".into())
            }
        }
    }
    struct Fixture(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Notify>);
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "parallel-uncertain-nested"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Read(self.0.clone(), self.1.clone())),
                Arc::new(Probe),
            ]
        }
    }
    let release = Arc::new(tokio::sync::Notify::new());
    let signal = release.clone();
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Barrier::new(2)),
            release,
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&[r#"{"id":1}"#, r#"{"id":2}"#])]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let unblock = async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        signal.notify_one();
    };
    let runner = async {
        agent(&model, &registry)
            .run(
                &mut session,
                Some("test".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut ignore,
            )
            .await
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(runner, unblock)
    })
    .await
    .expect("started sibling was stranded");
    assert!(matches!(result, Err(Error::Reconciliation(_))));
    assert!(
        matches!(&session.operations()[1].1, OperationState::Finished(Outcome::Succeeded(value)) if value == "settled")
    );
}

#[tokio::test]
async fn cancellation_during_preparation_does_not_run_after_tool_hook() {
    struct Read(CancellationToken);
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "lookup".into(),
                description: "read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn prepare_arguments(&self, _: Value) -> ExtensionResult<Value> {
            self.0.cancel();
            std::future::pending().await
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            panic!("cancelled tool started")
        }
    }
    struct Fixture(CancellationToken, Arc<AtomicUsize>);
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "cancel-preparation-hook"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(Read(self.0.clone()))]
        }
        fn after_tool(
            &self,
            _: &str,
            _: &Outcome,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<Option<ExtensionState>> {
            self.1.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }
    let cancel = CancellationToken::new();
    let hook_calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(cancel.clone(), hook_calls.clone())))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"])]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        agent(&model, &registry).run(
            &mut session,
            Some("test".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        ),
    )
    .await
    .expect("preparation cancellation stalled");
    assert!(result.is_err());
    assert_eq!(hook_calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        session.operations()[0].1,
        OperationState::Finished(Outcome::Skipped(_))
    ));
}

#[tokio::test]
async fn nested_cancellation_releases_blocked_progress_hook() {
    struct Parent(Arc<tokio::sync::Notify>);
    struct Child;
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
            context: ToolContext,
            _: Option<&ExtensionState>,
            host: Arc<dyn ToolHost>,
        ) -> Outcome {
            let call = host.call("progress_cancel_child", json!({}));
            let cancel = async {
                self.0.notified().await;
                context.cancellation.cancel();
            };
            let _ = tokio::join!(call, cancel);
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "progress_cancel_child".into(),
                description: "child".into(),
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
            host.report_progress(json!({"step":1})).await.unwrap();
            std::future::pending().await
        }
    }
    struct Fixture(Arc<tokio::sync::Notify>);
    #[async_trait::async_trait]
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "cancel-progress"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(Parent(self.0.clone())), Arc::new(Child)]
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_tool_execution_lifecycle(&self) -> bool {
            true
        }
        fn handles_tool_execution_updates(&self) -> bool {
            true
        }
        async fn lifecycle(
            &self,
            event: &LifecycleEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<()> {
            if matches!(event, LifecycleEvent::ToolExecutionUpdate { tool_name, .. } if tool_name == "progress_cancel_child")
            {
                self.0.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(())
        }
    }
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(Arc::new(tokio::sync::Notify::new()))))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"])]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        agent(&model, &registry).run(
            &mut session,
            Some("test".into()),
            &mut inputs,
            &cancel,
            &mut ignore,
        ),
    )
    .await
    .expect("nested cancellation stranded progress hook");
    assert!(result.is_err());
    assert!(!cancel.is_cancelled());
}

#[tokio::test]
async fn completed_read_opens_slot_for_late_ninth_sibling() {
    struct Parent;
    struct Read {
        barrier: Arc<tokio::sync::Barrier>,
        release: Arc<tokio::sync::Semaphore>,
        active: Arc<AtomicUsize>,
        maximum: Arc<AtomicUsize>,
    }
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
            let first = async {
                let result = host.call("slot_read", json!({"id":0})).await;
                assert!(matches!(result, Outcome::Succeeded(_)));
                host.call("slot_read", json!({"id":8})).await
            };
            let others = futures_util::future::join_all(
                (1..8).map(|id| host.call("slot_read", json!({"id":id}))),
            );
            let (last, rest) = tokio::join!(first, others);
            assert!(
                matches!(last, Outcome::Succeeded(_))
                    && rest
                        .iter()
                        .all(|outcome| matches!(outcome, Outcome::Succeeded(_)))
            );
            Outcome::Succeeded("done".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "slot_read".into(),
                description: "read".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            let id = args["id"].as_u64().unwrap();
            if id < 8 {
                self.barrier.wait().await;
                if id != 0 {
                    self.release.acquire().await.unwrap().forget();
                }
            } else {
                self.release.add_permits(7);
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
            Outcome::Succeeded(id.to_string())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Barrier>,
        Arc<tokio::sync::Semaphore>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "read-slots"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent),
                Arc::new(Read {
                    barrier: self.0.clone(),
                    release: self.1.clone(),
                    active: self.2.clone(),
                    maximum: self.3.clone(),
                }),
            ]
        }
    }
    let maximum = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Barrier::new(8)),
            Arc::new(tokio::sync::Semaphore::new(0)),
            Arc::new(AtomicUsize::new(0)),
            maximum.clone(),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    tokio::time::timeout(
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
    .expect("available read slot was not reused")
    .unwrap();
    assert_eq!(maximum.load(Ordering::SeqCst), 8);
}

#[tokio::test]
async fn pollable_pending_emits_original_tool_end_once() {
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
            Some(Outcome::Succeeded("polled".into()))
        }
    }
    struct Fixture(Arc<Mutex<Vec<Outcome>>>);
    #[async_trait::async_trait]
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "pending-lifecycle"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(Pending)]
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
            if let LifecycleEvent::ToolExecutionEnd { result, .. } = event {
                self.0.lock().unwrap().push(result.clone());
            }
            Ok(())
        }
    }
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(events.clone())))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    agent(&model, &registry)
        .run(
            &mut session,
            Some("test".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut ignore,
        )
        .await
        .unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        vec![Outcome::Pending {
            reference: "job".into()
        }]
    );
}

#[tokio::test]
async fn progress_hook_in_one_branch_does_not_block_another_branch_commit() {
    struct Parent(Arc<tokio::sync::Barrier>);
    struct Child(Arc<tokio::sync::Notify>);
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
            host.call("progress_branch_child", args).await
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "progress_branch_child".into(),
                description: "child".into(),
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
            if args["id"] == 1 {
                host.report_progress(json!({"branch":1})).await.unwrap();
            } else {
                self.0.notified().await;
            }
            Outcome::Succeeded(args["id"].to_string())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Barrier>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    #[async_trait::async_trait]
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "progress-branches"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone())),
                Arc::new(Child(self.1.clone())),
            ]
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_tool_execution_lifecycle(&self) -> bool {
            true
        }
        fn handles_tool_execution_updates(&self) -> bool {
            true
        }
        async fn lifecycle(
            &self,
            event: &LifecycleEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<()> {
            if matches!(event, LifecycleEvent::ToolExecutionUpdate { tool_name, args, .. } if tool_name == "progress_branch_child" && args["id"] == 1)
            {
                self.1.notify_one();
                self.2.notified().await;
            }
            Ok(())
        }
    }
    let released = Arc::new(tokio::sync::Notify::new());
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Barrier::new(2)),
            Arc::new(tokio::sync::Notify::new()),
            released.clone(),
        )))
        .unwrap();
    let model = Script::new(vec![
        tool_reply(&[r#"{"id":1}"#, r#"{"id":2}"#]),
        answer("done"),
    ]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let mut child_id = None;
    let mut observer = |event: Notification<'_>| {
        if let Notification::Committed(record) = event {
            match &record.event {
                Event::ToolPlanned { invocation, .. }
                    if invocation.call.name == "progress_branch_child"
                        && invocation.call.arguments.contains("2") =>
                {
                    child_id = Some(invocation.operation_id)
                }
                Event::ToolFinished { operation_id, .. } if Some(*operation_id) == child_id => {
                    released.notify_one()
                }
                _ => {}
            }
        }
    };
    tokio::time::timeout(
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
    .expect("progress hook blocked another branch")
    .unwrap();
}

async fn uncertain_peer_case(pending: bool) {
    struct Parent(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Notify>, bool);
    struct Write(Arc<AtomicUsize>);
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
                if self.2 {
                    Outcome::Pending {
                        reference: "job".into(),
                    }
                } else {
                    Outcome::Unknown("uncertain".into())
                }
            } else {
                self.1.notified().await;
                host.call("mutation", json!({})).await
            }
        }
    }
    #[async_trait::async_trait]
    impl Tool for Write {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "mutation".into(),
                description: "write".into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.0.fetch_add(1, Ordering::SeqCst);
            Outcome::Succeeded("wrote".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Barrier>,
        Arc<tokio::sync::Notify>,
        Arc<AtomicUsize>,
        bool,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "uncertain-descendant"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone(), self.1.clone(), self.3)),
                Arc::new(Write(self.2.clone())),
            ]
        }
    }
    let release = Arc::new(tokio::sync::Notify::new());
    let writes = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Barrier::new(2)),
            release.clone(),
            writes.clone(),
            pending,
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&[r#"{"id":1}"#, r#"{"id":2}"#])]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let mut observer = |notification: Notification<'_>| {
        if matches!(notification, Notification::Committed(record) if matches!(record.event, Event::ToolFinished { outcome: Outcome::Unknown(_) | Outcome::Pending { .. }, .. }))
        {
            release.notify_one();
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
    .expect("peer stranded");
    assert!(if pending {
        matches!(result, Err(Error::PendingOperation(_)))
    } else {
        matches!(result, Err(Error::Reconciliation(_)))
    });
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    assert!(
        session
            .operations()
            .iter()
            .any(|(call, state)| call.call.name == "mutation"
                && matches!(state, OperationState::Finished(Outcome::Skipped(_))))
    );
}

#[tokio::test]
async fn uncertain_peer_rejects_new_descendant_mutation() {
    uncertain_peer_case(false).await;
}

#[tokio::test]
async fn pending_peer_rejects_new_descendant_mutation() {
    uncertain_peer_case(true).await;
}

#[tokio::test]
async fn writes_in_sibling_branches_do_not_overlap() {
    struct Parent(Arc<tokio::sync::Barrier>);
    struct Write(
        Arc<tokio::sync::Semaphore>,
        Arc<tokio::sync::Notify>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
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
            _: Value,
            _: ToolContext,
            _: Option<&ExtensionState>,
            host: Arc<dyn ToolHost>,
        ) -> Outcome {
            self.0.wait().await;
            host.call("write", json!({})).await
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
            let active = self.2.fetch_add(1, Ordering::SeqCst) + 1;
            self.3.fetch_max(active, Ordering::SeqCst);
            self.0.add_permits(1);
            self.1.notified().await;
            self.2.fetch_sub(1, Ordering::SeqCst);
            Outcome::Succeeded("done".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Barrier>,
        Arc<tokio::sync::Semaphore>,
        Arc<tokio::sync::Notify>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    );
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "cross-branch-writes"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone())),
                Arc::new(Write(
                    self.1.clone(),
                    self.2.clone(),
                    self.3.clone(),
                    self.4.clone(),
                )),
            ]
        }
    }
    let starts = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let maximum = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Barrier::new(2)),
            starts.clone(),
            release.clone(),
            Arc::new(AtomicUsize::new(0)),
            maximum.clone(),
        )))
        .unwrap();
    let model = Script::new(vec![tool_reply(&["{}", "{}"]), answer("done")]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let planned = Arc::new(tokio::sync::Semaphore::new(0));
    let mut observer = |notification: Notification<'_>| {
        if matches!(notification, Notification::Committed(record) if matches!(&record.event, Event::ToolPlanned { invocation, .. } if invocation.call.name == "write"))
        {
            planned.add_permits(1);
        }
    };
    let control = async {
        starts.acquire().await.unwrap().forget();
        planned.acquire_many(2).await.unwrap().forget();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), starts.acquire())
                .await
                .is_err(),
            "second write started while first was active"
        );
        release.notify_one();
        starts.acquire().await.unwrap().forget();
        release.notify_one();
    };
    let runner = async {
        agent(&model, &registry)
            .run(
                &mut session,
                Some("test".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut observer,
            )
            .await
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(runner, control)
    })
    .await
    .expect("writes stalled");
    assert_eq!(result.unwrap(), "done");
    assert_eq!(maximum.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn result_hook_keeps_snapshot_when_foreign_commit_occurs_during_hook() {
    struct Parent(Arc<tokio::sync::Barrier>);
    struct Child;
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
            host.call("child", args).await
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            Outcome::Succeeded("child".into())
        }
    }
    struct Fixture(
        Arc<tokio::sync::Barrier>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );
    #[async_trait::async_trait]
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "result-state"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(Parent(self.0.clone())), Arc::new(Child)]
        }
        fn handles_tool_result(&self) -> bool {
            true
        }
        async fn tool_result(
            &self,
            call: &ToolCall,
            _: &Outcome,
            _: &ToolContext,
            state: Option<&ExtensionState>,
        ) -> ExtensionResult<Option<Outcome>> {
            if call.name != "child" {
                return Ok(None);
            }
            let args: Value = serde_json::from_str(&call.arguments).unwrap();
            if args["id"] == 1 {
                self.1.notified().await;
                Ok(None)
            } else {
                self.1.notify_one();
                self.2.notified().await;
                Ok(Some(Outcome::Succeeded(
                    state.map_or("missing", |_| "updated").into(),
                )))
            }
        }
        fn after_tool(
            &self,
            name: &str,
            outcome: &Outcome,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<Option<ExtensionState>> {
            if name == "child" && matches!(outcome, Outcome::Succeeded(value) if value == "child") {
                Ok(Some(ExtensionState {
                    version: 1,
                    data: json!({"updated":true}),
                }))
            } else {
                Ok(None)
            }
        }
    }
    let release = Arc::new(tokio::sync::Notify::new());
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Barrier::new(2)),
            Arc::new(tokio::sync::Notify::new()),
            release.clone(),
        )))
        .unwrap();
    let model = Script::new(vec![
        tool_reply(&[r#"{"id":1}"#, r#"{"id":2}"#]),
        answer("done"),
    ]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let mut first_id = None;
    let mut observer = |notification: Notification<'_>| {
        if let Notification::Committed(record) = notification {
            match &record.event {
                Event::ToolPlanned { invocation, .. }
                    if invocation.call.name == "child"
                        && invocation.call.arguments.contains("1") =>
                {
                    first_id = Some(invocation.operation_id)
                }
                Event::ToolFinished { operation_id, .. } if Some(*operation_id) == first_id => {
                    release.notify_one()
                }
                _ => {}
            }
        }
    };
    tokio::time::timeout(
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
    .expect("stateful hooks stalled")
    .unwrap();
    let result = session
        .operations()
        .into_iter()
        .find(|(call, _)| call.call.name == "child" && call.call.arguments.contains("2"))
        .unwrap();
    assert!(
        matches!(result.1, OperationState::Finished(Outcome::Succeeded(value)) if value == "missing")
    );
}

#[tokio::test]
async fn result_hooks_see_state_committed_before_they_start() {
    struct Parent(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Notify>);
    struct Child;
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
                let replies = host
                    .call_batch(vec![
                        ("child".into(), json!({"id":1})),
                        ("child".into(), json!({"id":2})),
                    ])
                    .await;
                assert!(
                    replies
                        .iter()
                        .all(|reply| matches!(reply, Outcome::Succeeded(_)))
                );
            } else {
                self.1.notified().await;
                assert!(matches!(
                    host.call("child", json!({"id":3})).await,
                    Outcome::Succeeded(_)
                ));
            }
            Outcome::Succeeded("parent".into())
        }
    }
    #[async_trait::async_trait]
    impl Tool for Child {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "child".into(),
                description: "child".into(),
                parameters: json!({"type":"object"}),
            }
        }
        fn execution(&self) -> ToolExecution {
            ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            Outcome::Succeeded("child".into())
        }
    }
    struct Fixture(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Notify>);
    #[async_trait::async_trait]
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "state-at-start"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![
                Arc::new(Parent(self.0.clone(), self.1.clone())),
                Arc::new(Child),
            ]
        }
        fn handles_tool_result(&self) -> bool {
            true
        }
        async fn tool_result(
            &self,
            call: &ToolCall,
            _: &Outcome,
            _: &ToolContext,
            state: Option<&ExtensionState>,
        ) -> ExtensionResult<Option<Outcome>> {
            if call.name != "child" {
                return Ok(None);
            }
            let args: Value = serde_json::from_str(&call.arguments).unwrap();
            if args["id"] == 1 {
                Ok(None)
            } else {
                Ok(Some(Outcome::Succeeded(
                    state
                        .and_then(|state| state.data["count"].as_u64())
                        .unwrap_or(0)
                        .to_string(),
                )))
            }
        }
        fn after_tool(
            &self,
            name: &str,
            _: &Outcome,
            state: Option<&ExtensionState>,
        ) -> ExtensionResult<Option<ExtensionState>> {
            if name != "child" {
                return Ok(None);
            }
            let count = state
                .and_then(|state| state.data["count"].as_u64())
                .unwrap_or(0);
            Ok(Some(ExtensionState {
                version: 1,
                data: json!({"count":count + 1}),
            }))
        }
    }
    let release = Arc::new(tokio::sync::Notify::new());
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(
            Arc::new(tokio::sync::Barrier::new(2)),
            release.clone(),
        )))
        .unwrap();
    let model = Script::new(vec![
        tool_reply(&[r#"{"id":1}"#, r#"{"id":2}"#]),
        answer("done"),
    ]);
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = mpsc::channel(1);
    let mut first_id = None;
    let mut observer = |notification: Notification<'_>| {
        if let Notification::Committed(record) = notification {
            match &record.event {
                Event::ToolPlanned { invocation, .. }
                    if invocation.call.name == "child"
                        && invocation.call.arguments.contains("1") =>
                {
                    first_id = Some(invocation.operation_id)
                }
                Event::ToolFinished { operation_id, .. } if Some(*operation_id) == first_id => {
                    release.notify_one()
                }
                _ => {}
            }
        }
    };
    tokio::time::timeout(
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
    .expect("state hooks stalled")
    .unwrap();
    let operations = session.operations();
    for id in [2, 3] {
        let (_, state) = operations
            .iter()
            .find(|(call, _)| {
                call.call.name == "child"
                    && serde_json::from_str::<Value>(&call.call.arguments).unwrap()["id"] == id
            })
            .unwrap();
        assert!(
            matches!(state, OperationState::Finished(Outcome::Succeeded(value)) if value.parse::<u64>().unwrap() >= 1),
            "child {id} did not see earlier commit: {state:?}"
        );
    }
    assert_eq!(
        session.extension_states()["state-at-start"].data["count"],
        3
    );
}

async fn failed_group_preparation_case(failed_id: u64) {
    struct Read(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Tool for Read {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "lookup".into(),
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
    struct Fixture(Arc<AtomicUsize>, u64);
    #[async_trait::async_trait]
    impl Extension for Fixture {
        fn id(&self) -> &str {
            "prepare-order"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(Read(self.0.clone()))]
        }
        async fn before_tool(
            &self,
            _: &str,
            args: &Value,
            _: &ToolContext,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<Decision> {
            if args["id"] == self.1 {
                tokio::task::yield_now().await;
                Err("failed policy".into())
            } else {
                Ok(Decision::Allow)
            }
        }
    }
    let started = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(Fixture(started.clone(), failed_id)))
        .unwrap();
    let model = Script::new(vec![tool_reply(&[r#"{"id":1}"#, r#"{"id":2}"#])]);
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
    .expect("consecutive unstarted operations stalled");
    assert!(result.is_err());
    assert_eq!(started.load(Ordering::SeqCst), 0);
    let operations = session.operations();
    assert_eq!(operations.len(), 2);
    assert!(
        operations
            .iter()
            .all(|(_, state)| matches!(state, OperationState::Finished(Outcome::Skipped(_))))
    );
}

#[tokio::test]
async fn failed_earlier_pre_tool_hook_prevents_later_read_start() {
    failed_group_preparation_case(1).await;
}

#[tokio::test]
async fn failed_later_pre_tool_hook_prevents_earlier_group_dispatch() {
    failed_group_preparation_case(2).await;
}

#[path = "rereview.rs"]
mod rereview;
