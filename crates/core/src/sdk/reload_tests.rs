use super::*;
use crate::{LifecycleEvent, Message, ModelEvent, ModelStream, RegistryUpdate, Tool, ToolSpec};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

struct ReloadModel(&'static str);

#[async_trait::async_trait]
impl Model for ReloadModel {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let system = request
            .messages
            .iter()
            .find_map(|message| match message {
                Message::System { content } => Some(content.as_str()),
                _ => None,
            })
            .unwrap_or_default();
        let (text, calls) = match (request.messages.last(), request.tools.first()) {
            (Some(Message::Tool { content, .. }), _) => {
                (format!("{}:{system}:{content}", self.0), vec![])
            }
            (_, Some(tool)) => (
                String::new(),
                vec![crate::ToolCall {
                    id: Uuid::new_v4().to_string(),
                    name: tool.name.clone(),
                    arguments: "{}".into(),
                }],
            ),
            (_, None) => (format!("{}:{system}:no tools", self.0), vec![]),
        };
        Ok(Box::pin(futures_util::stream::iter([
            Ok(ModelEvent::Text(text)),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])))
    }
}

struct ReloadTool(&'static str, &'static str);

#[async_trait::async_trait]
impl Tool for ReloadTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.0.into(),
            description: "Reload fixture".into(),
            parameters: serde_json::json!({"type":"object"}),
        }
    }

    async fn invoke(&self, _: serde_json::Value, _: crate::ToolContext) -> crate::Outcome {
        crate::Outcome::Succeeded(self.1.into())
    }
}

type LifecycleLog = Arc<Mutex<Vec<String>>>;

#[derive(Default)]
struct Pause {
    entered: Notify,
    release: Notify,
    attempted: AtomicBool,
}

impl Pause {
    async fn once(&self) {
        if !self.attempted.swap(true, Ordering::Relaxed) {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

struct ReloadExtension {
    id: &'static str,
    label: &'static str,
    tool: Option<ReloadTool>,
    log: LifecycleLog,
    pause_shutdown: Option<Arc<Pause>>,
    pause_start: Option<Arc<Pause>>,
}

#[async_trait::async_trait]
impl Extension for ReloadExtension {
    fn id(&self) -> &str {
        self.id
    }

    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        self.tool
            .as_ref()
            .map(|tool| Arc::new(ReloadTool(tool.0, tool.1)) as Arc<dyn Tool>)
            .into_iter()
            .collect()
    }

    fn handles_input(&self) -> bool {
        true
    }

    fn handles_lifecycle(&self) -> bool {
        true
    }

    fn handles_session_lifecycle(&self) -> bool {
        true
    }

    async fn lifecycle(
        &self,
        event: &LifecycleEvent,
        _: Option<&crate::ExtensionState>,
    ) -> crate::ExtensionResult<()> {
        let detail = match event {
            LifecycleEvent::SessionStart { reason, .. } => format!("start:{reason:?}"),
            LifecycleEvent::SessionShutdown { reason, .. } => format!("end:{reason:?}"),
            _ => return Ok(()),
        };
        self.log
            .lock()
            .unwrap()
            .push(format!("{}:{detail}", self.label));
        let pause = match event {
            LifecycleEvent::SessionStart { .. } => &self.pause_start,
            LifecycleEvent::SessionShutdown {
                reason: crate::SessionShutdownReason::Reload,
                ..
            } => &self.pause_shutdown,
            _ => return Ok(()),
        };
        if let Some(pause) = pause {
            pause.once().await;
        }
        Ok(())
    }

    async fn shutdown(&self) -> crate::ExtensionResult<()> {
        self.log
            .lock()
            .unwrap()
            .push(format!("{}:dispose", self.label));
        Ok(())
    }
}

fn extension(
    log: &LifecycleLog,
    id: &'static str,
    label: &'static str,
    tool: Option<ReloadTool>,
) -> Arc<dyn Extension> {
    Arc::new(ReloadExtension {
        id,
        label,
        tool,
        log: log.clone(),
        pause_shutdown: None,
        pause_start: None,
    })
}

fn registry(extensions: Vec<Arc<dyn Extension>>) -> Registry {
    Registry::compose(extensions, &Default::default()).unwrap()
}

#[tokio::test]
async fn sdk_reload_preserves_or_replaces_structured_prompt_with_its_text() {
    struct RemoveRules(&'static str);

    #[async_trait::async_trait]
    impl Extension for RemoveRules {
        fn id(&self) -> &str {
            "remove-rules"
        }

        fn handles_before_agent_start(&self) -> bool {
            true
        }

        async fn before_agent_start_with_prompt_options(
            &self,
            _: &crate::BeforeAgentStartEvent,
            options: &crate::BeforeAgentStartPromptOptions,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<crate::BeforeAgentStartPromptResult> {
            assert_eq!(
                options.sections.get("rules").map(String::as_str),
                Some(self.0)
            );
            Ok(crate::BeforeAgentStartPromptResult {
                system_prompt_options: crate::BeforeAgentStartPromptChanges {
                    sections: [("rules".into(), None)].into_iter().collect(),
                },
                ..Default::default()
            })
        }
    }

    let mut original = Registry::default();
    let original_prompt = original.compose_system_prompt(
        "original".into(),
        vec![("rules".into(), "original rules".into())],
    );
    let mut session = SessionBuilder::new(Arc::new(ReloadModel("fixture")), "fixture")
        .registry(original)
        .system_prompt(original_prompt.clone())
        .in_memory()
        .unwrap();
    session.start().await.unwrap();

    session
        .reload(&RegistryUpdate {
            registry: registry(vec![Arc::new(RemoveRules("original rules"))]),
            system_prompt: None,
        })
        .await
        .unwrap();
    assert_eq!(
        session.prompt("use retained prompt").await.unwrap(),
        "fixture:original:no tools"
    );
    assert_eq!(session.configured_system_prompt(), original_prompt);

    let mut replacement = registry(vec![Arc::new(RemoveRules("replacement rules"))]);
    let replacement_prompt = replacement.compose_system_prompt(
        "replacement".into(),
        vec![("rules".into(), "replacement rules".into())],
    );
    session
        .reload(&RegistryUpdate {
            registry: replacement,
            system_prompt: Some(replacement_prompt.clone()),
        })
        .await
        .unwrap();
    assert_eq!(
        session.prompt("use replacement prompt").await.unwrap(),
        "fixture:replacement:no tools"
    );
    assert_eq!(session.configured_system_prompt(), replacement_prompt);
}

#[tokio::test]
async fn sdk_reload_prompts_use_new_tools_and_prompt_with_existing_bindings() {
    let log = LifecycleLog::default();
    let kept = extension(&log, "kept", "kept", None);
    let mut original = registry(vec![
        extension(&log, "worker", "old", Some(ReloadTool("job", "old tool"))),
        kept.clone(),
    ]);
    original.set_active_tools(["job".into()]).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.jsonl");
    let primary = Arc::new(ReloadModel("primary"));
    let mut session = SessionBuilder::new(primary.clone(), "same")
        .provider("alpha")
        .unwrap()
        .provider_branch_model("beta", "same", Arc::new(ReloadModel("named beta")))
        .unwrap()
        .registry(original)
        .system_prompt("before")
        .open(&path)
        .unwrap();
    let id = session.journal().unwrap().session_id();
    session
        .controls()
        .set_auto_compaction_enabled(false)
        .unwrap();
    session
        .set_queue_mode(InputKind::FollowUp, crate::QueueMode::OneAtATime)
        .unwrap();
    assert_eq!(
        session.prompt("first").await.unwrap(),
        "primary:before:old tool"
    );
    let available = [
        ModelCycleEntry {
            provider: "alpha",
            model: "same",
            thinking: None,
        },
        ModelCycleEntry {
            provider: "beta",
            model: "same",
            thinking: Some("high"),
        },
    ];
    session
        .cycle_model(
            ModelCycleDirection::Forward,
            &available,
            &[],
            None,
            |provider, model, thinking| {
                assert_eq!((provider, model, thinking), ("beta", "same", "high"));
                Ok((Arc::new(ReloadModel("historical")), thinking.into()))
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        session.prompt("historical turn").await.unwrap(),
        "historical:before:old tool"
    );
    let fork_at = session.journal().unwrap().head();
    session
        .set_provider_model(primary, "alpha", "same")
        .unwrap();
    let head = session.journal().unwrap().head();
    let update = RegistryUpdate {
        registry: registry(vec![
            extension(&log, "worker", "new", Some(ReloadTool("job", "new tool"))),
            extension(
                &log,
                "extra",
                "extra",
                Some(ReloadTool("extra", "inactive tool")),
            ),
            kept,
        ]),
        system_prompt: Some("after".into()),
    };
    session.reload(&update).await.unwrap();
    drop(update);
    assert_eq!(session.journal().unwrap().session_id(), id);
    assert_eq!(session.journal().unwrap().head(), head);
    assert_eq!(
        session.get_last_assistant_text().unwrap().as_deref(),
        Some("historical:before:old tool")
    );
    assert_eq!(session.model_provider().unwrap(), Some("alpha"));
    assert!(!session.auto_compaction_enabled());
    assert_eq!(
        session.queue_mode(InputKind::FollowUp).unwrap(),
        crate::QueueMode::OneAtATime
    );
    assert_eq!(session.tools().len(), 2);
    assert_eq!(
        session.prompt("second").await.unwrap(),
        "primary:after:new tool"
    );
    assert_eq!(
        *log.lock().unwrap(),
        [
            "old:start:Startup",
            "kept:start:Startup",
            "old:end:Reload",
            "kept:end:Reload",
            "old:dispose",
            "new:start:Reload",
            "extra:start:Reload",
            "kept:start:Reload",
        ]
    );
    let mut fork = session.fork_at(fork_at).unwrap();
    assert_eq!(fork.model_provider().unwrap(), Some("beta"));
    assert_eq!(
        fork.journal().unwrap().model_selection(),
        Some(("same".into(), Some("high".into())))
    );
    assert_eq!(
        fork.get_last_assistant_text().unwrap().as_deref(),
        Some("historical:before:old tool")
    );
    assert_eq!(
        fork.prompt("fork").await.unwrap(),
        "historical:after:new tool"
    );
    assert_eq!(
        fork.get_last_assistant_text().unwrap().as_deref(),
        Some("historical:after:new tool")
    );
    fork.close().await.unwrap();
    assert!(
        !log.lock()
            .unwrap()
            .iter()
            .any(|entry| entry == "kept:dispose")
    );
    session.close().await.unwrap();
    assert_eq!(
        log.lock()
            .unwrap()
            .iter()
            .filter(|entry| *entry == "kept:dispose")
            .count(),
        1
    );
}

#[tokio::test]
async fn sdk_reload_rejects_busy_forks_and_recovery_then_retries_same_candidate() {
    let log = LifecycleLog::default();
    let mut session = SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
        .registry(registry(vec![extension(&log, "old", "old", None)]))
        .system_prompt("old")
        .in_memory()
        .unwrap();
    session.prompt("start").await.unwrap();
    let update = RegistryUpdate {
        registry: registry(vec![extension(&log, "new", "new", None)]),
        system_prompt: Some("new".into()),
    };
    let journal = session.session.clone();
    let busy = journal.lock().await;
    assert!(
        session
            .reload(&update)
            .await
            .unwrap_err()
            .to_string()
            .contains("in use")
    );
    drop(busy);
    let fork_at = session.journal().unwrap().head();
    let mut fork = session.fork_at(fork_at).unwrap();
    assert!(
        session
            .reload(&update)
            .await
            .unwrap_err()
            .to_string()
            .contains("sharing")
    );
    assert_eq!(
        fork.prompt("still old").await.unwrap(),
        "primary:old:no tools"
    );
    fork.close().await.unwrap();

    let run = Uuid::new_v4();
    let append_state = |state| {
        let mut journal = journal.try_lock().unwrap();
        journal.set_run_id(Some(run));
        journal
            .append(crate::Event::Run {
                state,
                initial: None,
            })
            .unwrap();
        journal.set_run_id(None);
    };
    append_state(crate::RunState::Running);
    assert!(
        session
            .reload(&update)
            .await
            .unwrap_err()
            .to_string()
            .contains("resume pending")
    );
    assert_eq!(session.configured_system_prompt(), "old");
    assert!(
        !log.lock()
            .unwrap()
            .iter()
            .any(|entry| entry.contains("Reload") || entry.contains("dispose"))
    );
    append_state(crate::RunState::Cancelled);
    assert_eq!(
        session.prompt("still usable").await.unwrap(),
        "primary:old:no tools"
    );
    session.reload(&update).await.unwrap();
    drop(update);
    assert_eq!(
        session.prompt("retry worked").await.unwrap(),
        "primary:new:no tools"
    );
    session.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn sdk_reload_finishes_old_shutdown_after_abort_or_drop() {
    for drop_reload in [false, true] {
        let log = LifecycleLog::default();
        let pause = Arc::new(Pause::default());
        let mut session = SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
            .registry(registry(vec![
                extension(&log, "first", "first", None),
                Arc::new(ReloadExtension {
                    id: "paused",
                    label: "paused",
                    tool: None,
                    log: log.clone(),
                    pause_shutdown: Some(pause.clone()),
                    pause_start: None,
                }),
                extension(&log, "last", "last", None),
            ]))
            .system_prompt("old")
            .in_memory()
            .unwrap();
        session.start().await.unwrap();
        let controls = session.controls();
        let update = RegistryUpdate {
            registry: registry(vec![extension(&log, "new", "new", None)]),
            system_prompt: Some("new".into()),
        };
        let mut reload = Box::pin(session.reload(&update));
        tokio::select! {
            _ = pause.entered.notified() => {}
            result = &mut reload => panic!("reload finished before shutdown paused: {result:?}"),
        }
        assert_eq!(
            &log.lock().unwrap()[3..],
            ["first:end:Reload", "paused:end:Reload"]
        );
        if drop_reload {
            drop(reload);
        } else {
            controls.abort();
            pause.release.notify_one();
            assert!(matches!(reload.await, Err(Error::Cancelled)));
        }
        pause.release.notify_one();
        assert_eq!(
            session.prompt("after cancellation").await.unwrap(),
            "primary:new:no tools"
        );
        session.close().await.unwrap();
        assert_eq!(
            &log.lock().unwrap()[3..],
            [
                "first:end:Reload",
                "paused:end:Reload",
                "last:end:Reload",
                "last:dispose",
                "paused:dispose",
                "first:dispose",
                "new:start:Reload",
                "new:end:Quit",
                "new:dispose",
            ]
        );
    }
}

#[tokio::test(start_paused = true)]
async fn sdk_reload_abort_while_waiting_for_startup_admission() {
    let log = LifecycleLog::default();
    let pause = Arc::new(Pause::default());
    let mut session = SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
        .extension(Arc::new(ReloadExtension {
            id: "old",
            label: "old",
            tool: None,
            log: log.clone(),
            pause_shutdown: Some(pause.clone()),
            pause_start: None,
        }))
        .unwrap()
        .in_memory()
        .unwrap();
    session.start().await.unwrap();
    let controls = session.controls();
    let update = RegistryUpdate {
        registry: registry(vec![extension(&log, "new", "new", None)]),
        system_prompt: None,
    };
    let mut reload = Box::pin(session.reload(&update));
    tokio::select! {
        _ = pause.entered.notified() => {}
        result = &mut reload => panic!("reload finished before shutdown paused: {result:?}"),
    }

    // Queue a permit ahead of the reload's second admission.
    let mut competing = Box::pin(controls.input_order.acquire());
    assert!(futures_util::poll!(&mut competing).is_pending());
    pause.release.notify_one();
    let permit = competing.await.unwrap();
    assert!(futures_util::poll!(&mut reload).is_pending());
    assert_eq!(controls.input_order.state.lock().unwrap().waiting.len(), 1);
    assert_eq!(
        *log.lock().unwrap(),
        ["old:start:Startup", "old:end:Reload", "old:dispose"]
    );

    controls.abort();
    assert!(matches!(
        futures_util::poll!(&mut reload),
        std::task::Poll::Ready(Err(Error::Cancelled))
    ));
    drop(reload);
    drop(permit);

    session.start().await.unwrap();
    session.close().await.unwrap();
    assert_eq!(
        &log.lock().unwrap()[3..],
        ["new:start:Reload", "new:end:Quit", "new:dispose"]
    );
}

#[tokio::test(start_paused = true)]
async fn sdk_reload_detached_shutdown_survives_dropping_session() {
    struct ShutdownEntry(Arc<Pause>);

    #[async_trait::async_trait]
    impl Extension for ShutdownEntry {
        fn id(&self) -> &str {
            "shutdown-entry"
        }

        fn handles_lifecycle(&self) -> bool {
            true
        }

        fn handles_session_lifecycle(&self) -> bool {
            true
        }

        async fn lifecycle_with_host(
            &self,
            event: &LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            if matches!(
                event,
                LifecycleEvent::SessionShutdown {
                    reason: crate::SessionShutdownReason::Reload,
                    ..
                }
            ) {
                self.0.once().await;
                assert!(host.supports_custom_entries());
                host.append_entry(crate::CustomEntry {
                    custom_type: "detached-shutdown".into(),
                    data: None,
                })
                .await?;
            }
            Ok(())
        }
    }

    let log = LifecycleLog::default();
    let pause = Arc::new(Pause::default());
    let mut session = SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
        .registry(registry(vec![
            Arc::new(ShutdownEntry(pause.clone())),
            extension(&log, "last", "last", None),
        ]))
        .in_memory()
        .unwrap();
    session.start().await.unwrap();
    let controls = session.controls();
    let journal = session.session.clone();
    let update = RegistryUpdate {
        registry: registry(vec![extension(&log, "new", "new", None)]),
        system_prompt: None,
    };
    let mut reload = Box::pin(session.reload(&update));
    tokio::select! {
        _ = pause.entered.notified() => {}
        result = &mut reload => panic!("reload finished before shutdown paused: {result:?}"),
    }
    drop(reload);
    drop(session);
    pause.release.notify_one();

    // The accepted shutdown owns admission until its callbacks and disposal finish.
    let _permit = controls.input_order.acquire().await.unwrap();
    assert!(journal.lock().await.records().iter().any(|record| {
        matches!(&record.event, crate::Event::CustomEntry { entry } if entry.custom_type == "detached-shutdown")
    }));
    assert_eq!(
        *log.lock().unwrap(),
        ["last:start:Startup", "last:end:Reload", "last:dispose"]
    );
}

#[tokio::test(start_paused = true)]
async fn sdk_reload_startup_retries_only_unfinished_hooks() {
    for (reload, cloned) in [(false, false), (false, true), (true, false)] {
        let log = LifecycleLog::default();
        let pause = Arc::new(Pause::default());
        let candidate = registry(vec![
            extension(&log, "first", "first", None),
            Arc::new(ReloadExtension {
                id: "paused",
                label: "paused",
                tool: None,
                log: log.clone(),
                pause_shutdown: None,
                pause_start: Some(pause.clone()),
            }),
            extension(&log, "last", "last", None),
        ]);
        let mut session = SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
            .registry(if reload {
                Registry::default()
            } else {
                candidate.clone()
            })
            .in_memory()
            .unwrap();
        let control = session.controls();
        let mut startup = Box::pin(async {
            if reload {
                session.start().await?;
                session
                    .reload(&RegistryUpdate {
                        registry: candidate,
                        system_prompt: None,
                    })
                    .await
            } else if cloned {
                control.steer("interrupted").await.map(|_| ())
            } else {
                session.start().await
            }
        });
        tokio::select! {
            _ = pause.entered.notified() => {}
            result = &mut startup => panic!("startup finished before the hook paused: {result:?}"),
        }
        if cloned {
            control.abort();
            assert!(matches!(startup.await, Err(Error::Cancelled)));
        } else {
            drop(startup);
        }
        if cloned {
            session.start().await.unwrap();
        } else {
            control.steer("retry").await.unwrap();
        }
        session.start().await.unwrap();
        let reason = if reload { "Reload" } else { "Startup" };
        assert_eq!(
            *log.lock().unwrap(),
            [
                format!("first:start:{reason}"),
                format!("paused:start:{reason}"),
                format!("paused:start:{reason}"),
                format!("last:start:{reason}"),
            ]
        );
        session.close().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn sdk_reload_close_rejoins_teardown_after_waiter_is_dropped() {
    for (pause_quit, reenter_pending) in [(true, false), (false, false), (false, true)] {
        let log = LifecycleLog::default();
        let pause = Arc::new(Pause::default());
        let mut session = SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
            .extension(Arc::new(CloseProbe {
                log: log.clone(),
                quit: pause_quit.then(|| pause.clone()),
                dispose: (!pause_quit).then(|| pause.clone()),
                rename: None,
            }))
            .unwrap()
            .in_memory()
            .unwrap();
        session.start().await.unwrap();
        let control = session.controls();
        let mut events = session.subscribe();
        let mut close = Box::pin(session.close());
        tokio::select! {
            _ = pause.entered.notified() => {}
            result = &mut close => panic!("close finished before teardown paused: {result:?}"),
        }
        drop(close);
        assert!(control.steer("after admission closed").await.is_err());
        assert!(matches!(
            events.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        assert!(matches!(
            session.subscribe().try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
        if reenter_pending {
            let mut close = Box::pin(session.close());
            assert!(futures_util::poll!(&mut close).is_pending());
            pause.release.notify_one();
            close.await.unwrap();
        } else {
            pause.release.notify_one();
        }
        assert!(matches!(events.recv().await, Ok(SessionEvent::Closed)));
        assert_eq!(*log.lock().unwrap(), ["quit", "dispose"]);
        assert!(matches!(
            events.try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
        session.close().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn sdk_reload_close_times_out_connection_after_waiter_is_dropped() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("close.jsonl");
    let log = LifecycleLog::default();
    let pause = Arc::new(Pause::default());
    let timeout = std::time::Duration::from_secs(1);
    let mut session = SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
        .limits(Limits {
            tool_timeout: timeout,
            ..Limits::default()
        })
        .extension(extension(&log, "next", "next", None))
        .unwrap()
        .extension(Arc::new(CloseProbe {
            log: log.clone(),
            quit: None,
            dispose: Some(pause.clone()),
            rename: None,
        }))
        .unwrap()
        .open(&path)
        .unwrap();
    session.start().await.unwrap();

    let mut events = session.subscribe();
    let mut close = Box::pin(session.close());
    tokio::select! {
        _ = pause.entered.notified() => {}
        result = &mut close => panic!("close finished before connection teardown: {result:?}"),
    }
    drop(close);
    let reopened = Session::open(&path).expect("close released the journal writer after Quit");
    assert_eq!(reopened.head(), session.journal().unwrap().head());
    drop(reopened);
    assert!(matches!(
        events.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));

    tokio::time::advance(timeout).await;
    assert!(matches!(events.recv().await, Ok(SessionEvent::Closed)));
    assert!(matches!(
        events.recv().await,
        Err(broadcast::error::RecvError::Closed)
    ));
    assert!(
        matches!(session.close().await, Err(Error::Extension { id, message })
        if id == "close-probe" && message == "extension shutdown timed out")
    );
    assert!(
        log.lock()
            .unwrap()
            .iter()
            .any(|event| event == "next:dispose")
    );
}

struct CloseProbe {
    log: LifecycleLog,
    quit: Option<Arc<Pause>>,
    dispose: Option<Arc<Pause>>,
    rename: Option<Arc<Pause>>,
}

#[async_trait::async_trait]
impl Extension for CloseProbe {
    fn id(&self) -> &str {
        "close-probe"
    }
    fn handles_lifecycle(&self) -> bool {
        true
    }
    fn handles_session_lifecycle(&self) -> bool {
        true
    }
    async fn lifecycle(
        &self,
        event: &LifecycleEvent,
        _: Option<&crate::ExtensionState>,
    ) -> crate::ExtensionResult<()> {
        match event {
            LifecycleEvent::SessionInfoChanged { .. } => {
                self.log.lock().unwrap().push("rename entered".into());
                if let Some(pause) = &self.rename {
                    pause.once().await;
                }
                self.log.lock().unwrap().push("rename finished".into());
            }
            LifecycleEvent::SessionShutdown {
                reason: crate::SessionShutdownReason::Quit,
                ..
            } => {
                self.log.lock().unwrap().push("quit".into());
                if let Some(pause) = &self.quit {
                    pause.once().await;
                }
            }
            _ => {}
        }
        Ok(())
    }
    async fn shutdown(&self) -> crate::ExtensionResult<()> {
        if let Some(pause) = &self.dispose {
            pause.once().await;
        }
        self.log.lock().unwrap().push("dispose".into());
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn sdk_reload_close_drains_acknowledged_rename_before_quit() {
    struct PausedModel(Arc<Pause>);

    #[async_trait::async_trait]
    impl Model for PausedModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            use futures_util::StreamExt;
            let pause = self.0.clone();
            Ok(Box::pin(
                futures_util::stream::once(async move {
                    pause.once().await;
                    Ok(ModelEvent::Text("done".into()))
                })
                .chain(futures_util::stream::iter([Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                })])),
            ))
        }
    }

    let log = LifecycleLog::default();
    let model = Arc::new(Pause::default());
    let rename = Arc::new(Pause::default());
    let mut session = SessionBuilder::new(Arc::new(PausedModel(model.clone())), "model")
        .extension(Arc::new(CloseProbe {
            log: log.clone(),
            quit: None,
            dispose: None,
            rename: Some(rename.clone()),
        }))
        .unwrap()
        .in_memory()
        .unwrap();
    let control = session.controls();
    let mut prompt = Box::pin(session.prompt("run"));
    tokio::select! {
        _ = model.entered.notified() => {}
        result = &mut prompt => panic!("prompt finished before the model paused: {result:?}"),
    }
    let rename_control = control.clone();
    let rename_task =
        tokio::spawn(async move { rename_control.set_session_name("new name").await });
    tokio::select! {
        _ = rename.entered.notified() => {}
        result = &mut prompt => panic!("prompt finished before rename paused: {result:?}"),
    }
    model.release.notify_one();
    assert_eq!(prompt.await.unwrap(), "done");
    assert_eq!(session.journal().unwrap().name(), Some("new name"));

    let mut close = Box::pin(session.close());
    tokio::select! {
        _ = control.input.closed() => {}
        result = &mut close => panic!("close finished before rename drained: {result:?}"),
    }
    assert!(futures_util::poll!(&mut close).is_pending());
    assert_eq!(*log.lock().unwrap(), ["rename entered"]);
    assert!(control.set_session_name("too late").await.is_err());
    rename.release.notify_one();
    rename_task.await.unwrap().unwrap();
    close.await.unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        ["rename entered", "rename finished", "quit", "dispose"]
    );
}

#[tokio::test(start_paused = true)]
async fn sdk_reload_reports_startup_failure_before_later_hook_is_cancelled() {
    struct FailedStart(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl Extension for FailedStart {
        fn id(&self) -> &str {
            "failed-start"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_session_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle(
            &self,
            event: &LifecycleEvent,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<()> {
            if matches!(event, LifecycleEvent::SessionStart { .. }) {
                self.0.fetch_add(1, Ordering::Relaxed);
                return Err("startup fixture failed".into());
            }
            Ok(())
        }
    }

    for drop_waiter in [false, true] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let log = LifecycleLog::default();
        let pause = Arc::new(Pause::default());
        let mut session = SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
            .in_memory()
            .unwrap();
        session.start().await.unwrap();
        let control = session.controls();
        let mut events = session.subscribe();
        let update = RegistryUpdate {
            registry: registry(vec![
                Arc::new(FailedStart(attempts.clone())),
                Arc::new(ReloadExtension {
                    id: "paused",
                    label: "paused",
                    tool: None,
                    log: log.clone(),
                    pause_shutdown: None,
                    pause_start: Some(pause.clone()),
                }),
            ]),
            system_prompt: None,
        };
        let mut reload = Box::pin(session.reload(&update));
        tokio::select! {
            _ = pause.entered.notified() => {}
            result = &mut reload => panic!("reload finished before startup paused: {result:?}"),
        }
        assert!(matches!(
            events.try_recv(),
            Ok(SessionEvent::ExtensionLifecycleError { extension, event: "session_start", message })
                if extension == "failed-start" && message == "startup fixture failed"
        ));
        if drop_waiter {
            drop(reload);
        } else {
            control.abort();
            assert!(matches!(reload.await, Err(Error::Cancelled)));
        }
        session.start().await.unwrap();
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
        assert!(matches!(
            events.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        session.close().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn sdk_reload_close_quits_completed_startup_hooks_on_a_fork() {
    let log = LifecycleLog::default();
    let pause = Arc::new(Pause::default());
    let mut parent = SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
        .registry(registry(vec![
            extension(&log, "first", "first", None),
            Arc::new(ReloadExtension {
                id: "paused",
                label: "paused",
                tool: None,
                log: log.clone(),
                pause_shutdown: None,
                pause_start: Some(pause.clone()),
            }),
            extension(&log, "last", "last", None),
        ]))
        .in_memory()
        .unwrap();
    parent.set_name("parent").unwrap();
    let head = parent.journal().unwrap().head();
    let mut fork = parent.fork_at(head).unwrap();
    let control = fork.controls();
    let mut startup = tokio::spawn(async move { control.steer("start fork").await });
    tokio::select! {
        _ = pause.entered.notified() => {}
        result = &mut startup => panic!("fork startup finished before the hook paused: {result:?}"),
    }
    fork.close().await.unwrap();
    assert!(startup.await.unwrap().is_err());
    assert_eq!(
        *log.lock().unwrap(),
        ["first:start:Fork", "paused:start:Fork", "first:end:Quit"]
    );

    parent.start().await.unwrap();
    parent.close().await.unwrap();
    assert_eq!(
        &log.lock().unwrap()[3..],
        [
            "first:start:Startup",
            "paused:start:Startup",
            "last:start:Startup",
            "first:end:Quit",
            "paused:end:Quit",
            "last:end:Quit",
            "last:dispose",
            "paused:dispose",
            "first:dispose",
        ]
    );
}

#[tokio::test]
async fn sdk_reload_candidate_instances_have_one_session_owner() {
    let log = LifecycleLog::default();
    let mut first = SessionBuilder::new(Arc::new(ReloadModel("first")), "model")
        .in_memory()
        .unwrap();
    let mut second = SessionBuilder::new(Arc::new(ReloadModel("second")), "model")
        .registry(registry(vec![extension(&log, "other", "other", None)]))
        .in_memory()
        .unwrap();
    first.start().await.unwrap();
    second.start().await.unwrap();
    let update = RegistryUpdate {
        registry: registry(vec![extension(
            &log,
            "shared",
            "shared",
            Some(ReloadTool("job", "tool")),
        )]),
        system_prompt: Some("installed".into()),
    };
    first.reload(&update).await.unwrap();
    let error = second.reload(&update).await.unwrap_err();
    assert!(error.to_string().contains("belongs to another SDK session"));
    assert!(
        second
            .register_extension(update.registry.extensions[0].clone())
            .unwrap_err()
            .to_string()
            .contains("belongs to another SDK session")
    );
    second.close().await.unwrap();
    assert_eq!(
        first.prompt("owner still works").await.unwrap(),
        "first:installed:tool"
    );
    assert_eq!(
        *log.lock().unwrap(),
        [
            "other:start:Startup",
            "shared:start:Reload",
            "other:end:Quit",
            "other:dispose"
        ]
    );
    first.close().await.unwrap();
    let mut next = SessionBuilder::new(Arc::new(ReloadModel("next")), "model")
        .in_memory()
        .unwrap();
    assert!(
        next.reload(&update)
            .await
            .unwrap_err()
            .to_string()
            .contains("belongs to another SDK session")
    );
    next.close().await.unwrap();
    assert_eq!(
        &log.lock().unwrap()[4..],
        ["shared:end:Quit", "shared:dispose"]
    );
}

#[tokio::test]
async fn sdk_reload_keeps_legacy_shared_instances_usable_without_promoting_them() {
    let log = LifecycleLog::default();
    let shared = extension(&log, "shared", "shared", None);
    let mut first = SessionBuilder::new(Arc::new(ReloadModel("first")), "model")
        .system_prompt("original")
        .registry(registry(vec![shared.clone()]))
        .in_memory()
        .unwrap();
    let mut second = SessionBuilder::new(Arc::new(ReloadModel("second")), "model")
        .in_memory()
        .unwrap();
    second.register_extension(shared.clone()).unwrap();
    first.start().await.unwrap();
    second.start().await.unwrap();
    let candidate = extension(&log, "replacement", "replacement", None);
    let update = RegistryUpdate {
        registry: registry(vec![candidate.clone()]),
        system_prompt: Some("replacement".into()),
    };
    assert!(first.reload(&update).await.is_err());
    assert_eq!(first.configured_system_prompt(), "original");
    assert_eq!(
        *log.lock().unwrap(),
        ["shared:start:Startup", "shared:start:Startup"]
    );
    second.close().await.unwrap();
    assert!(first.reload(&update).await.is_err());

    let mut later = SessionBuilder::new(Arc::new(ReloadModel("later")), "model")
        .registry(registry(vec![shared.clone(), candidate]))
        .in_memory()
        .unwrap();
    later.close().await.unwrap();
    first.close().await.unwrap();
    let mut owner = SessionBuilder::new(Arc::new(ReloadModel("owner")), "model")
        .in_memory()
        .unwrap();
    let previously_used = RegistryUpdate {
        registry: registry(vec![shared]),
        system_prompt: None,
    };
    assert!(owner.reload(&previously_used).await.is_err());
    owner.close().await.unwrap();
}

#[tokio::test]
async fn sdk_reload_legacy_close_waits_for_all_installations_and_forks() {
    for register_in_session in [false, true] {
        let log = LifecycleLog::default();
        let shared = extension(&log, "shared", "shared", None);
        let mut first = SessionBuilder::new(Arc::new(ReloadModel("first")), "model")
            .registry(registry(vec![shared.clone()]))
            .in_memory()
            .unwrap();
        let head = first.journal().unwrap().head();
        let mut fork = first.fork_at(head).unwrap();
        let builder = SessionBuilder::new(Arc::new(ReloadModel("second")), "model");
        let mut second = if register_in_session {
            let mut session = builder.in_memory().unwrap();
            session.register_extension(shared.clone()).unwrap();
            session
        } else {
            builder
                .registry(registry(vec![shared.clone()]))
                .in_memory()
                .unwrap()
        };

        first.close().await.unwrap();
        fork.close().await.unwrap();
        assert!(log.lock().unwrap().is_empty());
        second.close().await.unwrap();
        assert_eq!(*log.lock().unwrap(), ["shared:dispose"]);
    }
}

#[tokio::test(start_paused = true)]
async fn sdk_reload_legacy_close_reserves_shutdown_against_concurrent_installation() {
    let log = LifecycleLog::default();
    let pause = Arc::new(Pause::default());
    let shared: Arc<dyn Extension> = Arc::new(CloseProbe {
        log: log.clone(),
        quit: None,
        dispose: Some(pause.clone()),
        rename: None,
    });
    let build = || {
        SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
            .registry(registry(vec![shared.clone()]))
            .in_memory()
    };
    let (mut first, mut second) = (build().unwrap(), build().unwrap());
    let mut closing = Box::pin(async { tokio::join!(first.close(), second.close()) });
    tokio::select! {
        _ = pause.entered.notified() => {}
        result = &mut closing => panic!("close did not reserve the connection: {result:?}"),
    }
    let error = build()
        .err()
        .expect("builder must not install during shutdown");
    assert!(error.to_string().contains("is shutting down"));
    let mut empty = SessionBuilder::new(Arc::new(ReloadModel("other")), "model")
        .in_memory()
        .unwrap();
    let error = empty.register_extension(shared.clone()).unwrap_err();
    assert!(error.to_string().contains("is shutting down"));
    empty.close().await.unwrap();

    pause.release.notify_one();
    let (first, second) = closing.await;
    first.unwrap();
    second.unwrap();
    assert_eq!(*log.lock().unwrap(), ["dispose"]);

    let mut reused = build().unwrap();
    reused.close().await.unwrap();
    assert_eq!(*log.lock().unwrap(), ["dispose", "dispose"]);
}

#[tokio::test(start_paused = true)]
async fn sdk_reload_legacy_close_rechecks_a_builder_reservation_after_rollback() {
    let log = LifecycleLog::default();
    let pause = Arc::new(Pause::default());
    let shared: Arc<dyn Extension> = Arc::new(CloseProbe {
        log: log.clone(),
        quit: None,
        dispose: Some(pause.clone()),
        rename: None,
    });
    let mut session = SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
        .registry(registry(vec![shared.clone()]))
        .in_memory()
        .unwrap();
    let pending_owner = Arc::new(());
    let pending =
        LegacyExtensionReservation::reserve(std::slice::from_ref(&shared), &pending_owner).unwrap();
    let mut close = Box::pin(session.close());
    assert!(futures_util::poll!(&mut close).is_pending());
    tokio::task::yield_now().await;
    assert!(!pause.attempted.load(Ordering::Relaxed));

    drop(pending);
    tokio::select! {
        _ = pause.entered.notified() => {}
        result = &mut close => panic!("close did not retry after rollback: {result:?}"),
    }
    pause.release.notify_one();
    close.await.unwrap();
    assert_eq!(*log.lock().unwrap(), ["dispose"]);
}

#[tokio::test(start_paused = true)]
async fn sdk_reload_legacy_close_defers_cleanup_until_builder_resolves() {
    let timeout = std::time::Duration::from_secs(1);
    for commit in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("close.jsonl");
        let log = LifecycleLog::default();
        let shared = extension(&log, "shared", "shared", None);
        let mut session = SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
            .limits(Limits {
                tool_timeout: timeout,
                ..Limits::default()
            })
            .registry(registry(vec![shared.clone()]))
            .open(&path)
            .unwrap();
        let mut successor = SessionBuilder::new(Arc::new(ReloadModel("successor")), "model")
            .in_memory()
            .unwrap();
        let incoming = registry(vec![shared.clone()]);
        let pending = LegacyExtensionReservation::reserve(
            &incoming.extensions,
            &successor.extension_registration_scope,
        )
        .unwrap();
        let mut events = session.subscribe();

        let before_close = tokio::time::Instant::now();
        assert!(
            matches!(session.close().await, Err(Error::Extension { id, message })
            if id == "shared" && message == "extension shutdown timed out")
        );
        assert_eq!(before_close.elapsed(), timeout);
        assert!(matches!(events.try_recv(), Ok(SessionEvent::Closed)));
        assert!(matches!(
            events.try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
        assert!(matches!(
            session.subscribe().try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
        drop(Session::open(&path).expect("close released the journal with a pending builder"));
        session.close().await.unwrap();
        assert!(log.lock().unwrap().is_empty());

        if commit {
            // Complete the installation using the reservation that delayed close.
            pending.commit();
            successor.registry = Arc::new(incoming);
            tokio::time::sleep(timeout).await;
            assert!(log.lock().unwrap().is_empty());
            successor.close().await.unwrap();
            assert_eq!(*log.lock().unwrap(), ["shared:dispose"]);
        } else {
            drop(pending);
            tokio::time::timeout(timeout, async {
                while log.lock().unwrap().is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("rollback must wake the deferred shutdown");
            assert_eq!(*log.lock().unwrap(), ["shared:dispose"]);

            successor.register_extension(shared).unwrap();
            successor.close().await.unwrap();
            assert_eq!(*log.lock().unwrap(), ["shared:dispose", "shared:dispose"]);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn sdk_reload_legacy_shutdown_handoff_cannot_cross_owners() {
    let old_timeout = std::time::Duration::from_secs(1);
    let successor_timeout = std::time::Duration::from_secs(5);
    for already_deferred in [false, true] {
        let log = LifecycleLog::default();
        let pause = Arc::new(Pause::default());
        let shared: Arc<dyn Extension> = Arc::new(CloseProbe {
            log: log.clone(),
            quit: None,
            dispose: Some(pause.clone()),
            rename: None,
        });
        let registry = Arc::new(registry(vec![shared.clone()]));
        let first_scope = Arc::new(());
        let successor_scope = Arc::new(());
        LegacyExtensionReservation::reserve(&registry.extensions, &first_scope)
            .unwrap()
            .commit();
        let successor =
            LegacyExtensionReservation::reserve(&registry.extensions, &successor_scope).unwrap();
        let Some(ExtensionShutdown::Legacy(first)) =
            ExtensionShutdown::release(&registry, first_scope, false)
        else {
            panic!("first owner must release a shutdown claim");
        };
        assert!(
            tokio::time::timeout(
                old_timeout,
                LegacyShutdownReservation::acquire(&shared, &first, false)
            )
            .await
            .is_err()
        );
        let mut old_cleanup = if already_deferred {
            let mut cleanup =
                Box::pin(defer_legacy_shutdown(&shared, &first, old_timeout).unwrap());
            assert!(futures_util::poll!(&mut cleanup).is_pending());
            Some(cleanup)
        } else {
            None
        };

        successor.commit();
        let next_scope = Arc::new(());
        let next = LegacyExtensionReservation::reserve(&registry.extensions, &next_scope).unwrap();
        let Some(ExtensionShutdown::Legacy(successor)) =
            ExtensionShutdown::release(&registry, successor_scope, false)
        else {
            panic!("successor must release its own shutdown claim");
        };
        assert!(matches!(
            futures_util::poll!(Box::pin(LegacyShutdownReservation::acquire(
                &shared, &first, false
            ))),
            std::task::Poll::Ready(None)
        ));
        assert!(defer_legacy_shutdown(&shared, &first, old_timeout).is_none());
        let mut cleanup =
            Box::pin(defer_legacy_shutdown(&shared, &successor, successor_timeout).unwrap());
        drop(next);

        if let Some(old_cleanup) = &mut old_cleanup {
            assert!(futures_util::poll!(old_cleanup).is_ready());
            assert!(!pause.attempted.load(Ordering::Relaxed));
        }
        assert!(futures_util::poll!(&mut cleanup).is_pending());
        assert!(pause.attempted.load(Ordering::Relaxed));
        tokio::time::advance(old_timeout).await;
        assert!(futures_util::poll!(&mut cleanup).is_pending());
        tokio::time::advance(successor_timeout - old_timeout).await;
        assert!(futures_util::poll!(&mut cleanup).is_ready());
        assert!(log.lock().unwrap().is_empty());
        LegacyExtensionReservation::reserve(&registry.extensions, &next_scope)
            .expect("successor timeout released the connection");
    }
}

#[tokio::test]
async fn sdk_reload_claims_single_owner_and_blocks_in_flight_legacy_installations() {
    let log = LifecycleLog::default();
    let retained = extension(&log, "retained", "retained", None);
    let mut session = SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
        .registry(registry(vec![retained.clone()]))
        .in_memory()
        .unwrap();
    let update = RegistryUpdate {
        registry: registry(vec![retained.clone()]),
        system_prompt: None,
    };
    let pending_owner = Arc::new(());
    let pending =
        LegacyExtensionReservation::reserve(&update.registry.extensions, &pending_owner).unwrap();
    assert!(session.reload(&update).await.is_err());
    drop(pending);
    session.reload(&update).await.unwrap();

    let extra = extension(&log, "extra", "extra", None);
    session.register_extension(extra.clone()).unwrap();
    for instance in [retained, extra] {
        assert!(
            SessionBuilder::new(Arc::new(ReloadModel("other")), "model")
                .registry(registry(vec![instance]))
                .in_memory()
                .is_err()
        );
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn sdk_reload_waits_to_dispose_renderers_and_preserves_empty_selection() {
    let log = LifecycleLog::default();
    let original_extension = extension(
        &log,
        "original",
        "old",
        Some(ReloadTool("old-job", "old tool")),
    );
    let mut original = registry(vec![original_extension.clone()]);
    original.set_active_tools([]).unwrap();
    let mut session = SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
        .registry(original)
        .system_prompt("current")
        .in_memory()
        .unwrap();
    session.prompt("start").await.unwrap();
    let renderer = session.tool_renderers();
    let update = RegistryUpdate {
        registry: registry(vec![extension(
            &log,
            "replacement",
            "new",
            Some(ReloadTool("job", "new tool")),
        )]),
        system_prompt: None,
    };
    session.reload(&update).await.unwrap();
    drop(update);
    assert!(
        session
            .register_extension(original_extension)
            .unwrap_err()
            .to_string()
            .contains("retired extension instance")
    );
    assert!(
        !log.lock()
            .unwrap()
            .iter()
            .any(|entry| entry == "old:dispose")
    );
    assert_eq!(
        session.prompt("empty selection").await.unwrap(),
        "primary:current:no tools"
    );
    drop(renderer);
    session.close().await.unwrap();
    assert_eq!(
        log.lock()
            .unwrap()
            .iter()
            .filter(|entry| *entry == "old:dispose")
            .count(),
        1
    );
}

#[tokio::test]
async fn sdk_reload_close_disposes_ready_retirees_while_another_is_shared() {
    let log = LifecycleLog::default();
    let first = extension(&log, "first", "first", None);
    let second = extension(&log, "second", "second", None);
    let shared = extension(&log, "shared", "shared", None);
    let mut session = SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
        .registry(registry(vec![
            first.clone(),
            second.clone(),
            shared.clone(),
        ]))
        .in_memory()
        .unwrap();
    session.start().await.unwrap();
    session
        .reload(&RegistryUpdate {
            registry: Registry::default(),
            system_prompt: None,
        })
        .await
        .unwrap();
    assert_eq!(
        &log.lock().unwrap()[3..],
        ["first:end:Reload", "second:end:Reload", "shared:end:Reload"]
    );

    drop(first);
    drop(second);
    session.close().await.unwrap();
    assert_eq!(
        &log.lock().unwrap()[6..],
        ["second:dispose", "first:dispose"]
    );
    drop(shared);
}

struct PausedInput {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl Extension for PausedInput {
    fn id(&self) -> &str {
        "paused_input"
    }

    fn handles_input(&self) -> bool {
        true
    }

    async fn input(
        &self,
        event: &crate::InputEvent,
        _: Option<&crate::ExtensionState>,
    ) -> crate::ExtensionResult<crate::InputAction> {
        if event.text == "queued" {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(crate::InputAction::Continue)
    }
}

#[tokio::test]
async fn sdk_reload_serializes_against_cloned_control_input() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut session = SessionBuilder::new(Arc::new(ReloadModel("primary")), "model")
        .extension(Arc::new(PausedInput {
            entered: entered.clone(),
            release: release.clone(),
        }))
        .unwrap()
        .system_prompt("old")
        .in_memory()
        .unwrap();
    session.prompt("start").await.unwrap();
    let control = session.controls();
    let queued = tokio::spawn(async move { control.steer("queued").await });
    platform_time::timeout(std::time::Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    let update = RegistryUpdate {
        registry: Registry::default(),
        system_prompt: Some("new".into()),
    };
    assert!(
        session
            .reload(&update)
            .await
            .unwrap_err()
            .to_string()
            .contains("input control is in use")
    );
    release.notify_one();
    queued.await.unwrap().unwrap();
    session.reload(&update).await.unwrap();
    assert_eq!(
        session.prompt("next").await.unwrap(),
        "primary:new:no tools"
    );
    assert!(session.journal().unwrap().records().iter().any(|record| {
        matches!(&record.event, crate::Event::User { content, .. } if content == "queued")
    }));
}
