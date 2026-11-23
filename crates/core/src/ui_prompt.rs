//! Observe the interval in which a native extension is waiting on its UI host.
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{ExtensionStates, ExtensionUi, Registry};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiPromptKind {
    Select,
    Confirm,
    Input,
    Editor,
    Custom,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiPromptReason {
    UiPrompt,
}

#[derive(Default)]
pub(crate) struct RunEvents {
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    worker: Arc<std::sync::Mutex<Option<native::Worker>>>,
}

impl RunEvents {
    pub(crate) async fn scope<F: std::future::Future>(&self, future: F) -> F::Output {
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        return native::WORKER.scope(self.worker.clone(), future).await;
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        future.await
    }

    pub(crate) async fn finish(self, observer: &mut crate::Observer<'_>) {
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            let worker = self
                .worker
                .lock()
                .expect("extension UI event worker")
                .take();
            if let Some(worker) = worker {
                worker.finish(observer).await;
            }
        }
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        let _ = observer;
    }
}

pub(crate) fn for_turn(
    registry: &Registry,
    states: &ExtensionStates,
    timeout: Duration,
) -> Option<Arc<dyn ExtensionUi>> {
    let ui = registry.ui.clone()?;
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    if registry
        .extensions
        .iter()
        .any(|extension| extension.handles_lifecycle() && extension.handles_ui_prompt_lifecycle())
        && let Ok(wrapped) = native::WORKER.try_with(|worker| {
            worker
                .lock()
                .expect("extension UI event worker")
                .get_or_insert_with(native::Worker::new)
                .bind(ui.clone(), registry.clone(), states.clone(), timeout)
        })
    {
        return Some(Arc::new(wrapped));
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    let _ = (states, timeout);
    Some(ui)
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod native {
    use std::sync::Mutex;

    use tokio::sync::mpsc;

    use super::*;
    use crate::{
        CancellationToken, ExtensionResult, LifecycleEvent, Notification, UiRequest, UiResponse,
    };

    const EVENT_CAPACITY: usize = 32;
    const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

    tokio::task_local! {
        pub(super) static WORKER: Arc<Mutex<Option<Worker>>>;
    }

    struct ObserverSnapshot {
        registry: Registry,
        states: ExtensionStates,
        timeout: Duration,
    }

    struct Delivery {
        observer: Arc<ObserverSnapshot>,
        event: LifecycleEvent,
    }

    type ObserverErrors = Arc<Mutex<Vec<(String, &'static str, String)>>>;

    pub(super) struct Worker {
        queue: Option<mpsc::Sender<Delivery>>,
        task: tokio::task::JoinHandle<()>,
        active: Arc<Mutex<ActivePrompt>>,
        errors: ObserverErrors,
    }

    impl Worker {
        pub(super) fn new() -> Self {
            let (sender, mut queue) = mpsc::channel::<Delivery>(EVENT_CAPACITY);
            let errors = ObserverErrors::default();
            let reported = errors.clone();
            let task = tokio::spawn(async move {
                while let Some(Delivery { observer, event }) = queue.recv().await {
                    crate::lifecycle::notify(
                        &observer.registry,
                        event,
                        &observer.states,
                        observer.timeout,
                        &mut |notification| {
                            if let Notification::ExtensionLifecycleError {
                                extension,
                                event,
                                message,
                            } = notification
                            {
                                let mut errors =
                                    reported.lock().expect("extension UI observer errors");
                                if errors.len() < EVENT_CAPACITY {
                                    errors.push((extension.to_owned(), event, message.to_owned()));
                                }
                            }
                        },
                    )
                    .await;
                }
            });
            Self {
                queue: Some(sender),
                task,
                active: Arc::default(),
                errors,
            }
        }

        pub(super) fn bind(
            &self,
            inner: Arc<dyn ExtensionUi>,
            registry: Registry,
            states: ExtensionStates,
            timeout: Duration,
        ) -> ObservedUi {
            ObservedUi {
                inner,
                events: self.queue.as_ref().expect("active UI event queue").clone(),
                active: self.active.clone(),
                observer: Arc::new(ObserverSnapshot {
                    registry,
                    states,
                    timeout,
                }),
            }
        }

        pub(super) async fn finish(mut self, observer: &mut crate::Observer<'_>) {
            drop(self.queue.take());
            if tokio::time::timeout(DRAIN_TIMEOUT, &mut self.task)
                .await
                .is_err()
            {
                self.task.abort();
                let _ = (&mut self.task).await;
            }
            let errors =
                std::mem::take(&mut *self.errors.lock().expect("extension UI observer errors"));
            for (extension, event, message) in errors {
                observer(Notification::ExtensionLifecycleError {
                    extension: &extension,
                    event,
                    message: &message,
                });
            }
        }
    }

    impl Drop for Worker {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    pub(super) struct ObservedUi {
        inner: Arc<dyn ExtensionUi>,
        events: mpsc::Sender<Delivery>,
        observer: Arc<ObserverSnapshot>,
        active: Arc<Mutex<ActivePrompt>>,
    }

    #[derive(Default)]
    struct ActivePrompt {
        depth: usize,
        end: Option<(mpsc::OwnedPermit<Delivery>, Delivery)>,
    }

    struct PromptScope<'a>(&'a Mutex<ActivePrompt>);

    impl Drop for PromptScope<'_> {
        fn drop(&mut self) {
            let mut active = self.0.lock().expect("extension UI prompt activity");
            active.depth -= 1;
            if active.depth == 0
                && let Some((permit, end)) = active.end.take()
            {
                permit.send(end);
            }
        }
    }

    impl ObservedUi {
        fn enter(&self, kind: UiPromptKind, title: Option<String>) -> PromptScope<'_> {
            let mut active = self.active.lock().expect("extension UI prompt activity");
            // Reserve both edges before publishing the start. If observers
            // cannot keep up, skip the pair without delaying the prompt.
            if active.depth == 0
                && let Ok(start) = self.events.clone().try_reserve_owned()
                && let Ok(end) = self.events.clone().try_reserve_owned()
            {
                start.send(Delivery {
                    observer: self.observer.clone(),
                    event: LifecycleEvent::UiPromptStart {
                        reason: UiPromptReason::UiPrompt,
                        kind,
                        title: title.clone(),
                    },
                });
                active.end = Some((
                    end,
                    Delivery {
                        observer: self.observer.clone(),
                        event: LifecycleEvent::UiPromptEnd {
                            reason: UiPromptReason::UiPrompt,
                            kind,
                            title,
                        },
                    },
                ));
            }
            active.depth += 1;
            PromptScope(&self.active)
        }
    }

    #[async_trait::async_trait]
    impl ExtensionUi for ObservedUi {
        async fn request(
            &self,
            request: UiRequest,
            cancellation: CancellationToken,
        ) -> ExtensionResult<UiResponse> {
            let (kind, title) = match &request {
                UiRequest::Select { title, .. } => (UiPromptKind::Select, Some(title)),
                UiRequest::Confirm { title, .. } => (UiPromptKind::Confirm, Some(title)),
                UiRequest::Input { title, .. } => (UiPromptKind::Input, Some(title)),
                UiRequest::Editor { title, .. } => (UiPromptKind::Editor, Some(title)),
                UiRequest::Custom { .. } => (UiPromptKind::Custom, None),
                _ => return self.inner.request(request, cancellation).await,
            };
            let _scope = self.enter(kind, title.filter(|title| !title.is_empty()).cloned());
            self.inner.request(request, cancellation).await
        }
    }
}

#[cfg(all(test, not(all(target_arch = "wasm32", target_os = "unknown"))))]
mod tests {
    use serde_json::{Value, json};
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use super::*;
    use crate::{
        CancellationToken, Extension, ExtensionResult, ExtensionState, LifecycleEvent, UiRequest,
        UiResponse,
    };

    struct Listener(mpsc::UnboundedSender<Value>);

    #[async_trait::async_trait]
    impl Extension for Listener {
        fn id(&self) -> &str {
            "ui-observer"
        }

        fn handles_lifecycle(&self) -> bool {
            true
        }

        fn handles_ui_prompt_lifecycle(&self) -> bool {
            true
        }

        async fn lifecycle(
            &self,
            event: &LifecycleEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<()> {
            if matches!(event, LifecycleEvent::UiPromptStart { title: Some(title), .. } if title == "Review change 0")
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if matches!(
                event,
                LifecycleEvent::UiPromptStart { .. } | LifecycleEvent::UiPromptEnd { .. }
            ) {
                self.0.send(serde_json::to_value(event).unwrap()).unwrap();
            }
            Ok(())
        }
    }

    struct PendingUi(mpsc::UnboundedSender<()>);

    #[async_trait::async_trait]
    impl ExtensionUi for PendingUi {
        async fn request(
            &self,
            request: UiRequest,
            cancellation: CancellationToken,
        ) -> ExtensionResult<UiResponse> {
            if request.needs_response() {
                self.0.send(()).unwrap();
                cancellation.cancelled().await;
            }
            Ok(UiResponse::Cancelled)
        }
    }

    #[tokio::test]
    async fn overlapping_ui_prompts_use_the_first_kind_and_close_on_cancel_or_drop() {
        let (entered, mut entered_events) = mpsc::unbounded_channel();
        let (events, mut lifecycle_events) = mpsc::unbounded_channel();
        let mut registry = Registry::default();
        registry.register(Arc::new(Listener(events))).unwrap();
        registry.set_ui(Arc::new(PendingUi(entered)));
        let deadline = Duration::from_secs(1);
        let run = RunEvents::default();
        let ui = run
            .scope(async { for_turn(&registry, &Default::default(), deadline).unwrap() })
            .await;

        ui.request(
            UiRequest::Notify {
                message: "modal lifecycle should ignore this".into(),
                level: None,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(lifecycle_events.try_recv().is_err());

        let cancel_first = CancellationToken::new();
        let first = {
            let ui = ui.clone();
            let cancel = cancel_first.clone();
            tokio::spawn(async move {
                ui.request(
                    UiRequest::Input {
                        title: "Enter a value".into(),
                        placeholder: None,
                        initial_value: Some("private input".into()),
                        timeout: None,
                    },
                    cancel,
                )
                .await
            })
        };
        timeout(deadline, entered_events.recv())
            .await
            .unwrap()
            .unwrap();
        let second = {
            let ui = ui.clone();
            tokio::spawn(async move {
                ui.request(
                    UiRequest::Confirm {
                        title: "Second prompt".into(),
                        message: "private confirm message".into(),
                        timeout: None,
                    },
                    CancellationToken::new(),
                )
                .await
            })
        };
        timeout(deadline, entered_events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            timeout(deadline, lifecycle_events.recv())
                .await
                .unwrap()
                .unwrap(),
            json!({
                "type":"ui_prompt_start","reason":"ui_prompt","kind":"input","title":"Enter a value"
            })
        );

        cancel_first.cancel();
        assert_eq!(first.await.unwrap().unwrap(), UiResponse::Cancelled);
        assert!(lifecycle_events.try_recv().is_err());
        second.abort();
        assert!(second.await.unwrap_err().is_cancelled());
        assert_eq!(
            timeout(deadline, lifecycle_events.recv())
                .await
                .unwrap()
                .unwrap(),
            json!({
                "type":"ui_prompt_end","reason":"ui_prompt","kind":"input","title":"Enter a value"
            })
        );
        drop(ui);
        run.finish(&mut |_| {}).await;
    }
    #[tokio::test]
    async fn settlement_cancels_a_slow_prompt_observer() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct SlowListener {
            entered: mpsc::UnboundedSender<()>,
            cancelled: Arc<AtomicBool>,
        }
        struct CancellationFlag(Arc<AtomicBool>);
        impl Drop for CancellationFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }

        #[async_trait::async_trait]
        impl Extension for SlowListener {
            fn id(&self) -> &str {
                "slow-prompt-observer"
            }
            fn handles_lifecycle(&self) -> bool {
                true
            }
            fn handles_ui_prompt_lifecycle(&self) -> bool {
                true
            }
            async fn lifecycle(
                &self,
                event: &LifecycleEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<()> {
                assert!(matches!(event, LifecycleEvent::UiPromptStart { .. }));
                let _cancelled = CancellationFlag(self.cancelled.clone());
                self.entered.send(()).unwrap();
                std::future::pending().await
            }
        }

        let mut registry = Registry::default();
        let (entered, mut callback_entered) = mpsc::unbounded_channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        registry
            .register(Arc::new(SlowListener {
                entered,
                cancelled: cancelled.clone(),
            }))
            .unwrap();
        let (entered, _ui_entered) = mpsc::unbounded_channel();
        registry.set_ui(Arc::new(PendingUi(entered)));
        let run = RunEvents::default();
        let deadline = Duration::from_secs(5);
        run.scope(async {
            let ui = for_turn(&registry, &Default::default(), deadline).unwrap();
            let cancel = CancellationToken::new();
            cancel.cancel();
            assert_eq!(
                ui.request(
                    UiRequest::Confirm {
                        title: "Prompt".into(),
                        message: "Continue?".into(),
                        timeout: None,
                    },
                    cancel
                )
                .await
                .unwrap(),
                UiResponse::Cancelled
            );
        })
        .await;
        timeout(deadline, callback_entered.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(!cancelled.load(Ordering::Relaxed));
        timeout(deadline, run.finish(&mut |_| {})).await.unwrap();
        assert!(cancelled.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn sdk_tool_emits_prompt_edges_through_the_normal_host() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use crate::{
            Model, ModelEvent, ModelRequest, ModelStream, Outcome, SessionBuilder, Tool, ToolCall,
            ToolContext, ToolHost, ToolSpec,
        };

        struct ConfirmTool;
        #[async_trait::async_trait]
        impl Tool for ConfirmTool {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "ask_user".into(),
                    description: "Request a test confirmation".into(),
                    parameters: json!({"type":"object"}),
                }
            }
            async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
                panic!("tool requires its native host")
            }
            async fn invoke_with_host(
                &self,
                arguments: Value,
                _: ToolContext,
                _: Option<&ExtensionState>,
                host: Arc<dyn ToolHost>,
            ) -> Outcome {
                assert!(host.has_ui());
                let reply = host
                    .ui(UiRequest::Confirm {
                        title: format!("Review change {}", arguments["turn"]),
                        message: "Do you approve?".into(),
                        timeout: None,
                    })
                    .await
                    .unwrap();
                assert_eq!(reply, UiResponse::Confirmed(true));
                Outcome::Succeeded("confirmed".into())
            }
        }

        struct Tools;
        impl Extension for Tools {
            fn id(&self) -> &str {
                "test-tools"
            }
            fn tools(&self) -> Vec<Arc<dyn Tool>> {
                vec![Arc::new(ConfirmTool)]
            }
        }

        struct FixtureModel(AtomicUsize);
        #[async_trait::async_trait]
        impl Model for FixtureModel {
            async fn stream(&self, _: ModelRequest) -> crate::Result<ModelStream> {
                let turn = self.0.fetch_add(1, Ordering::Relaxed);
                let events = if turn >= 2 {
                    vec![
                        ModelEvent::Text("done".into()),
                        ModelEvent::Complete {
                            calls: vec![],
                            usage: None,
                        },
                    ]
                } else {
                    vec![ModelEvent::Complete {
                        calls: vec![ToolCall {
                            id: format!("fixture-{turn}"),
                            name: "ask_user".into(),
                            arguments: json!({"turn":turn}).to_string(),
                        }],
                        usage: None,
                    }]
                };
                Ok(Box::pin(futures_util::stream::iter(
                    events.into_iter().map(Ok),
                )))
            }
        }

        struct ConfirmUi;
        #[async_trait::async_trait]
        impl ExtensionUi for ConfirmUi {
            async fn request(
                &self,
                request: UiRequest,
                _: CancellationToken,
            ) -> ExtensionResult<UiResponse> {
                assert!(matches!(request, UiRequest::Confirm { .. }));
                Ok(UiResponse::Confirmed(true))
            }
        }

        let (events, mut observed) = mpsc::unbounded_channel();
        let mut session =
            SessionBuilder::new(Arc::new(FixtureModel(AtomicUsize::new(0))), "fixture")
                .extension(Arc::new(Listener(events)))
                .unwrap()
                .extension(Arc::new(Tools))
                .unwrap()
                .ui(Arc::new(ConfirmUi))
                .in_memory()
                .unwrap();
        assert_eq!(session.prompt("ask").await.unwrap(), "done");
        let deadline = Duration::from_secs(2);
        for turn in 0..2 {
            for phase in ["ui_prompt_start", "ui_prompt_end"] {
                let event = timeout(deadline, observed.recv()).await.unwrap().unwrap();
                assert_eq!(
                    event,
                    json!({
                        "type":phase,"reason":"ui_prompt","kind":"confirm","title":format!("Review change {turn}")
                    })
                );
            }
        }
        session.close().await.unwrap();
    }
}
