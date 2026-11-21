use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures_util::FutureExt;
use serde::{Deserialize, Serialize};

use crate::runtime::time::{Instant, timeout};
use crate::{
    Event, Extension, ExtensionState, Message, Notification, Observer, Registry, Session, ToolCall,
    ToolCallDelta,
};

/// Native assistant streaming updates before the corresponding public notification.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum AssistantMessageUpdate {
    TextStart {
        content_index: u64,
    },
    TextDelta {
        content_index: Option<u64>,
        delta: String,
    },
    TextEnd {
        content_index: u64,
        content: String,
    },
    ThinkingStart {
        content_index: u64,
        redacted: bool,
    },
    ThinkingDelta {
        content_index: Option<u64>,
        delta: String,
    },
    ThinkingEnd {
        content_index: u64,
        content: String,
    },
    #[serde(rename = "toolcall_delta")]
    ToolCallDelta {
        tool_call: ToolCallDelta,
    },
}

/// Whether a streamed message was completed or abandoned.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageStatus {
    Complete,
    Failed,
    Aborted,
}

/// User and assistant messages observed by a native extension.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum MessageLifecycleEvent {
    #[serde(rename = "message_start")]
    Start { message: Message },
    #[serde(rename = "message_update")]
    Update {
        message: Message,
        assistant_message_event: AssistantMessageUpdate,
    },
    #[serde(rename = "message_end")]
    End {
        message: Message,
        status: MessageStatus,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Start,
    TerminalStart,
    Update,
    End,
}

impl Phase {
    fn kind(self) -> &'static str {
        match self {
            Self::Start | Self::TerminalStart => "message_start",
            Self::Update => "message_update",
            Self::End => "message_end",
        }
    }
}

struct Handler {
    extension: Arc<dyn Extension>,
    state: Option<ExtensionState>,
}

struct Failure {
    index: usize,
    phase: Phase,
    error: String,
    timed_out: bool,
}

pub(crate) struct MessageHooks {
    handlers: Vec<Handler>,
    started: bool,
    stream_budget: Duration,
    started_at: Instant,
    model_timeout: Duration,
    end_timeout: Duration,
    failures: Vec<Failure>,
}

impl MessageHooks {
    pub(crate) fn for_session(
        registry: &Registry,
        session: &Session,
        model_timeout: Duration,
    ) -> Option<Self> {
        let extensions: Vec<_> = registry
            .extensions
            .iter()
            .filter(|extension| extension.handles_message_lifecycle())
            .cloned()
            .collect();
        if extensions.is_empty() {
            return None;
        }
        let mut states = session.extension_states();
        let count = u32::try_from(extensions.len()).unwrap_or(u32::MAX);
        Some(Self {
            handlers: extensions
                .into_iter()
                .map(|extension| Handler {
                    state: states.remove(extension.id()),
                    extension,
                })
                .collect(),
            started: false,
            stream_budget: model_timeout / 4,
            started_at: Instant::now(),
            model_timeout,
            end_timeout: (model_timeout / 2).min(Duration::from_secs(5)) / count,
            failures: Vec::new(),
        })
    }

    pub(crate) async fn start(&mut self, terminal: bool) {
        if self.started || (!terminal && self.stream_budget.is_zero()) {
            return;
        }
        self.started = true;
        self.observe(
            if terminal {
                Phase::TerminalStart
            } else {
                Phase::Start
            },
            &MessageLifecycleEvent::Start {
                message: assistant(String::new(), Vec::new()),
            },
        )
        .await;
    }

    pub(crate) async fn user(&mut self, mut event: Event) -> Event {
        let (input_id, message) = match &event {
            Event::User { content, input_id } => (
                *input_id,
                Message::User {
                    content: content.clone(),
                },
            ),
            Event::UserWithImages {
                content,
                images,
                input_id,
            } => (
                *input_id,
                Message::UserWithImages {
                    content: content.clone(),
                    images: images.clone(),
                },
            ),
            _ => return event,
        };
        self.observe(
            Phase::TerminalStart,
            &MessageLifecycleEvent::Start {
                message: message.clone(),
            },
        )
        .await;
        let mut end = MessageLifecycleEvent::End {
            message,
            status: MessageStatus::Complete,
        };
        for index in 0..self.handlers.len() {
            let Some(replacement) = self.invoke(index, Phase::End, &end).await else {
                continue;
            };
            let candidate = match (&event, &replacement) {
                (Event::User { .. }, Message::User { content }) if !content.trim().is_empty() => {
                    Event::User {
                        content: content.clone(),
                        input_id,
                    }
                }
                (
                    Event::UserWithImages { images, .. },
                    Message::UserWithImages {
                        content,
                        images: updated,
                    },
                ) if images == updated => Event::UserWithImages {
                    content: content.clone(),
                    images: updated.clone(),
                    input_id,
                },
                _ => {
                    self.record_failure(index, Phase::End, "message_end must retain the user role and image attachments and leave a sendable message".into(), false);
                    continue;
                }
            };
            if serde_json::to_vec(&candidate)
                .is_ok_and(|bytes| bytes.len() <= crate::journal::MAX_USER_EVENT_BYTES)
            {
                event = candidate;
                end = MessageLifecycleEvent::End {
                    message: replacement,
                    status: MessageStatus::Complete,
                };
            } else {
                self.record_failure(
                    index,
                    Phase::End,
                    "message_end user text exceeds the input size limit".into(),
                    false,
                );
            }
        }
        event
    }

    pub(crate) async fn update(&mut self, parts: &[String], update: AssistantMessageUpdate) {
        if self.stream_budget.is_zero() {
            return;
        }
        self.observe(
            Phase::Update,
            &MessageLifecycleEvent::Update {
                message: assistant(parts.concat(), Vec::new()),
                assistant_message_event: update,
            },
        )
        .await;
    }

    async fn observe(&mut self, phase: Phase, event: &MessageLifecycleEvent) {
        for index in 0..self.handlers.len() {
            self.invoke(index, phase, event).await;
        }
    }

    pub(crate) async fn end(
        &mut self,
        content: &str,
        calls: &[ToolCall],
        status: MessageStatus,
        response_limit: usize,
    ) -> Option<String> {
        if !self.started {
            return None;
        }
        let mut current = content.to_owned();
        for index in 0..self.handlers.len() {
            let event = MessageLifecycleEvent::End {
                message: assistant(current.clone(), calls.to_vec()),
                status,
            };
            if let Some(replacement) = self.invoke(index, Phase::End, &event).await
                && status == MessageStatus::Complete
            {
                match replacement {
                    Message::Assistant { content, calls: updated, replay: None }
                        if updated == calls && content.len() <= response_limit =>
                    {
                        current = content;
                    }
                    _ => self.record_failure(index, Phase::End,
                        "message_end may edit assistant text within the response limit and must preserve tool calls without supplying provider replay".into(), false),
                }
            }
        }
        (current != content).then_some(current)
    }

    pub(crate) async fn fail(
        &mut self,
        content: &str,
        status: MessageStatus,
        response_limit: usize,
    ) {
        let subscribers = u32::try_from(self.handlers.len()).unwrap_or(u32::MAX);
        self.end_timeout = self
            .end_timeout
            .min(self.model_timeout.min(Duration::from_secs(1)) / 2 / subscribers);
        self.start(true).await;
        self.end(content, &[], status, response_limit).await;
    }

    async fn invoke(
        &mut self,
        index: usize,
        phase: Phase,
        event: &MessageLifecycleEvent,
    ) -> Option<Message> {
        if self
            .failures
            .iter()
            .any(|failure| failure.index == index && failure.timed_out && failure.phase == phase)
        {
            return None;
        }
        let terminal = matches!(phase, Phase::TerminalStart | Phase::End);
        let duration = if terminal {
            self.end_timeout
        } else {
            let remaining = self.model_timeout.saturating_sub(self.started_at.elapsed());
            self.stream_budget
                .min(Duration::from_secs(5))
                .min(remaining / 2)
        };
        if duration.is_zero() {
            return None;
        }
        let handler = &self.handlers[index];
        let started = Instant::now();
        let callback = AssertUnwindSafe(async {
            handler
                .extension
                .message_lifecycle(event, handler.state.as_ref())
                .await
        })
        .catch_unwind();
        let result = timeout(duration, callback).await;
        if !terminal {
            self.stream_budget = self.stream_budget.saturating_sub(started.elapsed());
        }
        match result {
            Ok(Ok(Ok(replacement))) => replacement,
            result => {
                let timed_out = result.is_err();
                let error = match result {
                    Ok(Ok(Err(error))) => error,
                    Ok(Err(_)) => "message lifecycle hook panicked".into(),
                    _ => "message lifecycle hook timed out".into(),
                };
                self.record_failure(index, phase, error, timed_out);
                None
            }
        }
    }

    fn record_failure(&mut self, index: usize, phase: Phase, error: String, timed_out: bool) {
        if let Some(previous) = self
            .failures
            .iter_mut()
            .find(|failure| failure.index == index && failure.phase == phase)
        {
            if timed_out {
                previous.error = error;
                previous.timed_out = true;
            }
        } else {
            self.failures.push(Failure {
                index,
                phase,
                error,
                timed_out,
            });
        }
    }

    pub(crate) fn report_errors(self, observer: &mut Observer<'_>) {
        for failure in self.failures {
            observer(Notification::ExtensionLifecycleError {
                extension: self.handlers[failure.index].extension.id(),
                event: failure.phase.kind(),
                message: &failure.error,
            });
        }
    }
}

fn assistant(content: String, calls: Vec<ToolCall>) -> Message {
    Message::Assistant {
        content,
        calls,
        replay: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Agent, CancellationToken, ExtensionResult, Limits, MemoryStore, Model, ModelEvent,
        ModelMetadata, ModelRequest, ModelStream, ProviderReplay, ResolvedModelIdentity, Result,
    };
    use std::sync::Mutex;

    struct Hook {
        id: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl Extension for Hook {
        fn id(&self) -> &str {
            self.id
        }

        fn handles_message_lifecycle(&self) -> bool {
            true
        }

        async fn message_lifecycle(
            &self,
            event: &MessageLifecycleEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<Option<Message>> {
            let mut log = self.log.lock().unwrap();
            match event {
                MessageLifecycleEvent::Start {
                    message: Message::Assistant { content, .. },
                } => {
                    assert!(content.is_empty());
                    log.push("start".into());
                }
                MessageLifecycleEvent::Update {
                    message,
                    assistant_message_event: AssistantMessageUpdate::TextDelta { delta, .. },
                } => {
                    assert!(
                        matches!(message, Message::Assistant { content, .. } if content.ends_with(delta))
                    );
                    log.push(format!("extension:{delta}"));
                }
                MessageLifecycleEvent::End {
                    message:
                        Message::Assistant {
                            content,
                            calls,
                            replay: None,
                        },
                    status: MessageStatus::Complete,
                } => {
                    log.push(format!("end:{content}"));
                    if self.id == "grow" {
                        return Ok(Some(assistant("replacement ".repeat(1024), calls.clone())));
                    }
                    if self.id == "invalid" {
                        return Ok(Some(Message::User {
                            content: "wrong role".into(),
                        }));
                    }
                    return Ok(Some(assistant(
                        format!("{content}/{}", self.id),
                        calls.clone(),
                    )));
                }
                _ => {}
            }
            Ok(None)
        }
    }

    struct TextModel(std::sync::atomic::AtomicUsize);

    #[async_trait::async_trait]
    impl Model for TextModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Text("one".into())),
                Ok(ModelEvent::Text(" two".into())),
                Ok(ModelEvent::Metadata(ModelMetadata {
                    replay: Some(Box::new(ProviderReplay {
                        identity: ResolvedModelIdentity {
                            provider: "fixture".into(),
                            model: "model".into(),
                        },
                        blocks: vec![serde_json::json!({"type":"text", "text":"one two"})],
                    })),
                    ..ModelMetadata::default()
                })),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: Some(crate::Usage {
                        input_tokens: 1,
                        output_tokens: 2,
                    }),
                }),
            ])))
        }
    }

    #[tokio::test]
    async fn native_message_lifecycle_observes_stream_before_listeners_and_commits_edited_text() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut registry = Registry::default();
        for id in ["first", "invalid", "last"] {
            registry
                .register(Arc::new(Hook {
                    id,
                    log: log.clone(),
                }))
                .unwrap();
        }
        let model = TextModel(std::sync::atomic::AtomicUsize::new(0));
        let agent = Agent {
            model: &model,
            model_name: "model",
            registry: &registry,
            system_prompt: "",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let (_tx, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
        let mut failures = Vec::new();
        let result = agent
            .run(
                &mut session,
                Some("question".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |event| match event {
                    Notification::Text(delta) => {
                        log.lock().unwrap().push(format!("public:{delta}"))
                    }
                    Notification::ExtensionLifecycleError {
                        extension, event, ..
                    } => {
                        failures.push((extension.to_owned(), event));
                    }
                    _ => {}
                },
            )
            .await
            .unwrap();
        assert_eq!(result, "one two/first/last");
        assert_eq!(failures, [("invalid".into(), "message_end")]);
        assert_eq!(
            *log.lock().unwrap(),
            [
                "start",
                "start",
                "start",
                "extension:one",
                "extension:one",
                "extension:one",
                "public:one",
                "extension: two",
                "extension: two",
                "extension: two",
                "public: two",
                "end:one two",
                "end:one two/first",
                "end:one two/first",
            ]
        );
        let recorded = session
            .records()
            .iter()
            .find_map(|record| {
                if let crate::Event::ModelFinished {
                    reply: Some(reply),
                    metadata,
                    ..
                } = &record.event
                {
                    Some((reply, metadata))
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(recorded.0.content, result);
        assert!(recorded.1.replay.is_none());
        assert!(recorded.1.assistant_text_edited);
        let reopened = Session::from_records(session.records().to_vec()).unwrap();
        assert!(matches!(reopened.current_messages().last(),
            Some(Message::Assistant { content, .. }) if content == &result));
    }

    #[tokio::test]
    async fn native_message_edit_is_budgeted_as_context_on_the_next_request() {
        let mut registry = Registry::default();
        registry
            .register(Arc::new(Hook {
                id: "grow",
                log: Arc::default(),
            }))
            .unwrap();
        let model = TextModel(std::sync::atomic::AtomicUsize::new(0));
        let agent = Agent {
            model: &model,
            model_name: "model",
            registry: &registry,
            system_prompt: "",
            limits: Limits {
                context_window: Some(1000),
                output_tokens_per_request: 64,
                ..Limits::default()
            },
        };
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session.auto_compaction_control().set_enabled(false);
        let (_tx, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
        let cancel = CancellationToken::new();
        agent
            .run(
                &mut session,
                Some("first".into()),
                &mut inputs,
                &cancel,
                &mut |_| {},
            )
            .await
            .unwrap();
        assert!(session.context_usage_estimate().last_usage_record.is_none());
        assert!(session.context_budget_estimate().tokens > 1000);
        let second = agent
            .run(
                &mut session,
                Some("second".into()),
                &mut inputs,
                &cancel,
                &mut |_| {},
            )
            .await;
        assert!(
            matches!(second, Err(crate::Error::Model(error)) if error.contains("context does not fit"))
        );
        assert_eq!(model.0.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn native_message_end_rejects_new_tool_calls() {
        struct Inject;
        #[async_trait::async_trait]
        impl Extension for Inject {
            fn id(&self) -> &str {
                "inject"
            }
            fn handles_message_lifecycle(&self) -> bool {
                true
            }
            async fn message_lifecycle(
                &self,
                event: &MessageLifecycleEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<Option<Message>> {
                Ok(matches!(event, MessageLifecycleEvent::End { .. }).then(|| {
                    assistant(
                        "replacement".into(),
                        vec![ToolCall {
                            id: "new".into(),
                            name: "tool".into(),
                            arguments: "{}".into(),
                        }],
                    )
                }))
            }
        }
        let mut registry = Registry::default();
        registry.register(Arc::new(Inject)).unwrap();
        let session = Session::with_store(MemoryStore::default()).unwrap();
        let mut hooks =
            MessageHooks::for_session(&registry, &session, Duration::from_secs(2)).unwrap();
        hooks.start(false).await;
        assert!(
            hooks
                .end("original", &[], MessageStatus::Complete, 1024)
                .await
                .is_none()
        );
        let mut errors = 0;
        hooks.report_errors(&mut |notification| {
            if let Notification::ExtensionLifecycleError {
                event: "message_end",
                ..
            } = notification
            {
                errors += 1;
            }
        });
        assert_eq!(errors, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn native_message_opening_failure_still_has_start_and_end() {
        struct Refused;

        #[async_trait::async_trait]
        impl Model for Refused {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
                Err(crate::Error::Model("could not open fixture stream".into()))
            }
        }

        struct Stall;

        #[async_trait::async_trait]
        impl Extension for Stall {
            fn id(&self) -> &str {
                "stall"
            }
            fn handles_message_lifecycle(&self) -> bool {
                true
            }
            async fn message_lifecycle(
                &self,
                event: &MessageLifecycleEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<Option<Message>> {
                if matches!(
                    event,
                    MessageLifecycleEvent::Start {
                        message: Message::Assistant { .. }
                    }
                ) {
                    std::future::pending().await
                } else {
                    Ok(None)
                }
            }
        }

        struct Observe(Arc<Mutex<Vec<&'static str>>>);

        #[async_trait::async_trait]
        impl Extension for Observe {
            fn id(&self) -> &str {
                "observe"
            }
            fn handles_message_lifecycle(&self) -> bool {
                true
            }
            async fn message_lifecycle(
                &self,
                event: &MessageLifecycleEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<Option<Message>> {
                match event {
                    MessageLifecycleEvent::Start {
                        message: Message::Assistant { .. },
                    } => self.0.lock().unwrap().push("start"),
                    MessageLifecycleEvent::End {
                        status: MessageStatus::Failed,
                        ..
                    } => {
                        self.0.lock().unwrap().push("failed");
                    }
                    _ => {}
                }
                Ok(None)
            }
        }
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut registry = Registry::default();
        registry.register(Arc::new(Stall)).unwrap();
        registry.register(Arc::new(Observe(log.clone()))).unwrap();
        let agent = Agent {
            model: &Refused,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let (_tx, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
        let result = agent
            .run(
                &mut session,
                Some("question".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |_| {},
            )
            .await;
        assert!(matches!(result, Err(crate::Error::Model(_))));
        assert_eq!(*log.lock().unwrap(), ["start", "failed"]);
        assert!(
            !session
                .records()
                .iter()
                .any(|record| matches!(record.event, crate::Event::Assistant { .. }))
        );
    }

    #[tokio::test]
    async fn native_message_end_keeps_completed_edits_when_cancellation_arrives() {
        struct Cancel(CancellationToken);

        #[async_trait::async_trait]
        impl Extension for Cancel {
            fn id(&self) -> &str {
                "cancel-during-end"
            }
            fn handles_message_lifecycle(&self) -> bool {
                true
            }
            async fn message_lifecycle(
                &self,
                event: &MessageLifecycleEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<Option<Message>> {
                if let MessageLifecycleEvent::End {
                    message: Message::Assistant { content, .. },
                    status: MessageStatus::Complete,
                } = event
                {
                    assert_eq!(content, "one two/first");
                    self.0.cancel();
                    tokio::task::yield_now().await;
                }
                Ok(None)
            }
        }

        let cancel = CancellationToken::new();
        let mut registry = Registry::default();
        registry
            .register(Arc::new(Hook {
                id: "first",
                log: Arc::default(),
            }))
            .unwrap();
        registry.register(Arc::new(Cancel(cancel.clone()))).unwrap();
        let model = TextModel(std::sync::atomic::AtomicUsize::new(0));
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let (_tx, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
        let _ = agent
            .run(
                &mut session,
                Some("question".into()),
                &mut inputs,
                &cancel,
                &mut |_| {},
            )
            .await;
        assert!(cancel.is_cancelled());
        assert!(session.records().iter().any(|record| matches!(&record.event,
            crate::Event::ModelFinished { reply: Some(reply), .. } if reply.content == "one two/first"
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn native_message_stream_callback_leaves_time_after_late_first_frame() {
        struct Slow;
        #[async_trait::async_trait]
        impl Extension for Slow {
            fn id(&self) -> &str {
                "slow"
            }
            fn handles_message_lifecycle(&self) -> bool {
                true
            }
            async fn message_lifecycle(
                &self,
                _: &MessageLifecycleEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<Option<Message>> {
                std::future::pending().await
            }
        }

        let session = Session::with_store(MemoryStore::default()).unwrap();
        let mut registry = Registry::default();
        registry.register(Arc::new(Slow)).unwrap();
        let started = Instant::now();
        let model_timeout = Duration::from_millis(80);
        let mut hooks = MessageHooks::for_session(&registry, &session, model_timeout).unwrap();
        timeout(model_timeout, async {
            tokio::time::sleep(Duration::from_millis(70)).await;
            hooks.start(false).await;
        })
        .await
        .unwrap();
        assert!(started.elapsed() < model_timeout);
    }

    #[tokio::test]
    async fn native_user_message_hooks_edit_initial_and_queued_prompts_before_commit() {
        struct UserHook(Arc<Mutex<Vec<String>>>);

        #[async_trait::async_trait]
        impl Extension for UserHook {
            fn id(&self) -> &str {
                "user"
            }
            fn handles_message_lifecycle(&self) -> bool {
                true
            }
            async fn message_lifecycle(
                &self,
                event: &MessageLifecycleEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<Option<Message>> {
                match event {
                    MessageLifecycleEvent::Start {
                        message: Message::User { content },
                    } => {
                        self.0.lock().unwrap().push(format!("start:{content}"));
                        Ok(Some(Message::User {
                            content: "ignored at start".into(),
                        }))
                    }
                    MessageLifecycleEvent::End {
                        message: Message::User { content },
                        status: MessageStatus::Complete,
                    } => {
                        self.0.lock().unwrap().push(format!("end:{content}"));
                        Ok(Some(Message::User {
                            content: format!("{content}/edited"),
                        }))
                    }
                    _ => Ok(None),
                }
            }
        }

        let log = Arc::new(Mutex::new(Vec::new()));
        let mut registry = Registry::default();
        registry.register(Arc::new(UserHook(log.clone()))).unwrap();
        let model = TextModel(std::sync::atomic::AtomicUsize::new(0));
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let run_id = uuid::Uuid::new_v4();
        let followup_id = uuid::Uuid::new_v4();
        let (tx, mut inputs) = tokio::sync::mpsc::channel(1);
        tx.send(crate::QueuedInput {
            id: followup_id,
            kind: crate::InputKind::FollowUp,
            content: "follow-up".into(),
        })
        .await
        .unwrap();
        let mut committed = Vec::new();
        agent
            .run_correlated(
                &mut session,
                run_id,
                Some("initial".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |notification| {
                    if let Notification::Committed(record) = notification
                        && let Event::User {
                            content,
                            input_id: Some(id),
                        } = &record.event
                    {
                        log.lock().unwrap().push(format!("commit:{content}"));
                        committed.push(*id);
                    }
                },
            )
            .await
            .unwrap();
        assert_eq!(committed, [run_id, followup_id]);
        assert_eq!(
            *log.lock().unwrap(),
            [
                "start:initial",
                "end:initial",
                "commit:initial/edited",
                "start:follow-up",
                "end:follow-up",
                "commit:follow-up/edited",
            ]
        );
        assert!(session.queued_inputs().is_empty());
    }

    #[tokio::test]
    async fn native_user_message_cancellation_leaves_later_prompts_queued() {
        struct StopDelivery(CancellationToken);

        #[async_trait::async_trait]
        impl Extension for StopDelivery {
            fn id(&self) -> &str {
                "stop-delivery"
            }
            fn handles_message_lifecycle(&self) -> bool {
                true
            }
            async fn message_lifecycle(
                &self,
                event: &MessageLifecycleEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<Option<Message>> {
                if let MessageLifecycleEvent::Start {
                    message: Message::User { content },
                } = event
                {
                    assert_eq!(content, "first");
                    self.0.cancel();
                }
                Ok(None)
            }
        }

        let cancel = CancellationToken::new();
        let mut registry = Registry::default();
        registry
            .register(Arc::new(StopDelivery(cancel.clone())))
            .unwrap();
        let model = TextModel(std::sync::atomic::AtomicUsize::new(0));
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let first = crate::QueuedInput::from("first".to_owned());
        let second = crate::QueuedInput::from("second".to_owned());
        for input in [first.clone(), second.clone()] {
            session.append(Event::InputQueued { input }).unwrap();
        }
        let delivery = agent
            .apply_queued_inputs_with_lifecycle(&mut session, &cancel, &mut |_| {})
            .await;
        assert!(matches!(delivery, Err(crate::Error::Cancelled)));
        assert_eq!(
            session.queued_inputs(),
            [crate::QueuedMessage::from(second)]
        );
        assert!(
            matches!(&session.records().last().unwrap().event, Event::User { input_id: Some(id), .. } if *id == first.id)
        );
    }

    #[tokio::test]
    async fn native_user_message_hooks_preserve_original_images_and_report_rejected_edits() {
        struct ImageHook(&'static str);

        #[async_trait::async_trait]
        impl Extension for ImageHook {
            fn id(&self) -> &str {
                self.0
            }
            fn handles_message_lifecycle(&self) -> bool {
                true
            }
            async fn message_lifecycle(
                &self,
                event: &MessageLifecycleEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<Option<Message>> {
                let MessageLifecycleEvent::End {
                    message: Message::UserWithImages { content, images },
                    status: MessageStatus::Complete,
                } = event
                else {
                    return Ok(None);
                };
                assert_eq!(content, "original");
                let images = if self.0 == "strip" {
                    Vec::new()
                } else {
                    images.clone()
                };
                Ok(Some(Message::UserWithImages {
                    content: "edited".into(),
                    images,
                }))
            }
        }
        let mut registry = Registry::default();
        registry.register(Arc::new(ImageHook("strip"))).unwrap();
        registry.register(Arc::new(ImageHook("edit"))).unwrap();
        let model = TextModel(std::sync::atomic::AtomicUsize::new(0));
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let image = crate::ImageContent {
            mime_type: "image/png".into(),
            data: "iVBORw0KGgo=".into(),
        };
        let id = uuid::Uuid::new_v4();
        let mut errors = 0;
        agent
            .append_prompt_with_images(
                &mut session,
                "original".into(),
                vec![image.clone()],
                id,
                &CancellationToken::new(),
                &mut |notification| {
                    if matches!(
                        notification,
                        Notification::ExtensionLifecycleError {
                            event: "message_end",
                            ..
                        }
                    ) {
                        errors += 1;
                    }
                },
            )
            .await
            .unwrap();
        assert_eq!(errors, 1);
        assert!(
            matches!(&session.records().last().unwrap().event, Event::UserWithImages { content, input_id: Some(input_id), images } if content == "edited" && *input_id == id && images == &[image])
        );
    }

    #[test]
    fn native_message_update_serializes_an_enriched_text_boundary() {
        let event = MessageLifecycleEvent::Update {
            message: assistant("partial".into(), vec![]),
            assistant_message_event: AssistantMessageUpdate::TextStart { content_index: 3 },
        };
        let serialized = serde_json::to_value(event).unwrap();
        assert_eq!(serialized["type"], "message_update");
        assert_eq!(serialized["assistantMessageEvent"]["contentIndex"], 3);
    }
}
