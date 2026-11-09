//! Owned session API for embedding the agent in long-lived native applications.
use std::path::Path;
use std::sync::{Arc, Mutex};

use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

use crate::{
    Agent, CancellationToken, Compaction, Error, Extension, InputKind, Limits, MemoryStore,
    Message, Model, ModelRequest, Notification, Observer, QueuedInput, Record, Registry, Result,
    Session,
};

/// Owned notifications. Request and record payloads can include private context.
#[derive(Clone, Debug)]
pub enum SessionEvent {
    Closed,
    RunStarted { run_id: Uuid },
    Record(Arc<Record>),
    Request(Arc<ModelRequest>),
    Text(String),
    RunEnded { run_id: Uuid, succeeded: bool },
}

#[derive(Clone)]
pub struct SessionControl {
    input: mpsc::Sender<QueuedInput>,
    active: Arc<Mutex<Option<CancellationToken>>>,
}
impl SessionControl {
    pub async fn steer(&self, content: impl Into<String>) -> Result<Uuid> {
        self.enqueue(InputKind::Steer, content.into()).await
    }
    pub async fn follow_up(&self, content: impl Into<String>) -> Result<Uuid> {
        self.enqueue(InputKind::FollowUp, content.into()).await
    }
    async fn enqueue(&self, kind: InputKind, content: String) -> Result<Uuid> {
        if content.trim().is_empty() {
            return Err(Error::Config("queued input is empty".into()));
        }
        let id = Uuid::new_v4();
        self.input
            .send(QueuedInput { id, kind, content })
            .await
            .map_err(|_| Error::Config("session is closed".into()))?;
        Ok(id)
    }
    pub fn abort(&self) {
        if let Some(token) = self.active.lock().expect("session control").as_ref() {
            token.cancel();
        }
    }
}

/// Builds an owned local session while retaining the provider-neutral core APIs.
///
/// ```no_run
/// use std::sync::Arc;
/// use pi_rs_core::{Model, Result, SessionBuilder};
///
/// async fn chat(model: Arc<dyn Model>) -> Result<()> {
///     let mut session = SessionBuilder::new(model, "my-model")
///         .system_prompt("You are an expert coding assistant.")
///         .open("session.jsonl")?;
///     let _events = session.subscribe();
///     let answer = session.prompt("Explain this project").await?;
///     println!("{answer}");
///     session.close().await
/// }
/// ```
pub struct SessionBuilder {
    model: Arc<dyn Model>,
    model_name: String,
    system_prompt: String,
    registry: Registry,
    limits: Limits,
}
impl SessionBuilder {
    pub fn new(model: Arc<dyn Model>, model_name: impl Into<String>) -> Self {
        Self {
            model,
            model_name: model_name.into(),
            system_prompt: "You are a coding assistant running inside pi, a coding agent harness. Help users inspect files, run commands, and modify code.".into(),
            registry: Registry::default(),
            limits: Limits::default(),
        }
    }
    pub fn system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.system_prompt = prompt.into();
        self
    }
    pub fn registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
    pub fn extension(mut self, extension: Arc<dyn Extension>) -> Result<Self> {
        self.registry.register(extension)?;
        Ok(self)
    }
    pub fn open(self, path: impl AsRef<Path>) -> Result<AgentSession> {
        self.build(Session::open(path)?)
    }
    pub fn in_memory(self) -> Result<AgentSession> {
        self.build(Session::with_store(MemoryStore::default())?)
    }
    pub fn build(self, session: Session) -> Result<AgentSession> {
        if session.host_binding().is_some() {
            return Err(Error::Session(
                "managed sessions must be opened through their host".into(),
            ));
        }
        if self.model_name.trim().is_empty() {
            return Err(Error::Config("model name is empty".into()));
        }
        let (sender, input) = mpsc::channel(32);
        let (events, _) = broadcast::channel(64);
        Ok(AgentSession {
            model: self.model,
            model_name: self.model_name,
            system_prompt: self.system_prompt,
            registry: Arc::new(self.registry),
            limits: self.limits,
            session,
            input,
            events: Some(events),
            control: SessionControl {
                input: sender,
                active: Arc::new(Mutex::new(None)),
            },
            closed: false,
        })
    }
}

/// A session owns its model, registry and journal for its entire lifetime.
/// Use cloned controls while `prompt` is awaited, and call `close` for asynchronous cleanup.
pub struct AgentSession {
    model: Arc<dyn Model>,
    model_name: String,
    system_prompt: String,
    registry: Arc<Registry>,
    limits: Limits,
    session: Session,
    input: mpsc::Receiver<QueuedInput>,
    control: SessionControl,
    events: Option<broadcast::Sender<SessionEvent>>,
    closed: bool,
}
impl AgentSession {
    pub fn controls(&self) -> SessionControl {
        self.control.clone()
    }
    /// A slow subscriber receives a broadcast lag error instead of blocking execution.
    pub fn subscribe(&self) -> broadcast::Receiver<SessionEvent> {
        match &self.events {
            Some(events) => events.subscribe(),
            None => {
                let (sender, receiver) = broadcast::channel(1);
                drop(sender);
                receiver
            }
        }
    }
    pub fn journal(&self) -> &Session {
        &self.session
    }
    pub fn messages(&self) -> Result<Vec<Message>> {
        self.session.conversation_at(self.session.head())
    }
    pub fn tools(&self) -> Vec<crate::ToolSpec> {
        self.registry.tool_specs()
    }
    pub fn pending_inputs(&self) -> usize {
        self.input.len()
    }
    pub fn model_name(&self) -> &str {
        &self.model_name
    }

    pub fn set_model(&mut self, model: Arc<dyn Model>, name: impl Into<String>) -> Result<()> {
        self.ensure_open()?;
        let name = name.into();
        let count = self.session.records().len();
        self.session.select_model(&name, None)?;
        if count != self.session.records().len() {
            self.emit_last();
        }
        self.model = model;
        self.model_name = name;
        Ok(())
    }
    pub fn set_system_prompt(&mut self, prompt: impl Into<String>) -> Result<()> {
        self.ensure_open()?;
        self.system_prompt = prompt.into();
        Ok(())
    }
    /// Register before forking. Forks share the existing registry and its connections.
    pub fn register_extension(&mut self, extension: Arc<dyn Extension>) -> Result<()> {
        self.ensure_open()?;
        Arc::get_mut(&mut self.registry)
            .ok_or_else(|| {
                Error::Config("register extensions before sharing this session with a fork".into())
            })?
            .register(extension)
    }
    pub fn checkout(&mut self, record: u64) -> Result<()> {
        self.ensure_open()?;
        self.session.checkout(record)?;
        self.emit_last();
        if let Some((model, _)) = self.session.model_selection() {
            self.model_name = model;
        }
        Ok(())
    }
    pub fn set_name(&mut self, name: &str) -> Result<()> {
        self.ensure_open()?;
        self.session.set_name(name)?;
        self.emit_last();
        Ok(())
    }
    /// Changes future model context while retaining the original transcript and effects.
    pub fn edit_context(
        &mut self,
        target: u64,
        replacement: Option<crate::ContextContent>,
    ) -> Result<()> {
        self.ensure_open()?;
        self.session.edit_context(target, replacement)?;
        self.emit_last();
        Ok(())
    }
    pub fn set_label(&mut self, target: u64, label: Option<String>) -> Result<()> {
        self.ensure_open()?;
        self.session.set_label(target, label)?;
        self.emit_last();
        Ok(())
    }
    pub fn fork_at(&self, record: u64) -> Result<Self> {
        self.ensure_open()?;
        let session = Session::from_records(self.session.fork_records(record)?)?;
        self.fork_session(session)
    }
    pub fn fork_to(&self, record: u64, destination: impl AsRef<Path>) -> Result<Self> {
        self.ensure_open()?;
        self.fork_session(self.session.fork_to(record, destination)?)
    }
    pub fn export_jsonl(&self, destination: impl AsRef<Path>) -> Result<()> {
        self.ensure_open()?;
        self.session.export_jsonl(destination)
    }
    fn fork_session(&self, session: Session) -> Result<Self> {
        let model_name = session
            .model_selection()
            .map(|selection| selection.0)
            .unwrap_or_else(|| self.model_name.clone());
        let mut fork = SessionBuilder::new(self.model.clone(), model_name)
            .system_prompt(self.system_prompt.clone())
            .limits(self.limits.clone())
            .build(session)?;
        fork.registry = self.registry.clone();
        Ok(fork)
    }

    pub async fn prompt(&mut self, input: impl Into<String>) -> Result<String> {
        self.run(Some(input.into()), Uuid::new_v4(), &mut |_| {})
            .await
    }
    /// Submit text and images as one durable user message.
    pub async fn prompt_with_images(
        &mut self,
        text: impl Into<String>,
        images: Vec<crate::ImageContent>,
    ) -> Result<String> {
        self.run_input(Some(text.into()), Some(images), Uuid::new_v4(), &mut |_| {})
            .await
    }
    pub async fn prompt_with_observer(
        &mut self,
        input: impl Into<String>,
        observer: &mut Observer<'_>,
    ) -> Result<String> {
        self.run(Some(input.into()), Uuid::new_v4(), observer).await
    }
    pub async fn resume(&mut self) -> Result<String> {
        self.run(None, Uuid::new_v4(), &mut |_| {}).await
    }
    /// A duplicate run ID follows the core's durable idempotency rules.
    pub async fn run(
        &mut self,
        input: Option<String>,
        run_id: Uuid,
        observer: &mut Observer<'_>,
    ) -> Result<String> {
        self.run_input(input, None, run_id, observer).await
    }
    async fn run_input(
        &mut self,
        mut input: Option<String>,
        images: Option<Vec<crate::ImageContent>>,
        run_id: Uuid,
        observer: &mut Observer<'_>,
    ) -> Result<String> {
        self.ensure_open()?;
        let active = self.begin_run();
        let events = self.events.as_ref().expect("open session").clone();
        let _ = events.send(SessionEvent::RunStarted { run_id });
        let mut observe = |event: Notification<'_>| {
            emit(&events, &event);
            observer(event);
        };
        let agent = Agent {
            model: self.model.as_ref(),
            model_name: &self.model_name,
            registry: &self.registry,
            system_prompt: &self.system_prompt,
            limits: self.limits.clone(),
        };
        let result = async {
            if let Some(images) = images {
                self.session.append_user_with_images(
                    input.take().unwrap_or_default(),
                    images,
                    Some(run_id),
                )?;
                observe(Notification::Committed(
                    self.session
                        .records()
                        .last()
                        .expect("appended user message"),
                ));
            }
            agent
                .run_correlated(
                    &mut self.session,
                    run_id,
                    input,
                    &mut self.input,
                    &active.token,
                    &mut observe,
                )
                .await
        }
        .await;
        let _ = events.send(SessionEvent::RunEnded {
            run_id,
            succeeded: result.is_ok(),
        });
        result
    }
    pub fn extension_command(
        &mut self,
        extension: &str,
        command: &str,
        arguments: &str,
    ) -> Result<()> {
        self.ensure_open()?;
        let events = self.events.as_ref().expect("open session").clone();
        let agent = Agent {
            model: self.model.as_ref(),
            model_name: &self.model_name,
            registry: &self.registry,
            system_prompt: &self.system_prompt,
            limits: self.limits.clone(),
        };
        agent.extension_command(
            &mut self.session,
            extension,
            command,
            arguments,
            &mut |event| emit(&events, &event),
        )
    }
    pub fn compact(&mut self, compaction: Compaction) -> Result<()> {
        self.ensure_open()?;
        let events = self.events.as_ref().expect("open session").clone();
        let agent = Agent {
            model: self.model.as_ref(),
            model_name: &self.model_name,
            registry: &self.registry,
            system_prompt: &self.system_prompt,
            limits: self.limits.clone(),
        };
        agent.compact(&mut self.session, compaction, &mut |event| {
            emit(&events, &event)
        })
    }
    pub async fn summarize(&mut self, keep_turns: usize) -> Result<()> {
        self.ensure_open()?;
        let active = self.begin_run();
        let events = self.events.as_ref().expect("open session").clone();
        let agent = Agent {
            model: self.model.as_ref(),
            model_name: &self.model_name,
            registry: &self.registry,
            system_prompt: &self.system_prompt,
            limits: self.limits.clone(),
        };
        agent
            .summarize(&mut self.session, keep_turns, &active.token, &mut |event| {
                emit(&events, &event)
            })
            .await
    }
    pub async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.control.abort();
        self.input.close();
        while self.input.try_recv().is_ok() {}
        let registry = std::mem::replace(&mut self.registry, Arc::new(Registry::default()));
        let result = if Arc::strong_count(&registry) == 1 {
            registry.shutdown().await
        } else {
            Ok(())
        };
        if let Some(events) = self.events.take() {
            let _ = events.send(SessionEvent::Closed);
        }
        result
    }
    fn emit_last(&self) {
        if let (Some(events), Some(record)) = (&self.events, self.session.records().last()) {
            emit(events, &Notification::Committed(record));
        }
    }
    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            Err(Error::Config("session is closed".into()))
        } else {
            Ok(())
        }
    }
    fn begin_run(&self) -> ActiveRun {
        let token = CancellationToken::new();
        *self.control.active.lock().expect("session control") = Some(token.clone());
        ActiveRun {
            token,
            state: self.control.active.clone(),
        }
    }
}
impl Drop for AgentSession {
    fn drop(&mut self) {
        self.control.abort();
    }
}
struct ActiveRun {
    token: CancellationToken,
    state: Arc<Mutex<Option<CancellationToken>>>,
}
impl Drop for ActiveRun {
    fn drop(&mut self) {
        self.token.cancel();
        *self.state.lock().expect("session control") = None;
    }
}
fn emit(events: &broadcast::Sender<SessionEvent>, notification: &Notification<'_>) {
    if events.receiver_count() == 0 {
        return;
    }
    let event = match notification {
        Notification::Committed(record) => SessionEvent::Record(Arc::new((*record).clone())),
        Notification::Request(request) => SessionEvent::Request(Arc::new((*request).clone())),
        Notification::Text(text) => SessionEvent::Text((*text).into()),
    };
    let _ = events.send(event);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ModelEvent, ModelStream};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct EchoModel(AtomicUsize);
    #[async_trait::async_trait]
    impl Model for EchoModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            let count = self.0.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Text(format!("answer {count}"))),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])))
        }
    }
    struct ClosingExtension(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Extension for ClosingExtension {
        fn id(&self) -> &str {
            "closing-test"
        }
        async fn shutdown(&self) -> crate::ExtensionResult<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    #[tokio::test]
    async fn session_owns_history_and_events_and_consumes_queued_followups() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model, "echo").in_memory().unwrap();
        let mut events = session.subscribe();
        session
            .controls()
            .follow_up("second question")
            .await
            .unwrap();
        assert_eq!(session.prompt("first question").await.unwrap(), "answer 2");
        assert_eq!(
            session
                .messages()
                .unwrap()
                .iter()
                .filter(|message| matches!(message, Message::User { .. }))
                .count(),
            2
        );
        assert_eq!(session.pending_inputs(), 0);
        assert!(matches!(
            events.try_recv().unwrap(),
            SessionEvent::RunStarted { .. }
        ));
        session.close().await.unwrap();
        let mut closed = false;
        while let Ok(event) = events.try_recv() {
            closed |= matches!(event, SessionEvent::Closed);
        }
        assert!(closed);
        assert!(matches!(
            events.try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
        assert!(session.prompt("after close").await.is_err());
        assert!(session.controls().steer("after close").await.is_err());
    }
    #[tokio::test]
    async fn persistent_fork_and_export_reopen_without_overwriting() {
        let directory = tempfile::tempdir().unwrap();
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model, "echo").in_memory().unwrap();
        session.prompt("first").await.unwrap();
        let export = directory.path().join("export.jsonl");
        session.export_jsonl(&export).unwrap();
        assert!(session.export_jsonl(&export).is_err());
        let reopened = Session::open(&export).unwrap();
        assert_eq!(
            reopened.conversation_at(reopened.head()).unwrap(),
            session.messages().unwrap()
        );
        let path = directory.path().join("fork.jsonl");
        let mut fork = session.fork_to(session.journal().head(), &path).unwrap();
        assert_ne!(fork.journal().session_id(), session.journal().session_id());
        assert!(session.fork_to(session.journal().head(), &path).is_err());
        fork.prompt("second").await.unwrap();
        fork.close().await.unwrap();
        drop(fork);
        let reopened = Session::open(&path).unwrap();
        assert_eq!(reopened.conversation_at(reopened.head()).unwrap().len(), 4);
        assert_eq!(session.messages().unwrap().len(), 2);
    }
    #[tokio::test]
    async fn shared_extensions_close_only_after_the_last_fork() {
        let closed = Arc::new(AtomicUsize::new(0));
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model, "echo")
            .extension(Arc::new(ClosingExtension(closed.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        session.prompt("first").await.unwrap();
        let mut fork = session.fork_at(session.journal().head()).unwrap();
        assert_eq!(fork.messages().unwrap(), session.messages().unwrap());
        assert_ne!(fork.journal().session_id(), session.journal().session_id());
        session.close().await.unwrap();
        assert_eq!(closed.load(Ordering::SeqCst), 0);
        assert_eq!(fork.prompt("continue fork").await.unwrap(), "answer 2");
        fork.close().await.unwrap();
        assert_eq!(closed.load(Ordering::SeqCst), 1);
        fork.close().await.unwrap();
        assert_eq!(closed.load(Ordering::SeqCst), 1);
    }
    struct TaskModel;
    #[async_trait::async_trait]
    impl Model for TaskModel {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
            let finished = request.messages.iter().any(|message| matches!(message, Message::Tool { content, .. } if content == "job result"));
            let calls = if finished {
                vec![]
            } else {
                vec![crate::ToolCall {
                    id: "job-call".into(),
                    name: "job".into(),
                    arguments: "{}".into(),
                }]
            };
            let mut events = Vec::new();
            if finished {
                events.push(Ok(ModelEvent::Text("done".into())));
            }
            events.push(Ok(ModelEvent::Complete { calls, usage: None }));
            Ok(Box::pin(futures_util::stream::iter(events)))
        }
    }
    struct PollingJob {
        dispatched: AtomicUsize,
        polls: AtomicUsize,
        started: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl crate::Tool for PollingJob {
        fn can_poll(&self) -> bool {
            true
        }
        fn spec(&self) -> crate::ToolSpec {
            crate::ToolSpec {
                name: "job".into(),
                description: "Test job".into(),
                parameters: serde_json::json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: crate::ToolContext) -> crate::Outcome {
            self.dispatched.fetch_add(1, Ordering::SeqCst);
            crate::Outcome::Pending {
                reference: "job-1".into(),
            }
        }
        async fn poll(
            &self,
            reference: &str,
            context: crate::ToolContext,
        ) -> Option<crate::Outcome> {
            assert_eq!(reference, "job-1");
            if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.started.notify_one();
                context.cancellation.cancelled().await;
                return None;
            }
            Some(crate::Outcome::Succeeded("job result".into()))
        }
    }
    struct JobExtension(Arc<PollingJob>);
    impl Extension for JobExtension {
        fn id(&self) -> &str {
            "job-extension"
        }
        fn tools(&self) -> Vec<Arc<dyn crate::Tool>> {
            vec![self.0.clone()]
        }
    }
    #[tokio::test]
    async fn pending_job_survives_cancel_and_reopen_without_redispatch() {
        let job = Arc::new(PollingJob {
            dispatched: AtomicUsize::new(0),
            polls: AtomicUsize::new(0),
            started: tokio::sync::Notify::new(),
        });
        let mut session = SessionBuilder::new(Arc::new(TaskModel), "task")
            .extension(Arc::new(JobExtension(job.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        let control = session.controls();
        let (result, ()) = tokio::join!(session.prompt("start job"), async {
            job.started.notified().await;
            control.abort();
        });
        assert!(matches!(result, Err(Error::Cancelled)));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pending.jsonl");
        session.export_jsonl(&path).unwrap();
        session.close().await.unwrap();
        drop(session);
        let mut session = SessionBuilder::new(Arc::new(TaskModel), "task")
            .extension(Arc::new(JobExtension(job.clone())))
            .unwrap()
            .open(path)
            .unwrap();
        assert_eq!(session.resume().await.unwrap(), "done");
        assert_eq!(job.dispatched.load(Ordering::SeqCst), 1);
        assert_eq!(job.polls.load(Ordering::SeqCst), 2);
        session.close().await.unwrap();
    }
    struct NestedModel;
    #[async_trait::async_trait]
    impl Model for NestedModel {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
            let finished = request.messages.iter().any(|message| matches!(message, Message::Tool { content, .. } if content == "child result"));
            let events = if finished {
                assert_eq!(
                    request
                        .messages
                        .iter()
                        .filter(|message| matches!(message, Message::Tool { .. }))
                        .count(),
                    1
                );
                vec![
                    Ok(ModelEvent::Text("done".into())),
                    Ok(ModelEvent::Complete {
                        calls: vec![],
                        usage: None,
                    }),
                ]
            } else {
                vec![Ok(ModelEvent::Complete {
                    calls: vec![crate::ToolCall {
                        id: "bundle-call".into(),
                        name: "bundle".into(),
                        arguments: "{}".into(),
                    }],
                    usage: None,
                })]
            };
            Ok(Box::pin(futures_util::stream::iter(events)))
        }
    }
    struct Bundle;
    #[async_trait::async_trait]
    impl crate::Tool for Bundle {
        fn spec(&self) -> crate::ToolSpec {
            crate::ToolSpec {
                name: "bundle".into(),
                description: "Orchestrate tools".into(),
                parameters: serde_json::json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: crate::ToolContext) -> crate::Outcome {
            crate::Outcome::Failed("host required".into())
        }
        async fn invoke_with_host(
            &self,
            _: serde_json::Value,
            _: crate::ToolContext,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::Outcome {
            let result = host.call("child", serde_json::json!({"value":"ok"})).await;
            assert!(matches!(
                host.call("blocked", serde_json::json!({"value":"ok"}))
                    .await,
                crate::Outcome::Skipped(_)
            ));
            assert!(matches!(
                host.call("child", serde_json::json!({})).await,
                crate::Outcome::Skipped(_)
            ));
            result
        }
    }
    struct Child {
        name: &'static str,
        count: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl crate::Tool for Child {
        fn spec(&self) -> crate::ToolSpec {
            crate::ToolSpec {
                name: self.name.into(),
                description: "Child".into(),
                parameters: serde_json::json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: crate::ToolContext) -> crate::Outcome {
            self.count.fetch_add(1, Ordering::SeqCst);
            crate::Outcome::Succeeded("child result".into())
        }
    }
    struct BundleExtension(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Extension for BundleExtension {
        fn id(&self) -> &str {
            "bundle-extension"
        }
        fn tools(&self) -> Vec<Arc<dyn crate::Tool>> {
            vec![
                Arc::new(Bundle),
                Arc::new(Child {
                    name: "child",
                    count: self.0.clone(),
                }),
                Arc::new(Child {
                    name: "blocked",
                    count: self.0.clone(),
                }),
            ]
        }
        async fn before_tool(
            &self,
            name: &str,
            _: &serde_json::Value,
            _: &crate::ToolContext,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<crate::Decision> {
            Ok(if name == "blocked" {
                crate::Decision::Deny("blocked".into())
            } else {
                crate::Decision::Allow
            })
        }
    }
    #[tokio::test]
    async fn nested_tools_are_journaled_and_cannot_bypass_policy_or_schema() {
        let count = Arc::new(AtomicUsize::new(0));
        let mut session = SessionBuilder::new(Arc::new(NestedModel), "nested")
            .extension(Arc::new(BundleExtension(count.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        assert_eq!(session.prompt("run bundle").await.unwrap(), "done");
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let plans = session
            .journal()
            .records()
            .iter()
            .filter(|record| matches!(record.event, crate::Event::ToolPlanned { .. }))
            .count();
        assert_eq!(plans, 3);
        assert_eq!(session.journal().operations().len(), 4);
        assert!(
            session
                .journal()
                .operations()
                .iter()
                .all(|(_, state)| matches!(state, crate::OperationState::Finished(_)))
        );
        session.close().await.unwrap();
    }
    struct ParallelBundle;
    #[async_trait::async_trait]
    impl crate::Tool for ParallelBundle {
        fn spec(&self) -> crate::ToolSpec {
            Bundle.spec()
        }
        async fn invoke(&self, _: serde_json::Value, _: crate::ToolContext) -> crate::Outcome {
            crate::Outcome::Failed("host required".into())
        }
        async fn invoke_with_host(
            &self,
            _: serde_json::Value,
            _: crate::ToolContext,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::Outcome {
            let results = futures_util::future::join_all(
                (0..2).map(|_| host.call("barrier", serde_json::json!({}))),
            )
            .await;
            assert!(
                results
                    .iter()
                    .all(|outcome| matches!(outcome, crate::Outcome::Succeeded(_)))
            );
            crate::Outcome::Succeeded("child result".into())
        }
    }
    struct BarrierTool(Arc<tokio::sync::Barrier>);
    #[async_trait::async_trait]
    impl crate::Tool for BarrierTool {
        fn spec(&self) -> crate::ToolSpec {
            crate::ToolSpec {
                name: "barrier".into(),
                description: "Parallel read".into(),
                parameters: serde_json::json!({"type":"object"}),
            }
        }
        fn execution(&self) -> crate::ToolExecution {
            crate::ToolExecution::ParallelRead
        }
        async fn invoke(&self, _: serde_json::Value, _: crate::ToolContext) -> crate::Outcome {
            self.0.wait().await;
            crate::Outcome::Succeeded("read".into())
        }
    }
    struct ParallelExtension;
    impl Extension for ParallelExtension {
        fn id(&self) -> &str {
            "parallel-bundle"
        }
        fn tools(&self) -> Vec<Arc<dyn crate::Tool>> {
            vec![
                Arc::new(ParallelBundle),
                Arc::new(BarrierTool(Arc::new(tokio::sync::Barrier::new(2)))),
            ]
        }
    }
    #[tokio::test]
    async fn nested_parallel_reads_can_progress_together() {
        let mut session = SessionBuilder::new(Arc::new(NestedModel), "nested")
            .extension(Arc::new(ParallelExtension))
            .unwrap()
            .in_memory()
            .unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            session.prompt("parallel"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result, "done");
        assert_eq!(session.journal().operations().len(), 3);
        session.close().await.unwrap();
    }
    struct BlockingChild {
        started: Arc<tokio::sync::Notify>,
        calls: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl crate::Tool for BlockingChild {
        fn spec(&self) -> crate::ToolSpec {
            Child {
                name: "child",
                count: self.calls.clone(),
            }
            .spec()
        }
        async fn invoke(
            &self,
            _: serde_json::Value,
            context: crate::ToolContext,
        ) -> crate::Outcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            context.cancellation.cancelled().await;
            crate::Outcome::Unknown("cancelled after starting".into())
        }
    }
    struct BlockingExtension {
        started: Arc<tokio::sync::Notify>,
        calls: Arc<AtomicUsize>,
    }
    impl Extension for BlockingExtension {
        fn id(&self) -> &str {
            "blocking-bundle"
        }
        fn tools(&self) -> Vec<Arc<dyn crate::Tool>> {
            vec![
                Arc::new(Bundle),
                Arc::new(BlockingChild {
                    started: self.started.clone(),
                    calls: self.calls.clone(),
                }),
            ]
        }
    }
    #[tokio::test]
    async fn cancelling_nested_dispatch_records_unknown_and_blocks_replay() {
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let mut session = SessionBuilder::new(Arc::new(NestedModel), "nested")
            .extension(Arc::new(BlockingExtension {
                started: started.clone(),
                calls: calls.clone(),
            }))
            .unwrap()
            .in_memory()
            .unwrap();
        let controls = session.controls();
        let (result, ()) = tokio::join!(session.prompt("cancel"), async {
            started.notified().await;
            controls.abort();
        });
        assert!(matches!(
            result,
            Err(Error::Reconciliation(_)) | Err(Error::Cancelled)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(session.journal().operations().len(), 2);
        assert!(
            session
                .journal()
                .operations()
                .iter()
                .all(|(_, state)| matches!(
                    state,
                    crate::OperationState::Finished(crate::Outcome::Unknown(_))
                ))
        );
        assert!(matches!(
            session.resume().await,
            Err(Error::Reconciliation(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        session.close().await.unwrap();
    }
    struct StatefulBundle;
    #[async_trait::async_trait]
    impl crate::Tool for StatefulBundle {
        fn spec(&self) -> crate::ToolSpec {
            Bundle.spec()
        }
        async fn invoke(&self, _: serde_json::Value, _: crate::ToolContext) -> crate::Outcome {
            crate::Outcome::Succeeded("child result".into())
        }
        async fn invoke_transaction(
            &self,
            _: serde_json::Value,
            _: crate::ToolContext,
            _: Option<&crate::ExtensionState>,
            _: Arc<dyn crate::ToolHost>,
        ) -> crate::ToolReply {
            crate::ToolReply {
                outcome: crate::Outcome::Succeeded("child result".into()),
                state: Some(crate::ExtensionState {
                    version: 1,
                    data: serde_json::json!({"secret":"private state"}),
                }),
            }
        }
    }
    struct StatefulExtension;
    impl Extension for StatefulExtension {
        fn id(&self) -> &str {
            "stateful"
        }
        fn tools(&self) -> Vec<Arc<dyn crate::Tool>> {
            vec![Arc::new(StatefulBundle)]
        }
        fn after_tool(
            &self,
            _: &str,
            _: &crate::Outcome,
            state: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<Option<crate::ExtensionState>> {
            assert_eq!(state.unwrap().data["secret"], "private state");
            Ok(None)
        }
    }
    #[tokio::test]
    async fn tool_state_commits_with_output_and_stays_out_of_model_messages() {
        let mut session = SessionBuilder::new(Arc::new(NestedModel), "stateful")
            .extension(Arc::new(StatefulExtension))
            .unwrap()
            .in_memory()
            .unwrap();
        assert_eq!(session.prompt("save state").await.unwrap(), "done");
        assert_eq!(
            session.journal().extension_states()["stateful"].data["secret"],
            "private state"
        );
        assert!(
            !serde_json::to_string(&session.messages().unwrap())
                .unwrap()
                .contains("private state")
        );
        assert!(session.journal().records().iter().any(|record| matches!(&record.event, crate::Event::ToolFinished { states, outcome: crate::Outcome::Succeeded(_), .. } if states.contains_key("stateful"))));
        session.close().await.unwrap();
    }
    struct WaitingModel;
    #[async_trait::async_trait]
    impl Model for WaitingModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            Ok(Box::pin(futures_util::stream::pending()))
        }
    }
    #[tokio::test]
    async fn cloned_control_can_abort_a_running_session() {
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "wait")
            .in_memory()
            .unwrap();
        let control = session.controls();
        let mut events = session.subscribe();
        let abort = async move {
            while let Ok(event) = events.recv().await {
                if matches!(event, SessionEvent::Request(_)) {
                    control.abort();
                    break;
                }
            }
        };
        let (result, ()) = tokio::join!(session.prompt("wait for abort"), abort);
        assert!(matches!(result, Err(Error::Cancelled)));
        session.close().await.unwrap();
    }
    struct ImageModel;
    #[async_trait::async_trait]
    impl Model for ImageModel {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
            assert!(request.messages.iter().any(|message| matches!(message,
                Message::UserWithImages { content, images } if content == "describe" && images[0].data == "aW1hZ2U=")));
            Ok(Box::pin(futures_util::stream::iter(vec![
                Ok(ModelEvent::Text("image accepted".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])))
        }
    }
    #[tokio::test]
    async fn image_input_is_atomic_and_survives_a_fork() {
        let mut session = SessionBuilder::new(Arc::new(ImageModel), "image-model")
            .in_memory()
            .unwrap();
        let answer = session
            .prompt_with_images(
                "describe",
                vec![crate::ImageContent {
                    data: "aW1hZ2U=".into(),
                    mime_type: "image/png".into(),
                }],
            )
            .await
            .unwrap();
        assert_eq!(answer, "image accepted");
        session
            .prompt_with_images(
                "describe",
                vec![crate::ImageContent {
                    data: "aW1hZ2U=".into(),
                    mime_type: "image/png".into(),
                }],
            )
            .await
            .unwrap();
        assert_eq!(
            session
                .journal()
                .records()
                .iter()
                .filter(|record| matches!(record.event, crate::Event::Assistant { .. }))
                .count(),
            2
        );
        assert_eq!(
            session
                .journal()
                .records()
                .iter()
                .filter(|record| matches!(record.event, crate::Event::UserWithImages { .. }))
                .count(),
            2
        );
        let mut fork = session.fork_at(session.journal().head()).unwrap();
        assert!(fork.messages().unwrap().iter().any(
            |message| matches!(message, Message::UserWithImages { images, .. } if images.len() == 1)
        ));
        let count = session.journal().records().len();
        assert!(
            session
                .prompt_with_images(
                    "bad",
                    vec![crate::ImageContent {
                        data: "invalid".into(),
                        mime_type: "text/plain".into()
                    }]
                )
                .await
                .is_err()
        );
        assert_eq!(session.journal().records().len(), count);
        fork.close().await.unwrap();
        session.close().await.unwrap();
    }
}
