//! Owned session API for embedding the agent in long-lived native applications.
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

use crate::{
    Agent, CancellationToken, Compaction, Error, Extension, InputKind, Limits, MemoryStore,
    Message, Model, ModelRequest, Notification, Observer, QueuedInput, QueuedMessage, Record,
    Registry, Result, Session,
};

/// Owned notifications. Request and record payloads can include private context.
#[derive(Clone, Debug)]
pub enum SessionEvent {
    Closed,
    RunStarted {
        run_id: Uuid,
    },
    Record(Arc<Record>),
    SessionInfoChanged {
        name: Option<String>,
    },
    Request(Arc<ModelRequest>),
    Text(String),
    Reasoning(String),
    ToolCallDelta(crate::ToolCallDelta),
    ExtensionLifecycleError {
        extension: String,
        event: &'static str,
        message: String,
    },
    Retry(crate::RetryEvent),
    QueueUpdated {
        steering: Vec<String>,
        follow_up: Vec<String>,
    },
    RunEnded {
        run_id: Uuid,
        succeeded: bool,
    },
}

/// Result of navigating to a transcript entry through extension hooks.
#[derive(Clone, Debug)]
pub struct TreeNavigationResult {
    pub editor_text: Option<String>,
    pub summary_entry: Option<Box<Record>>,
}

#[derive(Clone)]
pub struct SessionControl {
    input: mpsc::Sender<crate::AgentInput>,
    active: Arc<Mutex<Option<CancellationToken>>>,
    auto_compaction: crate::AutoCompactionControl,
    retry: crate::RetryControl,
    input_hooks: Arc<Mutex<InputHooks>>,
    input_order: Arc<tokio::sync::Mutex<()>>,
    events: broadcast::WeakSender<SessionEvent>,
    tool_timeout: std::time::Duration,
}

struct InputMessageHost {
    input: mpsc::Sender<crate::AgentInput>,
}
#[async_trait::async_trait]
impl crate::ToolHost for InputMessageHost {
    fn tools(&self) -> Vec<crate::ToolSpec> {
        Vec::new()
    }
    fn supports_custom_messages(&self) -> bool {
        true
    }
    async fn call(&self, _: &str, _: serde_json::Value) -> crate::Outcome {
        crate::Outcome::Skipped("tool calls unavailable in input hooks".into())
    }
    async fn send_message(
        &self,
        message: crate::CustomMessage,
        delivery: crate::CustomMessageDelivery,
    ) -> crate::ExtensionResult<()> {
        crate::journal::validate_custom_message(&message).map_err(|error| error.to_string())?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.input
            .send(crate::AgentInput::EnqueueCustom {
                message: crate::QueuedCustomMessage {
                    id: Uuid::new_v4(),
                    message,
                    delivery,
                },
                reply,
            })
            .await
            .map_err(|_| "session is closed".to_owned())?;
        receive
            .await
            .map_err(|_| "custom message response lost".to_owned())?
            .map_err(|error| error.to_string())
    }
}

#[derive(Clone, Default)]
struct InputHooks {
    extensions: Vec<Arc<dyn Extension>>,
    lifecycle: Option<Registry>,
    states: crate::ExtensionStates,
}

impl InputHooks {
    fn new(registry: &Registry, session: &Session) -> Self {
        let extensions: Vec<_> = registry
            .extensions
            .iter()
            .filter(|extension| extension.handles_input())
            .cloned()
            .collect();
        let lifecycle = registry
            .has_session_lifecycle_hooks()
            .then(|| registry.clone());
        let states = if extensions.is_empty() && lifecycle.is_none() {
            Default::default()
        } else {
            session.extension_states()
        };
        Self {
            extensions,
            lifecycle,
            states,
        }
    }
}

impl SessionControl {
    pub fn set_retry_settings(&self, settings: crate::RetrySettings) -> Result<()> {
        if self.input.is_closed() {
            return Err(Error::Config("session is closed".into()));
        }
        self.retry.configure(settings);
        Ok(())
    }
    pub fn abort_retry(&self) -> Result<()> {
        if self.input.is_closed() {
            return Err(Error::Config("session is closed".into()));
        }
        self.retry.abort();
        Ok(())
    }
    pub fn set_auto_compaction_enabled(&self, enabled: bool) -> Result<()> {
        if self.input.is_closed() {
            return Err(Error::Config("session is closed".into()));
        }
        self.auto_compaction.set_enabled(enabled);
        Ok(())
    }
    pub fn set_compaction_tokens(&self, settings: crate::CompactionTokens) -> Result<()> {
        if self.input.is_closed() {
            return Err(Error::Config("session is closed".into()));
        }
        self.auto_compaction.set_token_settings(settings);
        Ok(())
    }
    pub async fn steer(&self, content: impl Into<String>) -> Result<Uuid> {
        self.enqueue(InputKind::Steer, content.into(), Vec::new())
            .await
    }
    pub async fn follow_up(&self, content: impl Into<String>) -> Result<Uuid> {
        self.enqueue(InputKind::FollowUp, content.into(), Vec::new())
            .await
    }
    pub async fn steer_with_images(
        &self,
        content: impl Into<String>,
        images: Vec<crate::ImageContent>,
    ) -> Result<Uuid> {
        self.enqueue(InputKind::Steer, content.into(), images).await
    }
    pub async fn follow_up_with_images(
        &self,
        content: impl Into<String>,
        images: Vec<crate::ImageContent>,
    ) -> Result<Uuid> {
        self.enqueue(InputKind::FollowUp, content.into(), images)
            .await
    }
    async fn enqueue(
        &self,
        kind: InputKind,
        content: String,
        images: Vec<crate::ImageContent>,
    ) -> Result<Uuid> {
        let id = Uuid::new_v4();
        let mut message = QueuedMessage {
            input: QueuedInput { id, kind, content },
            images,
        };
        message.validate()?;
        let _order = self.input_order.lock().await;
        if self.input.is_closed() {
            return Err(Error::Config("session is closed".into()));
        }
        let hooks = self.input_hooks.lock().expect("SDK input hooks").clone();
        if !hooks.extensions.is_empty() {
            let streaming = self
                .active
                .lock()
                .expect("session control")
                .as_ref()
                .is_some_and(|token| !token.is_cancelled());
            let event = crate::InputEvent {
                text: message.input.content,
                images: message.images,
                source: crate::InputSource::Extension,
                streaming_behavior: streaming.then_some(kind),
            };
            let prepared = tokio::select! {
                biased;
                _ = self.input.closed() => return Err(Error::Config("session is closed".into())),
                result = async {
                    if streaming {
                        crate::extension::prepare_input_with_host(&hooks.extensions, event, &hooks.states, Arc::new(InputMessageHost { input: self.input.clone() })).await
                    } else {
                        crate::extension::prepare_input(&hooks.extensions, event, &hooks.states).await
                    }
                } => result?,
            };
            let Some(prepared) = prepared else {
                return Ok(id);
            };
            message.input.content = prepared.text;
            message.images = prepared.images;
            message.validate()?;
        }
        self.input
            .send(message.into())
            .await
            .map_err(|_| Error::Config("session is closed".into()))?;
        Ok(id)
    }
    /// Send custom context during an active operation, acknowledged after journaling.
    pub async fn send_custom_message(
        &self,
        message: crate::CustomMessage,
        delivery: crate::CustomMessageDelivery,
    ) -> Result<()> {
        crate::journal::validate_custom_message(&message)?;
        self.queue_control(|reply| crate::AgentInput::EnqueueCustom {
            message: crate::QueuedCustomMessage {
                id: Uuid::new_v4(),
                message,
                delivery,
            },
            reply,
        })
        .await
    }
    /// Clear a running model operation's queue. Use the owned session method while idle.
    pub async fn clear_queued_inputs(&self) -> Result<Vec<QueuedMessage>> {
        self.queue_control(|reply| crate::AgentInput::ClearQueue { reply })
            .await
    }
    pub async fn set_queue_mode(&self, kind: InputKind, mode: crate::QueueMode) -> Result<()> {
        self.queue_control(|reply| crate::AgentInput::SetQueueMode { kind, mode, reply })
            .await
    }
    /// Save a display name and notify lifecycle extensions during an active operation.
    /// Use the owned session while idle.
    pub async fn set_session_name(&self, name: impl Into<String>) -> Result<()> {
        let name = crate::sanitize_session_name(&name.into());
        self.queue_control(|reply| crate::AgentInput::SetSessionName {
            name: name.clone(),
            reply,
        })
        .await?;
        let hooks = self.input_hooks.lock().expect("SDK input hooks").clone();
        if let Some(registry) = hooks.lifecycle {
            let events = self.events.upgrade();
            crate::lifecycle::notify(
                &registry,
                crate::LifecycleEvent::SessionInfoChanged { name: Some(name) },
                &hooks.states,
                self.tool_timeout,
                &mut |notification| {
                    if let Some(events) = &events {
                        emit(events, &notification);
                    }
                },
            )
            .await;
        }
        Ok(())
    }
    async fn queue_control<T>(
        &self,
        input: impl FnOnce(tokio::sync::oneshot::Sender<Result<T>>) -> crate::AgentInput,
    ) -> Result<T> {
        let (reply, receiver) = tokio::sync::oneshot::channel();
        let token = {
            let _order = self.input_order.lock().await;
            let state = self.active.lock().expect("session control");
            let active = state.as_ref().ok_or_else(|| {
                Error::Config(
                    "queue controls require an active operation; use the owned session while idle"
                        .into(),
                )
            })?;
            if active.is_cancelled() {
                return Err(Error::Cancelled);
            }
            self.input
                .try_send(input(reply))
                .map_err(|_| Error::Config("queue control channel is unavailable".into()))?;
            active.clone()
        };
        tokio::select! {
            biased;
            result = receiver => result.map_err(|_| Error::Config("queue control response was lost".into()))?,
            _ = token.cancelled() => Err(Error::Cancelled),
        }
    }
    pub fn abort(&self) {
        if let Some(active) = self.active.lock().expect("session control").as_ref() {
            active.cancel();
        }
    }

    fn observe(&self, event: &Notification<'_>) {
        let Notification::Committed(record) = event else {
            return;
        };
        let states = match &record.event {
            crate::Event::ExtensionStates { states }
            | crate::Event::Compacted { states, .. }
            | crate::Event::ToolFinished { states, .. } => states,
            _ => return,
        };
        if !states.is_empty() {
            let mut hooks = self.input_hooks.lock().expect("SDK input hooks");
            if !hooks.extensions.is_empty() || hooks.lifecycle.is_some() {
                hooks.states.extend(states.clone());
            }
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
    tool_timeout_explicit: bool,
}
impl SessionBuilder {
    pub fn new(model: Arc<dyn Model>, model_name: impl Into<String>) -> Self {
        Self {
            model,
            model_name: model_name.into(),
            system_prompt: "You are a coding assistant running inside pi, a coding agent harness. Help users inspect files, run commands, and modify code.".into(),
            registry: Registry::default(),
            limits: Limits::default(),
            tool_timeout_explicit: false,
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
        self.tool_timeout_explicit = true;
        self
    }
    pub fn extension(mut self, extension: Arc<dyn Extension>) -> Result<Self> {
        self.registry.register(extension)?;
        Ok(self)
    }
    pub fn open(self, path: impl AsRef<Path>) -> Result<AgentSession> {
        let path = std::path::absolute(path)?;
        let mut session = self.build(Session::open(&path)?)?;
        session.session_file = Some(path);
        Ok(session)
    }
    pub fn ui(mut self, ui: Arc<dyn crate::ExtensionUi>) -> Self {
        self.registry.set_ui(ui);
        self
    }
    pub fn in_memory(self) -> Result<AgentSession> {
        self.build(Session::with_store(MemoryStore::default())?)
    }
    pub fn build(mut self, session: Session) -> Result<AgentSession> {
        self.registry
            .omit_default_tool_timeout(!self.tool_timeout_explicit);
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
        let weak_events = events.downgrade();
        let tool_timeout = self.limits.tool_timeout;
        let auto_compaction = session.auto_compaction_control();
        let retry = session.retry_control();
        let input_hooks = Arc::new(Mutex::new(InputHooks::new(&self.registry, &session)));
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
                auto_compaction,
                retry,
                input_hooks,
                input_order: Arc::new(tokio::sync::Mutex::new(())),
                events: weak_events,
                tool_timeout,
            },
            closed: false,
            pending_start: Some((crate::SessionStartReason::Startup, None)),
            session_file: None,
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
    input: mpsc::Receiver<crate::AgentInput>,
    control: SessionControl,
    events: Option<broadcast::Sender<SessionEvent>>,
    closed: bool,
    pending_start: Option<(crate::SessionStartReason, Option<PathBuf>)>,
    session_file: Option<PathBuf>,
}
impl AgentSession {
    pub fn auto_compaction_enabled(&self) -> bool {
        self.session.auto_compaction_control().enabled()
    }
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
    /// Usage and message counts across the complete durable session.
    pub fn statistics(&self) -> crate::SessionStats {
        self.session.statistics()
    }

    /// Resolve the configured or provider-reported context window within the model timeout.
    pub async fn context_usage(&self) -> Result<Option<crate::ContextUsage>> {
        self.ensure_open()?;
        let window = match self.limits.context_window {
            Some(window) => Some(window),
            None => tokio::time::timeout(
                self.limits.model_timeout,
                self.model.context_window(&self.model_name),
            )
            .await
            .ok()
            .flatten(),
        };
        Ok(window.and_then(|window| self.session.context_usage(window)))
    }

    pub fn tools(&self) -> Vec<crate::ToolSpec> {
        self.registry.tool_specs()
    }
    pub fn pending_inputs(&self) -> usize {
        self.input.len() + self.session.queued_inputs().len()
    }
    /// Clear pending input while this owned session is idle, retaining each removal in the journal.
    pub fn clear_queued_inputs(&mut self) -> Result<Vec<QueuedMessage>> {
        self.ensure_open()?;
        let events = self.events.as_ref().expect("open session").clone();
        crate::agent::queue_inputs(&mut self.session, &mut self.input, &mut |event| {
            emit(&events, &event)
        })?;
        let count = self.session.records().len();
        let pending = self.session.clear_queued_inputs()?;
        if self.session.records().len() > count {
            self.emit_last();
        }
        Ok(pending)
    }
    pub fn set_queue_mode(&mut self, kind: InputKind, mode: crate::QueueMode) -> Result<()> {
        self.ensure_open()?;
        let count = self.session.records().len();
        self.session.set_queue_mode(kind, mode)?;
        if count != self.session.records().len() {
            self.emit_last();
        }
        Ok(())
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

    /// Set the model and await selection hooks after the journal is committed.
    /// Hosts with richer model metadata can provide a Pi-shaped descriptor; its
    /// `id` should identify the same model as `name`.
    pub async fn set_model_with_hooks(
        &mut self,
        model: Arc<dyn Model>,
        name: impl Into<String>,
        descriptor: serde_json::Value,
        previous_descriptor: Option<serde_json::Value>,
    ) -> Result<()> {
        self.start().await?;
        let name = name.into();
        let previous_name = self.model_name.clone();
        let previous_level = self
            .session
            .model_selection()
            .and_then(|(_, level)| level)
            .unwrap_or_else(|| "off".into());
        self.set_model(model, name.clone())?;
        if previous_level != "off" {
            self.notify_selection_event(crate::LifecycleEvent::ThinkingLevelSelect {
                level: "off".into(),
                previous_level,
            })
            .await?;
        }
        let descriptor_changed = previous_descriptor.as_ref().is_some_and(|previous| {
            previous
                .get("provider")
                .zip(descriptor.get("provider"))
                .is_some_and(|(left, right)| left != right)
                || previous
                    .get("id")
                    .zip(descriptor.get("id"))
                    .is_some_and(|(left, right)| left != right)
        });
        if previous_name != name || descriptor_changed {
            self.notify_selection_event(crate::LifecycleEvent::ModelSelect {
                model: descriptor,
                previous_model: previous_descriptor,
                source: crate::ModelSelectSource::Set,
            })
            .await?;
        }
        Ok(())
    }

    async fn notify_selection_event(&mut self, event: crate::LifecycleEvent) -> Result<()> {
        if self.registry.has_model_selection_hooks() {
            let events = self.events.as_ref().expect("open session");
            crate::lifecycle::notify_idle_with_host(
                &self.registry,
                &mut self.session,
                event,
                self.limits.tool_timeout,
                &mut |notification| emit(events, &notification),
            )
            .await?;
        }
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
            .register(extension)?;
        self.refresh_input_hooks();
        Ok(())
    }
    pub fn checkout(&mut self, record: u64) -> Result<()> {
        self.ensure_open()?;
        self.session.checkout(record)?;
        self.refresh_input_hooks();
        self.emit_last();
        if let Some((model, _)) = self.session.model_selection() {
            self.model_name = model;
        }
        Ok(())
    }

    /// Navigate like the terminal tree, retaining a selected user message as editor text.
    /// Controls can abort a pending extension hook or summary request.
    pub async fn navigate_tree(
        &mut self,
        selected: u64,
        options: crate::TreeNavigationOptions<'_>,
    ) -> Result<TreeNavigationResult> {
        self.start().await?;
        let first_record = self.session.records().len();
        let editor_text = self
            .session
            .records()
            .iter()
            .find(|record| record.id == selected && selected != self.session.head())
            .and_then(|record| match &record.event {
                crate::Event::User { content, .. }
                | crate::Event::UserWithImages { content, .. } => Some(content.clone()),
                _ => None,
            });
        let active = self.begin_run();
        let events = self.events.as_ref().expect("open session").clone();
        let agent = Agent {
            model: self.model.as_ref(),
            model_name: &self.model_name,
            registry: &self.registry,
            system_prompt: &self.system_prompt,
            limits: self.limits.clone(),
        };
        let mut observer = |event: Notification<'_>| {
            self.control.observe(&event);
            emit(&events, &event);
        };
        let result = agent
            .navigate_tree_with_options(
                &mut self.session,
                selected,
                options,
                &active.token,
                &mut observer,
            )
            .await;
        if result.is_ok() {
            self.refresh_input_hooks();
            if let Some((model, _)) = self.session.model_selection() {
                self.model_name = model;
            }
        }
        let final_inputs = active.finish_inputs(&mut self.session, &mut self.input, &mut observer);
        result.and(final_inputs)?;
        let summary_entry = self.session.records()[first_record..]
            .iter()
            .find(|record| matches!(record.event, crate::Event::BranchSummary { .. }))
            .cloned()
            .map(Box::new);
        Ok(TreeNavigationResult {
            editor_text,
            summary_entry,
        })
    }
    pub fn set_name(&mut self, name: &str) -> Result<()> {
        self.ensure_open()?;
        self.session.set_name(name)?;
        self.emit_last();
        Ok(())
    }

    /// Save the name and deliver the resulting metadata to session lifecycle extensions.
    pub async fn set_name_with_hooks(&mut self, name: &str) -> Result<()> {
        self.start().await?;
        self.set_name(name)?;
        self.notify_session_event(crate::LifecycleEvent::SessionInfoChanged {
            name: self.session.name().map(str::to_owned),
        })
        .await?;
        Ok(())
    }

    /// Deliver the session start hook once. Model, tree, and hook-aware rename operations also start it.
    pub async fn start(&mut self) -> Result<()> {
        self.ensure_open()?;
        if let Some((reason, previous_session_file)) = self.pending_start.take() {
            self.notify_session_event(crate::LifecycleEvent::SessionStart {
                reason,
                previous_session_file,
            })
            .await?;
        }
        Ok(())
    }

    async fn notify_session_event(&mut self, event: crate::LifecycleEvent) -> Result<()> {
        if self.registry.has_session_lifecycle_hooks() {
            let events = self.events.as_ref().expect("open session");
            crate::lifecycle::notify_idle_with_host(
                &self.registry,
                &mut self.session,
                event,
                self.limits.tool_timeout,
                &mut |notification| emit(events, &notification),
            )
            .await?;
        }
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
    /// Record a user shell result and notify subscribers without requesting a model turn.
    pub fn append_bash_execution(&mut self, execution: crate::BashExecution) -> Result<()> {
        self.ensure_open()?;
        self.session.append_bash_execution(execution)?;
        self.emit_last();
        Ok(())
    }

    /// Persist extension data on the active branch without changing model context.
    pub fn append_custom_entry(&mut self, entry: crate::CustomEntry) -> Result<()> {
        self.ensure_open()?;
        self.session.append_custom_entry(entry)?;
        self.emit_last();
        Ok(())
    }

    /// Persist an extension message in model context without starting a turn.
    pub fn append_custom_message(&mut self, message: crate::CustomMessage) -> Result<()> {
        self.ensure_open()?;
        self.session.append_custom_message(message)?;
        self.emit_last();
        Ok(())
    }

    /// Send a custom message from an idle host, optionally starting a model turn.
    ///
    /// When `trigger_turn` is false, the message is saved for the next turn and
    /// the model is not called. The returned answer is present only when a turn
    /// was requested and completed successfully.
    pub async fn send_custom_message(
        &mut self,
        message: crate::CustomMessage,
        trigger_turn: bool,
    ) -> Result<Option<String>> {
        self.start().await?;
        self.append_custom_message(message)?;
        if trigger_turn {
            let events = self.events.as_ref().expect("open session").clone();
            crate::agent::apply_custom_boundary(
                &mut self.session,
                crate::CustomMessageDelivery::NextTurn,
                &mut |event| emit(&events, &event),
            )?;
            self.resume().await.map(Some)
        } else {
            Ok(None)
        }
    }
    /// Queue custom context for the next run without starting it.
    pub fn queue_custom_message_next_turn(&mut self, message: crate::CustomMessage) -> Result<()> {
        self.ensure_open()?;
        let events = self.events.as_ref().expect("open session").clone();
        crate::agent::queue_custom_message(
            &mut self.session,
            crate::QueuedCustomMessage {
                id: Uuid::new_v4(),
                delivery: crate::CustomMessageDelivery::NextTurn,
                message,
            },
            &mut |event| emit(&events, &event),
        )
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
        let destination = std::path::absolute(destination)?;
        let mut fork = self.fork_session(self.session.fork_to(record, &destination)?)?;
        fork.session_file = Some(destination);
        Ok(fork)
    }
    /// Fork before or at an entry after consulting the source session's extensions.
    pub async fn fork_with_hooks(
        &mut self,
        entry_id: u64,
        position: crate::ForkPosition,
        destination: Option<&Path>,
    ) -> Result<Self> {
        self.ensure_open()?;
        let record = self
            .session
            .records()
            .iter()
            .find(|record| record.id == entry_id)
            .ok_or_else(|| Error::Session("invalid fork entry".into()))?;
        let target = match position {
            crate::ForkPosition::Before => record.parent.unwrap_or(1),
            crate::ForkPosition::At => record.id,
        };
        self.start().await?;
        if self.registry.has_session_transition_hooks() {
            let events = self.events.as_ref().expect("open session");
            if self
                .registry
                .session_transition_cancelled(
                    &crate::BeforeSessionTransition::Fork { entry_id, position },
                    &self.session.extension_states(),
                    self.limits.tool_timeout,
                    &mut |notification| emit(events, &notification),
                )
                .await
            {
                return Err(Error::Cancelled);
            }
        }
        match destination {
            Some(path) => self.fork_to(target, path),
            None => self.fork_at(target),
        }
    }
    pub fn export_jsonl(&self, destination: impl AsRef<Path>) -> Result<()> {
        self.ensure_open()?;
        self.session.export_jsonl(destination)
    }
    fn fork_session(&self, session: Session) -> Result<Self> {
        session
            .retry_control()
            .configure(self.session.retry_control().settings());
        let model_name = session
            .model_selection()
            .map(|selection| selection.0)
            .unwrap_or_else(|| self.model_name.clone());
        session
            .auto_compaction_control()
            .set_enabled(self.auto_compaction_enabled());
        session
            .auto_compaction_control()
            .set_token_settings(self.session.auto_compaction_control().token_settings());
        let mut fork = SessionBuilder::new(self.model.clone(), model_name)
            .system_prompt(self.system_prompt.clone())
            .limits(self.limits.clone())
            .build(session)?;
        fork.registry = self.registry.clone();
        fork.pending_start = Some((crate::SessionStartReason::Fork, self.session_file.clone()));
        fork.refresh_input_hooks();
        Ok(fork)
    }

    pub async fn prompt(&mut self, input: impl Into<String>) -> Result<String> {
        self.run_input(Some(input.into()), None, Uuid::new_v4(), true, &mut |_| {})
            .await
    }
    /// Submit text and images as one durable user message.
    pub async fn prompt_with_images(
        &mut self,
        text: impl Into<String>,
        images: Vec<crate::ImageContent>,
    ) -> Result<String> {
        self.run_input(
            Some(text.into()),
            Some(images),
            Uuid::new_v4(),
            true,
            &mut |_| {},
        )
        .await
    }
    pub async fn prompt_with_observer(
        &mut self,
        input: impl Into<String>,
        observer: &mut Observer<'_>,
    ) -> Result<String> {
        self.run_input(Some(input.into()), None, Uuid::new_v4(), true, observer)
            .await
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
        self.run_input(input, None, run_id, false, observer).await
    }
    async fn run_input(
        &mut self,
        mut input: Option<String>,
        mut images: Option<Vec<crate::ImageContent>>,
        run_id: Uuid,
        prepare: bool,
        observer: &mut Observer<'_>,
    ) -> Result<String> {
        self.start().await?;
        let active = self.begin_run();
        let events = self.events.as_ref().expect("open session").clone();
        let mut started = false;
        let mut observe = |event: Notification<'_>| {
            self.control.observe(&event);
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
        let mut result = async {
            if prepare && self.registry.has_input_hooks() {
                let event = crate::InputEvent {
                    text: input.take().unwrap_or_default(),
                    images: images.take().unwrap_or_default(),
                    source: crate::InputSource::Extension,
                    streaming_behavior: None,
                };
                let states = self.session.extension_states();
                let (sender, mut incoming) = mpsc::channel(32);
                let host = Arc::new(crate::agent::LifecycleMessageHost { sender });
                let prepared = {
                    let future = self.registry.prepare_input_with_host(event, &states, host);
                    tokio::pin!(future);
                    loop {
                        tokio::select! {
                            biased;
                            Some((input, reply)) = incoming.recv() => {
                                let result = if input.delivery == crate::CustomMessageDelivery::NextTurn {
                                    crate::agent::queue_custom_message(&mut self.session, input, &mut observe)
                                } else {
                                    self.session.append_custom_message(input.message).map(|()| {
                                        observe(Notification::Committed(self.session.records().last().expect("appended custom message")));
                                    })
                                };
                                match result { Ok(()) => { let _ = reply.send(Ok(())); }, Err(error) => { let _ = reply.send(Err(error.to_string())); return Err(error); } }
                            }
                            _ = active.token.cancelled() => return Err(Error::Cancelled),
                            result = &mut future => break result?,
                        }
                    }
                };
                let Some(prepared) = prepared else {
                    return Ok(String::new());
                };
                input = Some(prepared.text);
                images = (!prepared.images.is_empty()).then_some(prepared.images);
            }
            started = true;
            let _ = events.send(SessionEvent::RunStarted { run_id });
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
        let final_inputs = active.finish_inputs(&mut self.session, &mut self.input, &mut observe);
        if result.is_ok()
            && let Err(error) = final_inputs
        {
            result = Err(error);
        }
        if started {
            let _ = events.send(SessionEvent::RunEnded {
                run_id,
                succeeded: result.is_ok(),
            });
        }
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
            &mut |event| {
                self.control.observe(&event);
                emit(&events, &event);
            },
        )
    }

    /// Execute an extension command and run the model if its effects request it.
    pub async fn extension_command_and_run(
        &mut self,
        extension: &str,
        command: &str,
        arguments: &str,
    ) -> Result<Option<String>> {
        self.start().await?;
        let events = self.events.as_ref().expect("open session").clone();
        let agent = Agent {
            model: self.model.as_ref(),
            model_name: &self.model_name,
            registry: &self.registry,
            system_prompt: &self.system_prompt,
            limits: self.limits.clone(),
        };
        let trigger = agent.extension_command_with_trigger(
            &mut self.session,
            extension,
            command,
            arguments,
            &mut |event| {
                self.control.observe(&event);
                emit(&events, &event);
            },
        )?;
        if trigger {
            self.resume().await.map(Some)
        } else {
            Ok(None)
        }
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
            self.control.observe(&event);
            emit(&events, &event)
        })
    }
    pub async fn summarize(&mut self, keep_turns: usize) -> Result<()> {
        self.summarize_with_instructions(keep_turns, None).await
    }

    pub async fn summarize_with_instructions(
        &mut self,
        keep_turns: usize,
        instructions: Option<&str>,
    ) -> Result<()> {
        self.summarize_selected(Some(keep_turns), instructions)
            .await
    }

    pub async fn summarize_configured(&mut self, instructions: Option<&str>) -> Result<()> {
        self.summarize_selected(None, instructions).await
    }

    async fn summarize_selected(
        &mut self,
        keep_turns: Option<usize>,
        instructions: Option<&str>,
    ) -> Result<()> {
        self.start().await?;
        let active = self.begin_run();
        let events = self.events.as_ref().expect("open session").clone();
        let agent = Agent {
            model: self.model.as_ref(),
            model_name: &self.model_name,
            registry: &self.registry,
            system_prompt: &self.system_prompt,
            limits: self.limits.clone(),
        };
        let mut observer = |event: Notification<'_>| {
            self.control.observe(&event);
            emit(&events, &event);
        };
        let result = match keep_turns {
            Some(keep_turns) => {
                agent
                    .summarize_with_instructions(
                        &mut self.session,
                        keep_turns,
                        &active.token,
                        &mut observer,
                        Some(&mut self.input),
                        instructions,
                    )
                    .await
            }
            None => {
                agent
                    .summarize_configured(
                        &mut self.session,
                        &active.token,
                        &mut observer,
                        Some(&mut self.input),
                        instructions,
                    )
                    .await
            }
        };
        let final_inputs = active.finish_inputs(&mut self.session, &mut self.input, &mut observer);
        result.and(final_inputs)
    }
    pub async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.control.abort();
        self.input.close();
        *self.control.input_hooks.lock().expect("SDK input hooks") = InputHooks::default();
        while self.input.try_recv().is_ok() {}
        let notification_result = if self.pending_start.is_none() {
            self.notify_session_event(crate::LifecycleEvent::SessionShutdown {
                reason: crate::SessionShutdownReason::Quit,
                target_session_file: None,
            })
            .await
        } else {
            Ok(())
        };
        let registry = std::mem::replace(&mut self.registry, Arc::new(Registry::default()));
        let result = if Arc::strong_count(&registry) == 1 {
            registry.shutdown().await
        } else {
            Ok(())
        };
        if let Some(events) = self.events.take() {
            let _ = events.send(SessionEvent::Closed);
        }
        notification_result.and(result)
    }
    fn emit_last(&self) {
        if let (Some(events), Some(record)) = (&self.events, self.session.records().last()) {
            emit(events, &Notification::Committed(record));
        }
    }
    fn refresh_input_hooks(&self) {
        *self.control.input_hooks.lock().expect("SDK input hooks") =
            InputHooks::new(&self.registry, &self.session);
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
        *self.control.input_hooks.lock().expect("SDK input hooks") = InputHooks::default();
    }
}
struct ActiveRun {
    token: CancellationToken,
    state: Arc<Mutex<Option<CancellationToken>>>,
}
impl ActiveRun {
    fn finish_inputs(
        &self,
        session: &mut Session,
        inputs: &mut mpsc::Receiver<crate::AgentInput>,
        observer: &mut Observer<'_>,
    ) -> Result<()> {
        let first_record = session.records().len();
        let result = {
            let mut state = self.state.lock().expect("session control");
            *state = None;
            crate::agent::queue_inputs(session, inputs, &mut |_| {})
        };
        for record in &session.records()[first_record..] {
            observer(Notification::Committed(record));
        }
        result
    }
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
        Notification::Reasoning(text) => SessionEvent::Reasoning((*text).into()),
        Notification::ToolCallDelta(delta) => SessionEvent::ToolCallDelta((*delta).clone()),
        Notification::ExtensionLifecycleError {
            extension,
            event,
            message,
        } => SessionEvent::ExtensionLifecycleError {
            extension: (*extension).into(),
            event,
            message: (*message).into(),
        },
        Notification::Retry(event) => SessionEvent::Retry(event.clone()),
        Notification::SummaryRequest | Notification::ToolProgress(_) => return,
        Notification::QueueUpdated {
            steering,
            follow_up,
        } => SessionEvent::QueueUpdated {
            steering: steering.iter().map(|text| (*text).to_owned()).collect(),
            follow_up: follow_up.iter().map(|text| (*text).to_owned()).collect(),
        },
    };
    let _ = events.send(event);
    if let Notification::Committed(record) = notification
        && let crate::Event::SessionInfo {
            name: Some(name), ..
        } = &record.event
    {
        let _ = events.send(SessionEvent::SessionInfoChanged {
            name: (!name.is_empty()).then(|| name.clone()),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ModelEvent, ModelStream};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn default_and_explicit_sdk_limits_preserve_public_values() {
        let defaults = Limits::default();
        assert_eq!(defaults.tool_timeout, std::time::Duration::from_secs(30));
        let implicit = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
            .in_memory()
            .unwrap();
        assert!(implicit.registry.omit_default_tool_timeout);
        let explicit = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
            .limits(defaults)
            .in_memory()
            .unwrap();
        assert!(!explicit.registry.omit_default_tool_timeout);
        assert_eq!(
            explicit.limits.tool_timeout,
            std::time::Duration::from_secs(30)
        );
    }

    struct CustomEntryExtension;
    struct CustomMessageExtension;
    impl Extension for CustomMessageExtension {
        fn id(&self) -> &str {
            "message-extension"
        }
        fn commands(&self) -> &[&str] {
            &["context"]
        }
        fn supports_command_effects(&self) -> bool {
            true
        }
        fn command_effects(
            &self,
            _: &str,
            _: &str,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<crate::ExtensionCommandEffects> {
            Ok(crate::ExtensionCommandEffects {
                state: None,
                actions: vec![
                    crate::ExtensionCommandAction::Entry {
                        entry: crate::CustomEntry {
                            custom_type: "before".into(),
                            data: None,
                        },
                    },
                    crate::ExtensionCommandAction::Message {
                        message: crate::CustomMessage {
                            custom_type: "context".into(),
                            content: crate::ContextContent::Text("injected text".into()),
                            display: false,
                            details: Some(serde_json::json!({"private":"not model content"})),
                        },
                    },
                ],
            })
        }
    }

    #[test]
    fn extension_custom_messages_enter_context_and_follow_branch_edits() {
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
            .extension(Arc::new(CustomMessageExtension))
            .unwrap()
            .in_memory()
            .unwrap();
        let previous = session.journal().head();
        session
            .extension_command("message-extension", "context", "")
            .unwrap();
        let records = session.journal().records();
        assert!(matches!(
            records[records.len() - 2].event,
            crate::Event::CustomEntry { .. }
        ));
        assert!(matches!(
            records.last().unwrap().event,
            crate::Event::CustomMessage { .. }
        ));
        let target = session.journal().head();
        let messages = session.messages().unwrap();
        assert!(
            matches!(messages.last(), Some(Message::User { content }) if content == "injected text")
        );
        assert!(
            !serde_json::to_string(&messages)
                .unwrap()
                .contains("not model content")
        );
        session
            .edit_context(
                target,
                Some(crate::ContextContent::Text("replacement".into())),
            )
            .unwrap();
        assert!(
            matches!(session.messages().unwrap().last(), Some(Message::User { content }) if content == "replacement")
        );
        session.checkout(previous).unwrap();
        assert!(session.messages().unwrap().is_empty());
        session
            .append_custom_message(crate::CustomMessage {
                custom_type: "sdk".into(),
                content: crate::ContextContent::Text("sdk context".into()),
                display: true,
                details: None,
            })
            .unwrap();
        assert!(
            matches!(session.messages().unwrap().last(), Some(Message::User { content }) if content == "sdk context")
        );
    }
    impl Extension for CustomEntryExtension {
        fn id(&self) -> &str {
            "bookmark-extension"
        }
        fn commands(&self) -> &[&str] {
            &["bookmark"]
        }
        fn supports_command_entries(&self) -> bool {
            true
        }
        fn command_output(
            &self,
            _: &str,
            arguments: &str,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<crate::ExtensionCommandOutput> {
            Ok(crate::ExtensionCommandOutput {
                state: None,
                entries: vec![crate::CustomEntry {
                    custom_type: "bookmark".into(),
                    data: Some(serde_json::json!({"note":arguments})),
                }],
            })
        }
    }

    #[test]
    fn custom_entries_are_branch_local_and_do_not_enter_model_context() {
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
            .extension(Arc::new(CustomEntryExtension))
            .unwrap()
            .in_memory()
            .unwrap();
        session
            .session
            .append_user_with_images("hello".into(), Vec::new(), None)
            .unwrap();
        let previous = session.journal().head();
        let before = session.messages().unwrap();
        session
            .extension_command("bookmark-extension", "bookmark", "remember")
            .unwrap();
        let bookmarked = session.journal().head();
        assert!(
            matches!(&session.journal().records().last().unwrap().event, crate::Event::CustomEntry { entry } if entry.custom_type == "bookmark" && entry.data.as_ref().unwrap()["note"] == "remember")
        );
        assert_eq!(
            serde_json::to_value(session.messages().unwrap()).unwrap(),
            serde_json::to_value(before).unwrap()
        );
        session.checkout(previous).unwrap();
        assert!(
            !session
                .journal()
                .branch()
                .iter()
                .any(|record| matches!(record.event, crate::Event::CustomEntry { .. }))
        );
        session.checkout(bookmarked).unwrap();
        assert!(
            session
                .journal()
                .branch()
                .iter()
                .any(|record| matches!(record.event, crate::Event::CustomEntry { .. }))
        );
        session
            .append_custom_entry(crate::CustomEntry {
                custom_type: "sdk".into(),
                data: None,
            })
            .unwrap();
        assert!(
            matches!(&session.journal().records().last().unwrap().event, crate::Event::CustomEntry { entry } if entry.custom_type == "sdk")
        );
    }

    struct NameHook(Arc<Mutex<Vec<crate::LifecycleEvent>>>);

    struct SelectionHook(Arc<Mutex<Vec<crate::LifecycleEvent>>>);
    #[async_trait::async_trait]
    impl Extension for SelectionHook {
        fn id(&self) -> &str {
            "selection-hook"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_model_selection(&self) -> bool {
            true
        }
        async fn lifecycle(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<()> {
            self.0.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn sdk_model_selection_hooks_follow_committed_changes_and_skip_unchanged_models() {
        let notifications = Arc::new(Mutex::new(Vec::new()));
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "first")
            .extension(Arc::new(SelectionHook(notifications.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        session.session.select_model("first", Some("high")).unwrap();
        session
            .set_model_with_hooks(
                Arc::new(WaitingModel),
                "second",
                serde_json::json!({"id":"second", "provider":"fixture"}),
                Some(serde_json::json!({"id":"first", "provider":"fixture"})),
            )
            .await
            .unwrap();
        assert_eq!(
            session.journal().model_selection(),
            Some(("second".into(), None))
        );
        session
            .set_model_with_hooks(
                Arc::new(WaitingModel),
                "second",
                serde_json::json!({"id":"second"}),
                None,
            )
            .await
            .unwrap();
        let observed = notifications.lock().unwrap();
        assert_eq!(observed.len(), 2);
        assert!(
            matches!(&observed[0], crate::LifecycleEvent::ThinkingLevelSelect { previous_level, level } if previous_level == "high" && level == "off")
        );
        assert!(
            matches!(&observed[1], crate::LifecycleEvent::ModelSelect { model, previous_model: Some(previous), source: crate::ModelSelectSource::Set } if model["id"] == "second" && previous["id"] == "first")
        );
    }

    struct ForkHook(Arc<std::sync::atomic::AtomicBool>);

    #[async_trait::async_trait]
    impl Extension for ForkHook {
        fn id(&self) -> &str {
            "fork-hook"
        }
        fn handles_session_transitions(&self) -> bool {
            true
        }
        async fn before_session_transition(
            &self,
            event: &crate::BeforeSessionTransition,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<bool> {
            assert!(matches!(event, crate::BeforeSessionTransition::Fork { .. }));
            Ok(self.0.load(Ordering::SeqCst))
        }
    }

    #[tokio::test]
    async fn sdk_fork_hook_can_veto_before_creating_a_destination() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("fork.jsonl");
        let veto = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "echo")
            .extension(Arc::new(ForkHook(veto.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        session.prompt("first").await.unwrap();
        session.prompt("selected").await.unwrap();
        let entry_id = session
            .journal()
            .records()
            .iter()
            .find(|record| {
                matches!(
                    &record.event, crate::Event::User { content, .. } if content == "selected"
                )
            })
            .unwrap()
            .id;
        let head = session.journal().head();
        assert!(matches!(
            session
                .fork_with_hooks(entry_id, crate::ForkPosition::Before, Some(&destination))
                .await,
            Err(Error::Cancelled)
        ));
        assert!(!destination.exists());
        assert_eq!(session.journal().head(), head);
        veto.store(false, Ordering::SeqCst);
        let before = session
            .fork_with_hooks(entry_id, crate::ForkPosition::Before, Some(&destination))
            .await
            .unwrap();
        assert_eq!(before.messages().unwrap().len(), 2);
        let at = session
            .fork_with_hooks(entry_id, crate::ForkPosition::At, None)
            .await
            .unwrap();
        assert!(
            matches!(at.messages().unwrap().last(), Some(Message::User { content }) if content == "selected")
        );
    }

    #[async_trait::async_trait]
    impl Extension for NameHook {
        fn id(&self) -> &str {
            "name-hook"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_session_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<()> {
            if matches!(
                event,
                crate::LifecycleEvent::SessionStart { .. }
                    | crate::LifecycleEvent::SessionInfoChanged { .. }
                    | crate::LifecycleEvent::SessionShutdown { .. }
            ) {
                self.0.lock().unwrap().push(event.clone());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn sdk_name_updates_emit_committed_metadata_and_invoke_async_hooks() {
        let names = Arc::new(Mutex::new(Vec::new()));
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("session.jsonl");
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "echo")
            .extension(Arc::new(NameHook(names.clone())))
            .unwrap()
            .open(&source)
            .unwrap();
        let mut events = session.subscribe();
        for (input, expected) in [("  設計\nレビュー  ", Some("設計 レビュー")), (" ", None)]
        {
            session.set_name_with_hooks(input).await.unwrap();
            assert_eq!(session.journal().name(), expected);
            assert!(
                matches!(events.try_recv().unwrap(), SessionEvent::Record(record)
                if matches!(record.event, crate::Event::SessionInfo { .. }))
            );
            assert!(
                matches!(events.try_recv().unwrap(), SessionEvent::SessionInfoChanged { name }
                if name.as_deref() == expected)
            );
        }
        session.start().await.unwrap();
        let mut fork = session
            .fork_to(
                session.journal().head(),
                directory.path().join("fork.jsonl"),
            )
            .unwrap();
        fork.prompt("begin fork").await.unwrap();
        fork.close().await.unwrap();
        session.close().await.unwrap();
        use crate::{
            LifecycleEvent as Event, SessionShutdownReason as Stop, SessionStartReason as Start,
        };
        assert!(matches!(names.lock().unwrap().as_slice(), [
            Event::SessionStart { reason: Start::Startup, .. },
            Event::SessionInfoChanged { name: Some(name) },
            Event::SessionInfoChanged { name: None },
            Event::SessionStart { reason: Start::Fork, previous_session_file: Some(previous) },
            Event::SessionShutdown { reason: Stop::Quit, .. },
            Event::SessionShutdown { reason: Stop::Quit, .. },
        ] if name == "設計 レビュー" && previous == &source));
        assert!(session.set_name_with_hooks("closed").await.is_err());
    }

    struct TreeOptionsHook;
    #[async_trait::async_trait]
    impl Extension for TreeOptionsHook {
        fn id(&self) -> &str {
            "tree-options"
        }
        fn handles_tree_navigation(&self) -> bool {
            true
        }
        async fn before_tree(
            &self,
            preparation: &crate::TreePreparation,
            _: Option<&crate::ExtensionState>,
            _: &CancellationToken,
        ) -> crate::ExtensionResult<Option<crate::TreeDecision>> {
            assert_eq!(preparation.label.as_deref(), Some("SDK summary"));
            assert!(preparation.replace_instructions);
            assert_eq!(
                preparation.custom_instructions.as_deref(),
                Some("Preserve the decision")
            );
            Ok(Some(crate::TreeDecision::default()))
        }
    }

    #[tokio::test]
    async fn tree_navigation_restores_input_and_branch_state_and_emits_the_summary() {
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "echo")
            .extension(Arc::new(InputExtension))
            .unwrap()
            .in_memory()
            .unwrap();
        session.prompt("first").await.unwrap();
        session
            .extension_command("input-test", "prefix", "branch")
            .unwrap();
        session.prompt("second").await.unwrap();
        let selected = session
            .journal()
            .records()
            .iter()
            .find_map(|record| {
                matches!(record.event, crate::Event::User { .. }).then_some(record.id)
            })
            .unwrap();
        let result = session
            .navigate_tree(selected, crate::TreeNavigationOptions::default())
            .await
            .unwrap();
        assert_eq!(result.editor_text.as_deref(), Some("FIRST"));
        assert!(result.summary_entry.is_none());
        assert!(session.messages().unwrap().is_empty());
        session.controls().follow_up("restored").await.unwrap();
        assert_eq!(
            session.clear_queued_inputs().unwrap()[0].input.content,
            "RESTORED"
        );
        session.prompt("new branch").await.unwrap();
        session
            .register_extension(Arc::new(TreeOptionsHook))
            .unwrap();
        let mut events = session.subscribe();
        let result = session
            .navigate_tree(
                selected,
                crate::TreeNavigationOptions {
                    summary: Some(crate::BranchSummaryOptions {
                        instructions: Some("Preserve the decision"),
                        reserve_tokens: 1024,
                    }),
                    replace_instructions: true,
                    label: Some("SDK summary"),
                },
            )
            .await
            .unwrap();
        let summary = result.summary_entry.unwrap();
        assert_eq!(
            summary.parent,
            session.journal().records()[selected as usize - 1].parent
        );
        assert!(matches!(summary.event, crate::Event::BranchSummary { .. }));
        assert_eq!(session.journal().labels()[&summary.id], "SDK summary");
        let observed: Vec<_> = std::iter::from_fn(|| events.try_recv().ok()).collect();
        assert!(
            observed.iter().any(
                |event| matches!(event, SessionEvent::Record(record) if record.id == summary.id)
            )
        );
        assert!(observed.iter().any(|event| matches!(event, SessionEvent::Request(request)
            if matches!(request.messages.last(), Some(Message::User { content })
                if content.ends_with("\n\nPreserve the decision") && !content.contains("Create a concise summary")))));
        session.close().await.unwrap();
        assert!(
            session
                .navigate_tree(selected, crate::TreeNavigationOptions::default())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn tree_summary_can_be_aborted_through_owned_session_controls() {
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "echo")
            .in_memory()
            .unwrap();
        session.prompt("first").await.unwrap();
        let selected = session.journal().head();
        session.prompt("second").await.unwrap();
        session
            .set_model(Arc::new(WaitingModel), "waiting")
            .unwrap();
        let head = session.journal().head();
        let control = session.controls();
        let mut events = session.subscribe();
        let driver = async {
            while !matches!(events.recv().await.unwrap(), SessionEvent::Request(_)) {}
            control.abort();
        };
        let (result, ()) = tokio::join!(
            session.navigate_tree(
                selected,
                crate::TreeNavigationOptions {
                    summary: Some(crate::BranchSummaryOptions {
                        instructions: None,
                        reserve_tokens: 1024,
                    }),
                    ..Default::default()
                }
            ),
            driver
        );
        assert!(matches!(result, Err(Error::Cancelled)));
        assert!(
            session
                .journal()
                .branch()
                .iter()
                .any(|record| record.id == head)
        );
        assert!(!session.journal().records().iter().any(|record| matches!(
            record.event,
            crate::Event::Checkout { .. } | crate::Event::BranchSummary { .. }
        )));
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn idle_queue_clear_returns_messages_and_records_the_removal() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model, "fixture").in_memory().unwrap();
        session.controls().follow_up("queued").await.unwrap();
        assert_eq!(session.pending_inputs(), 1);
        let removed = session.clear_queued_inputs().unwrap();
        assert_eq!(removed[0].input.content, "queued");
        assert_eq!(session.pending_inputs(), 0);
        assert!(session.messages().unwrap().is_empty());
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn compaction_controls_are_shared_with_the_run_but_isolated_on_fork() {
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "echo")
            .in_memory()
            .unwrap();
        let control = session.controls();
        assert!(session.auto_compaction_enabled());
        control.set_auto_compaction_enabled(false).unwrap();
        assert!(!session.auto_compaction_enabled());
        control
            .set_compaction_tokens(crate::CompactionTokens {
                reserve_tokens: Some(123),
                keep_recent_tokens: Some(456),
            })
            .unwrap();
        session.prompt("fork point").await.unwrap();
        let mut fork = session.fork_at(session.journal().head()).unwrap();
        assert!(!fork.auto_compaction_enabled());
        assert_eq!(
            fork.journal()
                .auto_compaction_control()
                .token_settings()
                .reserve_tokens,
            Some(123)
        );
        fork.controls()
            .set_compaction_tokens(crate::CompactionTokens::default())
            .unwrap();
        assert_eq!(
            session
                .journal()
                .auto_compaction_control()
                .token_settings()
                .reserve_tokens,
            Some(123)
        );
        fork.controls().set_auto_compaction_enabled(true).unwrap();
        assert!(fork.auto_compaction_enabled());
        assert!(!session.auto_compaction_enabled());
        fork.close().await.unwrap();
        session.close().await.unwrap();
        assert!(control.set_auto_compaction_enabled(true).is_err());
    }

    #[tokio::test]
    async fn token_retention_counts_shell_context_without_creating_extra_turns() {
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "echo")
            .in_memory()
            .unwrap();
        session.prompt("first").await.unwrap();
        session.prompt("second").await.unwrap();
        session
            .session
            .append_bash_execution(crate::BashExecution {
                command: "report".into(),
                output: "x".repeat(600),
                exit_code: Some(0),
                cancelled: false,
                truncated: false,
                full_output_path: None,
                exclude_from_context: false,
            })
            .unwrap();
        let target = ("report".encode_utf16().count() as u64 + 600).div_ceil(4) + 1;
        let control = session.journal().auto_compaction_control();
        control.set_token_settings(crate::CompactionTokens {
            reserve_tokens: None,
            keep_recent_tokens: Some(target),
        });
        assert_eq!(control.retained_turns(session.journal(), 1).unwrap(), 1);
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn configured_summary_can_retain_an_assistant_inside_a_turn() {
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "echo")
            .in_memory()
            .unwrap();
        session.prompt("first").await.unwrap();
        session.prompt("second").await.unwrap();
        session
            .controls()
            .set_compaction_tokens(crate::CompactionTokens {
                reserve_tokens: None,
                keep_recent_tokens: Some(1),
            })
            .unwrap();
        session
            .summarize_configured(Some("Keep confirmed facts."))
            .await
            .unwrap();
        let messages = session.messages().unwrap();
        assert!(matches!(&messages[0], Message::User { content } if content.contains("answer 3")));
        assert!(
            matches!(&messages[1], Message::Assistant { content, .. } if content == "answer 2")
        );
        assert_eq!(messages.len(), 2);
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn reference_retention_rejects_a_small_session_without_requesting_a_summary() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model.clone(), "echo")
            .limits(Limits {
                context_window: Some(2048),
                output_tokens_per_request: 128,
                ..Limits::default()
            })
            .in_memory()
            .unwrap();
        session.prompt("small session").await.unwrap();
        session
            .controls()
            .set_compaction_tokens(crate::CompactionTokens::pi_defaults())
            .unwrap();
        session.prompt("still a small session").await.unwrap();
        let error = session.summarize_configured(None).await.unwrap_err();
        assert!(error.to_string().contains("Nothing to compact"));
        assert_eq!(model.0.load(Ordering::SeqCst), 2);
        assert!(
            !session
                .journal()
                .records()
                .iter()
                .any(|record| matches!(record.event, crate::Event::Compacted { .. }))
        );
        session.summarize(0).await.unwrap();
        assert_eq!(model.0.load(Ordering::SeqCst), 3);
        session.close().await.unwrap();
    }

    struct StatisticsModel {
        slow_discovery: bool,
    }
    #[async_trait::async_trait]
    impl Model for StatisticsModel {
        async fn context_window(&self, _: &str) -> Option<u32> {
            if self.slow_discovery {
                std::future::pending::<()>().await;
            }
            Some(8192)
        }
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Metadata(crate::ModelMetadata {
                    cached_input_tokens: Some(2),
                    cost_usd: serde_json::Number::from_f64(0.125),
                    ..Default::default()
                })),
                Ok(ModelEvent::Text("answer".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: Some(crate::Usage {
                        input_tokens: 10,
                        output_tokens: 3,
                    }),
                }),
            ])))
        }
    }

    #[tokio::test]
    async fn sdk_statistics_and_context_usage_share_the_durable_session() {
        let mut session = SessionBuilder::new(
            Arc::new(StatisticsModel {
                slow_discovery: false,
            }),
            "stats",
        )
        .limits(Limits {
            context_window: Some(4096),
            ..Default::default()
        })
        .in_memory()
        .unwrap();
        assert_eq!(session.statistics().cost, Some(0.0));
        session.prompt("hello").await.unwrap();
        let stats = session.statistics();
        assert_eq!(stats, session.journal().statistics());
        assert_eq!(stats.session_id, session.journal().session_id());
        assert_eq!((stats.user_messages, stats.assistant_messages), (1, 1));
        assert_eq!(
            (
                stats.tokens.input,
                stats.tokens.cache_read,
                stats.tokens.output,
                stats.tokens.total
            ),
            (8, 2, 3, 13)
        );
        assert_eq!(stats.cost, Some(0.125));
        let usage = session.context_usage().await.unwrap().unwrap();
        assert_eq!((usage.context_window, usage.tokens), (4096, Some(13)));
        assert_eq!(usage.percent, Some(13.0 / 4096.0 * 100.0));
        let mut fork = session.fork_at(session.journal().head()).unwrap();
        assert_eq!(fork.statistics().tokens, stats.tokens);
        assert_ne!(fork.statistics().session_id, stats.session_id);
        session.summarize(0).await.unwrap();
        assert_eq!(session.statistics().tokens.total, 26);
        assert_eq!(session.context_usage().await.unwrap().unwrap().tokens, None);
        fork.close().await.unwrap();
        session.close().await.unwrap();
        assert!(session.context_usage().await.is_err());
    }

    #[tokio::test]
    async fn sdk_context_discovery_is_bounded_and_used_without_an_override() {
        let mut session = SessionBuilder::new(
            Arc::new(StatisticsModel {
                slow_discovery: false,
            }),
            "stats",
        )
        .in_memory()
        .unwrap();
        assert_eq!(
            session
                .context_usage()
                .await
                .unwrap()
                .unwrap()
                .context_window,
            8192
        );
        session.close().await.unwrap();
        let mut session = SessionBuilder::new(
            Arc::new(StatisticsModel {
                slow_discovery: true,
            }),
            "stats",
        )
        .limits(Limits {
            model_timeout: std::time::Duration::from_millis(5),
            ..Default::default()
        })
        .in_memory()
        .unwrap();
        assert_eq!(session.context_usage().await.unwrap(), None);
        session.close().await.unwrap();
    }

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

    struct TriggerCommand;
    impl Extension for TriggerCommand {
        fn id(&self) -> &str {
            "trigger"
        }
        fn commands(&self) -> &[&str] {
            &["trigger"]
        }
        fn supports_command_effects(&self) -> bool {
            true
        }
        fn command_effects(
            &self,
            _: &str,
            _: &str,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<crate::ExtensionCommandEffects> {
            Ok(crate::ExtensionCommandEffects {
                state: None,
                actions: vec![crate::ExtensionCommandAction::TriggerMessage {
                    message: crate::CustomMessage {
                        custom_type: "trigger".into(),
                        content: crate::ContextContent::Text("go".into()),
                        display: false,
                        details: None,
                    },
                }],
            })
        }
    }

    #[tokio::test]
    async fn extension_command_can_trigger_model_turn() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model.clone(), "fixture")
            .extension(Arc::new(TriggerCommand))
            .unwrap()
            .in_memory()
            .unwrap();
        assert!(session.extension_command("trigger", "trigger", "").is_err());
        assert!(session.messages().unwrap().is_empty());
        assert_eq!(
            session
                .extension_command_and_run("trigger", "trigger", "")
                .await
                .unwrap()
                .as_deref(),
            Some("answer 1")
        );
        assert_eq!(model.0.load(Ordering::SeqCst), 1);
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn resume_ignores_user_input_removed_from_context() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model.clone(), "fixture")
            .in_memory()
            .unwrap();
        assert_eq!(session.prompt("first").await.unwrap(), "answer 1");
        let user = session
            .session
            .append(crate::Event::User {
                content: "removed".into(),
                input_id: None,
            })
            .unwrap()
            .id;
        session.edit_context(user, None).unwrap();
        assert_eq!(session.resume().await.unwrap(), "answer 1");
        assert_eq!(model.0.load(Ordering::SeqCst), 1);
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn idle_custom_message_can_trigger_a_fresh_model_turn() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model.clone(), "fixture")
            .in_memory()
            .unwrap();
        let message = |content: &str| crate::CustomMessage {
            custom_type: "extension".into(),
            content: crate::ContextContent::Text(content.into()),
            display: false,
            details: None,
        };

        assert_eq!(
            session
                .send_custom_message(message("first"), false)
                .await
                .unwrap(),
            None
        );
        assert_eq!(model.0.load(Ordering::SeqCst), 0);
        assert_eq!(
            session
                .send_custom_message(message("second"), true)
                .await
                .unwrap()
                .as_deref(),
            Some("answer 1")
        );
        assert_eq!(
            session
                .send_custom_message(message("third"), true)
                .await
                .unwrap()
                .as_deref(),
            Some("answer 2")
        );
        let messages = session.messages().unwrap();
        assert!(matches!(&messages[0], Message::User { content } if content == "first"));
        assert!(matches!(&messages[1], Message::User { content } if content == "second"));
        assert!(matches!(&messages[3], Message::User { content } if content == "third"));
        assert_eq!(model.0.load(Ordering::SeqCst), 2);
        assert_eq!(session.resume().await.unwrap(), "answer 2");
        assert_eq!(model.0.load(Ordering::SeqCst), 2);
        session
            .send_custom_message(message("removed"), false)
            .await
            .unwrap();
        let removed = session.journal().head();
        session.edit_context(removed, None).unwrap();
        assert_eq!(session.resume().await.unwrap(), "answer 2");
        assert_eq!(model.0.load(Ordering::SeqCst), 2);
        session.close().await.unwrap();
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
    struct InputExtension;

    #[async_trait::async_trait]
    impl Extension for InputExtension {
        fn id(&self) -> &str {
            "input-test"
        }

        fn handles_input(&self) -> bool {
            true
        }

        fn commands(&self) -> &[&str] {
            &["prefix"]
        }

        fn command(
            &self,
            _: &str,
            arguments: &str,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<crate::ExtensionState> {
            Ok(crate::ExtensionState {
                version: 1,
                data: serde_json::json!(arguments),
            })
        }

        async fn input(
            &self,
            event: &crate::InputEvent,
            state: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<crate::InputAction> {
            assert_eq!(event.source, crate::InputSource::Extension);
            Ok(if event.text == "handled" {
                crate::InputAction::Handled
            } else {
                let mut text = match event.streaming_behavior {
                    Some(InputKind::Steer) => "steer:".to_owned(),
                    Some(InputKind::FollowUp) => "follow-up:".to_owned(),
                    None => String::new(),
                };
                if let Some(prefix) = state.and_then(|state| state.data.as_str()) {
                    text.push_str(prefix);
                    text.push(':');
                }
                text.push_str(&event.text.to_uppercase());
                crate::InputAction::Transform { text, images: None }
            })
        }
    }

    #[tokio::test]
    async fn queued_hooks_use_branch_state_and_preserve_images() {
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "echo")
            .extension(Arc::new(InputExtension))
            .unwrap()
            .in_memory()
            .unwrap();
        let root = session.journal().head();
        session
            .extension_command("input-test", "prefix", "branch")
            .unwrap();
        let controls = session.controls();
        controls.follow_up("handled").await.unwrap();
        assert_eq!(session.pending_inputs(), 0);
        let image = crate::ImageContent {
            data: "aGVsbG8=".into(),
            mime_type: "image/png".into(),
        };
        let id = controls
            .steer_with_images("fixture", vec![image.clone()])
            .await
            .unwrap();
        let queued = session.clear_queued_inputs().unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].input.id, id);
        assert_eq!(queued[0].input.content, "branch:FIXTURE");
        assert_eq!(queued[0].images, vec![image]);
        let mut fork = session.fork_at(session.journal().head()).unwrap();
        fork.controls().follow_up("forked").await.unwrap();
        assert_eq!(
            fork.clear_queued_inputs().unwrap()[0].input.content,
            "branch:FORKED"
        );
        session.checkout(root).unwrap();
        controls.follow_up("root").await.unwrap();
        assert_eq!(
            session.clear_queued_inputs().unwrap()[0].input.content,
            "ROOT"
        );
    }

    #[tokio::test]
    async fn prompt_hooks_handle_or_transform_input_without_losing_images() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model.clone(), "echo")
            .extension(Arc::new(InputExtension))
            .unwrap()
            .in_memory()
            .unwrap();
        let mut events = session.subscribe();
        assert_eq!(session.prompt("handled").await.unwrap(), "");
        assert!(events.try_recv().is_err());
        assert_eq!(model.0.load(Ordering::SeqCst), 0);
        let image = crate::ImageContent {
            data: "aGVsbG8=".into(),
            mime_type: "image/png".into(),
        };
        session
            .prompt_with_images("fixture", vec![image.clone()])
            .await
            .unwrap();
        assert!(session.messages().unwrap().iter().any(|message| matches!(
            message,
            Message::UserWithImages { content, images } if content == "FIXTURE" && images.as_slice() == std::slice::from_ref(&image)
        )));
        assert_eq!(model.0.load(Ordering::SeqCst), 1);
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
    struct MessageDeliveryModel(Arc<Mutex<Vec<Vec<Message>>>>);
    #[async_trait::async_trait]
    impl Model for MessageDeliveryModel {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
            let mut requests = self.0.lock().unwrap();
            requests.push(request.messages);
            let calls = if requests.len() == 1 {
                vec![crate::ToolCall {
                    id: "send".into(),
                    name: "send_messages".into(),
                    arguments: "{}".into(),
                }]
            } else {
                vec![]
            };
            Ok(Box::pin(futures_util::stream::iter([Ok(
                ModelEvent::Complete { calls, usage: None },
            )])))
        }
    }
    struct MessageDeliveryTool;
    #[async_trait::async_trait]
    impl crate::Tool for MessageDeliveryTool {
        fn spec(&self) -> crate::ToolSpec {
            crate::ToolSpec {
                name: "send_messages".into(),
                description: "queue messages".into(),
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
            for (content, delivery) in [
                ("steer", crate::CustomMessageDelivery::Steer),
                ("follow", crate::CustomMessageDelivery::FollowUp),
                ("next", crate::CustomMessageDelivery::NextTurn),
                ("after", crate::CustomMessageDelivery::AfterTurn),
            ] {
                host.send_message(
                    crate::CustomMessage {
                        custom_type: "test".into(),
                        content: crate::ContextContent::Text(content.into()),
                        display: false,
                        details: Some(serde_json::json!({"value":content})),
                    },
                    delivery,
                )
                .await
                .unwrap();
            }
            crate::Outcome::Succeeded("tool result".into())
        }
    }
    struct MessageDeliveryExtension;
    impl Extension for MessageDeliveryExtension {
        fn id(&self) -> &str {
            "message-delivery"
        }
        fn tools(&self) -> Vec<Arc<dyn crate::Tool>> {
            vec![Arc::new(MessageDeliveryTool)]
        }
    }
    #[tokio::test]
    async fn tool_custom_messages_follow_boundaries_and_survive_restore() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let mut session =
            SessionBuilder::new(Arc::new(MessageDeliveryModel(requests.clone())), "fixture")
                .extension(Arc::new(MessageDeliveryExtension))
                .unwrap()
                .open(&path)
                .unwrap();
        session.prompt("first").await.unwrap();
        let snapshots = requests.lock().unwrap().clone();
        assert_eq!(snapshots.len(), 3);
        let text = |messages: &[Message]| {
            messages
                .iter()
                .filter_map(|m| match m {
                    Message::User { content } => Some(content.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(text(&snapshots[1]), ["first", "after", "steer"]);
        assert_eq!(text(&snapshots[2]), ["first", "after", "steer", "follow"]);
        let tool_pos = snapshots[1]
            .iter()
            .position(|m| matches!(m, Message::Tool { .. }))
            .unwrap();
        let steer_pos = snapshots[1]
            .iter()
            .position(|m| matches!(m, Message::User { content } if content == "steer"))
            .unwrap();
        assert!(tool_pos < steer_pos);
        assert_eq!(session.journal().queued_custom_messages().len(), 1);
        let restored = Session::from_records(session.journal().records().to_vec()).unwrap();
        assert_eq!(
            restored.queued_custom_messages()[0].1.delivery,
            crate::CustomMessageDelivery::NextTurn
        );
        drop(session);
        let mut session =
            SessionBuilder::new(Arc::new(MessageDeliveryModel(requests.clone())), "fixture")
                .extension(Arc::new(MessageDeliveryExtension))
                .unwrap()
                .open(&path)
                .unwrap();
        session.prompt("second").await.unwrap();
        let snapshots = requests.lock().unwrap();
        assert_eq!(
            text(snapshots.last().unwrap()),
            ["first", "after", "steer", "follow", "second", "next"]
        );
        assert!(session.journal().queued_custom_messages().is_empty());
    }

    struct LifecycleMessageExtension;
    struct LifecycleMessageTool;
    #[async_trait::async_trait]
    impl crate::Tool for LifecycleMessageTool {
        fn spec(&self) -> crate::ToolSpec {
            crate::ToolSpec {
                name: "send_messages".into(),
                description: "test".into(),
                parameters: serde_json::json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: crate::ToolContext) -> crate::Outcome {
            crate::Outcome::Succeeded("tool result".into())
        }
    }
    #[async_trait::async_trait]
    impl Extension for LifecycleMessageExtension {
        fn id(&self) -> &str {
            "lifecycle-messages"
        }
        fn tools(&self) -> Vec<Arc<dyn crate::Tool>> {
            vec![Arc::new(LifecycleMessageTool)]
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_tool_execution_lifecycle(&self) -> bool {
            true
        }
        fn handles_tool_result(&self) -> bool {
            true
        }
        async fn tool_call_with_host(
            &self,
            _: &str,
            _: &mut serde_json::Value,
            _: &crate::ToolContext,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::Decision> {
            host.send_message(
                crate::CustomMessage {
                    custom_type: "hook".into(),
                    content: crate::ContextContent::Text("tool call".into()),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::Steer,
            )
            .await?;
            Ok(crate::Decision::Allow)
        }
        async fn tool_result_update_with_host(
            &self,
            _: &crate::ToolCall,
            _: &crate::Outcome,
            _: &crate::ToolContext,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<Option<crate::Outcome>> {
            host.send_message(
                crate::CustomMessage {
                    custom_type: "hook".into(),
                    content: crate::ContextContent::Text("tool result".into()),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::Steer,
            )
            .await?;
            Ok(None)
        }
        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            let (text, delivery) = match event {
                crate::LifecycleEvent::AgentStart { .. } => {
                    ("agent start", crate::CustomMessageDelivery::Steer)
                }
                crate::LifecycleEvent::ToolExecutionStart { .. } => {
                    ("tool start", crate::CustomMessageDelivery::Steer)
                }
                crate::LifecycleEvent::ToolExecutionEnd { .. } => {
                    ("tool end", crate::CustomMessageDelivery::Steer)
                }
                crate::LifecycleEvent::TurnEnd { turn_index: 0, .. } => {
                    ("turn end", crate::CustomMessageDelivery::AfterTurn)
                }
                _ => return Ok(()),
            };
            if matches!(event, crate::LifecycleEvent::AgentStart { .. }) {
                host.send_message(
                    crate::CustomMessage {
                        custom_type: "lifecycle".into(),
                        content: crate::ContextContent::Text("next run".into()),
                        display: false,
                        details: None,
                    },
                    crate::CustomMessageDelivery::NextTurn,
                )
                .await?;
            }
            host.send_message(
                crate::CustomMessage {
                    custom_type: "lifecycle".into(),
                    content: crate::ContextContent::Text(text.into()),
                    display: false,
                    details: None,
                },
                delivery,
            )
            .await
        }
    }
    #[tokio::test]
    async fn lifecycle_host_calls_are_reentrant_and_ordered_at_turn_boundaries() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut session =
            SessionBuilder::new(Arc::new(MessageDeliveryModel(requests.clone())), "fixture")
                .extension(Arc::new(LifecycleMessageExtension))
                .unwrap()
                .in_memory()
                .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), session.prompt("first"))
            .await
            .expect("lifecycle host call deadlocked")
            .unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let text = |messages: &[Message]| {
            messages
                .iter()
                .filter_map(|m| match m {
                    Message::User { content } => Some(content.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(text(&requests[0]), ["first", "agent start"]);
        assert_eq!(
            text(&requests[1]),
            [
                "first",
                "agent start",
                "turn end",
                "tool start",
                "tool call",
                "tool result",
                "tool end"
            ]
        );
        let tool = requests[1]
            .iter()
            .position(|m| matches!(m, Message::Tool { .. }))
            .unwrap();
        let turn_end = requests[1]
            .iter()
            .position(|m| matches!(m, Message::User { content } if content == "turn end"))
            .unwrap();
        assert!(tool < turn_end);
        assert_eq!(session.journal().queued_custom_messages().len(), 1);
        assert_eq!(
            session.journal().queued_custom_messages()[0].1.delivery,
            crate::CustomMessageDelivery::NextTurn
        );
    }

    struct RequestMessageHook(AtomicUsize);
    #[async_trait::async_trait]
    impl Extension for RequestMessageHook {
        fn id(&self) -> &str {
            "request-message"
        }
        async fn before_request_with_host(
            &self,
            _: &mut ModelRequest,
            _: crate::RequestPurpose,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                host.send_message(
                    crate::CustomMessage {
                        custom_type: "request".into(),
                        content: crate::ContextContent::Text("request steer".into()),
                        display: false,
                        details: None,
                    },
                    crate::CustomMessageDelivery::Steer,
                )
                .await?;
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn provider_request_hook_message_is_journaled_and_delivered_after_current_request() {
        let count = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(count.clone(), "fixture")
            .extension(Arc::new(RequestMessageHook(AtomicUsize::new(0))))
            .unwrap()
            .in_memory()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), session.prompt("first"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(count.0.load(Ordering::SeqCst), 2);
        let records = session.journal().records();
        let queued = records
            .iter()
            .position(|r| matches!(r.event, crate::Event::CustomMessageQueued { .. }))
            .unwrap();
        let first_request = records
            .iter()
            .position(|r| matches!(r.event, crate::Event::ModelRequested { .. }))
            .unwrap();
        let delivered = records
            .iter()
            .position(|r| matches!(r.event, crate::Event::CustomMessageDelivered { .. }))
            .unwrap();
        assert!(queued < first_request && first_request < delivered);
    }

    struct InputMessageHook;
    #[async_trait::async_trait]
    impl Extension for InputMessageHook {
        fn id(&self) -> &str {
            "input-message"
        }
        fn handles_input(&self) -> bool {
            true
        }
        async fn input_with_host(
            &self,
            event: &crate::InputEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::InputAction> {
            let text = if event.streaming_behavior.is_some() {
                "streaming hook"
            } else {
                "idle hook"
            };
            host.send_message(
                crate::CustomMessage {
                    custom_type: "input".into(),
                    content: crate::ContextContent::Text(text.into()),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::Steer,
            )
            .await?;
            if event.streaming_behavior.is_none() {
                host.send_message(
                    crate::CustomMessage {
                        custom_type: "input".into(),
                        content: crate::ContextContent::Text("next".into()),
                        display: false,
                        details: None,
                    },
                    crate::CustomMessageDelivery::NextTurn,
                )
                .await?;
            }
            Ok(crate::InputAction::Continue)
        }
    }
    #[tokio::test]
    async fn initial_input_hook_messages_preserve_idle_and_next_turn_order() {
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "fixture")
            .extension(Arc::new(InputMessageHook))
            .unwrap()
            .in_memory()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), session.prompt("prompt"))
            .await
            .unwrap()
            .unwrap();
        let texts: Vec<_> = session
            .messages()
            .unwrap()
            .into_iter()
            .filter_map(|m| {
                if let Message::User { content } = m {
                    Some(content)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(texts, ["idle hook", "prompt", "next"]);
    }
    #[tokio::test]
    async fn streaming_input_hook_can_send_without_deadlocking_queue_order() {
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
            .extension(Arc::new(InputMessageHook))
            .unwrap()
            .in_memory()
            .unwrap();
        let control = session.controls();
        let mut events = session.subscribe();
        let driver = async move {
            while !matches!(events.recv().await.unwrap(), SessionEvent::Request(_)) {}
            control.steer("queued user").await.unwrap();
            control.abort();
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(session.prompt("prompt"), driver)
        })
        .await
        .expect("input hook deadlocked");
        assert!(matches!(result, Err(Error::Cancelled)));
        assert_eq!(
            session.journal().queued_custom_messages()[0]
                .1
                .message
                .content,
            crate::ContextContent::Text("streaming hook".into())
        );
    }

    struct IdleLifecycleMessages;
    #[async_trait::async_trait]
    impl Extension for IdleLifecycleMessages {
        fn id(&self) -> &str {
            "idle-lifecycle-messages"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_session_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            let (text, delivery) = match event {
                crate::LifecycleEvent::SessionStart { .. } => {
                    ("session start", crate::CustomMessageDelivery::Steer)
                }
                crate::LifecycleEvent::AgentSettled { .. } => {
                    ("after settled", crate::CustomMessageDelivery::NextTurn)
                }
                _ => return Ok(()),
            };
            host.send_message(
                crate::CustomMessage {
                    custom_type: "lifecycle".into(),
                    content: crate::ContextContent::Text(text.into()),
                    display: false,
                    details: None,
                },
                delivery,
            )
            .await
        }
    }
    #[tokio::test]
    async fn session_start_and_settled_hooks_deliver_at_idle_boundaries() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model.clone(), "fixture")
            .extension(Arc::new(IdleLifecycleMessages))
            .unwrap()
            .in_memory()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), session.prompt("first"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(model.0.load(Ordering::SeqCst), 1);
        assert_eq!(session.journal().queued_custom_messages().len(), 1);
        session.prompt("second").await.unwrap();
        let texts: Vec<_> = session
            .messages()
            .unwrap()
            .into_iter()
            .filter_map(|m| {
                if let Message::User { content } = m {
                    Some(content)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(texts, ["session start", "first", "second", "after settled"]);
        assert_eq!(model.0.load(Ordering::SeqCst), 2);
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
            let result = host
                .call("child", serde_json::json!({"wrapped":"ok"}))
                .await;
            assert!(
                matches!(&result, crate::Outcome::Structured { data, .. } if data["output"].as_str().unwrap().len() == 1024 * 1024)
            );
            assert!(matches!(
                host.call("blocked", serde_json::json!({"value":"ok"}))
                    .await,
                crate::Outcome::Skipped(_)
            ));
            assert!(matches!(
                host.call("child", serde_json::json!({})).await,
                crate::Outcome::Skipped(_)
            ));
            crate::Outcome::Succeeded("child result".into())
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
        async fn prepare_arguments(
            &self,
            mut arguments: serde_json::Value,
        ) -> crate::ExtensionResult<serde_json::Value> {
            if let Some(value) = arguments.as_object_mut().unwrap().remove("wrapped") {
                arguments["value"] = value;
            }
            Ok(arguments)
        }
        async fn invoke(&self, _: serde_json::Value, _: crate::ToolContext) -> crate::Outcome {
            self.count.fetch_add(1, Ordering::SeqCst);
            crate::Outcome::Succeeded("child result".into())
        }
        async fn invoke_programmatic(
            &self,
            _: serde_json::Value,
            _: crate::ToolContext,
            _: Option<&crate::ExtensionState>,
            _: Arc<dyn crate::ToolHost>,
        ) -> crate::ToolReply {
            self.count.fetch_add(1, Ordering::SeqCst);
            crate::Outcome::Structured {
                data: serde_json::json!({"output":"\u{0001}".repeat(1024 * 1024)}),
                attachments: vec![],
            }
            .into()
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
            arguments: &serde_json::Value,
            _: &crate::ToolContext,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<crate::Decision> {
            if name == "child" {
                assert_eq!(arguments["value"], "ok");
                assert!(arguments.get("wrapped").is_none());
            }
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
        assert!(
            session
                .journal()
                .operations()
                .iter()
                .any(|(invocation, _)| invocation.call.name == "child"
                    && invocation.call.arguments.contains("wrapped"))
        );
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
    struct SlowPrepare(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl crate::Tool for SlowPrepare {
        fn spec(&self) -> crate::ToolSpec {
            Bundle.spec()
        }
        async fn prepare_arguments(
            &self,
            _: serde_json::Value,
        ) -> crate::ExtensionResult<serde_json::Value> {
            std::future::pending().await
        }
        async fn invoke(&self, _: serde_json::Value, _: crate::ToolContext) -> crate::Outcome {
            self.0.fetch_add(1, Ordering::SeqCst);
            crate::Outcome::Succeeded("unexpected".into())
        }
    }
    struct SlowPrepareExtension(Arc<AtomicUsize>);
    impl Extension for SlowPrepareExtension {
        fn id(&self) -> &str {
            "slow-prepare"
        }
        fn tools(&self) -> Vec<Arc<dyn crate::Tool>> {
            vec![Arc::new(SlowPrepare(self.0.clone()))]
        }
    }
    #[tokio::test]
    async fn argument_preparation_timeout_never_dispatches_the_tool() {
        let count = Arc::new(AtomicUsize::new(0));
        let mut session = SessionBuilder::new(Arc::new(NestedModel), "nested")
            .limits(Limits {
                model_requests: 1,
                tool_timeout: std::time::Duration::from_millis(20),
                ..Limits::default()
            })
            .extension(Arc::new(SlowPrepareExtension(count.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        assert!(matches!(
            session.prompt("prepare").await,
            Err(Error::Budget)
        ));
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert!(
            !session
                .journal()
                .records()
                .iter()
                .any(|record| matches!(record.event, crate::Event::ToolStarted { .. }))
        );
        assert!(session.journal().operations().iter().any(|(_, state)| matches!(state, crate::OperationState::Finished(crate::Outcome::Skipped(reason)) if reason.contains("preparation timed out"))));
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
            let results = host
                .call_batch(
                    (0..2)
                        .map(|_| ("barrier".into(), serde_json::json!({})))
                        .collect(),
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
        async fn prepare_arguments(
            &self,
            arguments: serde_json::Value,
        ) -> crate::ExtensionResult<serde_json::Value> {
            Child {
                name: "child",
                count: self.calls.clone(),
            }
            .prepare_arguments(arguments)
            .await
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
    #[tokio::test]
    async fn active_controls_clear_and_change_mode_before_the_model_finishes() {
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "waiting")
            .in_memory()
            .unwrap();
        let control = session.controls();
        control.follow_up("pending").await.unwrap();
        let mut events = session.subscribe();
        let driver = async {
            while !matches!(events.recv().await.unwrap(), SessionEvent::Request(_)) {}
            let removed = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                control.clear_queued_inputs(),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(removed[0].input.content, "pending");
            control
                .set_queue_mode(InputKind::FollowUp, crate::QueueMode::OneAtATime)
                .await
                .unwrap();
            control.abort();
        };
        let (result, ()) = tokio::join!(session.prompt("start"), driver);
        assert!(matches!(result, Err(Error::Cancelled)));
        assert_eq!(session.pending_inputs(), 0);
        assert_eq!(
            session.journal().queue_mode(InputKind::FollowUp),
            crate::QueueMode::OneAtATime
        );
        assert!(control.clear_queued_inputs().await.is_err());
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn compaction_accepts_queue_controls_without_waiting_for_its_summary() {
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "echo")
            .in_memory()
            .unwrap();
        for input in ["first", "second", "third"] {
            session.prompt(input).await.unwrap();
        }
        session
            .set_model(Arc::new(WaitingModel), "waiting")
            .unwrap();
        let control = session.controls();
        let mut events = session.subscribe();
        control.follow_up("after compaction").await.unwrap();
        let driver = async {
            while !matches!(events.recv().await.unwrap(), SessionEvent::Request(_)) {}
            let removed = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                control.clear_queued_inputs(),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(removed[0].input.content, "after compaction");
            control.abort();
        };
        let (result, ()) = tokio::join!(session.summarize(1), driver);
        assert!(matches!(result, Err(Error::Cancelled)));
        assert_eq!(session.pending_inputs(), 0);
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn summary_focus_reaches_the_model_without_entering_user_history() {
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "echo")
            .in_memory()
            .unwrap();
        for input in ["first", "second", "third"] {
            session.prompt(input).await.unwrap();
        }
        let mut events = session.subscribe();
        session
            .summarize_with_instructions(1, Some("Preserve the deployment decision."))
            .await
            .unwrap();
        let mut summary_request = None;
        while let Ok(event) = events.try_recv() {
            if let SessionEvent::Request(request) = event {
                summary_request = Some(request);
            }
        }
        let request = summary_request.expect("summary request");
        assert!(request.tools.is_empty());
        assert!(
            matches!(request.messages.last(), Some(crate::Message::User { content }) if content.ends_with("Additional focus: Preserve the deployment decision."))
        );
        assert!(!session.journal().records().iter().any(|record| matches!(&record.event, crate::Event::User { content, .. } if content.contains("deployment decision"))));
        assert!(
            matches!(&session.journal().records().last().unwrap().event, crate::Event::Compacted { summary, .. } if summary == "answer 4")
        );
        session.close().await.unwrap();
    }

    struct WaitingContextModel(Arc<tokio::sync::Notify>);
    #[async_trait::async_trait]
    impl Model for WaitingContextModel {
        async fn context_window(&self, _: &str) -> Option<u32> {
            self.0.notify_one();
            std::future::pending().await
        }
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            panic!("cancelled metadata discovery must not reach inference")
        }
    }

    #[tokio::test]
    async fn context_capacity_discovery_can_be_cancelled() {
        let started = Arc::new(tokio::sync::Notify::new());
        let mut session =
            SessionBuilder::new(Arc::new(WaitingContextModel(started.clone())), "waiting")
                .in_memory()
                .unwrap();
        let control = session.controls();
        let driver = async {
            started.notified().await;
            control.abort();
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(session.prompt("cancel metadata lookup"), driver)
        })
        .await
        .expect("metadata cancellation must finish promptly");
        assert!(matches!(result, Err(Error::Cancelled)));
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
    async fn active_control_acknowledges_custom_message_before_abort() {
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "wait")
            .in_memory()
            .unwrap();
        let control = session.controls();
        let mut events = session.subscribe();
        let queue = async move {
            while !matches!(events.recv().await.unwrap(), SessionEvent::Request(_)) {}
            control
                .send_custom_message(
                    crate::CustomMessage {
                        custom_type: "active".into(),
                        content: crate::ContextContent::Text("preserved".into()),
                        display: true,
                        details: Some(serde_json::json!({"x":1})),
                    },
                    crate::CustomMessageDelivery::NextTurn,
                )
                .await
                .unwrap();
            control.abort();
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(session.prompt("wait"), queue)
        })
        .await
        .unwrap();
        assert!(matches!(result, Err(Error::Cancelled)));
        let restored = Session::from_records(session.journal().records().to_vec()).unwrap();
        assert_eq!(
            restored.queued_custom_messages()[0].1.message.details,
            Some(serde_json::json!({"x":1}))
        );
    }

    #[tokio::test]
    async fn active_queued_hooks_receive_the_delivery_mode_before_clear() {
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "wait")
            .extension(Arc::new(InputExtension))
            .unwrap()
            .in_memory()
            .unwrap();
        let controls = session.controls();
        let mut events = session.subscribe();
        let queue = async {
            while !matches!(events.recv().await.unwrap(), SessionEvent::Request(_)) {}
            controls.steer("handled").await.unwrap();
            let id = controls.follow_up("active").await.unwrap();
            let queued = controls.clear_queued_inputs().await.unwrap();
            assert_eq!(queued.len(), 1);
            assert_eq!(queued[0].input.id, id);
            assert_eq!(queued[0].input.content, "follow-up:ACTIVE");
            controls.abort();
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(session.prompt("wait"), queue)
        })
        .await
        .unwrap();
        assert!(matches!(result, Err(Error::Cancelled)));
    }
    #[tokio::test]
    async fn cloned_control_can_rename_and_abort_a_running_session() {
        let names = Arc::new(Mutex::new(Vec::new()));
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "wait")
            .extension(Arc::new(NameHook(names.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        let control = session.controls();
        let mut events = session.subscribe();
        let abort = async move {
            while let Ok(event) = events.recv().await {
                if matches!(event, SessionEvent::Request(_)) {
                    assert!(control.set_session_name(" ").await.is_err());
                    control.set_session_name("  In progress  ").await.unwrap();
                    assert!(names.lock().unwrap().iter().any(|event| matches!(
                        event,
                        crate::LifecycleEvent::SessionInfoChanged { name: Some(name) }
                            if name == "In progress"
                    )));
                    while let Ok(event) = events.recv().await {
                        if let SessionEvent::SessionInfoChanged { name } = event {
                            assert_eq!(name.as_deref(), Some("In progress"));
                            break;
                        }
                    }
                    control.abort();
                    break;
                }
            }
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(session.prompt("wait for abort"), abort)
        })
        .await
        .unwrap();
        assert!(matches!(result, Err(Error::Cancelled)));
        assert_eq!(session.journal().name(), Some("In progress"));
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
    #[tokio::test]
    async fn queued_images_survive_cancellation_and_reopening_without_duplicate_consumption() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("queued.jsonl");
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "wait")
            .in_memory()
            .unwrap();
        let control = session.controls();
        let input_id = control
            .follow_up_with_images(
                "queued image",
                vec![crate::ImageContent {
                    data: "aW1hZ2U=".into(),
                    mime_type: "image/png".into(),
                }],
            )
            .await
            .unwrap();
        let mut events = session.subscribe();
        let abort = async move {
            while let Ok(event) = events.recv().await {
                if matches!(event, SessionEvent::Request(_)) {
                    control.abort();
                    break;
                }
            }
        };
        let (result, ()) = tokio::join!(session.prompt("initial"), abort);
        assert!(matches!(result, Err(Error::Cancelled)));
        session.export_jsonl(&path).unwrap();
        session.close().await.unwrap();
        let mut reopened = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "echo")
            .open(path)
            .unwrap();
        assert_eq!(reopened.resume().await.unwrap(), "answer 2");
        assert_eq!(reopened.resume().await.unwrap(), "answer 2");
        assert_eq!(reopened.journal().records().iter().filter(|record| matches!(&record.event, crate::Event::InputQueuedWithImages { input } if input.input.id == input_id)).count(), 1);
        assert_eq!(reopened.journal().records().iter().filter(|record| matches!(&record.event, crate::Event::UserWithImages { input_id: Some(id), .. } if *id == input_id)).count(), 1);
        reopened.close().await.unwrap();
    }
    struct PriorityModel {
        release: Arc<tokio::sync::Notify>,
        seen: Arc<Mutex<Vec<Vec<String>>>>,
    }
    #[async_trait::async_trait]
    impl Model for PriorityModel {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
            use futures_util::StreamExt;
            let messages = request
                .messages
                .iter()
                .filter_map(|message| match message {
                    Message::User { content } | Message::UserWithImages { content, .. } => {
                        Some(content.clone())
                    }
                    _ => None,
                })
                .collect();
            let count = {
                let mut seen = self.seen.lock().unwrap();
                seen.push(messages);
                seen.len()
            };
            let release = self.release.clone();
            let text = futures_util::stream::once(async move {
                if count == 1 {
                    release.notified().await;
                }
                Ok(ModelEvent::Text(format!("answer {count}")))
            });
            Ok(Box::pin(text.chain(futures_util::stream::iter([Ok(
                ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                },
            )]))))
        }
    }
    #[tokio::test]
    async fn image_steering_precedes_followups_queued_during_a_response() {
        let release = Arc::new(tokio::sync::Notify::new());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut session = SessionBuilder::new(
            Arc::new(PriorityModel {
                release: release.clone(),
                seen: seen.clone(),
            }),
            "priority",
        )
        .in_memory()
        .unwrap();
        let control = session.controls();
        let mut events = session.subscribe();
        let producer = async move {
            while let Ok(event) = events.recv().await {
                if matches!(event, SessionEvent::Request(_)) {
                    let images = vec![crate::ImageContent {
                        data: "aW1hZ2U=".into(),
                        mime_type: "image/png".into(),
                    }];
                    control
                        .follow_up_with_images("follow", images.clone())
                        .await
                        .unwrap();
                    control.steer_with_images("steer", images).await.unwrap();
                    release.notify_one();
                    break;
                }
            }
        };
        let (result, ()) = tokio::join!(session.prompt("initial"), producer);
        assert_eq!(result.unwrap(), "answer 3");
        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![
                vec!["initial"],
                vec!["initial", "steer"],
                vec!["initial", "steer", "follow"]
            ]
        );
        session.close().await.unwrap();
    }
}
