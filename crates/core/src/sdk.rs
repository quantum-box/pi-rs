//! Owned session API for embedding the agent in long-lived native applications.
use std::collections::VecDeque;
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
    /// Selected runtime registry at request preparation or dispatch. It may
    /// differ from the configured registry and contains extension source paths.
    ToolInventory {
        run_id: Uuid,
        phase: crate::ToolInventoryPhase,
        tools: Arc<Vec<crate::RegisteredToolInfo>>,
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
    /// Diagnostic for a failed run, including automatically triggered runs.
    RunFailed {
        run_id: Uuid,
        message: String,
    },
}

/// Result of navigating to a transcript entry through extension hooks.
#[derive(Clone, Debug)]
pub struct TreeNavigationResult {
    pub editor_text: Option<String>,
    pub summary_entry: Option<Box<Record>>,
}

// Admission tickets serialize independent control operations without holding a
// mutex while extension callbacks are running. Host messages bypass admission
// and are serviced by the session owner while its callback is pending.
#[derive(Default)]
struct InputAdmission {
    state: Mutex<InputAdmissionState>,
}

#[derive(Default)]
struct InputAdmissionState {
    next: u64,
    busy: bool,
    waiting: VecDeque<(u64, tokio::sync::oneshot::Sender<InputPermit>)>,
}

struct InputPermit {
    admission: Option<Arc<InputAdmission>>,
    ticket: u64,
}

impl InputAdmission {
    async fn acquire(self: &Arc<Self>) -> Result<InputPermit> {
        let receiver = {
            let mut state = self.state.lock().expect("input admission");
            let ticket = state.next;
            state.next = state
                .next
                .checked_add(1)
                .ok_or_else(|| Error::Config("input admission tickets exhausted".into()))?;
            if !state.busy {
                state.busy = true;
                return Ok(InputPermit {
                    admission: Some(self.clone()),
                    ticket,
                });
            }
            let (sender, receiver) = tokio::sync::oneshot::channel();
            state.waiting.push_back((ticket, sender));
            receiver
        };
        receiver
            .await
            .map_err(|_| Error::Config("input admission closed".into()))
    }

    fn release(self: &Arc<Self>) {
        loop {
            let next = {
                let mut state = self.state.lock().expect("input admission");
                match state.waiting.pop_front() {
                    Some(next) => next,
                    None => {
                        state.busy = false;
                        return;
                    }
                }
            };
            let (ticket, sender) = next;
            let permit = InputPermit {
                admission: Some(self.clone()),
                ticket,
            };
            match sender.send(permit) {
                Ok(()) => return,
                Err(mut permit) => {
                    // A cancelled waiter must not recursively release a long
                    // chain of cancelled tickets or hold the queue lock.
                    permit.admission = None;
                }
            }
        }
    }
}

impl Drop for InputPermit {
    fn drop(&mut self) {
        let _ = self.ticket;
        if let Some(admission) = self.admission.take() {
            admission.release();
        }
    }
}

#[derive(Clone)]
struct RunConfig {
    model: std::sync::Weak<dyn Model>,
    model_name: String,
    system_prompt: String,
    registry: std::sync::Weak<Registry>,
    limits: Limits,
}

type PendingStart = Arc<Mutex<Option<(crate::SessionStartReason, Option<PathBuf>)>>>;

struct SessionOwner {
    id: Uuid,
    pending_start: PendingStart,
    closed: CancellationToken,
    journal: std::sync::Weak<tokio::sync::Mutex<Session>>,
    input: std::sync::Weak<tokio::sync::Mutex<mpsc::Receiver<crate::AgentInput>>>,
    config: Mutex<RunConfig>,
}

const MAX_IDLE_INPUT_DEPTH: usize = 16;

struct IdleChildRequest {
    input: crate::AgentInput,
    reply: tokio::sync::oneshot::Sender<Result<()>>,
    lifetime: CancellationToken,
    gate: Arc<Mutex<()>>,
}

#[derive(Clone)]
struct IdleInputScope {
    owner: Arc<SessionOwner>,
    children: mpsc::UnboundedSender<IdleChildRequest>,
    host: Arc<dyn crate::ToolHost>,
    states: crate::ExtensionStates,
    cancellation: CancellationToken,
    lifetime: CancellationToken,
    gate: Arc<Mutex<()>>,
    depth: usize,
}

tokio::task_local! {
    static IDLE_INPUT_SCOPE: IdleInputScope;
}

impl IdleInputScope {
    async fn submit(&self, input: crate::AgentInput) -> Result<()> {
        let (reply, receiver) = tokio::sync::oneshot::channel();
        self.children
            .send(IdleChildRequest {
                input,
                reply,
                lifetime: self.lifetime.clone(),
                gate: self.gate.clone(),
            })
            .map_err(|_| Error::Config("idle input callback has finished".into()))?;
        tokio::select! {
            biased;
            result = receiver => result.map_err(|_| Error::Config("idle input response was lost".into()))?,
            _ = self.lifetime.cancelled() => Err(Error::Config("idle input callback has finished".into())),
            _ = self.cancellation.cancelled() => Err(Error::Cancelled),
        }
    }
}

// Route callback host messages through the same lifetime-checked child lane as
// scoped controls. The owner checks expiry under the scope gate before writing.
struct ScopedIdleHost {
    inner: Arc<dyn crate::ToolHost>,
    scope: IdleInputScope,
}

#[async_trait::async_trait]
impl crate::ToolHost for ScopedIdleHost {
    fn callback_scope_owner(&self) -> Option<Uuid> {
        Some(self.scope.owner.id)
    }
    fn tools(&self) -> Vec<crate::ToolSpec> {
        self.inner.tools()
    }
    fn supports_progress(&self) -> bool {
        self.inner.supports_progress()
    }
    fn has_ui(&self) -> bool {
        self.inner.has_ui()
    }
    fn supports_custom_messages(&self) -> bool {
        self.inner.supports_custom_messages()
    }
    fn supports_keyed_messages(&self) -> bool {
        false
    }
    fn for_extension(&self, extension_id: &str) -> Option<Arc<dyn crate::ToolHost>> {
        self.inner.for_extension(extension_id).map(|inner| {
            Arc::new(Self {
                inner,
                scope: self.scope.clone(),
            }) as Arc<dyn crate::ToolHost>
        })
    }
    async fn call(&self, name: &str, arguments: serde_json::Value) -> crate::Outcome {
        if self.scope.lifetime.is_cancelled() {
            return crate::Outcome::Skipped("idle input callback has finished".into());
        }
        self.inner.call(name, arguments).await
    }
    async fn report_progress(&self, partial: serde_json::Value) -> crate::ExtensionResult<()> {
        if self.scope.lifetime.is_cancelled() {
            return Err("idle input callback has finished".into());
        }
        self.inner.report_progress(partial).await
    }
    async fn ui(&self, request: crate::UiRequest) -> crate::ExtensionResult<crate::UiResponse> {
        if self.scope.lifetime.is_cancelled() {
            return Err("idle input callback has finished".into());
        }
        self.inner.ui(request).await
    }
    async fn send_message(
        &self,
        message: crate::CustomMessage,
        delivery: crate::CustomMessageDelivery,
    ) -> crate::ExtensionResult<()> {
        crate::journal::validate_custom_message(&message).map_err(|error| error.to_string())?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.scope
            .submit(crate::AgentInput::EnqueueCustom {
                message: crate::QueuedCustomMessage {
                    id: Uuid::new_v4(),
                    message,
                    delivery,
                },
                reply,
            })
            .await
            .map_err(|error| error.to_string())?;
        receive
            .await
            .map_err(|_| "custom message response lost".to_owned())?
            .map_err(|error| error.to_string())
    }
}

pub(crate) fn scope_callback_host(host: Arc<dyn crate::ToolHost>) -> Arc<dyn crate::ToolHost> {
    match IDLE_INPUT_SCOPE.try_with(|scope| scope.clone()) {
        Ok(scope) if host.callback_scope_owner() == Some(scope.owner.id) => {
            Arc::new(ScopedIdleHost { inner: host, scope })
        }
        _ => host,
    }
}

// Marks the host with the owner whose journal services its messages. A nested
// session must never inherit the caller's task-local child lane.
struct OwnerBoundHost {
    inner: Arc<dyn crate::ToolHost>,
    owner_id: Uuid,
}
#[async_trait::async_trait]
impl crate::ToolHost for OwnerBoundHost {
    fn callback_scope_owner(&self) -> Option<Uuid> {
        Some(self.owner_id)
    }
    fn tools(&self) -> Vec<crate::ToolSpec> {
        self.inner.tools()
    }
    fn supports_progress(&self) -> bool {
        self.inner.supports_progress()
    }
    fn has_ui(&self) -> bool {
        self.inner.has_ui()
    }
    fn supports_custom_messages(&self) -> bool {
        self.inner.supports_custom_messages()
    }
    fn supports_keyed_messages(&self) -> bool {
        self.inner.supports_keyed_messages()
    }
    fn for_extension(&self, id: &str) -> Option<Arc<dyn crate::ToolHost>> {
        self.inner.for_extension(id).map(|inner| {
            Arc::new(Self {
                inner,
                owner_id: self.owner_id,
            }) as Arc<dyn crate::ToolHost>
        })
    }
    async fn call(&self, name: &str, args: serde_json::Value) -> crate::Outcome {
        self.inner.call(name, args).await
    }
    async fn report_progress(&self, partial: serde_json::Value) -> crate::ExtensionResult<()> {
        self.inner.report_progress(partial).await
    }
    async fn ui(&self, request: crate::UiRequest) -> crate::ExtensionResult<crate::UiResponse> {
        self.inner.ui(request).await
    }
    async fn send_message(
        &self,
        message: crate::CustomMessage,
        delivery: crate::CustomMessageDelivery,
    ) -> crate::ExtensionResult<()> {
        self.inner.send_message(message, delivery).await
    }
    async fn send_message_with_key(
        &self,
        message: crate::CustomMessage,
        delivery: crate::CustomMessageDelivery,
        key: &str,
    ) -> crate::ExtensionResult<()> {
        self.inner
            .send_message_with_key(message, delivery, key)
            .await
    }
}

#[derive(Clone)]
pub struct SessionControl {
    scope: Option<IdleInputScope>,
    owner: Arc<SessionOwner>,
    input: mpsc::Sender<crate::AgentInput>,
    active: Arc<Mutex<Option<CancellationToken>>>,
    auto_compaction: crate::AutoCompactionControl,
    retry: crate::RetryControl,
    input_hooks: Arc<Mutex<InputHooks>>,
    input_order: Arc<InputAdmission>,
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
        let delivery = if delivery == crate::CustomMessageDelivery::TriggerTurn {
            crate::CustomMessageDelivery::Steer
        } else {
            delivery
        };
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
    /// Capture the current idle callback's child lane for use in a spawned task.
    /// The returned control is valid only until that callback finishes.
    pub fn scoped(&self) -> Result<Self> {
        let _guard = self.scoped_guard()?;
        let scope = self
            .callback_scope()
            .ok_or_else(|| Error::Config("no idle input callback scope".into()))?;
        if scope.lifetime.is_cancelled() {
            return Err(Error::Config("idle input callback has finished".into()));
        }
        let mut control = self.clone();
        control.scope = Some(scope);
        Ok(control)
    }

    fn scoped_guard(&self) -> Result<Option<std::sync::MutexGuard<'_, ()>>> {
        let Some(scope) = &self.scope else {
            return Ok(None);
        };
        let guard = scope.gate.lock().expect("idle scope gate");
        if scope.lifetime.is_cancelled() {
            return Err(Error::Config("idle input callback has finished".into()));
        }
        Ok(Some(guard))
    }

    fn callback_scope(&self) -> Option<IdleInputScope> {
        let local = IDLE_INPUT_SCOPE
            .try_with(|scope| scope.clone())
            .ok()
            .filter(|scope| Arc::ptr_eq(&scope.owner, &self.owner));
        if let Some(explicit) = &self.scope {
            if !Arc::ptr_eq(&explicit.owner, &self.owner) {
                return None;
            }
            // An explicit handle remains bound to its original callback even
            // if it is used while another callback has a task-local scope.
            let mut scope = explicit.clone();
            if let Some(local) = local {
                scope.depth = scope.depth.max(local.depth);
            }
            return Some(scope);
        }
        local
    }

    pub fn set_retry_settings(&self, settings: crate::RetrySettings) -> Result<()> {
        let _guard = self.scoped_guard()?;
        if self.input.is_closed() {
            return Err(Error::Config("session is closed".into()));
        }
        self.retry.configure(settings);
        Ok(())
    }
    pub fn abort_retry(&self) -> Result<()> {
        let _guard = self.scoped_guard()?;
        if self.input.is_closed() {
            return Err(Error::Config("session is closed".into()));
        }
        self.retry.abort();
        Ok(())
    }
    pub fn set_auto_compaction_enabled(&self, enabled: bool) -> Result<()> {
        let _guard = self.scoped_guard()?;
        if self.input.is_closed() {
            return Err(Error::Config("session is closed".into()));
        }
        self.auto_compaction.set_enabled(enabled);
        Ok(())
    }
    pub fn set_compaction_tokens(&self, settings: crate::CompactionTokens) -> Result<()> {
        let _guard = self.scoped_guard()?;
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
        drop(self.scoped_guard()?);
        let child = self.callback_scope();
        if let Some(scope) = child {
            return self.enqueue_child(message, scope).await;
        }
        let order = self.input_order.acquire().await?;
        if self.input.is_closed() {
            return Err(Error::Config("session is closed".into()));
        }
        let hooks = self.input_hooks.lock().expect("SDK input hooks").clone();
        if !hooks.extensions.is_empty() {
            let active_token = self.active.lock().expect("session control").clone();
            let streaming = active_token
                .as_ref()
                .is_some_and(|token| !token.is_cancelled());
            if !streaming {
                return self.enqueue_idle(message, hooks, order).await;
            }
            let event = crate::InputEvent {
                text: message.input.content,
                images: message.images,
                source: crate::InputSource::Extension,
                streaming_behavior: streaming.then_some(kind),
            };
            let prepared = tokio::select! {
                biased;
                _ = self.input.closed() => return Err(Error::Config("session is closed".into())),
                _ = active_token.as_ref().expect("active operation").cancelled() => return Err(Error::Cancelled),
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
    async fn enqueue_child(
        &self,
        mut message: QueuedMessage,
        mut scope: IdleInputScope,
    ) -> Result<Uuid> {
        if scope.lifetime.is_cancelled() {
            return Err(Error::Config("idle input callback has finished".into()));
        }
        if scope.depth >= MAX_IDLE_INPUT_DEPTH {
            return Err(Error::Config(
                "idle input callback nesting limit exceeded".into(),
            ));
        }
        scope.depth += 1;
        let id = message.input.id;
        let hooks = self.input_hooks.lock().expect("SDK input hooks").clone();
        if !hooks.extensions.is_empty() {
            let event = crate::InputEvent {
                text: std::mem::take(&mut message.input.content),
                images: std::mem::take(&mut message.images),
                source: crate::InputSource::Extension,
                streaming_behavior: None,
            };
            let prepared = tokio::select! {
                biased;
                _ = scope.lifetime.cancelled() => return Err(Error::Config("idle input callback has finished".into())),
                _ = scope.cancellation.cancelled() => return Err(Error::Cancelled),
                result = IDLE_INPUT_SCOPE.scope(scope.clone(),
                    crate::extension::prepare_input_with_host(&hooks.extensions, event, &scope.states, scope.host.clone())) => result?,
            };
            let Some(prepared) = prepared else {
                return Ok(id);
            };
            message.input.content = prepared.text;
            message.images = prepared.images;
            message.validate()?;
        }
        scope.submit(message.into()).await?;
        Ok(id)
    }

    async fn enqueue_idle(
        &self,
        mut message: QueuedMessage,
        hooks: InputHooks,
        order: InputPermit,
    ) -> Result<Uuid> {
        let id = message.input.id;
        let closed = &self.owner.closed;
        let journal_owner = self
            .owner
            .journal
            .upgrade()
            .ok_or_else(|| Error::Config("session is closed".into()))?;
        let input_owner = self
            .owner
            .input
            .upgrade()
            .ok_or_else(|| Error::Config("session is closed".into()))?;
        let mut journal = tokio::select! {
            _ = closed.cancelled() => return Err(Error::Config("session is closed".into())),
            journal = journal_owner.lock() => journal,
        };
        let mut inputs = tokio::select! {
            _ = closed.cancelled() => return Err(Error::Config("session is closed".into())),
            inputs = input_owner.lock() => inputs,
        };
        if closed.is_cancelled() {
            return Err(Error::Config("session is closed".into()));
        }
        let token = CancellationToken::new();
        *self.active.lock().expect("session control") = Some(token.clone());
        let active = ActiveRun {
            token,
            state: self.active.clone(),
        };
        let events = self
            .events
            .upgrade()
            .ok_or_else(|| Error::Config("session is closed".into()))?;
        let mut observe = |event: Notification<'_>| {
            self.observe(&event);
            emit(&events, &event);
        };
        if let Some(record) = journal.recover_tree_trigger_handoff()?.cloned() {
            observe(Notification::Committed(&record));
            self.input_hooks.lock().expect("SDK input hooks").states = journal.extension_states();
        }
        if let Some((provider, name, _)) = journal.model_identity_at(journal.head()) {
            let mut config = self.owner.config.lock().expect("run config");
            let registry = config
                .registry
                .upgrade()
                .ok_or_else(|| Error::Config("session is closed".into()))?;
            let model = branch_client(&registry, journal.session_id(), provider.as_deref(), &name)
                .ok_or_else(|| {
                    Error::Config(format!("session branch requires registered model {name}"))
                })?;
            config.model = Arc::downgrade(&model);
            config.model_name = name;
        }
        let startup = self
            .owner
            .pending_start
            .lock()
            .expect("session startup")
            .take();
        if let Some((reason, previous_session_file)) = startup {
            self.notify_scoped_start(
                &mut journal,
                reason,
                previous_session_file,
                &active.token,
                &mut observe,
            )
            .await?;
        }
        let (sender, mut incoming) = mpsc::channel(32);
        let host: Arc<dyn crate::ToolHost> = Arc::new(OwnerBoundHost {
            inner: Arc::new(crate::agent::LifecycleMessageHost { sender, idle: true }),
            owner_id: self.owner.id,
        });
        let (children, mut child_requests) = mpsc::unbounded_channel();
        let event = crate::InputEvent {
            text: std::mem::take(&mut message.input.content),
            images: std::mem::take(&mut message.images),
            source: crate::InputSource::Extension,
            streaming_behavior: None,
        };
        let states = journal.extension_states();
        let scope = IdleInputScope {
            owner: self.owner.clone(),
            children,
            host: host.clone(),
            states: states.clone(),
            cancellation: active.token.clone(),
            lifetime: CancellationToken::new(),
            gate: Arc::new(Mutex::new(())),
            depth: 0,
        };
        let future = IDLE_INPUT_SCOPE.scope(
            scope.clone(),
            crate::extension::prepare_input_with_host(&hooks.extensions, event, &states, host),
        );
        let prepared = pump_idle_callback(
            &mut journal,
            &mut observe,
            &mut incoming,
            &mut child_requests,
            closed,
            &scope,
            future,
        )
        .await?;
        drop(child_requests);
        if let Some(prepared) = prepared {
            message.input.content = prepared.text;
            message.images = prepared.images;
            message.validate()?;
            // The receiver is held by this owner. Drain a full channel before
            // sending rather than awaiting capacity that only we can free.
            if self.input.capacity() == 0 {
                crate::agent::queue_inputs(&mut journal, &mut inputs, &mut observe)?;
            }
            self.input
                .try_send(message.into())
                .map_err(|_| Error::Config("session input channel is unavailable".into()))?;
        }
        let trigger = journal.pending_custom_trigger();
        // Host calls from the triggered run must be able to submit controls.
        drop(order);
        if let Some(trigger_id) = trigger {
            let config = self.owner.config.lock().expect("run config").clone();
            let model = config
                .model
                .upgrade()
                .ok_or_else(|| Error::Config("session is closed".into()))?;
            let registry = config
                .registry
                .upgrade()
                .ok_or_else(|| Error::Config("session is closed".into()))?;
            let agent = Agent {
                model: model.as_ref(),
                model_name: &config.model_name,
                registry: &registry,
                system_prompt: &config.system_prompt,
                limits: config.limits,
            };
            let result = agent
                .run_correlated(
                    &mut journal,
                    trigger_id,
                    None,
                    &mut inputs,
                    &active.token,
                    &mut observe,
                )
                .await;
            let final_inputs = active.finish_inputs(&mut journal, &mut inputs, &mut observe);
            result?;
            final_inputs?;
        } else {
            active.finish_inputs(&mut journal, &mut inputs, &mut observe)?;
        }
        Ok(id)
    }

    async fn notify_scoped_start(
        &self,
        journal: &mut Session,
        reason: crate::SessionStartReason,
        previous_session_file: Option<PathBuf>,
        cancellation: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<()> {
        let config = self.owner.config.lock().expect("run config").clone();
        let registry = config
            .registry
            .upgrade()
            .ok_or_else(|| Error::Config("session is closed".into()))?;
        if !registry.has_session_lifecycle_hooks() {
            return Ok(());
        }
        let states = journal.extension_states();
        let (sender, mut incoming) = mpsc::channel(32);
        let host: Arc<dyn crate::ToolHost> = Arc::new(OwnerBoundHost {
            inner: Arc::new(crate::agent::LifecycleMessageHost { sender, idle: true }),
            owner_id: self.owner.id,
        });
        let (children, mut child_requests) = mpsc::unbounded_channel();
        let scope = IdleInputScope {
            owner: self.owner.clone(),
            children,
            host: host.clone(),
            states: states.clone(),
            cancellation: cancellation.clone(),
            lifetime: CancellationToken::new(),
            gate: Arc::new(Mutex::new(())),
            depth: 0,
        };
        let mut errors = Vec::new();
        let mut collect = |event: Notification<'_>| {
            if let Notification::ExtensionLifecycleError {
                extension,
                event,
                message,
            } = event
            {
                errors.push((extension.to_owned(), event, message.to_owned()));
            }
        };
        let future = IDLE_INPUT_SCOPE.scope(scope.clone(), async {
            crate::lifecycle::notify_with_host(
                &registry,
                crate::LifecycleEvent::SessionStart {
                    reason,
                    previous_session_file,
                },
                &states,
                config.limits.tool_timeout,
                &mut collect,
                host,
            )
            .await;
            Ok(())
        });
        pump_idle_callback(
            journal,
            observer,
            &mut incoming,
            &mut child_requests,
            &self.owner.closed,
            &scope,
            future,
        )
        .await?;
        for (extension, event, message) in &errors {
            observer(Notification::ExtensionLifecycleError {
                extension,
                event,
                message,
            });
        }
        Ok(())
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
        let scope = self.callback_scope();
        if scope
            .as_ref()
            .is_some_and(|scope| scope.depth >= MAX_IDLE_INPUT_DEPTH)
        {
            return Err(Error::Config(
                "idle input callback nesting limit exceeded".into(),
            ));
        }
        let name = crate::sanitize_session_name(&name.into());
        self.queue_control(|reply| crate::AgentInput::SetSessionName {
            name: name.clone(),
            reply,
        })
        .await?;
        let hooks = self.input_hooks.lock().expect("SDK input hooks").clone();
        if let Some(registry) = hooks.lifecycle {
            let events = self.events.upgrade();
            let mut observe = |notification: Notification<'_>| {
                if let Some(events) = &events {
                    emit(events, &notification);
                }
            };
            let host = scope
                .as_ref()
                .map(|scope| scope.host.clone())
                .unwrap_or_else(|| {
                    Arc::new(InputMessageHost {
                        input: self.input.clone(),
                    })
                });
            let future = crate::lifecycle::notify_with_host(
                &registry,
                crate::LifecycleEvent::SessionInfoChanged { name: Some(name) },
                &hooks.states,
                self.tool_timeout,
                &mut observe,
                host,
            );
            if let Some(mut scope) = scope {
                scope.depth += 1;
                IDLE_INPUT_SCOPE.scope(scope, future).await;
            } else {
                future.await;
            }
        }
        Ok(())
    }
    async fn queue_control<T>(
        &self,
        input: impl FnOnce(tokio::sync::oneshot::Sender<Result<T>>) -> crate::AgentInput,
    ) -> Result<T> {
        drop(self.scoped_guard()?);
        let (reply, receiver) = tokio::sync::oneshot::channel();
        let child = self.callback_scope();
        if let Some(scope) = child {
            scope.submit(input(reply)).await?;
            return receiver
                .await
                .map_err(|_| Error::Config("queue control response was lost".into()))?;
        }
        let token = {
            let _order = self.input_order.acquire().await?;
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
        let Ok(_guard) = self.scoped_guard() else {
            return;
        };
        if let Some(active) = self.active.lock().expect("session control").as_ref() {
            active.cancel();
        }
    }

    fn observe(&self, event: &Notification<'_>) {
        if let Notification::BranchState(states) = event {
            self.input_hooks.lock().expect("SDK input hooks").states = (*states).clone();
            return;
        }
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

fn branch_client(
    registry: &Registry,
    session: Uuid,
    provider: Option<&str>,
    name: &str,
) -> Option<Arc<dyn Model>> {
    match provider {
        Some(provider) => registry
            .provider_branch_config(session, provider, name)
            .map(|(model, _)| model),
        None => registry.branch_model(session, name),
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
    model_provider: Option<String>,
    system_prompt: String,
    registry: Registry,
    limits: Limits,
    tool_timeout_explicit: bool,
    branch_models: std::collections::BTreeMap<String, Arc<dyn Model>>,
    provider_models: std::collections::BTreeMap<(String, String), Arc<dyn Model>>,
}
impl SessionBuilder {
    pub fn new(model: Arc<dyn Model>, model_name: impl Into<String>) -> Self {
        Self {
            model,
            model_name: model_name.into(),
            model_provider: None,
            system_prompt: "You are a coding assistant running inside pi, a coding agent harness. Help users inspect files, run commands, and modify code.".into(),
            registry: Registry::default(),
            limits: Limits::default(),
            tool_timeout_explicit: false,
            branch_models: Default::default(),
            provider_models: Default::default(),
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
    /// Register a client for a historical model name, including reopened,
    /// navigated and forked branches. Names must distinguish provider clients.
    pub fn branch_model(mut self, name: impl Into<String>, model: Arc<dyn Model>) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(Error::Config("model name is empty".into()));
        }
        self.branch_models.insert(name, model);
        Ok(self)
    }
    /// Identify the provider of the initial client. Without this, it can only
    /// restore legacy selections that do not name a provider.
    pub fn provider(mut self, provider: impl Into<String>) -> Result<Self> {
        let provider = provider.into();
        if provider.trim().is_empty() {
            return Err(Error::Config("provider name is empty".into()));
        }
        self.model_provider = Some(provider);
        Ok(self)
    }

    /// Register a client for an exact provider and model identity.
    pub fn provider_branch_model(
        mut self,
        provider: impl Into<String>,
        name: impl Into<String>,
        model: Arc<dyn Model>,
    ) -> Result<Self> {
        let (provider, name) = (provider.into(), name.into());
        if provider.trim().is_empty() || name.trim().is_empty() {
            return Err(Error::Config("provider or model name is empty".into()));
        }
        self.provider_models.insert((provider, name), model);
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
        for (name, model) in self.branch_models {
            self.registry
                .register_session_branch_model(session.session_id(), name, model)?;
        }
        for ((provider, name), model) in self.provider_models {
            self.registry.register_session_provider_model(
                session.session_id(),
                &provider,
                &name,
                model,
            )?;
        }
        if let Some(provider) = &self.model_provider {
            self.registry.register_session_provider_model(
                session.session_id(),
                provider,
                &self.model_name,
                self.model.clone(),
            )?;
        } else {
            self.registry.register_session_branch_model(
                session.session_id(),
                self.model_name.clone(),
                self.model.clone(),
            )?;
        }
        let (sender, input) = mpsc::channel(32);
        let (events, _) = broadcast::channel(64);
        let weak_events = events.downgrade();
        let tool_timeout = self.limits.tool_timeout;
        let auto_compaction = session.auto_compaction_control();
        let retry = session.retry_control();
        let input_hooks = Arc::new(Mutex::new(InputHooks::new(&self.registry, &session)));
        let session_id = session.session_id();
        let session = Arc::new(tokio::sync::Mutex::new(session));
        let input = Arc::new(tokio::sync::Mutex::new(input));
        let registry = Arc::new(self.registry);
        let pending_start = Arc::new(Mutex::new(Some((crate::SessionStartReason::Startup, None))));
        let owner = Arc::new(SessionOwner {
            id: Uuid::new_v4(),
            pending_start: pending_start.clone(),
            closed: CancellationToken::new(),
            journal: Arc::downgrade(&session),
            input: Arc::downgrade(&input),
            config: Mutex::new(RunConfig {
                model: Arc::downgrade(&self.model),
                model_name: self.model_name.clone(),
                system_prompt: self.system_prompt.clone(),
                registry: Arc::downgrade(&registry),
                limits: self.limits.clone(),
            }),
        });
        Ok(AgentSession {
            model: self.model,
            model_name: self.model_name,
            system_prompt: self.system_prompt,
            registry,
            limits: self.limits,
            session_id,
            session,
            input,
            events: Some(events),
            control: SessionControl {
                scope: None,
                owner,
                input: sender,
                active: Arc::new(Mutex::new(None)),
                auto_compaction,
                retry,
                input_hooks,
                input_order: Arc::new(InputAdmission::default()),
                events: weak_events,
                tool_timeout,
            },
            closed: false,
            pending_start,
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
    session_id: Uuid,
    session: Arc<tokio::sync::Mutex<Session>>,
    input: Arc<tokio::sync::Mutex<mpsc::Receiver<crate::AgentInput>>>,
    control: SessionControl,
    events: Option<broadcast::Sender<SessionEvent>>,
    closed: bool,
    pending_start: PendingStart,
    session_file: Option<PathBuf>,
}
impl AgentSession {
    fn validate_tree_model(&self, journal: &Session, selected: u64) -> Result<()> {
        if let Some(target) = journal.tree_navigation_target(selected)?
            && let Some((provider, name, _)) = journal.model_identity_at(target)
            && branch_client(&self.registry, self.session_id, provider.as_deref(), &name).is_none()
        {
            return Err(Error::Config(format!(
                "session branch requires registered model {name}"
            )));
        }
        Ok(())
    }
    fn restore_branch_model(&mut self, provider: Option<&str>, name: String) -> Result<()> {
        let model =
            branch_client(&self.registry, self.session_id, provider, &name).ok_or_else(|| {
                Error::Config(format!("session branch requires registered model {name}"))
            })?;
        self.model = model;
        self.model_name = name;
        self.refresh_run_config();
        Ok(())
    }
    pub fn auto_compaction_enabled(&self) -> bool {
        self.control.auto_compaction.enabled()
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
    /// Try to borrow the journal. Drop the guard before another session operation.
    pub fn journal(&self) -> Result<tokio::sync::MutexGuard<'_, Session>> {
        self.session
            .try_lock()
            .map_err(|_| Error::Session("session journal is in use".into()))
    }

    /// Wait asynchronously until the journal is available.
    pub async fn journal_async(&self) -> tokio::sync::MutexGuard<'_, Session> {
        self.session.lock().await
    }
    pub fn messages(&self) -> Result<Vec<Message>> {
        {
            let session = self.journal()?;
            session.conversation_at(session.head())
        }
    }
    /// Usage and message counts across the complete durable session.
    pub fn statistics(&self) -> Result<crate::SessionStats> {
        Ok(self.journal()?.statistics())
    }

    /// Resolve the configured or provider-reported context window within the model timeout.
    pub async fn context_usage(&self) -> Result<Option<crate::ContextUsage>> {
        self.ensure_open()?;
        let selection = {
            let journal = self.session.lock().await;
            journal.model_identity_at(journal.head())
        };
        let (name, model) = if let Some((provider, name, _)) = selection {
            let model = branch_client(&self.registry, self.session_id, provider.as_deref(), &name)
                .ok_or_else(|| {
                    Error::Config(format!("session branch requires registered model {name}"))
                })?;
            (name, model)
        } else {
            (self.model_name.clone(), self.model.clone())
        };
        let window = match self.limits.context_window {
            Some(window) => Some(window),
            None => tokio::time::timeout(self.limits.model_timeout, model.context_window(&name))
                .await
                .ok()
                .flatten(),
        };
        let journal = self.session.lock().await;
        Ok(window.and_then(|window| journal.context_usage(window)))
    }

    pub fn tools(&self) -> Vec<crate::ToolSpec> {
        self.registry.tool_specs()
    }
    /// Inspect the session's configured registry, including extension ownership
    /// and model-advertisement status for inactive as well as active tools.
    /// Per-request registry refreshes during a run are not reflected here.
    pub fn registered_tool_info(&self) -> Vec<crate::RegisteredToolInfo> {
        self.registry.registered_tool_info()
    }
    pub fn pending_inputs(&self) -> Result<usize> {
        let input = self
            .input
            .try_lock()
            .map_err(|_| Error::Session("session input is in use".into()))?;
        Ok(input.len() + self.journal()?.queued_inputs().len())
    }
    /// Clear pending input while this owned session is idle, retaining each removal in the journal.
    pub fn clear_queued_inputs(&mut self) -> Result<Vec<QueuedMessage>> {
        self.ensure_open()?;
        let events = self.events.as_ref().expect("open session").clone();
        let mut input = self
            .input
            .try_lock()
            .map_err(|_| Error::Session("session input is in use".into()))?;
        crate::agent::queue_inputs(
            &mut *self
                .session
                .try_lock()
                .map_err(|_| Error::Session("session journal is in use".into()))?,
            &mut *input,
            &mut |event| emit(&events, &event),
        )?;
        let count = self
            .session
            .try_lock()
            .map_err(|_| Error::Session("session journal is in use".into()))?
            .records()
            .len();
        let pending = self
            .session
            .try_lock()
            .map_err(|_| Error::Session("session journal is in use".into()))?
            .clear_queued_inputs()?;
        if self
            .session
            .try_lock()
            .map_err(|_| Error::Session("session journal is in use".into()))?
            .records()
            .len()
            > count
        {
            self.emit_last()?;
        }
        Ok(pending)
    }
    pub fn set_queue_mode(&mut self, kind: InputKind, mode: crate::QueueMode) -> Result<()> {
        self.ensure_open()?;
        let count = self
            .session
            .try_lock()
            .map_err(|_| Error::Session("session journal is in use".into()))?
            .records()
            .len();
        self.session
            .try_lock()
            .map_err(|_| Error::Session("session journal is in use".into()))?
            .set_queue_mode(kind, mode)?;
        if count
            != self
                .session
                .try_lock()
                .map_err(|_| Error::Session("session journal is in use".into()))?
                .records()
                .len()
        {
            self.emit_last()?;
        }
        Ok(())
    }
    pub fn model_name(&self) -> &str {
        &self.model_name
    }

    /// Supply a client for a historical model name in this session.
    pub fn register_branch_model(
        &self,
        name: impl Into<String>,
        model: Arc<dyn Model>,
    ) -> Result<()> {
        self.ensure_open()?;
        self.registry.register_session_branch_model(
            self.session
                .try_lock()
                .map_err(|_| Error::Session("session journal is in use".into()))?
                .session_id(),
            name.into(),
            model,
        )
    }

    /// Register a historical client without conflating identical model IDs.
    pub fn register_provider_branch_model(
        &self,
        provider: &str,
        name: &str,
        model: Arc<dyn Model>,
    ) -> Result<()> {
        self.ensure_open()?;
        self.registry
            .register_session_provider_model(self.session_id, provider, name, model)
    }

    pub fn set_model(&mut self, model: Arc<dyn Model>, name: impl Into<String>) -> Result<()> {
        self.set_model_identity(model, None, name.into())
    }

    /// Select and register a client under its exact provider and model identity.
    pub fn set_provider_model(
        &mut self,
        model: Arc<dyn Model>,
        provider: &str,
        name: impl Into<String>,
    ) -> Result<()> {
        if provider.trim().is_empty() {
            return Err(Error::Config("provider name is empty".into()));
        }
        self.set_model_identity(model, Some(provider), name.into())
    }

    fn set_model_identity(
        &mut self,
        model: Arc<dyn Model>,
        provider: Option<&str>,
        name: String,
    ) -> Result<()> {
        self.ensure_open()?;
        if name.trim().is_empty() || provider.is_some_and(|provider| provider.trim().is_empty()) {
            return Err(Error::Config("provider or model name is empty".into()));
        }
        let session = self.session.clone();
        let mut journal = session
            .try_lock()
            .map_err(|_| Error::Session("session journal is in use".into()))?;
        let count = journal.records().len();
        journal.select_provider_model(provider, &name, None)?;
        if let Some(provider) = provider {
            self.registry.register_session_provider_model(
                self.session_id,
                provider,
                &name,
                model.clone(),
            )?;
        } else {
            self.registry.register_session_branch_model(
                self.session_id,
                name.clone(),
                model.clone(),
            )?;
        }
        if count != journal.records().len()
            && let (Some(events), Some(record)) = (&self.events, journal.records().last())
        {
            emit(events, &Notification::Committed(record));
        }
        self.model = model;
        self.model_name = name;
        self.refresh_run_config();
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
        let name = name.into();
        if name.trim().is_empty() {
            return Err(Error::Config("model name is empty".into()));
        }
        let provider = match descriptor.get("provider") {
            Some(serde_json::Value::String(provider)) if !provider.trim().is_empty() => {
                Some(provider.clone())
            }
            Some(_) => return Err(Error::Config("invalid model provider".into())),
            None => None,
        };
        let (fallback_previous, startup_error) = self
            .start_with_fallback(Some((model.clone(), provider.clone(), name.clone())))
            .await?;
        let (previous_provider, previous_name, previous_level) = match fallback_previous {
            Some(previous) => previous,
            None => {
                let journal = self
                    .session
                    .try_lock()
                    .map_err(|_| Error::Session("session journal is in use".into()))?;
                let selection = journal.model_identity_at(journal.head());
                let (previous_provider, previous_level) = selection
                    .map(|(provider, _, level)| (provider, level))
                    .unwrap_or_default();
                (
                    previous_provider,
                    self.model_name.clone(),
                    previous_level.unwrap_or_else(|| "off".into()),
                )
            }
        };
        self.set_model_identity(model, provider.as_deref(), name.clone())?;
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
        if previous_name != name || previous_provider != provider || descriptor_changed {
            self.notify_selection_event(crate::LifecycleEvent::ModelSelect {
                model: descriptor,
                previous_model: previous_descriptor,
                source: crate::ModelSelectSource::Set,
            })
            .await?;
        }
        if let Some(error) = startup_error {
            return Err(error);
        }
        let _ = self.drain_idle_triggers().await?;
        Ok(())
    }

    async fn notify_selection_event(&mut self, event: crate::LifecycleEvent) -> Result<()> {
        if self.registry.has_model_selection_hooks() {
            let events = self.events.as_ref().expect("open session");
            crate::lifecycle::notify_idle_with_host(
                &self.registry,
                &mut *self
                    .session
                    .try_lock()
                    .map_err(|_| Error::Session("session journal is in use".into()))?,
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
        self.refresh_run_config();
        Ok(())
    }
    /// Register before forking. Forks share the existing registry and its connections.
    pub fn register_extension(&mut self, extension: Arc<dyn Extension>) -> Result<()> {
        self.ensure_open()?;
        if Arc::strong_count(&self.registry) != 1 {
            return Err(Error::Config(
                "register extensions before sharing this session with a fork".into(),
            ));
        }
        let result = Arc::make_mut(&mut self.registry).register(extension);
        self.refresh_run_config();
        result?;
        self.refresh_input_hooks()?;
        Ok(())
    }
    /// Check out a branch after validating that its selected model is registered.
    pub fn checkout(&mut self, record: u64) -> Result<()> {
        self.ensure_open()?;
        {
            let journal = self.journal()?;
            journal.validate_checkout(record)?;
            if let Some((provider, name, _)) = journal.model_identity_at(record)
                && branch_client(&self.registry, self.session_id, provider.as_deref(), &name)
                    .is_none()
            {
                return Err(Error::Config(format!(
                    "session branch requires registered model {name}"
                )));
            }
        }
        self.session
            .try_lock()
            .map_err(|_| Error::Session("session journal is in use".into()))?
            .checkout(record)?;
        self.refresh_input_hooks()?;
        self.emit_last()?;
        let selection = {
            let journal = self.journal()?;
            journal.model_identity_at(journal.head())
        };
        if let Some((provider, model, _)) = selection {
            self.restore_branch_model(provider.as_deref(), model)?;
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
        self.ensure_open()?;
        {
            let journal = self.session.lock().await;
            self.validate_tree_model(&journal, selected)?;
        }
        self.start().await?;
        let session = self.session.clone();
        let mut journal = session.lock().await;
        self.validate_tree_model(&journal, selected)?;
        let first_record = journal.records().len();
        let editor_text = {
            journal
                .records()
                .iter()
                .find(|record| record.id == selected && selected != journal.head())
                .and_then(|record| match &record.event {
                    crate::Event::User { content, .. }
                    | crate::Event::UserWithImages { content, .. } => Some(content.clone()),
                    _ => None,
                })
        };
        let input = self.input.clone();
        let mut input_queue = input.lock().await;
        let active = self.begin_run();
        let events = self.events.as_ref().expect("open session").clone();
        let agent = Agent {
            model: self.model.as_ref(),
            model_name: &self.model_name,
            registry: &self.registry,
            system_prompt: &self.system_prompt,
            limits: self.limits.clone(),
        };
        let control = self.control.clone();
        let mut observer = |event: Notification<'_>| {
            control.observe(&event);
            emit(&events, &event);
        };
        let mut result = agent
            .navigate_tree_with_options(
                &mut journal,
                selected,
                options,
                &active.token,
                &mut observer,
            )
            .await;
        if result.is_ok() {
            *self.control.input_hooks.lock().expect("SDK input hooks") =
                InputHooks::new(&self.registry, &journal);
            if let Some((provider, model, _)) = journal.model_identity_at(journal.head()) {
                result = self
                    .restore_branch_model(provider.as_deref(), model)
                    .map(|_| ());
            }
        }
        let final_inputs = active.finish_inputs(&mut journal, &mut input_queue, &mut observer);
        let result = result.and(final_inputs);
        if let Err(error) = result {
            drop(input_queue);
            drop(journal);
            let _ = self.drain_idle_triggers().await;
            return Err(error);
        }
        let summary_entry = journal.records()[first_record..]
            .iter()
            .find(|record| matches!(record.event, crate::Event::BranchSummary { .. }))
            .cloned()
            .map(Box::new);
        drop(input_queue);
        drop(journal);
        let _ = self.drain_idle_triggers().await?;
        Ok(TreeNavigationResult {
            editor_text,
            summary_entry,
        })
    }
    pub fn set_name(&mut self, name: &str) -> Result<()> {
        self.ensure_open()?;
        self.session
            .try_lock()
            .map_err(|_| Error::Session("session journal is in use".into()))?
            .set_name(name)?;
        self.emit_last()?;
        Ok(())
    }

    /// Save the name and deliver the resulting metadata to session lifecycle extensions.
    pub async fn set_name_with_hooks(&mut self, name: &str) -> Result<()> {
        self.start().await?;
        self.set_name(name)?;
        let name = self.journal()?.name().map(str::to_owned);
        self.notify_session_event(crate::LifecycleEvent::SessionInfoChanged { name })
            .await?;
        let _ = self.drain_idle_triggers().await?;
        Ok(())
    }

    /// Deliver the session start hook once. Model, tree, and hook-aware rename operations also start it.
    pub async fn start(&mut self) -> Result<()> {
        let (_, error) = self.start_with_fallback(None).await?;
        error.map_or(Ok(()), Err)
    }

    // Explicit model replacement is available during startup only when the
    // recovered branch's client is missing. Install it before startup hooks
    // can trigger work, while retaining the old selection for notifications.
    async fn start_with_fallback(
        &mut self,
        fallback: Option<(Arc<dyn Model>, Option<String>, String)>,
    ) -> Result<(Option<(Option<String>, String, String)>, Option<Error>)> {
        self.ensure_open()?;
        let order = self.control.input_order.acquire().await?;
        let session = self.session.clone();
        let mut journal = session.lock().await;
        let recovered = journal.recover_tree_trigger_handoff()?.cloned();
        if let Some(record) = recovered {
            if let Some(events) = &self.events {
                emit(events, &Notification::Committed(&record));
            }
            *self.control.input_hooks.lock().expect("SDK input hooks") =
                InputHooks::new(&self.registry, &journal);
        }
        let mut replaced = None;
        if let Some((provider, name, level)) = journal.model_identity_at(journal.head()) {
            if branch_client(&self.registry, self.session_id, provider.as_deref(), &name).is_some()
            {
                self.restore_branch_model(provider.as_deref(), name)?;
            } else if let Some((model, replacement_provider, replacement)) = fallback {
                let before = journal.records().len();
                journal.select_provider_model(
                    replacement_provider.as_deref(),
                    &replacement,
                    None,
                )?;
                if let Some(provider) = &replacement_provider {
                    self.registry.register_session_provider_model(
                        self.session_id,
                        provider,
                        &replacement,
                        model.clone(),
                    )?;
                } else {
                    self.registry.register_session_branch_model(
                        self.session_id,
                        replacement.clone(),
                        model.clone(),
                    )?;
                }
                if journal.records().len() != before {
                    emit(
                        self.events.as_ref().expect("open session"),
                        &Notification::Committed(
                            journal.records().last().expect("model selection"),
                        ),
                    );
                }
                self.model = model;
                self.model_name = replacement;
                self.refresh_run_config();
                replaced = Some((provider, name, level.unwrap_or_else(|| "off".into())));
            } else {
                self.restore_branch_model(provider.as_deref(), name)?;
            }
        }
        let startup = self.pending_start.lock().expect("session startup").take();
        let startup_result = if let Some((reason, previous_session_file)) = startup {
            let active = self.begin_run();
            let events = self.events.as_ref().expect("open session").clone();
            let mut observe = |event: Notification<'_>| {
                self.control.observe(&event);
                emit(&events, &event);
            };
            self.control
                .notify_scoped_start(
                    &mut journal,
                    reason,
                    previous_session_file,
                    &active.token,
                    &mut observe,
                )
                .await
        } else {
            Ok(())
        };
        drop(journal);
        drop(order);
        let startup_result = match startup_result {
            Ok(()) => self.drain_idle_triggers().await.map(|_| ()),
            Err(error) => Err(error),
        };
        match startup_result {
            Ok(()) => Ok((replaced, None)),
            Err(error) if replaced.is_some() => Ok((replaced, Some(error))),
            Err(error) => Err(error),
        }
    }

    async fn drain_idle_triggers(&mut self) -> Result<Option<String>> {
        let mut journal = self.session.lock().await;
        let Some(trigger_id) = journal.pending_custom_trigger() else {
            return Ok(None);
        };
        let mut input_queue = self.input.lock().await;
        let active = self.begin_run();
        let events = self.events.as_ref().expect("open session").clone();
        let mut observe = |event: Notification<'_>| {
            self.control.observe(&event);
            emit(&events, &event);
        };
        let agent = Agent {
            model: self.model.as_ref(),
            model_name: &self.model_name,
            registry: &self.registry,
            system_prompt: &self.system_prompt,
            limits: self.limits.clone(),
        };
        let result = agent
            .run_correlated(
                &mut journal,
                trigger_id,
                None,
                &mut input_queue,
                &active.token,
                &mut observe,
            )
            .await;
        let final_inputs = active.finish_inputs(&mut journal, &mut input_queue, &mut observe);
        let answer = result?;
        final_inputs?;
        Ok(Some(answer))
    }

    async fn notify_session_event(&mut self, event: crate::LifecycleEvent) -> Result<()> {
        if self.registry.has_session_lifecycle_hooks() {
            let events = self.events.as_ref().expect("open session");
            crate::lifecycle::notify_idle_with_host(
                &self.registry,
                &mut *self
                    .session
                    .try_lock()
                    .map_err(|_| Error::Session("session journal is in use".into()))?,
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
        self.session
            .try_lock()
            .map_err(|_| Error::Session("session journal is in use".into()))?
            .edit_context(target, replacement)?;
        self.emit_last()?;
        Ok(())
    }
    /// Record a user shell result and notify subscribers without requesting a model turn.
    pub fn append_bash_execution(&mut self, execution: crate::BashExecution) -> Result<()> {
        self.ensure_open()?;
        self.session
            .try_lock()
            .map_err(|_| Error::Session("session journal is in use".into()))?
            .append_bash_execution(execution)?;
        self.emit_last()?;
        Ok(())
    }

    /// Persist extension data on the active branch without changing model context.
    pub fn append_custom_entry(&mut self, entry: crate::CustomEntry) -> Result<()> {
        self.ensure_open()?;
        self.session
            .try_lock()
            .map_err(|_| Error::Session("session journal is in use".into()))?
            .append_custom_entry(entry)?;
        self.emit_last()?;
        Ok(())
    }

    /// Persist an extension message in model context without starting a turn.
    pub fn append_custom_message(&mut self, message: crate::CustomMessage) -> Result<()> {
        self.ensure_open()?;
        self.session
            .try_lock()
            .map_err(|_| Error::Session("session journal is in use".into()))?
            .append_custom_message(message)?;
        self.emit_last()?;
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
        if trigger_turn {
            let events = self.events.as_ref().expect("open session").clone();
            crate::agent::queue_custom_message(
                &mut *self
                    .session
                    .try_lock()
                    .map_err(|_| Error::Session("session journal is in use".into()))?,
                crate::QueuedCustomMessage {
                    id: Uuid::new_v4(),
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                    message,
                },
                &mut |event| emit(&events, &event),
            )?;
            self.drain_idle_triggers().await
        } else {
            self.append_custom_message(message)?;
            Ok(None)
        }
    }
    /// Queue custom context for the next run without starting it.
    pub fn queue_custom_message_next_turn(&mut self, message: crate::CustomMessage) -> Result<()> {
        self.ensure_open()?;
        let events = self.events.as_ref().expect("open session").clone();
        crate::agent::queue_custom_message(
            &mut *self
                .session
                .try_lock()
                .map_err(|_| Error::Session("session journal is in use".into()))?,
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
        self.session
            .try_lock()
            .map_err(|_| Error::Session("session journal is in use".into()))?
            .set_label(target, label)?;
        self.emit_last()?;
        Ok(())
    }
    pub fn fork_at(&self, record: u64) -> Result<Self> {
        self.ensure_open()?;
        let session = Session::from_records(self.journal()?.fork_records(record)?)?;
        self.fork_session(session)
    }
    pub fn fork_to(&self, record: u64, destination: impl AsRef<Path>) -> Result<Self> {
        self.ensure_open()?;
        let destination = std::path::absolute(destination)?;
        let journal = self.journal()?;
        let records = journal.fork_records(record)?;
        let snapshot = Session::from_records(records)?;
        if let Some((provider, name, _)) = snapshot.model_identity_at(snapshot.head())
            && branch_client(&self.registry, self.session_id, provider.as_deref(), &name).is_none()
        {
            return Err(Error::Config(format!(
                "session branch requires registered model {name}"
            )));
        }
        let forked = journal.fork_to(record, &destination)?;
        drop(journal);
        let mut fork = self.fork_session(forked)?;
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
        let journal = self.journal()?;
        let record = journal
            .records()
            .iter()
            .find(|record| record.id == entry_id)
            .ok_or_else(|| Error::Session("invalid fork entry".into()))?;
        let target = match position {
            crate::ForkPosition::Before => record.parent.unwrap_or(1),
            crate::ForkPosition::At => record.id,
        };
        drop(journal);
        self.start().await?;
        if self.registry.has_session_transition_hooks() {
            let events = self.events.as_ref().expect("open session");
            let transition = crate::BeforeSessionTransition::Fork { entry_id, position };
            let cancelled = self
                .registry
                .session_transition_cancelled_in_session(
                    &transition,
                    &mut *self
                        .session
                        .try_lock()
                        .map_err(|_| Error::Session("session journal is in use".into()))?,
                    self.limits.tool_timeout,
                    &mut |event| emit(events, &event),
                )
                .await?;
            let _ = self.drain_idle_triggers().await?;
            if cancelled {
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
        self.session
            .try_lock()
            .map_err(|_| Error::Session("session journal is in use".into()))?
            .export_jsonl(destination)
    }
    fn fork_session(&self, session: Session) -> Result<Self> {
        session.retry_control().configure(
            self.session
                .try_lock()
                .map_err(|_| Error::Session("session journal is in use".into()))?
                .retry_control()
                .settings(),
        );
        let selection = session.model_identity_at(session.head());
        let (provider, model_name, model) = if let Some((provider, name, _)) = selection {
            let model = branch_client(&self.registry, self.session_id, provider.as_deref(), &name)
                .ok_or_else(|| {
                    Error::Config(format!("session branch requires registered model {name}"))
                })?;
            (provider, name, model)
        } else {
            (None, self.model_name.clone(), self.model.clone())
        };
        session
            .auto_compaction_control()
            .set_enabled(self.auto_compaction_enabled());
        session.auto_compaction_control().set_token_settings(
            self.session
                .try_lock()
                .map_err(|_| Error::Session("session journal is in use".into()))?
                .auto_compaction_control()
                .token_settings(),
        );
        let mut builder = SessionBuilder::new(model, model_name);
        if let Some(provider) = provider {
            builder = builder.provider(provider)?;
        }
        let mut fork = builder
            .system_prompt(self.system_prompt.clone())
            .limits(self.limits.clone())
            .build(session)?;
        self.registry.copy_session_branch_models(
            self.session
                .try_lock()
                .map_err(|_| Error::Session("session journal is in use".into()))?
                .session_id(),
            fork.session
                .try_lock()
                .map_err(|_| Error::Session("session journal is in use".into()))?
                .session_id(),
        );
        fork.registry = self.registry.clone();
        fork.refresh_run_config();
        *fork.pending_start.lock().expect("session startup") =
            Some((crate::SessionStartReason::Fork, self.session_file.clone()));
        fork.refresh_input_hooks()?;
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
        let journal_owner = self
            .control
            .owner
            .journal
            .upgrade()
            .ok_or_else(|| Error::Config("session is closed".into()))?;
        let input_owner = self
            .control
            .owner
            .input
            .upgrade()
            .ok_or_else(|| Error::Config("session is closed".into()))?;
        let mut journal = journal_owner.lock().await;
        let mut input_queue = input_owner.lock().await;
        let active = self.begin_run();
        let events = self.events.as_ref().expect("open session").clone();
        let mut observe = |event: Notification<'_>| {
            self.control.observe(&event);
            emit(&events, &event);
            observer(event);
        };
        let config = self
            .control
            .owner
            .config
            .lock()
            .expect("run config")
            .clone();
        let registry = config
            .registry
            .upgrade()
            .ok_or_else(|| Error::Config("session is closed".into()))?;
        let model = config
            .model
            .upgrade()
            .ok_or_else(|| Error::Config("session is closed".into()))?;
        let agent = Agent {
            model: model.as_ref(),
            model_name: &config.model_name,
            registry: &registry,
            system_prompt: &config.system_prompt,
            limits: config.limits.clone(),
        };
        let mut result = async {
            if prepare && self.registry.has_input_hooks() {
                let event = crate::InputEvent {
                    text: input.take().unwrap_or_default(),
                    images: images.take().unwrap_or_default(),
                    source: crate::InputSource::Extension,
                    streaming_behavior: None,
                };
                let prepared = prepare_idle_input_with_host(
                    &self.registry,
                    &mut journal,
                    event,
                    &active.token,
                    &mut observe,
                    Some(&self.control),
                )
                .await?;
                let pending_trigger = journal.pending_custom_trigger();
                if let Some(trigger_id) = pending_trigger {
                    agent
                        .run_correlated(
                            &mut journal,
                            trigger_id,
                            None,
                            &mut input_queue,
                            &active.token,
                            &mut observe,
                        )
                        .await?;
                }
                let Some(prepared) = prepared else {
                    return Ok(String::new());
                };
                input = Some(prepared.text);
                images = (!prepared.images.is_empty()).then_some(prepared.images);
            }
            if let Some(images) = images {
                agent
                    .resolve_prior_turn_end(&mut journal, run_id, &active.token, &mut observe)
                    .await?;
                journal.append_user_with_images(
                    input.take().unwrap_or_default(),
                    images,
                    Some(run_id),
                )?;
                observe(Notification::Committed(
                    journal.records().last().expect("appended user message"),
                ));
            }
            agent
                .run_correlated(
                    &mut journal,
                    run_id,
                    input,
                    &mut input_queue,
                    &active.token,
                    &mut observe,
                )
                .await
        }
        .await;
        let final_inputs = active.finish_inputs(&mut journal, &mut input_queue, &mut observe);
        if result.is_ok()
            && let Err(error) = final_inputs
        {
            result = Err(error);
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
            &mut *self
                .session
                .try_lock()
                .map_err(|_| Error::Session("session journal is in use".into()))?,
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
            &mut *self
                .session
                .try_lock()
                .map_err(|_| Error::Session("session journal is in use".into()))?,
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
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| {
                            Error::Config(format!("could not start compaction runtime: {error}"))
                        })?;
                    let result = runtime.block_on(self.compact_with_hooks(compaction));
                    // A timed-out extension may have spawned blocking work that
                    // cannot be cancelled. Do not wait forever for it at shutdown.
                    runtime.shutdown_timeout(std::time::Duration::from_millis(100));
                    result
                })
                .join()
                .map_err(|_| Error::Config("compaction worker panicked".into()))?
        })
    }
    /// Compact a supplied summary while servicing asynchronous extension host calls.
    pub async fn compact_with_hooks(&mut self, compaction: Compaction) -> Result<()> {
        self.start().await?;
        let mut journal = self.session.lock().await;
        let mut input_queue = self.input.lock().await;
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
            .compact_with_host_and_inputs(
                &mut journal,
                compaction,
                &active.token,
                &mut observer,
                &mut input_queue,
            )
            .await;
        let final_inputs = active.finish_inputs(&mut journal, &mut input_queue, &mut observer);
        let result = result.and(final_inputs);
        drop(input_queue);
        drop(journal);
        let triggered = self.drain_idle_triggers().await.map(|_| ());
        result.and(triggered)
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
        let mut journal = self.session.lock().await;
        let mut input_queue = self.input.lock().await;
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
                        &mut journal,
                        keep_turns,
                        &active.token,
                        &mut observer,
                        Some(&mut input_queue),
                        instructions,
                    )
                    .await
            }
            None => {
                agent
                    .summarize_configured(
                        &mut journal,
                        &active.token,
                        &mut observer,
                        Some(&mut input_queue),
                        instructions,
                    )
                    .await
            }
        };
        let final_inputs = active.finish_inputs(&mut journal, &mut input_queue, &mut observer);
        let result = result.and(final_inputs);
        drop(input_queue);
        drop(journal);
        let triggered = self.drain_idle_triggers().await.map(|_| ());
        result.and(triggered)
    }
    pub async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.control.owner.closed.cancel();
        self.control.abort();
        {
            let mut input = self.input.lock().await;
            input.close();
            while input.try_recv().is_ok() {}
        }
        *self.control.input_hooks.lock().expect("SDK input hooks") = InputHooks::default();
        let started = self
            .pending_start
            .lock()
            .expect("session startup")
            .is_none();
        let notification_result = if started {
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
    fn refresh_run_config(&self) {
        *self.control.owner.config.lock().expect("run config") = RunConfig {
            model: Arc::downgrade(&self.model),
            model_name: self.model_name.clone(),
            system_prompt: self.system_prompt.clone(),
            registry: Arc::downgrade(&self.registry),
            limits: self.limits.clone(),
        };
    }
    fn emit_last(&self) -> Result<()> {
        let journal = self.journal()?;
        if let (Some(events), Some(record)) = (&self.events, journal.records().last()) {
            emit(events, &Notification::Committed(record));
        }
        Ok(())
    }
    fn refresh_input_hooks(&self) -> Result<()> {
        let journal = self.journal()?;
        *self.control.input_hooks.lock().expect("SDK input hooks") =
            InputHooks::new(&self.registry, &journal);
        Ok(())
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
        self.control.owner.closed.cancel();
        self.control.abort();
        self.registry.remove_session_branch_models(self.session_id);
        *self.control.input_hooks.lock().expect("SDK input hooks") = InputHooks::default();
    }
}
struct IdleScopeGuard {
    lifetime: CancellationToken,
    gate: Arc<Mutex<()>>,
}
impl Drop for IdleScopeGuard {
    fn drop(&mut self) {
        let _guard = self.gate.lock().expect("idle scope gate");
        self.lifetime.cancel();
    }
}

pub(crate) async fn scope_extension_callback<F: std::future::Future>(
    owner: Option<Uuid>,
    future: F,
) -> F::Output {
    let parent = IDLE_INPUT_SCOPE
        .try_with(|scope| scope.clone())
        .ok()
        .filter(|scope| owner == Some(scope.owner.id));
    if let Some(mut scope) = parent {
        scope.lifetime = scope.lifetime.child_token();
        scope.host = Arc::new(ScopedIdleHost {
            inner: scope.host.clone(),
            scope: scope.clone(),
        });
        let _guard = IdleScopeGuard {
            lifetime: scope.lifetime.clone(),
            gate: scope.gate.clone(),
        };
        IDLE_INPUT_SCOPE.scope(scope, future).await
    } else {
        future.await
    }
}

async fn pump_idle_callback<T, F>(
    journal: &mut Session,
    observer: &mut Observer<'_>,
    incoming: &mut mpsc::Receiver<(
        crate::QueuedCustomMessage,
        tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    )>,
    children: &mut mpsc::UnboundedReceiver<IdleChildRequest>,
    closed: &CancellationToken,
    scope: &IdleInputScope,
    future: F,
) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let _scope_guard = IdleScopeGuard {
        lifetime: scope.lifetime.clone(),
        gate: scope.gate.clone(),
    };
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            Some((input, reply)) = incoming.recv() => {
                let result = if matches!(input.delivery, crate::CustomMessageDelivery::NextTurn | crate::CustomMessageDelivery::TriggerTurn) {
                    crate::agent::queue_custom_message(journal, input, observer)
                } else {
                    journal.append_custom_message(input.message).map(|()| observer(Notification::Committed(journal.records().last().expect("appended custom message"))))
                };
                match result {
                    Ok(()) => { let _ = reply.send(Ok(())); }
                    Err(error) => { let _ = reply.send(Err(error.to_string())); return Err(error); }
                }
            }
            Some(child) = children.recv() => {
                if child.reply.is_closed() { continue; }
                let _guard = child.gate.lock().expect("idle scope gate");
                if child.lifetime.is_cancelled() || scope.cancellation.is_cancelled() || closed.is_cancelled() {
                    let _ = child.reply.send(Err(Error::Cancelled));
                    continue;
                }
                let result = child.input.apply(journal, observer);
                let _ = child.reply.send(result);
            }
            _ = closed.cancelled() => return Err(Error::Config("session is closed".into())),
            _ = scope.cancellation.cancelled() => return Err(Error::Cancelled),
            result = &mut future => return result,
        }
    }
}

/// Prepare an input while the session owner services durable extension messages.
/// The owner must continue polling this future until completion or cancellation;
/// acknowledging a host call before its journal write would lose messages on restart.
async fn prepare_idle_input_with_host(
    registry: &Registry,
    session: &mut Session,
    event: crate::InputEvent,
    cancellation: &CancellationToken,
    observer: &mut Observer<'_>,
    control: Option<&SessionControl>,
) -> Result<Option<crate::InputEvent>> {
    let states = session.extension_states();
    let (sender, mut incoming) = mpsc::channel(32);
    let host: Arc<dyn crate::ToolHost> =
        Arc::new(crate::agent::LifecycleMessageHost { sender, idle: true });
    if let Some(control) = control {
        let host: Arc<dyn crate::ToolHost> = Arc::new(OwnerBoundHost {
            inner: host,
            owner_id: control.owner.id,
        });
        let (children, mut child_requests) = mpsc::unbounded_channel();
        let scope = IdleInputScope {
            owner: control.owner.clone(),
            children,
            host: host.clone(),
            states: states.clone(),
            cancellation: cancellation.clone(),
            lifetime: CancellationToken::new(),
            gate: Arc::new(Mutex::new(())),
            depth: 0,
        };
        let future = IDLE_INPUT_SCOPE.scope(
            scope.clone(),
            registry.prepare_input_with_host(event, &states, host),
        );
        return pump_idle_callback(
            session,
            observer,
            &mut incoming,
            &mut child_requests,
            &control.owner.closed,
            &scope,
            future,
        )
        .await;
    }
    let future = registry.prepare_input_with_host(event, &states, host);
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            Some((input, reply)) = incoming.recv() => {
                let result = if matches!(input.delivery, crate::CustomMessageDelivery::NextTurn | crate::CustomMessageDelivery::TriggerTurn) {
                    crate::agent::queue_custom_message(session, input, observer)
                } else {
                    session.append_custom_message(input.message).map(|()| {
                        observer(Notification::Committed(session.records().last().expect("appended custom message")));
                    })
                };
                match result {
                    Ok(()) => { let _ = reply.send(Ok(())); }
                    Err(error) => { let _ = reply.send(Err(error.to_string())); return Err(error); }
                }
            }
            _ = cancellation.cancelled() => return Err(Error::Cancelled),
            result = &mut future => return result,
        }
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
        Notification::RunStarted { run_id } => SessionEvent::RunStarted { run_id: *run_id },
        Notification::RunEnded {
            run_id, succeeded, ..
        } => SessionEvent::RunEnded {
            run_id: *run_id,
            succeeded: *succeeded,
        },
        Notification::Committed(record) => SessionEvent::Record(Arc::new((*record).clone())),
        Notification::ToolInventory {
            run_id,
            phase,
            registry,
        } => SessionEvent::ToolInventory {
            run_id: *run_id,
            phase: *phase,
            tools: Arc::new(registry.registered_tool_info()),
        },
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
        Notification::SummaryRequest
        | Notification::ModelContext { .. }
        | Notification::ToolProgress(_)
        | Notification::BranchState(_) => return,
        Notification::QueueUpdated {
            steering,
            follow_up,
        } => SessionEvent::QueueUpdated {
            steering: steering.iter().map(|text| (*text).to_owned()).collect(),
            follow_up: follow_up.iter().map(|text| (*text).to_owned()).collect(),
        },
    };
    let _ = events.send(event);
    if let Notification::RunEnded {
        run_id,
        error: Some(error),
        ..
    } = notification
    {
        let _ = events.send(SessionEvent::RunFailed {
            run_id: *run_id,
            message: (*error).into(),
        });
    }
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

    #[tokio::test]
    async fn shared_owner_tracks_configuration_and_releases_session_resources() {
        let first: Arc<dyn Model> = Arc::new(WaitingModel);
        let mut session = SessionBuilder::new(first, "first").in_memory().unwrap();
        let control = session.controls();
        session.set_system_prompt("updated prompt").unwrap();
        let second: Arc<dyn Model> = Arc::new(WaitingModel);
        session.set_model(second.clone(), "second").unwrap();
        session
            .register_extension(Arc::new(InputExtension))
            .unwrap();
        {
            let config = control.owner.config.lock().unwrap().clone();
            assert_eq!(config.model_name, "second");
            assert_eq!(config.system_prompt, "updated prompt");
            assert!(Arc::ptr_eq(&config.model.upgrade().unwrap(), &second));
            assert!(Arc::ptr_eq(
                &config.registry.upgrade().unwrap(),
                &session.registry
            ));
            assert!(config.registry.upgrade().unwrap().has_input_hooks());
            assert_eq!(config.limits.tool_timeout, session.limits.tool_timeout);
        }
        let head = session.journal().unwrap().head();
        let fork = session.fork_at(head).unwrap();
        {
            let config = fork.control.owner.config.lock().unwrap().clone();
            assert_eq!(config.model_name, "second");
            assert!(Arc::ptr_eq(
                &config.registry.upgrade().unwrap(),
                &fork.registry
            ));
        }
        drop(fork);
        session.close().await.unwrap();
        drop(session);
        drop(second);
        assert!(control.owner.journal.upgrade().is_none());
        assert!(control.owner.input.upgrade().is_none());
        assert!(
            control
                .owner
                .config
                .lock()
                .unwrap()
                .model
                .upgrade()
                .is_none()
        );
        assert!(
            control
                .owner
                .config
                .lock()
                .unwrap()
                .registry
                .upgrade()
                .is_none()
        );
    }

    #[tokio::test]
    async fn admission_tickets_keep_order_and_skip_cancelled_waiters() {
        let admission = Arc::new(InputAdmission::default());
        let first = admission.acquire().await.unwrap();
        let mut cancelled = Box::pin(admission.acquire());
        let mut third = Box::pin(admission.acquire());
        let mut fourth = Box::pin(admission.acquire());
        assert!(futures_util::poll!(cancelled.as_mut()).is_pending());
        assert!(futures_util::poll!(third.as_mut()).is_pending());
        assert!(futures_util::poll!(fourth.as_mut()).is_pending());
        assert_eq!(first.ticket, 0);
        drop(cancelled);
        drop(first);
        let third = third.await.unwrap();
        assert_eq!(third.ticket, 2);
        assert!(futures_util::poll!(fourth.as_mut()).is_pending());
        drop(third);
        assert_eq!(fourth.await.unwrap().ticket, 3);
    }

    #[tokio::test]
    async fn journal_contention_is_reported_and_async_access_waits() {
        let session = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
            .in_memory()
            .unwrap();
        let guard = session.journal().unwrap();
        assert!(session.journal().is_err());
        let waiting = session.journal_async();
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut waiting)
                .await
                .is_err()
        );
        drop(guard);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
                .await
                .is_ok()
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
        let previous = session.journal().unwrap().head();
        session
            .extension_command("message-extension", "context", "")
            .unwrap();
        let records = session.journal().unwrap().records().to_vec();
        assert!(matches!(
            records[records.len() - 2].event,
            crate::Event::CustomEntry { .. }
        ));
        assert!(matches!(
            records.last().unwrap().event,
            crate::Event::CustomMessage { .. }
        ));
        let target = session.journal().unwrap().head();
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
            .try_lock()
            .unwrap()
            .append_user_with_images("hello".into(), Vec::new(), None)
            .unwrap();
        let previous = session.journal().unwrap().head();
        let before = session.messages().unwrap();
        session
            .extension_command("bookmark-extension", "bookmark", "remember")
            .unwrap();
        let bookmarked = session.journal().unwrap().head();
        assert!(
            matches!(&session.journal().unwrap().records().last().unwrap().event, crate::Event::CustomEntry { entry } if entry.custom_type == "bookmark" && entry.data.as_ref().unwrap()["note"] == "remember")
        );
        assert_eq!(
            serde_json::to_value(session.messages().unwrap()).unwrap(),
            serde_json::to_value(before).unwrap()
        );
        session.checkout(previous).unwrap();
        assert!(
            !session
                .journal()
                .unwrap()
                .branch()
                .iter()
                .any(|record| matches!(record.event, crate::Event::CustomEntry { .. }))
        );
        session.checkout(bookmarked).unwrap();
        assert!(
            session
                .journal()
                .unwrap()
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
            matches!(&session.journal().unwrap().records().last().unwrap().event, crate::Event::CustomEntry { entry } if entry.custom_type == "sdk")
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
        session
            .session
            .try_lock()
            .unwrap()
            .select_model("first", Some("high"))
            .unwrap();
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
            session.journal().unwrap().model_selection(),
            Some(("second".into(), None))
        );
        session
            .set_model_with_hooks(
                Arc::new(WaitingModel),
                "second",
                serde_json::json!({"id":"second", "provider":"fixture"}),
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

    struct ReplacementStartupHook(Arc<Mutex<Vec<crate::LifecycleEvent>>>);
    #[async_trait::async_trait]
    impl Extension for ReplacementStartupHook {
        fn id(&self) -> &str {
            "replacement-startup"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_session_lifecycle(&self) -> bool {
            true
        }
        fn handles_model_selection(&self) -> bool {
            true
        }
        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            if matches!(
                event,
                crate::LifecycleEvent::SessionStart { .. }
                    | crate::LifecycleEvent::ThinkingLevelSelect { .. }
                    | crate::LifecycleEvent::ModelSelect { .. }
            ) {
                self.0.lock().unwrap().push(event.clone());
            }
            if matches!(event, crate::LifecycleEvent::SessionStart { .. }) {
                host.send_message(
                    crate::CustomMessage {
                        custom_type: "startup".into(),
                        content: crate::ContextContent::Text("start".into()),
                        display: false,
                        details: None,
                    },
                    crate::CustomMessageDelivery::TriggerTurn,
                )
                .await?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn hook_aware_model_switch_replaces_missing_reopened_model_before_startup_triggers() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("session.jsonl");
        let mut original = SessionBuilder::new(Arc::new(NamedBranchModel("model-A")), "model-A")
            .open(&path)
            .unwrap();
        original
            .journal()
            .unwrap()
            .select_model("model-A", Some("high"))
            .unwrap();
        original.prompt("original").await.unwrap();
        original.close().await.unwrap();
        drop(original);

        let observed = Arc::new(Mutex::new(Vec::new()));
        let mut reopened = SessionBuilder::new(Arc::new(NamedBranchModel("model-B")), "model-B")
            .extension(Arc::new(ReplacementStartupHook(observed.clone())))
            .unwrap()
            .open(&path)
            .unwrap();
        reopened
            .set_model_with_hooks(
                Arc::new(NamedBranchModel("model-B")),
                "model-B",
                serde_json::json!({"id":"model-B", "provider":"fixture"}),
                Some(serde_json::json!({"id":"model-A", "provider":"fixture"})),
            )
            .await
            .unwrap();
        assert_eq!(reopened.model_name(), "model-B");
        assert_eq!(
            reopened.journal().unwrap().model_provider(),
            Some("fixture")
        );
        assert_eq!(
            reopened.journal().unwrap().model_selection(),
            Some(("model-B".into(), None))
        );
        assert_eq!(
            reopened.journal().unwrap().model_provider(),
            Some("fixture")
        );
        assert!(
            reopened
                .journal()
                .unwrap()
                .records()
                .iter()
                .any(|record| matches!(
                    &record.event, crate::Event::ModelRequested { model, .. } if model == "model-B"
                ))
        );
        assert_eq!(reopened.prompt("next").await.unwrap(), "model-B");
        {
            let events = observed.lock().unwrap();
            assert!(matches!(events.as_slice(), [
            crate::LifecycleEvent::SessionStart { .. },
            crate::LifecycleEvent::ThinkingLevelSelect { previous_level, level },
            crate::LifecycleEvent::ModelSelect { model, previous_model: Some(previous), source: crate::ModelSelectSource::Set },
        ] if previous_level == "high" && level == "off" && model["id"] == "model-B" && previous["id"] == "model-A"));
        }
        reopened.close().await.unwrap();
    }

    struct FailingStartupModel(AtomicUsize);
    #[async_trait::async_trait]
    impl Model for FailingStartupModel {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
            assert_eq!(request.model, "model-B");
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(Error::Model("startup fixture failure".into()));
            }
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Text("recovered".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])))
        }
    }

    #[tokio::test]
    async fn failed_startup_trigger_still_notifies_committed_model_transition() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("session.jsonl");
        let mut original = SessionBuilder::new(Arc::new(NamedBranchModel("model-A")), "model-A")
            .open(&path)
            .unwrap();
        original
            .journal()
            .unwrap()
            .select_model("model-A", Some("high"))
            .unwrap();
        original.prompt("original").await.unwrap();
        original.close().await.unwrap();
        drop(original);

        let observed = Arc::new(Mutex::new(Vec::new()));
        let replacement = Arc::new(FailingStartupModel(AtomicUsize::new(0)));
        let mut reopened = SessionBuilder::new(replacement.clone(), "model-B")
            .extension(Arc::new(ReplacementStartupHook(observed.clone())))
            .unwrap()
            .open(&path)
            .unwrap();
        let error = reopened
            .set_model_with_hooks(
                replacement.clone(),
                "model-B",
                serde_json::json!({"id":"model-B", "provider":"fixture"}),
                Some(serde_json::json!({"id":"model-A", "provider":"fixture"})),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("startup fixture failure"));
        assert_eq!(
            reopened.journal().unwrap().model_selection(),
            Some(("model-B".into(), None))
        );
        {
            let events = observed.lock().unwrap();
            assert!(matches!(events.as_slice(), [
                crate::LifecycleEvent::SessionStart { .. },
                crate::LifecycleEvent::ThinkingLevelSelect { previous_level, level },
                crate::LifecycleEvent::ModelSelect { model, previous_model: Some(previous), source: crate::ModelSelectSource::Set },
            ] if previous_level == "high" && level == "off" && model["id"] == "model-B" && previous["id"] == "model-A"));
        }
        reopened
            .set_model_with_hooks(
                replacement,
                "model-B",
                serde_json::json!({"id":"model-B", "provider":"fixture"}),
                None,
            )
            .await
            .unwrap();
        assert_eq!(observed.lock().unwrap().len(), 3);
        assert_eq!(reopened.prompt("next").await.unwrap(), "recovered");
        reopened.close().await.unwrap();
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
            .unwrap()
            .records()
            .iter()
            .find(|record| {
                matches!(
                    &record.event, crate::Event::User { content, .. } if content == "selected"
                )
            })
            .unwrap()
            .id;
        let head = session.journal().unwrap().head();
        assert!(matches!(
            session
                .fork_with_hooks(entry_id, crate::ForkPosition::Before, Some(&destination))
                .await,
            Err(Error::Cancelled)
        ));
        assert!(!destination.exists());
        assert_eq!(session.journal().unwrap().head(), head);
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
            assert_eq!(session.journal().unwrap().name(), expected);
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
        let head = session.journal().unwrap().head();
        let mut fork = session
            .fork_to(head, directory.path().join("fork.jsonl"))
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
    struct BeforeTreeMessages {
        cancel: bool,
        trigger: bool,
    }
    struct PausedBeforeTree {
        ready: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl Extension for PausedBeforeTree {
        fn id(&self) -> &str {
            "paused-before-tree"
        }
        fn handles_tree_navigation(&self) -> bool {
            true
        }
        async fn before_tree_with_host(
            &self,
            _: &crate::TreePreparation,
            _: Option<&crate::ExtensionState>,
            _: &CancellationToken,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<Option<crate::TreeDecision>> {
            host.send_message(
                crate::CustomMessage {
                    custom_type: "tree-crash".into(),
                    content: crate::ContextContent::Text("trigger".into()),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::TriggerTurn,
            )
            .await?;
            self.ready.notify_one();
            std::future::pending().await
        }
    }
    #[tokio::test]
    async fn before_tree_trigger_survives_interruption_after_acknowledgement() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let hook = Arc::new(PausedBeforeTree {
            ready: tokio::sync::Notify::new(),
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tree-crash.jsonl");
        let mut session =
            SessionBuilder::new(Arc::new(TreeCaptureModel(requests.clone())), "fixture")
                .extension(hook.clone())
                .unwrap()
                .open(&path)
                .unwrap();
        session.prompt("first").await.unwrap();
        let selected = session
            .journal()
            .unwrap()
            .records()
            .iter()
            .find(|record| matches!(record.event, crate::Event::User { .. }))
            .unwrap()
            .id;
        {
            let future = session.navigate_tree(selected, crate::TreeNavigationOptions::default());
            tokio::pin!(future);
            tokio::select! { _ = &mut future => panic!("tree hook did not pause"), _ = hook.ready.notified() => {} }
        }
        drop(session);
        let mut reopened =
            SessionBuilder::new(Arc::new(TreeCaptureModel(requests.clone())), "fixture")
                .open(&path)
                .unwrap();
        reopened.start().await.unwrap();
        assert!(
            reopened
                .journal()
                .unwrap()
                .pending_custom_trigger()
                .is_none()
        );
        assert_eq!(requests.lock().unwrap().len(), 2);
        assert_eq!(reopened.journal().unwrap().records().iter().filter(|record| matches!(&record.event,
            crate::Event::CustomMessageDelivered { message, .. } if message.custom_type == "tree-crash")).count(), 1);
        drop(reopened);
        let mut reopened =
            SessionBuilder::new(Arc::new(TreeCaptureModel(requests.clone())), "fixture")
                .open(&path)
                .unwrap();
        reopened.start().await.unwrap();
        assert_eq!(requests.lock().unwrap().len(), 2);
    }
    #[tokio::test]
    async fn before_tree_trigger_survives_checkout_before_run_creation() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tree-after-checkout.jsonl");
        let mut journal = Session::open(&path).unwrap();
        journal
            .append(crate::Event::User {
                content: "old".into(),
                input_id: None,
            })
            .unwrap();
        let id = Uuid::new_v4();
        journal
            .append(crate::Event::TreeMessageQueued {
                input: crate::QueuedCustomMessage {
                    id,
                    message: crate::CustomMessage {
                        custom_type: "tree-checkout-crash".into(),
                        content: crate::ContextContent::Text("trigger".into()),
                        display: false,
                        details: None,
                    },
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                },
            })
            .unwrap();
        journal.checkout(1).unwrap();
        drop(journal);
        let mut reopened =
            SessionBuilder::new(Arc::new(TreeCaptureModel(requests.clone())), "fixture")
                .open(&path)
                .unwrap();
        reopened.start().await.unwrap();
        assert_eq!(requests.lock().unwrap().len(), 1);
        assert!(
            reopened
                .journal()
                .unwrap()
                .pending_custom_trigger()
                .is_none()
        );
        assert_eq!(
            reopened
                .journal()
                .unwrap()
                .records()
                .iter()
                .filter(|record| matches!(record.event,
            crate::Event::CustomMessageDelivered { id: delivered, .. } if delivered == id))
                .count(),
            1
        );
        drop(reopened);
        let mut reopened =
            SessionBuilder::new(Arc::new(TreeCaptureModel(requests.clone())), "fixture")
                .open(&path)
                .unwrap();
        reopened.start().await.unwrap();
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn interrupted_tree_trigger_restores_its_branch_after_checkout() {
        for delivered in [false, true] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("tree-running.jsonl");
            let mut journal = Session::open(&path).unwrap();
            journal
                .append_user_with_images("branch A".into(), vec![], None)
                .unwrap();
            let trigger_id = Uuid::new_v4();
            let message = crate::CustomMessage {
                custom_type: "tree-running".into(),
                content: crate::ContextContent::Text("trigger".into()),
                display: false,
                details: None,
            };
            journal
                .append(crate::Event::TreeMessageQueued {
                    input: crate::QueuedCustomMessage {
                        id: trigger_id,
                        message: message.clone(),
                        delivery: crate::CustomMessageDelivery::TriggerTurn,
                    },
                })
                .unwrap();
            journal.set_run_id(Some(trigger_id));
            journal
                .append(crate::Event::Run {
                    state: crate::RunState::Running,
                    initial: None,
                })
                .unwrap();
            if delivered {
                journal
                    .append(crate::Event::CustomMessageDelivered {
                        id: trigger_id,
                        message,
                    })
                    .unwrap();
            }
            journal.set_run_id(None);
            journal.checkout(1).unwrap();
            journal
                .append_user_with_images("branch B".into(), vec![], None)
                .unwrap();
            let branch_b = journal.head();
            drop(journal);
            let mut reopened =
                SessionBuilder::new(Arc::new(TreeCaptureModel(requests.clone())), "fixture")
                    .open(&path)
                    .unwrap();
            reopened.start().await.unwrap();
            assert!(
                reopened
                    .journal()
                    .unwrap()
                    .pending_custom_trigger()
                    .is_none()
            );
            assert_eq!(reopened.journal().unwrap().head(), branch_b);
            assert_eq!(
                reopened
                    .journal()
                    .unwrap()
                    .records()
                    .iter()
                    .filter(|record| matches!(record.event,
                crate::Event::CustomMessageDelivered { id, .. } if id == trigger_id))
                    .count(),
                1
            );
            assert!(reopened.journal().unwrap().conversation_at(branch_b).unwrap().iter().any(|message| matches!(message, Message::User { content } if content == "branch B")));
            {
                let snapshots = requests.lock().unwrap();
                assert_eq!(snapshots.len(), 1);
                assert!(snapshots[0].iter().any(
                    |message| matches!(message, Message::User { content } if content == "branch A")
                ));
                assert!(!snapshots[0].iter().any(
                    |message| matches!(message, Message::User { content } if content == "branch B")
                ));
            }
            drop(reopened);
            let mut again =
                SessionBuilder::new(Arc::new(TreeCaptureModel(requests.clone())), "fixture")
                    .open(&path)
                    .unwrap();
            again.start().await.unwrap();
            assert_eq!(requests.lock().unwrap().len(), 1);
            again.prompt("follow up B").await.unwrap();
            let snapshots = requests.lock().unwrap();
            assert!(snapshots[1].iter().any(
                |message| matches!(message, Message::User { content } if content == "branch B")
            ));
            assert!(!snapshots[1].iter().any(
                |message| matches!(message, Message::User { content } if content == "branch A")
            ));
        }
    }
    #[tokio::test]
    async fn automatic_trigger_drain_restores_origin_after_another_run() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let model = TreeCaptureModel(requests.clone());
        let registry = Registry::default();
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "test",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        session
            .append_user_with_images("branch A".into(), vec![], None)
            .unwrap();
        let trigger_id = Uuid::new_v4();
        let message = crate::CustomMessage {
            custom_type: "auto-tree".into(),
            content: crate::ContextContent::Text("trigger".into()),
            display: false,
            details: None,
        };
        session
            .append(crate::Event::TreeMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: trigger_id,
                    message: message.clone(),
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                },
            })
            .unwrap();
        session.set_run_id(Some(trigger_id));
        session
            .append(crate::Event::Run {
                state: crate::RunState::Running,
                initial: None,
            })
            .unwrap();
        session
            .append(crate::Event::CustomMessageDelivered {
                id: trigger_id,
                message,
            })
            .unwrap();
        session.set_run_id(None);
        session.checkout(1).unwrap();
        session
            .append_user_with_images("branch B".into(), vec![], None)
            .unwrap();
        let branch_b = session.head();
        let (_sender, mut inputs) = mpsc::channel::<crate::AgentInput>(8);
        let answer = agent
            .run_correlated(
                &mut session,
                Uuid::new_v4(),
                Some("new prompt".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        assert_eq!(answer, "done");
        assert!(session.pending_custom_trigger().is_none());
        assert_eq!(
            session
                .records()
                .iter()
                .filter(|record| matches!(record.event,
            crate::Event::CustomMessageDelivered { id, .. } if id == trigger_id))
                .count(),
            1
        );
        assert!(
            session.conversation_at(branch_b).unwrap().iter().any(
                |message| matches!(message, Message::User { content } if content == "branch B")
            )
        );
        assert_eq!(requests.lock().unwrap().len(), 2);
        assert!(session.branch().iter().any(|record| matches!(&record.event, crate::Event::User { content, .. } if content == "new prompt")));
        assert!(!session.branch().iter().any(|record| matches!(&record.event, crate::Event::User { content, .. } if content == "branch A")));
    }

    struct BranchStateHook(Arc<Mutex<Vec<String>>>);
    #[async_trait::async_trait]
    impl Extension for BranchStateHook {
        fn id(&self) -> &str {
            "branch-state-hook"
        }
        fn handles_input(&self) -> bool {
            true
        }
        async fn input_with_host(
            &self,
            _: &crate::InputEvent,
            state: Option<&crate::ExtensionState>,
            _: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::InputAction> {
            self.0.lock().unwrap().push(
                state
                    .and_then(|state| state.data.as_str())
                    .unwrap_or("missing")
                    .into(),
            );
            Ok(crate::InputAction::Continue)
        }
    }
    struct BranchModelProbe {
        expected: &'static str,
        names: Arc<Mutex<Vec<String>>>,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        waits: AtomicUsize,
    }

    struct NamedBranchModel(&'static str);
    struct ProviderBranchModel(&'static str);
    #[async_trait::async_trait]
    impl Model for ProviderBranchModel {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
            assert_eq!(request.model, "shared");
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Text(self.0.into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])))
        }
    }

    #[tokio::test]
    async fn provider_identity_controls_reopen_checkout_and_fork() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("providers.jsonl");
        let a = Arc::new(ProviderBranchModel("provider-a"));
        let b = Arc::new(ProviderBranchModel("provider-b"));
        let mut first = SessionBuilder::new(a.clone(), "shared")
            .provider("a")
            .unwrap()
            .open(&path)
            .unwrap();
        first.set_provider_model(a.clone(), "a", "shared").unwrap();
        assert_eq!(first.prompt("a").await.unwrap(), "provider-a");
        let a_head = first.journal().unwrap().head();
        first.set_provider_model(b.clone(), "b", "shared").unwrap();
        assert_eq!(first.prompt("b").await.unwrap(), "provider-b");
        let b_head = first.journal().unwrap().head();
        first.close().await.unwrap();
        drop(first);

        let mut reopened = SessionBuilder::new(b.clone(), "shared")
            .provider("b")
            .unwrap()
            .provider_branch_model("a", "shared", a.clone())
            .unwrap()
            .open(&path)
            .unwrap();
        assert_eq!(reopened.prompt("reopened").await.unwrap(), "provider-b");
        reopened.checkout(a_head).unwrap();
        assert_eq!(reopened.prompt("checkout").await.unwrap(), "provider-a");
        let mut fork = reopened.fork_at(b_head).unwrap();
        assert_eq!(fork.prompt("fork").await.unwrap(), "provider-b");
        fork.close().await.unwrap();
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn qualified_history_rejects_name_only_client_before_navigation() {
        let mut journal = Session::with_store(crate::MemoryStore::default()).unwrap();
        journal
            .select_provider_model(Some("a"), "shared", None)
            .unwrap();
        let target = journal.head();
        journal
            .select_provider_model(Some("b"), "shared", None)
            .unwrap();
        let mut session =
            SessionBuilder::new(Arc::new(ProviderBranchModel("unqualified")), "shared")
                .provider_branch_model("b", "shared", Arc::new(ProviderBranchModel("provider-b")))
                .unwrap()
                .build(journal)
                .unwrap();
        session.start().await.unwrap();
        let head = session.journal().unwrap().head();
        assert!(session.checkout(target).is_err());
        assert!(
            session
                .navigate_tree(target, crate::TreeNavigationOptions::default())
                .await
                .is_err()
        );
        assert!(session.fork_at(target).is_err());
        assert_eq!(session.journal().unwrap().head(), head);
        assert_eq!(session.prompt("current").await.unwrap(), "provider-b");
        session
            .register_provider_branch_model(
                "a",
                "shared",
                Arc::new(ProviderBranchModel("provider-a")),
            )
            .unwrap();
        session.checkout(target).unwrap();
        assert_eq!(session.prompt("registered").await.unwrap(), "provider-a");
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn qualified_reopen_rejects_an_unqualified_client_with_the_same_name() {
        let mut journal = Session::with_store(crate::MemoryStore::default()).unwrap();
        journal
            .select_provider_model(Some("a"), "shared", None)
            .unwrap();
        let mut session = SessionBuilder::new(Arc::new(ProviderBranchModel("wrong")), "shared")
            .build(journal)
            .unwrap();
        let head = session.journal().unwrap().head();
        assert!(session.start().await.is_err());
        assert_eq!(session.journal().unwrap().head(), head);
        session
            .register_provider_branch_model("a", "shared", Arc::new(ProviderBranchModel("right")))
            .unwrap();
        assert_eq!(session.prompt("after registration").await.unwrap(), "right");
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn failed_selection_does_not_replace_registered_client() {
        let old: Arc<dyn Model> = Arc::new(ProviderBranchModel("old"));
        let new: Arc<dyn Model> = Arc::new(ProviderBranchModel("new"));
        let mut session = SessionBuilder::new(old.clone(), "shared")
            .provider("a")
            .unwrap()
            .in_memory()
            .unwrap();
        session
            .set_provider_model(old.clone(), "a", "shared")
            .unwrap();
        let journal = session.session.clone();
        let guard = journal.lock().await;
        assert!(session.set_provider_model(new, "a", "shared").is_err());
        assert!(Arc::ptr_eq(
            &branch_client(&session.registry, session.session_id, Some("a"), "shared").unwrap(),
            &old
        ));
        drop(guard);
        assert_eq!(session.prompt("still old").await.unwrap(), "old");
        session.close().await.unwrap();
    }

    struct FailingSelectionStore {
        inner: crate::MemoryStore,
        fail: Arc<std::sync::atomic::AtomicBool>,
    }
    impl crate::SessionStore for FailingSelectionStore {
        fn load(&mut self) -> Result<Vec<crate::Record>> {
            crate::SessionStore::load(&mut self.inner)
        }
        fn append(&mut self, record: &crate::Record) -> Result<()> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(Error::Session("fixture write failed".into()));
            }
            crate::SessionStore::append(&mut self.inner, record)
        }
    }

    #[test]
    fn failed_journal_write_does_not_replace_registered_client() {
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let journal = Session::with_store(FailingSelectionStore {
            inner: crate::MemoryStore::default(),
            fail: fail.clone(),
        })
        .unwrap();
        let old: Arc<dyn Model> = Arc::new(ProviderBranchModel("old"));
        let new: Arc<dyn Model> = Arc::new(ProviderBranchModel("new"));
        let mut session = SessionBuilder::new(old.clone(), "shared")
            .provider("a")
            .unwrap()
            .build(journal)
            .unwrap();
        fail.store(true, Ordering::SeqCst);
        assert!(session.set_provider_model(new, "a", "shared").is_err());
        assert!(Arc::ptr_eq(
            &branch_client(&session.registry, session.session_id, Some("a"), "shared").unwrap(),
            &old
        ));
    }

    #[tokio::test]
    async fn hook_selection_retains_provider_identity_for_identical_model_ids() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let a = Arc::new(ProviderBranchModel("a"));
        let b = Arc::new(ProviderBranchModel("b"));
        let mut session = SessionBuilder::new(a.clone(), "shared")
            .provider("a")
            .unwrap()
            .extension(Arc::new(SelectionHook(events.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        session.set_provider_model(a, "a", "shared").unwrap();
        let a_head = session.journal().unwrap().head();
        session
            .set_model_with_hooks(
                b,
                "shared",
                serde_json::json!({"provider":"b", "id":"shared"}),
                None,
            )
            .await
            .unwrap();
        assert_eq!(session.journal().unwrap().model_provider(), Some("b"));
        assert!(
            events
                .lock()
                .unwrap()
                .iter()
                .any(|event| matches!(event, crate::LifecycleEvent::ModelSelect { .. }))
        );
        let b_head = session.journal().unwrap().head();
        assert_eq!(session.prompt("b").await.unwrap(), "b");
        session.checkout(a_head).unwrap();
        assert_eq!(session.prompt("a").await.unwrap(), "a");
        session.checkout(b_head).unwrap();
        assert_eq!(session.prompt("b again").await.unwrap(), "b");
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn direct_tree_handoff_from_qualified_to_legacy_uses_legacy_client() {
        let mut journal = Session::with_store(crate::MemoryStore::default()).unwrap();
        journal.select_model("shared", None).unwrap();
        let id = Uuid::new_v4();
        let message = crate::CustomMessage {
            custom_type: "tree".into(),
            content: crate::ContextContent::Text("trigger".into()),
            display: false,
            details: None,
        };
        journal
            .append(crate::Event::TreeMessageQueued {
                input: crate::QueuedCustomMessage {
                    id,
                    message: message.clone(),
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                },
            })
            .unwrap();
        journal.set_run_id(Some(id));
        journal
            .append(crate::Event::Run {
                state: crate::RunState::Running,
                initial: None,
            })
            .unwrap();
        journal
            .append(crate::Event::CustomMessageDelivered { id, message })
            .unwrap();
        journal.set_run_id(None);
        journal.checkout(1).unwrap();
        journal
            .select_provider_model(Some("b"), "shared", None)
            .unwrap();
        let session = SessionBuilder::new(Arc::new(ProviderBranchModel("b")), "shared")
            .provider("b")
            .unwrap()
            .branch_model("shared", Arc::new(ProviderBranchModel("legacy")))
            .unwrap()
            .build(journal)
            .unwrap();
        let agent = Agent {
            model: session.model.as_ref(),
            model_name: &session.model_name,
            registry: &session.registry,
            system_prompt: &session.system_prompt,
            limits: session.limits.clone(),
        };
        let mut journal = session.session.lock().await;
        let mut input = session.input.lock().await;
        let answer = agent
            .run_correlated(
                &mut journal,
                id,
                None,
                &mut input,
                &CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        assert_eq!(answer, "legacy");
        assert_eq!(journal.model_provider(), Some("b"));
    }
    struct TriggerFromIdleInput;
    #[async_trait::async_trait]
    impl Extension for TriggerFromIdleInput {
        fn id(&self) -> &str {
            "idle-branch-trigger"
        }
        fn handles_input(&self) -> bool {
            true
        }
        async fn input_with_host(
            &self,
            _: &crate::InputEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::InputAction> {
            host.send_message(
                crate::CustomMessage {
                    custom_type: "idle".into(),
                    content: crate::ContextContent::Text("trigger".into()),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::TriggerTurn,
            )
            .await?;
            Ok(crate::InputAction::Handled)
        }
    }
    #[async_trait::async_trait]
    impl Model for NamedBranchModel {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
            assert_eq!(request.model, self.0);
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Text(self.0.into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])))
        }
        async fn context_window(&self, _: &str) -> Option<u32> {
            Some(if self.0 == "model-A" {
                100_000
            } else {
                200_000
            })
        }
    }

    #[tokio::test]
    async fn owned_session_restores_branch_model_client_for_navigation_and_forks() {
        let a = Arc::new(NamedBranchModel("model-A"));
        let b = Arc::new(NamedBranchModel("model-B"));
        let mut session = SessionBuilder::new(a.clone(), "model-A")
            .in_memory()
            .unwrap();
        session.set_model(a, "model-A").unwrap();
        assert_eq!(session.prompt("first").await.unwrap(), "model-A");
        let a_head = session.journal().unwrap().head();
        session.set_model(b, "model-B").unwrap();
        assert_eq!(session.prompt("second").await.unwrap(), "model-B");
        let b_head = session.journal().unwrap().head();

        session.checkout(a_head).unwrap();
        assert_eq!(session.model_name(), "model-A");
        assert_eq!(
            session
                .context_usage()
                .await
                .unwrap()
                .unwrap()
                .context_window,
            100_000
        );
        assert_eq!(session.prompt("after checkout").await.unwrap(), "model-A");
        session
            .navigate_tree(b_head, crate::TreeNavigationOptions::default())
            .await
            .unwrap();
        assert_eq!(session.model_name(), "model-B");
        assert_eq!(session.prompt("after tree").await.unwrap(), "model-B");

        let mut fork = session.fork_at(a_head).unwrap();
        assert_eq!(fork.model_name(), "model-A");
        assert_eq!(fork.prompt("on fork").await.unwrap(), "model-A");
        assert_eq!(session.prompt("on original").await.unwrap(), "model-B");
        fork.close().await.unwrap();
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn owned_session_restores_registered_model_on_reopen_and_rejects_missing_client() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("session.jsonl");
        let a = Arc::new(NamedBranchModel("model-A"));
        let b = Arc::new(NamedBranchModel("model-B"));
        let mut first = SessionBuilder::new(a.clone(), "model-A")
            .open(&path)
            .unwrap();
        first.set_model(a.clone(), "model-A").unwrap();
        assert_eq!(first.prompt("save").await.unwrap(), "model-A");
        first.close().await.unwrap();
        drop(first);

        let mut missing = SessionBuilder::new(b.clone(), "model-B")
            .open(&path)
            .unwrap();
        assert!(missing.context_usage().await.is_err());
        let fork_path = root.path().join("missing-fork.jsonl");
        let head = missing.journal().unwrap().head();
        assert!(missing.fork_to(head, &fork_path).is_err());
        assert!(!fork_path.exists());
        assert!(
            missing
                .prompt("not sent")
                .await
                .unwrap_err()
                .to_string()
                .contains("requires registered model model-A")
        );
        missing.register_branch_model("model-A", a.clone()).unwrap();
        assert_eq!(
            missing.prompt("after registering").await.unwrap(),
            "model-A"
        );
        missing.close().await.unwrap();
    }

    #[tokio::test]
    async fn checkout_and_persistent_fork_reject_missing_branch_model_before_mutation() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("source.jsonl");
        let a = Arc::new(NamedBranchModel("model-A"));
        let b = Arc::new(NamedBranchModel("model-B"));
        let mut source = SessionBuilder::new(a.clone(), "model-A")
            .open(&path)
            .unwrap();
        source.set_model(a, "model-A").unwrap();
        source.prompt("first").await.unwrap();
        let a_head = source.journal().unwrap().head();
        source.set_model(b.clone(), "model-B").unwrap();
        source.prompt("second").await.unwrap();
        source.close().await.unwrap();
        drop(source);

        let mut reopened = SessionBuilder::new(b, "model-B").open(&path).unwrap();
        let before = reopened.journal().unwrap().head();
        assert!(reopened.checkout(a_head).is_err());
        assert_eq!(reopened.journal().unwrap().head(), before);
        let fork_path = root.path().join("fork.jsonl");
        assert!(reopened.fork_to(a_head, &fork_path).is_err());
        assert!(!fork_path.exists());
        assert_eq!(reopened.prompt("unchanged").await.unwrap(), "model-B");
        reopened.close().await.unwrap();
    }

    struct TreePreflightHook(Arc<Mutex<Vec<&'static str>>>);
    #[async_trait::async_trait]
    impl Extension for TreePreflightHook {
        fn id(&self) -> &str {
            "tree-preflight"
        }
        fn handles_tree_navigation(&self) -> bool {
            true
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        async fn before_tree(
            &self,
            _: &crate::TreePreparation,
            _: Option<&crate::ExtensionState>,
            _: &CancellationToken,
        ) -> crate::ExtensionResult<Option<crate::TreeDecision>> {
            self.0.lock().unwrap().push("before");
            Ok(Some(crate::TreeDecision {
                summary: Some(crate::TreeSummary {
                    summary: "extension summary".into(),
                    details: None,
                    usage: None,
                }),
                label: Some("hook label".into()),
                ..Default::default()
            }))
        }
        async fn lifecycle(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<()> {
            if matches!(event, crate::LifecycleEvent::SessionTree { .. }) {
                self.0.lock().unwrap().push("after");
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn tree_navigation_rejects_missing_destination_before_hooks_or_commit() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("tree.jsonl");
        let a = Arc::new(NamedBranchModel("model-A"));
        let b = Arc::new(NamedBranchModel("model-B"));
        let mut original = SessionBuilder::new(a.clone(), "model-A")
            .open(&path)
            .unwrap();
        original.set_model(a.clone(), "model-A").unwrap();
        original.prompt("branch A").await.unwrap();
        let target = original.journal().unwrap().head();
        original.set_model(b.clone(), "model-B").unwrap();
        original.prompt("branch B").await.unwrap();
        original.close().await.unwrap();
        drop(original);

        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut session = SessionBuilder::new(b, "model-B")
            .extension(Arc::new(TreePreflightHook(calls.clone())))
            .unwrap()
            .open(&path)
            .unwrap();
        session.start().await.unwrap();
        let mut events = session.subscribe();
        let (head, count, labels) = {
            let journal = session.journal().unwrap();
            (journal.head(), journal.records().len(), journal.labels())
        };
        for summarize in [false, true] {
            let options = crate::TreeNavigationOptions {
                summary: summarize.then_some(crate::BranchSummaryOptions {
                    instructions: None,
                    reserve_tokens: 1024,
                }),
                label: Some("requested"),
                ..Default::default()
            };
            let error = session.navigate_tree(target, options).await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("requires registered model model-A")
            );
            let journal = session.journal().unwrap();
            assert_eq!(journal.head(), head);
            assert_eq!(journal.records().len(), count);
            assert_eq!(journal.labels(), labels);
            assert_eq!(session.model_name(), "model-B");
            assert!(events.try_recv().is_err());
            assert!(calls.lock().unwrap().is_empty());
        }
        session.register_branch_model("model-A", a).unwrap();
        let result = session
            .navigate_tree(
                target,
                crate::TreeNavigationOptions {
                    summary: Some(crate::BranchSummaryOptions {
                        instructions: None,
                        reserve_tokens: 1024,
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let summary = result.summary_entry.unwrap();
        assert!(
            matches!(&summary.event, crate::Event::BranchSummary { summary, .. } if summary == "extension summary")
        );
        assert_eq!(
            session.journal().unwrap().labels()[&summary.id],
            "hook label"
        );
        assert_eq!(&*calls.lock().unwrap(), &["before", "after"]);
        assert_eq!(session.model_name(), "model-A");
        assert_eq!(session.prompt("after navigation").await.unwrap(), "model-A");
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn idle_input_trigger_uses_restored_branch_model_before_session_start() {
        let mut journal = Session::with_store(crate::MemoryStore::default()).unwrap();
        journal.select_model("model-A", None).unwrap();
        let mut session = SessionBuilder::new(Arc::new(NamedBranchModel("model-B")), "model-B")
            .branch_model("model-A", Arc::new(NamedBranchModel("model-A")))
            .unwrap()
            .extension(Arc::new(TriggerFromIdleInput))
            .unwrap()
            .build(journal)
            .unwrap();
        session.controls().steer("hello").await.unwrap();
        assert!(
            session
                .journal()
                .unwrap()
                .records()
                .iter()
                .any(|record| matches!(
                    &record.event, crate::Event::ModelRequested { model, .. } if model == "model-A"
                ))
        );
        session.close().await.unwrap();
    }
    #[async_trait::async_trait]
    impl Model for BranchModelProbe {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
            assert_eq!(request.model, self.expected);
            self.names.lock().unwrap().push(request.model.clone());
            if request.model == "model-A" && self.waits.fetch_add(1, Ordering::SeqCst) == 0 {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Text("done".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])))
        }
    }
    #[tokio::test]
    async fn automatic_tree_handoff_uses_branch_model_and_input_hook_state_then_restores_both() {
        let names = Arc::new(Mutex::new(Vec::new()));
        let model_a = Arc::new(BranchModelProbe {
            expected: "model-A",
            names: names.clone(),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            waits: AtomicUsize::new(0),
        });
        let model_b = Arc::new(BranchModelProbe {
            expected: "model-B",
            names: names.clone(),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            waits: AtomicUsize::new(0),
        });
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut journal = Session::with_store(crate::MemoryStore::default()).unwrap();
        journal.select_model("model-A", None).unwrap();
        journal
            .append(crate::Event::ExtensionStates {
                states: [(
                    "branch-state-hook".into(),
                    crate::ExtensionState {
                        version: 1,
                        data: serde_json::json!("A"),
                    },
                )]
                .into_iter()
                .collect(),
            })
            .unwrap();
        journal
            .append_user_with_images("branch A".into(), vec![], None)
            .unwrap();
        let trigger_id = Uuid::new_v4();
        let message = crate::CustomMessage {
            custom_type: "branch-trigger".into(),
            content: crate::ContextContent::Text("trigger".into()),
            display: false,
            details: None,
        };
        journal
            .append(crate::Event::TreeMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: trigger_id,
                    message: message.clone(),
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                },
            })
            .unwrap();
        journal.set_run_id(Some(trigger_id));
        journal
            .append(crate::Event::Run {
                state: crate::RunState::Running,
                initial: None,
            })
            .unwrap();
        journal
            .append(crate::Event::CustomMessageDelivered {
                id: trigger_id,
                message,
            })
            .unwrap();
        journal.set_run_id(None);
        journal.checkout(1).unwrap();
        journal
            .select_provider_model(Some("provider"), "model-B", None)
            .unwrap();
        journal
            .append(crate::Event::ExtensionStates {
                states: [(
                    "branch-state-hook".into(),
                    crate::ExtensionState {
                        version: 1,
                        data: serde_json::json!("B"),
                    },
                )]
                .into_iter()
                .collect(),
            })
            .unwrap();
        journal
            .append_user_with_images("branch B".into(), vec![], None)
            .unwrap();
        let session = SessionBuilder::new(model_b, "model-B")
            .branch_model("model-A", model_a.clone())
            .unwrap()
            .extension(Arc::new(BranchStateHook(seen.clone())))
            .unwrap()
            .build(journal)
            .unwrap();
        let active = session.begin_run();
        let control = session.controls();
        let observer_control = control.clone();
        let mut observe = move |event: Notification<'_>| observer_control.observe(&event);
        let agent = Agent {
            model: session.model.as_ref(),
            model_name: &session.model_name,
            registry: &session.registry,
            system_prompt: &session.system_prompt,
            limits: session.limits.clone(),
        };
        let mut journal = session.session.lock().await;
        let mut input_queue = session.input.lock().await;
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(
                agent.run_correlated(
                    &mut journal,
                    Uuid::new_v4(),
                    Some("B prompt".into()),
                    &mut input_queue,
                    &active.token,
                    &mut observe
                ),
                async {
                    model_a.entered.notified().await;
                    control.steer("during A").await.unwrap();
                    model_a.release.notify_one();
                }
            )
        })
        .await
        .unwrap();
        drop(journal);
        assert_eq!(result.unwrap(), "done");
        control.steer("after B").await.unwrap();
        active
            .finish_inputs(
                &mut session.session.try_lock().unwrap(),
                &mut input_queue,
                &mut observe,
            )
            .unwrap();
        assert_eq!(&*seen.lock().unwrap(), &["A", "B"]);
        let names = names.lock().unwrap();
        assert_eq!(names[0], "model-B");
        assert!(names[1..].iter().all(|name| name == "model-A"));
        assert!(session.journal().unwrap().branch().iter().any(|record| matches!(&record.event, crate::Event::User { content, .. } if content == "B prompt")));
        assert!(!session.journal().unwrap().branch().iter().any(|record| matches!(&record.event, crate::Event::User { content, .. } if content == "branch A")));
    }

    #[tokio::test]
    async fn cross_provider_tree_handoff_uses_registered_origin_after_reopen() {
        let names = Arc::new(Mutex::new(Vec::new()));
        let model_a = Arc::new(BranchModelProbe {
            expected: "model-A",
            names: names.clone(),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            waits: AtomicUsize::new(0),
        });
        let model_b = Arc::new(BranchModelProbe {
            expected: "model-B",
            names: names.clone(),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            waits: AtomicUsize::new(0),
        });
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut journal = Session::with_store(crate::MemoryStore::default()).unwrap();
        journal
            .select_provider_model(Some("provider-a"), "model-A", None)
            .unwrap();
        journal
            .append(crate::Event::ExtensionStates {
                states: [(
                    "branch-state-hook".into(),
                    crate::ExtensionState {
                        version: 1,
                        data: serde_json::json!("A"),
                    },
                )]
                .into_iter()
                .collect(),
            })
            .unwrap();
        journal
            .append_user_with_images("branch A".into(), vec![], None)
            .unwrap();
        let trigger_id = Uuid::new_v4();
        let message = crate::CustomMessage {
            custom_type: "branch-trigger".into(),
            content: crate::ContextContent::Text("trigger".into()),
            display: false,
            details: None,
        };
        journal
            .append(crate::Event::TreeMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: trigger_id,
                    message: message.clone(),
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                },
            })
            .unwrap();
        journal.set_run_id(Some(trigger_id));
        journal
            .append(crate::Event::Run {
                state: crate::RunState::Running,
                initial: None,
            })
            .unwrap();
        journal
            .append(crate::Event::CustomMessageDelivered {
                id: trigger_id,
                message,
            })
            .unwrap();
        journal.set_run_id(None);
        journal.checkout(1).unwrap();
        journal
            .select_provider_model(Some("provider-b"), "model-B", None)
            .unwrap();
        journal
            .append(crate::Event::ExtensionStates {
                states: [(
                    "branch-state-hook".into(),
                    crate::ExtensionState {
                        version: 1,
                        data: serde_json::json!("B"),
                    },
                )]
                .into_iter()
                .collect(),
            })
            .unwrap();
        journal
            .append_user_with_images("branch B".into(), vec![], None)
            .unwrap();
        let journal = Session::from_records(journal.records().to_vec()).unwrap();
        let session = SessionBuilder::new(model_b, "model-B")
            .extension(Arc::new(BranchStateHook(seen.clone())))
            .unwrap()
            .build(journal)
            .unwrap();
        session
            .registry
            .register_session_provider_model(
                session.session_id,
                "provider-a",
                "model-A",
                model_a.clone(),
            )
            .unwrap();
        let active = session.begin_run();
        let control = session.controls();
        let observer_control = control.clone();
        let mut observe = move |event: Notification<'_>| observer_control.observe(&event);
        let agent = Agent {
            model: session.model.as_ref(),
            model_name: &session.model_name,
            registry: &session.registry,
            system_prompt: &session.system_prompt,
            limits: session.limits.clone(),
        };
        let mut journal = session.session.lock().await;
        let mut input_queue = session.input.lock().await;
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(
                agent.run_correlated(
                    &mut journal,
                    Uuid::new_v4(),
                    Some("B prompt".into()),
                    &mut input_queue,
                    &active.token,
                    &mut observe
                ),
                async {
                    model_a.entered.notified().await;
                    control.steer("during A").await.unwrap();
                    model_a.release.notify_one();
                }
            )
        })
        .await
        .unwrap();
        drop(journal);
        assert_eq!(result.unwrap(), "done");
        control.steer("after B").await.unwrap();
        active
            .finish_inputs(
                &mut session.session.try_lock().unwrap(),
                &mut input_queue,
                &mut observe,
            )
            .unwrap();
        assert_eq!(&*seen.lock().unwrap(), &["A", "B"]);
        let names = names.lock().unwrap();
        assert_eq!(names[0], "model-B");
        assert!(names[1..].iter().all(|name| name == "model-A"));
        assert!(session.journal().unwrap().branch().iter().any(|record| matches!(&record.event, crate::Event::User { content, .. } if content == "B prompt")));
        assert!(!session.journal().unwrap().branch().iter().any(|record| matches!(&record.event, crate::Event::User { content, .. } if content == "branch A")));
    }

    #[tokio::test]
    async fn missing_tree_branch_model_is_reported_then_recovers_after_registration() {
        let mut journal = Session::with_store(crate::MemoryStore::default()).unwrap();
        journal
            .select_provider_model(Some("provider"), "model-A", None)
            .unwrap();
        journal
            .append_user_with_images("branch A".into(), vec![], None)
            .unwrap();
        let run_id = Uuid::new_v4();
        let message = crate::CustomMessage {
            custom_type: "model-handoff".into(),
            content: crate::ContextContent::Text("trigger".into()),
            display: false,
            details: None,
        };
        journal
            .append(crate::Event::TreeMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: run_id,
                    message: message.clone(),
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                },
            })
            .unwrap();
        journal.set_run_id(Some(run_id));
        journal
            .append(crate::Event::Run {
                state: crate::RunState::Running,
                initial: None,
            })
            .unwrap();
        journal
            .append(crate::Event::CustomMessageDelivered {
                id: run_id,
                message,
            })
            .unwrap();
        journal.set_run_id(None);
        journal.checkout(1).unwrap();
        journal
            .select_provider_model(Some("provider"), "model-B", None)
            .unwrap();
        journal
            .append_user_with_images("branch B".into(), vec![], None)
            .unwrap();
        let branch_b = journal.head();
        let model_b = Arc::new(EchoModel(AtomicUsize::new(0)));
        let model_a = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model_b.clone(), "model-B")
            .provider("provider")
            .unwrap()
            .build(journal)
            .unwrap();
        assert!(
            session
                .start()
                .await
                .unwrap_err()
                .to_string()
                .contains("requires registered model model-A")
        );
        assert_eq!(session.journal().unwrap().head(), branch_b);
        assert_eq!(
            session.journal().unwrap().pending_custom_trigger(),
            Some(run_id)
        );
        session
            .register_provider_branch_model("provider", "model-A", model_a.clone())
            .unwrap();
        session.start().await.unwrap();
        assert_eq!(session.journal().unwrap().head(), branch_b);
        assert!(
            session
                .journal()
                .unwrap()
                .pending_custom_trigger()
                .is_none()
        );
        assert_eq!(model_a.0.load(Ordering::SeqCst), 1);
        assert_eq!(model_b.0.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn tree_handoff_uses_origin_compaction_tokens_without_changing_selected_settings() {
        let mut journal = Session::with_store(MemoryStore::default()).unwrap();
        journal
            .select_provider_model(Some("origin"), "origin-model", None)
            .unwrap();
        for index in 0..3 {
            journal
                .append_user_with_images(format!("old user {index}"), vec![], None)
                .unwrap();
            journal
                .append(crate::Event::Assistant {
                    content: format!("old assistant {index}"),
                    invocations: vec![],
                    usage: None,
                    request_id: None,
                })
                .unwrap();
        }
        let trigger = Uuid::new_v4();
        let message = crate::CustomMessage {
            custom_type: "handoff-compaction".into(),
            content: crate::ContextContent::Text("origin trigger".into()),
            display: false,
            details: None,
        };
        journal
            .append(crate::Event::TreeMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: trigger,
                    message: message.clone(),
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                },
            })
            .unwrap();
        journal.set_run_id(Some(trigger));
        journal
            .append(crate::Event::Run {
                state: crate::RunState::Running,
                initial: None,
            })
            .unwrap();
        journal
            .append(crate::Event::CustomMessageDelivered {
                id: trigger,
                message,
            })
            .unwrap();
        journal.set_run_id(None);
        journal.checkout(1).unwrap();
        journal
            .select_provider_model(Some("selected"), "selected-model", None)
            .unwrap();
        journal
            .append_user_with_images("selected branch".into(), vec![], None)
            .unwrap();
        let selected_tokens = crate::CompactionTokens {
            reserve_tokens: Some(0),
            keep_recent_tokens: Some(100_000),
        };
        journal
            .auto_compaction_control()
            .set_token_settings(selected_tokens);
        let origin = Arc::new(EchoModel(AtomicUsize::new(0)));
        let selected = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(selected, "selected-model")
            .provider("selected")
            .unwrap()
            .build(journal)
            .unwrap();
        session
            .registry
            .register_session_provider_model_with_limits(
                session.session_id,
                "origin",
                "origin-model",
                origin.clone(),
                Some(Limits {
                    context_window: Some(1_000),
                    output_tokens_per_request: 16,
                    output_tokens_per_run: 1_024,
                    compaction_tokens: Some(crate::CompactionTokens {
                        reserve_tokens: Some(1_000),
                        keep_recent_tokens: Some(0),
                    }),
                    ..Limits::default()
                }),
            )
            .unwrap();
        session.start().await.unwrap();
        let journal = session.journal().unwrap();
        assert!(
            journal
                .records()
                .iter()
                .any(|record| record.run_id == Some(trigger)
                    && matches!(record.event, crate::Event::Compacted { .. }))
        );
        assert_eq!(
            journal.auto_compaction_control().token_settings(),
            selected_tokens
        );
        assert!(origin.0.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn recovery_registry_separates_providers_with_the_same_model_name() {
        let registry = Registry::default();
        let session_id = Uuid::new_v4();
        let first: Arc<dyn Model> = Arc::new(EchoModel(AtomicUsize::new(0)));
        let second: Arc<dyn Model> = Arc::new(EchoModel(AtomicUsize::new(0)));
        registry
            .register_session_provider_model_with_limits(
                session_id,
                "provider-a",
                "shared",
                first.clone(),
                Some(Limits {
                    context_window: Some(12000),
                    output_tokens_per_request: 64,
                    ..Limits::default()
                }),
            )
            .unwrap();
        registry
            .register_session_provider_model_with_limits(
                session_id,
                "provider-b",
                "shared",
                second.clone(),
                Some(Limits {
                    context_window: Some(24000),
                    output_tokens_per_request: 128,
                    ..Limits::default()
                }),
            )
            .unwrap();
        assert!(Arc::ptr_eq(
            &registry
                .provider_branch_config(session_id, "provider-a", "shared")
                .unwrap()
                .0,
            &first
        ));
        assert!(Arc::ptr_eq(
            &registry
                .provider_branch_config(session_id, "provider-b", "shared")
                .unwrap()
                .0,
            &second
        ));
        let limits_a = registry
            .provider_branch_config(session_id, "provider-a", "shared")
            .unwrap()
            .1
            .unwrap();
        let limits_b = registry
            .provider_branch_config(session_id, "provider-b", "shared")
            .unwrap()
            .1
            .unwrap();
        assert_eq!(
            (limits_a.context_window, limits_a.output_tokens_per_request),
            (Some(12000), 64)
        );
        assert_eq!(
            (limits_b.context_window, limits_b.output_tokens_per_request),
            (Some(24000), 128)
        );
        assert!(
            registry
                .provider_branch_config(Uuid::new_v4(), "provider-a", "shared")
                .is_none()
        );
    }

    #[test]
    fn branch_model_instances_are_scoped_to_the_owning_session() {
        let registry = Registry::default();
        let first: Arc<dyn Model> = Arc::new(EchoModel(AtomicUsize::new(0)));
        let second: Arc<dyn Model> = Arc::new(EchoModel(AtomicUsize::new(0)));
        let first_session = SessionBuilder::new(first.clone(), "same-name")
            .registry(registry.clone())
            .in_memory()
            .unwrap();
        let second_session = SessionBuilder::new(second.clone(), "same-name")
            .registry(registry.clone())
            .in_memory()
            .unwrap();
        let first_id = first_session.journal().unwrap().session_id();
        let second_id = second_session.journal().unwrap().session_id();
        assert!(Arc::ptr_eq(
            &registry.branch_model(first_id, "same-name").unwrap(),
            &first
        ));
        assert!(Arc::ptr_eq(
            &registry.branch_model(second_id, "same-name").unwrap(),
            &second
        ));
        drop(first_session);
        assert!(registry.branch_model(first_id, "same-name").is_none());
        assert!(Arc::ptr_eq(
            &registry.branch_model(second_id, "same-name").unwrap(),
            &second
        ));
    }

    struct BranchFailureModel {
        cancel: bool,
        entered: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl Model for BranchFailureModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            if self.cancel {
                self.entered.notify_one();
                std::future::pending().await
            } else {
                Err(Error::Model("fixture failure".into()))
            }
        }
    }
    #[tokio::test]
    async fn tree_handoff_restores_selected_branch_on_failure_and_cancellation() {
        for cancel_run in [false, true] {
            let mut journal = Session::with_store(crate::MemoryStore::default()).unwrap();
            journal
                .append_user_with_images("branch A".into(), vec![], None)
                .unwrap();
            let run_id = Uuid::new_v4();
            let message = crate::CustomMessage {
                custom_type: "branch-failure".into(),
                content: crate::ContextContent::Text("trigger".into()),
                display: false,
                details: None,
            };
            journal
                .append(crate::Event::TreeMessageQueued {
                    input: crate::QueuedCustomMessage {
                        id: run_id,
                        message: message.clone(),
                        delivery: crate::CustomMessageDelivery::TriggerTurn,
                    },
                })
                .unwrap();
            journal.set_run_id(Some(run_id));
            journal
                .append(crate::Event::Run {
                    state: crate::RunState::Running,
                    initial: None,
                })
                .unwrap();
            journal
                .append(crate::Event::CustomMessageDelivered {
                    id: run_id,
                    message,
                })
                .unwrap();
            journal.set_run_id(None);
            journal.checkout(1).unwrap();
            journal
                .append_user_with_images("branch B".into(), vec![], None)
                .unwrap();
            let branch_b = journal.head();
            let model = Arc::new(BranchFailureModel {
                cancel: cancel_run,
                entered: tokio::sync::Notify::new(),
            });
            let mut session = SessionBuilder::new(model.clone(), "fixture")
                .build(journal)
                .unwrap();
            let control = session.controls();
            let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                tokio::join!(session.start(), async {
                    if cancel_run {
                        model.entered.notified().await;
                        control.abort();
                    }
                })
            })
            .await
            .unwrap();
            assert!(result.is_err());
            assert_eq!(session.journal().unwrap().head(), branch_b);
            assert!(session.messages().unwrap().iter().any(
                |message| matches!(message, Message::User { content } if content == "branch B")
            ));
        }
    }

    #[tokio::test]
    async fn tree_handoff_restores_selected_branch_when_operation_remains_pending() {
        let mut journal = Session::with_store(crate::MemoryStore::default()).unwrap();
        journal
            .append_user_with_images("branch A".into(), vec![], None)
            .unwrap();
        let run_id = Uuid::new_v4();
        let operation_id = Uuid::new_v4();
        let message = crate::CustomMessage {
            custom_type: "pending-handoff".into(),
            content: crate::ContextContent::Text("trigger".into()),
            display: false,
            details: None,
        };
        journal
            .append(crate::Event::TreeMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: run_id,
                    message: message.clone(),
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                },
            })
            .unwrap();
        journal.set_run_id(Some(run_id));
        journal
            .append(crate::Event::Run {
                state: crate::RunState::Running,
                initial: None,
            })
            .unwrap();
        journal
            .append(crate::Event::CustomMessageDelivered {
                id: run_id,
                message,
            })
            .unwrap();
        journal
            .append(crate::Event::Assistant {
                content: String::new(),
                invocations: vec![crate::Invocation {
                    operation_id,
                    call: crate::ToolCall {
                        id: "job".into(),
                        name: "missing-job".into(),
                        arguments: "{}".into(),
                    },
                }],
                usage: None,
                request_id: None,
            })
            .unwrap();
        journal
            .append(crate::Event::ToolStarted { operation_id })
            .unwrap();
        journal
            .append(crate::Event::ToolFinished {
                operation_id,
                outcome: crate::Outcome::Pending {
                    reference: "pending".into(),
                },
                states: Default::default(),
            })
            .unwrap();
        journal
            .append(crate::Event::Run {
                state: crate::RunState::WaitingForOperation,
                initial: None,
            })
            .unwrap();
        journal.set_run_id(None);
        // Simulate a previously selected branch from a legacy checkout.
        journal
            .append(crate::Event::Checkout { target: 1 })
            .unwrap();
        journal
            .append_user_with_images("branch B".into(), vec![], None)
            .unwrap();
        let branch_b = journal.head();
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "fixture")
            .build(journal)
            .unwrap();
        assert!(
            matches!(session.start().await, Err(Error::PendingOperation(id)) if id == operation_id)
        );
        assert_eq!(session.journal().unwrap().head(), branch_b);
        assert_eq!(
            session.journal().unwrap().pending_custom_trigger(),
            Some(run_id)
        );
    }

    #[tokio::test]
    async fn start_restores_interrupted_tree_handoff_without_relocking_journal() {
        let mut journal = Session::with_store(MemoryStore::default()).unwrap();
        journal
            .append_user_with_images("branch A".into(), vec![], None)
            .unwrap();
        let run_id = Uuid::new_v4();
        let message = crate::CustomMessage {
            custom_type: "handoff-start".into(),
            content: crate::ContextContent::Text("trigger".into()),
            display: false,
            details: None,
        };
        journal
            .append(crate::Event::TreeMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: run_id,
                    message: message.clone(),
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                },
            })
            .unwrap();
        journal.set_run_id(Some(run_id));
        journal
            .append(crate::Event::Run {
                state: crate::RunState::Running,
                initial: None,
            })
            .unwrap();
        journal
            .append(crate::Event::CustomMessageDelivered {
                id: run_id,
                message,
            })
            .unwrap();
        journal.set_run_id(None);
        journal.checkout(1).unwrap();
        journal
            .append_user_with_images("branch B".into(), vec![], None)
            .unwrap();
        let branch_b = journal.head();
        assert!(
            journal
                .begin_tree_trigger_handoff(run_id)
                .unwrap()
                .is_some()
        );
        journal.set_run_id(Some(run_id));
        journal
            .append(crate::Event::Run {
                state: crate::RunState::Completed,
                initial: None,
            })
            .unwrap();
        journal.set_run_id(None);
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
            .build(journal)
            .unwrap();
        session.start().await.unwrap();
        assert_eq!(session.journal().unwrap().head(), branch_b);
    }

    #[test]
    fn nested_journal_reads_return_busy_instead_of_panicking() {
        let session = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
            .in_memory()
            .unwrap();
        assert!(session.fork_at(session.journal().unwrap().head()).is_err());
        let dir = tempfile::tempdir().unwrap();
        assert!(
            session
                .fork_to(
                    session.journal().unwrap().head(),
                    dir.path().join("fork.jsonl")
                )
                .is_err()
        );
        let guard = session.journal().unwrap();
        assert!(session.statistics().is_err());
        assert!(session.pending_inputs().is_err());
        drop(guard);
    }

    #[test]
    fn interrupted_tree_handoff_reopens_on_originally_selected_branch() {
        for completed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("handoff-crash.jsonl");
            let mut journal = Session::open(&path).unwrap();
            journal
                .append_user_with_images("branch A".into(), vec![], None)
                .unwrap();
            let run_id = Uuid::new_v4();
            let message = crate::CustomMessage {
                custom_type: "handoff-crash".into(),
                content: crate::ContextContent::Text("trigger".into()),
                display: false,
                details: None,
            };
            journal
                .append(crate::Event::TreeMessageQueued {
                    input: crate::QueuedCustomMessage {
                        id: run_id,
                        message: message.clone(),
                        delivery: crate::CustomMessageDelivery::TriggerTurn,
                    },
                })
                .unwrap();
            journal.set_run_id(Some(run_id));
            journal
                .append(crate::Event::Run {
                    state: crate::RunState::Running,
                    initial: None,
                })
                .unwrap();
            journal
                .append(crate::Event::CustomMessageDelivered {
                    id: run_id,
                    message,
                })
                .unwrap();
            journal.set_run_id(None);
            journal.checkout(1).unwrap();
            journal
                .append_user_with_images("branch B".into(), vec![], None)
                .unwrap();
            let branch_b = journal.head();
            assert!(
                journal
                    .begin_tree_trigger_handoff(run_id)
                    .unwrap()
                    .is_some()
            );
            if completed {
                journal.set_run_id(Some(run_id));
                journal
                    .append(crate::Event::Run {
                        state: crate::RunState::Completed,
                        initial: None,
                    })
                    .unwrap();
            }
            drop(journal);
            let restored = Session::open(&path).unwrap();
            assert_eq!(restored.head(), branch_b);
            assert_eq!(
                restored.pending_custom_trigger(),
                (!completed).then_some(run_id)
            );
            let count = restored.records().len();
            drop(restored);
            let again = Session::open(&path).unwrap();
            assert_eq!(again.head(), branch_b);
            assert_eq!(again.records().len(), count);
        }
    }
    #[async_trait::async_trait]
    impl Extension for BeforeTreeMessages {
        fn id(&self) -> &str {
            "before-tree-messages"
        }
        fn handles_tree_navigation(&self) -> bool {
            true
        }
        async fn before_tree_with_host(
            &self,
            _: &crate::TreePreparation,
            _: Option<&crate::ExtensionState>,
            _: &CancellationToken,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<Option<crate::TreeDecision>> {
            for (content, delivery) in [
                ("tree steer", crate::CustomMessageDelivery::Steer),
                ("tree follow", crate::CustomMessageDelivery::FollowUp),
                ("tree next", crate::CustomMessageDelivery::NextTurn),
                ("tree trigger", crate::CustomMessageDelivery::TriggerTurn),
            ] {
                if delivery == crate::CustomMessageDelivery::TriggerTurn && !self.trigger {
                    continue;
                }
                host.send_message(
                    crate::CustomMessage {
                        custom_type: "tree-before".into(),
                        content: crate::ContextContent::Text(content.into()),
                        display: false,
                        details: None,
                    },
                    delivery,
                )
                .await?;
            }
            Ok(Some(crate::TreeDecision {
                cancel: self.cancel,
                ..Default::default()
            }))
        }
    }
    struct TreeCaptureModel(Arc<Mutex<Vec<Vec<Message>>>>);
    #[async_trait::async_trait]
    impl Model for TreeCaptureModel {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
            self.0.lock().unwrap().push(request.messages);
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Text("done".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])))
        }
    }
    #[tokio::test]
    async fn before_tree_queued_messages_survive_checkout_cancel_and_reopen() {
        const CHILD: &str = "PI_RS_TREE_MESSAGE_TEST_CHILD";
        let mode = match std::env::var(CHILD) {
            Ok(mode) => mode,
            Err(_) => {
                // A parent process can terminate a child even if synchronous
                // journal I/O or a current-thread runtime stalls indefinitely.
                for mode in ["memory", "file"] {
                    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                        .args(["--exact", "sdk::tests::before_tree_queued_messages_survive_checkout_cancel_and_reopen", "--nocapture"])
                        .env(CHILD, mode)
                        .spawn().unwrap();
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
                    let status = loop {
                        if let Some(status) = child.try_wait().unwrap() {
                            break status;
                        }
                        if std::time::Instant::now() >= deadline {
                            let _ = child.kill();
                            let _ = child.wait();
                            panic!("{mode} tree fixture exceeded the 180-second watchdog");
                        }
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    };
                    assert!(
                        status.success(),
                        "{mode} tree fixture child failed: {status}"
                    );
                }
                return;
            }
        };
        assert!(matches!(mode.as_str(), "memory" | "file"));
        for cancel_tree in [false, true] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("tree-messages.jsonl");
            let builder =
                SessionBuilder::new(Arc::new(TreeCaptureModel(requests.clone())), "fixture")
                    .extension(Arc::new(BeforeTreeMessages {
                        cancel: cancel_tree,
                        trigger: true,
                    }))
                    .unwrap();
            let mut session = if mode == "file" {
                builder.open(&path).unwrap()
            } else {
                builder.in_memory().unwrap()
            };
            session.prompt("first").await.unwrap();
            let selected = session
                .journal()
                .unwrap()
                .records()
                .iter()
                .find(|record| matches!(record.event, crate::Event::User { .. }))
                .unwrap()
                .id;
            let result = session
                .navigate_tree(selected, crate::TreeNavigationOptions::default())
                .await;
            if cancel_tree {
                assert!(matches!(result, Err(Error::Cancelled)), "{result:?}");
            } else {
                result.unwrap();
            }
            assert!(
                session
                    .journal()
                    .unwrap()
                    .pending_custom_trigger()
                    .is_none()
            );
            assert_eq!(
                session
                    .journal()
                    .unwrap()
                    .records()
                    .iter()
                    .filter(|record| matches!(&record.event,
                crate::Event::CustomMessage { message } if message.custom_type == "tree-before"))
                    .count(),
                2
            );
            let snapshots = requests.lock().unwrap();
            assert_eq!(snapshots.len(), 2);
            let texts: Vec<_> = snapshots[1]
                .iter()
                .filter_map(|message| match message {
                    Message::User { content } => Some(content.as_str()),
                    _ => None,
                })
                .collect();
            assert!(texts.ends_with(&["tree trigger", "tree next"]), "{texts:?}");
            drop(snapshots);
            let records = (mode == "memory").then(|| session.journal().unwrap().records().to_vec());
            drop(session);
            if let Some(records) = records {
                let mut contents = Vec::new();
                for record in &records {
                    serde_json::to_writer(&mut contents, record).unwrap();
                    contents.push(b'\n');
                }
                std::fs::write(&path, contents).unwrap();
            }
            let restored = Session::open(&path).unwrap();
            assert!(restored.pending_custom_trigger().is_none());
            assert!(restored.queued_custom_messages().is_empty());
            assert_eq!(restored.records().iter().filter(|record| matches!(&record.event,
                crate::Event::CustomMessageDelivered { message, .. } if message.custom_type == "tree-before")).count(), 2);
        }
    }
    #[tokio::test]
    async fn before_tree_next_turn_survives_checkout_reopen_and_later_branch_switch() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tree-next.jsonl");
        let mut session =
            SessionBuilder::new(Arc::new(TreeCaptureModel(requests.clone())), "fixture")
                .extension(Arc::new(BeforeTreeMessages {
                    cancel: false,
                    trigger: false,
                }))
                .unwrap()
                .open(&path)
                .unwrap();
        session.prompt("first").await.unwrap();
        let selected = session
            .journal()
            .unwrap()
            .records()
            .iter()
            .find(|record| matches!(record.event, crate::Event::User { .. }))
            .unwrap()
            .id;
        session
            .navigate_tree(selected, crate::TreeNavigationOptions::default())
            .await
            .unwrap();
        assert_eq!(session.journal().unwrap().queued_custom_messages().len(), 1);
        let head = session.journal().unwrap().head();
        let fork = session.fork_at(head).unwrap();
        assert_eq!(fork.journal().unwrap().queued_custom_messages().len(), 1);
        drop(fork);
        drop(session);
        let mut reopened =
            SessionBuilder::new(Arc::new(TreeCaptureModel(requests.clone())), "fixture")
                .open(&path)
                .unwrap();
        reopened.prompt("second").await.unwrap();
        let snapshots = requests.lock().unwrap();
        let texts: Vec<_> = snapshots[1]
            .iter()
            .filter_map(|message| match message {
                Message::User { content } => Some(content.as_str()),
                _ => None,
            })
            .collect();
        assert!(texts.ends_with(&["second", "tree next"]), "{texts:?}");
        drop(snapshots);
        reopened.checkout(selected).unwrap();
        assert!(
            reopened
                .journal()
                .unwrap()
                .queued_custom_messages()
                .is_empty()
        );
    }
    struct TreeLifecycleMessage;
    #[async_trait::async_trait]
    impl Extension for TreeLifecycleMessage {
        fn id(&self) -> &str {
            "tree-lifecycle-message"
        }
        fn handles_tree_navigation(&self) -> bool {
            true
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            if matches!(event, crate::LifecycleEvent::SessionTree { .. }) {
                host.send_message(
                    crate::CustomMessage {
                        custom_type: "tree".into(),
                        content: crate::ContextContent::Text("tree trigger".into()),
                        display: false,
                        details: None,
                    },
                    crate::CustomMessageDelivery::TriggerTurn,
                )
                .await?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn tree_lifecycle_trigger_runs_on_destination_branch() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tree.jsonl");
        let mut session = SessionBuilder::new(model.clone(), "fixture")
            .extension(Arc::new(TreeLifecycleMessage))
            .unwrap()
            .open(&path)
            .unwrap();
        session.prompt("first").await.unwrap();
        let selected = session
            .journal()
            .unwrap()
            .records()
            .iter()
            .find(|record| matches!(record.event, crate::Event::User { .. }))
            .unwrap()
            .id;
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            session.navigate_tree(selected, crate::TreeNavigationOptions::default()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(model.0.load(Ordering::SeqCst), 2);
        assert!(
            session
                .journal()
                .unwrap()
                .pending_custom_trigger()
                .is_none()
        );
        assert_eq!(session.journal().unwrap().branch().iter().filter(|record|
            matches!(&record.event, crate::Event::CustomMessageDelivered { message, .. }
                if message.custom_type == "tree")).count(), 1);
        drop(session);
        let restored = Session::open(&path).unwrap();
        assert!(restored.pending_custom_trigger().is_none());
        assert_eq!(restored.branch().iter().filter(|record|
            matches!(&record.event, crate::Event::CustomMessageDelivered { message, .. }
                if message.custom_type == "tree")).count(), 1);
    }

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
            .unwrap()
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
            session.journal().unwrap().records()[selected as usize - 1].parent
        );
        assert!(matches!(summary.event, crate::Event::BranchSummary { .. }));
        assert_eq!(
            session.journal().unwrap().labels()[&summary.id],
            "SDK summary"
        );
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
        let selected = session.journal().unwrap().head();
        session.prompt("second").await.unwrap();
        session
            .set_model(Arc::new(WaitingModel), "waiting")
            .unwrap();
        let head = session.journal().unwrap().head();
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
                .unwrap()
                .branch()
                .iter()
                .any(|record| record.id == head)
        );
        assert!(
            !session
                .journal()
                .unwrap()
                .records()
                .iter()
                .any(|record| matches!(
                    record.event,
                    crate::Event::Checkout { .. } | crate::Event::BranchSummary { .. }
                ))
        );
        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn idle_queue_clear_returns_messages_and_records_the_removal() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model, "fixture").in_memory().unwrap();
        session.controls().follow_up("queued").await.unwrap();
        assert_eq!(session.pending_inputs().unwrap(), 1);
        let removed = session.clear_queued_inputs().unwrap();
        assert_eq!(removed[0].input.content, "queued");
        assert_eq!(session.pending_inputs().unwrap(), 0);
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
        let head = session.journal().unwrap().head();
        let mut fork = session.fork_at(head).unwrap();
        assert!(!fork.auto_compaction_enabled());
        assert_eq!(
            fork.journal()
                .unwrap()
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
                .unwrap()
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
            .try_lock()
            .unwrap()
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
        let control = session.journal().unwrap().auto_compaction_control();
        control.set_token_settings(crate::CompactionTokens {
            reserve_tokens: None,
            keep_recent_tokens: Some(target),
        });
        assert_eq!(
            control
                .retained_turns(&session.journal().unwrap(), 1)
                .unwrap(),
            1
        );
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
                .unwrap()
                .records()
                .iter()
                .any(|record| matches!(record.event, crate::Event::Compacted { .. }))
        );
        session.summarize(0).await.unwrap();
        assert_eq!(model.0.load(Ordering::SeqCst), 3);
        session.close().await.unwrap();
    }

    struct CompactionHookMessages;
    #[async_trait::async_trait]
    impl Extension for CompactionHookMessages {
        fn id(&self) -> &str {
            "compaction-hook-messages"
        }
        async fn before_compaction_with_host(
            &self,
            _: &mut Compaction,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::Decision> {
            host.send_message(
                crate::CustomMessage {
                    custom_type: "compact-hook".into(),
                    content: crate::ContextContent::Text("before".into()),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::Steer,
            )
            .await?;
            Ok(crate::Decision::Allow)
        }
        async fn after_compaction_with_host(
            &self,
            _: &Compaction,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<Option<crate::ExtensionState>> {
            for (content, delivery) in [
                ("compact next", crate::CustomMessageDelivery::NextTurn),
                ("compact trigger", crate::CustomMessageDelivery::TriggerTurn),
            ] {
                host.send_message(
                    crate::CustomMessage {
                        custom_type: "compact-hook".into(),
                        content: crate::ContextContent::Text(content.into()),
                        display: false,
                        details: None,
                    },
                    delivery,
                )
                .await?;
            }
            Ok(None)
        }
    }
    #[tokio::test]
    async fn summary_compaction_hooks_journal_reentrant_messages_and_trigger() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compaction-hooks.jsonl");
        let mut session =
            SessionBuilder::new(Arc::new(TreeCaptureModel(requests.clone())), "fixture")
                .extension(Arc::new(CompactionHookMessages))
                .unwrap()
                .open(&path)
                .unwrap();
        session.prompt("first").await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), session.summarize(0))
            .await
            .unwrap()
            .unwrap();
        assert!(
            session
                .journal()
                .unwrap()
                .pending_custom_trigger()
                .is_none()
        );
        let snapshots = requests.lock().unwrap();
        assert_eq!(snapshots.len(), 3);
        let texts: Vec<_> = snapshots[2]
            .iter()
            .filter_map(|message| match message {
                Message::User { content } => Some(content.as_str()),
                _ => None,
            })
            .collect();
        assert!(
            texts.ends_with(&["compact trigger", "compact next"]),
            "{texts:?}"
        );
        drop(snapshots);
        drop(session);
        let restored = Session::open(&path).unwrap();
        assert!(restored.pending_custom_trigger().is_none());
        assert_eq!(restored.records().iter().filter(|record| matches!(&record.event,
            crate::Event::CustomMessageDelivered { message, .. } if message.custom_type == "compact-hook")).count(), 2);
    }

    #[tokio::test]
    async fn supplied_compaction_summary_can_service_host_calls() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut session =
            SessionBuilder::new(Arc::new(TreeCaptureModel(requests.clone())), "fixture")
                .extension(Arc::new(CompactionHookMessages))
                .unwrap()
                .in_memory()
                .unwrap();
        session.prompt("first").await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            session.compact_with_hooks(Compaction {
                summary: "supplied".into(),
                keep_turns: 0,
            }),
        )
        .await
        .unwrap()
        .unwrap();
        let snapshots = requests.lock().unwrap();
        assert_eq!(snapshots.len(), 2);
        let texts: Vec<_> = snapshots[1]
            .iter()
            .filter_map(|message| match message {
                Message::User { content } => Some(content.as_str()),
                _ => None,
            })
            .collect();
        assert!(
            texts.ends_with(&["compact trigger", "compact next"]),
            "{texts:?}"
        );
        assert!(
            session
                .journal()
                .unwrap()
                .pending_custom_trigger()
                .is_none()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn synchronous_sdk_compact_services_reentrant_hooks_and_trigger() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut session =
            SessionBuilder::new(Arc::new(TreeCaptureModel(requests.clone())), "fixture")
                .extension(Arc::new(CompactionHookMessages))
                .unwrap()
                .in_memory()
                .unwrap();
        session.prompt("first").await.unwrap();
        session
            .compact(Compaction {
                summary: "supplied".into(),
                keep_turns: 0,
            })
            .unwrap();
        assert!(
            session
                .journal()
                .unwrap()
                .pending_custom_trigger()
                .is_none()
        );
        let snapshots = requests.lock().unwrap();
        assert_eq!(snapshots.len(), 2);
        let texts: Vec<_> = snapshots[1]
            .iter()
            .filter_map(|message| match message {
                Message::User { content } => Some(content.as_str()),
                _ => None,
            })
            .collect();
        assert!(
            texts.ends_with(&["compact trigger", "compact next"]),
            "{texts:?}"
        );
    }

    struct ReentrantCompactionControl(Arc<Mutex<Option<SessionControl>>>);
    #[async_trait::async_trait]
    impl Extension for ReentrantCompactionControl {
        fn id(&self) -> &str {
            "reentrant-compaction-control"
        }
        async fn before_compaction_with_host(
            &self,
            _: &mut Compaction,
            _: Option<&crate::ExtensionState>,
            _: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::Decision> {
            let control = self.0.lock().unwrap().clone().expect("control installed");
            control
                .steer("control input")
                .await
                .map_err(|error| error.to_string())?;
            Ok(crate::Decision::Allow)
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn synchronous_compaction_services_reentrant_control_input_hook() {
        let slot = Arc::new(Mutex::new(None));
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "fixture")
            .limits(Limits {
                tool_timeout: std::time::Duration::from_secs(1),
                ..Limits::default()
            })
            .extension(Arc::new(InputMessageHook))
            .unwrap()
            .extension(Arc::new(ReentrantCompactionControl(slot.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        *slot.lock().unwrap() = Some(session.controls());
        session.prompt("first").await.unwrap();
        session
            .compact(Compaction {
                summary: "summary".into(),
                keep_turns: 0,
            })
            .unwrap();
        assert!(
            session
                .journal()
                .unwrap()
                .queued_custom_messages()
                .iter()
                .any(|(_, message)| message.message.content
                    == crate::ContextContent::Text("streaming hook".into()))
        );
        assert!(
            session
                .journal()
                .unwrap()
                .queued_inputs()
                .iter()
                .any(|input| input.input.content == "control input")
        );
    }

    struct NeverReturningCompaction(Arc<std::sync::atomic::AtomicBool>);
    #[async_trait::async_trait]
    impl Extension for NeverReturningCompaction {
        fn id(&self) -> &str {
            "never-returning-compaction"
        }
        async fn before_compaction_with_host(
            &self,
            _: &mut Compaction,
            _: Option<&crate::ExtensionState>,
            _: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::Decision> {
            let released = self.0.clone();
            tokio::task::spawn_blocking(move || {
                while !released.load(Ordering::SeqCst) {
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            });
            std::future::pending().await
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn synchronous_compaction_times_out_unresponsive_hook() {
        let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let watchdog = released.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(2));
            watchdog.store(true, Ordering::SeqCst);
        });
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "fixture")
            .limits(Limits {
                tool_timeout: std::time::Duration::from_millis(30),
                ..Limits::default()
            })
            .extension(Arc::new(NeverReturningCompaction(released.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        session.prompt("first").await.unwrap();
        let started = std::time::Instant::now();
        let error = session
            .compact(Compaction {
                summary: "summary".into(),
                keep_turns: 0,
            })
            .unwrap_err();
        released.store(true, Ordering::SeqCst);
        assert!(error.to_string().contains("compaction hook timed out"));
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }

    struct CompactionLifecycleMessage;
    #[async_trait::async_trait]
    impl Extension for CompactionLifecycleMessage {
        fn id(&self) -> &str {
            "compaction-lifecycle-message"
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
            if matches!(
                event,
                crate::LifecycleEvent::SessionCompact { .. }
                    | crate::LifecycleEvent::SessionCompactFailed { .. }
            ) {
                host.send_message(
                    crate::CustomMessage {
                        custom_type: "compaction-lifecycle".into(),
                        content: crate::ContextContent::Text("after compact".into()),
                        display: false,
                        details: None,
                    },
                    crate::CustomMessageDelivery::TriggerTurn,
                )
                .await?;
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn compaction_lifecycle_trigger_is_journaled_and_runs_after_summary() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compact.jsonl");
        let mut session = SessionBuilder::new(model.clone(), "fixture")
            .extension(Arc::new(CompactionLifecycleMessage))
            .unwrap()
            .open(&path)
            .unwrap();
        session.prompt("first").await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), session.summarize(0))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(model.0.load(Ordering::SeqCst), 3);
        assert!(
            session
                .journal()
                .unwrap()
                .pending_custom_trigger()
                .is_none()
        );
        drop(session);
        let restored = Session::open(&path).unwrap();
        assert!(restored.pending_custom_trigger().is_none());
        assert_eq!(restored.records().iter().filter(|record| matches!(&record.event,
            crate::Event::CustomMessageDelivered { message, .. } if message.custom_type == "compaction-lifecycle")).count(), 1);
    }

    #[tokio::test]
    async fn failed_compaction_preserves_error_and_drains_lifecycle_trigger() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model.clone(), "fixture")
            .extension(Arc::new(CompactionLifecycleMessage))
            .unwrap()
            .in_memory()
            .unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(3), session.summarize(0))
            .await
            .unwrap();
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Nothing to compact")
        );
        assert_eq!(model.0.load(Ordering::SeqCst), 1);
        assert!(
            session
                .journal()
                .unwrap()
                .pending_custom_trigger()
                .is_none()
        );
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
        assert_eq!(session.statistics().unwrap().cost, Some(0.0));
        session.prompt("hello").await.unwrap();
        let stats = session.statistics().unwrap();
        assert_eq!(stats, session.journal().unwrap().statistics());
        assert_eq!(stats.session_id, session.journal().unwrap().session_id());
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
        let head = session.journal().unwrap().head();
        let mut fork = session.fork_at(head).unwrap();
        assert_eq!(fork.statistics().unwrap().tokens, stats.tokens);
        assert_ne!(fork.statistics().unwrap().session_id, stats.session_id);
        session.summarize(0).await.unwrap();
        assert_eq!(session.statistics().unwrap().tokens.total, 26);
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
            .try_lock()
            .unwrap()
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
        let removed = session.journal().unwrap().head();
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
        let root = session.journal().unwrap().head();
        session
            .extension_command("input-test", "prefix", "branch")
            .unwrap();
        let controls = session.controls();
        controls.follow_up("handled").await.unwrap();
        assert_eq!(session.pending_inputs().unwrap(), 0);
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
        let head = session.journal().unwrap().head();
        let mut fork = session.fork_at(head).unwrap();
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
        assert_eq!(session.pending_inputs().unwrap(), 0);
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
        let head = session.journal().unwrap().head();
        let mut fork = session.fork_to(head, &path).unwrap();
        assert_ne!(
            fork.journal().unwrap().session_id(),
            session.journal().unwrap().session_id()
        );
        let head = session.journal().unwrap().head();
        assert!(session.fork_to(head, &path).is_err());
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
        let head = session.journal().unwrap().head();
        let mut fork = session.fork_at(head).unwrap();
        assert_eq!(fork.messages().unwrap(), session.messages().unwrap());
        assert_ne!(
            fork.journal().unwrap().session_id(),
            session.journal().unwrap().session_id()
        );
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
    struct LiveKeyResultHook(bool);
    struct LiveKeyTool(bool);
    #[async_trait::async_trait]
    impl crate::Tool for LiveKeyTool {
        fn spec(&self) -> crate::ToolSpec {
            <LifecycleMessageTool as crate::Tool>::spec(&LifecycleMessageTool)
        }
        fn can_poll(&self) -> bool {
            self.0
        }
        async fn invoke(&self, _: serde_json::Value, _: crate::ToolContext) -> crate::Outcome {
            if self.0 {
                crate::Outcome::Pending {
                    reference: "live".into(),
                }
            } else {
                crate::Outcome::Succeeded("done".into())
            }
        }
        async fn poll(&self, _: &str, _: crate::ToolContext) -> Option<crate::Outcome> {
            Some(crate::Outcome::Succeeded("done".into()))
        }
    }
    #[async_trait::async_trait]
    impl Extension for LiveKeyResultHook {
        fn id(&self) -> &str {
            "live-key-result"
        }
        fn tools(&self) -> Vec<Arc<dyn crate::Tool>> {
            vec![Arc::new(LiveKeyTool(self.0))]
        }
        fn handles_tool_result(&self) -> bool {
            true
        }
        async fn tool_result_update_with_host(
            &self,
            _: &crate::ToolCall,
            _: &crate::Outcome,
            _: &crate::ToolContext,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<Option<crate::Outcome>> {
            if !host.supports_keyed_messages() {
                return Err("keyed messages unavailable".into());
            }
            host.send_message_with_key(
                crate::CustomMessage {
                    custom_type: "live-key".into(),
                    content: crate::ContextContent::Text("result".into()),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::NextTurn,
                "result",
            )
            .await?;
            Ok(None)
        }
    }
    #[tokio::test]
    async fn live_and_live_polled_tool_results_support_keyed_messages() {
        for polling in [false, true] {
            let mut session = SessionBuilder::new(
                Arc::new(MessageDeliveryModel(Arc::new(Mutex::new(Vec::new())))),
                "fixture",
            )
            .extension(Arc::new(LiveKeyResultHook(polling)))
            .unwrap()
            .in_memory()
            .unwrap();
            session.prompt("prompt").await.unwrap();
            assert_eq!(session.journal().unwrap().records().iter().filter(|record| matches!(&record.event,
                crate::Event::CustomMessageQueued { input } if input.message.custom_type == "live-key")).count(), 1, "polling={polling}");
        }
    }

    struct PendingMessageHook;
    struct PendingKeyedResultHook {
        paused: tokio::sync::Notify,
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl Extension for PendingKeyedResultHook {
        fn id(&self) -> &str {
            "pending-keyed-result"
        }
        fn handles_tool_result(&self) -> bool {
            true
        }
        async fn tool_result_update_with_host(
            &self,
            _: &crate::ToolCall,
            _: &crate::Outcome,
            _: &crate::ToolContext,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<Option<crate::Outcome>> {
            assert!(host.supports_keyed_messages());
            host.send_message_with_key(
                crate::CustomMessage {
                    custom_type: "poll-keyed".into(),
                    content: crate::ContextContent::Text("once".into()),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::Steer,
                "result",
            )
            .await?;
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.paused.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(None)
        }
    }

    #[tokio::test]
    async fn recovered_poll_result_key_survives_crash_before_finish_tool() {
        let job = Arc::new(PollingJob {
            dispatched: AtomicUsize::new(0),
            polls: AtomicUsize::new(0),
            started: tokio::sync::Notify::new(),
        });
        let mut original = SessionBuilder::new(Arc::new(TaskModel), "task")
            .extension(Arc::new(JobExtension(job.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        let control = original.controls();
        let (result, ()) = tokio::join!(original.prompt("start job"), async {
            job.started.notified().await;
            control.abort();
        });
        assert!(matches!(result, Err(Error::Cancelled)));
        let run_id = original
            .journal()
            .unwrap()
            .records()
            .iter()
            .find_map(|record| match record.event {
                crate::Event::Run {
                    state: crate::RunState::Running,
                    ..
                } => record.run_id,
                _ => None,
            })
            .unwrap();
        let mut journal =
            Session::from_records(original.journal().unwrap().records().to_vec()).unwrap();
        let hook = Arc::new(PendingKeyedResultHook {
            paused: tokio::sync::Notify::new(),
            calls: AtomicUsize::new(0),
        });
        let mut registry = Registry::default();
        registry.register(Arc::new(JobExtension(job))).unwrap();
        registry.register(hook.clone()).unwrap();
        let agent = Agent {
            model: &TaskModel,
            model_name: "task",
            registry: &registry,
            system_prompt: "test",
            limits: Limits::default(),
        };
        let (_sender, mut inputs) = mpsc::channel::<crate::AgentInput>(16);
        {
            let cancel = CancellationToken::new();
            let mut observer = |_: Notification<'_>| {};
            let future = agent.run_correlated(
                &mut journal,
                run_id,
                None,
                &mut inputs,
                &cancel,
                &mut observer,
            );
            tokio::pin!(future);
            tokio::select! { _ = &mut future => panic!("callback did not pause"), _ = hook.paused.notified() => {} }
        }
        let mut restored = Session::from_records(journal.records().to_vec()).unwrap();
        assert_eq!(
            agent
                .run_correlated(
                    &mut restored,
                    run_id,
                    None,
                    &mut inputs,
                    &CancellationToken::new(),
                    &mut |_| {}
                )
                .await
                .unwrap(),
            "done"
        );
        assert_eq!(hook.calls.load(Ordering::SeqCst), 2);
        assert_eq!(restored.records().iter().filter(|record| matches!(&record.event,
            crate::Event::CustomMessageQueued { input } if input.message.custom_type == "poll-keyed")).count(), 1);
    }

    #[async_trait::async_trait]
    impl Extension for PendingMessageHook {
        fn id(&self) -> &str {
            "pending-messages"
        }
        fn handles_tool_result(&self) -> bool {
            true
        }
        async fn before_tool_with_host(
            &self,
            _: &str,
            _: &serde_json::Value,
            _: &crate::ToolContext,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::Decision> {
            host.send_message(
                crate::CustomMessage {
                    custom_type: "poll".into(),
                    content: crate::ContextContent::Text("before poll".into()),
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
                    custom_type: "poll".into(),
                    content: crate::ContextContent::Text("after poll".into()),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::Steer,
            )
            .await?;
            Ok(None)
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
            .extension(Arc::new(PendingMessageHook))
            .unwrap()
            .open(path)
            .unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(3), session.resume())
                .await
                .unwrap()
                .unwrap(),
            "done"
        );
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
        assert_eq!(texts, ["start job", "before poll", "after poll"]);
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
        assert_eq!(session.journal().unwrap().queued_custom_messages().len(), 1);
        let restored =
            Session::from_records(session.journal().unwrap().records().to_vec()).unwrap();
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
        assert!(
            session
                .journal()
                .unwrap()
                .queued_custom_messages()
                .is_empty()
        );
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
                crate::CustomMessageDelivery::TriggerTurn,
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
    struct BeforePolicyMessage;
    #[async_trait::async_trait]
    impl Extension for BeforePolicyMessage {
        fn id(&self) -> &str {
            "before-policy-message"
        }
        fn handles_before_tool_host(&self) -> bool {
            true
        }
        async fn before_tool_with_host(
            &self,
            _: &str,
            _: &serde_json::Value,
            _: &crate::ToolContext,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::Decision> {
            host.send_message(
                crate::CustomMessage {
                    custom_type: "policy".into(),
                    content: crate::ContextContent::Text("policy".into()),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::Steer,
            )
            .await?;
            Ok(crate::Decision::Allow)
        }
    }
    #[tokio::test]
    async fn lifecycle_host_calls_are_reentrant_and_ordered_at_turn_boundaries() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut session =
            SessionBuilder::new(Arc::new(MessageDeliveryModel(requests.clone())), "fixture")
                .extension(Arc::new(LifecycleMessageExtension))
                .unwrap()
                .extension(Arc::new(BeforePolicyMessage))
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
                "policy",
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
        assert_eq!(session.journal().unwrap().queued_custom_messages().len(), 1);
        assert_eq!(
            session.journal().unwrap().queued_custom_messages()[0]
                .1
                .delivery,
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
            _: &mut crate::ContextRequest<'_>,
            _: crate::RequestPurpose,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::ContextChange> {
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
            Ok(crate::ContextChange::Unchanged)
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
        let records = session.journal().unwrap().records().to_vec();
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

    struct ContextMessageHook(AtomicUsize);
    #[async_trait::async_trait]
    impl Extension for ContextMessageHook {
        fn id(&self) -> &str {
            "context-message"
        }
        fn handles_context_with_system(&self) -> bool {
            true
        }
        async fn context_with_system_with_host(
            &self,
            _: &mut crate::ContextMessages<'_>,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::ContextChange> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                host.send_message(
                    crate::CustomMessage {
                        custom_type: "context".into(),
                        content: crate::ContextContent::Text("context steer".into()),
                        display: false,
                        details: None,
                    },
                    crate::CustomMessageDelivery::Steer,
                )
                .await?;
            }
            Ok(crate::ContextChange::Unchanged)
        }
    }
    #[tokio::test]
    async fn context_hook_message_waits_for_current_request() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model.clone(), "fixture")
            .extension(Arc::new(ContextMessageHook(AtomicUsize::new(0))))
            .unwrap()
            .in_memory()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), session.prompt("first"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(model.0.load(Ordering::SeqCst), 2);
        let records = session.journal().unwrap().records().to_vec();
        let request = records
            .iter()
            .position(|r| matches!(r.event, crate::Event::ModelRequested { .. }))
            .unwrap();
        let delivery = records
            .iter()
            .position(|r| matches!(r.event, crate::Event::CustomMessageDelivered { .. }))
            .unwrap();
        assert!(request < delivery);
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
            session.journal().unwrap().queued_custom_messages()[0]
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
        assert_eq!(session.journal().unwrap().queued_custom_messages().len(), 1);
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

    struct TriggerFromLifecycle {
        settled: bool,
        fired: std::sync::atomic::AtomicBool,
    }
    #[async_trait::async_trait]
    impl Extension for TriggerFromLifecycle {
        fn id(&self) -> &str {
            "trigger-lifecycle"
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
            let matching = if self.settled {
                matches!(event, crate::LifecycleEvent::AgentSettled { .. })
            } else {
                matches!(event, crate::LifecycleEvent::SessionStart { .. })
            };
            if matching && !self.fired.swap(true, Ordering::SeqCst) {
                host.send_message(
                    crate::CustomMessage {
                        custom_type: "trigger".into(),
                        content: crate::ContextContent::Text("triggered".into()),
                        display: false,
                        details: Some(serde_json::json!({"test":true})),
                    },
                    crate::CustomMessageDelivery::TriggerTurn,
                )
                .await?;
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn idle_session_and_settled_callbacks_start_a_durable_triggered_run() {
        for settled in [false, true] {
            let model = Arc::new(EchoModel(AtomicUsize::new(0)));
            let mut session = SessionBuilder::new(model.clone(), "fixture")
                .extension(Arc::new(TriggerFromLifecycle {
                    settled,
                    fired: std::sync::atomic::AtomicBool::new(false),
                }))
                .unwrap()
                .in_memory()
                .unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(3), session.prompt("user"))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(model.0.load(Ordering::SeqCst), 2);
            let records = session.journal().unwrap().records().to_vec();
            let id = records
                .iter()
                .find_map(|record| match &record.event {
                    crate::Event::CustomMessageQueued { input }
                        if input.delivery == crate::CustomMessageDelivery::TriggerTurn =>
                    {
                        Some(input.id)
                    }
                    _ => None,
                })
                .unwrap();
            assert_eq!(records.iter().filter(|record| matches!(record.event, crate::Event::CustomMessageDelivered { id: delivered, .. } if delivered == id)).count(), 1);
            assert!(records.iter().any(|record| record.run_id == Some(id)
                && matches!(
                    record.event,
                    crate::Event::Run {
                        state: crate::RunState::Completed,
                        ..
                    }
                )));
            assert!(
                session
                    .journal()
                    .unwrap()
                    .pending_custom_trigger()
                    .is_none()
            );
        }
    }
    #[tokio::test]
    async fn interrupted_trigger_handoff_resumes_same_run_without_duplicate_delivery() {
        for phase in 0..4 {
            let id = Uuid::new_v4();
            let message = crate::CustomMessage {
                custom_type: "trigger".into(),
                content: crate::ContextContent::Text("recover".into()),
                display: false,
                details: None,
            };
            let mut journal = Session::with_store(crate::MemoryStore::default()).unwrap();
            crate::agent::queue_custom_message(
                &mut journal,
                crate::QueuedCustomMessage {
                    id,
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                    message: message.clone(),
                },
                &mut |_| {},
            )
            .unwrap();
            if phase >= 1 {
                journal.set_run_id(Some(id));
                journal
                    .append(crate::Event::Run {
                        state: crate::RunState::Running,
                        initial: None,
                    })
                    .unwrap();
                if phase >= 2 {
                    journal
                        .append(crate::Event::CustomMessageDelivered { id, message })
                        .unwrap();
                }
                if phase == 3 {
                    journal
                        .append(crate::Event::Assistant {
                            content: "already answered".into(),
                            invocations: vec![],
                            usage: None,
                            request_id: None,
                        })
                        .unwrap();
                }
                journal.set_run_id(None);
            }
            let restored = Session::from_records(journal.records().to_vec()).unwrap();
            let model = Arc::new(EchoModel(AtomicUsize::new(0)));
            let mut session = SessionBuilder::new(model.clone(), "fixture")
                .build(restored)
                .unwrap();
            session.start().await.unwrap();
            session.start().await.unwrap();
            assert_eq!(
                model.0.load(Ordering::SeqCst),
                usize::from(phase != 3),
                "phase {phase}"
            );
            assert_eq!(session.journal().unwrap().records().iter().filter(|r| matches!(r.event, crate::Event::CustomMessageDelivered { id: delivered, .. } if delivered == id)).count(), 1);
            assert!(
                session
                    .journal()
                    .unwrap()
                    .pending_custom_trigger()
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn waiting_and_undelivered_cancelled_triggers_resume_on_start() {
        for state in [
            crate::RunState::WaitingForOperation,
            crate::RunState::WaitingForReconciliation,
            crate::RunState::Cancelled,
        ] {
            let id = Uuid::new_v4();
            let mut journal = Session::with_store(crate::MemoryStore::default()).unwrap();
            crate::agent::queue_custom_message(
                &mut journal,
                crate::QueuedCustomMessage {
                    id,
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                    message: crate::CustomMessage {
                        custom_type: "trigger".into(),
                        content: crate::ContextContent::Text("pending".into()),
                        display: false,
                        details: None,
                    },
                },
                &mut |_| {},
            )
            .unwrap();
            journal.set_run_id(Some(id));
            journal
                .append(crate::Event::Run {
                    state,
                    initial: None,
                })
                .unwrap();
            journal.set_run_id(None);
            let restored = Session::from_records(journal.records().to_vec()).unwrap();
            let model = Arc::new(EchoModel(AtomicUsize::new(0)));
            let mut session = SessionBuilder::new(model.clone(), "fixture")
                .build(restored)
                .unwrap();
            session.start().await.unwrap();
            assert_eq!(model.0.load(Ordering::SeqCst), 1, "{state:?}");
            assert!(
                session
                    .journal()
                    .unwrap()
                    .pending_custom_trigger()
                    .is_none()
            );
            assert_eq!(session.journal().unwrap().records().iter().filter(|r| matches!(r.event, crate::Event::CustomMessageDelivered { id: delivered, .. } if delivered == id)).count(), 1);
        }
    }

    #[tokio::test]
    async fn delivered_waiting_triggers_resume_but_delivered_cancelled_trigger_does_not() {
        for (state, should_resume) in [
            (crate::RunState::WaitingForOperation, true),
            (crate::RunState::WaitingForReconciliation, true),
            (crate::RunState::Cancelled, false),
        ] {
            let id = Uuid::new_v4();
            let message = crate::CustomMessage {
                custom_type: "trigger".into(),
                content: crate::ContextContent::Text("already delivered".into()),
                display: false,
                details: None,
            };
            let mut journal = Session::with_store(crate::MemoryStore::default()).unwrap();
            crate::agent::queue_custom_message(
                &mut journal,
                crate::QueuedCustomMessage {
                    id,
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                    message: message.clone(),
                },
                &mut |_| {},
            )
            .unwrap();
            journal.set_run_id(Some(id));
            journal
                .append(crate::Event::Run {
                    state: crate::RunState::Running,
                    initial: None,
                })
                .unwrap();
            journal
                .append(crate::Event::CustomMessageDelivered { id, message })
                .unwrap();
            journal
                .append(crate::Event::Run {
                    state,
                    initial: None,
                })
                .unwrap();
            journal.set_run_id(None);
            let restored = Session::from_records(journal.records().to_vec()).unwrap();
            let model = Arc::new(EchoModel(AtomicUsize::new(0)));
            let mut session = SessionBuilder::new(model.clone(), "fixture")
                .build(restored)
                .unwrap();
            session.start().await.unwrap();
            assert_eq!(
                model.0.load(Ordering::SeqCst),
                usize::from(should_resume),
                "{state:?}"
            );
            assert_eq!(session.journal().unwrap().records().iter().filter(|r| matches!(r.event, crate::Event::CustomMessageDelivered { id: delivered, .. } if delivered == id)).count(), 1);
        }
    }

    #[tokio::test]
    async fn earlier_waiting_trigger_resumes_after_a_later_completed_run() {
        for delivered in [false, true] {
            let id = Uuid::new_v4();
            let message = crate::CustomMessage {
                custom_type: "trigger".into(),
                content: crate::ContextContent::Text("recover earlier".into()),
                display: false,
                details: None,
            };
            let mut journal = Session::with_store(crate::MemoryStore::default()).unwrap();
            crate::agent::queue_custom_message(
                &mut journal,
                crate::QueuedCustomMessage {
                    id,
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                    message: message.clone(),
                },
                &mut |_| {},
            )
            .unwrap();
            journal.set_run_id(Some(id));
            journal
                .append(crate::Event::Run {
                    state: crate::RunState::Running,
                    initial: None,
                })
                .unwrap();
            if delivered {
                journal
                    .append(crate::Event::CustomMessageDelivered { id, message })
                    .unwrap();
            }
            journal
                .append(crate::Event::Run {
                    state: crate::RunState::WaitingForOperation,
                    initial: None,
                })
                .unwrap();
            let later = Uuid::new_v4();
            journal.set_run_id(Some(later));
            journal
                .append(crate::Event::Run {
                    state: crate::RunState::Running,
                    initial: Some("later".into()),
                })
                .unwrap();
            journal
                .append(crate::Event::User {
                    content: "later".into(),
                    input_id: Some(later),
                })
                .unwrap();
            journal
                .append(crate::Event::Assistant {
                    content: "later answer".into(),
                    invocations: vec![],
                    usage: None,
                    request_id: None,
                })
                .unwrap();
            journal
                .append(crate::Event::Run {
                    state: crate::RunState::Completed,
                    initial: None,
                })
                .unwrap();
            journal.set_run_id(None);
            let model = Arc::new(EchoModel(AtomicUsize::new(0)));
            let mut session = SessionBuilder::new(model.clone(), "fixture")
                .build(Session::from_records(journal.records().to_vec()).unwrap())
                .unwrap();
            session.start().await.unwrap();
            assert_eq!(model.0.load(Ordering::SeqCst), 1, "delivered={delivered}");
            assert!(
                session
                    .journal()
                    .unwrap()
                    .pending_custom_trigger()
                    .is_none()
            );
            assert_eq!(session.journal().unwrap().records().iter().filter(|r| matches!(r.event, crate::Event::CustomMessageDelivered { id: x, .. } if x == id)).count(), 1);
        }
    }

    struct ReplayStartHook {
        ready: Arc<tokio::sync::Notify>,
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl Extension for ReplayStartHook {
        fn id(&self) -> &str {
            "replay-start"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            if matches!(event, crate::LifecycleEvent::AgentStart { .. }) {
                let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
                let message = crate::CustomMessage {
                    custom_type: "start".into(),
                    content: crate::ContextContent::Text(
                        if attempt == 0 { "once" } else { "changed path" }.into(),
                    ),
                    display: false,
                    details: None,
                };
                if attempt == 0 {
                    host.send_message(message, crate::CustomMessageDelivery::Steer)
                        .await?;
                    self.ready.notify_one();
                    std::future::pending::<()>().await;
                } else {
                    host.send_message(message, crate::CustomMessageDelivery::Steer)
                        .await?;
                }
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn unkeyed_agent_start_replay_accepts_changed_paths() {
        let ready = Arc::new(tokio::sync::Notify::new());
        let hook = Arc::new(ReplayStartHook {
            ready: ready.clone(),
            calls: AtomicUsize::new(0),
        });
        let mut registry = Registry::default();
        registry.register(hook.clone()).unwrap();
        let model = EchoModel(AtomicUsize::new(0));
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "test",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        let run_id = Uuid::new_v4();
        let cancel = CancellationToken::new();
        let (_sender, mut inputs) = mpsc::channel::<crate::AgentInput>(8);
        let mut observer = |_: Notification<'_>| {};
        let (first, ()) = tokio::join!(
            agent.run_correlated(
                &mut session,
                run_id,
                Some("prompt".into()),
                &mut inputs,
                &cancel,
                &mut observer
            ),
            async {
                ready.notified().await;
                cancel.cancel();
            }
        );
        assert!(matches!(first, Err(Error::Cancelled)));
        let original = session.records().to_vec();
        for before_delivery in [false, true] {
            let mut records = original.clone();
            if before_delivery {
                let index = records
                    .iter()
                    .position(|record| {
                        matches!(record.event, crate::Event::CustomMessageQueued { .. })
                    })
                    .unwrap();
                records.truncate(index + 1);
            }
            let mut session = Session::from_records(records).unwrap();
            agent
                .run_correlated(
                    &mut session,
                    run_id,
                    None,
                    &mut inputs,
                    &CancellationToken::new(),
                    &mut |_| {},
                )
                .await
                .unwrap();
            assert_eq!(session.records().iter().filter(|r| matches!(&r.event, crate::Event::CustomMessageQueued { input } if input.message.custom_type == "start")).count(), 2);
            assert_eq!(session.records().iter().filter(|r| matches!(&r.event, crate::Event::CustomMessageDelivered { message, .. } if message.custom_type == "start")).count(), 2);
        }
        assert_eq!(hook.calls.load(Ordering::SeqCst), 3);
    }

    struct KeyedCallbackHook {
        phase: &'static str,
        ready: Arc<tokio::sync::Notify>,
        calls: AtomicUsize,
    }
    impl KeyedCallbackHook {
        async fn send(&self, host: Arc<dyn crate::ToolHost>) -> crate::ExtensionResult<()> {
            let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
            let keys = if attempt == 0 {
                ["first", "second"]
            } else {
                ["second", "first"]
            };
            for key in keys {
                host.send_message_with_key(
                    crate::CustomMessage {
                        custom_type: "keyed".into(),
                        content: crate::ContextContent::Text(key.into()),
                        display: false,
                        details: None,
                    },
                    crate::CustomMessageDelivery::Steer,
                    key,
                )
                .await?;
            }
            if attempt == 0 {
                self.ready.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(())
        }
    }
    #[async_trait::async_trait]
    impl Extension for KeyedCallbackHook {
        fn id(&self) -> &str {
            "keyed-callback"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_context_with_system(&self) -> bool {
            self.phase == "context"
        }
        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            if (self.phase == "agent_start"
                && matches!(event, crate::LifecycleEvent::AgentStart { .. }))
                || (self.phase == "turn_start"
                    && matches!(event, crate::LifecycleEvent::TurnStart { .. }))
            {
                self.send(host).await?;
            }
            Ok(())
        }
        async fn before_request_with_host(
            &self,
            _: &mut crate::ContextRequest<'_>,
            _: crate::RequestPurpose,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::ContextChange> {
            if self.phase == "before_request" {
                self.send(host).await?;
            }
            Ok(crate::ContextChange::Unchanged)
        }
        async fn context_with_system_with_host(
            &self,
            _: &mut crate::ContextMessages<'_>,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::ContextChange> {
            if self.phase == "context" {
                self.send(host).await?;
            }
            Ok(crate::ContextChange::Unchanged)
        }
    }
    #[tokio::test]
    async fn keyed_callbacks_replay_after_crash_in_changed_order_without_duplicates() {
        for phase in ["agent_start", "before_request", "context", "turn_start"] {
            let ready = Arc::new(tokio::sync::Notify::new());
            let hook = Arc::new(KeyedCallbackHook {
                phase,
                ready: ready.clone(),
                calls: AtomicUsize::new(0),
            });
            let mut registry = Registry::default();
            registry.register(hook.clone()).unwrap();
            let model = EchoModel(AtomicUsize::new(0));
            let agent = Agent {
                model: &model,
                model_name: "fixture",
                registry: &registry,
                system_prompt: "test",
                limits: Limits::default(),
            };
            let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
            let run_id = Uuid::new_v4();
            let cancel = CancellationToken::new();
            let (_sender, mut inputs) = mpsc::channel::<crate::AgentInput>(8);
            let mut observer = |_: Notification<'_>| {};
            let (first, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                tokio::join!(
                    agent.run_correlated(
                        &mut session,
                        run_id,
                        Some("prompt".into()),
                        &mut inputs,
                        &cancel,
                        &mut observer
                    ),
                    async {
                        ready.notified().await;
                        cancel.cancel();
                    }
                )
            })
            .await
            .unwrap();
            assert!(matches!(first, Err(Error::Cancelled)), "{phase}");
            let mut session = Session::from_records(session.records().to_vec()).unwrap();
            agent
                .run_correlated(
                    &mut session,
                    run_id,
                    None,
                    &mut inputs,
                    &CancellationToken::new(),
                    &mut |_| {},
                )
                .await
                .unwrap();
            assert_eq!(hook.calls.load(Ordering::SeqCst), 2, "{phase}");
            assert_eq!(session.records().iter().filter(|r| matches!(&r.event, crate::Event::CustomMessageQueued { input } if input.message.custom_type == "keyed")).count(), 2, "{phase}");
            assert_eq!(session.records().iter().filter(|r| matches!(&r.event, crate::Event::CustomMessageDelivered { message, .. } if message.custom_type == "keyed")).count(), 2, "{phase}");
        }
    }

    struct TurnEndRecoveryHook {
        before_first: bool,
        calls: AtomicUsize,
        paused: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl Extension for TurnEndRecoveryHook {
        fn id(&self) -> &str {
            "turn-end-recovery"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            let crate::LifecycleEvent::TurnEnd {
                turn_index: 0,
                messages,
                ..
            } = event
            else {
                return Ok(());
            };
            let first_attempt = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
            for (index, key) in ["one", "two"].into_iter().enumerate() {
                if first_attempt && (index == 0) == self.before_first {
                    self.paused.notify_one();
                    std::future::pending::<()>().await;
                }
                host.send_message_with_key(
                    crate::CustomMessage {
                        custom_type: "turn-end-recovery".into(),
                        content: crate::ContextContent::Text(format!("{key}:{}", messages.len())),
                        display: false,
                        details: None,
                    },
                    if key == "one" {
                        crate::CustomMessageDelivery::AfterTurn
                    } else {
                        crate::CustomMessageDelivery::NextTurn
                    },
                    key,
                )
                .await?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn turn_end_recovers_before_and_between_keyed_sends() {
        for before_first in [true, false] {
            let hook = Arc::new(TurnEndRecoveryHook {
                before_first,
                calls: AtomicUsize::new(0),
                paused: tokio::sync::Notify::new(),
            });
            let mut registry = Registry::default();
            registry.register(hook.clone()).unwrap();
            let model = EchoModel(AtomicUsize::new(0));
            let agent = Agent {
                model: &model,
                model_name: "fixture",
                registry: &registry,
                system_prompt: "test",
                limits: Limits::default(),
            };
            let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
            let run_id = Uuid::new_v4();
            session.set_run_id(Some(run_id));
            session
                .append(crate::Event::Run {
                    state: crate::RunState::Running,
                    initial: Some("prompt".into()),
                })
                .unwrap();
            session
                .append(crate::Event::User {
                    content: "prompt".into(),
                    input_id: Some(run_id),
                })
                .unwrap();
            session
                .append(crate::Event::Assistant {
                    content: "answer".into(),
                    invocations: vec![],
                    usage: None,
                    request_id: None,
                })
                .unwrap();
            session.set_run_id(None);
            let cancel = CancellationToken::new();
            let (_sender, mut inputs) = mpsc::channel::<crate::AgentInput>(16);
            let mut observer = |_: Notification<'_>| {};
            let (first, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                tokio::join!(
                    agent.run_correlated(
                        &mut session,
                        run_id,
                        None,
                        &mut inputs,
                        &cancel,
                        &mut observer
                    ),
                    async {
                        hook.paused.notified().await;
                        cancel.cancel();
                    }
                )
            })
            .await
            .unwrap();
            assert!(matches!(first, Err(Error::Cancelled)));
            assert!(!session.records().iter().any(|record| matches!(&record.event,
                crate::Event::CustomMessageDelivered { message, .. } if message.custom_type == "turn-end-recovery")));
            let mut restored = Session::from_records(session.records().to_vec()).unwrap();
            let answer = agent
                .run_correlated(
                    &mut restored,
                    run_id,
                    None,
                    &mut inputs,
                    &CancellationToken::new(),
                    &mut |_| {},
                )
                .await
                .unwrap();
            assert_eq!(answer, "answer");
            assert_eq!(model.0.load(Ordering::SeqCst), 0);
            assert_eq!(restored.records().iter().filter(|record| matches!(&record.event,
                crate::Event::CustomMessageQueued { input } if input.message.custom_type == "turn-end-recovery")).count(), 2);
            assert_eq!(restored.records().iter().filter(|record| matches!(&record.event,
                crate::Event::CustomMessageDelivered { message, .. } if message.custom_type == "turn-end-recovery")).count(), 1);
            assert_eq!(
                restored
                    .records()
                    .iter()
                    .filter(|record| matches!(
                        record.event,
                        crate::Event::TurnEndCallbacksCompleted { .. }
                    ))
                    .count(),
                1
            );
        }
    }

    #[tokio::test]
    async fn new_run_finishes_cancelled_trigger_turn_end_before_deferred_delivery() {
        let hook = Arc::new(TurnEndRecoveryHook {
            before_first: false,
            calls: AtomicUsize::new(0),
            paused: tokio::sync::Notify::new(),
        });
        let mut registry = Registry::default();
        registry.register(hook.clone()).unwrap();
        let model = EchoModel(AtomicUsize::new(0));
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "test",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        let old_run = Uuid::new_v4();
        let initial = crate::CustomMessage {
            custom_type: "trigger".into(),
            content: crate::ContextContent::Text("start".into()),
            display: false,
            details: None,
        };
        session
            .append(crate::Event::CustomMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: old_run,
                    message: initial.clone(),
                    delivery: crate::CustomMessageDelivery::TriggerTurn,
                },
            })
            .unwrap();
        session.set_run_id(Some(old_run));
        session
            .append(crate::Event::Run {
                state: crate::RunState::Running,
                initial: None,
            })
            .unwrap();
        session
            .append(crate::Event::CustomMessageDelivered {
                id: old_run,
                message: initial,
            })
            .unwrap();
        let assistant_record = session
            .append(crate::Event::Assistant {
                content: "old answer".into(),
                invocations: vec![],
                usage: None,
                request_id: None,
            })
            .unwrap()
            .id;
        session.set_run_id(None);
        let (_sender, mut inputs) = mpsc::channel::<crate::AgentInput>(16);
        let cancel = CancellationToken::new();
        let mut observer = |_: Notification<'_>| {};
        let (first, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(
                agent.run_correlated(
                    &mut session,
                    old_run,
                    None,
                    &mut inputs,
                    &cancel,
                    &mut observer
                ),
                async {
                    hook.paused.notified().await;
                    cancel.cancel();
                }
            )
        })
        .await
        .unwrap();
        assert!(matches!(first, Err(Error::Cancelled)));
        assert!(session.pending_custom_trigger().is_none());
        let mut restored = Session::from_records(session.records().to_vec()).unwrap();
        let new_run = Uuid::new_v4();
        agent
            .run_correlated(
                &mut restored,
                new_run,
                Some("new prompt".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        let records = restored.records();
        let completed = records.iter().position(|record| matches!(record.event, crate::Event::TurnEndCallbacksCompleted { assistant_record: id } if id == assistant_record)).unwrap();
        let delivered = records.iter().position(|record| matches!(&record.event, crate::Event::CustomMessageDelivered { message, .. } if message.custom_type == "turn-end-recovery")).unwrap();
        let user = records.iter().position(|record| matches!(&record.event, crate::Event::User { content, .. } if content == "new prompt")).unwrap();
        assert!(completed < delivered && delivered < user);
        assert_eq!(records.iter().filter(|record| record.run_id == Some(old_run) && matches!(&record.event, crate::Event::CustomMessageQueued { input } if input.message.custom_type == "turn-end-recovery")).count(), 2);
    }

    #[tokio::test]
    async fn image_prompt_resolves_previous_turn_before_journaling_input() {
        let hook = Arc::new(TurnEndRecoveryHook {
            before_first: false,
            calls: AtomicUsize::new(1),
            paused: tokio::sync::Notify::new(),
        });
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut journal = Session::with_store(crate::MemoryStore::default()).unwrap();
        let previous = Uuid::new_v4();
        journal.set_run_id(Some(previous));
        journal
            .append(crate::Event::Run {
                state: crate::RunState::Running,
                initial: Some("old".into()),
            })
            .unwrap();
        journal
            .append(crate::Event::User {
                content: "old".into(),
                input_id: Some(previous),
            })
            .unwrap();
        let assistant_record = journal
            .append(crate::Event::Assistant {
                content: "old answer".into(),
                invocations: vec![],
                usage: None,
                request_id: None,
            })
            .unwrap()
            .id;
        journal.set_run_id(None);
        let mut session =
            SessionBuilder::new(Arc::new(TreeCaptureModel(requests.clone())), "fixture")
                .extension(hook)
                .unwrap()
                .build(journal)
                .unwrap();
        session
            .prompt_with_images(
                "new image",
                vec![crate::ImageContent {
                    data: "aGVsbG8=".into(),
                    mime_type: "image/png".into(),
                }],
            )
            .await
            .unwrap();
        let records = session.journal().unwrap().records().to_vec();
        let completed = records.iter().position(|record| matches!(record.event,
            crate::Event::TurnEndCallbacksCompleted { assistant_record: id } if id == assistant_record)).unwrap();
        let delivered = records.iter().position(|record| record.run_id == Some(previous) && matches!(&record.event,
            crate::Event::CustomMessageDelivered { message, .. } if message.custom_type == "turn-end-recovery")).unwrap();
        let image = records
            .iter()
            .position(|record| {
                matches!(&record.event,
            crate::Event::UserWithImages { content, .. } if content == "new image")
            })
            .unwrap();
        assert!(completed < delivered && delivered < image);
        let snapshots = requests.lock().unwrap();
        assert_eq!(snapshots.len(), 1);
        let before = snapshots[0]
            .iter()
            .position(|message| matches!(message, Message::User { content } if content == "one:1"))
            .unwrap();
        let image = snapshots[0].iter().position(|message| matches!(message, Message::UserWithImages { content, .. } if content == "new image")).unwrap();
        assert!(before < image);
    }

    #[tokio::test]
    async fn unfinished_turn_end_runs_before_recovered_tool_turn_advances() {
        let hook = Arc::new(TurnEndRecoveryHook {
            before_first: false,
            calls: AtomicUsize::new(1),
            paused: tokio::sync::Notify::new(),
        });
        let mut registry = Registry::default();
        registry.register(hook.clone()).unwrap();
        let agent = Agent {
            model: &TaskModel,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "test",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        let run_id = Uuid::new_v4();
        let operation_id = Uuid::new_v4();
        session.set_run_id(Some(run_id));
        session
            .append(crate::Event::Run {
                state: crate::RunState::Running,
                initial: Some("prompt".into()),
            })
            .unwrap();
        session
            .append(crate::Event::User {
                content: "prompt".into(),
                input_id: Some(run_id),
            })
            .unwrap();
        session
            .append(crate::Event::Assistant {
                content: String::new(),
                invocations: vec![crate::Invocation {
                    operation_id,
                    call: crate::ToolCall {
                        id: "job-call".into(),
                        name: "job".into(),
                        arguments: "{}".into(),
                    },
                }],
                usage: None,
                request_id: None,
            })
            .unwrap();
        session
            .append(crate::Event::ToolStarted { operation_id })
            .unwrap();
        session
            .append(crate::Event::ToolFinished {
                operation_id,
                outcome: crate::Outcome::Succeeded("job result".into()),
                states: Default::default(),
            })
            .unwrap();
        session.set_run_id(None);
        let (_sender, mut inputs) = mpsc::channel::<crate::AgentInput>(8);
        let answer = agent
            .run_correlated(
                &mut session,
                run_id,
                None,
                &mut inputs,
                &CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        assert_eq!(answer, "done");
        let records = session.records();
        let end = records
            .iter()
            .position(|record| {
                matches!(record.event, crate::Event::TurnEndCallbacksCompleted { .. })
            })
            .unwrap();
        let request = records
            .iter()
            .position(|record| matches!(record.event, crate::Event::ModelRequested { .. }))
            .unwrap();
        assert!(end < request);
        assert_eq!(records.iter().filter(|record| matches!(&record.event,
            crate::Event::CustomMessageQueued { input } if input.message.custom_type == "turn-end-recovery")).count(), 2);
    }

    #[tokio::test]
    async fn legacy_completed_turn_does_not_repeat_turn_end_on_resume() {
        let hook = Arc::new(TurnEndRecoveryHook {
            before_first: false,
            calls: AtomicUsize::new(1),
            paused: tokio::sync::Notify::new(),
        });
        let mut registry = Registry::default();
        registry.register(hook.clone()).unwrap();
        let model = EchoModel(AtomicUsize::new(0));
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "test",
            limits: Limits::default(),
        };
        let mut original = Session::with_store(crate::MemoryStore::default()).unwrap();
        let run_id = Uuid::new_v4();
        original.set_run_id(Some(run_id));
        original
            .append(crate::Event::Run {
                state: crate::RunState::Running,
                initial: Some("prompt".into()),
            })
            .unwrap();
        original
            .append(crate::Event::User {
                content: "prompt".into(),
                input_id: Some(run_id),
            })
            .unwrap();
        original
            .append(crate::Event::Assistant {
                content: "answer".into(),
                invocations: vec![],
                usage: None,
                request_id: None,
            })
            .unwrap();
        let mut legacy = original.records().to_vec();
        for record in &mut legacy {
            record.version = 2;
        }
        let mut restored = Session::from_records(legacy).unwrap();
        let (_sender, mut inputs) = mpsc::channel::<crate::AgentInput>(8);
        let answer = agent
            .run_correlated(
                &mut restored,
                run_id,
                None,
                &mut inputs,
                &CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        assert_eq!(answer, "answer");
        assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
        assert_eq!(model.0.load(Ordering::SeqCst), 0);
        assert!(
            !restored.records().iter().any(|record| matches!(
                record.event,
                crate::Event::TurnEndCallbacksCompleted { .. }
            ))
        );
    }

    struct LegacyDeferredModel;
    #[async_trait::async_trait]
    impl Model for LegacyDeferredModel {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
            let tool = request.messages.iter().position(|message| matches!(message, Message::Tool { content, .. } if content == "job result")).unwrap();
            let deferred = request.messages.iter().position(|message| matches!(message, Message::User { content } if content == "legacy deferred")).unwrap();
            assert!(tool < deferred);
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Text("done".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])))
        }
    }
    #[tokio::test]
    async fn legacy_tool_turn_delivers_deferred_context_before_next_request() {
        let hook = Arc::new(TurnEndRecoveryHook {
            before_first: false,
            calls: AtomicUsize::new(1),
            paused: tokio::sync::Notify::new(),
        });
        let mut registry = Registry::default();
        registry.register(hook.clone()).unwrap();
        let agent = Agent {
            model: &LegacyDeferredModel,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "test",
            limits: Limits::default(),
        };
        let mut original = Session::with_store(crate::MemoryStore::default()).unwrap();
        let run_id = Uuid::new_v4();
        let operation_id = Uuid::new_v4();
        original.set_run_id(Some(run_id));
        original
            .append(crate::Event::Run {
                state: crate::RunState::Running,
                initial: Some("prompt".into()),
            })
            .unwrap();
        original
            .append(crate::Event::User {
                content: "prompt".into(),
                input_id: Some(run_id),
            })
            .unwrap();
        original
            .append(crate::Event::Assistant {
                content: String::new(),
                invocations: vec![crate::Invocation {
                    operation_id,
                    call: crate::ToolCall {
                        id: "job-call".into(),
                        name: "job".into(),
                        arguments: "{}".into(),
                    },
                }],
                usage: None,
                request_id: None,
            })
            .unwrap();
        original
            .append(crate::Event::ToolStarted { operation_id })
            .unwrap();
        original
            .append(crate::Event::ToolFinished {
                operation_id,
                outcome: crate::Outcome::Succeeded("job result".into()),
                states: Default::default(),
            })
            .unwrap();
        original
            .append(crate::Event::CustomMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: Uuid::new_v4(),
                    delivery: crate::CustomMessageDelivery::AfterTurn,
                    message: crate::CustomMessage {
                        custom_type: "legacy".into(),
                        content: crate::ContextContent::Text("legacy deferred".into()),
                        display: false,
                        details: None,
                    },
                },
            })
            .unwrap();
        let mut records = original.records().to_vec();
        for record in &mut records {
            record.version = 2;
        }
        let mut restored = Session::from_records(records).unwrap();
        let (_sender, mut inputs) = mpsc::channel::<crate::AgentInput>(8);
        assert_eq!(
            agent
                .run_correlated(
                    &mut restored,
                    run_id,
                    None,
                    &mut inputs,
                    &CancellationToken::new(),
                    &mut |_| {}
                )
                .await
                .unwrap(),
            "done"
        );
        assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn recovery_applies_deferred_context_after_completed_turn_end_once() {
        let hook = Arc::new(TurnEndRecoveryHook {
            before_first: true,
            calls: AtomicUsize::new(0),
            paused: tokio::sync::Notify::new(),
        });
        let mut registry = Registry::default();
        registry.register(hook.clone()).unwrap();
        let model = EchoModel(AtomicUsize::new(0));
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "test",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        let run_id = Uuid::new_v4();
        session.set_run_id(Some(run_id));
        session
            .append(crate::Event::Run {
                state: crate::RunState::Running,
                initial: Some("prompt".into()),
            })
            .unwrap();
        session
            .append(crate::Event::User {
                content: "prompt".into(),
                input_id: Some(run_id),
            })
            .unwrap();
        let assistant_record = session
            .append(crate::Event::Assistant {
                content: "answer".into(),
                invocations: vec![],
                usage: None,
                request_id: None,
            })
            .unwrap()
            .id;
        let message_id = Uuid::new_v4();
        session
            .append(crate::Event::CustomMessageQueued {
                input: crate::QueuedCustomMessage {
                    id: message_id,
                    delivery: crate::CustomMessageDelivery::AfterTurn,
                    message: crate::CustomMessage {
                        custom_type: "deferred".into(),
                        content: crate::ContextContent::Text("context".into()),
                        display: false,
                        details: None,
                    },
                },
            })
            .unwrap();
        session
            .append(crate::Event::TurnEndCallbacksCompleted { assistant_record })
            .unwrap();
        session.set_run_id(None);
        let (_sender, mut inputs) = mpsc::channel::<crate::AgentInput>(8);
        let answer = agent
            .run_correlated(
                &mut session,
                run_id,
                None,
                &mut inputs,
                &CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        assert_eq!(answer, "answer");
        assert_eq!(hook.calls.load(Ordering::SeqCst), 0);
        assert_eq!(model.0.load(Ordering::SeqCst), 0);
        let restored = Session::from_records(session.records().to_vec()).unwrap();
        assert_eq!(
            restored
                .records()
                .iter()
                .filter(|record| matches!(record.event,
            crate::Event::CustomMessageDelivered { id, .. } if id == message_id))
                .count(),
            1
        );
    }

    struct ScopedKeyHook(&'static str);

    struct RetryOnceModel(AtomicUsize);
    #[async_trait::async_trait]
    impl Model for RetryOnceModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(Error::RetryableModel("retry".into()));
            }
            Ok(Box::pin(futures_util::stream::iter([Ok(
                ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                },
            )])))
        }
    }
    struct PrepareRetryModel(AtomicUsize);
    #[async_trait::async_trait]
    impl Model for PrepareRetryModel {
        async fn prepare_context<'a>(
            &'a self,
            _: &ModelRequest,
        ) -> Result<Option<Arc<dyn Model + 'a>>> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(Error::RetryableModel("prepare again".into()))
            } else {
                Ok(None)
            }
        }
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            Ok(Box::pin(futures_util::stream::iter([Ok(
                ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                },
            )])))
        }
    }
    struct RetryTurnStartHook;
    #[async_trait::async_trait]
    impl Extension for RetryTurnStartHook {
        fn id(&self) -> &str {
            "retry-turn-start"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            if matches!(event, crate::LifecycleEvent::TurnStart { .. }) {
                host.send_message(
                    crate::CustomMessage {
                        custom_type: "retry-turn-start".into(),
                        content: crate::ContextContent::Text("delivered".into()),
                        display: false,
                        details: None,
                    },
                    crate::CustomMessageDelivery::NextTurn,
                )
                .await?;
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn retryable_prepare_context_keeps_turn_start_host() {
        let mut session =
            SessionBuilder::new(Arc::new(PrepareRetryModel(AtomicUsize::new(0))), "fixture")
                .extension(Arc::new(RetryTurnStartHook))
                .unwrap()
                .in_memory()
                .unwrap();
        session
            .controls()
            .set_retry_settings(crate::RetrySettings {
                enabled: true,
                max_retries: 1,
                base_delay_ms: 0,
                max_agent_delay_ms: 0,
            })
            .unwrap();
        session.prompt("prompt").await.unwrap();
        let queued = session.journal().unwrap().queued_custom_messages();
        assert_eq!(
            queued
                .iter()
                .filter(|(_, item)| item.message.custom_type == "retry-turn-start")
                .count(),
            1
        );
    }

    struct RetryRequestHook(AtomicUsize);
    #[async_trait::async_trait]
    impl Extension for RetryRequestHook {
        fn id(&self) -> &str {
            "retry-request-key"
        }
        async fn before_request_with_host(
            &self,
            _: &mut crate::ContextRequest<'_>,
            _: crate::RequestPurpose,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::ContextChange> {
            let attempt = self.0.fetch_add(1, Ordering::SeqCst);
            host.send_message_with_key(
                crate::CustomMessage {
                    custom_type: "retry-key".into(),
                    content: crate::ContextContent::Text(format!("attempt {attempt}")),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::NextTurn,
                "same",
            )
            .await
            .map(|()| crate::ContextChange::Unchanged)
        }
    }
    #[tokio::test]
    async fn request_retry_uses_a_distinct_callback_identity() {
        let mut session =
            SessionBuilder::new(Arc::new(RetryOnceModel(AtomicUsize::new(0))), "fixture")
                .extension(Arc::new(RetryRequestHook(AtomicUsize::new(0))))
                .unwrap()
                .in_memory()
                .unwrap();
        session
            .controls()
            .set_retry_settings(crate::RetrySettings {
                enabled: true,
                max_retries: 1,
                base_delay_ms: 0,
                max_agent_delay_ms: 0,
            })
            .unwrap();
        session.prompt("prompt").await.unwrap();
        let ids: Vec<_> = session
            .journal()
            .unwrap()
            .records()
            .iter()
            .filter_map(|record| match &record.event {
                crate::Event::CustomMessageQueued { input }
                    if input.message.custom_type == "retry-key" =>
                {
                    Some(input.id)
                }
                _ => None,
            })
            .collect();
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
    }

    struct PerTurnKeyHook {
        paused: tokio::sync::Notify,
        second_starts: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl Extension for PerTurnKeyHook {
        fn id(&self) -> &str {
            "per-turn-key"
        }
        fn tools(&self) -> Vec<Arc<dyn crate::Tool>> {
            vec![Arc::new(LifecycleMessageTool)]
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        async fn before_request_with_host(
            &self,
            _: &mut crate::ContextRequest<'_>,
            _: crate::RequestPurpose,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::ContextChange> {
            host.send_message_with_key(
                crate::CustomMessage {
                    custom_type: "turn-request".into(),
                    content: crate::ContextContent::Text("request".into()),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::NextTurn,
                "same",
            )
            .await
            .map(|()| crate::ContextChange::Unchanged)
        }
        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            if let crate::LifecycleEvent::TurnStart { turn_index, .. } = event {
                host.send_message_with_key(
                    crate::CustomMessage {
                        custom_type: "turn-start".into(),
                        content: crate::ContextContent::Text(format!("turn {turn_index}")),
                        display: false,
                        details: None,
                    },
                    crate::CustomMessageDelivery::NextTurn,
                    "same",
                )
                .await?;
                if *turn_index == 1 && self.second_starts.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.paused.notify_one();
                    std::future::pending::<()>().await;
                }
                if *turn_index == 1 {
                    host.send_message(
                        crate::CustomMessage {
                            custom_type: "new-after-recovery".into(),
                            content: crate::ContextContent::Text("new".into()),
                            display: false,
                            details: None,
                        },
                        crate::CustomMessageDelivery::NextTurn,
                    )
                    .await?;
                }
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn per_turn_keys_and_indices_survive_second_turn_restart() {
        let hook = Arc::new(PerTurnKeyHook {
            paused: tokio::sync::Notify::new(),
            second_starts: AtomicUsize::new(0),
        });
        let mut registry = Registry::default();
        registry.register(hook.clone()).unwrap();
        let model = MessageDeliveryModel(Arc::new(Mutex::new(Vec::new())));
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "test",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        let run_id = Uuid::new_v4();
        let cancel = CancellationToken::new();
        let (_sender, mut inputs) = mpsc::channel::<crate::AgentInput>(32);
        let mut observer = |_: Notification<'_>| {};
        let (first, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(
                agent.run_correlated(
                    &mut session,
                    run_id,
                    Some("prompt".into()),
                    &mut inputs,
                    &cancel,
                    &mut observer
                ),
                async {
                    hook.paused.notified().await;
                    cancel.cancel();
                }
            )
        })
        .await
        .unwrap();
        assert!(matches!(first, Err(Error::Cancelled)));
        let mut restored = Session::from_records(session.records().to_vec()).unwrap();
        agent
            .run_correlated(
                &mut restored,
                run_id,
                None,
                &mut inputs,
                &CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap();
        assert_eq!(hook.second_starts.load(Ordering::SeqCst), 2);
        assert_eq!(restored.records().iter().filter(|record| matches!(&record.event,
            crate::Event::CustomMessageQueued { input } if input.message.custom_type == "new-after-recovery")).count(), 1);
        for kind in ["turn-request", "turn-start"] {
            let entries: Vec<_> = restored
                .records()
                .iter()
                .filter_map(|record| match &record.event {
                    crate::Event::CustomMessageQueued { input }
                        if input.message.custom_type == kind =>
                    {
                        Some(input.id)
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(entries.len(), 2, "{kind}");
            assert_ne!(entries[0], entries[1]);
        }
    }

    #[async_trait::async_trait]
    impl Extension for ScopedKeyHook {
        fn id(&self) -> &str {
            self.0
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            if matches!(event, crate::LifecycleEvent::AgentStart { .. }) {
                host.send_message_with_key(
                    crate::CustomMessage {
                        custom_type: self.0.into(),
                        content: crate::ContextContent::Text("start".into()),
                        display: false,
                        details: None,
                    },
                    crate::CustomMessageDelivery::Steer,
                    "same",
                )
                .await?;
            }
            Ok(())
        }
        async fn before_request_with_host(
            &self,
            _: &mut crate::ContextRequest<'_>,
            _: crate::RequestPurpose,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::ContextChange> {
            host.send_message_with_key(
                crate::CustomMessage {
                    custom_type: self.0.into(),
                    content: crate::ContextContent::Text("request".into()),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::NextTurn,
                "same",
            )
            .await
            .map(|()| crate::ContextChange::Unchanged)
        }
    }
    #[tokio::test]
    async fn message_keys_are_scoped_to_extension_and_callback_phase() {
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "fixture")
            .extension(Arc::new(ScopedKeyHook("one")))
            .unwrap()
            .extension(Arc::new(ScopedKeyHook("two")))
            .unwrap()
            .in_memory()
            .unwrap();
        session.prompt("prompt").await.unwrap();
        let queued: Vec<_> = session
            .journal()
            .unwrap()
            .records()
            .iter()
            .filter_map(|record| match &record.event {
                crate::Event::CustomMessageQueued { input } => Some(input.id),
                _ => None,
            })
            .collect();
        assert_eq!(queued.len(), 4);
        assert_eq!(
            queued
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            4
        );
    }

    struct UnkeyedReplayHook {
        phase: &'static str,
        ready: Arc<tokio::sync::Notify>,
        calls: AtomicUsize,
    }
    impl UnkeyedReplayHook {
        async fn send(&self, host: Arc<dyn crate::ToolHost>) -> crate::ExtensionResult<()> {
            let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
            if attempt >= 2 {
                return Ok(());
            }
            host.send_message(
                crate::CustomMessage {
                    custom_type: "unkeyed".into(),
                    content: crate::ContextContent::Text(
                        if attempt == 0 {
                            "first path"
                        } else {
                            "changed path"
                        }
                        .into(),
                    ),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::Steer,
            )
            .await?;
            if attempt == 0 {
                self.ready.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(())
        }
    }
    #[async_trait::async_trait]
    impl Extension for UnkeyedReplayHook {
        fn id(&self) -> &str {
            "unkeyed-replay"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_context_with_system(&self) -> bool {
            self.phase == "context"
        }
        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            if self.phase == "turn_start"
                && matches!(event, crate::LifecycleEvent::TurnStart { .. })
            {
                self.send(host).await?;
            }
            Ok(())
        }
        async fn before_request_with_host(
            &self,
            _: &mut crate::ContextRequest<'_>,
            _: crate::RequestPurpose,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::ContextChange> {
            if self.phase == "before_request" {
                self.send(host).await?;
            }
            Ok(crate::ContextChange::Unchanged)
        }
        async fn context_with_system_with_host(
            &self,
            _: &mut crate::ContextMessages<'_>,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::ContextChange> {
            if self.phase == "context" {
                self.send(host).await?;
            }
            Ok(crate::ContextChange::Unchanged)
        }
    }
    #[tokio::test]
    async fn unkeyed_callback_replay_accepts_changed_path() {
        for phase in ["before_request", "context", "turn_start"] {
            let ready = Arc::new(tokio::sync::Notify::new());
            let hook = Arc::new(UnkeyedReplayHook {
                phase,
                ready: ready.clone(),
                calls: AtomicUsize::new(0),
            });
            let mut registry = Registry::default();
            registry.register(hook.clone()).unwrap();
            let model = EchoModel(AtomicUsize::new(0));
            let agent = Agent {
                model: &model,
                model_name: "fixture",
                registry: &registry,
                system_prompt: "test",
                limits: Limits::default(),
            };
            let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
            let run_id = Uuid::new_v4();
            let cancel = CancellationToken::new();
            let (_sender, mut inputs) = mpsc::channel::<crate::AgentInput>(8);
            let mut observer = |_: Notification<'_>| {};
            let (first, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                tokio::join!(
                    agent.run_correlated(
                        &mut session,
                        run_id,
                        Some("prompt".into()),
                        &mut inputs,
                        &cancel,
                        &mut observer
                    ),
                    async {
                        ready.notified().await;
                        cancel.cancel();
                    }
                )
            })
            .await
            .unwrap();
            assert!(matches!(first, Err(Error::Cancelled)), "{phase}");
            let mut session = Session::from_records(session.records().to_vec()).unwrap();
            let resumed = agent
                .run_correlated(
                    &mut session,
                    run_id,
                    None,
                    &mut inputs,
                    &CancellationToken::new(),
                    &mut |_| {},
                )
                .await;
            assert!(resumed.is_ok(), "{phase}: {resumed:?}");
            assert!(hook.calls.load(Ordering::SeqCst) >= 2, "{phase}");
            assert_eq!(session.records().iter().filter(|r| matches!(&r.event, crate::Event::CustomMessageQueued { input } if input.message.custom_type == "unkeyed")).count(), 2, "{phase}");
            assert!(
                session
                    .records()
                    .iter()
                    .rev()
                    .any(|r| r.run_id == Some(run_id)
                        && matches!(
                            r.event,
                            crate::Event::Run {
                                state: crate::RunState::Completed | crate::RunState::Failed,
                                ..
                            }
                        )),
                "{phase}"
            );
        }
    }

    struct ConflictingKeyHook {
        ready: Arc<tokio::sync::Notify>,
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl Extension for ConflictingKeyHook {
        fn id(&self) -> &str {
            "conflicting-key"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            if matches!(event, crate::LifecycleEvent::AgentStart { .. }) {
                let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
                host.send_message_with_key(
                    crate::CustomMessage {
                        custom_type: "conflict".into(),
                        content: crate::ContextContent::Text(
                            if attempt == 0 { "first" } else { "changed" }.into(),
                        ),
                        display: false,
                        details: None,
                    },
                    crate::CustomMessageDelivery::Steer,
                    "same",
                )
                .await?;
                if attempt == 0 {
                    self.ready.notify_one();
                    std::future::pending::<()>().await;
                }
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn changed_keyed_payload_fails_terminally_without_automatic_recovery_loop() {
        let ready = Arc::new(tokio::sync::Notify::new());
        let hook = Arc::new(ConflictingKeyHook {
            ready: ready.clone(),
            calls: AtomicUsize::new(0),
        });
        let mut registry = Registry::default();
        registry.register(hook.clone()).unwrap();
        let model = EchoModel(AtomicUsize::new(0));
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "test",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        let run_id = Uuid::new_v4();
        crate::agent::queue_custom_message(
            &mut session,
            crate::QueuedCustomMessage {
                id: run_id,
                delivery: crate::CustomMessageDelivery::TriggerTurn,
                message: crate::CustomMessage {
                    custom_type: "trigger".into(),
                    content: crate::ContextContent::Text("go".into()),
                    display: false,
                    details: None,
                },
            },
            &mut |_| {},
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let (_sender, mut inputs) = mpsc::channel::<crate::AgentInput>(8);
        let mut observer = |_: Notification<'_>| {};
        let (first, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(
                agent.run_correlated(
                    &mut session,
                    run_id,
                    None,
                    &mut inputs,
                    &cancel,
                    &mut observer
                ),
                async {
                    ready.notified().await;
                    cancel.cancel();
                }
            )
        })
        .await
        .unwrap();
        assert!(matches!(first, Err(Error::Cancelled)));
        let mut records = session.records().to_vec();
        let index = records.iter().rposition(|r| matches!(&r.event, crate::Event::CustomMessageQueued { input } if input.message.custom_type == "conflict")).unwrap();
        records.truncate(index + 1);
        let mut restored = Session::from_records(records).unwrap();
        let result = agent
            .run_correlated(
                &mut restored,
                run_id,
                None,
                &mut inputs,
                &CancellationToken::new(),
                &mut |_| {},
            )
            .await;
        assert!(
            matches!(result, Err(Error::Config(message)) if message.contains("different content"))
        );
        assert!(restored.pending_custom_trigger().is_none());
        assert!(matches!(
            restored.records().last().unwrap().event,
            crate::Event::Run {
                state: crate::RunState::Failed,
                ..
            }
        ));
        assert_eq!(restored.records().iter().filter(|r| matches!(&r.event, crate::Event::CustomMessageQueued { input } if input.message.custom_type == "conflict")).count(), 1);
    }

    struct TwoRunModel {
        count: AtomicUsize,
        fail_first: bool,
    }
    #[async_trait::async_trait]
    impl Model for TwoRunModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
            let index = self.count.fetch_add(1, Ordering::SeqCst);
            if (index == 0) == self.fail_first {
                return Err(Error::Model(format!("failure {index}")));
            }
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Text(format!("answer {index}"))),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])))
        }
    }
    #[tokio::test]
    async fn triggered_result_does_not_replace_original_and_runs_have_separate_brackets() {
        for fail_first in [false, true] {
            let model = Arc::new(TwoRunModel {
                count: AtomicUsize::new(0),
                fail_first,
            });
            let mut session = SessionBuilder::new(model.clone(), "fixture")
                .extension(Arc::new(TriggerFromLifecycle {
                    settled: true,
                    fired: std::sync::atomic::AtomicBool::new(false),
                }))
                .unwrap()
                .in_memory()
                .unwrap();
            let mut events = session.subscribe();
            let result = session.prompt("user").await;
            if fail_first {
                assert!(
                    matches!(result, Err(Error::Model(message)) if message.contains("failure 0"))
                );
            } else {
                assert_eq!(result.unwrap(), "answer 0");
            }
            assert_eq!(model.count.load(Ordering::SeqCst), 2);
            let mut brackets = Vec::new();
            let mut failures = Vec::new();
            while let Ok(event) = events.try_recv() {
                match event {
                    SessionEvent::RunFailed { run_id, message } => failures.push((run_id, message)),
                    SessionEvent::RunStarted { run_id } => brackets.push((true, run_id, None)),
                    SessionEvent::RunEnded { run_id, succeeded } => {
                        brackets.push((false, run_id, Some(succeeded)))
                    }
                    _ => {}
                }
            }
            assert_eq!(brackets.len(), 4);
            assert!(brackets[0].0 && !brackets[1].0 && brackets[2].0 && !brackets[3].0);
            assert_eq!(brackets[0].1, brackets[1].1);
            assert_eq!(brackets[2].1, brackets[3].1);
            assert_ne!(brackets[0].1, brackets[2].1);
            assert_eq!(brackets[1].2, Some(!fail_first));
            assert_eq!(brackets[3].2, Some(fail_first));
            assert_eq!(failures.len(), 1);
            assert_eq!(
                failures[0].0,
                if fail_first {
                    brackets[0].1
                } else {
                    brackets[2].1
                }
            );
            assert!(
                failures[0]
                    .1
                    .contains(if fail_first { "failure 0" } else { "failure 1" })
            );
        }
    }

    #[tokio::test]
    async fn successful_trigger_does_not_replace_prompt_answer() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model.clone(), "fixture")
            .extension(Arc::new(TriggerFromLifecycle {
                settled: true,
                fired: std::sync::atomic::AtomicBool::new(false),
            }))
            .unwrap()
            .in_memory()
            .unwrap();
        assert_eq!(session.prompt("user").await.unwrap(), "answer 1");
        assert_eq!(model.0.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn next_turn_follows_starting_message_for_user_and_custom_runs() {
        for custom_start in [false, true] {
            let model = Arc::new(EchoModel(AtomicUsize::new(0)));
            let mut session = SessionBuilder::new(model, "fixture").in_memory().unwrap();
            let make = |text: &str| crate::CustomMessage {
                custom_type: "order".into(),
                content: crate::ContextContent::Text(text.into()),
                display: false,
                details: None,
            };
            session
                .queue_custom_message_next_turn(make("older next turn"))
                .unwrap();
            if custom_start {
                session
                    .send_custom_message(make("starting custom"), true)
                    .await
                    .unwrap();
            } else {
                session.prompt("starting user").await.unwrap();
            }
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
            assert_eq!(
                texts,
                [
                    if custom_start {
                        "starting custom"
                    } else {
                        "starting user"
                    },
                    "older next turn"
                ]
            );
        }
    }

    struct MultiTriggerStart;
    #[async_trait::async_trait]
    impl Extension for MultiTriggerStart {
        fn id(&self) -> &str {
            "multi-trigger"
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
            if matches!(event, crate::LifecycleEvent::SessionStart { .. }) {
                for text in ["one", "two"] {
                    host.send_message(
                        crate::CustomMessage {
                            custom_type: "trigger".into(),
                            content: crate::ContextContent::Text(text.into()),
                            display: false,
                            details: None,
                        },
                        crate::CustomMessageDelivery::TriggerTurn,
                    )
                    .await?;
                }
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn multiple_idle_triggers_start_distinct_ordered_runs() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model.clone(), "fixture")
            .extension(Arc::new(MultiTriggerStart))
            .unwrap()
            .in_memory()
            .unwrap();
        session.start().await.unwrap();
        assert_eq!(model.0.load(Ordering::SeqCst), 2);
        let branch: Vec<_> = session
            .journal()
            .unwrap()
            .branch()
            .into_iter()
            .cloned()
            .collect();
        let deliveries: Vec<_> = branch
            .iter()
            .filter_map(|r| match &r.event {
                crate::Event::CustomMessageDelivered { id, message } => {
                    Some((*id, &message.content))
                }
                _ => None,
            })
            .collect();
        assert_eq!(deliveries.len(), 2);
        assert_eq!(deliveries[0].1, &crate::ContextContent::Text("one".into()));
        assert_eq!(deliveries[1].1, &crate::ContextContent::Text("two".into()));
        assert_ne!(deliveries[0].0, deliveries[1].0);
        let first_done = branch
            .iter()
            .position(|r| {
                r.run_id == Some(deliveries[0].0)
                    && matches!(
                        r.event,
                        crate::Event::Run {
                            state: crate::RunState::Completed,
                            ..
                        }
                    )
            })
            .unwrap();
        let second_delivery = branch.iter().position(|r| matches!(r.event, crate::Event::CustomMessageDelivered { id, .. } if id == deliveries[1].0)).unwrap();
        assert!(first_done < second_delivery);
    }

    struct TriggerInputHook;
    #[async_trait::async_trait]
    impl Extension for TriggerInputHook {
        fn id(&self) -> &str {
            "trigger-input"
        }
        fn handles_input(&self) -> bool {
            true
        }
        async fn input_with_host(
            &self,
            _: &crate::InputEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::InputAction> {
            for (text, delivery) in [
                ("next", crate::CustomMessageDelivery::NextTurn),
                ("trigger", crate::CustomMessageDelivery::TriggerTurn),
            ] {
                host.send_message(
                    crate::CustomMessage {
                        custom_type: "input".into(),
                        content: crate::ContextContent::Text(text.into()),
                        display: false,
                        details: None,
                    },
                    delivery,
                )
                .await?;
            }
            Ok(crate::InputAction::Continue)
        }
    }
    #[tokio::test]
    async fn idle_input_trigger_runs_before_prompt_with_pending_next_turn_context() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model.clone(), "fixture")
            .extension(Arc::new(TriggerInputHook))
            .unwrap()
            .in_memory()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), session.prompt("user"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(model.0.load(Ordering::SeqCst), 2);
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
        assert_eq!(texts, ["trigger", "next", "user"]);
    }

    #[tokio::test]
    async fn idle_control_hook_runs_trigger_and_delivers_next_turn() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let lifecycle = Arc::new(Mutex::new(Vec::new()));
        let mut session = SessionBuilder::new(model.clone(), "fixture")
            .extension(Arc::new(TriggerInputHook))
            .unwrap()
            .extension(Arc::new(NameHook(lifecycle.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            session.controls().follow_up("queued"),
        )
        .await
        .expect("idle control deadlocked")
        .unwrap();
        assert!(model.0.load(Ordering::SeqCst) >= 1);
        let journal = session.journal().unwrap();
        let delivered: Vec<_> = journal
            .records()
            .iter()
            .filter_map(|record| match &record.event {
                crate::Event::CustomMessageDelivered { message, .. } => {
                    Some(message.content.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            delivered,
            [
                crate::ContextContent::Text("trigger".into()),
                crate::ContextContent::Text("next".into())
            ]
        );
        assert!(journal.pending_custom_trigger().is_none());
        drop(journal);
        session.start().await.unwrap();
        assert_eq!(
            lifecycle
                .lock()
                .unwrap()
                .iter()
                .filter(|event| matches!(event, crate::LifecycleEvent::SessionStart { .. }))
                .count(),
            1
        );
    }

    struct PausedIdleControlHook {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }
    #[async_trait::async_trait]
    impl Extension for PausedIdleControlHook {
        fn id(&self) -> &str {
            "paused-idle-control"
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
            if event.text == "control" {
                self.entered.notify_one();
                self.release.notified().await;
                host.send_message(
                    crate::CustomMessage {
                        custom_type: "paused".into(),
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
    async fn idle_control_and_prompt_serialize_without_deadlock() {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "fixture")
            .extension(Arc::new(PausedIdleControlHook {
                entered: entered.clone(),
                release: release.clone(),
            }))
            .unwrap()
            .in_memory()
            .unwrap();
        let control = session.controls();
        let (queued, prompted) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(control.follow_up("control"), async {
                entered.notified().await;
                release.notify_one();
                session.prompt("prompt").await
            })
        })
        .await
        .expect("concurrent control and prompt deadlocked");
        queued.unwrap();
        prompted.unwrap();
        assert!(session.journal().unwrap().records().iter().any(|record| matches!(&record.event,
            crate::Event::CustomMessageDelivered { message, .. } if message.custom_type == "paused")));
    }

    struct ReentrantTriggeredRun(Arc<Mutex<Option<SessionControl>>>);
    #[async_trait::async_trait]
    impl Extension for ReentrantTriggeredRun {
        fn id(&self) -> &str {
            "reentrant-triggered-run"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            _: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            if matches!(event, crate::LifecycleEvent::AgentStart { .. }) {
                let control = self.0.lock().unwrap().clone().unwrap();
                control
                    .steer("nested")
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn idle_trigger_run_allows_reentrant_control_input() {
        let slot = Arc::new(Mutex::new(None));
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "fixture")
            .extension(Arc::new(TriggerInputHook))
            .unwrap()
            .extension(Arc::new(ReentrantTriggeredRun(slot.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        let control = session.controls();
        *slot.lock().unwrap() = Some(control.clone());
        session.start().await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            control.follow_up("queued"),
        )
        .await
        .expect("triggered callback deadlocked")
        .unwrap();
        assert!(
            session
                .journal()
                .unwrap()
                .records()
                .iter()
                .any(|record| matches!(&record.event,
            crate::Event::InputQueued { input } if input.content == "nested"))
        );
    }

    struct ReentrantTurnStart {
        control: Arc<Mutex<Option<SessionControl>>>,
        sent: std::sync::atomic::AtomicBool,
    }
    #[async_trait::async_trait]
    impl Extension for ReentrantTurnStart {
        fn id(&self) -> &str {
            "reentrant-turn-start"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            _: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            if matches!(event, crate::LifecycleEvent::TurnStart { .. })
                && !self.sent.swap(true, Ordering::SeqCst)
            {
                let control = self.control.lock().unwrap().clone().unwrap();
                control
                    .steer("turn nested")
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn idle_trigger_turn_start_allows_reentrant_control() {
        let slot = Arc::new(Mutex::new(None));
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "fixture")
            .extension(Arc::new(TriggerInputHook))
            .unwrap()
            .extension(Arc::new(ReentrantTurnStart {
                control: slot.clone(),
                sent: std::sync::atomic::AtomicBool::new(false),
            }))
            .unwrap()
            .in_memory()
            .unwrap();
        let control = session.controls();
        *slot.lock().unwrap() = Some(control.clone());
        session.start().await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            control.follow_up("queued"),
        )
        .await
        .expect("turn start callback deadlocked")
        .unwrap();
        assert!(
            session
                .journal()
                .unwrap()
                .records()
                .iter()
                .any(|record| matches!(&record.event,
            crate::Event::InputQueued { input } if input.content == "turn nested"))
        );
    }

    #[tokio::test]
    async fn idle_control_ack_survives_abort_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control-input.jsonl");
        let notified = Arc::new(tokio::sync::Notify::new());
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
            .extension(Arc::new(PendingIdleInputHook(notified.clone())))
            .unwrap()
            .open(&path)
            .unwrap();
        let control = session.controls();
        let sender = control.clone();
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(sender.steer("input"), async {
                notified.notified().await;
                control.abort();
            })
        })
        .await
        .expect("idle cancellation deadlocked");
        assert!(matches!(result, Err(Error::Cancelled)));
        session.close().await.unwrap();
        drop(session);
        let restored = Session::open(&path).unwrap();
        assert_eq!(restored.queued_custom_messages().len(), 1);
        assert_eq!(
            restored.queued_custom_messages()[0].1.delivery,
            crate::CustomMessageDelivery::TriggerTurn
        );
        drop(restored);
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut reopened = SessionBuilder::new(model.clone(), "fixture")
            .open(&path)
            .unwrap();
        reopened.start().await.unwrap();
        assert_eq!(model.0.load(Ordering::SeqCst), 1);
        let journal = reopened.journal().unwrap();
        assert_eq!(journal.records().iter().filter(|record| matches!(&record.event,
            crate::Event::CustomMessageDelivered { message, .. } if message.custom_type == "idle-input")).count(), 1);
    }

    #[tokio::test]
    async fn closing_session_cancels_idle_control_hook() {
        let notified = Arc::new(tokio::sync::Notify::new());
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
            .extension(Arc::new(PendingIdleInputHook(notified.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        let control = session.controls();
        let (result, closed) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(control.steer("input"), async {
                notified.notified().await;
                session.close().await
            })
        })
        .await
        .expect("session close deadlocked");
        assert!(result.is_err());
        closed.unwrap();
    }

    struct ScopedIdleControlHook {
        control: Arc<Mutex<Option<SessionControl>>>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        events: Arc<Mutex<Vec<String>>>,
    }
    #[async_trait::async_trait]
    impl Extension for ScopedIdleControlHook {
        fn id(&self) -> &str {
            "scoped-idle-control"
        }
        fn handles_input(&self) -> bool {
            true
        }
        async fn input_with_host(
            &self,
            event: &crate::InputEvent,
            _: Option<&crate::ExtensionState>,
            _: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::InputAction> {
            self.events.lock().unwrap().push(event.text.clone());
            if event.text == "parent" {
                self.entered.notify_one();
                self.release.notified().await;
                let control = self.control.lock().unwrap().clone().unwrap();
                control
                    .steer("child")
                    .await
                    .map_err(|error| error.to_string())?;
                control
                    .set_queue_mode(InputKind::Steer, crate::QueueMode::All)
                    .await
                    .map_err(|error| error.to_string())?;
                self.events.lock().unwrap().push("parent-finished".into());
            }
            Ok(crate::InputAction::Continue)
        }
    }
    #[tokio::test]
    async fn idle_scoped_child_input_and_control_precede_waiting_external_ticket() {
        let slot = Arc::new(Mutex::new(None));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "fixture")
            .extension(Arc::new(ScopedIdleControlHook {
                control: slot.clone(),
                entered: entered.clone(),
                release: release.clone(),
                events: events.clone(),
            }))
            .unwrap()
            .in_memory()
            .unwrap();
        let control = session.controls();
        *slot.lock().unwrap() = Some(control.clone());
        let parent = control.clone();
        let external = control.clone();
        let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(parent.steer("parent"), async {
                entered.notified().await;
                let task = tokio::spawn(async move { external.follow_up("external").await });
                loop {
                    if control.input_order.state.lock().unwrap().next >= 2 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                release.notify_one();
                task.await.unwrap()
            })
        })
        .await
        .expect("scoped idle control deadlocked");
        first.unwrap();
        second.unwrap();
        assert_eq!(
            &*events.lock().unwrap(),
            &["parent", "child", "parent-finished", "external"]
        );
        let queued = session.clear_queued_inputs().unwrap();
        assert_eq!(
            queued
                .iter()
                .map(|message| message.input.content.as_str())
                .collect::<Vec<_>>(),
            ["child", "parent", "external"]
        );
    }

    struct CancelScopedIdleHook {
        control: Arc<Mutex<Option<SessionControl>>>,
        ready: Arc<tokio::sync::Notify>,
        after_commit: bool,
    }
    #[async_trait::async_trait]
    impl Extension for CancelScopedIdleHook {
        fn id(&self) -> &str {
            "cancel-scoped-idle"
        }
        fn handles_input(&self) -> bool {
            true
        }
        async fn input(
            &self,
            event: &crate::InputEvent,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<crate::InputAction> {
            if event.text == "parent" {
                if self.after_commit {
                    let control = self.control.lock().unwrap().clone().unwrap();
                    control
                        .steer("child")
                        .await
                        .map_err(|error| error.to_string())?;
                }
                self.ready.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(crate::InputAction::Continue)
        }
    }
    #[tokio::test]
    async fn scoped_child_cancellation_respects_journal_commit_boundary() {
        for after_commit in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("scoped-cancel.jsonl");
            let slot = Arc::new(Mutex::new(None));
            let ready = Arc::new(tokio::sync::Notify::new());
            let mut session = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
                .extension(Arc::new(CancelScopedIdleHook {
                    control: slot.clone(),
                    ready: ready.clone(),
                    after_commit,
                }))
                .unwrap()
                .open(&path)
                .unwrap();
            let control = session.controls();
            *slot.lock().unwrap() = Some(control.clone());
            let parent = control.clone();
            let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                tokio::join!(parent.steer("parent"), async {
                    ready.notified().await;
                    control.abort();
                })
            })
            .await
            .expect("scoped cancellation deadlocked");
            assert!(matches!(result, Err(Error::Cancelled)));
            session.close().await.unwrap();
            drop(session);
            let restored = Session::open(&path).unwrap();
            assert_eq!(
                restored
                    .queued_inputs()
                    .iter()
                    .filter(|input| input.input.content == "child")
                    .count(),
                usize::from(after_commit)
            );
        }
    }

    struct SpawnScopedHook {
        control: Arc<Mutex<Option<SessionControl>>>,
        saved: Arc<Mutex<Option<SessionControl>>>,
        startup: bool,
    }
    #[async_trait::async_trait]
    impl Extension for SpawnScopedHook {
        fn id(&self) -> &str {
            "spawn-scoped-hook"
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
        async fn input(
            &self,
            event: &crate::InputEvent,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<crate::InputAction> {
            if event.text == "parent" && !self.startup {
                let control = self
                    .control
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap()
                    .scoped()
                    .map_err(|error| error.to_string())?;
                *self.saved.lock().unwrap() = Some(control.clone());
                tokio::spawn(async move {
                    control.set_session_name("scoped").await?;
                    control.steer("spawned child").await
                })
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            }
            Ok(crate::InputAction::Continue)
        }
        async fn lifecycle(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<()> {
            if self.startup && matches!(event, crate::LifecycleEvent::SessionStart { .. }) {
                let control = self
                    .control
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap()
                    .scoped()
                    .map_err(|error| error.to_string())?;
                *self.saved.lock().unwrap() = Some(control.clone());
                tokio::spawn(async move {
                    control.follow_up("startup child").await?;
                    control
                        .set_queue_mode(InputKind::Steer, crate::QueueMode::All)
                        .await
                })
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn scoped_control_can_move_into_spawned_input_callback_task() {
        let slot = Arc::new(Mutex::new(None));
        let saved = Arc::new(Mutex::new(None));
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
            .extension(Arc::new(SpawnScopedHook {
                control: slot.clone(),
                saved: saved.clone(),
                startup: false,
            }))
            .unwrap()
            .in_memory()
            .unwrap();
        let control = session.controls();
        *slot.lock().unwrap() = Some(control.clone());
        tokio::time::timeout(std::time::Duration::from_secs(3), control.steer("parent"))
            .await
            .expect("spawned child deadlocked")
            .unwrap();
        assert_eq!(session.journal().unwrap().name(), Some("scoped"));
        let queued = session.clear_queued_inputs().unwrap();
        assert_eq!(
            queued
                .iter()
                .map(|input| input.input.content.as_str())
                .collect::<Vec<_>>(),
            ["spawned child", "parent"]
        );
        let stale = saved.lock().unwrap().clone().unwrap();
        assert!(stale.steer("late").await.is_err());
    }
    #[tokio::test]
    async fn first_session_start_services_spawned_scoped_controls() {
        let slot = Arc::new(Mutex::new(None));
        let saved = Arc::new(Mutex::new(None));
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
            .extension(Arc::new(SpawnScopedHook {
                control: slot.clone(),
                saved: saved.clone(),
                startup: true,
            }))
            .unwrap()
            .in_memory()
            .unwrap();
        let control = session.controls();
        *slot.lock().unwrap() = Some(control.clone());
        tokio::time::timeout(std::time::Duration::from_secs(3), control.steer("parent"))
            .await
            .expect("startup child deadlocked")
            .unwrap();
        let queued = session.clear_queued_inputs().unwrap();
        assert_eq!(
            queued
                .iter()
                .map(|input| input.input.content.as_str())
                .collect::<Vec<_>>(),
            ["startup child", "parent"]
        );
        let stale = saved.lock().unwrap().clone().unwrap();
        assert!(stale.follow_up("late").await.is_err());
    }

    #[tokio::test]
    async fn explicit_session_start_services_spawned_scoped_controls() {
        let slot = Arc::new(Mutex::new(None));
        let saved = Arc::new(Mutex::new(None));
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
            .extension(Arc::new(SpawnScopedHook {
                control: slot.clone(),
                saved,
                startup: true,
            }))
            .unwrap()
            .in_memory()
            .unwrap();
        *slot.lock().unwrap() = Some(session.controls());
        tokio::time::timeout(std::time::Duration::from_secs(3), session.start())
            .await
            .expect("explicit startup deadlocked")
            .unwrap();
        let queued = session.clear_queued_inputs().unwrap();
        assert_eq!(
            queued
                .iter()
                .map(|input| input.input.content.as_str())
                .collect::<Vec<_>>(),
            ["startup child"]
        );
    }

    struct CallbackExpiryHook {
        id: &'static str,
        control: Arc<Mutex<Option<SessionControl>>>,
        saved: Arc<Mutex<Option<SessionControl>>>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        startup: bool,
        capture: bool,
    }
    impl CallbackExpiryHook {
        async fn check(&self) -> crate::ExtensionResult<()> {
            let control = self.control.lock().unwrap().clone().unwrap();
            if self.capture {
                *self.saved.lock().unwrap() =
                    Some(control.scoped().map_err(|error| error.to_string())?);
            } else {
                let stale = self.saved.lock().unwrap().clone().unwrap();
                assert!(stale.scoped().is_err());
                assert!(stale.steer("late").await.is_err());
                assert!(
                    stale
                        .set_retry_settings(crate::RetrySettings::pi_defaults())
                        .is_err()
                );
                assert!(stale.abort_retry().is_err());
                assert!(stale.set_auto_compaction_enabled(false).is_err());
                assert!(
                    stale
                        .set_compaction_tokens(crate::CompactionTokens::pi_defaults())
                        .is_err()
                );
                stale.abort();
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(())
        }
    }
    #[async_trait::async_trait]
    impl Extension for CallbackExpiryHook {
        fn id(&self) -> &str {
            self.id
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
        async fn input(
            &self,
            _: &crate::InputEvent,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<crate::InputAction> {
            if !self.startup {
                self.check().await?;
            }
            Ok(crate::InputAction::Continue)
        }
        async fn lifecycle(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<()> {
            if self.startup && matches!(event, crate::LifecycleEvent::SessionStart { .. }) {
                self.check().await?;
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn scoped_handles_expire_between_input_and_startup_extension_callbacks() {
        for startup in [false, true] {
            let control_slot = Arc::new(Mutex::new(None));
            let saved = Arc::new(Mutex::new(None));
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let make_hook = |id, capture| {
                Arc::new(CallbackExpiryHook {
                    id,
                    capture,
                    startup,
                    control: control_slot.clone(),
                    saved: saved.clone(),
                    entered: entered.clone(),
                    release: release.clone(),
                })
            };
            let session = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
                .extension(make_hook("expiry-a", true))
                .unwrap()
                .extension(make_hook("expiry-b", false))
                .unwrap()
                .in_memory()
                .unwrap();
            let control = session.controls();
            *control_slot.lock().unwrap() = Some(control.clone());
            let driver = control.clone();
            let baseline_retry = control.retry.settings();
            let baseline_tokens = control.auto_compaction.token_settings();
            let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                tokio::join!(driver.steer("parent"), async {
                    entered.notified().await;
                    assert!(control.auto_compaction.enabled());
                    assert_eq!(control.auto_compaction.token_settings(), baseline_tokens);
                    assert_eq!(control.retry.settings().enabled, baseline_retry.enabled);
                    assert_eq!(
                        control.retry.settings().max_retries,
                        baseline_retry.max_retries
                    );
                    assert!(
                        control
                            .active
                            .lock()
                            .unwrap()
                            .as_ref()
                            .is_some_and(|token| !token.is_cancelled())
                    );
                    release.notify_one();
                })
            })
            .await
            .expect("callback expiry test deadlocked");
            result.unwrap();
            assert!(
                session
                    .journal()
                    .unwrap()
                    .queued_inputs()
                    .iter()
                    .all(|input| input.input.content != "late")
            );
        }
    }

    struct RetainedHostHook {
        id: &'static str,
        saved: Arc<Mutex<Option<Arc<dyn crate::ToolHost>>>>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        startup: bool,
        capture: bool,
    }
    impl RetainedHostHook {
        async fn check(&self, host: Arc<dyn crate::ToolHost>) -> crate::ExtensionResult<()> {
            if self.capture {
                *self.saved.lock().unwrap() = Some(host.clone());
            }
            host.send_message(
                crate::CustomMessage {
                    custom_type: self.id.into(),
                    content: crate::ContextContent::Text("allowed".into()),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::Steer,
            )
            .await?;
            if !self.capture {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(())
        }
    }
    #[async_trait::async_trait]
    impl Extension for RetainedHostHook {
        fn id(&self) -> &str {
            self.id
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
        async fn input_with_host(
            &self,
            _: &crate::InputEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::InputAction> {
            if !self.startup {
                self.check(host).await?;
            }
            Ok(crate::InputAction::Continue)
        }
        async fn lifecycle_with_host(
            &self,
            event: &crate::LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            if self.startup && matches!(event, crate::LifecycleEvent::SessionStart { .. }) {
                self.check(host).await?;
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn retained_host_expires_between_input_and_startup_callbacks() {
        for startup in [false, true] {
            let saved = Arc::new(Mutex::new(None));
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let make_hook = |id, capture| {
                Arc::new(RetainedHostHook {
                    id,
                    capture,
                    startup,
                    saved: saved.clone(),
                    entered: entered.clone(),
                    release: release.clone(),
                })
            };
            let session = SessionBuilder::new(Arc::new(WaitingModel), "fixture")
                .extension(make_hook("host-a", true))
                .unwrap()
                .extension(make_hook("host-b", false))
                .unwrap()
                .in_memory()
                .unwrap();
            let control = session.controls();
            let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                tokio::join!(control.steer("parent"), async {
                    entered.notified().await;
                    let stale = saved.lock().unwrap().clone().unwrap();
                    let late = tokio::spawn(async move {
                        stale
                            .send_message(
                                crate::CustomMessage {
                                    custom_type: "late-host".into(),
                                    content: crate::ContextContent::Text("late".into()),
                                    display: false,
                                    details: None,
                                },
                                crate::CustomMessageDelivery::Steer,
                            )
                            .await
                    })
                    .await
                    .unwrap();
                    assert!(late.is_err());
                    release.notify_one();
                })
            })
            .await
            .expect("retained host test deadlocked");
            result.unwrap();
            let journal = session.journal().unwrap();
            let kinds: Vec<_> = journal
                .records()
                .iter()
                .filter_map(|record| match &record.event {
                    crate::Event::CustomMessage { message } => Some(message.custom_type.as_str()),
                    _ => None,
                })
                .collect();
            assert_eq!(kinds, ["host-a", "host-b"]);
        }
    }

    struct NestedSessionHook(Arc<tokio::sync::Mutex<AgentSession>>);
    #[async_trait::async_trait]
    impl Extension for NestedSessionHook {
        fn id(&self) -> &str {
            "nested-session-hook"
        }
        fn handles_input(&self) -> bool {
            true
        }
        async fn input(
            &self,
            event: &crate::InputEvent,
            _: Option<&crate::ExtensionState>,
        ) -> crate::ExtensionResult<crate::InputAction> {
            if event.text == "outer" {
                self.0
                    .lock()
                    .await
                    .prompt("inner")
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Ok(crate::InputAction::Continue)
        }
    }
    #[tokio::test]
    async fn nested_session_prompt_host_messages_stay_with_their_owner() {
        let other = Arc::new(tokio::sync::Mutex::new(
            SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "session-b")
                .extension(Arc::new(InputMessageHook))
                .unwrap()
                .in_memory()
                .unwrap(),
        ));
        let session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "session-a")
            .extension(Arc::new(NestedSessionHook(other.clone())))
            .unwrap()
            .in_memory()
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            session.controls().steer("outer"),
        )
        .await
        .expect("nested session deadlocked")
        .unwrap();
        assert!(
            !session
                .journal()
                .unwrap()
                .records()
                .iter()
                .any(|record| matches!(&record.event,
            crate::Event::CustomMessage { message } if message.custom_type == "input"))
        );
        let other = other.lock().await;
        assert!(other.journal().unwrap().records().iter().any(|record| matches!(&record.event,
            crate::Event::CustomMessage { message } if message.custom_type == "input" && message.content == crate::ContextContent::Text("idle hook".into()))));
    }
    #[tokio::test]
    async fn initial_prompt_retained_host_expires_between_input_callbacks() {
        let saved = Arc::new(Mutex::new(None));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let make_hook = |id, capture| {
            Arc::new(RetainedHostHook {
                id,
                capture,
                startup: false,
                saved: saved.clone(),
                entered: entered.clone(),
                release: release.clone(),
            })
        };
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "fixture")
            .extension(make_hook("host-a", true))
            .unwrap()
            .extension(make_hook("host-b", false))
            .unwrap()
            .in_memory()
            .unwrap();
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(session.prompt("parent"), async {
                entered.notified().await;
                let stale = saved.lock().unwrap().clone().unwrap();
                let late = tokio::spawn(async move {
                    stale
                        .send_message(
                            crate::CustomMessage {
                                custom_type: "late-host".into(),
                                content: crate::ContextContent::Text("late".into()),
                                display: false,
                                details: None,
                            },
                            crate::CustomMessageDelivery::Steer,
                        )
                        .await
                })
                .await
                .unwrap();
                assert!(late.is_err());
                release.notify_one();
            })
        })
        .await
        .expect("initial prompt host test deadlocked");
        result.unwrap();
        let journal = session.journal().unwrap();
        let kinds: Vec<_> = journal
            .records()
            .iter()
            .filter_map(|record| match &record.event {
                crate::Event::CustomMessage { message } => Some(message.custom_type.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(kinds, ["host-a", "host-b"]);
    }

    struct PendingIdleInputHook(Arc<tokio::sync::Notify>);
    #[async_trait::async_trait]
    impl Extension for PendingIdleInputHook {
        fn id(&self) -> &str {
            "pending-idle-input"
        }
        fn handles_input(&self) -> bool {
            true
        }
        async fn input_with_host(
            &self,
            _: &crate::InputEvent,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<crate::InputAction> {
            host.send_message(
                crate::CustomMessage {
                    custom_type: "idle-input".into(),
                    content: crate::ContextContent::Text("durable before cancellation".into()),
                    display: false,
                    details: None,
                },
                crate::CustomMessageDelivery::TriggerTurn,
            )
            .await?;
            self.0.notify_one();
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn idle_input_host_ack_survives_cancellation_and_reopen() {
        let notified = Arc::new(tokio::sync::Notify::new());
        let mut registry = Registry::default();
        registry
            .register(Arc::new(PendingIdleInputHook(notified.clone())))
            .unwrap();
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let cancellation = CancellationToken::new();
        let cancel = async {
            notified.notified().await;
            cancellation.cancel();
        };
        let event = crate::InputEvent {
            text: "input".into(),
            images: vec![],
            source: crate::InputSource::Extension,
            streaming_behavior: None,
        };
        let result = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let mut observer = |_: Notification<'_>| {};
            tokio::join!(
                prepare_idle_input_with_host(
                    &registry,
                    &mut session,
                    event,
                    &cancellation,
                    &mut observer,
                    None,
                ),
                cancel
            )
            .0
        })
        .await
        .unwrap();
        assert!(matches!(result, Err(Error::Cancelled)));
        let reopened = Session::from_records(session.records().to_vec()).unwrap();
        let queued = reopened.queued_custom_messages();
        assert_eq!(queued.len(), 1);
        assert_eq!(
            queued[0].1.delivery,
            crate::CustomMessageDelivery::TriggerTurn
        );
        assert_eq!(
            queued[0].1.message.content,
            crate::ContextContent::Text("durable before cancellation".into())
        );
        assert!(reopened.pending_custom_trigger().is_some());
    }

    struct TransitionMessageHook(crate::CustomMessageDelivery);
    #[async_trait::async_trait]
    impl Extension for TransitionMessageHook {
        fn id(&self) -> &str {
            "transition-message"
        }
        fn handles_session_transitions(&self) -> bool {
            true
        }
        async fn before_session_transition_with_host(
            &self,
            _: &crate::BeforeSessionTransition,
            _: Option<&crate::ExtensionState>,
            host: Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<bool> {
            host.send_message(
                crate::CustomMessage {
                    custom_type: "transition".into(),
                    content: crate::ContextContent::Text("after cancelled fork".into()),
                    display: false,
                    details: None,
                },
                self.0,
            )
            .await?;
            Ok(true)
        }
    }
    #[tokio::test]
    async fn cancelled_fork_callback_journals_its_message_without_deadlock() {
        let mut session = SessionBuilder::new(Arc::new(EchoModel(AtomicUsize::new(0))), "fixture")
            .extension(Arc::new(TransitionMessageHook(
                crate::CustomMessageDelivery::NextTurn,
            )))
            .unwrap()
            .in_memory()
            .unwrap();
        session.prompt("first").await.unwrap();
        let target = session.journal().unwrap().head();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            session.fork_with_hooks(target, crate::ForkPosition::At, None),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(Error::Cancelled)));
        let restored =
            Session::from_records(session.journal().unwrap().records().to_vec()).unwrap();
        assert_eq!(restored.queued_custom_messages().len(), 1);
        assert_eq!(
            restored.queued_custom_messages()[0].1.message.content,
            crate::ContextContent::Text("after cancelled fork".into())
        );
    }

    #[tokio::test]
    async fn cancelled_fork_trigger_runs_once_before_returning() {
        let model = Arc::new(EchoModel(AtomicUsize::new(0)));
        let mut session = SessionBuilder::new(model.clone(), "fixture")
            .extension(Arc::new(TransitionMessageHook(
                crate::CustomMessageDelivery::TriggerTurn,
            )))
            .unwrap()
            .in_memory()
            .unwrap();
        session.prompt("first").await.unwrap();
        let target = session.journal().unwrap().head();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            session.fork_with_hooks(target, crate::ForkPosition::At, None),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(Error::Cancelled)));
        assert_eq!(model.0.load(Ordering::SeqCst), 2);
        assert!(
            session
                .journal()
                .unwrap()
                .pending_custom_trigger()
                .is_none()
        );
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
            .unwrap()
            .records()
            .iter()
            .filter(|record| matches!(record.event, crate::Event::ToolPlanned { .. }))
            .count();
        assert_eq!(plans, 3);
        assert!(
            session
                .journal()
                .unwrap()
                .operations()
                .iter()
                .any(|(invocation, _)| invocation.call.name == "child"
                    && invocation.call.arguments.contains("wrapped"))
        );
        assert_eq!(session.journal().unwrap().operations().len(), 4);
        assert!(
            session
                .journal()
                .unwrap()
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
                .unwrap()
                .records()
                .iter()
                .any(|record| matches!(record.event, crate::Event::ToolStarted { .. }))
        );
        assert!(session.journal().unwrap().operations().iter().any(|(_, state)| matches!(state, crate::OperationState::Finished(crate::Outcome::Skipped(reason)) if reason.contains("preparation timed out"))));
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
        assert_eq!(session.journal().unwrap().operations().len(), 3);
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
        assert_eq!(session.journal().unwrap().operations().len(), 2);
        assert!(
            session
                .journal()
                .unwrap()
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
            session.journal().unwrap().extension_states()["stateful"].data["secret"],
            "private state"
        );
        assert!(
            !serde_json::to_string(&session.messages().unwrap())
                .unwrap()
                .contains("private state")
        );
        assert!(session.journal().unwrap().records().iter().any(|record| matches!(&record.event, crate::Event::ToolFinished { states, outcome: crate::Outcome::Succeeded(_), .. } if states.contains_key("stateful"))));
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
        assert_eq!(session.pending_inputs().unwrap(), 0);
        assert_eq!(
            session.journal().unwrap().queue_mode(InputKind::FollowUp),
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
        assert_eq!(session.pending_inputs().unwrap(), 0);
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
        assert!(!session.journal().unwrap().records().iter().any(|record| matches!(&record.event, crate::Event::User { content, .. } if content.contains("deployment decision"))));
        assert!(
            matches!(&session.journal().unwrap().records().last().unwrap().event, crate::Event::Compacted { summary, .. } if summary == "answer 4")
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
    struct RenameMessageHook;
    #[async_trait::async_trait]
    impl Extension for RenameMessageHook {
        fn id(&self) -> &str {
            "rename-message"
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
            if matches!(event, crate::LifecycleEvent::SessionInfoChanged { .. }) {
                host.send_message(
                    crate::CustomMessage {
                        custom_type: "rename".into(),
                        content: crate::ContextContent::Text("renamed".into()),
                        display: false,
                        details: None,
                    },
                    crate::CustomMessageDelivery::NextTurn,
                )
                .await?;
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn active_rename_callback_can_send_custom_message() {
        let mut session = SessionBuilder::new(Arc::new(WaitingModel), "wait")
            .extension(Arc::new(RenameMessageHook))
            .unwrap()
            .in_memory()
            .unwrap();
        let control = session.controls();
        let mut events = session.subscribe();
        let queue = async move {
            while !matches!(events.recv().await.unwrap(), SessionEvent::Request(_)) {}
            control.set_session_name("new name").await.unwrap();
            control.abort();
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(session.prompt("wait"), queue)
        })
        .await
        .unwrap();
        assert!(matches!(result, Err(Error::Cancelled)));
        assert_eq!(session.journal().unwrap().queued_custom_messages().len(), 1);
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
        let restored =
            Session::from_records(session.journal().unwrap().records().to_vec()).unwrap();
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
        assert_eq!(session.journal().unwrap().name(), Some("In progress"));
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
                .unwrap()
                .records()
                .iter()
                .filter(|record| matches!(record.event, crate::Event::Assistant { .. }))
                .count(),
            2
        );
        assert_eq!(
            session
                .journal()
                .unwrap()
                .records()
                .iter()
                .filter(|record| matches!(record.event, crate::Event::UserWithImages { .. }))
                .count(),
            2
        );
        let head = session.journal().unwrap().head();
        let mut fork = session.fork_at(head).unwrap();
        assert!(fork.messages().unwrap().iter().any(
            |message| matches!(message, Message::UserWithImages { images, .. } if images.len() == 1)
        ));
        let count = session.journal().unwrap().records().len();
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
        assert_eq!(session.journal().unwrap().records().len(), count);
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
        reopened
            .set_model(Arc::new(EchoModel(AtomicUsize::new(0))), "echo")
            .unwrap();
        assert_eq!(reopened.resume().await.unwrap(), "answer 2");
        assert_eq!(reopened.resume().await.unwrap(), "answer 2");
        assert_eq!(reopened.journal().unwrap().records().iter().filter(|record| matches!(&record.event, crate::Event::InputQueuedWithImages { input } if input.input.id == input_id)).count(), 1);
        assert_eq!(reopened.journal().unwrap().records().iter().filter(|record| matches!(&record.event, crate::Event::UserWithImages { input_id: Some(id), .. } if *id == input_id)).count(), 1);
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
