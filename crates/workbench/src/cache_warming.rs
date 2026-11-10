//! Best-effort refreshes of the conversation’s current prompt cache entry.
use std::future::{Future, poll_fn};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use futures_util::{StreamExt, task::AtomicWaker};
use pi_rs::{
    AgentInput, CacheRetention, CacheWarmingAction, CacheWarmingDecision, CacheWarmingPhase, Event,
    Model, ModelEvent, ModelMetadata, ModelRequest, Notification, Registry, ResolvedModelIdentity,
    Session, Usage, UsageReport,
};
use tokio::time::{Instant, sleep_until, timeout, timeout_at};
use uuid::Uuid;

use super::{
    Cli, image_policy::WorkbenchModel, model_profiles::ChatPrice, providers::Adapter, runtime,
};

const MAX_IDLE_AGE: Duration = Duration::from_secs(30 * 60);
const MAX_STREAMING_AGE: Duration = Duration::from_secs(60 * 60);
const MINIMUM_SAVINGS_USD: f64 = 0.05;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Off,
    Streaming,
    Idle,
}

impl Mode {
    fn from_settings(settings: &super::config::Settings) -> Self {
        match settings.string("cacheWarming") {
            Some("off") => Self::Off,
            Some("idle") => Self::Idle,
            _ => Self::Streaming,
        }
    }

    fn load(workspace: &Path) -> Option<Self> {
        super::config::Settings::load(workspace, false)
            .ok()
            .map(|settings| Self::from_settings(&settings))
    }
}

#[derive(Clone, Copy)]
enum Scope {
    Streaming(Uuid),
    Idle(u64),
}

impl Scope {
    fn max_age(self) -> Duration {
        match self {
            Self::Streaming(_) => MAX_STREAMING_AGE,
            Self::Idle(_) => MAX_IDLE_AGE,
        }
    }

    fn probability(self) -> f64 {
        match self {
            Self::Streaming(_) => 1.0,
            Self::Idle(_) => 0.15,
        }
    }

    fn allowed(self, mode: Mode) -> bool {
        match self {
            Self::Streaming(_) => mode != Mode::Off,
            Self::Idle(_) => mode == Mode::Idle,
        }
    }

    fn current(self, session: &Session, id: Uuid) -> bool {
        match self {
            Self::Streaming(request_id) => session.cache_request_current(id, request_id),
            Self::Idle(head) => {
                session.session_id() == id
                    && session.head() == head
                    && session
                        .records()
                        .last()
                        .is_some_and(|record| record.id == head)
                    && session.host_binding().is_none()
            }
        }
    }
}

struct Context {
    scope: Scope,
    prompt_tokens: u64,
    started: Instant,
    next: Instant,
}

pub(super) struct Capture<'a> {
    args: &'a Cli,
    controller: Controller,
    policy: Option<Arc<Policy>>,
    model: Option<Arc<WorkbenchModel>>,
    registry: Option<Arc<Registry>>,
    session: Option<Uuid>,
    request_id: Option<Uuid>,
    context: Option<Arc<Mutex<Context>>>,
    prompt_tokens: u64,
    completed: Option<(Uuid, u64)>,
    summary: bool,
}

struct Policy {
    delay: Duration,
    late_margin: Duration,
    retention: CacheRetention,
    price: Option<ChatPrice>,
}

impl Policy {
    fn load(args: &Cli) -> Option<Self> {
        let profiles = &args.model_profiles;
        let model = profiles.model(&args.provider, &args.model)?;
        let long_supported = match profiles.model_adapter(&args.provider, &args.model) {
            Adapter::Anthropic => {
                if args.provider_thinking().is_some_and(|level| level != "off")
                    && model.adaptive_thinking() != Some(true)
                {
                    return None;
                }
                profiles
                    .messages_cache_support(&args.provider, Some(&model))
                    .0
            }
            Adapter::ChatCompletions => {
                profiles.chat_cache_support(&args.provider, &args.model, Some(&model))?
            }
            Adapter::PiMessages => true,
            Adapter::Google => return None,
        };
        let retention = super::providers::cache_retention(long_supported);
        let ttl = model.prompt_cache_seconds(retention)?;
        if ttl <= 10.0 || Mode::load(&args.workspace)? == Mode::Off {
            return None;
        }
        let delay = (ttl * 900.0).min(ttl * 1000.0 - 10_000.0).floor().max(1.0) / 1000.0;
        if delay > MAX_STREAMING_AGE.as_secs_f64() {
            return None;
        }
        Some(Self {
            delay: Duration::from_secs_f64(delay),
            late_margin: Duration::from_secs_f64((ttl - delay) / 2.0),
            retention,
            price: model.cost,
        })
    }

    fn economics(&self, input_tokens: u64) -> Option<(f64, f64)> {
        let price = self.price.as_ref()?;
        let usage = Usage {
            input_tokens,
            output_tokens: 0,
        };
        let hit = ModelMetadata {
            cached_input_tokens: Some(input_tokens),
            ..Default::default()
        };
        let written = ModelMetadata {
            cache_creation_input_tokens: Some(input_tokens),
            cache_write_1h_tokens: (self.retention == CacheRetention::Long).then_some(input_tokens),
            ..Default::default()
        };
        let total = |usage: Usage, metadata: &ModelMetadata| {
            price
                .estimate(usage, metadata)
                .and_then(|price| price.total())
                .and_then(|total| total.as_f64())
        };
        let hit_cost = total(usage, &hit)?;
        let miss_cost = total(usage, &written)
            .filter(|cost| *cost > 0.0)
            .or_else(|| total(usage, &ModelMetadata::default()))?;
        let warm_cost = total(
            Usage {
                output_tokens: 1,
                ..usage
            },
            &hit,
        )?;
        Some((warm_cost, (miss_cost - hit_cost).max(0.0)))
    }
}

impl<'a> Capture<'a> {
    pub fn new(args: &'a Cli, prompt: bool, controller: Controller) -> Self {
        let policy = prompt.then(|| Policy::load(args)).flatten().map(Arc::new);
        let model = policy.as_ref().and_then(|_| {
            runtime::model_client(args, super::thinking::reported(args).is_some(), false)
                .ok()
                .map(Arc::new)
        });
        Self {
            args,
            controller,
            policy,
            model,
            registry: None,
            session: None,
            request_id: None,
            context: None,
            prompt_tokens: 0,
            completed: None,
            summary: false,
        }
    }

    pub fn initialize(&mut self, session: &Session, registry: &Registry) {
        if self.model.is_none() {
            return;
        }
        self.registry = registry
            .has_cache_warming_hooks()
            .then(|| Arc::new(registry.clone()));
        self.session = Some(session.session_id());
        self.prompt_tokens = session
            .branch()
            .into_iter()
            .rev()
            .find_map(|record| match &record.event {
                Event::Assistant { usage, .. } => Some(usage.map_or(0, |usage| usage.input_tokens)),
                _ => None,
            })
            .unwrap_or_default();
    }

    pub fn observe(&mut self, event: &Notification<'_>) {
        if self.model.is_none() {
            return;
        }
        match event {
            Notification::SummaryRequest => self.summary = true,
            Notification::Committed(record) => match &record.event {
                Event::ModelRequested { request_id, .. } => {
                    self.cancel();
                    self.request_id = *request_id;
                }
                Event::ModelFinished {
                    request_id,
                    usage,
                    failure,
                    ..
                } if Some(*request_id) == self.request_id => {
                    if failure.is_some() {
                        self.cancel();
                    } else if let UsageReport::Measured { usage } = usage {
                        self.completed = Some((*request_id, usage.input_tokens));
                    }
                }
                Event::Assistant { request_id, .. } => {
                    if let Some((id, tokens)) = self.completed.take()
                        && Some(id) == *request_id
                    {
                        self.prompt_tokens = tokens;
                        if let Some(context) = &self.context {
                            context.lock().expect("cache context").prompt_tokens = tokens;
                        }
                    }
                }
                Event::Run { state, .. } if *state != pi_rs::RunState::Running => {
                    self.controller.park();
                }
                Event::Compacted { .. } | Event::BranchSummary { .. } => {
                    self.prompt_tokens = 0;
                    self.cancel();
                }
                Event::ContextEdited { .. }
                | Event::BashExecution { .. }
                | Event::User { .. }
                | Event::UserWithImages { .. }
                | Event::UserImages { .. } => {
                    self.cancel();
                }
                _ => {}
            },
            Notification::Request(request) => {
                if !std::mem::take(&mut self.summary) {
                    self.start(request);
                }
            }
            _ => {}
        }
    }

    fn start(&mut self, request: &ModelRequest) {
        let (Some(session), Some(request_id), Some(model), Some(policy)) =
            (self.session, self.request_id, &self.model, &self.policy)
        else {
            return;
        };
        if request.model != self.args.model {
            return;
        }
        let started = Instant::now();
        let next = started + policy.delay;
        let context = Arc::new(Mutex::new(Context {
            scope: Scope::Streaming(request_id),
            prompt_tokens: self.prompt_tokens,
            started,
            next,
        }));
        self.context = Some(context.clone());
        let mut request = request.clone();
        request.max_output_tokens = 1;
        let candidate = Candidate {
            path: self.args.session.clone(),
            workspace: self.args.workspace.clone(),
            session,
            identity: ResolvedModelIdentity {
                provider: self.args.provider.id().into(),
                model: request.model.clone(),
            },
            request,
            model: model.clone(),
            registry: self.registry.clone(),
            policy: policy.clone(),
            context,
            timeout: Duration::from_secs(self.args.model_timeout_secs),
        };
        self.controller.set(Some(candidate.schedule(next)));
    }

    fn cancel(&mut self) {
        self.context = None;
        self.completed = None;
        self.controller.set(None);
    }

    pub fn settle(&mut self, session: Option<&Session>) {
        let remains_idle = if let (Some(context), Some(session), Some(request_id)) =
            (&self.context, session, self.request_id)
            && Mode::load(&self.args.workspace) == Some(Mode::Idle)
            && session.queued_inputs().is_empty()
            && session.cache_request_current(session.session_id(), request_id)
        {
            let mut context = context.lock().expect("cache context");
            let until = context.started + MAX_IDLE_AGE;
            context.scope = Scope::Idle(session.head());
            context.next <= until && Instant::now() < until
        } else {
            false
        };
        if !remains_idle {
            self.cancel();
        }
        self.controller.resume();
    }
}

type Task = Pin<Box<dyn Future<Output = Option<Receipt>> + Send>>;

/// The observer replaces the scheduled request synchronously. The worker polls
/// it beside the agent and can keep the same future when a run becomes idle.
#[derive(Clone, Default)]
pub(super) struct Controller(Arc<Scheduled>);

#[derive(Default)]
struct Scheduled {
    task: Mutex<Option<Task>>,
    waker: AtomicWaker,
    parked: AtomicBool,
}

impl Controller {
    fn set(&self, task: Option<Task>) {
        *self.0.task.lock().expect("cache task") = task;
        self.0.waker.wake();
    }

    pub fn clear(&self) {
        self.set(None);
    }

    fn park(&self) {
        self.0.parked.store(true, Ordering::Relaxed);
    }

    fn resume(&self) {
        self.0.parked.store(false, Ordering::Relaxed);
        self.0.waker.wake();
    }

    pub fn is_parked(&self) -> bool {
        self.0.parked.load(Ordering::Relaxed)
    }

    pub async fn wait(&self) -> Option<Receipt> {
        poll_fn(|cx| {
            self.0.waker.register(cx.waker());
            if self.is_parked() {
                return Poll::Pending;
            }
            let mut slot = self.0.task.lock().expect("cache task");
            let Some(task) = slot.as_mut() else {
                return Poll::Pending;
            };
            match task.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(receipt) => {
                    *slot = None;
                    Poll::Ready(receipt)
                }
            }
        })
        .await
    }
}

struct Candidate {
    path: PathBuf,
    workspace: PathBuf,
    session: Uuid,
    identity: ResolvedModelIdentity,
    request: ModelRequest,
    model: Arc<WorkbenchModel>,
    registry: Option<Arc<Registry>>,
    policy: Arc<Policy>,
    context: Arc<Mutex<Context>>,
    timeout: Duration,
}

impl Candidate {
    fn schedule(self, next: Instant) -> Task {
        self.context.lock().expect("cache context").next = next;
        Box::pin(async move {
            sleep_until(next).await;
            let (scope, prompt_tokens, deadline, snapshot) = self.current_snapshot(next)?;
            let states = self.registry.as_ref().map(|_| snapshot.extension_states());
            drop(snapshot);
            let (warm_cost, miss_cost) = self.policy.economics(prompt_tokens).unwrap_or_default();
            let probability = scope.probability();
            let suggested = if probability * miss_cost - warm_cost >= MINIMUM_SAVINGS_USD {
                CacheWarmingAction::Warm
            } else {
                CacheWarmingAction::Stop
            };
            let action = if let (Some(registry), Some(states)) = (&self.registry, states) {
                let event = CacheWarmingDecision {
                    session_id: self.session,
                    model: self.identity.clone(),
                    phase: match scope {
                        Scope::Streaming(_) => CacheWarmingPhase::Streaming,
                        Scope::Idle(_) => CacheWarmingPhase::Idle,
                    },
                    prompt_tokens,
                    warm_cost,
                    miss_cost,
                    continuation_probability: probability,
                    action: suggested,
                };
                timeout_at(
                    deadline,
                    registry.cache_warming_decision(&event, &states, |extension, message| {
                        super::output(serde_json::json!({
                            "type": "extension_error", "extension": extension,
                            "event": "cache_warming_decision", "message": message
                        }));
                    }),
                )
                .await
                .ok()?
            } else {
                suggested
            };
            if action == CacheWarmingAction::Stop {
                return None;
            }
            let deadline = if self.registry.is_some() {
                self.current_snapshot(next)?.2
            } else {
                deadline
            };
            let now = Instant::now();
            if now > deadline {
                return None;
            }
            let (usage, metadata) = timeout(self.timeout, self.refresh()).await.ok()??;
            Some(Receipt {
                candidate: self,
                usage,
                metadata,
                requested_at: now,
                note: (action != suggested).then(|| "extension override".to_owned()),
            })
        })
    }

    fn current_snapshot(&self, next: Instant) -> Option<(Scope, u64, Instant, Session)> {
        let (scope, prompt_tokens, started) = {
            let context = self.context.lock().expect("cache context");
            (context.scope, context.prompt_tokens, context.started)
        };
        let deadline = (next + self.policy.late_margin).min(started + scope.max_age());
        if next > deadline
            || Instant::now() > deadline
            || !Mode::load(&self.workspace).is_some_and(|mode| scope.allowed(mode))
        {
            return None;
        }
        let snapshot = Session::snapshot(&self.path).ok()?;
        scope
            .current(&snapshot, self.session)
            .then_some((scope, prompt_tokens, deadline, snapshot))
    }

    async fn refresh(&self) -> Option<(Usage, ModelMetadata)> {
        let mut stream = self
            .model
            .stream_in_session(Uuid::new_v4(), self.session, self.request.clone())
            .await
            .ok()?;
        let mut metadata = ModelMetadata::default();
        while let Some(event) = stream.next().await {
            match event.ok()? {
                ModelEvent::Metadata(value) => metadata = value,
                ModelEvent::Complete {
                    usage: Some(usage), ..
                } => {
                    metadata
                        .resolved_model
                        .get_or_insert_with(|| self.identity.clone());
                    return Some((usage, metadata));
                }
                _ => {}
            }
        }
        None
    }
}

pub(super) struct Receipt {
    candidate: Candidate,
    usage: Usage,
    metadata: ModelMetadata,
    requested_at: Instant,
    note: Option<String>,
}

impl Receipt {
    pub fn persist(
        self,
        path: &Path,
        controller: &Controller,
        mut observe: impl FnMut(&pi_rs::Record),
    ) {
        if self.candidate.path != path {
            return;
        }
        let scope = self.candidate.context.lock().expect("cache context").scope;
        let Scope::Idle(head) = scope else {
            return;
        };
        let Ok(mut session) = Session::open(path) else {
            return;
        };
        if !scope.current(&session, self.candidate.session) {
            return;
        }
        let cache_hit = self.metadata.cached_input_tokens.unwrap_or_default() > 0;
        let Ok(Some(record)) = session.record_cache_warm_with_note(
            self.candidate.session,
            head,
            self.metadata,
            self.usage,
            self.note,
        ) else {
            return;
        };
        self.candidate.context.lock().expect("cache context").scope = Scope::Idle(record.id);
        observe(record);
        if cache_hit {
            let next = self.requested_at + self.candidate.policy.delay;
            controller.set(Some(self.candidate.schedule(next)));
        }
    }

    pub fn account(self, controller: &Controller) -> Option<AgentInput> {
        let scope = self.candidate.context.lock().expect("cache context").scope;
        let Scope::Streaming(request_id) = scope else {
            return None;
        };
        let cache_hit = self.metadata.cached_input_tokens.unwrap_or_default() > 0;
        let input = AgentInput::CacheWarm {
            session_id: self.candidate.session,
            request_id,
            usage: self.usage,
            metadata: Box::new(self.metadata),
            note: self.note,
        };
        if cache_hit {
            let next = self.requested_at + self.candidate.policy.delay;
            controller.set(Some(self.candidate.schedule(next)));
        }
        Some(input)
    }
}
