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

pub(super) mod status;

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

    fn age_reason(self) -> &'static str {
        match self {
            Self::Idle(_) => "30-minute idle safety limit reached",
            Self::Streaming(_) => "one-hour safety limit reached",
        }
    }

    fn head(self) -> Option<u64> {
        match self {
            Self::Idle(head) => Some(head),
            Self::Streaming(_) => None,
        }
    }

    fn phase(self) -> CacheWarmingPhase {
        match self {
            Self::Streaming(_) => CacheWarmingPhase::Streaming,
            Self::Idle(_) => CacheWarmingPhase::Idle,
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
    refreshing: Option<status::Decision>,
}

pub(super) struct Capture<'a> {
    args: &'a Cli,
    controller: Controller,
    policy: Result<Arc<Policy>, &'static str>,
    enabled: bool,
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
    fn load(args: &Cli) -> Result<Self, &'static str> {
        if Mode::load(&args.workspace).ok_or("cache warming settings unavailable")? == Mode::Off {
            return Err("cache warming disabled");
        }
        let profiles = &args.model_profiles;
        let model = profiles
            .model(&args.provider, &args.model)
            .ok_or("cache lifetime unavailable")?;
        let long_supported = match profiles.model_adapter(&args.provider, &args.model) {
            Adapter::Anthropic => {
                if args.provider_thinking().is_some_and(|level| level != "off")
                    && model.adaptive_thinking() != Some(true)
                {
                    return Err("request cannot be replayed safely");
                }
                profiles
                    .messages_cache_support(&args.provider, Some(&model))
                    .0
            }
            Adapter::ChatCompletions => profiles
                .chat_cache_support(&args.provider, &args.model, Some(&model))
                .ok_or("request disabled prompt caching")?,
            Adapter::PiMessages => true,
            Adapter::Google | Adapter::Bedrock | Adapter::OpenAiResponses => {
                return Err("cache lifetime unavailable");
            }
        };
        let retention = super::providers::cache_retention(long_supported);
        let ttl = model
            .prompt_cache_seconds(retention)
            .ok_or("cache lifetime unavailable")?;
        if ttl <= 10.0 {
            return Err("cache lifetime unavailable");
        }
        let delay = (ttl * 900.0).min(ttl * 1000.0 - 10_000.0).floor().max(1.0) / 1000.0;
        if delay > MAX_STREAMING_AGE.as_secs_f64() {
            return Err("one-hour safety limit reached");
        }
        Ok(Self {
            delay: Duration::from_secs_f64(delay),
            late_margin: Duration::from_secs_f64((ttl - delay) / 2.0),
            retention,
            price: model.cost,
        })
    }

    fn decision(&self, scope: Scope, prompt_tokens: u64) -> status::Decision {
        let (warm_cost, miss_cost, available) = self.economics(prompt_tokens).unwrap_or_default();
        let probability = scope.probability();
        let action = if probability * miss_cost - warm_cost >= MINIMUM_SAVINGS_USD {
            CacheWarmingAction::Warm
        } else {
            CacheWarmingAction::Stop
        };
        status::Decision {
            phase: scope.phase(),
            warm_cost,
            miss_cost,
            probability,
            available,
            action,
            overridden: false,
        }
    }

    fn economics(&self, input_tokens: u64) -> Option<(f64, f64, bool)> {
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
        Some((
            warm_cost,
            (miss_cost - hit_cost).max(0.0),
            input_tokens > 0 && (hit_cost > 0.0 || miss_cost > 0.0),
        ))
    }
}

impl<'a> Capture<'a> {
    pub fn new(args: &'a Cli, prompt: bool, controller: Controller) -> Self {
        let policy = if prompt {
            Policy::load(args).map(Arc::new)
        } else {
            Err("waiting for first request")
        };
        let model = policy.as_ref().ok().and_then(|_| {
            runtime::model_client(args, super::thinking::reported(args).is_some(), false)
                .ok()
                .map(Arc::new)
        });
        Self {
            args,
            controller,
            policy,
            enabled: prompt,
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
        if !self.enabled {
            return;
        }
        match event {
            Notification::SummaryRequest => self.summary = true,
            Notification::Committed(record) => match &record.event {
                Event::ModelRequested { request_id, .. } => {
                    self.cancel("conversation context changed");
                    self.request_id = *request_id;
                }
                Event::ModelFinished {
                    request_id,
                    usage,
                    failure,
                    ..
                } if Some(*request_id) == self.request_id => {
                    if failure.is_some() {
                        self.cancel("model request failed");
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
                            self.publish_current();
                        }
                    }
                }
                Event::Run { state, .. } if *state != pi_rs::RunState::Running => {
                    self.controller.park();
                }
                Event::Compacted { .. } | Event::BranchSummary { .. } => {
                    self.prompt_tokens = 0;
                    self.cancel("conversation context changed");
                }
                Event::ContextEdited { .. }
                | Event::BashExecution { .. }
                | Event::User { .. }
                | Event::UserWithImages { .. }
                | Event::UserImages { .. } => {
                    self.cancel("conversation context changed");
                }
                _ => {}
            },
            Notification::Request(request) => {
                if std::mem::take(&mut self.summary) {
                    status::stopped(&self.args.session, "request disabled prompt caching", None);
                } else {
                    self.start(request);
                }
            }
            _ => {}
        }
    }

    fn start(&mut self, request: &ModelRequest) {
        let policy = match &self.policy {
            Ok(policy) => policy,
            Err(reason) => {
                status::stopped(&self.args.session, reason, None);
                return;
            }
        };
        let (Some(session), Some(request_id), Some(model)) =
            (self.session, self.request_id, &self.model)
        else {
            status::stopped(&self.args.session, "cache warming unavailable", None);
            return;
        };
        if request.model != self.args.model {
            status::stopped(&self.args.session, "conversation context changed", None);
            return;
        }
        let started = Instant::now();
        let next = started + policy.delay;
        let context = Arc::new(Mutex::new(Context {
            scope: Scope::Streaming(request_id),
            prompt_tokens: self.prompt_tokens,
            started,
            next,
            refreshing: None,
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
            mode: Mode::load,
            registry: self.registry.clone(),
            policy: policy.clone(),
            context,
            timeout: Duration::from_secs(self.args.model_timeout_secs),
        };
        self.controller.set(candidate.schedule(next));
    }

    fn publish_current(&self) {
        if self.controller.has_task()
            && let (Some(context), Ok(policy)) = (&self.context, &self.policy)
        {
            let context = context.lock().expect("cache context");
            if let Some(decision) = context.refreshing {
                status::publish(
                    &self.args.session,
                    status::State::Refreshing {
                        decision,
                        head: context.scope.head(),
                    },
                );
            } else {
                status::scheduled(
                    &self.args.session,
                    context.next,
                    policy.decision(context.scope, context.prompt_tokens),
                    context.scope.head(),
                );
            }
        }
    }

    fn cancel(&mut self, reason: &str) {
        self.context = None;
        self.completed = None;
        self.controller.clear(&self.args.session, reason);
    }

    pub fn settle(&mut self, session: Option<&Session>) {
        if self.context.is_none() {
            self.controller.resume();
            return;
        }
        let mode = Mode::load(&self.args.workspace);
        let stop = if let (Some(context), Some(session), Some(request_id)) =
            (&self.context, session, self.request_id)
            && mode == Some(Mode::Idle)
            && session.queued_inputs().is_empty()
            && session.cache_request_current(session.session_id(), request_id)
        {
            let mut context = context.lock().expect("cache context");
            let until = context.started + MAX_IDLE_AGE;
            context.scope = Scope::Idle(session.head());
            (context.next > until || Instant::now() >= until)
                .then_some("30-minute idle safety limit reached")
        } else if mode == Some(Mode::Off) {
            Some("cache warming disabled")
        } else if session.is_some() && mode == Some(Mode::Streaming) {
            Some("agent run settled")
        } else {
            Some("conversation context changed")
        };
        if let Some(reason) = stop {
            self.cancel(reason);
        } else {
            self.publish_current();
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

    fn has_task(&self) -> bool {
        self.0.task.lock().expect("cache task").is_some()
    }

    pub fn clear(&self, path: &Path, reason: &str) {
        if self.0.task.lock().expect("cache task").take().is_some() {
            status::stopped(path, reason, None);
        }
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
    model: Arc<dyn Model>,
    mode: fn(&Path) -> Option<Mode>,
    registry: Option<Arc<Registry>>,
    policy: Arc<Policy>,
    context: Arc<Mutex<Context>>,
    timeout: Duration,
}

impl Candidate {
    fn prepare_schedule(&self, next: Instant) -> bool {
        {
            let mut context = self.context.lock().expect("cache context");
            if next > context.started + context.scope.max_age() {
                status::stopped(&self.path, context.scope.age_reason(), None);
                return false;
            }
            context.next = next;
            context.refreshing = None;
            status::scheduled(
                &self.path,
                next,
                self.policy.decision(context.scope, context.prompt_tokens),
                context.scope.head(),
            );
        }
        true
    }

    fn schedule(self, mut next: Instant) -> Option<Task> {
        if !self.prepare_schedule(next) {
            return None;
        }
        Some(Box::pin(async move {
            loop {
                sleep_until(next).await;
                let stop = |reason, decision| {
                    status::stopped(&self.path, reason, decision);
                    None
                };
                let (scope, prompt_tokens, deadline, snapshot) = match self.current_snapshot(next) {
                    Ok(current) => current,
                    Err(reason) => return stop(reason, None),
                };
                let states = self.registry.as_ref().map(|_| snapshot.extension_states());
                drop(snapshot);
                let mut decision = self.policy.decision(scope, prompt_tokens);
                let suggested = decision.action;
                if let (Some(registry), Some(states)) = (&self.registry, states) {
                    let event = CacheWarmingDecision {
                        session_id: self.session,
                        model: self.identity.clone(),
                        phase: decision.phase,
                        prompt_tokens,
                        warm_cost: decision.warm_cost,
                        miss_cost: decision.miss_cost,
                        continuation_probability: decision.probability,
                        action: suggested,
                    };
                    let action = timeout_at(
                        deadline,
                        registry.cache_warming_decision(&event, &states, |extension, message| {
                            super::output(serde_json::json!({
                                "type": "extension_error", "extension": extension,
                                "event": "cache_warming_decision", "message": message
                            }));
                        }),
                    )
                    .await;
                    let Ok(action) = action else {
                        return stop("cache refresh deadline missed", None);
                    };
                    decision.action = action;
                    decision.overridden = action != suggested;
                }
                if decision.action == CacheWarmingAction::Stop {
                    let reason = if decision.overridden {
                        "stopped by extension"
                    } else if decision.available {
                        "expected savings below threshold"
                    } else {
                        "cache economics unavailable"
                    };
                    return stop(reason, Some(decision));
                }
                let deadline = if self.registry.is_some() {
                    match self.current_snapshot(next) {
                        Ok((_, _, deadline, _)) => deadline,
                        Err(reason) => return stop(reason, None),
                    }
                } else {
                    deadline
                };
                let now = Instant::now();
                if now > deadline {
                    return stop("cache refresh deadline missed", None);
                }
                let head = {
                    let mut context = self.context.lock().expect("cache context");
                    context.refreshing = Some(decision);
                    context.scope.head()
                };
                status::publish(&self.path, status::State::Refreshing { decision, head });
                let Ok(Some((usage, metadata))) = timeout(self.timeout, self.refresh()).await
                else {
                    // Pi retries best-effort refreshes on the normal cadence. The
                    // new schedule still checks the original age and context.
                    next = Instant::now() + self.policy.delay;
                    if let Err(reason) = self.current_snapshot(next) {
                        return stop(reason, None);
                    }
                    if !self.prepare_schedule(next) {
                        return None;
                    }
                    continue;
                };
                return Some(Receipt {
                    candidate: self,
                    usage,
                    metadata,
                    note: decision.overridden.then(|| "extension override".to_owned()),
                    decision,
                });
            }
        }))
    }

    fn current_snapshot(
        &self,
        next: Instant,
    ) -> Result<(Scope, u64, Instant, Session), &'static str> {
        let (scope, prompt_tokens, started) = {
            let context = self.context.lock().expect("cache context");
            (context.scope, context.prompt_tokens, context.started)
        };
        let until = started + scope.max_age();
        if next > until || Instant::now() > until {
            return Err(scope.age_reason());
        }
        let deadline = (next + self.policy.late_margin).min(until);
        if Instant::now() > deadline {
            return Err("cache refresh deadline missed");
        }
        let mode = (self.mode)(&self.workspace).ok_or("cache warming settings unavailable")?;
        if !scope.allowed(mode) {
            return Err(if mode == Mode::Off {
                "cache warming disabled"
            } else {
                "agent run settled"
            });
        }
        let snapshot =
            Session::snapshot(&self.path).map_err(|_| "conversation context unavailable")?;
        if !scope.current(&snapshot, self.session) {
            return Err("conversation context changed");
        }
        Ok((scope, prompt_tokens, deadline, snapshot))
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
    note: Option<String>,
    decision: status::Decision,
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
            status::stopped(
                path,
                "cache refresh usage could not be recorded",
                Some(self.decision),
            );
            return;
        };
        if !scope.current(&session, self.candidate.session) {
            status::stopped(path, "conversation context changed", None);
            return;
        }
        let Ok(Some(record)) = session.record_cache_warm_with_note(
            self.candidate.session,
            head,
            self.metadata,
            self.usage,
            self.note,
        ) else {
            status::stopped(
                path,
                "cache refresh usage could not be recorded",
                Some(self.decision),
            );
            return;
        };
        self.candidate.context.lock().expect("cache context").scope = Scope::Idle(record.id);
        observe(record);
        let next = Instant::now() + self.candidate.policy.delay;
        controller.set(self.candidate.schedule(next));
    }

    pub fn account(self, controller: &Controller) -> Option<AgentInput> {
        let scope = self.candidate.context.lock().expect("cache context").scope;
        let Scope::Streaming(request_id) = scope else {
            return None;
        };
        let input = AgentInput::CacheWarm {
            session_id: self.candidate.session,
            request_id,
            usage: self.usage,
            metadata: Box::new(self.metadata),
            note: self.note,
        };
        let next = Instant::now() + self.candidate.policy.delay;
        controller.set(self.candidate.schedule(next));
        Some(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_rs::{Error, ModelStream};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FlakyRefresh {
        attempts: AtomicUsize,
        retry_started: tokio::sync::Notify,
        release_retry: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl Model for FlakyRefresh {
        async fn stream(&self, request: ModelRequest) -> pi_rs::Result<ModelStream> {
            assert_eq!(request.max_output_tokens, 1);
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                return Err(Error::Config("transient failure".into()));
            }
            if attempt == 1 {
                self.retry_started.notify_one();
                self.release_retry.notified().await;
            }
            let metadata = ModelMetadata {
                cached_input_tokens: Some(if attempt == 1 { 0 } else { 1_000_000 }),
                ..Default::default()
            };
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Metadata(metadata)),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: Some(Usage {
                        input_tokens: 1_000_000,
                        output_tokens: 1,
                    }),
                }),
            ])))
        }
    }

    fn idle_mode(_: &Path) -> Option<Mode> {
        Some(Mode::Idle)
    }

    #[tokio::test]
    async fn transient_failure_and_cache_miss_reschedule_without_double_accounting() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("session.jsonl");
        let session = Session::open(&path).unwrap();
        let id = session.session_id();
        let head = session.head();
        drop(session);
        let model = Arc::new(FlakyRefresh {
            attempts: AtomicUsize::new(0),
            retry_started: tokio::sync::Notify::new(),
            release_retry: tokio::sync::Notify::new(),
        });
        let policy = Arc::new(Policy {
            delay: Duration::from_millis(25), late_margin: Duration::from_secs(1),
            retention: CacheRetention::Short,
            price: Some(serde_json::from_value(serde_json::json!({"input":10.0,"output":1.0,"cacheRead":1.0,"cacheWrite":20.0})).unwrap()),
        });
        let now = Instant::now();
        let candidate = Candidate {
            path: path.clone(),
            workspace: root.path().to_owned(),
            session: id,
            identity: ResolvedModelIdentity {
                provider: "fixture".into(),
                model: "model".into(),
            },
            request: ModelRequest {
                model: "model".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 1,
            },
            model: model.clone(),
            mode: idle_mode,
            registry: None,
            policy,
            context: Arc::new(Mutex::new(Context {
                scope: Scope::Idle(head),
                prompt_tokens: 1_000_000,
                started: now,
                next: now,
                refreshing: None,
            })),
            timeout: Duration::from_secs(2),
        };
        let controller = Controller::default();
        controller.set(candidate.schedule(now));
        let (receipt, ()) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(controller.wait(), async {
                model.retry_started.notified().await;
                assert_eq!(model.attempts.load(Ordering::SeqCst), 2);
                assert!(
                    !Session::snapshot(&path)
                        .unwrap()
                        .records()
                        .iter()
                        .any(|r| matches!(r.event, Event::CacheWarm { .. }))
                );
                model.release_retry.notify_one();
            })
        })
        .await
        .unwrap();
        receipt.unwrap().persist(&path, &controller, |_| {});
        let receipt = tokio::time::timeout(Duration::from_secs(2), controller.wait())
            .await
            .unwrap()
            .unwrap();
        receipt.persist(&path, &controller, |_| {});
        assert_eq!(model.attempts.load(Ordering::SeqCst), 3);
        assert_eq!(
            Session::snapshot(&path)
                .unwrap()
                .records()
                .iter()
                .filter(|r| matches!(r.event, Event::CacheWarm { .. }))
                .count(),
            2
        );
        let mut changed = Session::open(&path).unwrap();
        changed
            .append_user_with_images("new context".into(), vec![], None)
            .unwrap();
        drop(changed);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), controller.wait())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(model.attempts.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn streaming_cache_miss_is_accounted_and_rescheduled() {
        let root = tempfile::tempdir().unwrap();
        let now = Instant::now();
        let request_id = Uuid::new_v4();
        let candidate = Candidate {
            path: root.path().join("session.jsonl"),
            workspace: root.path().to_owned(),
            session: Uuid::new_v4(),
            identity: ResolvedModelIdentity {
                provider: "fixture".into(),
                model: "model".into(),
            },
            request: ModelRequest {
                model: "model".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 1,
            },
            model: Arc::new(FlakyRefresh {
                attempts: AtomicUsize::new(0),
                retry_started: tokio::sync::Notify::new(),
                release_retry: tokio::sync::Notify::new(),
            }),
            mode: idle_mode,
            registry: None,
            policy: Arc::new(Policy {
                delay: Duration::from_secs(1),
                late_margin: Duration::from_secs(1),
                retention: CacheRetention::Short,
                price: None,
            }),
            context: Arc::new(Mutex::new(Context {
                scope: Scope::Streaming(request_id),
                prompt_tokens: 0,
                started: now,
                next: now,
                refreshing: None,
            })),
            timeout: Duration::from_secs(1),
        };
        let receipt = Receipt {
            candidate,
            usage: Usage {
                input_tokens: 10,
                output_tokens: 1,
            },
            metadata: ModelMetadata::default(),
            note: None,
            decision: status::Decision {
                phase: CacheWarmingPhase::Streaming,
                warm_cost: 0.0,
                miss_cost: 0.0,
                probability: 1.0,
                available: false,
                action: CacheWarmingAction::Warm,
                overridden: true,
            },
        };
        let controller = Controller::default();
        assert!(
            matches!(receipt.account(&controller), Some(AgentInput::CacheWarm { request_id: id, .. }) if id == request_id)
        );
        assert!(controller.has_task());
    }
}
