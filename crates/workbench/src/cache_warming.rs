//! Best-effort refreshes of an idle conversation's last confirmed prompt cache entry.
use std::future::{Future, pending};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::Duration;

use futures_util::StreamExt;
use pi_rs::{
    CacheRetention, Event, Model, ModelEvent, ModelMetadata, ModelRequest, Notification,
    ResolvedModelIdentity, Session, Usage, UsageReport,
};
use tokio::time::{Instant, sleep_until, timeout};
use uuid::Uuid;

use super::{
    Cli, image_policy::WorkbenchModel, model_profiles::ChatPrice, providers::Adapter, runtime,
};

const MAX_IDLE_AGE: Duration = Duration::from_secs(30 * 60);
const MINIMUM_SAVINGS_USD: f64 = 0.05;
const CONTINUATION_PROBABILITY: f64 = 0.15;

pub(super) struct Capture {
    policy: Option<Policy>,
    request_id: Option<Uuid>,
    request: Option<(ModelRequest, Instant)>,
    summary: bool,
}

struct Policy {
    delay: Duration,
    late_margin: Duration,
    retention: CacheRetention,
    price: ChatPrice,
}

impl Policy {
    fn load(args: &Cli) -> Option<Self> {
        let profiles = &args.model_profiles;
        if args.settings.string("cacheWarming") != Some("idle")
            || profiles.model_adapter(&args.provider, &args.model) != Adapter::Anthropic
            || args.provider_thinking().is_some_and(|level| level != "off")
        {
            return None;
        }
        let model = profiles.model(&args.provider, &args.model)?;
        let (long_supported, _) = profiles.messages_cache_support(&args.provider, Some(&model));
        let retention = super::providers::cache_retention(long_supported);
        let ttl = model.prompt_cache_seconds(retention)?;
        if ttl <= 10.0 {
            return None;
        }
        let delay = (ttl * 900.0).min(ttl * 1000.0 - 10_000.0).floor().max(1.0) / 1000.0;
        if delay > MAX_IDLE_AGE.as_secs_f64() {
            return None;
        }
        Some(Self {
            delay: Duration::from_secs_f64(delay),
            late_margin: Duration::from_secs_f64((ttl - delay) / 2.0),
            retention,
            price: model.cost?,
        })
    }

    fn economical(&self, input_tokens: u64) -> bool {
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
            self.price
                .estimate(usage, metadata)
                .and_then(|price| price.total())
                .and_then(|total| total.as_f64())
        };
        let Some(hit_cost) = total(usage, &hit) else {
            return false;
        };
        let Some(miss_cost) = total(usage, &written)
            .filter(|cost| *cost > 0.0)
            .or_else(|| total(usage, &ModelMetadata::default()))
        else {
            return false;
        };
        let Some(warm_cost) = total(
            Usage {
                output_tokens: 1,
                ..usage
            },
            &hit,
        ) else {
            return false;
        };
        CONTINUATION_PROBABILITY * (miss_cost - hit_cost).max(0.0) - warm_cost
            >= MINIMUM_SAVINGS_USD
    }
}

impl Capture {
    pub fn new(args: &Cli, prompt: bool) -> Self {
        Self {
            policy: prompt.then(|| Policy::load(args)).flatten(),
            request_id: None,
            request: None,
            summary: false,
        }
    }

    pub fn observe(&mut self, event: &Notification<'_>) {
        if self.policy.is_none() {
            return;
        }
        match event {
            Notification::SummaryRequest => self.summary = true,
            Notification::Committed(record) => match &record.event {
                Event::ModelRequested { request_id, .. } => self.request_id = *request_id,
                Event::Compacted { .. }
                | Event::BranchSummary { .. }
                | Event::ContextEdited { .. }
                | Event::BashExecution { .. } => self.request = None,
                _ => {}
            },
            Notification::Request(request) => {
                self.request = (!std::mem::take(&mut self.summary))
                    .then(|| ((*request).clone(), Instant::now()));
            }
            _ => {}
        }
    }

    pub fn schedule(self, session: &Session, args: &Cli) -> Option<Task> {
        let policy = self.policy?;
        let request_id = self.request_id?;
        let (mut request, started) = self.request?;
        if request.model != args.model || !session.queued_inputs().is_empty() {
            return None;
        }
        let branch = session.branch();
        let assistant = branch.iter().rev().find_map(|record| match record.event {
            Event::Assistant { request_id, .. } => Some(request_id),
            _ => None,
        })?;
        if assistant != Some(request_id) {
            return None;
        }
        let (metadata, usage) = branch.iter().rev().find_map(|record| match &record.event {
            Event::ModelFinished {
                request_id: id,
                metadata,
                usage: UsageReport::Measured { usage },
                failure: None,
                ..
            } if *id == request_id => Some((metadata, usage)),
            _ => None,
        })?;
        if (metadata.cached_input_tokens.unwrap_or_default() == 0
            && metadata.cache_creation_input_tokens.unwrap_or_default() == 0)
            || !policy.economical(usage.input_tokens)
        {
            return None;
        }
        request.max_output_tokens = 1;
        let next = started + policy.delay;
        let candidate = Candidate {
            path: args.session.clone(),
            workspace: args.workspace.clone(),
            head: session.head(),
            session: session.session_id(),
            identity: ResolvedModelIdentity {
                provider: args.provider.id().into(),
                model: request.model.clone(),
            },
            request,
            model: runtime::model_client(args, super::thinking::reported(args).is_some(), false)
                .ok()?,
            delay: policy.delay,
            late_margin: policy.late_margin,
            until: started + MAX_IDLE_AGE,
            timeout: Duration::from_secs(args.model_timeout_secs),
        };
        candidate.schedule(next)
    }
}

pub(super) type Task = Pin<Box<dyn Future<Output = Option<Receipt>> + Send>>;

struct Candidate {
    path: PathBuf,
    workspace: PathBuf,
    head: u64,
    session: Uuid,
    identity: ResolvedModelIdentity,
    request: ModelRequest,
    model: WorkbenchModel,
    delay: Duration,
    late_margin: Duration,
    until: Instant,
    timeout: Duration,
}

impl Candidate {
    fn current(&self, session: &Session) -> bool {
        session.session_id() == self.session
            && session.head() == self.head
            && session
                .records()
                .last()
                .is_some_and(|record| record.id == self.head)
            && session.host_binding().is_none()
    }

    fn schedule(self, next: Instant) -> Option<Task> {
        if next > self.until {
            return None;
        }
        Some(Box::pin(async move {
            sleep_until(next).await;
            let deadline = (next + self.late_margin).min(self.until);
            if Instant::now() > deadline
                || !Session::snapshot(&self.path).is_ok_and(|session| self.current(&session))
                || !super::config::Settings::load(&self.workspace, false)
                    .is_ok_and(|settings| settings.string("cacheWarming") == Some("idle"))
            {
                return None;
            }
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
            })
        }))
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
}

impl Receipt {
    pub fn persist(mut self, path: &Path, mut observe: impl FnMut(&pi_rs::Record)) -> Option<Task> {
        if self.candidate.path != path {
            return None;
        }
        let cache_hit = self.metadata.cached_input_tokens.unwrap_or_default() > 0;
        let mut session = Session::open(path).ok()?;
        if !self.candidate.current(&session) {
            return None;
        }
        let record = session
            .record_cache_warm(
                self.candidate.session,
                self.candidate.head,
                self.metadata,
                self.usage,
            )
            .ok()??;
        self.candidate.head = record.id;
        observe(record);
        let next = self.requested_at + self.candidate.delay;
        cache_hit.then(|| self.candidate.schedule(next)).flatten()
    }
}

pub(super) async fn wait(task: &mut Option<Task>) -> Option<Receipt> {
    match task {
        Some(task) => task.as_mut().await,
        None => pending().await,
    }
}
