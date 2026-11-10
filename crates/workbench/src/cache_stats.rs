//! Derive cache misses from completed conversation turns, separately from model context.
use pi_rs::{Event, ModelMetadata, Record, ResolvedModelIdentity, Usage, UsageReport};
use uuid::Uuid;

use super::model_profiles::Profiles;

#[derive(Default)]
pub(super) struct Tracker {
    selected_provider: Option<String>,
    requested_model: Option<String>,
    requested_at: Option<i64>,
    pending: Option<(Uuid, Measurement)>,
    previous: Option<Measurement>,
    pub missed_tokens: u64,
    pub missed_cost: f64,
    pub miss_count: u64,
}

struct Measurement {
    identity: Option<ResolvedModelIdentity>,
    requested_at: Option<i64>,
    finished_at: Option<i64>,
    prompt_tokens: u64,
    read: u64,
    reported_cache: bool,
    paid_per_token: f64,
    read_per_token: f64,
}

impl Measurement {
    fn new(
        usage: Usage,
        metadata: &ModelMetadata,
        identity: Option<ResolvedModelIdentity>,
        requested_at: Option<i64>,
        finished_at: Option<i64>,
        profiles: &Profiles,
    ) -> Self {
        let prompt_tokens = usage.input_tokens;
        let read = metadata
            .cached_input_tokens
            .unwrap_or_default()
            .min(prompt_tokens);
        let paid = prompt_tokens - read;
        let write = metadata
            .cache_creation_input_tokens
            .unwrap_or_default()
            .min(paid);
        let price = identity
            .as_ref()
            .and_then(|id| {
                profiles
                    .provider(&id.provider)
                    .and_then(|p| profiles.model(&p, &id.model))
            })
            .and_then(|model| model.cost);
        let estimated = || price.as_ref().and_then(|p| p.estimate(usage, metadata));
        let cost = metadata
            .cost_breakdown
            .clone()
            .or_else(estimated)
            .unwrap_or_default();
        let amount = |number: &serde_json::Number| number.as_f64().unwrap_or_default();
        let paid_per_token = if paid > 0 {
            (amount(&cost.input) + amount(&cost.cache_write)) / paid as f64
        } else {
            0.0
        };
        let read_per_token = if read > 0 {
            amount(&cost.cache_read) / read as f64
        } else if prompt_tokens > 0 {
            price
                .and_then(|p| {
                    p.estimate(
                        usage,
                        &ModelMetadata {
                            cached_input_tokens: Some(prompt_tokens),
                            ..Default::default()
                        },
                    )
                })
                .map_or(0.0, |cost| amount(&cost.cache_read) / prompt_tokens as f64)
        } else {
            0.0
        };
        Self {
            identity,
            requested_at,
            finished_at,
            prompt_tokens,
            read,
            reported_cache: read > 0 || write > 0,
            paid_per_token,
            read_per_token,
        }
    }
}

impl Tracker {
    pub fn select_provider(&mut self, provider: &str) {
        self.selected_provider = Some(provider.to_owned());
    }

    pub fn observe(&mut self, record: &Record, profiles: &Profiles) -> Option<String> {
        match &record.event {
            Event::ModelSelected { provider, .. } => self.selected_provider = provider.clone(),
            Event::ModelRequested { model, .. } => {
                self.requested_model = Some(model.clone());
                self.requested_at = record.timestamp_millis();
            }
            Event::ModelFinished {
                request_id,
                metadata,
                usage: UsageReport::Measured { usage },
                failure: None,
                ..
            } => {
                let identity = metadata.resolved_model.clone().or_else(|| {
                    Some(ResolvedModelIdentity {
                        provider: self.selected_provider.as_deref().unwrap_or("ollama").into(),
                        model: self.requested_model.clone()?,
                    })
                });
                let finished_at = record.timestamp_millis();
                self.pending = Some((
                    *request_id,
                    Measurement::new(
                        *usage,
                        metadata,
                        identity,
                        self.requested_at.take().or(finished_at),
                        finished_at,
                        profiles,
                    ),
                ));
            }
            Event::ModelFinished { .. } => {
                self.requested_at = None;
                self.pending = None;
            }
            Event::Compacted { .. } | Event::BranchSummary { .. } => {
                self.previous = None;
                self.pending = None;
            }
            Event::Assistant {
                request_id: Some(id),
                ..
            } => {
                let (pending_id, current) = self.pending.take()?;
                if pending_id == *id {
                    return self.completed(current);
                }
            }
            _ => {}
        }
        None
    }

    fn completed(&mut self, mut current: Measurement) -> Option<String> {
        if current.prompt_tokens == 0 {
            return None;
        }
        let previous = self.previous.take();
        let notice = previous.and_then(|previous| {
            current.reported_cache |= previous.reported_cache;
            let tokens = previous
                .prompt_tokens
                .min(current.prompt_tokens)
                .saturating_sub(current.read);
            if tokens <= 1024 || !current.reported_cache {
                return None;
            }
            let cost = tokens as f64 * (current.paid_per_token - current.read_per_token).max(0.0);
            self.missed_tokens = self.missed_tokens.saturating_add(tokens);
            self.miss_count = self.miss_count.saturating_add(1);
            self.missed_cost += cost;
            if tokens < 20_000 && cost < 0.1 {
                return None;
            }
            let idle_ms = current
                .requested_at
                .zip(previous.finished_at)
                .map_or(0, |(now, before)| now.saturating_sub(before));
            let label = if current
                .identity
                .as_ref()
                .zip(previous.identity.as_ref())
                .is_some_and(|(a, b)| a != b)
            {
                "Cache miss after model switch".into()
            } else if idle_ms >= 300_000 {
                format!(
                    "Cache miss after {}m idle",
                    idle_ms.saturating_add(30_000) / 60_000
                )
            } else {
                "Cache miss".into()
            };
            let cost = if cost >= 0.01 {
                format!(" (~${cost:.2})")
            } else {
                String::new()
            };
            Some(format!(
                "{label}: {} tokens re-billed{cost}",
                format_tokens(tokens)
            ))
        });
        self.previous = Some(current);
        notice
    }
}

fn format_tokens(tokens: u64) -> String {
    match tokens {
        0..=999 => tokens.to_string(),
        1_000..=9_999 => format!("{:.1}k", tokens as f64 / 1_000.0),
        10_000..=999_999 => format!("{}k", tokens.saturating_add(500) / 1_000),
        1_000_000..=9_999_999 => format!("{:.1}M", tokens as f64 / 1_000_000.0),
        _ => format!("{}M", tokens.saturating_add(500_000) / 1_000_000),
    }
}
