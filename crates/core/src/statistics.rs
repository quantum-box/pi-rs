//! Durable session statistics shared by frontends and embedded clients.
use crate::{Event, Session};
use serde::Serialize;

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionStats {
    pub session_id: uuid::Uuid,
    pub session_name: Option<String>,
    pub user_messages: u64,
    pub assistant_messages: u64,
    pub tool_calls: u64,
    pub tool_results: u64,
    pub total_messages: u64,
    pub tokens: TokenTotals,
    pub cost: Option<f64>,
}
impl SessionStats {
    fn add_usage(&mut self, usage: crate::Usage, metadata: Option<&crate::ModelMetadata>) {
        self.tokens.add(usage, metadata);
        let cost = metadata
            .and_then(|metadata| metadata.cost_usd.as_ref())
            .and_then(serde_json::Number::as_f64)
            .filter(|cost| cost.is_finite() && *cost >= 0.0);
        self.cost = self
            .cost
            .zip(cost)
            .map(|(total, cost)| total + cost)
            .filter(|cost| cost.is_finite());
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenTotals {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub total: u64,
}
impl TokenTotals {
    fn add(&mut self, usage: crate::Usage, metadata: Option<&crate::ModelMetadata>) {
        let cache_read = metadata
            .and_then(|value| value.cached_input_tokens)
            .unwrap_or(0);
        let cache_write = metadata
            .and_then(|value| value.cache_creation_input_tokens)
            .unwrap_or(0);
        self.input = self.input.saturating_add(
            usage
                .input_tokens
                .saturating_sub(cache_read.saturating_add(cache_write)),
        );
        self.output = self.output.saturating_add(usage.output_tokens);
        self.cache_read = self.cache_read.saturating_add(cache_read);
        self.cache_write = self.cache_write.saturating_add(cache_write);
        self.total = self
            .input
            .saturating_add(self.output)
            .saturating_add(self.cache_read)
            .saturating_add(self.cache_write);
    }
}

/// Durable session totals include branches and compacted history, as Pi does.
fn statistics(session: &Session) -> SessionStats {
    let records = session.records();
    let measured: std::collections::BTreeMap<_, _> = records
        .iter()
        .filter_map(|record| match &record.event {
            Event::ModelFinished {
                request_id,
                usage: crate::UsageReport::Measured { usage },
                metadata,
                ..
            } => Some((*request_id, (*usage, metadata))),
            _ => None,
        })
        .collect();
    let nested: std::collections::BTreeSet<_> = records
        .iter()
        .filter_map(|record| match &record.event {
            Event::ToolPlanned { invocation, .. } => Some(invocation.operation_id),
            _ => None,
        })
        .collect();
    let mut stats = SessionStats {
        session_id: session.session_id(),
        session_name: session.name().map(str::to_owned),
        cost: Some(0.0),
        ..SessionStats::default()
    };
    for (usage, metadata) in measured.values() {
        stats.add_usage(*usage, Some(metadata));
    }
    let mut pending_summary = None;
    for record in records {
        match &record.event {
            Event::User { .. } | Event::UserWithImages { .. } => {
                stats.user_messages += 1;
                stats.total_messages += 1;
            }
            Event::Assistant {
                invocations,
                usage,
                request_id,
                ..
            } => {
                stats.assistant_messages += 1;
                stats.total_messages += 1;
                stats.tool_calls = stats.tool_calls.saturating_add(invocations.len() as u64);
                if let Some(usage) = usage
                    && request_id.is_none_or(|id| !measured.contains_key(&id))
                {
                    stats.add_usage(*usage, None);
                }
                pending_summary = None;
            }
            Event::ToolFinished { operation_id, .. } if !nested.contains(operation_id) => {
                stats.tool_results += 1;
                stats.total_messages += 1;
            }
            Event::BashExecution { .. } => {
                stats.total_messages += 1;
            }
            Event::ModelRequested { request_id, .. } => {
                pending_summary = None;
                if request_id.is_none_or(|id| !measured.contains_key(&id)) {
                    stats.cost = None;
                }
            }
            Event::ModelFinished { usage, reply, .. } => {
                pending_summary = match (usage, reply) {
                    (crate::UsageReport::Measured { usage }, None) => Some(*usage),
                    _ => None,
                };
                if !matches!(usage, crate::UsageReport::Measured { .. }) {
                    stats.cost = None;
                }
            }
            Event::CacheWarm {
                usage, metadata, ..
            } => {
                stats.add_usage(*usage, Some(metadata));
            }
            Event::Compacted { usage, .. } | Event::BranchSummary { usage, .. } => {
                if let Some(usage) = usage
                    && Some(*usage) != pending_summary
                {
                    stats.add_usage(*usage, None);
                }
                pending_summary = None;
            }
            _ => {}
        }
    }
    stats
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextUsage {
    pub tokens: Option<u64>,
    pub context_window: u32,
    pub percent: Option<f64>,
}

impl Session {
    pub fn statistics(&self) -> SessionStats {
        statistics(self)
    }

    /// Current branch usage for a known context window. Zero means unknown.
    pub fn context_usage(&self, context_window: u32) -> Option<ContextUsage> {
        if context_window == 0 {
            return None;
        }
        let estimate = self.context_usage_estimate();
        let after_compaction = self
            .branch()
            .iter()
            .any(|record| matches!(record.event, Event::Compacted { .. }));
        let tokens = if after_compaction && estimate.last_usage_record.is_none() {
            None
        } else {
            Some(estimate.tokens)
        };
        Some(ContextUsage {
            tokens,
            context_window,
            percent: tokens.map(|tokens| tokens as f64 / f64::from(context_window) * 100.0),
        })
    }
}
