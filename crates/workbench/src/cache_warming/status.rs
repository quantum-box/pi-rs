use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pi_rs::{CacheWarmingAction, CacheWarmingPhase};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use super::MINIMUM_SAVINGS_USD;

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(crate) struct Decision {
    pub phase: CacheWarmingPhase,
    pub warm_cost: f64,
    pub miss_cost: f64,
    pub probability: f64,
    pub available: bool,
    pub action: CacheWarmingAction,
    pub overridden: bool,
}

impl Decision {
    fn details(self) -> String {
        let economics = if self.available {
            let phase = match self.phase {
                CacheWarmingPhase::Streaming => " while agent is running",
                CacheWarmingPhase::Idle => "",
            };
            let savings = self.probability * self.miss_cost - self.warm_cost;
            let comparison = if savings >= MINIMUM_SAVINGS_USD {
                ">="
            } else {
                "<"
            };
            let sign = if savings < 0.0 { "-" } else { "" };
            format!(
                "{:.0}% continuation probability{phase}, expected savings {sign}${:.3} {comparison} ${MINIMUM_SAVINGS_USD:.3}",
                self.probability * 100.0,
                savings.abs(),
            )
        } else {
            "cache economics unavailable".into()
        };
        if self.overridden {
            format!("extension override, {economics}")
        } else {
            let action = match self.action {
                CacheWarmingAction::Warm => "warm",
                CacheWarmingAction::Stop => "stop",
            };
            format!("{economics} -> {action}")
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum State {
    Inactive {
        reason: String,
        decision: Option<Decision>,
    },
    Scheduled {
        at_ms: u64,
        decision: Decision,
        head: Option<u64>,
    },
    Refreshing {
        decision: Decision,
        head: Option<u64>,
    },
}

pub(in crate::cache_warming) fn publish(path: &Path, state: State) {
    super::super::output(serde_json::json!({
        "type": "cache_warming_status",
        "session": path,
        "status": state,
    }));
}

pub(in crate::cache_warming) fn stopped(path: &Path, reason: &str, decision: Option<Decision>) {
    publish(
        path,
        State::Inactive {
            reason: reason.into(),
            decision,
        },
    );
}

fn timestamp() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
}

pub(in crate::cache_warming) fn scheduled(
    path: &Path,
    next: Instant,
    decision: Decision,
    head: Option<u64>,
) {
    let at_ms = (timestamp() + next.saturating_duration_since(Instant::now())).as_millis() as u64;
    publish(
        path,
        State::Scheduled {
            at_ms,
            decision,
            head,
        },
    );
}

pub(crate) fn describe(state: Option<&State>, mode: &str, current_head: u64) -> String {
    let (status, decision) = if mode == "off" {
        ("Inactive (cache warming disabled)".into(), None)
    } else {
        match state {
            Some(
                State::Scheduled {
                    head: Some(head), ..
                }
                | State::Refreshing {
                    head: Some(head), ..
                },
            ) if *head != current_head => ("Inactive (conversation context changed)".into(), None),
            Some(State::Inactive { reason, decision }) => {
                let status = match decision {
                    Some(decision) if decision.available || decision.overridden => {
                        format!("Stopped ({reason} · {})", decision.details())
                    }
                    _ => format!("Inactive ({reason})"),
                };
                (status, *decision)
            }
            Some(State::Scheduled {
                at_ms, decision, ..
            }) if decision.available => {
                let seconds = Duration::from_millis(*at_ms).saturating_sub(timestamp());
                let seconds = seconds.as_secs() + u64::from(seconds.subsec_nanos() > 0);
                let time = if seconds == 0 {
                    "Decision now".into()
                } else {
                    let parts = [
                        (seconds / 3600, "h"),
                        (seconds / 60 % 60, "m"),
                        (seconds % 60, "s"),
                    ];
                    let duration = parts
                        .into_iter()
                        .filter(|(n, _)| *n > 0)
                        .map(|(n, unit)| format!("{n}{unit}"))
                        .collect::<Vec<_>>()
                        .join(" ");
                    format!("Decision in {duration}")
                };
                (format!("{time} ({})", decision.details()), Some(*decision))
            }
            Some(State::Scheduled { .. }) => {
                ("Inactive (cache economics unavailable)".into(), None)
            }
            Some(State::Refreshing { decision, .. }) => (
                format!("Warming cache ({})", decision.details()),
                Some(*decision),
            ),
            None => ("Inactive (waiting for first request)".into(), None),
        }
    };
    let mut text = format!("Cache Warming\nMode: {mode}\nStatus: {status}");
    if let Some(decision) = decision.filter(|decision| decision.available) {
        text.push_str(&format!(
            "\nCache miss penalty: ${:.3}\nRefresh cost: ${:.3}",
            decision.miss_cost, decision.warm_cost
        ));
    }
    text
}
