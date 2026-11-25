//! Pure model retry decisions, based on upstream pi's agent retry and overflow rules.
//!
//! The host must persist each returned state before sleeping or issuing another
//! request. Deadlines are absolute Unix milliseconds, never recomputed on restart.
//! The host owns cancellation and must check it before waiting and before starting
//! an attempt; this module performs no I/O, sleeps, or provider calls.
//! Reference: pi 827383a15262905f82e418483825a49dbb1adbf5,
//! `packages/ai/src/utils/{retry,overflow}.ts` and coding-agent settings.

use pi_rs_core::Error;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub enabled: bool,
    /// Number of retries after the initial attempt.
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_retries: 3,
            base_delay_ms: 2_000,
            max_delay_ms: 60_000,
        }
    }
}

/// Persist as a unit with the failed attempt. The deadline remains fixed on recovery.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryState {
    pub retries_scheduled: u32,
    pub deadline_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    Stop,
    ContextOverflow,
    WaitUntil(u64),
    Ready,
}

impl RetryState {
    /// Decide once per failed attempt, and persist the mutated state atomically.
    /// A pending deadline must first be consumed with `poll`.
    pub fn failed(&mut self, policy: RetryPolicy, failure: Failure<'_>, now_ms: u64) -> Decision {
        if let Some(deadline) = self.deadline_ms {
            return Decision::WaitUntil(deadline);
        }
        match classify(failure) {
            Classification::Overflow => return Decision::ContextOverflow,
            Classification::Permanent => return Decision::Stop,
            Classification::Transient => {}
        }
        if !policy.enabled || self.retries_scheduled >= policy.max_retries {
            return Decision::Stop;
        }
        let attempt = self.retries_scheduled + 1;
        let delay = if policy.base_delay_ms == 0 {
            0
        } else {
            2u64.checked_pow(attempt - 1)
                .and_then(|factor| policy.base_delay_ms.checked_mul(factor))
                .unwrap_or(u64::MAX)
        };
        let deadline = now_ms.saturating_add(delay.min(policy.max_delay_ms));
        self.retries_scheduled = attempt;
        self.deadline_ms = Some(deadline);
        Decision::WaitUntil(deadline)
    }

    /// Consume a due deadline exactly once; persist before starting the retry.
    pub fn poll(&mut self, now_ms: u64) -> Decision {
        match self.deadline_ms {
            Some(deadline) if now_ms >= deadline => {
                self.deadline_ms = None;
                Decision::Ready
            }
            Some(deadline) => Decision::WaitUntil(deadline),
            None => Decision::Stop,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopReason {
    Error,
    Stop,
    Length,
    Other,
}

/// Usage must correspond to this response and its model's context window.
#[derive(Clone, Copy, Debug)]
pub struct Failure<'a> {
    pub stop_reason: StopReason,
    pub provider: &'a str,
    pub message: &'a str,
    pub input_tokens: u64,
    pub cache_read_tokens: u64,
    pub output_tokens: u64,
    pub context_window: Option<u64>,
    /// True only for an explicitly transient provider/transport classification.
    pub transient_hint: bool,
}

impl<'a> Failure<'a> {
    /// Preserve the core adapter's explicit classification. Other error kinds,
    /// including cancellation, must never become model retries.
    pub fn from_error(error: &'a Error) -> Option<Self> {
        let (message, transient_hint) = match error {
            Error::Model(message) => (message.as_str(), false),
            Error::RetryableModel(message) => (message.as_str(), true),
            _ => return None,
        };
        Some(Self {
            stop_reason: StopReason::Error,
            provider: "",
            message,
            input_tokens: 0,
            cache_read_tokens: 0,
            output_tokens: 0,
            context_window: None,
            transient_hint,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Classification {
    Overflow,
    Transient,
    Permanent,
}

fn has_any(s: &str, values: &[&str]) -> bool {
    values.iter().any(|v| s.contains(v))
}
fn begins_status(s: &str, status: &str) -> bool {
    s.strip_prefix(status).is_some_and(|tail| {
        tail.is_empty() || tail.starts_with(':') || tail.starts_with(char::is_whitespace)
    })
}
fn numeric_after(s: &str, prefix: &str) -> bool {
    s.split_once(prefix)
        .is_some_and(|(_, rest)| rest.chars().next().is_some_and(|c| c.is_ascii_digit()))
}
fn bounded_code(s: &str, code: &str) -> bool {
    s.match_indices(code).any(|(i, _)| {
        let valid = |c: char| !c.is_ascii_alphanumeric();
        s[..i].chars().next_back().is_none_or(valid)
            && s[i + code.len()..].chars().next().is_none_or(valid)
    })
}

/// Upstream error patterns take precedence over silent-usage overflow signals.
pub fn is_context_overflow(f: Failure<'_>) -> bool {
    let s = f.message.to_ascii_lowercase();
    if f.stop_reason == StopReason::Error
        && !s.is_empty()
        && !s.starts_with("throttling error:")
        && !s.starts_with("service unavailable:")
        && !has_any(&s, &["rate limit", "too many requests"])
    {
        let plain = has_any(
            &s,
            &[
                "prompt is too long",
                "prompt too long",
                "prompt exceeds max length",
                "request_too_large",
                "input is too long for requested model",
                "exceeds the context window",
                "context window exceeded",
                "reduce the length of the messages",
                "exceeds the available context size",
                "greater than the context length",
                "context window exceeds limit",
                "exceeded model token limit",
                "model_context_window_exceeded",
                "range of input length should be",
                "context_length_exceeded",
                "context length exceeded",
                "context_length exceeded",
                "context length_exceeded",
                "too many tokens",
                "token limit exceeded",
            ],
        );
        let max_context = [
            "exceeds maximum context length",
            "exceeds the maximum context length",
            "exceeds model's maximum context length",
            "exceeds the model's maximum context length",
            "exceeds models maximum context length",
            "exceeds the models maximum context length",
        ]
        .iter()
        .any(|p| {
            s.split_once(p).is_some_and(|(_, tail)| {
                let t = tail.trim_start();
                t.strip_prefix("of ").is_some_and(|v| {
                    v.chars().next().is_some_and(|c| c.is_ascii_digit()) && v.contains(" token")
                }) || t.strip_prefix('(').is_some_and(|v| {
                    v.chars().next().is_some_and(|c| c.is_ascii_digit()) && v.contains(')')
                })
            })
        });
        let other = (s.contains("input token count")
            && s.split_once("input token count")
                .is_some_and(|(_, t)| t.contains("exceeds the maximum")))
            || numeric_after(&s, "maximum prompt length is ")
            || (numeric_after(&s, "maximum context length is ") && s.contains(" tokens"))
            || ((s.contains("exceeds maximum allowed input length of ")
                || s.contains("exceeds the maximum allowed input length of "))
                && s.contains(" token"))
            || (s.contains("input (")
                && s.contains(" tokens) is longer than the model")
                && s.contains("context length ("))
            || numeric_after(&s, "exceeds the limit of ")
            || (numeric_after(&s, "too large for model with ")
                && s.contains(" maximum context length"))
            || (s.contains("prompt has ")
                && s.contains(" tokens, but the configured context size is ")
                && s.contains(" tokens"))
            || begins_status(&s, "413")
            || (s.contains(" api error (413):")
                && s.split_once(" api error (413):").is_some_and(|(p, _)| {
                    !p.is_empty()
                        && p.chars()
                            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ' ' || c == '-')
                }))
            || (f.provider == "cerebras" && (s.starts_with("400") || s.starts_with("413")) && {
                let t = s[3..].trim_start();
                t.starts_with("(no body)")
                    || t.strip_prefix("status code")
                        .is_some_and(|v| v.trim_start().starts_with("(no body)"))
            });
        if plain || max_context || other {
            return true;
        }
    }
    let Some(window) = f.context_window.filter(|w| *w > 0) else {
        return false;
    };
    let input = u128::from(f.input_tokens) + u128::from(f.cache_read_tokens);
    match f.stop_reason {
        StopReason::Stop => input > u128::from(window),
        StopReason::Length if f.output_tokens == 0 => input * 100 >= u128::from(window) * 99,
        _ => false,
    }
}

/// Explicit non-transient account limits override both message and adapter hints.
pub fn classify(f: Failure<'_>) -> Classification {
    if is_context_overflow(f) {
        return Classification::Overflow;
    }
    if f.stop_reason != StopReason::Error {
        return Classification::Permanent;
    }
    let s = f.message.to_ascii_lowercase();
    if has_any(
        &s,
        &[
            "gousagelimiterror",
            "freeusagelimiterror",
            "monthly usage limit reached",
            "available balance",
            "insufficient_quota",
            "out of budget",
            "quota exceeded",
            "billing",
            "subscription_sharing_usage_limit_exceeded",
            "subscription_sharing_workplace_individual_usage_limit_exceeded",
        ],
    ) || zhipu_limit(&s)
    {
        return Classification::Permanent;
    }
    let transient = has_any(
        &s,
        &[
            "overloaded",
            "server_busy",
            "servers are currently busy",
            "currently experiencing high demand",
            "model is at capacity",
            "too many requests",
            "exceeded request buffer limit while retrying upstream",
            "unable to forward upstream",
            "upstream_stream_read_error",
            "stream was interrupted",
            "http/2 stream failed",
            "http2 stream failed",
            "fetch failed",
            "getaddrinfo",
            "other side closed",
            "connection reset by peer",
            "enotfound",
            "eai_again",
            "reset before headers",
            "socket hang up",
            "socket connection was closed",
            "timeout",
            "timed out",
            "time out",
            "terminated",
            "ended without",
            "stream ended before message_stop",
            "stream ended before a terminal response event",
            "http2 request did not get a response",
            "stream disconnected before completion",
            "cut short after the reply had begun",
            "pending stream has been canceled",
            "retry delay",
            "you can retry your request",
            "try your request again",
            "please retry your request",
            "resourceexhausted",
            "subscription_sharing_usage_unavailable",
            "subscription_sharing_user_unavailable",
        ],
    ) || [
        "rate",
        "server",
        "internal",
        "network",
        "connection",
        "websocket",
        "upstream",
    ]
    .iter()
    .any(|p| {
        let tails: &[&str] = match *p {
            "rate" => &["limit"],
            "server" => &["error"],
            "internal" | "network" => &["error"],
            "connection" => &["error", "refused", "lost", "closed"],
            "websocket" => &["closed", "error"],
            _ => &["connect", "request failed"],
        };
        tails.iter().any(|t| {
            s.contains(&format!("{p}{t}"))
                || s.contains(&format!("{p} {t}"))
                || s.contains(&format!("{p}_{t}"))
        })
    }) || [
        "429",
        "500",
        "502",
        "503",
        "504",
        "520",
        "524",
        "econnreset",
        "econnaborted",
        "econnrefused",
        "etimedout",
        "epipe",
    ]
    .iter()
    .any(|c| bounded_code(&s, c))
        || (s.contains("service") && s.contains("unavailable"))
        || (s.contains("provider") && s.contains("returned") && s.contains("error"))
        || (s.contains("request to ") && s.contains(" failed, reason:"));
    if f.transient_hint || transient {
        Classification::Transient
    } else {
        Classification::Permanent
    }
}

fn zhipu_limit(s: &str) -> bool {
    s.split_once("\"code\"").is_some_and(|(_, tail)| {
        let Some(t) = tail.trim_start().strip_prefix(':') else {
            return false;
        };
        let t = t.trim_start().strip_prefix('"').unwrap_or(t.trim_start());
        t.strip_prefix("1308")
            .is_some_and(|t| !t.chars().next().is_some_and(|c| c.is_ascii_digit()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(message: &str) -> Failure<'_> {
        Failure {
            stop_reason: StopReason::Error,
            provider: "",
            message,
            input_tokens: 0,
            cache_read_tokens: 0,
            output_tokens: 0,
            context_window: None,
            transient_hint: false,
        }
    }

    #[test]
    fn default_budget_backoff_and_persisted_deadline() {
        let mut state = RetryState::default();
        let policy = RetryPolicy::default();
        for (now, deadline) in [(100, 2100), (2100, 6100), (6100, 14100)] {
            assert_eq!(
                state.failed(policy, failure("503 overloaded"), now),
                Decision::WaitUntil(deadline)
            );
            let persisted = serde_json::to_string(&state).unwrap();
            state = serde_json::from_str(&persisted).unwrap();
            assert_eq!(state.poll(deadline - 1), Decision::WaitUntil(deadline));
            assert_eq!(state.poll(deadline), Decision::Ready);
            assert_eq!(state.poll(deadline), Decision::Stop);
        }
        assert_eq!(state.failed(policy, failure("503"), 14100), Decision::Stop);
    }

    #[test]
    fn bounded_delays_and_no_double_schedule() {
        let policy = RetryPolicy {
            max_retries: 100,
            base_delay_ms: u64::MAX,
            max_delay_ms: 60_000,
            ..RetryPolicy::default()
        };
        let mut state = RetryState {
            retries_scheduled: 63,
            deadline_ms: None,
        };
        assert_eq!(
            state.failed(policy, failure("timeout"), u64::MAX - 1),
            Decision::WaitUntil(u64::MAX)
        );
        assert_eq!(
            state.failed(policy, failure("timeout"), 5),
            Decision::WaitUntil(u64::MAX)
        );
        assert_eq!(state.retries_scheduled, 64);
        assert_eq!(
            RetryState::default().failed(
                RetryPolicy {
                    enabled: false,
                    ..policy
                },
                failure("timeout"),
                0
            ),
            Decision::Stop
        );
    }

    #[test]
    fn classifications_do_not_retry_quota_or_overflow() {
        for s in [
            "429 insufficient_quota",
            "503 billing error",
            "rate limit: quota exceeded",
            "429 {\"code\": 1308}",
            "invalid api key",
            "unknown problem",
            "context length exceeded 503",
        ] {
            assert_ne!(classify(failure(s)), Classification::Transient, "{s}");
        }
        for s in [
            "503 overloaded",
            "rate_limit",
            "connection reset by peer",
            "fetch failed",
            "ECONNRESET",
            "try your request again",
        ] {
            assert_eq!(classify(failure(s)), Classification::Transient, "{s}");
        }
        assert_eq!(
            classify(failure("request id a503b")),
            Classification::Permanent
        );
        let mut f = failure("429 insufficient_quota");
        f.transient_hint = true;
        assert_eq!(classify(f), Classification::Permanent);
        assert!(Failure::from_error(&Error::Cancelled).is_none());
        assert_eq!(
            classify(Failure::from_error(&Error::Model("bad request".into())).unwrap()),
            Classification::Permanent
        );
    }

    #[test]
    fn upstream_provider_overflow_examples_and_exclusions() {
        for s in [
            "prompt is too long: 213462 tokens",
            "request_too_large",
            "Input length (265330) exceeds model's maximum context length (262144).",
            "input token count (99) exceeds the maximum",
            "maximum prompt length is 131072",
            "exceeds the maximum allowed input length of 10 tokens",
            "413 {\"error\":{}}",
            "OpenAI API error (413): large",
            "context_length_exceeded",
            "Range of input length should be [1, 20]",
        ] {
            assert!(is_context_overflow(failure(s)), "{s}");
        }
        for s in [
            "Throttling error: too many tokens",
            "Service unavailable: too many tokens",
            "rate limit: too many tokens",
            "too many requests: too many tokens",
            "error body mentions 413 but no overflow",
        ] {
            assert!(!is_context_overflow(failure(s)), "{s}");
        }
        let mut f = failure("400 status code (no body)");
        assert!(!is_context_overflow(f));
        f.provider = "cerebras";
        assert!(is_context_overflow(f));
    }

    #[test]
    fn silent_and_length_overflow_are_bounded_to_correct_stop() {
        let mut f = failure("");
        f.context_window = Some(100);
        f.stop_reason = StopReason::Stop;
        f.input_tokens = 90;
        f.cache_read_tokens = 11;
        assert!(is_context_overflow(f));
        f.input_tokens = 89;
        assert!(!is_context_overflow(f));
        f.stop_reason = StopReason::Length;
        f.cache_read_tokens = 0;
        f.input_tokens = 98;
        assert!(!is_context_overflow(f));
        f.input_tokens = 99;
        assert!(is_context_overflow(f));
        f.output_tokens = 1;
        assert!(!is_context_overflow(f));
        f.context_window = Some(0);
        f.output_tokens = 0;
        assert!(!is_context_overflow(f));
    }
}
