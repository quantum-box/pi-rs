use crate::CancellationToken;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, RwLock};

/// Inspect a bounded provider error stream for Pi's permanent account and
/// quota limits. The response body is never returned or logged. Unknown,
/// incomplete, failed, and slow responses retain the caller's retry policy.
#[doc(hidden)]
pub struct ProviderLimitInfo {
    pub permanent: bool,
    pub recognized_code: Option<&'static str>,
}

#[doc(hidden)]
pub async fn provider_limit_info<S, B, E>(stream: S) -> ProviderLimitInfo
where
    S: futures_util::Stream<Item = std::result::Result<B, E>>,
    B: AsRef<[u8]>,
{
    use futures_util::StreamExt;
    const LIMIT: usize = 64 * 1024;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        futures_util::pin_mut!(stream);
        let mut bytes = Vec::new();
        let mut complete = false;
        while bytes.len() < LIMIT {
            let Some(chunk) = stream.next().await else {
                complete = true;
                break;
            };
            let chunk = chunk.ok()?;
            let chunk = chunk.as_ref();
            bytes.extend_from_slice(&chunk[..chunk.len().min(LIMIT - bytes.len())]);
        }
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
        let recognized_code = match value["error"]["code"].as_str() {
            Some("subscription_sharing_usage_limit_exceeded") => {
                Some("subscription_sharing_usage_limit_exceeded")
            }
            Some("subscription_sharing_user_not_eligible") => {
                Some("subscription_sharing_user_not_eligible")
            }
            Some("subscription_sharing_usage_unavailable") => {
                Some("subscription_sharing_usage_unavailable")
            }
            _ => None,
        };
        Some(ProviderLimitInfo {
            permanent: permanent_limit_text(&bytes, complete),
            recognized_code,
        })
    })
    .await
    .ok()
    .flatten()
    .unwrap_or(ProviderLimitInfo {
        permanent: false,
        recognized_code: None,
    })
}

#[doc(hidden)]
pub async fn permanent_provider_limit<S, B, E>(stream: S) -> bool
where
    S: futures_util::Stream<Item = std::result::Result<B, E>>,
    B: AsRef<[u8]>,
{
    provider_limit_info(stream).await.permanent
}

fn permanent_limit_text(bytes: &[u8], complete: bool) -> bool {
    let text = String::from_utf8_lossy(bytes).to_ascii_lowercase();
    const MARKERS: &[&str] = &[
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
    ];
    if MARKERS.iter().any(|marker| text.contains(marker)) {
        return true;
    }
    text.match_indices("\"code\"").any(|(index, _)| {
        let rest = text[index + 6..].trim_start();
        let Some(rest) = rest.strip_prefix(':') else {
            return false;
        };
        let rest = rest
            .trim_start()
            .strip_prefix('\"')
            .unwrap_or(rest.trim_start());
        rest.strip_prefix("1308").is_some_and(|tail| {
            (complete || !tail.is_empty()) && !tail.starts_with(|c: char| c.is_ascii_digit())
        })
    })
}

/// Session-level retries. Low-level hosts opt in explicitly.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrySettings {
    pub enabled: bool,
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_agent_delay_ms: u64,
}
impl RetrySettings {
    pub const fn pi_defaults() -> Self {
        Self {
            enabled: true,
            max_retries: 3,
            base_delay_ms: 2000,
            max_agent_delay_ms: 60_000,
        }
    }
    pub fn delay_ms(self, attempt: u32) -> u64 {
        self.base_delay_ms
            .saturating_mul(
                1_u64
                    .checked_shl(attempt.saturating_sub(1))
                    .unwrap_or(u64::MAX),
            )
            .min(self.max_agent_delay_ms)
    }
}
impl Default for RetrySettings {
    fn default() -> Self {
        let mut settings = Self::pi_defaults();
        settings.enabled = false;
        settings
    }
}

#[derive(Clone, Debug, Default)]
pub struct RetryControl {
    settings: Arc<RwLock<RetrySettings>>,
    waiting: Arc<Mutex<Option<CancellationToken>>>,
}
impl RetryControl {
    pub fn new(settings: RetrySettings) -> Self {
        let control = Self::default();
        control.configure(settings);
        control
    }
    pub fn settings(&self) -> RetrySettings {
        *self.settings.read().expect("retry settings")
    }
    pub fn configure(&self, settings: RetrySettings) {
        *self.settings.write().expect("retry settings") = settings;
    }
    pub fn set_enabled(&self, enabled: bool) {
        self.settings.write().expect("retry settings").enabled = enabled;
    }
    pub fn is_retrying(&self) -> bool {
        self.waiting.lock().expect("retry wait").is_some()
    }
    pub fn abort(&self) {
        if let Some(cancel) = self.waiting.lock().expect("retry wait").as_ref() {
            cancel.cancel();
        }
    }
    pub(crate) fn begin_wait(&self) -> RetryWait {
        let cancel = CancellationToken::new();
        *self.waiting.lock().expect("retry wait") = Some(cancel.clone());
        RetryWait {
            control: self.clone(),
            cancel,
        }
    }
}

pub(crate) struct RetryWait {
    control: RetryControl,
    pub(crate) cancel: CancellationToken,
}
impl Drop for RetryWait {
    fn drop(&mut self) {
        self.control.waiting.lock().expect("retry wait").take();
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type")]
pub enum RetryEvent {
    #[serde(rename = "auto_retry_start", rename_all = "camelCase")]
    Start {
        attempt: u32,
        max_attempts: u32,
        delay_ms: u64,
        error_message: String,
    },
    #[serde(rename = "auto_retry_end", rename_all = "camelCase")]
    End {
        success: bool,
        attempt: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        final_error: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dropping_wait_clears_the_active_retry() {
        let control = RetryControl::default();
        let wait = control.begin_wait();
        assert!(control.is_retrying());
        control.abort();
        assert!(wait.cancel.is_cancelled());
        drop(wait);
        assert!(!control.is_retrying());
    }
    #[test]
    fn delay_doubles_and_caps_with_safe_arithmetic() {
        let settings = RetrySettings::pi_defaults();
        assert_eq!(
            [1, 2, 3, 20].map(|attempt| settings.delay_ms(attempt)),
            [2000, 4000, 8000, 60000]
        );
        assert!(!RetrySettings::default().enabled);
        assert_eq!(
            RetrySettings {
                base_delay_ms: 0,
                ..settings
            }
            .delay_ms(u32::MAX),
            0
        );
        assert_eq!(
            RetrySettings {
                base_delay_ms: u64::MAX,
                ..settings
            }
            .delay_ms(u32::MAX),
            60000
        );
        let event = serde_json::to_value(RetryEvent::Start {
            attempt: 1,
            max_attempts: 3,
            delay_ms: 2000,
            error_message: "temporary".into(),
        })
        .unwrap();
        assert_eq!(event["type"], "auto_retry_start");
        assert_eq!(event["maxAttempts"], 3);
        assert_eq!(event["delayMs"], 2000);
    }
}

#[cfg(test)]
mod quota_tests {
    use super::*;
    #[tokio::test]
    async fn bounded_quota_stream_handles_limits_failures_and_cancellation() {
        use futures_util::stream;
        let chunks = stream::iter(vec![
            Ok::<_, ()>(b"insufficient_".to_vec()),
            Ok(b"quota".to_vec()),
        ]);
        assert!(permanent_provider_limit(chunks).await);
        let mut too_long = vec![b'x'; 64 * 1024];
        too_long.extend_from_slice(b"insufficient_quota");
        assert!(!permanent_provider_limit(stream::iter(vec![Ok::<_, ()>(too_long)])).await);
        let failed = stream::iter(vec![Ok::<_, ()>(b"insufficient_".to_vec()), Err(())]);
        assert!(!permanent_provider_limit(failed).await);
        let pending = stream::pending::<std::result::Result<Vec<u8>, ()>>();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                permanent_provider_limit(pending)
            )
            .await
            .is_err()
        );
        let usage = br#"{"error":{"code":"subscription_sharing_usage_limit_exceeded"}}"#.to_vec();
        let info = provider_limit_info(stream::iter(vec![Ok::<_, ()>(usage)])).await;
        assert!(info.permanent);
        assert_eq!(
            info.recognized_code,
            Some("subscription_sharing_usage_limit_exceeded")
        );
    }

    #[test]
    fn permanent_limits_match_pi_without_matching_longer_codes() {
        for value in [
            "insufficient_quota",
            "Billing limit",
            r#"{"code":"1308"}"#,
            "FreeUsageLimitError",
        ] {
            assert!(permanent_limit_text(value.as_bytes(), true));
        }
        for value in ["rate limit exceeded", r#"{"code":13080}"#] {
            assert!(!permanent_limit_text(value.as_bytes(), true));
        }
        assert!(!permanent_limit_text(br#"{"code":1308"#, false));
    }
}
