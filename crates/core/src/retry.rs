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
pub async fn provider_limit_info<S, B, E>(stream: S, secrets: &[String]) -> ProviderLimitInfo
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
        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .ok()
            .or_else(|| (!complete).then(|| provider_prefix_value(&bytes)).flatten())
            .unwrap_or_default();
        let value = sanitized_provider_error(&value, secrets);
        let error = value.get("error").unwrap_or(&value);
        let recognized_code = match error["code"].as_str() {
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
            permanent: permanent_limit_body(&bytes, complete, secrets),
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
pub async fn permanent_provider_limit<S, B, E>(stream: S, secrets: &[String]) -> bool
where
    S: futures_util::Stream<Item = std::result::Result<B, E>>,
    B: AsRef<[u8]>,
{
    provider_limit_info(stream, secrets).await.permanent
}

/// Collect configured credentials and header values without retaining them in diagnostics.
#[doc(hidden)]
pub fn provider_error_secrets<'a>(
    token: Option<&'a str>,
    headers: impl Iterator<Item = &'a str>,
) -> Vec<String> {
    let mut secrets = Vec::new();
    for value in token.into_iter().chain(headers) {
        if value.is_empty() {
            continue;
        }
        secrets.push(value.to_owned());
        if let Some((scheme, credential)) = value.split_once(' ')
            && scheme.eq_ignore_ascii_case("bearer")
            && !credential.is_empty()
        {
            secrets.push(credential.to_owned());
        }
    }
    secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
    secrets.dedup();
    secrets
}

fn mask_provider_secrets(text: &str, secrets: &[String]) -> String {
    let mut masked = text.to_owned();
    for secret in secrets {
        if !secret.is_empty() {
            masked = masked.replace(secret, "\0\0");
        }
    }
    masked
}

#[doc(hidden)]
pub fn sanitized_provider_error(
    value: &serde_json::Value,
    secrets: &[String],
) -> serde_json::Value {
    let mut value = value.clone();
    fn scrub(value: &mut serde_json::Value, secrets: &[String]) {
        match value {
            serde_json::Value::String(text) => *text = mask_provider_secrets(text, secrets),
            serde_json::Value::Object(values) => {
                values.values_mut().for_each(|v| scrub(v, secrets))
            }
            serde_json::Value::Array(values) => values.iter_mut().for_each(|v| scrub(v, secrets)),
            _ => {}
        }
    }
    scrub(&mut value, secrets);
    value
}

// JSON envelope fields outside the provider error are metadata, not a reason
// to suppress retries. Never scan request IDs or arbitrary nested values.
#[doc(hidden)]
pub fn permanent_provider_error(value: &serde_json::Value, secrets: &[String]) -> bool {
    let value = sanitized_provider_error(value, secrets);
    let value = &value;
    let error = value.get("error").unwrap_or(value);
    if let Some(text) = error.as_str() {
        return permanent_limit_text(text.as_bytes(), true);
    }
    let Some(object) = error.as_object() else {
        return false;
    };
    ["message", "code", "type", "errorMessage"]
        .iter()
        .any(|key| {
            let Some(value) = object.get(*key) else {
                return false;
            };
            if *key == "code" && (value.as_u64() == Some(1308) || value.as_str() == Some("1308")) {
                return true;
            }
            value
                .as_str()
                .is_some_and(|text| permanent_limit_text(text.as_bytes(), true))
        })
}

/// Recognize Pi's transient provider messages without scanning unrelated
/// metadata. Permanent quota markers always take precedence.
#[doc(hidden)]
pub fn retryable_provider_error(value: &serde_json::Value, secrets: &[String]) -> bool {
    if permanent_provider_error(value, secrets) {
        return false;
    }
    let value = sanitized_provider_error(value, secrets);
    let error = value.get("error").unwrap_or(&value);
    let matches = |text: &str| {
        let lower = text.to_ascii_lowercase();
        static PATTERN: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        // This is the pinned Pi retry.ts expression, except status boundaries
        // are checked below because the Rust regex engine has no look-around.
        let pattern = PATTERN.get_or_init(|| regex::Regex::new(concat!(
            r"overloaded|server_busy|servers are currently busy|currently experiencing high demand|model is at capacity|rate.?limit|too many requests|",
            r"service.?(?:is.?)?(?:temporarily.?)?unavailable|server.?error|internal.?error|provider.?returned.?(?:an.?)?error|",
            r"exceeded request buffer limit while retrying upstream|unable to forward upstream|upstream.?request.?failed|upstream_stream_read_error|stream was interrupted|http/?2 stream failed|",
            r"network.?error|connection.?error|connection.?refused|connection.?lost|other side closed|fetch failed|request to \S+ failed, reason:|",
            r"getaddrinfo|enotfound|eai_again|upstream.?connect|reset before headers|socket hang up|socket connection was closed|timed? out|timeout|terminated|",
            r"websocket.?closed|websocket.?error|ended without|stream ended before message_stop|stream ended before a terminal response event|",
            r"http2 request did not get a response|stream disconnected before completion|pending stream has been canceled|retry delay|you can retry your request|",
            r"try your request again|please retry your request|resourceexhausted|subscription_sharing_usage_unavailable|subscription_sharing_user_unavailable"
        )).expect("valid provider retry pattern"));
        if pattern.is_match(&lower) {
            return true;
        }
        ["429", "500", "502", "503", "504", "520", "524"]
            .iter()
            .any(|code| {
                lower.match_indices(code).any(|(index, _)| {
                    let before = lower[..index].bytes().next_back();
                    let after = lower[index + code.len()..].bytes().next();
                    !before.is_some_and(|b| b.is_ascii_alphanumeric())
                        && !after.is_some_and(|b| b.is_ascii_alphanumeric())
                })
            })
    };
    if let Some(text) = error.as_str() {
        return matches(text);
    }
    let Some(object) = error.as_object() else {
        return false;
    };
    ["message", "code", "type", "errorMessage"]
        .iter()
        .any(|key| {
            let Some(value) = object.get(*key) else {
                return false;
            };
            value.as_str().is_some_and(matches)
                || value
                    .as_u64()
                    .is_some_and(|code| matches!(code, 429 | 500 | 502 | 503 | 504 | 520 | 524))
        })
}

// Keep only complete, directly nested error fields observed before an EOF at
// the bounded prefix. Unrelated metadata and incomplete values are ignored.
fn provider_prefix_value(body: &[u8]) -> Option<serde_json::Value> {
    use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor};
    use serde_json::Value;
    use std::fmt;
    struct Root<'a>(&'a mut serde_json::Map<String, Value>);
    struct Fields<'a>(&'a mut serde_json::Map<String, Value>);
    impl<'de> DeserializeSeed<'de> for Root<'_> {
        type Value = ();
        fn deserialize<D: serde::Deserializer<'de>>(
            self,
            d: D,
        ) -> std::result::Result<(), D::Error> {
            struct RootVisitor<'a>(&'a mut serde_json::Map<String, Value>);
            impl<'de> Visitor<'de> for RootVisitor<'_> {
                type Value = ();
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("error envelope")
                }
                fn visit_map<M: MapAccess<'de>>(
                    self,
                    mut map: M,
                ) -> std::result::Result<(), M::Error> {
                    while let Some(key) = map.next_key::<String>()? {
                        if key == "error" {
                            self.0.clear();
                            map.next_value_seed(Fields(self.0))?;
                        } else {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                    Ok(())
                }
            }
            d.deserialize_map(RootVisitor(self.0))
        }
    }
    impl<'de> DeserializeSeed<'de> for Fields<'_> {
        type Value = ();
        fn deserialize<D: serde::Deserializer<'de>>(
            self,
            d: D,
        ) -> std::result::Result<(), D::Error> {
            struct FieldsVisitor<'a>(&'a mut serde_json::Map<String, Value>);
            impl<'de> Visitor<'de> for FieldsVisitor<'_> {
                type Value = ();
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("error fields")
                }
                fn visit_map<M: MapAccess<'de>>(
                    self,
                    mut map: M,
                ) -> std::result::Result<(), M::Error> {
                    while let Some(key) = map.next_key::<String>()? {
                        if matches!(key.as_str(), "message" | "code" | "type" | "errorMessage") {
                            self.0.remove(&key);
                            let value = map.next_value::<Value>()?;
                            self.0.insert(key, value);
                        } else {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                    Ok(())
                }
            }
            d.deserialize_map(FieldsVisitor(self.0))
        }
    }
    let mut fields = serde_json::Map::new();
    let mut de = serde_json::Deserializer::from_slice(body);
    match Root(&mut fields).deserialize(&mut de) {
        Err(error) if error.is_eof() && !fields.is_empty() => Some(Value::Object(fields)),
        _ => None,
    }
}

fn permanent_limit_body(bytes: &[u8], complete: bool, secrets: &[String]) -> bool {
    let text = String::from_utf8_lossy(bytes);
    match serde_json::from_slice::<serde_json::Value>(bytes) {
        Ok(value) => permanent_provider_error(&value, secrets),
        // A partial or invalid JSON document can contain arbitrary metadata;
        // do not mistake it for an error message.
        Err(_) if !complete && text.trim_start().starts_with('{') => {
            let Some(value) = provider_prefix_value(bytes) else {
                return false;
            };
            let mut probe = Vec::with_capacity(bytes.len() + 1);
            probe.extend_from_slice(bytes);
            probe.push(if bytes.last().is_some_and(u8::is_ascii_digit) {
                b'0'
            } else {
                b' '
            });
            provider_prefix_value(&probe).is_some_and(|next| next == value)
                && permanent_provider_error(&value, secrets)
        }
        Err(_) if text.trim_start().starts_with(['{', '[']) => false,
        Err(_) => {
            let mut masked = mask_provider_secrets(&String::from_utf8_lossy(bytes), secrets);
            if !complete {
                // A bounded body can end in the middle of an echoed secret.
                let suffix = secrets
                    .iter()
                    .flat_map(|secret| {
                        secret
                            .char_indices()
                            .skip(1)
                            .map(move |(end, _)| &secret[..end])
                    })
                    .filter(|prefix| masked.ends_with(prefix))
                    .map(str::len)
                    .max()
                    .unwrap_or(0);
                masked.truncate(masked.len() - suffix);
            }
            permanent_limit_text(masked.as_bytes(), complete)
        }
    }
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
        assert!(permanent_provider_limit(chunks, &[]).await);
        let metadata =
            br#"{"error":{"message":"rate limit"},"request_id":"billing-secret"}"#.to_vec();
        assert!(!permanent_provider_limit(stream::iter(vec![Ok::<_, ()>(metadata)]), &[]).await);
        let mut too_long = vec![b'x'; 64 * 1024];
        too_long.extend_from_slice(b"insufficient_quota");
        assert!(!permanent_provider_limit(stream::iter(vec![Ok::<_, ()>(too_long)]), &[]).await);
        let failed = stream::iter(vec![Ok::<_, ()>(b"insufficient_".to_vec()), Err(())]);
        assert!(!permanent_provider_limit(failed, &[]).await);
        let pending = stream::pending::<std::result::Result<Vec<u8>, ()>>();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                permanent_provider_limit(pending, &[])
            )
            .await
            .is_err()
        );
        let usage = br#"{"error":{"code":"subscription_sharing_usage_limit_exceeded"}}"#.to_vec();
        let info = provider_limit_info(stream::iter(vec![Ok::<_, ()>(usage)]), &[]).await;
        assert!(info.permanent);
        assert_eq!(
            info.recognized_code,
            Some("subscription_sharing_usage_limit_exceeded")
        );
    }

    #[test]
    fn bounded_json_retains_only_complete_error_fields() {
        let long = "x".repeat(70_000);
        for prefix in [
            r#"{"error":{"type":"insufficient_quota","detail":""#,
            r#"{"request_id":"other","error":{"message":"quota exceeded","extra":""#,
        ] {
            let body = format!("{prefix}{long}\"}}}}");
            assert!(super::permanent_limit_body(
                &body.as_bytes()[..64 * 1024],
                false,
                &[]
            ));
        }
        for prefix in [
            r#"{"request_id":"billing","error":{"message":"temporary","extra":""#,
            r#"{"error":{"metadata":{"type":"insufficient_quota"},"message":"temporary","extra":""#,
            r#"{"error":{"message":"billing"#,
            r#"{"error":{"type":"insufficient_quota","type":""#,
        ] {
            let body = format!("{prefix}{long}");
            assert!(!super::permanent_limit_body(
                &body.as_bytes()[..64 * 1024],
                false,
                &[]
            ));
        }
        assert!(!super::permanent_limit_body(
            br#"{"error":{"type":"insufficient_quota",!"#,
            false,
            &[]
        ));
        assert!(!super::permanent_limit_body(
            br#"{"error":{"code":1308"#,
            false,
            &[]
        ));
        assert!(super::permanent_limit_body(
            br#"{"error":{"code":1308 "#,
            false,
            &[]
        ));
        assert!(super::permanent_limit_body(
            br#"{"error":{"type":"insufficient_quota""#,
            false,
            &[]
        ));
    }

    #[test]
    fn transient_stream_errors_respect_quota_precedence_and_metadata() {
        for value in [
            serde_json::json!({"error":{"message":"rate limit exceeded"}}),
            serde_json::json!({"code":503}),
            serde_json::json!("server_busy"),
        ] {
            assert!(retryable_provider_error(&value, &[]));
        }
        for value in [
            serde_json::json!({"error":{"message":"quota exceeded: rate limit"}}),
            serde_json::json!({"error":{"message":"invalid request"},"request_id":"rate_limit"}),
            serde_json::json!({"error":{"message":"request 14290"}}),
        ] {
            assert!(!retryable_provider_error(&value, &[]));
        }
    }

    #[test]
    fn transient_patterns_and_secret_masking_follow_pi() {
        let partial_secret = ["sk-provider-billing-secret".to_owned()];
        assert!(!permanent_limit_body(
            b"rate limit sk-provider-billing",
            false,
            &partial_secret
        ));
        let secrets = provider_error_secrets(
            Some("sk-provider-billing-secret"),
            ["Bearer header-quota exceeded-secret"].into_iter(),
        );
        for message in [
            "rate limit",
            "rate-limit",
            "rate_limit",
            "request to host failed, reason:",
        ] {
            assert!(
                retryable_provider_error(&serde_json::json!(message), &secrets),
                "{message}"
            );
        }
        for message in [
            "rate---limit",
            "server busy",
            "t o o many requests",
            "rate limit sk-provider-billing-secret quota exceeded",
        ] {
            assert!(
                !retryable_provider_error(&serde_json::json!(message), &secrets),
                "{message}"
            );
        }
        for secret in ["sk-provider-billing-secret", "header-quota exceeded-secret"] {
            let value = serde_json::json!({"error":{"message":format!("rate limit {secret}")}});
            assert!(!permanent_provider_error(&value, &secrets));
            assert!(retryable_provider_error(&value, &secrets));
        }
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
        assert!(!permanent_limit_body(br#"{"code":1308"#, false, &[]));
        assert!(!permanent_limit_body(
            br#"{"request_id":"billing-secret"}"#,
            true,
            &[]
        ));
        assert!(permanent_provider_error(
            &serde_json::json!({"error":{"code":1308}}),
            &[]
        ));
        assert!(permanent_provider_error(
            &serde_json::json!({"error":{"message":"quota exceeded"}}),
            &[]
        ));
    }
}
