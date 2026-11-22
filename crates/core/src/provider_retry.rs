use std::sync::LazyLock;
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};
use http::HeaderMap;
use regex::Regex;

use crate::{Error, Result};

/// The retry policy used before a provider response stream has been accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderRetry {
    pub max_retries: u32,
    /// A zero ceiling honors any representable server-requested delay.
    pub max_retry_delay_ms: u64,
    /// Timeout for receiving response headers on each HTTP attempt.
    pub timeout: Option<Duration>,
}

impl Default for ProviderRetry {
    fn default() -> Self {
        Self {
            max_retries: 0,
            max_retry_delay_ms: 60_000,
            timeout: None,
        }
    }
}

impl ProviderRetry {
    /// Retry transport failures and retryable HTTP responses before streaming.
    ///
    /// Dropping the future also cancels any pending retry wait.
    pub async fn send<T, Attempt>(
        self,
        attempt: impl FnMut() -> Attempt,
        response_info: impl Fn(&T) -> (u16, &HeaderMap),
    ) -> Result<T>
    where
        Attempt: Future<Output = Result<T>>,
    {
        self.send_with_policy(
            attempt,
            |value| {
                let (status, headers) = response_info(value);
                (status, Some(headers))
            },
            true,
        )
        .await
    }

    /// Retry only HTTP statuses for a provider whose API errors expose no headers.
    pub async fn send_status_only<T, Attempt>(
        self,
        attempt: impl FnMut() -> Attempt,
        response_status: impl Fn(&T) -> u16,
    ) -> Result<T>
    where
        Attempt: Future<Output = Result<T>>,
    {
        Self {
            timeout: None,
            ..self
        }
        .send_with_policy(attempt, |value| (response_status(value), None), false)
        .await
    }

    async fn send_with_policy<T, Attempt>(
        self,
        mut attempt: impl FnMut() -> Attempt,
        response_info: impl Fn(&T) -> (u16, Option<&HeaderMap>),
        retry_transport_errors: bool,
    ) -> Result<T>
    where
        Attempt: Future<Output = Result<T>>,
    {
        for index in 0..=self.max_retries {
            let response = match self.timeout {
                Some(timeout) => crate::runtime::time::timeout(timeout, attempt())
                    .await
                    .unwrap_or_else(|_| {
                        Err(Error::RetryableModel("HTTP request timed out".into()))
                    }),
                None => attempt().await,
            };
            if index == self.max_retries {
                return response;
            }
            let delay = match &response {
                Ok(value) => {
                    let (status, headers) = response_info(value);
                    if (200..300).contains(&status) || !retryable_status(status, headers) {
                        return response;
                    }
                    match headers {
                        Some(headers) => self.delay(headers, index, Utc::now())?,
                        None => fallback_delay(index),
                    }
                }
                Err(Error::RetryableModel(_)) if retry_transport_errors => fallback_delay(index),
                Err(_) => return response,
            };
            drop(response);
            crate::runtime::time::sleep(delay).await;
        }
        unreachable!("the last attempt returns its result")
    }

    fn delay(self, headers: &HeaderMap, index: u32, now: DateTime<Utc>) -> Result<Duration> {
        let delay = retry_header(headers, "retry-after-ms")
            .and_then(number_prefix)
            .or_else(|| {
                let value = retry_header(headers, "retry-after")?;
                number_prefix(value).map_or_else(
                    || http_date(value).map(|date| (date - now).num_milliseconds() as f64),
                    |seconds| Some(seconds * 1000.0),
                )
            })
            .filter(|delay| delay.is_finite());
        let Some(delay) = delay else {
            return Ok(fallback_delay(index));
        };
        if self.max_retry_delay_ms > 0 && delay > self.max_retry_delay_ms as f64 {
            let limit = Duration::from_millis(self.max_retry_delay_ms)
                .as_secs_f64()
                .ceil();
            let requested = (delay / 1000.0).ceil();
            return Err(Error::RetryableModel(format!(
                "Server requested {requested}s retry delay (max: {limit}s)"
            )));
        }
        Duration::try_from_secs_f64(delay.max(0.0) / 1000.0)
            .map_err(|_| Error::Model("Server requested an invalid retry delay".into()))
    }
}

fn retryable_status(status: u16, headers: Option<&HeaderMap>) -> bool {
    match headers.and_then(|headers| retry_header(headers, "x-should-retry")) {
        Some("true") => true,
        Some("false") => false,
        _ => matches!(status, 408 | 409 | 429 | 500..),
    }
}

fn retry_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let first = values.next()?.to_str().ok()?;
    // Fetch joins duplicate values. Numeric parsing still takes the first
    // prefix, but a joined retry directive or date is no longer exact.
    if values.next().is_some() && (name == "x-should-retry" || number_prefix(first).is_none()) {
        return None;
    }
    Some(first)
}

fn number_prefix(value: &str) -> Option<f64> {
    static NUMBER: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
            .expect("static numeric prefix pattern")
    });
    NUMBER
        .find(value.trim_start())?
        .as_str()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}

fn http_date(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(value)
        .ok()
        .map(|date| date.to_utc())
        .or_else(|| {
            // HTTP also retains the obsolete RFC 850 and asctime formats.
            if let Ok(date) = NaiveDateTime::parse_from_str(value, "%a %b %e %H:%M:%S %Y") {
                return Some(date.and_utc());
            }
            let (_, date) = value.split_once(", ")?;
            let (day_month, tail) = date.rsplit_once('-')?;
            let (year, time) = tail.split_once(' ')?;
            if year.len() != 2 {
                return None;
            }
            let year = year.parse::<u32>().ok()?;
            let year = year + if year < 50 { 2000 } else { 1900 };
            NaiveDateTime::parse_from_str(
                &format!("{day_month}-{year} {time}"),
                "%d-%b-%Y %H:%M:%S GMT",
            )
            .ok()
            .map(|date| date.and_utc())
        })
}

fn fallback_delay(index: u32) -> Duration {
    let baseline = 500 * (1u64 << index.min(4));
    let jitter = (uuid::Uuid::new_v4().as_u128() % u128::from(baseline / 4 + 1)) as u64;
    Duration::from_millis(baseline - jitter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[tokio::test]
    async fn provider_retries_use_only_configured_attempts_and_honor_http_hints() {
        let policy = ProviderRetry {
            max_retries: 2,
            ..ProviderRetry::default()
        };
        for (status, hints, expected_calls) in [
            (429, vec![], 3),
            (600, vec![], 3),
            (429, vec!["false"], 1),
            (400, vec!["true"], 3),
            (400, vec!["true", "true"], 1),
            (200, vec!["true"], 1),
        ] {
            let mut calls = 0;
            let mut headers = HeaderMap::new();
            headers.insert("retry-after-ms", HeaderValue::from_static("0"));
            for hint in hints {
                headers.append("x-should-retry", HeaderValue::from_static(hint));
            }
            let response = policy
                .send(
                    || {
                        calls += 1;
                        std::future::ready(Ok((status, headers.clone())))
                    },
                    |response| (response.0, &response.1),
                )
                .await
                .unwrap();
            assert_eq!(response.0, status);
            assert_eq!(calls, expected_calls);
        }
        let status = ProviderRetry::default()
            .send(
                async || Ok((429, HeaderMap::new())),
                |response| (response.0, &response.1),
            )
            .await
            .unwrap();
        assert_eq!(status.0, 429);
    }

    #[test]
    fn retry_delays_parse_numeric_prefixes_and_enforce_the_server_delay_ceiling() {
        let policy = ProviderRetry::default();
        let now = "2026-09-11T12:01:00Z".parse::<DateTime<Utc>>().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "retry-after",
            HeaderValue::from_static("Friday, 11-Sep-26 12:01:30 GMT"),
        );
        assert_eq!(
            policy.delay(&headers, 0, now).unwrap(),
            Duration::from_secs(30)
        );
        headers.insert("retry-after", HeaderValue::from_static("1.5, 2"));
        assert_eq!(
            policy.delay(&headers, 0, now).unwrap(),
            Duration::from_millis(1500)
        );
        headers.insert("retry-after-ms", HeaderValue::from_static("12.5ms"));
        assert_eq!(
            policy.delay(&headers, 0, now).unwrap(),
            Duration::from_micros(12500)
        );
        headers.insert("retry-after-ms", HeaderValue::from_static("6.001e4ms"));
        assert!(
            matches!(policy.delay(&headers, 0, now), Err(Error::RetryableModel(message)) if message.contains("retry delay"))
        );
        let unlimited = ProviderRetry {
            max_retry_delay_ms: 0,
            ..policy
        };
        assert_eq!(
            unlimited.delay(&headers, 0, now).unwrap(),
            Duration::from_millis(60010)
        );
    }
}
