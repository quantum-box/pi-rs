use crate::CancellationToken;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, RwLock};

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
