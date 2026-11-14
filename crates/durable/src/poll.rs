//! Read-only polling of an already accepted external tool job.
//!
//! The caller must have durably stored the reference and operation ID before
//! calling. Poll never invokes the original tool. An unavailable poll method,
//! cancellation or timeout leaves the remote job's outcome uncertain.
use pi_rs_core::{CancellationToken, Outcome, Tool, ToolContext};
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub enum PollResult {
    Outcome(Outcome),
    Unavailable,
    Cancelled,
    TimedOut,
}

pub async fn poll_once(
    tool: &dyn Tool,
    operation_id: Uuid,
    reference: &str,
    cancel: &CancellationToken,
    timeout: Duration,
) -> PollResult {
    if !tool.can_poll() {
        return PollResult::Unavailable;
    }
    if cancel.is_cancelled() {
        return PollResult::Cancelled;
    }
    let child = cancel.child_token();
    let _guard = child.clone().drop_guard();
    let context = ToolContext {
        operation_id,
        cancellation: child,
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => PollResult::Cancelled,
        result = tokio::time::timeout(timeout, tool.poll(reference, context)) => match result {
            Ok(Some(outcome)) => PollResult::Outcome(outcome),
            Ok(None) => PollResult::Unavailable,
            Err(_) => PollResult::TimedOut,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use pi_rs_core::ToolSpec;
    use serde_json::{Value, json};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct TestTool {
        invoked: AtomicUsize,
        polled: AtomicUsize,
        hangs: bool,
    }
    #[async_trait]
    impl Tool for TestTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "job".into(),
                description: "".into(),
                parameters: json!({}),
            }
        }
        fn can_poll(&self) -> bool {
            true
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            self.invoked.fetch_add(1, Ordering::SeqCst);
            Outcome::Succeeded("unexpected".into())
        }
        async fn poll(&self, reference: &str, _: ToolContext) -> Option<Outcome> {
            self.polled.fetch_add(1, Ordering::SeqCst);
            if self.hangs {
                std::future::pending().await
            } else {
                Some(Outcome::Succeeded(reference.into()))
            }
        }
    }
    #[tokio::test]
    async fn poll_is_read_only_and_reports_terminal() {
        let tool = TestTool {
            invoked: AtomicUsize::new(0),
            polled: AtomicUsize::new(0),
            hangs: false,
        };
        let result = poll_once(
            &tool,
            Uuid::new_v4(),
            "job-1",
            &CancellationToken::new(),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(
            result,
            PollResult::Outcome(Outcome::Succeeded("job-1".into()))
        );
        assert_eq!(tool.invoked.load(Ordering::SeqCst), 0);
        assert_eq!(tool.polled.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn timeout_is_not_a_terminal_result() {
        let tool = TestTool {
            invoked: AtomicUsize::new(0),
            polled: AtomicUsize::new(0),
            hangs: true,
        };
        let result = poll_once(
            &tool,
            Uuid::new_v4(),
            "job-1",
            &CancellationToken::new(),
            Duration::from_millis(1),
        )
        .await;
        assert_eq!(result, PollResult::TimedOut);
        assert_eq!(tool.invoked.load(Ordering::SeqCst), 0);
    }
}
