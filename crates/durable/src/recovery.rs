//! Decisions made when a task was interrupted after its intent was committed.
//!
//! Decisions are separate from side effects: callers must commit the selected
//! transition before dispatching work or reporting a result.

use serde::{Deserialize, Serialize};

/// A tool may opt into replay only if re-execution is safe for its effects.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplayPolicy {
    Safe,
    #[default]
    Unsafe,
}

/// Handling of an interrupted tool that cannot safely be replayed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UncertainPolicy {
    /// Follow Pi Durable: deliver an interruption result to the model.
    #[default]
    InterruptedResult,
    /// Preserve pi-rs's stricter host-owned reconciliation boundary.
    RequireReconciliation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolRecovery {
    Replay,
    Interrupted,
    Reconcile,
    RestartLimit,
}

/// The counter is incremented on recovery of each interrupted invocation and
/// reset only once that task commits a new checkpoint. A missing current tool
/// cannot be replayed. Both stored and current policy must permit replay.
pub fn recover_tool(
    stored: ReplayPolicy,
    current: Option<ReplayPolicy>,
    interrupted_count: u32,
    restart_limit: u32,
    uncertain: UncertainPolicy,
) -> ToolRecovery {
    let replayable = stored == ReplayPolicy::Safe && current == Some(ReplayPolicy::Safe);
    if !replayable && uncertain == UncertainPolicy::RequireReconciliation {
        return ToolRecovery::Reconcile;
    }
    if interrupted_count >= restart_limit {
        return ToolRecovery::RestartLimit;
    }
    if replayable {
        return ToolRecovery::Replay;
    }
    match uncertain {
        UncertainPolicy::InterruptedResult => ToolRecovery::Interrupted,
        UncertainPolicy::RequireReconciliation => ToolRecovery::Reconcile,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_requires_both_recorded_and_current_opt_in() {
        for stored in [ReplayPolicy::Safe, ReplayPolicy::Unsafe] {
            for current in [None, Some(ReplayPolicy::Safe), Some(ReplayPolicy::Unsafe)] {
                let result =
                    recover_tool(stored, current, 1, 5, UncertainPolicy::InterruptedResult);
                assert_eq!(
                    result == ToolRecovery::Replay,
                    stored == ReplayPolicy::Safe && current == Some(ReplayPolicy::Safe)
                );
            }
        }
    }

    #[test]
    fn uncertain_tools_follow_host_policy_and_stop_at_limit() {
        assert_eq!(
            recover_tool(
                ReplayPolicy::Unsafe,
                Some(ReplayPolicy::Safe),
                1,
                5,
                UncertainPolicy::RequireReconciliation
            ),
            ToolRecovery::Reconcile
        );
        assert_eq!(
            recover_tool(
                ReplayPolicy::Safe,
                Some(ReplayPolicy::Safe),
                5,
                5,
                UncertainPolicy::InterruptedResult
            ),
            ToolRecovery::RestartLimit
        );
        assert_eq!(
            recover_tool(
                ReplayPolicy::Unsafe,
                None,
                5,
                5,
                UncertainPolicy::RequireReconciliation
            ),
            ToolRecovery::Reconcile
        );
    }
}
