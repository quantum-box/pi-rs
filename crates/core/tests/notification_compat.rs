use pi_rs_core::{AgentEvent, Notification};

// A downstream exhaustive match written against the existing public enum must
// continue to compile without an extra metadata variant.
fn exhaustive_notification(event: Notification<'_>) {
    match event {
        Notification::RunStarted { .. }
        | Notification::RunEnded { .. }
        | Notification::Retry(_)
        | Notification::Committed(_)
        | Notification::BranchState(_)
        | Notification::SummaryRequest
        | Notification::QueueUpdated { .. }
        | Notification::ToolInventory { .. }
        | Notification::ModelContext { .. }
        | Notification::Request(_)
        | Notification::Text(_)
        | Notification::Reasoning(_)
        | Notification::ToolCallDelta(_)
        | Notification::ToolProgress(_)
        | Notification::ExtensionLifecycleError { .. } => {}
    }
}

#[test]
fn downstream_exhaustive_notification_match_compiles() {
    exhaustive_notification(Notification::SummaryRequest);
}

#[test]
fn downstream_exhaustive_agent_event_match_compiles() {
    fn exhaustive(event: AgentEvent<'_>) {
        match event {
            AgentEvent::Notification(event) => exhaustive_notification(event),
            AgentEvent::AutoCompaction { .. }
            | AgentEvent::TextStart { .. }
            | AgentEvent::TextDelta { .. }
            | AgentEvent::TextEnd { .. }
            | AgentEvent::ReasoningStart { .. }
            | AgentEvent::ReasoningDelta { .. }
            | AgentEvent::ReasoningEnd { .. } => {}
        }
    }
    exhaustive(AgentEvent::Notification(Notification::SummaryRequest));
}
