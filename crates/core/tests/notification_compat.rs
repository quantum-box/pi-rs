use pi_rs_core::Notification;

// A downstream exhaustive match written against the existing public enum must
// continue to compile without an extra metadata variant.
fn exhaustive(event: Notification<'_>) {
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
    exhaustive(Notification::SummaryRequest);
}
