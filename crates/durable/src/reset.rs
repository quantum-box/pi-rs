//! Pure reset and handoff primitives for a host-managed durable transcript.
//!
//! Upstream reference: `packages/durable` at 827383a15262905f82e418483825a49dbb1adbf5.
//! A reset is an append-only `pi.reset` entry whose head is itself. The host
//! must atomically commit the entry, inbox placement, and run settlement before
//! publishing them. These functions neither store data nor schedule work.
//!
//! The host owns entry IDs and must retain every committed entry in order. A
//! context is derived from the latest reset, rather than deleting history.
//! Other head markers (such as compaction) and context edits must be handled
//! by the host's full context implementation; this module only models resets.

use pi_rs_core::Message;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A persistent reset marker. `None` and `Some("")` are distinct: the latter
/// contributes an empty user message, exactly like upstream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reset {
    pub id: Uuid,
    pub handoff: Option<String>,
}

impl Reset {
    /// Construct a self-headed marker using a host-provided unique entry ID.
    pub fn new(id: Uuid, handoff: Option<String>) -> Self {
        Self { id, handoff }
    }

    /// The marker's own contribution, absent for a plain reset.
    pub fn message(&self) -> Option<Message> {
        self.handoff.as_ref().map(|content| Message::User {
            content: content.clone(),
        })
    }
}

/// The subset of an immutable transcript needed for reset filtering. An entry
/// can contribute multiple messages, or none (for example a display-only write).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Entry {
    Messages(Vec<Message>),
    /// Messages from a freshly rendered pi.system entry.
    SystemBaseline(Vec<Message>),
    Reset(Reset),
}

/// Produce the model messages for the current reset range without changing the
/// transcript. After a reset the host must append the freshly rendered system
/// baseline as an entry; an old system message is not carried across a reset.
/// A `SystemBaseline` entry after a reset identifies the freshly rendered baseline;
/// its first system message can move to the front even past non-user messages.
/// Other first system messages move only past users.
/// Hosts must use their full context implementation for other system entries.
pub fn context(entries: &[Entry]) -> Vec<Message> {
    let reset = entries.iter().rposition(|e| matches!(e, Entry::Reset(_)));
    let start = reset.unwrap_or(0);
    let mut messages = Vec::new();
    let mut baseline = None;
    let mut saw_baseline = false;
    for entry in &entries[start..] {
        match entry {
            Entry::Messages(m) => messages.extend_from_slice(m),
            Entry::SystemBaseline(m) => {
                if !saw_baseline && reset.is_some() {
                    saw_baseline = true;
                    baseline = m
                        .iter()
                        .position(|m| matches!(m, Message::System { .. }))
                        .map(|i| messages.len() + i);
                }
                messages.extend_from_slice(m);
            }
            Entry::Reset(r) => messages.extend(r.message()),
        }
    }
    if let Some(index) = messages
        .iter()
        .position(|m| matches!(m, Message::System { .. }))
        && (baseline == Some(index)
            || messages[..index]
                .iter()
                .all(|m| matches!(m, Message::User { .. } | Message::UserWithImages { .. })))
    {
        let system = messages.remove(index);
        messages.insert(0, system);
    }
    messages
}

/// Tool controls in original call order, including a default for failed or
/// missing results. Completion order must not be used for this selection.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolControl {
    /// Tool names to enable at the end of this round, in call order.
    pub add_tools: Vec<String>,
    pub handoff: Option<String>,
    pub terminate: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoundDecision {
    /// Append this reset before placing any queued boundary items.
    pub handoff: Option<Reset>,
    pub terminate: bool,
}

/// Last defined handoff wins, even when empty. Termination requires every call
/// to request it (and at least one call), unless an after-tools task requests it.
/// The supplied ID is used only if a handoff exists.
pub fn tool_round(
    controls: &[ToolControl],
    task_terminates: bool,
    reset_id: Uuid,
) -> RoundDecision {
    RoundDecision {
        handoff: controls
            .iter()
            .rev()
            .find_map(|c| c.handoff.as_ref())
            .map(|text| Reset::new(reset_id, Some(text.clone()))),
        terminate: task_terminates
            || (!controls.is_empty() && controls.iter().all(|c| c.terminate)),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Boundary {
    PostTools,
    Final,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueMode {
    One,
    All,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ItemKind {
    Write,
    Reset,
    Steer,
    FollowUp,
}

/// Indexes identify items in an inbox already ordered by admission ID. Writes
/// include reset markers. The host places writes first, then selected inputs;
/// it should leave unselected items queued and reject stale head writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundaryDecision {
    pub writes: Vec<usize>,
    pub users: Vec<usize>,
    pub run: RunDisposition,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunDisposition {
    /// Continue the current run; selected steers join it.
    Continue,
    /// End the current run unanswered with reason `reset`.
    UnansweredReset,
    /// The current run has already completed; selected inputs start a new run.
    Finished,
}

/// Select items for the boundary after any tool handoff has been appended. A
/// queued reset upgrades a post-tools boundary to final, including follow-ups.
pub fn boundary(
    items: &[ItemKind],
    at: Boundary,
    steering: QueueMode,
    follow_up: QueueMode,
) -> BoundaryDecision {
    let reset = items.contains(&ItemKind::Reset);
    let final_boundary = at == Boundary::Final || reset;
    let writes = items
        .iter()
        .enumerate()
        .filter_map(|(i, k)| matches!(k, ItemKind::Write | ItemKind::Reset).then_some(i))
        .collect();
    let mut users = Vec::new();
    for (kind, mode) in [(ItemKind::Steer, steering), (ItemKind::FollowUp, follow_up)] {
        if kind == ItemKind::FollowUp && !final_boundary {
            continue;
        }
        let matches = items
            .iter()
            .enumerate()
            .filter_map(|(i, k)| (*k == kind).then_some(i));
        match mode {
            QueueMode::One => users.extend(matches.take(1)),
            QueueMode::All => users.extend(matches),
        }
    }
    users.sort_unstable();
    BoundaryDecision {
        writes,
        users,
        run: match (at, reset) {
            (Boundary::Final, _) => RunDisposition::Finished,
            (Boundary::PostTools, true) => RunDisposition::UnansweredReset,
            (Boundary::PostTools, false) => RunDisposition::Continue,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn user(s: &str) -> Message {
        Message::User { content: s.into() }
    }
    fn system(s: &str) -> Message {
        Message::System { content: s.into() }
    }
    fn reset(s: Option<&str>) -> Entry {
        Entry::Reset(Reset::new(Uuid::new_v4(), s.map(str::to_owned)))
    }

    #[test]
    fn reset_is_persistent_and_never_changes_history() {
        let history = vec![
            Entry::Messages(vec![system("old"), user("private")]),
            reset(None),
            Entry::Messages(vec![user("new")]),
        ];
        let saved = serde_json::to_vec(&history).unwrap();
        let restored: Vec<Entry> = serde_json::from_slice(&saved).unwrap();
        assert_eq!(context(&restored), vec![user("new")]);
        assert_eq!(history, restored);
        assert_eq!(serde_json::to_vec(&history).unwrap(), saved);
    }
    #[test]
    fn repeated_resets_and_fresh_system_baseline() {
        let entries = vec![
            reset(Some("first")),
            Entry::Messages(vec![user("before")]),
            reset(Some("")),
            Entry::Messages(vec![user("steer"), system("fresh")]),
        ];
        assert_eq!(
            context(&entries),
            vec![system("fresh"), user(""), user("steer")]
        );
        assert_eq!(context(&[reset(None)]), Vec::<Message>::new());
        assert_eq!(context(&[]), Vec::<Message>::new());
    }
    #[test]
    fn baseline_moves_past_retained_assistant_after_reset() {
        let assistant = Message::Assistant {
            content: "response".into(),
            calls: vec![],
            replay: None,
        };
        let entries = vec![
            reset(Some("handoff")),
            Entry::Messages(vec![assistant.clone()]),
            Entry::SystemBaseline(vec![system("baseline")]),
        ];
        assert_eq!(
            context(&entries),
            vec![system("baseline"), user("handoff"), assistant]
        );
    }
    #[test]
    fn handoff_last_in_call_order_and_termination() {
        let id = Uuid::new_v4();
        let controls = vec![
            ToolControl {
                handoff: Some("first".into()),
                terminate: true,
                ..Default::default()
            },
            ToolControl::default(),
            ToolControl {
                handoff: Some("".into()),
                terminate: true,
                ..Default::default()
            },
        ];
        let decision = tool_round(&controls, false, id);
        assert_eq!(decision.handoff, Some(Reset::new(id, Some("".into()))));
        assert!(!decision.terminate);
        assert!(tool_round(&controls, true, id).terminate);
        assert!(!tool_round(&[], false, id).terminate);
        assert!(
            tool_round(
                &[ToolControl {
                    terminate: true,
                    ..Default::default()
                }],
                false,
                id
            )
            .terminate
        );
    }
    #[test]
    fn reset_upgrades_boundary_and_writes_precede_queued_inputs() {
        let items = [
            ItemKind::FollowUp,
            ItemKind::Steer,
            ItemKind::Write,
            ItemKind::Reset,
            ItemKind::FollowUp,
            ItemKind::Steer,
        ];
        let d = boundary(&items, Boundary::PostTools, QueueMode::One, QueueMode::All);
        assert_eq!(d.writes, vec![2, 3]);
        assert_eq!(d.users, vec![0, 1, 4]);
        assert_eq!(d.run, RunDisposition::UnansweredReset);
        assert_eq!(
            boundary(&items, Boundary::Final, QueueMode::All, QueueMode::One).run,
            RunDisposition::Finished
        );
    }
    #[test]
    fn handoff_alone_does_not_select_followups_or_end_run() {
        let d = boundary(
            &[ItemKind::FollowUp, ItemKind::Write, ItemKind::Steer],
            Boundary::PostTools,
            QueueMode::All,
            QueueMode::All,
        );
        assert_eq!(d.writes, vec![1]);
        assert_eq!(d.users, vec![2]);
        assert_eq!(d.run, RunDisposition::Continue);
        let d = boundary(
            &[ItemKind::FollowUp, ItemKind::Steer],
            Boundary::Final,
            QueueMode::All,
            QueueMode::All,
        );
        assert_eq!(d.users, vec![0, 1]);
        assert_eq!(d.run, RunDisposition::Finished);
    }
}
