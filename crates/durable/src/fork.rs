//! Extraction of historical fork points from committed observer snapshots.
//!
//! The runtime must atomically append a complete `Snapshot` with each state
//! transition. Existing observer records contain only messages and submissions;
//! they cannot establish quiescence or restore state and are rejected. A cursor
//! identifies a *commit*, not an index into its messages. In particular, never
//! truncate a later snapshot: its documents and configuration may have changed.
//! The caller must read the record from the intended conversation's stream and
//! atomically create the child, with a fresh identity and without live work.

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
};

use pi_rs_core::{ExtensionState, Message};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    observer::{Cursor, Observer, Record},
    runtime::{Config, Status, Submission},
};

/// A versioned, complete snapshot, captured in the same commit as the state.
/// `documents` contains conversation-scoped document values as of this commit,
/// including only documents that exist at this point. The producer must not
/// substitute current values or omit documents that exist at the commit.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub version: u32,
    pub conversation_id: Uuid,
    pub config: Config,
    pub messages: Vec<Message>,
    pub submissions: Vec<Submission>,
    pub tool_states: HashMap<String, ExtensionState>,
    pub documents: HashMap<String, Value>,
    pub active: Option<Uuid>,
    #[serde(default)]
    pub pending_resets: Vec<crate::reset::Reset>,
    /// The runtime's serialized phase; only the exact `Idle` variant is safe.
    pub phase: Value,
}

/// Child seed. The runtime must allocate a new conversation identity, start it
/// idle, and must not inherit submissions, active tasks or provider identity.
#[derive(Clone, Debug)]
pub struct ForkPoint {
    pub parent_id: Uuid,
    pub cursor: Cursor,
    pub config: Config,
    pub messages: Vec<Message>,
    pub tool_states: HashMap<String, ExtensionState>,
    pub documents: HashMap<String, Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("observation: {0}")]
    Observer(#[from] crate::observer::Error),
    #[error("fork cursor must identify an existing record")]
    InvalidCursor,
    #[error("record is not a supported complete fork snapshot")]
    InvalidSnapshot,
    #[error("snapshot belongs to a different conversation")]
    WrongConversation,
    #[error("snapshot is not settled")]
    Unsettled,
    #[error("snapshot contains an incomplete or invalid tool transcript")]
    InvalidTranscript,
}

/// Read exactly the selected commit, including historical commits. The caller
/// must construct `observer` for `parent_id`; observer cursors are stream-local.
pub fn read(observer: &Observer<'_>, parent_id: Uuid, cursor: Cursor) -> Result<ForkPoint, Error> {
    let previous = cursor.0.checked_sub(1).ok_or(Error::InvalidCursor)?;
    let page = match observer.read(Cursor(previous), NonZeroUsize::MIN) {
        Err(crate::observer::Error::FutureCursor) => return Err(Error::InvalidCursor),
        other => other?,
    };
    let record = page
        .records
        .first()
        .filter(|r| r.cursor == cursor)
        .ok_or(Error::InvalidCursor)?;
    extract(parent_id, record)
}

/// Extract a seed without consulting newer state. Legacy/incomplete snapshots
/// fail closed. A settled phase alone is insufficient: validate submissions and
/// the complete tool-call/result structure as well.
pub fn extract(parent_id: Uuid, record: &Record) -> Result<ForkPoint, Error> {
    if record.cursor.0 == 0 {
        return Err(Error::InvalidCursor);
    }
    let snapshot: Snapshot =
        serde_json::from_value(record.value.clone()).map_err(|_| Error::InvalidSnapshot)?;
    if snapshot.version != 1 {
        return Err(Error::InvalidSnapshot);
    }
    if snapshot.conversation_id != parent_id {
        return Err(Error::WrongConversation);
    }
    if snapshot.active.is_some()
        || !snapshot.pending_resets.is_empty()
        || snapshot.phase != Value::String("Idle".into())
        || snapshot
            .submissions
            .iter()
            .any(|s| !matches!(s.status, Status::Done { .. }))
    {
        return Err(Error::Unsettled);
    }
    validate_transcript(&snapshot.messages)?;
    Ok(ForkPoint {
        parent_id,
        cursor: record.cursor,
        config: snapshot.config,
        messages: snapshot.messages,
        tool_states: snapshot.tool_states,
        documents: snapshot.documents,
    })
}

/// Extract a committed cut even while its parent is working. Only a complete
/// model context can seed an idle child; no parent work is copied.
pub fn extract_cut(parent_id: Uuid, record: &Record) -> Result<ForkPoint, Error> {
    if record.cursor.0 == 0 {
        return Err(Error::InvalidCursor);
    }
    let snapshot: Snapshot =
        serde_json::from_value(record.value.clone()).map_err(|_| Error::InvalidSnapshot)?;
    if snapshot.version != 1 {
        return Err(Error::InvalidSnapshot);
    }
    if snapshot.conversation_id != parent_id {
        return Err(Error::WrongConversation);
    }
    validate_context(&snapshot.messages)?;
    Ok(ForkPoint {
        parent_id,
        cursor: record.cursor,
        config: snapshot.config,
        messages: snapshot.messages,
        tool_states: snapshot.tool_states,
        documents: snapshot.documents,
    })
}

fn validate_transcript(messages: &[Message]) -> Result<(), Error> {
    validate_context(messages)?;
    if matches!(
        messages.last(),
        Some(Message::Tool { .. } | Message::User { .. } | Message::UserWithImages { .. })
    ) {
        return Err(Error::InvalidTranscript);
    }
    Ok(())
}

pub(crate) fn validate_context(messages: &[Message]) -> Result<(), Error> {
    let mut pending = HashSet::new();
    let mut seen = HashSet::new();
    let mut non_system = false;
    for message in messages {
        match message {
            Message::System { .. } if !non_system => continue,
            Message::System { .. } => return Err(Error::InvalidTranscript),
            Message::Tool { call_id, .. } => {
                if !pending.remove(call_id) {
                    return Err(Error::InvalidTranscript);
                }
            }
            Message::Assistant { calls, .. } => {
                if !pending.is_empty() {
                    return Err(Error::InvalidTranscript);
                }
                for call in calls {
                    if call.id.is_empty() || !seen.insert(call.id.clone()) {
                        return Err(Error::InvalidTranscript);
                    }
                    pending.insert(call.id.clone());
                }
            }
            Message::User { .. } | Message::UserWithImages { .. } => {
                if !pending.is_empty() {
                    return Err(Error::InvalidTranscript);
                }
            }
        }
        non_system = true;
    }
    if !pending.is_empty() {
        return Err(Error::InvalidTranscript);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use durable_store::Store;
    use pi_rs_core::ToolCall;
    use serde_json::json;

    fn snapshot(id: Uuid) -> Snapshot {
        Snapshot {
            version: 1,
            conversation_id: id,
            config: Config {
                model: "old".into(),
                system_prompt: "old prompt".into(),
                max_output_tokens: 42,
            },
            messages: vec![
                Message::User {
                    content: "question".into(),
                },
                Message::Assistant {
                    content: "answer".into(),
                    calls: vec![],
                    replay: None,
                },
            ],
            submissions: vec![],
            tool_states: HashMap::from([(
                "tool".into(),
                ExtensionState {
                    version: 1,
                    data: json!({"n":1}),
                },
            )]),
            documents: HashMap::from([("todos".into(), json!(["old"]))]),
            active: None,
            pending_resets: vec![],
            phase: json!("Idle"),
        }
    }
    fn append(observer: &Observer<'_>, store: &Store, value: Value) -> Cursor {
        let a = observer.prepare_append(&value).unwrap();
        store.commit(&a.checks, &a.mutations).unwrap();
        a.cursor
    }
    fn record(s: &Snapshot) -> Record {
        Record {
            cursor: Cursor(1),
            value: serde_json::to_value(s).unwrap(),
        }
    }
    fn call(id: &str) -> ToolCall {
        ToolCall {
            id: id.into(),
            name: "t".into(),
            arguments: "{}".into(),
        }
    }

    #[test]
    fn reads_selected_historical_snapshot_not_newest() {
        let db = Store::memory().unwrap();
        let id = Uuid::new_v4();
        let observer = Observer::new(&db, id);
        let old = snapshot(id);
        let cursor = append(&observer, &db, serde_json::to_value(&old).unwrap());
        let mut newer = old.clone();
        newer.config.model = "new".into();
        newer.documents.insert("todos".into(), json!(["new"]));
        newer.tool_states.get_mut("tool").unwrap().data = json!({"n":2});
        append(&observer, &db, serde_json::to_value(newer).unwrap());
        let point = read(&observer, id, cursor).unwrap();
        assert_eq!(point.config.model, "old");
        assert_eq!(point.config.system_prompt, "old prompt");
        assert_eq!(point.documents["todos"], json!(["old"]));
        assert_eq!(point.tool_states["tool"].data, json!({"n":1}));
        assert_eq!(point.messages, old.messages);
        assert!(matches!(
            read(&observer, id, Cursor(0)),
            Err(Error::InvalidCursor)
        ));
        assert!(matches!(
            read(&observer, id, Cursor(3)),
            Err(Error::InvalidCursor)
        ));
    }
    #[test]
    fn fails_closed_for_legacy_wrong_parent_and_unsettled_state() {
        let id = Uuid::new_v4();
        let mut s = snapshot(id);
        assert!(matches!(
            extract(
                id,
                &Record {
                    cursor: Cursor(1),
                    value: json!({"messages":[],"submissions":[]})
                }
            ),
            Err(Error::InvalidSnapshot)
        ));
        assert!(matches!(
            extract(Uuid::new_v4(), &record(&s)),
            Err(Error::WrongConversation)
        ));
        s.phase = json!({"Tools":{"index":1}});
        assert!(matches!(extract(id, &record(&s)), Err(Error::Unsettled)));
        s.phase = json!("Idle");
        s.active = Some(Uuid::new_v4());
        assert!(matches!(extract(id, &record(&s)), Err(Error::Unsettled)));
        s.active = None;
        s.submissions.push(Submission {
            kind: crate::runtime::SubmissionKind::Input,
            id: Uuid::new_v4(),
            request_id: None,
            content: "q".into(),
            image_notes: Vec::new(),
            status: Status::Queued,
        });
        assert!(matches!(extract(id, &record(&s)), Err(Error::Unsettled)));
        s.submissions[0].status = Status::Unanswered {
            reason: "failed".into(),
        };
        assert!(matches!(extract(id, &record(&s)), Err(Error::Unsettled)));
    }
    #[test]
    fn active_cut_requires_complete_tool_batch() {
        let id = Uuid::new_v4();
        let mut s = snapshot(id);
        s.active = Some(Uuid::new_v4());
        s.phase = json!({"Tools": {"index": 0}});
        s.messages.push(Message::Assistant {
            content: String::new(),
            calls: vec![call("a"), call("b")],
            replay: None,
        });
        s.messages.push(Message::Tool {
            call_id: "a".into(),
            content: "done".into(),
        });
        assert!(matches!(
            extract_cut(id, &record(&s)),
            Err(Error::InvalidTranscript)
        ));
        s.messages.push(Message::Tool {
            call_id: "b".into(),
            content: "done".into(),
        });
        assert!(extract_cut(id, &record(&s)).is_ok());
        s.messages.push(Message::User {
            content: "next".into(),
        });
        assert!(extract_cut(id, &record(&s)).is_ok());
    }

    #[test]
    fn rejects_partial_and_malformed_tool_batches_even_if_marked_idle() {
        let id = Uuid::new_v4();
        let mut s = snapshot(id);
        s.messages = vec![
            Message::Assistant {
                content: "".into(),
                calls: vec![call("a"), call("b")],
                replay: None,
            },
            Message::Tool {
                call_id: "a".into(),
                content: "ok".into(),
            },
        ];
        assert!(matches!(
            extract(id, &record(&s)),
            Err(Error::InvalidTranscript)
        ));
        s.messages.push(Message::Tool {
            call_id: "b".into(),
            content: "ok".into(),
        });
        assert!(matches!(
            extract(id, &record(&s)),
            Err(Error::InvalidTranscript)
        )); // awaiting model
        s.messages.push(Message::Assistant {
            content: "done".into(),
            calls: vec![],
            replay: None,
        });
        assert!(extract(id, &record(&s)).is_ok());
        s.messages.push(Message::Tool {
            call_id: "b".into(),
            content: "duplicate".into(),
        });
        assert!(matches!(
            extract(id, &record(&s)),
            Err(Error::InvalidTranscript)
        ));
    }
}
