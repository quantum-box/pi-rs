//! Ephemeral continuation state for Anthropic Messages threads.
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use reqwest::{Response, StatusCode};
use serde_json::{Value, json};

const MAX_ENTRIES: usize = 10_000;

#[derive(Default)]
struct Store {
    generation: u64,
    sessions: HashMap<String, Session>,
}

struct Session {
    generation: u64,
    state: Option<State>,
}

enum State {
    Reply(Reply),
    Off { keep_beta: bool },
}

struct Reply {
    id: String,
    count: usize,
    fingerprint: String,
}

static SESSIONS: LazyLock<Mutex<Store>> = LazyLock::new(Mutex::default);

pub(crate) struct Attempt {
    key: String,
    generation: u64,
    pub(crate) body: Value,
}

impl Attempt {
    pub(crate) fn begin(key: String, body: &Value) -> Option<Self> {
        let (generation, state) = {
            let mut store = SESSIONS.lock().ok()?;
            if store
                .sessions
                .get(&key)
                .is_some_and(|session| matches!(session.state, Some(State::Off { .. })))
            {
                return None;
            }
            store.generation = store.generation.checked_add(1)?;
            let generation = store.generation;
            let previous = store.sessions.insert(
                key.clone(),
                Session {
                    generation,
                    state: None,
                },
            );
            if store.sessions.len() > MAX_ENTRIES
                && let Some(oldest) = store
                    .sessions
                    .iter()
                    .min_by_key(|(_, session)| session.generation)
                    .map(|(id, _)| id.clone())
            {
                store.sessions.remove(&oldest);
            }
            (generation, previous.and_then(|session| session.state))
        };
        let mut outgoing = create(body);
        if let Some(State::Reply(reply)) = state
            && let Some(messages) = body["messages"].as_array()
            && messages.len() > reply.count
            && prefix_fingerprint(body, &messages[..reply.count], None).as_ref()
                == Some(&reply.fingerprint)
        {
            outgoing["messages"] = json!(messages[reply.count..]);
            let object = outgoing.as_object_mut().expect("Anthropic request object");
            object.remove("system");
            object.remove("tools");
            outgoing["thread"] = json!({"type":"continue","previous_message_id":reply.id});
        }
        Some(Self {
            key,
            generation,
            body: outgoing,
        })
    }

    pub(crate) fn disable(self, keep_beta: bool) {
        self.store(State::Off { keep_beta });
    }

    pub(crate) fn record(self, full_body: &Value, id: String, assistant_blocks: Vec<Value>) {
        let Some(messages) = full_body["messages"].as_array() else {
            return;
        };
        if assistant_blocks.is_empty() {
            return;
        }
        let assistant = json!({"role":"assistant","content":assistant_blocks});
        if let Some(fingerprint) = prefix_fingerprint(full_body, messages, Some(&assistant)) {
            self.store(State::Reply(Reply {
                id,
                count: messages.len() + 1,
                fingerprint,
            }));
        }
    }

    fn store(self, state: State) {
        if let Ok(mut store) = SESSIONS.lock()
            && let Some(session) = store.sessions.get_mut(&self.key)
            && session.generation == self.generation
        {
            session.state = Some(state);
        }
    }
}

pub(crate) fn allows_beta(key: &str) -> bool {
    !SESSIONS.lock().ok().is_some_and(|store| {
        store
            .sessions
            .get(key)
            .is_some_and(|session| matches!(session.state, Some(State::Off { keep_beta: false })))
    })
}

/// Anthropic can report the dated release behind an unversioned model name.
pub(crate) fn reply_matches_model(requested: &str, resolved: &str) -> bool {
    resolved == requested
        || resolved
            .strip_prefix(requested)
            .and_then(|suffix| suffix.strip_prefix('-'))
            .is_some_and(|version| {
                version.len() == 8 && version.bytes().all(|byte| byte.is_ascii_digit())
            })
}

pub(crate) fn create(body: &Value) -> Value {
    let mut outgoing = body.clone();
    outgoing["thread"] = json!({"type":"create"});
    outgoing
}

fn prefix_fingerprint(body: &Value, messages: &[Value], reply: Option<&Value>) -> Option<String> {
    let settings = [
        "model",
        "system",
        "tools",
        "thinking",
        "output_config",
        "tool_choice",
        "fallbacks",
    ]
    .map(|key| &body[key]);
    let messages: Vec<_> = messages
        .iter()
        .chain(reply)
        .map(normalize_message)
        .collect();
    pi_rs_core::protected_model_fingerprint(&("anthropic-thread-prefix-v1", settings, messages))
}

fn normalize_message(message: &Value) -> Value {
    let mut message = message.clone();
    if let Some(text) = message["content"].as_str() {
        message["content"] = json!([{"type":"text","text":text}]);
    }
    if let Some(content) = message["content"].as_array_mut() {
        for block in content {
            if let Some(block) = block.as_object_mut() {
                block.remove("cache_control");
            }
        }
    }
    message
}

#[derive(Clone, Copy)]
pub(crate) enum Rejection {
    Restart(&'static str),
    Unsupported {
        reason: &'static str,
        keep_beta: bool,
    },
}

pub(crate) async fn read_error(mut response: Response) -> (Vec<u8>, bool) {
    const LIMIT: usize = 64 * 1024;
    let mut bytes = Vec::new();
    let complete = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while bytes.len() < LIMIT {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    bytes.extend_from_slice(&chunk[..chunk.len().min(LIMIT - bytes.len())]);
                }
                Ok(None) => return true,
                Err(_) => break,
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    (bytes, complete)
}

pub(crate) fn rejection(status: StatusCode, bytes: &[u8]) -> Option<Rejection> {
    let body = serde_json::from_slice::<Value>(bytes).ok();
    let error = body.as_ref().and_then(|body| body.get("error"));
    let code = error.and_then(|error| error["details"]["error_code"].as_str());
    const RESTART_CODES: &[&str] = &[
        "thread_not_found",
        "thread_already_continued",
        "thread_continue_in_flight",
        "thread_fingerprint_mismatch",
    ];
    if let Some(reason) = RESTART_CODES
        .iter()
        .copied()
        .find(|reason| Some(*reason) == code)
    {
        return Some(Rejection::Restart(reason));
    }
    match code {
        Some("thread_unsupported_request") => Some(Rejection::Unsupported {
            reason: "thread_unsupported_request",
            keep_beta: true,
        }),
        None if status == StatusCode::BAD_REQUEST => {
            let text = error
                .and_then(|error| error["message"].as_str())
                .or_else(|| body.as_ref().and_then(|body| body["message"].as_str()))
                .or_else(|| body.as_ref().and_then(Value::as_str))
                .or_else(|| {
                    body.is_none()
                        .then(|| std::str::from_utf8(bytes).ok())
                        .flatten()
                })?;
            if is_beta_rejection(text) {
                Some(Rejection::Unsupported {
                    reason: "thread_beta_unsupported",
                    keep_beta: false,
                })
            } else {
                text.contains("thread").then_some(Rejection::Unsupported {
                    reason: "400",
                    keep_beta: true,
                })
            }
        }
        _ => None,
    }
}

fn is_beta_rejection(message: &str) -> bool {
    let lowercase = message.to_ascii_lowercase();
    let Some((before, after)) = lowercase.split_once(super::MESSAGE_THREADS_BETA) else {
        return false;
    };
    let separator = |ch: char| ch.is_ascii_whitespace() || matches!(ch, ':' | '\'' | '"' | '`');
    let before = before.trim_end_matches(separator);
    let after = after.trim_start_matches(separator);
    let after = after
        .strip_prefix("is ")
        .or_else(|| after.strip_prefix("was "))
        .unwrap_or(after);
    let after = after
        .strip_prefix("an ")
        .or_else(|| after.strip_prefix("a "))
        .unwrap_or(after);
    [
        "unknown beta",
        "unsupported beta",
        "invalid beta",
        "unrecognized beta",
        "unrecognized anthropic api beta",
    ]
    .iter()
    .any(|prefix| before.ends_with(prefix))
        || [
            "not supported",
            "not recognized",
            "not enabled",
            "unsupported",
            "unknown",
            "invalid",
            "unrecognized",
        ]
        .iter()
        .any(|suffix| {
            after.strip_prefix(suffix).is_some_and(|rest| {
                rest.is_empty() || rest.starts_with(|ch: char| !ch.is_alphanumeric())
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reply_is_claimed_once_and_a_stale_dispatch_cannot_overwrite_newer_state() {
        let key = uuid::Uuid::new_v4().to_string();
        let user = json!({"role":"user","content":"hi"});
        let blocks = vec![json!({"type":"text","text":"hello"})];
        let full = json!({"model":"m","system":"system","messages":[user]});
        Attempt::begin(key.clone(), &full)
            .unwrap()
            .record(&full, "first".into(), blocks.clone());

        let next = json!({"model":"m","system":"system","messages":[user,
            {"role":"assistant","content":blocks},{"role":"user","content":"next"}]});
        let first = Attempt::begin(key.clone(), &next).unwrap();
        assert_eq!(first.body["thread"]["previous_message_id"], "first");
        let second = Attempt::begin(key.clone(), &next).unwrap();
        assert_eq!(second.body["thread"]["type"], "create");
        second.record(&next, "second".into(), blocks.clone());
        first.disable(true);

        let mut edited = next.clone();
        edited["system"] = json!("changed");
        edited["messages"].as_array_mut().unwrap().extend([
            json!({"role":"assistant","content":blocks}),
            json!({"role":"user","content":"last"}),
        ]);
        assert_eq!(
            Attempt::begin(key, &edited).unwrap().body["thread"]["type"],
            "create"
        );
    }

    #[test]
    fn only_explicit_beta_rejection_disables_the_beta_for_the_session() {
        let body = json!({"messages":[]});
        for beta_rejected in [false, true] {
            let key = uuid::Uuid::new_v4().to_string();
            Attempt::begin(key.clone(), &body)
                .unwrap()
                .disable(!beta_rejected);
            assert!(Attempt::begin(key.clone(), &body).is_none());
            assert_eq!(allows_beta(&key), !beta_rejected);
        }
        for message in [
            "unknown beta message-threads-2026-08-12",
            "message-threads-2026-08-12 is not supported",
            "message-threads-2026-08-12 is an unsupported beta",
            "message-threads-2026-08-12 was an unrecognized beta",
            "unrecognized Anthropic API beta: MESSAGE-THREADS-2026-08-12",
        ] {
            let body = serde_json::to_vec(&json!({"error":{"message":message}})).unwrap();
            assert!(matches!(
                rejection(StatusCode::BAD_REQUEST, &body),
                Some(Rejection::Unsupported {
                    reason: "thread_beta_unsupported",
                    keep_beta: false
                })
            ));
        }
    }

    #[test]
    fn only_thread_rejections_authorize_a_retry() {
        assert!(matches!(
            rejection(
                StatusCode::BAD_REQUEST,
                br#"{"error":{"message":"thread: Extra inputs are not permitted"}}"#
            ),
            Some(Rejection::Unsupported {
                reason: "400",
                keep_beta: true
            })
        ));
        assert!(matches!(
            rejection(
                StatusCode::BAD_REQUEST,
                b"thread: Extra inputs are not permitted"
            ),
            Some(Rejection::Unsupported {
                reason: "400",
                keep_beta: true
            })
        ));
        assert!(rejection(StatusCode::BAD_REQUEST,
            br#"{"error":{"message":"system example says thread","details":{"error_code":"invalid_request"}}}"#).is_none());
        assert!(matches!(
            rejection(
                StatusCode::BAD_REQUEST,
                br#"{"error":{"message":"invalid previous_message_id for message-threads-2026-08-12"}}"#
            ),
            Some(Rejection::Unsupported { keep_beta: true, .. })
        ));
    }
}
