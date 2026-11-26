use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use super::{
    Event, ModelFailure, Session, context_edits, is_ecmascript_whitespace, retained_message_floor,
    turn_partial_failure,
};
use crate::{ContentBlock, ContextContent};

#[derive(Clone, Copy, PartialEq, Eq)]
enum StopReason {
    Stop,
    Length,
    ToolUse,
    Aborted,
    Error,
    Other,
}

impl StopReason {
    fn from_name(reason: Option<&str>) -> Self {
        match reason {
            Some("stop") => Self::Stop,
            Some("length") => Self::Length,
            Some("toolUse" | "tool_calls") => Self::ToolUse,
            Some("aborted" | "cancelled") => Self::Aborted,
            Some("error") => Self::Error,
            _ => Self::Other,
        }
    }

    fn from_failure(failure: ModelFailure) -> Self {
        match failure {
            ModelFailure::Cancelled => Self::Aborted,
            ModelFailure::Failed | ModelFailure::Retryable => Self::Error,
        }
    }
}

struct AssistantReply<'a> {
    text: Cow<'a, str>,
    stop: StopReason,
}

impl AssistantReply<'_> {
    fn text_for_selection(&self) -> Option<&str> {
        let text = self.text.trim_matches(is_ecmascript_whitespace);
        (self.stop == StopReason::Stop || !text.is_empty()).then_some(text)
    }
}

impl Session {
    /// Text of the latest qualifying assistant reply on the active branch.
    /// A normal stop with empty text takes precedence over older replies.
    /// Display-only partials are eligible here without entering model context.
    pub fn last_assistant_text(&self) -> Option<String> {
        let branch = self.branch();
        let retain_from = retained_message_floor(&branch);
        let edits = context_edits(&branch);
        let mut completions = BTreeMap::new();
        let mut assistant_requests = BTreeSet::new();
        for record in &branch {
            match &record.event {
                Event::ModelFinished {
                    request_id,
                    metadata,
                    failure,
                    ..
                } => {
                    completions.insert(*request_id, (metadata.finish_reason.as_deref(), *failure));
                }
                Event::Assistant {
                    request_id: Some(id),
                    ..
                } => {
                    assistant_requests.insert(*id);
                }
                _ => {}
            }
        }

        for record in branch.into_iter().rev().take_while(|r| r.id >= retain_from) {
            let (text, stop) = match &record.event {
                Event::Assistant {
                    content,
                    invocations,
                    request_id,
                    ..
                } => {
                    if let Some(original) = self.imported_assistant_message(record.id) {
                        // The importer concatenates the original text blocks.
                        // Pi's selector only reads blocks from assistant content.
                        let text = if original["content"].is_array() {
                            content.as_str()
                        } else {
                            ""
                        };
                        (text, StopReason::from_name(original["stopReason"].as_str()))
                    } else {
                        let (reason, failure) = request_id
                            .and_then(|id| completions.get(&id).copied())
                            .unwrap_or_default();
                        let stop = if let Some(failure) = failure {
                            StopReason::from_failure(failure)
                        } else if let Some(reason) = reason {
                            StopReason::from_name(Some(reason))
                        } else if invocations.is_empty() {
                            StopReason::Stop
                        } else {
                            StopReason::ToolUse
                        };
                        (content.as_str(), stop)
                    }
                }
                Event::ModelFinished {
                    request_id,
                    partial,
                    ..
                } if !assistant_requests.contains(request_id) => {
                    let Some(failure) = turn_partial_failure(self.records(), record) else {
                        continue;
                    };
                    (partial.as_str(), StopReason::from_failure(failure))
                }
                _ => continue,
            };
            let text = match edits.get(&record.id) {
                Some(None) => continue,
                Some(Some(ContextContent::Text(text))) => Cow::Borrowed(text.as_str()),
                Some(Some(ContextContent::Blocks(blocks))) => Cow::Owned(
                    blocks
                        .iter()
                        .filter_map(|block| match block {
                            ContentBlock::Text { text, .. } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect(),
                ),
                None => Cow::Borrowed(text),
            };
            let reply = AssistantReply { text, stop };
            if let Some(text) = reply.text_for_selection() {
                return (!text.is_empty()).then(|| text.to_owned());
            }
        }
        None
    }
}
