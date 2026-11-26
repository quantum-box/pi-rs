use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use super::{
    Event, ModelFailure, Session, context_edits, is_ecmascript_whitespace, retained_message_floor,
    turn_partial_failure,
};
use crate::{ContentBlock, ContextContent};

/// Normalize native stops from the original reply. An edit can change its text
/// or calls, but cannot change how the turn ended.
#[doc(hidden)]
pub fn native_assistant_stop_reason(
    finish_reason: Option<&str>,
    failure: Option<ModelFailure>,
    has_calls: bool,
) -> &'static str {
    match (failure, finish_reason, has_calls) {
        (Some(ModelFailure::Cancelled), _, _) => "aborted",
        (Some(ModelFailure::Failed | ModelFailure::Retryable), _, _) => "error",
        (None, Some("length"), _) => "length",
        (None, _, true) => "toolUse",
        (None, _, false) => "stop",
    }
}

/// A retained assistant reply before tool-pair repair for model context.
#[doc(hidden)]
pub struct AssistantTranscriptEntry<'a> {
    pub record_id: u64,
    pub stop_reason: Option<&'a str>,
    content: &'a str,
    replacement: Option<&'a ContextContent>,
}

impl AssistantTranscriptEntry<'_> {
    pub fn is_edited(&self) -> bool {
        self.replacement.is_some()
    }

    pub fn text(&self) -> Cow<'_, str> {
        match self.replacement {
            Some(ContextContent::Text(text)) => Cow::Borrowed(text.as_str()),
            Some(ContextContent::Blocks(blocks)) => Cow::Owned(
                blocks
                    .iter()
                    .filter_map(|block| match block {
                        ContentBlock::Text { text, .. } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect(),
            ),
            None => Cow::Borrowed(self.content),
        }
    }
}

impl Session {
    /// Text of the latest qualifying assistant reply on the active branch.
    /// A normal stop with empty text takes precedence over older replies.
    /// Display-only partials are eligible here without entering model context.
    pub fn last_assistant_text(&self) -> Option<String> {
        for reply in self.assistant_transcript().into_iter().rev() {
            let text = reply.text();
            let text = text.trim_matches(is_ecmascript_whitespace);
            if reply.stop_reason == Some("stop") || !text.is_empty() {
                return (!text.is_empty()).then(|| text.to_owned());
            }
        }
        None
    }

    /// Assistant entries for display, including replies elided from model context.
    #[doc(hidden)]
    pub fn assistant_transcript(&self) -> Vec<AssistantTranscriptEntry<'_>> {
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

        branch
            .into_iter()
            .filter(|record| record.id >= retain_from)
            .filter_map(|record| {
                let replacement = match edits.get(&record.id) {
                    Some(None) => return None,
                    Some(Some(replacement)) => Some(replacement),
                    None => None,
                };
                let (content, stop_reason) = match &record.event {
                    Event::Assistant {
                        content,
                        invocations,
                        request_id,
                        ..
                    } => {
                        if let Some(original) = self.imported_assistant_message(record.id) {
                            // The importer concatenates the original text blocks.
                            let text = if original["content"].is_array() {
                                content.as_str()
                            } else {
                                ""
                            };
                            (text, original["stopReason"].as_str())
                        } else {
                            let (reason, failure) = request_id
                                .and_then(|id| completions.get(&id).copied())
                                .unwrap_or_default();
                            let stop = native_assistant_stop_reason(
                                reason,
                                failure,
                                !invocations.is_empty(),
                            );
                            (content.as_str(), Some(stop))
                        }
                    }
                    Event::ModelFinished {
                        request_id,
                        partial,
                        ..
                    } if !assistant_requests.contains(request_id) => {
                        let failure = turn_partial_failure(self.records(), record)?;
                        (
                            partial.as_str(),
                            Some(native_assistant_stop_reason(None, Some(failure), false)),
                        )
                    }
                    _ => return None,
                };
                Some(AssistantTranscriptEntry {
                    record_id: record.id,
                    stop_reason,
                    content,
                    replacement,
                })
            })
            .collect()
    }
}
