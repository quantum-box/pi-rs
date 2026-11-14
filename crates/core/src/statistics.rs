//! Durable session statistics shared by frontends and embedded clients.
use crate::{Event, Session};
use serde::Serialize;

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionStats {
    pub session_id: uuid::Uuid,
    pub session_name: Option<String>,
    pub user_messages: u64,
    pub assistant_messages: u64,
    pub tool_calls: u64,
    pub tool_results: u64,
    pub total_messages: u64,
    pub tokens: TokenTotals,
    pub cost: Option<f64>,
}
impl SessionStats {
    fn add_usage(&mut self, usage: crate::Usage, metadata: Option<&crate::ModelMetadata>) {
        self.tokens.add(usage, metadata);
        let cost = metadata
            .and_then(|metadata| metadata.cost_usd.as_ref())
            .and_then(serde_json::Number::as_f64)
            .filter(|cost| cost.is_finite() && *cost >= 0.0);
        self.cost = self
            .cost
            .zip(cost)
            .map(|(total, cost)| total + cost)
            .filter(|cost| cost.is_finite());
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenTotals {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub total: u64,
}
impl TokenTotals {
    fn add(&mut self, usage: crate::Usage, metadata: Option<&crate::ModelMetadata>) {
        let cache_read = metadata
            .and_then(|value| value.cached_input_tokens)
            .unwrap_or(0);
        let cache_write = metadata
            .and_then(|value| value.cache_creation_input_tokens)
            .unwrap_or(0);
        self.input = self.input.saturating_add(
            usage
                .input_tokens
                .saturating_sub(cache_read.saturating_add(cache_write)),
        );
        self.output = self.output.saturating_add(usage.output_tokens);
        self.cache_read = self.cache_read.saturating_add(cache_read);
        self.cache_write = self.cache_write.saturating_add(cache_write);
        self.total = self
            .input
            .saturating_add(self.output)
            .saturating_add(self.cache_read)
            .saturating_add(self.cache_write);
    }
}

/// Durable session totals include branches and compacted history, as Pi does.
fn statistics(session: &Session) -> SessionStats {
    let records = session.records();
    let measured: std::collections::BTreeMap<_, _> = records
        .iter()
        .filter_map(|record| match &record.event {
            Event::ModelFinished {
                request_id,
                usage: crate::UsageReport::Measured { usage },
                metadata,
                ..
            } => Some((*request_id, (*usage, metadata))),
            _ => None,
        })
        .collect();
    let nested: std::collections::BTreeSet<_> = records
        .iter()
        .filter_map(|record| match &record.event {
            Event::ToolPlanned { invocation, .. } => Some(invocation.operation_id),
            _ => None,
        })
        .collect();
    let mut stats = SessionStats {
        session_id: session.session_id(),
        session_name: session.name().map(str::to_owned),
        cost: Some(0.0),
        ..SessionStats::default()
    };
    for (usage, metadata) in measured.values() {
        stats.add_usage(*usage, Some(metadata));
    }
    let mut pending_summary = None;
    for record in records {
        match &record.event {
            Event::User { .. } | Event::UserWithImages { .. } => {
                stats.user_messages += 1;
                stats.total_messages += 1;
            }
            Event::Assistant {
                invocations,
                usage,
                request_id,
                ..
            } => {
                stats.assistant_messages += 1;
                stats.total_messages += 1;
                stats.tool_calls = stats.tool_calls.saturating_add(invocations.len() as u64);
                if let Some(usage) = usage
                    && request_id.is_none_or(|id| !measured.contains_key(&id))
                {
                    stats.add_usage(*usage, None);
                }
                pending_summary = None;
            }
            Event::ToolFinished { operation_id, .. } if !nested.contains(operation_id) => {
                stats.tool_results += 1;
                stats.total_messages += 1;
            }
            Event::BashExecution { .. } => {
                stats.total_messages += 1;
            }
            Event::ModelRequested { request_id, .. } => {
                pending_summary = None;
                if request_id.is_none_or(|id| !measured.contains_key(&id)) {
                    stats.cost = None;
                }
            }
            Event::ModelFinished { usage, reply, .. } => {
                pending_summary = match (usage, reply) {
                    (crate::UsageReport::Measured { usage }, None) => Some(*usage),
                    _ => None,
                };
                if !matches!(usage, crate::UsageReport::Measured { .. }) {
                    stats.cost = None;
                }
            }
            Event::CacheWarm {
                usage, metadata, ..
            } => {
                stats.add_usage(*usage, Some(metadata));
            }
            Event::Compacted { .. } | Event::BranchSummary { .. } => {
                if let Some((usage, details)) = record.summary_usage(records) {
                    let counted = details
                        .as_ref()
                        .map_or(Some(usage) == pending_summary, |details| {
                            measured.contains_key(&details.request_id)
                        });
                    if !counted {
                        stats.add_usage(usage, details.as_ref().map(|details| &details.metadata));
                    }
                }
                pending_summary = None;
            }
            _ => {}
        }
    }
    stats
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextUsage {
    pub tokens: Option<u64>,
    pub context_window: u32,
    pub percent: Option<f64>,
}

impl Session {
    pub fn statistics(&self) -> SessionStats {
        statistics(self)
    }

    /// Current branch usage for a known context window. Zero means unknown.
    pub fn context_usage(&self, context_window: u32) -> Option<ContextUsage> {
        if context_window == 0 {
            return None;
        }
        let estimate = self.context_usage_estimate();
        let branch = self.branch();
        let last_compaction = branch
            .iter()
            .rposition(|record| matches!(record.event, Event::Compacted { .. }));
        // An edit can invalidate a usage-based estimate without removing the
        // post-compaction assistant that makes a fresh estimate meaningful.
        let has_post_compaction_usage = last_compaction.is_none_or(|index| {
            let projected: std::collections::HashSet<_> =
                crate::journal::conversation_entries_unrepaired(&branch, self.operations())
                    .into_iter()
                    .filter_map(|(id, message)| {
                        matches!(message, crate::Message::Assistant { .. }).then_some(id)
                    })
                    .collect();
            let completions: std::collections::HashMap<_, _> = branch
                .iter()
                .filter_map(|record| match &record.event {
                    Event::ModelFinished {
                        request_id,
                        metadata,
                        failure,
                        ..
                    } => Some((*request_id, (metadata, failure))),
                    _ => None,
                })
                .collect();
            branch[index + 1..].iter().any(|record| {
                let Event::Assistant {
                    usage: Some(usage),
                    request_id,
                    ..
                } = &record.event
                else {
                    return false;
                };
                if !projected.contains(&record.id) {
                    return false;
                }
                let completion = request_id.and_then(|id| completions.get(&id).copied());
                if completion.is_some_and(|(metadata, failure)| {
                    failure.is_some()
                        || matches!(
                            metadata.finish_reason.as_deref(),
                            Some("error" | "aborted" | "cancelled")
                        )
                }) {
                    return false;
                }
                let input = completion
                    .and_then(|(metadata, _)| metadata.provider_prompt_tokens)
                    .filter(|n| *n > 0)
                    .unwrap_or(usage.input_tokens);
                self.imported_context_total(record.id)
                    .unwrap_or_else(|| input.saturating_add(usage.output_tokens))
                    > 0
            })
        });
        let tokens = if !has_post_compaction_usage {
            None
        } else {
            Some(estimate.tokens)
        };
        Some(ContextUsage {
            tokens,
            context_window,
            percent: tokens.map(|tokens| tokens as f64 / f64::from(context_window) * 100.0),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ContextContent, MemoryStore, Usage};

    fn assistant(
        session: &mut Session,
        content: &str,
        reasoning: Option<&str>,
        usage: Usage,
    ) -> u64 {
        let request_id = uuid::Uuid::new_v4();
        session
            .append(Event::ModelFinished {
                request_id,
                metadata: crate::ModelMetadata {
                    reasoning: reasoning.map(str::to_owned),
                    ..Default::default()
                },
                usage: crate::UsageReport::Measured { usage },
                failure: None,
                partial: String::new(),
                reply: None,
            })
            .unwrap();
        session
            .append(Event::Assistant {
                content: content.into(),
                invocations: vec![],
                usage: Some(usage),
                request_id: Some(request_id),
            })
            .unwrap();
        session.head()
    }

    #[test]
    fn estimation_counts_current_system_and_thinking_after_edit() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append(Event::SystemCheckpoint {
                content: "abcdefgh".into(),
                sections: vec![("rules".into(), Some("1234".into()))],
                tools_added: vec![],
                tools_removed: vec![],
            })
            .unwrap();
        session
            .append(Event::User {
                content: "abcd".into(),
                input_id: None,
            })
            .unwrap();
        let user = session.head();
        assistant(
            &mut session,
            "text",
            Some("thinking"),
            Usage {
                input_tokens: 100,
                output_tokens: 5,
            },
        );
        session
            .edit_context(user, Some(ContextContent::Text("abcd".into())))
            .unwrap();
        // System: 12 chars = 3 tokens; user: 1; assistant text+thinking: 3.
        assert_eq!(session.context_usage_estimate().tokens, 7);
    }

    #[test]
    fn assistant_edit_estimates_latest_replacement_thinking_blocks() {
        use crate::ContentBlock;
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let target = assistant(
            &mut session,
            "old",
            Some("old reasoning"),
            Usage {
                input_tokens: 100,
                output_tokens: 5,
            },
        );
        session
            .edit_context(
                target,
                Some(ContextContent::Blocks(vec![
                    ContentBlock::Text {
                        text: "a".into(),
                        text_signature: None,
                    },
                    ContentBlock::Thinking {
                        thinking: "🦀".into(),
                        thinking_signature: None,
                        redacted: false,
                    },
                    ContentBlock::Thinking {
                        thinking: "bc".into(),
                        thinking_signature: None,
                        redacted: false,
                    },
                ])),
            )
            .unwrap();
        // One text unit plus four UTF-16 thinking units, rounded together.
        assert_eq!(session.context_usage_estimate().tokens, 2);
        session
            .edit_context(target, Some(ContextContent::Text("x".into())))
            .unwrap();
        assert_eq!(session.context_usage_estimate().tokens, 1);
        session
            .edit_context(
                target,
                Some(ContextContent::Blocks(vec![ContentBlock::Thinking {
                    thinking: "123456789".into(),
                    thinking_signature: None,
                    redacted: false,
                }])),
            )
            .unwrap();
        assert_eq!(session.context_usage_estimate().tokens, 3);
    }

    #[test]
    fn model_selection_does_not_invalidate_measured_usage() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        assistant(
            &mut session,
            "answer",
            None,
            Usage {
                input_tokens: 100,
                output_tokens: 5,
            },
        );
        session
            .append(Event::ModelSelected {
                model: "other".into(),
                thinking: None,
                provider: None,
            })
            .unwrap();
        assert_eq!(session.context_usage_estimate().tokens, 105);
        assert_eq!(session.context_budget_estimate().last_usage_record, None);
        session
            .append(Event::SystemCheckpoint {
                content: "extra".into(),
                sections: vec![],
                tools_added: vec![],
                tools_removed: vec![],
            })
            .unwrap();
        assert_eq!(session.context_usage_estimate().tokens, 107);
    }

    #[test]
    fn native_system_replacements_invalidate_and_snapshotless_compaction_preserves_system() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let checkpoint = |value: Option<&str>| Event::SystemCheckpoint {
            content: String::new(),
            sections: vec![("rules".into(), value.map(str::to_owned))],
            tools_added: vec![],
            tools_removed: vec![],
        };
        session.append(checkpoint(Some("12345678"))).unwrap();
        assistant(
            &mut session,
            "ok",
            None,
            Usage {
                input_tokens: 100,
                output_tokens: 5,
            },
        );
        session.append(checkpoint(Some("abcd"))).unwrap();
        let estimate = session.context_usage_estimate();
        assert_eq!(estimate.last_usage_record, None);
        assert_eq!(estimate.tokens, 2); // one system token and one assistant token
        session.append(checkpoint(None)).unwrap();
        assert_eq!(session.context_usage_estimate().tokens, 1);
        session.append(checkpoint(Some("12345678"))).unwrap();
        let before = session.head();
        session
            .append(Event::Compacted {
                details: None,
                from_extension: false,
                summary: "abcd".into(),
                system_prompt: None,
                retain_from: Some(before + 1),
                tokens_before: None,
                states: Default::default(),
                usage: None,
                usage_metadata: None,
            })
            .unwrap();
        assert_eq!(session.context_usage_estimate().tokens, 3); // preserved system plus summary
    }

    #[test]
    fn edited_away_assistant_does_not_leave_its_tool_result_in_estimate() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let op = uuid::Uuid::new_v4();
        session
            .append(Event::Assistant {
                content: String::new(),
                invocations: vec![crate::Invocation {
                    operation_id: op,
                    call: crate::ToolCall {
                        id: "call".into(),
                        name: "read".into(),
                        arguments: "{}".into(),
                    },
                }],
                usage: None,
                request_id: None,
            })
            .unwrap();
        let assistant = session.head();
        session
            .append(Event::ToolFinished {
                operation_id: op,
                outcome: crate::Outcome::Succeeded("x".repeat(4000)),
                states: Default::default(),
            })
            .unwrap();
        session.edit_context(assistant, None).unwrap();
        assert_eq!(session.context_usage_estimate().tokens, 0);
        assert!(session.current_messages().is_empty());
    }

    #[test]
    fn omitted_tool_result_removes_only_its_call_from_estimate() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let request_id = uuid::Uuid::new_v4();
        session
            .append(Event::ModelFinished {
                request_id,
                metadata: crate::ModelMetadata {
                    reasoning: Some("abcd".into()),
                    ..Default::default()
                },
                usage: crate::UsageReport::Unconfirmed,
                failure: None,
                partial: String::new(),
                reply: None,
            })
            .unwrap();
        let omitted = uuid::Uuid::new_v4();
        let retained = uuid::Uuid::new_v4();
        let call = |operation_id, id: &str, name: &str, arguments: String| crate::Invocation {
            operation_id,
            call: crate::ToolCall {
                id: id.into(),
                name: name.into(),
                arguments,
            },
        };
        session
            .append(Event::Assistant {
                content: "ok".into(),
                request_id: Some(request_id),
                usage: None,
                invocations: vec![
                    call(
                        omitted,
                        "drop",
                        "large",
                        format!("{{\"x\":\"{}\"}}", "x".repeat(4000)),
                    ),
                    call(retained, "keep", "read", "{}".into()),
                ],
            })
            .unwrap();
        session
            .append(Event::ToolFinished {
                operation_id: omitted,
                outcome: crate::Outcome::Succeeded("discard".into()),
                states: Default::default(),
            })
            .unwrap();
        let omitted_result = session.head();
        session
            .append(Event::ToolFinished {
                operation_id: retained,
                outcome: crate::Outcome::Succeeded("yes".into()),
                states: Default::default(),
            })
            .unwrap();
        session.edit_context(omitted_result, None).unwrap();
        let messages = session.current_messages();
        assert!(
            matches!(&messages[0], crate::Message::Assistant { calls, .. } if calls.len() == 1 && calls[0].id == "keep")
        );
        assert_eq!(messages.len(), 2);
        // Assistant text + thinking + retained call = 3; retained result = 1.
        assert_eq!(session.context_usage_estimate().tokens, 4);
    }

    #[test]
    fn omitted_only_tool_result_keeps_assistant_thinking_for_estimation() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let request_id = uuid::Uuid::new_v4();
        session
            .append(Event::ModelFinished {
                request_id,
                metadata: crate::ModelMetadata {
                    reasoning: Some("thinking".into()),
                    ..Default::default()
                },
                usage: crate::UsageReport::Unconfirmed,
                failure: None,
                partial: String::new(),
                reply: None,
            })
            .unwrap();
        let operation_id = uuid::Uuid::new_v4();
        session
            .append(Event::Assistant {
                content: String::new(),
                request_id: Some(request_id),
                usage: None,
                invocations: vec![crate::Invocation {
                    operation_id,
                    call: crate::ToolCall {
                        id: "call".into(),
                        name: "read".into(),
                        arguments: "{}".into(),
                    },
                }],
            })
            .unwrap();
        session
            .append(Event::ToolFinished {
                operation_id,
                outcome: crate::Outcome::Succeeded("result".into()),
                states: Default::default(),
            })
            .unwrap();
        let result = session.head();
        session.edit_context(result, None).unwrap();
        assert!(session.current_messages().is_empty());
        assert_eq!(session.context_usage_estimate().tokens, 2);
    }

    #[test]
    fn replacement_tool_calls_follow_repair_without_losing_thinking() {
        use crate::ContentBlock;
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let operation_id = uuid::Uuid::new_v4();
        session
            .append(Event::Assistant {
                content: "original".into(),
                request_id: None,
                usage: None,
                invocations: vec![crate::Invocation {
                    operation_id,
                    call: crate::ToolCall {
                        id: "keep".into(),
                        name: "read".into(),
                        arguments: "{}".into(),
                    },
                }],
            })
            .unwrap();
        let assistant = session.head();
        session
            .append(Event::ToolFinished {
                operation_id,
                outcome: crate::Outcome::Succeeded("yes".into()),
                states: Default::default(),
            })
            .unwrap();
        let keep = ContentBlock::ToolCall {
            id: "keep".into(),
            name: "read".into(),
            arguments: serde_json::json!({}),
            thought_signature: None,
        };
        let unmatched = ContentBlock::ToolCall {
            id: "missing".into(),
            name: "large".into(),
            arguments: serde_json::json!({"data":"x".repeat(4000)}),
            thought_signature: None,
        };
        let thinking = ContentBlock::Thinking {
            thinking: "abcd".into(),
            thinking_signature: None,
            redacted: false,
        };
        session
            .edit_context(
                assistant,
                Some(ContextContent::Blocks(vec![
                    ContentBlock::Text {
                        text: "ok".into(),
                        text_signature: None,
                    },
                    thinking.clone(),
                    keep,
                    unmatched.clone(),
                ])),
            )
            .unwrap();
        let messages = session.current_messages();
        assert!(
            matches!(&messages[0], crate::Message::Assistant { calls, .. } if calls.len() == 1 && calls[0].id == "keep")
        );
        assert_eq!(session.context_usage_estimate().tokens, 4);
        session
            .edit_context(
                assistant,
                Some(ContextContent::Blocks(vec![thinking, unmatched])),
            )
            .unwrap();
        assert!(session.current_messages().is_empty());
        assert_eq!(session.context_usage_estimate().tokens, 1);
    }

    #[test]
    fn removing_a_native_system_tool_invalidates_old_usage() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append(Event::SystemCheckpoint {
                content: String::new(),
                sections: vec![],
                tools_added: vec![crate::ToolSpec {
                    name: "read".into(),
                    description: "a long schema".into(),
                    parameters: serde_json::json!({"type":"object"}),
                }],
                tools_removed: vec![],
            })
            .unwrap();
        assistant(
            &mut session,
            "ok",
            None,
            Usage {
                input_tokens: 100,
                output_tokens: 5,
            },
        );
        session
            .append(Event::SystemCheckpoint {
                content: String::new(),
                sections: vec![],
                tools_added: vec![],
                tools_removed: vec!["read".into()],
            })
            .unwrap();
        let estimate = session.context_usage_estimate();
        assert_eq!(estimate.last_usage_record, None);
        assert_eq!(estimate.tokens, 1);
    }

    #[test]
    fn post_compaction_thinking_only_assistant_survives_unrelated_edit_for_usage() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append(Event::User {
                content: "old".into(),
                input_id: None,
            })
            .unwrap();
        let before = session.head();
        session
            .append(Event::Compacted {
                details: None,
                from_extension: false,
                summary: "summary".into(),
                system_prompt: None,
                retain_from: Some(before),
                tokens_before: None,
                states: Default::default(),
                usage: None,
                usage_metadata: None,
            })
            .unwrap();
        session
            .append(Event::User {
                content: "question".into(),
                input_id: None,
            })
            .unwrap();
        let question = session.head();
        assistant(
            &mut session,
            "",
            Some("thinking"),
            Usage {
                input_tokens: 100,
                output_tokens: 5,
            },
        );
        session
            .edit_context(question, Some(ContextContent::Text("changed".into())))
            .unwrap();
        assert!(session.context_usage(1000).unwrap().tokens.is_some());
        assert!(session.context_usage_estimate().tokens >= 2);
    }

    #[test]
    fn context_usage_after_compaction_and_later_edit_uses_fresh_estimate() {
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        session
            .append(Event::User {
                content: "before".into(),
                input_id: None,
            })
            .unwrap();
        let before = session.head();
        session
            .append(Event::Compacted {
                details: None,
                from_extension: false,
                summary: "summary".into(),
                system_prompt: None,
                retain_from: Some(before),
                tokens_before: None,
                states: Default::default(),
                usage: None,
                usage_metadata: None,
            })
            .unwrap();
        assert_eq!(session.context_usage(1000).unwrap().tokens, None);
        session
            .append(Event::User {
                content: "question".into(),
                input_id: None,
            })
            .unwrap();
        let question = session.head();
        session
            .append(Event::Assistant {
                content: "no usage".into(),
                invocations: vec![],
                usage: Some(Usage {
                    input_tokens: 0,
                    output_tokens: 0,
                }),
                request_id: None,
            })
            .unwrap();
        assert_eq!(session.context_usage(1000).unwrap().tokens, None);
        session
            .append(Event::Assistant {
                content: "answer".into(),
                invocations: vec![],
                usage: Some(Usage {
                    input_tokens: 100,
                    output_tokens: 10,
                }),
                request_id: None,
            })
            .unwrap();
        let assistant = session.head();
        assert_eq!(session.context_usage(1000).unwrap().tokens, Some(110));
        session
            .edit_context(
                question,
                Some(ContextContent::Text("changed question".into())),
            )
            .unwrap();
        let estimate = session.context_usage_estimate();
        assert_eq!(estimate.last_usage_record, None);
        assert_eq!(
            session.context_usage(1000).unwrap().tokens,
            Some(estimate.tokens)
        );
        session.edit_context(assistant, None).unwrap();
        assert_eq!(session.context_usage(1000).unwrap().tokens, None);
    }
}
