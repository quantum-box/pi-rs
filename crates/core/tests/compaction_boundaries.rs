use pi_rs_core::{
    Agent, BeforeSummaryCompactionResult, CancellationToken, Compaction, CompactionPreparation,
    Decision, Event, Extension, ExtensionCompaction, ExtensionResult, ExtensionState, Limits,
    Model, ModelRequest, ModelStream, Record, Registry, Result, Session, ToolHost,
};
use std::sync::Arc;
struct NoModel;
#[async_trait::async_trait]
impl Model for NoModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        panic!("model must not run")
    }
}
struct Supplied;
#[async_trait::async_trait]
impl Extension for Supplied {
    fn id(&self) -> &str {
        "supplied"
    }
    fn handles_before_summary_compaction(&self) -> bool {
        true
    }
    async fn before_summary_compaction(
        &self,
        p: &CompactionPreparation,
        _: Option<&ExtensionState>,
        _: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Option<BeforeSummaryCompactionResult>> {
        let users: Vec<_> = p
            .branch_entries
            .iter()
            .filter(|r| matches!(r.event, Event::User { .. }))
            .collect();
        Ok(Some(BeforeSummaryCompactionResult {
            cancel: false,
            compaction: Some(ExtensionCompaction {
                summary: "summary of first turn".into(),
                first_kept_entry: Some(users[1].id),
                tokens_before: 12,
                usage: None,
                details: None,
            }),
        }))
    }
}
struct Legacy;
#[async_trait::async_trait]
impl Extension for Legacy {
    fn id(&self) -> &str {
        "legacy"
    }
    fn before_compaction(
        &self,
        c: &mut Compaction,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<Decision> {
        c.keep_turns = 2;
        Ok(Decision::Allow)
    }
}
#[tokio::test]
async fn earlier_boundary_survives_legacy_increase() {
    let mut registry = Registry::default();
    registry.register(Arc::new(Supplied)).unwrap();
    registry.register(Arc::new(Legacy)).unwrap();
    let a = Agent {
        model: &NoModel,
        model_name: "test",
        registry: &registry,
        system_prompt: "",
        limits: Limits::default(),
    };
    let mut records = vec![Record {
        timestamp: None,
        run_id: None,
        version: 3,
        id: 1,
        parent: None,
        event: Event::Created {
            session_id: uuid::Uuid::new_v4(),
        },
    }];
    let mut ids = Vec::new();
    for n in 1..=4 {
        for event in [
            Event::User {
                content: format!("turn{n}"),
                input_id: None,
            },
            Event::Assistant {
                content: format!("reply{n}"),
                invocations: vec![],
                usage: None,
                request_id: None,
            },
        ] {
            let id = records.len() as u64 + 1;
            if matches!(event, Event::User { .. }) {
                ids.push(id)
            }
            records.push(Record {
                timestamp: None,
                run_id: None,
                version: 3,
                id,
                parent: Some(id - 1),
                event,
            })
        }
    }
    let mut s = Session::from_records(records).unwrap();
    a.summarize(&mut s, 1, &CancellationToken::new(), &mut |_| {})
        .await
        .unwrap();
    let event = &s.records().last().unwrap().event;
    assert!(
        matches!(event,Event::Compacted{retain_from:Some(id),..} if *id<=ids[1]),
        "extension's retained second turn disappeared"
    );
}
struct SplitImages;
#[async_trait::async_trait]
impl Extension for SplitImages {
    fn id(&self) -> &str {
        "split"
    }
    fn handles_before_summary_compaction(&self) -> bool {
        true
    }
    async fn before_summary_compaction(
        &self,
        p: &CompactionPreparation,
        _: Option<&ExtensionState>,
        _: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Option<BeforeSummaryCompactionResult>> {
        let b = p
            .branch_entries
            .iter()
            .find(|r| matches!(r.event, Event::UserImages { .. }))
            .unwrap()
            .id;
        Ok(Some(BeforeSummaryCompactionResult {
            cancel: false,
            compaction: Some(ExtensionCompaction {
                summary: "earlier text only".into(),
                first_kept_entry: Some(b),
                tokens_before: 0,
                usage: None,
                details: None,
            }),
        }))
    }
}
#[tokio::test]
async fn image_record_boundary_preserves_image() {
    let mut registry = Registry::default();
    registry.register(Arc::new(SplitImages)).unwrap();
    let a = Agent {
        model: &NoModel,
        model_name: "test",
        registry: &registry,
        system_prompt: "",
        limits: Limits::default(),
    };
    let events = vec![
        Event::Created {
            session_id: uuid::Uuid::new_v4(),
        },
        Event::User {
            content: "old".into(),
            input_id: None,
        },
        Event::Assistant {
            content: "reply".into(),
            invocations: vec![],
            usage: None,
            request_id: None,
        },
        Event::User {
            content: "image question".into(),
            input_id: None,
        },
        Event::UserImages {
            images: vec![pi_rs_core::ImageContent {
                data: "aQ==".into(),
                mime_type: "image/png".into(),
            }],
        },
        Event::Assistant {
            content: "saw it".into(),
            invocations: vec![],
            usage: None,
            request_id: None,
        },
        Event::User {
            content: "recent".into(),
            input_id: None,
        },
    ];
    let records = events
        .into_iter()
        .enumerate()
        .map(|(i, event)| Record {
            timestamp: None,
            run_id: None,
            version: 3,
            id: i as u64 + 1,
            parent: (i > 0).then_some(i as u64),
            event,
        })
        .collect();
    let mut s = Session::from_records(records).unwrap();
    let before = s.current_messages();
    let result = a
        .summarize(&mut s, 1, &CancellationToken::new(), &mut |_| {})
        .await;
    assert!(
        result.is_err(),
        "splitting a user from its images must be rejected"
    );
    assert_eq!(s.current_messages(), before);
    assert!(
        !s.records()
            .iter()
            .any(|r| matches!(r.event, Event::Compacted { .. }))
    );
}

struct SelectedBoundary(u64);
#[async_trait::async_trait]
impl Extension for SelectedBoundary {
    fn id(&self) -> &str {
        "selected-boundary"
    }
    fn handles_before_summary_compaction(&self) -> bool {
        true
    }
    async fn before_summary_compaction(
        &self,
        _: &CompactionPreparation,
        _: Option<&ExtensionState>,
        _: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Option<BeforeSummaryCompactionResult>> {
        Ok(Some(BeforeSummaryCompactionResult {
            cancel: false,
            compaction: Some(ExtensionCompaction {
                summary: "summary".into(),
                first_kept_entry: Some(self.0),
                tokens_before: 0,
                usage: None,
                details: None,
            }),
        }))
    }
}

#[tokio::test]
async fn boundaries_cannot_split_images_or_tool_exchanges() {
    use pi_rs_core::{ImageContent, Invocation, Message, Outcome, ToolCall};
    for (boundary, accepted) in [
        (4, true),
        (5, false),
        (6, false),
        (7, true),
        (8, false),
        (9, false),
        (10, true),
    ] {
        let operation_id = uuid::Uuid::new_v4();
        let events = vec![
            Event::Created {
                session_id: uuid::Uuid::new_v4(),
            },
            Event::User {
                content: "old".into(),
                input_id: None,
            },
            Event::Assistant {
                content: "old reply".into(),
                invocations: vec![],
                usage: None,
                request_id: None,
            },
            Event::User {
                content: "image question".into(),
                input_id: None,
            },
            Event::SourceEntry {
                format: "test:metadata".into(),
                entry: serde_json::Value::Null,
            },
            Event::UserImages {
                images: vec![ImageContent {
                    data: "aQ==".into(),
                    mime_type: "image/png".into(),
                }],
            },
            Event::Assistant {
                content: "tool".into(),
                invocations: vec![Invocation {
                    operation_id,
                    call: ToolCall {
                        id: "call".into(),
                        name: "fixture".into(),
                        arguments: "{}".into(),
                    },
                }],
                usage: None,
                request_id: None,
            },
            Event::ToolStarted {
                operation_id,
                tool_execution: None,
            },
            Event::ToolFinished {
                operation_id,
                outcome: Outcome::Succeeded("result".into()),
                states: Default::default(),
            },
            Event::User {
                content: "recent".into(),
                input_id: None,
            },
        ];
        let records = events
            .into_iter()
            .enumerate()
            .map(|(i, event)| Record {
                timestamp: None,
                run_id: None,
                version: 3,
                id: i as u64 + 1,
                parent: (i > 0).then_some(i as u64),
                event,
            })
            .collect();
        let mut session = Session::from_records(records).unwrap();
        let before = session.current_messages();
        let mut registry = Registry::default();
        registry
            .register(Arc::new(SelectedBoundary(boundary)))
            .unwrap();
        let agent = Agent {
            model: &NoModel,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "",
            limits: Limits::default(),
        };
        let result = agent
            .summarize(&mut session, 1, &CancellationToken::new(), &mut |_| {})
            .await;
        assert_eq!(result.is_ok(), accepted, "boundary {boundary}: {result:?}");
        if !accepted {
            assert_eq!(session.current_messages(), before, "boundary {boundary}");
            assert!(
                !session
                    .records()
                    .iter()
                    .any(|r| matches!(r.event, Event::Compacted { .. }))
            );
        } else if boundary == 4 {
            assert!(session.current_messages().iter().any(|message| matches!(message, Message::UserWithImages { content, .. } if content == "image question")));
        } else if boundary == 7 {
            let messages = session.current_messages();
            assert!(messages.iter().any(|message| matches!(message, Message::Assistant { calls, .. } if calls.iter().any(|call| call.id == "call"))));
            assert!(messages.iter().any(
                |message| matches!(message, Message::Tool { call_id, .. } if call_id == "call")
            ));
        }
    }
}
