use futures_util::stream;
use pi_rs_core::{
    Agent, AgentEvent, CancellationToken, EnrichedModelEvent, EnrichedModelStream, Error, Limits,
    Model, ModelEvent, ModelRequest, ModelStream, Registry, Result, Session, Usage,
};
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
enum Kind {
    Text,
    Reasoning,
}

#[derive(Clone, Copy, Debug)]
enum FirstEvent {
    Start,
    Delta,
    End,
}

struct Items {
    count: u64,
    kind: Kind,
    first: FirstEvent,
}

#[async_trait::async_trait]
impl Model for Items {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        unreachable!("agent should request enriched events")
    }

    async fn stream_enriched_in_session_with_identity(
        &self,
        _: Uuid,
        _: Uuid,
        _: &str,
        _: ModelRequest,
    ) -> Result<EnrichedModelStream> {
        let mut events = vec![EnrichedModelEvent::Legacy(ModelEvent::Text("ok".into()))];
        for index in 0..self.count {
            let item = match (self.kind, self.first) {
                (Kind::Text, FirstEvent::Start) => EnrichedModelEvent::TextStart { index },
                (Kind::Text, FirstEvent::Delta) => EnrichedModelEvent::TextDelta {
                    index,
                    delta: "x".into(),
                },
                (Kind::Text, FirstEvent::End) => EnrichedModelEvent::TextEnd {
                    index,
                    content: "x".into(),
                },
                (Kind::Reasoning, FirstEvent::Start) => {
                    EnrichedModelEvent::ReasoningStart { index }
                }
                (Kind::Reasoning, FirstEvent::Delta) => EnrichedModelEvent::ReasoningDelta {
                    index,
                    delta: "x".into(),
                },
                (Kind::Reasoning, FirstEvent::End) => EnrichedModelEvent::ReasoningEnd {
                    index,
                    content: "x".into(),
                },
            };
            events.push(item);
        }
        events.push(EnrichedModelEvent::Legacy(ModelEvent::Complete {
            calls: vec![],
            usage: Some(Usage {
                input_tokens: 1,
                output_tokens: 1,
            }),
        }));
        Ok(Box::pin(stream::iter(events.into_iter().map(Ok))))
    }
}

#[tokio::test]
async fn enriched_items_allow_anthropic_block_count_and_remain_bounded() {
    for kind in [Kind::Text, Kind::Reasoning] {
        for first in [FirstEvent::Start, FirstEvent::Delta, FirstEvent::End] {
            for count in [65, 256, 257] {
                let model = Items { count, kind, first };
                let registry = Registry::default();
                let agent = Agent {
                    model: &model,
                    model_name: "fixture",
                    registry: &registry,
                    system_prompt: "",
                    limits: Limits::default(),
                };
                let mut session = Session::with_store(pi_rs_core::MemoryStore::default()).unwrap();
                let (_sender, mut inputs) = tokio::sync::mpsc::channel(1);
                let mut seen = 0;
                let result = agent
                    .run_with_events(
                        &mut session,
                        Some("hello".into()),
                        &mut inputs,
                        &CancellationToken::new(),
                        &mut |event| {
                            if matches!(
                                event,
                                AgentEvent::TextStart { .. }
                                    | AgentEvent::TextDelta { .. }
                                    | AgentEvent::TextEnd { .. }
                                    | AgentEvent::ReasoningStart { .. }
                                    | AgentEvent::ReasoningDelta { .. }
                                    | AgentEvent::ReasoningEnd { .. }
                            ) {
                                seen += 1;
                            }
                        },
                    )
                    .await;
                if count <= 256 {
                    let expected =
                        if matches!(kind, Kind::Text) && !matches!(first, FirstEvent::Start) {
                            format!("ok{}", "x".repeat(count as usize))
                        } else {
                            "ok".into()
                        };
                    assert_eq!(result.unwrap(), expected, "{kind:?} {first:?} {count}");
                    assert_eq!(seen, count, "{kind:?} {first:?} {count}");
                } else {
                    assert!(
                        matches!(result, Err(Error::Model(ref message)) if message.contains("items")),
                        "{kind:?} {first:?}: {result:?}"
                    );
                    assert_eq!(seen, 256, "{kind:?} {first:?}");
                }
            }
        }
    }
}

struct MixedText {
    legacy_segments: usize,
}

#[async_trait::async_trait]
impl Model for MixedText {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        unreachable!("agent should request enriched events")
    }

    async fn stream_enriched_in_session_with_identity(
        &self,
        _: Uuid,
        _: Uuid,
        _: &str,
        _: ModelRequest,
    ) -> Result<EnrichedModelStream> {
        let mut events = Vec::new();
        for index in 0..256 {
            events.push(EnrichedModelEvent::TextDelta {
                index,
                delta: "x".into(),
            });
        }
        for _ in 0..self.legacy_segments {
            // Reusing an indexed item separates consecutive legacy segments.
            events.push(EnrichedModelEvent::TextStart { index: 0 });
            events.push(EnrichedModelEvent::Legacy(ModelEvent::Text("y".into())));
        }
        events.push(EnrichedModelEvent::Legacy(ModelEvent::Complete {
            calls: vec![],
            usage: Some(Usage {
                input_tokens: 1,
                output_tokens: 1,
            }),
        }));
        Ok(Box::pin(stream::iter(events.into_iter().map(Ok))))
    }
}

#[tokio::test]
async fn mixed_indexed_and_legacy_text_respects_separate_limits() {
    for legacy_segments in [1, 128, 129] {
        let model = MixedText { legacy_segments };
        let registry = Registry::default();
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(pi_rs_core::MemoryStore::default()).unwrap();
        let (_sender, mut inputs) = tokio::sync::mpsc::channel(1);
        let mut observed_legacy = 0;
        let result = agent
            .run_with_events(
                &mut session,
                Some("hello".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |event| {
                    if matches!(
                        event,
                        AgentEvent::Notification(pi_rs_core::Notification::Text("y"))
                    ) {
                        observed_legacy += 1;
                    }
                },
            )
            .await;
        if legacy_segments <= 128 {
            assert_eq!(
                result.unwrap(),
                format!("{}{}", "x".repeat(256), "y".repeat(legacy_segments))
            );
            assert_eq!(observed_legacy, legacy_segments);
        } else {
            assert!(
                matches!(result, Err(Error::Model(ref message)) if message == "too many text segments")
            );
            assert_eq!(observed_legacy, 128);
        }
    }
}

struct LargeReasoning {
    bytes: usize,
}

#[async_trait::async_trait]
impl Model for LargeReasoning {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        unreachable!("agent should request enriched events")
    }

    async fn stream_enriched_in_session_with_identity(
        &self,
        _: Uuid,
        _: Uuid,
        _: &str,
        _: ModelRequest,
    ) -> Result<EnrichedModelStream> {
        let content = "x".repeat(self.bytes);
        let mut events = vec![
            EnrichedModelEvent::ReasoningStart { index: 0 },
            EnrichedModelEvent::ReasoningDelta {
                index: 0,
                delta: content[..self.bytes.min(1024 * 1024)].into(),
            },
        ];
        if self.bytes > 1024 * 1024 {
            events.push(EnrichedModelEvent::ReasoningDelta {
                index: 0,
                delta: content[1024 * 1024..].into(),
            });
        }
        let metadata = pi_rs_core::ModelMetadata {
            reasoning: Some(content.clone()),
            ..Default::default()
        };
        events.extend([
            EnrichedModelEvent::ReasoningEnd { index: 0, content },
            EnrichedModelEvent::Legacy(ModelEvent::Text("answer".into())),
            EnrichedModelEvent::Legacy(ModelEvent::Metadata(metadata)),
            EnrichedModelEvent::Legacy(ModelEvent::Complete {
                calls: vec![],
                usage: Some(Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                }),
            }),
        ]);
        Ok(Box::pin(stream::iter(events.into_iter().map(Ok))))
    }
}

#[tokio::test]
async fn oversized_reasoning_is_suppressed_without_failing_reply() {
    const LIMIT: usize = 1024 * 1024;
    for bytes in [LIMIT, LIMIT + 1] {
        let model = LargeReasoning { bytes };
        let registry = Registry::default();
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(pi_rs_core::MemoryStore::default()).unwrap();
        let (_sender, mut inputs) = tokio::sync::mpsc::channel(1);
        let mut starts = 0;
        let mut deltas = 0;
        let mut ends = 0;
        let answer = agent
            .run_with_events(
                &mut session,
                Some("hello".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |event| match event {
                    AgentEvent::ReasoningStart { index: 0 } => starts += 1,
                    AgentEvent::ReasoningDelta { index: 0, delta } => deltas += delta.len(),
                    AgentEvent::ReasoningEnd { index: 0, content } => ends += content.len(),
                    _ => {}
                },
            )
            .await
            .unwrap();
        assert_eq!(answer, "answer");
        assert_eq!(starts, 1);
        assert_eq!(deltas, LIMIT);
        assert_eq!(ends, LIMIT);
        assert!(session.records().iter().any(|record| matches!(
            &record.event,
            pi_rs_core::Event::ModelFinished { metadata, failure: None, .. }
                if metadata.reasoning.as_ref().is_some_and(|text| text.len() == bytes)
        )));

        let mut session = Session::with_store(pi_rs_core::MemoryStore::default()).unwrap();
        let (_sender, mut inputs) = tokio::sync::mpsc::channel(1);
        let mut legacy_bytes = 0;
        let answer = agent
            .run(
                &mut session,
                Some("hello".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |event| {
                    if let pi_rs_core::Notification::Reasoning(delta) = event {
                        legacy_bytes += delta.len();
                    }
                },
            )
            .await
            .unwrap();
        assert_eq!(answer, "answer");
        assert_eq!(legacy_bytes, LIMIT);
    }
}

struct InterleavedReasoning;

#[async_trait::async_trait]
impl Model for InterleavedReasoning {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        unreachable!("agent should request enriched events")
    }

    async fn stream_enriched_in_session_with_identity(
        &self,
        _: Uuid,
        _: Uuid,
        _: &str,
        _: ModelRequest,
    ) -> Result<EnrichedModelStream> {
        const LIMIT: usize = 1024 * 1024;
        let events = vec![
            EnrichedModelEvent::ReasoningStart { index: 1 },
            EnrichedModelEvent::ReasoningStart { index: 2 },
            EnrichedModelEvent::ReasoningDelta {
                index: 1,
                delta: "a".into(),
            },
            EnrichedModelEvent::ReasoningDelta {
                index: 2,
                delta: "bc".into(),
            },
            EnrichedModelEvent::ReasoningEnd {
                index: 1,
                content: "z".repeat(LIMIT - 1),
            },
            // The second final crosses the cumulative budget, but its already
            // observed deltas must be used to close the correct item.
            EnrichedModelEvent::ReasoningEnd {
                index: 2,
                content: "final".into(),
            },
            EnrichedModelEvent::ReasoningStart { index: 3 },
            EnrichedModelEvent::ReasoningDelta {
                index: 3,
                delta: "d".into(),
            },
            EnrichedModelEvent::ReasoningEnd {
                index: 3,
                content: "final d".into(),
            },
            EnrichedModelEvent::ReasoningStart { index: 4 },
            EnrichedModelEvent::ReasoningDelta {
                index: 4,
                delta: "x".repeat(LIMIT),
            },
            EnrichedModelEvent::ReasoningEnd {
                index: 4,
                content: "not observed".into(),
            },
            EnrichedModelEvent::Legacy(ModelEvent::Text("answer".into())),
            EnrichedModelEvent::Legacy(ModelEvent::Complete {
                calls: vec![],
                usage: Some(Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                }),
            }),
        ];
        Ok(Box::pin(stream::iter(events.into_iter().map(Ok))))
    }
}

#[tokio::test]
async fn reasoning_closures_use_each_items_observed_deltas_after_cumulative_limit() {
    let registry = Registry::default();
    let agent = Agent {
        model: &InterleavedReasoning,
        model_name: "fixture",
        registry: &registry,
        system_prompt: "",
        limits: Limits::default(),
    };
    let mut session = Session::with_store(pi_rs_core::MemoryStore::default()).unwrap();
    let (_sender, mut inputs) = tokio::sync::mpsc::channel(1);
    let mut ends = Vec::new();
    let mut deltas = Vec::new();
    let answer = agent
        .run_with_events(
            &mut session,
            Some("hello".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut |event| match event {
                AgentEvent::ReasoningDelta { index, delta } => {
                    deltas.push((index, delta.to_owned()))
                }
                AgentEvent::ReasoningEnd { index, content } => {
                    ends.push((index, content.to_owned()))
                }
                _ => {}
            },
        )
        .await
        .unwrap();
    assert_eq!(answer, "answer");
    assert_eq!(deltas, [(1, "a".into()), (2, "bc".into()), (3, "d".into())]);
    assert_eq!(
        ends,
        [
            (1, "z".repeat(1024 * 1024 - 1)),
            (2, "bc".into()),
            (3, "d".into()),
            (4, String::new())
        ]
    );
}
