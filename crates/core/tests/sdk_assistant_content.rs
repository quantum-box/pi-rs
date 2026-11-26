use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{StreamExt, stream};
use pi_rs_core::{
    AssistantContentUpdate, ContextContent, CustomMessage, CustomMessageDelivery,
    DetailedModelEvent, DetailedModelStream, EnrichedModelEvent, Error, Event, Extension,
    ExtensionState, ImageContent, InputAction, InputEvent, Model, ModelEvent, ModelRequest,
    ModelStream, Result, RetrySettings, SessionBuilder, SessionEvent, ToolHost,
};
use tokio::sync::{Notify, broadcast::error::TryRecvError};
use uuid::Uuid;

struct ScriptedModel {
    attempts: Mutex<std::vec::IntoIter<Vec<Result<DetailedModelEvent>>>>,
    complete_after: Option<Arc<Notify>>,
}

impl ScriptedModel {
    fn new(attempts: Vec<Vec<Result<DetailedModelEvent>>>) -> Self {
        Self {
            attempts: Mutex::new(attempts.into_iter()),
            complete_after: None,
        }
    }
}

#[async_trait::async_trait]
impl Model for ScriptedModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        unreachable!("the fixture supplies detailed model events")
    }

    async fn stream_detailed_in_session_with_identity(
        &self,
        _: Uuid,
        _: Uuid,
        _: &str,
        _: ModelRequest,
    ) -> Result<DetailedModelStream> {
        let events = self.attempts.lock().unwrap().next().expect("model attempt");
        let complete_after = self.complete_after.clone();
        Ok(Box::pin(stream::iter(events).then(move |event| {
            let complete_after = complete_after.clone();
            async move {
                if let Some(gate) = complete_after
                    && matches!(&event, Ok(event) if matches!(&event.event,
                        EnrichedModelEvent::Legacy(ModelEvent::Complete { .. })))
                {
                    gate.notified().await;
                }
                event
            }
        })))
    }
}

fn event(event: EnrichedModelEvent) -> Result<DetailedModelEvent> {
    Ok(event.into())
}

fn start(index: u64) -> Result<DetailedModelEvent> {
    event(EnrichedModelEvent::TextStart { index })
}

fn delta(index: u64, delta: &str) -> Result<DetailedModelEvent> {
    event(EnrichedModelEvent::TextDelta {
        index,
        delta: delta.into(),
    })
}

fn end(index: u64, content: &str) -> Result<DetailedModelEvent> {
    event(EnrichedModelEvent::TextEnd {
        index,
        content: content.into(),
    })
}

fn complete() -> Result<DetailedModelEvent> {
    event(EnrichedModelEvent::Legacy(ModelEvent::Complete {
        calls: vec![],
        usage: None,
    }))
}

#[tokio::test]
async fn image_prompts_preserve_one_vs_two_text_items() {
    use AssistantContentUpdate::{TextDelta, TextEnd, TextStart};

    let model = ScriptedModel::new(vec![
        vec![
            start(0),
            delta(0, "A"),
            delta(0, "B"),
            end(0, "AB"),
            complete(),
        ],
        vec![
            start(0),
            delta(0, "A"),
            end(0, "A"),
            start(1),
            delta(1, "B"),
            end(1, "B"),
            complete(),
        ],
    ]);
    let mut session = SessionBuilder::new(Arc::new(model), "fixture")
        .in_memory()
        .unwrap();
    let mut content = session.subscribe_assistant_content();
    let mut regular = session.subscribe();
    let mut previous_request = None;
    for expected in [
        vec![
            (0, TextStart),
            (0, TextDelta { delta: "A".into() }),
            (0, TextDelta { delta: "B".into() }),
            (
                0,
                TextEnd {
                    content: "AB".into(),
                },
            ),
        ],
        vec![
            (0, TextStart),
            (0, TextDelta { delta: "A".into() }),
            (
                0,
                TextEnd {
                    content: "A".into(),
                },
            ),
            (1, TextStart),
            (1, TextDelta { delta: "B".into() }),
            (
                1,
                TextEnd {
                    content: "B".into(),
                },
            ),
        ],
    ] {
        let attachment = ImageContent {
            mime_type: "image/png".into(),
            data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jV1sAAAAASUVORK5CYII=".into(),
        };
        session
            .prompt_with_images("describe", vec![attachment])
            .await
            .unwrap();
        let mut request = None;
        let mut flattened = Vec::new();
        while let Ok(event) = regular.try_recv() {
            match event {
                SessionEvent::Record(record) => {
                    if let Event::ModelRequested { request_id, .. } = record.event {
                        request = request_id;
                    }
                }
                SessionEvent::Text(text) => flattened.push(text),
                _ => {}
            }
        }
        assert_eq!(flattened, ["A", "B"]);
        assert!(request.is_some());
        assert_ne!(request, previous_request);
        previous_request = request;
        for (index, update) in expected {
            let event = content.try_recv().unwrap();
            assert_eq!(Some(event.request_id), request);
            assert_eq!((event.index, &event.update), (index, &update));
        }
        assert_eq!(content.try_recv().unwrap_err(), TryRecvError::Empty);
    }
}

#[tokio::test]
async fn retried_reasoning_keeps_request_identity_and_does_not_fabricate_boundaries() {
    use AssistantContentUpdate::{ReasoningDelta, ReasoningEnd, ReasoningStart};

    let model = ScriptedModel::new(vec![
        vec![
            event(EnrichedModelEvent::ReasoningStart { index: 2 }),
            event(EnrichedModelEvent::ReasoningDelta {
                index: 2,
                delta: "partial".into(),
            }),
            Err(Error::RetryableModel("fixture retry".into())),
        ],
        vec![
            Ok(DetailedModelEvent::redacted_start(2)),
            event(EnrichedModelEvent::ReasoningEnd {
                index: 2,
                content: String::new(),
            }),
            event(EnrichedModelEvent::Legacy(ModelEvent::Text("done".into()))),
            complete(),
        ],
    ]);
    let mut session = SessionBuilder::new(Arc::new(model), "fixture")
        .in_memory()
        .unwrap();
    session
        .controls()
        .set_retry_settings(RetrySettings {
            enabled: true,
            max_retries: 1,
            base_delay_ms: 0,
            max_agent_delay_ms: 0,
        })
        .unwrap();
    let mut content = session.subscribe_assistant_content();
    assert_eq!(session.prompt("retry").await.unwrap(), "done");
    let failed_start = content.try_recv().unwrap();
    let failed_delta = content.try_recv().unwrap();
    let retry_start = content.try_recv().unwrap();
    let retry_end = content.try_recv().unwrap();
    assert_eq!(failed_start.request_id, failed_delta.request_id);
    assert_eq!(retry_start.request_id, retry_end.request_id);
    assert_ne!(failed_start.request_id, retry_start.request_id);
    assert_eq!(
        (failed_start.index, &failed_start.update),
        (2, &ReasoningStart { redacted: false })
    );
    assert_eq!(
        (failed_delta.index, &failed_delta.update),
        (
            2,
            &ReasoningDelta {
                delta: "partial".into()
            }
        )
    );
    assert_eq!(
        (retry_start.index, &retry_start.update),
        (2, &ReasoningStart { redacted: true })
    );
    assert_eq!(
        (retry_end.index, &retry_end.update),
        (
            2,
            &ReasoningEnd {
                content: String::new()
            }
        )
    );
    // Exercise the same serialized payload a native IPC host can forward.
    assert_eq!(
        serde_json::to_value(retry_start.as_ref()).unwrap(),
        serde_json::json!({
            "requestId": retry_start.request_id,
            "index": 2,
            "update": {"type": "reasoningStart", "redacted": true}
        })
    );
    assert_eq!(content.try_recv().unwrap_err(), TryRecvError::Empty);
}

struct Trigger;

#[async_trait::async_trait]
impl Extension for Trigger {
    fn id(&self) -> &str {
        "content-trigger"
    }

    fn handles_input(&self) -> bool {
        true
    }

    async fn input_with_host(
        &self,
        _: &InputEvent,
        _: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> pi_rs_core::ExtensionResult<InputAction> {
        host.send_message(
            CustomMessage {
                custom_type: self.id().into(),
                content: ContextContent::Text("trigger".into()),
                display: false,
                details: None,
            },
            CustomMessageDelivery::TriggerTurn,
        )
        .await?;
        Ok(InputAction::Handled)
    }
}

#[tokio::test]
async fn cloned_control_can_publish_items_and_does_not_own_the_stream() {
    use AssistantContentUpdate::{TextDelta, TextEnd};

    let release = Arc::new(Notify::new());
    let mut model = ScriptedModel::new(vec![vec![delta(4, "done"), end(4, "done"), complete()]]);
    model.complete_after = Some(release.clone());
    let mut session = SessionBuilder::new(Arc::new(model), "fixture")
        .extension(Arc::new(Trigger))
        .unwrap()
        .in_memory()
        .unwrap();
    let control = session.controls().clone();
    let mut content = session.subscribe_assistant_content();
    let read = async {
        let delta = content.recv().await.unwrap();
        let end = content.recv().await.unwrap();
        assert_eq!(delta.request_id, end.request_id);
        assert_eq!(
            (delta.index, &delta.update),
            (
                4,
                &TextDelta {
                    delta: "done".into()
                }
            )
        );
        assert_eq!(
            (end.index, &end.update),
            (
                4,
                &TextEnd {
                    content: "done".into()
                }
            )
        );
        release.notify_one();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(control.follow_up("trigger a turn"), read)
    })
    .await
    .unwrap();
    result.unwrap();
    assert_eq!(content.try_recv().unwrap_err(), TryRecvError::Empty);
    session.close().await.unwrap();
    assert_eq!(content.try_recv().unwrap_err(), TryRecvError::Closed);
    drop(control);
}
