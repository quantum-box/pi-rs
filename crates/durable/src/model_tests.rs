use super::*;
use futures_util::{StreamExt, future, stream};

fn call(id: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: "read".into(),
        arguments: "{}".into(),
    }
}

fn opening(events: Vec<Result<ModelEvent>>) -> impl Future<Output = Result<ModelStream>> {
    future::ready(Ok(stream::iter(events).boxed()))
}

#[tokio::test]
async fn completion_is_authoritative_and_progress_is_separate() {
    let mut progress = Vec::new();
    let metadata = ModelMetadata {
        finish_reason: Some("length".into()),
        ..Default::default()
    };
    let result = collect_generation(
        opening(vec![
            Ok(ModelEvent::Text("hello".into())),
            Ok(ModelEvent::Reasoning("thinking".into())),
            Ok(ModelEvent::ToolCallDelta(ToolCallDelta {
                index: 0,
                id: "partial".into(),
                name: "read".into(),
                arguments: "{".into(),
            })),
            Ok(ModelEvent::Metadata(metadata.clone())),
            Ok(ModelEvent::Complete {
                calls: vec![call("final")],
                usage: Some(Usage {
                    input_tokens: 2,
                    output_tokens: 3,
                }),
            }),
        ]),
        &CancellationToken::new(),
        |event| progress.push(event),
    )
    .await
    .unwrap();
    assert_eq!(result.content, "hello");
    assert_eq!(result.calls, vec![call("final")]);
    assert_eq!(result.usage.unwrap().output_tokens, 3);
    assert_eq!(result.metadata, metadata);
    assert!(result.is_truncated());
    assert!(matches!(
        &progress[..],
        [
            GenerationProgress::Text(_),
            GenerationProgress::Reasoning(_),
            GenerationProgress::ToolCallDelta(_)
        ]
    ));
    assert!(
        matches!(result.assistant_message(), Message::Assistant { calls, .. } if calls == vec![call("final")])
    );
}

#[tokio::test]
async fn unfinished_and_failed_streams_never_return_partial_output() {
    let cancel = CancellationToken::new();
    let result = collect_generation(
        opening(vec![Ok(ModelEvent::Text("partial".into()))]),
        &cancel,
        |_| {},
    )
    .await;
    assert!(matches!(result, Err(Error::Model(message)) if message.contains("before completion")));
    let result = collect_generation(
        opening(vec![
            Ok(ModelEvent::Text("partial".into())),
            Err(Error::RetryableModel("busy".into())),
        ]),
        &cancel,
        |_| {},
    )
    .await;
    assert!(matches!(result, Err(Error::RetryableModel(message)) if message == "busy"));
}

#[tokio::test]
async fn cancellation_interrupts_opening_and_pending_stream() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result =
        collect_generation(future::pending::<Result<ModelStream>>(), &cancel, |_| {}).await;
    assert!(matches!(result, Err(Error::Cancelled)));
    let cancel = CancellationToken::new();
    let stream: ModelStream = stream::pending().boxed();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        collect_generation(future::ready(Ok(stream)), &task_cancel, |_| {}).await
    });
    tokio::task::yield_now().await;
    cancel.cancel();
    assert!(matches!(task.await.unwrap(), Err(Error::Cancelled)));
}

#[tokio::test]
async fn rejects_invalid_complete_calls() {
    let events = vec![Ok(ModelEvent::Complete {
        calls: vec![call("same"), call("same")],
        usage: None,
    })];
    assert!(matches!(
        collect_generation(opening(events), &CancellationToken::new(), |_| {}).await,
        Err(Error::Model(_))
    ));
}

#[test]
fn request_preserves_context_and_tool_schema() {
    let messages = vec![Message::User {
        content: "hi".into(),
    }];
    let tools = vec![ToolSpec {
        name: "read".into(),
        description: "Read".into(),
        parameters: serde_json::json!({"type":"object"}),
    }];
    let request = model_request("model", messages.clone(), tools.clone(), 42);
    assert_eq!(
        (request.model.as_str(), request.max_output_tokens),
        ("model", 42)
    );
    assert_eq!(request.messages, messages);
    assert_eq!(request.tools, tools);
}
