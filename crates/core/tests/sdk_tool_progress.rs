use std::sync::Arc;
use std::time::Duration;

use pi_rs_core::{
    AgentSession, ContextContent, CustomMessage, CustomMessageDelivery, Extension, ExtensionState,
    InputAction, InputEvent, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome,
    SessionBuilder, Tool, ToolCall, ToolContext, ToolHost, ToolSpec,
};
use serde_json::{Value, json};
use tokio::sync::{Notify, broadcast};

struct ProgressModel(&'static str);

#[async_trait::async_trait]
impl Model for ProgressModel {
    async fn stream(&self, request: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        let events = if matches!(request.messages.last(), Some(Message::Tool { .. })) {
            vec![
                ModelEvent::Text("done".into()),
                ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                },
            ]
        } else {
            vec![ModelEvent::Complete {
                calls: vec![ToolCall {
                    id: "fixture-call".into(),
                    name: self.0.into(),
                    arguments: r#"{"value":7}"#.into(),
                }],
                usage: None,
            }]
        };
        Ok(Box::pin(futures_util::stream::iter(
            events.into_iter().map(Ok),
        )))
    }
}

struct ProgressTool {
    name: &'static str,
    release: Option<Arc<Notify>>,
}

#[async_trait::async_trait]
impl Tool for ProgressTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name.into(),
            description: "Report a synthetic partial result".into(),
            parameters: json!({"type":"object"}),
        }
    }

    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!("fixture requires a tool host")
    }

    async fn invoke_with_host(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> Outcome {
        if self.name == "parent" {
            return host.call("child", json!({"value":9})).await;
        }
        assert!(host.supports_progress());
        host.report_progress(json!({
            "content":[{"type":"text","text":"transient-preview"}],
            "details":{"step":1},
        }))
        .await
        .unwrap();
        if let Some(release) = &self.release {
            release.notified().await;
        }
        Outcome::Succeeded("settled".into())
    }
}

struct ProgressExtension {
    tools: Vec<Arc<dyn Tool>>,
    trigger: bool,
}

#[async_trait::async_trait]
impl Extension for ProgressExtension {
    fn id(&self) -> &str {
        "progress-fixture"
    }

    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        self.tools.clone()
    }

    fn handles_input(&self) -> bool {
        self.trigger
    }

    async fn input_with_host(
        &self,
        _: &InputEvent,
        _: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> pi_rs_core::ExtensionResult<InputAction> {
        host.send_message(
            CustomMessage {
                custom_type: "progress-fixture".into(),
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

fn progress_session(release: Option<Arc<Notify>>, nested: bool) -> AgentSession {
    let mut tools: Vec<Arc<dyn Tool>> = vec![Arc::new(ProgressTool {
        name: "child",
        release,
    })];
    if nested {
        tools.push(Arc::new(ProgressTool {
            name: "parent",
            release: None,
        }));
    }
    SessionBuilder::new(
        Arc::new(ProgressModel(if nested { "parent" } else { "child" })),
        "fixture",
    )
    .extension(Arc::new(ProgressExtension {
        tools,
        trigger: nested,
    }))
    .unwrap()
    .in_memory()
    .unwrap()
}

#[tokio::test]
async fn progress_before_finish() {
    let release = Arc::new(Notify::new());
    let mut session = progress_session(Some(release.clone()), false);
    let mut progress = session.subscribe_tool_progress();
    let read = async {
        let update = progress.recv().await.unwrap();
        assert_eq!(update.tool_call_id, "fixture-call");
        assert_eq!(update.tool_name, "child");
        assert_eq!(update.args, json!({"value":7}));
        assert_eq!(update.parent_tool_call_id, None);
        assert_eq!(
            update.partial_result["content"][0]["text"],
            "transient-preview"
        );
        assert_eq!(update.partial_result["details"], json!({"step":1}));
        release.notify_one();
    };
    let (answer, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(session.prompt("run tool"), read)
    })
    .await
    .unwrap();
    assert_eq!(answer.unwrap(), "done");
    assert_eq!(
        progress.try_recv().unwrap_err(),
        broadcast::error::TryRecvError::Empty
    );
    let journal = session.journal().unwrap();
    let records = serde_json::to_string(journal.records()).unwrap();
    assert!(records.contains("settled"));
    assert!(!records.contains("transient-preview"));
}

#[tokio::test]
async fn nested_progress_in_triggered_run() {
    let release = Arc::new(Notify::new());
    let session = progress_session(Some(release.clone()), true);
    let control = session.controls();
    let mut progress = session.subscribe_tool_progress();
    let read = async {
        let update = progress.recv().await.unwrap();
        assert_eq!(update.tool_name, "child");
        assert_eq!(update.parent_tool_call_id.as_deref(), Some("fixture-call"));
        assert_ne!(update.tool_call_id, "fixture-call");
        assert_eq!(update.args, json!({"value":9}));
        assert_eq!(update.partial_result["details"], json!({"step":1}));
        release.notify_one();
    };
    let (queued, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(control.follow_up("trigger a turn"), read)
    })
    .await
    .unwrap();
    queued.unwrap();
    assert_eq!(
        progress.try_recv().unwrap_err(),
        broadcast::error::TryRecvError::Empty
    );
}

#[tokio::test]
async fn slow_progress_subscriber() {
    let mut session = progress_session(None, false);
    let control = session.controls();
    let mut progress = session.subscribe_tool_progress();
    tokio::time::timeout(Duration::from_secs(10), async {
        for _ in 0..33 {
            assert_eq!(session.prompt("run tool").await.unwrap(), "done");
        }
    })
    .await
    .unwrap();
    assert_eq!(
        progress.try_recv().unwrap_err(),
        broadcast::error::TryRecvError::Lagged(1)
    );
    session.close().await.unwrap();
    for _ in 0..32 {
        progress.try_recv().unwrap();
    }
    assert_eq!(
        progress.try_recv().unwrap_err(),
        broadcast::error::TryRecvError::Closed
    );
    assert_eq!(
        session.subscribe_tool_progress().try_recv().unwrap_err(),
        broadcast::error::TryRecvError::Closed
    );
    drop(control);

    let session = progress_session(None, false);
    let control = session.controls();
    let mut progress = session.subscribe_tool_progress();
    drop(session);
    assert_eq!(
        progress.try_recv().unwrap_err(),
        broadcast::error::TryRecvError::Closed
    );
    drop(control);
}
