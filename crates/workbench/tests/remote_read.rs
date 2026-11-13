use futures_util::stream;
use pi_rs::*;
use pi_rs_workbench::coding::{CodingTools, ReadOperations, create_read_tool};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Remote {
    calls: Mutex<Vec<(String, PathBuf)>>,
}
#[async_trait::async_trait]
impl ReadOperations for Remote {
    async fn access(&self, path: &Path) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(("access".into(), path.into()));
        Ok(())
    }
    async fn read_file(&self, path: &Path) -> Result<Vec<u8>> {
        self.calls
            .lock()
            .unwrap()
            .push(("read".into(), path.into()));
        if path.ends_with("wait") {
            std::future::pending().await
        } else if path.ends_with("long") {
            Ok("line\n".repeat(3000).into_bytes())
        } else if path.ends_with("image") {
            let mut bytes = std::io::Cursor::new(Vec::new());
            image::DynamicImage::new_rgb8(2, 2)
                .write_to(&mut bytes, image::ImageFormat::Png)
                .unwrap();
            Ok(bytes.into_inner())
        } else {
            Ok(b"alpha\nbeta\ngamma".to_vec())
        }
    }
    async fn detect_image_mime_type(&self, path: &Path) -> Result<Option<String>> {
        self.calls
            .lock()
            .unwrap()
            .push(("mime".into(), path.into()));
        Ok(path.ends_with("image").then(|| "image/png".into()))
    }
}
struct Script(Mutex<Vec<ModelRequest>>);
#[async_trait::async_trait]
impl Model for Script {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let mut requests = self.0.lock().unwrap();
        requests.push(request);
        let events = if requests.len() == 1 {
            vec![ModelEvent::Complete {
                calls: vec![
                    ToolCall {
                        id: "one".into(),
                        name: "read".into(),
                        arguments: json!({"path":"@nested/../remote.txt", "offset":2,"limit":1})
                            .to_string(),
                    },
                    ToolCall {
                        id: "invalid".into(),
                        name: "read".into(),
                        arguments: json!({"path":"bad", "offset":0}).to_string(),
                    },
                ],
                usage: None,
            }]
        } else {
            vec![
                ModelEvent::Text("done".into()),
                ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                },
            ]
        };
        Ok(Box::pin(stream::iter(events.into_iter().map(Ok))))
    }
}
#[tokio::test]
async fn model_calls_remote_read_with_validation_and_pagination() {
    let root = tempfile::tempdir().unwrap();
    let remote = Arc::new(Remote::default());
    let extension = CodingTools::local(root.path())
        .unwrap()
        .with_read_operations(remote.clone())
        .unwrap();
    let mut registry = Registry::default();
    registry.register(Arc::new(extension)).unwrap();
    let model = Script(Mutex::new(Vec::new()));
    let agent = Agent {
        model: &model,
        model_name: "fixture",
        registry: &registry,
        system_prompt: "fixture",
        limits: Limits::default(),
    };
    let mut session = Session::with_store(MemoryStore::default()).unwrap();
    let (_, mut inputs) = tokio::sync::mpsc::channel(1);
    agent
        .run(
            &mut session,
            Some("read".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut |_: Notification<'_>| {},
        )
        .await
        .unwrap();
    let requests = model.0.lock().unwrap();
    let results: Vec<_> = requests[1]
        .messages
        .iter()
        .filter_map(|m| match m {
            Message::Tool {
                call_id,
                content,
                is_error,
                ..
            } => Some((call_id.as_str(), content.as_str(), *is_error)),
            _ => None,
        })
        .collect();
    assert!(results.iter().any(|(id, content, error)| *id == "one"
        && !error
        && content.contains("beta\n\n[1 more lines")));
    assert!(
        results
            .iter()
            .any(|(id, _, error)| *id == "invalid" && *error)
    );
    let calls = remote.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert!(
        calls
            .iter()
            .all(|(_, p)| *p == root.path().canonicalize().unwrap().join("remote.txt"))
    );
}
#[tokio::test]
async fn remote_read_cancellation_and_scope() {
    let root = tempfile::tempdir().unwrap();
    let remote = Arc::new(Remote::default());
    assert!(
        CodingTools::new(root.path())
            .unwrap()
            .with_read_operations(remote.clone())
            .is_err()
    );
    let tool = create_read_tool(Path::new("/remote"), remote).unwrap();
    let token = CancellationToken::new();
    let task_token = token.clone();
    let task = tokio::spawn(async move {
        tool.invoke(
            json!({"path":"wait"}),
            ToolContext {
                operation_id: uuid::Uuid::new_v4(),
                cancellation: task_token,
            },
        )
        .await
    });
    tokio::task::yield_now().await;
    token.cancel();
    assert!(matches!(task.await.unwrap(), Outcome::Failed(text) if text.contains("aborted")));
}

#[tokio::test]
async fn remote_read_uses_shared_truncation_and_image_processing() {
    let tool = create_read_tool(Path::new("/remote"), Arc::new(Remote::default())).unwrap();
    let context = || ToolContext {
        operation_id: uuid::Uuid::new_v4(),
        cancellation: CancellationToken::new(),
    };
    let result = tool.invoke(json!({"path":"long"}), context()).await;
    assert!(
        matches!(result, Outcome::SucceededWithDetails { output, details } if output.contains("Showing lines 1-2000") && details["truncation"]["truncated"] == true)
    );
    let result = tool.invoke(json!({"path":"image"}), context()).await;
    assert!(
        matches!(result, Outcome::Structured { data, .. } if data["content"][1]["mimeType"] == "image/png")
    );
}
