use futures_util::stream;
use pi_rs::*;
use pi_rs_remote_find::{FindExtension, FindOperations, FindOptions, create_find_tool};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
struct Call {
    pattern: String,
    cwd: PathBuf,
    options: FindOptions,
}
#[derive(Default)]
struct Backend {
    calls: Mutex<Vec<Call>>,
}
#[async_trait::async_trait]
impl FindOperations for Backend {
    async fn exists(&self, path: &Path) -> Result<bool> {
        Ok(!path.ends_with("missing"))
    }
    async fn glob(&self, pattern: &str, cwd: &Path, options: FindOptions) -> Result<Vec<PathBuf>> {
        self.calls.lock().unwrap().push(Call {
            pattern: pattern.into(),
            cwd: cwd.into(),
            options,
        });
        match pattern {
            "wait" => std::future::pending().await,
            "empty" => Ok(vec![]),
            "escape" => Ok(vec![PathBuf::from("../outside")]),
            "large" => Ok(vec![PathBuf::from("x".repeat(52 * 1024))]),
            _ => Ok(vec![
                cwd.join("src/one.rs"),
                PathBuf::from("src/two.rs"),
                PathBuf::from("extra"),
            ]),
        }
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
                        id: "valid".into(),
                        name: "find".into(),
                        arguments: json!({"pattern":"*.rs","path":"virtual","limit":2}).to_string(),
                    },
                    ToolCall {
                        id: "invalid".into(),
                        name: "find".into(),
                        arguments: json!({"pattern":"*","limit":0}).to_string(),
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
fn context() -> ToolContext {
    ToolContext {
        operation_id: uuid::Uuid::new_v4(),
        cancellation: CancellationToken::new(),
    }
}
#[tokio::test]
async fn model_find_uses_remote_backend_and_validates_arguments() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let extension = FindExtension::new(root.path(), backend.clone()).unwrap();
    let mut registry = Registry::default();
    registry.register(Arc::new(extension)).unwrap();
    let model = Script(Mutex::new(vec![]));
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
            Some("find".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut |_: Notification<'_>| {},
        )
        .await
        .unwrap();
    let requests = model.0.lock().unwrap();
    let replies: Vec<_> = requests[1]
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
    assert!(replies.iter().any(|(id, text, error)| *id == "valid"
        && !error
        && text.contains("src/one.rs\nsrc/two.rs")
        && text.contains("2 results limit reached")
        && !text.contains("extra")));
    assert!(
        replies
            .iter()
            .any(|(id, _, error)| *id == "invalid" && *error)
    );
    let calls = backend.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].pattern, "*.rs");
    assert_eq!(calls[0].cwd, root.path().join("virtual"));
    assert_eq!(calls[0].options.limit, 2);
    assert_eq!(
        calls[0].options.ignore,
        ["**/node_modules/**", "**/.git/**"]
    );
}
#[tokio::test]
async fn remote_find_limits_errors_and_cancellation() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let tool = create_find_tool(root.path(), backend).unwrap();
    assert!(
        matches!(tool.invoke(json!({"pattern":"empty"}),context()).await,Outcome::Succeeded(text) if text=="No files found matching pattern")
    );
    assert!(
        matches!(tool.invoke(json!({"pattern":"x","path":"missing"}),context()).await,Outcome::Failed(text) if text.contains("Path not found"))
    );
    assert!(
        matches!(tool.invoke(json!({"pattern":"escape"}),context()).await,Outcome::Failed(text) if text.contains("outside"))
    );
    assert!(
        matches!(tool.invoke(json!({"pattern":"large"}),context()).await,Outcome::SucceededWithDetails{details,..} if details["truncation"]["truncated"]==true)
    );
    let token = CancellationToken::new();
    let child = token.clone();
    let running = tokio::spawn(async move {
        tool.invoke(
            json!({"pattern":"wait"}),
            ToolContext {
                operation_id: uuid::Uuid::new_v4(),
                cancellation: child,
            },
        )
        .await
    });
    tokio::task::yield_now().await;
    token.cancel();
    assert!(matches!(running.await.unwrap(),Outcome::Failed(text) if text.contains("aborted")));
}
struct BlockingBackend {
    started: tokio::sync::Notify,
    dropped: Arc<std::sync::atomic::AtomicBool>,
}
struct Dropped(Arc<std::sync::atomic::AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl FindOperations for BlockingBackend {
    async fn exists(&self, _: &Path) -> Result<bool> {
        Ok(true)
    }
    async fn glob(&self, _: &str, _: &Path, _: FindOptions) -> Result<Vec<PathBuf>> {
        let _guard = Dropped(self.dropped.clone());
        self.started.notify_one();
        std::future::pending().await
    }
}
#[tokio::test]
async fn cancelling_find_drops_inflight_backend_operation() {
    let root = tempfile::tempdir().unwrap();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let backend = Arc::new(BlockingBackend {
        started: tokio::sync::Notify::new(),
        dropped: dropped.clone(),
    });
    let tool = create_find_tool(root.path(), backend.clone()).unwrap();
    let token = CancellationToken::new();
    let child = token.clone();
    let task = tokio::spawn(async move {
        tool.invoke(
            json!({"pattern":"*"}),
            ToolContext {
                operation_id: uuid::Uuid::new_v4(),
                cancellation: child,
            },
        )
        .await
    });
    backend.started.notified().await;
    token.cancel();
    assert!(matches!(task.await.unwrap(),Outcome::Failed(text) if text.contains("aborted")));
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
}
