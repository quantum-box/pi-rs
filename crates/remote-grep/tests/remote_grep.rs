use futures_util::stream;
use pi_rs::*;
use pi_rs_remote_grep::{
    GrepExtension, GrepMatch, GrepOperations, GrepRequest, GrepToolOptions, create_grep_tool,
    create_grep_tool_with_options,
};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Backend {
    requests: Mutex<Vec<GrepRequest>>,
}
#[async_trait::async_trait]
impl GrepOperations for Backend {
    async fn is_directory(&self, path: &Path) -> Result<bool> {
        if path.ends_with("missing") {
            return Err(Error::Config("missing".into()));
        }
        Ok(!path.ends_with("file.rs"))
    }
    async fn search(
        &self,
        request: GrepRequest,
        _: CancellationToken,
        sender: tokio::sync::mpsc::Sender<GrepMatch>,
    ) -> Result<()> {
        self.requests.lock().unwrap().push(request.clone());
        if request.pattern == "empty" {
            return Ok(());
        }
        if request.pattern == "wait" {
            std::future::pending::<()>().await;
        }
        let path = if request.path.ends_with("file.rs") {
            "file.rs".to_owned()
        } else {
            match request.pattern.as_str() {
                "escape" => "../secret".to_owned(),
                "absolute" => request
                    .path
                    .join("src/file.rs")
                    .to_string_lossy()
                    .into_owned(),
                "huge" => "x".repeat(52 * 1024),
                _ => "src/file.rs".into(),
            }
        };
        let line = if request.pattern == "long" {
            "😀".repeat(251)
        } else {
            "found\r\n".into()
        };
        let item = GrepMatch {
            path,
            line_number: 2,
            line,
            before: if request.context > 0 {
                vec!["before".into()]
            } else {
                vec![]
            },
            after: if request.context > 0 {
                vec!["after".into()]
            } else {
                vec![]
            },
        };
        let _ = sender.send(item).await;
        Ok(())
    }
}
struct Script(Mutex<Vec<ModelRequest>>);
#[async_trait::async_trait]
impl Model for Script {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let mut requests = self.0.lock().unwrap();
        requests.push(request);
        let events = if requests.len() == 1 {
            vec![ModelEvent::Complete{calls:vec![
            ToolCall{id:"valid".into(),name:"grep".into(),arguments:json!({"pattern":"needle","path":"virtual","glob":"*.rs","ignoreCase":true,"literal":true,"context":1,"limit":2}).to_string()},
            ToolCall{id:"invalid".into(),name:"grep".into(),arguments:json!({"pattern":"x","limit":0}).to_string()},
        ],usage:None}]
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
async fn model_grep_forwards_search_options_and_formats_context() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let mut registry = Registry::default();
    registry
        .register(Arc::new(
            GrepExtension::new(root.path(), backend.clone()).unwrap(),
        ))
        .unwrap();
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
            Some("search".into()),
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
        && text.contains("src/file.rs-1- before\nsrc/file.rs:2: found\nsrc/file.rs-3- after")));
    assert!(
        replies
            .iter()
            .any(|(id, _, error)| *id == "invalid" && *error)
    );
    let calls = backend.requests.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let call = &calls[0];
    assert_eq!(call.path, root.path().join("virtual"));
    assert_eq!(call.pattern, "needle");
    assert_eq!(call.glob.as_deref(), Some("*.rs"));
    assert!(call.ignore_case && call.literal);
    assert_eq!(call.context, 1);
    assert_eq!(call.limit, 2);
}
#[tokio::test]
async fn grep_enforces_paths_limits_and_line_truncation() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let tool = create_grep_tool(root.path(), backend).unwrap();
    assert!(
        matches!(tool.invoke(json!({"pattern":"empty"}),context()).await,Outcome::Succeeded(text) if text=="No matches found")
    );
    assert!(
        matches!(tool.invoke(json!({"pattern":"x","path":"file.rs"}),context()).await, Outcome::Succeeded(text) if text == "file.rs:2: found")
    );
    assert!(
        matches!(tool.invoke(json!({"pattern":"x","path":"missing"}),context()).await,Outcome::Failed(text) if text.contains("Path not found"))
    );
    assert!(
        matches!(tool.invoke(json!({"pattern":"escape"}),context()).await,Outcome::Failed(text) if text.contains("outside"))
    );
    assert!(
        matches!(tool.invoke(json!({"pattern":"long","limit":1}),context()).await,Outcome::SucceededWithDetails{details,..} if details["linesTruncated"]==true && details["matchLimitReached"]==1)
    );
    assert!(
        matches!(tool.invoke(json!({"pattern":"huge"}),context()).await,Outcome::SucceededWithDetails{details,..} if details["truncation"]["truncated"]==true)
    );
}
struct Blocking {
    started: tokio::sync::Notify,
    dropped: Arc<std::sync::atomic::AtomicBool>,
}
struct DropFlag(Arc<std::sync::atomic::AtomicBool>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl GrepOperations for Blocking {
    async fn is_directory(&self, _: &Path) -> Result<bool> {
        Ok(true)
    }
    async fn search(
        &self,
        _: GrepRequest,
        _: CancellationToken,
        _: tokio::sync::mpsc::Sender<GrepMatch>,
    ) -> Result<()> {
        let _drop = DropFlag(self.dropped.clone());
        self.started.notify_one();
        std::future::pending().await
    }
}
#[tokio::test]
async fn cancellation_drops_backend_search() {
    let root = tempfile::tempdir().unwrap();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let backend = Arc::new(Blocking {
        started: tokio::sync::Notify::new(),
        dropped: dropped.clone(),
    });
    let tool = create_grep_tool(root.path(), backend.clone()).unwrap();
    let token = CancellationToken::new();
    let child = token.clone();
    let task = tokio::spawn(async move {
        tool.invoke(
            json!({"pattern":"x"}),
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
#[tokio::test]
async fn grep_tilde_uses_explicit_backend_home() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("backend-home");
    let backend = Arc::new(Backend::default());
    let tool = create_grep_tool_with_options(
        root.path(),
        backend.clone(),
        GrepToolOptions {
            remote_home: Some(home.clone()),
        },
    )
    .unwrap();
    assert!(matches!(
        tool.invoke(json!({"path":"~/src","pattern":"empty"}), context())
            .await,
        Outcome::Succeeded(_)
    ));
    assert_eq!(backend.requests.lock().unwrap()[0].path, home.join("src"));
    let tool = create_grep_tool(root.path(), backend).unwrap();
    assert!(
        matches!(tool.invoke(json!({"path":"~","pattern":"empty"}),context()).await,Outcome::Failed(text) if text.contains("remote home directory is not configured"))
    );
    assert!(
        create_grep_tool_with_options(
            root.path(),
            Arc::new(Backend::default()),
            GrepToolOptions {
                remote_home: Some(PathBuf::from("relative"))
            }
        )
        .is_err()
    );
}

struct LimitedBackend {
    dropped: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait::async_trait]
impl GrepOperations for LimitedBackend {
    async fn is_directory(&self, _: &Path) -> Result<bool> {
        Ok(true)
    }
    async fn search(
        &self,
        _: GrepRequest,
        _: CancellationToken,
        sender: tokio::sync::mpsc::Sender<GrepMatch>,
    ) -> Result<()> {
        let _guard = DropFlag(self.dropped.clone());
        sender
            .send(GrepMatch {
                path: "file".into(),
                line_number: 1,
                line: "first".into(),
                before: vec![],
                after: vec![],
            })
            .await
            .map_err(|_| Error::Cancelled)?;
        std::future::pending().await
    }
}
#[tokio::test]
async fn reaching_match_limit_drops_backend_search() {
    let root = tempfile::tempdir().unwrap();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let backend = Arc::new(LimitedBackend {
        dropped: dropped.clone(),
    });
    let tool = create_grep_tool(root.path(), backend).unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        tool.invoke(json!({"pattern":"x","limit":1}), context()),
    )
    .await
    .unwrap();
    assert!(
        matches!(result,Outcome::SucceededWithDetails{details,..} if details["matchLimitReached"]==1)
    );
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
}

struct NoCalls;
#[async_trait::async_trait]
impl GrepOperations for NoCalls {
    async fn is_directory(&self, _: &Path) -> Result<bool> {
        panic!("backend must not be called")
    }
    async fn search(
        &self,
        _: GrepRequest,
        _: CancellationToken,
        _: tokio::sync::mpsc::Sender<GrepMatch>,
    ) -> Result<()> {
        panic!("backend must not be called")
    }
}
#[cfg(windows)]
#[tokio::test]
async fn rejects_drive_relative_path_before_backend_calls() {
    let root = tempfile::tempdir().unwrap();
    let tool = create_grep_tool(root.path(), Arc::new(NoCalls)).unwrap();
    assert!(matches!(
        tool.invoke(json!({"pattern":"x", "path":"D:logs"}), context())
            .await,
        Outcome::Failed(_)
    ));
}

#[tokio::test]
async fn rejects_rooted_home_tail_before_backend_calls() {
    let root = tempfile::tempdir().unwrap();
    let tool = create_grep_tool_with_options(
        root.path(),
        Arc::new(NoCalls),
        GrepToolOptions {
            remote_home: Some(root.path().to_owned()),
        },
    )
    .unwrap();
    assert!(matches!(
        tool.invoke(json!({"pattern":"x", "path":"~//etc"}), context())
            .await,
        Outcome::Failed(_)
    ));
    #[cfg(windows)]
    assert!(matches!(
        tool.invoke(json!({"pattern":"x", "path":"~/D:logs"}), context())
            .await,
        Outcome::Failed(_)
    ));
}
