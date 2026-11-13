use futures_util::stream;
use pi_rs::*;
use pi_rs_workbench::coding::{CodingTools, WriteOperations, create_write_tool};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Remote {
    calls: Mutex<Vec<(String, PathBuf)>>,
}
#[async_trait::async_trait]
impl WriteOperations for Remote {
    async fn mkdir(&self, path: &Path) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(("mkdir".into(), path.into()));
        Ok(())
    }
    async fn write_file(&self, path: &Path, content: &str) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push((content.into(), path.into()));
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
            vec![ModelEvent::Complete {
                calls: vec![
                    ToolCall {
                        id: "valid".into(),
                        name: "write".into(),
                        arguments: json!({"path":"nested/file.txt","content":"hello"}).to_string(),
                    },
                    ToolCall {
                        id: "invalid".into(),
                        name: "write".into(),
                        arguments: json!({"path":"other","content":5}).to_string(),
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
async fn model_write_uses_backend_and_validates_arguments() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Remote::default());
    assert!(
        CodingTools::new(root.path())
            .unwrap()
            .with_write_operations(backend.clone())
            .is_err()
    );
    let extension = CodingTools::local(root.path())
        .unwrap()
        .with_write_operations(backend.clone())
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
            Some("write".into()),
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
        && text.contains("Successfully wrote to nested/file.txt")));
    assert!(
        replies
            .iter()
            .any(|(id, _, error)| *id == "invalid" && *error)
    );
    let root = root.path().canonicalize().unwrap();
    assert_eq!(
        *backend.calls.lock().unwrap(),
        vec![
            ("mkdir".into(), root.join("nested")),
            ("hello".into(), root.join("nested/file.txt"))
        ]
    );
    assert!(!root.join("nested/file.txt").exists());
}
struct Blocking {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
    calls: Mutex<Vec<String>>,
}
#[async_trait::async_trait]
impl WriteOperations for Blocking {
    async fn mkdir(&self, _: &Path) -> Result<()> {
        Ok(())
    }
    async fn write_file(&self, _: &Path, content: &str) -> Result<()> {
        self.calls.lock().unwrap().push(content.into());
        if content == "first" {
            self.started.notify_one();
            self.release.notified().await;
        }
        Ok(())
    }
}
fn context(token: CancellationToken) -> ToolContext {
    ToolContext {
        operation_id: uuid::Uuid::new_v4(),
        cancellation: token,
    }
}
#[tokio::test]
async fn cancelled_inflight_write_retains_lock_until_backend_settles() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Blocking {
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        calls: Mutex::new(vec![]),
    });
    let tool = create_write_tool(root.path(), backend.clone()).unwrap();
    let first_tool = tool.clone();
    let tool = create_write_tool(root.path(), backend.clone()).unwrap();
    let first = tokio::spawn(async move {
        first_tool
            .invoke(
                json!({"path":"same","content":"first"}),
                context(CancellationToken::new()),
            )
            .await
    });
    backend.started.notified().await;
    first.abort();
    let second = tokio::spawn(async move {
        tool.invoke(
            json!({"path":"same","content":"second"}),
            context(CancellationToken::new()),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(*backend.calls.lock().unwrap(), vec!["first"]);
    backend.release.notify_one();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .unwrap()
            .unwrap(),
        Outcome::Succeeded(_)
    ));
    assert_eq!(*backend.calls.lock().unwrap(), vec!["first", "second"]);
}

struct BlockingMkdir {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
    writes: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl WriteOperations for BlockingMkdir {
    async fn mkdir(&self, _: &Path) -> Result<()> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(())
    }
    async fn write_file(&self, _: &Path, _: &str) -> Result<()> {
        self.writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn cancellation_during_mkdir_prevents_write_after_it_settles() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(BlockingMkdir {
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        writes: std::sync::atomic::AtomicUsize::new(0),
    });
    let tool = create_write_tool(root.path(), backend.clone()).unwrap();
    let token = CancellationToken::new();
    let task_token = token.clone();
    let task = tokio::spawn(async move {
        tool.invoke(json!({"path":"file", "content":"no"}), context(task_token))
            .await
    });
    backend.started.notified().await;
    token.cancel();
    backend.release.notify_one();
    assert!(matches!(task.await.unwrap(), Outcome::Failed(text) if text.contains("aborted")));
    assert_eq!(backend.writes.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn distinct_remote_backends_do_not_share_a_path_lock() {
    let root = tempfile::tempdir().unwrap();
    let slow = Arc::new(Blocking {
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        calls: Mutex::new(vec![]),
    });
    let fast = Arc::new(Remote::default());
    let first_tool = create_write_tool(root.path(), slow.clone()).unwrap();
    let second_tool = create_write_tool(root.path(), fast.clone()).unwrap();
    let first = tokio::spawn(async move {
        first_tool
            .invoke(
                json!({"path":"same", "content":"first"}),
                context(CancellationToken::new()),
            )
            .await
    });
    slow.started.notified().await;
    let second = tokio::time::timeout(
        Duration::from_secs(1),
        second_tool.invoke(
            json!({"path":"same", "content":"second"}),
            context(CancellationToken::new()),
        ),
    )
    .await;
    slow.release.notify_one();
    assert!(
        matches!(second, Ok(Outcome::Succeeded(_))),
        "unrelated backend should not wait"
    );
    assert!(matches!(first.await.unwrap(), Outcome::Succeeded(_)));
}

#[cfg(unix)]
struct BlockingLocal {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[cfg(unix)]
#[async_trait::async_trait]
impl WriteOperations for BlockingLocal {
    fn uses_local_filesystem(&self) -> bool {
        true
    }
    async fn mkdir(&self, _: &Path) -> Result<()> {
        Ok(())
    }
    async fn write_file(&self, path: &Path, content: &str) -> Result<()> {
        self.started.notify_one();
        self.release.notified().await;
        std::fs::write(path, content)?;
        Ok(())
    }
}
#[cfg(unix)]
#[tokio::test]
async fn local_alias_write_serializes_with_real_path_edit() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("real.txt");
    std::fs::write(&target, "before").unwrap();
    std::os::unix::fs::symlink(&target, root.path().join("alias.txt")).unwrap();
    let backend = Arc::new(BlockingLocal {
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let write = create_write_tool(root.path(), backend.clone()).unwrap();
    let extension = CodingTools::local(root.path()).unwrap();
    let edit = extension
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == "edit")
        .unwrap();
    let first = tokio::spawn(async move {
        write
            .invoke(
                json!({"path":"alias.txt", "content":"written"}),
                context(CancellationToken::new()),
            )
            .await
    });
    backend.started.notified().await;
    let second = tokio::spawn(async move {
        edit.invoke(
            json!({"path":"real.txt", "edits":[{"oldText":"written", "newText":"edited"}]}),
            context(CancellationToken::new()),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let finished_early = second.is_finished();
    backend.release.notify_one();
    assert!(!finished_early, "edit must wait for the alias write");
    assert!(matches!(first.await.unwrap(), Outcome::Succeeded(_)));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .unwrap()
            .unwrap(),
        Outcome::SucceededWithDetails { .. }
    ));
    assert_eq!(std::fs::read_to_string(target).unwrap(), "edited");
}
