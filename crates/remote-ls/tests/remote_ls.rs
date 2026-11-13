use futures_util::stream;
use pi_rs::*;
use pi_rs_remote_ls::{LsExtension, LsOperations, LsStat, create_ls_tool};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
#[derive(Default)]
struct Backend {
    calls: Mutex<Vec<(String, PathBuf)>>,
}
#[async_trait::async_trait]
impl LsOperations for Backend {
    async fn exists(&self, path: &Path) -> Result<bool> {
        self.calls
            .lock()
            .unwrap()
            .push(("exists".into(), path.into()));
        Ok(!path.ends_with("missing"))
    }
    async fn stat(&self, path: &Path) -> Result<LsStat> {
        self.calls
            .lock()
            .unwrap()
            .push(("stat".into(), path.into()));
        if path.ends_with("bad") {
            return Err(Error::Config("denied".into()));
        }
        Ok(LsStat {
            is_directory: path.ends_with("virtual")
                || path.ends_with("skip")
                || path.ends_with("large")
                || path.ends_with("invalid")
                || path.ends_with("empty")
                || path.ends_with("Dir"),
        })
    }
    async fn readdir(&self, path: &Path) -> Result<Vec<String>> {
        self.calls
            .lock()
            .unwrap()
            .push(("readdir".into(), path.into()));
        if path.ends_with("empty") {
            return Ok(vec![]);
        }
        if path.ends_with("invalid") {
            return Ok(vec!["../escape".into()]);
        }
        if path.ends_with("large") {
            return Ok(vec!["x".repeat(52 * 1024)]);
        }
        if path.ends_with("skip") {
            return Ok(vec!["zeta".into(), "bad".into(), "Alpha".into()]);
        }
        Ok(vec![
            "zeta".into(),
            "Dir".into(),
            "beta".into(),
            "Alpha".into(),
        ])
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
                        name: "ls".into(),
                        arguments: json!({"path":"virtual","limit":3}).to_string(),
                    },
                    ToolCall {
                        id: "invalid".into(),
                        name: "ls".into(),
                        arguments: json!({"limit":0}).to_string(),
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
async fn model_listing_uses_remote_backend_and_validates_arguments() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let extension = LsExtension::new(root.path(), backend.clone()).unwrap();
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
            Some("list".into()),
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
        && text.contains("Alpha\nbeta\nDir/")
        && text.contains("3 entries limit reached")
        && !text.contains("zeta")));
    assert!(
        replies
            .iter()
            .any(|(id, _, error)| *id == "invalid" && *error)
    );
    let calls = backend.calls.lock().unwrap();
    assert_eq!(calls[0], ("exists".into(), root.path().join("virtual")));
    assert!(calls.iter().all(|(_, p)| p.starts_with(root.path())));
}
#[tokio::test]
async fn listing_skips_inaccessible_entries_and_handles_limits() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let tool = create_ls_tool(root.path(), backend.clone()).unwrap();
    assert!(
        matches!(tool.invoke(json!({"path":"skip","limit":2}),context()).await,Outcome::Succeeded(text) if text=="Alpha\nzeta")
    );
    assert!(
        matches!(tool.invoke(json!({"path":"empty"}),context()).await,Outcome::Succeeded(text) if text=="(empty directory)")
    );
    assert!(
        matches!(tool.invoke(json!({"path":"missing"}),context()).await,Outcome::Failed(text) if text.contains("Path not found"))
    );
    assert!(
        matches!(tool.invoke(json!({"path":"file"}),context()).await,Outcome::Failed(text) if text.contains("Not a directory"))
    );
    assert!(
        matches!(tool.invoke(json!({"path":"large"}),context()).await,Outcome::SucceededWithDetails{details,..} if details["truncation"]["truncated"]==true)
    );
    let before = backend.calls.lock().unwrap().len();
    assert!(
        matches!(tool.invoke(json!({"path":"invalid"}),context()).await,Outcome::Failed(text) if text.contains("invalid entry"))
    );
    assert_eq!(
        backend.calls.lock().unwrap().len() - before,
        3,
        "invalid names must not trigger entry stats"
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
impl LsOperations for Blocking {
    async fn exists(&self, _: &Path) -> Result<bool> {
        Ok(true)
    }
    async fn stat(&self, _: &Path) -> Result<LsStat> {
        Ok(LsStat { is_directory: true })
    }
    async fn readdir(&self, _: &Path) -> Result<Vec<String>> {
        let _guard = DropFlag(self.dropped.clone());
        self.started.notify_one();
        std::future::pending().await
    }
}
#[tokio::test]
async fn cancellation_drops_inflight_backend_listing() {
    let root = tempfile::tempdir().unwrap();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let backend = Arc::new(Blocking {
        started: tokio::sync::Notify::new(),
        dropped: dropped.clone(),
    });
    let tool = create_ls_tool(root.path(), backend.clone()).unwrap();
    let token = CancellationToken::new();
    let child = token.clone();
    let task = tokio::spawn(async move {
        tool.invoke(
            json!({}),
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
async fn tilde_uses_backend_home_and_never_host_home() {
    use pi_rs_remote_ls::{LsToolOptions, create_ls_tool_with_options};
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("backend-home");
    let backend = Arc::new(Backend::default());
    let tool = create_ls_tool_with_options(
        root.path(),
        backend.clone(),
        LsToolOptions {
            remote_home: Some(home.clone()),
        },
    )
    .unwrap();
    assert!(matches!(
        tool.invoke(json!({"path":"~/virtual", "limit":3}), context())
            .await,
        Outcome::SucceededWithDetails { .. }
    ));
    assert_eq!(backend.calls.lock().unwrap()[0].1, home.join("virtual"));
    let before = backend.calls.lock().unwrap().len();
    let tool = create_ls_tool(root.path(), backend.clone()).unwrap();
    assert!(
        matches!(tool.invoke(json!({"path":"~"}), context()).await, Outcome::Failed(text) if text.contains("remote home directory is not configured"))
    );
    assert_eq!(backend.calls.lock().unwrap().len(), before);
    assert!(
        create_ls_tool_with_options(
            root.path(),
            backend,
            LsToolOptions {
                remote_home: Some(PathBuf::from("relative"))
            }
        )
        .is_err()
    );
}

#[cfg(unix)]
struct BackslashBackend;
#[cfg(unix)]
#[async_trait::async_trait]
impl LsOperations for BackslashBackend {
    async fn exists(&self, _: &Path) -> Result<bool> {
        Ok(true)
    }
    async fn stat(&self, path: &Path) -> Result<LsStat> {
        Ok(LsStat {
            is_directory: !path.ends_with("literal\\"),
        })
    }
    async fn readdir(&self, _: &Path) -> Result<Vec<String>> {
        Ok(vec!["literal\\".into()])
    }
}
#[cfg(unix)]
#[tokio::test]
async fn listing_preserves_unix_backslash_filename() {
    let root = tempfile::tempdir().unwrap();
    let tool = create_ls_tool(root.path(), Arc::new(BackslashBackend)).unwrap();
    assert!(
        matches!(tool.invoke(json!({}), context()).await, Outcome::Succeeded(text) if text == "literal\\")
    );
}

struct ImmediateStats {
    root: PathBuf,
    started: tokio::sync::Notify,
    count: std::sync::atomic::AtomicUsize,
    fail: bool,
}
#[async_trait::async_trait]
impl LsOperations for ImmediateStats {
    async fn exists(&self, _: &Path) -> Result<bool> {
        Ok(true)
    }
    async fn stat(&self, path: &Path) -> Result<LsStat> {
        if path == self.root {
            return Ok(LsStat { is_directory: true });
        }
        if self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            self.started.notify_one();
        }
        if self.fail {
            Err(Error::Config("inaccessible".into()))
        } else {
            Ok(LsStat {
                is_directory: false,
            })
        }
    }
    async fn readdir(&self, _: &Path) -> Result<Vec<String>> {
        Ok((0..100_000).map(|n| format!("entry-{n:06}")).collect())
    }
}
#[tokio::test(flavor = "current_thread")]
async fn cancellation_interrupts_immediate_entry_stats_even_when_they_fail() {
    for fail in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let backend = Arc::new(ImmediateStats {
            root: root.path().to_owned(),
            started: tokio::sync::Notify::new(),
            count: std::sync::atomic::AtomicUsize::new(0),
            fail,
        });
        let tool = create_ls_tool(root.path(), backend.clone()).unwrap();
        let token = CancellationToken::new();
        let child = token.clone();
        let task = tokio::spawn(async move {
            tool.invoke(
                json!({"limit":100000}),
                ToolContext {
                    operation_id: uuid::Uuid::new_v4(),
                    cancellation: child,
                },
            )
            .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            backend.started.notified(),
        )
        .await
        .unwrap();
        token.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("listing must yield for cancellation")
            .unwrap();
        assert!(matches!(result, Outcome::Failed(text) if text.contains("aborted")));
        assert!(backend.count.load(std::sync::atomic::Ordering::SeqCst) < 100_000);
    }
}
