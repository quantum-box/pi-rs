use futures_util::stream;
use pi_rs::*;
use pi_rs_workbench::coding::{
    CodingTools, EditOperations, EditToolOptions, WriteOperations, create_edit_tool,
    create_edit_tool_with_options, create_write_tool,
};
use serde_json::json;
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

struct Store {
    value: Mutex<String>,
    calls: Mutex<Vec<&'static str>>,
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
    block_write: bool,
}
impl Store {
    fn new(value: &str, block_write: bool) -> Self {
        Self {
            value: Mutex::new(value.into()),
            calls: Mutex::new(vec![]),
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            block_write,
        }
    }
}
#[async_trait::async_trait]
impl EditOperations for Store {
    async fn access(&self, _: &Path) -> Result<()> {
        self.calls.lock().unwrap().push("access");
        Ok(())
    }
    async fn read_file(&self, _: &Path) -> Result<Vec<u8>> {
        self.calls.lock().unwrap().push("read");
        Ok(self.value.lock().unwrap().as_bytes().to_vec())
    }
    async fn write_file(&self, _: &Path, content: &str) -> Result<()> {
        self.calls.lock().unwrap().push("edit");
        *self.value.lock().unwrap() = content.into();
        Ok(())
    }
}
#[async_trait::async_trait]
impl WriteOperations for Store {
    async fn mkdir(&self, _: &Path) -> Result<()> {
        Ok(())
    }
    async fn write_file(&self, _: &Path, content: &str) -> Result<()> {
        self.calls.lock().unwrap().push("write");
        if self.block_write {
            self.started.notify_one();
            self.release.notified().await;
        }
        *self.value.lock().unwrap() = content.into();
        Ok(())
    }
}
fn compatible_tool(root: &Path, operations: Arc<dyn EditOperations>) -> Result<Arc<dyn Tool>> {
    create_edit_tool_with_options(
        root,
        operations,
        EditToolOptions {
            allow_non_atomic_fallback: true,
        },
    )
}
fn context(token: CancellationToken) -> ToolContext {
    ToolContext {
        operation_id: uuid::Uuid::new_v4(),
        cancellation: token,
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
                        name: "edit".into(),
                        arguments: json!({"path":"file", "oldText":"one", "newText":"two"})
                            .to_string(),
                    },
                    ToolCall {
                        id: "invalid".into(),
                        name: "edit".into(),
                        arguments: json!({"path":"file", "edits":[{"oldText":"", "newText":"x"}]})
                            .to_string(),
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
async fn model_edit_uses_backend_legacy_preparation_and_validation() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::new("one", false));
    assert!(
        CodingTools::new(root.path())
            .unwrap()
            .with_edit_operations(store.clone())
            .is_err()
    );
    let extension = CodingTools::local(root.path())
        .unwrap()
        .with_edit_operations(store.clone())
        .unwrap()
        .with_edit_options(EditToolOptions {
            allow_non_atomic_fallback: true,
        });
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
            Some("edit".into()),
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
        && text.contains("Successfully replaced 1 block")));
    assert!(
        replies
            .iter()
            .any(|(id, _, error)| *id == "invalid" && *error)
    );
    assert_eq!(*store.value.lock().unwrap(), "two");
    assert_eq!(
        *store.calls.lock().unwrap(),
        vec!["access", "read", "read", "edit"]
    );
}
#[tokio::test]
async fn edit_preserves_bom_and_endings_and_reports_diff() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::new("\u{feff}one\r\nthree\r\n", false));
    let tool = compatible_tool(root.path(), store.clone()).unwrap();
    let result = tool
        .invoke(
            json!({"path":"file","edits":[{"oldText":"one","newText":"two"}]}),
            context(CancellationToken::new()),
        )
        .await;
    assert!(
        matches!(result,Outcome::SucceededWithDetails{details,..} if details["firstChangedLine"]==1 && details["patch"].as_str().unwrap().contains("+two"))
    );
    assert_eq!(*store.value.lock().unwrap(), "\u{feff}two\r\nthree\r\n");
}
#[tokio::test]
async fn remote_write_and_edit_on_same_backend_share_lock() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::new("old", true));
    let write = create_write_tool(root.path(), store.clone()).unwrap();
    let edit = compatible_tool(root.path(), store.clone()).unwrap();
    let first = tokio::spawn(async move {
        write
            .invoke(
                json!({"path":"file","content":"one"}),
                context(CancellationToken::new()),
            )
            .await
    });
    store.started.notified().await;
    let second = tokio::spawn(async move {
        edit.invoke(
            json!({"path":"file","edits":[{"oldText":"one","newText":"two"}]}),
            context(CancellationToken::new()),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    let finished = second.is_finished();
    store.release.notify_one();
    assert!(!finished, "edit must wait for in-flight write");
    assert!(matches!(first.await.unwrap(), Outcome::Succeeded(_)));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .unwrap()
            .unwrap(),
        Outcome::SucceededWithDetails { .. }
    ));
    assert_eq!(*store.value.lock().unwrap(), "two");
}
struct SlowRead {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
    writes: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl EditOperations for SlowRead {
    async fn access(&self, _: &Path) -> Result<()> {
        Ok(())
    }
    async fn read_file(&self, _: &Path) -> Result<Vec<u8>> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(b"one".to_vec())
    }
    async fn write_file(&self, _: &Path, _: &str) -> Result<()> {
        self.writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn cancellation_during_read_prevents_mutation_after_read_settles() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SlowRead {
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        writes: std::sync::atomic::AtomicUsize::new(0),
    });
    let tool = compatible_tool(root.path(), store.clone()).unwrap();
    let token = CancellationToken::new();
    let t = token.clone();
    let task = tokio::spawn(async move {
        tool.invoke(
            json!({"path":"file","edits":[{"oldText":"one","newText":"two"}]}),
            context(t),
        )
        .await
    });
    store.started.notified().await;
    token.cancel();
    store.release.notify_one();
    assert!(matches!(task.await.unwrap(),Outcome::Failed(text) if text.contains("aborted")));
    assert_eq!(store.writes.load(std::sync::atomic::Ordering::SeqCst), 0);
}

struct Changing {
    reads: std::sync::atomic::AtomicUsize,
    writes: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl EditOperations for Changing {
    async fn access(&self, _: &Path) -> Result<()> {
        Ok(())
    }
    async fn read_file(&self, _: &Path) -> Result<Vec<u8>> {
        let read = self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(if read == 0 {
            b"one".to_vec()
        } else {
            b"external".to_vec()
        })
    }
    async fn write_file(&self, _: &Path, _: &str) -> Result<()> {
        self.writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn edit_refuses_to_overwrite_changed_backend_content() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Changing {
        reads: std::sync::atomic::AtomicUsize::new(0),
        writes: std::sync::atomic::AtomicUsize::new(0),
    });
    let tool = compatible_tool(root.path(), backend.clone()).unwrap();
    let result = tool
        .invoke(
            json!({"path":"file", "edits":[{"oldText":"one", "newText":"two"}]}),
            context(CancellationToken::new()),
        )
        .await;
    assert!(matches!(result, Outcome::Failed(text) if text.contains("file changed")));
    assert_eq!(backend.writes.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn local_edit_adapter_rejects_fifo_without_waiting() {
    use std::os::unix::ffi::OsStrExt;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("pipe");
    let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    let ops = pi_rs_workbench::coding::LocalEditOperations;
    let checks = async {
        assert!(ops.access(&path).await.is_err());
        assert!(ops.read_file(&path).await.is_err());
        assert!(ops.write_file(&path, "x").await.is_err());
    };
    tokio::time::timeout(Duration::from_secs(2), checks)
        .await
        .expect("FIFO must not block edit adapter");
}

struct Missing;
#[async_trait::async_trait]
impl EditOperations for Missing {
    async fn access(&self, _: &Path) -> Result<()> {
        Err(std::io::Error::from(std::io::ErrorKind::NotFound).into())
    }
    async fn read_file(&self, _: &Path) -> Result<Vec<u8>> {
        panic!("access must succeed first")
    }
    async fn write_file(&self, _: &Path, _: &str) -> Result<()> {
        panic!("access must succeed first")
    }
}
#[tokio::test]
async fn edit_reports_missing_file_before_reading() {
    let root = tempfile::tempdir().unwrap();
    let tool = create_edit_tool(root.path(), Arc::new(Missing)).unwrap();
    let result = tool
        .invoke(
            json!({"path":"missing", "edits":[{"oldText":"one", "newText":"two"}]}),
            context(CancellationToken::new()),
        )
        .await;
    assert!(
        matches!(result, Outcome::Failed(text) if text.contains("Could not edit file: missing. Error code: ENOENT."))
    );
}

#[tokio::test]
async fn ambiguous_replacement_does_not_mutate_remote_file() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::new("one\none", false));
    let tool = compatible_tool(root.path(), store.clone()).unwrap();
    let result = tool
        .invoke(
            json!({"path":"file", "edits":[{"oldText":"one", "newText":"two"}]}),
            context(CancellationToken::new()),
        )
        .await;
    assert!(matches!(result, Outcome::Failed(text) if text.contains("occurrences")));
    assert_eq!(*store.value.lock().unwrap(), "one\none");
    assert!(!store.calls.lock().unwrap().contains(&"edit"));
}

struct AliasedStore(Store);
#[async_trait::async_trait]
impl WriteOperations for AliasedStore {
    async fn resource_identity(&self, path: &Path) -> Result<std::path::PathBuf> {
        Ok(path.parent().unwrap().join("canonical"))
    }
    async fn mkdir(&self, path: &Path) -> Result<()> {
        WriteOperations::mkdir(&self.0, path).await
    }
    async fn write_file(&self, path: &Path, value: &str) -> Result<()> {
        WriteOperations::write_file(&self.0, path, value).await
    }
}
#[async_trait::async_trait]
impl EditOperations for AliasedStore {
    async fn resource_identity(&self, path: &Path) -> Result<std::path::PathBuf> {
        Ok(path.parent().unwrap().join("canonical"))
    }
    async fn access(&self, path: &Path) -> Result<()> {
        EditOperations::access(&self.0, path).await
    }
    async fn read_file(&self, path: &Path) -> Result<Vec<u8>> {
        EditOperations::read_file(&self.0, path).await
    }
    async fn write_file(&self, path: &Path, value: &str) -> Result<()> {
        EditOperations::write_file(&self.0, path, value).await
    }
}
#[tokio::test]
async fn backend_alias_identity_serializes_write_and_edit() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(AliasedStore(Store::new("old", true)));
    let write = create_write_tool(root.path(), backend.clone()).unwrap();
    let edit = compatible_tool(root.path(), backend.clone()).unwrap();
    let first = tokio::spawn(async move {
        write
            .invoke(
                json!({"path":"alias", "content":"one"}),
                context(CancellationToken::new()),
            )
            .await
    });
    backend.0.started.notified().await;
    let second = tokio::spawn(async move {
        edit.invoke(
            json!({"path":"canonical", "edits":[{"oldText":"one", "newText":"two"}]}),
            context(CancellationToken::new()),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    let finished = second.is_finished();
    backend.0.release.notify_one();
    assert!(!finished, "alias edit must wait for write");
    assert!(matches!(first.await.unwrap(), Outcome::Succeeded(_)));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .unwrap()
            .unwrap(),
        Outcome::SucceededWithDetails { .. }
    ));
    assert_eq!(*backend.0.value.lock().unwrap(), "two");
}

struct ConditionalStore {
    value: Mutex<String>,
    external_update: bool,
    fallback_calls: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl EditOperations for ConditionalStore {
    async fn access(&self, _: &Path) -> Result<()> {
        Ok(())
    }
    async fn read_file(&self, _: &Path) -> Result<Vec<u8>> {
        Ok(self.value.lock().unwrap().as_bytes().to_vec())
    }
    async fn write_file(&self, _: &Path, _: &str) -> Result<()> {
        self.fallback_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    async fn write_if_unchanged(
        &self,
        _: &Path,
        expected: &[u8],
        content: &str,
    ) -> Result<pi_rs_workbench::coding::ConditionalEdit> {
        let mut current = self.value.lock().unwrap();
        if self.external_update {
            *current = "external".into();
        }
        if current.as_bytes() != expected {
            return Ok(pi_rs_workbench::coding::ConditionalEdit::Conflict);
        }
        *current = content.into();
        Ok(pi_rs_workbench::coding::ConditionalEdit::Applied)
    }
}
#[tokio::test]
async fn conditional_edit_prevents_external_update_being_overwritten() {
    let root = tempfile::tempdir().unwrap();
    for external in [false, true] {
        let backend = Arc::new(ConditionalStore {
            value: Mutex::new("one".into()),
            external_update: external,
            fallback_calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let tool = create_edit_tool(root.path(), backend.clone()).unwrap();
        let result = tool
            .invoke(
                json!({"path":"file", "edits":[{"oldText":"one", "newText":"two"}]}),
                context(CancellationToken::new()),
            )
            .await;
        if external {
            assert!(matches!(result, Outcome::Failed(text) if text.contains("file changed")));
        } else {
            assert!(matches!(result, Outcome::SucceededWithDetails { .. }));
        }
        assert_eq!(
            *backend.value.lock().unwrap(),
            if external { "external" } else { "two" }
        );
        assert_eq!(
            backend
                .fallback_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn local_edit_adapter_preserves_permissions_and_symlink() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("target");
    let alias = root.path().join("alias");
    std::fs::write(&target, "old").unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o640)).unwrap();
    std::os::unix::fs::symlink(&target, &alias).unwrap();
    let ops = pi_rs_workbench::coding::LocalEditOperations;
    ops.write_file(&alias, "new").await.unwrap();
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "new");
    assert!(
        std::fs::symlink_metadata(&alias)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
        0o640
    );
}

struct RacyFallback {
    value: Mutex<String>,
    reads: std::sync::atomic::AtomicUsize,
    writes: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl EditOperations for RacyFallback {
    async fn access(&self, _: &Path) -> Result<()> {
        Ok(())
    }
    async fn read_file(&self, _: &Path) -> Result<Vec<u8>> {
        let mut value = self.value.lock().unwrap();
        let previous = value.as_bytes().to_vec();
        if self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
            *value = "external".into();
        }
        Ok(previous)
    }
    async fn write_file(&self, _: &Path, content: &str) -> Result<()> {
        self.writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        *self.value.lock().unwrap() = content.into();
        Ok(())
    }
}
#[tokio::test]
async fn non_atomic_fallback_requires_opt_in_and_exposes_external_race() {
    let root = tempfile::tempdir().unwrap();
    for opt_in in [false, true] {
        let backend = Arc::new(RacyFallback {
            value: Mutex::new("one".into()),
            reads: std::sync::atomic::AtomicUsize::new(0),
            writes: std::sync::atomic::AtomicUsize::new(0),
        });
        let tool = if opt_in {
            compatible_tool(root.path(), backend.clone()).unwrap()
        } else {
            create_edit_tool(root.path(), backend.clone()).unwrap()
        };
        let result = tool
            .invoke(
                json!({"path":"file", "edits":[{"oldText":"one", "newText":"two"}]}),
                context(CancellationToken::new()),
            )
            .await;
        if opt_in {
            assert!(matches!(result, Outcome::SucceededWithDetails { .. }));
            assert_eq!(*backend.value.lock().unwrap(), "two");
            assert_eq!(backend.writes.load(std::sync::atomic::Ordering::SeqCst), 1);
        } else {
            assert!(
                matches!(result, Outcome::Failed(text) if text.contains("enable non-atomic fallback explicitly"))
            );
            assert_eq!(*backend.value.lock().unwrap(), "one");
            assert_eq!(backend.writes.load(std::sync::atomic::Ordering::SeqCst), 0);
        }
    }
}
