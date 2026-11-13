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
    let tool = create_read_tool(root.path(), remote).unwrap();
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
    let root = tempfile::tempdir().unwrap();
    let tool = create_read_tool(root.path(), Arc::new(Remote::default())).unwrap();
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

struct StreamOnly;
struct ChunkReader(std::io::Cursor<Vec<u8>>);
impl std::io::Read for ChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        assert!(buf.len() <= 8192, "reader must use bounded buffers");
        std::io::Read::read(&mut self.0, buf)
    }
}
impl std::io::Seek for ChunkReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        std::io::Seek::seek(&mut self.0, pos)
    }
}
#[async_trait::async_trait]
impl ReadOperations for StreamOnly {
    async fn access(&self, _: &Path) -> Result<()> {
        Ok(())
    }
    async fn read_file(&self, _: &Path) -> Result<Vec<u8>> {
        panic!("streaming backend must not buffer whole file")
    }
    async fn open_file(
        &self,
        _: &Path,
    ) -> Result<Option<Box<dyn pi_rs_workbench::coding::ReadSeek>>> {
        Ok(Some(Box::new(ChunkReader(std::io::Cursor::new(
            "line\n".repeat(30_000).into_bytes(),
        )))))
    }
}
#[tokio::test]
async fn streaming_backend_uses_bounded_pagination_buffers() {
    let root = tempfile::tempdir().unwrap();
    let tool = create_read_tool(root.path(), Arc::new(StreamOnly)).unwrap();
    let result = tool
        .invoke(
            json!({"path":"long", "offset":2, "limit":2}),
            ToolContext {
                operation_id: uuid::Uuid::new_v4(),
                cancellation: CancellationToken::new(),
            },
        )
        .await;
    assert!(
        matches!(result, Outcome::Succeeded(text) if text.starts_with("line\nline\n\n[") && text.contains("offset=4"))
    );
}

#[tokio::test]
async fn injectable_local_adapter_rejects_large_image_and_pages_text() {
    use std::io::Write;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("large.txt"), "line\n".repeat(100_000)).unwrap();
    let mut image = std::fs::File::create(root.path().join("large.png")).unwrap();
    image.write_all(b"\x89PNG\r\n\x1a\n").unwrap();
    image.set_len(20 * 1024 * 1024 + 1).unwrap();
    let tool = create_read_tool(
        root.path(),
        Arc::new(pi_rs_workbench::coding::LocalReadOperations),
    )
    .unwrap();
    let context = || ToolContext {
        operation_id: uuid::Uuid::new_v4(),
        cancellation: CancellationToken::new(),
    };
    assert!(
        matches!(tool.invoke(json!({"path":"large.txt", "offset":1000, "limit":2}), context()).await, Outcome::Succeeded(text) if text.starts_with("line\nline\n\n[") && text.contains("offset=1002"))
    );
    assert!(
        matches!(tool.invoke(json!({"path":"large.png"}), context()).await, Outcome::Failed(text) if text.contains("image exceeds 20 MiB"))
    );
}

struct SlowImageState {
    reads: std::sync::atomic::AtomicUsize,
    started: tokio::sync::Notify,
    dropped: tokio::sync::Notify,
    released: (std::sync::Mutex<bool>, std::sync::Condvar),
}
struct SlowImageReader {
    state: Arc<SlowImageState>,
    source: std::io::Cursor<Vec<u8>>,
}
impl std::io::Read for SlowImageReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.state
            .reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.state.started.notify_one();
        let (lock, ready) = &self.state.released;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        let count = buffer.len().min(1024);
        std::io::Read::read(&mut self.source, &mut buffer[..count])
    }
}
impl std::io::Seek for SlowImageReader {
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
        std::io::Seek::seek(&mut self.source, position)
    }
}
impl Drop for SlowImageReader {
    fn drop(&mut self) {
        self.state.dropped.notify_one();
    }
}
struct SlowImage(Arc<SlowImageState>);
#[async_trait::async_trait]
impl ReadOperations for SlowImage {
    async fn access(&self, _: &Path) -> Result<()> {
        Ok(())
    }
    async fn read_file(&self, _: &Path) -> Result<Vec<u8>> {
        unreachable!()
    }
    async fn detect_image_mime_type(&self, _: &Path) -> Result<Option<String>> {
        Ok(Some("image/png".into()))
    }
    async fn open_file(
        &self,
        _: &Path,
    ) -> Result<Option<Box<dyn pi_rs_workbench::coding::ReadSeek>>> {
        Ok(Some(Box::new(SlowImageReader {
            state: self.0.clone(),
            source: std::io::Cursor::new(vec![0; 100_000]),
        })))
    }
}
#[tokio::test]
async fn cancelling_image_stops_subsequent_stream_reads() {
    let state = Arc::new(SlowImageState {
        reads: std::sync::atomic::AtomicUsize::new(0),
        started: tokio::sync::Notify::new(),
        dropped: tokio::sync::Notify::new(),
        released: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
    });
    let root = tempfile::tempdir().unwrap();
    let tool = create_read_tool(root.path(), Arc::new(SlowImage(state.clone()))).unwrap();
    let cancellation = CancellationToken::new();
    let token = cancellation.clone();
    let task = tokio::spawn(async move {
        tool.invoke(
            json!({"path":"image.png"}),
            ToolContext {
                operation_id: uuid::Uuid::new_v4(),
                cancellation: token,
            },
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), state.started.notified())
        .await
        .unwrap();
    cancellation.cancel();
    assert!(matches!(task.await.unwrap(), Outcome::Failed(text) if text.contains("aborted")));
    *state.released.0.lock().unwrap() = true;
    state.released.1.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(2), state.dropped.notified())
        .await
        .expect("blocking reader must be dropped after its current read");
    assert_eq!(state.reads.load(std::sync::atomic::Ordering::SeqCst), 1);
}
