use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use futures_util::stream;
use pi_rs::{CancellationToken, Extension, Outcome, Registry, Result, ToolContext};
use pi_rs_workbench::{
    coding::{
        BashOperations, CodingTools, ConditionalEdit, EditOperations, ReadOperations,
        ShellSpawnContext, WriteOperations,
    },
    coding_factories::{
        RemoteCodingToolOptions, coding_tools_from, coding_tools_from_with_local_shell,
        create_coding_tools, create_read_only_tools, create_remote_coding_tools,
        injected_file_tools_from, read_only_tools_from,
    },
};
use serde_json::json;

fn names(set: &impl Extension) -> Vec<String> {
    set.tools().iter().map(|tool| tool.spec().name).collect()
}
fn context() -> ToolContext {
    ToolContext {
        operation_id: uuid::Uuid::new_v4(),
        cancellation: CancellationToken::new(),
    }
}

#[cfg(unix)]
#[tokio::test]
async fn scoped_grep_path_with_literal_backslash_can_be_read() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("nested")).unwrap();
    std::fs::write(directory.path().join("nested/name\\part.txt"), "needle\n").unwrap();
    let scoped = CodingTools::new(directory.path()).unwrap();
    let tools = read_only_tools_from(&scoped).unwrap().tools();
    let grep = tools
        .iter()
        .find(|tool| tool.spec().name == "grep")
        .unwrap();
    let read = tools
        .iter()
        .find(|tool| tool.spec().name == "read")
        .unwrap();
    let result = grep.invoke(json!({"pattern":"needle"}), context()).await;
    let output = result.tool_content();
    assert_eq!(output, "nested/name\\part.txt:1: needle");
    let path = output.strip_suffix(":1: needle").unwrap();
    assert!(matches!(read.invoke(json!({"path":path}), context()).await,
        Outcome::Succeeded(text) if text == "needle\n"));
}

#[test]
fn factory_sets_match_pi_and_register_without_extra_tools() {
    let directory = tempfile::tempdir().unwrap();
    let coding = create_coding_tools(directory.path()).unwrap();
    assert_eq!(names(&coding), ["read", "bash", "edit", "write"]);
    let readonly = create_read_only_tools(directory.path()).unwrap();
    assert_eq!(names(&readonly), ["read", "grep", "find", "ls"]);
    let mut registry = Registry::default();
    registry.register(Arc::new(readonly)).unwrap();
    let mut registered: Vec<_> = registry
        .tool_specs()
        .into_iter()
        .map(|spec| spec.name)
        .collect();
    registered.sort();
    assert_eq!(registered, ["find", "grep", "ls", "read"]);
    assert!(create_coding_tools(&directory.path().join("missing")).is_err());
}

#[tokio::test]
async fn local_factory_tools_resolve_relative_to_supplied_cwd() {
    let directory = tempfile::tempdir().unwrap();
    let tools = create_coding_tools(directory.path()).unwrap().tools();
    let write = tools
        .iter()
        .find(|tool| tool.spec().name == "write")
        .unwrap();
    let read = tools
        .iter()
        .find(|tool| tool.spec().name == "read")
        .unwrap();
    assert!(matches!(
        write
            .invoke(
                json!({"path":"nested/file.txt","content":"hello"}),
                context()
            )
            .await,
        Outcome::Succeeded(_)
    ));
    assert_eq!(
        std::fs::read_to_string(directory.path().join("nested/file.txt")).unwrap(),
        "hello"
    );
    assert!(
        matches!(read.invoke(json!({"path":"nested/file.txt"}), context()).await, Outcome::Succeeded(text) if text.contains("hello"))
    );
}

#[tokio::test]
async fn failed_image_preparation_returns_an_omission_note() {
    struct Host;
    #[async_trait::async_trait]
    impl pi_rs::ToolHost for Host {
        fn tools(&self) -> Vec<pi_rs::ToolSpec> {
            Vec::new()
        }
        async fn call(&self, _: &str, _: serde_json::Value) -> Outcome {
            unreachable!()
        }
    }
    struct CorruptImage;
    #[async_trait::async_trait]
    impl ReadOperations for CorruptImage {
        async fn access(&self, _: &Path) -> Result<()> {
            Ok(())
        }
        async fn read_file(&self, _: &Path) -> Result<Vec<u8>> {
            Ok(b"GIF89a broken".to_vec())
        }
        async fn detect_image_mime_type(&self, _: &Path) -> Result<Option<String>> {
            Ok(Some("image/gif".into()))
        }
    }
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("broken.gif"), b"GIF89a broken").unwrap();
    let local = create_coding_tools(directory.path()).unwrap().tools();
    let local = local
        .into_iter()
        .find(|tool| tool.spec().name == "read")
        .unwrap();
    let remote =
        pi_rs_workbench::coding::create_read_tool(directory.path(), Arc::new(CorruptImage))
            .unwrap();
    for tool in [local, remote] {
        let outcome = tool.invoke(json!({"path":"broken.gif"}), context()).await;
        assert!(matches!(outcome, Outcome::Structured { ref data, .. }
            if data["content"] == json!([{"type":"text", "text":"Read image file [image/gif]\n[Image omitted: could not be resized below the inline image size limit.]"}])));
        let result = tool
            .invoke_programmatic(
                json!({"path":"broken.gif"}),
                context(),
                None,
                Arc::new(Host),
            )
            .await;
        assert!(
            matches!(result.outcome, Outcome::Succeeded(ref text) if text == "Read image file [image/gif]\n[Image omitted: could not be resized below the inline image size limit.]")
        );
    }
}

#[derive(Default)]
struct Remote {
    calls: Mutex<Vec<(String, PathBuf)>>,
    local_shell: bool,
}
#[async_trait::async_trait]
impl BashOperations for Remote {
    fn is_local(&self) -> bool {
        self.local_shell
    }
    async fn exec(
        &self,
        context: ShellSpawnContext,
        _: Option<std::time::Duration>,
        _: CancellationToken,
        out: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> Result<Option<i32>> {
        let output = if context.command == "large" {
            "remote line\n".repeat(11_000).into_bytes()
        } else {
            b"remote shell output\n".to_vec()
        };
        self.calls
            .lock()
            .unwrap()
            .push((format!("bash:{}", context.command), context.cwd));
        out.send(output).await.unwrap();
        Ok(Some(0))
    }
}
#[async_trait::async_trait]
impl ReadOperations for Remote {
    async fn access(&self, _: &Path) -> Result<()> {
        Ok(())
    }
    async fn read_file(&self, path: &Path) -> Result<Vec<u8>> {
        self.calls
            .lock()
            .unwrap()
            .push(("read".into(), path.into()));
        Ok(b"remote content".to_vec())
    }
}
#[async_trait::async_trait]
impl WriteOperations for Remote {
    async fn mkdir(&self, _: &Path) -> Result<()> {
        Ok(())
    }
    async fn write_file(&self, path: &Path, _: &str) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(("write".into(), path.into()));
        Ok(())
    }
}
#[async_trait::async_trait]
impl EditOperations for Remote {
    async fn access(&self, _: &Path) -> Result<()> {
        Ok(())
    }
    async fn read_file(&self, path: &Path) -> Result<Vec<u8>> {
        self.calls
            .lock()
            .unwrap()
            .push(("edit-read".into(), path.into()));
        Ok(b"remote content".to_vec())
    }
    async fn write_file(&self, _: &Path, _: &str) -> Result<()> {
        panic!("conditional edit required")
    }
    async fn write_if_unchanged(
        &self,
        path: &Path,
        expected: &[u8],
        content: &str,
    ) -> Result<ConditionalEdit> {
        assert_eq!(expected, b"remote content");
        assert_eq!(content, "updated content");
        self.calls
            .lock()
            .unwrap()
            .push(("edit".into(), path.into()));
        Ok(ConditionalEdit::Applied)
    }
}

#[tokio::test]
async fn remote_constructor_preserves_backend_cwd_home_and_shell_transcripts() {
    let directory = tempfile::tempdir().unwrap();
    let cwd = directory.path().join("not-present-remotely");
    let home = directory.path().join("remote-home");
    assert!(!cwd.exists());
    let backend = Arc::new(Remote::default());
    let no_home =
        create_remote_coding_tools(&cwd, backend.clone(), RemoteCodingToolOptions::default())
            .unwrap()
            .tools();
    let read = no_home
        .iter()
        .find(|tool| tool.spec().name == "read")
        .unwrap();
    assert!(matches!(
        read.invoke(json!({"path":"~/file"}), context()).await,
        Outcome::Failed(_)
    ));
    assert!(backend.calls.lock().unwrap().is_empty());
    let options = RemoteCodingToolOptions {
        paths: pi_rs_workbench::coding::RemotePathOptions {
            remote_home: Some(home.clone()),
        },
        ..Default::default()
    };
    let set = create_remote_coding_tools(&cwd, backend.clone(), options).unwrap();
    assert_eq!(names(&set), ["read", "bash", "edit", "write"]);
    let tools = set.tools();
    let tool = |name| tools.iter().find(|tool| tool.spec().name == name).unwrap();
    assert!(matches!(
        tool("read")
            .invoke(json!({"path":"~/file"}), context())
            .await,
        Outcome::Succeeded(_)
    ));
    assert!(matches!(
        tool("write")
            .invoke(json!({"path":"child/file", "content":"x"}), context())
            .await,
        Outcome::Succeeded(_)
    ));
    assert!(matches!(
        tool("edit")
            .invoke(
                json!({"path":"child/file", "edits":[{"oldText":"remote","newText":"updated"}]}),
                context()
            )
            .await,
        Outcome::SucceededWithDetails { .. }
    ));
    let result = tool("bash")
        .invoke(json!({"command":"large"}), context())
        .await;
    let path = match result {
        Outcome::TextResultWithStructuredContent {
            details: Some(details),
            ..
        } => details["fullOutputPath"].as_str().unwrap().to_owned(),
        other => panic!("unexpected shell result: {other:?}"),
    };
    let calls_before = backend.calls.lock().unwrap().len();
    assert!(
        matches!(tool("read").invoke(json!({"path":path,"offset":2,"limit":1}), context()).await, Outcome::Succeeded(text) if text.starts_with("remote line"))
    );
    assert_eq!(
        backend.calls.lock().unwrap().len(),
        calls_before,
        "transcript must use the shared output capability"
    );
    std::fs::remove_file(path).unwrap();
    {
        let calls = backend.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|(name, path)| name == "read" && *path == home.join("file"))
        );
        assert!(
            calls
                .iter()
                .any(|(name, path)| name == "write" && *path == cwd.join("child/file"))
        );
        assert!(
            calls
                .iter()
                .any(|(name, path)| name == "edit" && *path == cwd.join("child/file"))
        );
        assert!(
            calls
                .iter()
                .any(|(name, path)| name == "bash:large" && *path == cwd)
        );
    }
    let local = Arc::new(Remote {
        local_shell: true,
        ..Default::default()
    });
    assert!(create_remote_coding_tools(&cwd, local, RemoteCodingToolOptions::default()).is_err());
    assert!(
        create_remote_coding_tools(
            Path::new("relative"),
            backend.clone(),
            RemoteCodingToolOptions::default()
        )
        .is_err()
    );
    assert!(
        create_remote_coding_tools(
            &cwd,
            backend,
            RemoteCodingToolOptions {
                paths: pi_rs_workbench::coding::RemotePathOptions {
                    remote_home: Some("relative".into())
                },
                ..Default::default()
            }
        )
        .is_err()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn remote_constructor_does_not_follow_host_symlink_for_cwd() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("host-target");
    let alias = directory.path().join("remote-alias");
    std::fs::create_dir(&target).unwrap();
    std::os::unix::fs::symlink(&target, &alias).unwrap();
    let backend = Arc::new(Remote::default());
    let tools =
        create_remote_coding_tools(&alias, backend.clone(), RemoteCodingToolOptions::default())
            .unwrap()
            .tools();
    let read = tools
        .iter()
        .find(|tool| tool.spec().name == "read")
        .unwrap();
    assert!(matches!(
        read.invoke(json!({"path":"file"}), context()).await,
        Outcome::Succeeded(_)
    ));
    assert_eq!(backend.calls.lock().unwrap()[0].1, alias.join("file"));
}

#[tokio::test]
async fn configured_sets_reject_injected_operations_and_keep_scope() {
    let directory = tempfile::tempdir().unwrap();
    let backend = Arc::new(Remote::default());
    let configured = CodingTools::local(directory.path())
        .unwrap()
        .with_read_operations(backend.clone())
        .unwrap()
        .with_write_operations(backend.clone())
        .unwrap();
    assert!(coding_tools_from(&configured).is_err());
    let error = match read_only_tools_from(&configured) {
        Ok(_) => panic!("mixed remote read and local search accepted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("custom read operations"));
    assert!(backend.calls.lock().unwrap().is_empty());
    let scoped = CodingTools::new(directory.path()).unwrap();
    let read = read_only_tools_from(&scoped)
        .unwrap()
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == "read")
        .unwrap();
    assert!(matches!(
        read.invoke(json!({"path":"../outside"}), context()).await,
        Outcome::Failed(_)
    ));
}

#[tokio::test]
async fn remote_file_backends_cannot_expose_local_edit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("host.txt");
    std::fs::write(&path, "original").unwrap();
    let backend = Arc::new(Remote::default());
    let read_only = CodingTools::local(directory.path())
        .unwrap()
        .with_read_operations(backend.clone())
        .unwrap();
    let write_only = CodingTools::local(directory.path())
        .unwrap()
        .with_write_operations(backend.clone())
        .unwrap();
    let both = CodingTools::local(directory.path())
        .unwrap()
        .with_read_operations(backend.clone())
        .unwrap()
        .with_write_operations(backend)
        .unwrap();
    for source in [&read_only, &write_only, &both] {
        if let Ok(set) = coding_tools_from(source) {
            let edit = set
                .tools()
                .into_iter()
                .find(|tool| tool.spec().name == "edit")
                .unwrap();
            let result = edit
                .invoke(
                    json!({"path":path, "edits":[{"oldText":"original","newText":"modified"}]}),
                    context(),
                )
                .await;
            panic!("mixed set exposed local edit: {result:?}");
        }
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "original");
    }
}

#[tokio::test]
async fn fully_injected_file_tools_work_without_local_shell() {
    let directory = tempfile::tempdir().unwrap();
    let backend = Arc::new(Remote::default());
    let source = CodingTools::local(directory.path())
        .unwrap()
        .with_read_operations(backend.clone())
        .unwrap()
        .with_write_operations(backend.clone())
        .unwrap()
        .with_edit_operations(backend.clone())
        .unwrap();
    assert!(coding_tools_from(&source).is_err());
    let set = injected_file_tools_from(&source).unwrap();
    assert_eq!(names(&set), ["read", "edit", "write"]);
    let tools = set.tools();
    let tool = |name| tools.iter().find(|tool| tool.spec().name == name).unwrap();
    assert!(
        matches!(tool("read").invoke(json!({"path":"file"}), context()).await, Outcome::Succeeded(text) if text.contains("remote content"))
    );
    assert!(matches!(
        tool("write")
            .invoke(json!({"path":"file","content":"written"}), context())
            .await,
        Outcome::Succeeded(_)
    ));
    assert!(matches!(
        tool("edit")
            .invoke(
                json!({"path":"file","edits":[{"oldText":"remote","newText":"updated"}]}),
                context()
            )
            .await,
        Outcome::SucceededWithDetails { .. }
    ));
    assert!(!directory.path().join("file").exists());
    {
        let calls = backend.calls.lock().unwrap();
        assert!(calls.iter().any(|(name, _)| name == "read"));
        assert!(calls.iter().any(|(name, _)| name == "write"));
        assert!(calls.iter().any(|(name, _)| name == "edit"));
    }
    let set = coding_tools_from_with_local_shell(&source).unwrap();
    assert_eq!(names(&set), ["read", "bash", "edit", "write"]);
    #[cfg(unix)]
    {
        std::fs::write(directory.path().join("host-only"), "host shell fixture").unwrap();
        let tools = set.tools();
        let shell = tools
            .iter()
            .find(|tool| tool.spec().name == "bash")
            .unwrap();
        assert!(
            matches!(shell.invoke(json!({"command":"cat host-only"}), context()).await, Outcome::TextResultWithStructuredContent { output, is_error: false, .. } if output.contains("host shell fixture"))
        );
    }
}

#[tokio::test]
async fn fully_injected_coding_set_uses_one_remote_backend() {
    let directory = tempfile::tempdir().unwrap();
    let backend = Arc::new(Remote::default());
    let source = CodingTools::local(directory.path())
        .unwrap()
        .with_read_operations(backend.clone())
        .unwrap()
        .with_write_operations(backend.clone())
        .unwrap()
        .with_edit_operations(backend.clone())
        .unwrap()
        .with_bash_operations(backend.clone())
        .unwrap();
    let set = coding_tools_from(&source).unwrap();
    assert_eq!(names(&set), ["read", "bash", "edit", "write"]);
    assert!(coding_tools_from_with_local_shell(&source).is_err());
    let tools = set.tools();
    let tool = |name| tools.iter().find(|tool| tool.spec().name == name).unwrap();
    assert!(
        matches!(tool("read").invoke(json!({"path":"file"}), context()).await, Outcome::Succeeded(text) if text.contains("remote content"))
    );
    assert!(
        matches!(tool("bash").invoke(json!({"command":"remote-command"}), context()).await, Outcome::TextResultWithStructuredContent { output, is_error: false, .. } if output.contains("remote shell output"))
    );
    assert!(matches!(
        tool("write")
            .invoke(json!({"path":"file","content":"written"}), context())
            .await,
        Outcome::Succeeded(_)
    ));
    assert!(matches!(
        tool("edit")
            .invoke(
                json!({"path":"file","edits":[{"oldText":"remote","newText":"updated"}]}),
                context()
            )
            .await,
        Outcome::SucceededWithDetails { .. }
    ));
    assert!(!directory.path().join("file").exists());
    let calls = backend.calls.lock().unwrap();
    for expected in ["read", "bash:remote-command", "write", "edit"] {
        assert!(
            calls.iter().any(|(name, _)| name == expected),
            "missing {expected}: {calls:?}"
        );
    }
}

#[test]
fn coding_factory_rejects_mixed_shell_and_file_backends() {
    let directory = tempfile::tempdir().unwrap();
    for mask in 1..15 {
        let backend = Arc::new(Remote::default());
        let mut source = CodingTools::local(directory.path()).unwrap();
        if mask & 1 != 0 {
            source = source.with_read_operations(backend.clone()).unwrap();
        }
        if mask & 2 != 0 {
            source = source.with_write_operations(backend.clone()).unwrap();
        }
        if mask & 4 != 0 {
            source = source.with_edit_operations(backend.clone()).unwrap();
        }
        if mask & 8 != 0 {
            source = source.with_bash_operations(backend).unwrap();
        }
        assert!(coding_tools_from(&source).is_err(), "partial mask {mask}");
    }
    let file_backend = Arc::new(Remote::default());
    let shell_backend = Arc::new(Remote::default());
    let source = CodingTools::local(directory.path())
        .unwrap()
        .with_read_operations(file_backend.clone())
        .unwrap()
        .with_write_operations(file_backend.clone())
        .unwrap()
        .with_edit_operations(file_backend)
        .unwrap()
        .with_bash_operations(shell_backend)
        .unwrap();
    assert!(coding_tools_from(&source).is_err());
    assert!(coding_tools_from_with_local_shell(&source).is_err());
    let local = Arc::new(Remote {
        local_shell: true,
        ..Default::default()
    });
    let source = CodingTools::local(directory.path())
        .unwrap()
        .with_read_operations(local.clone())
        .unwrap()
        .with_write_operations(local.clone())
        .unwrap()
        .with_edit_operations(local.clone())
        .unwrap()
        .with_bash_operations(local)
        .unwrap();
    assert!(coding_tools_from(&source).is_err());
}

#[test]
fn injected_factories_reject_partial_or_distinct_backends() {
    let directory = tempfile::tempdir().unwrap();
    for mask in 1..7 {
        let backend = Arc::new(Remote::default());
        let mut source = CodingTools::local(directory.path()).unwrap();
        if mask & 1 != 0 {
            source = source.with_read_operations(backend.clone()).unwrap();
        }
        if mask & 2 != 0 {
            source = source.with_write_operations(backend.clone()).unwrap();
        }
        if mask & 4 != 0 {
            source = source.with_edit_operations(backend).unwrap();
        }
        assert!(coding_tools_from(&source).is_err());
        assert!(injected_file_tools_from(&source).is_err());
        assert!(coding_tools_from_with_local_shell(&source).is_err());
    }
    let read = Arc::new(Remote::default());
    let other = Arc::new(Remote::default());
    let source = CodingTools::local(directory.path())
        .unwrap()
        .with_read_operations(read)
        .unwrap()
        .with_write_operations(other.clone())
        .unwrap()
        .with_edit_operations(other)
        .unwrap();
    assert!(injected_file_tools_from(&source).is_err());
    assert!(coding_tools_from_with_local_shell(&source).is_err());
    let read_write = Arc::new(Remote::default());
    let edit = Arc::new(Remote::default());
    let source = CodingTools::local(directory.path())
        .unwrap()
        .with_read_operations(read_write.clone())
        .unwrap()
        .with_write_operations(read_write)
        .unwrap()
        .with_edit_operations(edit)
        .unwrap();
    assert!(injected_file_tools_from(&source).is_err());
}

#[test]
fn scoped_coding_set_selects_platform_shell() {
    let directory = tempfile::tempdir().unwrap();
    let source = CodingTools::new(directory.path()).unwrap();
    let set = coding_tools_from(&source).unwrap();
    let expected = if cfg!(windows) { "powershell" } else { "bash" };
    assert_eq!(names(&set), ["read", expected, "edit", "write"]);
}

#[cfg(unix)]
#[tokio::test]
async fn scoped_file_profile_does_not_sandbox_shell() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("workspace");
    std::fs::create_dir(&root).unwrap();
    let secret = directory.path().join("outside.txt");
    std::fs::write(&secret, "outside fixture").unwrap();
    let set = coding_tools_from(&CodingTools::new(&root).unwrap())
        .unwrap()
        .tools();
    let read = set.iter().find(|tool| tool.spec().name == "read").unwrap();
    assert!(matches!(
        read.invoke(json!({"path":secret}), context()).await,
        Outcome::Failed(_)
    ));
    let shell = set.iter().find(|tool| tool.spec().name == "bash").unwrap();
    assert!(
        matches!(shell.invoke(json!({"command":"cat ../outside.txt"}), context()).await, Outcome::Succeeded(output) if output.contains("outside fixture"))
    );
}

#[tokio::test]
async fn remote_read_cannot_expose_local_absolute_search_paths() {
    let directory = tempfile::tempdir().unwrap();
    let secret = directory.path().join("outside.txt");
    std::fs::write(&secret, "local secret").unwrap();
    let configured = CodingTools::local(directory.path())
        .unwrap()
        .with_read_operations(Arc::new(Remote::default()))
        .unwrap();
    // The requested grep would otherwise search this host path; no mixed set is returned.
    let candidate = read_only_tools_from(&configured);
    if let Ok(set) = candidate {
        let grep = set
            .tools()
            .into_iter()
            .find(|tool| tool.spec().name == "grep")
            .unwrap();
        let result = grep
            .invoke(json!({"pattern":"local secret", "path":secret}), context())
            .await;
        panic!("mixed set exposed local absolute path: {result:?}");
    }
}

#[derive(Default)]
struct Script(Mutex<Vec<pi_rs::ModelRequest>>);
#[async_trait::async_trait]
impl pi_rs::Model for Script {
    async fn stream(&self, request: pi_rs::ModelRequest) -> Result<pi_rs::ModelStream> {
        let mut requests = self.0.lock().unwrap();
        requests.push(request);
        let events = if requests.len() == 1 {
            vec![pi_rs::ModelEvent::Complete {
                calls: vec![pi_rs::ToolCall {
                    id: "call".into(),
                    name: "read".into(),
                    arguments: json!({"path":"note.txt"}).to_string(),
                }],
                usage: None,
            }]
        } else {
            vec![
                pi_rs::ModelEvent::Text("done".into()),
                pi_rs::ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                },
            ]
        };
        Ok(Box::pin(stream::iter(events.into_iter().map(Ok))))
    }
}

#[tokio::test]
async fn factory_registers_with_owned_sdk_session() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("note.txt"), "sdk fixture").unwrap();
    let model = Arc::new(Script::default());
    let mut session = pi_rs::SessionBuilder::new(model.clone(), "fixture")
        .extension(Arc::new(create_read_only_tools(directory.path()).unwrap()))
        .unwrap()
        .in_memory()
        .unwrap();
    assert_eq!(session.prompt("read it").await.unwrap(), "done");
    let requests = model.0.lock().unwrap();
    assert_eq!(requests[0].tools.len(), 4);
    assert!(requests[0].tools.iter().all(|spec| spec.name != "write"));
    assert!(requests[1].messages.iter().any(|message| matches!(message, pi_rs::Message::Tool { content, is_error: false, .. } if content.contains("sdk fixture"))));
}

#[derive(Default)]
struct ObserveReadOutcome(Mutex<Vec<Outcome>>);

#[async_trait::async_trait]
impl Extension for ObserveReadOutcome {
    fn id(&self) -> &str {
        "read-outcome-observer"
    }
    fn handles_tool_result(&self) -> bool {
        true
    }
    async fn tool_result(
        &self,
        call: &pi_rs::ToolCall,
        outcome: &Outcome,
        _: &ToolContext,
        _: Option<&pi_rs::ExtensionState>,
    ) -> pi_rs::ExtensionResult<Option<Outcome>> {
        if call.name == "read" {
            self.0.lock().unwrap().push(outcome.clone());
        }
        Ok(None)
    }
    fn after_tool(
        &self,
        name: &str,
        outcome: &Outcome,
        _: Option<&pi_rs::ExtensionState>,
    ) -> pi_rs::ExtensionResult<Option<pi_rs::ExtensionState>> {
        if name == "read" {
            self.0.lock().unwrap().push(outcome.clone());
        }
        Ok(None)
    }
}

#[tokio::test]
async fn sdk_read_outcomes_remain_compatible_in_hooks_and_observers() {
    for truncated in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let text = if truncated {
            "x".repeat(51_201)
        } else {
            "sdk fixture".into()
        };
        std::fs::write(directory.path().join("note.txt"), text).unwrap();
        let hook = Arc::new(ObserveReadOutcome::default());
        let mut session = pi_rs::SessionBuilder::new(Arc::new(Script::default()), "fixture")
            .extension(Arc::new(create_read_only_tools(directory.path()).unwrap()))
            .unwrap()
            .extension(hook.clone())
            .unwrap()
            .in_memory()
            .unwrap();
        let mut events = session.subscribe();
        assert_eq!(session.prompt("read it").await.unwrap(), "done");
        let values = hook.0.lock().unwrap().clone();
        assert_eq!(values.len(), 2);
        if truncated {
            assert!(
                matches!(&values[0], Outcome::SucceededWithDetails { output, details } if output.contains("exceeds 50.0KB") && details["truncation"]["firstLineExceedsLimit"] == true)
            );
        } else {
            assert_eq!(values[0], Outcome::Succeeded("sdk fixture".into()));
        }
        assert_eq!(values[0], values[1]);
        let mut outcomes = Vec::new();
        while let Ok(event) = events.try_recv() {
            if let pi_rs::SessionEvent::Record(record) = event
                && let pi_rs::Event::ToolFinished { outcome, .. } = &record.event
            {
                outcomes.push(outcome.clone());
            }
        }
        assert_eq!(outcomes, vec![values[0].clone()]);
    }
}

struct ReplaceReadOutcome(bool);
#[async_trait::async_trait]
impl Extension for ReplaceReadOutcome {
    fn id(&self) -> &str {
        "replace-read-outcome"
    }
    fn handles_tool_result(&self) -> bool {
        true
    }
    async fn tool_result(
        &self,
        call: &pi_rs::ToolCall,
        outcome: &Outcome,
        _: &ToolContext,
        _: Option<&pi_rs::ExtensionState>,
    ) -> pi_rs::ExtensionResult<Option<Outcome>> {
        if call.name == "read" {
            assert!(matches!(outcome, Outcome::Succeeded(text) if text == "sdk fixture"));
            return Ok(Some(Outcome::Succeeded(
                if self.0 { "sdk fixture" } else { "replacement" }.into(),
            )));
        }
        Ok(None)
    }
}

#[tokio::test]
async fn read_event_metadata_tracks_outcome_without_leaking_stale_content() {
    for replace in [None, Some(false), Some(true)] {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("note.txt"), "sdk fixture").unwrap();
        let mut registry = Registry::default();
        registry
            .register(Arc::new(create_read_only_tools(directory.path()).unwrap()))
            .unwrap();
        if let Some(same) = replace {
            registry
                .register(Arc::new(ReplaceReadOutcome(same)))
                .unwrap();
        } else {
            registry
                .register(Arc::new(ObserveReadOutcome::default()))
                .unwrap();
        }
        let model = Script::default();
        let agent = pi_rs::Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "",
            limits: pi_rs::Limits::default(),
        };
        let mut session = pi_rs::Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        let (_, mut inputs) = tokio::sync::mpsc::channel(1);
        let mut metadata = Vec::new();
        let mut committed = Vec::new();
        agent
            .run(
                &mut session,
                Some("read".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |event| match event {
                    pi_rs::Notification::ToolResultMetadata {
                        operation_id,
                        structured_content,
                    } => metadata.push((operation_id, structured_content.clone())),
                    pi_rs::Notification::Committed(record) => {
                        if let pi_rs::Event::ToolFinished {
                            operation_id,
                            outcome,
                            ..
                        } = &record.event
                        {
                            committed.push((*operation_id, outcome.clone()));
                        }
                    }
                    _ => {}
                },
            )
            .await
            .unwrap();
        let expected = if replace == Some(false) {
            "replacement"
        } else {
            "sdk fixture"
        };
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].1, Outcome::Succeeded(expected.into()));
        if replace.is_some() {
            assert!(metadata.is_empty());
        } else {
            assert_eq!(metadata, vec![(committed[0].0, json!("sdk fixture"))]);
        }
    }
}
