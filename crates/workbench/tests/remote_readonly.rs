use pi_rs::{CancellationToken, Extension, Outcome, Result, ToolContext};
use pi_rs_remote_find::{FindOperations, FindOptions};
use pi_rs_remote_grep::{GrepMatch, GrepOperations, GrepRequest};
use pi_rs_remote_ls::{LsOperations, LsStat};
use pi_rs_workbench::{coding::ReadOperations, coding_factories::create_remote_read_only_tools};
use serde_json::json;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Backend {
    calls: Mutex<Vec<(&'static str, PathBuf)>>,
    files: BTreeMap<PathBuf, String>,
}
impl Backend {
    fn record(&self, name: &'static str, path: &Path) {
        self.calls.lock().unwrap().push((name, path.into()));
    }
}
#[async_trait::async_trait]
impl ReadOperations for Backend {
    async fn access(&self, p: &Path) -> Result<()> {
        self.record("read", p);
        if self.files.contains_key(p) {
            Ok(())
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::NotFound).into())
        }
    }
    async fn read_file(&self, path: &Path) -> Result<Vec<u8>> {
        self.files
            .get(path)
            .map(|s| s.as_bytes().to_vec())
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound).into())
    }
    async fn detect_image_mime_type(&self, _: &Path) -> Result<Option<String>> {
        Ok(None)
    }
}
#[async_trait::async_trait]
impl FindOperations for Backend {
    async fn exists(&self, p: &Path) -> Result<bool> {
        self.record("find", p);
        Ok(true)
    }
    async fn glob(&self, _: &str, cwd: &Path, _: FindOptions) -> Result<Vec<PathBuf>> {
        Ok(self
            .files
            .keys()
            .filter_map(|path| path.strip_prefix(cwd).ok().map(Path::to_owned))
            .collect())
    }
}
#[async_trait::async_trait]
impl GrepOperations for Backend {
    async fn is_directory(&self, p: &Path) -> Result<bool> {
        self.record("grep", p);
        Ok(true)
    }
    async fn search(
        &self,
        request: GrepRequest,
        _: CancellationToken,
        output: tokio::sync::mpsc::Sender<GrepMatch>,
    ) -> Result<()> {
        for (path, contents) in &self.files {
            if let Ok(relative) = path.strip_prefix(&request.path) {
                for (index, line) in contents.lines().enumerate() {
                    if line.contains(&request.pattern) {
                        output
                            .send(GrepMatch {
                                path: relative.to_string_lossy().into_owned(),
                                line_number: index as u64 + 1,
                                line: line.into(),
                                before: vec![],
                                after: vec![],
                            })
                            .await
                            .unwrap();
                    }
                }
            }
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl LsOperations for Backend {
    async fn exists(&self, p: &Path) -> Result<bool> {
        self.record("ls", p);
        Ok(true)
    }
    async fn stat(&self, _: &Path) -> Result<LsStat> {
        Ok(LsStat { is_directory: true })
    }
    async fn readdir(&self, path: &Path) -> Result<Vec<String>> {
        Ok(self
            .files
            .keys()
            .filter_map(|file| file.strip_prefix(path).ok()?.to_str().map(str::to_owned))
            .collect())
    }
}
fn context() -> ToolContext {
    ToolContext {
        operation_id: uuid::Uuid::new_v4(),
        cancellation: CancellationToken::new(),
    }
}
#[tokio::test]
async fn all_tools_share_backend_and_home_without_local_files() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("remote-root");
    let home = base.path().join("remote-home");
    let backend = Arc::new(Backend {
        calls: Mutex::new(vec![]),
        files: [(home.join("dir/file"), "remote needle".into())].into(),
    });
    let set = create_remote_read_only_tools(&root, backend.clone(), Some(home.clone())).unwrap();
    let tools = set.tools();
    assert_eq!(
        tools.iter().map(|t| t.spec().name).collect::<Vec<_>>(),
        ["read", "grep", "find", "ls"]
    );
    for tool in tools {
        let name = tool.spec().name;
        let args = match name.as_str() {
            "grep" => json!({"path":"~/dir", "pattern":"needle"}),
            "find" => json!({"path":"~/dir", "pattern":"*"}),
            "read" => json!({"path":"~/dir/file"}),
            _ => json!({"path":"~/dir"}),
        };
        let result = tool.invoke(args, context()).await;
        let output = match result {
            Outcome::Succeeded(text) | Outcome::SucceededWithDetails { output: text, .. } => text,
            other => panic!("{name}: {other:?}"),
        };
        assert!(
            output.contains(match name.as_str() {
                "read" | "grep" => "remote needle",
                _ => "file",
            }),
            "{name}: {output}"
        );
    }
    let calls = backend.calls.lock().unwrap();
    assert_eq!(calls.len(), 4);
    for (name, path) in calls.iter() {
        assert_eq!(
            path,
            &home.join(if *name == "read" { "dir/file" } else { "dir" })
        );
    }
}
#[tokio::test]
async fn absent_home_rejects_tilde_for_all_tools_before_backend_calls() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let set = create_remote_read_only_tools(root.path(), backend.clone(), None).unwrap();
    for tool in set.tools() {
        let name = tool.spec().name;
        let args = match name.as_str() {
            "grep" => json!({"path":"~/dir", "pattern":"needle"}),
            "find" => json!({"path":"~/dir", "pattern":"*"}),
            "read" => json!({"path":"~/dir/file"}),
            _ => json!({"path":"~/dir"}),
        };
        assert!(matches!(
            tool.invoke(args, context()).await,
            Outcome::Failed(_)
        ));
    }
    assert!(backend.calls.lock().unwrap().is_empty());
}

struct ModelScript(Mutex<Vec<pi_rs::ModelRequest>>);
#[async_trait::async_trait]
impl pi_rs::Model for ModelScript {
    async fn stream(&self, request: pi_rs::ModelRequest) -> Result<pi_rs::ModelStream> {
        let mut requests = self.0.lock().unwrap();
        requests.push(request);
        let calls = if requests.len() == 1 {
            [
                ("read", json!({"path":"~/dir/file"})),
                ("grep", json!({"path":"~/dir", "pattern":"needle"})),
                ("find", json!({"path":"~/dir", "pattern":"*"})),
                ("ls", json!({"path":"~/dir"})),
            ]
            .into_iter()
            .map(|(name, args)| pi_rs::ToolCall {
                id: name.into(),
                name: name.into(),
                arguments: args.to_string(),
            })
            .collect()
        } else {
            vec![]
        };
        Ok(Box::pin(futures_util::stream::iter(vec![Ok(
            pi_rs::ModelEvent::Complete { calls, usage: None },
        )])))
    }
}
#[tokio::test]
async fn model_calls_use_one_remote_namespace() {
    let base = tempfile::tempdir().unwrap();
    let home = base.path().join("backend-home");
    let backend = Arc::new(Backend {
        calls: Mutex::new(vec![]),
        files: [(home.join("dir/file"), "remote needle".into())].into(),
    });
    let set = create_remote_read_only_tools(
        &base.path().join("backend-root"),
        backend.clone(),
        Some(home.clone()),
    )
    .unwrap();
    let mut registry = pi_rs::Registry::default();
    registry.register(Arc::new(set)).unwrap();
    let model = ModelScript(Mutex::new(vec![]));
    let agent = pi_rs::Agent {
        model: &model,
        model_name: "fixture",
        registry: &registry,
        system_prompt: "fixture",
        limits: pi_rs::Limits::default(),
    };
    let mut session = pi_rs::Session::with_store(pi_rs::MemoryStore::default()).unwrap();
    let (_, mut inputs) = tokio::sync::mpsc::channel(1);
    agent
        .run(
            &mut session,
            Some("search".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut |_: pi_rs::Notification<'_>| {},
        )
        .await
        .unwrap();
    let requests = model.0.lock().unwrap();
    let results: Vec<_> = requests[1]
        .messages
        .iter()
        .filter_map(|m| {
            if let pi_rs::Message::Tool {
                call_id,
                content,
                is_error,
                ..
            } = m
            {
                Some((call_id, content, is_error))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(results.len(), 4);
    for (id, content, is_error) in results {
        assert!(!is_error, "{id}: {content}");
        assert!(
            content.contains(match id.as_str() {
                "read" | "grep" => "remote needle",
                _ => "file",
            }),
            "{id}: {content}"
        );
    }
    let calls = backend.calls.lock().unwrap();
    assert_eq!(calls.len(), 4);
    assert!(
        calls.iter().all(
            |(name, path)| path == &home.join(if *name == "read" { "dir/file" } else { "dir" })
        )
    );
}
