use pi_rs::{CancellationToken, Extension, Outcome, Result, ToolContext};
use pi_rs_remote_find::{FindOperations, FindOptions};
use pi_rs_remote_grep::{GrepMatch, GrepOperations, GrepRequest};
use pi_rs_remote_ls::{LsOperations, LsStat};
use pi_rs_workbench::{coding::ReadOperations, coding_factories::create_remote_read_only_tools};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Backend(Mutex<Vec<(&'static str, PathBuf)>>);
impl Backend {
    fn record(&self, name: &'static str, path: &Path) {
        self.0.lock().unwrap().push((name, path.into()));
    }
}
#[async_trait::async_trait]
impl ReadOperations for Backend {
    async fn access(&self, p: &Path) -> Result<()> {
        self.record("read", p);
        Ok(())
    }
    async fn read_file(&self, _: &Path) -> Result<Vec<u8>> {
        Ok(b"remote".to_vec())
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
    async fn glob(&self, _: &str, _: &Path, _: FindOptions) -> Result<Vec<PathBuf>> {
        Ok(vec!["file".into()])
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
        _: GrepRequest,
        _: CancellationToken,
        output: tokio::sync::mpsc::Sender<GrepMatch>,
    ) -> Result<()> {
        output
            .send(GrepMatch {
                path: "file".into(),
                line_number: 1,
                line: "remote".into(),
                before: vec![],
                after: vec![],
            })
            .await
            .unwrap();
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
    async fn readdir(&self, _: &Path) -> Result<Vec<String>> {
        Ok(vec![])
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
    let backend = Arc::new(Backend::default());
    let set = create_remote_read_only_tools(&root, backend.clone(), Some(home.clone())).unwrap();
    let tools = set.tools();
    assert_eq!(
        tools.iter().map(|t| t.spec().name).collect::<Vec<_>>(),
        ["read", "grep", "find", "ls"]
    );
    for tool in tools {
        let name = tool.spec().name;
        let args = match name.as_str() {
            "grep" => json!({"path":"~/dir", "pattern":"x"}),
            "find" => json!({"path":"~/dir", "pattern":"*"}),
            _ => json!({"path":"~/dir"}),
        };
        let result = tool.invoke(args, context()).await;
        assert!(
            matches!(
                result,
                Outcome::Succeeded(_) | Outcome::SucceededWithDetails { .. }
            ),
            "{name}: {result:?}"
        );
    }
    let calls = backend.0.lock().unwrap();
    assert_eq!(calls.len(), 4);
    for (_, path) in calls.iter() {
        assert_eq!(path, &home.join("dir"));
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
            "grep" => json!({"path":"~/dir", "pattern":"x"}),
            "find" => json!({"path":"~/dir", "pattern":"*"}),
            _ => json!({"path":"~/dir"}),
        };
        assert!(matches!(
            tool.invoke(args, context()).await,
            Outcome::Failed(_)
        ));
    }
    assert!(backend.0.lock().unwrap().is_empty());
}

struct ModelScript(Mutex<Vec<pi_rs::ModelRequest>>);
#[async_trait::async_trait]
impl pi_rs::Model for ModelScript {
    async fn stream(&self, request: pi_rs::ModelRequest) -> Result<pi_rs::ModelStream> {
        let mut requests = self.0.lock().unwrap();
        requests.push(request);
        let calls = if requests.len() == 1 {
            [
                ("read", json!({"path":"~/dir"})),
                ("grep", json!({"path":"~/dir", "pattern":"x"})),
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
    let backend = Arc::new(Backend::default());
    let home = base.path().join("backend-home");
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
            if let pi_rs::Message::Tool { is_error, .. } = m {
                Some(*is_error)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(results, [false; 4]);
    let calls = backend.0.lock().unwrap();
    assert_eq!(calls.len(), 4);
    assert!(calls.iter().all(|(_, path)| path == &home.join("dir")));
}
