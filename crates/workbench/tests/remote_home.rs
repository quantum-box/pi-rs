use pi_rs::{CancellationToken, Extension, Outcome, Result, Tool, ToolContext};
use pi_rs_workbench::coding::{
    CodingTools, ConditionalEdit, EditOperations, EditToolOptions, ReadOperations,
    RemotePathOptions, WriteOperations, create_edit_tool_with_path_options, create_read_tool,
    create_read_tool_with_options, create_write_tool, create_write_tool_with_options,
};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Backend(Mutex<Vec<PathBuf>>);
impl Backend {
    fn record(&self, path: &Path) {
        self.0.lock().unwrap().push(path.into());
    }
}
#[async_trait::async_trait]
impl ReadOperations for Backend {
    async fn access(&self, path: &Path) -> Result<()> {
        self.record(path);
        Ok(())
    }
    async fn read_file(&self, _: &Path) -> Result<Vec<u8>> {
        Ok(b"old".to_vec())
    }
    async fn detect_image_mime_type(&self, _: &Path) -> Result<Option<String>> {
        Ok(None)
    }
}
#[async_trait::async_trait]
impl WriteOperations for Backend {
    async fn mkdir(&self, _: &Path) -> Result<()> {
        Ok(())
    }
    async fn write_file(&self, path: &Path, _: &str) -> Result<()> {
        self.record(path);
        Ok(())
    }
}
#[async_trait::async_trait]
impl EditOperations for Backend {
    async fn access(&self, path: &Path) -> Result<()> {
        self.record(path);
        Ok(())
    }
    async fn read_file(&self, _: &Path) -> Result<Vec<u8>> {
        Ok(b"old".to_vec())
    }
    async fn write_file(&self, _: &Path, _: &str) -> Result<()> {
        unreachable!()
    }
    async fn write_if_unchanged(&self, _: &Path, _: &[u8], _: &str) -> Result<ConditionalEdit> {
        Ok(ConditionalEdit::Applied)
    }
}
fn context() -> ToolContext {
    ToolContext {
        operation_id: uuid::Uuid::new_v4(),
        cancellation: CancellationToken::new(),
    }
}
fn args(name: &str) -> Value {
    match name {
        "read" => json!({"path":"@~/file"}),
        "write" => json!({"path":"@~/file", "content":"x"}),
        _ => json!({"path":"@~/file", "oldText":"old", "newText":"new"}),
    }
}
async fn exercise(tools: Vec<Arc<dyn Tool>>, backend: &Backend, expected: Option<&Path>) {
    for tool in tools {
        let outcome = tool.invoke(args(&tool.spec().name), context()).await;
        match expected {
            Some(path) => {
                assert!(
                    matches!(
                        outcome,
                        Outcome::Succeeded(_) | Outcome::SucceededWithDetails { .. }
                    ),
                    "{outcome:?}"
                );
                assert_eq!(backend.0.lock().unwrap().pop().unwrap(), path);
            }
            None => {
                assert!(
                    matches!(outcome, Outcome::Failed(ref text) if text.contains("home directory is unavailable")),
                    "{outcome:?}"
                );
                assert!(backend.0.lock().unwrap().is_empty());
            }
        }
        backend.0.lock().unwrap().clear();
    }
}
#[tokio::test]
async fn injected_factories_use_configured_home_or_reject_tilde() {
    let root = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let options = RemotePathOptions {
        remote_home: Some(home.path().to_owned()),
    };
    let tools = vec![
        create_read_tool_with_options(root.path(), backend.clone(), options.clone()).unwrap(),
        create_write_tool_with_options(root.path(), backend.clone(), options.clone()).unwrap(),
        create_edit_tool_with_path_options(
            root.path(),
            backend.clone(),
            EditToolOptions::default(),
            options,
        )
        .unwrap(),
    ];
    exercise(tools, &backend, Some(&home.path().join("file"))).await;
    let tools = vec![
        create_read_tool(root.path(), backend.clone()).unwrap(),
        create_write_tool(root.path(), backend.clone()).unwrap(),
        create_edit_tool_with_path_options(
            root.path(),
            backend.clone(),
            EditToolOptions::default(),
            RemotePathOptions::default(),
        )
        .unwrap(),
    ];
    exercise(tools, &backend, None).await;
    assert!(
        create_read_tool_with_options(
            root.path(),
            backend,
            RemotePathOptions {
                remote_home: Some("relative".into())
            }
        )
        .is_err()
    );
}
#[tokio::test]
async fn coding_tools_apply_shared_backend_home_only_to_injected_tools() {
    let root = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    for configured in [false, true] {
        let tools = CodingTools::local(root.path())
            .unwrap()
            .with_read_operations(backend.clone())
            .unwrap()
            .with_write_operations(backend.clone())
            .unwrap()
            .with_edit_operations(backend.clone())
            .unwrap()
            .with_remote_home(configured.then(|| home.path().to_owned()))
            .unwrap();
        let selected = tools
            .tools()
            .into_iter()
            .filter(|tool| ["read", "write", "edit"].contains(&tool.spec().name.as_str()))
            .collect();
        exercise(
            selected,
            &backend,
            configured.then_some(home.path().join("file")).as_deref(),
        )
        .await;
    }
}
