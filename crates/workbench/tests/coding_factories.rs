use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use futures_util::stream;
use pi_rs::{CancellationToken, Extension, Outcome, Registry, Result, ToolContext};
use pi_rs_workbench::{
    coding::{CodingTools, ReadOperations, WriteOperations},
    coding_factories::{
        coding_tools_from, create_coding_tools, create_read_only_tools, read_only_tools_from,
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

#[derive(Default)]
struct Remote {
    calls: Mutex<Vec<(String, PathBuf)>>,
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

#[tokio::test]
async fn configured_sets_keep_injected_operations_and_scope() {
    let directory = tempfile::tempdir().unwrap();
    let backend = Arc::new(Remote::default());
    let configured = CodingTools::local(directory.path())
        .unwrap()
        .with_read_operations(backend.clone())
        .unwrap()
        .with_write_operations(backend.clone())
        .unwrap();
    let coding = coding_tools_from(&configured).unwrap().tools();
    let error = match read_only_tools_from(&configured) {
        Ok(_) => panic!("mixed remote read and local search accepted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("custom read operations"));
    let read = coding
        .iter()
        .find(|tool| tool.spec().name == "read")
        .unwrap();
    let write = coding
        .iter()
        .find(|tool| tool.spec().name == "write")
        .unwrap();
    assert!(
        matches!(read.invoke(json!({"path":"remote"}), context()).await, Outcome::Succeeded(text) if text.contains("remote content"))
    );
    assert!(matches!(
        write
            .invoke(json!({"path":"remote","content":"hello"}), context())
            .await,
        Outcome::Succeeded(_)
    ));
    assert!(!directory.path().join("remote").exists());
    assert_eq!(backend.calls.lock().unwrap().len(), 2);
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
