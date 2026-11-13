use futures_util::stream;
use pi_rs::*;
use pi_rs_workbench::coding::{
    BashOperations, CodingTools, ReadOperations, ShellConfig, ShellOutputStore, ShellSpawnContext,
    create_bash_tool, create_bash_tool_with_shell_outputs, create_read_tool_with_shell_outputs,
};
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

#[derive(Default)]
struct EnvironmentCapture(
    Mutex<Vec<std::collections::BTreeMap<std::ffi::OsString, std::ffi::OsString>>>,
);

#[async_trait::async_trait]
impl BashOperations for EnvironmentCapture {
    async fn exec(
        &self,
        context: ShellSpawnContext,
        _: Option<Duration>,
        _: CancellationToken,
        _: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> Result<Option<i32>> {
        self.0.lock().unwrap().push(context.env);
        Ok(Some(0))
    }
}

#[tokio::test]
async fn remote_environment_is_opt_in_and_hook_can_supply_variables() {
    let capture = Arc::new(EnvironmentCapture::default());
    let context = || ToolContext {
        operation_id: uuid::Uuid::new_v4(),
        cancellation: CancellationToken::new(),
    };
    let hook = Arc::new(|mut context: ShellSpawnContext| {
        context
            .env
            .insert("EXPLICIT_REMOTE_VALUE".into(), "allowed".into());
        Ok(context)
    });
    let tool = create_bash_tool(
        Path::new("/remote"),
        ShellConfig::default().with_spawn_hook(hook),
        capture.clone(),
    )
    .unwrap();
    tool.invoke(json!({"command":"test"}), context()).await;
    let tool = create_bash_tool(
        Path::new("/remote"),
        ShellConfig::default().with_remote_environment(true),
        capture.clone(),
    )
    .unwrap();
    tool.invoke(json!({"command":"test"}), context()).await;
    let captured = capture.0.lock().unwrap();
    assert_eq!(captured[0].len(), 1);
    assert_eq!(
        captured[0]
            .get(std::ffi::OsStr::new("EXPLICIT_REMOTE_VALUE"))
            .unwrap(),
        "allowed"
    );
    if std::env::var_os("PATH").is_some() {
        assert!(captured[1].contains_key(std::ffi::OsStr::new("PATH")));
    }
}

struct DenyRead;
#[async_trait::async_trait]
impl ReadOperations for DenyRead {
    async fn access(&self, _: &Path) -> Result<()> {
        Err(Error::Config("remote access denied".into()))
    }
    async fn read_file(&self, _: &Path) -> Result<Vec<u8>> {
        unreachable!()
    }
}

struct Lines;
#[async_trait::async_trait]
impl BashOperations for Lines {
    async fn exec(
        &self,
        _: ShellSpawnContext,
        _: Option<Duration>,
        _: CancellationToken,
        out: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> Result<Option<i32>> {
        out.send("hello\n".repeat(11_000).into_bytes())
            .await
            .unwrap();
        Ok(Some(0))
    }
}

#[tokio::test]
async fn paired_remote_read_can_page_only_its_shell_transcripts() {
    let store = ShellOutputStore::default();
    let bash = create_bash_tool_with_shell_outputs(
        Path::new("/remote"),
        ShellConfig::default(),
        Arc::new(Lines),
        store.clone(),
    )
    .unwrap();
    let read = create_read_tool_with_shell_outputs(Path::new("/remote"), Arc::new(DenyRead), store)
        .unwrap();
    let unrelated = create_read_tool_with_shell_outputs(
        Path::new("/remote"),
        Arc::new(DenyRead),
        ShellOutputStore::default(),
    )
    .unwrap();
    let context = || ToolContext {
        operation_id: uuid::Uuid::new_v4(),
        cancellation: CancellationToken::new(),
    };
    let result = bash.invoke(json!({"command":"large"}), context()).await;
    let path = match result {
        Outcome::SucceededWithDetails { details, .. } => {
            details["fullOutputPath"].as_str().unwrap().to_owned()
        }
        other => panic!("unexpected result: {other:?}"),
    };
    let page = read
        .invoke(json!({"path":path, "offset":2, "limit":1}), context())
        .await;
    assert!(matches!(page, Outcome::Succeeded(text) if text.starts_with("hello\n")));
    let denied = unrelated.invoke(json!({"path":path}), context()).await;
    assert!(matches!(denied, Outcome::Failed(text) if text.contains("remote access denied")));
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn coding_tools_share_transcripts_between_injected_backends() {
    let root = tempfile::tempdir().unwrap();
    let tools = CodingTools::local(root.path())
        .unwrap()
        .with_bash_operations(Arc::new(Lines))
        .unwrap()
        .with_read_operations(Arc::new(DenyRead))
        .unwrap()
        .tools();
    let bash = tools
        .iter()
        .find(|tool| tool.spec().name == "bash")
        .unwrap();
    let read = tools
        .iter()
        .find(|tool| tool.spec().name == "read")
        .unwrap();
    let context = || ToolContext {
        operation_id: uuid::Uuid::new_v4(),
        cancellation: CancellationToken::new(),
    };
    let result = bash.invoke(json!({"command":"large"}), context()).await;
    let path = match result {
        Outcome::SucceededWithDetails { details, .. } => {
            details["fullOutputPath"].as_str().unwrap().to_owned()
        }
        other => panic!("unexpected result: {other:?}"),
    };
    let page = read
        .invoke(json!({"path":path, "offset":2, "limit":1}), context())
        .await;
    assert!(matches!(page, Outcome::Succeeded(text) if text.starts_with("hello\n")));
    std::fs::remove_file(path).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn cancellation_after_shell_exit_kills_descendant_during_pipe_drain() {
    let root = tempfile::tempdir().unwrap();
    let marker = root.path().join("must-not-exist");
    let ready = root.path().join("ready");
    let bash = create_bash_tool(
        root.path(),
        ShellConfig::default(),
        pi_rs_workbench::coding::create_local_bash_operations(None),
    )
    .unwrap();
    let token = CancellationToken::new();
    let task_token = token.clone();
    let task = tokio::spawn(async move {
        bash.invoke(
            json!({"command":"echo ready > ready; (sleep 0.8; touch must-not-exist) &"}),
            ToolContext {
                operation_id: uuid::Uuid::new_v4(),
                cancellation: task_token,
            },
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !ready.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    token.cancel();
    task.await.unwrap();
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(!marker.exists(), "cancelled descendant continued running");
}
#[async_trait::async_trait]
impl BashOperations for Remote {
    async fn exec(
        &self,
        context: ShellSpawnContext,
        _: Option<Duration>,
        cancel: CancellationToken,
        output: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> Result<Option<i32>> {
        self.calls
            .lock()
            .unwrap()
            .push((context.command.clone(), context.cwd));
        output.send(b"first".to_vec()).await.unwrap();
        if context.command.contains("wait") {
            cancel.cancelled().await;
            return Ok(None);
        }
        if context.command.contains("error") {
            return Err(Error::Config("remote failure".into()));
        }
        if context.command.contains("large") {
            output.send(vec![b'x'; 60_000]).await.unwrap();
        }
        output.send(b"second".to_vec()).await.unwrap();
        Ok(Some(if context.command.contains("fail") {
            7
        } else {
            0
        }))
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
                        name: "bash".into(),
                        arguments: json!({"command":"run"}).to_string(),
                    },
                    ToolCall {
                        id: "invalid".into(),
                        name: "bash".into(),
                        arguments: json!({"command":"bad", "timeout":0}).to_string(),
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
async fn model_calls_remote_shell_with_validation_and_prefix() {
    let root = tempfile::tempdir().unwrap();
    let remote = Arc::new(Remote::default());
    let config = ShellConfig::new(None, Some("setup"));
    let extension = CodingTools::local(root.path())
        .unwrap()
        .with_shell_config(config)
        .with_bash_operations(remote.clone())
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
            Some("run".into()),
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
    assert!(
        results
            .iter()
            .any(|(id, text, error)| *id == "one" && *text == "firstsecond" && !error)
    );
    assert!(
        results
            .iter()
            .any(|(id, _, error)| *id == "invalid" && *error)
    );
    assert_eq!(
        remote.calls.lock().unwrap().as_slice(),
        &[("setup\nrun".into(), root.path().canonicalize().unwrap())]
    );
}
#[tokio::test]
async fn remote_shell_cancellation_timeout_exit_and_truncation() {
    let root = tempfile::tempdir().unwrap();
    let remote = Arc::new(Remote::default());
    assert!(
        CodingTools::new(root.path())
            .unwrap()
            .with_bash_operations(remote.clone())
            .is_err()
    );
    let tool = create_bash_tool(Path::new("/remote"), ShellConfig::default(), remote).unwrap();
    let context = || ToolContext {
        operation_id: uuid::Uuid::new_v4(),
        cancellation: CancellationToken::new(),
    };
    assert!(
        matches!(tool.invoke(json!({"command":"fail"}), context()).await, Outcome::Failed(text) if text.contains("firstsecond") && text.contains("code 7"))
    );
    assert!(
        matches!(tool.invoke(json!({"command":"large"}), context()).await, Outcome::SucceededWithDetails { details, .. } if details["truncation"]["truncated"] == true)
    );
    assert!(
        matches!(tool.invoke(json!({"command":"wait", "timeout":0.01}), context()).await, Outcome::Failed(text) if text.contains("first") && text.contains("timed out"))
    );
    let token = CancellationToken::new();
    let t = token.clone();
    let task = tokio::spawn(async move {
        tool.invoke(
            json!({"command":"wait"}),
            ToolContext {
                operation_id: uuid::Uuid::new_v4(),
                cancellation: t,
            },
        )
        .await
    });
    tokio::task::yield_now().await;
    token.cancel();
    assert!(
        matches!(task.await.unwrap(), Outcome::Failed(text) if text.contains("aborted") || text.contains("cancelled"))
    );
}

#[cfg(unix)]
#[tokio::test]
async fn local_shell_operations_factory_streams_and_applies_exit_status() {
    let root = tempfile::tempdir().unwrap();
    let tool = create_bash_tool(
        root.path(),
        ShellConfig::new(None, Some("export REMOTE_TEST=ok")),
        pi_rs_workbench::coding::create_local_bash_operations(None),
    )
    .unwrap();
    let context = ToolContext {
        operation_id: uuid::Uuid::new_v4(),
        cancellation: CancellationToken::new(),
    };
    let result = tool
        .invoke(
            json!({"command":"printf %s \\\"$REMOTE_TEST\\\"; printf err >&2; exit 4"}),
            context,
        )
        .await;
    assert!(
        matches!(result, Outcome::Failed(text) if text.contains("ok") && text.contains("err") && text.contains("code 4"))
    );
}

#[cfg(unix)]
#[tokio::test]
async fn local_operations_stop_when_output_channel_is_full() {
    use std::collections::BTreeMap;
    for timed in [false, true] {
        let ops = pi_rs_workbench::coding::create_local_bash_operations(None);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let token = CancellationToken::new();
        let child_token = token.clone();
        let task = tokio::spawn(async move {
            ops.exec(
                ShellSpawnContext {
                    command: "while :; do printf 1234567890; done".into(),
                    cwd: std::env::current_dir().unwrap(),
                    env: std::env::vars_os().collect::<BTreeMap<_, _>>(),
                },
                timed.then_some(Duration::from_millis(200)),
                child_token,
                sender,
            )
            .await
        });
        receiver.recv().await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        if !timed {
            token.cancel();
        }
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("blocked output must not prevent termination")
            .unwrap();
        assert!(result.is_err());
    }
}

#[cfg(windows)]
#[tokio::test]
async fn bash_injection_does_not_replace_powershell() {
    let root = tempfile::tempdir().unwrap();
    let remote = Arc::new(Remote::default());
    let extension = CodingTools::local(root.path())
        .unwrap()
        .with_bash_operations(remote.clone())
        .unwrap();
    let powershell = extension
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == "powershell")
        .unwrap();
    let result = powershell
        .invoke(
            json!({"command":"Write-Output 'powershell-only'"}),
            ToolContext {
                operation_id: uuid::Uuid::new_v4(),
                cancellation: CancellationToken::new(),
            },
        )
        .await;
    assert!(matches!(result, Outcome::Succeeded(text) if text.contains("powershell-only")));
    assert!(remote.calls.lock().unwrap().is_empty());
}
