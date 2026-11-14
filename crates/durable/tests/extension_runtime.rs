use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    CancellationToken, Error, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool,
    ToolCall, ToolContext, ToolSpec,
};
use pi_rs_durable::{
    extension::{BeforeTool, Context, Extension, Hook, Memo, Section, Sleep, ToolCall as HookCall},
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, Harness, Options, RegisteredTool, Status},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Default)]
struct FakeModel(Mutex<Vec<ModelRequest>>);
#[async_trait]
impl Model for FakeModel {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let answered = request
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }));
        self.0.lock().unwrap().push(request);
        let events = if answered {
            vec![
                Ok(ModelEvent::Text("done".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ]
        } else {
            vec![Ok(ModelEvent::Complete {
                calls: vec![ToolCall {
                    id: "call".into(),
                    name: "test".into(),
                    arguments: "{\"n\":1}".into(),
                }],
                usage: None,
            })]
        };
        Ok(stream::iter(events).boxed())
    }
}
#[derive(Default)]
struct FakeTool(Mutex<Vec<Value>>);
#[async_trait]
impl Tool for FakeTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "test".into(),
            description: String::new(),
            parameters: json!({"type":"object","properties":{"n":{"type":"integer"}},"required":["n"],"additionalProperties":false}),
        }
    }
    async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
        self.0.lock().unwrap().push(args);
        Outcome::Succeeded("ok".into())
    }
}
fn options(model: Arc<FakeModel>, tool: Arc<FakeTool>) -> Options {
    Options {
        model,
        tools: vec![RegisteredTool {
            tool,
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn config() -> Config {
    Config {
        model: "fake".into(),
        system_prompt: "base".into(),
        max_output_tokens: 100,
    }
}
struct Prompt(&'static str);
#[async_trait]
impl Section for Prompt {
    fn key(&self) -> &str {
        "extra"
    }
    async fn render(&self, _: &Context<'_>) -> Result<Option<String>, String> {
        Ok(Some(self.0.into()))
    }
}
struct Rewrite(i64);
#[async_trait]
impl Hook for Rewrite {
    async fn before_tool(&self, call: &HookCall, _: &Context<'_>) -> Result<BeforeTool, String> {
        Ok(BeforeTool::Rewrite(
            json!({"n":call.arguments["n"].as_i64().unwrap() * 10 + self.0}),
        ))
    }
}
struct Stop(&'static str);
#[async_trait]
impl Hook for Stop {
    async fn before_tool(&self, _: &HookCall, _: &Context<'_>) -> Result<BeforeTool, String> {
        Ok(match self.0 {
            "sleep" => BeforeTool::Sleep(Sleep {
                signals: vec!["wake".into()],
                until_millis: None,
            }),
            "invalid" => BeforeTool::Rewrite(json!({"n":"bad"})),
            _ => BeforeTool::Block("blocked by policy".into()),
        })
    }
}
fn ext(name: &str, section: &'static str, hook: Arc<dyn Hook>) -> Extension {
    Extension {
        name: name.into(),
        sections: vec![Arc::new(Prompt(section))],
        hooks: vec![hook],
        ..Default::default()
    }
}
fn tool_message(c: &pi_rs_durable::runtime::Conversation) -> String {
    c.messages
        .iter()
        .find_map(|m| {
            if let Message::Tool { content, .. } = m {
                Some(content.clone())
            } else {
                None
            }
        })
        .unwrap()
}

#[tokio::test]
async fn selection_persists_and_hooks_run_in_order_with_last_section_winning() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(FakeModel::default());
    let tool = Arc::new(FakeTool::default());
    let id;
    {
        let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
        h.install_extension(ext("a", "first", Arc::new(Rewrite(2))))
            .unwrap();
        h.install_extension(ext("b", "second", Arc::new(Rewrite(3))))
            .unwrap();
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["b".into(), "a".into()])
            .unwrap();
    }
    let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
    h.install_extension(ext("a", "first", Arc::new(Rewrite(2))))
        .unwrap();
    h.install_extension(ext("b", "second", Arc::new(Rewrite(3))))
        .unwrap();
    assert_eq!(h.conversation(id).unwrap().extensions, vec!["b", "a"]);
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(*tool.0.lock().unwrap(), vec![json!({"n":132})]);
    let requests = model.0.lock().unwrap();
    assert!(
        matches!(&requests[0].messages[0],Message::System{content} if content == "base\n\n<extra>\nfirst\n</extra>")
    );
}

#[tokio::test]
async fn block_sleep_and_invalid_rewrite_never_dispatch() {
    for case in ["block", "sleep", "invalid"] {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(FakeModel::default());
        let tool = Arc::new(FakeTool::default());
        let mut h =
            Harness::open_paused(dir.path().join("db"), options(model, tool.clone())).unwrap();
        h.install_extension(ext("policy", "policy", Arc::new(Stop(case))))
            .unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["policy".into()]).unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert!(tool.0.lock().unwrap().is_empty());
        let message = tool_message(&h.conversation(id).unwrap());
        assert!(
            match case {
                "block" => message.contains("blocked by policy"),
                "sleep" => message.contains("sleep is unsupported"),
                _ => message.contains("schema"),
            },
            "{case}: {message}"
        );
    }
}

struct CrashHook;
#[async_trait]
impl Hook for CrashHook {
    async fn before_tool(&self, _: &HookCall, ctx: &Context<'_>) -> Result<BeforeTool, String> {
        let operation = ctx.snapshot["phase"]["ToolPreparing"]["operation_id"].clone();
        let stored = ctx
            .memo
            .get_or_insert("operation", operation.clone())
            .await?;
        assert_eq!(stored, operation);
        if std::env::var_os("PI_EXTENSION_CRASH_DB").is_some() {
            std::process::abort();
        }
        Ok(BeforeTool::Pass)
    }
}
#[tokio::test]
async fn crash_during_hook_child() {
    let Ok(path) = std::env::var("PI_EXTENSION_CRASH_DB") else {
        return;
    };
    let mut h = Harness::open_paused(
        &path,
        options(
            Arc::new(FakeModel::default()),
            Arc::new(FakeTool::default()),
        ),
    )
    .unwrap();
    h.install_extension(ext("crash", "section", Arc::new(CrashHook)))
        .unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["crash".into()]).unwrap();
    std::fs::write(format!("{path}.id"), id.to_string()).unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    panic!("hook did not abort");
}
#[tokio::test]
async fn crash_during_hook_reuses_operation_memo_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_during_hook_child"])
        .env("PI_EXTENSION_CRASH_DB", &path)
        .status()
        .unwrap();
    assert!(!status.success());
    let id: Uuid = std::fs::read_to_string(format!("{}.id", path.display()))
        .unwrap()
        .parse()
        .unwrap();
    let tool = Arc::new(FakeTool::default());
    let mut h =
        Harness::open_paused(&path, options(Arc::new(FakeModel::default()), tool.clone())).unwrap();
    let state = serde_json::to_value(h.conversation(id).unwrap()).unwrap();
    let operation = state["phase"]["ToolPreparing"]["operation_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        h.extension_memo(operation)
            .unwrap()
            .get("operation")
            .await
            .unwrap(),
        Some(json!(operation))
    );
    h.install_extension(ext("crash", "section", Arc::new(CrashHook)))
        .unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(tool.0.lock().unwrap().len(), 1);
    assert!(matches!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
}

struct WaitHook {
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}
#[async_trait]
impl Hook for WaitHook {
    async fn before_tool(&self, call: &HookCall, _: &Context<'_>) -> Result<BeforeTool, String> {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        Ok(BeforeTool::Rewrite(
            json!({"n": call.arguments["n"].as_i64().unwrap() + 10}),
        ))
    }
}
#[tokio::test]
async fn reload_keeps_inflight_hook_snapshot_and_updates_next_phase() {
    use pi_rs_durable::runtime::Scheduler;
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(FakeModel::default());
    let tool = Arc::new(FakeTool::default());
    let mut scheduler =
        Scheduler::open(dir.path().join("db"), options(model.clone(), tool.clone())).unwrap();
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let original = Extension {
        name: "policy".into(),
        sections: vec![Arc::new(Prompt("old"))],
        hooks: vec![
            Arc::new(WaitHook {
                entered: entered.clone(),
                release: release.clone(),
            }),
            Arc::new(Rewrite(2)),
        ],
        ..Default::default()
    };
    scheduler.install_extension(original).unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    scheduler
        .select_extensions(id, vec!["policy".into()])
        .unwrap();
    scheduler.submit(id, None, "go".into()).unwrap();
    entered.acquire().await.unwrap().forget();
    scheduler
        .install_extension(ext("policy", "new", Arc::new(Stop("block"))))
        .unwrap();
    release.add_permits(1);
    scheduler.wait(id).await.unwrap();
    assert_eq!(*tool.0.lock().unwrap(), vec![json!({"n":112})]);
    {
        let requests = model.0.lock().unwrap();
        assert!(
            matches!(&requests[0].messages[0], Message::System{content} if content.contains("old"))
        );
        assert!(
            matches!(&requests[1].messages[0], Message::System{content} if content.contains("new"))
        );
    }
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn scheduler_restores_extensions_before_automatic_resume() {
    use pi_rs_durable::{extension::Registry, runtime::Scheduler};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(FakeModel::default());
    let tool = Arc::new(FakeTool::default());
    let id;
    {
        let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
        h.install_extension(ext("policy", "restored", Arc::new(Rewrite(2))))
            .unwrap();
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["policy".into()]).unwrap();
        h.submit(id, None, "go".into()).unwrap();
    }
    let mut registry = Registry::default();
    registry
        .install(ext("policy", "restored", Arc::new(Rewrite(2))))
        .unwrap();
    let mut scheduler =
        Scheduler::open_with_extensions(&path, options(model.clone(), tool.clone()), registry)
            .unwrap();
    scheduler.wait(id).await.unwrap();
    assert!(matches!(
        scheduler.conversation(id).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
    assert_eq!(*tool.0.lock().unwrap(), vec![json!({"n": 12})]);
    assert!(
        matches!(&model.0.lock().unwrap()[0].messages[0], Message::System { content } if content.contains("restored"))
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_selection_fails_closed_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(FakeModel::default());
    let tool = Arc::new(FakeTool::default());
    let id;
    {
        let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
        h.install_extension(ext("policy", "p", Arc::new(Stop("block"))))
            .unwrap();
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["policy".into()]).unwrap();
    }
    let mut h = Harness::open_paused(&path, options(model.clone(), tool)).unwrap();
    let error = h
        .submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("selected extension unavailable: policy")
    );
    assert!(model.0.lock().unwrap().is_empty());
}

#[test]
fn forks_preserve_historical_extension_selection() {
    use pi_rs_durable::observer::Cursor;
    use std::num::NonZeroUsize;
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(
        dir.path().join("db"),
        options(
            Arc::new(FakeModel::default()),
            Arc::new(FakeTool::default()),
        ),
    )
    .unwrap();
    h.install_extension(ext("first", "one", Arc::new(Stop("block"))))
        .unwrap();
    h.install_extension(ext("second", "two", Arc::new(Stop("block"))))
        .unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["first".into()]).unwrap();
    let page = h
        .history(id, Cursor(0), NonZeroUsize::new(50).unwrap())
        .unwrap();
    let cursor = page.records.last().unwrap().cursor;
    h.select_extensions(id, vec!["second".into()]).unwrap();
    let current = h.fork(id).unwrap();
    let historical = h.fork_at(id, cursor).unwrap();
    assert_eq!(h.conversation(current).unwrap().extensions, vec!["second"]);
    assert_eq!(
        h.conversation(historical).unwrap().extensions,
        vec!["first"]
    );
}
