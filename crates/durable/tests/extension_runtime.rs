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
async fn block_and_invalid_rewrite_never_dispatch() {
    for case in ["block", "invalid"] {
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
                _ => message.contains("schema"),
            },
            "{case}: {message}"
        );
    }
}

struct SignalGate;
#[async_trait]
impl Hook for SignalGate {
    async fn before_tool(&self, _: &HookCall, ctx: &Context<'_>) -> Result<BeforeTool, String> {
        if ctx.snapshot["phase"]["ToolPreparing"]["wake"]["reason"] == "signal" {
            return Ok(BeforeTool::Pass);
        }
        Ok(BeforeTool::Sleep(Sleep {
            signals: vec!["approve".into()],
            until_millis: None,
        }))
    }
}

struct DeadlineGate;
struct NotifyingGate(Arc<tokio::sync::Semaphore>);
#[async_trait]
impl Hook for NotifyingGate {
    async fn before_tool(&self, _: &HookCall, _: &Context<'_>) -> Result<BeforeTool, String> {
        self.0.add_permits(1);
        Ok(BeforeTool::Sleep(Sleep {
            signals: vec!["approve".into()],
            until_millis: None,
        }))
    }
}

#[tokio::test]
async fn abort_sleeping_gate_never_dispatches_tool() {
    use pi_rs_durable::runtime::Scheduler;
    let dir = tempfile::tempdir().unwrap();
    let tool = Arc::new(FakeTool::default());
    let seen = Arc::new(tokio::sync::Semaphore::new(0));
    let mut scheduler = Scheduler::open(
        dir.path().join("db"),
        options(Arc::new(FakeModel::default()), tool.clone()),
    )
    .unwrap();
    scheduler
        .install_extension(ext("gate", "p", Arc::new(NotifyingGate(seen.clone()))))
        .unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    scheduler
        .select_extensions(id, vec!["gate".into()])
        .unwrap();
    scheduler.submit(id, None, "go".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), seen.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    scheduler.abort_and_wait(id).await.unwrap();
    assert!(tool.0.lock().unwrap().is_empty());
    assert!(tool_message(&scheduler.conversation(id).unwrap()).contains("aborted"));
    scheduler.shutdown().await.unwrap();
}

#[async_trait]
impl Hook for DeadlineGate {
    async fn before_tool(&self, _: &HookCall, ctx: &Context<'_>) -> Result<BeforeTool, String> {
        if ctx.snapshot["phase"]["ToolPreparing"]["wake"]["reason"] == "time" {
            Ok(BeforeTool::Pass)
        } else {
            Ok(BeforeTool::Sleep(Sleep {
                signals: vec![],
                until_millis: Some(0),
            }))
        }
    }
}

#[tokio::test]
async fn scheduler_wakes_expired_gate_and_dispatches_tool() {
    use pi_rs_durable::runtime::Scheduler;
    let dir = tempfile::tempdir().unwrap();
    let tool = Arc::new(FakeTool::default());
    let mut scheduler = Scheduler::open(
        dir.path().join("db"),
        options(Arc::new(FakeModel::default()), tool.clone()),
    )
    .unwrap();
    scheduler
        .install_extension(ext("gate", "p", Arc::new(DeadlineGate)))
        .unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    scheduler
        .select_extensions(id, vec!["gate".into()])
        .unwrap();
    scheduler.submit(id, None, "go".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tool.0.lock().unwrap().len(), 1);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn sleeping_gate_resumes_after_signal_and_reopen() {
    use pi_rs_durable::conversation_signal::{Inbox, Signal};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(FakeModel::default());
    let tool = Arc::new(FakeTool::default());
    let id;
    {
        let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
        h.install_extension(ext("gate", "p", Arc::new(SignalGate)))
            .unwrap();
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["gate".into()]).unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert!(tool.0.lock().unwrap().is_empty());
    }
    let mut h = Harness::open_paused(&path, options(model, tool.clone())).unwrap();
    h.install_extension(ext("gate", "p", Arc::new(SignalGate)))
        .unwrap();
    let signal = Signal::new(Uuid::new_v4(), id, "approve".into(), json!({"allow": true}));
    h.send_signal(&signal).unwrap();
    h.send_signal(&signal).unwrap();
    h.resume(&CancellationToken::new()).await.unwrap();
    assert_eq!(tool.0.lock().unwrap().len(), 1);
    assert!(matches!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
    drop(h);
    let store = durable_store::Store::open(&path).unwrap();
    assert!(
        Inbox::new(&store)
            .receipt(id, signal.id)
            .unwrap()
            .unwrap()
            .is_consumed()
    );
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

#[tokio::test]
async fn abort_during_before_tool_records_skipped_call_without_dispatch() {
    use pi_rs_durable::runtime::Scheduler;
    let dir = tempfile::tempdir().unwrap();
    let tool = Arc::new(FakeTool::default());
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let mut scheduler = Scheduler::open(
        dir.path().join("db"),
        options(Arc::new(FakeModel::default()), tool.clone()),
    )
    .unwrap();
    scheduler
        .install_extension(ext(
            "gate",
            "p",
            Arc::new(WaitHook {
                entered: entered.clone(),
                release: Arc::new(tokio::sync::Semaphore::new(0)),
            }),
        ))
        .unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    scheduler
        .select_extensions(id, vec!["gate".into()])
        .unwrap();
    scheduler.submit(id, None, "go".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    scheduler.abort_and_wait(id).await.unwrap();
    assert!(tool.0.lock().unwrap().is_empty());
    assert!(tool_message(&scheduler.conversation(id).unwrap()).contains("aborted before dispatch"));
    scheduler.shutdown().await.unwrap();
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

struct ResultHook;
struct ParkAfterTool(Arc<tokio::sync::Semaphore>);
#[async_trait]
impl Hook for ParkAfterTool {
    async fn after_tool(
        &self,
        _: &HookCall,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        self.0.add_permits(1);
        std::future::pending().await
    }
}

#[tokio::test]
async fn abort_during_after_tool_retains_committed_result() {
    use pi_rs_durable::runtime::Scheduler;
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(FakeModel::default());
    let tool = Arc::new(FakeTool::default());
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let mut scheduler =
        Scheduler::open(dir.path().join("db"), options(model, tool.clone())).unwrap();
    scheduler
        .install_extension(ext("park", "p", Arc::new(ParkAfterTool(entered.clone()))))
        .unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    scheduler
        .select_extensions(id, vec!["park".into()])
        .unwrap();
    scheduler.submit(id, None, "go".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    scheduler.abort_and_wait(id).await.unwrap();
    assert!(tool_message(&scheduler.conversation(id).unwrap()).contains("ok"));
    assert_eq!(tool.0.lock().unwrap().len(), 1);
    scheduler.shutdown().await.unwrap();
}

#[async_trait]
impl Hook for ResultHook {
    async fn after_tool(
        &self,
        _: &HookCall,
        result: &Value,
        ctx: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        let saved = ctx.memo.get_or_insert("result", result.clone()).await?;
        if std::env::var_os("PI_AFTER_TOOL_CRASH_DB").is_some() {
            std::process::abort();
        }
        Ok(Some(json!(format!("hooked: {}", saved.as_str().unwrap()))))
    }
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        value: &Value,
        _: &Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision, GenerationEvent};
        if event == GenerationEvent::BeforeRequest {
            let mut v = value.clone();
            v["messages"]
                .as_array_mut()
                .unwrap()
                .push(json!({"role":"user", "content":"from hook"}));
            Ok(GenerationDecision::Replace(v))
        } else {
            Ok(GenerationDecision::Pass)
        }
    }
}
fn extension_only_options(model: Arc<FakeModel>) -> Options {
    Options {
        model,
        tools: vec![],
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn tool_extension(tool: Arc<FakeTool>) -> Extension {
    Extension {
        name: "provider".into(),
        tools: vec![tool],
        hooks: vec![Arc::new(ResultHook)],
        ..Default::default()
    }
}
#[tokio::test]
async fn extension_tool_and_generation_hooks_are_used() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(FakeModel::default());
    let tool = Arc::new(FakeTool::default());
    let mut h =
        Harness::open_paused(dir.path().join("db"), extension_only_options(model.clone())).unwrap();
    h.install_extension(tool_extension(tool.clone())).unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["provider".into()]).unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(*tool.0.lock().unwrap(), vec![json!({"n":1})]);
    assert_eq!(tool_message(&h.conversation(id).unwrap()), "hooked: ok");
    let requests = model.0.lock().unwrap();
    assert_eq!(requests[0].tools[0].name, "test");
    assert!(
        requests[0]
            .messages
            .iter()
            .any(|m| matches!(m, Message::User { content } if content == "from hook"))
    );
}
#[tokio::test]
async fn after_tool_crash_child() {
    let Ok(path) = std::env::var("PI_AFTER_TOOL_CRASH_DB") else {
        return;
    };
    let mut h = Harness::open_paused(
        &path,
        extension_only_options(Arc::new(FakeModel::default())),
    )
    .unwrap();
    h.install_extension(tool_extension(Arc::new(FakeTool::default())))
        .unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["provider".into()]).unwrap();
    std::fs::write(format!("{path}.id"), id.to_string()).unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    panic!("hook did not crash");
}
#[tokio::test]
async fn after_tool_recovery_does_not_execute_tool_again() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "after_tool_crash_child"])
        .env("PI_AFTER_TOOL_CRASH_DB", &path)
        .status()
        .unwrap();
    assert!(!status.success());
    let id = std::fs::read_to_string(format!("{}.id", path.display()))
        .unwrap()
        .parse()
        .unwrap();
    let tool = Arc::new(FakeTool::default());
    let mut h = Harness::open_paused(
        &path,
        extension_only_options(Arc::new(FakeModel::default())),
    )
    .unwrap();
    h.install_extension(tool_extension(tool.clone())).unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert!(tool.0.lock().unwrap().is_empty());
    assert_eq!(tool_message(&h.conversation(id).unwrap()), "hooked: ok");
}

struct ParkBeforeTool(Arc<tokio::sync::Semaphore>);
#[async_trait]
impl Hook for ParkBeforeTool {
    async fn before_tool(&self, _: &HookCall, _: &Context<'_>) -> Result<BeforeTool, String> {
        self.0.add_permits(1);
        std::future::pending().await
    }
}

#[tokio::test]
async fn abort_during_before_tool_adds_skipped_result() {
    use pi_rs_durable::runtime::Scheduler;
    let dir = tempfile::tempdir().unwrap();
    let tool = Arc::new(FakeTool::default());
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let mut scheduler = Scheduler::open(
        dir.path().join("db"),
        options(Arc::new(FakeModel::default()), tool.clone()),
    )
    .unwrap();
    scheduler
        .install_extension(ext("park", "p", Arc::new(ParkBeforeTool(entered.clone()))))
        .unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    scheduler
        .select_extensions(id, vec!["park".into()])
        .unwrap();
    scheduler.submit(id, None, "go".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    scheduler.abort_and_wait(id).await.unwrap();
    assert!(tool_message(&scheduler.conversation(id).unwrap()).contains("aborted before dispatch"));
    assert!(tool.0.lock().unwrap().is_empty());
    scheduler.shutdown().await.unwrap();
}

struct PendingTool(Arc<std::sync::atomic::AtomicBool>);
#[async_trait]
impl Tool for PendingTool {
    fn spec(&self) -> ToolSpec {
        FakeTool::default().spec()
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Pending {
            reference: "job".into(),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        if self.0.load(std::sync::atomic::Ordering::SeqCst) {
            Some(Outcome::Succeeded("done".into()))
        } else {
            None
        }
    }
}
struct CaptureArguments(Arc<Mutex<Vec<Value>>>);
#[async_trait]
impl Hook for CaptureArguments {
    async fn before_tool(&self, _: &HookCall, _: &Context<'_>) -> Result<BeforeTool, String> {
        Ok(BeforeTool::Rewrite(json!({"n":42})))
    }
    async fn after_tool(
        &self,
        call: &HookCall,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        self.0.lock().unwrap().push(call.arguments.clone());
        Ok(None)
    }
}
#[tokio::test]
async fn pending_result_hook_receives_persisted_rewritten_arguments_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let captured = Arc::new(Mutex::new(vec![]));
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let extension = || Extension {
        name: "provider".into(),
        tools: vec![Arc::new(PendingTool(ready.clone()))],
        hooks: vec![Arc::new(CaptureArguments(captured.clone()))],
        ..Default::default()
    };
    let id;
    {
        let mut h = Harness::open_paused(
            &path,
            extension_only_options(Arc::new(FakeModel::default())),
        )
        .unwrap();
        h.install_extension(extension()).unwrap();
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["provider".into()]).unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert!(h.blocked(id).unwrap().is_some());
    }
    ready.store(true, std::sync::atomic::Ordering::SeqCst);
    let mut h = Harness::open_paused(
        &path,
        extension_only_options(Arc::new(FakeModel::default())),
    )
    .unwrap();
    h.install_extension(extension()).unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(*captured.lock().unwrap(), vec![json!({"n":42})]);
    assert_eq!(tool_message(&h.conversation(id).unwrap()), "done");
}

#[tokio::test]
async fn reconciled_pending_result_hook_receives_rewritten_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let captured = Arc::new(Mutex::new(vec![]));
    let mut h = Harness::open_paused(
        dir.path().join("db"),
        extension_only_options(Arc::new(FakeModel::default())),
    )
    .unwrap();
    h.install_extension(Extension {
        name: "provider".into(),
        tools: vec![Arc::new(PendingTool(Arc::new(
            std::sync::atomic::AtomicBool::new(false),
        )))],
        hooks: vec![Arc::new(CaptureArguments(captured.clone()))],
        ..Default::default()
    })
    .unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["provider".into()]).unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    let (operation, _) = h.blocked(id).unwrap().unwrap();
    h.reconcile(id, operation, Outcome::Succeeded("reconciled".into()))
        .unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(*captured.lock().unwrap(), vec![json!({"n":42})]);
    assert_eq!(tool_message(&h.conversation(id).unwrap()), "reconciled");
}

struct ParkGeneration(Arc<tokio::sync::Semaphore>);
#[async_trait]
impl Hook for ParkGeneration {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        if event == pi_rs_durable::extension::GenerationEvent::BeforeRequest {
            self.0.add_permits(1);
            std::future::pending().await
        } else {
            Ok(pi_rs_durable::extension::GenerationDecision::Pass)
        }
    }
}
#[tokio::test]
async fn abort_during_generation_hook_never_dispatches_model() {
    use pi_rs_durable::runtime::Scheduler;
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(FakeModel::default());
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let mut scheduler = Scheduler::open(
        dir.path().join("db"),
        options(model.clone(), Arc::new(FakeTool::default())),
    )
    .unwrap();
    scheduler
        .install_extension(ext("park", "p", Arc::new(ParkGeneration(entered.clone()))))
        .unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    scheduler
        .select_extensions(id, vec!["park".into()])
        .unwrap();
    scheduler.submit(id, None, "go".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    scheduler.abort_and_wait(id).await.unwrap();
    assert!(model.0.lock().unwrap().is_empty());
    scheduler.shutdown().await.unwrap();
}

struct LifecycleHook {
    events: Arc<Mutex<Vec<String>>>,
    task_event: Option<pi_rs_durable::extension::GenerationEvent>,
    park_event: Option<pi_rs_durable::extension::GenerationEvent>,
    entered: Arc<tokio::sync::Semaphore>,
}
#[async_trait]
impl Hook for LifecycleHook {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        _: &Value,
        ctx: &Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision, GenerationEvent, HookTask};
        if !matches!(
            event,
            GenerationEvent::AfterTools | GenerationEvent::OnYield | GenerationEvent::AfterRun
        ) {
            return Ok(GenerationDecision::Pass);
        }
        let name = format!("{event:?}");
        let phase = &ctx.snapshot["phase"][&name];
        let operation = phase["operation_id"].clone();
        assert_eq!(
            ctx.memo
                .get_or_insert("operation", operation.clone())
                .await?,
            operation
        );
        self.events.lock().unwrap().push(name.clone());
        if std::env::var("PI_LIFECYCLE_CRASH").ok().as_deref() == Some(&name) {
            std::process::abort();
        }
        if self.park_event == Some(event) {
            self.entered.add_permits(1);
            std::future::pending::<()>().await;
        }
        if self.task_event == Some(event) {
            return Ok(GenerationDecision::Tasks(vec![HookTask {
                name: "job".into(),
                input: json!(null),
            }]));
        }
        Ok(GenerationDecision::Pass)
    }
}
fn lifecycle(
    h: &mut Harness,
    task_event: Option<pi_rs_durable::extension::GenerationEvent>,
    park_event: Option<pi_rs_durable::extension::GenerationEvent>,
    events: Arc<Mutex<Vec<String>>>,
) {
    h.install_extension(Extension {
        name: "lifecycle".into(),
        hooks: vec![Arc::new(LifecycleHook {
            events,
            task_event,
            park_event,
            entered: Arc::new(tokio::sync::Semaphore::new(0)),
        })],
        ..Default::default()
    })
    .unwrap();
}
#[tokio::test]
async fn lifecycle_hooks_run_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let events = Arc::new(Mutex::new(vec![]));
    let mut h = Harness::open_paused(
        dir.path().join("db"),
        options(
            Arc::new(FakeModel::default()),
            Arc::new(FakeTool::default()),
        ),
    )
    .unwrap();
    lifecycle(&mut h, None, None, events.clone());
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["lifecycle".into()]).unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        vec!["AfterTools", "OnYield", "AfterRun"]
    );
}
#[tokio::test]
async fn lifecycle_tasks_fail_closed_and_leave_phase_recoverable() {
    use pi_rs_durable::extension::GenerationEvent;
    for event in [
        GenerationEvent::AfterTools,
        GenerationEvent::OnYield,
        GenerationEvent::AfterRun,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(FakeModel::default());
        let tool = Arc::new(FakeTool::default());
        let id;
        {
            let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
            lifecycle(&mut h, Some(event), None, Arc::default());
            id = h.create_conversation(config()).unwrap();
            h.select_extensions(id, vec!["lifecycle".into()]).unwrap();
            let driver = h.start_tasks(Default::default()).unwrap();
            let err = h
                .submit_and_run(id, None, "go".into(), &CancellationToken::new())
                .await
                .unwrap_err();
            assert!(err.to_string().contains("unselected task registration"));
            driver.shutdown().unwrap();
        }
        let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
        let c = h.conversation(id).unwrap();
        assert!(serde_json::to_value(&c).unwrap()["phase"][format!("{event:?}")].is_object());
        if event == GenerationEvent::AfterRun {
            assert!(matches!(c.submissions[0].status, Status::Done { .. }));
        }
        lifecycle(&mut h, None, None, Arc::default());
        h.resume_conversation(id, &CancellationToken::new())
            .await
            .unwrap();
        assert!(matches!(
            h.conversation(id).unwrap().submissions[0].status,
            Status::Done { .. }
        ));
        assert_eq!(tool.0.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn lifecycle_crash_child() {
    let Ok(path) = std::env::var("PI_LIFECYCLE_DB") else {
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
    lifecycle(&mut h, None, None, Arc::default());
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["lifecycle".into()]).unwrap();
    std::fs::write(format!("{path}.id"), id.to_string()).unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    panic!("hook did not crash");
}
#[tokio::test]
async fn lifecycle_crash_recovers_stable_memo_without_replaying_tool() {
    for phase in ["AfterTools", "OnYield", "AfterRun"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "lifecycle_crash_child"])
            .env("PI_LIFECYCLE_DB", &path)
            .env("PI_LIFECYCLE_CRASH", phase)
            .status()
            .unwrap();
        assert!(!status.success());
        let id = std::fs::read_to_string(format!("{}.id", path.display()))
            .unwrap()
            .parse()
            .unwrap();
        let tool = Arc::new(FakeTool::default());
        let mut h =
            Harness::open_paused(&path, options(Arc::new(FakeModel::default()), tool.clone()))
                .unwrap();
        let state = serde_json::to_value(h.conversation(id).unwrap()).unwrap();
        let operation = state["phase"][phase]["operation_id"]
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
        lifecycle(&mut h, None, None, Arc::default());
        h.resume_conversation(id, &CancellationToken::new())
            .await
            .unwrap();
        assert!(tool.0.lock().unwrap().is_empty());
        assert!(matches!(
            h.conversation(id).unwrap().submissions[0].status,
            Status::Done { .. }
        ));
    }
}
#[tokio::test]
async fn abort_during_lifecycle_hook_stops_without_replaying_tool() {
    use pi_rs_durable::{extension::GenerationEvent, runtime::Scheduler};
    for event in [
        GenerationEvent::AfterTools,
        GenerationEvent::OnYield,
        GenerationEvent::AfterRun,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let tool = Arc::new(FakeTool::default());
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let mut scheduler = Scheduler::open(
            dir.path().join("db"),
            options(Arc::new(FakeModel::default()), tool.clone()),
        )
        .unwrap();
        scheduler
            .install_extension(Extension {
                name: "lifecycle".into(),
                hooks: vec![Arc::new(LifecycleHook {
                    events: Arc::default(),
                    task_event: None,
                    park_event: Some(event),
                    entered: entered.clone(),
                })],
                ..Default::default()
            })
            .unwrap();
        let id = scheduler.create_conversation(config()).unwrap();
        scheduler
            .select_extensions(id, vec!["lifecycle".into()])
            .unwrap();
        scheduler.submit(id, None, "go".into()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        if event == GenerationEvent::AfterRun {
            let error = scheduler
                .submit_with_options(
                    id,
                    None,
                    "later".into(),
                    pi_rs_durable::runtime::AdmissionOptions {
                        reject_when_busy: true,
                        ..Default::default()
                    },
                )
                .unwrap_err();
            assert!(matches!(
                error,
                pi_rs_durable::runtime::Error::Admission(
                    pi_rs_durable::runtime::AdmissionError::Busy
                )
            ));
        }
        scheduler.abort_and_wait(id).await.unwrap();
        let c = scheduler.conversation(id).unwrap();
        assert_eq!(tool.0.lock().unwrap().len(), 1);
        assert!(match event {
            GenerationEvent::AfterRun => matches!(c.submissions[0].status, Status::Done { .. }),
            _ => matches!(c.submissions[0].status, Status::Unanswered { .. }),
        });
        scheduler.shutdown().await.unwrap();
    }
}

struct ContinueOnce;
#[async_trait]
impl Hook for ContinueOnce {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        _: &Value,
        ctx: &Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision, GenerationEvent};
        if event == GenerationEvent::OnYield
            && !ctx.snapshot["messages"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m.to_string().contains("continue please"))
        {
            Ok(GenerationDecision::Continue(json!("continue please")))
        } else {
            Ok(GenerationDecision::Pass)
        }
    }
}
#[tokio::test]
async fn yield_continuation_keeps_submission_open_and_runs_after_run_once() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(FakeModel::default());
    let events = Arc::new(Mutex::new(vec![]));
    let mut opts = options(model.clone(), Arc::new(FakeTool::default()));
    opts.max_turns = 2;
    let mut h = Harness::open_paused(dir.path().join("db"), opts).unwrap();
    h.install_extension(Extension {
        name: "continue".into(),
        hooks: vec![Arc::new(ContinueOnce)],
        ..Default::default()
    })
    .unwrap();
    lifecycle(&mut h, None, None, events.clone());
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["continue".into(), "lifecycle".into()])
        .unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert!(matches!(c.submissions[0].status, Status::Done { .. }));
    assert_eq!(model.0.lock().unwrap().len(), 3);
    assert_eq!(
        *events.lock().unwrap(),
        vec!["AfterTools", "OnYield", "AfterRun"]
    );
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m, Message::User { content } if content == "continue please"))
    );
}

struct HeldYield {
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
    calls: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl Hook for HeldYield {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision, GenerationEvent};
        if event == GenerationEvent::OnYield
            && self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0
        {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            return Ok(GenerationDecision::Continue(json!("discard me")));
        }
        Ok(GenerationDecision::Pass)
    }
}
#[tokio::test]
async fn queued_steer_or_followup_supersedes_yield_continuation() {
    use pi_rs_durable::{runtime::Scheduler, state::InputMode};
    for mode in [InputMode::Steer, InputMode::FollowUp] {
        let dir = tempfile::tempdir().unwrap();
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let mut scheduler = Scheduler::open(
            dir.path().join("db"),
            options(
                Arc::new(FakeModel::default()),
                Arc::new(FakeTool::default()),
            ),
        )
        .unwrap();
        scheduler
            .install_extension(Extension {
                name: "held".into(),
                hooks: vec![Arc::new(HeldYield {
                    entered: entered.clone(),
                    release: release.clone(),
                    calls: Default::default(),
                })],
                ..Default::default()
            })
            .unwrap();
        let id = scheduler.create_conversation(config()).unwrap();
        scheduler
            .select_extensions(id, vec!["held".into()])
            .unwrap();
        scheduler.submit(id, None, "first".into()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        scheduler
            .submit_with_mode(id, None, "queued".into(), mode)
            .unwrap();
        release.add_permits(1);
        scheduler.wait(id).await.unwrap();
        let c = scheduler.conversation(id).unwrap();
        assert!(
            c.submissions
                .iter()
                .all(|s| matches!(s.status, Status::Done { .. }))
        );
        assert!(
            !c.messages
                .iter()
                .any(|m| matches!(m, Message::User { content } if content == "discard me"))
        );
        scheduler.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn lifecycle_children_resume_after_reopen() {
    use pi_rs_durable::{
        extension::GenerationEvent,
        task::{Checkpoint, Task, WaitFor},
        task_scheduler::{Handler, Registry, Step},
        task_store::Signal,
    };
    for event in [GenerationEvent::AfterTools, GenerationEvent::AfterRun] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let tool = Arc::new(FakeTool::default());
        let handler: Arc<dyn Handler> = Arc::new(|task: &Task| {
            if task.delivered_signal().is_some() {
                Ok(Step::Succeed(json!(null)))
            } else {
                Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "waiting".into(),
                        data: task.checkpoint().data.clone(),
                    },
                    condition: WaitFor::Signal("go".into()),
                    children: vec![],
                })
            }
        });
        let install = |h: &Harness| {
            h.install_extension(Extension {
                name: "lifecycle".into(),
                hooks: vec![Arc::new(LifecycleHook {
                    events: Arc::default(),
                    task_event: Some(event),
                    park_event: None,
                    entered: Arc::new(tokio::sync::Semaphore::new(0)),
                })],
                tasks: vec![("job".into(), handler.clone())],
                ..Default::default()
            })
            .unwrap();
        };
        let registry = || {
            let mut r = Registry::default();
            assert!(r.register_shared("job", true, handler.clone()));
            r
        };
        let id;
        let child;
        {
            let mut h =
                Harness::open_paused(&path, options(Arc::new(FakeModel::default()), tool.clone()))
                    .unwrap();
            install(&h);
            let driver = h.start_tasks(registry()).unwrap();
            id = h.create_conversation(config()).unwrap();
            h.select_extensions(id, vec!["lifecycle".into()]).unwrap();
            h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
                .await
                .unwrap();
            let phase = serde_json::to_value(h.conversation(id).unwrap()).unwrap();
            child = phase["phase"]["HookTasksWaiting"]["children"][0]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            assert!(driver.get(child).unwrap().is_some());
            driver.shutdown().unwrap();
        }
        {
            let mut h =
                Harness::open_paused(&path, options(Arc::new(FakeModel::default()), tool.clone()))
                    .unwrap();
            install(&h);
            let driver = h.start_tasks(registry()).unwrap();
            // Wait until the recovered child has registered its signal wait.
            for _ in 0..100 {
                if matches!(
                    driver.get(child).unwrap().unwrap().state(),
                    pi_rs_durable::task::TaskState::Waiting(_)
                ) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            driver
                .signal(&Signal::new(
                    uuid::Uuid::new_v4(),
                    child,
                    "go".into(),
                    json!(null),
                ))
                .unwrap();
            for _ in 0..100 {
                if driver.get(child).unwrap().unwrap().is_terminal() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            assert!(driver.get(child).unwrap().unwrap().is_terminal());
            h.resume_conversation(id, &CancellationToken::new())
                .await
                .unwrap();
            assert!(matches!(
                h.conversation(id).unwrap().submissions[0].status,
                Status::Done { .. }
            ));
            assert_eq!(tool.0.lock().unwrap().len(), 1);
            driver.shutdown().unwrap();
        }
    }
}

#[tokio::test]
async fn lifecycle_rejects_a_different_driver_handler() {
    use pi_rs_durable::{
        extension::GenerationEvent,
        task::Task,
        task_scheduler::{Handler, Registry, Step},
    };
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(
        dir.path().join("db"),
        options(
            Arc::new(FakeModel::default()),
            Arc::new(FakeTool::default()),
        ),
    )
    .unwrap();
    let selected: Arc<dyn Handler> = Arc::new(|_: &Task| Ok(Step::Succeed(json!(null))));
    let other: Arc<dyn Handler> = Arc::new(|_: &Task| Ok(Step::Succeed(json!(null))));
    h.install_extension(Extension {
        name: "lifecycle".into(),
        hooks: vec![Arc::new(LifecycleHook {
            events: Arc::default(),
            task_event: Some(GenerationEvent::AfterRun),
            park_event: None,
            entered: Arc::new(tokio::sync::Semaphore::new(0)),
        })],
        tasks: vec![("job".into(), selected)],
        ..Default::default()
    })
    .unwrap();
    let mut registry = Registry::default();
    registry.register_shared("job", true, other);
    let driver = h.start_tasks(registry).unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["lifecycle".into()]).unwrap();
    let error = h
        .submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("task driver registration mismatch")
    );
    driver.shutdown().unwrap();
    drop(h);
    let reopened = Harness::open_paused(
        dir.path().join("db"),
        options(
            Arc::new(FakeModel::default()),
            Arc::new(FakeTool::default()),
        ),
    )
    .unwrap();
    assert!(
        serde_json::to_value(reopened.conversation(id).unwrap()).unwrap()["phase"]["AfterRun"]
            .is_object()
    );
}
