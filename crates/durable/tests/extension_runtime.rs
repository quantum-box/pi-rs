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
            vec![
                Ok(ModelEvent::Text("tool answer".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![ToolCall {
                        id: "call".into(),
                        name: "test".into(),
                        arguments: "{\"n\":1}".into(),
                    }],
                    usage: None,
                }),
            ]
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
struct NotifyingSignalGate(Arc<tokio::sync::Semaphore>);
#[async_trait]
impl Hook for NotifyingSignalGate {
    async fn before_tool(&self, _: &HookCall, ctx: &Context<'_>) -> Result<BeforeTool, String> {
        self.0.add_permits(1);
        if ctx.snapshot["phase"]["ToolPreparing"]["wake"]["reason"] == "signal" {
            Ok(BeforeTool::Pass)
        } else {
            Ok(BeforeTool::Sleep(Sleep {
                signals: vec!["approve".into()],
                until_millis: None,
            }))
        }
    }
}
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

#[tokio::test]
async fn abort_with_retry_queued_at_sleeping_gate() {
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
    // Abort fences both the parked worker and the newly admitted retry.
    for _ in 0..8 {
        scheduler.schedule(id);
    }
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        scheduler.abort_and_wait(id),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(tool.0.lock().unwrap().is_empty());
    assert!(tool_message(&scheduler.conversation(id).unwrap()).contains("aborted"));
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn scheduled_retry_wakes_a_parked_hook() {
    use pi_rs_durable::{conversation_signal::Signal, runtime::Scheduler};
    let dir = tempfile::tempdir().unwrap();
    let tool = Arc::new(FakeTool::default());
    let seen = Arc::new(tokio::sync::Semaphore::new(0));
    let mut scheduler = Scheduler::open(
        dir.path().join("db"),
        options(Arc::new(FakeModel::default()), tool.clone()),
    )
    .unwrap();
    scheduler
        .install_extension(ext(
            "gate",
            "p",
            Arc::new(NotifyingSignalGate(seen.clone())),
        ))
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
    for _ in 0..8 {
        scheduler.schedule(id);
    }
    tokio::task::yield_now().await;
    scheduler
        .send_signal(&Signal::new(
            Uuid::new_v4(),
            id,
            "approve".into(),
            json!({}),
        ))
        .unwrap();
    scheduler.schedule(id);
    tokio::time::timeout(std::time::Duration::from_millis(500), seen.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    scheduler.wait(id).await.unwrap();
    assert_eq!(tool.0.lock().unwrap().len(), 1);
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
async fn invalid_lifecycle_tasks_are_reported_and_skipped() {
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
        let reports;
        {
            let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
            lifecycle(&mut h, Some(event), None, Arc::default());
            let id = h.create_conversation(config()).unwrap();
            h.select_extensions(id, vec!["lifecycle".into()]).unwrap();
            h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
                .await
                .unwrap();
            assert!(matches!(
                h.conversation(id).unwrap().submissions[0].status,
                Status::Done { .. }
            ));
            reports = h.extension_reports().unwrap();
            assert_eq!(reports.len(), 1);
            assert_eq!(reports[0].phase, format!("{event:?}"));
            assert!(
                reports[0].reports[0]
                    .error
                    .contains("unselected task registration")
            );
        }
        let mut h = Harness::open_paused(&path, options(model, tool.clone())).unwrap();
        h.resume(&CancellationToken::new()).await.unwrap();
        assert_eq!(h.extension_reports().unwrap(), reports);
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

struct TimedSignalGate;
#[async_trait]
impl Hook for TimedSignalGate {
    async fn before_tool(&self, _: &HookCall, ctx: &Context<'_>) -> Result<BeforeTool, String> {
        if ctx.snapshot["phase"]["ToolPreparing"]["wake"].is_object() {
            return Ok(BeforeTool::Pass);
        }
        Ok(BeforeTool::Sleep(Sleep {
            signals: vec!["approval".into()],
            until_millis: Some(100),
        }))
    }
}

#[tokio::test]
async fn gate_deadline_and_early_signal_survive_restart() {
    use pi_rs_durable::conversation_signal::{Error as SignalError, Signal};
    use std::sync::atomic::{AtomicI64, Ordering};
    for early in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let clock = Arc::new(AtomicI64::new(99));
        let model = Arc::new(FakeModel::default());
        let tool = Arc::new(FakeTool::default());
        let id;
        let signal;
        {
            let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
            let clock = clock.clone();
            h.set_signal_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
            h.install_extension(ext("gate", "p", Arc::new(TimedSignalGate)))
                .unwrap();
            id = h.create_conversation(config()).unwrap();
            h.select_extensions(id, vec!["gate".into()]).unwrap();
            signal = Signal::new(Uuid::new_v4(), id, "approval".into(), json!(1));
            if early {
                h.send_signal(&signal).unwrap();
            }
            h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
                .await
                .unwrap();
        }
        clock.store(100, Ordering::SeqCst);
        let mut h = Harness::open_paused(&path, options(model, tool.clone())).unwrap();
        let c = clock.clone();
        h.set_signal_clock(Arc::new(move || c.load(Ordering::SeqCst)));
        h.install_extension(ext("gate", "p", Arc::new(TimedSignalGate)))
            .unwrap();
        if early {
            h.send_signal(&signal).unwrap();
            let other = Signal::new(Uuid::new_v4(), id, "approval".into(), json!(2));
            assert!(matches!(
                h.send_signal(&other),
                Err(pi_rs_durable::runtime::Error::Signal(
                    SignalError::Duplicate
                ))
            ));
        } else {
            assert!(matches!(
                h.send_signal(&signal),
                Err(pi_rs_durable::runtime::Error::Signal(SignalError::Closed))
            ));
        }
        h.resume(&CancellationToken::new()).await.unwrap();
        assert_eq!(tool.0.lock().unwrap().len(), 1);
        let later = Signal::new(Uuid::new_v4(), id, "approval".into(), json!(3));
        assert!(matches!(
            h.send_signal(&later),
            Err(pi_rs_durable::runtime::Error::Signal(
                SignalError::Duplicate | SignalError::Closed
            ))
        ));
    }
}

struct TerminateRoundHook {
    after_run: Arc<Mutex<Vec<Value>>>,
}
#[async_trait]
impl Hook for TerminateRoundHook {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        _: &Value,
        ctx: &Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision, GenerationEvent, HookTask};
        match event {
            GenerationEvent::AfterTools => Ok(GenerationDecision::Tasks(vec![HookTask {
                name: "decision".into(),
                input: json!(null),
            }])),
            GenerationEvent::AfterRun => {
                self.after_run.lock().unwrap().push(ctx.snapshot.clone());
                Ok(GenerationDecision::Pass)
            }
            _ => Ok(GenerationDecision::Pass),
        }
    }
}

#[tokio::test]
async fn after_tools_termination_settles_before_after_run_and_recovers_queued_inputs() {
    use pi_rs_durable::{
        state::InputMode,
        task::{Checkpoint, Task, TaskState, WaitFor},
        task_scheduler::{Handler, Registry, Step},
        task_store::Signal,
    };
    for (decision, terminates) in [
        (json!({"terminate": true}), true),
        (json!({"terminate": true, "extra": 1}), false),
        (json!({"terminate": false}), false),
        (json!(null), false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(FakeModel::default());
        let tool = Arc::new(FakeTool::default());
        let after_run = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<dyn Handler> = Arc::new(move |task: &Task| {
            if task.delivered_signal().is_some() {
                Ok(Step::Succeed(decision.clone()))
            } else {
                Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "waiting".into(),
                        data: json!(null),
                    },
                    condition: WaitFor::Signal("go".into()),
                    children: vec![],
                })
            }
        });
        let install = |h: &Harness| {
            h.install_extension(Extension {
                name: "decision".into(),
                hooks: vec![Arc::new(TerminateRoundHook {
                    after_run: after_run.clone(),
                })],
                tasks: vec![("decision".into(), handler.clone())],
                ..Default::default()
            })
            .unwrap()
        };
        let registry = || {
            let mut registry = Registry::default();
            registry.register_shared("decision", true, handler.clone());
            registry
        };
        let id;
        let child;
        {
            let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
            install(&h);
            let driver = h.start_tasks(registry()).unwrap();
            id = h.create_conversation(config()).unwrap();
            h.select_extensions(id, vec!["decision".into()]).unwrap();
            h.submit_and_run(id, None, "first".into(), &CancellationToken::new())
                .await
                .unwrap();
            let state = serde_json::to_value(h.conversation(id).unwrap()).unwrap();
            child = state["phase"]["HookTasksWaiting"]["children"][0]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            h.submit_with_mode(id, None, "queued steer".into(), InputMode::Steer)
                .unwrap();
            h.submit(id, None, "queued followup".into()).unwrap();
            driver.shutdown().unwrap();
        }
        {
            let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
            install(&h);
            let driver = h.start_tasks(registry()).unwrap();
            for _ in 0..200 {
                if matches!(
                    driver.get(child).unwrap().unwrap().state(),
                    TaskState::Waiting(_)
                ) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            driver
                .signal(&Signal::new(
                    Uuid::new_v4(),
                    child,
                    "go".into(),
                    json!(null),
                ))
                .unwrap();
            for _ in 0..200 {
                if driver.get(child).unwrap().unwrap().is_terminal() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert!(driver.get(child).unwrap().unwrap().is_terminal());
            h.resume_conversation(id, &CancellationToken::new())
                .await
                .unwrap();
            let c = h.conversation(id).unwrap();
            assert!(
                c.submissions
                    .iter()
                    .all(|s| matches!(s.status, Status::Done { .. }))
            );
            let expected = if terminates { "tool answer" } else { "done" };
            assert!(
                matches!(&c.submissions[0].status, Status::Done { answer } if answer == expected),
                "expected {expected}: {:?}",
                c.submissions[0].status
            );
            let snapshots = after_run.lock().unwrap();
            if terminates {
                assert_eq!(snapshots.len(), 2);
                let first = &snapshots[0];
                assert!(first["submissions"][0]["status"]["Done"].is_object());
                assert_eq!(first["submissions"][1]["status"], "Queued");
                assert_eq!(first["submissions"][2]["status"], "Queued");
                assert_eq!(model.0.lock().unwrap().len(), 2);
            } else {
                assert!(!snapshots.is_empty());
            }
            assert_eq!(tool.0.lock().unwrap().len(), 1);
            driver.shutdown().unwrap();
        }
    }
}

#[tokio::test]
async fn terminate_crash_worker() {
    use pi_rs_durable::task::Task;
    use pi_rs_durable::task_scheduler::{Handler, Registry, Step};
    let Ok(path) = std::env::var("PI_TERMINATE_CRASH_DB") else {
        return;
    };
    let release = format!("{path}.release");
    let handler: Arc<dyn Handler> = Arc::new(move |_: &Task| {
        while !std::path::Path::new(&release).exists() {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        Ok(Step::Succeed(json!({"terminate": true})))
    });
    let mut h = Harness::open_paused(
        &path,
        options(
            Arc::new(FakeModel::default()),
            Arc::new(FakeTool::default()),
        ),
    )
    .unwrap();
    h.install_extension(Extension {
        name: "decision".into(),
        hooks: vec![Arc::new(TerminateRoundHook {
            after_run: Arc::default(),
        })],
        tasks: vec![("decision".into(), handler.clone())],
        ..Default::default()
    })
    .unwrap();
    let mut registry = Registry::default();
    registry.register_shared("decision", true, handler);
    let driver = h.start_tasks(registry).unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["decision".into()]).unwrap();
    h.submit_and_run(id, None, "first".into(), &CancellationToken::new())
        .await
        .unwrap();
    let state = serde_json::to_value(h.conversation(id).unwrap()).unwrap();
    let child = state["phase"]["HookTasksWaiting"]["children"][0]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    std::fs::write(format!("{path}.release"), "").unwrap();
    for _ in 0..500 {
        if driver.get(child).unwrap().unwrap().is_terminal() {
            std::fs::write(format!("{path}.ready"), id.to_string()).unwrap();
            std::future::pending::<()>().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("child did not finish");
}

#[tokio::test]
async fn after_tools_termination_recovers_after_process_kill() {
    use pi_rs_durable::task::Task;
    use pi_rs_durable::task_scheduler::{Handler, Registry, Step};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut worker = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "terminate_crash_worker"])
        .env("PI_TERMINATE_CRASH_DB", &path)
        .spawn()
        .unwrap();
    let ready = format!("{}.ready", path.display());
    for _ in 0..500 {
        if std::path::Path::new(&ready).exists() {
            break;
        }
        assert!(
            worker.try_wait().unwrap().is_none(),
            "worker exited before barrier"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let id = std::fs::read_to_string(ready).unwrap().parse().unwrap();
    worker.kill().unwrap();
    worker.wait().unwrap();
    let model = Arc::new(FakeModel::default());
    let tool = Arc::new(FakeTool::default());
    let events = Arc::new(Mutex::new(Vec::new()));
    let handler: Arc<dyn Handler> =
        Arc::new(|_: &Task| Ok(Step::Succeed(json!({"terminate": true}))));
    let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
    h.install_extension(Extension {
        name: "decision".into(),
        hooks: vec![Arc::new(TerminateRoundHook {
            after_run: events.clone(),
        })],
        tasks: vec![("decision".into(), handler.clone())],
        ..Default::default()
    })
    .unwrap();
    let mut registry = Registry::default();
    registry.register_shared("decision", true, handler);
    let driver = h.start_tasks(registry).unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert!(
        matches!(&h.conversation(id).unwrap().submissions[0].status, Status::Done { answer } if answer == "tool answer")
    );
    assert_eq!(events.lock().unwrap().len(), 1);
    assert!(model.0.lock().unwrap().is_empty());
    assert!(tool.0.lock().unwrap().is_empty());
    driver.shutdown().unwrap();
}

struct YieldReviews;
#[async_trait]
impl Hook for YieldReviews {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        _: &Value,
        ctx: &Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision, GenerationEvent, HookTask};
        if event == GenerationEvent::OnYield
            && !ctx.snapshot["messages"]
                .to_string()
                .contains("chosen review")
            && !ctx.snapshot["messages"]
                .to_string()
                .contains("queued message")
        {
            Ok(GenerationDecision::Tasks(vec![
                HookTask {
                    name: "review".into(),
                    input: json!({"continue": " "}),
                },
                HookTask {
                    name: "review".into(),
                    input: json!({"continue": "chosen review"}),
                },
                HookTask {
                    name: "review".into(),
                    input: json!({"continue": "later review"}),
                },
            ]))
        } else {
            Ok(GenerationDecision::Pass)
        }
    }
}

#[tokio::test]
async fn yield_children_choose_first_valid_result_or_queued_input() {
    use pi_rs_durable::{
        task::Task,
        task_scheduler::{Handler, Registry, Step},
    };
    for queued in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(
            dir.path().join("db"),
            options(
                Arc::new(FakeModel::default()),
                Arc::new(FakeTool::default()),
            ),
        )
        .unwrap();
        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handler_release = release.clone();
        let handler: Arc<dyn Handler> = Arc::new(move |task: &Task| {
            while !handler_release.load(std::sync::atomic::Ordering::Acquire) {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Ok(Step::Succeed(task.checkpoint().data.clone()))
        });
        h.install_extension(Extension {
            name: "reviewer".into(),
            hooks: vec![Arc::new(YieldReviews)],
            tasks: vec![("review".into(), handler.clone())],
            ..Default::default()
        })
        .unwrap();
        let mut registry = Registry::default();
        assert!(registry.register_shared("review", true, handler));
        let driver = h.start_tasks(registry).unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["reviewer".into()]).unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        let c = h.conversation(id).unwrap();
        assert!(matches!(c.submissions[0].status, Status::Running));
        let state = serde_json::to_value(&c).unwrap();
        let ids = state["phase"]["YieldTasksWaiting"]["children"]
            .as_array()
            .unwrap();
        if queued {
            h.submit(id, None, "queued message".into()).unwrap();
        }
        release.store(true, std::sync::atomic::Ordering::Release);
        for child in ids {
            let child = child.as_str().unwrap().parse().unwrap();
            for _ in 0..200 {
                if driver.get(child).unwrap().unwrap().is_terminal() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert!(driver.get(child).unwrap().unwrap().is_terminal());
        }
        h.resume_conversation(id, &CancellationToken::new())
            .await
            .unwrap();
        let c = h.conversation(id).unwrap();
        let chosen = c
            .messages
            .iter()
            .any(|m| matches!(m, Message::User { content } if content == "chosen review"));
        assert_eq!(chosen, !queued);
        assert!(
            !c.messages
                .iter()
                .any(|m| matches!(m, Message::User { content } if content == "later review"))
        );
        assert!(matches!(c.submissions[0].status, Status::Done { .. }));
        driver.shutdown().unwrap();
    }
}

struct FailingSection;
#[async_trait]
impl Section for FailingSection {
    fn key(&self) -> &str {
        "broken"
    }
    async fn render(&self, _: &Context<'_>) -> Result<Option<String>, String> {
        Err("section failed".into())
    }
}
// A hand-written implementation can panic before returning its future.
struct SynchronousPanicSection;
impl Section for SynchronousPanicSection {
    fn key(&self) -> &str {
        "panic"
    }
    fn render<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        _: &'life1 Context<'life2>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<String>, String>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        panic!("section failed before future construction")
    }
}

struct PollPanicSection;
struct PanicOnDrop;
impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("panic payload destructor failed");
    }
}
#[async_trait]
impl Section for PollPanicSection {
    fn key(&self) -> &str {
        "poll-panic"
    }
    async fn render(&self, _: &Context<'_>) -> Result<Option<String>, String> {
        std::panic::panic_any(PanicOnDrop)
    }
}

struct SynchronousPanicHook;
impl Hook for SynchronousPanicHook {
    fn before_tool<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        _: &'life1 HookCall,
        _: &'life2 Context<'life3>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<BeforeTool, String>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        panic!("hook failed before future construction")
    }
}

#[tokio::test]
async fn synchronous_extension_panics_are_reported_and_replayed_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(FakeModel::default());
    let tool = Arc::new(FakeTool::default());
    let first;
    {
        let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
        h.install_extension(Extension {
            name: "panic-extension".into(),
            sections: vec![
                Arc::new(SynchronousPanicSection),
                Arc::new(PollPanicSection),
            ],
            hooks: vec![Arc::new(SynchronousPanicHook)],
            ..Default::default()
        })
        .unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["panic-extension".into()])
            .unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert!(matches!(
            h.conversation(id).unwrap().submissions[0].status,
            Status::Done { .. }
        ));
        let subscription = h.subscribe_extension_reports().unwrap();
        first = subscription.read(0, 256).unwrap();
        for phase in ["section", "before_tool"] {
            assert!(first.iter().any(|d| d.batch.phase == phase
                && d.batch.reports[0].extension == "panic-extension"
                && d.batch.reports[0].error == "extension panicked"));
        }
        let section = first.iter().find(|d| d.batch.phase == "section").unwrap();
        assert_eq!(section.batch.reports.len(), 2);
        assert!(
            section
                .batch
                .reports
                .iter()
                .all(|r| r.error == "extension panicked")
        );
        assert!(tool.0.lock().unwrap().is_empty());
        assert_eq!(subscription.read(0, 256).unwrap(), first);
    }
    let h = Harness::open_paused(&path, options(model, tool)).unwrap();
    let subscription = h.subscribe_extension_reports().unwrap();
    assert_eq!(subscription.read(0, 256).unwrap(), first);
    assert!(
        subscription
            .read(first.last().unwrap().cursor, 256)
            .unwrap()
            .is_empty()
    );
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut tx = Some(tx);
    let mut failed = h
        .dispatch_extension_reports(0, std::time::Duration::from_millis(1), move |delivery| {
            tx.take().unwrap().send(delivery).unwrap();
            async { Err("retry".into()) }
        })
        .unwrap();
    assert_eq!(rx.await.unwrap(), first[0]);
    assert!(matches!(
        failed.close().await,
        Err(pi_rs_durable::report::DispatchError::Listener { cursor: 0, .. })
    ));
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut tx = Some(tx);
    let mut replay = h
        .dispatch_extension_reports(0, std::time::Duration::from_millis(1), move |delivery| {
            if let Some(tx) = tx.take() {
                tx.send(delivery).unwrap();
            }
            async { Ok(()) }
        })
        .unwrap();
    assert_eq!(rx.await.unwrap(), first[0]);
    assert!(replay.close().await.unwrap() >= first[0].cursor);
}

struct ReportingHook;
#[async_trait]
impl Hook for ReportingHook {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        Err(format!("{event:?} failed"))
    }
    async fn before_tool(&self, _: &HookCall, _: &Context<'_>) -> Result<BeforeTool, String> {
        Err("guard failed".into())
    }
    async fn after_tool(
        &self,
        _: &HookCall,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        Err("after failed".into())
    }
}
#[tokio::test]
async fn extension_failures_are_observable_and_guard_still_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(FakeModel::default());
    let tool = Arc::new(FakeTool::default());
    let reports;
    {
        let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
        h.install_extension(Extension {
            name: "broken".into(),
            sections: vec![Arc::new(FailingSection)],
            hooks: vec![Arc::new(ReportingHook)],
            ..Default::default()
        })
        .unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["broken".into()]).unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert!(matches!(
            h.conversation(id).unwrap().submissions[0].status,
            Status::Done { .. }
        ));
        assert!(tool.0.lock().unwrap().is_empty());
        reports = h.extension_reports().unwrap();
        for phase in [
            "section",
            "before_request",
            "after_response",
            "before_tool",
            "after_tool",
            "AfterTools",
            "OnYield",
            "AfterRun",
        ] {
            assert!(
                reports
                    .iter()
                    .any(|batch| batch.phase == phase && batch.reports[0].extension == "broken"),
                "missing {phase}: {reports:?}"
            );
        }
    }
    let h = Harness::open_paused(&path, options(model, tool)).unwrap();
    assert_eq!(h.extension_reports().unwrap(), reports);
}

#[tokio::test]
async fn failed_hook_children_are_reported_and_run_continues() {
    use pi_rs_durable::{
        extension::GenerationEvent,
        task::Task,
        task_scheduler::{Handler, Registry, Step},
    };
    for event in [
        GenerationEvent::OnYield,
        GenerationEvent::AfterTools,
        GenerationEvent::AfterRun,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(
            dir.path().join("db"),
            options(
                Arc::new(FakeModel::default()),
                Arc::new(FakeTool::default()),
            ),
        )
        .unwrap();
        let handler: Arc<dyn Handler> =
            Arc::new(|_: &Task| -> Result<Step, String> { Ok(Step::Fail("task broke".into())) });
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
        let mut registry = Registry::default();
        assert!(registry.register_shared("job", true, handler));
        let driver = h.start_tasks(registry).unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["lifecycle".into()]).unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        for _ in 0..100 {
            h.resume_conversation(id, &CancellationToken::new())
                .await
                .unwrap();
            if h.extension_reports()
                .unwrap()
                .iter()
                .any(|r| r.phase == "task_failed")
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let reports = h.extension_reports().unwrap();
        let report = reports
            .iter()
            .find(|r| r.phase == "task_failed")
            .expect("task failure");
        assert_eq!(report.reports[0].extension, "lifecycle");
        assert!(report.reports[0].error.contains("task broke"));
        assert!(matches!(
            h.conversation(id).unwrap().submissions[0].status,
            Status::Done { .. }
        ));
        driver.shutdown().unwrap();
    }
}

#[tokio::test]
async fn runtime_owner_recovers_waiting_hook_task_and_conversation() {
    use pi_rs_durable::{
        extension::{GenerationEvent, Registry as Extensions},
        runtime::RuntimeOwner,
        task::{Checkpoint, Task, TaskState, WaitFor},
        task_scheduler::{Handler, Registry, Step},
        task_store::Signal,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let handler: Arc<dyn Handler> = Arc::new(|task: &Task| {
        if task.delivered_signal().is_some() {
            Ok(Step::Succeed(json!(null)))
        } else {
            Ok(Step::Wait {
                checkpoint: Checkpoint {
                    phase: "waiting".into(),
                    data: json!(null),
                },
                condition: WaitFor::Signal("go".into()),
                children: vec![],
            })
        }
    });
    let extension = || Extension {
        name: "lifecycle".into(),
        hooks: vec![Arc::new(LifecycleHook {
            events: Arc::default(),
            task_event: Some(GenerationEvent::AfterTools),
            park_event: None,
            entered: Arc::new(tokio::sync::Semaphore::new(0)),
        })],
        tasks: vec![("job".into(), handler.clone())],
        ..Default::default()
    };
    let registry = || {
        let mut r = Registry::default();
        assert!(r.register_shared("job", true, handler.clone()));
        r
    };
    let opts = || {
        options(
            Arc::new(FakeModel::default()),
            Arc::new(FakeTool::default()),
        )
    };
    let (id, child);
    {
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        h.install_extension(extension()).unwrap();
        let driver = h.start_tasks(registry()).unwrap();
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["lifecycle".into()]).unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        let state = serde_json::to_value(h.conversation(id).unwrap()).unwrap();
        child = state["phase"]["HookTasksWaiting"]["children"][0]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if matches!(
                    driver.get(child).unwrap().unwrap().state(),
                    TaskState::Waiting(_)
                ) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        driver.shutdown().unwrap();
    }
    let mut extensions = Extensions::default();
    extensions.install(extension()).unwrap();
    let mut owner = RuntimeOwner::open(&path, opts(), extensions, registry()).unwrap();
    owner
        .tasks()
        .signal(&Signal::new(
            uuid::Uuid::new_v4(),
            child,
            "go".into(),
            json!(null),
        ))
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            owner.scheduler_mut().wait(id).await.unwrap();
            if matches!(
                owner.scheduler().conversation(id).unwrap().submissions[0].status,
                Status::Done { .. }
            ) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(owner.tasks().get(child).unwrap().unwrap().is_terminal());
    owner.shutdown().await.unwrap();
    RuntimeOwner::open(&path, opts(), Extensions::default(), Registry::default())
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn configured_runtime_owner_recovers_waiting_hook_task_and_conversation() {
    use pi_rs_durable::{
        extension::{GenerationEvent, Registry as Extensions},
        runtime::RuntimeOwner,
        task::{Checkpoint, Task, TaskState, WaitFor},
        task_scheduler::{Handler, Registry, Step},
        task_store::Signal,
    };
    struct Adapter;
    #[async_trait]
    impl pi_rs_durable::model::ProviderAdapter for Adapter {
        fn id(&self) -> &str {
            "unused/v1"
        }
        async fn prepare(
            &self,
            _: &pi_rs_durable::model::ProviderAttempt,
            _: &pi_rs_core::ModelRequest,
        ) -> Result<Value, pi_rs_core::Error> {
            unreachable!()
        }
        fn validate(
            &self,
            _: &pi_rs_durable::model::ProviderAttempt,
            _: &pi_rs_core::ModelRequest,
            _: &Value,
        ) -> Result<(), pi_rs_core::Error> {
            unreachable!()
        }
        async fn dispatch(
            &self,
            _: &pi_rs_durable::model::ProviderAttempt,
            _: &pi_rs_core::ModelRequest,
            _: Value,
        ) -> Result<pi_rs_core::ModelStream, pi_rs_core::Error> {
            unreachable!()
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let handler: Arc<dyn Handler> = Arc::new(|task: &Task| {
        if task.delivered_signal().is_some() {
            Ok(Step::Succeed(json!(null)))
        } else {
            Ok(Step::Wait {
                checkpoint: Checkpoint {
                    phase: "waiting".into(),
                    data: json!(null),
                },
                condition: WaitFor::Signal("go".into()),
                children: vec![],
            })
        }
    });
    let extension = || Extension {
        name: "lifecycle".into(),
        hooks: vec![Arc::new(LifecycleHook {
            events: Arc::default(),
            task_event: Some(GenerationEvent::AfterTools),
            park_event: None,
            entered: Arc::new(tokio::sync::Semaphore::new(0)),
        })],
        tasks: vec![("job".into(), handler.clone())],
        ..Default::default()
    };
    let registry = || {
        let mut r = Registry::default();
        assert!(r.register_shared("job", true, handler.clone()));
        r
    };
    let opts = || {
        options(
            Arc::new(FakeModel::default()),
            Arc::new(FakeTool::default()),
        )
    };
    let (id, child);
    {
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        h.install_extension(extension()).unwrap();
        let driver = h.start_tasks(registry()).unwrap();
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["lifecycle".into()]).unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        let state = serde_json::to_value(h.conversation(id).unwrap()).unwrap();
        child = state["phase"]["HookTasksWaiting"]["children"][0]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if matches!(
                    driver.get(child).unwrap().unwrap().state(),
                    TaskState::Waiting(_)
                ) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        driver.shutdown().unwrap();
    }
    let mut extensions = Extensions::default();
    extensions.install(extension()).unwrap();
    let mut owner = RuntimeOwner::open_with_provider_and_subagent_management(
        &path,
        opts(),
        extensions,
        registry(),
        Arc::new(Adapter),
        id,
        config(),
        Some(vec![]),
    )
    .unwrap();
    owner
        .tasks()
        .signal(&Signal::new(
            uuid::Uuid::new_v4(),
            child,
            "go".into(),
            json!(null),
        ))
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            owner.scheduler_mut().wait(id).await.unwrap();
            if matches!(
                owner.scheduler().conversation(id).unwrap().submissions[0].status,
                Status::Done { .. }
            ) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(owner.tasks().get(child).unwrap().unwrap().is_terminal());
    owner.shutdown().await.unwrap();
    RuntimeOwner::open(&path, opts(), Extensions::default(), Registry::default())
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn runtime_owner_stops_conversations_before_joining_task_driver() {
    use pi_rs_durable::{
        extension::{GenerationEvent, Registry as Extensions},
        runtime::RuntimeOwner,
        task::{Checkpoint, Task, TaskOwner},
        task_scheduler::{Registry, Step},
    };
    struct OnDrop(Arc<tokio::sync::Semaphore>);
    impl Drop for OnDrop {
        fn drop(&mut self) {
            self.0.add_permits(1);
        }
    }
    struct Held {
        entered: Arc<tokio::sync::Semaphore>,
        dropped: Arc<tokio::sync::Semaphore>,
    }
    #[async_trait]
    impl Hook for Held {
        async fn generation(
            &self,
            event: GenerationEvent,
            _: &Value,
            _: &Context<'_>,
        ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
            if event == GenerationEvent::BeforeRequest {
                let _guard = OnDrop(self.dropped.clone());
                self.entered.add_permits(1);
                std::future::pending::<()>().await;
            }
            Ok(pi_rs_durable::extension::GenerationDecision::Pass)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let dropped = Arc::new(tokio::sync::Semaphore::new(0));
    let mut extensions = Extensions::default();
    extensions
        .install(Extension {
            name: "held".into(),
            hooks: vec![Arc::new(Held {
                entered: entered.clone(),
                dropped: dropped.clone(),
            })],
            ..Default::default()
        })
        .unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let mut tasks = Registry::default();
    tasks.register("held-task", true, move |_: &Task| {
        started_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
        Ok(Step::Succeed(json!(null)))
    });
    let opts = || {
        options(
            Arc::new(FakeModel::default()),
            Arc::new(FakeTool::default()),
        )
    };
    let mut owner = RuntimeOwner::open(&path, opts(), extensions, tasks).unwrap();
    let id = owner.scheduler_mut().create_conversation(config()).unwrap();
    owner
        .scheduler_mut()
        .select_extensions(id, vec!["held".into()])
        .unwrap();
    owner.scheduler_mut().submit(id, None, "go".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let task = Task::new(
        uuid::Uuid::new_v4(),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        5,
    )
    .unwrap()
    .registered("held-task".into(), true);
    owner.tasks().submit(&task).unwrap();
    tokio::task::spawn_blocking(move || {
        started_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap()
    })
    .await
    .unwrap();
    let shutdown = tokio::spawn(owner.shutdown());
    tokio::time::timeout(std::time::Duration::from_secs(5), dropped.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(!shutdown.is_finished());
    assert!(Harness::open_paused(&path, opts()).is_err());
    release_tx.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    Harness::open_paused(&path, opts()).unwrap();
}

struct ControlModel(Mutex<usize>);
#[async_trait]
impl Model for ControlModel {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        *self.0.lock().unwrap() += 1;
        let answered = request
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }));
        let calls = if answered {
            vec![]
        } else {
            (0..2)
                .map(|i| ToolCall {
                    id: format!("call{i}"),
                    name: "controlled".into(),
                    arguments: format!("{{\"n\":{i}}}"),
                })
                .collect()
        };
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text(
                if answered { "followup" } else { "tool answer" }.into(),
            )),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])
        .boxed())
    }
}
struct ControlledTool {
    controls: [bool; 2],
    handoffs: [Option<String>; 2],
    additions: [Vec<String>; 2],
    calls: Arc<Mutex<Vec<usize>>>,
    fail_second: bool,
}
#[async_trait]
impl pi_rs_durable::keyed_tool::KeyedTool for ControlledTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "controlled".into(),
            description: String::new(),
            parameters: json!({"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]}),
        }
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        _: pi_rs_durable::keyed_tool::KeyedHost,
    ) -> pi_rs_core::ToolReply {
        unreachable!()
    }
    async fn invoke_controlled(
        &self,
        args: Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        _: pi_rs_durable::keyed_tool::KeyedHost,
    ) -> (pi_rs_core::ToolReply, pi_rs_durable::reset::ToolControl) {
        let n = args["n"].as_u64().unwrap() as usize;
        self.calls.lock().unwrap().push(n);
        if n == 1 && self.fail_second {
            return (Outcome::Failed("failed".into()).into(), Default::default());
        }
        (
            Outcome::Succeeded(format!("result {n}")).into(),
            pi_rs_durable::reset::ToolControl {
                add_tools: self.additions[n].clone(),
                terminate: self.controls[n],
                handoff: self.handoffs[n].clone(),
            },
        )
    }
}
struct ControlHook {
    cancel: Option<CancellationToken>,
    after_run: Arc<Mutex<usize>>,
}
#[async_trait]
impl Hook for ControlHook {
    async fn after_tool(
        &self,
        _: &HookCall,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
        }
        Ok(None)
    }
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        if matches!(event, pi_rs_durable::extension::GenerationEvent::AfterRun) {
            *self.after_run.lock().unwrap() += 1;
        }
        Ok(pi_rs_durable::extension::GenerationDecision::Pass)
    }
}
fn control_options(
    model: Arc<ControlModel>,
    calls: Arc<Mutex<Vec<usize>>>,
    controls: [bool; 2],
    fail_second: bool,
) -> Options {
    let tool = Arc::new(pi_rs_durable::keyed_tool::KeyedToolAdapter::new(
        ControlledTool {
            controls,
            handoffs: [None, None],
            additions: Default::default(),
            calls,
            fail_second,
        },
    ));
    Options {
        model,
        tools: vec![tool.register(ReplayPolicy::Safe)],
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn install_control_hook(
    h: &Harness,
    cancel: Option<CancellationToken>,
    after_run: Arc<Mutex<usize>>,
) {
    h.install_extension(Extension {
        name: "control-hook".into(),
        hooks: vec![Arc::new(ControlHook { cancel, after_run })],
        ..Default::default()
    })
    .unwrap();
}
#[tokio::test]
async fn controlled_multi_tool_round_requires_every_result() {
    for (controls, fail_second, expected) in [
        ([true, true], false, 1),
        ([true, false], false, 2),
        ([false, true], false, 2),
        ([true, true], true, 2),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(ControlModel(Mutex::new(0)));
        let calls = Arc::new(Mutex::new(vec![]));
        let after = Arc::new(Mutex::new(0));
        let mut h = Harness::open_paused(
            dir.path().join("db"),
            control_options(model.clone(), calls.clone(), controls, fail_second),
        )
        .unwrap();
        install_control_hook(&h, None, after.clone());
        let id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["control-hook".into()])
            .unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(*model.0.lock().unwrap(), expected);
        assert_eq!(*calls.lock().unwrap(), vec![0, 1]);
        assert_eq!(*after.lock().unwrap(), 1);
        let c = h.conversation(id).unwrap();
        assert!(
            matches!(&c.submissions[0].status, Status::Done { answer } if answer == if expected == 1 { "tool answer" } else { "followup" })
        );
    }
}
#[tokio::test]
async fn controlled_result_survives_cancel_and_reopen_without_reexecution() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(ControlModel(Mutex::new(0)));
    let calls = Arc::new(Mutex::new(vec![]));
    let after = Arc::new(Mutex::new(0));
    let id;
    {
        let cancel = CancellationToken::new();
        let mut h = Harness::open_paused(
            &path,
            control_options(model.clone(), calls.clone(), [true, true], false),
        )
        .unwrap();
        install_control_hook(&h, Some(cancel.clone()), after.clone());
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["control-hook".into()])
            .unwrap();
        assert!(
            h.submit_and_run(id, None, "go".into(), &cancel)
                .await
                .is_err()
        );
        assert_eq!(*calls.lock().unwrap(), vec![0]);
    }
    let mut h = Harness::open_paused(
        &path,
        control_options(model.clone(), calls.clone(), [true, true], false),
    )
    .unwrap();
    install_control_hook(&h, None, after.clone());
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(*calls.lock().unwrap(), vec![0, 1]);
    assert_eq!(*model.0.lock().unwrap(), 1);
    assert_eq!(*after.lock().unwrap(), 1);
    assert!(
        matches!(&h.conversation(id).unwrap().submissions[0].status, Status::Done { answer } if answer == "tool answer")
    );
}

struct CrashControlHook;
#[async_trait]
impl Hook for CrashControlHook {
    async fn after_tool(
        &self,
        _: &HookCall,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        std::process::abort()
    }
}
#[tokio::test]
async fn controlled_crash_worker() {
    let Ok(path) = std::env::var("PI_CONTROL_CRASH_DB") else {
        return;
    };
    let model = Arc::new(HandoffModel(Mutex::new(vec![])));
    let mut h = Harness::open_paused(
        &path,
        handoff_options(
            model,
            Arc::default(),
            [Some("crash handoff".into()), None],
            true,
        ),
    )
    .unwrap();
    h.install_extension(Extension {
        name: "control-hook".into(),
        hooks: vec![Arc::new(CrashControlHook)],
        ..Default::default()
    })
    .unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["control-hook".into()])
        .unwrap();
    std::fs::write(format!("{path}.id"), id.to_string()).unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    panic!("expected crash");
}
#[tokio::test]
async fn controlled_result_survives_process_crash() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "controlled_crash_worker"])
        .env("PI_CONTROL_CRASH_DB", &path)
        .status()
        .unwrap();
    assert!(!status.success());
    let id = std::fs::read_to_string(format!("{}.id", path.display()))
        .unwrap()
        .parse()
        .unwrap();
    let model = Arc::new(HandoffModel(Mutex::new(vec![])));
    let calls = Arc::new(Mutex::new(vec![]));
    let after = Arc::new(Mutex::new(0));
    let mut h = Harness::open_paused(
        &path,
        handoff_options(
            model.clone(),
            calls.clone(),
            [Some("crash handoff".into()), None],
            true,
        ),
    )
    .unwrap();
    install_control_hook(&h, None, after.clone());
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(*calls.lock().unwrap(), vec![1]);
    assert_eq!(model.0.lock().unwrap().len(), 0);
    assert_eq!(
        h.conversation(id).unwrap().resets[0].handoff.as_deref(),
        Some("crash handoff")
    );
    assert_eq!(*after.lock().unwrap(), 1);
    assert!(
        matches!(&h.conversation(id).unwrap().submissions[0].status, Status::Done { answer } if answer == "tool answer")
    );
}

struct HandoffModel(Mutex<Vec<Vec<Message>>>);
#[async_trait]
impl Model for HandoffModel {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let first = {
            let mut requests = self.0.lock().unwrap();
            requests.push(request.messages);
            requests.len() == 1
        };
        let calls = if first {
            (0..2)
                .map(|n| ToolCall {
                    id: format!("c{n}"),
                    name: "controlled".into(),
                    arguments: format!(r#"{{"n":{n}}}"#),
                })
                .collect()
        } else {
            vec![]
        };
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text(
                if first { "tool answer" } else { "done" }.into(),
            )),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])
        .boxed())
    }
}
fn handoff_options(
    model: Arc<HandoffModel>,
    calls: Arc<Mutex<Vec<usize>>>,
    handoffs: [Option<String>; 2],
    terminate: bool,
) -> Options {
    let tool = Arc::new(pi_rs_durable::keyed_tool::KeyedToolAdapter::new(
        ControlledTool {
            controls: [terminate; 2],
            handoffs,
            additions: Default::default(),
            calls,
            fail_second: false,
        },
    ));
    Options {
        model,
        tools: vec![tool.register(ReplayPolicy::Safe)],
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
#[tokio::test]
async fn handoff_last_call_including_empty_starts_new_context_and_preserves_history() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(HandoffModel(Mutex::new(vec![])));
    let calls = Arc::new(Mutex::new(vec![]));
    let mut h = Harness::open_paused(
        dir.path().join("db"),
        handoff_options(
            model.clone(),
            calls,
            [Some("first".into()), Some("".into())],
            false,
        ),
    )
    .unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.submit_and_run(id, None, "old input".into(), &CancellationToken::new())
        .await
        .unwrap();
    let requests = model.0.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1]
            .iter()
            .any(|m| matches!(m, Message::User { content } if content.is_empty()))
    );
    assert!(
        !requests[1].iter().any(|m| matches!(m, Message::Tool { .. })
            || matches!(m, Message::User { content } if content == "old input"))
    );
    let c = h.conversation(id).unwrap();
    assert_eq!(c.resets.len(), 1);
    assert_eq!(c.resets[0].handoff.as_deref(), Some(""));
}
#[tokio::test]
async fn handoff_terminating_round_survives_cancel_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(HandoffModel(Mutex::new(vec![])));
    let calls = Arc::new(Mutex::new(vec![]));
    let after = Arc::new(Mutex::new(0));
    let id;
    {
        let cancel = CancellationToken::new();
        let mut h = Harness::open_paused(
            &path,
            handoff_options(
                model.clone(),
                calls.clone(),
                [Some("saved".into()), None],
                true,
            ),
        )
        .unwrap();
        install_control_hook(&h, Some(cancel.clone()), after.clone());
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["control-hook".into()])
            .unwrap();
        assert!(
            h.submit_and_run(id, None, "go".into(), &cancel)
                .await
                .is_err()
        );
    }
    let mut h = Harness::open_paused(
        &path,
        handoff_options(
            model.clone(),
            calls.clone(),
            [Some("saved".into()), None],
            true,
        ),
    )
    .unwrap();
    install_control_hook(&h, None, after.clone());
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(*calls.lock().unwrap(), vec![0, 1]);
    assert_eq!(c.resets.len(), 1);
    assert_eq!(c.resets[0].handoff.as_deref(), Some("saved"));
    assert!(matches!(&c.submissions[0].status, Status::Done { answer } if answer == "tool answer"));
    assert_eq!(*after.lock().unwrap(), 1);
}

#[tokio::test]
async fn handoff_waits_for_after_tools_and_survives_reopen() {
    use pi_rs_durable::task::Task;
    use pi_rs_durable::task_scheduler::{Handler, Registry, Step};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(HandoffModel(Mutex::new(vec![])));
    let calls = Arc::new(Mutex::new(vec![]));
    let after = Arc::new(Mutex::new(vec![]));
    let handler: Arc<dyn Handler> =
        Arc::new(|_: &Task| Ok(Step::Succeed(json!({"terminate": true}))));
    let install = |h: &Harness| {
        h.install_extension(Extension {
            name: "decision".into(),
            hooks: vec![Arc::new(TerminateRoundHook {
                after_run: after.clone(),
            })],
            tasks: vec![("decision".into(), handler.clone())],
            ..Default::default()
        })
        .unwrap()
    };
    let id;
    {
        let mut h = Harness::open_paused(
            &path,
            handoff_options(
                model.clone(),
                calls.clone(),
                [Some("fresh".into()), None],
                false,
            ),
        )
        .unwrap();
        install(&h);
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["decision".into()]).unwrap();
        h.submit_and_run(id, None, "old".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert!(h.conversation(id).unwrap().resets.is_empty());
        h.submit(id, None, "queued".into()).unwrap();
    }
    let mut h = Harness::open_paused(
        &path,
        handoff_options(
            model.clone(),
            calls.clone(),
            [Some("fresh".into()), None],
            false,
        ),
    )
    .unwrap();
    install(&h);
    let mut registry = Registry::default();
    registry.register_shared("decision", true, handler);
    let driver = h.start_tasks(registry).unwrap();
    for _ in 0..200 {
        h.resume_conversation(id, &CancellationToken::new())
            .await
            .unwrap();
        if !h.conversation(id).unwrap().resets.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let c = h.conversation(id).unwrap();
    assert_eq!(c.resets.len(), 1);
    assert_eq!(c.resets[0].handoff.as_deref(), Some("fresh"));
    assert!(matches!(&c.submissions[0].status, Status::Done { answer } if answer == "tool answer"));
    assert_eq!(*calls.lock().unwrap(), vec![0, 1]);
    let snapshots = after.lock().unwrap();
    assert!(!snapshots.is_empty());
    assert_eq!(snapshots[0]["resets"][0]["handoff"], "fresh");
    drop(snapshots);
    driver.shutdown().unwrap();
}

#[derive(Default)]
struct AddToolsModel(Mutex<Vec<Vec<String>>>);
struct EarlyTool;
#[async_trait]
impl Tool for EarlyTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "aaa".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Succeeded("ok".into())
    }
}
#[async_trait]
impl Model for AddToolsModel {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let answered = request
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }));
        self.0
            .lock()
            .unwrap()
            .push(request.tools.iter().map(|t| t.name.clone()).collect());
        let calls = if answered {
            vec![]
        } else {
            (0..2)
                .map(|n| ToolCall {
                    id: format!("call{n}"),
                    name: "controlled".into(),
                    arguments: format!("{{\"n\":{n}}}"),
                })
                .collect()
        };
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])
        .boxed())
    }
}

#[tokio::test]
async fn added_tools_are_persisted_in_call_order_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(AddToolsModel::default());
    let calls = Arc::new(Mutex::new(vec![]));
    let options = || Options {
        model: model.clone(),
        tools: vec![
            Arc::new(pi_rs_durable::keyed_tool::KeyedToolAdapter::new(
                ControlledTool {
                    controls: [false, false],
                    handoffs: [None, None],
                    additions: [
                        vec!["test".into(), "".into(), "missing".into()],
                        vec!["missing".into(), "aaa".into(), "later".into()],
                    ],
                    calls: calls.clone(),
                    fail_second: false,
                },
            ))
            .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(FakeTool::default()),
                replay: ReplayPolicy::Safe,
            },
            RegisteredTool {
                tool: Arc::new(EarlyTool),
                replay: ReplayPolicy::Safe,
            },
        ],
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let id;
    {
        let cancel = CancellationToken::new();
        let mut h = Harness::open_paused(&path, options()).unwrap();
        install_control_hook(&h, Some(cancel.clone()), Arc::new(Mutex::new(0)));
        id = h.create_conversation(config()).unwrap();
        h.select_tools(id, Some(vec!["controlled".into()])).unwrap();
        h.select_extensions(id, vec!["control-hook".into()])
            .unwrap();
        assert!(
            h.submit_and_run(id, None, "go".into(), &cancel)
                .await
                .is_err()
        );
    }
    {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        install_control_hook(&h, None, Arc::new(Mutex::new(0)));
        assert_eq!(
            h.conversation(id).unwrap().allowed_tools,
            Some(vec!["controlled".into()])
        );
        h.resume_conversation(id, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            h.conversation(id).unwrap().allowed_tools,
            Some(vec![
                "controlled".into(),
                "test".into(),
                "missing".into(),
                "aaa".into(),
                "later".into()
            ])
        );
    }
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(id).unwrap().allowed_tools.unwrap().len(), 5);
    assert_eq!(*calls.lock().unwrap(), vec![0, 1]);
    let requests = model.0.lock().unwrap();
    assert_eq!(requests[0], vec!["controlled"]);
    assert_eq!(requests[1], vec!["controlled", "test", "aaa"]);
}

struct YieldDecisionHook(&'static str);
#[async_trait]
impl Hook for YieldDecisionHook {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        _: &Value,
        ctx: &Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision as D, GenerationEvent as E, HookTask};
        if event != E::OnYield {
            return Ok(D::Pass);
        }
        match self.0 {
            "invalid" => Ok(D::Continue(json!({"bad": true}))),
            "error" => Err("yield failed".into()),
            "tasks" => {
                let operation = ctx.snapshot["phase"]["OnYield"]["operation_id"].clone();
                assert_eq!(
                    ctx.memo.get_or_insert("replay", operation.clone()).await?,
                    operation
                );
                Ok(D::Tasks(vec![HookTask {
                    name: "review".into(),
                    input: json!(null),
                }]))
            }
            "continue"
                if !ctx.snapshot["messages"]
                    .to_string()
                    .contains("retry answer") =>
            {
                Ok(D::Continue(json!("retry answer")))
            }
            _ => Ok(D::Pass),
        }
    }
}

#[tokio::test]
async fn yield_invalid_and_failed_hooks_allow_later_continuation() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(FakeModel::default());
    let mut h = Harness::open_paused(
        dir.path().join("db"),
        options(model.clone(), Arc::new(FakeTool::default())),
    )
    .unwrap();
    h.install_extension(Extension {
        name: "yield".into(),
        hooks: vec![
            Arc::new(YieldDecisionHook("invalid")),
            Arc::new(YieldDecisionHook("error")),
            Arc::new(YieldDecisionHook("continue")),
        ],
        ..Default::default()
    })
    .unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.select_extensions(id, vec!["yield".into()]).unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert!(matches!(c.submissions[0].status, Status::Done { .. }));
    assert_eq!(
        c.messages
            .iter()
            .filter(|m| matches!(m, Message::User { content } if content == "retry answer"))
            .count(),
        1
    );
    assert_eq!(model.0.lock().unwrap().len(), 3);
    let reports = h.extension_reports().unwrap();
    let yields: Vec<_> = reports.iter().filter(|r| r.phase == "OnYield").collect();
    assert_eq!(yields.len(), 2);
    for batch in yields {
        assert_eq!(batch.reports.len(), 2);
        assert!(
            batch.reports[0]
                .error
                .contains("continuation must be a string")
        );
        assert_eq!(batch.reports[1].error, "yield failed");
    }
}

#[tokio::test]
async fn yield_report_replay_deduplicates_and_later_hook_creates_child() {
    use pi_rs_durable::{
        task::Task,
        task_scheduler::{Handler, Registry, Step},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(FakeModel::default());
    let tool = Arc::new(FakeTool::default());
    let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let handler_release = release.clone();
    let handler: Arc<dyn Handler> = Arc::new(move |_: &Task| {
        while !handler_release.load(std::sync::atomic::Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        Ok(Step::Succeed(json!(null)))
    });
    let install = |h: &mut Harness| {
        h.install_extension(Extension {
            name: "yield".into(),
            hooks: vec![
                Arc::new(YieldDecisionHook("invalid")),
                Arc::new(YieldDecisionHook("error")),
                Arc::new(YieldDecisionHook("tasks")),
            ],
            tasks: vec![("review".into(), handler.clone())],
            ..Default::default()
        })
        .unwrap()
    };
    let id;
    let reports;
    {
        let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
        install(&mut h);
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["yield".into()]).unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert!(matches!(
            h.conversation(id).unwrap().submissions[0].status,
            Status::Running
        ));
        reports = h.extension_reports().unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].reports.len(), 2);
        h.resume_conversation(id, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(h.extension_reports().unwrap(), reports);
    }
    let mut h = Harness::open_paused(&path, options(model.clone(), tool.clone())).unwrap();
    install(&mut h);
    let mut registry = Registry::default();
    assert!(registry.register_shared("review", true, handler));
    let driver = h.start_tasks(registry).unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let state = serde_json::to_value(h.conversation(id).unwrap()).unwrap();
    let children = state["phase"]["YieldTasksWaiting"]["children"]
        .as_array()
        .unwrap();
    assert_eq!(children.len(), 1);
    let child = children[0].as_str().unwrap().parse().unwrap();
    release.store(true, std::sync::atomic::Ordering::Release);
    for _ in 0..200 {
        if driver.get(child).unwrap().unwrap().is_terminal() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(driver.get(child).unwrap().unwrap().is_terminal());
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert!(matches!(
        h.conversation(id).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
    assert_eq!(h.extension_reports().unwrap(), reports);
    assert_eq!(tool.0.lock().unwrap().len(), 1);
    driver.shutdown().unwrap();
}
