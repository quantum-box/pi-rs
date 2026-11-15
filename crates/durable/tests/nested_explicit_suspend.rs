use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    Error, ExtensionState, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool,
    ToolCall, ToolContext, ToolReply, ToolSpec,
};
use pi_rs_durable::{
    keyed_tool::{KeyedHost, KeyedTool, KeyedToolAdapter},
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, Options, RegisteredTool, Scheduler},
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

struct ModelStub;
#[async_trait]
impl Model for ModelStub {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let done = request
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }));
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text(if done { "done" } else { "" }.into())),
            Ok(ModelEvent::Complete {
                calls: if done {
                    vec![]
                } else {
                    vec![ToolCall {
                        id: "call".into(),
                        name: "parent".into(),
                        arguments: "{}".into(),
                    }]
                },
                usage: None,
            }),
        ])
        .boxed())
    }
}
fn spec(name: &str) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        description: String::new(),
        parameters: json!({"type":"object"}),
    }
}
struct Parent(Arc<AtomicUsize>);
#[async_trait]
impl KeyedTool for Parent {
    fn spec(&self) -> ToolSpec {
        spec("parent")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        self.0.fetch_add(1, Ordering::SeqCst);
        match host
            .call_keyed_unpolled("child-key", "child", json!({}))
            .await
        {
            Outcome::Pending { .. } => {
                host.suspend_pending().unwrap();
                Outcome::Succeeded("discarded".into()).into()
            }
            outcome => outcome.into(),
        }
    }
}
struct Child(Arc<AtomicUsize>);
struct GateChild(Arc<AtomicUsize>);

struct PreSuspendParent {
    root: std::path::PathBuf,
    stop: bool,
}

struct MultiGateCrashParent {
    root: std::path::PathBuf,
    stop: bool,
}
#[async_trait]
impl KeyedTool for MultiGateCrashParent {
    fn spec(&self) -> ToolSpec {
        spec("parent")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        let a = host.call_keyed_unpolled("a", "gate_a", json!({})).await;
        let b = host.call_keyed_unpolled("b", "gate_b", json!({})).await;
        let remote = host.call_keyed_unpolled("r", "remote", json!({})).await;
        if [&a, &b, &remote]
            .iter()
            .any(|outcome| matches!(outcome, Outcome::Pending { .. }))
        {
            if self.stop {
                std::fs::write(self.root.join("multi-before-suspend"), b"ready").unwrap();
                std::future::pending::<()>().await;
            }
            host.suspend_pending().unwrap();
            Outcome::Succeeded("discarded".into()).into()
        } else if matches!(a, Outcome::Succeeded(_))
            && matches!(b, Outcome::Succeeded(_))
            && matches!(remote, Outcome::Succeeded(_))
        {
            Outcome::Succeeded("all complete".into()).into()
        } else {
            Outcome::Failed("unexpected nested result".into()).into()
        }
    }
}
struct MultiGateTool {
    root: std::path::PathBuf,
    name: &'static str,
}
#[async_trait]
impl Tool for MultiGateTool {
    fn spec(&self) -> ToolSpec {
        spec(self.name)
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        use std::io::Write;
        writeln!(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.root.join(format!("{}-invocations", self.name)))
                .unwrap(),
            "invoke"
        )
        .unwrap();
        if self.name == "remote" {
            Outcome::Pending {
                reference: "remote-job".into(),
            }
        } else {
            Outcome::Succeeded(self.name.into())
        }
    }
    fn can_poll(&self) -> bool {
        self.name == "remote"
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        if self.root.join("slow-poll").exists() {
            std::fs::write(self.root.join("poll-entered"), b"ready").unwrap();
            while !self.root.join("remote-ready").exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        Some(if self.root.join("remote-ready").exists() {
            Outcome::Succeeded("remote done".into())
        } else {
            Outcome::Pending {
                reference: "remote-job".into(),
            }
        })
    }
}
struct MultiGateHook(std::path::PathBuf);
#[async_trait]
impl pi_rs_durable::extension::Hook for MultiGateHook {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        ctx: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::BeforeTool, String> {
        use pi_rs_durable::extension::{BeforeTool, Sleep};
        if !call.name.starts_with("gate_") || ctx.snapshot.get("nested_before_tool_wake").is_some()
        {
            return Ok(BeforeTool::Pass);
        }
        Ok(BeforeTool::Sleep(Sleep {
            signals: vec![call.name.clone()],
            until_millis: None,
        }))
    }
    async fn after_tool_result(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &pi_rs_durable::extension::ToolResult,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolResult>, String> {
        if call.name == "remote" {
            std::fs::write(self.0.join("remote-hook"), b"done").unwrap();
        }
        Ok(None)
    }
}
fn multigate_options(root: &std::path::Path, stop: bool, unsafe_parent: bool) -> Options {
    let parent = Arc::new(KeyedToolAdapter::new(MultiGateCrashParent {
        root: root.into(),
        stop,
    }));
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            parent.register(if unsafe_parent {
                ReplayPolicy::Unsafe
            } else {
                ReplayPolicy::Safe
            }),
            RegisteredTool {
                tool: Arc::new(MultiGateTool {
                    root: root.into(),
                    name: "gate_a",
                }),
                replay: ReplayPolicy::Unsafe,
            },
            RegisteredTool {
                tool: Arc::new(MultiGateTool {
                    root: root.into(),
                    name: "gate_b",
                }),
                replay: ReplayPolicy::Unsafe,
            },
            RegisteredTool {
                tool: Arc::new(MultiGateTool {
                    root: root.into(),
                    name: "remote",
                }),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
#[tokio::test]
async fn multi_gate_pre_suspend_kill_worker() {
    let Some(root) = std::env::var_os("PI_MULTIGATE_KILL_DIR") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let mut scheduler = Scheduler::open_with_extensions(
        root.join("db"),
        multigate_options(
            &root,
            true,
            std::env::var_os("PI_MULTIGATE_UNSAFE").is_some(),
        ),
        hook_registry(Arc::new(MultiGateHook(root.clone()))),
    )
    .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["hooks".into()])
        .unwrap();
    std::fs::write(root.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn killed_before_suspend_recovers_multiple_gates_and_pending_without_redispatch() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for mode in [
        "gates-first",
        "remote-first",
        "slow",
        "abort",
        "abort-slow",
        "unsafe",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "multi_gate_pre_suspend_kill_worker"])
            .env("PI_MULTIGATE_KILL_DIR", root);
        if mode == "unsafe" {
            command.env("PI_MULTIGATE_UNSAFE", "1");
        }
        let mut process = Kill(command.spawn().unwrap());
        until(|| root.join("multi-before-suspend").exists()).await;
        process.0.kill().unwrap();
        process.0.wait().unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("remote-invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert!(!root.join("gate_a-invocations").exists());
        assert!(!root.join("gate_b-invocations").exists());
        let id = std::fs::read_to_string(root.join("conversation"))
            .unwrap()
            .parse()
            .unwrap();
        if mode == "remote-first" {
            std::fs::write(root.join("remote-ready"), b"ready").unwrap();
        }
        if mode == "slow" || mode == "abort-slow" {
            std::fs::write(root.join("slow-poll"), b"slow").unwrap();
        }
        let mut scheduler = Scheduler::open_with_extensions(
            root.join("db"),
            multigate_options(root, false, mode == "unsafe"),
            hook_registry(Arc::new(MultiGateHook(root.into()))),
        )
        .unwrap();
        if mode == "unsafe" {
            assert!(scheduler.blocked(id).unwrap().is_some());
            assert!(!scheduler.conversation(id).unwrap().nested_suspended());
            scheduler.shutdown().await.unwrap();
            continue;
        }
        until(|| scheduler.conversation(id).unwrap().nested_suspended()).await;
        if mode == "remote-first" {
            until(|| root.join("remote-hook").exists()).await;
        }
        if mode == "slow" || mode == "abort-slow" {
            until(|| root.join("poll-entered").exists()).await;
        }
        let signal_id = uuid::Uuid::new_v4();
        scheduler
            .send_signal(&pi_rs_durable::conversation_signal::Signal::new(
                signal_id,
                id,
                "gate_b".into(),
                json!(2),
            ))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        if mode == "slow" || mode == "abort-slow" {
            let store = durable_store::Store::open(root.join("db")).unwrap();
            until(|| {
                pi_rs_durable::conversation_signal::Inbox::new(&store)
                    .receipt(id, signal_id)
                    .unwrap()
                    .is_some_and(|receipt| receipt.is_consumed())
            })
            .await;
        }
        assert!(!root.join("gate_a-invocations").exists());
        assert!(!root.join("gate_b-invocations").exists());
        if mode == "abort" || mode == "abort-slow" {
            scheduler.abort(id).unwrap();
            scheduler.shutdown().await.unwrap();
            let mut reopened = Scheduler::open_with_extensions(
                root.join("db"),
                multigate_options(root, false, false),
                hook_registry(Arc::new(MultiGateHook(root.into()))),
            )
            .unwrap();
            reopened.schedule(id);
            wait(&mut reopened, id).await;
            reopened.shutdown().await.unwrap();
            assert!(!root.join("gate_a-invocations").exists());
            assert!(!root.join("gate_b-invocations").exists());
            assert_eq!(
                std::fs::read_to_string(root.join("remote-invocations"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
            continue;
        }
        scheduler
            .send_signal(&pi_rs_durable::conversation_signal::Signal::new(
                uuid::Uuid::new_v4(),
                id,
                "gate_a".into(),
                json!(1),
            ))
            .unwrap();
        if mode == "gates-first" || mode == "slow" {
            std::fs::write(root.join("remote-ready"), b"ready").unwrap();
        }
        scheduler.schedule(id);
        wait(&mut scheduler, id).await;
        assert!(scheduler.blocked(id).unwrap().is_none());
        for name in ["remote", "gate_a", "gate_b"] {
            assert_eq!(
                std::fs::read_to_string(root.join(format!("{name}-invocations")))
                    .unwrap()
                    .lines()
                    .count(),
                1,
                "{mode}: {name}"
            );
        }
        assert!(scheduler.conversation(id).unwrap().messages.iter().any(
            |message| matches!(message, Message::Tool { content, .. } if content == "all complete")
        ));
        scheduler.shutdown().await.unwrap();
    }
}
#[async_trait]
impl KeyedTool for PreSuspendParent {
    fn spec(&self) -> ToolSpec {
        spec("parent")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        match host
            .call_keyed_unpolled("child-key", "child", json!({}))
            .await
        {
            Outcome::Pending { .. } => {
                if self.stop {
                    std::fs::write(self.root.join("before-suspend"), b"ready").unwrap();
                    std::future::pending::<()>().await;
                }
                host.suspend_pending().unwrap();
                Outcome::Succeeded("discarded".into()).into()
            }
            outcome => outcome.into(),
        }
    }
}
struct PreparedGateChild(std::path::PathBuf);
#[async_trait]
impl Tool for PreparedGateChild {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn prepare_arguments(&self, mut args: Value) -> pi_rs_core::ExtensionResult<Value> {
        let path = self.0.join("preparations");
        let n = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(0)
            + 1;
        std::fs::write(path, n.to_string()).unwrap();
        if n > 1 && self.0.join("reject-prepare").exists() {
            return Err("wake prepare rejected".into());
        }
        args["prepared"] = json!(n);
        Ok(args)
    }
    async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
        std::fs::write(self.0.join("dispatched"), args.to_string()).unwrap();
        Outcome::Succeeded("prepared child".into())
    }
}
fn crash_gate_options(root: &std::path::Path, stop: bool, unsafe_parent: bool) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(PreSuspendParent {
                root: root.into(),
                stop,
            }))
            .register(if unsafe_parent {
                ReplayPolicy::Unsafe
            } else {
                ReplayPolicy::Safe
            }),
            RegisteredTool {
                tool: Arc::new(PreparedGateChild(root.into())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
#[tokio::test]
async fn before_suspend_gate_kill_worker() {
    let Some(root) = std::env::var_os("PI_GATE_PRESUSPEND_DIR") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let mode = if std::env::var_os("PI_GATE_DEADLINE").is_some() {
        "time"
    } else {
        "signal"
    };
    let mut scheduler = Scheduler::open_with_extensions(
        root.join("db"),
        crash_gate_options(&root, true, std::env::var_os("PI_GATE_UNSAFE").is_some()),
        hook_registry(Arc::new(NestedGate {
            mode,
            seen: Arc::default(),
        })),
    )
    .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["hooks".into()])
        .unwrap();
    std::fs::write(root.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn killed_before_suspend_gate_recovers_and_reprepares_after_signal_or_deadline() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for mode in ["signal", "time", "abort", "unsafe", "invalid"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "before_suspend_gate_kill_worker"])
            .env("PI_GATE_PRESUSPEND_DIR", root);
        if mode == "time" {
            command.env("PI_GATE_DEADLINE", "1");
        }
        if mode == "unsafe" {
            command.env("PI_GATE_UNSAFE", "1");
        }
        let mut process = Kill(command.spawn().unwrap());
        until(|| root.join("before-suspend").exists()).await;
        process.0.kill().unwrap();
        process.0.wait().unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("preparations")).unwrap(),
            "1"
        );
        let id = std::fs::read_to_string(root.join("conversation"))
            .unwrap()
            .parse()
            .unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        if mode == "invalid" {
            std::fs::write(root.join("reject-prepare"), b"yes").unwrap();
        }
        let mut scheduler = Scheduler::open_with_extensions(
            root.join("db"),
            crash_gate_options(root, false, mode == "unsafe"),
            hook_registry(Arc::new(NestedGate {
                mode: if mode == "time" { "time" } else { "signal" },
                seen: seen.clone(),
            })),
        )
        .unwrap();
        if mode == "abort" {
            scheduler.abort(id).unwrap();
            scheduler.shutdown().await.unwrap();
            let mut reopened = Scheduler::open_with_extensions(
                root.join("db"),
                crash_gate_options(root, false, false),
                hook_registry(Arc::new(NestedGate {
                    mode: "signal",
                    seen: Arc::default(),
                })),
            )
            .unwrap();
            reopened.schedule(id);
            wait(&mut reopened, id).await;
            reopened.shutdown().await.unwrap();
            assert!(!root.join("dispatched").exists());
            continue;
        }
        if mode == "unsafe" {
            assert!(scheduler.blocked(id).unwrap().is_some());
            assert!(!scheduler.conversation(id).unwrap().nested_suspended());
            scheduler.shutdown().await.unwrap();
            assert!(!root.join("dispatched").exists());
            continue;
        }
        if mode == "signal" || mode == "invalid" {
            scheduler
                .send_signal(&pi_rs_durable::conversation_signal::Signal::new(
                    uuid::Uuid::new_v4(),
                    id,
                    "gate".into(),
                    json!(42),
                ))
                .unwrap();
        }
        scheduler.schedule(id);
        wait(&mut scheduler, id).await;
        assert!(scheduler.blocked(id).unwrap().is_none(), "{mode}");
        assert_eq!(
            std::fs::read_to_string(root.join("preparations")).unwrap(),
            "2"
        );
        if mode == "invalid" {
            assert!(!root.join("dispatched").exists());
            assert!(seen.lock().unwrap().is_empty());
            assert!(scheduler.conversation(id).unwrap().messages.iter().any(|message| matches!(message, Message::Tool { content, .. } if content.contains("wake prepare rejected"))));
            scheduler.shutdown().await.unwrap();
            continue;
        }
        let args: Value =
            serde_json::from_slice(&std::fs::read(root.join("dispatched")).unwrap()).unwrap();
        assert_eq!(args["prepared"], 2);
        assert_eq!(
            seen.lock().unwrap()[0]["reason"],
            if mode == "time" { "time" } else { "signal" }
        );
        assert_eq!(seen.lock().unwrap()[0]["prepared"], 2);
        scheduler.shutdown().await.unwrap();
    }
}
#[async_trait]
impl Tool for GateChild {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        self.0.fetch_add(1, Ordering::SeqCst);
        Outcome::Succeeded("gate child done".into())
    }
}
struct NestedGate {
    mode: &'static str,
    seen: Arc<std::sync::Mutex<Vec<Value>>>,
}
#[async_trait]
impl pi_rs_durable::extension::Hook for NestedGate {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        ctx: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::BeforeTool, String> {
        use pi_rs_durable::extension::{BeforeTool, Sleep};
        if call.name != "child" {
            return Ok(BeforeTool::Pass);
        }
        let mut wake = ctx
            .snapshot
            .get("nested_before_tool_wake")
            .cloned()
            .unwrap_or(Value::Null);
        if let (Some(wake), Some(prepared)) = (wake.as_object_mut(), call.arguments.get("prepared"))
        {
            wake.insert("prepared".into(), prepared.clone());
        }
        self.seen.lock().unwrap().push(wake.clone());
        if !wake.is_null() && self.mode != "repeat" {
            return Ok(BeforeTool::Pass);
        }
        Ok(BeforeTool::Sleep(match self.mode {
            "signal" | "repeat" => Sleep {
                signals: vec!["gate".into()],
                until_millis: None,
            },
            "time" => Sleep {
                signals: vec![],
                until_millis: Some(0),
            },
            _ => Sleep {
                signals: vec![],
                until_millis: None,
            },
        }))
    }
    async fn after_tool_result(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &pi_rs_durable::extension::ToolResult,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolResult>, String> {
        if call.name == "child" && (self.mode == "invalid" || self.mode == "repeat") {
            assert!(result.is_error);
            assert_eq!(result.diagnostics[0].code.as_deref(), Some("invalid_park"));
        }
        Ok(None)
    }
}
fn gate_options(parent: Arc<KeyedToolAdapter<Parent>>, child: Arc<GateChild>) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            parent.register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: child,
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
#[tokio::test]
async fn nested_gate_signal_reopen_and_deadline_wake() {
    for mode in ["signal", "time"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let pc = Arc::new(AtomicUsize::new(0));
        let cc = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let parent = Arc::new(KeyedToolAdapter::new(Parent(pc.clone())));
        let child = Arc::new(GateChild(cc.clone()));
        let registry = || {
            hook_registry(Arc::new(NestedGate {
                mode,
                seen: seen.clone(),
            }))
        };
        let id;
        {
            let mut scheduler = Scheduler::open_with_extensions(
                &path,
                gate_options(parent.clone(), child.clone()),
                registry(),
            )
            .unwrap();
            id = scheduler
                .create_conversation(Config {
                    model: "test".into(),
                    system_prompt: "".into(),
                    max_output_tokens: 100,
                })
                .unwrap();
            scheduler
                .select_extensions(id, vec!["hooks".into()])
                .unwrap();
            scheduler.submit(id, None, "work".into()).unwrap();
            until(|| !seen.lock().unwrap().is_empty()).await;
            if mode == "signal" {
                until(|| scheduler.conversation(id).unwrap().nested_suspended()).await;
            }
            scheduler.shutdown().await.unwrap();
        }
        let mut scheduler =
            Scheduler::open_with_extensions(&path, gate_options(parent, child), registry())
                .unwrap();
        if mode == "signal" {
            scheduler
                .send_signal(&pi_rs_durable::conversation_signal::Signal::new(
                    uuid::Uuid::new_v4(),
                    id,
                    "gate".into(),
                    json!(7),
                ))
                .unwrap();
        }
        scheduler.schedule(id);
        wait(&mut scheduler, id).await;
        assert_eq!(cc.load(Ordering::SeqCst), 1, "{mode}");
        assert_eq!(pc.load(Ordering::SeqCst), 2, "{mode}");
        {
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 2, "{mode}");
            assert_eq!(
                seen[1]["reason"],
                if mode == "signal" { "signal" } else { "time" }
            );
        }
        scheduler.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn nested_invalid_gate_and_consumed_signal_settle_without_child_execution() {
    for mode in ["invalid", "repeat"] {
        let dir = tempfile::tempdir().unwrap();
        let cc = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let parent = Arc::new(KeyedToolAdapter::new(Parent(Arc::new(AtomicUsize::new(0)))));
        let child = Arc::new(GateChild(cc.clone()));
        let mut scheduler = Scheduler::open_with_extensions(
            dir.path().join("db"),
            gate_options(parent, child),
            hook_registry(Arc::new(NestedGate {
                mode,
                seen: seen.clone(),
            })),
        )
        .unwrap();
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        scheduler
            .select_extensions(id, vec!["hooks".into()])
            .unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        if mode == "repeat" {
            until(|| scheduler.conversation(id).unwrap().nested_suspended()).await;
            scheduler
                .send_signal(&pi_rs_durable::conversation_signal::Signal::new(
                    uuid::Uuid::new_v4(),
                    id,
                    "gate".into(),
                    json!(1),
                ))
                .unwrap();
            scheduler.schedule(id);
        }
        wait(&mut scheduler, id).await;
        assert_eq!(cc.load(Ordering::SeqCst), 0);
        assert!(scheduler.blocked(id).unwrap().is_none());
        assert!(scheduler.extension_reports().unwrap().is_empty());
        assert!(scheduler.conversation(id).unwrap().messages.iter().any(
            |m| matches!(m, Message::Tool { content, .. } if content.contains("cannot wait"))
        ));
        scheduler.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn abort_nested_gate_fences_wake_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let pc = Arc::new(AtomicUsize::new(0));
    let cc = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let parent = Arc::new(KeyedToolAdapter::new(Parent(pc.clone())));
    let child = Arc::new(GateChild(cc.clone()));
    let registry = || {
        hook_registry(Arc::new(NestedGate {
            mode: "signal",
            seen: seen.clone(),
        }))
    };
    let mut scheduler = Scheduler::open_with_extensions(
        &path,
        gate_options(parent.clone(), child.clone()),
        registry(),
    )
    .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["hooks".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until(|| scheduler.conversation(id).unwrap().nested_suspended()).await;
    scheduler.abort(id).unwrap();
    scheduler.shutdown().await.unwrap();
    let mut scheduler =
        Scheduler::open_with_extensions(&path, gate_options(parent, child), registry()).unwrap();
    scheduler.schedule(id);
    wait(&mut scheduler, id).await;
    assert_eq!(cc.load(Ordering::SeqCst), 0);
    assert_eq!(pc.load(Ordering::SeqCst), 1);
    scheduler.shutdown().await.unwrap();
}
#[async_trait]
impl Tool for Child {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        self.0.fetch_add(1, Ordering::SeqCst);
        Outcome::Pending {
            reference: "external-job".into(),
        }
    }
}
fn options(parent: Arc<KeyedToolAdapter<Parent>>, child: Arc<Child>) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            parent.register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: child,
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
async fn wait(s: &mut Scheduler, id: uuid::Uuid) {
    tokio::time::timeout(Duration::from_secs(5), s.wait(id))
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn suspend_releases_worker_and_reopens_without_redispatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let pc = Arc::new(AtomicUsize::new(0));
    let cc = Arc::new(AtomicUsize::new(0));
    let parent = Arc::new(KeyedToolAdapter::new(Parent(pc.clone())));
    let child = Arc::new(Child(cc.clone()));
    let mut s = Scheduler::open(&path, options(parent.clone(), child.clone())).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.submit(id, None, "work".into()).unwrap();
    wait(&mut s, id).await;
    assert!(s.conversation(id).unwrap().nested_suspended());
    let root = s.blocked(id).unwrap().unwrap().0;
    let job = s.nested_pending_jobs(id, root).unwrap().pop().unwrap();
    // A second scheduled execution completes while the child remains pending.
    s.schedule(id);
    wait(&mut s, id).await;
    assert_eq!(pc.load(Ordering::SeqCst), 1);
    assert_eq!(cc.load(Ordering::SeqCst), 1);
    s.shutdown().await.unwrap();
    let mut s = Scheduler::open(&path, options(parent, child)).unwrap();
    wait(&mut s, id).await;
    assert_eq!(pc.load(Ordering::SeqCst), 1);
    assert!(s.conversation(id).unwrap().nested_suspended());
    s.reconcile_nested_pending(
        id,
        root,
        job.operation_id,
        &job.reference,
        Outcome::Succeeded("child-result".into()),
    )
    .unwrap();
    wait(&mut s, id).await;
    assert!(s.blocked(id).unwrap().is_none());
    assert_eq!(pc.load(Ordering::SeqCst), 2);
    assert_eq!(cc.load(Ordering::SeqCst), 1);
    assert!(
        s.conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content, ..} if content == "child-result"))
    );
    s.shutdown().await.unwrap();
}
#[tokio::test]
async fn abort_while_suspended_survives_reopen_and_fences_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let pc = Arc::new(AtomicUsize::new(0));
    let cc = Arc::new(AtomicUsize::new(0));
    let parent = Arc::new(KeyedToolAdapter::new(Parent(pc.clone())));
    let child = Arc::new(Child(cc.clone()));
    let mut s = Scheduler::open(&path, options(parent.clone(), child.clone())).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.submit(id, None, "work".into()).unwrap();
    wait(&mut s, id).await;
    let root = s.blocked(id).unwrap().unwrap().0;
    let job = s.nested_pending_jobs(id, root).unwrap().pop().unwrap();
    s.abort(id).unwrap();
    wait(&mut s, id).await;
    s.shutdown().await.unwrap();
    let mut s = Scheduler::open(&path, options(parent, child)).unwrap();
    wait(&mut s, id).await;
    assert!(s.conversation(id).unwrap().abort_requested);
    assert!(s.conversation(id).unwrap().nested_suspended());
    s.cancel_nested_pending(
        id,
        root,
        job.operation_id,
        &job.reference,
        "confirmed cancelled".into(),
    )
    .unwrap();
    wait(&mut s, id).await;
    assert_eq!(pc.load(Ordering::SeqCst), 1);
    assert_eq!(cc.load(Ordering::SeqCst), 1);
    s.shutdown().await.unwrap();
}

struct MultiParent(Arc<AtomicUsize>);
#[async_trait]
impl KeyedTool for MultiParent {
    fn spec(&self) -> ToolSpec {
        spec("parent")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        self.0.fetch_add(1, Ordering::SeqCst);
        let a = host
            .call_keyed_unpolled("a", "child", json!({"job":"a"}))
            .await;
        let b = host
            .call_keyed_unpolled("b", "child", json!({"job":"b"}))
            .await;
        if matches!(a, Outcome::Pending { .. }) || matches!(b, Outcome::Pending { .. }) {
            host.suspend_pending().unwrap();
            Outcome::Succeeded("discarded".into()).into()
        } else {
            match (a, b) {
                (Outcome::Succeeded(a), Outcome::Succeeded(b)) => {
                    Outcome::Succeeded(format!("{a},{b}")).into()
                }
                _ => Outcome::Failed("unexpected child outcome".into()).into(),
            }
        }
    }
}
struct AutoChild {
    invoked: Arc<AtomicUsize>,
    polls: Arc<AtomicUsize>,
    ready_a: Arc<std::sync::atomic::AtomicBool>,
    ready_b: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait]
impl Tool for AutoChild {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
        self.invoked.fetch_add(1, Ordering::SeqCst);
        Outcome::Pending {
            reference: args["job"].as_str().unwrap().into(),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn poll(&self, reference: &str, _: ToolContext) -> Option<Outcome> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        let ready = if reference == "a" {
            &self.ready_a
        } else {
            &self.ready_b
        };
        Some(if ready.load(Ordering::SeqCst) {
            Outcome::Succeeded(reference.into())
        } else {
            Outcome::Pending {
                reference: reference.into(),
            }
        })
    }
}
fn multi_options<T: KeyedTool + 'static>(
    parent: Arc<KeyedToolAdapter<T>>,
    child: Arc<AutoChild>,
) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            parent.register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: child,
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition did not become true");
}
#[tokio::test]
async fn multiple_children_are_polled_and_parent_wakes_automatically() {
    use std::sync::atomic::AtomicBool;
    let dir = tempfile::tempdir().unwrap();
    let pc = Arc::new(AtomicUsize::new(0));
    let cc = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let a = Arc::new(AtomicBool::new(false));
    let b = Arc::new(AtomicBool::new(false));
    let parent = Arc::new(KeyedToolAdapter::new(MultiParent(pc.clone())));
    let child = Arc::new(AutoChild {
        invoked: cc.clone(),
        polls: polls.clone(),
        ready_a: a.clone(),
        ready_b: b.clone(),
    });
    let mut s = Scheduler::open(dir.path().join("db"), multi_options(parent, child)).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.submit(id, None, "work".into()).unwrap();
    until(|| polls.load(Ordering::SeqCst) >= 2).await;
    let root = s.blocked(id).unwrap().unwrap().0;
    assert!(s.conversation(id).unwrap().nested_suspended());
    assert_eq!(s.nested_pending_jobs(id, root).unwrap().len(), 2);
    a.store(true, Ordering::SeqCst);
    until(|| {
        s.nested_pending_jobs(id, root)
            .is_ok_and(|jobs| jobs.len() == 1)
    })
    .await;
    assert_eq!(pc.load(Ordering::SeqCst), 1);
    b.store(true, Ordering::SeqCst);
    wait(&mut s, id).await;
    assert!(s.blocked(id).unwrap().is_none());
    assert_eq!(cc.load(Ordering::SeqCst), 2);
    assert_eq!(pc.load(Ordering::SeqCst), 2);
    assert!(
        s.conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { content, .. } if content == "a,b"))
    );
    s.shutdown().await.unwrap();
}

struct HangingChild {
    started: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
}
struct ActivePoll(Arc<AtomicUsize>);
impl Drop for ActivePoll {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl Tool for HangingChild {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Pending {
            reference: "hang".into(),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn poll(&self, _: &str, _: ToolContext) -> Option<Outcome> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActivePoll(self.active.clone());
        self.started.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
    }
}
fn hanging_options(parent: Arc<KeyedToolAdapter<Parent>>, child: Arc<HangingChild>) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            parent.register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: child,
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
#[tokio::test]
async fn abort_cancels_and_joins_live_poll_without_replay_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let pc = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let parent = Arc::new(KeyedToolAdapter::new(Parent(pc.clone())));
    let child = Arc::new(HangingChild {
        started: started.clone(),
        active: active.clone(),
    });
    let mut s = Scheduler::open(&path, hanging_options(parent.clone(), child.clone())).unwrap();
    let id = s
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    s.submit(id, None, "work".into()).unwrap();
    until(|| started.load(Ordering::SeqCst) == 1).await;
    let root = s.blocked(id).unwrap().unwrap().0;
    s.abort(id).unwrap();
    wait(&mut s, id).await;
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(s.nested_pending_jobs(id, root).unwrap().len(), 1);
    s.shutdown().await.unwrap();
    let mut s = Scheduler::open(&path, hanging_options(parent, child)).unwrap();
    wait(&mut s, id).await;
    assert!(s.conversation(id).unwrap().abort_requested);
    assert_eq!(started.load(Ordering::SeqCst), 1);
    assert_eq!(pc.load(Ordering::SeqCst), 1);
    s.shutdown().await.unwrap();
}

struct FileChild(std::path::PathBuf);
#[async_trait]
impl Tool for FileChild {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        use std::io::Write;
        writeln!(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("dispatches"))
                .unwrap(),
            "dispatch"
        )
        .unwrap();
        Outcome::Pending {
            reference: "persisted-reference".into(),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn poll(&self, reference: &str, _: ToolContext) -> Option<Outcome> {
        assert_eq!(reference, "persisted-reference");
        if self.0.join("finish").exists() {
            return Some(Outcome::Succeeded("after-kill".into()));
        }
        std::fs::write(self.0.join("poll-entered"), b"ready").unwrap();
        std::future::pending().await
    }
}
fn file_options<T: KeyedTool + 'static>(
    parent: Arc<KeyedToolAdapter<T>>,
    root: &std::path::Path,
) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            parent.register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(FileChild(root.into())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
#[tokio::test]
async fn nested_auto_poll_kill_worker() {
    let Some(root) = std::env::var_os("PI_NESTED_AUTO_KILL_DIR") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let parent = Arc::new(KeyedToolAdapter::new(Parent(Arc::new(AtomicUsize::new(0)))));
    let mut scheduler = Scheduler::open(root.join("db"), file_options(parent, &root)).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    std::fs::write(root.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn killed_suspended_poller_reopens_and_wakes_without_child_redispatch() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut process = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "nested_auto_poll_kill_worker"])
            .env("PI_NESTED_AUTO_KILL_DIR", root)
            .spawn()
            .unwrap(),
    );
    until(|| root.join("poll-entered").exists()).await;
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let id = std::fs::read_to_string(root.join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
    std::fs::write(root.join("finish"), b"done").unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let parent = Arc::new(KeyedToolAdapter::new(Parent(calls.clone())));
    let mut scheduler = Scheduler::open(root.join("db"), file_options(parent, root)).unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        std::fs::read_to_string(root.join("dispatches"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content, ..} if content == "after-kill"))
    );
    scheduler.shutdown().await.unwrap();
}

struct OrderedHooks {
    calls: Arc<std::sync::Mutex<Vec<String>>>,
}
struct OrderedParent {
    calls: Arc<AtomicUsize>,
    hooks: Arc<std::sync::Mutex<Vec<String>>>,
}
#[async_trait]
impl KeyedTool for OrderedParent {
    fn spec(&self) -> ToolSpec {
        spec("parent")
    }
    async fn invoke(
        &self,
        args: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        if self.calls.load(Ordering::SeqCst) > 0 {
            assert_eq!(*self.hooks.lock().unwrap(), vec!["legacy", "rich"]);
        }
        Parent(self.calls.clone())
            .invoke(args, context, state, host)
            .await
    }
}
#[async_trait]
impl pi_rs_durable::extension::Hook for OrderedHooks {
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &Value,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<Value>, String> {
        if call.name != "child" {
            return Ok(None);
        }
        self.calls.lock().unwrap().push("legacy".into());
        Ok(Some(Value::String(format!(
            "legacy:{}",
            result.as_str().unwrap()
        ))))
    }
    async fn after_tool_result(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &pi_rs_durable::extension::ToolResult,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolResult>, String> {
        if call.name != "child" {
            return Ok(None);
        }
        self.calls.lock().unwrap().push("rich".into());
        let mut result = result.clone();
        result.output = format!("rich:{}", result.output);
        Ok(Some(result))
    }
}
fn hook_registry(
    hook: Arc<dyn pi_rs_durable::extension::Hook>,
) -> pi_rs_durable::extension::Registry {
    let mut registry = pi_rs_durable::extension::Registry::default();
    registry
        .install(pi_rs_durable::extension::Extension {
            name: "hooks".into(),
            hooks: vec![hook],
            ..Default::default()
        })
        .unwrap();
    registry
}
#[tokio::test]
async fn selected_extension_runs_nested_hooks_in_order_before_automatic_parent_replay() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(root.join("finish"), b"ready").unwrap();
    let parent_calls = Arc::new(AtomicUsize::new(0));
    let hooks = Arc::new(std::sync::Mutex::new(Vec::new()));
    let parent = Arc::new(KeyedToolAdapter::new(OrderedParent {
        calls: parent_calls.clone(),
        hooks: hooks.clone(),
    }));
    let registry = hook_registry(Arc::new(OrderedHooks {
        calls: hooks.clone(),
    }));
    let mut scheduler =
        Scheduler::open_with_extensions(root.join("db"), file_options(parent, root), registry)
            .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["hooks".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(*hooks.lock().unwrap(), vec!["legacy", "rich"]);
    assert_eq!(parent_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        std::fs::read_to_string(root.join("dispatches"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert!(
        scheduler.conversation(id).unwrap().messages.iter().any(
            |m| matches!(m, Message::Tool {content,..} if content == "rich:legacy:after-kill")
        )
    );
    scheduler.shutdown().await.unwrap();
}

struct PauseHook {
    entered: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
}
#[async_trait]
impl pi_rs_durable::extension::Hook for PauseHook {
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &Value,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<Value>, String> {
        if call.name == "child" {
            self.active.fetch_add(1, Ordering::SeqCst);
            let _guard = ActivePoll(self.active.clone());
            self.entered.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
        }
        Ok(None)
    }
}
#[tokio::test]
async fn abort_joins_nested_result_hook_and_never_publishes_its_result() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(root.join("finish"), b"ready").unwrap();
    let entered = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let parent = Arc::new(KeyedToolAdapter::new(Parent(Arc::new(AtomicUsize::new(0)))));
    let registry = hook_registry(Arc::new(PauseHook {
        entered: entered.clone(),
        active: active.clone(),
    }));
    let mut scheduler =
        Scheduler::open_with_extensions(root.join("db"), file_options(parent, root), registry)
            .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["hooks".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until(|| entered.load(Ordering::SeqCst) == 1).await;
    scheduler.abort(id).unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(
        !scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content,..} if content == "after-kill"))
    );
    scheduler.shutdown().await.unwrap();
}
struct FileHook(std::path::PathBuf);
#[async_trait]
impl pi_rs_durable::extension::Hook for FileHook {
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &Value,
        context: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<Value>, String> {
        if call.name != "child" {
            return Ok(None);
        }
        use std::io::Write;
        let snapshot = serde_json::to_vec(context.snapshot).unwrap();
        let snapshot_path = self.0.join("hook-snapshot");
        if snapshot_path.exists() {
            assert_eq!(std::fs::read(snapshot_path).unwrap(), snapshot);
        } else {
            std::fs::write(snapshot_path, snapshot).unwrap();
        }
        writeln!(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("hook-attempts"))
                .unwrap(),
            "attempt"
        )
        .unwrap();
        if !self.0.join("release-hook").exists() {
            std::fs::write(self.0.join("hook-entered"), b"ready").unwrap();
            std::future::pending::<()>().await;
        }
        Ok(Some(Value::String("hook-finished".into())))
    }
}
#[tokio::test]
async fn nested_hook_kill_worker() {
    let Some(root) = std::env::var_os("PI_NESTED_HOOK_KILL_DIR") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    std::fs::write(root.join("finish"), b"ready").unwrap();
    let parent = Arc::new(KeyedToolAdapter::new(Parent(Arc::new(AtomicUsize::new(0)))));
    let registry = hook_registry(Arc::new(FileHook(root.clone())));
    let mut scheduler =
        Scheduler::open_with_extensions(root.join("db"), file_options(parent, &root), registry)
            .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["hooks".into()])
        .unwrap();
    std::fs::write(root.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn killed_nested_result_hook_replays_staged_result_without_child_redispatch() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut process = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "nested_hook_kill_worker"])
            .env("PI_NESTED_HOOK_KILL_DIR", root)
            .spawn()
            .unwrap(),
    );
    until(|| root.join("hook-entered").exists()).await;
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let id = std::fs::read_to_string(root.join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
    std::fs::write(root.join("release-hook"), b"ready").unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let parent = Arc::new(KeyedToolAdapter::new(Parent(calls.clone())));
    let registry = hook_registry(Arc::new(FileHook(root.into())));
    let mut scheduler =
        Scheduler::open_with_extensions(root.join("db"), file_options(parent, root), registry)
            .unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(
        std::fs::read_to_string(root.join("dispatches"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert_eq!(
        std::fs::read_to_string(root.join("hook-attempts"))
            .unwrap()
            .lines()
            .count(),
        2
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content,..} if content == "hook-finished"))
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn selected_extension_runs_hooks_for_every_automatically_polled_child() {
    use std::sync::atomic::AtomicBool;
    let dir = tempfile::tempdir().unwrap();
    let pc = Arc::new(AtomicUsize::new(0));
    let cc = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let parent = Arc::new(KeyedToolAdapter::new(MultiParent(pc.clone())));
    let child = Arc::new(AutoChild {
        invoked: cc.clone(),
        polls,
        ready_a: Arc::new(AtomicBool::new(true)),
        ready_b: Arc::new(AtomicBool::new(true)),
    });
    let hooks = Arc::new(std::sync::Mutex::new(Vec::new()));
    let registry = hook_registry(Arc::new(OrderedHooks {
        calls: hooks.clone(),
    }));
    let mut scheduler = Scheduler::open_with_extensions(
        dir.path().join("db"),
        multi_options(parent, child),
        registry,
    )
    .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["hooks".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(
        *hooks.lock().unwrap(),
        vec!["legacy", "rich", "legacy", "rich"]
    );
    assert_eq!(cc.load(Ordering::SeqCst), 2);
    assert_eq!(pc.load(Ordering::SeqCst), 2);
    assert!(scheduler.conversation(id).unwrap().messages.iter().any(
        |m| matches!(m,Message::Tool {content,..} if content == "rich:legacy:a,rich:legacy:b")
    ));
    scheduler.shutdown().await.unwrap();
}

struct PauseAfterChild(std::path::PathBuf);
#[async_trait]
impl KeyedTool for PauseAfterChild {
    fn spec(&self) -> ToolSpec {
        spec("parent")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        let result = host
            .call_keyed_unpolled("child-key", "child", json!({}))
            .await;
        if matches!(result, Outcome::Pending { .. }) {
            host.suspend_pending().unwrap();
            return Outcome::Succeeded("discarded".into()).into();
        }
        if !self.0.join("release-parent").exists() {
            std::fs::write(self.0.join("parent-after-child"), b"ready").unwrap();
            std::future::pending::<()>().await;
        }
        result.into()
    }
}
fn pause_parent_options(root: &std::path::Path) -> Options {
    let parent = Arc::new(KeyedToolAdapter::new(PauseAfterChild(root.into())));
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            parent.register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(FileChild(root.into())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
#[tokio::test]
async fn nested_after_hook_kill_worker() {
    let Some(root) = std::env::var_os("PI_NESTED_AFTER_HOOK_KILL_DIR") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    std::fs::write(root.join("finish"), b"ready").unwrap();
    std::fs::write(root.join("release-hook"), b"ready").unwrap();
    let mut scheduler = Scheduler::open_with_extensions(
        root.join("db"),
        pause_parent_options(&root),
        hook_registry(Arc::new(FileHook(root.clone()))),
    )
    .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["hooks".into()])
        .unwrap();
    std::fs::write(root.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn killed_parent_after_hook_does_not_repeat_completed_nested_hook() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut process = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "nested_after_hook_kill_worker"])
            .env("PI_NESTED_AFTER_HOOK_KILL_DIR", root)
            .spawn()
            .unwrap(),
    );
    until(|| root.join("parent-after-child").exists()).await;
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let id = std::fs::read_to_string(root.join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
    std::fs::write(root.join("release-parent"), b"ready").unwrap();
    let mut scheduler = Scheduler::open_with_extensions(
        root.join("db"),
        pause_parent_options(root),
        hook_registry(Arc::new(FileHook(root.into()))),
    )
    .unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(
        std::fs::read_to_string(root.join("dispatches"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert_eq!(
        std::fs::read_to_string(root.join("hook-attempts"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m,Message::Tool {content,..} if content == "hook-finished"))
    );
    scheduler.shutdown().await.unwrap();
}

struct OverrideParent(Arc<AtomicUsize>);
#[async_trait]
impl Tool for OverrideParent {
    fn spec(&self) -> ToolSpec {
        spec("parent")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        self.0.fetch_add(1, Ordering::SeqCst);
        Outcome::Succeeded("unexpected".into())
    }
}
#[tokio::test]
async fn selected_extension_override_fences_parent_replay() {
    use std::sync::atomic::AtomicBool;
    let dir = tempfile::tempdir().unwrap();
    let parent_calls = Arc::new(AtomicUsize::new(0));
    let child_calls = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let a = Arc::new(AtomicBool::new(false));
    let b = Arc::new(AtomicBool::new(false));
    let parent = Arc::new(KeyedToolAdapter::new(MultiParent(parent_calls.clone())));
    let child = Arc::new(AutoChild {
        invoked: child_calls,
        polls: polls.clone(),
        ready_a: a.clone(),
        ready_b: b.clone(),
    });
    let hooks = Arc::new(OrderedHooks {
        calls: Arc::new(std::sync::Mutex::new(Vec::new())),
    });
    let mut scheduler = Scheduler::open_with_extensions(
        dir.path().join("db"),
        multi_options(parent, child),
        hook_registry(hooks.clone()),
    )
    .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["hooks".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until(|| polls.load(Ordering::SeqCst) >= 2).await;
    let override_calls = Arc::new(AtomicUsize::new(0));
    scheduler
        .install_extension(pi_rs_durable::extension::Extension {
            name: "hooks".into(),
            tools: vec![Arc::new(OverrideParent(override_calls.clone()))],
            hooks: vec![hooks],
            ..Default::default()
        })
        .unwrap();
    a.store(true, Ordering::SeqCst);
    b.store(true, Ordering::SeqCst);
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_some());
    assert_eq!(parent_calls.load(Ordering::SeqCst), 1);
    assert_eq!(override_calls.load(Ordering::SeqCst), 0);
    scheduler.shutdown().await.unwrap();
}

struct NestedBlocks(std::path::PathBuf);
#[async_trait]
impl pi_rs_durable::extension::Hook for NestedBlocks {
    fn supports_runtime_tool_blocks(&self) -> bool {
        true
    }
    async fn after_tool_blocks(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &pi_rs_durable::extension::ToolBlockResult,
        nested: bool,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolBlockResult>, String> {
        if call.name != "child" {
            return Ok(None);
        }
        assert!(nested);
        use std::io::Write;
        writeln!(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("block-hooks"))
                .unwrap(),
            "hook"
        )
        .unwrap();
        if self.0.join("pause-block-hook").exists() && !self.0.join("release-block-hook").exists() {
            std::fs::write(self.0.join("block-hook-ready"), b"ready").unwrap();
            std::future::pending::<()>().await;
        }
        let mut result = result.clone();
        result.output = vec![
            json!({"type":"text","text":"before"}),
            json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
            json!({"type":"text","text":"after"}),
        ];
        Ok(Some(result))
    }
}
struct BlockParent(std::path::PathBuf);
#[async_trait]
impl KeyedTool for BlockParent {
    fn spec(&self) -> ToolSpec {
        spec("parent")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        let result = host
            .call_keyed_unpolled_with_blocks("child-key", "child", json!({}))
            .await;
        if matches!(result.outcome, Outcome::Pending { .. }) {
            host.suspend_pending().unwrap();
            return Outcome::Succeeded("discarded".into()).into();
        }
        assert_eq!(
            result.output_blocks,
            Some(vec![
                json!({"type":"text","text":"before"}),
                json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
                json!({"type":"text","text":"after"})
            ])
        );
        if !self.0.join("release-block-parent").exists() {
            std::fs::write(self.0.join("block-parent-ready"), b"ready").unwrap();
            std::future::pending::<()>().await;
        }
        Outcome::Succeeded("received ordered image".into()).into()
    }
}
fn block_parent_options(root: &std::path::Path) -> Options {
    let parent = Arc::new(KeyedToolAdapter::new(BlockParent(root.into())));
    file_options(parent, root)
}
#[tokio::test]
async fn nested_block_kill_worker() {
    let Some(root) = std::env::var_os("PI_NESTED_BLOCK_KILL_DIR") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    std::fs::write(root.join("finish"), b"ready").unwrap();
    let registry = hook_registry(Arc::new(NestedBlocks(root.clone())));
    let mut scheduler =
        Scheduler::open_with_extensions(root.join("db"), block_parent_options(&root), registry)
            .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["hooks".into()])
        .unwrap();
    std::fs::write(root.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn nested_image_blocks_survive_kill_and_reach_parent_in_order() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut process = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "nested_block_kill_worker"])
            .env("PI_NESTED_BLOCK_KILL_DIR", root)
            .spawn()
            .unwrap(),
    );
    until(|| root.join("block-parent-ready").exists()).await;
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let id = std::fs::read_to_string(root.join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
    std::fs::write(root.join("release-block-parent"), b"ready").unwrap();
    let mut scheduler = Scheduler::open_with_extensions(
        root.join("db"),
        block_parent_options(root),
        hook_registry(Arc::new(NestedBlocks(root.into()))),
    )
    .unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(
        std::fs::read_to_string(root.join("dispatches"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert_eq!(
        std::fs::read_to_string(root.join("block-hooks"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m,Message::Tool {content,..} if content == "received ordered image"))
    );
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn legacy_nested_caller_cannot_silently_drop_image_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(root.join("finish"), b"ready").unwrap();
    let parent = Arc::new(KeyedToolAdapter::new(Parent(Arc::new(AtomicUsize::new(0)))));
    let mut scheduler = Scheduler::open_with_extensions(
        root.join("db"),
        file_options(parent, root),
        hook_registry(Arc::new(NestedBlocks(root.into()))),
    )
    .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["hooks".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_some());
    assert!(
        !scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }))
    );
    scheduler.shutdown().await.unwrap();
}

struct ImageChild;
#[async_trait]
impl Tool for ImageChild {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Structured {
            data: json!({"blocks":[{"type":"text","text":"before"},{"type":"image","data":"aGk=","mimeType":"image/png"},{"type":"text","text":"after"}]}),
            attachments: vec![],
        }
    }
}
struct StructuredBlocks;
#[async_trait]
impl pi_rs_durable::extension::Hook for StructuredBlocks {
    fn supports_runtime_tool_blocks(&self) -> bool {
        true
    }
    async fn after_tool_blocks(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &pi_rs_durable::extension::ToolBlockResult,
        nested: bool,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolBlockResult>, String> {
        if call.name != "child" {
            return Ok(None);
        }
        assert!(nested);
        let mut result = result.clone();
        result.output = result.extra["structuredOutput"]["blocks"]
            .as_array()
            .unwrap()
            .clone();
        Ok(Some(result))
    }
}
#[tokio::test]
async fn nested_structured_image_is_exposed_as_ordered_blocks_to_parent() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(root.join("release-block-parent"), b"ready").unwrap();
    let parent = Arc::new(KeyedToolAdapter::new(BlockParent(root.into())));
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![
            parent.register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(ImageChild),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open_with_extensions(
        root.join("db"),
        options,
        hook_registry(Arc::new(StructuredBlocks)),
    )
    .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["hooks".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m,Message::Tool {content,..} if content == "received ordered image"))
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn killed_nested_block_hook_replays_without_redispatch() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(root.join("pause-block-hook"), b"ready").unwrap();
    let mut process = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "nested_block_kill_worker"])
            .env("PI_NESTED_BLOCK_KILL_DIR", root)
            .spawn()
            .unwrap(),
    );
    until(|| root.join("block-hook-ready").exists()).await;
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let id = std::fs::read_to_string(root.join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
    std::fs::write(root.join("release-block-hook"), b"ready").unwrap();
    std::fs::write(root.join("release-block-parent"), b"ready").unwrap();
    let mut scheduler = Scheduler::open_with_extensions(
        root.join("db"),
        block_parent_options(root),
        hook_registry(Arc::new(NestedBlocks(root.into()))),
    )
    .unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(
        std::fs::read_to_string(root.join("dispatches"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert_eq!(
        std::fs::read_to_string(root.join("block-hooks"))
            .unwrap()
            .lines()
            .count(),
        2
    );
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m,Message::Tool {content,..} if content == "received ordered image"))
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_during_nested_block_hook_does_not_publish_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(root.join("finish"), b"ready").unwrap();
    std::fs::write(root.join("pause-block-hook"), b"ready").unwrap();
    let mut scheduler = Scheduler::open_with_extensions(
        root.join("db"),
        block_parent_options(root),
        hook_registry(Arc::new(NestedBlocks(root.into()))),
    )
    .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["hooks".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until(|| root.join("block-hook-ready").exists()).await;
    scheduler.abort(id).unwrap();
    wait(&mut scheduler, id).await;
    assert!(!root.join("block-parent-ready").exists());
    assert!(scheduler.conversation(id).unwrap().abort_requested);
    assert!(
        !scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }))
    );
    scheduler.shutdown().await.unwrap();
}

struct InvalidBlocks;
#[async_trait]
impl pi_rs_durable::extension::Hook for InvalidBlocks {
    fn supports_runtime_tool_blocks(&self) -> bool {
        true
    }
    async fn after_tool_blocks(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &pi_rs_durable::extension::ToolBlockResult,
        _: bool,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolBlockResult>, String> {
        if call.name != "child" {
            return Ok(None);
        }
        let mut result = result.clone();
        result.output = vec![json!({"type":"image"})];
        Ok(Some(result))
    }
}
struct IgnoreBadBlocks;
#[async_trait]
impl KeyedTool for IgnoreBadBlocks {
    fn spec(&self) -> ToolSpec {
        spec("parent")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        let _ = host
            .call_keyed_with_blocks("child-key", "child", json!({}))
            .await;
        Outcome::Succeeded("ignored".into()).into()
    }
}
#[tokio::test]
async fn invalid_nested_block_hook_cannot_be_hidden_by_parent() {
    let dir = tempfile::tempdir().unwrap();
    let parent = Arc::new(KeyedToolAdapter::new(IgnoreBadBlocks));
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![
            parent.register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(ImageChild),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open_with_extensions(
        dir.path().join("db"),
        options,
        hook_registry(Arc::new(InvalidBlocks)),
    )
    .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["hooks".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_some());
    assert!(
        !scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m,Message::Tool {content,..} if content == "ignored"))
    );
    assert!(!scheduler.extension_reports().unwrap().is_empty());
    scheduler.shutdown().await.unwrap();
}

struct CompleteBlockChain(Arc<std::sync::Mutex<Vec<&'static str>>>);
#[async_trait]
impl pi_rs_durable::extension::Hook for CompleteBlockChain {
    fn supports_runtime_tool_blocks(&self) -> bool {
        true
    }
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &Value,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<Value>, String> {
        if call.name == "child" {
            self.0.lock().unwrap().push("legacy");
        }
        Ok(None)
    }
    async fn after_tool_result(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &pi_rs_durable::extension::ToolResult,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolResult>, String> {
        if call.name == "child" {
            self.0.lock().unwrap().push("rich");
        }
        Ok(None)
    }
    async fn after_tool_blocks(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &pi_rs_durable::extension::ToolBlockResult,
        nested: bool,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolBlockResult>, String> {
        if call.name != "child" {
            return Ok(None);
        }
        assert!(nested);
        self.0.lock().unwrap().push("blocks");
        let mut result = result.clone();
        result.output = result.extra["structuredOutput"]["blocks"]
            .as_array()
            .unwrap()
            .clone();
        Ok(Some(result))
    }
}
#[tokio::test]
async fn nested_result_hook_chain_runs_once_in_order_before_parent() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(root.join("release-block-parent"), b"ready").unwrap();
    let parent = Arc::new(KeyedToolAdapter::new(BlockParent(root.into())));
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![
            parent.register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(ImageChild),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut scheduler = Scheduler::open_with_extensions(
        root.join("db"),
        options,
        hook_registry(Arc::new(CompleteBlockChain(order.clone()))),
    )
    .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["hooks".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(*order.lock().unwrap(), vec!["legacy", "rich", "blocks"]);
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m,Message::Tool {content,..} if content == "received ordered image"))
    );
    scheduler.shutdown().await.unwrap();
}

struct CheckpointParent;
#[async_trait]
impl KeyedTool for CheckpointParent {
    fn spec(&self) -> ToolSpec {
        spec("parent")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        let wake = host.pending_wake();
        let call = match host
            .call_keyed_unpolled_with_id("child-key", "child", json!({}))
            .await
        {
            Ok(call) => call,
            Err(error) => return Outcome::Failed(error).into(),
        };
        match call.outcome {
            Outcome::Pending { .. } if wake.is_none() => {
                host.suspend_pending_with_checkpoint(
                    json!({"operation": call.operation_id, "value": 42}),
                )
                .unwrap();
                Outcome::Succeeded("discarded".into()).into()
            }
            Outcome::Succeeded(value) => {
                let Some(wake) = wake else {
                    return Outcome::Failed("missing checkpoint wake".into()).into();
                };
                if wake.checkpoint != json!({"operation": call.operation_id, "value": 42})
                    || wake.ended != vec![call.operation_id]
                {
                    return Outcome::Failed("checkpoint or ended operation changed".into()).into();
                }
                Outcome::Succeeded(format!("checkpoint:{value}")).into()
            }
            other => Outcome::Failed(format!("unexpected child: {other:?}")).into(),
        }
    }
}

#[tokio::test]
async fn checkpointed_nested_kill_worker() {
    let Some(root) = std::env::var_os("PI_NESTED_CHECKPOINT_KILL_DIR") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let parent = Arc::new(KeyedToolAdapter::new(CheckpointParent));
    let mut scheduler = Scheduler::open(root.join("db"), file_options(parent, &root)).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    std::fs::write(root.join("checkpoint-conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn checkpointed_nested_operation_survives_kill_and_wakes_parent() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut process = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "checkpointed_nested_kill_worker"])
            .env("PI_NESTED_CHECKPOINT_KILL_DIR", root)
            .spawn()
            .unwrap(),
    );
    until(|| root.join("poll-entered").exists()).await;
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let id = std::fs::read_to_string(root.join("checkpoint-conversation"))
        .unwrap()
        .parse()
        .unwrap();
    std::fs::write(root.join("finish"), b"done").unwrap();
    let parent = Arc::new(KeyedToolAdapter::new(CheckpointParent));
    let mut scheduler = Scheduler::open(root.join("db"), file_options(parent, root)).unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert_eq!(
        std::fs::read_to_string(root.join("dispatches"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert!(
        scheduler.conversation(id).unwrap().messages.iter().any(
            |m| matches!(m, Message::Tool {content, ..} if content == "checkpoint:after-kill")
        )
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_checkpointed_nested_poll_does_not_resume_parent_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let parent = Arc::new(KeyedToolAdapter::new(CheckpointParent));
    let mut scheduler =
        Scheduler::open(root.join("db"), file_options(parent.clone(), root)).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until(|| root.join("poll-entered").exists()).await;
    let operation = scheduler.blocked(id).unwrap().unwrap().0;
    scheduler.abort(id).unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(
        scheduler.nested_pending_jobs(id, operation).unwrap().len(),
        1
    );
    scheduler.shutdown().await.unwrap();
    std::fs::write(root.join("finish"), b"done").unwrap();
    let mut scheduler = Scheduler::open(root.join("db"), file_options(parent, root)).unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.conversation(id).unwrap().abort_requested);
    assert!(
        !scheduler.conversation(id).unwrap().messages.iter().any(
            |m| matches!(m, Message::Tool {content, ..} if content == "checkpoint:after-kill")
        )
    );
    assert_eq!(
        std::fs::read_to_string(root.join("dispatches"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    scheduler.shutdown().await.unwrap();
}

struct WakeCrashParent(std::path::PathBuf);
#[async_trait]
impl KeyedTool for WakeCrashParent {
    fn spec(&self) -> ToolSpec {
        spec("parent")
    }
    async fn invoke(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        if host.pending_wake().is_some() {
            std::fs::write(self.0.join("wake-entered"), b"ready").unwrap();
            std::future::pending::<()>().await;
        }
        CheckpointParent
            .invoke(arguments, context, state, host)
            .await
    }
}

#[tokio::test]
async fn checkpointed_nested_wake_kill_worker() {
    let Some(root) = std::env::var_os("PI_NESTED_WAKE_KILL_DIR") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let parent = Arc::new(KeyedToolAdapter::new(WakeCrashParent(root.clone())));
    let _scheduler = Scheduler::open(root.join("db"), file_options(parent, &root)).unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn checkpoint_wake_survives_process_death_during_parent_replay() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut first = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "checkpointed_nested_kill_worker"])
            .env("PI_NESTED_CHECKPOINT_KILL_DIR", root)
            .spawn()
            .unwrap(),
    );
    until(|| root.join("poll-entered").exists()).await;
    first.0.kill().unwrap();
    first.0.wait().unwrap();
    let id = std::fs::read_to_string(root.join("checkpoint-conversation"))
        .unwrap()
        .parse()
        .unwrap();
    std::fs::write(root.join("finish"), b"done").unwrap();
    let mut second = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "checkpointed_nested_wake_kill_worker"])
            .env("PI_NESTED_WAKE_KILL_DIR", root)
            .spawn()
            .unwrap(),
    );
    until(|| root.join("wake-entered").exists()).await;
    second.0.kill().unwrap();
    second.0.wait().unwrap();
    let parent = Arc::new(KeyedToolAdapter::new(CheckpointParent));
    let mut scheduler = Scheduler::open(root.join("db"), file_options(parent, root)).unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert_eq!(
        std::fs::read_to_string(root.join("dispatches"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert!(
        scheduler.conversation(id).unwrap().messages.iter().any(
            |m| matches!(m, Message::Tool {content, ..} if content == "checkpoint:after-kill")
        )
    );
    scheduler.shutdown().await.unwrap();
}

struct MultiCheckpointParent(Arc<AtomicUsize>);
#[async_trait]
impl KeyedTool for MultiCheckpointParent {
    fn spec(&self) -> ToolSpec {
        spec("parent")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        self.0.fetch_add(1, Ordering::SeqCst);
        let wake = host.pending_wake();
        let a = host
            .call_keyed_unpolled_with_id("a", "child", json!({"job":"a"}))
            .await
            .unwrap();
        let b = host
            .call_keyed_unpolled_with_id("b", "child", json!({"job":"b"}))
            .await
            .unwrap();
        if matches!(a.outcome, Outcome::Pending { .. })
            || matches!(b.outcome, Outcome::Pending { .. })
        {
            assert!(wake.is_none());
            host.suspend_pending_with_checkpoint(json!({"a":a.operation_id,"b":b.operation_id}))
                .unwrap();
            Outcome::Succeeded("park".into()).into()
        } else {
            let wake = wake.expect("checkpoint wake");
            assert_eq!(
                wake.checkpoint,
                json!({"a":a.operation_id,"b":b.operation_id})
            );
            assert_eq!(wake.ended, vec![a.operation_id, b.operation_id]);
            Outcome::Succeeded("both-ended".into()).into()
        }
    }
}

#[tokio::test]
async fn checkpoint_wake_lists_all_original_pending_operations() {
    use std::sync::atomic::AtomicBool;
    let dir = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let a = Arc::new(AtomicBool::new(false));
    let b = Arc::new(AtomicBool::new(false));
    let parent = Arc::new(KeyedToolAdapter::new(MultiCheckpointParent(calls.clone())));
    let child = Arc::new(AutoChild {
        invoked: invoked.clone(),
        polls: polls.clone(),
        ready_a: a.clone(),
        ready_b: b.clone(),
    });
    let mut scheduler =
        Scheduler::open(dir.path().join("db"), multi_options(parent, child)).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until(|| polls.load(Ordering::SeqCst) >= 2).await;
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    a.store(true, Ordering::SeqCst);
    until(|| {
        scheduler
            .nested_pending_jobs(id, root)
            .is_ok_and(|jobs| jobs.len() == 1)
    })
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    b.store(true, Ordering::SeqCst);
    wait(&mut scheduler, id).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(invoked.load(Ordering::SeqCst), 2);
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content, ..} if content == "both-ended"))
    );
    scheduler.shutdown().await.unwrap();
}
