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

struct WakeThenGateParent {
    root: std::path::PathBuf,
    stop_at_gate: bool,
}
#[async_trait]
impl KeyedTool for WakeThenGateParent {
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
        let remote = host
            .call_keyed_unpolled_with_id("remote-key", "remote", json!({}))
            .await
            .unwrap();
        if matches!(remote.outcome, Outcome::Pending { .. }) {
            assert!(wake.is_none());
            host.suspend_pending_with_checkpoint(
                json!({"saved": 42, "remote": remote.operation_id}),
            )
            .unwrap();
            std::fs::write(self.root.join("checkpoint-requested"), b"ready").unwrap();
            return Outcome::Succeeded("park".into()).into();
        }
        let Some(wake) = wake else {
            return Outcome::Failed("checkpoint wake lost after nested gate recovery".into())
                .into();
        };
        if wake.checkpoint != json!({"saved": 42, "remote": remote.operation_id})
            || wake.ended != vec![remote.operation_id]
        {
            return Outcome::Failed("checkpoint wake changed after nested gate recovery".into())
                .into();
        }
        match host
            .call_keyed_unpolled("gate-key", "gate_a", json!({}))
            .await
        {
            Outcome::Pending { .. } => {
                if self.stop_at_gate {
                    std::fs::write(self.root.join("gate-persisted-with-wake"), b"ready").unwrap();
                    std::future::pending::<()>().await;
                }
                host.suspend_pending().unwrap();
                Outcome::Succeeded("gate park".into()).into()
            }
            Outcome::Succeeded(value) if value == "gate_a" => {
                Outcome::Succeeded("checkpoint-and-gate-recovered".into()).into()
            }
            outcome => Outcome::Failed(format!("unexpected gate outcome: {outcome:?}")).into(),
        }
    }
}

fn wake_then_gate_options(root: &std::path::Path, stop_at_gate: bool) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(WakeThenGateParent {
                root: root.into(),
                stop_at_gate,
            }))
            .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(MultiGateTool {
                    root: root.into(),
                    name: "remote",
                }),
                replay: ReplayPolicy::Unsafe,
            },
            RegisteredTool {
                tool: Arc::new(MultiGateTool {
                    root: root.into(),
                    name: "gate_a",
                }),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 8,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}

#[tokio::test]
async fn checkpoint_wake_then_gate_kill_worker() {
    let Some(root) = std::env::var_os("PI_WAKE_THEN_GATE_KILL_DIR") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let mut scheduler = Scheduler::open_with_extensions(
        root.join("db"),
        wake_then_gate_options(&root, true),
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
    std::fs::write(root.join("wake-gate-conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn killed_after_checkpoint_wake_and_new_gate_retains_wake_on_reopen() {
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
            .args(["--exact", "checkpoint_wake_then_gate_kill_worker"])
            .env("PI_WAKE_THEN_GATE_KILL_DIR", root)
            .spawn()
            .unwrap(),
    );
    until(|| root.join("checkpoint-requested").exists()).await;
    std::fs::write(root.join("remote-ready"), b"ready").unwrap();
    until(|| root.join("gate-persisted-with-wake").exists()).await;
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    assert!(!root.join("gate_a-invocations").exists());
    let id = std::fs::read_to_string(root.join("wake-gate-conversation"))
        .unwrap()
        .parse()
        .unwrap();
    let mut scheduler = Scheduler::open_with_extensions(
        root.join("db"),
        wake_then_gate_options(root, false),
        hook_registry(Arc::new(MultiGateHook(root.into()))),
    )
    .unwrap();
    until(|| scheduler.conversation(id).unwrap().nested_suspended()).await;
    scheduler
        .send_signal(&pi_rs_durable::conversation_signal::Signal::new(
            uuid::Uuid::new_v4(),
            id,
            "gate_a".into(),
            json!("release"),
        ))
        .unwrap();
    scheduler.schedule(id);
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert!(scheduler.conversation(id).unwrap().messages.iter().any(
        |message| matches!(message, Message::Tool {content, ..} if content == "checkpoint-and-gate-recovered")
    ));
    for name in ["remote", "gate_a"] {
        assert_eq!(
            std::fs::read_to_string(root.join(format!("{name}-invocations")))
                .unwrap()
                .lines()
                .count(),
            1,
            "{name} redispatched"
        );
    }
    scheduler.shutdown().await.unwrap();
}

struct ParkedIntermediate(Arc<AtomicUsize>);
struct RotatingChild(Arc<AtomicUsize>);
#[async_trait]
impl Tool for RotatingChild {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        self.0.fetch_add(1, Ordering::SeqCst);
        Outcome::Pending {
            reference: "first".into(),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn poll(&self, reference: &str, _: ToolContext) -> Option<Outcome> {
        Some(match reference {
            "first" => Outcome::Pending {
                reference: "second".into(),
            },
            "second" => Outcome::Succeeded("a".into()),
            _ => Outcome::Failed("unexpected reference".into()),
        })
    }
}
#[async_trait]
impl KeyedTool for ParkedIntermediate {
    fn spec(&self) -> ToolSpec {
        spec("middle")
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
        let leaf = match host
            .call_keyed_unpolled_with_id("leaf", "child", json!({"job":"a"}))
            .await
        {
            Ok(leaf) => leaf,
            Err(error) => return Outcome::Failed(error).into(),
        };
        match (wake, leaf.outcome) {
            (None, Outcome::Pending { .. }) => {
                if let Err(error) = host
                    .suspend_pending_with_checkpoint(json!({"leaf":leaf.operation_id,"token":42}))
                {
                    return Outcome::Failed(error).into();
                }
                Outcome::Succeeded("park".into()).into()
            }
            (Some(wake), Outcome::Succeeded(value))
                if wake.checkpoint == json!({"leaf":leaf.operation_id,"token":42})
                    && wake.ended == vec![leaf.operation_id] =>
            {
                Outcome::Succeeded(format!("middle:{value}")).into()
            }
            (wake, result) => {
                Outcome::Failed(format!("unexpected middle: {wake:?} {result:?}")).into()
            }
        }
    }
}
struct ParkedRoot;
#[async_trait]
impl KeyedTool for ParkedRoot {
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
        let middle = match host
            .call_keyed_unpolled_with_id("middle", "middle", json!({}))
            .await
        {
            Ok(call) => call,
            Err(error) => return Outcome::Failed(error).into(),
        };
        match (wake, middle.outcome) {
            (None, Outcome::Pending { .. }) => {
                if let Err(error) =
                    host.suspend_pending_with_checkpoint(json!({"middle":middle.operation_id}))
                {
                    return Outcome::Failed(error).into();
                }
                Outcome::Succeeded("park".into()).into()
            }
            (Some(wake), Outcome::Succeeded(result))
                if wake.checkpoint == json!({"middle":middle.operation_id}) =>
            {
                Outcome::Succeeded(result).into()
            }
            (wake, outcome) => {
                Outcome::Failed(format!("unexpected root: {wake:?} {outcome:?}")).into()
            }
        }
    }
}
#[tokio::test]
async fn keyed_intermediate_parks_one_descendant_and_wakes_with_own_checkpoint() {
    use std::sync::atomic::AtomicBool;
    let dir = tempfile::tempdir().unwrap();
    let ready = Arc::new(AtomicBool::new(false));
    let child_calls = Arc::new(AtomicUsize::new(0));
    let middle_calls = Arc::new(AtomicUsize::new(0));
    let root = Arc::new(KeyedToolAdapter::new(ParkedRoot));
    let middle = Arc::new(KeyedToolAdapter::new(ParkedIntermediate(
        middle_calls.clone(),
    )));
    let child = Arc::new(AutoChild {
        invoked: child_calls.clone(),
        polls: Arc::new(AtomicUsize::new(0)),
        ready_a: ready.clone(),
        ready_b: ready.clone(),
    });
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![
            root.register(ReplayPolicy::Safe),
            middle.register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: child,
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until(|| {
        scheduler
            .conversation(id)
            .is_ok_and(|c| c.nested_suspended())
    })
    .await;
    assert_eq!(child_calls.load(Ordering::SeqCst), 1);
    scheduler.submit(id, None, "unrelated one".into()).unwrap();
    scheduler.submit(id, None, "unrelated two".into()).unwrap();
    ready.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(middle_calls.load(Ordering::SeqCst), 2);
    assert_eq!(child_calls.load(Ordering::SeqCst), 1);
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content, ..} if content == "middle:a"))
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn intermediate_wakes_after_descendant_rotates_pending_reference() {
    let dir = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let middle_calls = Arc::new(AtomicUsize::new(0));
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(ParkedRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(ParkedIntermediate(
                middle_calls.clone(),
            )))
            .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(RotatingChild(calls.clone())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(middle_calls.load(Ordering::SeqCst), 2);
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content, ..} if content == "middle:a"))
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn intermediate_park_rejects_public_resolution_and_leaf_wakes_after_reopen() {
    for cancel in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let child_calls = Arc::new(AtomicUsize::new(0));
        let middle_calls = Arc::new(AtomicUsize::new(0));
        let options = || Options {
            model: Arc::new(ModelStub),
            tools: vec![
                Arc::new(KeyedToolAdapter::new(ParkedRoot)).register(ReplayPolicy::Safe),
                Arc::new(KeyedToolAdapter::new(ParkedIntermediate(
                    middle_calls.clone(),
                )))
                .register(ReplayPolicy::Safe),
                RegisteredTool {
                    tool: Arc::new(Child(child_calls.clone())),
                    replay: ReplayPolicy::Unsafe,
                },
            ],
            restart_limit: 5,
            max_turns: 8,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        };
        let mut scheduler = Scheduler::open(&path, options()).unwrap();
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        wait(&mut scheduler, id).await;
        assert!(scheduler.conversation(id).unwrap().nested_suspended());
        let root = scheduler.blocked(id).unwrap().unwrap().0;
        let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
        assert_eq!(jobs.len(), 1);
        let leaf = &jobs[0];
        assert_eq!(leaf.reference, "external-job");

        // The public pending-job listing exposes only the remote leaf. A caller
        // that retained or guessed the internal marker must not resolve it.
        let store = durable_store::Store::open(&path).unwrap();
        let (key, marker) = store
            .list(format!("nested/{root}/").as_bytes())
            .unwrap()
            .into_iter()
            .find(|(_, entry)| {
                let value: Value = serde_json::from_slice(&entry.value).unwrap();
                !value["child_park"].is_null()
            })
            .expect("child park marker");
        let value: Value = serde_json::from_slice(&marker.value).unwrap();
        let intermediate = uuid::Uuid::parse_str(value["operation_id"].as_str().unwrap()).unwrap();
        let reference = format!(
            "nested-child-park:{}",
            value["child_park"]["suspension"].as_str().unwrap()
        );
        let result = if cancel {
            scheduler.cancel_nested_pending(id, root, intermediate, &reference, "cancelled".into())
        } else {
            scheduler.reconcile_nested_pending(
                id,
                root,
                intermediate,
                &reference,
                Outcome::Succeeded("forged".into()),
            )
        };
        let error = result.expect_err("internal marker must not be resolved");
        assert!(error.to_string().contains("resolve its pending descendant"));
        assert_eq!(store.get(&key).unwrap().unwrap().value, marker.value);
        scheduler.shutdown().await.unwrap();

        let mut scheduler = Scheduler::open(&path, options()).unwrap();
        wait(&mut scheduler, id).await;
        assert!(scheduler.conversation(id).unwrap().nested_suspended());
        scheduler
            .reconcile_nested_pending(
                id,
                root,
                leaf.operation_id,
                &leaf.reference,
                Outcome::Succeeded("leaf-result".into()),
            )
            .unwrap();
        wait(&mut scheduler, id).await;
        assert!(scheduler.blocked(id).unwrap().is_none());
        assert_eq!(child_calls.load(Ordering::SeqCst), 1);
        assert_eq!(middle_calls.load(Ordering::SeqCst), 2);
        assert!(
            scheduler.conversation(id).unwrap().messages.iter().any(
                |m| matches!(m, Message::Tool {content, ..} if content == "middle:leaf-result")
            )
        );
        scheduler.shutdown().await.unwrap();
    }
}

struct MultiParkLeaf(Arc<AtomicUsize>);
#[async_trait]
impl Tool for MultiParkLeaf {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
        self.0.fetch_add(1, Ordering::SeqCst);
        Outcome::Pending {
            reference: args["job"].as_str().unwrap().into(),
        }
    }
}

struct MultiParkFileLeaf(std::path::PathBuf);
#[async_trait]
impl Tool for MultiParkFileLeaf {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
        use std::io::Write;
        let name = args["job"].as_str().unwrap();
        writeln!(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("dispatches"))
                .unwrap(),
            "{name}"
        )
        .unwrap();
        Outcome::Pending {
            reference: name.into(),
        }
    }
}

struct MultiRotatingLeaf {
    calls: AtomicUsize,
    initial_polls: AtomicUsize,
    barrier: tokio::sync::Barrier,
}
#[async_trait]
impl Tool for MultiRotatingLeaf {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Outcome::Pending {
            reference: args["job"].as_str().unwrap().into(),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn poll(&self, reference: &str, _: ToolContext) -> Option<Outcome> {
        if reference == "a" || reference == "b" {
            if self.initial_polls.fetch_add(1, Ordering::SeqCst) < 2 {
                self.barrier.wait().await;
            }
            return Some(Outcome::Pending {
                reference: format!("{reference}-rotated"),
            });
        }
        Some(match reference {
            "a-rotated" => Outcome::Succeeded("first".into()),
            "b-rotated" => Outcome::Failed("cancelled".into()),
            _ => Outcome::Unknown("unexpected reference".into()),
        })
    }
}

#[tokio::test]
async fn concurrent_rotations_preserve_both_child_references_and_order() {
    let dir = tempfile::tempdir().unwrap();
    let leaf = Arc::new(MultiRotatingLeaf {
        calls: AtomicUsize::new(0),
        initial_polls: AtomicUsize::new(0),
        barrier: tokio::sync::Barrier::new(2),
    });
    let middle = Arc::new(AtomicUsize::new(0));
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(ParkedRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(MultiParkMiddle(middle.clone())))
                .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: leaf.clone(),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(20), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert_eq!(leaf.calls.load(Ordering::SeqCst), 2);
    assert_eq!(middle.load(Ordering::SeqCst), 2);
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content, ..} if content == "both resolved"))
    );
    scheduler.shutdown().await.unwrap();
}
fn multi_park_disk_options(path: &std::path::Path) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(ParkedRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(MultiParkMiddle(Arc::new(
                AtomicUsize::new(0),
            ))))
            .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(MultiParkFileLeaf(path.into())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}

#[tokio::test]
async fn multi_park_crash_worker() {
    let Some(path) = std::env::var_os("PI_MULTI_PARK_CRASH") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let mut scheduler = Scheduler::open(path.join("db"), multi_park_disk_options(&path)).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    std::fs::write(path.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
    let a = jobs.iter().find(|job| job.reference == "a").unwrap();
    scheduler
        .reconcile_nested_pending(
            id,
            root,
            a.operation_id,
            &a.reference,
            Outcome::Succeeded("first".into()),
        )
        .unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.conversation(id).unwrap().nested_suspended());
    std::fs::write(path.join("one-resolved"), b"ready").unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn multiple_child_jobs_recover_after_process_loss() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let mut process = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "multi_park_crash_worker"])
            .env("PI_MULTI_PARK_CRASH", path)
            .spawn()
            .unwrap(),
    );
    until(|| path.join("one-resolved").exists()).await;
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let id = std::fs::read_to_string(path.join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
    let mut scheduler = Scheduler::open(path.join("db"), multi_park_disk_options(path)).unwrap();
    wait(&mut scheduler, id).await;
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].reference, "b");
    scheduler
        .cancel_nested_pending(
            id,
            root,
            jobs[0].operation_id,
            &jobs[0].reference,
            "cancelled".into(),
        )
        .unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content, ..} if content == "both resolved"))
    );
    assert_eq!(
        std::fs::read_to_string(path.join("dispatches")).unwrap(),
        "a\nb\n"
    );
    scheduler.shutdown().await.unwrap();
}

struct MultiParkMiddle(Arc<AtomicUsize>);

struct AnyRoot;
#[async_trait]
impl KeyedTool for AnyRoot {
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
        let middle = host
            .call_keyed_unpolled_with_id("middle", "middle", json!({}))
            .await
            .unwrap();
        match middle.outcome {
            Outcome::Pending { .. } => {
                host.suspend_pending_with_checkpoint(json!({"middle": middle.operation_id}))
                    .unwrap();
                Outcome::Succeeded("park".into()).into()
            }
            other => other.into(),
        }
    }
}
struct AnyMiddle(Arc<std::sync::Mutex<Vec<Vec<uuid::Uuid>>>>);

struct ManyAnyMiddle(Arc<std::sync::Mutex<Vec<Vec<uuid::Uuid>>>>);
#[async_trait]
impl KeyedTool for ManyAnyMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        if let Some(wake) = host.pending_wake() {
            self.0.lock().unwrap().push(wake.ended);
        }
        let mut pending = false;
        for i in 0..6 {
            let key = format!("job{i}");
            let child = host
                .call_keyed_unpolled_with_id(&key, "child", json!({"job":key}))
                .await
                .unwrap();
            pending |= matches!(child.outcome, Outcome::Pending { .. });
        }
        if pending {
            host.suspend_pending_any_with_checkpoint(json!("many"))
                .unwrap();
            Outcome::Succeeded("park".into()).into()
        } else {
            Outcome::Succeeded("six complete".into()).into()
        }
    }
}

#[tokio::test]
async fn sequential_any_wakes_do_not_exhaust_restart_budget_across_reopen() {
    for limit in [1, 3] {
        check_sequential_any_wakes(limit).await;
    }
}

async fn check_sequential_any_wakes(limit: u32) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let wakes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let options = || Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(AnyRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(ManyAnyMiddle(wakes.clone())))
                .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(MultiParkLeaf(calls.clone())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: limit,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    for i in 0..6 {
        let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
        let job = jobs
            .iter()
            .find(|job| job.reference == format!("job{i}"))
            .unwrap();
        let expected = job.operation_id;
        scheduler
            .reconcile_nested_pending(
                id,
                root,
                expected,
                &job.reference,
                Outcome::Succeeded("done".into()),
            )
            .unwrap();
        wait(&mut scheduler, id).await;
        assert_eq!(
            wakes.lock().unwrap().last(),
            Some(&vec![expected]),
            "wake {i}"
        );
        if i < 5 {
            assert!(
                scheduler.conversation(id).unwrap().nested_suspended(),
                "wake {i}"
            );
            scheduler.shutdown().await.unwrap();
            scheduler = Scheduler::open(&path, options()).unwrap();
            wait(&mut scheduler, id).await;
        }
    }
    assert_eq!(wakes.lock().unwrap().len(), 6);
    assert_eq!(calls.load(Ordering::SeqCst), 6);
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m,Message::Tool {content,..} if content == "six complete"))
    );
    scheduler.shutdown().await.unwrap();
}
#[async_trait]
impl KeyedTool for AnyMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        let wake = host.pending_wake();
        let a = host
            .call_keyed_unpolled_with_id("a", "child", json!({"job":"a"}))
            .await
            .unwrap();
        let b = host
            .call_keyed_unpolled_with_id("b", "child", json!({"job":"b"}))
            .await
            .unwrap();
        if let Some(wake) = wake {
            self.0.lock().unwrap().push(wake.ended.clone());
            assert!(wake.checkpoint == json!("first") || wake.checkpoint == json!("again"));
        }
        if matches!(a.outcome, Outcome::Pending { .. })
            || matches!(b.outcome, Outcome::Pending { .. })
        {
            host.suspend_pending_any_with_checkpoint(if host.pending_wake().is_some() {
                json!("again")
            } else {
                json!("first")
            })
            .unwrap();
            Outcome::Succeeded("park".into()).into()
        } else {
            Outcome::Succeeded("any complete".into()).into()
        }
    }
}
fn any_options(
    wakes: Arc<std::sync::Mutex<Vec<Vec<uuid::Uuid>>>>,
    calls: Arc<AtomicUsize>,
) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(AnyRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(AnyMiddle(wakes))).register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(MultiParkLeaf(calls)),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 8,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}

#[tokio::test]
async fn any_child_wakes_early_and_reparks_remaining_job() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let wakes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut scheduler = Scheduler::open(&path, any_options(wakes.clone(), calls.clone())).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
    let a = jobs.iter().find(|job| job.reference == "a").unwrap();
    let b = jobs.iter().find(|job| job.reference == "b").unwrap();
    scheduler
        .reconcile_nested_pending(
            id,
            root,
            a.operation_id,
            &a.reference,
            Outcome::Succeeded("a done".into()),
        )
        .unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(*wakes.lock().unwrap(), vec![vec![a.operation_id]]);
    assert!(scheduler.conversation(id).unwrap().nested_suspended());
    assert_eq!(
        scheduler.nested_pending_jobs(id, root).unwrap(),
        vec![b.clone()]
    );
    scheduler.shutdown().await.unwrap();
    let mut scheduler = Scheduler::open(&path, any_options(wakes.clone(), calls.clone())).unwrap();
    scheduler
        .cancel_nested_pending(id, root, b.operation_id, &b.reference, "cancelled".into())
        .unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(
        *wakes.lock().unwrap(),
        vec![vec![a.operation_id], vec![b.operation_id]]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content, ..} if content == "any complete"))
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_after_any_wake_fences_reparked_child_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let wakes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut scheduler = Scheduler::open(&path, any_options(wakes.clone(), calls.clone())).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
    let a = jobs.iter().find(|job| job.reference == "a").unwrap();
    let b = jobs.iter().find(|job| job.reference == "b").unwrap();
    scheduler
        .reconcile_nested_pending(
            id,
            root,
            a.operation_id,
            &a.reference,
            Outcome::Succeeded("a".into()),
        )
        .unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(*wakes.lock().unwrap(), vec![vec![a.operation_id]]);
    scheduler.abort(id).unwrap();
    wait(&mut scheduler, id).await;
    scheduler.shutdown().await.unwrap();
    let mut scheduler = Scheduler::open(&path, any_options(wakes.clone(), calls.clone())).unwrap();
    scheduler
        .cancel_nested_pending(id, root, b.operation_id, &b.reference, "cancelled".into())
        .unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.conversation(id).unwrap().abort_requested);
    assert_eq!(*wakes.lock().unwrap(), vec![vec![a.operation_id]]);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    scheduler.shutdown().await.unwrap();
}

struct AnyHangingPoll {
    in_flight: Arc<AtomicUsize>,
    invoked: Arc<AtomicUsize>,
    release_a: Arc<std::sync::atomic::AtomicBool>,
}
struct PollGuard(Arc<AtomicUsize>);
impl Drop for PollGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl Tool for AnyHangingPoll {
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
        if reference == "a" {
            while !self.release_a.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            return Some(Outcome::Succeeded("done".into()));
        }
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        let _guard = PollGuard(self.in_flight.clone());
        std::future::pending().await
    }
}
struct AssertPollRetiredMiddle {
    active: Arc<AtomicUsize>,
    wakes: Arc<std::sync::Mutex<Vec<Vec<uuid::Uuid>>>>,
}
#[async_trait]
impl KeyedTool for AssertPollRetiredMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
    }
    async fn invoke(
        &self,
        args: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        if host.pending_wake().is_some() {
            assert_eq!(
                self.active.load(Ordering::SeqCst),
                0,
                "old poll still running during replay"
            );
        }
        AnyMiddle(self.wakes.clone())
            .invoke(args, context, state, host)
            .await
    }
}
#[tokio::test]
async fn any_wake_retires_other_inflight_poll_before_child_replay() {
    let dir = tempfile::tempdir().unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let wakes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let release_a = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(AnyRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(AssertPollRetiredMiddle {
                active: active.clone(),
                wakes: wakes.clone(),
            }))
            .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(AnyHangingPoll {
                    in_flight: active.clone(),
                    invoked: calls.clone(),
                    release_a: release_a.clone(),
                }),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 8,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until(|| active.load(Ordering::SeqCst) == 1).await;
    scheduler.schedule(id);
    release_a.store(true, Ordering::SeqCst);
    until(|| !wakes.lock().unwrap().is_empty()).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    scheduler.abort(id).unwrap();
    scheduler.shutdown().await.unwrap();
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn any_wake_freezes_all_results_already_committed() {
    let dir = tempfile::tempdir().unwrap();
    let wakes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut scheduler = Scheduler::open(
        dir.path().join("db"),
        any_options(wakes.clone(), calls.clone()),
    )
    .unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
    let a = jobs.iter().find(|job| job.reference == "a").unwrap();
    let b = jobs.iter().find(|job| job.reference == "b").unwrap();
    // Both synchronous resolutions commit before this task yields to the worker.
    scheduler
        .reconcile_nested_pending(
            id,
            root,
            a.operation_id,
            &a.reference,
            Outcome::Succeeded("a".into()),
        )
        .unwrap();
    scheduler
        .reconcile_nested_pending(
            id,
            root,
            b.operation_id,
            &b.reference,
            Outcome::Succeeded("b".into()),
        )
        .unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(
        *wakes.lock().unwrap(),
        vec![vec![a.operation_id, b.operation_id]]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(scheduler.blocked(id).unwrap().is_none());
    scheduler.shutdown().await.unwrap();
}

struct AnyDiskRoot(std::path::PathBuf);
#[async_trait]
impl KeyedTool for AnyDiskRoot {
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
        let waking = host.pending_wake().is_some();
        let middle = host
            .call_keyed_unpolled_with_id("middle", "middle", json!({}))
            .await
            .unwrap();
        match middle.outcome {
            Outcome::Pending { .. } => {
                if waking && self.0.join("stop-repark").exists() {
                    std::fs::write(self.0.join("at-repark"), b"ready").unwrap();
                    std::future::pending::<()>().await;
                }
                host.suspend_pending_with_checkpoint(json!({"middle":middle.operation_id}))
                    .unwrap();
                Outcome::Succeeded("park".into()).into()
            }
            other => other.into(),
        }
    }
}
struct AnyDiskMiddle(std::path::PathBuf);
#[async_trait]
impl KeyedTool for AnyDiskMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
    }
    async fn invoke(
        &self,
        args: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        if let Some(wake) = host.pending_wake() {
            use std::io::Write;
            writeln!(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.0.join("wakes"))
                    .unwrap(),
                "{}",
                serde_json::to_string(&wake.ended).unwrap()
            )
            .unwrap();
            if self.0.join("stop-wake").exists() {
                std::fs::write(self.0.join("at-wake"), b"ready").unwrap();
                std::future::pending::<()>().await;
            }
        }
        AnyMiddle(Arc::new(std::sync::Mutex::new(Vec::new())))
            .invoke(args, context, state, host)
            .await
    }
}
fn any_disk_options(path: &std::path::Path) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(AnyDiskRoot(path.into()))).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(AnyDiskMiddle(path.into())))
                .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(MultiParkFileLeaf(path.into())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: if path.join("low-restart-limit").exists() {
            3
        } else {
            8
        },
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}

#[tokio::test]
async fn any_park_crash_worker() {
    let Some(path) = std::env::var_os("PI_ANY_PARK_CRASH") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let mut scheduler = Scheduler::open(path.join("db"), any_disk_options(&path)).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    std::fs::write(path.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
    let a = jobs.iter().find(|job| job.reference == "a").unwrap();
    scheduler
        .reconcile_nested_pending(
            id,
            root,
            a.operation_id,
            &a.reference,
            Outcome::Succeeded("a".into()),
        )
        .unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn any_park_reopen_crash_worker() {
    let Some(path) = std::env::var_os("PI_ANY_PARK_REOPEN_CRASH") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let _scheduler = Scheduler::open(path.join("db"), any_disk_options(&path)).unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn interrupted_any_wake_still_exhausts_restart_budget() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    std::fs::write(path.join("low-restart-limit"), b"yes").unwrap();
    std::fs::write(path.join("stop-wake"), b"yes").unwrap();
    for attempt in 0..3 {
        let marker = path.join("at-wake");
        let _ = std::fs::remove_file(&marker);
        let (worker, env) = if attempt == 0 {
            ("any_park_crash_worker", "PI_ANY_PARK_CRASH")
        } else {
            ("any_park_reopen_crash_worker", "PI_ANY_PARK_REOPEN_CRASH")
        };
        let mut process = Kill(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", worker])
                .env(env, path)
                .spawn()
                .unwrap(),
        );
        until(|| marker.exists()).await;
        process.0.kill().unwrap();
        process.0.wait().unwrap();
    }
    std::fs::remove_file(path.join("stop-wake")).unwrap();
    let id = std::fs::read_to_string(path.join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
    let mut scheduler = Scheduler::open(path.join("db"), any_disk_options(path)).unwrap();
    wait(&mut scheduler, id).await;
    let state = scheduler.conversation(id).unwrap();
    assert!(state.messages.iter().any(|message| matches!(message,Message::Tool {content,..} if content.contains("restart limit"))), "{state:?}");
    assert_eq!(
        std::fs::read_to_string(path.join("wakes"))
            .unwrap()
            .lines()
            .count(),
        3
    );
    assert_eq!(
        std::fs::read_to_string(path.join("dispatches")).unwrap(),
        "a\nb\n"
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn any_wake_and_repark_recover_after_process_loss() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for stage in ["wake", "repark"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        std::fs::write(path.join(format!("stop-{stage}")), b"yes").unwrap();
        let mut process = Kill(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "any_park_crash_worker"])
                .env("PI_ANY_PARK_CRASH", path)
                .spawn()
                .unwrap(),
        );
        until(|| path.join(format!("at-{stage}")).exists()).await;
        process.0.kill().unwrap();
        process.0.wait().unwrap();
        std::fs::remove_file(path.join(format!("stop-{stage}"))).unwrap();
        let id = std::fs::read_to_string(path.join("conversation"))
            .unwrap()
            .parse()
            .unwrap();
        let mut scheduler = Scheduler::open(path.join("db"), any_disk_options(path)).unwrap();
        wait(&mut scheduler, id).await;
        assert!(
            scheduler.conversation(id).unwrap().nested_suspended(),
            "{stage}"
        );
        let store = durable_store::Store::open(path.join("db")).unwrap();
        let persisted = store
            .get(format!("runtime/conversation/{id}").as_bytes())
            .unwrap()
            .unwrap();
        let state: Value = serde_json::from_slice(&persisted.value).unwrap();
        assert_eq!(
            state["nested_resume"]["restarts"],
            json!(0),
            "repark should reset the crash budget: {stage}"
        );
        let root = scheduler.blocked(id).unwrap().unwrap().0;
        let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
        assert_eq!(jobs.len(), 1, "{stage}");
        assert_eq!(jobs[0].reference, "b");
        scheduler
            .cancel_nested_pending(
                id,
                root,
                jobs[0].operation_id,
                &jobs[0].reference,
                "cancelled".into(),
            )
            .unwrap();
        wait(&mut scheduler, id).await;
        assert!(
            scheduler.blocked(id).unwrap().is_none(),
            "{stage}: {:?}",
            scheduler.conversation(id).unwrap()
        );
        assert!(
            scheduler
                .conversation(id)
                .unwrap()
                .messages
                .iter()
                .any(|m| matches!(m,Message::Tool{content,..} if content == "any complete"))
        );
        assert_eq!(
            std::fs::read_to_string(path.join("dispatches")).unwrap(),
            "a\nb\n"
        );
        let wake_log = std::fs::read_to_string(path.join("wakes")).unwrap();
        let wake_log: Vec<Vec<uuid::Uuid>> = wake_log
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        if stage == "wake" {
            assert_eq!(wake_log.len(), 3);
            assert_eq!(wake_log[0], wake_log[1], "frozen wake changed after crash");
            assert_ne!(wake_log[1], wake_log[2]);
        } else {
            assert_eq!(wake_log.len(), 2);
            assert_ne!(wake_log[0], wake_log[1]);
        }
        scheduler.shutdown().await.unwrap();
    }
}

struct ImmediateMixedLeaf(Arc<AtomicUsize>);
#[async_trait]
impl Tool for ImmediateMixedLeaf {
    fn spec(&self) -> ToolSpec {
        spec("immediate")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        self.0.fetch_add(1, Ordering::SeqCst);
        Outcome::Succeeded("already done".into())
    }
}
struct MixedParkMiddle(Arc<AtomicUsize>);
#[async_trait]
impl KeyedTool for MixedParkMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
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
        let done = host
            .call_keyed_unpolled_with_id("done", "immediate", json!({}))
            .await
            .unwrap();
        let pending = host
            .call_keyed_unpolled_with_id("pending", "child", json!({"job":"pending"}))
            .await
            .unwrap();
        assert_eq!(done.outcome, Outcome::Succeeded("already done".into()));
        match (wake, pending.outcome) {
            (None, Outcome::Pending { .. }) => {
                if let Err(error) =
                    host.suspend_pending_with_checkpoint(json!({"saved":pending.operation_id}))
                {
                    return Outcome::Failed(error).into();
                }
                Outcome::Succeeded("park".into()).into()
            }
            (Some(wake), Outcome::Succeeded(value)) => {
                assert_eq!(wake.checkpoint, json!({"saved":pending.operation_id}));
                assert_eq!(wake.ended, vec![pending.operation_id]);
                Outcome::Succeeded(format!("already done:{value}")).into()
            }
            (wake, outcome) => Outcome::Failed(format!("unexpected: {wake:?} {outcome:?}")).into(),
        }
    }
}

#[tokio::test]
async fn completed_and_pending_keyed_children_park_and_reopen_without_redispatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let immediate = Arc::new(AtomicUsize::new(0));
    let pending = Arc::new(AtomicUsize::new(0));
    let middle = Arc::new(AtomicUsize::new(0));
    let options = || Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(ParkedRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(MixedParkMiddle(middle.clone())))
                .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(ImmediateMixedLeaf(immediate.clone())),
                replay: ReplayPolicy::Unsafe,
            },
            RegisteredTool {
                tool: Arc::new(MultiParkLeaf(pending.clone())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.conversation(id).unwrap().nested_suspended());
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].reference, "pending");
    scheduler.shutdown().await.unwrap();
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    scheduler
        .reconcile_nested_pending(
            id,
            root,
            jobs[0].operation_id,
            &jobs[0].reference,
            Outcome::Succeeded("resolved".into()),
        )
        .unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert_eq!(immediate.load(Ordering::SeqCst), 1);
    assert_eq!(pending.load(Ordering::SeqCst), 1);
    assert_eq!(middle.load(Ordering::SeqCst), 2);
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m,Message::Tool{content,..} if content == "already done:resolved"))
    );
    scheduler.shutdown().await.unwrap();
}

struct FileImmediateMixedLeaf(std::path::PathBuf);
#[async_trait]
impl Tool for FileImmediateMixedLeaf {
    fn spec(&self) -> ToolSpec {
        spec("immediate")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        use std::io::Write;
        writeln!(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("immediate-dispatches"))
                .unwrap(),
            "done"
        )
        .unwrap();
        Outcome::Succeeded("already done".into())
    }
}
struct MixedDiskMiddle(std::path::PathBuf);
#[async_trait]
impl KeyedTool for MixedDiskMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
    }
    async fn invoke(
        &self,
        args: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        if host.pending_wake().is_some() && self.0.join("stop-wake").exists() {
            std::fs::write(self.0.join("mixed-wake"), b"ready").unwrap();
            std::future::pending::<()>().await;
        }
        MixedParkMiddle(Arc::new(AtomicUsize::new(0)))
            .invoke(args, context, state, host)
            .await
    }
}
fn mixed_park_disk_options(path: &std::path::Path) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(ChildParkDiskRoot(path.into())))
                .register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(MixedDiskMiddle(path.into())))
                .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(FileImmediateMixedLeaf(path.into())),
                replay: ReplayPolicy::Unsafe,
            },
            RegisteredTool {
                tool: Arc::new(MultiParkFileLeaf(path.into())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 8,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}

#[tokio::test]
async fn mixed_park_crash_worker() {
    let Some(path) = std::env::var_os("PI_MIXED_PARK_CRASH") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let mut scheduler = Scheduler::open(path.join("db"), mixed_park_disk_options(&path)).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    std::fs::write(path.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    if path.join("stop-before").exists() {
        std::future::pending::<()>().await;
    }
    wait(&mut scheduler, id).await;
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
    scheduler
        .reconcile_nested_pending(
            id,
            root,
            jobs[0].operation_id,
            &jobs[0].reference,
            Outcome::Succeeded("resolved".into()),
        )
        .unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn completed_child_is_not_redispatched_after_park_boundary_crashes() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for stage in ["before", "wake"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        std::fs::write(path.join(format!("stop-{stage}")), b"yes").unwrap();
        let mut process = Kill(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "mixed_park_crash_worker"])
                .env("PI_MIXED_PARK_CRASH", path)
                .spawn()
                .unwrap(),
        );
        let marker = if stage == "before" {
            "before-park"
        } else {
            "mixed-wake"
        };
        until(|| path.join(marker).exists()).await;
        process.0.kill().unwrap();
        process.0.wait().unwrap();
        std::fs::remove_file(path.join(format!("stop-{stage}"))).unwrap();
        let id = std::fs::read_to_string(path.join("conversation"))
            .unwrap()
            .parse()
            .unwrap();
        let mut scheduler =
            Scheduler::open(path.join("db"), mixed_park_disk_options(path)).unwrap();
        if stage == "before" {
            wait(&mut scheduler, id).await;
            assert!(scheduler.conversation(id).unwrap().nested_suspended());
            let root = scheduler.blocked(id).unwrap().unwrap().0;
            let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
            assert_eq!(jobs.len(), 1);
            scheduler
                .reconcile_nested_pending(
                    id,
                    root,
                    jobs[0].operation_id,
                    &jobs[0].reference,
                    Outcome::Succeeded("resolved".into()),
                )
                .unwrap();
        }
        wait(&mut scheduler, id).await;
        assert!(
            scheduler.blocked(id).unwrap().is_none(),
            "{stage}: {:?}",
            scheduler.conversation(id).unwrap()
        );
        assert!(
            scheduler.conversation(id).unwrap().messages.iter().any(
                |m| matches!(m,Message::Tool{content,..} if content == "already done:resolved")
            )
        );
        assert_eq!(
            std::fs::read_to_string(path.join("immediate-dispatches")).unwrap(),
            "done\n"
        );
        assert_eq!(
            std::fs::read_to_string(path.join("dispatches")).unwrap(),
            "pending\n"
        );
        scheduler.shutdown().await.unwrap();
    }
}

struct MixedParkRaceMiddle {
    requested: Arc<std::sync::atomic::AtomicBool>,
    release: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait]
impl KeyedTool for MixedParkRaceMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        assert!(matches!(
            host.call_keyed_unpolled("done", "immediate", json!({}))
                .await,
            Outcome::Succeeded(_)
        ));
        assert!(matches!(
            host.call_keyed_unpolled("pending", "child", json!({"job":"pending"}))
                .await,
            Outcome::Pending { .. }
        ));
        host.suspend_pending_with_checkpoint(json!("checkpoint"))
            .unwrap();
        self.requested.store(true, Ordering::SeqCst);
        while !self.release.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Outcome::Succeeded("park".into()).into()
    }
}
#[tokio::test]
async fn completed_child_version_change_fences_mixed_park_commit() {
    use durable_store::{Check, Expected, Mutation, Store};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let requested = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let immediate = Arc::new(AtomicUsize::new(0));
    let pending = Arc::new(AtomicUsize::new(0));
    let options = || Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(ParkedRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(MixedParkRaceMiddle {
                requested: requested.clone(),
                release: release.clone(),
            }))
            .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(ImmediateMixedLeaf(immediate.clone())),
                replay: ReplayPolicy::Unsafe,
            },
            RegisteredTool {
                tool: Arc::new(MultiParkLeaf(pending.clone())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until(|| requested.load(Ordering::SeqCst)).await;
    let store = Store::open(&path).unwrap();
    let (key, entry) = store
        .list(b"nested/")
        .unwrap()
        .into_iter()
        .find(|(_, entry)| {
            let value: Value = serde_json::from_slice(&entry.value).unwrap();
            value["name"] == "immediate"
        })
        .unwrap();
    // Rewriting the same value still changes its version (ABA).
    store
        .commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[Mutation::Put {
                key,
                value: entry.value,
            }],
        )
        .unwrap();
    release.store(true, Ordering::SeqCst);
    wait(&mut scheduler, id).await;
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    assert!(!scheduler.conversation(id).unwrap().nested_suspended());
    for (_, entry) in store.list(format!("nested/{root}/").as_bytes()).unwrap() {
        let value: Value = serde_json::from_slice(&entry.value).unwrap();
        assert!(value["child_park"].is_null(), "stale park committed");
    }
    scheduler.shutdown().await.unwrap();
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_some());
    assert_eq!(immediate.load(Ordering::SeqCst), 1);
    assert_eq!(pending.load(Ordering::SeqCst), 1);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_during_mixed_park_does_not_redispatch_completed_child() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let requested = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let immediate = Arc::new(AtomicUsize::new(0));
    let pending = Arc::new(AtomicUsize::new(0));
    let options = || Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(ParkedRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(MixedParkRaceMiddle {
                requested: requested.clone(),
                release: release.clone(),
            }))
            .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(ImmediateMixedLeaf(immediate.clone())),
                replay: ReplayPolicy::Unsafe,
            },
            RegisteredTool {
                tool: Arc::new(MultiParkLeaf(pending.clone())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until(|| requested.load(Ordering::SeqCst)).await;
    scheduler.abort(id).unwrap();
    release.store(true, Ordering::SeqCst);
    wait(&mut scheduler, id).await;
    assert!(scheduler.conversation(id).unwrap().abort_requested);
    scheduler.shutdown().await.unwrap();
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.conversation(id).unwrap().abort_requested);
    assert_eq!(immediate.load(Ordering::SeqCst), 1);
    assert_eq!(pending.load(Ordering::SeqCst), 1);
    scheduler.shutdown().await.unwrap();
}
struct OverLimitMiddle(Arc<std::sync::Mutex<Option<String>>>);
#[async_trait]
impl KeyedTool for OverLimitMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        for i in 0..9 {
            let call = host
                .call_keyed_unpolled_with_id(
                    &format!("key{i}"),
                    "child",
                    json!({"job":format!("{i}")}),
                )
                .await
                .unwrap();
            assert!(matches!(call.outcome, Outcome::Pending { .. }));
        }
        let error = host.suspend_pending_with_checkpoint(json!({})).unwrap_err();
        *self.0.lock().unwrap() = Some(error);
        Outcome::Failed("over limit".into()).into()
    }
}

#[tokio::test]
async fn intermediate_rejects_more_than_eight_pending_descendants() {
    let dir = tempfile::tempdir().unwrap();
    let error = Arc::new(std::sync::Mutex::new(None));
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(ParkedRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(OverLimitMiddle(error.clone())))
                .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(MultiParkLeaf(Arc::new(AtomicUsize::new(0)))),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    assert!(
        error
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .contains("one to eight")
    );
    assert!(scheduler.blocked(id).unwrap().is_some());
    scheduler.shutdown().await.unwrap();
}

#[async_trait]
impl KeyedTool for MultiParkMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
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
        let duplicate = host
            .call_keyed_unpolled_with_id("a", "child", json!({"job":"a"}))
            .await
            .unwrap();
        assert_eq!(a.operation_id, duplicate.operation_id);
        let b = host
            .call_keyed_unpolled_with_id("b", "child", json!({"job":"b"}))
            .await
            .unwrap();
        match wake {
            None => {
                assert!(matches!(a.outcome, Outcome::Pending { .. }));
                assert!(matches!(b.outcome, Outcome::Pending { .. }));
                host.suspend_pending_with_checkpoint(
                    json!({"a":a.operation_id,"b":b.operation_id}),
                )
                .unwrap();
                Outcome::Succeeded("park".into()).into()
            }
            Some(wake) => {
                assert_eq!(
                    wake.checkpoint,
                    json!({"a":a.operation_id,"b":b.operation_id})
                );
                assert_eq!(wake.ended, vec![a.operation_id, b.operation_id]);
                assert_eq!(a.outcome, Outcome::Succeeded("first".into()));
                assert_eq!(b.outcome, Outcome::Failed("cancelled".into()));
                Outcome::Succeeded("both resolved".into()).into()
            }
        }
    }
}

#[tokio::test]
async fn multiple_child_jobs_wait_for_all_and_resume_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let leaf_calls = Arc::new(AtomicUsize::new(0));
    let middle_calls = Arc::new(AtomicUsize::new(0));
    let options = || Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(ParkedRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(MultiParkMiddle(middle_calls.clone())))
                .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(MultiParkLeaf(leaf_calls.clone())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.conversation(id).unwrap().nested_suspended());
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
    assert_eq!(jobs.len(), 2);
    let a = jobs.iter().find(|job| job.reference == "a").unwrap();
    let b = jobs.iter().find(|job| job.reference == "b").unwrap();
    scheduler
        .reconcile_nested_pending(
            id,
            root,
            a.operation_id,
            &a.reference,
            Outcome::Succeeded("first".into()),
        )
        .unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.conversation(id).unwrap().nested_suspended());
    assert_eq!(middle_calls.load(Ordering::SeqCst), 1);
    scheduler.shutdown().await.unwrap();
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    wait(&mut scheduler, id).await;
    assert_eq!(middle_calls.load(Ordering::SeqCst), 1);
    scheduler
        .cancel_nested_pending(id, root, b.operation_id, &b.reference, "cancelled".into())
        .unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert_eq!(middle_calls.load(Ordering::SeqCst), 2);
    assert_eq!(leaf_calls.load(Ordering::SeqCst), 2);
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content, ..} if content == "both resolved"))
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_with_multiple_child_jobs_fences_replay_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let leaf_calls = Arc::new(AtomicUsize::new(0));
    let middle_calls = Arc::new(AtomicUsize::new(0));
    let options = || Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(ParkedRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(MultiParkMiddle(middle_calls.clone())))
                .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(MultiParkLeaf(leaf_calls.clone())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
    assert_eq!(jobs.len(), 2);
    scheduler.abort(id).unwrap();
    wait(&mut scheduler, id).await;
    scheduler.shutdown().await.unwrap();
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    for job in &jobs {
        scheduler
            .cancel_nested_pending(
                id,
                root,
                job.operation_id,
                &job.reference,
                "cancelled".into(),
            )
            .unwrap();
    }
    wait(&mut scheduler, id).await;
    assert!(scheduler.conversation(id).unwrap().abort_requested);
    assert_eq!(middle_calls.load(Ordering::SeqCst), 1);
    assert_eq!(leaf_calls.load(Ordering::SeqCst), 2);
    scheduler.shutdown().await.unwrap();
}

struct ChildParkDiskRoot(std::path::PathBuf);
#[async_trait]
impl KeyedTool for ChildParkDiskRoot {
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
        let middle = match host
            .call_keyed_unpolled_with_id("middle", "middle", json!({}))
            .await
        {
            Ok(call) => call,
            Err(error) => return Outcome::Failed(error).into(),
        };
        match (wake, middle.outcome) {
            (None, Outcome::Pending { .. }) => {
                if self.0.join("stop-before").exists() {
                    std::fs::write(self.0.join("before-park"), b"ready").unwrap();
                    std::future::pending::<()>().await;
                }
                if let Err(error) =
                    host.suspend_pending_with_checkpoint(json!({"middle":middle.operation_id}))
                {
                    return Outcome::Failed(error).into();
                }
                Outcome::Succeeded("park".into()).into()
            }
            (Some(wake), Outcome::Succeeded(result))
                if wake.checkpoint == json!({"middle":middle.operation_id}) =>
            {
                Outcome::Succeeded(result).into()
            }
            (wake, outcome) => {
                Outcome::Failed(format!("unexpected root: {wake:?} {outcome:?}")).into()
            }
        }
    }
}
struct ChildParkDiskMiddle(std::path::PathBuf);
#[async_trait]
impl KeyedTool for ChildParkDiskMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
    }
    async fn invoke(
        &self,
        args: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        if host.pending_wake().is_some() && self.0.join("stop-wake").exists() {
            std::fs::write(self.0.join("child-wake"), b"ready").unwrap();
            std::future::pending::<()>().await;
        }
        ParkedIntermediate(Arc::new(AtomicUsize::new(0)))
            .invoke(args, context, state, host)
            .await
    }
}
fn child_park_disk_options(root: &std::path::Path) -> Options {
    let parent = Arc::new(KeyedToolAdapter::new(ChildParkDiskRoot(root.into())));
    let middle = Arc::new(KeyedToolAdapter::new(ChildParkDiskMiddle(root.into())));
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            parent.register(ReplayPolicy::Safe),
            middle.register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(FileChild(root.into())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 8,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
#[tokio::test]
async fn child_park_crash_worker() {
    let Some(root) = std::env::var_os("PI_CHILD_PARK_CRASH") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let mut scheduler = Scheduler::open(root.join("db"), child_park_disk_options(&root)).unwrap();
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
async fn child_park_recovers_before_after_and_during_wake() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for stage in ["before", "parked", "wake"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        if stage == "before" {
            std::fs::write(root.join("stop-before"), b"yes").unwrap();
        }
        if stage == "wake" {
            std::fs::write(root.join("stop-wake"), b"yes").unwrap();
            std::fs::write(root.join("finish"), b"yes").unwrap();
        }
        let mut process = Kill(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "child_park_crash_worker"])
                .env("PI_CHILD_PARK_CRASH", root)
                .spawn()
                .unwrap(),
        );
        let marker = match stage {
            "before" => "before-park",
            "parked" => "poll-entered",
            _ => "child-wake",
        };
        until(|| root.join(marker).exists()).await;
        process.0.kill().unwrap();
        process.0.wait().unwrap();
        let id = std::fs::read_to_string(root.join("conversation"))
            .unwrap()
            .parse()
            .unwrap();
        let _ = std::fs::remove_file(root.join("stop-before"));
        let _ = std::fs::remove_file(root.join("stop-wake"));
        std::fs::write(root.join("finish"), b"yes").unwrap();
        let mut scheduler =
            Scheduler::open(root.join("db"), child_park_disk_options(root)).unwrap();
        tokio::time::timeout(Duration::from_secs(20), scheduler.wait(id))
            .await
            .unwrap()
            .unwrap();
        assert!(
            scheduler
                .conversation(id)
                .unwrap()
                .messages
                .iter()
                .any(|m| matches!(m, Message::Tool {content,..} if content == "middle:after-kill")),
            "stage={stage}, state={:?}",
            scheduler.conversation(id).unwrap()
        );
        assert_eq!(
            std::fs::read_to_string(root.join("dispatches"))
                .unwrap()
                .lines()
                .count(),
            1,
            "{stage}"
        );
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn abort_while_intermediate_parked_prevents_child_wake_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut scheduler = Scheduler::open(root.join("db"), child_park_disk_options(root)).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until(|| root.join("poll-entered").exists()).await;
    scheduler.abort(id).unwrap();
    tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    scheduler.shutdown().await.unwrap();
    std::fs::write(root.join("finish"), b"yes").unwrap();
    let mut reopened = Scheduler::open(root.join("db"), child_park_disk_options(root)).unwrap();
    tokio::time::timeout(Duration::from_secs(15), reopened.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert!(reopened.conversation(id).unwrap().abort_requested);
    assert!(!root.join("child-wake").exists());
    assert_eq!(
        std::fs::read_to_string(root.join("dispatches"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn intermediate_park_rejects_unsafe_root_or_intermediate() {
    use std::sync::atomic::AtomicBool;
    for root_unsafe in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let ready = Arc::new(AtomicBool::new(false));
        let root = Arc::new(KeyedToolAdapter::new(ParkedRoot));
        let middle = Arc::new(KeyedToolAdapter::new(ParkedIntermediate(Arc::new(
            AtomicUsize::new(0),
        ))));
        let child = Arc::new(AutoChild {
            invoked: Arc::new(AtomicUsize::new(0)),
            polls: Arc::new(AtomicUsize::new(0)),
            ready_a: ready.clone(),
            ready_b: ready,
        });
        let options = Options {
            model: Arc::new(ModelStub),
            tools: vec![
                root.register(if root_unsafe {
                    ReplayPolicy::Unsafe
                } else {
                    ReplayPolicy::Safe
                }),
                middle.register(if root_unsafe {
                    ReplayPolicy::Safe
                } else {
                    ReplayPolicy::Unsafe
                }),
                RegisteredTool {
                    tool: child,
                    replay: ReplayPolicy::Unsafe,
                },
            ],
            restart_limit: 5,
            max_turns: 8,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        };
        let path = dir.path().join("db");
        let mut scheduler = Scheduler::open(&path, options).unwrap();
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), scheduler.wait(id))
            .await
            .unwrap()
            .unwrap();
        assert!(
            durable_store::Store::open(&path)
                .unwrap()
                .list(b"durable/nested-suspension/")
                .unwrap()
                .is_empty()
        );
        assert!(!scheduler.conversation(id).unwrap().nested_suspended());
        scheduler.shutdown().await.unwrap();
    }
}

struct AnyMixedRaceMiddle {
    requested: Arc<std::sync::atomic::AtomicBool>,
    release: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait]
impl KeyedTool for AnyMixedRaceMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        let wake = host.pending_wake();
        assert!(matches!(
            host.call_keyed_unpolled("done", "immediate", json!({}))
                .await,
            Outcome::Succeeded(_)
        ));
        let a = host
            .call_keyed_unpolled_with_id("a", "child", json!({"job":"a"}))
            .await
            .unwrap();
        let b = host
            .call_keyed_unpolled_with_id("b", "child", json!({"job":"b"}))
            .await
            .unwrap();
        let resumed = wake.is_some();
        if let Some(wake) = wake {
            assert_eq!(wake.ended, vec![a.operation_id]);
            assert!(matches!(a.outcome, Outcome::Succeeded(_)));
        }
        assert!(matches!(b.outcome, Outcome::Pending { .. }));
        host.suspend_pending_any_with_checkpoint(json!("mixed"))
            .unwrap();
        if resumed {
            self.requested.store(true, Ordering::SeqCst);
            while !self.release.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        Outcome::Succeeded("park".into()).into()
    }
}

#[tokio::test]
async fn any_repark_with_completed_siblings_is_fenced_by_cas_and_abort() {
    use durable_store::{Check, Expected, Mutation, Store};
    for mode in ["success", "cas", "abort"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let requested = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let immediate = Arc::new(AtomicUsize::new(0));
        let pending = Arc::new(AtomicUsize::new(0));
        let options = || Options {
            model: Arc::new(ModelStub),
            tools: vec![
                Arc::new(KeyedToolAdapter::new(AnyRoot)).register(ReplayPolicy::Safe),
                Arc::new(KeyedToolAdapter::new(AnyMixedRaceMiddle {
                    requested: requested.clone(),
                    release: release.clone(),
                }))
                .register(ReplayPolicy::Safe),
                RegisteredTool {
                    tool: Arc::new(ImmediateMixedLeaf(immediate.clone())),
                    replay: ReplayPolicy::Unsafe,
                },
                RegisteredTool {
                    tool: Arc::new(MultiParkLeaf(pending.clone())),
                    replay: ReplayPolicy::Unsafe,
                },
            ],
            restart_limit: 5,
            max_turns: 8,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        };
        let mut scheduler = Scheduler::open(&path, options()).unwrap();
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        wait(&mut scheduler, id).await;
        let root = scheduler.blocked(id).unwrap().unwrap().0;
        let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
        let a = jobs.iter().find(|job| job.reference == "a").unwrap();
        let b = jobs.iter().find(|job| job.reference == "b").unwrap();
        scheduler
            .reconcile_nested_pending(
                id,
                root,
                a.operation_id,
                &a.reference,
                Outcome::Succeeded("resolved".into()),
            )
            .unwrap();
        if tokio::time::timeout(Duration::from_secs(3), async {
            while !requested.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_err()
        {
            panic!(
                "repark not requested: {:?}",
                scheduler.conversation(id).unwrap()
            );
        }
        let store = Store::open(&path).unwrap();
        if mode == "abort" {
            scheduler.abort(id).unwrap();
        } else if mode == "cas" {
            let (key, entry) = store
                .list(b"nested/")
                .unwrap()
                .into_iter()
                .find(|(_, entry)| {
                    let value: Value = serde_json::from_slice(&entry.value).unwrap();
                    value["operation_id"] == json!(a.operation_id)
                })
                .unwrap();
            store
                .commit(
                    &[Check {
                        key: key.clone(),
                        expected: Expected::Version(entry.version),
                    }],
                    &[Mutation::Put {
                        key,
                        value: entry.value,
                    }],
                )
                .unwrap();
        }
        release.store(true, Ordering::SeqCst);
        wait(&mut scheduler, id).await;
        if mode == "abort" {
            assert!(scheduler.conversation(id).unwrap().abort_requested);
        } else if mode == "cas" {
            assert!(!scheduler.conversation(id).unwrap().nested_suspended());
        } else {
            assert!(scheduler.conversation(id).unwrap().nested_suspended());
            assert_eq!(
                scheduler.nested_pending_jobs(id, root).unwrap(),
                vec![b.clone()]
            );
        }
        scheduler.shutdown().await.unwrap();
        let mut scheduler = Scheduler::open(&path, options()).unwrap();
        if mode == "abort" {
            scheduler
                .cancel_nested_pending(id, root, b.operation_id, &b.reference, "cancelled".into())
                .unwrap();
            wait(&mut scheduler, id).await;
            assert!(scheduler.conversation(id).unwrap().abort_requested);
        } else {
            wait(&mut scheduler, id).await;
        }
        assert_eq!(immediate.load(Ordering::SeqCst), 1);
        assert_eq!(pending.load(Ordering::SeqCst), 2);
        scheduler.shutdown().await.unwrap();
    }
}

// Exercise early-any with an already-completed unsafe sibling through both wakes.
struct MixedAnyDiskMiddle(std::path::PathBuf);
#[async_trait]
impl KeyedTool for MixedAnyDiskMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        use std::io::Write;
        let wake = host.pending_wake();
        if let Some(wake) = &wake {
            writeln!(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.0.join("mixed-any-wakes"))
                    .unwrap(),
                "{}",
                serde_json::to_string(wake).unwrap()
            )
            .unwrap();
        }
        let done = host
            .call_keyed_unpolled_with_id("done", "immediate", json!({}))
            .await
            .unwrap();
        let a = host
            .call_keyed_unpolled_with_id("a", "child", json!({"job":"a"}))
            .await
            .unwrap();
        let b = host
            .call_keyed_unpolled_with_id("b", "child", json!({"job":"b"}))
            .await
            .unwrap();
        assert_eq!(done.outcome, Outcome::Succeeded("already done".into()));
        if matches!(a.outcome, Outcome::Pending { .. })
            || matches!(b.outcome, Outcome::Pending { .. })
        {
            host.suspend_pending_any_with_checkpoint(json!("mixed-any-checkpoint"))
                .unwrap();
            if wake.is_some() && self.0.join("stop-before-repark").exists() {
                std::fs::write(self.0.join("at-before-repark"), b"ready").unwrap();
                std::future::pending::<()>().await;
            }
            Outcome::Succeeded("park".into()).into()
        } else {
            assert_eq!(a.outcome, Outcome::Succeeded("a resolved".into()));
            assert_eq!(b.outcome, Outcome::Succeeded("b resolved".into()));
            Outcome::Succeeded("mixed any complete".into()).into()
        }
    }
}
fn mixed_any_disk_options(path: &std::path::Path) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(AnyDiskRoot(path.into()))).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(MixedAnyDiskMiddle(path.into())))
                .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(FileImmediateMixedLeaf(path.into())),
                replay: ReplayPolicy::Unsafe,
            },
            RegisteredTool {
                tool: Arc::new(MultiParkFileLeaf(path.into())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 8,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}

#[tokio::test]
async fn mixed_any_process_worker() {
    let Some(path) = std::env::var_os("PI_MIXED_ANY_PROCESS") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let mut scheduler = Scheduler::open(path.join("db"), mixed_any_disk_options(&path)).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    std::fs::write(path.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
    let a = jobs.iter().find(|job| job.reference == "a").unwrap();
    let b = jobs.iter().find(|job| job.reference == "b").unwrap();
    std::fs::write(
        path.join("job-ids"),
        serde_json::to_vec(&vec![a.operation_id, b.operation_id]).unwrap(),
    )
    .unwrap();
    scheduler
        .reconcile_nested_pending(
            id,
            root,
            a.operation_id,
            &a.reference,
            Outcome::Succeeded("a resolved".into()),
        )
        .unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.conversation(id).unwrap().nested_suspended());
    assert_eq!(
        scheduler.nested_pending_jobs(id, root).unwrap(),
        vec![b.clone()]
    );
    std::fs::write(path.join("at-after-repark"), b"ready").unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn mixed_any_completes_after_repark_and_process_loss() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for stage in ["before", "after"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        if stage == "before" {
            std::fs::write(path.join("stop-before-repark"), b"yes").unwrap();
        }
        let mut process = Kill(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "mixed_any_process_worker"])
                .env("PI_MIXED_ANY_PROCESS", path)
                .spawn()
                .unwrap(),
        );
        until(|| path.join(format!("at-{stage}-repark")).exists()).await;
        process.0.kill().unwrap();
        process.0.wait().unwrap();
        if stage == "before" {
            std::fs::remove_file(path.join("stop-before-repark")).unwrap();
        }
        let id = std::fs::read_to_string(path.join("conversation"))
            .unwrap()
            .parse()
            .unwrap();
        let ids: Vec<uuid::Uuid> =
            serde_json::from_slice(&std::fs::read(path.join("job-ids")).unwrap()).unwrap();
        let mut scheduler = Scheduler::open(path.join("db"), mixed_any_disk_options(path)).unwrap();
        wait(&mut scheduler, id).await;
        assert!(
            scheduler.conversation(id).unwrap().nested_suspended(),
            "{stage}"
        );
        let root = scheduler.blocked(id).unwrap().unwrap().0;
        let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].reference, "b");
        assert_eq!(jobs[0].operation_id, ids[1]);
        scheduler
            .reconcile_nested_pending(
                id,
                root,
                ids[1],
                &jobs[0].reference,
                Outcome::Succeeded("b resolved".into()),
            )
            .unwrap();
        wait(&mut scheduler, id).await;
        assert!(
            scheduler.blocked(id).unwrap().is_none(),
            "{stage}: {:?}",
            scheduler.conversation(id).unwrap()
        );
        assert!(scheduler.conversation(id).unwrap().messages.iter().any(
            |m| matches!(m, Message::Tool { content, .. } if content == "mixed any complete")
        ));
        let wakes: Vec<Value> = std::fs::read_to_string(path.join("mixed-any-wakes"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let expected_count = if stage == "before" { 3 } else { 2 };
        assert_eq!(wakes.len(), expected_count, "{stage}");
        for wake in &wakes {
            assert_eq!(wake["checkpoint"], json!("mixed-any-checkpoint"));
        }
        for wake in &wakes[..wakes.len() - 1] {
            assert_eq!(wake["ended"], json!([ids[0]]));
        }
        assert_eq!(wakes.last().unwrap()["ended"], json!([ids[1]]));
        assert_eq!(
            std::fs::read_to_string(path.join("immediate-dispatches")).unwrap(),
            "done\n"
        );
        assert_eq!(
            std::fs::read_to_string(path.join("dispatches")).unwrap(),
            "a\nb\n"
        );
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn mixed_any_reparks_then_completes_after_both_wakes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let mut scheduler = Scheduler::open(path.join("db"), mixed_any_disk_options(path)).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    wait(&mut scheduler, id).await;
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
    assert_eq!(jobs.len(), 2);
    let a = jobs.iter().find(|job| job.reference == "a").unwrap();
    let b = jobs.iter().find(|job| job.reference == "b").unwrap();
    scheduler
        .reconcile_nested_pending(
            id,
            root,
            a.operation_id,
            &a.reference,
            Outcome::Succeeded("a resolved".into()),
        )
        .unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.conversation(id).unwrap().nested_suspended());
    assert_eq!(
        scheduler.nested_pending_jobs(id, root).unwrap(),
        vec![b.clone()]
    );
    scheduler
        .reconcile_nested_pending(
            id,
            root,
            b.operation_id,
            &b.reference,
            Outcome::Succeeded("b resolved".into()),
        )
        .unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { content, .. } if content == "mixed any complete"))
    );
    let wakes: Vec<Value> = std::fs::read_to_string(path.join("mixed-any-wakes"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        wakes,
        vec![
            json!({"checkpoint":"mixed-any-checkpoint", "ended":[a.operation_id]}),
            json!({"checkpoint":"mixed-any-checkpoint", "ended":[b.operation_id]})
        ]
    );
    assert_eq!(
        std::fs::read_to_string(path.join("immediate-dispatches")).unwrap(),
        "done\n"
    );
    assert_eq!(
        std::fs::read_to_string(path.join("dispatches")).unwrap(),
        "a\nb\n"
    );
    scheduler.shutdown().await.unwrap();
}

// The middle's before hook runs before its first intent, not on child wake.
// The leaf hook's memo survives interruption between its side effect and commit.
struct IntermediateHook(std::path::PathBuf);
#[async_trait]
impl pi_rs_durable::extension::Hook for IntermediateHook {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::BeforeTool, String> {
        if call.name == "middle" {
            use std::io::Write;
            writeln!(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.0.join("middle-before"))
                    .unwrap(),
                "before"
            )
            .unwrap();
        }
        Ok(pi_rs_durable::extension::BeforeTool::Pass)
    }
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
        let candidate = if self.0.join("release-hook").exists() {
            "second"
        } else {
            "first"
        };
        let memo = context
            .memo
            .get_or_insert("hook-key", json!(candidate))
            .await?;
        writeln!(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("hook-attempts"))
                .unwrap(),
            "{memo}"
        )
        .unwrap();
        if !self.0.join("release-hook").exists() {
            std::fs::write(self.0.join("hook-entered"), b"ready").unwrap();
            std::future::pending::<()>().await;
        }
        Ok(Some(json!(format!("hook:{}", memo.as_str().unwrap()))))
    }
}

fn open_intermediate_hooks(root: &std::path::Path) -> Scheduler {
    Scheduler::open_with_extensions(
        root.join("db"),
        child_park_disk_options(root),
        hook_registry(Arc::new(IntermediateHook(root.into()))),
    )
    .unwrap()
}

async fn start_intermediate_hooks(root: &std::path::Path) -> (Scheduler, uuid::Uuid) {
    let mut scheduler = open_intermediate_hooks(root);
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
    (scheduler, id)
}

#[tokio::test]
async fn intermediate_hook_rewrites_leaf_before_wake() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(root.join("release-hook"), b"yes").unwrap();
    std::fs::write(root.join("finish"), b"yes").unwrap();
    let (mut scheduler, id) = start_intermediate_hooks(root).await;
    wait(&mut scheduler, id).await;
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content,..} if content == "middle:hook:second"))
    );
    assert_eq!(
        std::fs::read_to_string(root.join("middle-before"))
            .unwrap()
            .lines()
            .count(),
        1
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

#[tokio::test]
async fn intermediate_hook_crash_worker() {
    let Some(root) = std::env::var_os("PI_INTERMEDIATE_HOOK_CRASH") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let (_scheduler, id) = start_intermediate_hooks(&root).await;
    std::fs::write(root.join("conversation"), id.to_string()).unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn intermediate_hook_crash_replays_memo_without_redispatch() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(root.join("finish"), b"yes").unwrap();
    let mut process = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "intermediate_hook_crash_worker"])
            .env("PI_INTERMEDIATE_HOOK_CRASH", root)
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
    std::fs::write(root.join("release-hook"), b"yes").unwrap();
    let mut scheduler = open_intermediate_hooks(root);
    wait(&mut scheduler, id).await;
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content,..} if content == "middle:hook:first"))
    );
    assert_eq!(
        std::fs::read_to_string(root.join("hook-attempts"))
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        vec!["\"first\"", "\"first\""]
    );
    assert_eq!(
        std::fs::read_to_string(root.join("middle-before"))
            .unwrap()
            .lines()
            .count(),
        1
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

#[tokio::test]
async fn intermediate_hook_abort_prevents_wake_and_publication() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(root.join("finish"), b"yes").unwrap();
    let (mut scheduler, id) = start_intermediate_hooks(root).await;
    until(|| root.join("hook-entered").exists()).await;
    scheduler.abort(id).unwrap();
    wait(&mut scheduler, id).await;
    scheduler.shutdown().await.unwrap();
    std::fs::write(root.join("release-hook"), b"yes").unwrap();
    let mut scheduler = open_intermediate_hooks(root);
    wait(&mut scheduler, id).await;
    assert!(scheduler.conversation(id).unwrap().abort_requested);
    assert!(
        !scheduler.conversation(id).unwrap().messages.iter().any(
            |m| matches!(m, Message::Tool {content,..} if content.starts_with("middle:hook:"))
        )
    );
    assert_eq!(
        std::fs::read_to_string(root.join("hook-attempts"))
            .unwrap()
            .lines()
            .count(),
        1
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

struct MixedAnyHook {
    calls: Arc<std::sync::Mutex<Vec<String>>>,
}
#[async_trait]
impl pi_rs_durable::extension::Hook for MixedAnyHook {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::BeforeTool, String> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("before:{}:{}", call.name, call.arguments));
        Ok(pi_rs_durable::extension::BeforeTool::Pass)
    }
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &Value,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<Value>, String> {
        if call.name != "child" && call.name != "immediate" {
            return Ok(None);
        }
        self.calls
            .lock()
            .unwrap()
            .push(format!("after:{}:{}", call.name, call.arguments));
        Ok(Some(json!(format!("hook:{}", result.as_str().unwrap()))))
    }
}
struct HookedMixedAnyMiddle(Arc<std::sync::Mutex<Vec<pi_rs_durable::nested::PendingWake>>>);
#[async_trait]
impl KeyedTool for HookedMixedAnyMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        if let Some(wake) = host.pending_wake() {
            self.0.lock().unwrap().push(wake);
        }
        let done = host
            .call_keyed_unpolled("done", "immediate", json!({}))
            .await;
        let a = host
            .call_keyed_unpolled("a", "child", json!({"job":"a"}))
            .await;
        let b = host
            .call_keyed_unpolled("b", "child", json!({"job":"b"}))
            .await;
        assert_eq!(done, Outcome::Succeeded("hook:already done".into()));
        if matches!(a, Outcome::Pending { .. }) || matches!(b, Outcome::Pending { .. }) {
            host.suspend_pending_any_with_checkpoint(json!("hooked-any"))
                .unwrap();
            Outcome::Succeeded("park".into()).into()
        } else {
            assert_eq!(a, Outcome::Succeeded("hook:a resolved".into()));
            assert_eq!(b, Outcome::Succeeded("hook:b resolved".into()));
            Outcome::Succeeded("hooked mixed complete".into()).into()
        }
    }
}
#[tokio::test]
async fn hooks_transform_mixed_any_results_across_repark_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let wakes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let immediate = Arc::new(AtomicUsize::new(0));
    let pending = Arc::new(AtomicUsize::new(0));
    let options = || Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(AnyRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(HookedMixedAnyMiddle(wakes.clone())))
                .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(ImmediateMixedLeaf(immediate.clone())),
                replay: ReplayPolicy::Unsafe,
            },
            RegisteredTool {
                tool: Arc::new(MultiParkLeaf(pending.clone())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 8,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let open = || {
        Scheduler::open_with_extensions(
            &path,
            options(),
            hook_registry(Arc::new(MixedAnyHook {
                calls: calls.clone(),
            })),
        )
        .unwrap()
    };
    let mut scheduler = open();
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
        scheduler.conversation(id).unwrap().nested_suspended(),
        "{:?}",
        scheduler.conversation(id).unwrap()
    );
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
    let a = jobs.iter().find(|job| job.reference == "a").unwrap();
    let b = jobs.iter().find(|job| job.reference == "b").unwrap();
    scheduler
        .reconcile_nested_pending(
            id,
            root,
            a.operation_id,
            &a.reference,
            Outcome::Succeeded("a resolved".into()),
        )
        .unwrap();
    wait(&mut scheduler, id).await;
    assert!(
        scheduler.conversation(id).unwrap().nested_suspended(),
        "{:?}",
        scheduler.conversation(id).unwrap()
    );
    assert_eq!(
        scheduler.nested_pending_jobs(id, root).unwrap(),
        vec![b.clone()]
    );
    scheduler.shutdown().await.unwrap();
    let mut scheduler = open();
    scheduler
        .reconcile_nested_pending(
            id,
            root,
            b.operation_id,
            &b.reference,
            Outcome::Succeeded("b resolved".into()),
        )
        .unwrap();
    wait(&mut scheduler, id).await;
    assert!(
        scheduler.blocked(id).unwrap().is_none(),
        "{:?}",
        scheduler.conversation(id).unwrap()
    );
    assert!(
        scheduler.conversation(id).unwrap().messages.iter().any(
            |m| matches!(m, Message::Tool { content, .. } if content == "hooked mixed complete")
        )
    );
    let wakes = wakes.lock().unwrap();
    assert_eq!(wakes.len(), 2);
    assert_eq!(wakes[0].checkpoint, json!("hooked-any"));
    assert_eq!(wakes[0].ended, vec![a.operation_id]);
    assert_eq!(wakes[1].checkpoint, json!("hooked-any"));
    assert_eq!(wakes[1].ended, vec![b.operation_id]);
    assert_eq!(immediate.load(Ordering::SeqCst), 1);
    assert_eq!(pending.load(Ordering::SeqCst), 2);
    let calls = calls.lock().unwrap();
    for entry in [
        "before:middle:{}",
        "before:immediate:{}",
        "after:immediate:{}",
        "before:child:{\"job\":\"a\"}",
        "before:child:{\"job\":\"b\"}",
        "after:child:{\"job\":\"a\"}",
        "after:child:{\"job\":\"b\"}",
    ] {
        assert_eq!(
            calls.iter().filter(|call| *call == entry).count(),
            1,
            "{entry}: {calls:?}"
        );
    }
    drop(calls);
    scheduler.shutdown().await.unwrap();
}

struct ManualReceiptHook(std::path::PathBuf);
#[async_trait]
impl pi_rs_durable::extension::Hook for ManualReceiptHook {
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        value: &Value,
        context: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<Value>, String> {
        if call.name != "child" {
            return Ok(None);
        }
        use std::io::Write;
        let name = call.arguments["job"].as_str().unwrap();
        let candidate = if self.0.join("changed-candidate").exists() {
            "later"
        } else {
            "original"
        };
        let memo = context
            .memo
            .get_or_insert("receipt-memo", json!(candidate))
            .await?;
        writeln!(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join(format!("hook-{name}")))
                .unwrap(),
            "{}:{}",
            memo.as_str().unwrap(),
            value.as_str().unwrap()
        )
        .unwrap();
        if name == "b" && !self.0.join("release-multi-hook").exists() {
            std::fs::write(self.0.join("b-hook-entered"), b"yes").unwrap();
            std::future::pending::<()>().await;
        }
        Ok(Some(json!(format!(
            "hook:{name}:{}:{}",
            memo.as_str().unwrap(),
            value.as_str().unwrap()
        ))))
    }
}

struct HookReceiptMiddle;
#[async_trait]
impl KeyedTool for HookReceiptMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        let wake = host.pending_wake();
        let a = host
            .call_keyed_unpolled_with_id("a", "child", json!({"job":"a"}))
            .await
            .unwrap();
        let b = host
            .call_keyed_unpolled_with_id("b", "child", json!({"job":"b"}))
            .await
            .unwrap();
        if let Some(wake) = wake {
            assert_eq!(
                wake.checkpoint,
                json!({"a":a.operation_id,"b":b.operation_id})
            );
            assert_eq!(wake.ended, vec![a.operation_id, b.operation_id]);
            assert_eq!(
                a.outcome,
                Outcome::Succeeded("hook:a:original:raw-a".into())
            );
            assert_eq!(
                b.outcome,
                Outcome::Succeeded("hook:b:original:raw-b".into())
            );
            Outcome::Succeeded("both hooked".into()).into()
        } else {
            assert!(matches!(a.outcome, Outcome::Pending { .. }));
            assert!(matches!(b.outcome, Outcome::Pending { .. }));
            host.suspend_pending_with_checkpoint(json!({"a":a.operation_id,"b":b.operation_id}))
                .unwrap();
            Outcome::Succeeded("park".into()).into()
        }
    }
}

fn manual_receipt_options(root: &std::path::Path) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(ParkedRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(HookReceiptMiddle)).register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(MultiParkFileLeaf(root.into())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
fn manual_receipt_scheduler(root: &std::path::Path) -> Scheduler {
    Scheduler::open_with_extensions(
        root.join("db"),
        manual_receipt_options(root),
        hook_registry(Arc::new(ManualReceiptHook(root.into()))),
    )
    .unwrap()
}
async fn start_manual_receipts(
    root: &std::path::Path,
) -> (
    Scheduler,
    uuid::Uuid,
    uuid::Uuid,
    Vec<pi_rs_durable::nested::PendingJob>,
) {
    let mut scheduler = manual_receipt_scheduler(root);
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
    assert!(scheduler.conversation(id).unwrap().nested_suspended());
    let root_op = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root_op).unwrap();
    assert_eq!(jobs.len(), 2);
    (scheduler, id, root_op, jobs)
}

#[tokio::test]
async fn manual_receipt_hook_rewrite_is_replayable_and_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let (mut scheduler, id, root_op, jobs) = start_manual_receipts(root).await;
    let a = jobs.iter().find(|job| job.reference == "a").unwrap();
    let b = jobs.iter().find(|job| job.reference == "b").unwrap();
    let raw = Outcome::Succeeded("raw-a".into());
    scheduler
        .reconcile_nested_pending(id, root_op, a.operation_id, &a.reference, raw.clone())
        .unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.conversation(id).unwrap().nested_suspended());
    assert_eq!(
        std::fs::read_to_string(root.join("hook-a")).unwrap(),
        "original:raw-a\n"
    );
    scheduler
        .reconcile_nested_pending(id, root_op, a.operation_id, &a.reference, raw)
        .unwrap();
    assert!(
        scheduler
            .reconcile_nested_pending(
                id,
                root_op,
                a.operation_id,
                &a.reference,
                Outcome::Succeeded("different".into())
            )
            .is_err()
    );
    std::fs::write(root.join("release-multi-hook"), b"yes").unwrap();
    scheduler
        .reconcile_nested_pending(
            id,
            root_op,
            b.operation_id,
            &b.reference,
            Outcome::Succeeded("raw-b".into()),
        )
        .unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content,..} if content == "both hooked"))
    );
    assert_eq!(
        std::fs::read_to_string(root.join("hook-a")).unwrap(),
        "original:raw-a\n"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("hook-b")).unwrap(),
        "original:raw-b\n"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("dispatches")).unwrap(),
        "a\nb\n"
    );
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn multiple_manual_receipt_hooks_crash_worker() {
    let Some(path) = std::env::var_os("PI_MULTI_RECEIPT_HOOK_CRASH") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let (mut scheduler, id, root_op, jobs) = start_manual_receipts(&path).await;
    std::fs::write(path.join("conversation"), id.to_string()).unwrap();
    let a = jobs.iter().find(|job| job.reference == "a").unwrap();
    let b = jobs.iter().find(|job| job.reference == "b").unwrap();
    scheduler
        .reconcile_nested_pending(
            id,
            root_op,
            a.operation_id,
            &a.reference,
            Outcome::Succeeded("raw-a".into()),
        )
        .unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.conversation(id).unwrap().nested_suspended());
    assert_eq!(
        std::fs::read_to_string(path.join("hook-a")).unwrap(),
        "original:raw-a\n"
    );
    scheduler
        .reconcile_nested_pending(
            id,
            root_op,
            b.operation_id,
            &b.reference,
            Outcome::Succeeded("raw-b".into()),
        )
        .unwrap();
    until(|| path.join("b-hook-entered").exists()).await;
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn multiple_manual_receipt_hooks_resume_after_crash_without_repeating_committed_hook() {
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
            .args(["--exact", "multiple_manual_receipt_hooks_crash_worker"])
            .env("PI_MULTI_RECEIPT_HOOK_CRASH", root)
            .spawn()
            .unwrap(),
    );
    until(|| root.join("b-hook-entered").exists()).await;
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let id = std::fs::read_to_string(root.join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
    std::fs::write(root.join("changed-candidate"), b"yes").unwrap();
    std::fs::write(root.join("release-multi-hook"), b"yes").unwrap();
    let mut scheduler = manual_receipt_scheduler(root);
    wait(&mut scheduler, id).await;
    assert!(scheduler.blocked(id).unwrap().is_none());
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content,..} if content == "both hooked"))
    );
    assert_eq!(
        std::fs::read_to_string(root.join("hook-a")).unwrap(),
        "original:raw-a\n"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("hook-b")).unwrap(),
        "original:raw-b\noriginal:raw-b\n"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("dispatches")).unwrap(),
        "a\nb\n"
    );
    scheduler.shutdown().await.unwrap();
}

struct MixedAnyReceiptMiddle(std::path::PathBuf);
#[async_trait]
impl KeyedTool for MixedAnyReceiptMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        use std::io::Write;
        let wake = host.pending_wake();
        let done = host
            .call_keyed_unpolled("done", "immediate", json!({}))
            .await;
        let a = host
            .call_keyed_unpolled_with_id("a", "child", json!({"job":"a"}))
            .await
            .unwrap();
        let b = host
            .call_keyed_unpolled_with_id("b", "child", json!({"job":"b"}))
            .await
            .unwrap();
        assert_eq!(done, Outcome::Succeeded("already done".into()));
        if let Some(wake) = wake {
            assert_eq!(wake.checkpoint, json!("mixed-receipt"));
            writeln!(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.0.join("mixed-receipt-wakes"))
                    .unwrap(),
                "{}",
                serde_json::to_string(&wake.ended).unwrap()
            )
            .unwrap();
        }
        if matches!(a.outcome, Outcome::Pending { .. })
            || matches!(b.outcome, Outcome::Pending { .. })
        {
            host.suspend_pending_any_with_checkpoint(json!("mixed-receipt"))
                .unwrap();
            Outcome::Succeeded("park".into()).into()
        } else {
            assert_eq!(
                a.outcome,
                Outcome::Succeeded("hook:a:original:raw-a".into())
            );
            assert_eq!(
                b.outcome,
                Outcome::Succeeded("hook:b:original:raw-b".into())
            );
            Outcome::Succeeded("mixed receipts complete".into()).into()
        }
    }
}
fn mixed_any_receipt_scheduler(root: &std::path::Path) -> Scheduler {
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(AnyRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(MixedAnyReceiptMiddle(root.into())))
                .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(FileImmediateMixedLeaf(root.into())),
                replay: ReplayPolicy::Unsafe,
            },
            RegisteredTool {
                tool: Arc::new(MultiParkFileLeaf(root.into())),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 8,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    Scheduler::open_with_extensions(
        root.join("db"),
        options,
        hook_registry(Arc::new(ManualReceiptHook(root.into()))),
    )
    .unwrap()
}
#[tokio::test]
async fn mixed_any_receipt_hook_crash_worker() {
    let Some(root) = std::env::var_os("PI_MIXED_ANY_RECEIPT_CRASH") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let mut scheduler = mixed_any_receipt_scheduler(&root);
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
    wait(&mut scheduler, id).await;
    let root_op = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root_op).unwrap();
    assert_eq!(jobs.len(), 2);
    let a = jobs.iter().find(|job| job.reference == "a").unwrap();
    let b = jobs.iter().find(|job| job.reference == "b").unwrap();
    std::fs::write(
        root.join("job-ids"),
        serde_json::to_vec(&vec![a.operation_id, b.operation_id]).unwrap(),
    )
    .unwrap();
    scheduler
        .reconcile_nested_pending(
            id,
            root_op,
            a.operation_id,
            &a.reference,
            Outcome::Succeeded("raw-a".into()),
        )
        .unwrap();
    wait(&mut scheduler, id).await;
    assert!(scheduler.conversation(id).unwrap().nested_suspended());
    assert_eq!(
        scheduler.nested_pending_jobs(id, root_op).unwrap(),
        vec![b.clone()]
    );
    scheduler
        .reconcile_nested_pending(
            id,
            root_op,
            b.operation_id,
            &b.reference,
            Outcome::Succeeded("raw-b".into()),
        )
        .unwrap();
    until(|| root.join("b-hook-entered").exists()).await;
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn mixed_any_reconciled_hook_recovers_after_process_loss() {
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
            .args(["--exact", "mixed_any_receipt_hook_crash_worker"])
            .env("PI_MIXED_ANY_RECEIPT_CRASH", root)
            .spawn()
            .unwrap(),
    );
    until(|| root.join("b-hook-entered").exists()).await;
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let id = std::fs::read_to_string(root.join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
    let ids: Vec<uuid::Uuid> =
        serde_json::from_slice(&std::fs::read(root.join("job-ids")).unwrap()).unwrap();
    std::fs::write(root.join("changed-candidate"), b"yes").unwrap();
    std::fs::write(root.join("release-multi-hook"), b"yes").unwrap();
    let mut scheduler = mixed_any_receipt_scheduler(root);
    wait(&mut scheduler, id).await;
    assert!(
        scheduler.blocked(id).unwrap().is_none(),
        "{:?}",
        scheduler.conversation(id).unwrap()
    );
    assert!(scheduler.conversation(id).unwrap().messages.iter().any(
        |m| matches!(m, Message::Tool { content, .. } if content == "mixed receipts complete")
    ));
    let wakes: Vec<Vec<uuid::Uuid>> = std::fs::read_to_string(root.join("mixed-receipt-wakes"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(wakes, vec![vec![ids[0]], vec![ids[1]]]);
    assert_eq!(
        std::fs::read_to_string(root.join("hook-a")).unwrap(),
        "original:raw-a\n"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("hook-b")).unwrap(),
        "original:raw-b\noriginal:raw-b\n"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("immediate-dispatches")).unwrap(),
        "done\n"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("dispatches")).unwrap(),
        "a\nb\n"
    );
    scheduler.shutdown().await.unwrap();
}

struct StagedAnyHook {
    active: Arc<AtomicUsize>,
    entered: Arc<AtomicUsize>,
    hold: bool,
}

struct VerifyStagedAnyMiddle {
    active: Arc<AtomicUsize>,
    wakes: Arc<std::sync::Mutex<Vec<Vec<uuid::Uuid>>>>,
}
#[async_trait]
impl KeyedTool for VerifyStagedAnyMiddle {
    fn spec(&self) -> ToolSpec {
        spec("middle")
    }
    async fn invoke(
        &self,
        args: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: KeyedHost,
    ) -> ToolReply {
        if host.pending_wake().is_some() {
            assert_eq!(self.active.load(Ordering::SeqCst), 0);
            assert_eq!(
                host.call_keyed_unpolled("a", "child", json!({"job":"a"}))
                    .await,
                Outcome::Succeeded("hook:done".into())
            );
        }
        AnyMiddle(self.wakes.clone())
            .invoke(args, context, state, host)
            .await
    }
}
#[async_trait]
impl pi_rs_durable::extension::Hook for StagedAnyHook {
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &Value,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<Value>, String> {
        if call.name != "child" {
            return Ok(None);
        }
        assert_eq!(
            self.active.load(Ordering::SeqCst),
            0,
            "sibling poll must stop before hook"
        );
        self.entered.fetch_add(1, Ordering::SeqCst);
        if self.hold {
            std::future::pending::<()>().await;
        }
        Ok(Some(json!(format!("hook:{}", result.as_str().unwrap()))))
    }
}

fn staged_any_options(
    active: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    wakes: Arc<std::sync::Mutex<Vec<Vec<uuid::Uuid>>>>,
    release_a: Arc<std::sync::atomic::AtomicBool>,
) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![
            Arc::new(KeyedToolAdapter::new(AnyRoot)).register(ReplayPolicy::Safe),
            Arc::new(KeyedToolAdapter::new(VerifyStagedAnyMiddle {
                active: active.clone(),
                wakes,
            }))
            .register(ReplayPolicy::Safe),
            RegisteredTool {
                tool: Arc::new(AnyHangingPoll {
                    in_flight: active,
                    invoked: calls,
                    release_a,
                }),
                replay: ReplayPolicy::Unsafe,
            },
        ],
        restart_limit: 8,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}

async fn staged_any_case(abort_in_hook: bool) {
    let dir = tempfile::tempdir().unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(AtomicUsize::new(0));
    let wakes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let options = staged_any_options(
        active.clone(),
        calls.clone(),
        wakes.clone(),
        release.clone(),
    );
    let registry = hook_registry(Arc::new(StagedAnyHook {
        active: active.clone(),
        entered: entered.clone(),
        hold: abort_in_hook,
    }));
    let mut scheduler =
        Scheduler::open_with_extensions(dir.path().join("db"), options, registry).unwrap();
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
    until(|| active.load(Ordering::SeqCst) == 1).await;
    let root = scheduler.blocked(id).unwrap().unwrap().0;
    let jobs = scheduler.nested_pending_jobs(id, root).unwrap();
    let a = jobs
        .iter()
        .find(|job| job.reference == "a")
        .unwrap()
        .clone();
    let b = jobs
        .iter()
        .find(|job| job.reference == "b")
        .unwrap()
        .clone();
    release.store(true, Ordering::SeqCst);
    until(|| entered.load(Ordering::SeqCst) == 1).await;
    if abort_in_hook {
        scheduler.abort(id).unwrap();
        wait(&mut scheduler, id).await;
        assert!(scheduler.conversation(id).unwrap().abort_requested);
        assert!(wakes.lock().unwrap().is_empty());
        assert_eq!(active.load(Ordering::SeqCst), 0);
        scheduler.shutdown().await.unwrap();
        let options = staged_any_options(
            active.clone(),
            calls.clone(),
            wakes.clone(),
            release.clone(),
        );
        let registry = hook_registry(Arc::new(StagedAnyHook {
            active: active.clone(),
            entered: entered.clone(),
            hold: false,
        }));
        let mut reopened =
            Scheduler::open_with_extensions(dir.path().join("db"), options, registry).unwrap();
        wait(&mut reopened, id).await;
        assert!(reopened.conversation(id).unwrap().abort_requested);
        assert!(wakes.lock().unwrap().is_empty());
        assert_eq!(entered.load(Ordering::SeqCst), 1);
        reopened.shutdown().await.unwrap();
    } else {
        until(|| !wakes.lock().unwrap().is_empty()).await;
        assert_eq!(*wakes.lock().unwrap(), vec![vec![a.operation_id]]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        // A's hook completed and only B remains unresolved after the repark.
        until(|| {
            scheduler
                .conversation(id)
                .is_ok_and(|c| c.nested_suspended())
        })
        .await;
        assert_eq!(scheduler.nested_pending_jobs(id, root).unwrap(), vec![b]);
        scheduler.abort(id).unwrap();
        scheduler.shutdown().await.unwrap();
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn any_staged_poll_retires_slow_sibling_before_hook_and_wake() {
    staged_any_case(false).await;
}
#[tokio::test]
async fn abort_after_staged_any_poll_during_hook_fences_wake_after_reopen() {
    staged_any_case(true).await;
}

struct DiskStagedAnyPoll(std::path::PathBuf);
struct DiskPollGuard(std::path::PathBuf);
impl Drop for DiskPollGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
#[async_trait]
impl Tool for DiskStagedAnyPoll {
    fn spec(&self) -> ToolSpec {
        spec("child")
    }
    async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
        use std::io::Write;
        let job = args["job"].as_str().unwrap();
        writeln!(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("dispatches"))
                .unwrap(),
            "{job}"
        )
        .unwrap();
        Outcome::Pending {
            reference: job.into(),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn poll(&self, reference: &str, _: ToolContext) -> Option<Outcome> {
        if reference == "a" {
            while !self.0.join("b-poll-active").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            return Some(Outcome::Succeeded("a raw".into()));
        }
        let marker = self.0.join("b-poll-active");
        std::fs::write(&marker, b"yes").unwrap();
        let _guard = DiskPollGuard(marker);
        std::future::pending().await
    }
}
struct DiskStagedAnyHook(std::path::PathBuf);
#[async_trait]
impl pi_rs_durable::extension::Hook for DiskStagedAnyHook {
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &Value,
        context: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<Value>, String> {
        if call.name != "child" {
            return Ok(None);
        }
        use std::io::Write;
        assert!(
            !self.0.join("b-poll-active").exists(),
            "sibling poll still active"
        );
        let candidate = if self.0.join("release-hook").exists() {
            "later"
        } else {
            "first"
        };
        let memo = context
            .memo
            .get_or_insert("staged-any", json!(candidate))
            .await?;
        writeln!(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("hook-attempts"))
                .unwrap(),
            "{}:{}",
            memo.as_str().unwrap(),
            result.as_str().unwrap()
        )
        .unwrap();
        if !self.0.join("release-hook").exists() {
            std::fs::write(self.0.join("hook-entered"), b"yes").unwrap();
            std::future::pending::<()>().await;
        }
        Ok(Some(json!(format!("hook:{}", result.as_str().unwrap()))))
    }
}
fn disk_staged_any_open(path: &std::path::Path) -> Scheduler {
    let mut options = any_disk_options(path);
    options.tools[2] = RegisteredTool {
        tool: Arc::new(DiskStagedAnyPoll(path.into())),
        replay: ReplayPolicy::Unsafe,
    };
    Scheduler::open_with_extensions(
        path.join("db"),
        options,
        hook_registry(Arc::new(DiskStagedAnyHook(path.into()))),
    )
    .unwrap()
}
#[tokio::test]
async fn staged_any_poll_crash_worker() {
    let Some(path) = std::env::var_os("PI_STAGED_ANY_POLL_CRASH") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let mut scheduler = disk_staged_any_open(&path);
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
    std::fs::write(path.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn staged_any_poll_crash_replays_hook_then_wakes_without_redispatch() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let mut process = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "staged_any_poll_crash_worker"])
            .env("PI_STAGED_ANY_POLL_CRASH", path)
            .spawn()
            .unwrap(),
    );
    until(|| path.join("hook-entered").exists()).await;
    assert!(!path.join("b-poll-active").exists());
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let id = std::fs::read_to_string(path.join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
    std::fs::write(path.join("release-hook"), b"yes").unwrap();
    let mut scheduler = disk_staged_any_open(path);
    until(|| path.join("wakes").exists()).await;
    assert_eq!(
        std::fs::read_to_string(path.join("hook-attempts")).unwrap(),
        "first:a raw\nfirst:a raw\n"
    );
    assert_eq!(
        std::fs::read_to_string(path.join("dispatches")).unwrap(),
        "a\nb\n"
    );
    assert_eq!(
        std::fs::read_to_string(path.join("wakes"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    scheduler.abort(id).unwrap();
    scheduler.shutdown().await.unwrap();
    assert!(!path.join("b-poll-active").exists());
}
