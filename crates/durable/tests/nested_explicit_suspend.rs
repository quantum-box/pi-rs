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
        let wake = ctx
            .snapshot
            .get("nested_before_tool_wake")
            .cloned()
            .unwrap_or(Value::Null);
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
fn multi_options(parent: Arc<KeyedToolAdapter<MultiParent>>, child: Arc<AutoChild>) -> Options {
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
