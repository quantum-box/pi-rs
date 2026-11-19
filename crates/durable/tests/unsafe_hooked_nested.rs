use async_trait::async_trait;
use pi_rs_core::{Outcome, Tool, ToolContext, ToolSpec};
use pi_rs_durable::{
    async_task_tool::{
        AsyncTaskCall, AsyncTaskResult, DurableAsyncTool, DurableTaskHost, DurableToolResume,
        DurableToolStep, DurableWait, ReplaySafeAuthorization, StandaloneBeforeHookHost,
        UnsafeDurableAuthorization,
    },
    extension::{BeforeTool, Context, Hook, ToolCall},
    task::{Checkpoint, Task, TaskState, WaitFor},
    task_scheduler::{Handler, Registry, Step},
    task_store::Signal,
    task_tool::TaskTool,
};
use serde_json::{Value, json};
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;

fn spec(name: &str) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        description: String::new(),
        parameters: json!({"type":"object"}),
    }
}
struct Gate;
impl Handler for Gate {
    fn run(&self, task: &Task) -> Result<Step, String> {
        match task.checkpoint().phase.as_str() {
            "initial" => Ok(Step::Wait {
                checkpoint: Checkpoint {
                    phase: "gate".into(),
                    data: json!(null),
                },
                condition: WaitFor::Signal("go".into()),
                children: vec![],
            }),
            "gate" if task.delivered_signal().is_some() => Ok(Step::Succeed(
                serde_json::to_value(Outcome::Succeeded("gate".into())).unwrap(),
            )),
            _ => Err("invalid gate".into()),
        }
    }
}
struct Hooks {
    dir: PathBuf,
    cut: Option<String>,
}
#[async_trait]
impl Hook for Hooks {
    async fn before_tool(&self, _: &ToolCall, ctx: &Context<'_>) -> Result<BeforeTool, String> {
        ctx.memo.get_or_insert("before", json!("saved")).await?;
        if self.cut.as_deref() == Some("before") {
            mark_and_wait(&self.dir, "cut");
        }
        if self.cut.as_deref() == Some("abort-before") {
            std::fs::write(self.dir.join("hook-running"), b"before").unwrap();
            std::future::pending::<()>().await;
        }
        Ok(BeforeTool::Rewrite(json!({"bound":true})))
    }
    async fn after_tool(
        &self,
        call: &ToolCall,
        _: &Value,
        ctx: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        if call.arguments != json!({"bound":true})
            || ctx.memo.get("before").await? != Some(json!("saved"))
        {
            return Err("hook state lost".into());
        }
        if self.cut.as_deref() == Some("after") {
            mark_and_wait(&self.dir, "cut");
        }
        if self.cut.as_deref() == Some("abort-after") {
            std::fs::write(self.dir.join("hook-running"), b"after").unwrap();
            std::future::pending::<()>().await;
        }
        Ok(Some(json!("rewritten")))
    }
}
fn mark_and_wait(dir: &Path, name: &str) -> ! {
    std::fs::write(dir.join(name), b"ready").unwrap();
    loop {
        std::thread::park();
    }
}
struct UnsafeChild {
    dir: PathBuf,
    mode: String,
    cut: Option<String>,
}
#[async_trait]
impl Tool for UnsafeChild {
    fn spec(&self) -> ToolSpec {
        spec("unsafe")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for UnsafeChild {
    async fn execute_durable(
        &self,
        args: Value,
        context: ToolContext,
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        if args != json!({"bound":true}) {
            return Err("prepared arguments lost".into());
        }
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(self.dir.join("effects"))
            .map_err(|e| e.to_string())?;
        writeln!(
            f,
            "{}",
            if resume.checkpoint.is_some() {
                "resume"
            } else {
                "initial"
            }
        )
        .map_err(|e| e.to_string())?;
        f.sync_all().map_err(|e| e.to_string())?;
        if (self.cut.as_deref() == Some("execute") && resume.checkpoint.is_none())
            || (self.cut.as_deref() == Some("resume-execute") && resume.checkpoint.is_some())
        {
            mark_and_wait(&self.dir, "cut");
        }
        if let Some(data) = resume.checkpoint {
            let gate: AsyncTaskCall =
                serde_json::from_value(data["gate"].clone()).map_err(|e| e.to_string())?;
            if resume.ended != vec![gate.task_id()] {
                return Err("bad wake".into());
            }
            if self.mode == "abort" {
                std::fs::write(self.dir.join("running"), b"ready").unwrap();
                context.cancellation.cancelled().await;
                return Ok(DurableToolStep::Complete(Outcome::Succeeded(
                    "abort-job".into(),
                )));
            }
            if self.mode == "two-parks" && data["job"] == "external-17" {
                let next = host.start_child("gate2", "gate", json!({})).await?;
                return Ok(DurableToolStep::Park {
                    checkpoint: json!({"gate":next,"job":"external-18"}),
                    wait: DurableWait::All(vec![next]),
                });
            }
            let outcome = match self.mode.as_str() {
                "pending" => Outcome::Pending {
                    reference: "remote-job".into(),
                },
                "unknown" => Outcome::Unknown("unknown-job".into()),
                _ => Outcome::Succeeded("raw".into()),
            };
            Ok(DurableToolStep::Complete(outcome))
        } else {
            let gate = host.start_child("gate", "gate", json!({})).await?;
            if self.mode == "finalized-uncertain" {
                let bad = host.start_child("bad", "bad", json!({})).await?;
                Ok(DurableToolStep::Park {
                    checkpoint: json!({"gate":gate,"job":"external-17"}),
                    wait: DurableWait::Any(vec![gate, bad]),
                })
            } else {
                Ok(DurableToolStep::Park {
                    checkpoint: json!({"gate":gate,"job":"external-17"}),
                    wait: DurableWait::All(vec![gate]),
                })
            }
        }
    }
}
struct BadLeaf {
    dir: PathBuf,
}
#[async_trait]
impl Tool for BadLeaf {
    fn spec(&self) -> ToolSpec {
        spec("bad")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for BadLeaf {
    async fn execute_durable(
        &self,
        _: Value,
        _: ToolContext,
        _: DurableToolResume,
        _: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        std::fs::write(self.dir.join("bad-running"), b"ready").unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while !self.dir.join("release-bad").exists() {
            if Instant::now() >= deadline {
                return Err("bad child release timed out".into());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        Ok(DurableToolStep::Complete(Outcome::Pending {
            reference: "bad-job".into(),
        }))
    }
}
struct Parent {
    mode: String,
    dir: PathBuf,
}
#[async_trait]
impl Tool for Parent {
    fn spec(&self) -> ToolSpec {
        spec("parent")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for Parent {
    async fn execute_durable(
        &self,
        _: Value,
        _: ToolContext,
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        if let Some(data) = resume.checkpoint {
            let child: AsyncTaskCall =
                serde_json::from_value(data["child"].clone()).map_err(|e| e.to_string())?;
            let actual = host.child_result(&child).await?;
            let matches = match (self.mode.as_str(), actual) {
                ("pending", Some(AsyncTaskResult::Uncertain(Outcome::Pending { reference }))) => {
                    reference == "remote-job"
                }
                ("unknown", Some(AsyncTaskResult::Uncertain(Outcome::Unknown(reason)))) => {
                    reason == "unknown-job"
                }
                (
                    "after-incomplete",
                    Some(AsyncTaskResult::AfterIncomplete(Outcome::Succeeded(value))),
                ) if value == "raw" => {
                    std::fs::write(self.dir.join("after-incomplete-observed"), b"yes").unwrap();
                    true
                }
                (
                    "finalized-uncertain",
                    Some(AsyncTaskResult::Uncertain(Outcome::Succeeded(value))),
                ) if value == "raw" => {
                    std::fs::write(self.dir.join("finalized-uncertain-observed"), b"yes").unwrap();
                    true
                }
                (
                    "after-incomplete-missing",
                    Some(AsyncTaskResult::Uncertain(Outcome::Unknown(_))),
                ) => {
                    std::fs::write(self.dir.join("unknown-observed"), b"yes").unwrap();
                    true
                }
                (_, Some(AsyncTaskResult::Outcome(Outcome::Succeeded(value)))) => {
                    value == "rewritten"
                }
                _ => false,
            };
            if !matches {
                return Err("child outcome mismatch".into());
            }
            Ok(DurableToolStep::Complete(Outcome::Succeeded(
                "parent-done".into(),
            )))
        } else {
            let child = host.start_child("child", "unsafe", json!({})).await?;
            if matches!(
                self.mode.as_str(),
                "after-incomplete-missing" | "after-incomplete-corrupt"
            ) {
                let sibling = host.start_child("sibling", "gate", json!({})).await?;
                Ok(DurableToolStep::Park {
                    checkpoint: json!({"child":child, "sibling":sibling}),
                    wait: DurableWait::All(vec![child, sibling]),
                })
            } else {
                Ok(DurableToolStep::Park {
                    checkpoint: json!({"child":child}),
                    wait: DurableWait::All(vec![child]),
                })
            }
        }
    }
}
fn build(
    dir: &Path,
    mode: &str,
    cut: Option<&str>,
    identity: &str,
) -> (StandaloneBeforeHookHost, Arc<TaskTool>) {
    build_with_parent(dir, mode, cut, identity, true)
}
fn build_with_parent(
    dir: &Path,
    mode: &str,
    cut: Option<&str>,
    identity: &str,
    parent_safe: bool,
) -> (StandaloneBeforeHookHost, Arc<TaskTool>) {
    let handler: Arc<dyn Handler> = Arc::new(Gate);
    let mut registry = Registry::default();
    assert!(registry.register_shared("gate", true, handler.clone()));
    assert!(registry.set_abort("gate", |_: &Task| Ok(Step::Succeed(json!(null)))));
    let gate = Arc::new(TaskTool::new(spec("gate"), "gate", true, 1, handler));
    let mut host = StandaloneBeforeHookHost::open(dir.join("db"), registry).unwrap();
    let mut child_tools = vec![gate.clone()];
    if mode == "finalized-uncertain" {
        let bad = host
            .register_unsafe_durable_tool_with_hook(
                Arc::new(BadLeaf { dir: dir.into() }),
                "bad-v1",
                1,
                vec![],
                "bad-hook",
                Arc::new(Pass),
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        child_tools.push(bad);
    }
    let child = host
        .register_unsafe_durable_tool_with_hook(
            Arc::new(UnsafeChild {
                dir: dir.into(),
                mode: mode.into(),
                cut: cut.map(str::to_owned),
            }),
            "unsafe-hooked",
            1,
            child_tools,
            identity,
            Arc::new(Hooks {
                dir: dir.into(),
                cut: cut.map(str::to_owned),
            }),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap();
    let parent: Arc<dyn DurableAsyncTool> = Arc::new(Parent {
        mode: mode.into(),
        dir: dir.into(),
    });
    let parent = if parent_safe {
        host.register_replay_safe_durable_tool_with_hook(
            parent,
            "parent",
            1,
            vec![child, gate],
            "parent-hook",
            Arc::new(Pass),
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap()
    } else {
        host.register_unsafe_durable_tool_with_hook(
            parent,
            "parent",
            1,
            vec![child, gate],
            "parent-hook",
            Arc::new(Pass),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap()
    };
    (host, parent)
}
struct Pass;
#[async_trait]
impl Hook for Pass {}
fn until(
    driver: &pi_rs_durable::task_driver::Driver,
    id: Uuid,
    p: impl Fn(&TaskState) -> bool,
) -> Task {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let task = driver.get(id).unwrap().unwrap();
        if p(task.state()) {
            return task;
        }
        assert!(Instant::now() < deadline, "{id}: {:?}", task.state());
        std::thread::sleep(Duration::from_millis(5));
    }
}
fn child(driver: &pi_rs_durable::task_driver::Driver, root: Uuid) -> Uuid {
    let task = until(driver, root, |s| matches!(s, TaskState::Waiting(_)));
    let call: AsyncTaskCall =
        serde_json::from_value(task.checkpoint().data["checkpoint"]["child"].clone()).unwrap();
    call.task_id()
}
fn gate(driver: &pi_rs_durable::task_driver::Driver, id: Uuid) -> Uuid {
    let task = until(driver, id, |s| matches!(s, TaskState::Waiting(_)));
    let call: AsyncTaskCall =
        serde_json::from_value(task.checkpoint().data["checkpoint"]["gate"].clone()).unwrap();
    until(driver, call.task_id(), |s| {
        matches!(s, TaskState::Waiting(_))
    });
    call.task_id()
}
fn signal(driver: &pi_rs_durable::task_driver::Driver, id: Uuid) {
    driver
        .signal(&Signal::new(Uuid::new_v4(), id, "go".into(), json!(null)))
        .unwrap();
}
#[test]
fn unsafe_hooked_nested_completes_with_raw_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let (host, parent) = build(dir.path(), "success", None, "hook-v1");
    let root = host.start_replay_safe_root(&parent, json!({}), 5).unwrap();
    let driver = host.into_driver();
    let child_id = child(&driver, root);
    let gate_id = gate(&driver, child_id);
    signal(&driver, gate_id);
    let done = until(&driver, root, |s| {
        matches!(
            s,
            TaskState::Succeeded(_) | TaskState::Failed(_) | TaskState::Cancelled
        )
    });
    assert_eq!(
        done.state(),
        &TaskState::Succeeded(
            serde_json::to_value(Outcome::Succeeded("parent-done".into())).unwrap()
        )
    );
    driver.shutdown().unwrap();
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let receipts =
        pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, child_id).unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].outcome, Outcome::Succeeded("raw".into()));
    assert_eq!(
        std::fs::read_to_string(dir.path().join("effects")).unwrap(),
        "initial\nresume\n"
    );
}

fn wait_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !path.exists() {
        assert!(Instant::now() < deadline, "missing {}", path.display());
        std::thread::sleep(Duration::from_millis(5));
    }
}
#[test]
fn kill_fixture() {
    let Some(dir) = std::env::var_os("PI_UNSAFE_HOOK_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let cut = std::env::var("PI_UNSAFE_HOOK_CUT").unwrap();
    let mode = std::env::var("PI_UNSAFE_HOOK_MODE").unwrap_or_else(|_| "success".into());
    let limit = std::env::var("PI_UNSAFE_HOOK_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let (host, parent) = build(&dir, &mode, Some(&cut), "hook-v1");
    let root = host
        .start_replay_safe_root(&parent, json!({}), limit)
        .unwrap();
    let driver = host.into_driver();
    let child_id = child(&driver, root);
    if cut == "before" || cut == "execute" {
        wait_file(&dir.join("cut"));
    } else {
        let gate_id = gate(&driver, child_id);
        if matches!(cut.as_str(), "after" | "resume-execute") {
            signal(&driver, gate_id);
            wait_file(&dir.join("cut"));
        } else if cut == "second-park" {
            signal(&driver, gate_id);
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                let task = driver.get(child_id).unwrap().unwrap();
                if matches!(task.state(), TaskState::Waiting(_))
                    && task.checkpoint().data["checkpoint"]["job"] == "external-18"
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "second park not committed: {:?}",
                    task.state()
                );
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
    std::fs::write(
        dir.join("ready.tmp"),
        serde_json::to_vec(&(root, child_id)).unwrap(),
    )
    .unwrap();
    std::fs::rename(dir.join("ready.tmp"), dir.join("ready")).unwrap();
    loop {
        std::thread::park();
    }
}
fn killed(dir: &Path, cut: &str) -> (Uuid, Uuid) {
    killed_mode(dir, cut, "success", 5)
}
fn killed_mode(dir: &Path, cut: &str, mode: &str, limit: u32) -> (Uuid, Uuid) {
    let mut process = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "kill_fixture"])
        .env("PI_UNSAFE_HOOK_DIR", dir)
        .env("PI_UNSAFE_HOOK_CUT", cut)
        .env("PI_UNSAFE_HOOK_MODE", mode)
        .env("PI_UNSAFE_HOOK_LIMIT", limit.to_string())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !dir.join("ready").exists() {
        if Instant::now() >= deadline || process.try_wait().unwrap().is_some() {
            let _ = process.kill();
            let _ = process.wait();
            panic!("fixture failed at {cut}");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    process.kill().unwrap();
    process.wait().unwrap();
    serde_json::from_slice(&std::fs::read(dir.join("ready")).unwrap()).unwrap()
}
#[test]
fn unsafe_nested_hook_phases_survive_kill_reopen() {
    for cut in ["before", "park", "after"] {
        let dir = tempfile::tempdir().unwrap();
        let (root, child_id) = killed(dir.path(), cut);
        let (host, _) = build(dir.path(), "success", None, "hook-v1");
        let driver = host.into_driver();
        if cut != "after" {
            let gate_id = gate(&driver, child_id);
            signal(&driver, gate_id);
        }
        let done = until(&driver, root, |s| {
            matches!(s, TaskState::Succeeded(_) | TaskState::Failed(_))
        });
        assert_eq!(
            done.state(),
            &TaskState::Succeeded(
                serde_json::to_value(Outcome::Succeeded("parent-done".into())).unwrap()
            ),
            "{cut}"
        );
        driver.shutdown().unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("effects")).unwrap(),
            "initial\nresume\n",
            "{cut}"
        );
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let receipts =
            pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, child_id).unwrap();
        assert_eq!(receipts.len(), 1, "{cut}");
        assert_eq!(receipts[0].outcome, Outcome::Succeeded("raw".into()));
    }
}
#[test]
fn unsafe_nested_pending_and_unknown_are_preserved() {
    for mode in ["pending", "unknown"] {
        let dir = tempfile::tempdir().unwrap();
        let (host, parent) = build(dir.path(), mode, None, "hook-v1");
        let root = host.start_replay_safe_root(&parent, json!({}), 5).unwrap();
        let driver = host.into_driver();
        let child_id = child(&driver, root);
        let gate_id = gate(&driver, child_id);
        signal(&driver, gate_id);
        let done = until(&driver, root, |s| {
            matches!(s, TaskState::Succeeded(_) | TaskState::Failed(_))
        });
        assert!(
            matches!(done.state(),TaskState::Failed(reason) if reason.contains("reconciliation")),
            "{mode}: {:?}",
            done.state()
        );
        driver.shutdown().unwrap();
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let receipts =
            pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, child_id).unwrap();
        let expected = if mode == "pending" {
            Outcome::Pending {
                reference: "remote-job".into(),
            }
        } else {
            Outcome::Unknown("unknown-job".into())
        };
        assert_eq!(receipts[0].outcome, expected);
    }
}
#[test]
fn unsafe_nested_abort_preserves_joined_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let (host, parent) = build(dir.path(), "abort", None, "hook-v1");
    let root = host.start_replay_safe_root(&parent, json!({}), 5).unwrap();
    let driver = host.into_driver();
    let child_id = child(&driver, root);
    let gate_id = gate(&driver, child_id);
    signal(&driver, gate_id);
    wait_file(&dir.path().join("running"));
    driver.request_abort(root).unwrap();
    until(&driver, root, |s| matches!(s, TaskState::Cancelled));
    driver.shutdown().unwrap();
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let receipts =
        pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, child_id).unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].outcome, Outcome::Succeeded("abort-job".into()));
}
#[test]
fn changed_hook_identity_cannot_resume_unsafe_child() {
    for cut in ["before", "park", "after"] {
        let dir = tempfile::tempdir().unwrap();
        let (root, child_id) = killed(dir.path(), cut);
        let (host, _) = build(dir.path(), "success", None, "hook-v2");
        let driver = host.into_driver();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !driver
            .blocked_tasks()
            .unwrap()
            .iter()
            .any(|task| task.id == child_id)
        {
            assert!(Instant::now() < deadline, "child was not blocked at {cut}");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(matches!(
            driver.get(root).unwrap().unwrap().state(),
            TaskState::Waiting(_)
        ));
        let effects = std::fs::read_to_string(dir.path().join("effects")).ok();
        match cut {
            "before" => assert!(effects.is_none()),
            "park" => assert_eq!(effects.as_deref(), Some("initial\n")),
            _ => assert_eq!(effects.as_deref(), Some("initial\nresume\n")),
        }
        driver.shutdown().unwrap();
    }
}
#[test]
fn unsafe_tool_interrupted_after_intent_is_not_replayed() {
    for cut in ["execute", "resume-execute"] {
        let dir = tempfile::tempdir().unwrap();
        let (root, child_id) = killed(dir.path(), cut);
        let (host, _) = build(dir.path(), "success", None, "hook-v1");
        let driver = host.into_driver();
        let task = until(&driver, child_id, |s| matches!(s, TaskState::Failed(_)));
        assert!(
            matches!(task.state(),TaskState::Failed(reason) if reason.contains("reconciliation"))
        );
        let parent = until(&driver, root, |s| matches!(s, TaskState::Failed(_)));
        assert!(matches!(parent.state(), TaskState::Failed(_)));
        driver.shutdown().unwrap();
        let expected = if cut == "execute" {
            "initial\n"
        } else {
            "initial\nresume\n"
        };
        assert_eq!(
            std::fs::read_to_string(dir.path().join("effects")).unwrap(),
            expected
        );
    }
}
#[test]
fn abort_parked_unsafe_hooked_child_retains_proposal_without_resume() {
    let dir = tempfile::tempdir().unwrap();
    let (host, parent) = build(dir.path(), "success", None, "hook-v1");
    let root = host.start_replay_safe_root(&parent, json!({}), 5).unwrap();
    let driver = host.into_driver();
    let child_id = child(&driver, root);
    let gate_id = gate(&driver, child_id);
    driver.request_abort(root).unwrap();
    for id in [root, child_id, gate_id] {
        until(&driver, id, |s| matches!(s, TaskState::Cancelled));
    }
    driver.shutdown().unwrap();
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let proposals =
        pi_rs_durable::async_task_tool::read_unsafe_durable_park_evidence(&store, child_id)
            .unwrap();
    assert_eq!(proposals.len(), 1);
    assert_eq!(proposals[0].checkpoint["job"], "external-17");
    assert!(
        pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, child_id)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("effects")).unwrap(),
        "initial\n"
    );
}
#[test]
fn abort_fences_running_hooks_and_preserves_raw_outcome() {
    for cut in ["abort-before", "abort-after"] {
        let dir = tempfile::tempdir().unwrap();
        let (host, parent) = build(dir.path(), "success", Some(cut), "hook-v1");
        let root = host.start_replay_safe_root(&parent, json!({}), 5).unwrap();
        let driver = host.into_driver();
        let child_id = child(&driver, root);
        if cut == "abort-after" {
            let gate_id = gate(&driver, child_id);
            signal(&driver, gate_id);
        }
        wait_file(&dir.path().join("hook-running"));
        driver.request_abort(root).unwrap();
        until(&driver, root, |s| matches!(s, TaskState::Cancelled));
        until(&driver, child_id, |s| matches!(s, TaskState::Cancelled));
        driver.shutdown().unwrap();
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let receipts =
            pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, child_id).unwrap();
        if cut == "abort-before" {
            assert!(receipts.is_empty());
            assert!(!dir.path().join("effects").exists());
        } else {
            assert_eq!(receipts.len(), 1);
            assert_eq!(receipts[0].outcome, Outcome::Succeeded("raw".into()));
            assert_eq!(
                std::fs::read_to_string(dir.path().join("effects")).unwrap(),
                "initial\nresume\n"
            );
        }
    }
}
#[test]
fn unsafe_hooked_wake_refuses_missing_park_proof() {
    let dir = tempfile::tempdir().unwrap();
    let (root, child_id) = killed(dir.path(), "park");
    {
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let key = format!("durable/unsafe-park-async/{child_id}/park").into_bytes();
        assert!(store.get(&key).unwrap().is_some());
        store
            .commit(&[], &[durable_store::Mutation::Delete { key }])
            .unwrap();
    }
    let (host, _) = build(dir.path(), "success", None, "hook-v1");
    let driver = host.into_driver();
    let gate_id = gate(&driver, child_id);
    signal(&driver, gate_id);
    let failed = until(&driver, child_id, |s| matches!(s, TaskState::Failed(_)));
    assert!(matches!(failed.state(),TaskState::Failed(reason) if reason.contains("park proof")));
    let _ = until(&driver, root, |s| matches!(s, TaskState::Failed(_)));
    driver.shutdown().unwrap();
    assert_eq!(
        std::fs::read_to_string(dir.path().join("effects")).unwrap(),
        "initial\n"
    );
}
#[test]
fn unsafe_parent_and_unsafe_child_hooks_run_on_same_driver() {
    let dir = tempfile::tempdir().unwrap();
    let (host, parent) = build_with_parent(dir.path(), "success", None, "hook-v1", false);
    let root = host
        .start_unsafe_durable_root(&parent, json!({}), 5)
        .unwrap();
    let driver = host.into_driver();
    let child_id = child(&driver, root);
    let gate_id = gate(&driver, child_id);
    signal(&driver, gate_id);
    let done = until(&driver, root, |s| {
        matches!(s, TaskState::Succeeded(_) | TaskState::Failed(_))
    });
    assert_eq!(
        done.state(),
        &TaskState::Succeeded(
            serde_json::to_value(Outcome::Succeeded("parent-done".into())).unwrap()
        )
    );
    driver.shutdown().unwrap();
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let child_receipts =
        pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, child_id).unwrap();
    let root_receipts =
        pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, root).unwrap();
    assert_eq!(child_receipts[0].outcome, Outcome::Succeeded("raw".into()));
    assert_eq!(
        root_receipts[0].outcome,
        Outcome::Succeeded("parent-done".into())
    );
}
#[test]
fn foreign_hooked_unsafe_child_is_rejected() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let mut host_a =
        StandaloneBeforeHookHost::open(first.path().join("db"), Registry::default()).unwrap();
    let foreign = host_a
        .register_unsafe_durable_tool_with_hook(
            Arc::new(UnsafeChild {
                dir: first.path().into(),
                mode: "success".into(),
                cut: None,
            }),
            "unsafe-hooked",
            1,
            vec![],
            "hook-v1",
            Arc::new(Hooks {
                dir: first.path().into(),
                cut: None,
            }),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap();
    let mut host_b =
        StandaloneBeforeHookHost::open(second.path().join("db"), Registry::default()).unwrap();
    assert!(
        host_b
            .register_replay_safe_durable_tool_with_hook(
                Arc::new(Parent {
                    mode: "success".into(),
                    dir: second.path().into(),
                }),
                "parent",
                1,
                vec![foreign],
                "parent-hook",
                Arc::new(Pass),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .is_err()
    );
}

#[test]
fn unsafe_wake_rejects_replaced_task_and_park_arguments() {
    for (cut, mode, expected_effects) in [
        ("park", "success", "initial\n"),
        ("second-park", "two-parks", "initial\nresume\n"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (root, child_id) = killed_mode(dir.path(), cut, mode, 5);
        {
            let store = durable_store::Store::open(dir.path().join("db")).unwrap();
            let mut task_key = b"durable/task/".to_vec();
            task_key.extend_from_slice(child_id.as_bytes());
            let park_key = format!("durable/unsafe-park-async/{child_id}/park").into_bytes();
            let task_entry = store.get(&task_key).unwrap().unwrap();
            let park_entry = store.get(&park_key).unwrap().unwrap();
            let mut task: Value = serde_json::from_slice(&task_entry.value).unwrap();
            let mut proof: Value = serde_json::from_slice(&park_entry.value).unwrap();
            assert_eq!(
                task["checkpoint"]["data"]["arguments"],
                json!({"bound":true})
            );
            assert_eq!(
                proof["checkpoint"]["data"]["arguments"],
                json!({"bound":true})
            );
            task["checkpoint"]["data"]["arguments"] = json!({"bound":false});
            proof["checkpoint"]["data"]["arguments"] = json!({"bound":false});
            store
                .commit(
                    &[
                        durable_store::Check {
                            key: task_key.clone(),
                            expected: durable_store::Expected::Version(task_entry.version),
                        },
                        durable_store::Check {
                            key: park_key.clone(),
                            expected: durable_store::Expected::Version(park_entry.version),
                        },
                    ],
                    &[
                        durable_store::Mutation::Put {
                            key: task_key,
                            value: serde_json::to_vec(&task).unwrap(),
                        },
                        durable_store::Mutation::Put {
                            key: park_key,
                            value: serde_json::to_vec(&proof).unwrap(),
                        },
                    ],
                )
                .unwrap();
        }
        let (host, _) = build(dir.path(), mode, None, "hook-v1");
        let driver = host.into_driver();
        let gate_id = gate(&driver, child_id);
        signal(&driver, gate_id);
        let failed = until(&driver, child_id, |s| matches!(s, TaskState::Failed(_)));
        assert!(
            matches!(failed.state(),TaskState::Failed(reason) if reason.contains("prepared arguments changed")),
            "{cut}: {:?}",
            failed.state()
        );
        until(&driver, root, |s| matches!(s, TaskState::Failed(_)));
        driver.shutdown().unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("effects")).unwrap(),
            expected_effects
        );
    }
}

#[test]
fn unsafe_after_restart_limit_exposes_after_incomplete_to_parent() {
    let dir = tempfile::tempdir().unwrap();
    let (root, child_id) = killed_mode(dir.path(), "after", "after-incomplete", 1);
    let (host, _) = build(dir.path(), "after-incomplete", None, "hook-v1");
    let driver = host.into_driver();
    let child_task = until(&driver, child_id, |s| matches!(s, TaskState::Failed(_)));
    assert!(
        matches!(child_task.state(),TaskState::Failed(reason) if reason.contains("hook restart limit"))
    );
    let root_task = until(&driver, root, |s| matches!(s, TaskState::Failed(_)));
    assert!(
        matches!(root_task.state(),TaskState::Failed(reason) if reason.contains("reconciliation"))
    );
    assert!(
        dir.path().join("after-incomplete-observed").exists(),
        "parent did not observe AfterIncomplete(raw)"
    );
    driver.shutdown().unwrap();
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let receipts =
        pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, child_id).unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].outcome, Outcome::Succeeded("raw".into()));
    assert_eq!(
        std::fs::read_to_string(dir.path().join("effects")).unwrap(),
        "initial\nresume\n"
    );
}

#[test]
fn unsafe_after_incomplete_requires_intact_receipt_and_missing_is_uncertain() {
    for mode in ["after-incomplete-missing", "after-incomplete-corrupt"] {
        let dir = tempfile::tempdir().unwrap();
        let (root, child_id) = killed_mode(dir.path(), "after", mode, 1);
        let (host, _) = build(dir.path(), mode, None, "hook-v1");
        let driver = host.into_driver();
        until(&driver, child_id, |s| matches!(s, TaskState::Failed(_)));
        let root_task = until(&driver, root, |s| matches!(s, TaskState::Waiting(_)));
        let sibling: AsyncTaskCall =
            serde_json::from_value(root_task.checkpoint().data["checkpoint"]["sibling"].clone())
                .unwrap();
        until(&driver, sibling.task_id(), |s| {
            matches!(s, TaskState::Waiting(_))
        });
        {
            let store = durable_store::Store::open(dir.path().join("db")).unwrap();
            let prefix = format!("durable/unsafe-park-async/{child_id}/");
            let (key, entry) = store
                .list(prefix.as_bytes())
                .unwrap()
                .into_iter()
                .find(|(key, _)| key.ends_with(b"/intent/receipt"))
                .unwrap();
            let mutation = if mode == "after-incomplete-missing" {
                durable_store::Mutation::Delete { key: key.clone() }
            } else {
                let mut receipt: Value = serde_json::from_slice(&entry.value).unwrap();
                receipt["outcome"] =
                    serde_json::to_value(Outcome::Succeeded("forged".into())).unwrap();
                durable_store::Mutation::Put {
                    key: key.clone(),
                    value: serde_json::to_vec(&receipt).unwrap(),
                }
            };
            store
                .commit(
                    &[durable_store::Check {
                        key,
                        expected: durable_store::Expected::Version(entry.version),
                    }],
                    &[mutation],
                )
                .unwrap();
        }
        signal(&driver, sibling.task_id());
        let parent = until(&driver, root, |s| matches!(s, TaskState::Failed(_)));
        if mode == "after-incomplete-missing" {
            assert!(dir.path().join("unknown-observed").exists());
            assert!(
                matches!(parent.state(),TaskState::Failed(reason) if reason.contains("reconciliation"))
            );
        } else {
            assert!(!dir.path().join("unknown-observed").exists());
            assert!(!dir.path().join("after-incomplete-observed").exists());
            assert!(
                matches!(parent.state(),TaskState::Failed(reason) if reason.contains("receipt mismatch")),
                "{:?}",
                parent.state()
            );
        }
        driver.shutdown().unwrap();
    }
}

#[test]
fn finalized_after_then_uncertain_descendant_is_not_after_incomplete() {
    let dir = tempfile::tempdir().unwrap();
    let (host, parent) = build(dir.path(), "finalized-uncertain", None, "hook-v1");
    let root = host.start_replay_safe_root(&parent, json!({}), 5).unwrap();
    let driver = host.into_driver();
    let child_id = child(&driver, root);
    let gate_id = gate(&driver, child_id);
    wait_file(&dir.path().join("bad-running"));
    signal(&driver, gate_id);
    until(&driver, child_id, |s| matches!(s, TaskState::Completing(_)));
    std::fs::write(dir.path().join("release-bad"), b"go").unwrap();
    let failed = until(&driver, child_id, |s| matches!(s, TaskState::Failed(_)));
    assert!(
        matches!(failed.state(),TaskState::Failed(reason) if reason.contains("reconciliation"))
    );
    until(&driver, root, |s| matches!(s, TaskState::Failed(_)));
    assert!(dir.path().join("finalized-uncertain-observed").exists());
    assert!(!dir.path().join("after-incomplete-observed").exists());
    driver.shutdown().unwrap();
}
