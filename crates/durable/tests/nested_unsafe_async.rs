use async_trait::async_trait;
use pi_rs_core::{Outcome, Tool, ToolContext, ToolSpec};
use pi_rs_durable::{
    async_task_tool::{
        AsyncTaskCall, AsyncTaskResult, DurableAsyncTool, DurableTaskHost, DurableToolResume,
        DurableToolStep, DurableWait, ReplaySafeAuthorization, StandaloneNoHooksHost,
        UnhookedAuthorization, UnsafeDurableAuthorization,
    },
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
                serde_json::to_value(Outcome::Succeeded("sibling".into())).unwrap(),
            )),
            _ => Err("invalid gate wake".into()),
        }
    }
}
struct UnsafeChild {
    dir: PathBuf,
    mode: String,
}
#[async_trait]
impl Tool for UnsafeChild {
    fn spec(&self) -> ToolSpec {
        spec("unsafe-child")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for UnsafeChild {
    async fn execute_durable(
        &self,
        _: Value,
        context: ToolContext,
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        let phase = if resume.checkpoint.is_some() {
            "resumed"
        } else {
            "initial"
        };
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("child-effects"))
            .map_err(|e| e.to_string())?;
        writeln!(f, "{phase}").map_err(|e| e.to_string())?;
        f.sync_all().map_err(|e| e.to_string())?;
        if phase == "initial" && self.mode == "interrupt" {
            std::fs::write(self.dir.join("running.tmp"), "running").map_err(|e| e.to_string())?;
            std::fs::rename(self.dir.join("running.tmp"), self.dir.join("running"))
                .map_err(|e| e.to_string())?;
            loop {
                std::thread::park();
            }
        }
        match resume.checkpoint {
            None => {
                let call = host.start_child("gate", "gate", json!({})).await?;
                Ok(DurableToolStep::Park {
                    checkpoint: json!({"gate":call,"external":"job-17"}),
                    wait: DurableWait::All(vec![call]),
                })
            }
            Some(data) => {
                let call: AsyncTaskCall =
                    serde_json::from_value(data["gate"].clone()).map_err(|e| e.to_string())?;
                if resume.ended != vec![call.task_id()] {
                    return Err("invalid child wake".into());
                }
                if !matches!(
                    host.child_result(&call).await?,
                    Some(AsyncTaskResult::Outcome(Outcome::Succeeded(_)))
                ) {
                    return Err("gate result missing".into());
                }
                if self.mode == "abort-result" {
                    std::fs::write(self.dir.join("running.tmp"), "running")
                        .map_err(|e| e.to_string())?;
                    std::fs::rename(self.dir.join("running.tmp"), self.dir.join("running"))
                        .map_err(|e| e.to_string())?;
                    context.cancellation.cancelled().await;
                    return Ok(DurableToolStep::Complete(Outcome::Succeeded(
                        "after-abort-id".into(),
                    )));
                }
                let outcome = if self.mode == "pending" {
                    Outcome::Pending {
                        reference: "remote-17".into(),
                    }
                } else {
                    Outcome::Succeeded("child".into())
                };
                Ok(DurableToolStep::Complete(outcome))
            }
        }
    }
}
struct Parent {
    expected: String,
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
        match resume.checkpoint {
            None => {
                let child = host.start_child("child", "unsafe-child", json!({})).await?;
                let sibling = host.start_child("sibling", "sibling", json!({})).await?;
                Ok(DurableToolStep::Park {
                    checkpoint: json!({"child":child,"sibling":sibling}),
                    wait: DurableWait::All(vec![child, sibling]),
                })
            }
            Some(data) => {
                let child: AsyncTaskCall =
                    serde_json::from_value(data["child"].clone()).map_err(|e| e.to_string())?;
                let sibling: AsyncTaskCall =
                    serde_json::from_value(data["sibling"].clone()).map_err(|e| e.to_string())?;
                if resume.ended.len() != 2
                    || !resume.ended.contains(&child.task_id())
                    || !resume.ended.contains(&sibling.task_id())
                {
                    return Err("incorrect all wake".into());
                }
                match (self.expected.as_str(), host.child_result(&child).await?) {
                    ("success", Some(AsyncTaskResult::Outcome(Outcome::Succeeded(value))))
                        if value == "child" =>
                    {
                        ()
                    }
                    (
                        "pending",
                        Some(AsyncTaskResult::Uncertain(Outcome::Pending { reference })),
                    ) if reference == "remote-17" => (),
                    ("interrupt", Some(AsyncTaskResult::Uncertain(Outcome::Unknown(reason))))
                        if reason.contains("reconciliation") =>
                    {
                        ()
                    }
                    (_, other) => return Err(format!("unexpected child result: {other:?}")),
                }
                if !matches!(
                    host.child_result(&sibling).await?,
                    Some(AsyncTaskResult::Outcome(Outcome::Succeeded(_)))
                ) {
                    return Err("sibling result missing".into());
                }
                Ok(DurableToolStep::Complete(Outcome::Succeeded(
                    "parent-done".into(),
                )))
            }
        }
    }
}
fn build(
    dir: &Path,
    parent_safe: bool,
    mode: &str,
    allow_hang: bool,
) -> (StandaloneNoHooksHost, Arc<TaskTool>) {
    let gate: Arc<dyn Handler> = Arc::new(Gate);
    let mut registry = Registry::default();
    assert!(registry.register_shared("nested-gate", true, gate.clone()));
    assert!(registry.set_abort("nested-gate", |_: &Task| Ok(Step::Succeed(json!(null)))));
    let gate_tool = Arc::new(TaskTool::new(
        spec("gate"),
        "nested-gate",
        true,
        1,
        gate.clone(),
    ));
    let sibling_tool = Arc::new(TaskTool::new(spec("sibling"), "nested-gate", true, 1, gate));
    let mut host = StandaloneNoHooksHost::open(
        dir.join("db"),
        registry,
        UnhookedAuthorization::standalone_only(),
    )
    .unwrap();
    let child_mode = if mode == "interrupt" && !allow_hang {
        "success"
    } else {
        mode
    };
    let child = host
        .register_unsafe_durable_tool(
            Arc::new(UnsafeChild {
                dir: dir.into(),
                mode: child_mode.into(),
            }),
            "nested-unsafe-v1",
            1,
            vec![gate_tool],
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
        .unwrap();
    let parent: Arc<dyn DurableAsyncTool> = Arc::new(Parent {
        expected: mode.into(),
    });
    let tool = if parent_safe {
        host.register_replay_safe_durable_tool(
            parent,
            "nested-parent-v1",
            1,
            vec![child, sibling_tool],
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap()
    } else {
        host.register_unsafe_durable_tool(
            parent,
            "nested-parent-v1",
            1,
            vec![child, sibling_tool],
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
        .unwrap()
    };
    (host, tool)
}
fn until(
    driver: &pi_rs_durable::task_driver::Driver,
    id: Uuid,
    condition: impl Fn(&TaskState) -> bool,
) -> Task {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let task = driver.get(id).unwrap().unwrap();
        if condition(task.state()) {
            return task;
        }
        assert!(
            Instant::now() < deadline,
            "task {id} stuck: {:?}",
            task.state()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}
fn parent_children(driver: &pi_rs_durable::task_driver::Driver, root: Uuid) -> (Uuid, Uuid) {
    let parent = until(driver, root, |s| matches!(s, TaskState::Waiting(_)));
    let data = &parent.checkpoint().data["checkpoint"];
    let child: AsyncTaskCall = serde_json::from_value(data["child"].clone()).unwrap();
    let sibling: AsyncTaskCall = serde_json::from_value(data["sibling"].clone()).unwrap();
    until(driver, sibling.task_id(), |s| {
        matches!(s, TaskState::Waiting(_))
    });
    (child.task_id(), sibling.task_id())
}
fn child_gate(driver: &pi_rs_durable::task_driver::Driver, id: Uuid) -> Uuid {
    let child = until(driver, id, |s| matches!(s, TaskState::Waiting(_)));
    let gate: AsyncTaskCall =
        serde_json::from_value(child.checkpoint().data["checkpoint"]["gate"].clone()).unwrap();
    until(driver, gate.task_id(), |s| {
        matches!(s, TaskState::Waiting(_))
    });
    gate.task_id()
}
fn signal(driver: &pi_rs_durable::task_driver::Driver, id: Uuid) {
    driver
        .signal(&Signal::new(Uuid::new_v4(), id, "go".into(), json!(null)))
        .unwrap();
}

#[test]
fn process_fixture() {
    let Some(dir) = std::env::var_os("PI_NESTED_UNSAFE_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let mode = std::env::var("PI_NESTED_UNSAFE_MODE").unwrap();
    let safe = std::env::var("PI_NESTED_PARENT_SAFE").unwrap() == "1";
    let (host, tool) = build(&dir, safe, &mode, true);
    let root = if safe {
        host.start_replay_safe_root(&tool, json!({}), 3).unwrap()
    } else {
        host.start_unsafe_durable_root(&tool, json!({}), 3).unwrap()
    };
    let driver = host.into_driver();
    let (child, sibling) = parent_children(&driver, root);
    if mode == "interrupt" {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !dir.join("running").exists() {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
    } else {
        child_gate(&driver, child);
    }
    std::fs::write(
        dir.join("ready.tmp"),
        serde_json::to_vec(&(root, child, sibling)).unwrap(),
    )
    .unwrap();
    std::fs::rename(dir.join("ready.tmp"), dir.join("ready")).unwrap();
    loop {
        std::thread::park();
    }
}
fn kill_fixture(dir: &Path, safe: bool, mode: &str) -> (Uuid, Uuid, Uuid) {
    let mut process = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("process_fixture")
        .env("PI_NESTED_UNSAFE_DIR", dir)
        .env("PI_NESTED_UNSAFE_MODE", mode)
        .env("PI_NESTED_PARENT_SAFE", if safe { "1" } else { "0" })
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !dir.join("ready").exists() {
        if Instant::now() >= deadline || process.try_wait().unwrap().is_some() {
            let _ = process.kill();
            let _ = process.wait();
            panic!("fixture did not reach {mode}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    process.kill().unwrap();
    process.wait().unwrap();
    serde_json::from_slice(&std::fs::read(dir.join("ready")).unwrap()).unwrap()
}
#[test]
fn unsafe_child_park_kill_reopen_and_sibling_all_wait() {
    for parent_safe in [false, true] {
        for mode in ["success", "pending"] {
            let dir = tempfile::tempdir().unwrap();
            let (root, child, sibling) = kill_fixture(dir.path(), parent_safe, mode);
            let (host, _) = build(dir.path(), parent_safe, mode, false);
            let driver = host.into_driver();
            let gate = child_gate(&driver, child);
            signal(&driver, gate);
            until(&driver, child, |s| {
                matches!(s, TaskState::Succeeded(_) | TaskState::Failed(_))
            });
            assert!(
                matches!(
                    driver.get(root).unwrap().unwrap().state(),
                    TaskState::Waiting(_)
                ),
                "all wait must retain the live sibling"
            );
            signal(&driver, sibling);
            if mode == "success" {
                let task = until(&driver, root, |s| matches!(s, TaskState::Succeeded(_)));
                assert_eq!(
                    task.state(),
                    &TaskState::Succeeded(
                        serde_json::to_value(Outcome::Succeeded("parent-done".into())).unwrap()
                    )
                );
            } else {
                let task = until(&driver, root, |s| matches!(s, TaskState::Failed(_)));
                assert!(
                    matches!(task.state(), TaskState::Failed(reason) if reason.contains("reconciliation") && reason.contains(&child.to_string()))
                );
            }
            assert_eq!(
                std::fs::read_to_string(dir.path().join("child-effects")).unwrap(),
                "initial\nresumed\n"
            );
            driver.shutdown().unwrap();
        }
    }
}
#[test]
fn interrupted_unsafe_child_is_uncertain_and_sibling_still_waits() {
    for parent_safe in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (root, child, sibling) = kill_fixture(dir.path(), parent_safe, "interrupt");
        let (host, _) = build(dir.path(), parent_safe, "interrupt", false);
        let driver = host.into_driver();
        until(
            &driver,
            child,
            |s| matches!(s, TaskState::Failed(reason) if reason.contains("reconciliation")),
        );
        assert!(matches!(
            driver.get(root).unwrap().unwrap().state(),
            TaskState::Waiting(_)
        ));
        signal(&driver, sibling);
        let parent = until(&driver, root, |s| matches!(s, TaskState::Failed(_)));
        assert!(
            matches!(parent.state(), TaskState::Failed(reason) if reason.contains("reconciliation") && reason.contains(&child.to_string()))
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("child-effects")).unwrap(),
            "initial\n"
        );
        driver.shutdown().unwrap();
    }
}
#[test]
fn parent_abort_after_kill_drains_unsafe_child_and_sibling() {
    for parent_safe in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (root, child, sibling) = kill_fixture(dir.path(), parent_safe, "success");
        let (host, _) = build(dir.path(), parent_safe, "success", false);
        let driver = host.into_driver();
        let gate = child_gate(&driver, child);
        driver.request_abort(root).unwrap();
        for id in [root, child, sibling, gate] {
            until(&driver, id, |s| matches!(s, TaskState::Cancelled));
        }
        assert_eq!(
            std::fs::read_to_string(dir.path().join("child-effects")).unwrap(),
            "initial\n"
        );
        driver.shutdown().unwrap();
    }
}

#[test]
fn parent_abort_cancels_running_unsafe_child_but_retains_joined_result() {
    for safe in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (host, root_tool) = build(dir.path(), safe, "abort-result", false);
        let root = if safe {
            host.start_replay_safe_root(&root_tool, json!({}), 3)
                .unwrap()
        } else {
            host.start_unsafe_durable_root(&root_tool, json!({}), 3)
                .unwrap()
        };
        let driver = host.into_driver();
        let (child, sibling) = parent_children(&driver, root);
        let gate = child_gate(&driver, child);
        signal(&driver, gate);
        let deadline = Instant::now() + Duration::from_secs(15);
        while !dir.path().join("running").exists() {
            assert!(Instant::now() < deadline, "child did not resume");
            std::thread::sleep(Duration::from_millis(5));
        }
        driver.request_abort(root).unwrap();
        for id in [root, child, sibling] {
            until(&driver, id, |s| matches!(s, TaskState::Cancelled));
        }
        driver.shutdown().unwrap();
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let receipts =
            pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, child).unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0].outcome,
            Outcome::Succeeded("after-abort-id".into())
        );
    }
}

struct AnyParent;
#[async_trait]
impl Tool for AnyParent {
    fn spec(&self) -> ToolSpec {
        spec("any-parent")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}

struct BlockingAnyParent(PathBuf);
#[async_trait]
impl Tool for BlockingAnyParent {
    fn spec(&self) -> ToolSpec {
        spec("any-parent")
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for BlockingAnyParent {
    async fn execute_durable(
        &self,
        args: Value,
        context: ToolContext,
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        let resumed = resume.checkpoint.is_some();
        let result = AnyParent
            .execute_durable(args, context, resume, host)
            .await?;
        if resumed {
            std::fs::write(self.0.join("parent-running.tmp"), "running")
                .map_err(|e| e.to_string())?;
            std::fs::rename(
                self.0.join("parent-running.tmp"),
                self.0.join("parent-running"),
            )
            .map_err(|e| e.to_string())?;
            loop {
                std::thread::park();
            }
        }
        Ok(result)
    }
}
#[async_trait]
impl DurableAsyncTool for AnyParent {
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
            let sibling: AsyncTaskCall =
                serde_json::from_value(data["sibling"].clone()).map_err(|e| e.to_string())?;
            if resume.ended != vec![sibling.task_id()] || host.child_result(&child).await?.is_some()
            {
                return Err("unexpected any wake".into());
            }
            if !matches!(
                host.child_result(&sibling).await?,
                Some(AsyncTaskResult::Outcome(Outcome::Succeeded(_)))
            ) {
                return Err("missing sibling result".into());
            }
            return Ok(DurableToolStep::Complete(Outcome::Succeeded(
                "any-done".into(),
            )));
        }
        let child = host.start_child("child", "unsafe-child", json!({})).await?;
        let sibling = host.start_child("sibling", "sibling", json!({})).await?;
        Ok(DurableToolStep::Park {
            checkpoint: json!({"child":child,"sibling":sibling}),
            wait: DurableWait::Any(vec![child, sibling]),
        })
    }
}
fn build_any(dir: &Path, safe: bool) -> (StandaloneNoHooksHost, Arc<TaskTool>) {
    build_any_mode(dir, safe, "success")
}
fn build_any_mode(dir: &Path, safe: bool, mode: &str) -> (StandaloneNoHooksHost, Arc<TaskTool>) {
    let gate: Arc<dyn Handler> = Arc::new(Gate);
    let mut registry = Registry::default();
    assert!(registry.register_shared("nested-gate", true, gate.clone()));
    assert!(registry.set_abort("nested-gate", |_: &Task| Ok(Step::Succeed(json!(null)))));
    let gate_tool = Arc::new(TaskTool::new(
        spec("gate"),
        "nested-gate",
        true,
        1,
        gate.clone(),
    ));
    let sibling = Arc::new(TaskTool::new(spec("sibling"), "nested-gate", true, 1, gate));
    let mut host = StandaloneNoHooksHost::open(
        dir.join("db"),
        registry,
        UnhookedAuthorization::standalone_only(),
    )
    .unwrap();
    let child = host
        .register_unsafe_durable_tool(
            Arc::new(UnsafeChild {
                dir: dir.into(),
                mode: if mode == "parent-interrupt" {
                    "interrupt"
                } else {
                    mode
                }
                .into(),
            }),
            "nested-unsafe-v1",
            1,
            vec![gate_tool],
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
        .unwrap();
    let parent: Arc<dyn DurableAsyncTool> = if mode == "parent-interrupt" {
        Arc::new(BlockingAnyParent(dir.to_owned()))
    } else {
        Arc::new(AnyParent)
    };
    let root = if safe {
        host.register_replay_safe_durable_tool(
            parent,
            "any-parent-v1",
            1,
            vec![child, sibling],
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap()
    } else {
        host.register_unsafe_durable_tool(
            parent,
            "any-parent-v1",
            1,
            vec![child, sibling],
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
        .unwrap()
    };
    (host, root)
}

#[test]
fn unsafe_running_ancestor_kill_fixture() {
    let Some(dir) = std::env::var_os("PI_UNSAFE_PARENT_RUNNING_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let (host, tool) = build_any_mode(&dir, false, "parent-interrupt");
    let root = host.start_unsafe_durable_root(&tool, json!({}), 3).unwrap();
    let driver = host.into_driver();
    let (child, sibling) = parent_children(&driver, root);
    let deadline = Instant::now() + Duration::from_secs(15);
    while !dir.join("running").exists() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    signal(&driver, sibling);
    while !dir.join("parent-running").exists() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    std::fs::write(
        dir.join("ready.tmp"),
        serde_json::to_vec(&(root, child)).unwrap(),
    )
    .unwrap();
    std::fs::rename(dir.join("ready.tmp"), dir.join("ready")).unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
fn interrupted_unsafe_parent_with_running_unsafe_child_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let mut process = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("unsafe_running_ancestor_kill_fixture")
        .env("PI_UNSAFE_PARENT_RUNNING_DIR", dir.path())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !dir.path().join("ready").exists() {
        if Instant::now() >= deadline || process.try_wait().unwrap().is_some() {
            let _ = process.kill();
            let _ = process.wait();
            panic!("unsafe parent fixture did not start");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    process.kill().unwrap();
    process.wait().unwrap();
    let (root, child): (Uuid, Uuid) =
        serde_json::from_slice(&std::fs::read(dir.path().join("ready")).unwrap()).unwrap();
    let (host, _) = build_any_mode(dir.path(), false, "parent-interrupt");
    let driver = host.into_driver();
    let parent = until(&driver, root, |s| matches!(s, TaskState::Failed(_)));
    assert!(
        matches!(parent.state(), TaskState::Failed(reason) if reason.contains("reconciliation"))
    );
    until(&driver, child, |s| {
        matches!(s, TaskState::Cancelled | TaskState::Failed(_))
    });
    assert_eq!(
        std::fs::read_to_string(dir.path().join("child-effects")).unwrap(),
        "initial\n"
    );
    driver.shutdown().unwrap();
}

#[test]
fn any_completing_interrupted_child_fixture() {
    let Some(dir) = std::env::var_os("PI_UNSAFE_COMPLETING_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let safe = std::env::var("PI_UNSAFE_COMPLETING_SAFE").unwrap() == "1";
    let (host, root_tool) = build_any_mode(&dir, safe, "interrupt");
    let root = if safe {
        host.start_replay_safe_root(&root_tool, json!({}), 3)
            .unwrap()
    } else {
        host.start_unsafe_durable_root(&root_tool, json!({}), 3)
            .unwrap()
    };
    let driver = host.into_driver();
    let (child, sibling) = parent_children(&driver, root);
    let deadline = Instant::now() + Duration::from_secs(15);
    while !dir.join("running").exists() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    signal(&driver, sibling);
    until(&driver, root, |s| matches!(s, TaskState::Completing(_)));
    assert!(matches!(
        driver.get(child).unwrap().unwrap().state(),
        TaskState::Running
    ));
    std::fs::write(
        dir.join("ready.tmp"),
        serde_json::to_vec(&(root, child)).unwrap(),
    )
    .unwrap();
    std::fs::rename(dir.join("ready.tmp"), dir.join("ready")).unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
fn any_held_success_is_not_published_when_unsafe_child_is_interrupted() {
    for safe in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("any_completing_interrupted_child_fixture")
            .env("PI_UNSAFE_COMPLETING_DIR", dir.path())
            .env("PI_UNSAFE_COMPLETING_SAFE", if safe { "1" } else { "0" })
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        while !dir.path().join("ready").exists() {
            if Instant::now() >= deadline || process.try_wait().unwrap().is_some() {
                let _ = process.kill();
                let _ = process.wait();
                panic!("fixture did not reach held success");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let (root, child): (Uuid, Uuid) =
            serde_json::from_slice(&std::fs::read(dir.path().join("ready")).unwrap()).unwrap();
        let (host, _) = build_any_mode(dir.path(), safe, "interrupt");
        let driver = host.into_driver();
        until(
            &driver,
            child,
            |s| matches!(s, TaskState::Failed(reason) if reason.contains("reconciliation")),
        );
        let task = until(&driver, root, |s| matches!(s, TaskState::Failed(_)));
        assert!(
            matches!(task.state(), TaskState::Failed(reason) if reason.contains("reconciliation") && reason.contains(&child.to_string()))
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("child-effects")).unwrap(),
            "initial\n"
        );
        driver.shutdown().unwrap();
    }
}

#[test]
fn foreign_or_substituted_unsafe_child_registration_is_rejected() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let (host_a, foreign) = build(a.path(), false, "success", false);
    let (mut host_b, local) = build(b.path(), false, "success", false);
    assert!(
        host_b
            .start_unsafe_durable_root(&foreign, json!({}), 3)
            .is_err()
    );
    assert!(
        host_b
            .register_replay_safe_durable_tool(
                Arc::new(AnyParent),
                "foreign-parent",
                1,
                vec![foreign],
                ReplaySafeAuthorization::host_attests_replay_safe()
            )
            .is_err()
    );
    let forged = Arc::new(TaskTool::new(
        spec("forged"),
        "pi-rs.unsafe-park-async.v1.forged",
        false,
        1,
        Arc::new(Gate),
    ));
    assert!(
        host_b
            .register_unsafe_durable_tool(
                Arc::new(AnyParent),
                "forged-parent",
                1,
                vec![forged],
                UnsafeDurableAuthorization::interruption_requires_reconciliation()
            )
            .is_err()
    );
    assert!(host_b.start_replay_safe_root(&local, json!({}), 3).is_err());
    host_a.into_driver().shutdown().unwrap();
    host_b.into_driver().shutdown().unwrap();
}
#[test]
fn any_wake_keeps_unsafe_child_alive_under_completing_parent_after_reopen() {
    for safe in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (host, root_tool) = build_any(dir.path(), safe);
        let root = if safe {
            host.start_replay_safe_root(&root_tool, json!({}), 3)
                .unwrap()
        } else {
            host.start_unsafe_durable_root(&root_tool, json!({}), 3)
                .unwrap()
        };
        let driver = host.into_driver();
        let (child, sibling) = parent_children(&driver, root);
        let gate = child_gate(&driver, child);
        signal(&driver, sibling);
        until(&driver, root, |s| matches!(s, TaskState::Completing(_)));
        assert!(matches!(
            driver.get(child).unwrap().unwrap().state(),
            TaskState::Waiting(_)
        ));
        driver.shutdown().unwrap();
        let (host, _) = build_any(dir.path(), safe);
        let driver = host.into_driver();
        signal(&driver, gate);
        until(&driver, child, |s| matches!(s, TaskState::Succeeded(_)));
        let task = until(&driver, root, |s| matches!(s, TaskState::Succeeded(_)));
        assert_eq!(
            task.state(),
            &TaskState::Succeeded(
                serde_json::to_value(Outcome::Succeeded("any-done".into())).unwrap()
            )
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("child-effects")).unwrap(),
            "initial\nresumed\n"
        );
        driver.shutdown().unwrap();
    }
}
