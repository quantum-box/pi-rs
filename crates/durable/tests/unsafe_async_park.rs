use async_trait::async_trait;
use pi_rs_core::{Outcome, Tool, ToolContext, ToolSpec};
use pi_rs_durable::{
    async_task_tool::{
        AsyncTaskCall, AsyncTaskResult, DurableAsyncTool, DurableTaskHost, DurableToolResume,
        DurableToolStep, DurableWait, StandaloneNoHooksHost, UnhookedAuthorization,
        UnsafeDurableAuthorization,
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
                serde_json::to_value(Outcome::Succeeded("child".into())).unwrap(),
            )),
            _ => Err("bad gate wake".into()),
        }
    }
}
struct UnsafeParent {
    directory: PathBuf,
    cut: Option<String>,
}
#[async_trait]
impl Tool for UnsafeParent {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "unsafe-parent".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for UnsafeParent {
    async fn execute_durable(
        &self,
        _: Value,
        context: ToolContext,
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        let stage = if resume.checkpoint.is_none() {
            "initial"
        } else {
            "resumed"
        };
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("effects"))
            .map_err(|e| e.to_string())?;
        writeln!(file, "{stage}").map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        if self.cut.as_deref() == Some(stage) {
            std::fs::write(
                self.directory.join("cut.tmp"),
                context.operation_id.to_string(),
            )
            .map_err(|e| e.to_string())?;
            std::fs::rename(self.directory.join("cut.tmp"), self.directory.join("cut"))
                .map_err(|e| e.to_string())?;
            loop {
                std::thread::park();
            }
        }
        match resume.checkpoint {
            None => {
                let call = host.start_child("gate", "gate", json!({})).await?;
                Ok(DurableToolStep::Park {
                    checkpoint: json!({"child":call}),
                    wait: DurableWait::All(vec![call]),
                })
            }
            Some(data) => {
                let call: AsyncTaskCall =
                    serde_json::from_value(data["child"].clone()).map_err(|e| e.to_string())?;
                if resume.ended != vec![call.task_id()] {
                    return Err("bad wake".into());
                }
                if !matches!(host.child_result(&call).await?, Some(AsyncTaskResult::Outcome(Outcome::Succeeded(value))) if value == "child")
                {
                    return Err("bad child result".into());
                }
                Ok(DurableToolStep::Complete(Outcome::Succeeded("done".into())))
            }
        }
    }
}
fn host(dir: &Path, cut: Option<&str>) -> (StandaloneNoHooksHost, Arc<TaskTool>) {
    let gate: Arc<dyn Handler> = Arc::new(Gate);
    let mut registry = Registry::default();
    assert!(registry.register_shared("unsafe-test-gate", true, gate.clone()));
    assert!(
        registry.set_abort("unsafe-test-gate", |_: &Task| Ok(Step::Succeed(json!(
            null
        ))))
    );
    let child = Arc::new(TaskTool::new(
        ToolSpec {
            name: "gate".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        },
        "unsafe-test-gate",
        true,
        1,
        gate,
    ));
    let mut host = StandaloneNoHooksHost::open(
        dir.join("db"),
        registry,
        UnhookedAuthorization::standalone_only(),
    )
    .unwrap();
    let root = host
        .register_unsafe_durable_tool(
            Arc::new(UnsafeParent {
                directory: dir.to_owned(),
                cut: cut.map(str::to_owned),
            }),
            "unsafe-parent-v1",
            1,
            vec![child],
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
        .unwrap();
    (host, root)
}
fn until(
    driver: &pi_rs_durable::task_driver::Driver,
    id: Uuid,
    predicate: impl Fn(&TaskState) -> bool,
) -> Task {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let task = driver.get(id).unwrap().unwrap();
        if predicate(task.state()) {
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
fn parked(driver: &pi_rs_durable::task_driver::Driver, root: Uuid) -> Uuid {
    let task = until(driver, root, |s| matches!(s, TaskState::Waiting(_)));
    let call: AsyncTaskCall =
        serde_json::from_value(task.checkpoint().data["checkpoint"]["child"].clone()).unwrap();
    until(driver, call.task_id(), |s| {
        matches!(s, TaskState::Waiting(_))
    });
    call.task_id()
}
fn signal(driver: &pi_rs_durable::task_driver::Driver, child: Uuid) {
    driver
        .signal(&Signal::new(
            Uuid::new_v4(),
            child,
            "go".into(),
            json!(null),
        ))
        .unwrap();
}

#[test]
fn unsafe_process_fixture() {
    let Some(dir) = std::env::var_os("PI_UNSAFE_ASYNC_TEST_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let mode = std::env::var("PI_UNSAFE_ASYNC_TEST_MODE").unwrap();
    let cut = if mode == "initial" || mode == "resumed" {
        Some(mode.as_str())
    } else {
        None
    };
    let (host, root) = host(&dir, cut);
    let id = host.start_unsafe_durable_root(&root, json!({}), 3).unwrap();
    std::fs::write(dir.join("root"), id.to_string()).unwrap();
    let driver = host.into_driver();
    if mode == "parked" || mode == "resumed" {
        let child = parked(&driver, id);
        if mode == "resumed" {
            signal(&driver, child);
        } else {
            std::fs::write(dir.join("cut.tmp"), child.to_string()).unwrap();
            std::fs::rename(dir.join("cut.tmp"), dir.join("cut")).unwrap();
        }
    }
    loop {
        std::thread::park();
    }
}
fn kill_fixture(dir: &Path, mode: &str) -> Uuid {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("unsafe_process_fixture")
        .env("PI_UNSAFE_ASYNC_TEST_DIR", dir)
        .env("PI_UNSAFE_ASYNC_TEST_MODE", mode)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !dir.join("cut").exists() {
        if Instant::now() >= deadline || child.try_wait().unwrap().is_some() {
            let _ = child.kill();
            let _ = child.wait();
            panic!("fixture failed to reach {mode}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    std::fs::read_to_string(dir.join("root"))
        .unwrap()
        .parse()
        .unwrap()
}
#[test]
fn unsafe_parked_kill_reopen_child_wake_resumes_once() {
    let dir = tempfile::tempdir().unwrap();
    let root = kill_fixture(dir.path(), "parked");
    assert_eq!(
        std::fs::read_to_string(dir.path().join("effects")).unwrap(),
        "initial\n"
    );
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    assert!(
        pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, root)
            .unwrap()
            .is_empty()
    );
    drop(store);
    let (host, _) = host(dir.path(), None);
    let driver = host.into_driver();
    let child = parked(&driver, root);
    signal(&driver, child);
    let task = until(&driver, root, |s| matches!(s, TaskState::Succeeded(_)));
    assert_eq!(
        task.state(),
        &TaskState::Succeeded(serde_json::to_value(Outcome::Succeeded("done".into())).unwrap())
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("effects")).unwrap(),
        "initial\nresumed\n"
    );
    driver.shutdown().unwrap();
}
#[test]
fn unsafe_interrupted_running_attempt_never_replays() {
    for stage in ["initial", "resumed"] {
        let dir = tempfile::tempdir().unwrap();
        let root = kill_fixture(dir.path(), stage);
        let before = std::fs::read_to_string(dir.path().join("effects")).unwrap();
        let (host, _) = host(dir.path(), None);
        let driver = host.into_driver();
        let task = until(&driver, root, |s| matches!(s, TaskState::Failed(_)));
        assert!(
            matches!(task.state(), TaskState::Failed(reason) if reason.contains("reconciliation"))
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("effects")).unwrap(),
            before
        );
        driver.shutdown().unwrap();
    }
}

#[test]
fn unsafe_wake_without_park_proof_fails_closed() {
    for remove_result in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let root = kill_fixture(dir.path(), "parked");
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let prefix = format!("durable/unsafe-park-async/{root}/");
        let key = if remove_result {
            store
                .list(prefix.as_bytes())
                .unwrap()
                .into_iter()
                .find(|(key, _)| key.ends_with(b"/intent/result"))
                .unwrap()
                .0
        } else {
            format!("{prefix}park").into_bytes()
        };
        assert!(store.get(&key).unwrap().is_some());
        store
            .commit(&[], &[durable_store::Mutation::Delete { key }])
            .unwrap();
        drop(store);
        let (host, _) = host(dir.path(), None);
        let driver = host.into_driver();
        let child = parked(&driver, root);
        signal(&driver, child);
        let task = until(&driver, root, |s| matches!(s, TaskState::Failed(_)));
        assert!(matches!(task.state(), TaskState::Failed(reason) if reason.contains("park")));
        assert_eq!(
            std::fs::read_to_string(dir.path().join("effects")).unwrap(),
            "initial\n"
        );
        driver.shutdown().unwrap();
    }
}
#[test]
fn unsafe_parked_abort_after_kill_drains_child_without_resume() {
    let dir = tempfile::tempdir().unwrap();
    let root = kill_fixture(dir.path(), "parked");
    let (host, _) = host(dir.path(), None);
    let driver = host.into_driver();
    let child = parked(&driver, root);
    driver.request_abort(root).unwrap();
    until(&driver, root, |s| matches!(s, TaskState::Cancelled));
    until(&driver, child, |s| matches!(s, TaskState::Cancelled));
    assert_eq!(
        std::fs::read_to_string(dir.path().join("effects")).unwrap(),
        "initial\n"
    );
    driver.shutdown().unwrap();
}

#[test]
fn unsafe_registration_is_explicit_and_can_be_a_checked_safe_parent_child() {
    let dir = tempfile::tempdir().unwrap();
    let (mut host, unsafe_root) = host(dir.path(), None);
    assert!(
        host.start_replay_safe_root(&unsafe_root, json!({}), 3)
            .is_err()
    );
    let tool: Arc<dyn DurableAsyncTool> = Arc::new(UnsafeParent {
        directory: dir.path().to_owned(),
        cut: None,
    });
    assert!(
        host.register_replay_safe_durable_tool(
            tool,
            "other",
            1,
            vec![unsafe_root],
            pi_rs_durable::async_task_tool::ReplaySafeAuthorization::host_attests_replay_safe()
        )
        .is_ok()
    );
    host.into_driver().shutdown().unwrap();
}

struct TwoParks;
#[async_trait]
impl Tool for TwoParks {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "unsafe-twice".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for TwoParks {
    async fn execute_durable(
        &self,
        _: Value,
        _: ToolContext,
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        let step = if let Some(data) = resume.checkpoint {
            let call: AsyncTaskCall =
                serde_json::from_value(data["child"].clone()).map_err(|e| e.to_string())?;
            if resume.ended != vec![call.task_id()]
                || !matches!(host.child_result(&call).await?, Some(AsyncTaskResult::Outcome(Outcome::Succeeded(value))) if value == "child")
            {
                return Err("bad wake".into());
            }
            data["step"].as_u64().ok_or("bad step")? + 1
        } else {
            1
        };
        if step == 3 {
            return Ok(DurableToolStep::Complete(Outcome::Succeeded(
                "twice".into(),
            )));
        }
        let call = host
            .start_child(&format!("gate-{step}"), "gate", json!({}))
            .await?;
        Ok(DurableToolStep::Park {
            checkpoint: json!({"step":step,"child":call}),
            wait: DurableWait::All(vec![call]),
        })
    }
}
#[test]
fn unsafe_tool_can_commit_two_distinct_park_generations() {
    let dir = tempfile::tempdir().unwrap();
    let gate: Arc<dyn Handler> = Arc::new(Gate);
    let mut registry = Registry::default();
    assert!(registry.register_shared("unsafe-test-gate", true, gate.clone()));
    let child = Arc::new(TaskTool::new(
        ToolSpec {
            name: "gate".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        },
        "unsafe-test-gate",
        true,
        1,
        gate,
    ));
    let mut host = StandaloneNoHooksHost::open(
        dir.path().join("db"),
        registry,
        UnhookedAuthorization::standalone_only(),
    )
    .unwrap();
    let root = host
        .register_unsafe_durable_tool(
            Arc::new(TwoParks),
            "twice-v1",
            1,
            vec![child],
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
        .unwrap();
    let id = host.start_unsafe_durable_root(&root, json!({}), 3).unwrap();
    let driver = host.into_driver();
    let first = parked(&driver, id);
    signal(&driver, first);
    let deadline = Instant::now() + Duration::from_secs(15);
    let second = loop {
        let task = driver.get(id).unwrap().unwrap();
        if matches!(task.state(), TaskState::Waiting(_))
            && task.checkpoint().data["checkpoint"]["step"] == 2
        {
            let call: AsyncTaskCall =
                serde_json::from_value(task.checkpoint().data["checkpoint"]["child"].clone())
                    .unwrap();
            break call.task_id();
        }
        assert!(
            Instant::now() < deadline,
            "second park did not commit: {:?}",
            task.state()
        );
        std::thread::sleep(Duration::from_millis(5));
    };
    assert_ne!(first, second);
    until(&driver, second, |s| matches!(s, TaskState::Waiting(_)));
    signal(&driver, second);
    let task = until(&driver, id, |s| matches!(s, TaskState::Succeeded(_)));
    assert_eq!(
        task.state(),
        &TaskState::Succeeded(serde_json::to_value(Outcome::Succeeded("twice".into())).unwrap())
    );
    driver.shutdown().unwrap();
}

struct UnsafeAny;
#[async_trait]
impl Tool for UnsafeAny {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "unsafe-any".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for UnsafeAny {
    async fn execute_durable(
        &self,
        _: Value,
        _: ToolContext,
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        if let Some(data) = resume.checkpoint {
            let calls: Vec<AsyncTaskCall> =
                serde_json::from_value(data["calls"].clone()).map_err(|e| e.to_string())?;
            if resume.ended.len() != 1 {
                return Err("bad any wake".into());
            }
            let call = calls
                .iter()
                .find(|c| c.task_id() == resume.ended[0])
                .ok_or("bad child")?;
            if !matches!(host.child_result(call).await?, Some(AsyncTaskResult::Outcome(Outcome::Succeeded(value))) if value == "twice")
            {
                return Err("bad child result".into());
            }
            return Ok(DurableToolStep::Complete(Outcome::Succeeded("root".into())));
        }
        let a = host.start_child("a", "unsafe-twice", json!({})).await?;
        let b = host.start_child("b", "unsafe-twice", json!({})).await?;
        let calls = vec![a, b];
        Ok(DurableToolStep::Park {
            checkpoint: json!({"calls":calls}),
            wait: DurableWait::Any(calls.clone()),
        })
    }
}
fn finish_two_parks(driver: &pi_rs_durable::task_driver::Driver, id: Uuid, first_gate: Uuid) {
    signal(driver, first_gate);
    let deadline = Instant::now() + Duration::from_secs(15);
    let second_gate = loop {
        let task = driver.get(id).unwrap().unwrap();
        if matches!(task.state(), TaskState::Waiting(_))
            && task.checkpoint().data["checkpoint"]["step"] == 2
        {
            let call: AsyncTaskCall =
                serde_json::from_value(task.checkpoint().data["checkpoint"]["child"].clone())
                    .unwrap();
            break call.task_id();
        }
        assert!(
            Instant::now() < deadline,
            "child did not repark: {:?}",
            task.state()
        );
        std::thread::sleep(Duration::from_millis(5));
    };
    until(driver, second_gate, |s| matches!(s, TaskState::Waiting(_)));
    signal(driver, second_gate);
    until(driver, id, |s| matches!(s, TaskState::Succeeded(_)));
}
#[test]
fn unsafe_any_success_retains_safe_async_child_until_it_reparks_and_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let gate: Arc<dyn Handler> = Arc::new(Gate);
    let mut registry = Registry::default();
    assert!(registry.register_shared("unsafe-test-gate", true, gate.clone()));
    let gate_tool = Arc::new(TaskTool::new(
        ToolSpec {
            name: "gate".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        },
        "unsafe-test-gate",
        true,
        1,
        gate,
    ));
    let mut host = StandaloneNoHooksHost::open(
        dir.path().join("db"),
        registry,
        UnhookedAuthorization::standalone_only(),
    )
    .unwrap();
    let safe = host
        .register_replay_safe_durable_tool(
            Arc::new(TwoParks),
            "safe-twice-v1",
            1,
            vec![gate_tool],
            pi_rs_durable::async_task_tool::ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap();
    let root = host
        .register_unsafe_durable_tool(
            Arc::new(UnsafeAny),
            "unsafe-any-v1",
            1,
            vec![safe],
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
        .unwrap();
    let id = host.start_unsafe_durable_root(&root, json!({}), 3).unwrap();
    let driver = host.into_driver();
    let task = until(&driver, id, |s| matches!(s, TaskState::Waiting(_)));
    let calls: Vec<AsyncTaskCall> =
        serde_json::from_value(task.checkpoint().data["checkpoint"]["calls"].clone()).unwrap();
    let first_gate = parked(&driver, calls[0].task_id());
    let second_gate = parked(&driver, calls[1].task_id());
    finish_two_parks(&driver, calls[0].task_id(), first_gate);
    until(&driver, id, |s| matches!(s, TaskState::Completing(_)));
    finish_two_parks(&driver, calls[1].task_id(), second_gate);
    let task = until(&driver, id, |s| matches!(s, TaskState::Succeeded(_)));
    assert_eq!(
        task.state(),
        &TaskState::Succeeded(serde_json::to_value(Outcome::Succeeded("root".into())).unwrap())
    );
    driver.shutdown().unwrap();
}

struct ReturnedOutcome {
    entered: Option<std::sync::mpsc::Sender<()>>,
    outcome: Outcome,
}
#[async_trait]
impl Tool for ReturnedOutcome {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "unsafe-result".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for ReturnedOutcome {
    async fn execute_durable(
        &self,
        _: Value,
        context: ToolContext,
        _: DurableToolResume,
        _: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        if let Some(entered) = &self.entered {
            entered.send(()).map_err(|e| e.to_string())?;
            context.cancellation.cancelled().await;
        }
        Ok(DurableToolStep::Complete(self.outcome.clone()))
    }
}
fn outcome_host(dir: &Path, tool: ReturnedOutcome) -> (StandaloneNoHooksHost, Arc<TaskTool>) {
    let mut host = StandaloneNoHooksHost::open(
        dir.join("db"),
        Registry::default(),
        UnhookedAuthorization::standalone_only(),
    )
    .unwrap();
    let root = host
        .register_unsafe_durable_tool(
            Arc::new(tool),
            "unsafe-outcome-v1",
            1,
            vec![],
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
        .unwrap();
    (host, root)
}
#[test]
fn unsafe_outcome_returned_after_abort_is_retained_for_reconciliation() {
    let dir = tempfile::tempdir().unwrap();
    let (entered, receiver) = std::sync::mpsc::channel();
    let outcome = Outcome::Succeeded("external-id:confirmed-42".into());
    let (host, root) = outcome_host(
        dir.path(),
        ReturnedOutcome {
            entered: Some(entered),
            outcome: outcome.clone(),
        },
    );
    let id = host.start_unsafe_durable_root(&root, json!({}), 3).unwrap();
    let driver = host.into_driver();
    receiver.recv_timeout(Duration::from_secs(10)).unwrap();
    driver.request_abort(id).unwrap();
    until(&driver, id, |s| matches!(s, TaskState::Cancelled));
    driver.shutdown().unwrap();
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let receipts =
        pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, id).unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].task_id, id);
    assert_eq!(receipts[0].outcome, outcome);
}
#[test]
fn unsafe_pending_reference_is_retained_as_uncertain_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let outcome = Outcome::Pending {
        reference: "remote-job-42".into(),
    };
    let (host, root) = outcome_host(
        dir.path(),
        ReturnedOutcome {
            entered: None,
            outcome: outcome.clone(),
        },
    );
    let id = host.start_unsafe_durable_root(&root, json!({}), 3).unwrap();
    let driver = host.into_driver();
    let task = until(&driver, id, |s| matches!(s, TaskState::Failed(_)));
    assert!(matches!(task.state(), TaskState::Failed(reason) if reason.contains("reconciliation")));
    driver.shutdown().unwrap();
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let receipts =
        pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, id).unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].outcome, outcome);
}

struct CoexistingHook;
#[async_trait]
impl pi_rs_durable::extension::Hook for CoexistingHook {
    async fn before_tool(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        ctx: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::BeforeTool, String> {
        if ctx.memo.get_or_insert("phase", json!("before")).await? != json!("before") {
            return Err("unexpected hook memo".into());
        }
        Ok(pi_rs_durable::extension::BeforeTool::Rewrite(
            json!({"value":"rewritten"}),
        ))
    }
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &Value,
        ctx: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<Value>, String> {
        if call.arguments != json!({"value":"rewritten"})
            || result != &json!("rewritten")
            || ctx.memo.get("phase").await? != Some(json!("before"))
        {
            return Err("hook arguments, result, or memo changed".into());
        }
        Ok(Some(json!("after rewrite")))
    }
}

struct CoexistingSafeTool;
#[async_trait]
impl Tool for CoexistingSafeTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "coexisting-safe".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for CoexistingSafeTool {
    async fn execute_durable(
        &self,
        args: Value,
        _: ToolContext,
        _: DurableToolResume,
        _: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        let value = args["value"].as_str().ok_or("missing rewritten value")?;
        Ok(DurableToolStep::Complete(Outcome::Succeeded(value.into())))
    }
}

#[test]
fn unsafe_park_and_hooked_replay_safe_drivers_coexist() {
    use pi_rs_durable::async_task_tool::{ReplaySafeAuthorization, StandaloneBeforeHookHost};
    let dir = tempfile::tempdir().unwrap();
    let unsafe_dir = dir.path().join("unsafe");
    std::fs::create_dir(&unsafe_dir).unwrap();
    let (unsafe_host, unsafe_tool) = host(&unsafe_dir, None);
    let unsafe_id = unsafe_host
        .start_unsafe_durable_root(&unsafe_tool, json!({}), 3)
        .unwrap();
    let unsafe_driver = unsafe_host.into_driver();
    let child = parked(&unsafe_driver, unsafe_id);

    let mut hook_host =
        StandaloneBeforeHookHost::open(dir.path().join("hooks-db"), Registry::default()).unwrap();
    let hooked = hook_host
        .register_replay_safe_durable_tool_with_hook(
            Arc::new(CoexistingSafeTool),
            "coexisting-safe-v1",
            1,
            vec![],
            "coexisting-hook-v1",
            Arc::new(CoexistingHook),
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap();
    assert!(
        hook_host
            .start_replay_safe_root(&unsafe_tool, json!({}), 3)
            .is_err()
    );
    let hooked_id = hook_host
        .start_replay_safe_root(&hooked, json!({"value":"original"}), 3)
        .unwrap();
    let hook_driver = hook_host.into_driver();
    let hooked_task = until(&hook_driver, hooked_id, |s| {
        matches!(
            s,
            TaskState::Succeeded(_) | TaskState::Failed(_) | TaskState::Cancelled
        )
    });
    assert_eq!(
        hooked_task.state(),
        &TaskState::Succeeded(
            serde_json::to_value(Outcome::Succeeded("after rewrite".into())).unwrap()
        )
    );
    assert!(matches!(
        unsafe_driver.get(unsafe_id).unwrap().unwrap().state(),
        TaskState::Waiting(_)
    ));
    signal(&unsafe_driver, child);
    let unsafe_task = until(&unsafe_driver, unsafe_id, |s| {
        matches!(
            s,
            TaskState::Succeeded(_) | TaskState::Failed(_) | TaskState::Cancelled
        )
    });
    assert_eq!(
        unsafe_task.state(),
        &TaskState::Succeeded(serde_json::to_value(Outcome::Succeeded("done".into())).unwrap())
    );
    assert_eq!(
        std::fs::read_to_string(unsafe_dir.join("effects")).unwrap(),
        "initial\nresumed\n"
    );
    hook_driver.shutdown().unwrap();
    unsafe_driver.shutdown().unwrap();
    let store = durable_store::Store::open(unsafe_dir.join("db")).unwrap();
    let proposals =
        pi_rs_durable::async_task_tool::read_unsafe_durable_park_evidence(&store, unsafe_id)
            .unwrap();
    let receipts =
        pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, unsafe_id).unwrap();
    assert_eq!(proposals.len(), 1);
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].outcome, Outcome::Succeeded("done".into()));
}
