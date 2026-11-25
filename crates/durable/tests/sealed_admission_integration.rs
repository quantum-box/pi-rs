use async_trait::async_trait;
use pi_rs_core::{Outcome, Tool, ToolContext, ToolSpec};
use pi_rs_durable::{
    async_task_tool::{
        AsyncTaskCall, AsyncTaskResult, AsyncTaskTool, StandaloneNoHooksHost, UnhookedAuthorization,
    },
    document::TaskDocuments,
    nested_identity,
    task::{Checkpoint, Task, TaskOwner, TaskState, WaitFor},
    task_driver::Driver,
    task_scheduler::{AdmissionHandle, Handler, Registry, Step},
    task_tool::{NestedTaskCall, TaskTool},
};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use uuid::Uuid;

fn cp(phase: &str, data: Value) -> Checkpoint {
    Checkpoint {
        phase: phase.into(),
        data,
    }
}
fn spec(name: &str) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        description: String::new(),
        parameters: json!({"type":"object"}),
    }
}

struct Leaf {
    calls: Arc<AtomicUsize>,
    valid: bool,
    log: Option<PathBuf>,
    preparation_gate: Option<Arc<AtomicBool>>,
}
#[async_trait]
impl Tool for Leaf {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "leaf".into(),
            description: String::new(),
            parameters: json!({"type":"object", "required":["normalized"], "properties":{"normalized":{"const":true}}}),
        }
    }
    async fn prepare_arguments(&self, mut args: Value) -> pi_rs_core::ExtensionResult<Value> {
        if let Some(gate) = &self.preparation_gate {
            while !gate.load(Ordering::SeqCst) {
                std::thread::park_timeout(Duration::from_millis(5));
            }
        }
        args.as_object_mut().unwrap().remove("raw");
        args["normalized"] = json!(self.valid);
        Ok(args)
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(path) = &self.log {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap();
            writeln!(file, "invoke").unwrap();
            file.sync_all().unwrap();
        }
        Outcome::Succeeded("ok".into())
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Staged,
    Immediate,
    Kill,
    Abort,
}
struct Parent {
    leaf: Arc<OnceLock<Arc<AsyncTaskTool>>>,
    mode: Mode,
    marker: Option<PathBuf>,
    release: Arc<AtomicBool>,
}
impl Handler for Parent {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_admission(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
        admission: &AdmissionHandle<'_>,
    ) -> Result<Step, String> {
        let leaf = self.leaf.get().ok_or("missing leaf")?;
        if task.checkpoint().phase == "initial" {
            let input = json!({"raw":"first"});
            let call = match self.mode {
                Mode::Staged => leaf.start_child(task, docs, "leaf", input.clone())?,
                _ => leaf
                    .start_child_now(admission, "leaf", input.clone())
                    .map_err(|e| format!("first immediate: {e}"))?,
            };
            let repeated = match self.mode {
                Mode::Staged => leaf.start_child(task, docs, "leaf", input)?,
                _ => leaf
                    .start_child_now(admission, "leaf", input)
                    .map_err(|e| format!("repeat immediate: {e}"))?,
            };
            if call != repeated {
                return Err("identical raw input did not reconnect".into());
            }
            let different = match self.mode {
                Mode::Staged => leaf.start_child(task, docs, "leaf", json!({"raw":"second"})),
                _ => leaf.start_child_now(admission, "leaf", json!({"raw":"second"})),
            };
            if different.is_ok() {
                return Err("different raw input was accepted".into());
            }
            if let Some(marker) = &self.marker {
                std::fs::write(marker, format!("{} {}", task.id(), call.task_id()))
                    .map_err(|e| e.to_string())?;
                while !self.release.load(Ordering::SeqCst) {
                    std::thread::park_timeout(Duration::from_millis(5));
                }
            }
            Ok(Step::Wait {
                checkpoint: cp("waiting", serde_json::to_value(&call).unwrap()),
                condition: WaitFor::Tasks(vec![call.task_id()]),
                children: vec![],
            })
        } else {
            let call: AsyncTaskCall = serde_json::from_value(task.checkpoint().data.clone())
                .map_err(|e| e.to_string())?;
            match call.result(task, docs)? {
                Some(AsyncTaskResult::Outcome(Outcome::Succeeded(s))) => {
                    Ok(Step::Succeed(json!(s)))
                }
                Some(AsyncTaskResult::AfterIncomplete(Outcome::Succeeded(s))) => {
                    Ok(Step::Succeed(json!(format!("after-incomplete:{s}"))))
                }
                Some(AsyncTaskResult::Uncertain(Outcome::Unknown(reason)))
                    if reason.contains("normalized arguments") =>
                {
                    Ok(Step::Succeed(json!("invalid")))
                }
                other => Err(format!("unexpected outcome: {other:?}")),
            }
        }
    }
}
struct Root {
    parent: Arc<TaskTool>,
}
impl Handler for Root {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        if task.checkpoint().phase == "initial" {
            let call = self.parent.start_child(task, docs, "parent", json!({}))?;
            Ok(Step::Wait {
                checkpoint: cp("waiting", serde_json::to_value(&call).unwrap()),
                condition: WaitFor::Tasks(vec![call.task_id()]),
                children: vec![],
            })
        } else {
            let call: NestedTaskCall = serde_json::from_value(task.checkpoint().data.clone())
                .map_err(|e| e.to_string())?;
            let child = docs.get_owned_child(call.task_id())?;
            Ok(Step::Succeed(serde_json::to_value(child.state()).unwrap()))
        }
    }
}
fn host(
    path: &Path,
    mode: Mode,
    valid: bool,
    marker: Option<PathBuf>,
    release: Arc<AtomicBool>,
    log: Option<PathBuf>,
    calls: Arc<AtomicUsize>,
) -> Driver {
    let preparation_gate = matches!(mode, Mode::Abort).then(|| release.clone());
    host_with_preparation_gate(
        path,
        mode,
        valid,
        marker,
        release,
        log,
        calls,
        preparation_gate,
    )
}
#[allow(clippy::too_many_arguments)]
fn host_with_preparation_gate(
    path: &Path,
    mode: Mode,
    valid: bool,
    marker: Option<PathBuf>,
    release: Arc<AtomicBool>,
    log: Option<PathBuf>,
    calls: Arc<AtomicUsize>,
    preparation_gate: Option<Arc<AtomicBool>>,
) -> Driver {
    let leaf: Arc<dyn Tool> = Arc::new(Leaf {
        calls,
        valid,
        log,
        preparation_gate,
    });
    let _binding = nested_identity::register(&leaf, Some("backend".into())).unwrap();
    let cell = Arc::new(OnceLock::new());
    let parent: Arc<dyn Handler> = Arc::new(Parent {
        leaf: cell.clone(),
        mode,
        marker,
        release,
    });
    let parent_tool = Arc::new(TaskTool::new(
        spec("parent"),
        "parent",
        true,
        1,
        parent.clone(),
    ));
    let mut registry = Registry::default();
    assert!(registry.register_shared("parent", true, parent));
    assert!(registry.register(
        "root",
        true,
        Root {
            parent: parent_tool
        }
    ));
    assert!(registry.set_abort("parent", |_: &Task| Ok(Step::Succeed(Value::Null))));
    assert!(registry.set_abort("root", |_: &Task| Ok(Step::Succeed(Value::Null))));
    let mut host =
        StandaloneNoHooksHost::open(path, registry, UnhookedAuthorization::standalone_only())
            .unwrap();
    let leaf = host.register_tool("leaf", leaf, "backend", 1).unwrap();
    assert!(cell.set(leaf).is_ok());
    host.into_driver()
}
fn seed(path: &Path) -> Uuid {
    let id = Uuid::new_v4();
    let root = Task::new(
        id,
        TaskOwner::Conversation(Uuid::new_v4()),
        cp("initial", Value::Null),
        8,
    )
    .unwrap()
    .registered("root".into(), true);
    let store = durable_store::Store::open(path).unwrap();
    pi_rs_durable::task_store::TaskStore::new(&store)
        .save(&[pi_rs_durable::task_store::Save::Create(&root)])
        .unwrap();
    id
}
fn until(mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !f() {
        assert!(Instant::now() < deadline, "timed out");
        std::thread::sleep(Duration::from_millis(5));
    }
}
fn marker_ids(path: &Path) -> (Uuid, Uuid) {
    let s = std::fs::read_to_string(path).unwrap();
    let mut parts = s.split_whitespace();
    (
        parts.next().unwrap().parse().unwrap(),
        parts.next().unwrap().parse().unwrap(),
    )
}
#[test]
fn schema_failure_and_raw_key_identity_ungated_through_both_admission_paths() {
    for mode in [Mode::Staged, Mode::Immediate] {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let id = seed(&dir.path().join("db"));
        let driver = host(
            &dir.path().join("db"),
            mode,
            false,
            None,
            Arc::new(AtomicBool::new(true)),
            None,
            calls.clone(),
        );
        until(|| {
            driver
                .get(id)
                .unwrap()
                .is_some_and(|task| task.is_terminal())
        });
        assert_eq!(
            driver.get(id).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!({"Succeeded":"invalid"}))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        driver.shutdown().unwrap();
    }
}

#[test]
fn schema_failure_and_raw_key_identity_through_both_admission_paths() {
    for mode in [Mode::Staged, Mode::Immediate] {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let id = seed(&dir.path().join("db"));
        let marker = dir.path().join("parent-ready");
        let preparation_gate = Arc::new(AtomicBool::new(false));
        let driver = host_with_preparation_gate(
            &dir.path().join("db"),
            mode,
            false,
            Some(marker.clone()),
            Arc::new(AtomicBool::new(true)),
            None,
            calls.clone(),
            Some(preparation_gate.clone()),
        );
        until(|| {
            std::fs::read_to_string(&marker).ok().is_some_and(|text| {
                let mut parts = text.split_whitespace();
                matches!(
                    (
                        parts.next().and_then(|id| id.parse::<Uuid>().ok()),
                        parts.next().and_then(|id| id.parse::<Uuid>().ok())
                    ),
                    (Some(_), Some(_))
                )
            })
        });
        let (parent, _) = marker_ids(&marker);
        until(|| {
            driver
                .get(parent)
                .unwrap()
                .is_some_and(|t| matches!(t.state(), TaskState::Waiting(_)))
        });
        preparation_gate.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(15);
        let root = loop {
            match driver.get(id) {
                Ok(Some(task)) if task.is_terminal() => break task,
                Ok(_) => assert!(Instant::now() < deadline, "timed out waiting for root"),
                Err(error) => {
                    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
                    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
                        .list()
                        .unwrap();
                    let report = driver.shutdown_with_pending().unwrap();
                    let handler_results: Vec<_> = report
                        .pending()
                        .iter()
                        .filter_map(|pending| {
                            if let pi_rs_durable::task_scheduler::PendingSettlement::Unprepared(
                                completed,
                            ) = pending
                            {
                                Some((
                                    completed.task_id(),
                                    completed.handler_result().map(|r| match r {
                                        Ok(_) => "ok".to_owned(),
                                        Err(error) => error.clone(),
                                    }),
                                ))
                            } else {
                                None
                            }
                        })
                        .collect();
                    eprintln!("pending handler results: {handler_results:?}");
                    panic!(
                        "driver failed: {error}; cause: {:?}; pending: {:?}; tasks: {tasks:?}",
                        report.failure(),
                        report.pending()
                    );
                }
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        assert_eq!(
            root.state(),
            &TaskState::Succeeded(json!({"Succeeded":"invalid"}))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        driver.shutdown().unwrap();
    }
}
#[test]
fn sealed_admission_kill_worker() {
    let Some(dir) = std::env::var_os("PI_SEALED_KILL_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let id = seed(&dir.join("db"));
    let driver = host(
        &dir.join("db"),
        Mode::Kill,
        true,
        Some(dir.join("admitted")),
        Arc::new(AtomicBool::new(false)),
        Some(dir.join("effects")),
        Arc::new(AtomicUsize::new(0)),
    );
    std::fs::write(dir.join("root"), id.to_string()).unwrap();
    until(|| dir.join("admitted").exists());
    let (_, child) = marker_ids(&dir.join("admitted"));
    until(|| {
        driver
            .get(child)
            .unwrap()
            .is_some_and(|task| task.is_terminal())
    });
    std::fs::write(dir.join("ready"), "child settled, parent still running").unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
fn sealed_admission_survives_kill_before_parent_park() {
    let dir = tempfile::tempdir().unwrap();
    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut process = ChildGuard(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "sealed_admission_kill_worker"])
            .env("PI_SEALED_KILL_DIR", dir.path())
            .spawn()
            .unwrap(),
    );
    let marker = dir.path().join("admitted");
    until(|| dir.path().join("ready").exists() || process.0.try_wait().unwrap().is_some());
    assert!(
        dir.path().join("ready").exists(),
        "worker exited before admission was ready"
    );
    let (parent, child) = marker_ids(&marker);
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let root: Uuid = std::fs::read_to_string(dir.path().join("root"))
        .unwrap()
        .parse()
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let driver = host(
        &dir.path().join("db"),
        Mode::Immediate,
        true,
        None,
        Arc::new(AtomicBool::new(true)),
        Some(dir.path().join("effects")),
        calls,
    );
    until(|| driver.get(root).unwrap().is_some_and(|t| t.is_terminal()));
    assert_eq!(
        driver.get(root).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!({"Succeeded":"ok"}))
    );
    assert!(driver.get(parent).unwrap().unwrap().is_terminal());
    assert!(driver.get(child).unwrap().unwrap().is_terminal());
    assert_eq!(
        std::fs::read_to_string(dir.path().join("effects"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    driver.shutdown().unwrap();
}
#[test]
fn abort_after_sealed_admission_before_park() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("admitted");
    let release = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let _root = seed(&dir.path().join("db"));
    let driver = host(
        &dir.path().join("db"),
        Mode::Abort,
        true,
        Some(marker.clone()),
        release.clone(),
        None,
        calls.clone(),
    );
    until(|| marker.exists());
    let (parent, child) = marker_ids(&marker);
    driver.request_abort(parent).unwrap();
    release.store(true, Ordering::SeqCst);
    until(|| {
        driver.get(parent).unwrap().is_some_and(|t| t.is_terminal())
            && driver.get(child).unwrap().is_some_and(|t| t.is_terminal())
    });
    assert!(driver.get(parent).unwrap().unwrap().abort_requested());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
}

struct RewriteLeaf {
    invocations: Arc<AtomicUsize>,
    log: Option<PathBuf>,
}
#[async_trait]
impl Tool for RewriteLeaf {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "rewrite-leaf".into(),
            description: String::new(),
            parameters: json!({"type":"object", "required":["normalized", "value"], "properties":{"normalized":{"const":true},"value":{"type":"string"}}}),
        }
    }
    async fn prepare_arguments(&self, mut input: Value) -> pi_rs_core::ExtensionResult<Value> {
        input["normalized"] = json!(true);
        input["value"] = json!("prepared");
        Ok(input)
    }
    async fn invoke(&self, input: Value, _: ToolContext) -> Outcome {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        if let Some(path) = &self.log {
            record(
                path,
                &format!("invoke:{}", input["value"].as_str().unwrap()),
            );
        }
        Outcome::Succeeded(input["value"].as_str().unwrap().to_string())
    }
}
fn record(path: &Path, line: &str) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{line}").unwrap();
    file.sync_all().unwrap();
}
struct RewriteHook {
    calls: Arc<AtomicUsize>,
    entered: Arc<AtomicBool>,
    pending: bool,
    log: Option<PathBuf>,
}
#[async_trait]
impl pi_rs_durable::extension::Hook for RewriteHook {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::BeforeTool, String> {
        assert_eq!(call.arguments["value"], json!("prepared"));
        assert_eq!(call.arguments["normalized"], json!(true));
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(path) = &self.log {
            record(path, "hook");
        }
        self.entered.store(true, Ordering::SeqCst);
        if self.pending {
            std::future::pending::<()>().await;
        }
        let mut rewritten = call.arguments.clone();
        rewritten["value"] = json!("rewritten");
        Ok(pi_rs_durable::extension::BeforeTool::Rewrite(rewritten))
    }
}
#[allow(clippy::too_many_arguments)] // Explicit knobs keep crash and abort fixtures readable.
fn hook_host(
    path: &Path,
    marker: Option<PathBuf>,
    release: Arc<AtomicBool>,
    pending: bool,
    entered: Arc<AtomicBool>,
    hook_calls: Arc<AtomicUsize>,
    invocations: Arc<AtomicUsize>,
    log: Option<PathBuf>,
) -> Driver {
    let tool: Arc<dyn Tool> = Arc::new(RewriteLeaf {
        invocations,
        log: log.clone(),
    });
    let _binding = nested_identity::register(&tool, Some("hook-backend".into())).unwrap();
    let cell = Arc::new(OnceLock::new());
    let parent: Arc<dyn Handler> = Arc::new(Parent {
        leaf: cell.clone(),
        mode: Mode::Immediate,
        marker,
        release,
    });
    let parent_tool = Arc::new(TaskTool::new(
        spec("parent"),
        "parent",
        true,
        1,
        parent.clone(),
    ));
    let mut registry = Registry::default();
    assert!(registry.register_shared("parent", true, parent));
    assert!(registry.register(
        "root",
        true,
        Root {
            parent: parent_tool
        }
    ));
    assert!(registry.set_abort("parent", |_: &Task| Ok(Step::Succeed(Value::Null))));
    assert!(registry.set_abort("root", |_: &Task| Ok(Step::Succeed(Value::Null))));
    let mut host =
        pi_rs_durable::async_task_tool::StandaloneBeforeHookHost::open(path, registry).unwrap();
    let hook = Arc::new(RewriteHook {
        calls: hook_calls,
        entered,
        pending,
        log,
    });
    let leaf = host
        .register_tool("leaf", tool, "hook-backend", 1, "rewrite-hook-v1", hook)
        .unwrap();
    assert!(cell.set(leaf).is_ok());
    host.into_driver()
}
#[test]
fn immediate_before_hook_rewrites_while_parent_is_before_park() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("admitted");
    let root = seed(&dir.path().join("db"));
    let release = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(AtomicBool::new(false));
    let hooks = Arc::new(AtomicUsize::new(0));
    let invokes = Arc::new(AtomicUsize::new(0));
    let driver = hook_host(
        &dir.path().join("db"),
        Some(marker.clone()),
        release.clone(),
        false,
        entered,
        hooks.clone(),
        invokes.clone(),
        None,
    );
    until(|| marker.exists());
    let (parent, child) = marker_ids(&marker);
    until(|| driver.get(child).unwrap().is_some_and(|t| t.is_terminal()));
    assert_eq!(
        driver.get(parent).unwrap().unwrap().state(),
        &TaskState::Running
    );
    assert_eq!(hooks.load(Ordering::SeqCst), 1);
    assert_eq!(invokes.load(Ordering::SeqCst), 1);
    release.store(true, Ordering::SeqCst);
    until(|| driver.get(root).unwrap().is_some_and(|t| t.is_terminal()));
    assert_eq!(
        driver.get(root).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!({"Succeeded":"rewritten"}))
    );
    driver.shutdown().unwrap();
}
#[test]
fn immediate_before_hook_abort_during_preflight_before_parent_park() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("admitted");
    let _root = seed(&dir.path().join("db"));
    let release = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(AtomicBool::new(false));
    let hooks = Arc::new(AtomicUsize::new(0));
    let invokes = Arc::new(AtomicUsize::new(0));
    let driver = hook_host(
        &dir.path().join("db"),
        Some(marker.clone()),
        release.clone(),
        true,
        entered.clone(),
        hooks.clone(),
        invokes.clone(),
        None,
    );
    until(|| marker.exists() && entered.load(Ordering::SeqCst));
    let (parent, child) = marker_ids(&marker);
    assert_eq!(
        driver.get(parent).unwrap().unwrap().state(),
        &TaskState::Running
    );
    driver.request_abort(parent).unwrap();
    release.store(true, Ordering::SeqCst);
    until(|| {
        driver.get(parent).unwrap().is_some_and(|t| t.is_terminal())
            && driver.get(child).unwrap().is_some_and(|t| t.is_terminal())
    });
    assert!(driver.get(parent).unwrap().unwrap().abort_requested());
    assert_eq!(hooks.load(Ordering::SeqCst), 1);
    assert_eq!(invokes.load(Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
}
#[test]
fn immediate_before_hook_kill_worker() {
    let Some(dir) = std::env::var_os("PI_HOOK_IMMEDIATE_KILL_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let root = seed(&dir.join("db"));
    let driver = hook_host(
        &dir.join("db"),
        Some(dir.join("admitted")),
        Arc::new(AtomicBool::new(false)),
        false,
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Some(dir.join("events")),
    );
    std::fs::write(dir.join("root"), root.to_string()).unwrap();
    until(|| dir.join("admitted").exists());
    let (parent, child) = marker_ids(&dir.join("admitted"));
    until(|| driver.get(child).unwrap().is_some_and(|t| t.is_terminal()));
    assert_eq!(
        driver.get(parent).unwrap().unwrap().state(),
        &TaskState::Running
    );
    std::fs::write(dir.join("ready"), "ready").unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
fn immediate_before_hook_reconnects_after_kill_before_parent_park() {
    let dir = tempfile::tempdir().unwrap();
    struct Guard(std::process::Child);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut process = Guard(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "immediate_before_hook_kill_worker"])
            .env("PI_HOOK_IMMEDIATE_KILL_DIR", dir.path())
            .spawn()
            .unwrap(),
    );
    until(|| dir.path().join("ready").exists() || process.0.try_wait().unwrap().is_some());
    assert!(
        dir.path().join("ready").exists(),
        "worker failed before ready"
    );
    let (parent, child) = marker_ids(&dir.path().join("admitted"));
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let root: Uuid = std::fs::read_to_string(dir.path().join("root"))
        .unwrap()
        .parse()
        .unwrap();
    let hooks = Arc::new(AtomicUsize::new(0));
    let invokes = Arc::new(AtomicUsize::new(0));
    let driver = hook_host(
        &dir.path().join("db"),
        None,
        Arc::new(AtomicBool::new(true)),
        false,
        Arc::new(AtomicBool::new(false)),
        hooks.clone(),
        invokes.clone(),
        Some(dir.path().join("events")),
    );
    until(|| driver.get(root).unwrap().is_some_and(|t| t.is_terminal()));
    assert_eq!(
        driver.get(root).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!({"Succeeded":"rewritten"}))
    );
    assert!(driver.get(parent).unwrap().unwrap().is_terminal());
    assert!(driver.get(child).unwrap().unwrap().is_terminal());
    assert_eq!(hooks.load(Ordering::SeqCst), 0);
    assert_eq!(invokes.load(Ordering::SeqCst), 0);
    assert_eq!(
        std::fs::read_to_string(dir.path().join("events")).unwrap(),
        "hook\ninvoke:rewritten\n"
    );
    driver.shutdown().unwrap();
}

struct IntegrationAfterHook {
    fail_memo: bool,
    before_calls: Arc<AtomicUsize>,
    after_calls: Arc<AtomicUsize>,
}
#[async_trait]
impl pi_rs_durable::extension::Hook for IntegrationAfterHook {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::BeforeTool, String> {
        assert_eq!(call.arguments["value"], json!("prepared"));
        self.before_calls.fetch_add(1, Ordering::SeqCst);
        let mut args = call.arguments.clone();
        args["value"] = json!("before");
        Ok(pi_rs_durable::extension::BeforeTool::Rewrite(args))
    }
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &Value,
        ctx: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<Option<Value>, String> {
        assert_eq!(call.arguments["value"], json!("before"));
        assert_eq!(*result, json!("before"));
        self.after_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_memo {
            // Even if ignored by the hook, the failed fenced operation rejects
            // the after phase and preserves the raw result for the parent.
            let _ = ctx.memo.get_or_insert("", json!(true)).await;
        } else {
            assert_eq!(
                ctx.memo.get_or_insert("phase", json!("after")).await?,
                json!("after")
            );
        }
        Ok(Some(json!("after")))
    }
}
fn after_integration_host(
    path: &Path,
    fail_memo: bool,
    before_calls: Arc<AtomicUsize>,
    after_calls: Arc<AtomicUsize>,
    invocations: Arc<AtomicUsize>,
) -> Driver {
    let tool: Arc<dyn Tool> = Arc::new(RewriteLeaf {
        invocations,
        log: None,
    });
    let _binding = nested_identity::register(&tool, Some("after-backend".into())).unwrap();
    let cell = Arc::new(OnceLock::new());
    let parent: Arc<dyn Handler> = Arc::new(Parent {
        leaf: cell.clone(),
        mode: Mode::Immediate,
        marker: None,
        release: Arc::new(AtomicBool::new(true)),
    });
    let parent_tool = Arc::new(TaskTool::new(
        spec("parent"),
        "parent",
        true,
        1,
        parent.clone(),
    ));
    let mut registry = Registry::default();
    assert!(registry.register_shared("parent", true, parent));
    assert!(registry.register(
        "root",
        true,
        Root {
            parent: parent_tool
        }
    ));
    assert!(registry.set_abort("parent", |_: &Task| Ok(Step::Succeed(Value::Null))));
    assert!(registry.set_abort("root", |_: &Task| Ok(Step::Succeed(Value::Null))));
    let mut host =
        pi_rs_durable::async_task_tool::StandaloneBeforeHookHost::open(path, registry).unwrap();
    let hook = Arc::new(IntegrationAfterHook {
        fail_memo,
        before_calls,
        after_calls,
    });
    let adapter = host
        .register_tool_with_after("leaf", tool, "after-backend", 1, "both-hooks-v1", hook)
        .unwrap();
    assert!(cell.set(adapter).is_ok());
    host.into_driver()
}
#[test]
fn immediate_child_runs_before_and_after_with_fenced_memo() {
    let dir = tempfile::tempdir().unwrap();
    let root = seed(&dir.path().join("db"));
    let before = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let driver = after_integration_host(
        &dir.path().join("db"),
        false,
        before.clone(),
        after.clone(),
        calls.clone(),
    );
    until(|| driver.get(root).unwrap().is_some_and(|t| t.is_terminal()));
    assert_eq!(
        driver.get(root).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!({"Succeeded":"after"}))
    );
    assert_eq!(
        (
            before.load(Ordering::SeqCst),
            calls.load(Ordering::SeqCst),
            after.load(Ordering::SeqCst)
        ),
        (1, 1, 1)
    );
    driver.shutdown().unwrap();
}
#[test]
fn parent_observes_raw_result_when_after_memo_fails() {
    let dir = tempfile::tempdir().unwrap();
    let root = seed(&dir.path().join("db"));
    let before = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let driver = after_integration_host(
        &dir.path().join("db"),
        true,
        before.clone(),
        after.clone(),
        calls.clone(),
    );
    until(|| driver.get(root).unwrap().is_some_and(|t| t.is_terminal()));
    assert_eq!(
        driver.get(root).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!({"Succeeded":"after-incomplete:before"}))
    );
    assert_eq!(
        (
            before.load(Ordering::SeqCst),
            calls.load(Ordering::SeqCst),
            after.load(Ordering::SeqCst)
        ),
        (1, 1, 1)
    );
    driver.shutdown().unwrap();
}

#[cfg(feature = "test-hooks")]
#[test]
fn sealed_receipt_busy_retains_outcome_and_revalidates_lease() {
    use pi_rs_durable::task_scheduler::hook_fence_test_hook;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let root = seed(&path);
    let release = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let driver = host(
        &path,
        Mode::Abort,
        true,
        None,
        release.clone(),
        None,
        calls.clone(),
    );
    let store = durable_store::Store::open(&path).unwrap();
    until(|| {
        pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap()
            .iter()
            .any(|item| item.task.kind() == Some("leaf"))
    });
    let child = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap()
        .into_iter()
        .find(|item| item.task.kind() == Some("leaf"))
        .unwrap()
        .task
        .id();
    let (at_tx, at_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let (busy_tx, busy_rx) = std::sync::mpsc::channel();
    hook_fence_test_hook::install(child, "receipt", move || {
        at_tx.send(()).unwrap();
        resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    });
    hook_fence_test_hook::install(child, "receipt-busy", move || {
        busy_tx.send(()).unwrap();
    });
    release.store(true, Ordering::SeqCst);
    at_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        locked_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(6500));
        conn.execute_batch("COMMIT").unwrap();
    });
    locked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    resume_tx.send(()).unwrap();
    busy_rx
        .recv_timeout(Duration::from_secs(7))
        .expect("receipt capture did not reach busy timeout");
    writer.join().unwrap();
    until(|| {
        driver
            .get(root)
            .unwrap()
            .is_some_and(|task| task.is_terminal())
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        driver.get(root).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!({"Succeeded":"ok"}))
    );
    driver.shutdown().unwrap();
}
