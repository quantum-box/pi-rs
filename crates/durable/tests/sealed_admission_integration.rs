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
                _ => leaf.start_child_now(admission, "leaf", input.clone())?,
            };
            let repeated = match self.mode {
                Mode::Staged => leaf.start_child(task, docs, "leaf", input)?,
                _ => leaf.start_child_now(admission, "leaf", input)?,
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
fn schema_failure_and_raw_key_identity_through_both_admission_paths() {
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
        until(|| driver.get(id).unwrap().is_some_and(|t| t.is_terminal()));
        let root = driver.get(id).unwrap().unwrap();
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
