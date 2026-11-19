use async_trait::async_trait;
use pi_rs_core::{ExtensionResult, Outcome, Tool, ToolContext, ToolSpec};
use pi_rs_durable::{
    async_task_tool::{
        AsyncTaskCall, AsyncTaskResult, DurableAsyncTool, DurableTaskHost, DurableToolResume,
        DurableToolStep, DurableWait, ReplaySafeAuthorization, StandaloneNoHooksHost,
        UnhookedAuthorization,
    },
    task::TaskState,
    task_scheduler::Registry,
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct NormalizingTool {
    name: &'static str,
    prepares: Arc<AtomicUsize>,
    root: bool,
}
#[async_trait]
impl Tool for NormalizingTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name.into(),
            description: String::new(),
            parameters: json!({"type":"object", "required":["prepared"], "properties":{"prepared":{"type":"integer"}}}),
        }
    }
    async fn prepare_arguments(&self, mut args: Value) -> ExtensionResult<Value> {
        let next = self.prepares.fetch_add(1, Ordering::SeqCst) + 1;
        args["prepared"] = json!(next);
        Ok(args)
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for NormalizingTool {
    async fn execute_durable(
        &self,
        args: Value,
        _: ToolContext,
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        if args["prepared"] != 1 {
            return Err(format!("prepared arguments changed: {args}"));
        }
        if !self.root {
            return Ok(DurableToolStep::Complete(Outcome::Succeeded(
                "child".into(),
            )));
        }
        match resume.checkpoint {
            None => {
                let child = host
                    .start_child("child", "normalized-child", json!({}))
                    .await?;
                Ok(DurableToolStep::Park {
                    checkpoint: json!({"child":child}),
                    wait: DurableWait::All(vec![child]),
                })
            }
            Some(checkpoint) => {
                let child: AsyncTaskCall = serde_json::from_value(checkpoint["child"].clone())
                    .map_err(|e| e.to_string())?;
                if resume.ended != vec![child.task_id()] {
                    return Err("bad wake".into());
                }
                if !matches!(host.child_result(&child).await?, Some(AsyncTaskResult::Outcome(Outcome::Succeeded(value))) if value == "child")
                {
                    return Err("bad child outcome".into());
                }
                Ok(DurableToolStep::Complete(Outcome::Succeeded("done".into())))
            }
        }
    }
}

#[test]
fn public_root_nested_normalization_and_resume_use_prepared_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let mut host = StandaloneNoHooksHost::open(
        dir.path().join("db"),
        Registry::default(),
        UnhookedAuthorization::standalone_only(),
    )
    .unwrap();
    let root_prepares = Arc::new(AtomicUsize::new(0));
    let child_prepares = Arc::new(AtomicUsize::new(0));
    let child = host
        .register_replay_safe_durable_tool(
            Arc::new(NormalizingTool {
                name: "normalized-child",
                prepares: child_prepares.clone(),
                root: false,
            }),
            "child-v1",
            1,
            vec![],
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap();
    let root = host
        .register_replay_safe_durable_tool(
            Arc::new(NormalizingTool {
                name: "normalized-root",
                prepares: root_prepares.clone(),
                root: true,
            }),
            "root-v1",
            1,
            vec![child],
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap();
    let id = host.start_replay_safe_root(&root, json!({}), 3).unwrap();
    let driver = host.into_driver();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let task = driver.get(id).unwrap().unwrap();
        match task.state() {
            TaskState::Succeeded(value) => {
                assert_eq!(
                    *value,
                    serde_json::to_value(Outcome::Succeeded("done".into())).unwrap()
                );
                break;
            }
            TaskState::Failed(reason) => panic!("root failed: {reason}"),
            _ => assert!(
                std::time::Instant::now() < deadline,
                "root did not finish: {:?}",
                task.state()
            ),
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(root_prepares.load(Ordering::SeqCst), 1);
    assert_eq!(child_prepares.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
}

struct Gate;
impl pi_rs_durable::task_scheduler::Handler for Gate {
    fn run(
        &self,
        task: &pi_rs_durable::task::Task,
    ) -> Result<pi_rs_durable::task_scheduler::Step, String> {
        use pi_rs_durable::task::{Checkpoint, WaitFor};
        use pi_rs_durable::task_scheduler::Step;
        match task.checkpoint().phase.as_str() {
            "initial" => Ok(Step::Wait {
                checkpoint: Checkpoint {
                    phase: "gate".into(),
                    data: json!(null),
                },
                condition: WaitFor::Signal("go".into()),
                children: vec![],
            }),
            "gate" => Ok(Step::Succeed(
                serde_json::to_value(Outcome::Succeeded("child".into())).unwrap(),
            )),
            _ => Err("bad checkpoint".into()),
        }
    }
}
fn gated_host(
    path: &std::path::Path,
    prepares: Arc<AtomicUsize>,
) -> (
    StandaloneNoHooksHost,
    Arc<pi_rs_durable::task_tool::TaskTool>,
) {
    let leaf: Arc<dyn pi_rs_durable::task_scheduler::Handler> = Arc::new(Gate);
    let mut registry = Registry::default();
    assert!(registry.register_shared("public-gate", true, leaf.clone()));
    let child = Arc::new(pi_rs_durable::task_tool::TaskTool::new(
        ToolSpec {
            name: "normalized-child".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        },
        "public-gate",
        true,
        1,
        leaf,
    ));
    let mut host =
        StandaloneNoHooksHost::open(path, registry, UnhookedAuthorization::standalone_only())
            .unwrap();
    let root = host
        .register_replay_safe_durable_tool(
            Arc::new(NormalizingTool {
                name: "normalized-root",
                prepares,
                root: true,
            }),
            "root-v1",
            1,
            vec![child],
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap();
    (host, root)
}
#[test]
fn public_root_reopen_preserves_prepared_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let first_prepares = Arc::new(AtomicUsize::new(0));
    let (host, root) = gated_host(&path, first_prepares.clone());
    let id = host.start_replay_safe_root(&root, json!({}), 3).unwrap();
    let driver = host.into_driver();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let child = loop {
        let task = driver.get(id).unwrap().unwrap();
        if matches!(task.state(), TaskState::Waiting(_)) {
            let call: AsyncTaskCall =
                serde_json::from_value(task.checkpoint().data["checkpoint"]["child"].clone())
                    .unwrap();
            if driver
                .get(call.task_id())
                .unwrap()
                .is_some_and(|t| matches!(t.state(), TaskState::Waiting(_)))
            {
                break call.task_id();
            }
        }
        assert!(std::time::Instant::now() < deadline, "child did not park");
        std::thread::sleep(std::time::Duration::from_millis(5));
    };
    driver.shutdown().unwrap();
    assert_eq!(first_prepares.load(Ordering::SeqCst), 1);
    let reopened_prepares = Arc::new(AtomicUsize::new(200));
    let (host, _root) = gated_host(&path, reopened_prepares.clone());
    let driver = host.into_driver();
    driver
        .signal(&pi_rs_durable::task_store::Signal::new(
            uuid::Uuid::new_v4(),
            child,
            "go".into(),
            json!(null),
        ))
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let task = driver.get(id).unwrap().unwrap();
        match task.state() {
            TaskState::Succeeded(value) => {
                assert_eq!(
                    *value,
                    serde_json::to_value(Outcome::Succeeded("done".into())).unwrap()
                );
                break;
            }
            TaskState::Failed(reason) => panic!("root failed after reopen: {reason}"),
            _ => assert!(std::time::Instant::now() < deadline, "root did not resume"),
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(reopened_prepares.load(Ordering::SeqCst), 200);
    driver.shutdown().unwrap();
}

#[test]
fn public_root_rejects_foreign_registration_and_non_object_arguments() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let (host_a, tool_a) = gated_host(&first.path().join("db"), Arc::new(AtomicUsize::new(0)));
    let (host_b, tool_b) = gated_host(&second.path().join("db"), Arc::new(AtomicUsize::new(0)));
    assert!(
        host_b
            .start_replay_safe_root(&tool_a, json!({}), 3)
            .is_err()
    );
    assert!(
        host_b
            .start_replay_safe_root(&tool_b, json!([]), 3)
            .is_err()
    );
    assert!(
        host_b
            .start_replay_safe_root(&tool_b, json!({}), 0)
            .is_err()
    );
    host_a.into_driver().shutdown().unwrap();
    host_b.into_driver().shutdown().unwrap();
}

struct Node {
    name: &'static str,
    child: &'static str,
    expected: &'static str,
}
#[async_trait]
impl Tool for Node {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name.into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for Node {
    async fn execute_durable(
        &self,
        _: Value,
        _: ToolContext,
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        match resume.checkpoint {
            None => {
                let call = host.start_child("child", self.child, json!({})).await?;
                Ok(DurableToolStep::Park {
                    checkpoint: json!({"call":call}),
                    wait: DurableWait::All(vec![call]),
                })
            }
            Some(data) => {
                let call: AsyncTaskCall =
                    serde_json::from_value(data["call"].clone()).map_err(|e| e.to_string())?;
                if resume.ended != vec![call.task_id()] {
                    return Err("bad ended children".into());
                }
                if !matches!(host.child_result(&call).await?, Some(AsyncTaskResult::Outcome(Outcome::Succeeded(value))) if value == self.expected)
                {
                    return Err("bad child result".into());
                }
                Ok(DurableToolStep::Complete(Outcome::Succeeded(
                    self.name.into(),
                )))
            }
        }
    }
}
#[test]
fn public_root_runs_two_levels_of_async_child_parks() {
    let dir = tempfile::tempdir().unwrap();
    let mut host = StandaloneNoHooksHost::open(
        dir.path().join("db"),
        Registry::default(),
        UnhookedAuthorization::standalone_only(),
    )
    .unwrap();
    let leaf = host
        .register_replay_safe_durable_tool(
            Arc::new(NormalizingTool {
                name: "normalized-child",
                prepares: Arc::new(AtomicUsize::new(0)),
                root: false,
            }),
            "leaf-v1",
            1,
            vec![],
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap();
    let middle = host
        .register_replay_safe_durable_tool(
            Arc::new(Node {
                name: "middle",
                child: "normalized-child",
                expected: "child",
            }),
            "middle-v1",
            1,
            vec![leaf],
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap();
    let root = host
        .register_replay_safe_durable_tool(
            Arc::new(Node {
                name: "root",
                child: "middle",
                expected: "middle",
            }),
            "root-v1",
            1,
            vec![middle],
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap();
    let id = host.start_replay_safe_root(&root, json!({}), 3).unwrap();
    let driver = host.into_driver();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let task = driver.get(id).unwrap().unwrap();
        match task.state() {
            TaskState::Succeeded(value) => {
                assert_eq!(
                    *value,
                    serde_json::to_value(Outcome::Succeeded("root".into())).unwrap()
                );
                break;
            }
            TaskState::Failed(reason) => panic!("root failed: {reason}"),
            _ => assert!(std::time::Instant::now() < deadline, "root did not finish"),
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    driver.shutdown().unwrap();
}
