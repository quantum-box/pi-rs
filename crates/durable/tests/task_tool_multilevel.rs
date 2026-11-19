use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    Error, Message, Model, ModelEvent, ModelRequest, ModelStream, ToolCall, ToolSpec,
};
use pi_rs_durable::{
    document::TaskDocuments,
    recovery::UncertainPolicy,
    runtime::{Config, Options, Scheduler, Status},
    task::{Checkpoint, Task, TaskState, WaitFor},
    task_scheduler::{Handler, Registry, Step},
    task_store::Signal,
    task_tool::{NestedTaskCall, NestedTaskResult, TaskTool},
};
use serde_json::json;
use std::{
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use uuid::Uuid;

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
                        name: "root".into(),
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
struct Parent {
    child: Arc<TaskTool>,
    level: &'static str,
    ids: Arc<Mutex<Vec<(Uuid, Uuid)>>>,
}
impl Handler for Parent {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        match task.checkpoint().phase.as_str() {
            "initial" => {
                if self.level == "root" && std::env::var_os("PI_MULTILEVEL_RUNNING").is_some() {
                    println!("RUNNING_ROOT {}", task.id());
                    std::io::stdout().flush().unwrap();
                    loop {
                        std::thread::park();
                    }
                }
                let call = self.child.start_child(task, docs, "child", json!({}))?;
                let id = call.task_id();
                self.ids.lock().unwrap().push((task.id(), id));
                Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "child".into(),
                        data: json!({"level":self.level,"call":call}),
                    },
                    condition: WaitFor::TasksAny(vec![id]),
                    children: vec![],
                })
            }
            "child" => {
                if self.level == "root" && std::env::var_os("PI_MULTILEVEL_WAKE_RUNNING").is_some()
                {
                    println!("RUNNING_WAKE {}", task.id());
                    std::io::stdout().flush().unwrap();
                    loop {
                        std::thread::park();
                    }
                }
                let data = &task.checkpoint().data;
                if data["level"] != self.level {
                    return Err("checkpoint lost".into());
                }
                let call: NestedTaskCall =
                    serde_json::from_value(data["call"].clone()).map_err(|e| e.to_string())?;
                let reconnected = self.child.start_child(task, docs, "child", json!({}))?;
                if reconnected.task_id() != call.task_id() {
                    return Err("key reconnected to a different child".into());
                }
                if task.ended_children() != Some(&[call.task_id()][..]) {
                    return Err("wrong ended children".into());
                }
                let expected = if self.level == "middle" {
                    "leaf-result"
                } else {
                    "middle-result"
                };
                if call.result(task, docs)? != Some(NestedTaskResult::Succeeded(json!(expected))) {
                    return Err("wrong child result".into());
                }
                if self.level == "middle" {
                    Ok(Step::Wait {
                        checkpoint: Checkpoint {
                            phase: "gate".into(),
                            data: data.clone(),
                        },
                        condition: WaitFor::Signal("release".into()),
                        children: vec![],
                    })
                } else {
                    Ok(Step::Succeed(json!("root-result")))
                }
            }
            "gate" => {
                if task.checkpoint().data["level"] != "middle" {
                    return Err("gate checkpoint lost".into());
                }
                Ok(Step::Succeed(json!("middle-result")))
            }
            _ => Err("unknown checkpoint".into()),
        }
    }
}
struct Leaf {
    count: std::path::PathBuf,
    park: bool,
}
impl Handler for Leaf {
    fn run(&self, task: &Task) -> Result<Step, String> {
        if self.park {
            return Ok(Step::Wait {
                checkpoint: Checkpoint {
                    phase: "parked".into(),
                    data: json!({"leaf":"checkpoint"}),
                },
                condition: WaitFor::Signal("never".into()),
                children: vec![],
            });
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.count)
            .map_err(|e| e.to_string())?;
        writeln!(file, "{}", task.id()).map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        Ok(Step::Succeed(json!("leaf-result")))
    }
}
struct Fixture {
    root: Arc<dyn Handler>,
    middle: Arc<dyn Handler>,
    leaf: Arc<dyn Handler>,
    tool: Arc<TaskTool>,
    ids: Arc<Mutex<Vec<(Uuid, Uuid)>>>,
}
impl Fixture {
    fn new(path: &std::path::Path, park: bool) -> Self {
        let ids = Arc::new(Mutex::new(Vec::new()));
        let leaf: Arc<dyn Handler> = Arc::new(Leaf {
            count: path.join("count"),
            park,
        });
        let leaf_tool = Arc::new(TaskTool::new(spec("leaf"), "leaf", true, 1, leaf.clone()));
        let middle: Arc<dyn Handler> = Arc::new(Parent {
            child: leaf_tool,
            level: "middle",
            ids: ids.clone(),
        });
        let middle_tool = Arc::new(TaskTool::new(
            spec("middle"),
            "middle",
            true,
            1,
            middle.clone(),
        ));
        let root: Arc<dyn Handler> = Arc::new(Parent {
            child: middle_tool,
            level: "root",
            ids: ids.clone(),
        });
        let tool = Arc::new(TaskTool::new(spec("root"), "root", false, 1, root.clone()));
        Self {
            root,
            middle,
            leaf,
            tool,
            ids,
        }
    }
    fn options(&self) -> Options {
        Options {
            model: Arc::new(ModelStub),
            tools: vec![self.tool.register()],
            restart_limit: 5,
            max_turns: 8,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        }
    }
    fn registry(&self) -> Registry {
        let mut r = Registry::default();
        for (name, handler) in [
            ("root", &self.root),
            ("middle", &self.middle),
            ("leaf", &self.leaf),
        ] {
            assert!(r.register_shared(name, name != "root", handler.clone()));
            assert!(r.set_abort(name, |_: &Task| Ok(Step::Succeed(json!(null)))));
        }
        r
    }
}
async fn until(mut f: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("timeout");
}
fn child_id(t: &Task) -> Uuid {
    serde_json::from_value::<NestedTaskCall>(t.checkpoint().data["call"].clone())
        .unwrap()
        .task_id()
}

#[tokio::test]
async fn multilevel_process_child() {
    let Ok(dir) = std::env::var("PI_MULTILEVEL_DIR") else {
        return;
    };
    let path = std::path::Path::new(&dir);
    let park = std::env::var_os("PI_MULTILEVEL_ABORT").is_some();
    let f = Fixture::new(path, park);
    let mut scheduler = Scheduler::open(path.join("db"), f.options()).unwrap();
    let driver = scheduler.start_tasks(f.registry()).unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(conversation, None, "work".into()).unwrap();
    let mut found = None;
    until(|| {
        let ids = f.ids.lock().unwrap();
        if ids.len() < 2 {
            return false;
        }
        let root = ids[0].0;
        let middle = ids[0].1;
        let leaf = ids[1].1;
        assert_eq!(ids[1].0, middle);
        // IDs are shared by handlers before their staged children commit.
        // Wait for each record; a storage error must still fail the test.
        let (Some(root_task), Some(middle_task), Some(leaf_task)) = (
            driver.get(root).unwrap(),
            driver.get(middle).unwrap(),
            driver.get(leaf).unwrap(),
        ) else {
            return false;
        };
        let ready = matches!(root_task.state(), TaskState::Waiting(_))
            && root_task.checkpoint().phase == "child"
            && if park {
                matches!(middle_task.state(), TaskState::Waiting(_))
                    && middle_task.checkpoint().phase == "child"
                    && leaf_task.checkpoint().phase == "parked"
                    && matches!(leaf_task.state(), TaskState::Waiting(_))
            } else {
                middle_task.checkpoint().phase == "gate"
                    && matches!(middle_task.state(), TaskState::Waiting(_))
                    && matches!(leaf_task.state(), TaskState::Succeeded(_))
            };
        if ready {
            found = Some((root, middle, leaf));
        }
        ready
    })
    .await;
    let (root, middle, leaf) = found.unwrap();
    println!("MULTILEVEL_READY {conversation} {root} {middle} {leaf}");
    std::io::stdout().flush().unwrap();
    if std::env::var_os("PI_MULTILEVEL_WAKE_RUNNING").is_some() {
        driver
            .signal(&Signal::new(
                Uuid::new_v4(),
                middle,
                "release".into(),
                json!(null),
            ))
            .unwrap();
    }
    std::future::pending::<()>().await;
}
struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn launch(path: &std::path::Path, abort: bool) -> (KillOnDrop, [Uuid; 4]) {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args(["--exact", "multilevel_process_child", "--nocapture"])
        .env("PI_MULTILEVEL_DIR", path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    if abort {
        cmd.env("PI_MULTILEVEL_ABORT", "1");
    }
    let mut child = KillOnDrop(cmd.spawn().unwrap());
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let line = line.unwrap();
            if let Some((_, rest)) = line.split_once("MULTILEVEL_READY ") {
                let ids: Vec<Uuid> = rest
                    .split_whitespace()
                    .map(|s| s.parse().unwrap())
                    .collect();
                tx.send([ids[0], ids[1], ids[2], ids[3]]).unwrap();
                break;
            }
        }
    });
    let ids = rx
        .recv_timeout(Duration::from_secs(25))
        .expect("child did not reach checkpoint");
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    (child, ids)
}
#[tokio::test]
async fn multilevel_kill_reopen_no_leaf_reexecution() {
    let dir = tempfile::tempdir().unwrap();
    let (_child, [conversation, root, middle, leaf]) = launch(dir.path(), false);
    let f = Fixture::new(dir.path(), false);
    let mut scheduler = Scheduler::open(dir.path().join("db"), f.options()).unwrap();
    let driver = scheduler.start_tasks(f.registry()).unwrap();
    let root_task = driver.get(root).unwrap().unwrap();
    let middle_task = driver.get(middle).unwrap().unwrap();
    assert_eq!(root_task.checkpoint().phase, "child");
    assert!(
        f.ids.lock().unwrap().is_empty(),
        "parked root was run on reopen"
    );
    assert_eq!(child_id(&root_task), middle);
    assert_eq!(middle_task.checkpoint().phase, "gate");
    assert_eq!(child_id(&middle_task), leaf);
    assert_eq!(
        driver.get(leaf).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!("leaf-result"))
    );
    driver
        .signal(&Signal::new(
            Uuid::new_v4(),
            middle,
            "release".into(),
            json!(null),
        ))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), scheduler.wait(conversation))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        scheduler.conversation(conversation).unwrap().submissions[0].status,
        Status::Done {
            answer: "done".into()
        }
    );
    assert_eq!(
        driver.get(root).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!("root-result"))
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("count"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}
#[tokio::test]
async fn multilevel_abort_cascades_after_kill() {
    let dir = tempfile::tempdir().unwrap();
    let (_child, [conversation, root, middle, leaf]) = launch(dir.path(), true);
    let f = Fixture::new(dir.path(), true);
    let mut scheduler = Scheduler::open(dir.path().join("db"), f.options()).unwrap();
    let driver = scheduler.start_tasks(f.registry()).unwrap();
    assert_eq!(child_id(&driver.get(root).unwrap().unwrap()), middle);
    assert_eq!(child_id(&driver.get(middle).unwrap().unwrap()), leaf);
    assert_eq!(
        driver.get(leaf).unwrap().unwrap().checkpoint().data["leaf"],
        "checkpoint"
    );
    scheduler.abort(conversation).unwrap();
    until(|| {
        [root, middle, leaf]
            .iter()
            .all(|id| driver.get(*id).unwrap().unwrap().is_terminal())
    })
    .await;
    tokio::time::timeout(Duration::from_secs(20), scheduler.wait(conversation))
        .await
        .unwrap()
        .unwrap();
    for id in [root, middle, leaf] {
        assert_eq!(
            driver.get(id).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
    }
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn unsafe_root_killed_while_running_is_not_replayed() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = KillOnDrop(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "multilevel_process_child", "--nocapture"])
            .env("PI_MULTILEVEL_DIR", dir.path())
            .env("PI_MULTILEVEL_RUNNING", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let line = line.unwrap();
            if let Some((_, id)) = line.split_once("RUNNING_ROOT ") {
                tx.send(id.trim().parse::<Uuid>().unwrap()).unwrap();
                break;
            }
        }
    });
    let root = rx
        .recv_timeout(Duration::from_secs(25))
        .expect("root did not start");
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let f = Fixture::new(dir.path(), false);
    let scheduler = Scheduler::open(dir.path().join("db"), f.options()).unwrap();
    let driver = scheduler.start_tasks(f.registry()).unwrap();
    until(|| driver.get(root).unwrap().is_some_and(|t| t.is_terminal())).await;
    let task = driver.get(root).unwrap().unwrap();
    assert!(
        matches!(task.state(), TaskState::Failed(reason) if reason.contains("outcome requires reconciliation")),
        "{:?}",
        task.state()
    );
    assert!(
        f.ids.lock().unwrap().is_empty(),
        "interrupted root was replayed"
    );
    assert!(!dir.path().join("count").exists());
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn unsafe_root_interrupted_after_wake_is_not_replayed() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = KillOnDrop(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "multilevel_process_child", "--nocapture"])
            .env("PI_MULTILEVEL_DIR", dir.path())
            .env("PI_MULTILEVEL_WAKE_RUNNING", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut ready = None;
        for line in BufReader::new(stdout).lines() {
            let line = line.unwrap();
            if let Some((_, rest)) = line.split_once("MULTILEVEL_READY ") {
                ready = Some(
                    rest.split_whitespace()
                        .map(|s| s.parse::<Uuid>().unwrap())
                        .collect::<Vec<_>>(),
                );
            }
            if let Some((_, id)) = line.split_once("RUNNING_WAKE ") {
                let ids = ready.unwrap();
                assert_eq!(ids[1], id.trim().parse::<Uuid>().unwrap());
                tx.send(ids).unwrap();
                break;
            }
        }
    });
    let ids = rx
        .recv_timeout(Duration::from_secs(25))
        .expect("root did not wake");
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let f = Fixture::new(dir.path(), false);
    let scheduler = Scheduler::open(dir.path().join("db"), f.options()).unwrap();
    let driver = scheduler.start_tasks(f.registry()).unwrap();
    until(|| driver.get(ids[1]).unwrap().is_some_and(|t| t.is_terminal())).await;
    let root = driver.get(ids[1]).unwrap().unwrap();
    assert!(
        matches!(root.state(), TaskState::Failed(reason) if reason.contains("outcome requires reconciliation")),
        "{:?}",
        root.state()
    );
    assert_eq!(root.checkpoint().phase, "child");
    assert!(f.ids.lock().unwrap().is_empty());
    assert_eq!(
        std::fs::read_to_string(dir.path().join("count"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}
