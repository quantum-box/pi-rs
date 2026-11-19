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
    task_scheduler::{AdmissionHandle, Handler, Registry, Step},
    task_store::Signal,
    task_tool::TaskTool,
};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use uuid::Uuid;

type SeenAdmissions = Arc<Mutex<Vec<(Uuid, Vec<Uuid>)>>>;

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
fn cp(phase: &str, data: Value) -> Checkpoint {
    Checkpoint {
        phase: phase.into(),
        data,
    }
}
fn ids(task: &Task) -> Vec<Uuid> {
    serde_json::from_value(task.checkpoint().data["ids"].clone()).unwrap()
}
fn append(path: &std::path::Path, line: &str) -> Result<(), String> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    writeln!(file, "{line}").map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())
}
struct Leaf {
    log: std::path::PathBuf,
}
impl Handler for Leaf {
    fn run(&self, task: &Task) -> Result<Step, String> {
        match task.checkpoint().phase.as_str() {
            "initial" => {
                append(&self.log, &task.id().to_string())?;
                Ok(Step::Wait {
                    checkpoint: cp("parked", task.input().clone()),
                    condition: WaitFor::Signal("release".into()),
                    children: vec![],
                })
            }
            "parked" => Ok(Step::Succeed(task.checkpoint().data.clone())),
            _ => Err("invalid leaf checkpoint".into()),
        }
    }
}
struct Parent {
    child: Arc<TaskTool>,
    middle: bool,
    any: bool,
    seen: SeenAdmissions,
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
        match task.checkpoint().phase.as_str() {
            "initial" => {
                if self.middle && std::env::var_os("PI_IMMEDIATE_BEFORE_PARK").is_some() {
                    println!("MIDDLE_STARTED {}", task.id());
                    std::io::stdout().flush().unwrap();
                    loop {
                        std::thread::park();
                    }
                }
                let keys: &[&str] = if self.middle {
                    &["a", "b"]
                } else {
                    &["middle"]
                };
                let children = keys
                    .iter()
                    .map(|key| {
                        admission
                            .admit_task_tool_child(&self.child, key, json!({"key":key}))
                            .map_err(|e| e.to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if !self.middle && std::env::var_os("PI_IMMEDIATE_BEFORE_PARK").is_some() {
                    println!("ROOT_ADMITTED {} {}", task.id(), children[0]);
                    std::io::stdout().flush().unwrap();
                    loop {
                        std::thread::park();
                    }
                }
                self.seen
                    .lock()
                    .unwrap()
                    .push((task.id(), children.clone()));
                let wait = if self.middle && !self.any {
                    WaitFor::Tasks(children.clone())
                } else {
                    WaitFor::TasksAny(children.clone())
                };
                Ok(Step::Wait {
                    checkpoint: cp(
                        "waiting",
                        json!({"ids":children,"level":if self.middle {"middle"} else {"root"}}),
                    ),
                    condition: wait,
                    children: vec![],
                })
            }
            "waiting" | "rest" => {
                let children = ids(task);
                let expected_level = if self.middle { "middle" } else { "root" };
                if task.checkpoint().data["level"] != expected_level {
                    return Err("checkpoint level changed".into());
                }
                let ended = task.ended_children().ok_or("missing wake")?;
                if self.middle {
                    for (index, key) in ["a", "b"].iter().enumerate() {
                        let id = admission
                            .admit_task_tool_child(&self.child, key, json!({"key":key}))
                            .map_err(|e| e.to_string())?;
                        if id != children[index] {
                            return Err("child identity changed".into());
                        }
                    }
                    if self.any && task.checkpoint().phase == "waiting" {
                        if ended != [children[0]] {
                            return Err(format!("wrong first any wake: {ended:?}"));
                        }
                        if !matches!(
                            docs.get_owned_child(children[0])?.state(),
                            TaskState::Succeeded(_)
                        ) {
                            return Err("first child not done".into());
                        }
                        return Ok(Step::Wait {
                            checkpoint: cp("rest", task.checkpoint().data.clone()),
                            condition: WaitFor::Tasks(vec![children[1]]),
                            children: vec![],
                        });
                    }
                    let expected = if self.any {
                        vec![children[1]]
                    } else {
                        children.clone()
                    };
                    if ended != expected {
                        return Err(format!("wrong middle wake: {ended:?}"));
                    }
                    for id in &children {
                        if !matches!(docs.get_owned_child(*id)?.state(), TaskState::Succeeded(_)) {
                            return Err("leaf not done".into());
                        }
                    }
                    Ok(Step::Succeed(json!("middle-result")))
                } else {
                    if ended != children {
                        return Err("wrong root wake".into());
                    }
                    let id = admission
                        .admit_task_tool_child(&self.child, "middle", json!({"key":"middle"}))
                        .map_err(|e| e.to_string())?;
                    if id != children[0]
                        || !matches!(docs.get_owned_child(id)?.state(), TaskState::Succeeded(_))
                    {
                        return Err("middle result changed".into());
                    }
                    Ok(Step::Succeed(json!("root-result")))
                }
            }
            _ => Err("invalid parent checkpoint".into()),
        }
    }
}
struct Fixture {
    root: Arc<dyn Handler>,
    middle: Arc<dyn Handler>,
    leaf: Arc<dyn Handler>,
    tool: Arc<TaskTool>,
    seen: SeenAdmissions,
}
impl Fixture {
    fn new(dir: &std::path::Path, any: bool) -> Self {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let leaf: Arc<dyn Handler> = Arc::new(Leaf {
            log: dir.join("leaf-count"),
        });
        let leaf_tool = Arc::new(TaskTool::new(spec("leaf"), "leaf", false, 1, leaf.clone()));
        let middle: Arc<dyn Handler> = Arc::new(Parent {
            child: leaf_tool,
            middle: true,
            any,
            seen: seen.clone(),
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
            middle: false,
            any,
            seen: seen.clone(),
        });
        let tool = Arc::new(TaskTool::new(spec("root"), "root", true, 1, root.clone()));
        Self {
            root,
            middle,
            leaf,
            tool,
            seen,
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
        let mut registry = Registry::default();
        for (name, safe, handler) in [
            ("root", true, &self.root),
            ("middle", true, &self.middle),
            ("leaf", false, &self.leaf),
        ] {
            assert!(registry.register_shared(name, safe, handler.clone()));
            assert!(registry.set_abort(name, |_: &Task| Ok(Step::Succeed(Value::Null))));
        }
        registry
    }
}
async fn until(mut check: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while !check() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("timeout");
}
async fn start(
    dir: &std::path::Path,
    any: bool,
) -> (
    Fixture,
    Scheduler,
    pi_rs_durable::task_driver::Driver,
    Uuid,
    [Uuid; 4],
) {
    let f = Fixture::new(dir, any);
    let mut scheduler = Scheduler::open(dir.join("db"), f.options()).unwrap();
    let driver = scheduler.start_tasks(f.registry()).unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(conversation, None, "work".into()).unwrap();
    let mut found = None;
    until(|| {
        let seen = f.seen.lock().unwrap();
        if seen.len() != 2 {
            return false;
        }
        let Some((root, root_children)) = seen.iter().find(|(_, children)| children.len() == 1)
        else {
            return false;
        };
        let root = *root;
        let middle = root_children[0];
        let Some((_, leaves)) = seen
            .iter()
            .find(|(id, children)| *id == middle && children.len() == 2)
        else {
            return false;
        };
        let (Some(r), Some(m), Some(a), Some(b)) = (
            driver.get(root).unwrap(),
            driver.get(middle).unwrap(),
            driver.get(leaves[0]).unwrap(),
            driver.get(leaves[1]).unwrap(),
        ) else {
            return false;
        };
        if !matches!(r.state(), TaskState::Waiting(_))
            || r.checkpoint().phase != "waiting"
            || !matches!(m.state(), TaskState::Waiting(_))
            || m.checkpoint().phase != "waiting"
            || !matches!(a.state(), TaskState::Waiting(_))
            || !matches!(b.state(), TaskState::Waiting(_))
        {
            return false;
        }
        assert_eq!(ids(&r), vec![middle]);
        assert_eq!(ids(&m), *leaves);
        found = Some([root, middle, leaves[0], leaves[1]]);
        true
    })
    .await;
    (f, scheduler, driver, conversation, found.unwrap())
}
fn signal(driver: &pi_rs_durable::task_driver::Driver, task: Uuid) {
    driver
        .signal(&Signal::new(
            Uuid::new_v4(),
            task,
            "release".into(),
            Value::Null,
        ))
        .unwrap();
}
async fn finish(
    scheduler: &mut Scheduler,
    driver: &pi_rs_durable::task_driver::Driver,
    conversation: Uuid,
    ids: [Uuid; 4],
    any: bool,
) {
    signal(driver, ids[2]);
    if any {
        until(|| {
            driver.get(ids[1]).unwrap().is_some_and(|t| {
                t.checkpoint().phase == "rest" && matches!(t.state(), TaskState::Waiting(_))
            })
        })
        .await;
        assert!(matches!(
            driver.get(ids[3]).unwrap().unwrap().state(),
            TaskState::Waiting(_)
        ));
    } else {
        until(|| {
            driver
                .get(ids[2])
                .unwrap()
                .is_some_and(|t| matches!(t.state(), TaskState::Succeeded(_)))
        })
        .await;
        let middle = driver.get(ids[1]).unwrap().unwrap();
        assert_eq!(middle.checkpoint().phase, "waiting");
        assert!(matches!(middle.state(), TaskState::Waiting(_)));
    }
    signal(driver, ids[3]);
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
    for id in ids {
        assert!(matches!(
            driver.get(id).unwrap().unwrap().state(),
            TaskState::Succeeded(_)
        ));
    }
}
#[tokio::test]
async fn immediate_three_levels_multiple_children_any_and_all() {
    for any in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (_f, mut scheduler, driver, conversation, ids) = start(dir.path(), any).await;
        finish(&mut scheduler, &driver, conversation, ids, any).await;
        assert_eq!(
            std::fs::read_to_string(dir.path().join("leaf-count"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}

#[tokio::test]
async fn immediate_park_releases_workers_for_six_levels() {
    let dir = tempfile::tempdir().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let leaf: Arc<dyn Handler> = Arc::new(Leaf {
        log: dir.path().join("leaf-count"),
    });
    let mut child = Arc::new(TaskTool::new(spec("leaf"), "leaf", false, 1, leaf.clone()));
    let mut handlers = Vec::new();
    for index in (0..5).rev() {
        let name = if index == 0 {
            "root".to_owned()
        } else {
            format!("level-{index}")
        };
        let handler: Arc<dyn Handler> = Arc::new(Parent {
            child,
            middle: false,
            any: true,
            seen: seen.clone(),
        });
        child = Arc::new(TaskTool::new(
            spec(&name),
            name.clone(),
            true,
            1,
            handler.clone(),
        ));
        handlers.push((name, handler));
    }
    let root_tool = child;
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![root_tool.register()],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut registry = Registry::default();
    assert!(registry.register_shared("leaf", false, leaf));
    for (name, handler) in handlers {
        assert!(registry.register_shared(name, true, handler));
    }
    let mut scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(conversation, None, "work".into()).unwrap();
    let mut chain = Vec::new();
    until(|| {
        let entries = seen.lock().unwrap();
        if entries.len() != 5 {
            return false;
        }
        let Some((root, _)) = entries
            .iter()
            .find(|(id, _)| !entries.iter().any(|(_, children)| children.contains(id)))
        else {
            return false;
        };
        let mut current = *root;
        let mut result = Vec::new();
        for _ in 0..5 {
            let Some((_, children)) = entries.iter().find(|(id, _)| *id == current) else {
                return false;
            };
            let Some(task) = driver.get(current).unwrap() else {
                return false;
            };
            if !matches!(task.state(), TaskState::Waiting(_)) {
                return false;
            }
            result.push(current);
            current = children[0];
        }
        let Some(leaf) = driver.get(current).unwrap() else {
            return false;
        };
        if !matches!(leaf.state(), TaskState::Waiting(_)) {
            return false;
        }
        result.push(current);
        chain = result;
        true
    })
    .await;
    assert_eq!(chain.len(), 6);
    signal(&driver, chain[5]);
    tokio::time::timeout(Duration::from_secs(20), scheduler.wait(conversation))
        .await
        .unwrap()
        .unwrap();
    for id in chain {
        assert!(matches!(
            driver.get(id).unwrap().unwrap().state(),
            TaskState::Succeeded(_)
        ));
    }
    assert_eq!(
        std::fs::read_to_string(dir.path().join("leaf-count"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}
#[tokio::test]
async fn immediate_park_process_worker() {
    let Ok(dir) = std::env::var("PI_IMMEDIATE_PARK_DIR") else {
        return;
    };
    let any = std::env::var_os("PI_IMMEDIATE_PARK_ANY").is_some();
    let (_f, _scheduler, driver, conversation, ids) = start(std::path::Path::new(&dir), any).await;
    if std::env::var_os("PI_IMMEDIATE_AFTER_FIRST").is_some() {
        assert!(any);
        signal(&driver, ids[2]);
        until(|| {
            driver.get(ids[1]).unwrap().is_some_and(|task| {
                task.checkpoint().phase == "rest" && matches!(task.state(), TaskState::Waiting(_))
            })
        })
        .await;
        println!(
            "IMMEDIATE_FIRST {conversation} {} {} {} {}",
            ids[0], ids[1], ids[2], ids[3]
        );
        std::io::stdout().flush().unwrap();
        std::future::pending::<()>().await;
    }
    println!(
        "IMMEDIATE_READY {conversation} {} {} {} {}",
        ids[0], ids[1], ids[2], ids[3]
    );
    std::io::stdout().flush().unwrap();
    std::future::pending::<()>().await;
}
struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn kill_parked(dir: &std::path::Path, any: bool) -> (KillOnDrop, Uuid, [Uuid; 4]) {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args(["--exact", "immediate_park_process_worker", "--nocapture"])
        .env("PI_IMMEDIATE_PARK_DIR", dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    if any {
        cmd.env("PI_IMMEDIATE_PARK_ANY", "1");
    }
    let mut child = KillOnDrop(cmd.spawn().unwrap());
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let line = line.unwrap();
            if let Some((_, rest)) = line.split_once("IMMEDIATE_READY ") {
                let parsed: Vec<Uuid> = rest
                    .split_whitespace()
                    .map(|s| s.parse().unwrap())
                    .collect();
                tx.send((parsed[0], [parsed[1], parsed[2], parsed[3], parsed[4]]))
                    .unwrap();
                break;
            }
        }
    });
    let (conversation, ids) = rx
        .recv_timeout(Duration::from_secs(25))
        .expect("child never parked");
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    (child, conversation, ids)
}
#[tokio::test]
async fn immediate_park_kill_reopen_and_abort() {
    for (any, abort) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let (_child, conversation, ids) = kill_parked(dir.path(), any);
        let f = Fixture::new(dir.path(), any);
        let mut scheduler = Scheduler::open(dir.path().join("db"), f.options()).unwrap();
        let driver = scheduler.start_tasks(f.registry()).unwrap();
        for id in ids {
            assert!(matches!(
                driver.get(id).unwrap().unwrap().state(),
                TaskState::Waiting(_)
            ));
        }
        assert!(f.seen.lock().unwrap().is_empty());
        if abort {
            scheduler.abort(conversation).unwrap();
            until(|| {
                ids.iter()
                    .all(|id| driver.get(*id).unwrap().is_some_and(|t| t.is_terminal()))
            })
            .await;
            for id in ids {
                assert_eq!(
                    driver.get(id).unwrap().unwrap().state(),
                    &TaskState::Cancelled
                );
            }
        } else {
            finish(&mut scheduler, &driver, conversation, ids, any).await;
        }
        assert_eq!(
            std::fs::read_to_string(dir.path().join("leaf-count"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}

#[tokio::test]
async fn immediate_admission_survives_kill_before_parent_park() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = KillOnDrop(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "immediate_park_process_worker", "--nocapture"])
            .env("PI_IMMEDIATE_PARK_DIR", dir.path())
            .env("PI_IMMEDIATE_BEFORE_PARK", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut root = None;
        let mut middle = None;
        for line in BufReader::new(stdout).lines() {
            let line = line.unwrap();
            if let Some((_, rest)) = line.split_once("ROOT_ADMITTED ") {
                let ids: Vec<Uuid> = rest
                    .split_whitespace()
                    .map(|s| s.parse().unwrap())
                    .collect();
                root = Some((ids[0], ids[1]));
            }
            if let Some((_, rest)) = line.split_once("MIDDLE_STARTED ") {
                middle = Some(rest.trim().parse::<Uuid>().unwrap());
            }
            if let (Some((r, m)), Some(started)) = (root, middle) {
                assert_eq!(m, started);
                tx.send((r, m)).unwrap();
                break;
            }
        }
    });
    let (root, middle) = rx
        .recv_timeout(Duration::from_secs(25))
        .expect("running parent and child not reached");
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let f = Fixture::new(dir.path(), false);
    let mut scheduler = Scheduler::open(dir.path().join("db"), f.options()).unwrap();
    let driver = scheduler.start_tasks(f.registry()).unwrap();
    let mut found = None;
    until(|| {
        let (Some(root_task), Some(middle_task)) =
            (driver.get(root).unwrap(), driver.get(middle).unwrap())
        else {
            return false;
        };
        if !matches!(root_task.state(), TaskState::Waiting(_))
            || root_task.checkpoint().phase != "waiting"
            || !matches!(middle_task.state(), TaskState::Waiting(_))
            || middle_task.checkpoint().phase != "waiting"
        {
            return false;
        }
        assert_eq!(ids(&root_task), vec![middle]);
        let leaves = ids(&middle_task);
        if leaves.len() != 2
            || !leaves.iter().all(|id| {
                driver
                    .get(*id)
                    .unwrap()
                    .is_some_and(|task| matches!(task.state(), TaskState::Waiting(_)))
            })
        {
            return false;
        }
        found = Some([root, middle, leaves[0], leaves[1]]);
        true
    })
    .await;
    let root_task = driver.get(root).unwrap().unwrap();
    let conversation = match root_task.owner() {
        pi_rs_durable::task::TaskOwner::Conversation(id) => *id,
        _ => panic!("root owner changed"),
    };
    finish(&mut scheduler, &driver, conversation, found.unwrap(), false).await;
    assert_eq!(
        std::fs::read_to_string(dir.path().join("leaf-count"))
            .unwrap()
            .lines()
            .count(),
        2
    );
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn immediate_any_repark_survives_kill_after_first_wake() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = KillOnDrop(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "immediate_park_process_worker", "--nocapture"])
            .env("PI_IMMEDIATE_PARK_DIR", dir.path())
            .env("PI_IMMEDIATE_PARK_ANY", "1")
            .env("PI_IMMEDIATE_AFTER_FIRST", "1")
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
            if let Some((_, rest)) = line.split_once("IMMEDIATE_FIRST ") {
                let parsed: Vec<Uuid> = rest
                    .split_whitespace()
                    .map(|s| s.parse().unwrap())
                    .collect();
                tx.send((parsed[0], [parsed[1], parsed[2], parsed[3], parsed[4]]))
                    .unwrap();
                break;
            }
        }
    });
    let (conversation, ids) = rx
        .recv_timeout(Duration::from_secs(25))
        .expect("first wake not parked");
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let f = Fixture::new(dir.path(), true);
    let mut scheduler = Scheduler::open(dir.path().join("db"), f.options()).unwrap();
    let driver = scheduler.start_tasks(f.registry()).unwrap();
    assert_eq!(
        driver.get(ids[1]).unwrap().unwrap().checkpoint().phase,
        "rest"
    );
    assert!(matches!(
        driver.get(ids[2]).unwrap().unwrap().state(),
        TaskState::Succeeded(_)
    ));
    assert!(matches!(
        driver.get(ids[3]).unwrap().unwrap().state(),
        TaskState::Waiting(_)
    ));
    signal(&driver, ids[3]);
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
        std::fs::read_to_string(dir.path().join("leaf-count"))
            .unwrap()
            .lines()
            .count(),
        2
    );
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}
