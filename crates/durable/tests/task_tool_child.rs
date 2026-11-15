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
    task_tool::TaskTool,
};
use serde_json::json;
use std::{
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
struct Parent {
    fast: Arc<TaskTool>,
    altered_fast: Arc<TaskTool>,
    wrong_handler: Arc<TaskTool>,
    slow: Arc<TaskTool>,
    ids: Arc<Mutex<Option<(Uuid, Uuid, Uuid)>>>,
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
        if task.checkpoint().phase == "initial" {
            if self
                .fast
                .stage_child(task, docs, "invalid", json!("not-an-object"))
                .is_ok()
            {
                return Err("invalid input accepted".into());
            }
            let a = self
                .fast
                .stage_child(task, docs, "fast", json!({"job":"fast"}))?;
            if self
                .fast
                .stage_child(task, docs, "fast", json!({"job":"fast"}))?
                != a
            {
                return Err("staged child identity changed".into());
            }
            if self
                .fast
                .stage_child(task, docs, "fast", json!({"job":"changed"}))
                .is_ok()
                || self
                    .altered_fast
                    .stage_child(task, docs, "fast", json!({"job":"fast"}))
                    .is_ok()
                || self
                    .wrong_handler
                    .stage_child(task, docs, "fast", json!({"job":"fast"}))
                    .is_ok()
            {
                return Err("conflicting staged child accepted".into());
            }
            let b = self
                .slow
                .stage_child(task, docs, "slow", json!({"job":"slow"}))?;
            *self.ids.lock().unwrap() = Some((task.id(), a, b));
            return Ok(Step::Wait {
                checkpoint: Checkpoint {
                    phase: "first".into(),
                    data: json!({"a":a,"b":b,"checkpoint":"retained"}),
                },
                condition: WaitFor::TasksAny(vec![a, b]),
                children: vec![],
            });
        }
        let data = &task.checkpoint().data;
        if data["checkpoint"] != "retained" {
            return Err("checkpoint changed".into());
        }
        let a: Uuid = serde_json::from_value(data["a"].clone()).map_err(|e| e.to_string())?;
        let b: Uuid = serde_json::from_value(data["b"].clone()).map_err(|e| e.to_string())?;
        if task.checkpoint().phase == "first" {
            if self
                .altered_fast
                .stage_child(task, docs, "fast", json!({"job":"fast"}))
                .is_ok()
            {
                return Err("changed tool specification accepted".into());
            }
            if self
                .fast
                .stage_child(task, docs, "fast", json!({"job":"changed"}))
                .is_ok()
            {
                return Err("changed child identity accepted".into());
            }
            if self
                .fast
                .stage_child(task, docs, "fast", json!({"job":"fast"}))?
                != a
            {
                return Err("child identity changed".into());
            }
            if task.ended_children() != Some(&[a][..]) {
                return Err("wrong first wake".into());
            }
            if !matches!(docs.get_owned_child(a)?.state(),TaskState::Succeeded(v) if v == &json!("fast-result"))
            {
                return Err("wrong fast result".into());
            }
            return Ok(Step::Wait {
                checkpoint: Checkpoint {
                    phase: "second".into(),
                    data: data.clone(),
                },
                condition: WaitFor::TasksAny(vec![b]),
                children: vec![],
            });
        }
        if task.ended_children() != Some(&[b][..]) {
            return Err("wrong second wake".into());
        }
        if !matches!(docs.get_owned_child(b)?.state(),TaskState::Succeeded(v) if v == &json!("slow-result"))
        {
            return Err("wrong slow result".into());
        }
        Ok(Step::Succeed(json!("both results")))
    }
}
struct Slow;
impl Handler for Slow {
    fn run(&self, task: &Task) -> Result<Step, String> {
        if task.checkpoint().phase == "initial" {
            Ok(Step::Wait {
                checkpoint: Checkpoint {
                    phase: "waiting".into(),
                    data: json!({}),
                },
                condition: WaitFor::Signal("release".into()),
                children: vec![],
            })
        } else {
            Ok(Step::Succeed(json!("slow-result")))
        }
    }
}
struct Fixture {
    parent: Arc<dyn Handler>,
    fast: Arc<dyn Handler>,
    slow: Arc<dyn Handler>,
    adapter: Arc<TaskTool>,
    ids: Arc<Mutex<Option<(Uuid, Uuid, Uuid)>>>,
}
impl Fixture {
    fn new() -> Self {
        let fast: Arc<dyn Handler> = Arc::new(|_: &Task| Ok(Step::Succeed(json!("fast-result"))));
        let slow: Arc<dyn Handler> = Arc::new(Slow);
        let fast_tool = Arc::new(TaskTool::new(
            spec("fast-tool"),
            "fast",
            true,
            1,
            fast.clone(),
        ));
        let mut altered_spec = spec("fast-tool");
        altered_spec.description = "changed".into();
        let altered_fast = Arc::new(TaskTool::new(altered_spec, "fast", true, 1, fast.clone()));
        let wrong_handler = Arc::new(TaskTool::new(
            spec("fast-tool"),
            "fast",
            true,
            1,
            Arc::new(|_: &Task| Ok(Step::Succeed(json!("wrong")))),
        ));
        let slow_tool = Arc::new(TaskTool::new(
            spec("slow-tool"),
            "slow",
            true,
            1,
            slow.clone(),
        ));
        let ids = Arc::new(Mutex::new(None));
        let parent: Arc<dyn Handler> = Arc::new(Parent {
            fast: fast_tool,
            altered_fast,
            wrong_handler,
            slow: slow_tool,
            ids: ids.clone(),
        });
        let adapter = Arc::new(TaskTool::new(
            spec("parent"),
            "parent",
            true,
            1,
            parent.clone(),
        ));
        Self {
            parent,
            fast,
            slow,
            adapter,
            ids,
        }
    }
    fn options(&self) -> Options {
        Options {
            model: Arc::new(ModelStub),
            tools: vec![self.adapter.register()],
            restart_limit: 5,
            max_turns: 8,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        }
    }
    fn registry(&self) -> Registry {
        let mut r = Registry::default();
        assert!(r.register_shared("parent", true, self.parent.clone()));
        assert!(r.register_shared("fast", true, self.fast.clone()));
        assert!(r.register_shared("slow", true, self.slow.clone()));
        assert!(r.set_abort("parent", |_: &Task| Ok(Step::Succeed(json!(null)))));
        assert!(r.set_abort("slow", |_: &Task| Ok(Step::Succeed(json!(null)))));
        r
    }
}
async fn until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("timeout");
}
#[tokio::test]
async fn task_tool_stages_two_owned_tools_wait_any_and_recovers_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let f = Fixture::new();
    let mut scheduler = Scheduler::open(&path, f.options()).unwrap();
    let driver = scheduler.start_tasks(f.registry()).unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(conversation, None, "work".into()).unwrap();
    until(|| {
        f.ids
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|(parent, _, slow)| {
                driver
                    .get(*parent)
                    .unwrap()
                    .is_some_and(|t| t.checkpoint().phase == "second")
                    && driver
                        .get(*slow)
                        .unwrap()
                        .is_some_and(|t| matches!(t.state(), TaskState::Waiting(_)))
            })
    })
    .await;
    let (parent, fast, slow) = f.ids.lock().unwrap().unwrap();
    until(|| {
        !driver.worker_activity().snapshot().iter().any(|worker| {
            worker.kind == pi_rs_durable::worker_activity::WorkerKind::Task
                && (worker.id == parent || worker.id == slow)
        })
    })
    .await;
    assert!(matches!(
        driver.get(fast).unwrap().unwrap().state(),
        TaskState::Succeeded(_)
    ));
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
    let mut scheduler = Scheduler::open(&path, f.options()).unwrap();
    let driver = scheduler.start_tasks(f.registry()).unwrap();
    assert_eq!(
        driver.get(parent).unwrap().unwrap().checkpoint().phase,
        "second"
    );
    driver
        .signal(&Signal::new(
            Uuid::new_v4(),
            slow,
            "release".into(),
            json!(null),
        ))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), scheduler.wait(conversation))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        scheduler.conversation(conversation).unwrap().submissions[0].status,
        Status::Done {
            answer: "done".into()
        }
    );
    assert!(
        scheduler
            .conversation(conversation)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m,Message::Tool {content,..} if content == "both results"))
    );
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn abort_drains_parked_task_tool_child_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let f = Fixture::new();
    let mut scheduler = Scheduler::open(&path, f.options()).unwrap();
    let driver = scheduler.start_tasks(f.registry()).unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(conversation, None, "work".into()).unwrap();
    until(|| {
        f.ids
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|(parent, _, slow)| {
                driver
                    .get(*parent)
                    .unwrap()
                    .is_some_and(|t| t.checkpoint().phase == "second")
                    && driver
                        .get(*slow)
                        .unwrap()
                        .is_some_and(|t| matches!(t.state(), TaskState::Waiting(_)))
            })
    })
    .await;
    let (parent, _, slow) = f.ids.lock().unwrap().unwrap();
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
    let mut scheduler = Scheduler::open(&path, f.options()).unwrap();
    let driver = scheduler.start_tasks(f.registry()).unwrap();
    scheduler.abort(conversation).unwrap();
    until(|| {
        driver.get(parent).unwrap().is_some_and(|t| t.is_terminal())
            && driver.get(slow).unwrap().is_some_and(|t| t.is_terminal())
    })
    .await;
    tokio::time::timeout(Duration::from_secs(15), scheduler.wait(conversation))
        .await
        .unwrap()
        .unwrap();
    let slow_state = driver.get(slow).unwrap().unwrap();
    let parent_state = driver.get(parent).unwrap().unwrap();
    assert!(
        matches!(slow_state.state(), TaskState::Cancelled),
        "slow: {:?}",
        slow_state.state()
    );
    assert!(
        matches!(parent_state.state(), TaskState::Cancelled),
        "parent: {:?}",
        parent_state.state()
    );
    assert!(
        !scheduler
            .conversation(conversation)
            .unwrap()
            .abort_requested
    );
    assert!(
        !scheduler
            .conversation(conversation)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m,Message::Tool {content,..} if content == "both results"))
    );
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn task_tool_child_kill_worker() {
    let Some(root) = std::env::var_os("PI_TASK_TOOL_CHILD_KILL") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let f = Fixture::new();
    let mut scheduler = Scheduler::open(root.join("db"), f.options()).unwrap();
    let driver = scheduler.start_tasks(f.registry()).unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(conversation, None, "work".into()).unwrap();
    until(|| {
        f.ids
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|(parent, _, slow)| {
                driver
                    .get(*parent)
                    .unwrap()
                    .is_some_and(|t| t.checkpoint().phase == "second")
                    && driver
                        .get(*slow)
                        .unwrap()
                        .is_some_and(|t| matches!(t.state(), TaskState::Waiting(_)))
            })
    })
    .await;
    let (parent, fast, slow) = f.ids.lock().unwrap().unwrap();
    std::fs::write(
        root.join("ready.tmp"),
        format!("{conversation}\n{parent}\n{fast}\n{slow}\n"),
    )
    .unwrap();
    std::fs::rename(root.join("ready.tmp"), root.join("ready")).unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn killed_task_tool_parent_reopens_its_keyed_children_and_wakes() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut child = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "task_tool_child_kill_worker"])
            .env("PI_TASK_TOOL_CHILD_KILL", root)
            .spawn()
            .unwrap(),
    );
    until(|| root.join("ready").exists()).await;
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let ids = std::fs::read_to_string(root.join("ready")).unwrap();
    let mut ids = ids.lines().map(|id| id.parse::<Uuid>().unwrap());
    let conversation = ids.next().unwrap();
    let parent = ids.next().unwrap();
    let fast = ids.next().unwrap();
    let slow = ids.next().unwrap();
    let f = Fixture::new();
    let mut scheduler = Scheduler::open(root.join("db"), f.options()).unwrap();
    let driver = scheduler.start_tasks(f.registry()).unwrap();
    assert_eq!(
        driver.get(parent).unwrap().unwrap().checkpoint().phase,
        "second"
    );
    assert!(matches!(
        driver.get(fast).unwrap().unwrap().state(),
        TaskState::Succeeded(_)
    ));
    driver
        .signal(&Signal::new(
            Uuid::new_v4(),
            slow,
            "release".into(),
            json!(null),
        ))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), scheduler.wait(conversation))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        scheduler.conversation(conversation).unwrap().submissions[0].status,
        Status::Done {
            answer: "done".into()
        }
    );
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn task_tool_child_registration_mismatch_commits_no_child_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let f = Fixture::new();
    let mut registry = Registry::default();
    assert!(registry.register_shared("parent", true, f.parent.clone()));
    let wrong: Arc<dyn Handler> = Arc::new(|_: &Task| Ok(Step::Succeed(json!("wrong"))));
    assert!(registry.register_shared("fast", true, wrong));
    assert!(registry.register_shared("slow", true, f.slow.clone()));
    let mut scheduler = Scheduler::open(&path, f.options()).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(conversation, None, "work".into()).unwrap();
    until(|| f.ids.lock().unwrap().is_some()).await;
    let (parent, fast, slow) = f.ids.lock().unwrap().unwrap();
    until(|| driver.get(parent).is_err()).await;
    let _ = scheduler.shutdown().await;
    assert!(driver.shutdown().is_err());
    {
        let owner = durable_store::owner::OwnedStore::open(&path).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(owner.store());
        assert!(tasks.load(fast).unwrap().is_none());
        assert!(tasks.load(slow).unwrap().is_none());
    }
    let mut scheduler = Scheduler::open(&path, f.options()).unwrap();
    let driver = scheduler.start_tasks(f.registry()).unwrap();
    until(|| {
        f.ids.lock().unwrap().as_ref().is_some_and(|(p, _, b)| {
            driver
                .get(*p)
                .unwrap()
                .is_some_and(|t| t.checkpoint().phase == "second")
                && driver
                    .get(*b)
                    .unwrap()
                    .is_some_and(|t| matches!(t.state(), TaskState::Waiting(_)))
        })
    })
    .await;
    let (_, _, resumed_slow) = f.ids.lock().unwrap().unwrap();
    driver
        .signal(&Signal::new(
            Uuid::new_v4(),
            resumed_slow,
            "release".into(),
            json!(null),
        ))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), scheduler.wait(conversation))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        scheduler.conversation(conversation).unwrap().submissions[0].status,
        Status::Done {
            answer: "done".into()
        }
    );
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}
