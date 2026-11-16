use async_trait::async_trait;
use durable_store::Store;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    Error, Message, Model, ModelEvent, ModelRequest, ModelStream, ToolCall, ToolSpec,
};
use pi_rs_durable::{
    document::{Address, Documents, Error as DocumentError},
    recovery::UncertainPolicy,
    runtime::{Config, Options, Scheduler},
    task::{Checkpoint, Task, TaskOwner, TaskState},
    task_scheduler::{Handler, Registry, Step},
    task_store::TaskStore,
    task_tool::TaskTool,
};
use serde_json::json;
use std::{
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};
use uuid::Uuid;

struct ToolModel;
#[async_trait]
impl Model for ToolModel {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let done = request
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }));
        Ok(stream::iter(vec![Ok(ModelEvent::Complete {
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
        })])
        .boxed())
    }
}
fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 100,
    }
}
fn child(parent: Uuid) -> Task {
    Task::new(
        Uuid::new_v4(),
        TaskOwner::Task(parent),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered_versioned("child".into(), true, 1, json!({"value": 1}))
    .unwrap()
}
fn address() -> Address {
    Address {
        scope: "session".into(),
        name: "tool-parent".into(),
        key: String::new(),
    }
}

#[tokio::test]
async fn task_tool_parent_admission_is_atomic_owned_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (started_tx, started_rx) = mpsc::channel();
    let (child_started_tx, child_started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let handler: Arc<dyn Handler> = Arc::new(move |task: &Task| {
        started_tx.send(task.id()).unwrap();
        release_rx
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(30))
            .map_err(|e| e.to_string())?;
        Ok(Step::Succeed(json!("done")))
    });
    let adapter = Arc::new(TaskTool::new(
        ToolSpec {
            name: "parent".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        },
        "parent",
        true,
        1,
        handler.clone(),
    ));
    let options = || Options {
        model: Arc::new(ToolModel),
        tools: vec![adapter.register()],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register_shared("parent", true, handler);
    registry.register("child", true, move |task: &Task| {
        child_started_tx.send(task.id()).unwrap();
        Ok(Step::Succeed(json!(null)))
    });
    let driver = scheduler.start_tasks(registry).unwrap();
    let conversation = scheduler.create_conversation(config()).unwrap();
    let other = scheduler.create_conversation(config()).unwrap();
    scheduler.submit(conversation, None, "work".into()).unwrap();
    let parent = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(id) = started_rx.try_recv() {
                break id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let store = Store::open(&path).unwrap();
    let snapshot = TaskStore::new(&store).load(parent).unwrap().unwrap();
    let wrong = child(parent);
    assert!(
        scheduler
            .session_transaction(other, |tx| {
                tx.put_document(
                    &address(),
                    json!({"wrong": true}).as_object().unwrap().clone(),
                )?;
                tx.create_child_task(&snapshot, wrong.clone())
            })
            .is_err()
    );
    assert!(TaskStore::new(&store).load(wrong.id()).unwrap().is_none());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    let rejected = child(parent);
    let result: Result<(), _> = scheduler.session_transaction(conversation, |tx| {
        tx.create_child_task(&snapshot, rejected.clone())?;
        tx.put_document(
            &address(),
            json!({"bad": true}).as_object().unwrap().clone(),
        )?;
        Err(DocumentError::InvalidSessionEntry)
    });
    assert!(result.is_err());
    assert!(
        TaskStore::new(&store)
            .load(rejected.id())
            .unwrap()
            .is_none()
    );
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    let accepted = child(parent);
    let id = accepted.id();
    scheduler
        .session_transaction(conversation, |tx| {
            tx.put_document(&address(), json!({"ok": true}).as_object().unwrap().clone())?;
            assert_eq!(
                tx.create_keyed_child(&snapshot, "key", accepted.clone())?,
                id
            );
            assert_eq!(tx.create_keyed_child(&snapshot, "key", child(parent))?, id);
            tx.append_entry("note", None)?;
            Ok(())
        })
        .unwrap();
    let ran = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(id) = child_started_rx.try_recv() {
                break id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("child did not run while the parent was blocked");
    assert_eq!(ran, id);
    assert_eq!(
        TaskStore::new(&store)
            .load(parent)
            .unwrap()
            .unwrap()
            .task
            .state(),
        &TaskState::Running
    );
    let stored = TaskStore::new(&store).load(id).unwrap().unwrap();
    assert_eq!(stored.task.owner(), &TaskOwner::Task(parent));
    assert_eq!(
        Documents::new(&store)
            .get(&address())
            .unwrap()
            .unwrap()
            .value["ok"],
        true
    );
    let stale = child(parent);
    assert!(
        scheduler
            .session_transaction(conversation, |tx| tx
                .create_child_task(&snapshot, stale.clone()))
            .is_err()
    );
    assert!(TaskStore::new(&store).load(stale.id()).unwrap().is_none());
    release_tx.send(()).unwrap();
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
    let reopened = Scheduler::open(&path, options()).unwrap();
    assert_eq!(
        TaskStore::new(&store)
            .load(id)
            .unwrap()
            .unwrap()
            .task
            .owner(),
        &TaskOwner::Task(parent)
    );
    assert_eq!(
        Documents::new(&store)
            .get(&address())
            .unwrap()
            .unwrap()
            .value["ok"],
        true
    );
    assert_eq!(
        reopened
            .conversation(conversation)
            .unwrap()
            .application_writes
            .len(),
        1
    );
}

#[tokio::test]
async fn running_child_result_is_fenced_by_parent_failure_and_abort() {
    for fail_parent in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (parent_tx, parent_rx) = mpsc::channel();
        let (parent_release_tx, parent_release_rx) = mpsc::channel();
        let parent_release_rx = Arc::new(Mutex::new(parent_release_rx));
        let parent_handler: Arc<dyn Handler> = Arc::new(move |task: &Task| {
            parent_tx.send(task.id()).unwrap();
            parent_release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(30))
                .map_err(|e| e.to_string())?;
            Ok(if fail_parent {
                Step::Fail("parent failed".into())
            } else {
                Step::Succeed(json!("late parent"))
            })
        });
        let adapter = Arc::new(TaskTool::new(
            ToolSpec {
                name: "parent".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            },
            "parent",
            true,
            1,
            parent_handler.clone(),
        ));
        let options = Options {
            model: Arc::new(ToolModel),
            tools: vec![adapter.register()],
            restart_limit: 3,
            max_turns: 3,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        };
        let (child_tx, child_rx) = mpsc::channel();
        let (child_release_tx, child_release_rx) = mpsc::channel();
        let child_release_rx = Arc::new(Mutex::new(child_release_rx));
        let mut registry = Registry::default();
        registry.register_shared("parent", true, parent_handler);
        registry.register("child", true, move |task: &Task| {
            child_tx.send(task.id()).unwrap();
            child_release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(30))
                .map_err(|e| e.to_string())?;
            Ok(Step::Succeed(json!("must not commit")))
        });
        registry.set_abort("parent", |_: &Task| Ok(Step::Succeed(json!(null))));
        registry.set_abort("child", |_: &Task| Ok(Step::Succeed(json!(null))));
        let mut scheduler = Scheduler::open(&path, options).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        let conversation = scheduler.create_conversation(config()).unwrap();
        scheduler.submit(conversation, None, "work".into()).unwrap();
        let parent = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(id) = parent_rx.try_recv() {
                    break id;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let store = Store::open(&path).unwrap();
        let snapshot = TaskStore::new(&store).load(parent).unwrap().unwrap();
        let new_child = child(parent);
        let child_id = new_child.id();
        scheduler
            .session_transaction(conversation, |tx| {
                tx.create_child_task(&snapshot, new_child)
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if child_rx.try_recv().is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("child must run under parent");
        if fail_parent {
            parent_release_tx.send(()).unwrap();
        } else {
            scheduler.abort_task(parent).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if TaskStore::new(&store)
                    .load(child_id)
                    .unwrap()
                    .unwrap()
                    .task
                    .abort_requested()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("failure or abort must mark child before it can publish");
        child_release_tx.send(()).unwrap();
        if !fail_parent {
            parent_release_tx.send(()).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if TaskStore::new(&store)
                    .load(child_id)
                    .unwrap()
                    .unwrap()
                    .task
                    .is_terminal()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("child did not drain");
        assert!(!matches!(
            TaskStore::new(&store)
                .load(child_id)
                .unwrap()
                .unwrap()
                .task
                .state(),
            TaskState::Succeeded(_)
        ));
        scheduler.shutdown().await.unwrap();
        driver.shutdown().unwrap();
    }
}

#[tokio::test]
async fn admitted_children_run_when_four_task_tool_parents_block() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let handler: Arc<dyn Handler> = Arc::new(move |task: &Task| {
        started_tx.send(task.id()).unwrap();
        release_rx
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(30))
            .map_err(|error| error.to_string())?;
        Ok(Step::Succeed(json!("done")))
    });
    let adapter = Arc::new(TaskTool::new(
        ToolSpec {
            name: "parent".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        },
        "parent",
        true,
        1,
        handler.clone(),
    ));
    let options = Options {
        model: Arc::new(ToolModel),
        tools: vec![adapter.register()],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let (child_tx, child_rx) = mpsc::channel();
    let (unrelated_tx, unrelated_rx) = mpsc::channel();
    let mut registry = Registry::default();
    registry.register_shared("parent", true, handler);
    registry.register("child", true, move |task: &Task| {
        child_tx.send(task.id()).unwrap();
        Ok(Step::Succeed(json!(null)))
    });
    registry.register("unrelated", true, move |task: &Task| {
        unrelated_tx.send(task.id()).unwrap();
        Ok(Step::Succeed(json!(null)))
    });
    let mut scheduler = Scheduler::open(&path, options).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let mut conversations = Vec::new();
    for _ in 0..4 {
        let conversation = scheduler.create_conversation(config()).unwrap();
        scheduler.submit(conversation, None, "work".into()).unwrap();
        conversations.push(conversation);
    }
    let parents = tokio::time::timeout(Duration::from_secs(10), async {
        let mut parents = Vec::new();
        while parents.len() < 4 {
            if let Ok(id) = started_rx.try_recv() {
                parents.push(id);
            } else {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        parents
    })
    .await
    .expect("four TaskTool parents must start");
    let unrelated = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(conversations[0]),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered_versioned("unrelated".into(), true, 1, json!(null))
    .unwrap();
    driver.submit(&unrelated).unwrap();
    let store = Store::open(&path).unwrap();
    let tasks = TaskStore::new(&store);
    let mut children = std::collections::HashSet::new();
    for parent in &parents {
        let snapshot = tasks.load(*parent).unwrap().unwrap();
        let TaskOwner::Conversation(conversation) = snapshot.task.owner() else {
            panic!("parent owner")
        };
        assert!(conversations.contains(conversation));
        assert_eq!(snapshot.task.state(), &TaskState::Running);
        let child = child(*parent);
        children.insert(child.id());
        scheduler
            .session_transaction(*conversation, |tx| tx.create_child_task(&snapshot, child))
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while !children.is_empty() {
            if let Ok(id) = child_rx.try_recv() {
                assert!(children.remove(&id));
            } else {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    })
    .await
    .expect("children must execute despite saturated ordinary workers");
    assert!(
        unrelated_rx.try_recv().is_err(),
        "unrelated work must not use the reserved worker"
    );
    assert_eq!(
        tasks.load(unrelated.id()).unwrap().unwrap().task.state(),
        &TaskState::Ready
    );
    for parent in &parents {
        assert_eq!(
            tasks.load(*parent).unwrap().unwrap().task.state(),
            &TaskState::Running
        );
    }
    for _ in &parents {
        release_tx.send(()).unwrap();
    }
    let ran_unrelated = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(id) = unrelated_rx.try_recv() {
                break id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("unrelated work must resume when ordinary capacity is free");
    assert_eq!(ran_unrelated, unrelated.id());
    scheduler.shutdown().await.unwrap();
    driver.shutdown().unwrap();
}
