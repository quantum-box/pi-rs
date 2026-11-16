use async_trait::async_trait;
use futures_util::{StreamExt, poll, stream};
use pi_rs_core::{
    CancellationToken, Error as ModelError, Model, ModelEvent, ModelRequest, ModelStream,
};
use pi_rs_durable::{
    recovery::UncertainPolicy,
    runtime::{Config, Error, Options, Scheduler},
    task::{Checkpoint, Task, TaskOwner},
    task_scheduler::{Registry, Step},
};
use serde_json::json;
use std::{
    sync::{Arc, Mutex, mpsc},
    task::Poll,
    time::Duration,
};
use uuid::Uuid;

struct ModelStub;
#[async_trait]
impl Model for ModelStub {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("ok".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
fn options() -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 100,
    }
}
fn task(owner: Uuid, background: bool) -> Task {
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(owner),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        5,
    )
    .unwrap()
    .registered("gated".into(), true);
    if background {
        task.background().unwrap()
    } else {
        task
    }
}

#[tokio::test]
async fn waits_for_foreground_and_cancellation_does_not_abort() {
    let dir = tempfile::tempdir().unwrap();
    let mut scheduler = Scheduler::open(dir.path().join("db"), options()).unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    let other = scheduler.create_conversation(config()).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let mut registry = Registry::default();
    registry.register("gated", true, move |_: &Task| {
        started_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
        Ok(Step::Succeed(json!(null)))
    });
    let driver = scheduler.start_tasks(registry).unwrap();
    let work = task(id, false);
    driver.submit(&work).unwrap();
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let cancel = CancellationToken::new();
    let mut wait = Box::pin(scheduler.wait_for_idle(id, &cancel));
    assert!(matches!(poll!(&mut wait), Poll::Pending));
    scheduler
        .wait_for_idle(other, &CancellationToken::new())
        .await
        .unwrap();
    let all_cancel = CancellationToken::new();
    let mut all = Box::pin(scheduler.wait_for_all_idle(&all_cancel));
    assert!(matches!(poll!(&mut all), Poll::Pending));
    all_cancel.cancel();
    assert!(matches!((&mut all).await, Err(Error::WaitCancelled)));
    drop(all);
    cancel.cancel();
    assert!(matches!((&mut wait).await, Err(Error::WaitCancelled)));
    drop(wait);
    assert!(!driver.get(work.id()).unwrap().unwrap().abort_requested());
    release_tx.send(()).unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.wait_for_idle(id, &CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    scheduler
        .wait_for_all_idle(&CancellationToken::new())
        .await
        .unwrap();
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn background_is_ignored_and_unknown_conversation_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut scheduler = Scheduler::open(dir.path().join("db"), options()).unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let mut registry = Registry::default();
    registry.register("gated", true, move |_: &Task| {
        started_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
        Ok(Step::Succeed(json!(null)))
    });
    let driver = scheduler.start_tasks(registry).unwrap();
    driver.submit(&task(id, true)).unwrap();
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    scheduler
        .wait_for_idle(id, &CancellationToken::new())
        .await
        .unwrap();
    scheduler
        .wait_for_all_idle(&CancellationToken::new())
        .await
        .unwrap();
    assert!(matches!(
        scheduler
            .wait_for_idle(Uuid::new_v4(), &CancellationToken::new())
            .await,
        Err(Error::NotFound)
    ));
    release_tx.send(()).unwrap();
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovered_live_task_requires_driver_then_settles() {
    use pi_rs_durable::task_scheduler;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    scheduler.shutdown().await.unwrap();
    let mut registry = Registry::default();
    registry.register("gated", true, |_: &Task| Ok(Step::Succeed(json!(null))));
    let standalone = task_scheduler::Scheduler::open(&path, registry).unwrap();
    standalone.submit(&task(id, false)).unwrap();
    drop(standalone);
    let scheduler = Scheduler::open(&path, options()).unwrap();
    assert!(
        matches!(scheduler.wait_for_idle(id, &CancellationToken::new()).await, Err(Error::Invalid(message)) if message.contains("task driver is not running"))
    );
    let mut registry = Registry::default();
    registry.register("gated", true, |_: &Task| Ok(Step::Succeed(json!(null))));
    let driver = scheduler.start_tasks(registry).unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.wait_for_idle(id, &CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

struct GatedModel {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl Model for GatedModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("ok".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}

#[tokio::test]
async fn waits_for_running_and_queued_submissions() {
    let dir = tempfile::tempdir().unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut opts = options();
    opts.model = Arc::new(GatedModel {
        started: started.clone(),
        release: release.clone(),
    });
    let mut scheduler = Scheduler::open(dir.path().join("db"), opts).unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    scheduler.submit(id, None, "first".into()).unwrap();
    started.notified().await;
    scheduler.submit(id, None, "second".into()).unwrap();
    let cancel = CancellationToken::new();
    let mut wait = Box::pin(scheduler.wait_for_idle(id, &cancel));
    assert!(matches!(poll!(&mut wait), Poll::Pending));
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    assert!(matches!(poll!(&mut wait), Poll::Pending));
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), &mut wait)
        .await
        .unwrap()
        .unwrap();
    drop(wait);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn follows_terminal_task_anchor_to_recovered_child() {
    use durable_store::Store;
    use pi_rs_durable::{
        ownership_edge::{Edge, EdgeStore, Mode},
        runtime::Harness,
        task_store::{Save, TaskStore},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (parent, child) = {
        let mut harness = Harness::open_paused(&path, options()).unwrap();
        let parent = harness.create_conversation(config()).unwrap();
        let child = harness.create_conversation(config()).unwrap();
        harness.submit(child, None, "child work".into()).unwrap();
        (parent, child)
    };
    {
        let db = Store::open(&path).unwrap();
        let anchor = task(parent, false);
        let tasks = TaskStore::new(&db);
        tasks.save(&[Save::Create(&anchor)]).unwrap();
        let edge = Edge {
            task: anchor.id(),
            task_owner: anchor.owner().clone(),
            child,
            mode: Mode::Foreground,
        };
        let batch = EdgeStore::new(&db).prepare_create(&edge).unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        let snapshot = tasks.load(anchor.id()).unwrap().unwrap();
        tasks.update(&snapshot, |t| t.start()).unwrap();
        let snapshot = tasks.load(anchor.id()).unwrap().unwrap();
        tasks.update(&snapshot, |t| t.succeed(json!(null))).unwrap();
    }
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut opts = options();
    opts.model = Arc::new(GatedModel {
        started: started.clone(),
        release: release.clone(),
    });
    let scheduler = Scheduler::open(&path, opts).unwrap();
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    let mut wait = Box::pin(scheduler.wait_for_idle(parent, &cancel));
    assert!(matches!(poll!(&mut wait), Poll::Pending));
    let mut all = Box::pin(scheduler.wait_for_all_idle(&cancel));
    assert!(matches!(poll!(&mut all), Poll::Pending));
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), &mut wait)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), &mut all)
        .await
        .unwrap()
        .unwrap();
    drop(wait);
    drop(all);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn conversation_abort_fences_nested_task_entry_and_document_transaction() {
    use durable_store::{Expected, Store};
    use pi_rs_durable::{
        application_entry,
        document::{Address, Documents, TaskDocuments, Transaction},
        task::{TaskState, WaitFor},
        task_scheduler::Handler,
        task_store::TaskStore,
    };

    struct Gated {
        started: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        address: Address,
    }
    impl Handler for Gated {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.get(&self.address).unwrap().unwrap();
            docs.put(
                &self.address,
                json!({"value": "after"}).as_object().unwrap().clone(),
            )
            .unwrap();
            docs.append_entry("app.nested", json!("after")).unwrap();
            self.started.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
            Ok(Step::Continue(Checkpoint {
                phase: "after".into(),
                data: json!(null),
            }))
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let address = Address {
        scope: "test".into(),
        name: "race".into(),
        key: "".into(),
    };
    {
        let db = Store::open(&path).unwrap();
        let mut tx = Transaction::new();
        tx.put(
            &address,
            Expected::Missing,
            json!({"value": "before"}).as_object().unwrap(),
        )
        .unwrap();
        tx.commit(&db).unwrap();
    }
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    let conversation = scheduler.create_conversation(config()).unwrap();
    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let checkpoint = Checkpoint {
        phase: "before".into(),
        data: json!(null),
    };
    let parent = Task::new(
        parent_id,
        TaskOwner::Conversation(conversation),
        checkpoint.clone(),
        3,
    )
    .unwrap()
    .registered("parent".into(), true);
    let child = Task::new(child_id, TaskOwner::Task(parent_id), checkpoint.clone(), 3)
        .unwrap()
        .registered("child".into(), true);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut registry = Registry::default();
    registry.register_with_abort(
        "parent",
        true,
        move |_: &Task| {
            Ok(Step::Wait {
                checkpoint: Checkpoint {
                    phase: "waiting".into(),
                    data: json!(null),
                },
                condition: WaitFor::Tasks(vec![child_id]),
                children: vec![child.clone()],
            })
        },
        |_: &Task| Ok(Step::Succeed(json!(null))),
    );
    registry.register_with_abort(
        "child",
        true,
        Gated {
            started: started_tx,
            release: Mutex::new(release_rx),
            address: address.clone(),
        },
        |_: &Task| Ok(Step::Succeed(json!(null))),
    );
    let driver = scheduler.start_tasks(registry).unwrap();
    driver.submit(&parent).unwrap();
    tokio::task::spawn_blocking(move || started_rx.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();

    // Abort must finish while the handler is still held, before its staged output can commit.
    let abort = tokio::task::spawn_blocking(move || {
        let result = scheduler.abort(conversation);
        (scheduler, result)
    });
    let result = tokio::time::timeout(Duration::from_secs(5), abort).await;
    release_tx.send(()).unwrap();
    let (scheduler, abort_result) = result
        .expect("abort deadlocked behind nested handler")
        .unwrap();
    abort_result.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.wait_for_idle(conversation, &CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    let shutdown = tokio::task::spawn_blocking(move || driver.shutdown());
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), scheduler.shutdown())
        .await
        .unwrap()
        .unwrap();

    let db = Store::open(&path).unwrap();
    assert!(
        application_entry::list(&db, conversation)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        Documents::new(&db).get(&address).unwrap().unwrap().value["value"],
        "before"
    );
    let tasks = TaskStore::new(&db);
    let child = tasks.load(child_id).unwrap().unwrap().task;
    assert_eq!(child.checkpoint(), &checkpoint);
    assert_eq!(child.state(), &TaskState::Cancelled);
    assert!(child.abort_requested());
    let parent = tasks.load(parent_id).unwrap().unwrap().task;
    assert_eq!(parent.checkpoint().phase, "waiting");
    assert_eq!(parent.state(), &TaskState::Cancelled);
    assert!(parent.abort_requested());
    drop(db);
    let reopened = Scheduler::open(&path, options()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), reopened.shutdown())
        .await
        .unwrap()
        .unwrap();
}
