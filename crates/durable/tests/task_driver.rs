use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{CancellationToken, Error, Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::{
    recovery::UncertainPolicy,
    runtime::{Config, Options, Scheduler},
    task::{Checkpoint, Task, TaskOwner, TaskState},
    task_scheduler::{Registry, Step},
};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;
struct FakeModel;
#[async_trait]
impl Model for FakeModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
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
        model: Arc::new(FakeModel),
        tools: vec![],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn registry() -> Registry {
    let mut registry = Registry::default();
    registry.register("test", true, |_: &Task| {
        Ok(Step::Succeed(json!("finished")))
    });
    registry
}
#[tokio::test]
async fn generic_tasks_run_under_the_same_owner_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let id = Uuid::new_v4();
    {
        let mut s = Scheduler::open(&path, options()).unwrap();
        let conversation = s
            .create_conversation(Config {
                model: "m".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        let driver = s.start_tasks(registry()).unwrap();
        assert!(s.start_tasks(registry()).is_err());
        let task = Task::new(
            id,
            TaskOwner::Conversation(conversation),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            5,
        )
        .unwrap()
        .registered("test".into(), true);
        driver.submit(&task).unwrap();
        let settled = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            driver.wait_for_task(id, &CancellationToken::new()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(settled.state(), &TaskState::Succeeded(json!("finished")));
        driver.shutdown().unwrap();
        s.shutdown().await.unwrap();
    }
    let s = Scheduler::open(&path, options()).unwrap();
    let driver = s.start_tasks(registry()).unwrap();
    assert_eq!(
        driver
            .wait_for_task(id, &CancellationToken::new())
            .await
            .unwrap()
            .state(),
        &TaskState::Succeeded(json!("finished"))
    );
    driver.shutdown().unwrap();
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn wait_cancellation_and_completion_races_do_not_abort_work() {
    use pi_rs_durable::task_driver::Error as DriverError;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let mut s = Scheduler::open(dir.path().join("db"), options()).unwrap();
    let conversation = s
        .create_conversation(Config {
            model: "m".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    let mut registry = Registry::default();
    registry.register("gated", true, move |_: &Task| {
        started_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
        Ok(Step::Succeed(json!(42)))
    });
    let driver = s.start_tasks(registry).unwrap();
    let id = Uuid::new_v4();
    let task = Task::new(
        id,
        TaskOwner::Conversation(conversation),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        5,
    )
    .unwrap()
    .registered("gated".into(), true);
    driver.submit(&task).unwrap();
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        driver
            .wait_for_task(Uuid::new_v4(), &CancellationToken::new())
            .await,
        Err(DriverError::UnknownTask(_))
    ));
    {
        let cancel = CancellationToken::new();
        let wait = driver.wait_for_task(id, &cancel);
        tokio::pin!(wait);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut wait)
                .await
                .is_err()
        );
        cancel.cancel();
        assert!(matches!(wait.await, Err(DriverError::WaitCancelled)));
    }
    assert!(!driver.get(id).unwrap().unwrap().abort_requested());
    let token = CancellationToken::new();
    let (a, b) = tokio::join!(
        async {
            release_tx.send(()).unwrap();
            driver.wait_for_task(id, &token).await
        },
        driver.wait_for_task(id, &token)
    );
    assert_eq!(a.unwrap().state(), &TaskState::Succeeded(json!(42)));
    assert_eq!(b.unwrap().state(), &TaskState::Succeeded(json!(42)));
    driver.shutdown().unwrap();
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn wait_for_preexisting_live_task_after_reopen() {
    use pi_rs_durable::{task_driver::Driver, task_scheduler};
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let id = Uuid::new_v4();
    let task = Task::new(
        id,
        TaskOwner::Conversation(Uuid::new_v4()),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        5,
    )
    .unwrap()
    .registered("test".into(), true);
    let scheduler = task_scheduler::Scheduler::open(&path, registry()).unwrap();
    scheduler.submit(&task).unwrap();
    drop(scheduler);
    let scheduler = task_scheduler::Scheduler::open(&path, registry()).unwrap();
    let driver = Driver::start(scheduler);
    let settled = tokio::time::timeout(
        Duration::from_secs(5),
        driver.wait_for_task(id, &CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(settled.state(), &TaskState::Succeeded(json!("finished")));
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn wait_returns_failed_task_outcome() {
    use pi_rs_durable::{task_driver::Driver, task_scheduler};
    let dir = tempfile::tempdir().unwrap();
    let id = Uuid::new_v4();
    let task = Task::new(
        id,
        TaskOwner::Conversation(Uuid::new_v4()),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        5,
    )
    .unwrap()
    .registered("fail".into(), true);
    let mut registry = Registry::default();
    registry.register("fail", true, |_: &Task| Ok(Step::Fail("expected".into())));
    let scheduler = task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
    scheduler.submit(&task).unwrap();
    let driver = Driver::start(scheduler);
    let settled = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        driver.wait_for_task(id, &CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(settled.state(), &TaskState::Failed("expected".into()));
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn signal_is_admitted_while_handler_is_running() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Scheduler::open(dir.path().join("db"), options()).unwrap();
    let conversation = s
        .create_conversation(Config {
            model: "m".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release = Arc::new(std::sync::Mutex::new(release_rx));
    let mut registry = Registry::default();
    registry.register("gated", true, move |_: &Task| {
        started_tx.send(()).unwrap();
        release.lock().unwrap().recv().unwrap();
        Ok(Step::Succeed(json!("done")))
    });
    let driver = Arc::new(s.start_tasks(registry).unwrap());
    let id = Uuid::new_v4();
    let task = Task::new(
        id,
        TaskOwner::Conversation(conversation),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        5,
    )
    .unwrap()
    .registered("gated".into(), true);
    driver.submit(&task).unwrap();
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let d = driver.clone();
    let mut signal = tokio::task::spawn_blocking(move || {
        d.signal(&pi_rs_durable::task_store::Signal::new(
            Uuid::new_v4(),
            id,
            "wake".into(),
            json!(null),
        ))
    });
    let before_release = tokio::time::timeout(std::time::Duration::from_secs(2), &mut signal).await;
    release_tx.send(()).unwrap();
    assert!(
        before_release.is_ok(),
        "signal was blocked by the running handler"
    );
    before_release.unwrap().unwrap().unwrap();
    Arc::try_unwrap(driver).ok().unwrap().shutdown().unwrap();
    s.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_delivery_wakes_background_task() {
    use pi_rs_durable::task::WaitFor;
    use pi_rs_durable::task_store::{SignalResult, SignalToken};
    let dir = tempfile::tempdir().unwrap();
    let mut scheduler = Scheduler::open(dir.path().join("db"), options()).unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "m".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    let token = SignalToken::new("approval", 1).unwrap();
    let key = token.key("decision").unwrap();
    let mut registry = Registry::default();
    registry.register("waiter", true, move |task: &Task| {
        if task.checkpoint().phase == "start" {
            Ok(Step::Wait {
                checkpoint: Checkpoint {
                    phase: "done".into(),
                    data: json!(null),
                },
                condition: WaitFor::TypedSignal(key.clone()),
                children: vec![],
            })
        } else {
            Ok(Step::Succeed(json!(task.delivered_signal().is_some())))
        }
    });
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = Uuid::new_v4();
    let task = Task::new(
        id,
        TaskOwner::Conversation(conversation),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        5,
    )
    .unwrap()
    .registered("waiter".into(), true);
    driver.submit(&task).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if matches!(
                driver.get(id).unwrap().unwrap().state(),
                TaskState::Waiting(_)
            ) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        driver
            .deliver_signal(id, &token, "decision", json!(true))
            .unwrap(),
        SignalResult::Delivered
    );
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if matches!(driver.get(id).unwrap().unwrap().state(), TaskState::Succeeded(value) if value == &json!(true)) { break; }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}
