use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{Error, Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::{
    recovery::UncertainPolicy,
    runtime::{Config, Options, Scheduler},
    task::{Checkpoint, Task, TaskOwner},
    task_scheduler::{Registry, Step},
    worker_activity::{WorkerKind, WorkerPhase},
};
use serde_json::json;
use std::{
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};
use tokio::sync::Semaphore;
use uuid::Uuid;

struct HoldingModel {
    entered: Semaphore,
    release: Semaphore,
}
#[async_trait]
impl Model for HoldingModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
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
fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: "".into(),
        max_output_tokens: 100,
    }
}
fn options(model: Arc<dyn Model>) -> Options {
    Options {
        model,
        tools: vec![],
        restart_limit: 3,
        max_turns: 5,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn task(id: Uuid, conversation: Uuid) -> Task {
    Task::new(
        id,
        TaskOwner::Conversation(conversation),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("work".into(), true)
}

#[tokio::test]
async fn shared_activity_tracks_concurrent_workers_abort_and_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(HoldingModel {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let mut scheduler = Scheduler::open(dir.path().join("db"), options(model.clone())).unwrap();
    let conversation = scheduler.create_conversation(config()).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let mut registry = Registry::default();
    registry.register("work", true, move |_: &Task| {
        entered_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
        Ok(Step::Succeed(json!(null)))
    });
    let driver = scheduler.start_tasks(registry).unwrap();
    let task_id = Uuid::new_v4();
    driver.submit(&task(task_id, conversation)).unwrap();
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();
    scheduler.submit(conversation, None, "go".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), model.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let activity = scheduler.worker_activity();
    let snapshot = activity.snapshot();
    assert!(
        snapshot.iter().any(|w| w.kind == WorkerKind::Task
            && w.id == task_id
            && w.phase == WorkerPhase::Running)
    );
    assert!(snapshot.iter().any(|w| w.kind == WorkerKind::Conversation
        && w.id == conversation
        && w.phase == WorkerPhase::Running));
    assert_eq!(driver.worker_activity().snapshot(), snapshot);
    scheduler.abort(conversation).unwrap();
    assert!(activity.snapshot().iter().any(|w| w.id == task_id));
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), scheduler.wait(conversation))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while activity.snapshot().iter().any(|w| w.id == task_id) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    scheduler.shutdown().await.unwrap();
    tokio::task::spawn_blocking(move || driver.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(activity.snapshot().is_empty());
}

#[test]
fn panicking_task_drops_live_activity() {
    use pi_rs_durable::task_scheduler::Scheduler as TaskScheduler;
    let dir = tempfile::tempdir().unwrap();
    let mut registry = Registry::default();
    registry.register("work", true, |_: &Task| -> Result<Step, String> {
        panic!("handler panic")
    });
    let mut scheduler = TaskScheduler::open(dir.path().join("db"), registry).unwrap();
    let id = Uuid::new_v4();
    scheduler.submit(&task(id, Uuid::new_v4())).unwrap();
    let activity = scheduler.worker_activity();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| scheduler.tick(0))).is_err());
    assert!(activity.snapshot().is_empty());
}

struct PendingModel;
#[async_trait]
impl Model for PendingModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        Ok(stream::iter(vec![Ok(ModelEvent::Complete {
            calls: vec![pi_rs_core::ToolCall {
                id: "call".into(),
                name: "job".into(),
                arguments: "{}".into(),
            }],
            usage: None,
        })])
        .boxed())
    }
}
struct PendingJob;
#[async_trait]
impl pi_rs_core::Tool for PendingJob {
    fn spec(&self) -> pi_rs_core::ToolSpec {
        pi_rs_core::ToolSpec {
            name: "job".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    fn can_poll(&self) -> bool {
        true
    }
    async fn invoke(
        &self,
        _: serde_json::Value,
        _: pi_rs_core::ToolContext,
    ) -> pi_rs_core::Outcome {
        pi_rs_core::Outcome::Pending {
            reference: "opaque".into(),
        }
    }
    async fn poll(&self, _: &str, _: pi_rs_core::ToolContext) -> Option<pi_rs_core::Outcome> {
        None
    }
}

#[tokio::test]
async fn concurrent_conversation_pollers_report_wait_and_cancel() {
    use pi_rs_durable::{recovery::ReplayPolicy, runtime::RegisteredTool};
    let dir = tempfile::tempdir().unwrap();
    let mut opts = options(Arc::new(PendingModel));
    opts.tools.push(RegisteredTool {
        tool: Arc::new(PendingJob),
        replay: ReplayPolicy::Unsafe,
    });
    let mut scheduler = Scheduler::open(dir.path().join("db"), opts).unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    scheduler.submit(id, None, "run".into()).unwrap();
    let activity = scheduler.worker_activity();
    let waiting = || {
        activity
            .snapshot()
            .iter()
            .filter(|w| {
                w.kind == WorkerKind::Conversation && w.id == id && w.phase == WorkerPhase::Waiting
            })
            .count()
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while waiting() != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    scheduler.schedule(id);
    tokio::time::timeout(Duration::from_secs(5), async {
        while waiting() != 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    scheduler.abort(id).unwrap();
    tokio::time::timeout(Duration::from_secs(5), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    scheduler.shutdown().await.unwrap();
    assert!(activity.snapshot().is_empty());
}

struct PanicModel;
#[async_trait]
impl Model for PanicModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        panic!("model panic")
    }
}
#[tokio::test]
async fn panicking_conversation_drops_activity() {
    let dir = tempfile::tempdir().unwrap();
    let mut scheduler =
        Scheduler::open(dir.path().join("db"), options(Arc::new(PanicModel))).unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    let activity = scheduler.worker_activity();
    scheduler.submit(id, None, "run".into()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), scheduler.wait(id))
            .await
            .unwrap()
            .is_err()
    );
    assert!(activity.snapshot().is_empty());
    drop(scheduler);
}

#[test]
fn driver_shutdown_keeps_running_attempt_visible_until_joined() {
    use pi_rs_durable::{task_driver::Driver, task_scheduler::Scheduler as TaskScheduler};
    let dir = tempfile::tempdir().unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let mut registry = Registry::default();
    registry.register("work", true, move |_: &Task| {
        started_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
        Ok(Step::Succeed(json!(null)))
    });
    let scheduler = TaskScheduler::open(dir.path().join("db"), registry).unwrap();
    let id = Uuid::new_v4();
    scheduler.submit(&task(id, Uuid::new_v4())).unwrap();
    let driver = Driver::start(scheduler);
    let activity = driver.worker_activity();
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let (stopping_tx, stopping_rx) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        stopping_tx.send(()).unwrap();
        driver.shutdown()
    });
    stopping_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        activity
            .snapshot()
            .iter()
            .any(|w| w.id == id && w.phase == WorkerPhase::Running)
    );
    release_tx.send(()).unwrap();
    thread.join().unwrap().unwrap();
    assert!(activity.snapshot().is_empty());
}
