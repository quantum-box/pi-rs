use pi_rs_durable::{
    task::{Checkpoint, Task, TaskOwner, WaitFor},
    task_driver::Driver,
    task_scheduler::{BlockedReason, Registry, Scheduler, Step},
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use uuid::Uuid;

fn task(id: u128, kind: &str) -> Task {
    Task::new(
        Uuid::from_u128(id),
        TaskOwner::Conversation(Uuid::from_u128(100)),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered(kind.into(), true)
}

fn registry(calls: Arc<AtomicUsize>) -> Registry {
    let mut registry = Registry::default();
    registry.register("nap", true, move |t: &Task| {
        calls.fetch_add(1, Ordering::SeqCst);
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "sleep".into(),
                data: json!(null),
            },
            condition: WaitFor::DeadlineMillis(if t.id() == Uuid::from_u128(1) {
                100
            } else {
                200
            }),
            children: vec![],
        })
    });
    registry
}

#[test]
fn clock_reopen_and_registry_snapshot_do_not_run_handlers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    let mut scheduler = Scheduler::open(&path, registry(calls.clone())).unwrap();
    scheduler.submit(&task(1, "nap")).unwrap();
    scheduler.submit(&task(2, "nap")).unwrap();
    assert!(scheduler.wake_snapshot(0).unwrap().active);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    scheduler.tick(0).unwrap();
    scheduler.tick(0).unwrap();
    let before = scheduler.wake_snapshot(99).unwrap();
    assert!(!before.active);
    assert_eq!(before.at, Some(100));
    assert!(before.blocked.is_empty());
    let due = scheduler.wake_snapshot(100).unwrap();
    assert!(due.active);
    assert_eq!(due.at, Some(200));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    drop(scheduler);
    let mut scheduler = Scheduler::open(&path, Registry::default()).unwrap();
    let reopened = scheduler.wake_snapshot(99).unwrap();
    assert_eq!(reopened.at, Some(100));
    assert_eq!(reopened.blocked.len(), 2);
    assert!(
        reopened
            .blocked
            .iter()
            .all(|b| b.reason == BlockedReason::MissingTask)
    );
    scheduler.replace_registry(registry(calls.clone())).unwrap();
    assert!(scheduler.wake_snapshot(99).unwrap().blocked.is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn driver_exposes_blocked_snapshot_and_coalesced_registry_hints() {
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    let scheduler = Scheduler::open(&path, registry(calls.clone())).unwrap();
    scheduler.submit(&task(1, "nap")).unwrap();
    drop(scheduler);
    let driver = Driver::start(Scheduler::open(&path, Registry::default()).unwrap());
    let rx = driver.subscribe_wake().unwrap().unwrap();
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let snap = driver.wake_snapshot(0).unwrap();
    assert!(!snap.active);
    assert_eq!(snap.blocked.len(), 1);
    for _ in 0..10 {
        driver.replace_registry(Registry::default()).unwrap();
    }
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(rx.try_recv().is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
}

#[test]
fn inspecting_a_signalled_wait_does_not_consume_it() {
    use pi_rs_durable::{task::TaskState, task_store::Signal};
    let dir = tempfile::tempdir().unwrap();
    let mut registry = Registry::default();
    registry.register("signal", true, |_: &Task| {
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "waiting".into(),
                data: json!(null),
            },
            condition: WaitFor::Sleep {
                until_millis: Some(100),
                signals: vec!["go".into()],
            },
            children: vec![],
        })
    });
    let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
    let task = task(3, "signal");
    scheduler.submit(&task).unwrap();
    scheduler.tick(0).unwrap();
    assert_eq!(scheduler.wake_snapshot(0).unwrap().at, Some(100));
    let signal = Signal::new(Uuid::new_v4(), task.id(), "go".into(), json!(1));
    scheduler.signal(&signal).unwrap();
    for _ in 0..2 {
        let snap = scheduler.wake_snapshot(0).unwrap();
        assert!(snap.active);
        assert_eq!(snap.at, None);
        assert!(matches!(
            scheduler.get(task.id()).unwrap().unwrap().state(),
            TaskState::Waiting(_)
        ));
    }
    scheduler.tick(0).unwrap();
    let after = scheduler.wake_snapshot(0).unwrap();
    assert!(!after.active);
    assert_eq!(after.at, Some(100));
}

#[test]
fn driver_snapshot_reads_deadline_during_live_handler() {
    use std::{
        sync::{Mutex, mpsc},
        time::Duration,
    };
    let dir = tempfile::tempdir().unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let mut registry = Registry::default();
    registry.register("nap", true, |_: &Task| {
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "sleep".into(),
                data: json!(null),
            },
            condition: WaitFor::DeadlineMillis(i64::MAX - 1),
            children: vec![],
        })
    });
    registry.register("hold", true, move |_: &Task| {
        entered_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
        Ok(Step::Succeed(json!(null)))
    });
    let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
    scheduler.submit(&task(1, "nap")).unwrap();
    assert!(scheduler.tick(0).unwrap());
    scheduler.submit(&task(2, "hold")).unwrap();
    let driver = Arc::new(Driver::start(scheduler));
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let copy = driver.clone();
    let (tx, rx) = mpsc::channel();
    let observer = std::thread::spawn(move || {
        tx.send(copy.wake_snapshot(0)).unwrap();
    });
    let snapshot = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("snapshot waited for handler")
        .unwrap();
    assert!(snapshot.active);
    assert_eq!(snapshot.at, Some(i64::MAX - 1));
    observer.join().unwrap();
    release_tx.send(()).unwrap();
    Arc::try_unwrap(driver).ok().unwrap().shutdown().unwrap();
}

#[test]
fn driver_wake_snapshot_rejects_panicked_worker() {
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let (release, wait) = std::sync::mpsc::channel();
    let wait = std::sync::Mutex::new(wait);
    let mut registry = Registry::default();
    registry.register("panic", true, move |_: &Task| -> Result<Step, String> {
        wait.lock().unwrap().recv().unwrap();
        panic!("expected handler panic")
    });
    let scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
    scheduler.submit(&task(7, "panic")).unwrap();
    let driver = Driver::start(scheduler);
    let rx = driver.subscribe_wake().unwrap().unwrap();
    release.send(()).unwrap();
    loop {
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(()) => (),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            Err(error) => panic!("worker did not stop: {error}"),
        }
    }
    assert!(driver.wake_snapshot(0).is_err());
    assert!(driver.shutdown().is_err());
}
