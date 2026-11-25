#![cfg(feature = "test-hooks")]

use pi_rs_durable::{
    task::{Checkpoint, Task, TaskOwner, TaskState},
    task_scheduler::{
        Error, Registry, Scheduler, SettlementError, Step, TickReservation, hook_fence_test_hook,
    },
};
use serde_json::json;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use uuid::Uuid;

fn task(id: u128, kind: &str, replay_safe: bool) -> Task {
    Task::new(
        Uuid::from_u128(id),
        TaskOwner::Conversation(Uuid::from_u128(200 + id)),
        Checkpoint {
            phase: "initial".into(),
            data: json!(null),
        },
        4,
    )
    .unwrap()
    .registered(kind.into(), replay_safe)
}

fn hold_writer_at_settlement(
    id: Uuid,
    path: PathBuf,
    phase: &'static str,
) -> std::sync::mpsc::Receiver<std::thread::JoinHandle<()>> {
    let (writer_tx, writer_rx) = std::sync::mpsc::channel();
    hook_fence_test_hook::install(id, phase, move || {
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            let conn = rusqlite::Connection::open(path).unwrap();
            conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            locked_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(6500));
            conn.execute_batch("COMMIT").unwrap();
        });
        locked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        writer_tx.send(writer).unwrap();
    });
    writer_rx
}

#[test]
fn single_settlement_preserves_unsafe_outcome_and_lease_during_busy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut registry = Registry::default();
    registry.register("unsafe", false, move |_: &Task| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(Step::Succeed(json!("observed")))
    });
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    let task = task(1, "unsafe", false);
    scheduler.submit(&task).unwrap();
    let TickReservation::Invocation(job) = scheduler.reserve_tick(0).unwrap() else {
        panic!("missing invocation")
    };
    let completed = job.execute().unwrap();
    let writer = hold_writer_at_settlement(task.id(), path, "settlement");
    let pending = match scheduler.settle_invocation(completed) {
        Err(SettlementError::PendingPrepared { pending }) => pending,
        other => panic!("expected retained prepared outcome: {other:?}"),
    };
    assert_eq!(pending.task_id(), task.id());
    assert!(pending.attempt_id().is_some());
    writer
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .join()
        .unwrap();
    assert!(scheduler.try_resume_pending(*pending).unwrap());
    assert_eq!(
        scheduler.get(task.id()).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!("observed"))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn tick_busy_rechecks_abort_and_allows_independent_graph() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut registry = Registry::default();
    registry.register_with_abort(
        "unsafe",
        false,
        move |_: &Task| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(Step::Succeed(json!("discard")))
        },
        |_: &Task| Ok(Step::Succeed(json!(null))),
    );
    registry.register("independent", true, |_: &Task| {
        Ok(Step::Succeed(json!("done")))
    });
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    let target = task(11, "unsafe", false);
    let independent = task(12, "independent", true);
    scheduler.submit(&target).unwrap();
    scheduler.submit(&independent).unwrap();
    let writer = hold_writer_at_settlement(target.id(), path, "settlement");
    assert!(matches!(scheduler.tick(0), Err(Error::Busy)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let handle = writer.recv_timeout(Duration::from_secs(5)).unwrap();
    scheduler.request_abort(target.id()).unwrap();
    handle.join().unwrap();
    for _ in 0..20 {
        match scheduler.tick(0) {
            Ok(_) | Err(Error::Busy) => (),
            Err(error) => panic!("tick failed: {error}"),
        }
        if scheduler.get(target.id()).unwrap().unwrap().is_terminal()
            && scheduler
                .get(independent.id())
                .unwrap()
                .unwrap()
                .is_terminal()
        {
            break;
        }
    }
    let target = scheduler.get(target.id()).unwrap().unwrap();
    assert_eq!(target.state(), &TaskState::Cancelled);
    assert!(target.abort_requested());
    assert_eq!(
        scheduler.get(independent.id()).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!("done"))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(scheduler.take_serial_pending().is_none());
}

#[test]
fn tick_pending_transfers_to_driver_without_reexecuting_unsafe_work() {
    use pi_rs_durable::task_driver::Driver;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut registry = Registry::default();
    registry.register("unsafe", false, move |_: &Task| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(Step::Succeed(json!("once")))
    });
    registry.register("independent", true, |_: &Task| {
        Ok(Step::Succeed(json!("independent")))
    });
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    let target = task(21, "unsafe", false);
    let independent = task(22, "independent", true);
    scheduler.submit(&target).unwrap();
    scheduler.submit(&independent).unwrap();
    let writer = hold_writer_at_settlement(target.id(), path, "settlement");
    assert!(matches!(scheduler.tick(0), Err(Error::Busy)));
    writer
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .join()
        .unwrap();
    let driver = Driver::start(scheduler);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if driver.get(target.id()).unwrap().unwrap().is_terminal()
            && driver.get(independent.id()).unwrap().unwrap().is_terminal()
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "transferred settlement did not finish"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        driver.get(target.id()).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!("once"))
    );
    assert_eq!(
        driver.get(independent.id()).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!("independent"))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
}

#[test]
fn single_failure_settlement_retains_reason_until_retry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut registry = Registry::default();
    registry.register("failure", true, move |_: &Task| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(Step::Fail("exact failure".into()))
    });
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    let task = task(31, "failure", true);
    scheduler.submit(&task).unwrap();
    let TickReservation::Invocation(job) = scheduler.reserve_tick(0).unwrap() else {
        panic!("missing invocation")
    };
    let completed = job.execute().unwrap();
    let writer = hold_writer_at_settlement(task.id(), path, "failure-settlement");
    let pending = match scheduler.settle_invocation(completed) {
        Err(SettlementError::PendingPrepared { pending }) => pending,
        other => panic!("expected retained failure: {other:?}"),
    };
    assert_eq!(pending.task_id(), task.id());
    writer
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .join()
        .unwrap();
    assert!(scheduler.try_resume_pending(*pending).unwrap());
    assert_eq!(
        scheduler.get(task.id()).unwrap().unwrap().state(),
        &TaskState::Failed("exact failure".into())
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
