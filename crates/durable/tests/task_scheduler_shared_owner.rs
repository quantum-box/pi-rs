#![cfg(unix)]

use durable_store::owner::{OwnedStore, OwnerError};
use pi_rs_durable::{
    task::{
        Checkpoint, Task, TaskOwner, TaskState,
        scheduler::{Error, Registry, Scheduler, Step},
    },
    task_store::{Save, TaskStore},
};
use serde_json::json;
use std::{
    io::Read,
    process::{Command, Stdio},
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

fn task(safe: bool) -> Task {
    Task::new(
        Uuid::from_u128(1),
        TaskOwner::Conversation(Uuid::from_u128(2)),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("work".into(), safe)
}
fn registry(safe: bool) -> Registry {
    let mut registry = Registry::default();
    registry.register("work", safe, |_: &Task| Ok(Step::Succeed(json!("done"))));
    registry
}

#[test]
fn attachment_fences_recovery_and_handler_until_completion() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let owner = Arc::new(OwnedStore::open(&path).unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    let calls = count.clone();
    let mut reg = Registry::default();
    reg.register("work", true, move |_: &Task| {
        calls.fetch_add(1, Ordering::SeqCst);
        entered_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
        Ok(Step::Succeed(json!("done")))
    });
    let mut scheduler = Scheduler::attach(owner.clone(), reg).unwrap();
    scheduler.submit(&task(true)).unwrap();
    let worker = thread::spawn(move || {
        scheduler.tick(0).unwrap();
        scheduler
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(matches!(
        Scheduler::attach(owner.clone(), registry(true)),
        Err(Error::AlreadyAttached)
    ));
    assert!(matches!(
        Scheduler::open(&path, registry(true)),
        Err(Error::Owner(OwnerError::Busy))
    ));
    assert_eq!(
        TaskStore::new(owner.store())
            .load(task(true).id())
            .unwrap()
            .unwrap()
            .task
            .state(),
        &TaskState::Running
    );
    release_tx.send(()).unwrap();
    let scheduler = worker.join().unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(matches!(
        Scheduler::attach(owner.clone(), registry(true)),
        Err(Error::AlreadyAttached)
    ));
    drop(scheduler);
    let mut next = Scheduler::attach(owner.clone(), registry(true)).unwrap();
    assert!(!next.tick(0).unwrap());
    assert_eq!(
        next.get(task(true).id()).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!("done"))
    );
    drop(owner);
    assert!(matches!(OwnedStore::open(&path), Err(OwnerError::Busy)));
    drop(next);
    assert!(OwnedStore::open(&path).is_ok());
}

#[test]
fn concurrent_attach_has_one_winner_and_missing_definition_stays_blocked() {
    let dir = tempfile::tempdir().unwrap();
    let owner = Arc::new(OwnedStore::open(dir.path().join("store.db")).unwrap());
    let start = Arc::new(Barrier::new(9));
    let finish = Arc::new(Barrier::new(9));
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let (owner, start, finish) = (owner.clone(), start.clone(), finish.clone());
            thread::spawn(move || {
                start.wait();
                let result = Scheduler::attach(owner, registry(true));
                finish.wait();
                result.is_ok()
            })
        })
        .collect();
    start.wait();
    finish.wait();
    assert_eq!(
        workers
            .into_iter()
            .map(|w| w.join().unwrap())
            .filter(|won| *won)
            .count(),
        1
    );
    let ts = TaskStore::new(owner.store());
    let t = task(true);
    ts.save(&[Save::Create(&t)]).unwrap();
    ts.reserve(&ts.load(t.id()).unwrap().unwrap()).unwrap();
    let blocked = Scheduler::attach(owner.clone(), Registry::default()).unwrap();
    assert_eq!(
        blocked.get(t.id()).unwrap().unwrap().state(),
        &TaskState::Running
    );
    drop(blocked);
    let mut recovered = Scheduler::attach(owner, registry(true)).unwrap();
    assert_eq!(recovered.get(t.id()).unwrap().unwrap().restarts(), 1);
    assert!(recovered.tick(0).unwrap());
    assert_eq!(
        recovered.get(t.id()).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!("done"))
    );
}

#[test]
fn child_running_handler() {
    let Ok(path) = std::env::var("TASK_SCHEDULER_CHILD_DB") else {
        return;
    };
    let ready = std::env::var("TASK_SCHEDULER_CHILD_READY").unwrap();
    let owner = Arc::new(OwnedStore::open(path).unwrap());
    let mut reg = Registry::default();
    reg.register("work", true, move |_: &Task| {
        std::fs::write(&ready, b"running").unwrap();
        let mut byte = [0];
        std::io::stdin().read_exact(&mut byte).unwrap();
        Ok(Step::Succeed(json!("done")))
    });
    let mut scheduler = Scheduler::attach(owner, reg).unwrap();
    scheduler.submit(&task(true)).unwrap();
    scheduler.tick(0).unwrap();
}

#[test]
fn killed_process_releases_owner_and_attached_scheduler_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let ready = dir.path().join("ready");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_running_handler", "--nocapture"])
        .env("TASK_SCHEDULER_CHILD_DB", &path)
        .env("TASK_SCHEDULER_CHILD_READY", &ready)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !ready.exists() {
        assert!(child.try_wait().unwrap().is_none());
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(10));
    }
    assert!(matches!(OwnedStore::open(&path), Err(OwnerError::Busy)));
    child.kill().unwrap();
    child.wait().unwrap();
    let owner = Arc::new(OwnedStore::open(&path).unwrap());
    let mut scheduler = Scheduler::attach(owner, registry(true)).unwrap();
    assert_eq!(
        scheduler.get(task(true).id()).unwrap().unwrap().restarts(),
        1
    );
    assert!(scheduler.tick(0).unwrap());
    assert_eq!(
        scheduler.get(task(true).id()).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!("done"))
    );
}
