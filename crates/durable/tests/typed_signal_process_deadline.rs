//! Typed signal deadline decisions survive interruption after the wake is reserved.
#![cfg(unix)]

use durable_store::Store;
use pi_rs_durable::{
    task::{
        Checkpoint, Task, TaskOwner, TaskState, TypedWake, WaitFor,
        scheduler::{Registry, Scheduler, Step},
    },
    task_store::{SignalResult, SignalState, SignalToken, TaskStore},
};
use serde_json::json;
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::process::ExitStatusExt,
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};
use uuid::Uuid;

const ID: Uuid = Uuid::from_u128(1);
fn token() -> SignalToken {
    SignalToken::new("deadline.answer", 1).unwrap()
}
fn cp(phase: &str) -> Checkpoint {
    Checkpoint {
        phase: phase.into(),
        data: json!(null),
    }
}
fn registry(pause: bool, early: bool) -> Registry {
    let mut registry = Registry::default();
    registry.register("deadline", true, move |task: &Task| {
        if task.checkpoint().phase == "start" {
            return Ok(Step::Wait {
                checkpoint: cp("wake"),
                condition: WaitFor::TypedSleep {
                    until_millis: Some(10_000),
                    signals: vec![token().key("a").unwrap(), token().key("b").unwrap()],
                },
                children: vec![],
            });
        }
        let expected = if early {
            TypedWake::Signal(token().key("a").unwrap())
        } else {
            TypedWake::Expired
        };
        assert_eq!(task.typed_wake(), Some(&expected));
        if pause {
            println!("DEADLINE_WAKE_RESERVED");
            std::io::stdout().flush().unwrap();
            loop {
                std::thread::park();
            }
        }
        Ok(Step::Succeed(json!("resumed")))
    });
    registry
}

#[test]
fn deadline_child() {
    let Some(path) = std::env::var_os("PI_TYPED_DEADLINE_DB") else {
        return;
    };
    let early = std::env::var_os("PI_TYPED_DEADLINE_EARLY").is_some();
    let mut scheduler = Scheduler::open(&path, registry(true, early)).unwrap();
    let task = Task::new(
        ID,
        TaskOwner::Conversation(Uuid::from_u128(9)),
        cp("start"),
        3,
    )
    .unwrap()
    .registered("deadline".into(), true);
    scheduler.submit(&task).unwrap();
    assert!(scheduler.tick(0).unwrap());
    let db = Store::open(&path).unwrap();
    let ts = TaskStore::new(&db);
    let result = ts
        .deliver_signal_at(ID, &token(), "a", json!(7), if early { 0 } else { 10_000 })
        .unwrap();
    assert_eq!(
        result,
        if early {
            SignalResult::Delivered
        } else {
            SignalResult::Closed
        }
    );
    drop(db);
    if std::env::var_os("PI_TYPED_DEADLINE_PREWAKE").is_some() {
        println!("DEADLINE_RECEIPT_COMMITTED");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
    scheduler.tick(10_000).unwrap();
    panic!("wake handler should have blocked");
}

struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn kill_child(path: &std::path::Path, early: bool, prewake: bool) {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "deadline_child", "--nocapture"])
        .env("PI_TYPED_DEADLINE_DB", path)
        .env_remove("PI_TYPED_DEADLINE_EARLY")
        .env_remove("PI_TYPED_DEADLINE_PREWAKE")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    if early {
        command.env("PI_TYPED_DEADLINE_EARLY", "1");
    }
    if prewake {
        command.env("PI_TYPED_DEADLINE_PREWAKE", "1");
    }
    let mut child = KillOnDrop(command.spawn().unwrap());
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let marker = if prewake {
            "DEADLINE_RECEIPT_COMMITTED"
        } else {
            "DEADLINE_WAKE_RESERVED"
        };
        for line in BufReader::new(stdout).lines() {
            if line.unwrap().contains(marker) {
                let _ = tx.send(());
                break;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(20))
        .expect("child did not reach the requested commit boundary");
    child.0.kill().unwrap();
    assert_eq!(child.0.wait().unwrap().signal(), Some(9));
}

#[test]
fn deadline_wake_and_receipts_survive_kill_after_reservation() {
    for early in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        kill_child(&path, early, false);
        let mut scheduler = Scheduler::open(&path, registry(false, early)).unwrap();
        let recovered = scheduler.get(ID).unwrap().unwrap();
        assert_eq!(recovered.state(), &TaskState::Ready);
        assert_eq!(recovered.checkpoint(), &cp("wake"));
        assert_eq!(recovered.restarts(), 1);
        assert_eq!(
            recovered.typed_wake(),
            Some(&if early {
                TypedWake::Signal(token().key("a").unwrap())
            } else {
                TypedWake::Expired
            })
        );
        let db = Store::open(&path).unwrap();
        let ts = TaskStore::new(&db);
        assert_eq!(
            ts.read_typed_signal_state(ID, &token(), "a").unwrap(),
            Some(if early {
                SignalState::Value(json!(7))
            } else {
                SignalState::Expired
            })
        );
        assert_eq!(
            ts.read_typed_signal_state(ID, &token(), "b").unwrap(),
            if early {
                None
            } else {
                Some(SignalState::Expired)
            }
        );
        assert_eq!(
            ts.deliver_signal_at(ID, &token(), "a", json!(8), 10_000)
                .unwrap(),
            if early {
                SignalResult::Duplicate
            } else {
                SignalResult::Closed
            }
        );
        assert_eq!(
            ts.deliver_signal_at(ID, &token(), "b", json!(9), 10_000)
                .unwrap(),
            if early {
                SignalResult::Delivered
            } else {
                SignalResult::Closed
            }
        );
        drop(db);
        assert!(scheduler.tick(10_000).unwrap());
        assert_eq!(
            scheduler.get(ID).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("resumed"))
        );
        drop(scheduler);
        let reopened = Scheduler::open(&path, Registry::default()).unwrap();
        assert_eq!(
            reopened.get(ID).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("resumed"))
        );
    }
}

#[test]
fn deadline_rejection_survives_kill_before_wake() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    kill_child(&path, false, true);
    let mut scheduler = Scheduler::open(&path, registry(false, false)).unwrap();
    assert!(matches!(
        scheduler.get(ID).unwrap().unwrap().state(),
        TaskState::Waiting(_)
    ));
    let db = Store::open(&path).unwrap();
    let ts = TaskStore::new(&db);
    assert_eq!(
        ts.read_typed_signal_state(ID, &token(), "a").unwrap(),
        Some(SignalState::Expired)
    );
    assert_eq!(ts.read_typed_signal_state(ID, &token(), "b").unwrap(), None);
    assert_eq!(
        ts.deliver_signal_at(ID, &token(), "a", json!(8), 10_000)
            .unwrap(),
        SignalResult::Closed
    );
    drop(db);
    assert!(scheduler.tick(10_000).unwrap());
    assert_eq!(
        scheduler.get(ID).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!("resumed"))
    );
    drop(scheduler);
    let db = Store::open(&path).unwrap();
    let ts = TaskStore::new(&db);
    assert_eq!(
        ts.read_typed_signal_state(ID, &token(), "b").unwrap(),
        Some(SignalState::Expired)
    );
    assert_eq!(
        ts.deliver_signal_at(ID, &token(), "b", json!(9), 10_000)
            .unwrap(),
        SignalResult::Closed
    );
}
